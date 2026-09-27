package queue

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

const dedupMagic uint16 = 0xDEDA

const (
	dedupOpBegin  byte = 0
	dedupOpCommit byte = 1
	dedupOpForget byte = 2
)

// DedupKind 是幂等判定结果。
type DedupKind int

const (
	DedupNew      DedupKind = iota // 首次执行
	DedupPending                   // 另一协程/崩溃前正在执行
	DedupComplete                  // 副作用已生效，必须跳过
)

func (k DedupKind) String() string {
	switch k {
	case DedupNew:
		return "NEW"
	case DedupPending:
		return "PENDING"
	case DedupComplete:
		return "COMPLETE"
	}
	return "UNKNOWN"
}

type dedupStatus byte

const (
	statusPending  dedupStatus = 0
	statusComplete dedupStatus = 1
)

// dedupStore 是幂等键的持久化去重表（data/dedup.log）。
// 所有变更先 fsync 追加日志再更新内存，保证崩溃后状态不丢。
type dedupStore struct {
	path       string
	f          *os.File
	status     map[string]dedupStatus
	completed  []string       // 已完成键的到达顺序，用于历史窗口淘汰
	completeOf map[string]int // completed 中位置
	history    int
}

func openDedupStore(dir string, history int) (*dedupStore, error) {
	if history <= 0 {
		history = 100_000
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "dedup.log")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	d := &dedupStore{
		path:       path,
		f:          f,
		status:     make(map[string]dedupStatus),
		completeOf: make(map[string]int),
		history:    history,
	}
	if err := d.recover(); err != nil {
		f.Close()
		return nil, err
	}
	return d, nil
}

func encodeDedupRec(op byte, key string) []byte {
	body := make([]byte, 0, 4+1+4+len(key))
	crcBuf := make([]byte, 4)
	body = append(body, crcBuf...)
	body = append(body, op)
	var l [4]byte
	binary.BigEndian.PutUint32(l[:], uint32(len(key)))
	body = append(body, l[:]...)
	body = append(body, key...)
	crc := crc32.ChecksumIEEE(body[4:])
	binary.BigEndian.PutUint32(body[:4], crc)

	rec := make([]byte, 0, 6+len(body))
	var hdr [6]byte
	binary.BigEndian.PutUint16(hdr[:], dedupMagic)
	binary.BigEndian.PutUint32(hdr[2:], uint32(len(body)))
	rec = append(rec, hdr[:]...)
	rec = append(rec, body...)
	return rec
}

// recover 扫描日志重建内存表，截断尾部撕裂记录。
func (d *dedupStore) recover() error {
	data, err := os.ReadFile(d.path)
	if err != nil {
		return err
	}
	pos := 0
	for pos+6 <= len(data) {
		if binary.BigEndian.Uint16(data[pos:]) != dedupMagic {
			break
		}
		bodyLen := int(binary.BigEndian.Uint32(data[pos+2:]))
		if bodyLen < 4+1+4 || pos+6+bodyLen > len(data) {
			break
		}
		body := data[pos+6 : pos+6+bodyLen]
		wantCRC := binary.BigEndian.Uint32(body[:4])
		if crc32.ChecksumIEEE(body[4:]) != wantCRC {
			break
		}
		op := body[4]
		keyLen := int(binary.BigEndian.Uint32(body[5:9]))
		if 9+keyLen != bodyLen {
			break
		}
		key := string(body[9 : 9+keyLen])
		d.apply(op, key)
		pos += 6 + bodyLen
	}
	if pos < len(data) {
		if err := d.f.Truncate(int64(pos)); err != nil {
			return err
		}
		if err := d.f.Sync(); err != nil {
			return err
		}
	}
	if _, err := d.f.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	return nil
}

// apply 只更新内存结构。
func (d *dedupStore) apply(op byte, key string) {
	switch op {
	case dedupOpBegin:
		if _, ok := d.status[key]; !ok {
			d.status[key] = statusPending
		}
	case dedupOpCommit:
		if _, ok := d.status[key]; !ok {
			d.status[key] = statusComplete
			d.completeOf[key] = len(d.completed)
			d.completed = append(d.completed, key)
		} else if d.status[key] == statusPending {
			d.status[key] = statusComplete
			d.completeOf[key] = len(d.completed)
			d.completed = append(d.completed, key)
		}
	case dedupOpForget:
		if idx, ok := d.completeOf[key]; ok {
			d.completed[idx] = ""
			delete(d.completeOf, key)
		}
		delete(d.status, key)
	}
}

// appendRecord 追加一条记录并 fsync。调用方持有 d 的外部锁。
func (d *dedupStore) appendRecord(op byte, key string) error {
	rec := encodeDedupRec(op, key)
	if _, err := d.f.Write(rec); err != nil {
		return err
	}
	return d.f.Sync()
}

// begin 原子地登记一个键：
//   - 键不存在：持久化 BEGIN 后返回 DedupNew；
//   - 键为 pending（上次执行可能崩溃）：返回 DedupPending；
//   - 键已 complete：返回 DedupComplete（调用方必须跳过副作用）。
func (d *dedupStore) begin(key string) (DedupKind, error) {
	if key == "" {
		return 0, errors.New("durablequeue: empty dedup key")
	}
	if st, ok := d.status[key]; ok {
		if st == statusComplete {
			return DedupComplete, nil
		}
		return DedupPending, nil
	}
	if err := d.appendRecord(dedupOpBegin, key); err != nil {
		return 0, err
	}
	d.status[key] = statusPending
	return DedupNew, nil
}

// complete 把键标记为已完成（副作用已生效），并在超出历史窗口时压缩日志。
func (d *dedupStore) complete(key string) error {
	if err := d.appendRecord(dedupOpCommit, key); err != nil {
		return err
	}
	d.status[key] = statusComplete
	if _, ok := d.completeOf[key]; !ok {
		d.completeOf[key] = len(d.completed)
		d.completed = append(d.completed, key)
	}
	if len(d.completed) > d.history {
		return d.compact()
	}
	return nil
}

// forget 移除一个 pending 键（副作用明确失败，允许将来重试）。
func (d *dedupStore) forget(key string) error {
	if err := d.appendRecord(dedupOpForget, key); err != nil {
		return err
	}
	if idx, ok := d.completeOf[key]; ok {
		d.completed[idx] = ""
		delete(d.completeOf, key)
	}
	delete(d.status, key)
	return nil
}

// compact 重写去重日志：保留全部 pending 与最近 history 个 completed。
func (d *dedupStore) compact() error {
	keep := make(map[string]dedupStatus)
	// 保留最近 history 个 completed
	cut := len(d.completed) - d.history
	if cut < 0 {
		cut = 0
	}
	newCompleted := make([]string, 0, d.history)
	newCompleteOf := make(map[string]int, d.history)
	for i := cut; i < len(d.completed); i++ {
		key := d.completed[i]
		if key == "" {
			continue
		}
		if d.status[key] != statusComplete {
			continue
		}
		keep[key] = statusComplete
		newCompleteOf[key] = len(newCompleted)
		newCompleted = append(newCompleted, key)
	}
	// pending 全部保留
	for key, st := range d.status {
		if st == statusPending {
			keep[key] = statusPending
		}
	}

	tmpPath := d.path + ".compact"
	f, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	for key, st := range keep {
		if _, err := f.Write(encodeDedupRec(dedupOpBegin, key)); err != nil {
			f.Close()
			return err
		}
		if st == statusComplete {
			if _, err := f.Write(encodeDedupRec(dedupOpCommit, key)); err != nil {
				f.Close()
				return err
			}
		}
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, d.path); err != nil {
		return err
	}
	if err := fsyncDir(filepath.Dir(d.path)); err != nil {
		return err
	}
	nf, err := os.OpenFile(d.path, os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	old := d.f
	d.f = nf
	d.completed = newCompleted
	d.completeOf = newCompleteOf
	d.status = keep
	return old.Close()
}

func (d *dedupStore) close() error { return d.f.Close() }
