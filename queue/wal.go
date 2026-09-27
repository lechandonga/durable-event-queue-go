package queue

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

// msgMagic 是消息日志记录的起始魔数。
const msgMagic uint16 = 0xD9A1

// 单条记录布局（大端）：
//
//	magic      uint16        2B
//	bodyLen    uint32        4B   body = crc|offset|keylen|key|paylen|payload
//	crc        uint32        4B   覆盖 offset|keylen|key|paylen|payload
//	offset     int64         8B
//	keyLen     uint32        4B
//	key        ...
//	payLen     uint32        4B
//	payload    ...
const (
	recHeaderLen = 6 // magic + bodyLen
	recCrcLen    = 4
)

// messageRecord 是一条已落盘消息。
type messageRecord struct {
	Offset  int64
	Key     string
	Payload []byte
}

// messageLog 是仅追加的消息日志，每次写入都 fsync 以保证崩溃一致性。
type messageLog struct {
	f         *os.File
	path      string
	next      int64           // 下一条消息的 offset（从 0 起）
	index     map[int64]int64 // offset -> 文件字节位置
	positions []int64         // 按顺序的记录起始位置（用于校验扫描）
	size      int64
}

// openMessageLog 打开（或创建）消息日志，并截断尾部损坏/撕裂记录。
func openMessageLog(dir string) (*messageLog, error) {
	path := filepath.Join(dir, "messages.log")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	l := &messageLog{
		f:     f,
		path:  path,
		next:  0,
		index: make(map[int64]int64),
	}
	if err := l.recover(); err != nil {
		f.Close()
		return nil, err
	}
	return l, nil
}

// recover 从头扫描日志，重建 offset -> 位置索引；遇到第一条非法记录即截断。
func (l *messageLog) recover() error {
	data, err := os.ReadFile(l.path)
	if err != nil {
		return err
	}
	pos := int64(0)
	for pos+recHeaderLen <= int64(len(data)) {
		magic := binary.BigEndian.Uint16(data[pos:])
		if magic != msgMagic {
			break
		}
		bodyLen := int64(binary.BigEndian.Uint32(data[pos+2:]))
		recLen := recHeaderLen + bodyLen
		if bodyLen < recCrcLen+8 || pos+recLen > int64(len(data)) {
			break // 撕裂的尾部记录
		}
		body := data[pos+recHeaderLen : pos+recHeaderLen+bodyLen]
		wantCRC := binary.BigEndian.Uint32(body[:4])
		if crc32.ChecksumIEEE(body[4:]) != wantCRC {
			break // CRC 不匹配，视为损坏边界
		}
		bp := recCrcLen
		offset := int64(binary.BigEndian.Uint64(body[bp:]))
		bp += 8
		keyLen := int(binary.BigEndian.Uint32(body[bp:]))
		bp += 4
		if bp+keyLen+4 > len(body) {
			break
		}
		key := string(body[bp : bp+keyLen])
		bp += keyLen
		payLen := int(binary.BigEndian.Uint32(body[bp:]))
		bp += 4
		if bp+payLen != len(body) {
			break
		}
		if _, dup := l.index[offset]; dup {
			return fmt.Errorf("message log: duplicate offset %d at %d", offset, pos)
		}
		l.index[offset] = pos
		l.positions = append(l.positions, pos)
		if offset >= l.next {
			l.next = offset + 1
		}
		_ = key
		pos += recLen
	}
	if pos < int64(len(data)) {
		// 截断撕裂/损坏尾部（崩溃可能恰好发生在一次 write 中间）。
		if err := l.f.Truncate(pos); err != nil {
			return err
		}
		if err := l.f.Sync(); err != nil {
			return err
		}
	}
	l.size = pos
	return nil
}

// encodeRecord 编码一条记录。
func encodeRecord(offset int64, key string, payload []byte) []byte {
	body := make([]byte, 0, recCrcLen+8+4+len(key)+4+len(payload))
	crcBuf := make([]byte, 4)
	body = append(body, crcBuf...)
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], uint64(offset))
	body = append(body, tmp[:]...)
	binary.BigEndian.PutUint32(tmp[:4], uint32(len(key)))
	body = append(body, tmp[:4]...)
	body = append(body, key...)
	binary.BigEndian.PutUint32(tmp[:4], uint32(len(payload)))
	body = append(body, tmp[:4]...)
	body = append(body, payload...)
	crc := crc32.ChecksumIEEE(body[4:])
	binary.BigEndian.PutUint32(body[:4], crc)

	rec := make([]byte, 0, recHeaderLen+len(body))
	var hdr [6]byte
	binary.BigEndian.PutUint16(hdr[:], msgMagic)
	binary.BigEndian.PutUint32(hdr[2:], uint32(len(body)))
	rec = append(rec, hdr[:]...)
	rec = append(rec, body...)
	return rec
}

// append 追加一条消息并 fsync，返回分配的 offset。
// 调用方必须持有外部串行化锁。
func (l *messageLog) append(key string, payload []byte) (int64, error) {
	offset := l.next
	rec := encodeRecord(offset, key, payload)
	if _, err := l.f.Write(rec); err != nil {
		return 0, err
	}
	if err := l.f.Sync(); err != nil {
		return 0, err
	}
	l.index[offset] = l.size
	l.positions = append(l.positions, l.size)
	l.size += int64(len(rec))
	l.next++
	return offset, nil
}

// readAt 读取指定 offset 的消息。
func (l *messageLog) readAt(offset int64) (*messageRecord, error) {
	pos, ok := l.index[offset]
	if !ok {
		return nil, io.EOF
	}
	// 读取 header
	hdr := make([]byte, recHeaderLen)
	if _, err := l.f.ReadAt(hdr, pos); err != nil {
		return nil, err
	}
	if binary.BigEndian.Uint16(hdr) != msgMagic {
		return nil, errors.New("message log: bad magic on read")
	}
	bodyLen := int64(binary.BigEndian.Uint32(hdr[2:]))
	body := make([]byte, bodyLen)
	if _, err := l.f.ReadAt(body, pos+recHeaderLen); err != nil {
		return nil, err
	}
	wantCRC := binary.BigEndian.Uint32(body[:4])
	if crc32.ChecksumIEEE(body[4:]) != wantCRC {
		return nil, errors.New("message log: crc mismatch")
	}
	bp := recCrcLen
	gotOffset := int64(binary.BigEndian.Uint64(body[bp:]))
	bp += 8
	keyLen := int(binary.BigEndian.Uint32(body[bp:]))
	bp += 4
	key := string(body[bp : bp+keyLen])
	bp += keyLen
	payLen := int(binary.BigEndian.Uint32(body[bp:]))
	bp += 4
	payload := body[bp : bp+payLen]
	if gotOffset != offset {
		return nil, fmt.Errorf("message log: offset mismatch %d != %d", gotOffset, offset)
	}
	return &messageRecord{Offset: offset, Key: key, Payload: append([]byte(nil), payload...)}, nil
}

// nextOffset 返回下一条消息的 offset。
func (l *messageLog) nextOffset() int64 { return l.next }

// sizeBytes 返回日志文件当前字节数。
func (l *messageLog) sizeBytes() int64 { return l.size }

// close 关闭日志。
func (l *messageLog) close() error { return l.f.Close() }

// discardBefore 重写日志，丢弃 offset 之前（不含）的全部消息，
// 保留消息的原始 offset 编号不变。用于所有消费组都提交后回收空间。
// 调用方必须持有外部串行化锁。
func (l *messageLog) discardBefore(offset int64) error {
	if offset <= 0 || offset >= l.next {
		return nil
	}
	tmpPath := l.path + ".compact"
	f, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	newIndex := make(map[int64]int64)
	pos := int64(0)
	for off := offset; off < l.next; off++ {
		rec, err := l.readAt(off)
		if err != nil {
			f.Close()
			return fmt.Errorf("discardBefore read offset %d: %w", off, err)
		}
		buf := encodeRecord(off, rec.Key, rec.Payload)
		if _, err := f.Write(buf); err != nil {
			f.Close()
			return err
		}
		newIndex[off] = pos
		pos += int64(len(buf))
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	old := l.f
	if err := os.Rename(tmpPath, l.path); err != nil {
		return err
	}
	if err := fsyncDir(filepath.Dir(l.path)); err != nil {
		return err
	}
	nf, err := os.OpenFile(l.path, os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	l.f = nf
	l.index = newIndex
	l.positions = nil
	l.size = pos
	return old.Close()
}
