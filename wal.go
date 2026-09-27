package eventqueue

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// walEntry 是一次读到的 WAL 记录。Position 为该帧起始物理位置
// （相对全部分段拼接后的逻辑偏移）。
type walEntry struct {
	position int64
	seq      int64 // 从 0 起的记录序号
	recType  recordType
	payload  []byte
}

const (
	segSuffix = ".log"
	segPrefix = "seg-"
)

// segment 描述一个分段文件。
type segment struct {
	path string
	base int64 // 该分段起始的逻辑物理位置
	size int64 // 当前文件大小
}

// wal 是顺序追加的分段日志。多 goroutine 并发安全。
type wal struct {
	dir      string
	maxSegSz int64
	fsync    bool

	mu     sync.Mutex
	segs   []*segment
	active *os.File
	pos    int64 // 逻辑物理位置（= 各分段大小之和）
	closed bool
}

func segmentPath(dir string, base int64) string {
	return filepath.Join(dir, fmt.Sprintf("%s%012d%s", segPrefix, base, segSuffix))
}

func parseSegmentBase(name string) (int64, bool) {
	if !strings.HasPrefix(name, segPrefix) || !strings.HasSuffix(name, segSuffix) {
		return 0, false
	}
	var base int64
	_, err := fmt.Sscanf(name, segPrefix+"%012d"+segSuffix, &base)
	if err != nil {
		return 0, false
	}
	return base, true
}

// openWAL 打开/恢复一个日志目录。每个分段最大约 maxSegmentSize 字节。
// 重放时若最后一个分段末尾存在崩溃造成的半截/损坏帧，会被截断，
// 保证最后一条完整记录之后的内容全部丢弃（不会读到脏数据）。
func openWAL(dir string, maxSegmentSize int64, fsync bool) (*wal, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var segs []*segment
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		base, ok := parseSegmentBase(e.Name())
		if !ok {
			continue
		}
		fi, err := e.Info()
		if err != nil {
			return nil, err
		}
		segs = append(segs, &segment{path: filepath.Join(dir, e.Name()), base: base, size: fi.Size()})
	}
	sort.Slice(segs, func(i, j int) bool { return segs[i].base < segs[j].base })

	w := &wal{dir: dir, maxSegSz: maxSegmentSize, fsync: fsync}
	if len(segs) == 0 {
		if err := w.rollLocked(0); err != nil {
			return nil, err
		}
	} else {
		w.segs = segs
		w.pos = segs[len(segs)-1].base + segs[len(segs)-1].size
		if err := w.repairLastSegment(); err != nil {
			return nil, err
		}
		last := w.segs[len(w.segs)-1]
		f, err := os.OpenFile(last.path, os.O_RDWR|os.O_APPEND, 0o644)
		if err != nil {
			return nil, err
		}
		w.active = f
	}
	return w, nil
}

// repairLastSegment 扫描最后一个分段，截断首个损坏帧及其之后的全部字节，
// 并同步修正内存中的分段大小。
func (w *wal) repairLastSegment() error {
	last := w.segs[len(w.segs)-1]
	data, err := os.ReadFile(last.path)
	if err != nil {
		return err
	}
	off := 0
	for off < len(data) {
		_, _, frameLen, err := decodeFrame(data[off:])
		if err != nil {
			break
		}
		off += frameLen
	}
	if off == len(data) {
		return nil
	}
	if err := os.Truncate(last.path, int64(off)); err != nil {
		return err
	}
	last.size = int64(off)
	w.pos = last.base + last.size
	// 前面的分段大小也需要在恢复时确认（一般不变）。
	return nil
}

func (w *wal) rollLocked(base int64) error {
	if w.active != nil {
		if w.fsync {
			if err := w.active.Sync(); err != nil {
				return err
			}
		}
		if err := w.active.Close(); err != nil {
			return err
		}
	}
	path := segmentPath(w.dir, base)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	seg := &segment{path: path, base: base, size: 0}
	w.segs = append(w.segs, seg)
	w.active = f
	return nil
}

// append 追加一帧，返回该帧起始的逻辑物理位置。
func (w *wal) append(recType recordType, payload []byte) (int64, error) {
	frame := encodeFrame(recType, payload)
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, ErrClosed
	}
	cur := w.segs[len(w.segs)-1]
	if cur.size > 0 && w.maxSegSz > 0 && cur.size+int64(len(frame)) > w.maxSegSz {
		if err := w.rollLocked(w.pos); err != nil {
			return 0, err
		}
		cur = w.segs[len(w.segs)-1]
	}
	position := w.pos
	n, err := w.active.Write(frame)
	if err != nil {
		return 0, err
	}
	if w.fsync {
		if err := w.active.Sync(); err != nil {
			return 0, err
		}
	}
	cur.size += int64(n)
	w.pos += int64(n)
	return position, nil
}

// replay 按顺序遍历全部分段。fn 返回错误则中止。
func (w *wal) replay(fn func(walEntry) error) error {
	w.mu.Lock()
	segs := make([]*segment, len(w.segs))
	copy(segs, w.segs)
	w.mu.Unlock()

	var seq int64
	for _, seg := range segs {
		f, err := os.Open(seg.path)
		if err != nil {
			return err
		}
		data, err := io.ReadAll(f)
		f.Close()
		if err != nil {
			return err
		}
		off := 0
		for off < len(data) {
			recType, payload, frameLen, err := decodeFrame(data[off:])
			if err != nil {
				return err
			}
			err = fn(walEntry{
				position: seg.base + int64(off),
				seq:      seq,
				recType:  recType,
				payload:  payload,
			})
			if err != nil {
				return err
			}
			seq++
			off += frameLen
		}
	}
	return nil
}

// deleteSegmentsBefore 删除逻辑物理位置早于 minPosition 的全部分段。
// 活跃分段不会被删除。返回释放的字节数。
func (w *wal) deleteSegmentsBefore(minPosition int64) (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	var freed int64
	keep := w.segs[:0]
	activePath := ""
	if len(w.segs) > 0 {
		activePath = w.segs[len(w.segs)-1].path
	}
	for _, seg := range w.segs {
		if seg.base < minPosition && seg.path != activePath {
			if err := os.Remove(seg.path); err != nil && !os.IsNotExist(err) {
				return 0, err
			}
			freed += seg.size
			continue
		}
		keep = append(keep, seg)
	}
	w.segs = keep
	return freed, nil
}

// segmentBases 返回各分段基址（用于 GC 判断）。
func (w *wal) segmentBases() []int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	bases := make([]int64, len(w.segs))
	for i, s := range w.segs {
		bases[i] = s.base
	}
	return bases
}

func (w *wal) close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	if w.fsync && w.active != nil {
		_ = w.active.Sync()
	}
	if w.active != nil {
		return w.active.Close()
	}
	return nil
}
