package eventqueue

import (
	"encoding/binary"
	"io"
	"os"
	"sync"
)

// metaLog 在一个只增文件里顺序保存组位点 / 幂等确认 / 死信记录，
// 并通过快照文件周期性压缩，避免无限增长。
//
// 快照文件前 8 字节（小端 uint64）记录该快照已覆盖的 meta.log 逻辑大小，
// 之后是若干条完整帧。重启时：先加载快照，再从 meta.log 的该偏移继续重放。
type metaLog struct {
	path     string
	snapPath string
	fsync    bool

	mu     sync.Mutex
	f      *os.File
	size   int64
	closed bool
}

func openMetaLog(path, snapPath string, fsync bool) (*metaLog, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return &metaLog{path: path, snapPath: snapPath, fsync: fsync, f: f, size: fi.Size()}, nil
}

func (m *metaLog) append(rt recordType, payload []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	if _, err := m.f.Write(encodeFrame(rt, payload)); err != nil {
		return err
	}
	if m.fsync {
		if err := m.f.Sync(); err != nil {
			return err
		}
	}
	fi, err := m.f.Stat()
	if err != nil {
		return err
	}
	m.size = fi.Size()
	return nil
}

// snapshotBase 返回快照覆盖的 meta.log 逻辑大小（0 表示无快照）。
func readSnapshotBase(path string) int64 {
	b := make([]byte, 8)
	f, err := os.Open(path)
	if err != nil {
		return 0
	}
	defer f.Close()
	if _, err := io.ReadFull(f, b); err != nil {
		return 0
	}
	return int64(binary.LittleEndian.Uint64(b))
}

// loadSnapshot 重放快照内所有记录帧（跳过前 8 字节 base）。
func loadSnapshot(path string, fn func(rt recordType, payload []byte) error) error {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if len(data) < 8 {
		return nil
	}
	buf := data[8:]
	off := 0
	for off < len(buf) {
		rt, payload, frameLen, err := decodeFrame(buf[off:])
		if err != nil {
			return err // 快照损坏：上层可选择忽略快照全量重放
		}
		if err := fn(rt, payload); err != nil {
			return err
		}
		off += frameLen
	}
	return nil
}

// writeSnapshot 原子写入新快照：base + 全量帧。
func writeSnapshot(path string, base int64, frames [][]byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	hdr := make([]byte, 8)
	binary.LittleEndian.PutUint64(hdr, uint64(base))
	if _, err := f.Write(hdr); err != nil {
		f.Close()
		return err
	}
	for _, fr := range frames {
		if _, err := f.Write(fr); err != nil {
			f.Close()
			return err
		}
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// replayFrom 从 meta.log 的指定逻辑偏移开始重放。
func (m *metaLog) replayFrom(base int64, fn func(rt recordType, payload []byte) error) error {
	data, err := os.ReadFile(m.path)
	if err != nil {
		return err
	}
	if base > int64(len(data)) {
		return errFrame
	}
	buf := data[base:]
	off := 0
	for off < len(buf) {
		rt, payload, frameLen, err := decodeFrame(buf[off:])
		if err != nil {
			return err
		}
		if err := fn(rt, payload); err != nil {
			return err
		}
		off += frameLen
	}
	return nil
}

// replay 兼容全量重放（无快照场景）。
func (m *metaLog) replay(fn func(rt recordType, payload []byte) error) error {
	return m.replayFrom(0, fn)
}

// truncateAndReset 将 meta.log 截断到 0（压缩成功后调用）。
func (m *metaLog) truncateAndReset() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.f.Sync(); err != nil {
		return err
	}
	if err := m.f.Truncate(0); err != nil {
		return err
	}
	if _, err := m.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	m.size = 0
	return nil
}

func (m *metaLog) getSize() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.size
}

func (m *metaLog) close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}
	m.closed = true
	if m.fsync {
		_ = m.f.Sync()
	}
	return m.f.Close()
}
