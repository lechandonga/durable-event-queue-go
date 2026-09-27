package eventqueue

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"time"
)

// Message 生产者提交的业务消息。
type Message struct {
	Key     string            // 路由键，同一 key 在组内严格有序
	Body    []byte            // 消息体
	Headers map[string]string // 可选头
	// DedupKey 业务幂等键（建议为下游副作用的唯一标识）。
	DedupKey string
}

// Envelope 投递给消费者的消息包装。
type Envelope struct {
	Offset     int64 // 全局单调递增的消息位点
	Partition  uint32
	Attempt    int // 当前是第几次投递（1 起）
	Message    Message
	EnqueuedAt time.Time
	// FirstDeliveredAt 首次投递时间。
	FirstDeliveredAt time.Time
}

// DeadLetter 进入死信队列的记录。
type DeadLetter struct {
	Offset         int64
	Partition      uint32
	Key            string
	DedupKey       string
	Body           []byte
	Headers        map[string]string
	Attempts       int
	Reason         string
	EnqueuedAt     time.Time
	DeadLetteredAt time.Time
}

// recordType 磁盘记录类型。
type recordType uint8

const (
	recMsg    recordType = iota + 1 // 消息
	recCommit                       // 消费组位点提交
	recDedup                        // 幂等键已确认生效
	recDead                         // 死信
)

const (
	frameMagic byte = 0xA5
	headerLen       = 6 // magic(1) + type(1) + payloadLen(4)
	crcLen          = 4
	maxPayload      = 64 << 20
)

var errFrame = errors.New("eventqueue: corrupt log frame")

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// ---- 小工具：紧凑二进制编码（大端，长度前缀） ----

type binWriter struct{ b []byte }

func (w *binWriter) u8(v byte)      { w.b = append(w.b, v) }
func (w *binWriter) u32(v uint32)   { w.b = binary.BigEndian.AppendUint32(w.b, v) }
func (w *binWriter) u64(v uint64)   { w.b = binary.BigEndian.AppendUint64(w.b, v) }
func (w *binWriter) i64(v int64)    { w.u64(uint64(v)) }
func (w *binWriter) bytes(v []byte) { w.u32(uint32(len(v))); w.b = append(w.b, v...) }
func (w *binWriter) str(v string)   { w.bytes([]byte(v)) }
func (w *binWriter) headers(h map[string]string) {
	w.u32(uint32(len(h)))
	for k, v := range h {
		w.str(k)
		w.str(v)
	}
}

type binReader struct {
	b []byte
	i int
}

func (r *binReader) u8() (byte, error) {
	if r.i+1 > len(r.b) {
		return 0, errFrame
	}
	v := r.b[r.i]
	r.i++
	return v, nil
}

func (r *binReader) u32() (uint32, error) {
	if r.i+4 > len(r.b) {
		return 0, errFrame
	}
	v := binary.BigEndian.Uint32(r.b[r.i:])
	r.i += 4
	return v, nil
}

func (r *binReader) u64() (uint64, error) {
	if r.i+8 > len(r.b) {
		return 0, errFrame
	}
	v := binary.BigEndian.Uint64(r.b[r.i:])
	r.i += 8
	return v, nil
}

func (r *binReader) i64() (int64, error) {
	v, err := r.u64()
	return int64(v), err
}

func (r *binReader) bytes() ([]byte, error) {
	n, err := r.u32()
	if err != nil {
		return nil, err
	}
	if r.i+int(n) > len(r.b) {
		return nil, errFrame
	}
	v := r.b[r.i : r.i+int(n)]
	r.i += int(n)
	return v, nil
}

func (r *binReader) str() (string, error) {
	v, err := r.bytes()
	return string(v), err
}

func (r *binReader) headers() (map[string]string, error) {
	n, err := r.u32()
	if err != nil {
		return nil, err
	}
	if n > 1<<20 {
		return nil, errFrame
	}
	h := make(map[string]string, n)
	for i := uint32(0); i < n; i++ {
		k, err := r.str()
		if err != nil {
			return nil, err
		}
		v, err := r.str()
		if err != nil {
			return nil, err
		}
		h[k] = v
	}
	return h, nil
}

func (r *binReader) end() error {
	if r.i != len(r.b) {
		return errFrame
	}
	return nil
}

// ---- 帧：magic | type | payloadLen | payload | crc32 ----

func encodeFrame(recType recordType, payload []byte) []byte {
	b := make([]byte, 0, headerLen+len(payload)+crcLen)
	b = append(b, frameMagic, byte(recType))
	b = binary.BigEndian.AppendUint32(b, uint32(len(payload)))
	b = append(b, payload...)
	c := crc32.Checksum(b, crcTable)
	b = binary.BigEndian.AppendUint32(b, c)
	return b
}

// decodeFrame 解析一帧（不含后续字节），返回 payload 与总帧长。
func decodeFrame(buf []byte) (recType recordType, payload []byte, frameLen int, err error) {
	if len(buf) < headerLen+crcLen || buf[0] != frameMagic {
		return 0, nil, 0, errFrame
	}
	recType = recordType(buf[1])
	plen := int(binary.BigEndian.Uint32(buf[2:6]))
	frameLen = headerLen + plen + crcLen
	if plen > maxPayload || frameLen > len(buf) {
		return 0, nil, 0, errFrame
	}
	want := binary.BigEndian.Uint32(buf[frameLen-crcLen : frameLen])
	got := crc32.Checksum(buf[:frameLen-crcLen], crcTable)
	if want != got {
		return 0, nil, 0, errFrame
	}
	payload = buf[headerLen : headerLen+plen]
	return recType, payload, frameLen, nil
}

// ---- 各记录 payload 编解码 ----

type msgRecord struct {
	offset     int64
	partition  uint32
	enqueuedAt int64
	key        string
	dedupKey   string
	body       []byte
	headers    map[string]string
}

func encodeMsg(m *msgRecord) []byte {
	w := binWriter{}
	w.i64(m.offset)
	w.u32(m.partition)
	w.i64(m.enqueuedAt)
	w.str(m.key)
	w.str(m.dedupKey)
	w.bytes(m.body)
	w.headers(m.headers)
	return w.b
}

func decodeMsg(p []byte) (*msgRecord, error) {
	r := &binReader{b: p}
	var err error
	m := &msgRecord{}
	if m.offset, err = r.i64(); err != nil {
		return nil, err
	}
	if m.partition, err = r.u32(); err != nil {
		return nil, err
	}
	if m.enqueuedAt, err = r.i64(); err != nil {
		return nil, err
	}
	if m.key, err = r.str(); err != nil {
		return nil, err
	}
	if m.dedupKey, err = r.str(); err != nil {
		return nil, err
	}
	if m.body, err = r.bytes(); err != nil {
		return nil, err
	}
	if m.headers, err = r.headers(); err != nil {
		return nil, err
	}
	return m, r.end()
}

type commitRecord struct {
	group     string
	partition uint32
	offset    int64
}

func encodeCommit(c *commitRecord) []byte {
	w := binWriter{}
	w.str(c.group)
	w.u32(c.partition)
	w.i64(c.offset)
	return w.b
}

func decodeCommit(p []byte) (*commitRecord, error) {
	r := &binReader{b: p}
	c := &commitRecord{}
	var err error
	if c.group, err = r.str(); err != nil {
		return nil, err
	}
	if c.partition, err = r.u32(); err != nil {
		return nil, err
	}
	if c.offset, err = r.i64(); err != nil {
		return nil, err
	}
	return c, r.end()
}

type dedupRecord struct {
	group  string
	dedup  string
	offset int64
}

func encodeDedup(d *dedupRecord) []byte {
	w := binWriter{}
	w.str(d.group)
	w.str(d.dedup)
	w.i64(d.offset)
	return w.b
}

func decodeDedup(p []byte) (*dedupRecord, error) {
	r := &binReader{b: p}
	d := &dedupRecord{}
	var err error
	if d.group, err = r.str(); err != nil {
		return nil, err
	}
	if d.dedup, err = r.str(); err != nil {
		return nil, err
	}
	if d.offset, err = r.i64(); err != nil {
		return nil, err
	}
	return d, r.end()
}

type deadRecord struct {
	offset     int64
	partition  uint32
	key        string
	dedupKey   string
	body       []byte
	headers    map[string]string
	attempts   int32
	reason     string
	enqueuedAt int64
	deadAt     int64
}

func encodeDead(d *deadRecord) []byte {
	w := binWriter{}
	w.i64(d.offset)
	w.u32(d.partition)
	w.str(d.key)
	w.str(d.dedupKey)
	w.bytes(d.body)
	w.headers(d.headers)
	w.u32(uint32(d.attempts))
	w.str(d.reason)
	w.i64(d.enqueuedAt)
	w.i64(d.deadAt)
	return w.b
}

func decodeDead(p []byte) (*deadRecord, error) {
	r := &binReader{b: p}
	d := &deadRecord{}
	var err error
	read := func() bool {
		if d.offset, err = r.i64(); err != nil {
			return false
		}
		if d.partition, err = r.u32(); err != nil {
			return false
		}
		if d.key, err = r.str(); err != nil {
			return false
		}
		if d.dedupKey, err = r.str(); err != nil {
			return false
		}
		if d.body, err = r.bytes(); err != nil {
			return false
		}
		if d.headers, err = r.headers(); err != nil {
			return false
		}
		a, err := r.u32()
		if err != nil {
			return false
		}
		d.attempts = int32(a)
		if d.reason, err = r.str(); err != nil {
			return false
		}
		if d.enqueuedAt, err = r.i64(); err != nil {
			return false
		}
		if d.deadAt, err = r.i64(); err != nil {
			return false
		}
		return true
	}
	if !read() {
		return nil, err
	}
	return d, r.end()
}
