package eventqueue

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	dirMessages = "messages"
	fileMeta    = "meta.log"
	fileSnap    = "snap.meta"
	fileLock    = "LOCK"

	msgSegmentSize int64 = 4 << 20
	metaCompactAt  int64 = 1 << 20
)

// indexEntry 消息在 message WAL 中的位置信息（内存索引）。
type indexEntry struct {
	offset     int64
	partition  uint32
	key        string
	dedupKey   string
	framePos   int64
	frameSize  int64
	enqueuedAt time.Time
}

// deliveryEntry 某条消息在某个消费组内的投递状态。
type deliveryEntry struct {
	offset           int64
	partition        uint32
	attempt          int // 已发生的投递次数
	nextVisibleAt    time.Time
	leasedAt         time.Time
	leaseDeadline    time.Time
	inflight         bool
	firstDeliveredAt time.Time
}

// groupState 一个消费组的运行状态。
type groupState struct {
	name       string
	watermark  []int64 // 每分区已提交位点（连续确认到此处）
	cursor     []int   // 每分区下一个待投递消息在 partOffsets[p] 中的下标
	delivering map[int64]*deliveryEntry
}

// store 是队列的持久化核心。
type store struct {
	dir  string
	opts Options

	msgWAL *wal
	meta   *metaLog
	lockF  *os.File

	mu          sync.Mutex
	closed      bool
	index       []*indexEntry // 按 offset 严格递增；offset 即下标+1
	byOffset    map[int64]*indexEntry
	partOffsets [][]int64 // 每个分区按写入顺序排列的 offset
	groups      map[string]*groupState
	dlq         []DeadLetter
	nextOff     int64

	retained int64 // 当前必须保留（至少一个组尚未提交）的消息帧字节数

	// gcPos: 物理位置 < gcPos 的消息分段已被删除，对应消息不可能再重放。
	// 用它来安全回收幂等判重键，而不必改动有序索引切片（避免游标下标错位）。
	gcPos int64

	// 启动重放出的 confirmed 幂等表快照，queue 构建 guard 后取走。
	recoveredGuard map[string]map[string]int64
}

func openStore(dir string, opts Options) (*store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	lockF, err := acquireLock(filepath.Join(dir, fileLock))
	if err != nil {
		return nil, err
	}

	msgWAL, err := openWAL(filepath.Join(dir, dirMessages), msgSegmentSize, opts.Fsync)
	if err != nil {
		_ = releaseLock(lockF)
		return nil, err
	}
	meta, err := openMetaLog(filepath.Join(dir, fileMeta), filepath.Join(dir, fileSnap), opts.Fsync)
	if err != nil {
		_ = msgWAL.close()
		_ = releaseLock(lockF)
		return nil, err
	}

	s := &store{
		dir:         dir,
		opts:        opts,
		msgWAL:      msgWAL,
		meta:        meta,
		lockF:       lockF,
		byOffset:    make(map[int64]*indexEntry),
		partOffsets: make([][]int64, opts.Partitions),
		groups:      make(map[string]*groupState),
	}

	if err := s.recover(); err != nil {
		_ = s.close()
		return nil, err
	}
	return s, nil
}

func (s *store) close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()

	var errs []error
	if s.msgWAL != nil {
		if err := s.msgWAL.close(); err != nil {
			errs = append(errs, err)
		}
	}
	if s.meta != nil {
		if err := s.meta.close(); err != nil {
			errs = append(errs, err)
		}
	}
	if s.lockF != nil {
		if err := releaseLock(s.lockF); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// ---- 崩溃恢复 ----

func (s *store) recover() error {
	// 1) 重建消息索引。
	var maxOff int64
	err := s.msgWAL.replay(func(e walEntry) error {
		if e.recType != recMsg {
			return nil
		}
		m, err := decodeMsg(e.payload)
		if err != nil {
			return err
		}
		frameSize := int64(len(encodeFrame(recMsg, e.payload)))
		ie := &indexEntry{
			offset:     m.offset,
			partition:  m.partition,
			key:        m.key,
			dedupKey:   m.dedupKey,
			framePos:   e.position,
			frameSize:  frameSize,
			enqueuedAt: time.Unix(0, m.enqueuedAt),
		}
		s.byOffset[ie.offset] = ie
		s.index = append(s.index, ie)
		s.partOffsets[ie.partition] = append(s.partOffsets[ie.partition], ie.offset)
		if ie.offset > maxOff {
			maxOff = ie.offset
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("recover messages: %w", err)
	}
	s.nextOff = maxOff

	// 2) 重建组位点 / 幂等表 / 死信：先加载快照，再重放快照之后的增量。
	var dlq []DeadLetter
	guardSnapshot := map[string]map[string]int64{}

	applyMeta := func(rt recordType, payload []byte) error {
		switch rt {
		case recCommit:
			c, err := decodeCommit(payload)
			if err != nil {
				return err
			}
			g := s.getOrCreateGroup(c.group)
			if int(c.partition) < len(g.watermark) && c.offset > g.watermark[c.partition] {
				g.watermark[c.partition] = c.offset
			}
		case recDedup:
			d, err := decodeDedup(payload)
			if err != nil {
				return err
			}
			m := guardSnapshot[d.group]
			if m == nil {
				m = map[string]int64{}
				guardSnapshot[d.group] = m
			}
			m[d.dedup] = d.offset
		case recDead:
			d, err := decodeDead(payload)
			if err != nil {
				return err
			}
			dlq = append(dlq, DeadLetter{
				Offset:         d.offset,
				Partition:      d.partition,
				Key:            d.key,
				DedupKey:       d.dedupKey,
				Body:           d.body,
				Headers:        d.headers,
				Attempts:       int(d.attempts),
				Reason:         d.reason,
				EnqueuedAt:     time.Unix(0, d.enqueuedAt),
				DeadLetteredAt: time.Unix(0, d.deadAt),
			})
		}
		return nil
	}

	snapBase := readSnapshotBase(s.meta.snapPath)
	if err := loadSnapshot(s.meta.snapPath, applyMeta); err != nil {
		// 快照损坏时退化为全量重放（meta.log 中仍有全部已压缩前的记录
		// 的前提不成立；因此快照损坏属于磁盘损坏，这里直接报错）。
		return fmt.Errorf("load snapshot: %w", err)
	}
	if err := s.meta.replayFrom(snapBase, applyMeta); err != nil {
		return fmt.Errorf("recover meta: %w", err)
	}
	s.dlq = dlq

	// 3) 初始化每个组的分区游标（跳过已提交位点）。
	for _, g := range s.groups {
		g.cursor = make([]int, s.opts.Partitions)
		for p := range g.cursor {
			g.cursor[p] = s.indexForWatermark(uint32(p), g.watermark[p])
		}
	}
	s.recomputeRetainedLocked()

	// 把重建的幂等快照暂存，queue 会通过 takeRecoveredGuard 取走。
	s.recoveredGuard = guardSnapshot
	return nil
}

func (s *store) getOrCreateGroup(name string) *groupState {
	g, ok := s.groups[name]
	if !ok {
		g = &groupState{
			name:       name,
			watermark:  make([]int64, s.opts.Partitions),
			cursor:     make([]int, s.opts.Partitions),
			delivering: make(map[int64]*deliveryEntry),
		}
		s.groups[name] = g
	}
	return g
}

// indexForWatermark 返回分区中第一个 offset > watermark 的元素下标。
func (s *store) indexForWatermark(partition uint32, watermark int64) int {
	offs := s.partOffsets[partition]
	// 分区 offset 随下标单调递增，二分查找。
	lo, hi := 0, len(offs)
	for lo < hi {
		mid := (lo + hi) / 2
		if offs[mid] <= watermark {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// recomputeRetainedLocked 统计尚未被所有组提交的消息字节数。
func (s *store) recomputeRetainedLocked() {
	var retained int64
	if len(s.groups) == 0 {
		for _, ie := range s.index {
			retained += ie.frameSize
		}
		s.retained = retained
		return
	}
	for _, ie := range s.index {
		for _, g := range s.groups {
			if g.watermark[ie.partition] < ie.offset {
				retained += ie.frameSize
				break
			}
		}
	}
	s.retained = retained
}

// appendMessage 持久化一条消息并更新内存索引。调用方必须持有容量许可。
func (s *store) appendMessage(msg Message) (*indexEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrClosed
	}
	off := s.nextOff + 1
	part := partitionOf(msg.Key, s.opts.Partitions)
	now := time.Now()
	rec := &msgRecord{
		offset:     off,
		partition:  part,
		enqueuedAt: now.UnixNano(),
		key:        msg.Key,
		dedupKey:   msg.DedupKey,
		body:       msg.Body,
		headers:    msg.Headers,
	}
	payload := encodeMsg(rec)
	position, err := s.msgWAL.append(recMsg, payload)
	if err != nil {
		return nil, err
	}
	ie := &indexEntry{
		offset:     off,
		partition:  part,
		key:        msg.Key,
		dedupKey:   msg.DedupKey,
		framePos:   position,
		frameSize:  int64(len(encodeFrame(recMsg, payload))),
		enqueuedAt: now,
	}
	s.nextOff = off
	s.index = append(s.index, ie)
	s.byOffset[off] = ie
	s.partOffsets[part] = append(s.partOffsets[part], off)
	s.retained += ie.frameSize
	return ie, nil
}

// frameSizeOf 估算一条消息落盘后的帧大小（用于容量判断）。
func frameSizeOf(msg Message) int64 {
	rec := &msgRecord{
		key:      msg.Key,
		dedupKey: msg.DedupKey,
		body:     msg.Body,
		headers:  msg.Headers,
	}
	return int64(len(encodeFrame(recMsg, encodeMsg(rec))))
}

// loadMessage 从磁盘读取一条完整消息。
func (s *store) loadMessage(ie *indexEntry) (*Message, error) {
	// 直接重放该物理位置所在的帧：通过 message WAL 的分段定位读取。
	data, err := s.readFrameAt(ie.framePos, ie.frameSize)
	if err != nil {
		return nil, err
	}
	rt, payload, _, err := decodeFrame(data)
	if err != nil {
		return nil, err
	}
	if rt != recMsg {
		return nil, errFrame
	}
	m, err := decodeMsg(payload)
	if err != nil {
		return nil, err
	}
	return &Message{Key: m.key, Body: m.body, Headers: m.headers, DedupKey: m.dedupKey}, nil
}

// readFrameAt 读取指定逻辑物理位置、指定长度的帧字节。
func (s *store) readFrameAt(position, size int64) ([]byte, error) {
	bases := s.msgWAL.segmentBases()
	var segBase int64
	for _, b := range bases {
		if b <= position {
			segBase = b
		}
	}
	path := segmentPath(filepath.Join(s.dir, dirMessages), segBase)
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	buf := make([]byte, size)
	if _, err := f.ReadAt(buf, position-segBase); err != nil {
		return nil, err
	}
	return buf, nil
}

// snapshotState 返回需要进入快照的全部元数据帧与当前 meta.log 大小。
func (s *store) snapshotState() (base int64, frames [][]byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	base = s.meta.getSize()
	for _, g := range s.groups {
		for p, w := range g.watermark {
			if w > 0 {
				frames = append(frames, encodeFrame(recCommit, encodeCommit(&commitRecord{
					group: g.name, partition: uint32(p), offset: w,
				})))
			}
		}
	}
	snap := s.guardSnapshotLocked()
	for group, m := range snap {
		for d, off := range m {
			frames = append(frames, encodeFrame(recDedup, encodeDedup(&dedupRecord{
				group: group, dedup: d, offset: off,
			})))
		}
	}
	for _, d := range s.dlq {
		frames = append(frames, encodeFrame(recDead, encodeDead(&deadRecord{
			offset: d.Offset, partition: d.Partition, key: d.Key, dedupKey: d.DedupKey,
			body: d.Body, headers: d.Headers, attempts: int32(d.Attempts), reason: d.Reason,
			enqueuedAt: d.EnqueuedAt.UnixNano(), deadAt: d.DeadLetteredAt.UnixNano(),
		})))
	}
	return base, frames
}

// guardSnapshotLocked 由 queue 通过回调注入（避免 store 反向依赖 guard）。
// 默认返回 nil；queue 启动时会设置该函数。
var guardSnapshotFn func() map[string]map[string]int64

func (s *store) guardSnapshotLocked() map[string]map[string]int64 {
	if guardSnapshotFn != nil {
		return guardSnapshotFn()
	}
	return nil
}

// compactMeta 若 meta.log 过大，则将内存状态原子快照并清空日志。
func (s *store) compactMeta() error {
	if s.meta.getSize() < metaCompactAt {
		return nil
	}
	base, frames := s.snapshotState()
	if err := writeSnapshot(s.meta.snapPath, base, frames); err != nil {
		return err
	}
	return s.meta.truncateAndReset()
}

// ---- 消费组与投递状态 ----

func (s *store) ensureGroup(name string) *groupState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.getOrCreateGroup(name)
}

// nextDeliverable 找到一个当前可投递给 group 的消息索引项：
//   - 严格按分区顺序（分区队头阻塞）；
//   - 已提交位点之前的不返回；
//   - 退避未到时间、正在租约期内的不返回；
//   - 返回时将其置为 in-flight 并设置租约与尝试次数。
//
// 各分区轮流尝试，避免某分区空转饿死其它分区。
// 返回的 newHint 用于调用方下一轮轮转。
func (s *store) nextDeliverable(group string, now time.Time, ackTimeout time.Duration, startHint uint32) (*indexEntry, *deliveryEntry, uint32, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, ok := s.groups[group]
	if !ok {
		g = s.getOrCreateGroup(group)
	}
	for i := uint32(0); i < s.opts.Partitions; i++ {
		p := (startHint + i) % s.opts.Partitions
		// 游标可能落后于 watermark，防御性前推。
		cursor := g.cursor[p]
		offs := s.partOffsets[p]
		for cursor < len(offs) && offs[cursor] <= g.watermark[p] {
			cursor++
		}
		g.cursor[p] = cursor
		if cursor >= len(offs) {
			continue // 该分区暂无更多已发布消息
		}
		off := offs[cursor]
		ie := s.byOffset[off]
		if d, inflight := g.delivering[off]; inflight {
			if d.inflight && now.Before(d.leaseDeadline) {
				continue // 正在投递且租约未到期
			}
			if now.Before(d.nextVisibleAt) {
				continue // 退避中
			}
			// 租约到期或退避结束：重新投递，尝试次数 +1。
			d.inflight = true
			d.attempt++
			d.leasedAt = now
			d.leaseDeadline = now.Add(ackTimeout)
			if d.firstDeliveredAt.IsZero() {
				d.firstDeliveredAt = now
			}
			return ie, d, p + 1, true
		}
		// 全新投递。
		d := &deliveryEntry{
			offset:           off,
			partition:        p,
			attempt:          1,
			inflight:         true,
			leasedAt:         now,
			leaseDeadline:    now.Add(ackTimeout),
			firstDeliveredAt: now,
		}
		g.delivering[off] = d
		return ie, d, p + 1, true
	}
	return nil, nil, startHint, false
}

// commit 提交位点。offset < 已提交位点时返回 ErrOffsetRollback。
// 返回提交后的位点。
func (s *store) commit(group string, partition uint32, offset int64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, ok := s.groups[group]
	if !ok {
		g = s.getOrCreateGroup(group)
	}
	if offset < g.watermark[partition] {
		return g.watermark[partition], ErrOffsetRollback
	}
	if offset == g.watermark[partition] {
		return offset, nil
	}
	if err := s.ensureContiguousLocked(g, partition, offset); err != nil {
		return g.watermark[partition], err
	}
	if err := s.meta.append(recCommit, encodeCommit(&commitRecord{
		group: group, partition: partition, offset: offset,
	})); err != nil {
		return g.watermark[partition], err
	}
	old := g.watermark[partition]
	g.watermark[partition] = offset
	g.cursor[int(partition)] = s.indexForWatermark(partition, offset)
	// 清理 (old, offset] 范围内的投递状态并释放保留量。
	for off := old + 1; off <= offset; off++ {
		if ie, ok := s.byOffset[off]; ok && ie.partition == partition {
			s.releaseIfSafeLocked(ie)
		}
		delete(g.delivering, off)
	}
	return offset, nil
}

// releaseIfSafeLocked 当一条消息已被所有组提交（或死信等价推进）时，
// 从保留量中减去其大小。调用方持锁。
func (s *store) releaseIfSafeLocked(ie *indexEntry) {
	for _, g := range s.groups {
		if g.watermark[ie.partition] < ie.offset {
			return
		}
	}
	s.retained -= ie.frameSize
	if s.retained < 0 {
		s.retained = 0
	}
}

// nackResult 表示一次 Nack 的处理结果。
type nackResult int

const (
	nackRetrying     nackResult = iota // 安排退避重试
	nackDeadLettered                   // 超过上限，进入死信
)

// nack 标记一条在途消息消费失败：
//   - 未超 MaxAttempts：设置退避时间，等待重新投递；
//   - 达到 MaxAttempts：写死信记录并提交位点（崩溃一致地跳过该消息）。
func (s *store) nack(group string, off int64, reason string, now time.Time, o Options) (nackResult, *DeadLetter, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, ok := s.groups[group]
	if !ok {
		return nackRetrying, nil, ErrConsumerNotFound
	}
	d, ok := g.delivering[off]
	if !ok {
		// 已提交或未知：幂等忽略。
		return nackRetrying, nil, nil
	}
	ie, exists := s.byOffset[off]
	if !exists {
		return nackRetrying, nil, errFrame
	}
	if d.attempt >= o.MaxAttempts {
		// 进入死信：recDead + recCommit 顺序落盘。
		dl := &DeadLetter{
			Offset:         off,
			Partition:      ie.partition,
			Key:            ie.key,
			DedupKey:       ie.dedupKey,
			Attempts:       d.attempt,
			Reason:         reason,
			EnqueuedAt:     ie.enqueuedAt,
			DeadLetteredAt: now,
		}
		msg, err := s.loadMessageLocked(ie)
		if err != nil {
			return nackRetrying, nil, err
		}
		dl.Body = msg.Body
		dl.Headers = msg.Headers
		dr := &deadRecord{
			offset: off, partition: ie.partition, key: ie.key, dedupKey: ie.dedupKey,
			body: msg.Body, headers: msg.Headers, attempts: int32(d.attempt),
			reason: reason, enqueuedAt: ie.enqueuedAt.UnixNano(), deadAt: now.UnixNano(),
		}
		if err := s.meta.append(recDead, encodeDead(dr)); err != nil {
			return nackRetrying, nil, err
		}
		// 位点必须前进到死信消息本身，保证重启后不再重复投递。
		newWM := off
		if g.watermark[ie.partition] < newWM {
			if err := s.meta.append(recCommit, encodeCommit(&commitRecord{
				group: group, partition: ie.partition, offset: newWM,
			})); err != nil {
				return nackRetrying, nil, err
			}
			old := g.watermark[ie.partition]
			g.watermark[ie.partition] = newWM
			g.cursor[int(ie.partition)] = s.indexForWatermark(ie.partition, newWM)
			for o2 := old + 1; o2 <= newWM; o2++ {
				if ie2, ok := s.byOffset[o2]; ok && ie2.partition == ie.partition {
					s.releaseIfSafeLocked(ie2)
				}
				delete(g.delivering, o2)
			}
		}
		s.dlq = append(s.dlq, *dl)
		return nackDeadLettered, dl, nil
	}
	// 安排第 d.attempt 次失败后的退避。
	d.inflight = false
	d.nextVisibleAt = now.Add(o.backoffDelay(d.attempt))
	return nackRetrying, nil, nil
}

// loadMessageLocked 调用方持锁时读取消息内容。
func (s *store) loadMessageLocked(ie *indexEntry) (*Message, error) {
	// 读取磁盘时短暂不持锁会更优，但本实现读路径很短；直接读。
	data, err := s.readFrameAt(ie.framePos, ie.frameSize)
	if err != nil {
		return nil, err
	}
	rt, payload, _, err := decodeFrame(data)
	if err != nil || rt != recMsg {
		return nil, errFrame
	}
	m, err := decodeMsg(payload)
	if err != nil {
		return nil, err
	}
	return &Message{Key: m.key, Body: m.body, Headers: m.headers, DedupKey: m.dedupKey}, nil
}

// expireInflight 将所有租约到期但既未确认也未 Nack 的消息标记为可重投，
// 返回受影响的 (group, offset) 列表。这些消息的下一次投递会在
// nextDeliverable 中以 attempt+1 重新发出。
func (s *store) expireInflight(now time.Time) []struct {
	group  string
	offset int64
} {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []struct {
		group  string
		offset int64
	}
	for name, g := range s.groups {
		for off, d := range g.delivering {
			if d.inflight && !now.Before(d.leaseDeadline) {
				d.inflight = false
				out = append(out, struct {
					group  string
					offset int64
				}{name, off})
			}
		}
	}
	return out
}

// watermark 返回组在某分区的已提交位点。
func (s *store) watermark(group string, partition uint32) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, ok := s.groups[group]
	if !ok {
		return 0
	}
	return g.watermark[partition]
}

// retainedBytes 返回当前必须保留的消息字节数。
func (s *store) retainedBytes() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.retained
}

// deadLetters 返回死信快照。
func (s *store) deadLetters() []DeadLetter {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]DeadLetter, len(s.dlq))
	copy(out, s.dlq)
	return out
}

// gcMessages 删除所有组都已提交位点之前的消息分段（只删整段，保守安全）。
func (s *store) gcMessages() error {
	s.mu.Lock()
	minPos := int64(1 << 62)
	if len(s.groups) == 0 {
		s.mu.Unlock()
		return nil
	}
	for _, ie := range s.index {
		retained := false
		for _, g := range s.groups {
			if g.watermark[ie.partition] < ie.offset {
				retained = true
				break
			}
		}
		if retained && ie.framePos < minPos {
			minPos = ie.framePos
		}
	}
	s.mu.Unlock()
	if minPos == 1<<62 {
		// 没有需要保留的消息：保留最后一个活跃分段，删除更早分段。
		bases := s.msgWAL.segmentBases()
		if len(bases) <= 1 {
			return nil
		}
		minPos = bases[len(bases)-1]
	}
	if _, err := s.msgWAL.deleteSegmentsBefore(minPos); err != nil {
		return err
	}
	// 记录物理回收水位：framePos < minPos 的消息已被删除，
	// 其对应的 confirmed 幂等键可以安全回收（不可能再重放/重复确认）。
	s.mu.Lock()
	if minPos > s.gcPos {
		s.gcPos = minPos
	}
	s.mu.Unlock()
	return nil
}

// entryOf 返回某 offset 的索引项。
func (s *store) entryOf(offset int64) (*indexEntry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ie, ok := s.byOffset[offset]
	return ie, ok
}

// offsetExists 判断某 offset 的消息物理数据是否仍在（未被分段 GC）。
// 物理分段被删除后返回 false，幂等判重键可随之安全回收。
func (s *store) offsetExists(offset int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	ie, ok := s.byOffset[offset]
	if !ok {
		return false
	}
	return ie.framePos >= s.gcPos
}

// ensureContiguousLocked 校验从 watermark+1 到 offset 的同分区消息均已处理
// （不在 delivering 中，offset 自身除外）。调用方持锁。
func (s *store) ensureContiguousLocked(g *groupState, partition uint32, offset int64) error {
	begin := s.indexForWatermark(partition, g.watermark[partition])
	offs := s.partOffsets[partition]
	for i := begin; i < len(offs) && offs[i] < offset; i++ {
		if _, busy := g.delivering[offs[i]]; busy {
			return ErrInvalidArgument
		}
	}
	return nil
}

// commitDedup 原子地（崩溃一致的顺序写）完成“幂等键生效 + 位点前进”：
// 先持久化 recDedup，再持久化 recCommit。
// 若在两条记录之间崩溃，重启后该消息会被重新投递，但幂等判定为
// GuardDuplicate，从而跳过业务副作用并补提位点，不会重复生效。
func (s *store) commitDedup(group string, partition uint32, offset int64, dedupKey string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, ok := s.groups[group]
	if !ok {
		g = s.getOrCreateGroup(group)
	}
	if offset < g.watermark[partition] {
		return g.watermark[partition], ErrOffsetRollback
	}
	if offset == g.watermark[partition] {
		return offset, nil
	}
	if err := s.ensureContiguousLocked(g, partition, offset); err != nil {
		return g.watermark[partition], err
	}
	if dedupKey != "" {
		if err := s.meta.append(recDedup, encodeDedup(&dedupRecord{
			group: group, dedup: dedupKey, offset: offset,
		})); err != nil {
			return g.watermark[partition], err
		}
	}
	if err := s.meta.append(recCommit, encodeCommit(&commitRecord{
		group: group, partition: partition, offset: offset,
	})); err != nil {
		return g.watermark[partition], err
	}
	old := g.watermark[partition]
	g.watermark[partition] = offset
	g.cursor[int(partition)] = s.indexForWatermark(partition, offset)
	for off := old + 1; off <= offset; off++ {
		if ie, ok := s.byOffset[off]; ok && ie.partition == partition {
			s.releaseIfSafeLocked(ie)
		}
		delete(g.delivering, off)
	}
	return offset, nil
}

// earliestWakeup 返回该组下一个可能有消息可投递的最早时间提示
// （所有分区队头都在退避时取最小值）；零值表示无需等待退避。
func (s *store) earliestWakeup(group string) time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, ok := s.groups[group]
	if !ok {
		return time.Time{}
	}
	var earliest time.Time
	for p := uint32(0); p < s.opts.Partitions; p++ {
		cursor := g.cursor[p]
		offs := s.partOffsets[p]
		for cursor < len(offs) && offs[cursor] <= g.watermark[p] {
			cursor++
		}
		if cursor >= len(offs) {
			continue
		}
		if d, busy := g.delivering[offs[cursor]]; busy && !d.inflight && d.nextVisibleAt.After(time.Now()) {
			if earliest.IsZero() || d.nextVisibleAt.Before(earliest) {
				earliest = d.nextVisibleAt
			}
		}
	}
	return earliest
}
