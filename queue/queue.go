package queue

import (
	"errors"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Message 是生产请求。
type Message struct {
	Key     string // 相同 key 在组内严格按写入顺序消费
	Payload []byte
}

// DeliveredMessage 是投递给消费者的消息。
type DeliveredMessage struct {
	Offset  int64
	Key     string
	Payload []byte
	Attempt int // 当前是第几次投递（1 表示首次）
}

// Queue 是可嵌入单机进程的持久化事件队列引擎。
type Queue struct {
	cfg    Config
	logger Logger

	wal    *messageLog
	groups map[string]*groupState
	stores map[string]*groupStore

	dedup *dedupStore

	mu          sync.Mutex
	produceCond *sync.Cond // 容量释放时唤醒阻塞的生产者
	pullCond    *sync.Cond // 新消息 / 重试到期时唤醒拉取
	closed      bool

	stopReaper chan struct{}
	reaperDone chan struct{}

	dedupMu      sync.Mutex
	dedupLeaders map[string]chan struct{}
}

type nopLogger struct{}

func (nopLogger) Printf(string, ...any) {}

// Open 创建/打开一个队列引擎。
func Open(cfg Config) (*Queue, error) {
	if cfg.DataDir == "" {
		return nil, errors.New("durablequeue: DataDir required")
	}
	def := DefaultConfig(cfg.DataDir)
	if cfg.MaxPendingMessages == 0 && cfg.MaxDataBytes == 0 {
		cfg.MaxPendingMessages = def.MaxPendingMessages
		cfg.MaxDataBytes = def.MaxDataBytes
	}
	if cfg.MaxDeliveryAttempts == 0 {
		cfg.MaxDeliveryAttempts = def.MaxDeliveryAttempts
	}
	if cfg.Backoff == nil {
		cfg.Backoff = def.Backoff
	}
	if cfg.VisibilityTimeout == 0 {
		cfg.VisibilityTimeout = def.VisibilityTimeout
	}
	if cfg.DedupHistory <= 0 {
		cfg.DedupHistory = def.DedupHistory
	}
	if cfg.Logger == nil {
		cfg.Logger = log.New(os.Stderr, "[durablequeue] ", log.LstdFlags|log.Lmicroseconds)
	}
	if err := os.MkdirAll(filepath.Join(cfg.DataDir, "groups"), 0o755); err != nil {
		return nil, err
	}
	wal, err := openMessageLog(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	dedup, err := openDedupStore(cfg.DataDir, cfg.DedupHistory)
	if err != nil {
		wal.close()
		return nil, err
	}
	q := &Queue{
		cfg:          cfg,
		logger:       cfg.Logger,
		wal:          wal,
		groups:       make(map[string]*groupState),
		stores:       make(map[string]*groupStore),
		dedup:        dedup,
		stopReaper:   make(chan struct{}),
		reaperDone:   make(chan struct{}),
		dedupLeaders: make(map[string]chan struct{}),
	}
	q.produceCond = sync.NewCond(&q.mu)
	q.pullCond = sync.NewCond(&q.mu)
	if err := q.loadGroups(); err != nil {
		wal.close()
		dedup.close()
		return nil, err
	}
	go q.visibilityReaper()
	return q, nil
}

// loadGroups 加载 groups 目录下全部消费组状态，并把未确认消息恢复为可重投。
func (q *Queue) loadGroups() error {
	entries, err := os.ReadDir(filepath.Join(q.cfg.DataDir, "groups"))
	if err != nil {
		return err
	}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || filepath.Ext(name) != ".json" {
			continue
		}
		name = name[:len(name)-len(".json")]
		store := newGroupStore(q.cfg.DataDir, name)
		st, err := store.load()
		if err != nil {
			return err
		}
		if st == nil {
			continue
		}
		// 崩溃恢复：所有 inflight 都未确认，必须可重新投递。
		// 保留已记录的投递次数，重试计时立即到期。
		for off := range st.Inflight {
			st.Retry[off] = 0
		}
		st.Inflight = make(map[int64]*inflightState)
		q.groups[name] = st
		q.stores[name] = store
		q.logger.Printf("recover group=%s committed=%d inflightReset=true messages=%d",
			name, st.CommittedOffset, q.wal.nextOffset()-st.CommittedOffset)
	}
	return nil
}

// ---------- 生产 ----------

// Produce 写入一条消息。满时按 FullPolicy 拒绝（ErrQueueFull）或阻塞。
func (q *Queue) Produce(m Message) (int64, error) {
	if q.cfg.FullPolicy == PolicyBlock {
		return q.ProduceWait(m, q.cfg.BlockProduceTimeout)
	}
	return q.produceLocked(m, false, 0)
}

// ProduceWait 队列满时阻塞等待容量；timeout<=0 表示永久等待，超时返回 ErrTimeout。
func (q *Queue) ProduceWait(m Message, timeout time.Duration) (int64, error) {
	return q.produceLocked(m, true, timeout)
}

func (q *Queue) validate(m Message) error {
	if len(m.Payload) == 0 {
		return ErrInvalidMessage
	}
	return nil
}

func (q *Queue) produceLocked(m Message, wait bool, timeout time.Duration) (int64, error) {
	if err := q.validate(m); err != nil {
		return 0, err
	}
	deadline := time.Time{}
	if wait && timeout > 0 {
		deadline = time.Now().Add(timeout)
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	for {
		if q.closed {
			return 0, ErrQueueClosed
		}
		if !q.capacityFull() {
			break
		}
		if !wait {
			return 0, ErrQueueFull
		}
		if !deadline.IsZero() && !waitCond(q.produceCond, time.Until(deadline)) {
			return 0, ErrTimeout
		} else if deadline.IsZero() {
			q.produceCond.Wait()
		}
	}
	offset, err := q.wal.append(m.Key, m.Payload)
	if err != nil {
		return 0, err
	}
	q.logger.Printf("produce ok offset=%d key=%q bytes=%d", offset, m.Key, len(m.Payload))
	q.pullCond.Broadcast()
	return offset, nil
}

// capacityFull 在持有 q.mu 时调用。
func (q *Queue) capacityFull() bool {
	oldest := q.oldestCommitted()
	pending := q.wal.nextOffset() - oldest
	if q.cfg.MaxPendingMessages > 0 && pending >= q.cfg.MaxPendingMessages {
		return true
	}
	if q.cfg.MaxDataBytes > 0 && q.wal.sizeBytes() >= q.cfg.MaxDataBytes {
		return true
	}
	return false
}

// oldestCommitted 返回所有消费组中最小的已提交位点；没有消费组时为 0。
func (q *Queue) oldestCommitted() int64 {
	oldest := q.wal.nextOffset()
	if len(q.groups) == 0 {
		return 0
	}
	for _, st := range q.groups {
		if st.CommittedOffset < oldest {
			oldest = st.CommittedOffset
		}
	}
	return oldest
}

// ---------- 消费组 ----------

// CreateConsumerGroup 创建消费组，位点初始化为 startOffset（第一条消息 offset 为 0）。
func (q *Queue) CreateConsumerGroup(name string, startOffset int64) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrQueueClosed
	}
	if _, ok := q.groups[name]; ok {
		return ErrAlreadyExists
	}
	store := newGroupStore(q.cfg.DataDir, name)
	if ex, err := store.exists(); err != nil {
		return err
	} else if ex {
		st, err := store.load()
		if err != nil {
			return err
		}
		q.groups[name] = st
		q.stores[name] = store
		return ErrAlreadyExists
	}
	if startOffset < 0 {
		startOffset = 0
	}
	if startOffset > q.wal.nextOffset() {
		startOffset = q.wal.nextOffset()
	}
	st := newGroupState(name, startOffset)
	if err := store.persist(st); err != nil {
		return err
	}
	q.groups[name] = st
	q.stores[name] = store
	q.logger.Printf("group created name=%s startOffset=%d", name, startOffset)
	return nil
}

// ConsumerGroup 返回已存在的消费组句柄。
func (q *Queue) ConsumerGroup(name string) (*ConsumerGroup, error) {
	q.mu.Lock()
	_, ok := q.groups[name]
	q.mu.Unlock()
	if !ok {
		return nil, ErrNotFound
	}
	return &ConsumerGroup{name: name, q: q}, nil
}

// ---------- 拉取 ----------

// Pull 阻塞拉取最多 max 条消息；timeout<=0 在有至少一条前一直等待。
// 相同 key 只有队首消息可被投递，保证组内按写入顺序消费。
func (q *Queue) Pull(group string, max int, timeout time.Duration) ([]*DeliveredMessage, error) {
	if max <= 0 {
		max = 32
	}
	deadline := time.Time{}
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	for {
		if q.closed {
			return nil, ErrQueueClosed
		}
		st, ok := q.groups[group]
		if !ok {
			return nil, ErrNotFound
		}
		if changed := q.sweepExpired(st); changed {
			if err := q.persistGroup(st); err != nil {
				return nil, err
			}
		}
		batch := q.selectBatch(st, max)
		if len(batch) > 0 {
			if err := q.persistGroup(st); err != nil {
				return nil, err
			}
			return batch, nil
		}
		if deadline.IsZero() {
			q.pullCond.Wait()
			continue
		}
		d := time.Until(deadline)
		if d <= 0 {
			return nil, ErrTimeout
		}
		waitCond(q.pullCond, d)
	}
}

// selectBatch 在持有 q.mu 时挑选可投递消息，每个 key 仅选队首。
func (q *Queue) selectBatch(st *groupState, max int) []*DeliveredMessage {
	now := time.Now().UnixNano()
	chosenKey := make(map[string]bool)
	blockedKey := make(map[string]bool)
	var batch []*DeliveredMessage
	for off := st.CommittedOffset; off < q.wal.nextOffset() && len(batch) < max; off++ {
		if st.Acked[off] {
			continue
		}
		if _, inflight := st.Inflight[off]; inflight {
			rec, err := q.wal.readAt(off)
			if err == nil {
				blockedKey[rec.Key] = true
			}
			continue
		}
		if isDead(st, off) {
			continue
		}
		rec, err := q.wal.readAt(off)
		if err != nil {
			continue
		}
		if blockedKey[rec.Key] || chosenKey[rec.Key] {
			continue
		}
		if nb := st.Retry[off]; nb > now {
			blockedKey[rec.Key] = true // 队首退避中，同 key 后续消息一并等待
			continue
		}
		// 投递：计入次数并设置可见性超时。
		attempts := st.Attempts[off] + 1
		st.Attempts[off] = attempts
		dl := time.Time{}
		if q.cfg.VisibilityTimeout > 0 {
			dl = time.Now().Add(q.cfg.VisibilityTimeout)
		}
		st.Inflight[off] = &inflightState{
			Offset:        off,
			Attempts:      attempts,
			FirstPullUnix: now,
			DeadlineUnix:  dl.UnixNano(),
		}
		delete(st.Retry, off)
		if off > st.MaxDelivered {
			st.MaxDelivered = off
		}
		chosenKey[rec.Key] = true
		batch = append(batch, &DeliveredMessage{
			Offset:  off,
			Key:     rec.Key,
			Payload: append([]byte(nil), rec.Payload...),
			Attempt: attempts,
		})
		q.logger.Printf("deliver group=%s offset=%d key=%q attempt=%d committed=%d",
			st.Name, off, rec.Key, attempts, st.CommittedOffset)
	}
	return batch
}

// sweepExpired 把可见性超时的 inflight 消息转为重试或死信。返回是否有变更。
func (q *Queue) sweepExpired(st *groupState) bool {
	now := time.Now().UnixNano()
	changed := false
	for off, inf := range st.Inflight {
		if inf.DeadlineUnix == 0 || now < inf.DeadlineUnix {
			continue
		}
		delete(st.Inflight, off)
		q.scheduleRetryOrDead(st, off, inf.Attempts, errors.New("visibility timeout"))
		changed = true
	}
	return changed
}

func isDead(st *groupState, off int64) bool {
	for _, dl := range st.DeadLetters {
		if dl.Offset == off {
			return true
		}
	}
	return false
}

// scheduleRetryOrDead 在持有 q.mu 时调用：按次数决定退避重试还是进入死信。
func (q *Queue) scheduleRetryOrDead(st *groupState, off int64, attempts int, cause error) {
	maxed := q.cfg.MaxDeliveryAttempts > 0 && attempts >= q.cfg.MaxDeliveryAttempts
	if maxed {
		rec, err := q.wal.readAt(off)
		var key string
		var payload []byte
		if err == nil {
			key = rec.Key
			payload = rec.Payload
		}
		dl := &deadLetter{
			Offset:       off,
			Key:          key,
			Payload:      append([]byte(nil), payload...),
			Attempts:     attempts,
			LastError:    cause.Error(),
			DeadUnixNano: time.Now().UnixNano(),
		}
		st.DeadLetters = append(st.DeadLetters, dl)
		delete(st.Retry, off)
		q.logger.Printf("deadletter group=%s offset=%d key=%q attempts=%d cause=%q committed=%d",
			st.Name, off, key, attempts, cause, st.CommittedOffset)
		q.advanceAndReclaim(st)
		return
	}
	nb := time.Now().Add(q.cfg.Backoff(attempts)).UnixNano()
	st.Retry[off] = nb
	q.logger.Printf("retry schedule group=%s offset=%d attempts=%d nextIn=%s committed=%d",
		st.Name, off, attempts, q.cfg.Backoff(attempts), st.CommittedOffset)
}

// ---------- Ack / Nack / Commit ----------

// Ack 确认单条消息处理完成；连续已确认前缀会自动推进位点。
func (q *Queue) Ack(group string, offset int64) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	st, ok := q.groups[group]
	if !ok {
		return ErrNotFound
	}
	if offset < st.CommittedOffset {
		return nil // 位点之前的消息视为已确认（重复 ack 幂等）
	}
	if _, inflight := st.Inflight[offset]; !inflight {
		// 允许 ack 已被可见性超时重投、但本次处理其实成功的消息：
		// 若它在重试等待中，直接确认可以避免一次多余重投。
		if _, retry := st.Retry[offset]; !retry {
			// 从未投递给本组的消息不能被 ack，防止错误位点跳过未处理消息。
			if st.Attempts[offset] == 0 {
				return ErrNotFound
			}
			return nil
		}
	}
	delete(st.Inflight, offset)
	delete(st.Retry, offset)
	st.Acked[offset] = true
	q.advanceAndReclaim(st)
	if err := q.persistGroup(st); err != nil {
		return err
	}
	q.logger.Printf("ack group=%s offset=%d committed=%d", group, offset, st.CommittedOffset)
	q.pullCond.Broadcast()
	q.produceCond.Broadcast()
	return nil
}

// Nack 否定确认：按退避策略安排重试，超过 MaxDeliveryAttempts 进入死信。
func (q *Queue) Nack(group string, offset int64, cause error) error {
	if cause == nil {
		cause = errors.New("nack without cause")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	st, ok := q.groups[group]
	if !ok {
		return ErrNotFound
	}
	inf, inflight := st.Inflight[offset]
	if !inflight {
		if offset < st.CommittedOffset || isDead(st, offset) {
			return nil // 已终结的消息，nack 幂等
		}
		if st.Attempts[offset] == 0 {
			return ErrNotFound
		}
		inf = &inflightState{Offset: offset, Attempts: st.Attempts[offset]}
	}
	delete(st.Inflight, offset)
	attempts := inf.Attempts
	if attempts == 0 {
		attempts = st.Attempts[offset]
	}
	q.scheduleRetryOrDead(st, offset, attempts, cause)
	if err := q.persistGroup(st); err != nil {
		return err
	}
	q.pullCond.Broadcast()
	return nil
}

// Commit 显式提交位点。语义：offset 之前（不含 offset）的消息不再投递。
// 回退到已提交位点之前/等于已提交位点的请求返回 ErrOffsetRollback。
// 超过连续已确认前缀的请求返回 ErrOffsetOutOfRange（状态不变）。
func (q *Queue) Commit(group string, offset int64) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	st, ok := q.groups[group]
	if !ok {
		return ErrNotFound
	}
	if offset <= st.CommittedOffset {
		q.logger.Printf("commit REJECTED rollback group=%s requested=%d committed=%d",
			group, offset, st.CommittedOffset)
		return ErrOffsetRollback
	}
	cursor := q.contiguousCursor(st)
	if cursor < offset {
		q.logger.Printf("commit REJECTED outofrange group=%s requested=%d safeCursor=%d",
			group, offset, cursor)
		return ErrOffsetOutOfRange
	}
	st.CommittedOffset = cursor
	if err := q.persistGroup(st); err != nil {
		return err
	}
	q.afterCommit()
	q.logger.Printf("commit ok group=%s offset=%d committed=%d", group, offset, st.CommittedOffset)
	return nil
}

// contiguousCursor 返回从当前位点起，连续被 ack 或已进死信的最大前缀位点。
func (q *Queue) contiguousCursor(st *groupState) int64 {
	cursor := st.CommittedOffset
	for {
		if st.Acked[cursor] || isDead(st, cursor) {
			cursor++
			continue
		}
		break
	}
	return cursor
}

// advanceAndReclaim 自动推进位点并尝试回收消息日志前缀。持锁调用。
func (q *Queue) advanceAndReclaim(st *groupState) {
	cursor := q.contiguousCursor(st)
	if cursor > st.CommittedOffset {
		st.CommittedOffset = cursor
		if err := q.reclaimIfPossible(); err != nil {
			q.logger.Printf("log reclaim error: %v", err)
		}
	}
}

func (q *Queue) afterCommit() {
	if err := q.reclaimIfPossible(); err != nil {
		q.logger.Printf("log reclaim error: %v", err)
	}
	q.produceCond.Broadcast()
}

// reclaimIfPossible 丢弃所有消费组都已提交的消息前缀，缩小日志占用。
func (q *Queue) reclaimIfPossible() error {
	oldest := q.oldestCommitted()
	if oldest <= 0 {
		return nil
	}
	return q.wal.discardBefore(oldest)
}

// CommittedOffset 查询已提交位点。
func (q *Queue) CommittedOffset(group string) (int64, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	st, ok := q.groups[group]
	if !ok {
		return 0, ErrNotFound
	}
	return st.CommittedOffset, nil
}

// DeadLetters 返回死信快照（副本）。
func (q *Queue) DeadLetters(group string) ([]*deadLetter, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	st, ok := q.groups[group]
	if !ok {
		return nil, ErrNotFound
	}
	out := make([]*deadLetter, len(st.DeadLetters))
	for i, dl := range st.DeadLetters {
		cp := *dl
		cp.Payload = append([]byte(nil), dl.Payload...)
		out[i] = &cp
	}
	return out, nil
}

func (q *Queue) persistGroup(st *groupState) error {
	return q.stores[st.Name].persist(st)
}

// ---------- 可见性超时重投 ----------

func (q *Queue) visibilityReaper() {
	defer close(q.reaperDone)
	// 即使关闭可见性超时，reaper 仍需运行，以便重试退避到期时唤醒阻塞的 Pull。
	t := time.NewTicker(50 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-q.stopReaper:
			return
		case <-t.C:
		}
		q.mu.Lock()
		if q.closed {
			q.mu.Unlock()
			return
		}
		now := time.Now().UnixNano()
		var names []string
		for name, st := range q.groups {
			changed := false
			if q.cfg.VisibilityTimeout > 0 {
				changed = q.sweepExpired(st)
			}
			// 重试到期也唤醒拉取。
			for _, nb := range st.Retry {
				if nb <= now {
					changed = true
					break
				}
			}
			if changed {
				names = append(names, name)
			}
		}
		for _, name := range names {
			if err := q.persistGroup(q.groups[name]); err != nil {
				q.logger.Printf("reaper persist error group=%s: %v", name, err)
			}
		}
		if len(names) > 0 {
			q.pullCond.Broadcast()
		}
		q.mu.Unlock()
	}
}

// ---------- 幂等副作用 ----------

// ExecuteOnce 保证同一个 dedupKey 的业务副作用在本引擎生命周期内只生效一次：
//   - DedupNew：本次调用实际执行了 fn；
//   - DedupComplete：副作用此前已生效，fn 被跳过；
//   - DedupPending：当前有其它协程正在执行，调用方应稍后重试（本次不执行 fn）。
//
// 判定结果持久化：COMMIT 记录落盘（fsync）后才认为副作用生效，
// 因此重复投递、重试、进程重启都不会让副作用二次生效。
func (q *Queue) ExecuteOnce(dedupKey string, fn func() error) (DedupKind, error) {
	if dedupKey == "" {
		return 0, errors.New("durablequeue: empty dedup key")
	}
	if fn == nil {
		return 0, errors.New("durablequeue: nil side effect")
	}
	for {
		q.dedupMu.Lock()
		if ch, ok := q.dedupLeaders[dedupKey]; ok {
			q.dedupMu.Unlock()
			<-ch // 等待在途执行结束
			continue
		}
		ch := make(chan struct{})
		q.dedupLeaders[dedupKey] = ch
		q.dedupMu.Unlock()

		kind, err := q.dedup.begin(dedupKey)
		if err != nil {
			q.finishLeader(dedupKey, ch)
			return 0, err
		}
		leader := kind == DedupNew || kind == DedupPending
		// 重启后残留的 PENDING 没有属主，本调用接管为 leader。
		if !leader {
			q.finishLeader(dedupKey, ch)
			q.logger.Printf("idempotent key=%q verdict=COMPLETE (skip side effect)", dedupKey)
			return DedupComplete, nil
		}
		verdict := kind
		if kind == DedupNew {
			q.logger.Printf("idempotent key=%q verdict=NEW (execute side effect)", dedupKey)
		} else {
			q.logger.Printf("idempotent key=%q verdict=PENDING-RECLAIMED (execute after crash)", dedupKey)
		}
		execErr := fn()
		if execErr != nil {
			if ferr := q.dedup.forget(dedupKey); ferr != nil {
				q.logger.Printf("idempotent key=%q forget persist error: %v", dedupKey, ferr)
			}
			q.finishLeader(dedupKey, ch)
			return verdict, execErr
		}
		if cerr := q.dedup.complete(dedupKey); cerr != nil {
			// COMMIT 未能落盘：为避免副作用与去重记录不一致，保守地移除属主，
			// 交由上层重试（at-least-once 边界）。
			q.finishLeader(dedupKey, ch)
			return verdict, cerr
		}
		q.finishLeader(dedupKey, ch)
		q.logger.Printf("idempotent key=%q committed=COMPLETE", dedupKey)
		return verdict, nil
	}
}

func (q *Queue) finishLeader(key string, ch chan struct{}) {
	q.dedupMu.Lock()
	if cur, ok := q.dedupLeaders[key]; ok && cur == ch {
		delete(q.dedupLeaders, key)
	}
	close(ch)
	q.dedupMu.Unlock()
}

// ---------- 关闭 ----------

// Close 干净关闭：状态在每次变更时均已 fsync，这里仅停止后台任务并关闭文件。
func (q *Queue) Close() error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return nil
	}
	q.closed = true
	q.produceCond.Broadcast()
	q.pullCond.Broadcast()
	close(q.stopReaper)
	q.mu.Unlock()
	<-q.reaperDone

	var errs []error
	q.mu.Lock()
	for _, st := range q.groups {
		if err := q.persistGroup(st); err != nil {
			errs = append(errs, err)
		}
	}
	errs = append(errs, q.wal.close(), q.dedup.close())
	q.mu.Unlock()
	return errors.Join(errs...)
}

// ---------- 工具 ----------

// waitCond 等待 cond，最多 d。d<=0 时立即返回 false（不等待）。
// 返回 true 表示被唤醒，false 表示超时。
func waitCond(c *sync.Cond, d time.Duration) bool {
	if d <= 0 {
		return false
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	ch := make(chan struct{})
	go func() {
		select {
		case <-timer.C:
			c.Broadcast()
		case <-ch:
		}
	}()
	c.Wait()
	close(ch)
	select {
	case <-timer.C:
		return false
	default:
		return true
	}
}
