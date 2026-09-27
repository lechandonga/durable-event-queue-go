package eventqueue

import (
	"context"
	"sync"
	"time"
)

// Queue 是可嵌入单机进程的持久化事件队列引擎。
// 零值不可用，请通过 Open 创建。
type Queue struct {
	dir  string
	opts Options

	st    *store
	guard *idempotencyGuard

	// 容量许可：capTokens 为当前可用字节数。
	capMu     sync.Mutex
	capTokens int64
	space     *sync.Cond

	// 轮转提示：让各分区被公平调度。
	rrMu sync.Mutex
	rr   uint32

	closeOnce sync.Once
	closeCh   chan struct{}
	closed    bool

	wg sync.WaitGroup
}

// Open 打开（或创建）数据目录下的队列，并完成崩溃恢复：
// 已提交位点之前的消息不会再投递；未确认消息租约到期后重新投递。
func Open(dir string, opts Options) (*Queue, error) {
	opts.DataDir = dir
	opts = opts.withDefaults()
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	st, err := openStore(dir, opts)
	if err != nil {
		return nil, err
	}
	q := &Queue{
		dir:     dir,
		opts:    opts,
		st:      st,
		guard:   newIdempotencyGuard(),
		closeCh: make(chan struct{}),
	}
	q.space = sync.NewCond(&q.capMu)

	// 安装崩溃恢复出的已确认幂等表。
	snap := st.recoveredGuard
	for group, m := range snap {
		for d, off := range m {
			q.guard.loadConfirmed(group, d, off)
		}
	}
	st.recoveredGuard = nil

	// 注入快照函数（meta 压缩时获取 guard 的 confirmed 全量）。
	guardSnapshotFn = q.guard.confirmedSnapshot

	q.capTokens = opts.MaxBytes - st.retainedBytes()
	if q.capTokens < 0 {
		q.capTokens = 0
	}

	q.wg.Add(1)
	go q.backgroundLoop()
	return q, nil
}

// Publish 并发安全地写入一条消息。
// 容量达到上限时：
//   - PolicyReject：立即返回 *PublishError{Reason: RejectQueueFull}；
//   - PolicyBlock：阻塞等待，直到腾出空间、ctx 取消或队列关闭，绝不静默丢弃。
func (q *Queue) Publish(ctx context.Context, msg Message) (int64, error) {
	if msg.Key == "" {
		return 0, &PublishError{Reason: RejectInvalidMessage}
	}
	need := frameSizeOf(msg)
	if need > q.opts.MaxBytes {
		return 0, &PublishError{Reason: RejectInvalidMessage}
	}

	if q.opts.Policy == PolicyBlock {
		if err := q.acquireSpace(ctx, need); err != nil {
			return 0, err
		}
	} else {
		if !q.tryAcquireSpace(need) {
			return 0, &PublishError{Reason: RejectQueueFull}
		}
	}

	ie, err := q.st.appendMessage(msg)
	if err != nil {
		// 落盘失败：归还许可。
		q.releaseSpace(need)
		if err == ErrClosed {
			return 0, &PublishError{Reason: RejectQueueClosed}
		}
		return 0, err
	}
	return ie.offset, nil
}

func (q *Queue) tryAcquireSpace(need int64) bool {
	q.capMu.Lock()
	defer q.capMu.Unlock()
	if q.closed {
		return false
	}
	if q.capTokens >= need {
		q.capTokens -= need
		return true
	}
	return false
}

func (q *Queue) acquireSpace(ctx context.Context, need int64) error {
	// 任一等待结束条件触发时都要广播，确保 sync.Cond.Wait 能醒来重新判断。
	ctxStop := context.AfterFunc(ctx, func() { q.space.Broadcast() })
	defer ctxStop()

	timeout := q.opts.BlockTimeout
	deadline := time.Time{}
	var timer *time.Timer
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
		timer = time.AfterFunc(timeout, func() { q.space.Broadcast() })
		defer timer.Stop()
	}
	closeStop := goOnClose(q.closeCh, func() { q.space.Broadcast() })
	defer closeStop()

	q.capMu.Lock()
	defer q.capMu.Unlock()
	for {
		if q.closed {
			return &PublishError{Reason: RejectQueueClosed}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if timeout > 0 && !time.Now().Before(deadline) {
			return &PublishError{Reason: RejectQueueFull}
		}
		if q.capTokens >= need {
			q.capTokens -= need
			return nil
		}
		q.space.Wait()
	}
}

// goOnClose 在 ch 关闭时执行 fn，返回取消函数。
func goOnClose(ch <-chan struct{}, fn func()) (cancel func()) {
	done := make(chan struct{})
	go func() {
		select {
		case <-ch:
			fn()
		case <-done:
		}
	}()
	return sync.OnceFunc(func() { close(done) })
}

// releaseSpace 在消息被确认、死信（位点前进导致保留量下降）后归还容量。
func (q *Queue) releaseSpace(n int64) {
	if n <= 0 {
		return
	}
	q.capMu.Lock()
	q.capTokens += n
	if q.capTokens > q.opts.MaxBytes {
		q.capTokens = q.opts.MaxBytes
	}
	q.space.Broadcast()
	q.capMu.Unlock()
}

// refreshCapacity 用 store 的实际保留量校准容量许可。
// 提交位点/死信都会降低保留量，此处把腾出的空间还给阻塞中的生产者。
func (q *Queue) refreshCapacity() {
	retained := q.st.retainedBytes()
	q.capMu.Lock()
	avail := q.opts.MaxBytes - retained
	if avail < 0 {
		avail = 0
	}
	if avail > q.opts.MaxBytes {
		avail = q.opts.MaxBytes
	}
	q.capTokens = avail
	q.space.Broadcast()
	q.capMu.Unlock()
}

// RetainedBytes 返回当前因未确认而保留的字节数。
func (q *Queue) RetainedBytes() int64 { return q.st.retainedBytes() }

// DeadLetters 返回进入死信的全部记录（快照）。
func (q *Queue) DeadLetters() []DeadLetter { return q.st.deadLetters() }

// Close 优雅关闭（刷盘、停止后台循环）。
func (q *Queue) Close() error {
	var err error
	q.closeOnce.Do(func() {
		q.capMu.Lock()
		q.closed = true
		q.capMu.Unlock()
		close(q.closeCh)
		q.space.Broadcast()
		q.wg.Wait()
		err = q.st.close()
		guardSnapshotFn = nil
	})
	return err
}

// backgroundLoop 周期性处理租约超时、幂等租约过期、元数据压缩与消息 GC。
func (q *Queue) backgroundLoop() {
	defer q.wg.Done()
	ticker := time.NewTicker(q.opts.LeaseScanInterval)
	defer ticker.Stop()
	for {
		select {
		case <-q.closeCh:
			return
		case now := <-ticker.C:
			q.scanTick(now)
		}
	}
}

func (q *Queue) scanTick(now time.Time) {
	// 1) guard 中进行中的幂等租约超时：释放内存占位。
	_ = q.guard.expireLeases(now)
	// 2) 未确认消息租约超时：置为可重投，下一次 Poll 以 attempt+1 投递
	//    （至少一次语义；崩溃/强杀后重启也会经历同样路径）。
	_ = q.st.expireInflight(now)
	// 3) 元数据压缩。
	if err := q.st.compactMeta(); err != nil {
		_ = err // 压缩失败不影响正确性，下轮再试。
	}
	// 4) 先回收已无任何组需要的消息分段，再据此回收幂等判重表：
	//    只有消息物理数据已删除（不可能再重投/重复确认）时才安全删除判重键。
	if err := q.st.gcMessages(); err != nil {
		_ = err
	}
	_ = q.guard.gcConfirmed(q.st.offsetExists)
}
