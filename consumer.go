package eventqueue

import (
	"context"
	"errors"
	"sync"
	"time"
)

// pollWaitIdle 空闲无消息时的轮询间隔（不依赖任何外部时钟服务）。
const pollWaitIdle = 10 * time.Millisecond

// Subscriber 是一个消费组的拉取句柄。
type Subscriber struct {
	q     *Queue
	group string

	mu     sync.Mutex
	rr     uint32
	closed bool
}

// Subscriber 打开/获取一个消费组。不同组各自维护位点；
// 同一组内同一 key（同一分区）严格按写入顺序消费。
func (q *Queue) Subscriber(group string) (*Subscriber, error) {
	if group == "" {
		return nil, errors.New("eventqueue: empty consumer group")
	}
	q.st.ensureGroup(group)
	return &Subscriber{q: q, group: group}, nil
}

// TryPoll 非阻塞地尝试拉取一条消息；当前没有可投递消息时返回 (nil, nil)。
// 退避中的消息、租约未到期的消息同样视为暂不可投递。
func (s *Subscriber) TryPoll(ctx context.Context) (*Envelope, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	hint := s.rr
	s.mu.Unlock()
	now := time.Now()
	ie, d, newHint, ok := s.q.st.nextDeliverable(s.group, now, s.q.opts.AckTimeout, hint)
	s.mu.Lock()
	s.rr = newHint
	s.mu.Unlock()
	if !ok {
		return nil, nil
	}
	return s.wrapEnvelope(ie, d)
}

func (s *Subscriber) wrapEnvelope(ie *indexEntry, d *deliveryEntry) (*Envelope, error) {
	msg, err := s.q.st.loadMessage(ie)
	if err != nil {
		return nil, err
	}
	return &Envelope{
		Offset:           ie.offset,
		Partition:        ie.partition,
		Attempt:          d.attempt,
		Message:          *msg,
		EnqueuedAt:       ie.enqueuedAt,
		FirstDeliveredAt: d.firstDeliveredAt,
	}, nil
}

// Poll 阻塞拉取一条当前可投递的消息，直到有消息或 ctx 取消。
// 投递会跳过已提交位点、应用重试退避与 Ack 租约超时重投。
// 返回的 Envelope.Attempt 从 1 开始。
func (s *Subscriber) Poll(ctx context.Context) (*Envelope, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if env, err := s.TryPoll(ctx); err != nil {
			return nil, err
		} else if env != nil {
			return env, nil
		}
		// 无消息：若分区队头处于退避中，则等到最近的退避结束。
		wake := s.q.st.earliestWakeup(s.group)
		wait := pollWaitIdle
		if !wake.IsZero() {
			if d := time.Until(wake); d > 0 {
				wait = d
			}
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-s.q.closeCh:
			timer.Stop()
			return nil, ErrClosed
		case <-timer.C:
		}
	}
}

// Guard 判定业务幂等键当前状态，不落盘、不前进位点：
//   - GuardFirstTime：此前未生效，调用方可以执行副作用，随后必须 Commit；
//   - GuardDuplicate：此前已确认生效，跳过副作用（位点会随 Commit 前进）；
//   - GuardLeased：另一次进行中的尝试持有该键（租约期内），本次应稍后重试。
func (s *Subscriber) Guard(dedupKey string) (GuardResult, error) {
	if dedupKey == "" {
		return GuardFirstTime, nil
	}
	return s.q.guard.statusOf(s.group, dedupKey), nil
}

// Commit 确认消息并（当 dedupKey 非空时）原子记录业务幂等生效。
//
// 语义：
//   - dedupKey 为空：等价于 CommitOffset；
//   - 首次确认非空 dedupKey：返回 GuardFirstTime，recDedup 与 recCommit
//     顺序落盘，保证“副作用只生效一次 + 位点前进”在崩溃下一致；
//   - 重复确认同一 dedupKey：返回 GuardDuplicate，不重复落盘；
//   - offset 小于已提交位点：返回 ErrOffsetRollback（位点回退被明确拒绝）。
func (s *Subscriber) Commit(ctx context.Context, offset int64, dedupKey string) (GuardResult, error) {
	if err := ctx.Err(); err != nil {
		return GuardFirstTime, err
	}
	ie, ok := s.q.st.entryOf(offset)
	if !ok {
		return GuardFirstTime, ErrInvalidArgument
	}

	if dedupKey == "" {
		if _, err := s.q.st.commit(s.group, ie.partition, offset); err != nil {
			return GuardFirstTime, err
		}
		s.q.refreshCapacity()
		return GuardFirstTime, nil
	}

	// 已确认生效：重复调用直接判重（不重复落盘）。
	switch s.q.guard.statusOf(s.group, dedupKey) {
	case GuardDuplicate:
		// 位点也同步前进（若落后），幂等安全。
		if s.q.st.watermark(s.group, ie.partition) < offset {
			if _, err := s.q.st.commit(s.group, ie.partition, offset); err != nil && !errors.Is(err, ErrOffsetRollback) {
				return GuardDuplicate, err
			}
			s.q.refreshCapacity()
		}
		return GuardDuplicate, nil
	case GuardLeased:
		return GuardLeased, nil
	}

	// 先在内存中占位（租约），再顺序落盘 recDedup + recCommit。
	now := time.Now()
	res, err := s.q.guard.claim(s.group, dedupKey, now, s.q.opts.AckTimeout, offset)
	if err != nil {
		return res, err
	}
	if res == GuardDuplicate {
		return GuardDuplicate, nil
	}
	if res == GuardLeased {
		return GuardLeased, nil
	}

	if _, err := s.q.st.commitDedup(s.group, ie.partition, offset, dedupKey); err != nil {
		// 落盘失败：撤销提交中标记，允许重试。
		s.q.guard.abortCommit(s.group, dedupKey)
		return GuardFirstTime, err
	}
	s.q.guard.confirm(s.group, dedupKey, offset)
	s.q.refreshCapacity()
	return GuardFirstTime, nil
}

// CommitOffset 仅提交位点（不使用业务幂等表）。
// offset 小于已提交位点时返回 ErrOffsetRollback。
func (s *Subscriber) CommitOffset(ctx context.Context, offset int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	ie, ok := s.q.st.entryOf(offset)
	if !ok {
		// 即便位点不是有效消息，只要它小于已提交位点，也按回退明确拒绝。
		if off := maxCommitted(s); off > 0 && offset < off {
			return ErrOffsetRollback
		}
		return ErrInvalidArgument
	}
	if cur := s.q.st.watermark(s.group, ie.partition); offset < cur {
		return ErrOffsetRollback
	}
	if _, err := s.q.st.commit(s.group, ie.partition, offset); err != nil {
		return err
	}
	s.q.refreshCapacity()
	return nil
}

// maxCommitted 返回该订阅组所有分区中最大的已提交位点（仅用于回退判定兜底）。
func maxCommitted(s *Subscriber) int64 {
	var m int64
	for p := uint32(0); p < s.q.opts.Partitions; p++ {
		if v := s.q.st.watermark(s.group, p); v > m {
			m = v
		}
	}
	return m
}

// CommittedOffset 返回某分区已提交位点。
func (s *Subscriber) CommittedOffset(partition uint32) int64 {
	return s.q.st.watermark(s.group, partition)
}

// Nack 显式报告消费失败：
//   - 未超 MaxAttempts：按配置退避后重新投递（attempt+1）；
//   - 达到 MaxAttempts：消息进入死信队列，位点前进，不再重复投递。
//
// 若该消息曾通过 Guard/Commit 流程占用幂等键，这里释放占位。
func (s *Subscriber) Nack(ctx context.Context, offset int64, cause error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	ie, ok := s.q.st.entryOf(offset)
	if !ok {
		return ErrInvalidArgument
	}
	// 释放可能存在的内存幂等占位（confirmed 的不会被释放）。
	s.q.guard.release(s.group, ie.dedupKey)

	reason := "nack"
	if cause != nil {
		reason = cause.Error()
	}
	res, dl, err := s.q.st.nack(s.group, offset, reason, time.Now(), s.q.opts)
	if err != nil {
		return err
	}
	s.q.refreshCapacity()
	if res == nackDeadLettered && dl != nil {
		// 死信后确认对应幂等键，避免残留 leased（若有）。
		if dl.DedupKey != "" {
			s.q.guard.confirm(s.group, dl.DedupKey, dl.Offset)
		}
	}
	return nil
}

// Close 关闭订阅句柄（不影响队列本身与已落盘数据）。
func (s *Subscriber) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}
