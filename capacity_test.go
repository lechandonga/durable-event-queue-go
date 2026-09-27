package eventqueue

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCapacityRejectReason(t *testing.T) {
	dir := t.TempDir()
	opts := testOpts(dir)
	opts.Policy = PolicyReject
	opts.MaxBytes = 1024 // 很小
	q, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	ctx := context.Background()

	rejectedFull := 0
	ok := 0
	for i := 0; i < 200; i++ {
		_, err := q.Publish(ctx, Message{Key: "k", Body: make([]byte, 200)})
		if err != nil {
			var pe *PublishError
			if !errors.As(err, &pe) {
				t.Fatalf("want *PublishError, got %T %v", err, err)
			}
			if pe.Reason != RejectQueueFull {
				t.Fatalf("reject reason = %s want queue-full", pe.Reason)
			}
			rejectedFull++
		} else {
			ok++
		}
	}
	if rejectedFull == 0 {
		t.Fatal("expected capacity rejections")
	}
	t.Logf("capacity: accepted=%d rejected(queue-full)=%d retained=%d/%d",
		ok, rejectedFull, q.RetainedBytes(), opts.MaxBytes)

	// 非法消息（空 key）必须区分原因。
	_, err = q.Publish(ctx, Message{Key: "", Body: []byte("x")})
	var pe *PublishError
	if !errors.As(err, &pe) || pe.Reason != RejectInvalidMessage {
		t.Fatalf("empty key reject = %v want invalid-message", err)
	}
}

func TestCapacityBlockThenRelease(t *testing.T) {
	dir := t.TempDir()
	opts := testOpts(dir)
	opts.Policy = PolicyBlock
	opts.BlockTimeout = time.Second
	opts.MaxBytes = 1024
	q, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	ctx := context.Background()
	sub, _ := q.Subscriber("g-cap")

	// 塞满。
	var offsets []int64
	for {
		off, err := q.Publish(ctx, Message{Key: "k", Body: make([]byte, 150)})
		if err != nil {
			break
		}
		offsets = append(offsets, off)
	}
	t.Logf("filled queue with %d messages, publishing blocked", len(offsets))

	// 再发一条：阻塞策略下应等待，超时后以 queue-full 明确失败（不静默丢）。
	start := time.Now()
	_, err = q.Publish(ctx, Message{Key: "k", Body: make([]byte, 150)})
	elapsed := time.Since(start)
	var pe *PublishError
	if !errors.As(err, &pe) || pe.Reason != RejectQueueFull {
		t.Fatalf("blocked publish timeout should reject queue-full, got %v", err)
	}
	if elapsed < time.Second-100*time.Millisecond {
		t.Fatalf("should block ~BlockTimeout, blocked only %v", elapsed)
	}
	t.Logf("blocked publish correctly failed after %v with reason=%s", elapsed, pe.Reason)

	// 消费并提交腾出空间后，新的发布应立即成功。
	e, err := sub.Poll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := sub.CommitOffset(ctx, e.Offset); err != nil {
		t.Fatal(err)
	}
	off, err := q.Publish(ctx, Message{Key: "k", Body: make([]byte, 150)})
	if err != nil {
		t.Fatalf("publish after release should succeed: %v", err)
	}
	t.Logf("after consuming 1 message, new publish succeeded offset=%d", off)
}
