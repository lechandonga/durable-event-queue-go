package eventqueue

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

func TestConcurrentProducers(t *testing.T) {
	dir := t.TempDir()
	opts := testOpts(dir)
	opts.Policy = PolicyReject
	opts.MaxBytes = 16 << 20
	q, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer q.Close()
	ctx := context.Background()

	const producers = 8
	const perProducer = 100
	var wg sync.WaitGroup
	var rejected int64
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(pid int) {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				key := fmt.Sprintf("key-%d", (pid*7+i)%11) // 11 个不同 key
				off, err := q.Publish(ctx, Message{Key: key, Body: []byte(fmt.Sprintf("p%d-%d", pid, i))})
				if err != nil {
					if pe, ok := err.(*PublishError); ok && pe.Reason == RejectQueueFull {
						atomic.AddInt64(&rejected, 1)
						continue
					}
					t.Errorf("publish: %v", err)
					return
				}
				_ = off
			}
		}(p)
	}
	wg.Wait()
	total := producers*perProducer - int(rejected)
	t.Logf("published ok=%d rejected(full)=%d", total, rejected)

	sub, err := q.Subscriber("grp-concurrent")
	if err != nil {
		t.Fatal(err)
	}

	consumed := int64(0)
	seen := make(map[int64]bool)
	var seenMu sync.Mutex
	perKeyOrder := make(map[string][]int64)

	pollCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for atomic.LoadInt64(&consumed) < int64(total) {
			e, err := sub.Poll(pollCtx)
			if err != nil {
				return
			}
			seenMu.Lock()
			if seen[e.Offset] {
				t.Errorf("duplicate delivery offset=%d", e.Offset)
			}
			seen[e.Offset] = true
			perKeyOrder[e.Message.Key] = append(perKeyOrder[e.Message.Key], e.Offset)
			seenMu.Unlock()
			if err := sub.CommitOffset(ctx, e.Offset); err != nil {
				t.Errorf("commit: %v", err)
				return
			}
			atomic.AddInt64(&consumed, 1)
		}
	}()
	select {
	case <-done:
	case <-pollTimeoutAfter():
		cancel()
		t.Fatalf("timeout: consumed=%d/%d", atomic.LoadInt64(&consumed), total)
	}
	cancel()

	if atomic.LoadInt64(&consumed) != int64(total) {
		t.Fatalf("message loss: consumed=%d want=%d", consumed, total)
	}
	// 同一 key 的消费顺序必须与全局 offset 顺序一致（分区内有序）。
	for key, offs := range perKeyOrder {
		for i := 1; i < len(offs); i++ {
			if offs[i] <= offs[i-1] {
				t.Fatalf("key %s delivered out of order: %v", key, offs)
			}
		}
	}
	t.Logf("committed offsets per partition: p0=%d p1=%d p2=%d",
		sub.CommittedOffset(0), sub.CommittedOffset(1), sub.CommittedOffset(2))
}
