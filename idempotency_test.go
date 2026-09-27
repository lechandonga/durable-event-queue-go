package eventqueue

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestRetryAndIdempotency 覆盖：
//  1. 消费失败按退避重试，日志打印 attempt；
//  2. 超过 MaxAttempts 进入死信；
//  3. 同一逻辑消息即使被重复投递（模拟重试成功后又因租约超时重投），
//     下游业务副作用（用 effect map 模拟）只生效一次；
//  4. 并发重复确认同一 dedupKey：只有一个 first-time，其余 duplicate，
//     绝不出现重复生效。
func TestRetryAndIdempotency(t *testing.T) {
	dir := t.TempDir()
	opts := testOpts(dir)
	opts.MaxAttempts = 3
	q, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	ctx := context.Background()
	sub, _ := q.Subscriber("grp-idem")

	// 业务副作用存储：dedupKey -> 生效次数。必须恒为 0 或 1。
	var mu sync.Mutex
	effects := map[string]int{}
	applyEffect := func(key string) int {
		mu.Lock()
		defer mu.Unlock()
		effects[key]++
		return effects[key]
	}

	// 消息 A：前两次 Nack（退避重试），第三次成功。
	if _, err := q.Publish(ctx, Message{Key: "ka", DedupKey: "eff-A", Body: []byte("A")}); err != nil {
		t.Fatal(err)
	}
	// 消息 B：一直失败直到死信。
	if _, err := q.Publish(ctx, Message{Key: "kb", DedupKey: "eff-B", Body: []byte("B")}); err != nil {
		t.Fatal(err)
	}

	pollDeadline := time.Now().Add(3 * time.Second)

	// 处理 A：前两次失败重试。
	var seenAAttempts []int
	for {
		e, err := pollUntil(sub, ctx, pollDeadline)
		if err != nil {
			t.Fatal(err)
		}
		if e.Message.DedupKey != "eff-A" {
			t.Fatalf("head-of-line blocking violated: got %s before A resolved", e.Message.DedupKey)
		}
		seenAAttempts = append(seenAAttempts, e.Attempt)
		if e.Attempt < 3 {
			t.Logf("A attempt=%d -> Nack (retry scheduled)", e.Attempt)
			if err := sub.Nack(ctx, e.Offset, errFail); err != nil {
				t.Fatal(err)
			}
			continue
		}
		// 第三次成功：幂等判定 + 提交。
		res, err := sub.Commit(ctx, e.Offset, e.Message.DedupKey)
		if err != nil {
			t.Fatal(err)
		}
		if res != GuardFirstTime {
			t.Fatalf("A first commit result=%s want first-time", res)
		}
		if n := applyEffect("eff-A"); n != 1 {
			t.Fatalf("A side effect applied %d times, want 1", n)
		}
		t.Logf("A attempt=%d committed, guard=%s effect-count=1", e.Attempt, res)
		break
	}
	if len(seenAAttempts) != 3 {
		t.Fatalf("A attempts=%v want 3 attempts (1,2,3)", seenAAttempts)
	}

	// 重复提交同一 dedupKey：必须判定 duplicate，副作用不增加。
	res, err := sub.Commit(ctx, 1, "eff-A")
	if err != nil {
		t.Fatal(err)
	}
	if res != GuardDuplicate {
		t.Fatalf("A duplicate commit result=%s want duplicate", res)
	}
	t.Logf("A duplicate commit -> guard=%s, effect-count stays %d", res, effects["eff-A"])

	// 处理 B：失败到死信。
	for {
		e, err := pollUntil(sub, ctx, pollDeadline)
		if err != nil {
			break
		}
		if e.Message.DedupKey != "eff-B" {
			t.Fatalf("unexpected message %s", e.Message.DedupKey)
		}
		if e.Attempt < opts.MaxAttempts {
			t.Logf("B attempt=%d -> Nack", e.Attempt)
			_ = sub.Nack(ctx, e.Offset, errFail)
			continue
		}
		t.Logf("B attempt=%d -> Nack -> dead letter expected", e.Attempt)
		if err := sub.Nack(ctx, e.Offset, errFail); err != nil {
			t.Fatal(err)
		}
		break
	}
	dlq := q.DeadLetters()
	if len(dlq) != 1 || dlq[0].DedupKey != "eff-B" {
		t.Fatalf("dead letter = %+v, want exactly eff-B", dlq)
	}
	t.Logf("B dead-lettered: offset=%d attempts=%d reason=%q", dlq[0].Offset, dlq[0].Attempts, dlq[0].Reason)
	if effects["eff-B"] != 0 {
		t.Fatalf("B side effect should never apply, got %d", effects["eff-B"])
	}

	// ---- 并发重复确认：同一 dedupKey 只有一个 first-time ----
	if _, err := q.Publish(ctx, Message{Key: "kc", DedupKey: "eff-C", Body: []byte("C")}); err != nil {
		t.Fatal(err)
	}
	e, err := pollUntil(sub, ctx, time.Now().Add(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if e.Message.DedupKey != "eff-C" {
		t.Fatalf("got %s want eff-C", e.Message.DedupKey)
	}
	const racers = 16
	var wg sync.WaitGroup
	var firstN, dupN, leasedN int
	var nm sync.Mutex
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := sub.Commit(ctx, e.Offset, "eff-C")
			if err != nil {
				return
			}
			nm.Lock()
			switch r {
			case GuardFirstTime:
				firstN++
				applyEffect("eff-C")
			case GuardDuplicate:
				dupN++
			case GuardLeased:
				leasedN++
			}
			nm.Unlock()
		}()
	}
	wg.Wait()
	t.Logf("concurrent commit eff-C: first-time=%d duplicate=%d leased=%d (racers=%d)",
		firstN, dupN, leasedN, racers)
	if firstN != 1 {
		t.Fatalf("exactly one first-time required, got %d", firstN)
	}
	if firstN+dupN+leasedN != racers {
		t.Fatalf("all racers must reach a verdict, got first=%d dup=%d leased=%d", firstN, dupN, leasedN)
	}
	if effects["eff-C"] != 1 {
		t.Fatalf("C side effect applied %d times, want 1", effects["eff-C"])
	}
	t.Logf("idempotency verdict: effect counts = %v (all <= 1 => no duplicate side effect)", effects)
}

var errFail = stderrf("simulated downstream failure")

type errString string

func (e errString) Error() string { return string(e) }

func stderrf(s string) error { return errString(s) }

func pollUntil(sub *Subscriber, ctx context.Context, deadline time.Time) (*Envelope, error) {
	c, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	return sub.Poll(c)
}
