package queue

import (
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// testLogger 把引擎日志（位点/重试次数/幂等判定）打到测试输出。
type testLogger struct{ t *testing.T }

func (l testLogger) Printf(format string, args ...any) {
	l.t.Logf("[engine] "+format, args...)
}

func newTestQueue(t *testing.T, mutate func(*Config)) *Queue {
	t.Helper()
	cfg := DefaultConfig(filepath.Join(t.TempDir(), "q"))
	cfg.Logger = testLogger{t}
	if mutate != nil {
		mutate(&cfg)
	}
	q, err := Open(cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = q.Close() })
	return q
}

func mustProduce(t *testing.T, q *Queue, key string, payload string) int64 {
	t.Helper()
	off, err := q.Produce(Message{Key: key, Payload: []byte(payload)})
	if err != nil {
		t.Fatalf("produce %q: %v", payload, err)
	}
	return off
}

// ---------- 基础投递 + 位点 ----------

func TestBasicDeliverAndCommit(t *testing.T) {
	q := newTestQueue(t, nil)
	if err := q.CreateConsumerGroup("g", 0); err != nil {
		t.Fatal(err)
	}
	mustProduce(t, q, "a", "m0")
	mustProduce(t, q, "b", "m1")

	msgs, err := q.Pull("g", 10, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 2 {
		t.Fatalf("want 2 delivered, got %d", len(msgs))
	}
	if msgs[0].Attempt != 1 || msgs[1].Attempt != 1 {
		t.Fatalf("first attempts must be 1: %+v", msgs)
	}
	if err := q.Ack("g", 0); err != nil {
		t.Fatal(err)
	}
	// 只 ack 0，位点不应越过未确认的 1。
	if off, _ := q.CommittedOffset("g"); off != 1 {
		t.Fatalf("committed want 1, got %d", off)
	}
	// 位点回退必须被明确拒绝。
	if err := q.Commit("g", 0); !errors.Is(err, ErrOffsetRollback) {
		t.Fatalf("want ErrOffsetRollback, got %v", err)
	}
	t.Logf("rollback rejected as expected: committed=%d", 1)

	if err := q.Ack("g", 1); err != nil {
		t.Fatal(err)
	}
	if off, _ := q.CommittedOffset("g"); off != 2 {
		t.Fatalf("committed want 2, got %d", off)
	}
	// 没有新消息，带超时拉取应返回 ErrTimeout。
	if _, err := q.Pull("g", 10, 60*time.Millisecond); !errors.Is(err, ErrTimeout) {
		t.Fatalf("want ErrTimeout, got %v", err)
	}
	// 提交超出安全前缀的位点被拒绝。
	if err := q.Commit("g", 100); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Fatalf("want ErrOffsetOutOfRange, got %v", err)
	}
	t.Logf("offset out-of-range rejected as expected")
}

// ---------- 容量：拒绝原因可区分 + 阻塞策略 ----------

func TestCapacityRejectReasons(t *testing.T) {
	q := newTestQueue(t, func(c *Config) {
		c.MaxPendingMessages = 2
		c.FullPolicy = PolicyReject
	})
	if err := q.CreateConsumerGroup("g", 0); err != nil {
		t.Fatal(err)
	}
	mustProduce(t, q, "k", "0")
	mustProduce(t, q, "k", "1")
	_, err := q.Produce(Message{Key: "k", Payload: []byte("2")})
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("want ErrQueueFull, got %v", err)
	}
	t.Logf("full queue rejected with ErrQueueFull")

	_, err = q.Produce(Message{Key: "k"})
	if !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("want ErrInvalidMessage, got %v", err)
	}
	t.Logf("invalid message rejected with ErrInvalidMessage")

	// 消费掉一条后腾出容量，再次写入必须成功（不允许静默丢消息）。
	msgs, _ := q.Pull("g", 1, time.Second)
	if err := q.Ack("g", msgs[0].Offset); err != nil {
		t.Fatal(err)
	}
	if off := mustProduce(t, q, "k", "2new"); off != 2 {
		t.Fatalf("want offset 2 after reclaim, got %d", off)
	}

	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Produce(Message{Key: "k", Payload: []byte("x")}); !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("want ErrQueueClosed, got %v", err)
	}
	t.Logf("closed queue rejected with ErrQueueClosed")
}

func TestCapacityBlockPolicy(t *testing.T) {
	q := newTestQueue(t, func(c *Config) {
		c.MaxPendingMessages = 1
		c.FullPolicy = PolicyBlock
	})
	if err := q.CreateConsumerGroup("g", 0); err != nil {
		t.Fatal(err)
	}
	mustProduce(t, q, "k", "0")
	// 超时短：应返回 ErrTimeout 而不是丢消息。
	start := time.Now()
	_, err := q.ProduceWait(Message{Key: "k", Payload: []byte("1")}, 80*time.Millisecond)
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("want ErrTimeout, got %v", err)
	}
	t.Logf("blocked produce timed out after %v (message not lost)", time.Since(start).Round(time.Millisecond))

	// 后台消费一条后，阻塞的生产者应被唤醒并写入成功。
	done := make(chan int64, 1)
	go func() {
		off, err := q.ProduceWait(Message{Key: "k", Payload: []byte("1")}, 5*time.Second)
		if err != nil {
			done <- -1
			return
		}
		done <- off
	}()
	time.Sleep(60 * time.Millisecond)
	msgs, _ := q.Pull("g", 1, time.Second)
	if err := q.Ack("g", msgs[0].Offset); err != nil {
		t.Fatal(err)
	}
	select {
	case off := <-done:
		if off != 1 {
			t.Fatalf("blocked produce want offset 1, got %d", off)
		}
		t.Logf("blocked produce unblocked, offset=%d", off)
	case <-time.After(2 * time.Second):
		t.Fatal("blocked produce never unblocked")
	}
}

// ---------- 重试退避 + 死信 ----------

func TestRetryBackoffAndDeadLetter(t *testing.T) {
	q := newTestQueue(t, func(c *Config) {
		c.MaxDeliveryAttempts = 3
		c.Backoff = ConstantBackoff(120 * time.Millisecond)
	})
	if err := q.CreateConsumerGroup("g", 0); err != nil {
		t.Fatal(err)
	}
	// 同 key 两条：第一条持续失败时，第二条必须被顺序阻塞。
	mustProduce(t, q, "k", "first")
	mustProduce(t, q, "k", "second")

	pullOne := func() *DeliveredMessage {
		msgs, err := q.Pull("g", 10, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if len(msgs) != 1 {
			t.Fatalf("ordering: want exactly head message, got %d: %+v", len(msgs), msgs)
		}
		return msgs[0]
	}

	m := pullOne()
	if string(m.Payload) != "first" || m.Attempt != 1 {
		t.Fatalf("unexpected first delivery: %+v", m)
	}
	if err := q.Nack("g", m.Offset, errors.New("boom-1")); err != nil {
		t.Fatal(err)
	}
	t.Logf("offset=%d nacked, retry-count so far=%d, awaiting backoff", m.Offset, m.Attempt)

	// 退避窗口内拉不到任何消息（同 key 第二条也被阻塞，保证顺序）。
	if _, err := q.Pull("g", 10, 50*time.Millisecond); !errors.Is(err, ErrTimeout) {
		t.Fatalf("want timeout during backoff, got %v", err)
	}

	m = pullOne()
	if m.Attempt != 2 {
		t.Fatalf("want attempt 2, got %d", m.Attempt)
	}
	if err := q.Nack("g", m.Offset, errors.New("boom-2")); err != nil {
		t.Fatal(err)
	}
	t.Logf("offset=%d redelivered retry-count=%d", m.Offset, m.Attempt)

	m = pullOne()
	if m.Attempt != 3 {
		t.Fatalf("want attempt 3, got %d", m.Attempt)
	}
	if err := q.Nack("g", m.Offset, errors.New("boom-3")); err != nil {
		t.Fatal(err)
	}
	t.Logf("offset=%d retry-count=%d reached cap, expect dead letter", m.Offset, m.Attempt)

	dls, err := q.DeadLetters("g")
	if err != nil {
		t.Fatal(err)
	}
	if len(dls) != 1 || dls[0].Offset != 0 || dls[0].Attempts != 3 || dls[0].LastError != "boom-3" {
		t.Fatalf("unexpected DLQ: %+v", dls)
	}
	t.Logf("dead-letter confirmed: offset=%d attempts=%d cause=%q committed=%d",
		dls[0].Offset, dls[0].Attempts, dls[0].LastError, mustCommitted(t, q))

	// 死信解锁同 key：第二条现在可以投递，位点应已跳过死信消息。
	m = pullOne()
	if string(m.Payload) != "second" {
		t.Fatalf("want second after dead letter, got %q", m.Payload)
	}
	if err := q.Ack("g", m.Offset); err != nil {
		t.Fatal(err)
	}
	if off, _ := q.CommittedOffset("g"); off != 2 {
		t.Fatalf("committed want 2 after dead letter skip, got %d", off)
	}
	t.Logf("final committed offset=%d", 2)
}

func mustCommitted(t *testing.T, q *Queue) int64 {
	t.Helper()
	off, err := q.CommittedOffset("g")
	if err != nil {
		t.Fatal(err)
	}
	return off
}

// ---------- 并发生产：同 key 顺序、不重复、不丢失 + 容量拒绝 ----------

func TestConcurrentProducersPerKeyOrder(t *testing.T) {
	const (
		keys      = 8
		producers = 16
		perProd   = 200
		total     = producers * perProd
	)
	q := newTestQueue(t, func(c *Config) {
		c.MaxPendingMessages = int64(total + 10)
		c.MaxDataBytes = 0
	})
	if err := q.CreateConsumerGroup("g", 0); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	var rejected int64
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(pid int) {
			defer wg.Done()
			for i := 0; i < perProd; i++ {
				key := fmt.Sprintf("k%d", pid%keys)
				payload := fmt.Sprintf("p%d-%04d", pid, i)
				for {
					off, err := q.Produce(Message{Key: key, Payload: []byte(payload)})
					if err == nil {
						_ = off
						break
					}
					if errors.Is(err, ErrQueueFull) {
						atomic.AddInt64(&rejected, 1)
						time.Sleep(time.Millisecond)
						continue
					}
					t.Errorf("produce: %v", err)
					return
				}
			}
		}(p)
	}
	wg.Wait()
	t.Logf("producers done: total=%d rejected(backpressure)=%d", total, atomic.LoadInt64(&rejected))

	// 并发消费：任何时刻同一 key 只能有一条在途消息（顺序保证的核心）。
	var (
		inflightMu   sync.Mutex
		inflightKey  = map[string]int64{}
		delivered    = map[int64]int{} // offset -> 出现次数
		lastOffByKey = map[string]int64{}
	)
	var consumed int64
	var workersWg sync.WaitGroup
	for w := 0; w < 8; w++ {
		workersWg.Add(1)
		go func() {
			defer workersWg.Done()
			for atomic.LoadInt64(&consumed) < total {
				msgs, err := q.Pull("g", 1, 150*time.Millisecond)
				if err != nil {
					if errors.Is(err, ErrTimeout) {
						continue
					}
					return
				}
				m := msgs[0]
				inflightMu.Lock()
				if prev, exists := inflightKey[m.Key]; exists {
					t.Errorf("ordering violated: key=%q offsets %d and %d inflight concurrently", m.Key, prev, m.Offset)
				}
				inflightKey[m.Key] = m.Offset
				if last, ok := lastOffByKey[m.Key]; ok && m.Offset < last {
					t.Errorf("key=%q offset went backwards: %d < %d", m.Key, m.Offset, last)
				}
				lastOffByKey[m.Key] = m.Offset
				delivered[m.Offset]++
				inflightMu.Unlock()

				time.Sleep(time.Duration(rand.Intn(2)) * time.Millisecond) // 制造乱序完成

				inflightMu.Lock()
				delete(inflightKey, m.Key)
				inflightMu.Unlock()
				if err := q.Ack("g", m.Offset); err != nil {
					t.Errorf("ack %d: %v", m.Offset, err)
				}
				atomic.AddInt64(&consumed, 1)
			}
		}()
	}
	workersWg.Wait()

	if len(delivered) != total {
		t.Fatalf("lost messages: distinct delivered=%d want %d", len(delivered), total)
	}
	for off, n := range delivered {
		if n != 1 {
			t.Fatalf("offset %d delivered %d times in no-failure path", off, n)
		}
	}
	off, _ := q.CommittedOffset("g")
	t.Logf("all consumed exactly once: count=%d committed=%d", total, off)
	if off != total {
		t.Fatalf("committed want %d, got %d", total, off)
	}
	keyNames := make([]string, 0, len(lastOffByKey))
	for k := range lastOffByKey {
		keyNames = append(keyNames, k)
	}
	sort.Strings(keyNames)
	t.Logf("last offset per key: %v", keyNames)
}

// ---------- 幂等副作用：并发 + 重试路径只生效一次 ----------

func TestExecuteOnceConcurrentAndRetry(t *testing.T) {
	q := newTestQueue(t, nil)
	var effectCount int64
	key := "order-123/charge"

	// 并发 32 个调用，同一个 dedupKey，副作用只允许生效一次。
	var wg sync.WaitGroup
	verdicts := make([]DedupKind, 32)
	gate := make(chan struct{})
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-gate
			kind, err := q.ExecuteOnce(key, func() error {
				atomic.AddInt64(&effectCount, 1)
				time.Sleep(40 * time.Millisecond)
				return nil
			})
			if err != nil {
				t.Errorf("executeonce: %v", err)
				return
			}
			verdicts[i] = kind
		}(i)
	}
	close(gate)
	wg.Wait()
	if n := atomic.LoadInt64(&effectCount); n != 1 {
		t.Fatalf("side effect applied %d times, want 1", n)
	}
	var nNew, nComplete, nPending int
	for _, v := range verdicts {
		switch v {
		case DedupNew:
			nNew++
		case DedupComplete:
			nComplete++
		case DedupPending:
			nPending++
		}
	}
	t.Logf("idempotency verdicts: NEW=%d PENDING=%d COMPLETE=%d (effectCount=%d)",
		nNew, nPending, nComplete, atomic.LoadInt64(&effectCount))
	if nNew != 1 || nPending+nComplete != 31 {
		t.Fatalf("unexpected verdict mix: new=%d pending=%d complete=%d", nNew, nPending, nComplete)
	}

	// 模拟消息重复投递：再来 N 次调用，全部命中 COMPLETE，副作用不再发生。
	for i := 0; i < 10; i++ {
		kind, err := q.ExecuteOnce(key, func() error {
			atomic.AddInt64(&effectCount, 1)
			return nil
		})
		if err != nil || kind != DedupComplete {
			t.Fatalf("duplicate delivery round %d: kind=%v err=%v", i, kind, err)
		}
	}
	t.Logf("repeated duplicate deliveries all deduped: effectCount=%d", atomic.LoadInt64(&effectCount))

	// 失败路径：副作用返回错误 -> 去重登记被清除，允许重试。
	failKey := "order-456/charge"
	kind, err := q.ExecuteOnce(failKey, func() error {
		atomic.AddInt64(&effectCount, 1)
		return errors.New("downstream unavailable")
	})
	if err == nil || kind != DedupNew {
		t.Fatalf("want NEW+error, got %v %v", kind, err)
	}
	kind, err = q.ExecuteOnce(failKey, func() error {
		atomic.AddInt64(&effectCount, 1)
		return nil
	})
	if err != nil || kind != DedupNew {
		t.Fatalf("retry after failure should be NEW, got %v %v", kind, err)
	}
	kind, err = q.ExecuteOnce(failKey, func() error {
		atomic.AddInt64(&effectCount, 1)
		return nil
	})
	if err != nil || kind != DedupComplete {
		t.Fatalf("after success should be COMPLETE, got %v %v", kind, err)
	}
	t.Logf("failure/retry path: effectCount=%d final verdict=COMPLETE", atomic.LoadInt64(&effectCount))
}

// ---------- 进程内重启：未确认消息重投、已提交不重投 ----------

func TestRestartRedeliveryAndDedupPersistence(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "q")
	cfg := DefaultConfig(dir)
	cfg.Logger = testLogger{t}
	q, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.CreateConsumerGroup("g", 0); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 6; i++ {
		mustProduce(t, q, fmt.Sprintf("distinct-key-%d", i), fmt.Sprintf("m%d", i))
	}
	msgs, _ := q.Pull("g", 10, time.Second)
	if len(msgs) != 6 {
		t.Fatalf("want 6, got %d", len(msgs))
	}
	// 只确认前 3 条，后 3 条留在 inflight，随后“崩溃”（Close 也会持久化；
	// 真正的强杀场景在 kill_test.go 中验证）。
	for i := 0; i < 3; i++ {
		if err := q.Ack("g", int64(i)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := q.ExecuteOnce("persisted-key", func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}

	// 重新打开。
	cfg2 := DefaultConfig(dir)
	cfg2.Logger = testLogger{t}
	q2, err := Open(cfg2)
	if err != nil {
		t.Fatal(err)
	}
	if off, _ := q2.CommittedOffset("g"); off != 3 {
		t.Fatalf("after restart committed want 3, got %d", off)
	}
	again, err := q2.Pull("g", 10, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 3 {
		t.Fatalf("want 3 redelivered, got %d", len(again))
	}
	for _, m := range again {
		if m.Offset < 3 {
			t.Fatalf("committed-prior message redelivered: %d", m.Offset)
		}
		if m.Attempt != 2 {
			t.Fatalf("redelivery attempt want 2, got %d (offset=%d)", m.Attempt, m.Offset)
		}
	}
	t.Logf("after restart: committed=3, redelivered offsets=%d,%d,%d attempts=2",
		again[0].Offset, again[1].Offset, again[2].Offset)
	if err := q2.Commit("g", 2); !errors.Is(err, ErrOffsetRollback) {
		t.Fatalf("rollback after restart want ErrOffsetRollback, got %v", err)
	}
	// 幂等记录同样持久化：重启后命中 COMPLETE。
	var effect int64
	kind, err := q2.ExecuteOnce("persisted-key", func() error {
		atomic.AddInt64(&effect, 1)
		return nil
	})
	if err != nil || kind != DedupComplete {
		t.Fatalf("dedup after restart want COMPLETE, got %v %v", kind, err)
	}
	t.Logf("dedup survives restart: verdict=%s effectCount=%d", kind, atomic.LoadInt64(&effect))
	if err := q2.Close(); err != nil {
		t.Fatal(err)
	}
}

// dedupStore 单元测试：崩溃留下的 PENDING 在重启后可被接管。
func TestDedupPendingReclaimAfterCrash(t *testing.T) {
	dir := t.TempDir()
	d, err := openDedupStore(dir, 10)
	if err != nil {
		t.Fatal(err)
	}
	kind, err := d.begin("x")
	if err != nil || kind != DedupNew {
		t.Fatalf("want NEW, got %v %v", kind, err)
	}
	// 没有 complete 直接“崩溃”。
	if err := d.close(); err != nil {
		t.Fatal(err)
	}
	d2, err := openDedupStore(dir, 10)
	if err != nil {
		t.Fatal(err)
	}
	kind, err = d2.begin("x")
	if err != nil || kind != DedupPending {
		t.Fatalf("stale BEGIN should be PENDING, got %v %v", kind, err)
	}
	if err := d2.complete("x"); err != nil {
		t.Fatal(err)
	}
	kind, _ = d2.begin("x")
	if kind != DedupComplete {
		t.Fatalf("want COMPLETE, got %v", kind)
	}
	t.Logf("stale pending reclaimed then committed: verdict=COMPLETE")
}

// ---------- 可见性超时：消费者拿到消息后崩溃（未 ack），自动重投 ----------

func TestVisibilityTimeoutRedelivery(t *testing.T) {
	q := newTestQueue(t, func(c *Config) {
		c.VisibilityTimeout = 120 * time.Millisecond
		c.MaxDeliveryAttempts = 0 // 不进死信，便于观察重投
	})
	if err := q.CreateConsumerGroup("g", 0); err != nil {
		t.Fatal(err)
	}
	mustProduce(t, q, "k", "work")

	first, err := q.Pull("g", 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if first[0].Attempt != 1 {
		t.Fatalf("want attempt 1, got %d", first[0].Attempt)
	}
	t.Logf("first delivery offset=%d attempt=1; consumer crashes without ack", first[0].Offset)

	// 退避窗口内不会重投。
	if _, err := q.Pull("g", 1, 60*time.Millisecond); !errors.Is(err, ErrTimeout) {
		t.Fatalf("want timeout before visibility deadline, got %v", err)
	}

	// 超时后必须自动重新投递（attempt=2），且位点没有前进。
	var second []*DeliveredMessage
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		msgs, err := q.Pull("g", 1, 100*time.Millisecond)
		if errors.Is(err, ErrTimeout) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		second = msgs
		break
	}
	if second == nil {
		t.Fatal("message never redelivered after visibility timeout")
	}
	if second[0].Offset != first[0].Offset || second[0].Attempt != 2 {
		t.Fatalf("redelivery mismatch: %+v", second[0])
	}
	off, _ := q.CommittedOffset("g")
	t.Logf("visibility redelivery ok: offset=%d retry-count=%d committed=%d",
		second[0].Offset, second[0].Attempt, off)
	if off != 0 {
		t.Fatalf("committed must stay 0, got %d", off)
	}
	if err := q.Ack("g", second[0].Offset); err != nil {
		t.Fatal(err)
	}
	off, _ = q.CommittedOffset("g")
	if off != 1 {
		t.Fatalf("committed want 1, got %d", off)
	}
}

// ---------- 多消费组：各自独立位点，同一条消息各组都能消费 ----------

func TestMultipleConsumerGroups(t *testing.T) {
	q := newTestQueue(t, nil)
	if err := q.CreateConsumerGroup("g1", 0); err != nil {
		t.Fatal(err)
	}
	mustProduce(t, q, "k1", "shared")
	// g2 从 offset 1 开始（只消费未来消息）。
	if err := q.CreateConsumerGroup("g2", 1); err != nil {
		t.Fatal(err)
	}
	mustProduce(t, q, "k2", "future")

	m1, err := q.Pull("g1", 10, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	m2, err := q.Pull("g2", 10, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(m1) != 2 || m1[0].Offset != 0 || m1[1].Offset != 1 {
		t.Fatalf("g1 unexpected: %+v", m1)
	}
	if len(m2) != 1 || m2[0].Offset != 1 {
		t.Fatalf("g2 unexpected: %+v", m2)
	}
	if err := q.Ack("g1", 0); err != nil {
		t.Fatal(err)
	}
	if err := q.Ack("g1", 1); err != nil {
		t.Fatal(err)
	}
	if err := q.Ack("g2", 1); err != nil {
		t.Fatal(err)
	}
	off1, _ := q.CommittedOffset("g1")
	off2, _ := q.CommittedOffset("g2")
	t.Logf("group offsets independent: g1.committed=%d g2.committed=%d", off1, off2)
	if off1 != 2 || off2 != 2 {
		t.Fatalf("unexpected group offsets: %d %d", off1, off2)
	}
	if err := q.CreateConsumerGroup("g1", 0); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("dup group want ErrAlreadyExists, got %v", err)
	}
}
