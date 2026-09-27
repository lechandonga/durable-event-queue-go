package eventqueue

import (
	"context"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

// 该测试通过子进程真实模拟进程被 SIGKILL：
//   - 父进程以 env CRASH_CHILD=1 启动自己；
//   - 子进程发布 N 条消息，提交前一半，然后被父进程 Kill()（等价强杀）；
//   - 父进程重新打开队列，验证已提交的不再投递、未提交的重新投递、
//     回退位点提交被明确拒绝，并打印位点日志。
func TestKillRestart(t *testing.T) {
	if os.Getenv("CRASH_CHILD") == "1" {
		crashChild(t)
		return
	}

	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=TestKillRestart")
	cmd.Env = append(os.Environ(),
		"CRASH_CHILD=1",
		"CRASH_DIR="+dir,
	)
	out, err := cmd.CombinedOutput()
	t.Logf("child output:\n%s", string(out))
	if err == nil {
		t.Fatalf("child should have been killed, but exited cleanly")
	}

	q, err := Open(dir, testOpts(dir))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer q.Close()
	ctx := context.Background()
	sub, err := q.Subscriber("grp-crash")
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	wm := []int64{sub.CommittedOffset(0), sub.CommittedOffset(1), sub.CommittedOffset(2)}
	t.Logf("after restart committed offsets per partition: %v", wm)

	// 位点回退必须被明确拒绝：
	//  (1) 回退到 0（非法位点但明确小于已提交位点）；
	//  (2) 回退到“合法但已提交”的更早位点。
	if err := sub.CommitOffset(ctx, 0); !errorIs(err, ErrOffsetRollback) {
		t.Fatalf("rollback to 0 should be rejected, got err=%v", err)
	}
	var validEarlier int64 = -1
	func() {
		q.st.mu.Lock()
		defer q.st.mu.Unlock()
		for _, ie := range q.st.index {
			if g, ok := q.st.groups["grp-crash"]; ok && ie.offset < g.watermark[ie.partition] && g.watermark[ie.partition] > 0 {
				validEarlier = ie.offset
				return
			}
		}
	}()
	if validEarlier > 0 {
		if err := sub.CommitOffset(ctx, validEarlier); !errorIs(err, ErrOffsetRollback) {
			t.Fatalf("rollback to %d should be rejected, got err=%v", validEarlier, err)
		}
		t.Logf("rollback to earlier offset %d correctly rejected", validEarlier)
	}

	// 重启后：只能消费到此前未确认的消息（按分区位点判断，不重复已提交消息）。
	// 重新消费剩余消息。
	reDelivered := 0
	pollCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for {
		e, err := sub.Poll(pollCtx)
		if err != nil {
			break
		}
		if e.Offset <= wm[e.Partition] {
			t.Fatalf("committed message re-delivered after restart: offset=%d partition=%d (committed=%d)",
				e.Offset, e.Partition, wm[e.Partition])
		}
		// 注意：未确认消息的投递状态不跨进程保留，强杀重启后以 attempt=1
		// 重新投递，这是至少一次语义的预期行为（attempt 计数仅在进程内有效）。
		t.Logf("redelivery after kill: offset=%d partition=%d attempt=%d key=%s",
			e.Offset, e.Partition, e.Attempt, e.Message.Key)
		if err := sub.CommitOffset(ctx, e.Offset); err != nil {
			t.Fatalf("commit: %v", err)
		}
		reDelivered++
	}
	if reDelivered == 0 {
		t.Fatal("expected unacked messages to be redelivered after kill, got none")
	}
	t.Logf("re-delivered and committed %d previously-unacked messages", reDelivered)
}

const crashTotal = 30

func crashChild(t *testing.T) {
	dir := os.Getenv("CRASH_DIR")
	q, err := Open(dir, testOpts(dir))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	sub, _ := q.Subscriber("grp-crash")

	// 发布 crashTotal 条消息。
	for i := 0; i < crashTotal; i++ {
		if _, err := q.Publish(ctx, Message{Key: "k" + strconv.Itoa(i%4), Body: []byte("x")}); err != nil {
			t.Fatal(err)
		}
	}
	// 只确认前一半消息，后一半保持未确认（连 Nack 都不做）。
	committed := 0
	pollCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	for committed < crashTotal/2 {
		e, err := sub.Poll(pollCtx)
		if err != nil {
			t.Fatal(err)
		}
		if err := sub.CommitOffset(ctx, e.Offset); err != nil {
			t.Fatal(err)
		}
		committed++
	}
	// 再拉取后一半但绝不提交，制造未确认消息。
	for i := 0; i < crashTotal/2; i++ {
		if _, err := sub.Poll(pollCtx); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("child: committed %d, holding %d unacked; killing self", committed, crashTotal/2)

	// 强制自杀：不关闭、不 defer flush（WAL 每条 fsync，已落盘数据完好）。
	// 直接 os.Exit 也模拟“强杀”效果（无 Close 路径）。
	_ = q
	os.Exit(9)
}

func errorIs(err, target error) bool {
	if err == nil || target == nil {
		return err == target
	}
	return strings.Contains(err.Error(), target.Error()) || err == target
}

// TestKillRestartIdempotency 模拟崩溃发生在“幂等记录已落盘、位点记录尚未落盘”
// 的窗口：重启后消息会被重新投递，但 dedup 表已恢复，Commit 必须判定
// GuardDuplicate，业务副作用不重复生效，且位点随后正常前进。
func TestKillRestartIdempotency(t *testing.T) {
	if os.Getenv("CRASH_CHILD") == "2" {
		crashDedupChild(t)
		return
	}
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=TestKillRestartIdempotency")
	cmd.Env = append(os.Environ(), "CRASH_CHILD=2", "CRASH_DIR="+dir)
	out, err := cmd.CombinedOutput()
	t.Logf("child output:\n%s", string(out))
	if err == nil {
		t.Fatal("child should be killed")
	}

	q, err := Open(dir, testOpts(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	ctx := context.Background()
	sub, _ := q.Subscriber("grp-idem-crash")

	pollCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	env, err := sub.Poll(pollCtx)
	if err != nil {
		t.Fatalf("message should be redelivered after crash window: %v", err)
	}
	t.Logf("redelivered after crash: offset=%d partition=%d attempt=%d dedup=%s",
		env.Offset, env.Partition, env.Attempt, env.Message.DedupKey)

	res, err := sub.Commit(ctx, env.Offset, env.Message.DedupKey)
	if err != nil {
		t.Fatal(err)
	}
	if res != GuardDuplicate {
		t.Fatalf("idempotency after crash window = %s, want duplicate", res)
	}
	t.Logf("idempotency verdict after restart: %s -> business side effect NOT re-applied", res)
	if sub.CommittedOffset(env.Partition) < env.Offset {
		t.Fatalf("offset should advance after duplicate commit, wm=%d", sub.CommittedOffset(env.Partition))
	}
	t.Logf("offset advanced after duplicate commit: partition=%d committed=%d",
		env.Partition, sub.CommittedOffset(env.Partition))
}

func crashDedupChild(t *testing.T) {
	dir := os.Getenv("CRASH_DIR")
	q, err := Open(dir, testOpts(dir))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	off, err := q.Publish(ctx, Message{Key: "idem-key", DedupKey: "idem-eff", Body: []byte("z")})
	if err != nil {
		t.Fatal(err)
	}
	part := partitionOf("idem-key", q.opts.Partitions)
	// 仅写 recDedup（模拟副作用去重已持久化），刻意不写 recCommit，
	// 然后不经过 Close 直接退出 —— 精确制造崩溃窗口。
	if err := q.st.meta.append(recDedup, encodeDedup(&dedupRecord{
		group: "grp-idem-crash", dedup: "idem-eff", offset: off,
	})); err != nil {
		t.Fatal(err)
	}
	t.Logf("child: wrote dedup for offset=%d partition=%d, committing NOT done; killing", off, part)
	os.Exit(9)
}
