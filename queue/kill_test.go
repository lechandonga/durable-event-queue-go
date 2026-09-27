package queue_test

import (
	"bufio"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/lechandonga/durable-event-queue-go/queue"
)

// killTestLogger 把引擎日志打到测试输出（位点、重试次数、幂等判定都会出现）。
type killTestLogger struct{ t *testing.T }

func (l killTestLogger) Printf(format string, args ...any) {
	l.t.Logf("[engine] "+format, args...)
}

// TestMain 拦截“强杀助手”子进程模式：重新执行测试二进制自身，不依赖外部程序。
func TestMain(m *testing.M) {
	if os.Getenv(queue.KillChildEnv) == "1" {
		if err := queue.RunKillChild(); err != nil {
			os.Stderr.WriteString(err.Error() + "\n")
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func TestHardKillRestart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "q")

	// ---- 阶段一：子进程写入/消费，然后被 SIGKILL ----
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(),
		queue.KillChildEnv+"=1",
		queue.KillChildEnv+"_DIR="+dir,
	)
	cmd.Stderr = &lineCapture{t: t}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	sc := bufio.NewScanner(stdout)
	ready := make(chan struct{})
	go func() {
		for sc.Scan() {
			line := sc.Text()
			t.Logf("[child-stdout] %s", line)
			if line == "PHASE1-DONE" {
				close(ready)
			}
		}
	}()
	select {
	case <-ready:
	case <-time.After(15 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatal("child never reached PHASE1-DONE")
	}
	// 给子进程一点时间让它停留在“未确认消息在途”状态，然后强杀：不发 Close、不走 defer。
	time.Sleep(150 * time.Millisecond)
	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatalf("sigkill: %v", err)
	}
	waitErr := cmd.Wait()
	if waitErr == nil {
		t.Fatal("child exited cleanly; expected SIGKILL")
	}
	if runtime.GOOS != "windows" {
		var ee *exec.ExitError
		if errors.As(waitErr, &ee) && ee.ExitCode() != -1 {
			t.Fatalf("expected killed by signal, exit=%d", ee.ExitCode())
		}
	}
	t.Logf("child killed successfully (no Close ran): %v", waitErr)

	// ---- 阶段二：父进程重新打开同一目录 ----
	cfg := queue.DefaultConfig(dir)
	cfg.Logger = killTestLogger{t}
	q, err := queue.Open(cfg)
	if err != nil {
		t.Fatalf("reopen after kill: %v", err)
	}
	defer q.Close()

	committed, err := q.CommittedOffset("g")
	if err != nil {
		t.Fatal(err)
	}
	if committed != 5 {
		t.Fatalf("after kill: committed want 5, got %d", committed)
	}
	t.Logf("after kill restart: committed offset=%d (offsets 0-4 must never redeliver)", committed)

	// 位点回退到已提交之前必须被拒绝。
	if err := q.Commit("g", 4); !errors.Is(err, queue.ErrOffsetRollback) {
		t.Fatalf("rollback want ErrOffsetRollback, got %v", err)
	}
	if err := q.Commit("g", 5); !errors.Is(err, queue.ErrOffsetRollback) {
		t.Fatalf("commit at already committed want ErrOffsetRollback, got %v", err)
	}
	t.Log("rollback to committed-prior rejected after restart")

	// 未确认的 5..9 必须重新投递，且 attempt=2；10..29 首次投递。
	msgs, err := q.Pull("g", 100, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 25 {
		t.Fatalf("after restart want 25 deliverable (5 redelivery + 20 fresh), got %d", len(msgs))
	}
	seen := map[int64]int{}
	for _, m := range msgs {
		seen[m.Offset] = m.Attempt
		if m.Offset < 5 {
			t.Fatalf("committed-prior offset %d redelivered after restart", m.Offset)
		}
	}
	for off := int64(5); off <= 9; off++ {
		if seen[off] != 2 {
			t.Fatalf("redelivered offset %d attempt want 2, got %d", off, seen[off])
		}
	}
	for off := int64(10); off <= 29; off++ {
		if seen[off] != 1 {
			t.Fatalf("fresh offset %d attempt want 1, got %d", off, seen[off])
		}
	}
	t.Logf("redelivery verified: offsets 5-9 attempt=2, offsets 10-29 attempt=1; " +
		"committed-prior (0-4) never seen")

	// 幂等：killed-key 的副作用在被杀前已 COMMIT，重启后必须跳过。
	var effects int64
	kind, err := q.ExecuteOnce("killed-key", func() error {
		atomic.AddInt64(&effects, 1)
		return nil
	})
	if err != nil || kind != queue.DedupComplete {
		t.Fatalf("killed-key want COMPLETE after restart, got %v err=%v", kind, err)
	}
	t.Logf("idempotent verdict for committed-before-kill key: %s, effects=%d", kind, atomic.LoadInt64(&effects))

	// pending-key 崩溃在 BEGIN 与 COMMIT 之间：重启后必须被接管执行，且只执行一次。
	kind, err = q.ExecuteOnce("pending-key", func() error {
		atomic.AddInt64(&effects, 1)
		return nil
	})
	if err != nil {
		t.Fatalf("pending-key reclaim: %v", err)
	}
	t.Logf("idempotent verdict for crash-pending key: %s (reclaimed & executed once)", kind)
	kind, err = q.ExecuteOnce("pending-key", func() error {
		atomic.AddInt64(&effects, 1)
		return nil
	})
	if err != nil || kind != queue.DedupComplete {
		t.Fatalf("pending-key second call want COMPLETE, got %v err=%v", kind, err)
	}
	if n := atomic.LoadInt64(&effects); n != 1 {
		t.Fatalf("total side effects want exactly 1 (pending reclaim), got %d", n)
	}
	t.Logf("crash-pending idempotency settled: verdict=%s totalEffects=%d", kind, atomic.LoadInt64(&effects))

	// 全部确认，位点推进到 30。
	for _, m := range msgs {
		if err := q.Ack("g", m.Offset); err != nil {
			t.Fatal(err)
		}
	}
	finalOff, _ := q.CommittedOffset("g")
	if finalOff != 30 {
		t.Fatalf("final committed want 30, got %d", finalOff)
	}
	t.Logf("all messages acked after restart: final committed offset=%d", finalOff)
}

type lineCapture struct{ t *testing.T }

func (l *lineCapture) Write(p []byte) (int, error) {
	l.t.Logf("[child] %s", trimNewline(p))
	return len(p), nil
}

func trimNewline(p []byte) []byte {
	for len(p) > 0 && (p[len(p)-1] == '\n' || p[len(p)-1] == '\r') {
		p = p[:len(p)-1]
	}
	return p
}
