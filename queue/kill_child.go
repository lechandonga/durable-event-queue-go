package queue

import (
	"errors"
	"fmt"
	"os"
	"time"
)

// KillChildEnv 非空时表示进程以“强杀演练助手”模式运行。
const KillChildEnv = "DURABLE_QUEUE_KILL_CHILD"

// RunKillChild 是强杀测试的子进程入口。
// 它故意不调用 Close：阶段一完成后测试会对其发送 SIGKILL。
//
// 阶段一（phase=write）：
//   - 写入 30 条消息；
//   - 拉取 offset 0..9；ack 连续的 0..4（位点推进到 5），5..9 留在 inflight；
//   - 对 "killed-key" 提交幂等副作用（COMMIT 落盘）；
//   - 对 "pending-key" 登记后开始一个耗时 2s 的副作用（不 COMMIT）；
//   - 打印 "PHASE1-DONE" 后由父进程 SIGKILL。
func RunKillChild() error {
	dir := os.Getenv(KillChildEnv + "_DIR")
	cfg := DefaultConfig(dir)
	cfg.Logger = logWriter{}
	cfg.VisibilityTimeout = 5 * time.Second
	cfg.Backoff = ConstantBackoff(20 * time.Millisecond)
	cfg.MaxDeliveryAttempts = 0
	q, err := Open(cfg)
	if err != nil {
		return err
	}
	if err := q.CreateConsumerGroup("g", 0); err != nil {
		return err
	}
	// 登记一个“做到一半崩溃”的幂等键：BEGIN 已 fsync，COMMIT 还没落盘。
	go func() {
		_, _ = q.ExecuteOnce("pending-key", func() error {
			time.Sleep(2 * time.Second)
			return nil
		})
	}()

	for i := 0; i < 30; i++ {
		if _, err := q.Produce(Message{
			Key:     fmt.Sprintf("key-%d", i),
			Payload: []byte(fmt.Sprintf("payload-%02d", i)),
		}); err != nil {
			return err
		}
	}
	// 等待后台 goroutine 的 BEGIN 落盘。
	time.Sleep(100 * time.Millisecond)

	msgs, err := q.Pull("g", 10, 5*time.Second)
	if err != nil {
		return err
	}
	if len(msgs) != 10 {
		return fmt.Errorf("child: want 10 delivered, got %d", len(msgs))
	}
	// 确认连续的前 5 条（0..4），5..9 保持 inflight 直到被强杀。
	for _, m := range msgs {
		if m.Offset < 5 {
			if err := q.Ack("g", m.Offset); err != nil {
				return err
			}
			continue
		}
		// offset 5..9 故意不 ack 也不 nack，保持 inflight 直到被杀。
	}
	// 已提交的幂等键：COMMIT 必须在被杀前 fsync。
	if _, err := q.ExecuteOnce("killed-key", func() error { return nil }); err != nil {
		return err
	}
	fmt.Println("PHASE1-DONE")
	os.Stdout.Sync()
	// 再多活一会儿，确保父进程是“强杀”而不是读到正常退出。
	time.Sleep(2 * time.Second)
	return errors.New("child should have been killed")
}

type logWriter struct{}

func (logWriter) Printf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "[child-engine] "+format+"\n", args...)
}
