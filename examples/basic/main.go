// 最小示例：在单机进程内嵌入持久化事件队列，
// 生产 -> 拉取 -> 幂等副作用 -> ack，崩溃重启后语义保持。
package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	dq "github.com/lechandonga/durable-event-queue-go/queue"
)

func main() {
	dir := filepath.Join(os.TempDir(), "deq-example")
	_ = os.MkdirAll(dir, 0o755)

	cfg := dq.DefaultConfig(dir)
	cfg.MaxPendingMessages = 10_000  // 容量上限（条）
	cfg.MaxDataBytes = 256 << 20     // 容量上限（字节）
	cfg.FullPolicy = dq.PolicyReject // 满了就拒绝（ErrQueueFull），绝不静默丢弃
	cfg.MaxDeliveryAttempts = 5      // 超过 5 次投递进死信
	cfg.Backoff = dq.ExponentialBackoff(100*time.Millisecond, 10*time.Second)
	cfg.VisibilityTimeout = 30 * time.Second // 30s 未 ack 自动重投
	cfg.Logger = log.New(os.Stderr, "[orders] ", log.LstdFlags)

	q, err := dq.Open(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer q.Close()

	if err := q.CreateConsumerGroup("billing", 0); err != nil && !errors.Is(err, dq.ErrAlreadyExists) {
		log.Fatal(err)
	}

	off, err := q.Produce(dq.Message{Key: "order-42", Payload: []byte(`{"amount":100}`)})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("produced offset:", off)

	msgs, err := q.Pull("billing", 16, 5*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	for _, m := range msgs {
		// dedupKey 必须是业务侧天然幂等的标识（如 订单号+动作）。
		verdict, err := q.ExecuteOnce(fmt.Sprintf("order-%d/charge", m.Offset), func() error {
			fmt.Printf("charging for offset=%d attempt=%d payload=%s\n", m.Offset, m.Attempt, m.Payload)
			return nil // 真实副作用：扣款、发货、调用外部接口……
		})
		if err != nil {
			_ = q.Nack("billing", m.Offset, err) // 退避重试
			continue
		}
		fmt.Println("idempotency verdict:", verdict) // NEW / PENDING / COMPLETE
		if err := q.Ack("billing", m.Offset); err != nil {
			log.Fatal(err)
		}
	}

	dls, _ := q.DeadLetters("billing")
	for _, dl := range dls {
		fmt.Printf("dead letter: offset=%d attempts=%d cause=%s\n", dl.Offset, dl.Attempts, dl.LastError)
	}
}
