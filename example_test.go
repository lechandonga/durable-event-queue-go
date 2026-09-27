package eventqueue_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	eventqueue "github.com/lechandonga/durable-event-queue-go"
)

// 演示最小的“生产 - 幂等消费 - 提交位点”闭环。
func Example() {
	dir, err := os.MkdirTemp("", "eq-example-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	opts := eventqueue.DefaultOptions(dir)
	opts.Partitions = 4
	opts.MaxBytes = 1 << 20
	opts.MaxAttempts = 3
	opts.BaseBackoff = 10 * time.Millisecond

	q, err := eventqueue.Open(dir, opts)
	if err != nil {
		log.Fatal(err)
	}
	defer q.Close()
	ctx := context.Background()

	// 生产者。
	off, err := q.Publish(ctx, eventqueue.Message{
		Key:      "order-42",
		DedupKey: "charge-order-42", // 下游副作用（扣款）的业务幂等键
		Body:     []byte(`{"amount":100}`),
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("published offset", off)

	// 消费者（消费组）。
	sub, err := q.Subscriber("billing")
	if err != nil {
		log.Fatal(err)
	}
	env, err := sub.Poll(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// Commit 原子完成“副作用去重记录 + 位点前进”：
	// 同一 DedupKey 即使消息重复投递，扣款也只生效一次。
	result, err := sub.Commit(ctx, env.Offset, env.Message.DedupKey)
	if err != nil {
		log.Fatal(err)
	}
	switch result {
	case eventqueue.GuardFirstTime:
		fmt.Println("first time: apply charge once")
	case eventqueue.GuardDuplicate:
		fmt.Println("duplicate: skip charge")
	case eventqueue.GuardLeased:
		fmt.Println("leased by another in-flight attempt, retry later")
	}
	fmt.Println("committed partition offset", sub.CommittedOffset(env.Partition))

	// 失败路径：Nack 触发退避重试，超过 MaxAttempts 自动进入死信。
	// _ = sub.Nack(ctx, env.Offset, someErr)
	// _ = q.DeadLetters()

	// Output:
	// published offset 1
	// first time: apply charge once
	// committed partition offset 1
}
