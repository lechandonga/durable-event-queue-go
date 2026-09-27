package eventqueue

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func testOpts(dir string) Options {
	o := DefaultOptions(dir)
	o.Partitions = 3
	o.MaxAttempts = 3
	o.AckTimeout = 250 * time.Millisecond
	o.BaseBackoff = 20 * time.Millisecond
	o.MaxBackoff = 100 * time.Millisecond
	o.LeaseScanInterval = 15 * time.Millisecond
	return o
}

func TestSmokePublishPollCommit(t *testing.T) {
	dir := t.TempDir()
	q, err := Open(dir, testOpts(dir))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer q.Close()

	ctx := context.Background()
	const n = 20
	for i := 0; i < n; i++ {
		off, err := q.Publish(ctx, Message{Key: fmt.Sprintf("k%02d", i%5), Body: []byte(fmt.Sprintf("v%d", i))})
		if err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
		if off != int64(i+1) {
			t.Fatalf("offset = %d want %d", off, i+1)
		}
	}

	sub, err := q.Subscriber("g1")
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	got := make(map[uint32][]int64)
	for i := 0; i < n; i++ {
		e, err := sub.Poll(ctx)
		if err != nil {
			t.Fatalf("poll: %v", err)
		}
		got[e.Partition] = append(got[e.Partition], e.Offset)
		if err := sub.CommitOffset(ctx, e.Offset); err != nil {
			t.Fatalf("commit: %v", err)
		}
	}
	// 每个分区内消息位点必须严格递增（即按写入顺序消费）。
	for p, offs := range got {
		for i := 1; i < len(offs); i++ {
			if offs[i] <= offs[i-1] {
				t.Fatalf("partition %d offsets not increasing: %v", p, offs)
			}
		}
	}
	t.Logf("per-partition consumed offsets: %v", got)
}
