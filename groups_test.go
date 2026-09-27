package eventqueue

import (
	"context"
	"testing"
	"time"
)

func TestMultipleGroupsIndependent(t *testing.T) {
	dir := t.TempDir()
	q, err := Open(dir, testOpts(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		if _, err := q.Publish(ctx, Message{Key: "gk", Body: []byte{byte(i)}}); err != nil {
			t.Fatal(err)
		}
	}

	consumeAll := func(group string) int {
		sub, err := q.Subscriber(group)
		if err != nil {
			t.Fatal(err)
		}
		n := 0
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			e, err := sub.TryPoll(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if e == nil {
				if n == 10 {
					break
				}
				time.Sleep(5 * time.Millisecond)
				continue
			}
			if err := sub.CommitOffset(ctx, e.Offset); err != nil {
				t.Fatal(err)
			}
			n++
		}
		return n
	}
	a := consumeAll("group-A")
	b := consumeAll("group-B")
	if a != 10 || b != 10 {
		t.Fatalf("each group should consume all 10 messages, got A=%d B=%d", a, b)
	}
	t.Logf("groups independent: group-A consumed=%d group-B consumed=%d, retained=%d (all acked)",
		a, b, q.RetainedBytes())
	if q.RetainedBytes() != 0 {
		t.Fatalf("retained bytes should be 0 after all groups acked, got %d", q.RetainedBytes())
	}
}
