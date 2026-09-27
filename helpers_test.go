package eventqueue

import "time"

func pollTimeoutAfter() <-chan time.Time {
	return time.After(10 * time.Second)
}
