package eventqueue

import "errors"

// 写入被拒绝的原因，便于调用方区分处理。
type RejectReason int

const (
	RejectNone           RejectReason = iota
	RejectQueueFull                   // 容量已达上限，策略为 Reject
	RejectQueueClosed                 // 队列已关闭
	RejectInvalidMessage              // 消息非法（空 key/body 等）
)

func (r RejectReason) String() string {
	switch r {
	case RejectQueueFull:
		return "queue-full"
	case RejectQueueClosed:
		return "queue-closed"
	case RejectInvalidMessage:
		return "invalid-message"
	default:
		return "none"
	}
}

// PublishError 携带写入被拒绝的具体原因。
type PublishError struct {
	Reason RejectReason
}

func (e *PublishError) Error() string { return "publish rejected: " + e.Reason.String() }

// 哨兵错误。
var (
	ErrClosed           = errors.New("eventqueue: closed")
	ErrOffsetRollback   = errors.New("eventqueue: commit offset must not roll back before last committed offset")
	ErrConsumerNotFound = errors.New("eventqueue: consumer group not found")
	ErrInvalidArgument  = errors.New("eventqueue: invalid argument")
	ErrMaxAttempts      = errors.New("eventqueue: max delivery attempts exhausted")
	ErrLeaseHeld        = errors.New("eventqueue: deduplication key lease is held by an in-flight attempt")
)
