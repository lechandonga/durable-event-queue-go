package queue

import (
	"errors"
	"time"
)

// CapacityPolicy 决定队列达到容量上限时生产者的行为。
type CapacityPolicy int

const (
	// PolicyReject 队列满时立即拒绝写入（返回 ErrQueueFull）。
	PolicyReject CapacityPolicy = iota
	// PolicyBlock 队列满时阻塞生产者，直到腾出空间或队列关闭/超时。
	PolicyBlock
)

// Sentinel errors。被拒绝的写入必须能区分原因，因此每个拒绝路径都有独立错误。
var (
	// ErrQueueFull 容量已满（PolicyReject 下）。
	ErrQueueFull = errors.New("durablequeue: queue capacity reached")
	// ErrQueueClosed 队列已关闭，拒绝新写入。
	ErrQueueClosed = errors.New("durablequeue: queue is closed")
	// ErrInvalidMessage 消息非法（空 payload 等）。
	ErrInvalidMessage = errors.New("durablequeue: invalid message")
	// ErrOffsetRollback 位点回退：请求提交的位点小于等于已提交位点。
	ErrOffsetRollback = errors.New("durablequeue: offset rollback rejected")
	// ErrOffsetOutOfRange 位点超过已投递范围，无法提交。
	ErrOffsetOutOfRange = errors.New("durablequeue: offset out of range")
	// ErrNotFound 消费组或消息不存在。
	ErrNotFound = errors.New("durablequeue: not found")
	// ErrAlreadyExists 消费组已存在。
	ErrAlreadyExists = errors.New("durablequeue: already exists")
	// ErrTimeout 阻塞等待容量/消息超时。
	ErrTimeout = errors.New("durablequeue: operation timed out")
)

// BackoffFunc 根据第 attempt 次投递（attempt 从 1 开始）计算下次重试的等待时间。
type BackoffFunc func(attempt int) time.Duration

// ConstantBackoff 返回固定间隔的退避策略。
func ConstantBackoff(d time.Duration) BackoffFunc {
	return func(int) time.Duration { return d }
}

// LinearBackoff 返回 base*attempt 的线性退避策略。
func LinearBackoff(base time.Duration) BackoffFunc {
	return func(attempt int) time.Duration { return base * time.Duration(attempt) }
}

// ExponentialBackoff 返回 base*2^(attempt-1)，并以 max 封顶。
func ExponentialBackoff(base, max time.Duration) BackoffFunc {
	return func(attempt int) time.Duration {
		d := base
		for i := 1; i < attempt; i++ {
			d *= 2
			if d >= max {
				return max
			}
		}
		if d > max {
			return max
		}
		return d
	}
}

// Config 是队列引擎的全部可配置项。零值在 Open 时会由 DefaultConfig 填充。
type Config struct {
	// DataDir 持久化目录（消息日志、位点、死信、去重表都在其下）。
	DataDir string

	// MaxPendingMessages 未完成消息条数上限（尚未被所有消费组提交的消息计数）。
	// 0 表示不限制条数。
	MaxPendingMessages int64
	// MaxDataBytes 数据文件总字节数上限。0 表示不限制大小。
	MaxDataBytes int64
	// FullPolicy 达到容量上限时的策略：拒绝或阻塞。
	FullPolicy CapacityPolicy
	// BlockProduceTimeout PolicyBlock 下生产者最长等待时间；0 表示永久等待。
	BlockProduceTimeout time.Duration

	// MaxDeliveryAttempts 单条消息最大投递次数；超过后进入死信。<=0 表示无限重试。
	MaxDeliveryAttempts int
	// Backoff 消费失败后的重试退避计算函数。
	Backoff BackoffFunc
	// VisibilityTimeout 消息被拉取后多久未 ack 即视为崩溃、可重新投递。
	// <=0 表示关闭自动重投（仍可在重启后重新投递未确认消息）。
	VisibilityTimeout time.Duration

	// DedupHistory 幂等判定保留多少个已完成 key 的历史记录（<=0 用默认值）。
	DedupHistory int

	// Logger 日志接口；测试中可注入以打印位点/重试/幂等判定结果。
	Logger Logger
}

// DefaultConfig 返回带默认值的配置。
func DefaultConfig(dataDir string) Config {
	return Config{
		DataDir:             dataDir,
		MaxPendingMessages:  100_000,
		MaxDataBytes:        1 << 30, // 1GiB
		FullPolicy:          PolicyReject,
		BlockProduceTimeout: 0,
		MaxDeliveryAttempts: 5,
		Backoff:             ExponentialBackoff(50*time.Millisecond, 10*time.Second),
		VisibilityTimeout:   30 * time.Second,
		DedupHistory:        100_000,
	}
}

// Logger 是队列使用的最小日志接口。
type Logger interface {
	Printf(format string, args ...any)
}
