package eventqueue

import (
	"errors"
	"time"
)

// CapacityPolicy 队列达到容量上限时的策略。
//
// 无论哪种策略都不会静默丢弃消息：
//   - PolicyReject：立即返回携带 RejectQueueFull 的 PublishError；
//   - PolicyBlock：阻塞等待空间，直到有空位、上下文取消、超时或队列关闭。
type CapacityPolicy int

const (
	// PolicyReject 立即拒绝写入（返回 RejectQueueFull）。
	PolicyReject CapacityPolicy = iota
	// PolicyBlock 阻塞生产者直到腾出空间或队列关闭/超时。
	PolicyBlock
)

// BackoffStrategy 失败重试的退避算法。
type BackoffStrategy int

const (
	BackoffFixed       BackoffStrategy = iota // 固定间隔
	BackoffLinear                             // 线性增长：base * attempt
	BackoffExponential                        // 指数增长：base * 2^(attempt-1)，封顶 MaxBackoff
)

// Options 引擎配置。零值字段在 Open 时会用 DefaultOptions 的值补齐。
type Options struct {
	// 数据目录（必需，不存在会自动创建）。
	DataDir string
	// 分区数：按 key 哈希分区。同一 key 一定落在同一分区，
	// 分区内严格按写入顺序投递。默认 4。
	Partitions uint32
	// 容量上限：尚未被“所有消费组”确认而必须保留在磁盘上的消息总字节数。
	// 超过后按 Policy 处理。默认 64MiB。
	MaxBytes int64
	// 达到容量时的策略，默认 PolicyReject。
	Policy CapacityPolicy
	// PolicyBlock 下最长等待时间；<=0 表示一直等到有空位或队列关闭。
	BlockTimeout time.Duration

	// 单条消息最大投递尝试次数（首次投递算第 1 次），超过后进入死信。默认 5。
	MaxAttempts int
	// 退避算法，默认 BackoffExponential。
	Backoff BackoffStrategy
	// 基础退避间隔，默认 100ms。
	BaseBackoff time.Duration
	// 退避间隔上限，默认 10s。
	MaxBackoff time.Duration
	// 消息被拉取后多久未确认（既未 Commit 也未 Nack）视为消费失败，
	// 租约到期后重新投递。默认 30s。
	AckTimeout time.Duration
	// 租约扫描间隔（内部使用），默认 50ms。
	LeaseScanInterval time.Duration

	// 每次落盘是否 fsync。默认 true；测试可关闭以加速（关闭后不保证崩溃一致性）。
	Fsync bool
}

// DefaultOptions 返回带默认值的配置。
func DefaultOptions(dataDir string) Options {
	return Options{
		DataDir:           dataDir,
		Partitions:        4,
		MaxBytes:          64 << 20,
		Policy:            PolicyReject,
		BlockTimeout:      0,
		MaxAttempts:       5,
		Backoff:           BackoffExponential,
		BaseBackoff:       100 * time.Millisecond,
		MaxBackoff:        10 * time.Second,
		AckTimeout:        30 * time.Second,
		LeaseScanInterval: 50 * time.Millisecond,
		Fsync:             true,
	}
}

// withDefaults 用默认值补齐零值字段。
func (o Options) withDefaults() Options {
	d := DefaultOptions(o.DataDir)
	if o.DataDir == "" {
		o.DataDir = d.DataDir
	}
	if o.Partitions == 0 {
		o.Partitions = d.Partitions
	}
	if o.MaxBytes == 0 {
		o.MaxBytes = d.MaxBytes
	}
	if o.MaxAttempts == 0 {
		o.MaxAttempts = d.MaxAttempts
	}
	if o.BaseBackoff == 0 {
		o.BaseBackoff = d.BaseBackoff
	}
	if o.MaxBackoff == 0 {
		o.MaxBackoff = d.MaxBackoff
	}
	if o.AckTimeout == 0 {
		o.AckTimeout = d.AckTimeout
	}
	if o.LeaseScanInterval == 0 {
		o.LeaseScanInterval = d.LeaseScanInterval
	}
	return o
}

// Validate 校验配置。
func (o Options) Validate() error {
	if o.DataDir == "" {
		return errors.New("eventqueue: DataDir is required")
	}
	if o.Partitions == 0 {
		return errors.New("eventqueue: Partitions must be > 0")
	}
	if o.MaxBytes <= 0 {
		return errors.New("eventqueue: MaxBytes must be > 0")
	}
	if o.MaxAttempts < 1 {
		return errors.New("eventqueue: MaxAttempts must be >= 1")
	}
	if o.BaseBackoff < 0 || o.MaxBackoff < 0 || o.AckTimeout <= 0 || o.LeaseScanInterval <= 0 {
		return errors.New("eventqueue: invalid duration option")
	}
	if o.MaxBackoff > 0 && o.BaseBackoff > o.MaxBackoff {
		return errors.New("eventqueue: BaseBackoff must be <= MaxBackoff")
	}
	if o.Policy != PolicyReject && o.Policy != PolicyBlock {
		return errors.New("eventqueue: invalid CapacityPolicy")
	}
	if o.Backoff != BackoffFixed && o.Backoff != BackoffLinear && o.Backoff != BackoffExponential {
		return errors.New("eventqueue: invalid BackoffStrategy")
	}
	return nil
}

// backoffDelay 计算第 attempt 次失败（attempt 从 1 开始，表示第 attempt 次投递失败后）
// 到下一次投递之间的等待时间。
func (o Options) backoffDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	var d time.Duration
	switch o.Backoff {
	case BackoffFixed:
		d = o.BaseBackoff
	case BackoffLinear:
		d = o.BaseBackoff * time.Duration(attempt)
	case BackoffExponential:
		d = o.BaseBackoff << (attempt - 1)
	}
	if o.MaxBackoff > 0 && d > o.MaxBackoff {
		d = o.MaxBackoff
	}
	return d
}
