# durable-event-queue-go

一个可以**直接嵌入单机 Go 进程**的持久化事件队列引擎。不依赖任何外部消息中间件、数据库或网络服务，
只为解决一个问题：

> 不开 Kafka / RabbitMQ，也要有可靠的**至少一次投递（at-least-once）**，
> 并且通过内置幂等设施让下游业务副作用**实际只生效一次**。

- 多生产者并发写入，消息顺序落盘（每条消息有单调递增的 `offset`）。
- 消费组（consumer group）拉取模型；**同一个 `Key` 的消息在组内严格按写入顺序消费**
  （任意时刻同 key 只有队首消息在途），不同 key 可并行。
- 消息、位点、重试/死信状态、幂等判定全部落在本地磁盘，每次状态变更都 `fsync`，
  崩溃（包括 `SIGKILL` 强杀、断电级退出）后重启可恢复。
- 容量可配置：满时**拒绝**或**阻塞**，拒绝原因可区分，绝不静默丢消息。
- 失败按可配置退避重试，超过上限进入死信队列（DLQ）。
- 内置持久化幂等原语 `ExecuteOnce`，覆盖并发调用、消息重复投递、重试、进程重启路径。

纯 Go 标准库实现，无第三方依赖。

---

## 1. 快速开始

```go
import dq "github.com/lechandonga/durable-event-queue-go/queue"

q, err := dq.Open(dq.DefaultConfig("./data"))
if err != nil { panic(err) }
defer q.Close()

// 1) 建消费组（位点从 offset 0 开始）
_ = q.CreateConsumerGroup("billing", 0)

// 2) 生产
off, err := q.Produce(dq.Message{Key: "order-42", Payload: []byte(`{"amount":100}`)})

// 3) 拉取（阻塞；timeout<=0 永久等待）
msgs, err := q.Pull("billing", 16, 5*time.Second)
for _, m := range msgs {
    // 4) 用业务幂等键包裹副作用
    verdict, err := q.ExecuteOnce("order-42/charge", func() error {
        return chargeCustomer(m.Payload) // 真实副作用
    })
    if err != nil {
        _ = q.Nack("billing", m.Offset, err) // 退避后重试
        continue
    }
    // verdict: DedupNew=本次执行 / DedupPending=有在途执行 / DedupComplete=已生效跳过
    _ = q.Ack("billing", m.Offset)
}
```

完整可运行示例见 [`examples/basic/main.go`](examples/basic/main.go)。

---

## 2. 投递语义（Delivery Semantics）

整体语义是 **at-least-once + 持久化幂等原语 = 业务视角的 effectively-once**。

### 2.1 生产端

- `Produce` 先把消息以「魔数 + 长度 + CRC32 + offset + key + payload」追加到
  `messages.log`，`write` 后立即 **`fsync`**，成功才返回 offset。
- 崩溃恰好发生在写一半时，重启会检测到尾部记录不完整或 CRC 不匹配并**截断撕裂尾部**，
  已确认返回的消息不会受影响，未完成的写入由生产者重试（at-least-once）。

### 2.2 消费端：同 key 顺序

- 每个消费组独立维护位点与状态。
- 调度规则：从组位点开始扫描，**对每个 key 只投递它“最靠前的一条未完成消息”**。
  - 队首在途（已拉取未 ack）→ 该 key 后续消息等待；
  - 队首处于退避等待 → 该 key 后续消息一并等待；
  - 队首进死信 → 视为该 key 的一个终结点，顺序继续向后推进。
- 不同 key 之间互不阻塞，可在同一批 `Pull` 中一起返回并由多个 worker 并发处理。

### 2.3 未确认消息的重新投递（两条路径）

1. **可见性超时（visibility timeout）**：消息被拉取后若在 `VisibilityTimeout`
   内没有 ack（消费者拿到后崩溃、卡死），后台 reaper 会把它转回重试状态，按退避重新投递。
2. **进程崩溃重启**：所有处于 inflight 的消息在重新打开时一律重置为待投递，
   重启后的第一次 `Pull` 会重新拿到它们（`Attempt` 累加）。

> 因此下游**必须**配合 ack 或幂等机制处理重复投递，这正是 at-least-once 的含义。

### 2.4 失败重试与死信

- `Nack(group, offset, cause)`：按 `Backoff(attempts)` 计算退避时间，到期前不会重投；
  到期后以 `Attempt+1` 重新投递。
- 当投递次数达到 `MaxDeliveryAttempts` 时消息进入**死信队列**（快照保留 payload、次数、
  最后错误原因、时间），通过 `DeadLetters(group)` 查询。
- 死信消息视作“已终结”，位点可以连续跳过它继续前进（它不再被投递，但记录保留）。

---

## 3. 位点（Offset）与去重（Idempotency）规则

### 3.1 位点规则

- offset 从 **0** 开始单调递增，全局唯一；消息日志压缩（回收已消费前缀）**不改写剩余消息的
  offset 编号**，因此位点在任何时刻都稳定指向同一条消息。
- 消费组状态（`CommittedOffset`、inflight、重试时间、投递次数、死信）保存在
  `groups/<name>.json`，采用 **临时文件写满 + fsync + rename + 目录 fsync** 原子替换。
  任何崩溃时刻，状态文件要么是旧版本、要么是新版本，不会读到半个文件。
- `Ack(offset)`：确认单条消息。当从当前位点起出现**连续的已确认（或已死信）前缀**时，
  位点自动向前推进并持久化。
- `Commit(group, offset)`：显式提交，语义是「`offset` 之前（不含 offset）的消息不再投递」：
  - `offset <= CommittedOffset` → 返回 **`ErrOffsetRollback`**（位点回退被明确拒绝，状态不变）；
  - `offset` 超过连续已确认前缀 → 返回 **`ErrOffsetOutOfRange`**（状态不变）；
  - 合法提交后位点持久化，位点之前的消息即使在崩溃后也**绝不会被重复投递**。
- 重启保证：
  - 已提交位点之前的消息不再投递；
  - 已提交位点之后、崩溃时未 ack 的消息重新投递（attempt 累加）；
  - 回退到已提交之前的 `Commit` 在重启后同样被拒绝。

### 3.2 去重 / 幂等规则（`ExecuteOnce`）

```go
verdict, err := q.ExecuteOnce(businessDedupKey, func() error {
    // 下游业务副作用
    return nil
})
// DedupNew      = 0  首次执行，fn 本次实际运行
// DedupPending  = 1  另一个协程正在执行（或上次执行崩溃，本调用接管）
// DedupComplete = 2  副作用此前已生效，fn 被跳过
```

- 去重键应是**业务侧天然幂等标识**，例如 `订单号 + 动作`（`"order-123/charge"`），
  而不是消息 offset（offset 在不同组/重放场景会复用）。
- 判定结果持久化在 `dedup.log`（BEGIN / COMMIT / FORGET 三种追加记录，各自 CRC 校验并
  fsync）。规则：
  1. `BEGIN` 先落盘，才执行副作用；
  2. 副作用成功后写 `COMMIT` 并 fsync —— **只有 COMMIT 落盘才视为“已生效”**；
  3. 副作用返回错误 → 写 `FORGET` 清除登记，允许下次重试；
  4. 后续任何重复投递/重试/重启后的同键调用，一律返回 `DedupComplete` 且**不再执行 fn**；
  5. 若崩溃发生在 BEGIN 之后、COMMIT 之前（重启后该键处于 pending 且无属主），
     下一个同键调用会以 `DedupPending` 身份**接管执行**；进程内并发时，后到的调用会等待
     在途执行结束并拿到 `DedupPending`/`DedupComplete`，绝不会并发执行同一副作用。
- 完成键的历史按 `DedupHistory` 保留（FIFO），超出后压缩去重日志；pending 键永不淘汰。
- **边界说明（诚实声明）**：若业务副作用是对**外部系统**的调用，而外部系统本身在
  COMMIT 落盘前不可回滚（例如外部扣款已成功、随后本进程断电），没有任何本地队列能单独
  做到跨系统 exactly-once；此时应让外部调用携带业务幂等键（幂等令牌），把去重下沉到外部。
  `ExecuteOnce` 保证的是：本引擎记录到“已完成”之后，重复消息不会再触发副作用。

---

## 4. 可配置项

```go
type Config struct {
    DataDir string // 持久化目录

    // ---- 容量 ----
    MaxPendingMessages  int64         // 未完成消息条数上限（0=不限），默认 100000
    MaxDataBytes        int64         // 日志字节上限（0=不限），默认 1GiB
    FullPolicy          CapacityPolicy// PolicyReject(默认) | PolicyBlock
    BlockProduceTimeout time.Duration // PolicyBlock 下最长等待，0=永久等待

    // ---- 重试 / 死信 ----
    MaxDeliveryAttempts int           // 最大投递次数，超过进 DLQ；<=0 无限重试，默认 5
    Backoff             BackoffFunc   // ConstantBackoff / LinearBackoff / ExponentialBackoff
    VisibilityTimeout   time.Duration // 拉取后多久未 ack 自动重投，默认 30s；<=0 关闭自动重投

    // ---- 幂等 ----
    DedupHistory int // 已完成幂等键保留窗口，默认 100000

    Logger Logger // 日志注入点（测试用它打印位点/重试/判定结果）
}
```

### 4.1 容量策略

- 容量统计的是“**尚未被所有消费组提交**”的消息：`pending = 下一条offset - 各组最旧位点`，
  同时检查日志字节数；任一上限触顶即视为满。
- `PolicyReject`：`Produce` 立即返回 **`ErrQueueFull`**，调用方可稍后重试。
- `PolicyBlock`：`Produce`（或 `ProduceWait(msg, timeout)`）阻塞，直到消费者提交腾出容量；
  `timeout>0` 时超时返回 **`ErrTimeout`**，消息依然没有丢失。
- 其它可区分的拒绝原因：
  `ErrQueueFull`（满）、`ErrQueueClosed`（已关闭）、`ErrInvalidMessage`（空 payload 等）。
- 位点推进后，所有消费组都已提交的消息前缀会从日志中回收（压缩重写，offset 不变），
  从而为生产者腾出容量。
- 注意：只要存在一个“慢消费组”，其位点之前的消息就不能被物理回收——这是多组广播语义的
  必然结果，容量规划需考虑最慢的组。

### 4.2 退避策略

```go
cfg.Backoff = dq.ConstantBackoff(100 * time.Millisecond)
cfg.Backoff = dq.LinearBackoff(50 * time.Millisecond)        // 50ms * attempt
cfg.Backoff = dq.ExponentialBackoff(50*time.Millisecond, 10*time.Second) // 默认
cfg.Backoff = func(attempt int) time.Duration { return time.Duration(attempt) * time.Millisecond }
```

重试间隔期间该消息（及其同 key 后续消息）不可见；到期由后台 reaper（50ms tick）唤醒拉取。

### 4.3 磁盘布局

```
<DataDir>/
├── messages.log        # 仅追加消息日志（CRC 保护，崩溃后截断撕裂尾部）
├── dedup.log           # 幂等 BEGIN/COMMIT/FORGET 日志（超窗口自动压缩）
└── groups/
    └── <group>.json    # 每个消费组的位点/inflight/重试/死信状态（原子替换）
```

---

## 5. 本地验证方法

**无任何外部服务、无真实凭据、无需网络**，只需要 Go 工具链：

```bash
# 一条命令跑通全部测试（详细日志）
make test
# 等价于：
go test ./... -count=1 -v

# 带竞态检测（推荐在 CI 中使用）
make test-race
```

测试覆盖三类核心场景：

| 测试 | 场景 | 断言内容 |
| --- | --- | --- |
| `TestConcurrentProducersPerKeyOrder` | **并发生产**（16 生产者 / 8 消费 worker） | 每条消息恰好投递一次；同 key 任意时刻只有一条在途、offset 不倒序；位点最终推进到总数 |
| `TestHardKillRestart` | **强杀重启**（真实子进程 `SIGKILL`，不走 Close/defer） | 已提交位点之前不重投；未确认消息重启后重投且 attempt=2；位点回退被拒；COMMIT 过的幂等键重启后 COMPLETE；BEGIN 后被杀的键重启后可接管执行 |
| `TestExecuteOnceConcurrentAndRetry` / `TestRestartRedeliveryAndDedupPersistence` | **重复消费 / 幂等** | 32 并发同键副作用只生效 1 次；重复投递全部 COMPLETE；失败 FORGET 后可重试；重启后判定结果持久 |
| `TestRetryBackoffAndDeadLetter` | 重试退避 + 死信 | 退避窗口不可见；attempt 1→2→3；超限进 DLQ 且携带原因；死信解锁同 key 后续消息 |
| `TestVisibilityTimeoutRedelivery` | 消费者拿到消息后“假死” | 超时自动重投，位点不前移，ack 后推进 |
| `TestCapacityRejectReasons` / `TestCapacityBlockPolicy` | 容量 | 满 → `ErrQueueFull`、阻塞 → 唤醒/超时、关闭 → `ErrQueueClosed`，全程不丢消息 |
| `TestMultipleConsumerGroups` | 多消费组 | 组间位点独立，同一条消息各组独立消费 |

测试日志（`-v` 可见）会直接打印：

- **位点**：`deliver ... committed=N`、`ack ... committed=N`、`commit ok ... committed=N`、
  `commit REJECTED rollback/outofrange ...`、重启后的 `recover group=... committed=N`；
- **重试次数**：`deliver ... attempt=k`、`retry schedule ... attempts=k`、
  `deadletter ... attempts=k`；
- **幂等判定结果**：`idempotent key=... verdict=NEW|PENDING|COMPLETE`、
  测试汇总行 `idempotency verdicts: NEW=x PENDING=y COMPLETE=z (effectCount=n)`。

---

## 6. API 速览

| API | 说明 |
| --- | --- |
| `Open(Config)` / `Close()` | 打开/干净关闭（每次状态变更都已 fsync） |
| `Produce(Message)` | 写入；按容量策略拒绝或阻塞 |
| `ProduceWait(Message, timeout)` | 显式阻塞写入，超时 `ErrTimeout` |
| `CreateConsumerGroup(name, startOffset)` | 建组，初始位点 `startOffset` |
| `Pull(group, max, timeout)` | 阻塞拉取，返回 `[]*DeliveredMessage{Offset,Key,Payload,Attempt}` |
| `Ack(group, offset)` | 确认；连续前缀自动推进位点 |
| `Nack(group, offset, cause)` | 否定；退避重试，超限进死信 |
| `Commit(group, offset)` | 显式提交位点；回退 `ErrOffsetRollback` |
| `CommittedOffset(group)` | 查询已提交位点 |
| `DeadLetters(group)` | 死信快照 |
| `ExecuteOnce(key, fn)` | 持久化幂等执行，返回 `DedupKind` 判定 |

`ConsumerGroup` 句柄还提供同样方法的便捷形式，以及 `PullChan`（通道式持续消费）。

---

## 7. 设计边界与非目标

- 单进程、本地磁盘、多 goroutine；不做跨进程网络协议与复制（多进程同时打开同一目录
  属于未定义行为）。
- fsync 保证的是操作系统确认写入后机器整体断电也不丢；不防御磁盘硬件静默损坏（CRC 只做
  检测与尾部截断）。
- 消费组状态文件为完整 JSON 快照原子替换：状态变更持久化开销与状态大小成正比，面向的是
  嵌入式中等吞吐场景。
- 不提供消息 TTL；死信永久保留在状态文件中，直到消费组目录被外部清理。
