# durable-event-queue-go

一个可以直接嵌入单机 Go 进程的**持久化事件队列引擎**。无需部署 Kafka / RabbitMQ
等外部中间件、无任何外部依赖与真实凭据，即可在本地磁盘上提供可靠的
**至少一次（at-least-once）投递**，并通过业务幂等键把「至少一次」收敛为
下游副作用的**精确一次生效（effectively-once）**。

## 特性一览

- 多生产者并发写入；消息按 `Key` 的 FNV-1a 哈希分区，**同一 Key 在同一消费组内
  严格按写入顺序消费**（分区队头阻塞）。
- 容量上限可配置，满了以后可在「立即拒绝」与「阻塞等待」两种策略间选择，
  **绝不静默丢消息**；拒绝通过 `*PublishError.Reason` 区分原因。
- 消息、消费位点、幂等记录、死信全部顺序落盘并 `fsync`；分段 WAL + CRC 帧，
  崩溃产生的半截帧在重启时自动截断。
- 进程被 `SIGKILL` 强杀后重启：**已提交位点之前的消息不会再被投递**；
  已拉取但未确认的消息在租约到期后**自动重新投递**。
- 位点只能单调前进，**回退到已提交位点之前会被明确拒绝**
  （`ErrOffsetRollback`）。
- 消费失败按「固定 / 线性 / 指数」退避重试，超过 `MaxAttempts` 进入**死信队列**。
- 基于业务幂等键的去重状态机（`first-time / duplicate / leased`），
  去重记录与位点提交在磁盘上按固定顺序写入，保证崩溃窗口下也不重复生效。
- 目录级排它文件锁，禁止两个进程同时打开同一数据目录。

## 快速上手

```go
opts := eventqueue.DefaultOptions("/var/lib/myapp/queue")
q, _ := eventqueue.Open("/var/lib/myapp/queue", opts)
defer q.Close()

// 生产
off, err := q.Publish(ctx, eventqueue.Message{
    Key:      "order-42",
    DedupKey: "charge-order-42", // 下游副作用的业务唯一键
    Body:     []byte(`{"amount":100}`),
})

// 消费（不同 group 各自维护独立位点，类似 Kafka consumer group）
sub, _ := q.Subscriber("billing")
for {
    env, err := sub.Poll(ctx)
    if err != nil { return }

    // Commit 原子完成「幂等去重落盘 + 位点前进」
    verdict, err := sub.Commit(ctx, env.Offset, env.Message.DedupKey)
    switch verdict {
    case eventqueue.GuardFirstTime:
        applyChargeOnce(env) // 只有这里允许执行副作用
    case eventqueue.GuardDuplicate:
        // 重试 / 重复投递路径：副作用此前已生效，直接跳过
    case eventqueue.GuardLeased:
        time.Sleep(time.Millisecond * 50) // 另一次尝试仍在进行，稍后再试
    }

    // 失败时：退避重试，超过 MaxAttempts 自动进死信
    // _ = sub.Nack(ctx, env.Offset, errCharge)
}
```

不需要业务幂等时，`DedupKey` 留空并使用 `CommitOffset` 即可（纯至少一次语义）。

## 投递语义

| 场景 | 行为 |
| --- | --- |
| 正常消费 | `Poll` → 业务处理 → `Commit/CommitOffset`，位点前进 |
| 业务失败 | `Nack`：未超 `MaxAttempts` 按退避时间重投；达到上限进入死信并推进位点 |
| 处理中崩溃 / 强杀 | 未确认消息租约到期后重投（至少一次） |
| 重复投递 | `Commit(off, dedupKey)` 返回 `GuardDuplicate`，跳过副作用 |
| 乱序提交 | 分区内必须连续确认；跳过未解决消息会返回错误，不会静默吞消息 |
| 位点回退 | 小于该分区已提交位点的提交返回 `ErrOffsetRollback` |
| 多消费组 | 位点互相独立，同一消息每组各消费一次 |

**顺序保证的边界**：同一 `Key`（哈希到同一分区）在**同一消费组**内严格有序；
不同 Key 之间、不同消费组之间不保证全局顺序。前一条消息未 `Commit/Nack`
（或仍在退避/租约中）时，同分区后续消息会被队头阻塞——这是顺序投递的必要代价。

## 位点与去重规则

- 位点 `Offset` 为全局单调递增的消息序号；每个消费组为每个分区单独保存
  已提交位点（watermark）。
- 持久化在 `meta.log`（+ `snap.meta` 快照压缩）中的记录类型：
  - `commit(group, partition, offset)`：位点前进；
  - `dedup(group, key, offset)`：某业务幂等键的副作用**已确认生效**；
  - `dead(...)`：死信记录。
- `Commit(off, dedupKey)` 的磁盘写入顺序固定为 **先 `dedup` 后 `commit`**：
  - 两条都落盘后崩溃：位点已前进，消息不再投递；
  - 只写了 `dedup` 就崩溃：重启后消息重投，但去重表已恢复，`Commit` 返回
    `GuardDuplicate`，副作用不重复，位点随后补提；
  - 两条都没写：消息重投，正常执行一次。
- 去重状态机（组内、内存态 + 上述持久化）：
  `unknown → leased（在途，租约=AckTimeout）→ confirmed（永久判重）`。
  `leased` 在显式 `Nack` 或租约超时后释放；`confirmed` 不可被覆盖。
  **confirmed 键不会在「位点刚越过」时就回收**——位点越过之后仍可能到达对
  旧消息的重复提交/重投确认，必须继续判定 `GuardDuplicate`；只有当该消息的
  物理分段已被 GC（数据不可能再重放）后，对应判重键才随之回收。
  提交落盘期间的进行中条目带 `committing` 标记，不会被后台租约扫描误回收。
- 注意：`attempt`（投递次数）是**进程内运行状态**，不落盘；强杀重启后未确认
  消息以 `attempt=1` 重新计数，这是至少一次语义的预期行为。是否重复执行
  副作用一律以持久化的 dedup 判定为准。

## 可配置项（`Options`）

| 字段 | 默认值 | 说明 |
| --- | --- | --- |
| `DataDir` | 必填 | 数据目录（自动创建，加排它文件锁） |
| `Partitions` | `4` | 分区数，按 Key 哈希；越大并行度越高，顺序边界为同 Key |
| `MaxBytes` | `64 MiB` | 未被所有组确认而必须保留的消息总字节上限 |
| `Policy` | `PolicyReject` | 满容量策略：`PolicyReject` 立即拒绝 / `PolicyBlock` 阻塞 |
| `BlockTimeout` | `0`（一直等） | 阻塞策略下的最长等待，超时以 `RejectQueueFull` 失败 |
| `MaxAttempts` | `5` | 最大投递尝试次数（首投算 1），超过进死信 |
| `Backoff` | `BackoffExponential` | 退避算法：固定 / 线性 / 指数 |
| `BaseBackoff` | `100ms` | 基础退避间隔（指数：`base·2^(n-1)`） |
| `MaxBackoff` | `10s` | 退避间隔封顶 |
| `AckTimeout` | `30s` | 拉取后多久未确认视为失败并可重投（幂等租约同长） |
| `LeaseScanInterval` | `50ms` | 后台租约扫描 / GC 间隔 |
| `Fsync` | `true` | 每次写盘是否 fsync（仅性能测试可关，关闭后不保证崩溃一致） |

写入被拒绝时的原因（`PublishError.Reason`）：

- `RejectQueueFull`：达到容量上限（拒绝策略立即返回 / 阻塞策略超时返回）；
- `RejectQueueClosed`：队列已关闭；
- `RejectInvalidMessage`：消息非法（空 Key、单条体积超过 `MaxBytes` 等）。

## 磁盘布局

```
<DataDir>/
├── LOCK                 # 目录排它锁（flock）
├── messages/            # 消息分段 WAL（seg-000000000000.log ...）
│   └── seg-*.log        # magic|type|len|payload|crc32 帧，每条 append+fsync
├── meta.log             # 位点 / 幂等 / 死信的顺序日志
└── snap.meta            # meta 压缩快照（前 8 字节为覆盖偏移，原子 rename）
```

所有帧带 CRC32-C 校验；重启时若最后一个分段末尾存在崩溃造成的半截帧，
会自动截断到最后一条完整记录。

## 本地验证

无需任何外部服务或真实凭据，一条命令跑通全部测试：

```bash
make test          # = go test -timeout 120s -v ./...
```

带竞态检测（需要 gcc/cgo）：

```bash
make test-race     # = CGO_ENABLED=1 go test -race -v ./...
```

测试覆盖三类核心场景（日志中会打印**位点、重试次数、幂等判定结果**）：

1. **并发生产** `TestConcurrentProducers`：8 个生产者并发写 800 条，
   校验无丢失、无重复投递、同 Key 严格有序；
2. **强杀重启**：
   - `TestKillRestart` 以**真实子进程 + `os.Exit`（不走 Close）**模拟强杀，
     验证已提交消息不再投递、未确认消息重投、位点回退被拒；
   - `TestKillRestartIdempotency` 精确制造「dedup 已落盘、commit 未写」的
     崩溃窗口，验证重启后判定 `duplicate`、副作用不重复；
3. **重复消费** `TestRetryAndIdempotency`：退避重试（打印 attempt）、
   超上限进死信、16 个 goroutine 并发确认同一幂等键时副作用恰好生效一次。

另有容量拒绝/阻塞（`TestCapacity*`）、多组独立位点
（`TestMultipleGroupsIndependent`）等用例。

## 设计取舍

- 单机嵌入式：以「简洁、可验证、无外部依赖」为目标，不追求多节点复制；
  持久化依赖本地文件系统的 fsync 语义（建议部署在可靠磁盘上）。
- 消息保留量以「尚未被**所有**消费组确认」为准；消息分段只做整段 GC，
  保守而绝不误删仍需重放的数据。
- 死信消息的位点会前进（保证不无限重投），原始内容与失败原因可通过
  `Queue.DeadLetters()` 获取以便人工或补偿任务处理。
