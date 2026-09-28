# 事件时间流式处理引擎（本地可跑）

一个零外部依赖的 Go 库，用**本地有界消息缓冲**模拟分区数据源，实现事件时间（event time）
滚动窗口聚合，并让乱序、重复消费、重启恢复、并发与消费组再均衡下的窗口结果
**可预期、可解释、恰好一次**。

- 模块：`github.com/h3yow/event-time-stream-engine-go`
- 入口文件：`event.go` / `watermark.go` / `window.go` / `disposition.go` /
  `source.go` / `checkpoint.go` / `processor.go` / `group.go`
- 运行要求：Go 1.25+，仅用标准库

## 快速验证

```bash
go test -race -count=1 -v ./...
```

`-v` 日志会在关键节点打印三类信息（见下文"可观测性"）：

- 当前**水位线**（watermark）；
- 各分区**消费位置**（下一个待处理 offset）；
- **状态快照**（每个 keyed window 的 count/sum/fired/revision）与侧输出。

## 一、数据模型

| 概念 | 说明 |
| --- | --- |
| `Event` | 业务事件：`Key`、`EventTime`（事件时间）、`Value` |
| `Record` | 数据记录：`Partition` + `Offset` 唯一确定消费位置，`Offset` 在分区内单调递增 |
| `Window` | 左闭右开 `[Start, End)` 的滚动窗口，边界对齐 Unix 纪元并统一为 UTC |
| `Aggregate` | 单个 `(Key, Window)` 的状态：`Count/Sum`、`Fired`、`Revision` |
| `Result` | 窗口发出的结果；`Revision=0` 是最终结果，`Revision>0` 是迟到修订结果 |
| `SideEvent` | 侧输出：超出容忍范围的迟到事件 + 可区分原因 |

## 二、水位线与迟到数据规则

### 水位线

```
watermark = maxObservedEventTime − maxOutOfOrderness
```

- 水位线**单调不减**，由观测到的最大事件时间减去固定的乱序容忍 `maxOutOfOrderness` 得到；
- 含义：断言"事件时间 `≤ watermark` 的正常事件基本到齐"；
- 某条事件是否迟到，用**推进水位线之前**的旧水位线判断，当前事件不会改变自己的结论。

### 迟到数据的两级容忍（结论可区分，绝不静默处理）

设窗口结束时间为 `E`、当前水位线为 `W`、允许迟到为 `allowedLateness = L`：

| 情况 | 条件 | `Disposition` | 处理 |
| --- | --- | --- | --- |
| 准时 | 窗口未触发，或 `eventTime ≥ W` | `ON_TIME` | 正常计入聚合 |
| 迟到但可容忍 | 窗口已触发 且 `eventTime ≥ W−L` | `LATE_ACCEPTED` | 更新聚合，发出 `Revision+1` 的**修订结果** |
| 超出容忍 | `eventTime < W−L` | `LATE_SIDE_OUTPUT` | 不计数，进入**侧输出**并附 `ErrLateBeyondTolerance` |
| 重复消费 | 同一 `(Partition, Offset)` 已处理 | `DUPLICATE` | 幂等跳过，状态不变 |

要点：

- 窗口在 `windowEnd ≤ watermark` 时触发，**最终结果只发一次**（`Revision=0`）；
- 可容忍迟到不会重发旧结果，而是发一个可识别为"更新"的修订结果（`Revision=1,2,…`），
  下游据此用 upsert/按修订号覆盖，而不是累加；
- 超出容忍的事件**既不丢、也不计数**：可在侧输出中查询到原因 `ErrLateBeyondTolerance`；
  它同时被登记为"已处理"，重试不会造成反复侧输出。

对应测试：`TestOutOfOrderAndLateness`、`TestDuplicateConsumption`。

## 三、数据源、背压与消费位置

`MemorySource` 为每个分区维护一条"下标即 offset"的 FIFO 记录切片：

- `ProduceNonBlocking`：缓冲满返回哨兵错误 `ErrBackpressure`，**调用方重试即可，绝不丢数据**；
- `Produce(ctx)`：缓冲满时阻塞等待空位，`ctx` 取消则返回上下文错误，数据仍在；
- `Poll(ctx)`：按读取游标顺序投递；
- `Commit(p, off)` / `Seek(p, off)`：位点语义为"**下一个待处理 offset**"，
  严格单调，回退返回 `ErrOffsetRollback`。`Seek` 不得回到已提交位点之前，
  因此不能从比已确认位置更旧的"中间"接着算。

对应测试：`TestBackpressureNoLoss`、`TestRollbackRejected`。

## 四、断点（checkpoint）语义

`Checkpoint` 是一个一致性快照，三者原子地一起保存：

```
Checkpoint { ID, Watermark, MaxTS, Offsets: partition->next-offset, Aggregates }
```

- 状态（窗口聚合）与消费位置在**同一份快照**里，恢复后严格对齐；
- `CheckpointStore.Save` 要求 ID 单调：用更旧的 ID 覆盖更新的断点 →
  返回 `ErrCheckpointRollback`，**断点回退被明确拒绝**；
- 两种存储：
  - `MemoryCheckpointStore`：同进程内模拟持久化；
  - `FileCheckpointStore`：JSON 文件 + 临时文件原子 rename + fsync，
    可跨进程重启恢复（`encoding/json` 不支持 struct 作 map 键，故自定义了序列化）。

### 运行器的一致性提交顺序

`Runner.Checkpoint`：

1. 先 `Save(处理器快照{状态+水位线+位点})`；
2. 保存成功后再向源 `Commit` 位点。

失败时只会"快照领先"，恢复后**最多重放、绝不丢失**；而重放会被幂等防线挡掉，
因此也不会重复计数。

### 恢复流程 `Runner.Recover`

1. 从存储加载最新断点，恢复聚合状态与水位线；
2. 依据位点把每个分区 `[0, start)` 标记为已处理（`RecoverSeen`，幂等防线）；
3. 对每个分区 `Seek(start)`，回到断点位置继续消费。

于是：

- 断点之后的记录不缺；
- 断点之前若被重复投递（重启/重复消费），只会得到 `DUPLICATE`，不会二次计数；
- 已发出的窗口最终结果不会在恢复后重发（避免下游重复），迟到修订仍可继续产生。

对应测试：`TestRestartRecoveryFromFile`（使用文件存储模拟真正的进程重启）、
`TestRollbackRejected`。

## 五、并发、消费组与再均衡

`GroupCoordinator` 模拟消费组：成员 `Join/Leave` 即**推进世代（generation）**
并确定性地轮询均分分区。世代单调递增。

`Runner` 为当前世代拥有的每个分区运行一个消费 goroutine，所有摄入 goroutine
共享同一个带锁的 `Processor`：

- 记录唯一入口 `Submit(gen, record)` 会校验：
  - 世代过期 → `ErrStaleGeneration`；
  - 分区不属于当前世代分配 → `ErrPartitionNotAssigned`；
- 再均衡 `Rebalance(next)` 的顺序保证不重不漏：
  1. 标记停止并 `cancel`，**等待旧世代所有 goroutine 退出**；
  2. 静止后做一次一致性断点；
  3. 把各分区读取位点 `Seek` 到处理器"已处理"位置
     （旧 goroutine 可能"已 Poll 未处理"，对齐后这些记录会被重新投递）；
  4. 用新世代分配启动；重新投递的记录命中 `(partition,offset)` 幂等去重。

结合"位点对齐 + 处理器幂等"，在并发摄入、周期性断点和多次消费组调整下，
每个窗口的最终结果保持**恰好一次**。

对应测试：`TestConcurrentRebalanceExactlyOnce`（2 分区、3 个成员两次再均衡、
周期断点并存，最终校验 `count/sum` 与单条结果）。

## 六、恰好一次的保证范围与边界

**保证：**

1. 每条被处理记录对聚合状态恰好贡献一次（`(partition,offset)` 幂等去重）；
2. 每个 keyed window 的**最终结果只发一次**，迟到更新以独立的、带 `Revision`
   的修订结果发出，语义上是"对同一 keyed window 的 upsert"；
3. 崩溃/重启/再均衡后，状态与消费位置从同一断点对齐恢复：不重复、不缺失；
4. 背压以 `ErrBackpressure` 显式反馈（阻塞或重试），不静默丢数据；
5. 断点回退、位点回退、过期世代、错误分区归属均有**可区分的哨兵错误**。

**边界（需要接入方知晓）：**

- `Result` 的"恰好一次输出"指处理器语义：结果在内存/断点内不重复，恢复后不重发最终结果。
  若要对**外部系统**（DB/消息下游）做到端到端恰好一次，接入方需按
  `(key, windowStart, revision)` 做幂等 upsert；
- `FileCheckpointStore` 保证单文件原子替换，未实现跨多文件的分布式事务；
- 水位线是经典的"有界乱序"启发式：超出 `maxOutOfOrderness + allowedLateness`
  的迟到数据不进聚合，而是进侧输出（可补偿、可解释），并非无限等待；
- 窗口触发依赖水位线推进；若数据完全停止，需要额外的空闲超时触发策略（本库未内置）。

## 七、可区分的失败原因速查

| 哨兵错误 | 触发场景 |
| --- | --- |
| `ErrBackpressure` | 分区缓冲满，非阻塞生产被拒，可重试 |
| `ErrCheckpointRollback` | 用更旧的断点 ID 覆盖更新断点 |
| `ErrOffsetRollback` | `Commit/Seek` 的位点比已提交位点更旧 |
| `ErrStaleGeneration` | 旧世代提交记录 / 用旧分配启动或再均衡 |
| `ErrPartitionNotAssigned` | 记录的分区不属于当前世代分配 |
| `ErrLateBeyondTolerance` | 迟到事件超出允许迟到范围（记录于侧输出） |
| `ErrSourceClosed` / `ErrUnknownPartition` | 源已关闭 / 访问未知分区 |

## 八、可观测性

测试辅助函数 `dumpState` 会打印：

```
[tag] watermark=<RFC3339>
[tag] consumed-position partition=<p> next-offset=<n>
[tag] state key=<k> window=[start,end) count=.. sum=.. fired=.. revision=..
[tag] side-output partition=.. offset=.. eventTime=.. reason=..
```

对应覆盖：

| 场景 | 测试 |
| --- | --- |
| 乱序到达、两级迟到 | `TestOutOfOrderAndLateness` |
| 重复消费幂等 | `TestDuplicateConsumption` |
| 背压不丢数 | `TestBackpressureNoLoss` |
| 断点/位点回退被拒 | `TestRollbackRejected` |
| 重启恢复（文件断点） | `TestRestartRecoveryFromFile` |
| 并发 + 多次再均衡恰好一次 | `TestConcurrentRebalanceExactlyOnce` |

压测稳定性：`go test -race -count=N ./...`。
