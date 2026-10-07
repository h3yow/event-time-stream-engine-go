# 事件时间流式处理引擎（本地可跑）

一个零外部依赖的 Go 库，用**本地有界消息缓冲**模拟分区数据源，实现事件时间（event time）
滚动窗口聚合，并让乱序、重复消费、重启恢复、并发与消费组再均衡（含**整批换人**）、
**分区冷热不均/空闲**与**高频断点**下的窗口结果**可预期、可解释、恰好一次**。

- 模块：`github.com/h3yow/event-time-stream-engine-go`
- 入口文件：`event.go` / `watermark.go` / `window.go` / `disposition.go` /
  `source.go` / `checkpoint.go` / `processor.go` / `group.go`
- 运行要求：Go 1.25+，仅用标准库

## 快速验证

```bash
go test -race -count=1 -v ./...
```

`-v` 日志会在关键节点打印三类信息（见下文"可观测性与本地验证"）：

- 当前**全局水位线**（watermark）与**每个分区的水位线、参与状态
  （WAITING/ACTIVE/IDLE）、空闲时刻**；
- 各分区**消费位置**（下一个待处理 offset）；
- **状态快照**（每个 keyed window 的 count/sum/fired/revision）与侧输出。

## 一、数据模型

| 概念 | 说明 |
| --- | --- |
| `Event` | 业务事件：`Key`、`EventTime`（事件时间）、`Value` |
| `Record` | 数据记录：`Partition` + `Offset` 唯一确定消费位置，`Offset` 在分区内单调递增 |
| `PartitionState` | 分区参与水位线的状态：`WAITING`（已注册无事件）/`ACTIVE`/`IDLE`（超时无数据） |
| `Window` | 左闭右开 `[Start, End)` 的滚动窗口，边界对齐 Unix 纪元并统一为 UTC |
| `Aggregate` | 单个 `(Key, Window)` 的状态：`Count/Sum`、`Fired`、`Revision` |
| `Result` | 窗口发出的结果；`Revision=0` 是最终结果，`Revision>0` 是迟到修订结果 |
| `SideEvent` | 侧输出：超出容忍范围的迟到事件 + 可区分原因 |

## 二、水位线与迟到数据规则

### 分区级水位线（多分区进度不齐时仍然讲得通）

真实数据源各分区进度并不齐：有的分区一直很热，有的隔很久才来一条，有的彻底空闲。
水位线因此按**分区**分别跟踪，再汇总：

```
partitionWatermark(p) = maxObservedEventTime(p) − maxOutOfOrderness
globalWatermark       = min(partitionWatermark over 参与中的分区)
```

每个分区处于三种参与状态之一，状态变化都有明确原因、可在日志中看到：

| 分区状态 | 含义 | 对全局水位线的影响 |
| --- | --- | --- |
| `WAITING` | 已注册但还没观测到任何事件（含新加入/冷启动分区） | 以时间零点参与，即**继续拖住**（它可能随时来数据） |
| `ACTIVE` | 在空闲超时内有数据，正常参与 | 贡献自己的分区水位线，全局取最小 |
| `IDLE` | 超过 `idleTimeout` 墙钟时间没有任何数据 | **不参与**，不再拖住窗口 |

由此区分题目要求的三种进度情况：

- **走得快的分区不能冤枉走得慢的分区**：全局水位线是各 `ACTIVE` 分区的最小值，
  热分区一路狂奔也不会让冷分区里正常到达的事件被判成"太迟"；
- **走得慢但没停的分区正常拖住水位线**：只要它还在空闲超时内活动，窗口就按
  最慢的活跃分区推进，这正是事件时间语义要求的"等齐"；
- **彻底空闲的分区不再无限拖住窗口**：墙钟超过 `idleTimeout` 后，Runner 的
  水位线驱动循环把它翻转为 `IDLE`（日志可见 `ACTIVE/WAITING -> IDLE` 转换），
  窗口可由其余热分区正常触发。

其它要点：

- 全局水位线**单调不减**。空闲分区之后"复活"即使带来更旧的事件，也不会把水位线
  拉回去；该事件继续走下面既有的两级迟到规则（准时 / 可容忍迟到 / 侧输出），
  分区状态回到 `ACTIVE`；
- **所有分区都 `IDLE`（或没有任何分区）时，全局水位线保持不变**：
  数据完全停止时不会凭空宣布窗口到点，避免空转误触发；
- 空闲检测是墙钟驱动的，通过 `NewProcessorWithIdle(..., idleTimeout, partitions)`
  开启；`idleTimeout=0`（含 `NewProcessor`）表示关闭，此时只有 `ACTIVE` 分区参与，
  `WAITING` 分区一直拖住，行为与"显式推进每个分区"的经典做法一致；
- Runner 默认以 10ms 周期驱动一次空闲检测，可用 `SetWatermarkTickInterval` 调整；
  也可直接调用 `Processor.TickWatermarks()` 手动驱动；
- 单分区场景下，分区水位线就是全局水位线，与旧行为完全一致。

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

对应测试：`TestOutOfOrderAndLateness`、`TestDuplicateConsumption`；
分区冷热不均与空闲：`TestPartitionSkewHoldsBackWatermark`、
`TestIdlePartitionReleasesWatermark`、`TestWaitingPartitionTimesOutToIdle`、
`TestRunnerIdlePartitionAutoFiresWindows`。

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

`Checkpoint` 是一个一致性快照，以下内容原子地一起保存：

```
Checkpoint {
  ID, Watermark, MaxTS,
  Offsets:      partition -> 下一个待处理 offset,
  PartitionWMs: 每个分区的 {state(WAITING/ACTIVE/IDLE), 分区水位线,
                            最大事件时间, 最近事件时间, 空闲时刻},
  Aggregates
}
```

- 状态（窗口聚合）、各分区水位线/空闲状态与消费位置在**同一份快照**里，恢复后严格对齐；
  新批次消费者接手后能立刻知道哪些分区是 `IDLE`、各分区水位线到了哪里；
- `CheckpointStore.Save` 要求 ID 单调：用更旧的 ID 覆盖更新的断点 →
  返回 `ErrCheckpointRollback`，**断点回退被明确拒绝**（相同 ID 的幂等重存允许）；
- 两种存储：
  - `MemoryCheckpointStore`：同进程内模拟持久化；
  - `FileCheckpointStore`：JSON 文件 + 临时文件原子 rename + fsync，
    可跨进程重启恢复（`encoding/json` 不支持 struct 作 map 键，故自定义了序列化；
    不含分区水位线明细的旧断点文件仍可加载，缺失分区按到达重新注册）。

### 断点与处理并发时的一致性边界（高频打点也成立）

高频断点和并发摄入同时发生时，必须保证"存下来的进度"和"已经算进去的状态"
对应**同一个处理瞬间**。实现上：

- `Runner.Submit` 在同一把 `r.mu` 临界区内完成"`Processor.Process(记录)` +
  推进该分区位点"。处理器状态的更新与位点推进因此是一个不可分割的步骤，
  其它摄入不可能插在两者之间；
- `Runner.Checkpoint` 先持 `r.mu` 拷贝位点，再在同一临界区经处理器锁
  （锁序恒为 `r.mu -> proc.mu`）捕获聚合/水位线快照。于是任何快照都满足不变量：

  ```
  快照中所有聚合的记录条数之和 == 快照 offsets 覆盖的记录条数之和
  ```

  即"已计入聚合的记录，其位点必已在快照里；位点之前不存在状态里没有的记录"；
- 快照捕获只短暂持锁，随后的持久化 I/O（JSON 编码、文件落盘）在锁外进行，
  **高频打点不会明显拖慢正常处理**；
- 提交顺序仍是"先 `Save` 快照、成功后再向源 `Commit` 位点"：失败时最多"快照领先"，
  恢复后最多重放、绝不丢失，而重放命中幂等防线不会重复计数。

`TestHighFrequencyCheckpointConcurrency`（1ms 打点 + 双分区并发，逐快照校验上面的
不变量并检查处理耗时）与 `TestRecoverFromEverySnapshot`（从压测期间抽取的每一版
快照恢复续跑，结果都等于全程单消费者）直接覆盖这一保证。

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
并确定性地轮询均分分区。世代单调递增。`ReplaceAll(newMembers)` 对应
"**整批发版**"：老实例整批下线、一批全新实例整体顶上，整个更换只推进一次世代
（不会产生一个中间的"零成员"世代）。

`Runner` 为当前世代拥有的每个分区运行一个消费 goroutine，所有摄入 goroutine
共享同一个带锁的 `Processor`：

- 记录唯一入口 `Submit(gen, record)` 会校验：
  - 世代过期 → `ErrStaleGeneration`；
  - 分区不属于当前世代分配 → `ErrPartitionNotAssigned`；
- 再均衡 `Rebalance(next)` 的顺序保证不重不漏：
  1. 标记停止并 `cancel`，**等待旧世代所有 goroutine 退出**（水位线驱动循环一并停止）；
  2. 静止后做一次一致性断点；
  3. 把各分区读取位点 `Seek` 到处理器"已处理"位置
     （旧 goroutine 可能"已 Poll 未处理"，对齐后这些记录会被重新投递）；
  4. 用新世代分配启动并重新注册全部分区；重新投递的记录命中 `(partition,offset)` 幂等去重。

### 整批换人 / 部分增减成员之后的恢复兼容范围

两种成员变化都支持，恢复语义相同：

- **只增减部分成员**（`Join/Leave`，如扩容/缩容/单实例故障）：见
  `TestPartialMembershipChanges`（3 分区、成员离开再加入）；
- **整批换人**（`ReplaceAll`，如发一次版老实例全下线）：
  - 同一进程内直接 `Rebalance(ReplaceAll(...))`：见
    `TestWholeGroupReplacementInProcess`；
  - 老批次整批下线、**一批全新的 Runner/Processor 实例**上线，从共享的文件断点
    `Recover()` 接手：见 `TestWholeGroupReplacementWithFreshRunners`。

兼容保证：

1. 新批次从同一份断点恢复**共享进度**，各分区位点、聚合状态、分区水位线/空闲状态
   全部接上，最终每个窗口的结果与全程只用一个消费者算出来的**相同**（不重不漏）；
2. **已经发过的窗口最终结果不会再发一遍**（恢复不重放 `results`）；
3. 恢复**之后**到达的迟到事件，仍可在 `allowedLateness` 内继续产生
   `Revision+1` 的修订结果，超出范围的仍进侧输出；
4. 恢复要求**同构**：窗口大小、乱序容忍、允许迟到、空闲超时等处理参数与
   断点生成时一致；分区集合为消费组当前管辖分区（新增分区以 `WAITING` 注册，
   被移除分区的历史状态仍保留在断点中但不再参与处理）。

结合"位点对齐 + 处理器幂等 + 原子快照"，在并发摄入、周期性/高频断点和任意消费组
调整（部分或整批）下，每个窗口的最终结果保持**恰好一次**。

对应测试：`TestConcurrentRebalanceExactlyOnce`、`TestPartialMembershipChanges`、
`TestWholeGroupReplacementInProcess`、`TestWholeGroupReplacementWithFreshRunners`。

## 六、恰好一次的保证范围与边界

**保证：**

1. 每条被处理记录对聚合状态恰好贡献一次（`(partition,offset)` 幂等去重）；
2. 每个 keyed window 的**最终结果只发一次**，迟到更新以独立的、带 `Revision`
   的修订结果发出，语义上是"对同一 keyed window 的 upsert"；
3. 崩溃/重启/再均衡（含整批换人）后，状态、各分区水位线与消费位置从同一断点
   对齐恢复：不重复、不缺失；高频断点与并发处理同时发生时，快照与位点仍严格
   对应同一处理瞬间；
4. 背压以 `ErrBackpressure` 显式反馈（阻塞或重试），不静默丢数据；
5. 断点回退、位点回退、过期世代、错误分区归属均有**可区分的哨兵错误**；
6. 分区进度不齐时水位线按最慢的活跃分区推进，空闲分区在 `idleTimeout` 后
   明确转为 `IDLE` 释放水位线，"快/慢/空闲"三种情况状态可区分、原因可解释。

**边界（需要接入方知晓）：**

- `Result` 的"恰好一次输出"指处理器语义：结果在内存/断点内不重复，恢复后不重发最终结果。
  若要对**外部系统**（DB/消息下游）做到端到端恰好一次，接入方需按
  `(key, windowStart, revision)` 做幂等 upsert；
- `FileCheckpointStore` 保证单文件原子替换，未实现跨多文件的分布式事务；
- 水位线是经典的"有界乱序"启发式：超出 `maxOutOfOrderness + allowedLateness`
  的迟到数据不进聚合，而是进侧输出（可补偿、可解释），并非无限等待；
- 空闲判定是**墙钟超时**：只有超过 `idleTimeout` 墙钟时间没有任何数据的分区才会
  转为 `IDLE`。空闲超时设为 0 表示关闭该特性（分区未显式推进时窗口会一直等，
  需调用方自行推进）；当**所有**分区都空闲时水位线保持不变，不会因为"全都没数据"
  而凭空推进窗口——若需要"全部空闲也定时关窗"，接入方可在业务层叠加定时关闭策略；
- 一个 `IDLE` 分区在超时之后才到达的事件，可能落在全局水位线之后，此时它按
  既有迟到规则处理（可容忍则修订、超容忍则侧输出），这是空闲检测为不拖住窗口
  而付出的标准代价；
- 断点恢复要求处理参数（窗口大小、容忍度、空闲超时）与生成断点时一致。

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

## 八、可观测性与本地验证

测试辅助函数 `dumpState` 会打印全局水位线、**每个分区的状态/分区水位线/最近事件/
空闲时刻/消费位点**、聚合状态快照与侧输出：

```
[tag] watermark=<RFC3339>
[tag] partition=<p> state=WAITING|ACTIVE|IDLE contributes=<bool> \
       part-watermark=.. max-event=.. last-event=.. idle-since=.. next-offset=<n>
[tag] state key=<k> window=[start,end) count=.. sum=.. fired=.. revision=..
[tag] side-output partition=.. offset=.. eventTime=.. reason=..
```

Runner 的日志钩子（`SetLogger`）还会输出空闲状态翻转与空闲触发的窗口：

```
[runner] partition 1 transition ACTIVE -> IDLE at <RFC3339>
[runner] watermark tick fired 1 window(s)
```

本地验证：

```bash
# 全部用例（含竞态检测），-v 可看到上述水位线/位点/状态日志
go test -race -count=1 -v ./...

# 稳定性压测：反复跑，防止时序性 flaky
go test -race -count=20 ./...

# 只看本次新增的几类场景
go test -race -count=1 -v -run \
  'PartitionSkew|IdlePartition|WaitingPartition|IdlePartitionAuto|WholeGroupReplacement|PartialMembership|HighFrequencyCheckpoint|RecoverFromEverySnapshot' ./...
```

对应覆盖：

| 场景 | 测试 |
| --- | --- |
| 乱序到达、两级迟到 | `TestOutOfOrderAndLateness` |
| 重复消费幂等 | `TestDuplicateConsumption` |
| 背压不丢数 | `TestBackpressureNoLoss` |
| 断点/位点回退被拒 | `TestRollbackRejected`、`TestStaleCheckpointRejectedOnRecover` |
| 重启恢复（文件断点） | `TestRestartRecoveryFromFile` |
| 并发 + 多次再均衡恰好一次 | `TestConcurrentRebalanceExactlyOnce` |
| 分区冷热不均（快不冤枉慢） | `TestPartitionSkewHoldsBackWatermark` |
| 分区彻底空闲后释放水位线 / 空闲复活 | `TestIdlePartitionReleasesWatermark`、`TestRunnerIdlePartitionAutoFiresWindows` |
| 只注册未到数的分区超时转空闲 | `TestWaitingPartitionTimesOutToIdle` |
| 整批换人（同进程 / 全新实例接手） | `TestWholeGroupReplacementInProcess`、`TestWholeGroupReplacementWithFreshRunners` |
| 只增减部分成员 | `TestPartialMembershipChanges` |
| 高频断点 + 并发处理原子性 | `TestHighFrequencyCheckpointConcurrency` |
| 从任意一版快照恢复不重不漏 | `TestRecoverFromEverySnapshot` |
| 断点 JSON（含分区水位线）往返 | `TestCheckpointJSONRoundTripWithPartitions` |

压测稳定性：`go test -race -count=N ./...`。
