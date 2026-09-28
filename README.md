# 事件时间流式处理引擎（本地可跑）

用本地内存消息缓冲模拟分区日志数据源，实现一套**按事件时间**的分组窗口
聚合引擎，重点解决四类问题，并让每类处理结果都可预期、可解释：

1. **乱序到达**：水位线（watermark）带明确的乱序容忍范围；
2. **迟到数据**：容忍范围内给出可解释的“修正结果”，超范围进入侧输出被
   明确拒绝——既不悄悄丢弃，也不重复计数；
3. **断点与恢复**：检查点原子保存“消费位置 + 窗口状态 + 水位线”，
   重启/重复消费后不重不漏；断点回退被明确拒绝，而不是从中间接着算；
4. **并发摄入与消费组调整**：多分区并发摄入不丢数据（背压），消费组
   再平衡后同一窗口仍保持恰好一次（exactly-once）。

```bash
go run ./cmd/demo        # 端到端演示
go test ./... -v         # 全部单测（日志含水位线/消费位置/状态快照）
go test -race ./...      # 竞态检测
```

---

## 1. 核心概念

### 事件、分区日志、消费位置

- `Event{Key, EventTime, Value}`：一条业务事件；`Key` 是分组键。
- `PartitionLog`：本地内存分区日志（模拟 Kafka 分区）。偏移从 0 起单调
  递增，写入后不消失，可按任意偏移**重复读取**。`ReadAt` 在偏移尚未写入时
  阻塞等待（背压），分区 `Close` 后读到末尾返回 `source-gone`（正常结束）。
- 每条记录由 `(partition, offset)` 唯一标识，这是去重与恰好一次的基础。

### 水位线与乱序容忍

```
watermark = min(每个活跃分区观测到的最大事件时间) - maxOutOfOrderness
```

- 已注册但**尚无数据**的分区会以 −∞ 参与最小值，从而拖住全局水位线：
  慢分区上的数据不会仅仅因为别的分区跑得快就被误判为迟到；
- 分区 `Close` 读完后退出最小值计算；所有分区退出后水位线保持最后值
  （单调不减，不会回落为 −∞）；
- 同一分区内，事件必须在该分区已观测到更大事件时间之后到达，才可能被判
  迟到——这与现实中“慢分区拖住水位线”的语义一致。

### 窗口与迟到数据规则

固定（tumbling）窗口，左闭右开 `[start, end)`，`start = floor(t / size) * size`。

对一条事件时间为 `t` 的事件：

| 情形 | 判定条件 | 处理结果 |
|---|---|---|
| 正常到达 | 窗口未触发 | 计入窗口 |
| 窗口已触发，但仍在容忍内 | `watermark <= end + allowedLateness` | 计入并立即输出 `CORRECTED`（版本号 `v2,v3,...`） |
| 超出容忍 | `watermark > end + allowedLateness` | **拒绝**，进入侧输出 `LateEvents`，错误原因为 `late-rejected` |

- 首次触发：`watermark >= end` 时输出 `INITIAL`（`v1`）。
- 容忍内迟到：窗口状态在 `allowedLateness` 内保留，迟到事件触发一条
  `CORRECTED` 修正结果，`Version` 单调递增，下游能清楚区分“首算”与“修正”。
- 超容忍：窗口状态被 GC，后续同窗口事件进入**侧输出** `LateRejection`，
  其中记录了该事件、拒绝时的水位线和容忍截止线 `deadline=end+allowedLateness`，
  可核对、可对账；**它不静默丢弃，也绝不计入任何聚合**。

去重：窗口状态记录了所有已处理记录的 `(partition, offset)` 集合。同一记录
被重复投递（至少一次重放/重启）时返回 `duplicate`，跳过、不双算。

---

## 2. 断点（检查点）语义

一次 `Checkpoint` 原子持久化以下内容（缺一会对不上）：

- `PartitionNext`：各分区“下一条待处理偏移”（消费位置）；
- `Windows`：所有窗口状态，**含每条已处理记录的去重集合**（同时记录 value，
  便于多消费者快照合并后重算聚合）；
- `Watermark / PartitionMax / Registered / Finished`：水位线及各分区观测值；
- `Generation`：单调递增的代际号。

`FileCheckpointStore` 用“临时文件 + fsync + 原子 rename”落盘，文件要么是
旧快照、要么是新快照。

### 恢复与回退规则

启动时把“请求起始位置”与“已提交位置”逐分区比较（`ValidateStartOffsets`）：

- `start < committed`：**断点回退，明确拒绝**（`checkpoint-rollback`），
  引擎不会从中间重新计算；
- `start == committed`：正常重启。检查点之前的结果不会重发，未提交区域
  若被源重放，由记录级去重保证不双算；
- `start > committed` / 新分区：允许。

存储层同样拒绝旧代际号覆盖新快照（即使是重启后的新进程，也会先读盘比对）。

### 消费组并发保存：检查点合并

同一消费组的多个消费者各负责一部分分区，并发向共享存储保存检查点。
`GroupCheckpointStore` 把每次保存串行化为“读最新 → 合并 → 写回”，
合并规则（`MergeCheckpoints`）：

- 消费位置按分区取最大（不回退）；
- 窗口内记录按 `(partition,offset)` 求并集去重，并由记录值**重算**
  sum/count（不双算）；`fired` 取 OR，`version` 取最大；
- 水位线由合并后的分区观测值重新计算；
- 代际号严格递增。

---

## 3. 恰好一次（exactly-once）的保证范围

**保证：**

- 每条记录对每个窗口的聚合**恰好贡献一次**：记录级去重 + 原子检查点；
- 检查点边界之前已触发的窗口，恢复后不会再次输出 `INITIAL`；
- 容忍内迟到产生的每次修正都有单调版本号，可与首算区分；
- 再平衡把分区从消费者 A 迁到 B：状态随检查点迁移、去重集合跟随，
  同一窗口不会因迁移而漏算或重算（见 `TestRunner_GroupRebalance_ExactlyOnce`）。

**需要下游配合的边界（诚实说明）：**

- `Outputs` 通道是引擎与下游之间的交付边界。检查点代表“引擎内部状态已
  提交”，并不自动等价于“下游已完成事务”。若下游需要端到端事务，应在
  处理完某批输出后再推进其自身位点；本引擎保证重放可幂等（重复输出可被
  去重/按版本号识别）。
- 超容忍迟到事件**不进入聚合**，但不会丢——它出现在 `LateEvents` 侧输出，
  由调用方决定如何补偿（对账、告警、人工处理）。

### 背压不丢数据

摄入 goroutine 只在单一状态锁内更新状态并入待发队列；唯一的投递器
（dispatcher）在锁外把结果写入 `Outputs/LateEvents` 通道，保证多分区下
结果的全局交付顺序确定。待发队列有界（`deliverQueueLimit`）：

- 下游消费慢 → 通道满 → 投递器阻塞 → 待发队列满 → 摄入在锁内等待；
- 等待期间消费位置不前进，事件停留在分区日志里，**不报错、不丢数据**；
- 下游恢复后自动排空。见 `TestEngine_Backpressure_NoLoss`。

### 失败原因可区分

所有错误都是携带 `FailureReason` 的 `EngineError`，可用 `errors.As` 分支：

| Reason | 含义 |
|---|---|
| `late-rejected` | 超出乱序/迟到容忍，进侧输出，不计入 |
| `duplicate` | 同一 (分区,偏移) 重复投递，去重跳过（体现在 `Stats.Duplicates`） |
| `checkpoint-rollback` | 起始位置早于已提交位点，或旧代际覆盖，明确拒绝 |
| `backpressure` | 背压导致的可重试情况（数据仍在，不丢） |
| `source-gone` | 分区已关闭且偏移越界（正常读完），或向已关闭分区追加 |
| `closed` | 引擎已关闭/未运行即检查点等 |

---

## 4. 验证方法

### 端到端演示

```bash
go run ./cmd/demo
```

演示一条包含以下情节的时间线：乱序计入 → 检查点 → 容忍内迟到输出
`CORRECTED` → 超容忍迟到进侧输出 `LATE-REJECTED` → 关闭收尾 →
从文件检查点重启（无重复输出、无缺失）。

### 单元测试场景

| 测试 | 覆盖场景 |
|---|---|
| `TestWatermark_RegisteredIdleAndFinish` | 空闲分区拖水位线、关闭退出、单调不减 |
| `TestWindowStore_FireCorrectReject` | 首算 / 容忍内修正 / 边界 / 超容忍拒绝 / GC |
| `TestWindowStore_SnapshotRestoreDedup` | 快照恢复后重放判重 |
| `TestEngine_OutOfOrder_Baseline` | 乱序到达，窗口最终值正确 |
| `TestEngine_DuplicateConsumption_Dedup` | 中途检查点、重复投递去重、回退被拒、重启结果一致 |
| `TestRunner_GroupRebalance_ExactlyOnce` | 消费组并发摄入 + 再平衡，窗口不重不漏 |
| `TestEngine_ConcurrentPartitions_Deterministic` | 多分区并发 20 轮结果一致 |
| `TestEngine_Backpressure_NoLoss` | 无消费者时背压停顿，恢复后 100 条恰好一次 |
| `TestMergeCheckpoints_GroupStateUnion` | 多消费者检查点合并不双算 |
| `TestFileStore_RoundTripAndRollbackGuard` | 文件原子落盘、重启后旧代际被拒 |
| `TestFailureReasons_*` | 六类失败原因彼此可区分 |

测试日志会打印三类可观测信息（满足“水位线、消费位置、状态快照”要求）：

- 水位线及各分区观测最大值：`watermark=.. partitionMax={p0:..,..}`；
- 消费位置：`offsets={p0:..,p1:..}`；
- 状态快照：每个窗口的 key/区间/sum/count/fired/version/已处理记录数。

```bash
go test -race -count=3 ./...   # 重复多轮 + 竞态检测
```

---

## 5. 代码结构

```
streamengine/
  event.go        Event / Envelope / WindowResult / LateRejection
  errors.go       FailureReason、EngineError
  source.go       PartitionLog（内存分区日志，阻塞读=背压）、Source
  watermark.go    分区最小值水位线（注册/观测/关闭/恢复）
  window.go       窗口状态：聚合、去重、首算/修正/拒绝、快照
  checkpoint.go   Checkpoint、内存/文件存储、回退校验、多消费者合并
  processor.go    Engine：并发摄入、单投递器、背压、检查点/恢复
  group.go        Runner 消费组、GroupCheckpointStore
cmd/demo/         端到端演示
```
