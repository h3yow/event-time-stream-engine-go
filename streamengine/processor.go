package streamengine

import (
	"context"
	"math"
	"sync"
)

// 默认结果通道容量与待发队列上限：消费者跟不上时，队列满后生产者在锁内
// 等待，背压直接传导到读取循环——事件停留在源日志中，不前进偏移也不丢失。
const (
	defaultOutputBuffer = 4096
	deliverQueueLimit   = 256
)

// Config 是引擎配置。
type Config struct {
	WindowSize        int64 // 固定窗口大小（毫秒）
	MaxOutOfOrderness int64 // 水位线乱序容忍（毫秒）
	AllowedLateness   int64 // 窗口触发后仍接受迟到事件的额外容忍范围（毫秒）
}

// Stats 是引擎运行期处理统计，使各类处理结果可区分、可观测。
type Stats struct {
	Accepted    int64 // 正常计入（含容忍内迟到，其修正结果数另见 Corrections）
	Duplicates  int64 // 重复交付被去重跳过
	LateReject  int64 // 超容忍迟到被拒绝（同时进入侧输出）
	Corrections int64 // 容忍内迟到触发的修正输出条数
	Emitted     int64 // INITIAL 首次输出条数
}

// Engine 是本地流式事件处理引擎。
//
// 并发模型：每个被分配的分区一个摄入 goroutine；所有状态变更（窗口、水位线、
// 消费位置）都在 e.mu 下串行应用，保证结果确定。结果通过带缓冲通道交付，
// 缓冲打满时发送方阻塞——背压直接传导到读取循环，事件停留在源日志中，
// 既不前进偏移也不丢失。
type Engine struct {
	cfg   Config
	src   *Source
	store CheckpointStore

	// 用户显式指定的启动位置（模拟重复消费/断点回退）；nil 表示从 0 或检查点继续。
	startOffsets map[int]int64
	assigned     map[int]struct{} // 本引擎消费的分区；nil 表示全部

	mu         sync.Mutex
	cond       *sync.Cond
	wm         *WatermarkGenerator
	windows    *WindowStore
	next       map[int]int64 // 各分区已处理到的下一条偏移（运行时消费位置）
	generation int
	stats      Stats
	// deliveredOut/deliveredLate 是投递器已实际写入对应通道的条数。
	deliveredOut  int
	deliveredLate int
	closed        bool
	ran           bool
	finished      bool

	// pendingOut/pendingLate 在锁内累积，由唯一的投递器 goroutine 取出并
	// 按提交顺序交付；deliverCond 唤醒投递器。
	pendingOut  []WindowResult
	pendingLate []LateRejection
	deliverCond *sync.Cond // 唤醒投递器/背压等待（绑定 e.mu）

	outCh  chan WindowResult
	lateCh chan LateRejection
}

// NewEngine 创建引擎。
func NewEngine(cfg Config) *Engine {
	return newEngine(cfg, defaultOutputBuffer)
}

// newEngineWithBuffer 用指定结果通道容量创建引擎（测试背压时用小容量）。
func newEngineWithBuffer(cfg Config, buffer int) *Engine {
	return newEngine(cfg, buffer)
}

func newEngine(cfg Config, buffer int) *Engine {
	e := &Engine{
		cfg:    cfg,
		store:  NewMemoryCheckpointStore(),
		outCh:  make(chan WindowResult, buffer),
		lateCh: make(chan LateRejection, buffer),
	}
	e.cond = sync.NewCond(&e.mu)
	e.deliverCond = sync.NewCond(&e.mu)
	return e
}

// BindSource 绑定数据源（必须在 Run 之前）。
func (e *Engine) BindSource(src *Source) { e.src = src }

// SetCheckpointStore 设置检查点存储（不设置则使用进程内内存存储）。
func (e *Engine) SetCheckpointStore(store CheckpointStore) {
	if store != nil {
		e.store = store
	}
}

// SetStartOffsets 指定各分区起始消费位置（模拟重复消费/断点回退场景）。
func (e *Engine) SetStartOffsets(offsets map[int]int64) {
	e.startOffsets = make(map[int]int64, len(offsets))
	for p, o := range offsets {
		e.startOffsets[p] = o
	}
}

// setAssigned 限定本引擎消费的分区（消费组再平衡）；nil 表示消费全部。
func (e *Engine) setAssigned(assigned []int) {
	if assigned == nil {
		e.assigned = nil
		return
	}
	e.assigned = make(map[int]struct{}, len(assigned))
	for _, p := range assigned {
		e.assigned[p] = struct{}{}
	}
}

// Outputs 返回窗口结果通道（首次输出 INITIAL 与容忍内迟到修正 CORRECTED）。
func (e *Engine) Outputs() <-chan WindowResult { return e.outCh }

// LateEvents 返回超出容忍范围被拒绝事件的侧输出通道（不静默丢弃）。
func (e *Engine) LateEvents() <-chan LateRejection { return e.lateCh }

// activePartitions 返回本引擎负责的分区编号（升序）。
func (e *Engine) activePartitions() []int {
	if e.assigned == nil {
		return e.src.IDs()
	}
	var out []int
	for _, id := range e.src.IDs() {
		if _, ok := e.assigned[id]; ok {
			out = append(out, id)
		}
	}
	return out
}

// Run 启动处理循环，直到所有负责分区关闭并读完或 ctx 取消。
// 启动时先加载检查点并校验启动位置（断点回退在此被拒绝）。
func (e *Engine) Run(ctx context.Context) error {
	if e.src == nil {
		return newError(ReasonSourceGone, "engine has no source bound")
	}
	e.mu.Lock()
	if e.ran {
		e.mu.Unlock()
		// 同一 Engine 的交付通道在生命周期内固定（消费者在 Run 前即可订阅）。
		// 再平衡/重启请新建 Engine（消费组 Runner 也是每次新建）。
		return newError(ReasonClosed, "engine already ran; create a new engine to restart")
	}
	e.mu.Unlock()

	e.wm = NewWatermarkGenerator(e.cfg.MaxOutOfOrderness)
	e.windows = NewWindowStore(e.cfg.WindowSize, e.cfg.AllowedLateness)
	e.next = make(map[int]int64)

	parts := e.activePartitions()
	e.wm.Register(parts) // 空闲分区也参与最小值，慢分区不会被快分区越过

	startAt := make(map[int]int64, len(parts))
	for _, p := range parts {
		startAt[p] = 0
	}

	// 1) 恢复检查点。
	cp, err := e.store.Load(ctx)
	if err != nil {
		return err
	}
	if cp != nil {
		for p, off := range cp.PartitionNext {
			if _, mine := lookup(parts, p); mine {
				startAt[p] = off
			}
		}
	}
	// 2) 用户显式启动位置覆盖；任何回退在此被明确拒绝。
	if e.startOffsets != nil {
		if err := ValidateStartOffsets(startAt, e.startOffsets); err != nil {
			return err
		}
		for p, off := range e.startOffsets {
			if _, mine := lookup(parts, p); mine {
				startAt[p] = off
			}
		}
	}
	if cp != nil {
		e.windows.Restore(cp.Windows)
		// 再平衡后本机只负责 parts：以本机分配重建注册集合。
		// 注意：不恢复 cp.Finished——"已读完退出水位线计算"是运行时状态：
		// 重启后本机仍要消费该分区（可能重放尾部），应在本次循环读到末尾时
		// 自行 Finish，否则该分区的结果通道永不收尾。
		e.wm.Restore(parts, nil, cp.PartitionMax, cp.Watermark)
		e.generation = cp.Generation
	}
	e.next = startAt

	// 所有启动校验（含断点回退拒绝）通过后，才进入运行状态，
	// 保证失败的启动尝试不改变引擎、不启动任何 goroutine。
	// 交付通道沿用构造时创建的（消费者可在 Run 前订阅）。
	e.mu.Lock()
	e.ran = true
	e.finished = false
	e.closed = false
	e.stats = Stats{}
	e.deliveredOut = 0
	e.deliveredLate = 0
	e.mu.Unlock()

	// 3) 启动唯一投递器（保证多分区结果的全局交付顺序）。
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	dispDone := make(chan struct{})
	dispReady := make(chan struct{})
	go func() {
		defer close(dispDone)
		e.dispatchLoop(runCtx, dispReady)
	}()
	<-dispReady // 确保投递器已持锁完成首次谓词检查，杜绝早期 Broadcast 丢失

	var wg sync.WaitGroup
	var failMu sync.Mutex
	var firstErr error
	setErr := func(err error) {
		failMu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		failMu.Unlock()
		cancel()
	}

	for _, pid := range parts {
		part := e.src.Partition(pid)
		wg.Add(1)
		go func(pid int, p *PartitionLog) {
			defer wg.Done()
			offset := e.committedStart(pid)
			for {
				if err := runCtx.Err(); err != nil {
					return
				}
				env, err := p.ReadAt(runCtx, offset)
				if err != nil {
					if ee, ok := err.(*EngineError); ok && ee.Reason == ReasonSourceGone {
						e.partitionFinished(pid) // 分区有界读完：退出水位线计算
						return
					}
					if runCtx.Err() != nil {
						return
					}
					setErr(err)
					return
				}
				if err := e.apply(env); err != nil {
					setErr(err)
					return
				}
				offset++
			}
		}(pid, part)
	}
	wg.Wait()

	// 4) 所有负责分区都正常读完：以 +∞ 水位线触发剩余窗口（有界源收尾），
	//    再让投递器排空队列并关闭交付通道。
	if firstErr == nil && ctx.Err() == nil {
		e.finish()
	} else {
		e.stopDispatch() // ctx 取消/出错：也唤醒并结束投递器
	}
	<-dispDone
	e.closeOutputs()
	return firstErr
}

func lookup(xs []int, v int) (int, bool) {
	for i, x := range xs {
		if x == v {
			return i, true
		}
	}
	return -1, false
}

func (e *Engine) committedStart(pid int) int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.next[pid]
}

// partitionFinished 在分区关闭读完后推进水位线（该分区退出最小值计算）。
func (e *Engine) partitionFinished(pid int) {
	e.mu.Lock()
	newWM := e.wm.Finish(pid)
	if newWM != MinWatermark {
		fired := e.windows.Advance(newWM)
		e.stats.Emitted += int64(len(fired))
		e.enqueueOutLocked(fired)
	}
	e.cond.Broadcast()        // 唤醒 AwaitOffsets
	e.deliverCond.Broadcast() // 唤醒投递器（若在容量上等待则唤醒）
	e.mu.Unlock()
}

// apply 串行地处理一条事件：迟到判定/去重 -> 观测水位线 -> 触发窗口。
// 结果只在锁内入队，由投递器锁外交付，因此通道打满不会锁死状态变更。
func (e *Engine) apply(env Envelope) error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return newError(ReasonClosed, "engine closed")
	}
	wmBefore := e.wm.Current()
	outcome, correction, deadline := e.windows.Add(env, wmBefore)

	switch outcome {
	case outcomeRejected:
		// 超出容忍范围：进入侧输出，明确可区分；消费位置照常前进。
		e.stats.LateReject++
		e.enqueueLateLocked(LateRejection{Envelope: env, Watermark: wmBefore, Deadline: deadline})
	case outcomeDuplicate:
		// 重复交付（重启/至少一次重放）：去重跳过，不重复计数。
		e.stats.Duplicates++
	default:
		e.stats.Accepted++
		if correction != nil {
			// 容忍范围内迟到 => 修正版本
			e.stats.Corrections++
			e.enqueueOutLocked([]WindowResult{*correction})
		}
	}

	newWM := e.wm.Observe(env.Partition, env.Event.EventTime)
	if newWM != wmBefore {
		fired := e.windows.Advance(newWM)
		e.stats.Emitted += int64(len(fired))
		e.enqueueOutLocked(fired)
	}

	e.next[env.Partition] = env.Offset + 1
	e.cond.Broadcast()        // 唤醒 AwaitOffsets
	e.deliverCond.Broadcast() // 唤醒投递器
	e.mu.Unlock()
	return nil
}

// enqueueOutLocked 有界入队：待发总数达到上限时在 deliverCond 上等待
// 投递器取走数据。这就是摄入侧背压——持锁等待让各分区读取串行暂停，
// 事件停留在源日志中，不前进偏移、不报错、绝不丢弃。
func (e *Engine) enqueueOutLocked(rs []WindowResult) {
	for len(e.pendingOut)+len(e.pendingLate)+len(rs) > deliverQueueLimit {
		e.deliverCond.Wait()
	}
	e.pendingOut = append(e.pendingOut, rs...)
}

func (e *Engine) enqueueLateLocked(l LateRejection) {
	for len(e.pendingOut)+len(e.pendingLate) >= deliverQueueLimit {
		e.deliverCond.Wait()
	}
	e.pendingLate = append(e.pendingLate, l)
}

// dispatchLoop 是唯一的交付 goroutine。它在 e.cond 上等待队列非空，
// Wait 原子释放 e.mu，锁内 Broadcast 不会丢失；锁外完成实际通道发送，
// 消费者慢只阻塞投递器（数据在内存队列中积压，绝不丢失）。
// 当 closed=true 且队列排空时退出；通道由调用方（Run）统一关闭。
//
// ready 在首次持锁后关闭：Run 等到 ready 后才启动摄入 goroutine，
// 从而杜绝“生产者在投递器进入 Wait 前先 Broadcast”导致的早期信号丢失。
func (e *Engine) dispatchLoop(ctx context.Context, ready chan<- struct{}) {
	stop := context.AfterFunc(ctx, e.deliverCond.Broadcast)
	defer stop()

	e.mu.Lock()
	close(ready) // 持锁发就绪信号：解锁前任何摄入 goroutine 都无法入队/Broadcast
	for {
		for len(e.pendingOut) == 0 && len(e.pendingLate) == 0 && !e.closed {
			e.deliverCond.Wait()
		}
		out := e.pendingOut
		late := e.pendingLate
		e.pendingOut = nil
		e.pendingLate = nil
		drain := e.closed
		e.deliverCond.Broadcast() // 通知背压等待的生产者：队列已腾空
		e.mu.Unlock()

		for i := range out {
			e.outCh <- out[i]
		}
		for i := range late {
			e.lateCh <- late[i]
		}
		if len(out) > 0 || len(late) > 0 {
			e.mu.Lock()
			e.deliveredOut += len(out)
			e.deliveredLate += len(late)
			e.deliverCond.Broadcast()
			e.mu.Unlock()
		}
		if drain {
			return
		}
		e.mu.Lock()
	}
}

// closeOutputs 在投递器排空退出后关闭两个交付通道（close 本身不阻塞）。
func (e *Engine) closeOutputs() {
	close(e.outCh)
	close(e.lateCh)
}

// stopDispatch 在运行异常结束（ctx 取消等）时通知投递器排空退出。
func (e *Engine) stopDispatch() {
	e.mu.Lock()
	e.closed = true
	e.deliverCond.Broadcast()
	e.mu.Unlock()
}

// finish 在所有分区读完后冲发剩余窗口，通知投递器排空退出。
func (e *Engine) finish() {
	e.mu.Lock()
	if !e.finished {
		final := e.windows.Advance(math.MaxInt64)
		e.stats.Emitted += int64(len(final))
		e.enqueueOutLocked(final)
		e.closed = true
		e.finished = true
	}
	e.deliverCond.Broadcast()
	e.mu.Unlock()
}

// Checkpoint 原子提交一次检查点（消费位置 + 窗口状态 + 水位线 + 代际号）。
func (e *Engine) Checkpoint(ctx context.Context) error {
	e.mu.Lock()
	if !e.ran {
		e.mu.Unlock()
		return newError(ReasonClosed, "checkpoint before run")
	}
	e.generation++
	cp := &Checkpoint{
		Generation:    e.generation,
		PartitionNext: copyOffsets(e.next),
		Windows:       e.windows.Snapshot(),
		Watermark:     e.wm.Current(),
		PartitionMax:  e.wm.snapshot(),
		Registered:    e.wm.registeredList(),
		Finished:      e.wm.finishedList(),
	}
	e.mu.Unlock()
	return e.store.Save(ctx, cp) // 存储自身拒绝代际回退
}

func copyOffsets(m map[int]int64) map[int]int64 {
	out := make(map[int]int64, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// AwaitDelivered 阻塞直到投递器已实际向结果通道交付至少 n 条窗口结果，
// 用于在检查点等时刻获得确定的“已可见输出”边界（无轮询、不依赖调度）。
func (e *Engine) AwaitDelivered(ctx context.Context, n int) error {
	stop := context.AfterFunc(ctx, func() {
		e.mu.Lock()
		defer e.mu.Unlock()
		e.deliverCond.Broadcast()
	})
	defer stop()

	e.mu.Lock()
	defer e.mu.Unlock()
	for e.deliveredOut < n {
		if err := ctx.Err(); err != nil {
			return err
		}
		e.deliverCond.Wait()
	}
	return nil
}

// AwaitOffsets 阻塞直到各指定分区都已处理到给定偏移（测试确定性断言用）。
func (e *Engine) AwaitOffsets(ctx context.Context, offsets map[int]int64) error {
	stop := context.AfterFunc(ctx, func() {
		e.mu.Lock()
		defer e.mu.Unlock()
		e.cond.Broadcast()
	})
	defer stop()

	e.mu.Lock()
	defer e.mu.Unlock()
	for {
		all := true
		for p, want := range offsets {
			if e.next[p] < want {
				all = false
				break
			}
		}
		if all {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		e.cond.Wait()
	}
}

// CommittedOffsets 返回当前运行时消费位置的拷贝（测试日志用）。
func (e *Engine) CommittedOffsets() map[int]int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return copyOffsets(e.next)
}

// CurrentWatermark 返回当前水位线。
func (e *Engine) CurrentWatermark() int64 { return e.wm.Current() }

// Stats 返回处理统计的拷贝（各类处理结果可区分）。
func (e *Engine) Stats() Stats {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.stats
}

// Snapshot 返回当前实时状态快照描述：水位线、消费位置、窗口状态（测试日志用）。
func (e *Engine) Snapshot() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	cp := &Checkpoint{
		Generation:    e.generation,
		PartitionNext: copyOffsets(e.next),
		Windows:       e.windows.Snapshot(),
		Watermark:     e.wm.Current(),
		PartitionMax:  e.wm.snapshot(),
		Registered:    e.wm.registeredList(),
		Finished:      e.wm.finishedList(),
	}
	return DescribeSnapshot(cp)
}
