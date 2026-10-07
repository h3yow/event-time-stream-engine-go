package streamengine

import (
	"sort"
	"sync"
	"time"
)

// Result 是窗口发出的一次结果。
// Revision==0 表示窗口关闭时的最终结果（只发一次）；
// Revision>0 表示窗口关闭后、允许迟到期内被迟到事件修订后重发的结果，
// 修订号单调递增，下游据此识别"更新"而非"重复"。
type Result struct {
	Key      string
	Window   Window
	Count    int64
	Sum      int64
	Revision int64
}

// SideEvent 是侧输出记录：超出容忍范围的迟到事件及其可区分原因。
type SideEvent struct {
	Record Record
	Reason error
}

// ProcessResult 是单条记录的处理结论。
type ProcessResult struct {
	Disposition Disposition
	Watermark   time.Time
	Window      Window
	NewResults  []Result // 本次处理新产生的窗口结果（最终结果或修订结果）
}

// Processor 是核心的事件时间窗口处理器，并发安全。
//
// 迟到数据两级规则（设窗口结束时间 E、水位线 W、允许迟到 L）：
//  1. 事件时间 > W：准时（ON_TIME），正常计入；
//  2. 窗口已触发（W >= E）但 事件时间 >= W-L：在允许迟到范围内
//     （LATE_ACCEPTED），更新聚合并发出 Revision 递增的修订结果；
//  3. 事件时间 < W-L：超出乱序+迟到容忍（LATE_SIDE_OUTPUT），
//     不静默丢弃，转入侧输出并附 ErrLateBeyondTolerance，也不计入聚合。
type Processor struct {
	mu              sync.Mutex
	assigner        *TumblingAssigner
	wm              *WatermarkManager
	allowedLateness time.Duration
	aggregates      map[WindowKey]*Aggregate
	seen            map[string]struct{} // 已处理的 partition:offset，幂等去重
	results         []Result
	sideOutput      []SideEvent
}

// NewProcessor 构造处理器：仅启用固定乱序容忍，不开启空闲分区检测。
// 分区在第一条事件到达时自动注册。
func NewProcessor(assigner *TumblingAssigner, maxOutOfOrderness, allowedLateness time.Duration) *Processor {
	return newProcessor(assigner, maxOutOfOrderness, allowedLateness, 0, nil)
}

// NewProcessorWithIdle 构造处理器并开启空闲分区检测：
// 某分区超过 idleTimeout 墙钟时间没有任何数据，就不再参与全局水位线，
// 窗口不会被长期无数据的分区拖住；空闲超时为 0 表示关闭该特性。
// partitions 为启动时已知的全部分区（WAITING 状态注册），可为 nil。
func NewProcessorWithIdle(assigner *TumblingAssigner, maxOutOfOrderness, allowedLateness, idleTimeout time.Duration, partitions []int) *Processor {
	p := newProcessor(assigner, maxOutOfOrderness, allowedLateness, idleTimeout, nil)
	p.wm.RegisterAll(partitions)
	return p
}

// newProcessor 允许注入墙钟，仅供测试使用。
func newProcessor(assigner *TumblingAssigner, maxOutOfOrderness, allowedLateness, idleTimeout time.Duration, now func() time.Time) *Processor {
	p := &Processor{
		assigner:        assigner,
		allowedLateness: allowedLateness,
		aggregates:      make(map[WindowKey]*Aggregate),
		seen:            make(map[string]struct{}),
	}
	if now != nil {
		p.wm = newWatermarkManagerWithClock(maxOutOfOrderness, idleTimeout, now)
	} else {
		p.wm = NewWatermarkManager(maxOutOfOrderness, idleTimeout)
	}
	return p
}

// RegisterPartitions 注册已知分区（WAITING），重复注册幂等。
// 通常由 Runner 在世代启动时调用，使"还没来过数据"的分区也能正确拖住全局水位线，
// 直到它超过空闲超时。
func (p *Processor) RegisterPartitions(partitions []int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.wm.RegisterAll(partitions)
}

// TickWatermarks 驱动一次空闲检测：把达到 idleTimeout 的分区转为 IDLE 并重算水位线，
// 可能触发已到结束时间的窗口。返回发生的分区状态变化与新产生的窗口结果。
func (p *Processor) TickWatermarks() ([]PartitionTransition, []Result) {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, tr := p.wm.Tick()
	fired := p.fireWindowsLocked()
	return tr, fired
}

// Process 处理一条记录，返回可区分的处理结论。
// 同一 (Partition, Offset) 的重复消费永远幂等：只返回 DUPLICATE，状态不变。
func (p *Processor) Process(r Record) ProcessResult {
	p.mu.Lock()
	defer p.mu.Unlock()

	key := positionKey(r.Partition, r.Offset)
	if _, ok := p.seen[key]; ok {
		return ProcessResult{
			Disposition: DispositionDuplicate,
			Watermark:   p.wm.Watermark(),
			Window:      p.assigner.Assign(r.Event.EventTime),
		}
	}

	e := r.Event
	win := p.assigner.Assign(e.EventTime)
	wk := WindowKey{Key: e.Key, Start: win.Start}
	ag, exists := p.aggregates[wk]
	if !exists {
		ag = &Aggregate{Key: e.Key, Window: win}
		p.aggregates[wk] = ag
	}

	// 判定迟到：在更新水位线"之前"比较，使当前事件不改变自身的结论。
	oldWM := p.wm.Watermark()
	var disp Disposition
	if !e.EventTime.Before(oldWM) || !ag.Fired {
		// 事件时间 >= 水位线，或窗口尚未触发：都算准时。
		// （水位线本身含义是"早于它的正常事件已到齐"。）
		disp = DispositionOnTime
	} else if !e.EventTime.Before(oldWM.Add(-p.allowedLateness)) {
		// 窗口已触发，但仍在 allowedLateness 容忍范围内：接受并修订。
		disp = DispositionLateAccepted
	} else {
		// 超出两级容忍范围：侧输出，绝不计入、绝不静默丢弃。
		p.seen[key] = struct{}{} // 仍登记为已处理，重试不会反复侧输出
		p.sideOutput = append(p.sideOutput, SideEvent{Record: r, Reason: ErrLateBeyondTolerance})
		p.wm.Observe(r.Partition, e.EventTime) // 极旧事件不影响水位线（Observe 单调）
		return ProcessResult{
			Disposition: DispositionLateSideOutput,
			Watermark:   p.wm.Watermark(),
			Window:      win,
		}
	}

	ag.apply(e)
	p.seen[key] = struct{}{}

	out := ProcessResult{Disposition: disp, Window: win}

	if disp == DispositionLateAccepted {
		// 已触发窗口的迟到更新：直接发出一版修订结果。
		ag.Revision++
		res := resultOf(ag)
		p.results = append(p.results, res)
		out.NewResults = append(out.NewResults, res)
	}

	// 观测事件时间并按分区推进全局水位线，触发所有 end <= watermark 的窗口。
	newWM := p.wm.Observe(r.Partition, e.EventTime)
	out.Watermark = newWM
	out.NewResults = append(out.NewResults, p.fireWindowsLocked()...)
	return out
}

// fireWindowsLocked 触发所有结束时间已越过水位线、尚未触发的窗口，每个只发一次。
func (p *Processor) fireWindowsLocked() []Result {
	wm := p.wm.Watermark()
	var fired []Result
	keys := make([]WindowKey, 0, len(p.aggregates))
	for k := range p.aggregates {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if !keys[i].Start.Equal(keys[j].Start) {
			return keys[i].Start.Before(keys[j].Start)
		}
		return keys[i].Key < keys[j].Key
	})
	for _, k := range keys {
		ag := p.aggregates[k]
		if !ag.Fired && !ag.Window.End.After(wm) {
			ag.Fired = true
			res := resultOf(ag)
			p.results = append(p.results, res)
			fired = append(fired, res)
		}
	}
	return fired
}

func resultOf(ag *Aggregate) Result {
	return Result{
		Key:      ag.Key,
		Window:   ag.Window,
		Count:    ag.Count,
		Sum:      ag.Sum,
		Revision: ag.Revision,
	}
}

// Snapshot 产出一致性断点：聚合状态 + 位点 + 水位线（含各分区水位线状态）一并捕获。
func (p *Processor) Snapshot(cpID int64, offsets map[int]int64) *Checkpoint {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.snapshotLocked(cpID, offsets)
}

// snapshotLocked 在已持有处理器锁时产出快照。
func (p *Processor) snapshotLocked(cpID int64, offsets map[int]int64) *Checkpoint {
	cp := &Checkpoint{
		ID:           cpID,
		CreatedAt:    time.Now().UTC(),
		Watermark:    p.wm.Watermark(),
		MaxTS:        p.wm.MaxTimestamp(),
		Offsets:      make(map[int]int64, len(offsets)),
		PartitionWMs: p.wm.SnapshotPartitions(),
		Aggregates:   make(map[WindowKey]*Aggregate, len(p.aggregates)),
	}
	for k, v := range offsets {
		cp.Offsets[k] = v
	}
	for k, ag := range p.aggregates {
		cp.Aggregates[k] = ag.clone()
	}
	return cp
}

// SnapshotLocked 供 Runner 在自身位点锁与处理器锁成对持有时调用（见 Runner.Checkpoint），
// 使快照中的聚合/水位线状态与位点对应同一个处理瞬间。
func (p *Processor) SnapshotLocked(cpID int64, offsets map[int]int64) *Checkpoint {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.snapshotLocked(cpID, offsets)
}

// Restore 从断点恢复聚合与水位线，返回其中的消费位点供源 Seek。
// 恢复后必须再调用 RecoverSeen 重建幂等防线。
func (p *Processor) Restore(cp *Checkpoint) map[int]int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.aggregates = make(map[WindowKey]*Aggregate, len(cp.Aggregates))
	for k, ag := range cp.Aggregates {
		p.aggregates[k] = ag.clone()
	}
	p.results = nil
	p.sideOutput = nil
	p.seen = make(map[string]struct{})
	idle := p.wm.IdleTimeout()
	p.wm = NewWatermarkManager(p.wm.maxOutOfOrderness, idle)
	if len(cp.PartitionWMs) > 0 {
		p.wm.RestorePartitions(cp.PartitionWMs, cp.Watermark)
	} else {
		// 兼容旧版本断点（无分区水位线明细）：仅恢复全局水位线，
		// 分区状态在后续事件到达时重新注册。
		p.wm.RestorePartitions(nil, cp.Watermark)
	}

	offsets := make(map[int]int64, len(cp.Offsets))
	for k, v := range cp.Offsets {
		offsets[k] = v
	}
	return offsets
}

// RecoverSeen 依据位点把 [0,start) 标记为已处理。
// 这是恢复后重复回放旧记录时的幂等防线：快照之前的记录一律 DUPLICATE，
// 因而状态不会被重复计数。
func (p *Processor) RecoverSeen(offsets map[int]int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	parts := make([]int, 0, len(offsets))
	for k := range offsets {
		parts = append(parts, k)
	}
	sort.Ints(parts)
	for _, part := range parts {
		for off := int64(0); off < offsets[part]; off++ {
			p.seen[positionKey(part, off)] = struct{}{}
		}
	}
}

// Results 返回已发出全部结果的拷贝（最终结果 + 修订结果，按发出顺序）。
func (p *Processor) Results() []Result {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]Result, len(p.results))
	copy(out, p.results)
	return out
}

// SideOutput 返回侧输出拷贝。
func (p *Processor) SideOutput() []SideEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]SideEvent, len(p.sideOutput))
	copy(out, p.sideOutput)
	return out
}

// Aggregates 返回聚合状态快照拷贝。
func (p *Processor) Aggregates() map[WindowKey]*Aggregate {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[WindowKey]*Aggregate, len(p.aggregates))
	for k, ag := range p.aggregates {
		out[k] = ag.clone()
	}
	return out
}

func (p *Processor) Watermark() time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.wm.Watermark()
}

// PartitionStatuses 返回各分区的水位线/空闲状态快照（测试与日志用）。
func (p *Processor) PartitionStatuses() []PartitionStatus {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.wm.Statuses()
}
