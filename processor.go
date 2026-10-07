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
// Watermark 记录判定时刻的水位线，便于排查"为什么被判太迟"。
type SideEvent struct {
	Record    Record
	Reason    error
	Watermark time.Time
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
//
// 水位线按分区推进（见 WatermarkTracker）：整体水位线 = 各活跃分区
// （分区最大事件时间 - 乱序容忍）的最小值；空闲分区暂时退出计算。
//
// 一致性断点的基础：各分区消费位点（offsets）与聚合状态在同一把锁内更新，
// Snapshot 因此在同一瞬间原子捕获"状态 + 位点 + 水位线"，三者严格对得上。
type Processor struct {
	mu              sync.Mutex
	assigner        *TumblingAssigner
	wm              *WatermarkTracker
	allowedLateness time.Duration
	aggregates      map[WindowKey]*Aggregate
	seen            map[string]struct{} // 已处理的 partition:offset，幂等去重
	offsets         map[int]int64       // partition -> 下一个待处理 offset（与聚合同锁更新）
	results         []Result
	sideOutput      []SideEvent
}

func NewProcessor(assigner *TumblingAssigner, maxOutOfOrderness, allowedLateness time.Duration) *Processor {
	return &Processor{
		assigner:        assigner,
		wm:              NewWatermarkTracker(maxOutOfOrderness),
		allowedLateness: allowedLateness,
		aggregates:      make(map[WindowKey]*Aggregate),
		seen:            make(map[string]struct{}),
		offsets:         make(map[int]int64),
	}
}

// SetPartitionIdleTimeout 设置分区空闲超时（<=0 禁用空闲检测），
// 默认 DefaultPartitionIdleTimeout。须在开始处理前调用。
func (p *Processor) SetPartitionIdleTimeout(d time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.wm.idleTimeout = d
}

// SetClock 注入时钟（测试用），影响空闲判定。须在开始处理前调用。
func (p *Processor) SetClock(now func() time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.wm.now = now
}

// TrackPartitions 注册预期会消费的分区：已注册但暂无数据的分区会拖住水位线，
// 直到首次来数或被判空闲。Runner 启动每个世代时会按分配自动注册。
func (p *Processor) TrackPartitions(partitions ...int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, part := range partitions {
		p.wm.Track(part)
	}
}

// Process 处理一条记录，返回可区分的处理结论。
// 同一 (Partition, Offset) 的重复消费永远幂等：只返回 DUPLICATE，状态不变。
func (p *Processor) Process(r Record) ProcessResult {
	p.mu.Lock()
	defer p.mu.Unlock()

	// 位点与聚合在同一把锁内推进：任何时刻 Snapshot 都能拿到自洽的一对。
	if next := r.Offset + 1; next > p.offsets[r.Partition] {
		p.offsets[r.Partition] = next
	}

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
		p.sideOutput = append(p.sideOutput, SideEvent{Record: r, Reason: ErrLateBeyondTolerance, Watermark: oldWM})
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

	// 观测事件时间并推进水位线，触发所有 end <= watermark 的窗口。
	newWM := p.wm.Observe(r.Partition, e.EventTime)
	out.Watermark = newWM
	out.NewResults = append(out.NewResults, p.fireWindowsLocked()...)
	return out
}

// CheckIdle 检测空闲分区；若水位线因此推进则触发相应窗口，返回新发出的结果。
// 由 Runner 周期调用；裸用 Processor 时可手动调用（配合 SetClock 可确定性测试）。
func (p *Processor) CheckIdle() []Result {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.wm.CheckIdle() {
		return nil
	}
	return p.fireWindowsLocked()
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

// Snapshot 产出一致性断点：聚合状态 + 各分区消费位点 + 水位线（含每分区进度）
// 在同一把锁内原子捕获，三者严格对应同一个瞬间——无论断点落在处理的哪一时刻，
// 恢复后都不会因此重复计数或漏记录。
func (p *Processor) Snapshot(cpID int64) *Checkpoint {
	p.mu.Lock()
	defer p.mu.Unlock()
	cp := &Checkpoint{
		ID:         cpID,
		CreatedAt:  time.Now().UTC(),
		Watermark:  p.wm.Watermark(),
		MaxTS:      p.wm.MaxTimestamp(),
		Offsets:    make(map[int]int64, len(p.offsets)),
		Partitions: p.wm.PartitionMaxEventTimes(),
		Aggregates: make(map[WindowKey]*Aggregate, len(p.aggregates)),
	}
	for k, v := range p.offsets {
		cp.Offsets[k] = v
	}
	for k, ag := range p.aggregates {
		cp.Aggregates[k] = ag.clone()
	}
	return cp
}

// Restore 从断点恢复聚合、消费位点与（每分区）水位线，返回其中的消费位点供源 Seek。
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
	p.offsets = make(map[int]int64, len(cp.Offsets))
	for k, v := range cp.Offsets {
		p.offsets[k] = v
	}
	// 保留已配置的乱序容忍/空闲超时/时钟，仅重置水位线进度。
	old := p.wm
	nw := NewWatermarkTracker(old.maxOutOfOrderness)
	nw.idleTimeout = old.idleTimeout
	nw.now = old.now
	nw.restore(cp.Watermark, cp.MaxTS, cp.Partitions)
	p.wm = nw

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

// PartitionWatermarks 返回各分区水位状态快照（按分区号排序），
// 用于日志区分"跑得快 / 跑得慢（拖住水位线）/ 空闲"。
func (p *Processor) PartitionWatermarks() []PartitionWMState {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.wm.PartitionStates()
}

// Offsets 返回各分区消费位点快照（下一个待处理 offset）。
func (p *Processor) Offsets() map[int]int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[int]int64, len(p.offsets))
	for k, v := range p.offsets {
		out[k] = v
	}
	return out
}
