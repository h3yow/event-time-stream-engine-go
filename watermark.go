package streamengine

import (
	"sort"
	"time"
)

// DefaultPartitionIdleTimeout 是分区空闲超时的默认值：
// 超过该时长未观测到新事件的分区被判定为空闲，暂时退出水位线的 min 计算。
const DefaultPartitionIdleTimeout = 200 * time.Millisecond

// PartitionWMState 是单个分区水位线状态的可观测快照，用于日志排查：
// 能区分"跑得快 / 跑得慢（正拖住水位线）/ 彻底空闲"三种情况。
type PartitionWMState struct {
	Partition      int
	HasData        bool      // 是否已观测到任何事件
	MaxEventTime   time.Time // 该分区观测到的最大事件时间
	Contribution   time.Time // 该分区对整体水位线的贡献：MaxEventTime - maxOutOfOrderness
	Idle           bool      // 超过 idleTimeout 未观测到新事件，已退出 min 计算
	HoldsWatermark bool      // 当前正是该分区拖住了整体水位线（活跃且贡献最小，或暂无数据）
}

// partitionWM 是单个分区的内部水位状态。
type partitionWM struct {
	registered  bool // 已注册（预期会来数）或已观测到事件
	hasData     bool // 是否已观测到任何事件
	maxObserved time.Time
	lastActive  time.Time // 最近一次观测到事件（或注册）的处理时间
	idle        bool
}

// WatermarkTracker 按分区跟踪事件时间进度，整体水位线取各活跃分区贡献的最小值：
//
//	watermark = min over active partitions ( maxObserved[p] - maxOutOfOrderness )
//
// 语义：
//   - 跑得快的分区不会单方面推高水位线，其它分区正常到达的事件不会因此被误判迟到；
//   - 已注册但暂无数据的分区会拖住水位线（贡献视为 -∞），直到首次来数或被判空闲；
//   - 超过 idleTimeout 未活动的分区被标记为空闲、暂时退出 min 计算，
//     避免长时间无数据的分区把水位线永远拖住、窗口永远发不出；
//   - 若全部分区都空闲（整体输入静默），水位线追平各分区贡献的最大值，
//     让窗口得以关闭；之后迟到的数据仍按乱序/允许迟到规则处理；
//   - 空闲分区恢复来数后重新加入 min 计算，但水位线单调不减、绝不回退；
//     其"补交"的旧事件按既有的乱序容忍 + 允许迟到规则判定（可能进入侧输出）。
//
// 本类型不加锁，并发安全由 Processor 的统一锁保证。
type WatermarkTracker struct {
	maxOutOfOrderness time.Duration
	idleTimeout       time.Duration
	now               func() time.Time

	parts       map[int]*partitionWM
	watermark   time.Time
	maxObserved time.Time // 全部分区的最大事件时间（断点/调试兼容字段）
}

func NewWatermarkTracker(maxOutOfOrderness time.Duration) *WatermarkTracker {
	return &WatermarkTracker{
		maxOutOfOrderness: maxOutOfOrderness,
		idleTimeout:       DefaultPartitionIdleTimeout,
		now:               time.Now,
		parts:             make(map[int]*partitionWM),
	}
}

// Track 注册一个预期会来数的分区。已注册但暂无数据的分区会拖住水位线，
// 直到它首次来数或超过空闲超时。重复注册是幂等的（不重置已有状态）。
func (w *WatermarkTracker) Track(partition int) {
	if _, ok := w.parts[partition]; !ok {
		w.parts[partition] = &partitionWM{registered: true, lastActive: w.now()}
	}
}

// Observe 观测分区 partition 的一个事件时间，返回推进后的水位线。水位线单调不减。
func (w *WatermarkTracker) Observe(partition int, ts time.Time) time.Time {
	pw := w.parts[partition]
	if pw == nil {
		pw = &partitionWM{registered: true}
		w.parts[partition] = pw
	}
	if !pw.hasData || ts.After(pw.maxObserved) {
		pw.maxObserved = ts
	}
	pw.hasData = true
	pw.idle = false
	pw.lastActive = w.now()
	if ts.After(w.maxObserved) {
		w.maxObserved = ts
	}
	w.recompute()
	return w.watermark
}

// recompute 推进水位线（只进不退）。两条规则：
//  1. 有活跃分区：水位线 = 各活跃分区贡献的最小值（空闲分区被排除，
//     跑得更快的活跃分区不会被空闲分区拖住）；
//  2. 全部分区都空闲：整体输入已静默，水位线追平"最远观测进度"
//     （各分区贡献的最大值），避免数据停流后窗口永远发不出；
//     之后迟到的数据仍按乱序容忍 + 允许迟到规则处理。
func (w *WatermarkTracker) recompute() {
	var cand time.Time
	active := false
	for _, pw := range w.parts {
		if !pw.registered || pw.idle {
			continue
		}
		if !pw.hasData {
			// 有活跃分区还没来数：水位线保持不动（贡献视为 -∞）。
			return
		}
		c := pw.maxObserved.Add(-w.maxOutOfOrderness)
		if !active || c.Before(cand) {
			cand = c
			active = true
		}
	}
	if active {
		w.advance(cand)
		return
	}
	// 全部空闲：追平最远观测进度。
	var best time.Time
	any := false
	for _, pw := range w.parts {
		if !pw.hasData {
			continue
		}
		c := pw.maxObserved.Add(-w.maxOutOfOrderness)
		if !any || c.After(best) {
			best = c
			any = true
		}
	}
	if any {
		w.advance(best)
	}
}

func (w *WatermarkTracker) advance(cand time.Time) {
	if cand.After(w.watermark) {
		w.watermark = cand
	}
}

// CheckIdle 把超过空闲超时的分区标记为空闲并重新计算水位线，
// 返回水位线是否因此推进（推进时调用方应触发窗口）。
func (w *WatermarkTracker) CheckIdle() bool {
	if w.idleTimeout <= 0 {
		return false
	}
	now := w.now()
	changed := false
	for _, pw := range w.parts {
		if pw.registered && !pw.idle && now.Sub(pw.lastActive) >= w.idleTimeout {
			pw.idle = true
			changed = true
		}
	}
	if !changed {
		return false
	}
	old := w.watermark
	w.recompute()
	return w.watermark.After(old)
}

// restore 从断点恢复水位线：整体水位线 + 各分区最大事件时间。
// 恢复的分区视为活跃（lastActive=now），之后按正常空闲规则判定。
func (w *WatermarkTracker) restore(wm, maxTS time.Time, parts map[int]time.Time) {
	w.watermark = wm
	w.maxObserved = maxTS
	now := w.now()
	for p, ts := range parts {
		w.parts[p] = &partitionWM{
			registered:  true,
			hasData:     true,
			maxObserved: ts,
			lastActive:  now,
		}
	}
}

func (w *WatermarkTracker) Watermark() time.Time {
	return w.watermark
}

func (w *WatermarkTracker) MaxTimestamp() time.Time {
	return w.maxObserved
}

// PartitionMaxEventTimes 返回各分区已观测最大事件时间的拷贝（写入断点用）。
func (w *WatermarkTracker) PartitionMaxEventTimes() map[int]time.Time {
	out := make(map[int]time.Time, len(w.parts))
	for p, pw := range w.parts {
		if pw.hasData {
			out[p] = pw.maxObserved
		}
	}
	return out
}

// PartitionStates 返回各分区水位状态快照（按分区号排序），并标记
// 当前拖住整体水位线的分区（可能多个并列），用于日志区分快/慢/空闲。
func (w *WatermarkTracker) PartitionStates() []PartitionWMState {
	out := make([]PartitionWMState, 0, len(w.parts))
	for p, pw := range w.parts {
		st := PartitionWMState{
			Partition:    p,
			HasData:      pw.hasData,
			MaxEventTime: pw.maxObserved,
			Idle:         pw.idle,
		}
		if pw.hasData {
			st.Contribution = pw.maxObserved.Add(-w.maxOutOfOrderness)
		}
		out = append(out, st)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Partition < out[j].Partition })

	// 找活跃分区中的最小贡献；无数据的活跃分区贡献视为 -∞（最优先拖住）。
	holding := -1
	for i, st := range out {
		pw := w.parts[st.Partition]
		if !pw.registered || st.Idle {
			continue
		}
		if holding == -1 {
			holding = i
			continue
		}
		hs := out[holding]
		switch {
		case !hs.HasData:
			// 现任持有者是"无数据"（-∞），不可超越。
		case !st.HasData:
			holding = i
		case st.Contribution.Before(hs.Contribution):
			holding = i
		}
	}
	if holding >= 0 {
		out[holding].HoldsWatermark = true
	}
	return out
}
