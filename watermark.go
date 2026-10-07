package streamengine

import (
	"time"
)

// PartitionState 表示一个分区在全局水位线推进中的参与状态。
type PartitionState int

const (
	// PartitionWaiting 分区已注册但还没观测到任何事件；
	// 它代表一个"可能随时来数据"的分区，暂时参与全局水位线（贡献为时间零点，即拖住）。
	PartitionWaiting PartitionState = iota
	// PartitionActive 活跃分区：其分区水位线参与全局水位线取最小。
	PartitionActive
	// PartitionIdle 空闲分区：超过 idleTimeout 没有任何数据，
	// 不再参与全局水位线，避免长期无数据的分区把窗口永远拖住。
	PartitionIdle
)

// String 返回状态可读名，用于日志与断点 JSON。
func (s PartitionState) String() string {
	switch s {
	case PartitionWaiting:
		return "WAITING"
	case PartitionActive:
		return "ACTIVE"
	case PartitionIdle:
		return "IDLE"
	default:
		return "UNKNOWN"
	}
}

// MarshalText 让状态以可读文本进入断点 JSON。
func (s PartitionState) MarshalText() ([]byte, error) {
	return []byte(s.String()), nil
}

// UnmarshalText 从断点 JSON 恢复状态。
func (s *PartitionState) UnmarshalText(data []byte) error {
	switch string(data) {
	case "WAITING":
		*s = PartitionWaiting
	case "ACTIVE":
		*s = PartitionActive
	case "IDLE":
		*s = PartitionIdle
	default:
		*s = PartitionWaiting
	}
	return nil
}

// PartitionWM 是单个分区的水位线状态。
type PartitionWM struct {
	State         PartitionState `json:"state"`
	MaxObserved   time.Time      `json:"maxObserved"`
	Watermark     time.Time      `json:"watermark"` // 分区水位线 = MaxObserved - maxOutOfOrderness
	RegisteredAt  time.Time      `json:"registeredAt"`
	LastEventAt   time.Time      `json:"lastEventAt"`   // 最近一条事件的"事件时间"（观测用）
	LastProcessAt time.Time      `json:"lastProcessAt"` // 最近处理时刻的"墙钟"（空闲判定用）
	IdleAt        time.Time      `json:"idleAt"`        // 转为空闲的墙钟时刻，零值表示从未空闲
}

// PartitionStatus 是对外可观测的分区水位线状态（日志/测试用）。
type PartitionStatus struct {
	Partition   int
	State       PartitionState
	Watermark   time.Time
	MaxEvent    time.Time
	LastEvent   time.Time
	IdleAt      time.Time
	Contributes bool // 当前是否参与全局水位线
}

// PartitionTransition 记录一次分区参与状态的变化（Tick 时产生，便于解释原因）。
type PartitionTransition struct {
	Partition int
	From      PartitionState
	To        PartitionState
	At        time.Time
}

// WatermarkManager 管理多分区水位线。
//
// 推进规则：
//   - 每个分区各自维护 partitionWatermark = 该分区最大事件时间 - maxOutOfOrderness；
//   - 全局水位线 = 所有 ACTIVE 分区水位线的最小值；
//     WAITING（已注册、尚无事件、未超过空闲超时）的分区贡献时间零点，即继续拖住；
//     IDLE（超过 idleTimeout 无数据）的分区不参与，不再拖住全局水位线；
//   - 所有分区都 IDLE（或没有分区）时，全局水位线保持不变（数据完全停止时不凭空推进）；
//   - 全局水位线单调不减：空闲分区恢复后即使带来更旧的事件，也不会把水位线拉回去，
//     该事件按既有迟到规则判定。
//
// 该结构本身不加锁，由 Processor 在自己的互斥锁内使用。
type WatermarkManager struct {
	maxOutOfOrderness time.Duration
	idleTimeout       time.Duration
	now               func() time.Time
	parts             map[int]*PartitionWM
	watermark         time.Time
}

func NewWatermarkManager(maxOutOfOrderness, idleTimeout time.Duration) *WatermarkManager {
	return newWatermarkManagerWithClock(maxOutOfOrderness, idleTimeout, func() time.Time {
		return time.Now().UTC()
	})
}

// newWatermarkManagerWithClock 注入墙钟来源，测试可用可控时钟验证空闲判定。
func newWatermarkManagerWithClock(maxOutOfOrderness, idleTimeout time.Duration, now func() time.Time) *WatermarkManager {
	if maxOutOfOrderness < 0 || idleTimeout < 0 {
		panic("watermark durations must be non-negative")
	}
	return &WatermarkManager{
		maxOutOfOrderness: maxOutOfOrderness,
		idleTimeout:       idleTimeout,
		now:               now,
		parts:             make(map[int]*PartitionWM),
	}
}

// Register 注册一个分区。已存在的分区不受影响（重复注册幂等）。
func (m *WatermarkManager) Register(partition int) {
	if _, ok := m.parts[partition]; ok {
		return
	}
	m.parts[partition] = &PartitionWM{State: PartitionWaiting, RegisteredAt: m.now()}
}

// RegisterAll 批量注册。
func (m *WatermarkManager) RegisterAll(partitions []int) {
	for _, p := range partitions {
		m.Register(p)
	}
}

func (m *WatermarkManager) partWM(p *PartitionWM) time.Time {
	return p.MaxObserved.Add(-m.maxOutOfOrderness)
}

// Observe 观测某分区上的一个事件时间，返回推进后的全局水位线。
// 首个事件使 WAITING -> ACTIVE；IDLE 分区恢复时 -> ACTIVE（但全局水位线保持单调）。
func (m *WatermarkManager) Observe(partition int, ts time.Time) time.Time {
	now := m.now()
	p, ok := m.parts[partition]
	if !ok {
		p = &PartitionWM{State: PartitionWaiting, RegisteredAt: now}
		m.parts[partition] = p
	}
	if ts.After(p.MaxObserved) {
		p.MaxObserved = ts
		p.Watermark = m.partWM(p)
	}
	p.LastEventAt = ts
	p.LastProcessAt = now
	p.IdleAt = time.Time{} // 有新数据到达：若此前空闲则复活，清空空闲时刻
	p.State = PartitionActive
	return m.advance(now)
}

// candidate 计算当前各参与分区给出的全局水位线上界。
// found=false 表示没有任何分区参与（全 IDLE 或无分区），此时水位线应保持不变。
//
// 是否空闲的权威判定只在 Tick 中依据墙钟翻转状态；这里严格按当前状态计算：
// WAITING 一律以时间零点拖住（它可能随时来数据），ACTIVE 贡献其分区水位线，
// IDLE 不参与。空闲超时的语义由 Runner 周期性 Tick 保证及时翻转。
func (m *WatermarkManager) candidate(now time.Time) (time.Time, bool) {
	_ = now
	var minWM time.Time
	found := false
	for _, p := range m.parts {
		if p.State == PartitionIdle {
			continue
		}
		if p.State == PartitionWaiting {
			// 尚未收到第一个事件的分区：仍可能随时来数据，以时间零点拖住。
			return time.Time{}, true
		}
		if !found || p.Watermark.Before(minWM) {
			minWM = p.Watermark
			found = true
		}
	}
	return minWM, found
}

// advance 重算并单调推进全局水位线。
func (m *WatermarkManager) advance(now time.Time) time.Time {
	cand, found := m.candidate(now)
	if found && cand.After(m.watermark) {
		m.watermark = cand
	}
	return m.watermark
}

// Tick 依据墙钟把超时未活动的 WAITING/ACTIVE 分区标记为 IDLE，并重算全局水位线。
// 返回本次发生的状态变化（ACTIVE/WAITING -> IDLE），供日志解释"为什么水位线能动"。
func (m *WatermarkManager) Tick() (time.Time, []PartitionTransition) {
	now := m.now()
	if m.idleTimeout <= 0 {
		return m.watermark, nil
	}
	var tr []PartitionTransition
	for id, p := range m.parts {
		if p.State == PartitionIdle {
			continue
		}
		var lastActivity time.Time
		if p.State == PartitionActive {
			// 空闲按"墙钟多久没处理到数据"衡量，与事件时间本身是否偏斜无关；
			// 极端情况下 LastProcessAt 缺失（旧断点），退回用注册时刻。
			lastActivity = p.LastProcessAt
			if lastActivity.IsZero() {
				lastActivity = p.RegisteredAt
			}
		} else {
			lastActivity = p.RegisteredAt
		}
		if !lastActivity.After(now.Add(-m.idleTimeout)) {
			tr = append(tr, PartitionTransition{Partition: id, From: p.State, To: PartitionIdle, At: now})
			p.State = PartitionIdle
			p.IdleAt = now
		}
	}
	return m.advance(now), tr
}

// Watermark 返回当前全局水位线。
func (m *WatermarkManager) Watermark() time.Time {
	return m.watermark
}

// MaxTimestamp 返回所有分区观测到的最大事件时间（恢复后重建乱序上界用）。
func (m *WatermarkManager) MaxTimestamp() time.Time {
	var maxTS time.Time
	for _, p := range m.parts {
		if p.MaxObserved.After(maxTS) {
			maxTS = p.MaxObserved
		}
	}
	return maxTS
}

// IdleTimeout 返回空闲超时配置（0 表示关闭空闲检测）。
func (m *WatermarkManager) IdleTimeout() time.Duration {
	return m.idleTimeout
}

// Statuses 返回各分区水位线状态的可观测快照，按分区号排序。
func (m *WatermarkManager) Statuses() []PartitionStatus {
	ids := make([]int, 0, len(m.parts))
	for id := range m.parts {
		ids = append(ids, id)
	}
	// 避免在本文件引入 sort：用简单插入排序（分区数通常很少）。
	for i := 1; i < len(ids); i++ {
		for j := i; j > 0 && ids[j-1] > ids[j]; j-- {
			ids[j-1], ids[j] = ids[j], ids[j-1]
		}
	}
	out := make([]PartitionStatus, 0, len(ids))
	now := m.now()
	for _, id := range ids {
		p := m.parts[id]
		contrib := p.State == PartitionActive
		if p.State == PartitionWaiting {
			contrib = p.RegisteredAt.After(now.Add(-m.idleTimeout)) || m.idleTimeout <= 0
		}
		out = append(out, PartitionStatus{
			Partition:   id,
			State:       p.State,
			Watermark:   p.Watermark,
			MaxEvent:    p.MaxObserved,
			LastEvent:   p.LastEventAt,
			IdleAt:      p.IdleAt,
			Contributes: contrib,
		})
	}
	return out
}

// SnapshotPartitions 导出各分区水位线状态（断点用）。
func (m *WatermarkManager) SnapshotPartitions() map[int]*PartitionWM {
	out := make(map[int]*PartitionWM, len(m.parts))
	for id, p := range m.parts {
		cp := *p
		out[id] = &cp
	}
	return out
}

// RestorePartitions 从断点恢复各分区水位线状态。
func (m *WatermarkManager) RestorePartitions(parts map[int]*PartitionWM, wm time.Time) {
	m.parts = make(map[int]*PartitionWM, len(parts))
	for id, p := range parts {
		cp := *p
		m.parts[id] = &cp
	}
	m.watermark = wm
}
