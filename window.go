package streamengine

import "time"

// Window 表示一个左闭右开 [Start, End) 的事件时间窗口。
type Window struct {
	Start time.Time
	End   time.Time
}

// Contains 判断事件时间是否落在窗口内。
func (w Window) Contains(ts time.Time) bool {
	return !ts.Before(w.Start) && ts.Before(w.End)
}

// TumblingAssigner 按固定大小分配滚动窗口，窗口边界对齐到 Unix 纪元。
type TumblingAssigner struct {
	size time.Duration
}

func NewTumblingAssigner(size time.Duration) *TumblingAssigner {
	if size <= 0 {
		panic("window size must be positive")
	}
	return &TumblingAssigner{size: size}
}

// Assign 返回 ts 所属的滚动窗口。
func (a *TumblingAssigner) Assign(ts time.Time) Window {
	nano := ts.UnixNano()
	sizeNs := int64(a.size)
	startNano := nano - ((nano%sizeNs)+sizeNs)%sizeNs // 兼容负值时间
	// 统一归一化到 UTC，避免 Location 差异导致 time.Time 作为 map key 时失配。
	start := time.Unix(0, startNano).UTC()
	return Window{Start: start, End: start.Add(a.size)}
}

// Size 返回窗口大小。
func (a *TumblingAssigner) Size() time.Duration {
	return a.size
}

// WindowKey 定位一个带键窗口。
type WindowKey struct {
	Key   string
	Start time.Time
}

// Aggregate 是单个 (Key, Window) 的聚合状态。
// Fired 表示已发出最终结果（窗口结束且越过水位线）；
// 之后在 allowedLateness 内到达的迟到事件会更新聚合并使 Revision++，
// 由处理器发出带修订号的结果，而不会重复第一次的最终结果。
type Aggregate struct {
	Key         string
	Window      Window
	Count       int64
	Sum         int64
	Revision    int64
	Fired       bool
	LastEventAt time.Time
}

// apply 将一个事件计入聚合。
func (ag *Aggregate) apply(e Event) {
	ag.Count++
	ag.Sum += e.Value
	if e.EventTime.After(ag.LastEventAt) {
		ag.LastEventAt = e.EventTime
	}
}

// clone 深拷贝，用于快照。
func (ag *Aggregate) clone() *Aggregate {
	cp := *ag
	return &cp
}
