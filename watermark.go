package streamengine

import "time"

// WatermarkTracker 跟踪观测到的最大事件时间，并按固定乱序容忍度推进水位线。
//
// 水位线语义：watermark(t) = maxObservedEventTime - maxOutOfOrderness。
// 水位线断言"事件时间 <= watermark 的正常事件基本都已到达"；
// 水位线单调不减，任何更晚到达但事件时间仍 > watermark 的事件都算准时。
type WatermarkTracker struct {
	maxOutOfOrderness time.Duration
	maxObserved       time.Time
	watermark         time.Time
}

func NewWatermarkTracker(maxOutOfOrderness time.Duration) *WatermarkTracker {
	return &WatermarkTracker{maxOutOfOrderness: maxOutOfOrderness}
}

// Observe 观测一个事件时间，返回推进后的水位线。水位线单调不减。
func (w *WatermarkTracker) Observe(ts time.Time) time.Time {
	if ts.After(w.maxObserved) {
		w.maxObserved = ts
	}
	candidate := w.maxObserved.Add(-w.maxOutOfOrderness)
	if candidate.After(w.watermark) {
		w.watermark = candidate
	}
	return w.watermark
}

// advanceTo 从断点恢复水位线。
func (w *WatermarkTracker) advanceTo(wm, maxTs time.Time) {
	w.watermark = wm
	w.maxObserved = maxTs
}

func (w *WatermarkTracker) Watermark() time.Time {
	return w.watermark
}

func (w *WatermarkTracker) MaxTimestamp() time.Time {
	return w.maxObserved
}
