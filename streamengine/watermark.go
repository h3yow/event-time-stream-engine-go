package streamengine

import (
	"math"
	"sort"
	"sync"
)

// MinWatermark 表示尚无有效观测时的水位线（此时不触发任何窗口）。
const MinWatermark = math.MinInt64

// WatermarkGenerator 跟踪各已注册分区观测到的最大事件时间。
//
// 水位线规则（明确的乱序容忍范围）：
//
//	watermark = min(每个"活跃"分区的最大事件时间) - maxOutOfOrderness
//
// 其中"活跃分区"= 已通过 Register 注册、且尚未 Finish 的分区：
//   - 已注册但还没产生任何数据的分区，以 MinWatermark 参与最小值，
//     即它会拖住全局水位线——慢分区/暂未到达的分区上的数据因此不会
//     仅仅因为别的分区跑得快就被误判为迟到；
//   - 分区关闭（有界源读完）后调用 Finish 退出计算；
//   - 全部活跃分区都退出后，不再有新数据，水位线保持 MinWatermark，
//     由引擎在收尾时冲发剩余窗口。
//
// 水位线单调不减：较早的事件不会让水位线倒退。生成器并发安全。
type WatermarkGenerator struct {
	mu                sync.Mutex
	maxOutOfOrderness int64
	registered        map[int]bool // 已注册（参与计算）
	finished          map[int]bool // 已关闭退出
	partitionMax      map[int]int64
	lastWM            int64 // 最近一次有效水位线（所有分区退出后保持，单调不减）
}

func NewWatermarkGenerator(maxOutOfOrderness int64) *WatermarkGenerator {
	return &WatermarkGenerator{
		maxOutOfOrderness: maxOutOfOrderness,
		registered:        make(map[int]bool),
		finished:          make(map[int]bool),
		partitionMax:      make(map[int]int64),
		lastWM:            MinWatermark,
	}
}

// Register 注册参与水位线计算的分区集合（启动时调用一次）。
func (w *WatermarkGenerator) Register(partitions []int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, p := range partitions {
		w.registered[p] = true
	}
}

// Observe 记录某分区一条事件的事件时间，返回推进后的当前水位线。
func (w *WatermarkGenerator) Observe(partition int, eventTime int64) int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	if cur, ok := w.partitionMax[partition]; !ok || eventTime > cur {
		w.partitionMax[partition] = eventTime
	}
	return w.currentLocked()
}

// Finish 标记分区已关闭（不会再有数据），退出水位线最小值计算，
// 返回重新计算后的水位线。
func (w *WatermarkGenerator) Finish(partition int) int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.finished[partition] = true
	return w.currentLocked()
}

// Current 返回当前水位线。
func (w *WatermarkGenerator) Current() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.currentLocked()
}

// currentLocked 计算 min（活跃分区最大事件时间）- 容忍量。
func (w *WatermarkGenerator) currentLocked() int64 {
	min := int64(math.MaxInt64)
	active := 0
	for p := range w.registered {
		if w.finished[p] {
			continue
		}
		active++
		m, observed := w.partitionMax[p]
		if !observed {
			// 慢/空闲分区拖住全局水位线，维持在最近有效值。
			return w.lastWM
		}
		if m < min {
			min = m
		}
	}
	if active == 0 {
		return w.lastWM // 所有分区已退出：保持最后水位线，单调不减
	}
	cand := min - w.maxOutOfOrderness
	if cand > w.lastWM {
		w.lastWM = cand
	}
	return w.lastWM
}

// PartitionMax 返回各分区观测最大值的拷贝（快照/日志用）。
func (w *WatermarkGenerator) PartitionMax() map[int]int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.snapshotMaxLocked()
}

func (w *WatermarkGenerator) snapshot() map[int]int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.snapshotMaxLocked()
}

func (w *WatermarkGenerator) snapshotMaxLocked() map[int]int64 {
	out := make(map[int]int64, len(w.partitionMax))
	for k, v := range w.partitionMax {
		out[k] = v
	}
	return out
}

func (w *WatermarkGenerator) registeredList() []int {
	out := make([]int, 0, len(w.registered))
	for p := range w.registered {
		out = append(out, p)
	}
	sort.Ints(out)
	return out
}

func (w *WatermarkGenerator) finishedList() []int {
	out := make([]int, 0, len(w.finished))
	for p := range w.finished {
		out = append(out, p)
	}
	sort.Ints(out)
	return out
}

// Restore 从检查点恢复注册集合、已关闭集合、各分区观测最大值与最近水位线。
func (w *WatermarkGenerator) Restore(registered, finished []int, partitionMax map[int]int64, lastWM int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.registered = make(map[int]bool, len(registered))
	w.finished = make(map[int]bool, len(finished))
	w.partitionMax = make(map[int]int64, len(partitionMax))
	for _, p := range registered {
		w.registered[p] = true
	}
	for _, p := range finished {
		w.finished[p] = true
	}
	for k, v := range partitionMax {
		w.partitionMax[k] = v
	}
	w.lastWM = lastWM
}

// describeWatermark 生成可读的水位线描述（测试日志用）。
func describeWatermark(wm int64, partitionMax map[int]int64) string {
	parts := make([]int, 0, len(partitionMax))
	for p := range partitionMax {
		parts = append(parts, p)
	}
	sort.Ints(parts)
	s := "watermark="
	if wm == MinWatermark {
		s += "-inf"
	} else {
		s += itoa(wm)
	}
	s += " partitionMax={"
	for i, p := range parts {
		if i > 0 {
			s += ","
		}
		s += "p" + itoa(int64(p)) + ":" + itoa(partitionMax[p])
	}
	s += "}"
	return s
}
