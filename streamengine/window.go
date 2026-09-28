package streamengine

import (
	"sort"
	"strconv"
)

// Window 表示一个左闭右开 [Start, End) 的事件时间固定窗口。
type Window struct {
	Start int64
	End   int64
}

func (w Window) String() string {
	return "[" + strconv.FormatInt(w.Start, 10) + "," + strconv.FormatInt(w.End, 10) + ")"
}

// RecordRef 唯一标识一条已处理记录（分区 + 偏移），用于去重；
// Value 随快照持久化，使多个消费者的检查点合并后可由记录并集重算聚合。
type RecordRef struct {
	Partition int   `json:"partition"`
	Offset    int64 `json:"offset"`
	Value     int64 `json:"value"`
}

type recordRef struct {
	partition int
	offset    int64
}

// windowState 是单个 keyed 窗口的聚合状态。
type windowState struct {
	key     string
	window  Window
	sum     int64
	count   int64
	fired   bool
	version int
	// 记录引用 -> 该记录的 value，去重与检查点合并共用。
	records map[recordRef]int64
}

// addOutcome 区分一条事件进入窗口时的结果。
type addOutcome int

const (
	outcomeAccepted  addOutcome = iota // 新事件被计入
	outcomeDuplicate                   // 重复记录，跳过
	outcomeRejected                    // 超过容忍范围的迟到事件，拒绝
)

// WindowStore 管理所有 keyed 窗口状态。
//
// 迟到规则（与水位线配合）：
//   - 事件按事件时间落入窗口 [start,end)；
//   - 水位线 >= end 时窗口首次触发（INITIAL, v1）；
//   - 窗口触发后，只要 watermark <= end+allowedLateness，仍接受迟到事件，
//     并立即产生一条 CORRECTED 修正结果（v2,v3,...），可解释、不重复计数；
//   - 一旦 watermark > end+allowedLateness，该窗口状态被清理，之后到达的
//     同窗口事件以 ReasonLateRejected 明确拒绝并进入侧输出。
type WindowStore struct {
	size            int64
	allowedLateness int64
	states          map[string]*windowState
}

// NewWindowStore 创建窗口存储器：固定窗口大小 + 允许迟到容忍范围（毫秒）。
func NewWindowStore(windowSize, allowedLateness int64) *WindowStore {
	return &WindowStore{
		size:            windowSize,
		allowedLateness: allowedLateness,
		states:          make(map[string]*windowState),
	}
}

func stateKey(key string, start int64) string {
	return key + "|" + strconv.FormatInt(start, 10)
}

// Deadline 返回某窗口还能接受迟到事件的最后水位线（end + allowedLateness）。
func (s *WindowStore) Deadline(w Window) int64 { return w.End + s.allowedLateness }

// Add 尝试把一条事件计入其所属窗口。
// 返回处理结果；若事件属于已触发窗口且被接受，correction 非空（修正输出）。
// deadline 返回该窗口的容忍上限，便于侧输出给出可核对的拒绝理由。
func (s *WindowStore) Add(e Envelope, watermark int64) (outcome addOutcome, correction *WindowResult, deadline int64) {
	w := AssignWindow(e.Event.EventTime, s.size)
	deadline = s.Deadline(w)

	// 水位线已越过容忍上限：明确拒绝，不计入、不静默丢弃。
	if watermark != MinWatermark && watermark > deadline {
		return outcomeRejected, nil, deadline
	}

	st := s.states[stateKey(e.Event.Key, w.Start)]
	if st == nil {
		st = &windowState{
			key:     e.Event.Key,
			window:  w,
			records: make(map[recordRef]int64),
		}
		s.states[stateKey(e.Event.Key, w.Start)] = st
	}

	ref := recordRef{partition: e.Partition, offset: e.Offset}
	if _, dup := st.records[ref]; dup {
		return outcomeDuplicate, nil, deadline
	}
	st.records[ref] = e.Event.Value
	st.sum += e.Event.Value
	st.count++

	if st.fired {
		// 容忍范围内迟到：立即产生修正版本，版本号单调递增。
		st.version++
		correction = &WindowResult{
			Key:     st.key,
			Window:  st.window,
			Kind:    RecordCorrected,
			Sum:     st.sum,
			Count:   st.count,
			Version: st.version,
		}
	}
	return outcomeAccepted, correction, deadline
}

// Advance 处理水位线推进：触发所有 watermark>=窗口end 的窗口（首次输出），
// 并清理 watermark>deadline 的窗口状态。返回新触发的窗口结果。
func (s *WindowStore) Advance(watermark int64) []WindowResult {
	if watermark == MinWatermark {
		return nil
	}
	var fired []WindowResult
	var expired []string
	for k, st := range s.states {
		if !st.fired && watermark >= st.window.End {
			st.fired = true
			st.version = 1
			fired = append(fired, WindowResult{
				Key:     st.key,
				Window:  st.window,
				Kind:    RecordInitial,
				Sum:     st.sum,
				Count:   st.count,
				Version: 1,
			})
		}
		if watermark > s.Deadline(st.window) {
			expired = append(expired, k)
		}
	}
	for _, k := range expired {
		delete(s.states, k)
	}
	// 让输出顺序稳定，便于断言与解释。
	sort.Slice(fired, func(i, j int) bool {
		if fired[i].Key != fired[j].Key {
			return fired[i].Key < fired[j].Key
		}
		return fired[i].Window.Start < fired[j].Window.Start
	})
	return fired
}

// WindowSnapshot 是单个窗口的可持久化状态快照。
type WindowSnapshot struct {
	Key     string      `json:"key"`
	Start   int64       `json:"start"`
	End     int64       `json:"end"`
	Sum     int64       `json:"sum"`
	Count   int64       `json:"count"`
	Fired   bool        `json:"fired"`
	Version int         `json:"version"`
	Records []RecordRef `json:"records"`
}

// Snapshot 导出全部窗口状态。
func (s *WindowStore) Snapshot() []WindowSnapshot {
	snaps := make([]WindowSnapshot, 0, len(s.states))
	for _, st := range s.states {
		refs := make([]RecordRef, 0, len(st.records))
		for ref, v := range st.records {
			refs = append(refs, RecordRef{
				Partition: ref.partition, Offset: ref.offset, Value: v,
			})
		}
		sort.Slice(refs, func(i, j int) bool {
			if refs[i].Partition != refs[j].Partition {
				return refs[i].Partition < refs[j].Partition
			}
			return refs[i].Offset < refs[j].Offset
		})
		snaps = append(snaps, WindowSnapshot{
			Key:     st.key,
			Start:   st.window.Start,
			End:     st.window.End,
			Sum:     st.sum,
			Count:   st.count,
			Fired:   st.fired,
			Version: st.version,
			Records: refs,
		})
	}
	sort.Slice(snaps, func(i, j int) bool {
		if snaps[i].Key != snaps[j].Key {
			return snaps[i].Key < snaps[j].Key
		}
		return snaps[i].Start < snaps[j].Start
	})
	return snaps
}

// Restore 从快照重建窗口状态（包括每条已处理记录的去重集合）。
func (s *WindowStore) Restore(snaps []WindowSnapshot) {
	s.states = make(map[string]*windowState, len(snaps))
	for _, sn := range snaps {
		records := make(map[recordRef]int64, len(sn.Records))
		for _, ref := range sn.Records {
			records[recordRef{partition: ref.Partition, offset: ref.Offset}] = ref.Value
		}
		st := &windowState{
			key:     sn.Key,
			window:  Window{Start: sn.Start, End: sn.End},
			sum:     sn.Sum,
			count:   sn.Count,
			fired:   sn.Fired,
			version: sn.Version,
			records: records,
		}
		s.states[stateKey(sn.Key, sn.Start)] = st
	}
}

// WindowCount 返回当前保留的窗口数（测试/日志用）。
func (s *WindowStore) WindowCount() int { return len(s.states) }

// AssignWindow 按事件时间分配固定窗口，对负时间也成立（floor 语义）。
func AssignWindow(eventTime, windowSize int64) Window {
	if windowSize <= 0 {
		windowSize = 1
	}
	mod := eventTime % windowSize
	if mod < 0 {
		mod += windowSize
	}
	start := eventTime - mod
	return Window{Start: start, End: start + windowSize}
}
