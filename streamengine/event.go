package streamengine

import "fmt"

// Event 是来自数据源的一条事件。
type Event struct {
	Key       string // 分组键
	EventTime int64  // 事件时间（毫秒）
	Value     int64  // 待聚合的值
}

// Envelope 是分区日志中的一条消息，携带其在分区内的物理位置。
type Envelope struct {
	Partition int
	Offset    int64
	Event     Event
}

func (e Envelope) String() string {
	return fmt.Sprintf("(p%d@%d key=%s t=%d v=%d)",
		e.Partition, e.Offset, e.Event.Key, e.Event.EventTime, e.Event.Value)
}

// RecordKind 标记输出结果的种类，使迟到数据造成的结果变化可解释。
type RecordKind int

const (
	// RecordInitial 窗口首次输出。
	RecordInitial RecordKind = iota
	// RecordCorrected 容忍范围内迟到事件触发的修正输出（同一窗口的新版本）。
	RecordCorrected
)

func (k RecordKind) String() string {
	switch k {
	case RecordInitial:
		return "INITIAL"
	case RecordCorrected:
		return "CORRECTED"
	default:
		return "UNKNOWN"
	}
}

// WindowResult 是一个 keyed 窗口的聚合输出（sum/count）。
// 同一窗口可能输出多次：INITIAL 之后因容忍范围内迟到事件再输出 CORRECTED，
// Version 单调递增，下游可据此区分“新结果”与“修正结果”。
type WindowResult struct {
	Key     string
	Window  Window
	Kind    RecordKind
	Sum     int64
	Count   int64
	Version int
}

func (r WindowResult) String() string {
	return fmt.Sprintf("%s key=%s window=[%d,%d) sum=%d count=%d v%d",
		r.Kind, r.Key, r.Window.Start, r.Window.End, r.Sum, r.Count, r.Version)
}

// LateRejection 记录一条超过容忍范围、被明确拒绝的迟到事件（侧输出）。
// 它既不静默丢弃，也不计入窗口；Watermark/Deadline 给出可核对的拒绝理由。
type LateRejection struct {
	Envelope  Envelope
	Watermark int64 // 拒绝时的水位线
	Deadline  int64 // 该事件所属窗口可被接受的最后水位线（windowEnd + allowedLateness）
}

func (l LateRejection) String() string {
	return fmt.Sprintf("LATE-REJECTED %s watermark=%d deadline=%d",
		l.Envelope, l.Watermark, l.Deadline)
}
