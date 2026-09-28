package streamengine

import "fmt"

// FailureReason 以可区分的方式标记各类失败原因，调用方可以按原因分支处理，
// 而不是只能得到一个模糊的错误。
type FailureReason int

const (
	// ReasonNone 无失败。
	ReasonNone FailureReason = iota
	// ReasonLateRejected 事件晚于水位线且超出乱序容忍（含允许迟到）范围，
	// 不能被悄悄丢掉：它会进入侧输出 LateEvents，并给出明确拒绝结果。
	ReasonLateRejected
	// ReasonDuplicate 重复事件（同一分区同一偏移被再次交付），
	// 去重后跳过，不会重复计数。
	ReasonDuplicate
	// ReasonCheckpointRollback 试图从早于已提交检查点的位置启动，
	// 即“断点回退”，必须明确拒绝，而不是从中间重新计算。
	ReasonCheckpointRollback
	// ReasonBackpressure 摄入背压：结果通道或等待队列已满，
	// 调用方必须稍后重试；数据保留在源中不前进，绝不悄悄丢弃。
	ReasonBackpressure
	// ReasonSourceGone 数据源分区已关闭且偏移越界（正常结束），
	// 或在已关闭分区上追加。
	ReasonSourceGone
	// ReasonClosed 引擎已关闭，拒绝新操作。
	ReasonClosed
)

func (r FailureReason) String() string {
	switch r {
	case ReasonNone:
		return "none"
	case ReasonLateRejected:
		return "late-rejected"
	case ReasonDuplicate:
		return "duplicate"
	case ReasonCheckpointRollback:
		return "checkpoint-rollback"
	case ReasonBackpressure:
		return "backpressure"
	case ReasonSourceGone:
		return "source-gone"
	case ReasonClosed:
		return "closed"
	default:
		return fmt.Sprintf("unknown(%d)", int(r))
	}
}

// EngineError 携带可区分失败原因的错误。
type EngineError struct {
	Reason FailureReason
	Msg    string
}

func (e *EngineError) Error() string {
	return fmt.Sprintf("stream-engine[%s]: %s", e.Reason, e.Msg)
}

func newError(reason FailureReason, msg string) *EngineError {
	return &EngineError{Reason: reason, Msg: msg}
}
