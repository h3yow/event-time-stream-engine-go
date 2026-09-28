package streamengine

import "errors"

// Disposition 明确区分每条记录的处理结论，禁止"悄悄丢掉/重复计数"。
type Disposition int

const (
	// DispositionUnknown 空实现占位。
	DispositionUnknown Disposition = iota
	// DispositionOnTime 准时事件，已计入聚合。
	DispositionOnTime
	// DispositionLateAccepted 迟到但仍在允许迟到范围内，已计入并产生修订结果。
	DispositionLateAccepted
	// DispositionLateSideOutput 超出容忍范围的迟到事件，转入侧输出，不计入聚合。
	DispositionLateSideOutput
	// DispositionDuplicate 重复消费（分区+位点已处理），幂等跳过。
	DispositionDuplicate
)

// 可区分的失败原因，调用方用 errors.Is 精确判断。
var (
	// ErrBackpressure 本地缓冲已满，非阻塞生产被拒绝（不丢数据，可重试）。
	ErrBackpressure = errors.New("backpressure: partition buffer full")
	// ErrCheckpointRollback 试图用更旧的断点覆盖/恢复，被明确拒绝。
	ErrCheckpointRollback = errors.New("checkpoint rollback rejected: newer checkpoint exists")
	// ErrOffsetRollback 断点中的位点比当前已提交位点更旧。
	ErrOffsetRollback = errors.New("offset rollback rejected")
	// ErrPartitionNotAssigned 消费者处理了不属于自己当前世代的分区。
	ErrPartitionNotAssigned = errors.New("partition not assigned in current generation")
	// ErrStaleGeneration 再均衡后旧世代 worker 继续提交，被拒绝。
	ErrStaleGeneration = errors.New("stale consumer generation")
	// ErrLateBeyondTolerance 迟到事件超出允许迟到范围（记录于侧输出，仅作原因标记）。
	ErrLateBeyondTolerance = errors.New("late event beyond allowed lateness")
)

// String 返回处理结论的可读名称，用于日志区分。
func (d Disposition) String() string {
	switch d {
	case DispositionOnTime:
		return "ON_TIME"
	case DispositionLateAccepted:
		return "LATE_ACCEPTED"
	case DispositionLateSideOutput:
		return "LATE_SIDE_OUTPUT"
	case DispositionDuplicate:
		return "DUPLICATE"
	default:
		return "UNKNOWN"
	}
}
