package streamengine

import (
	"context"
	"sync"
)

// PartitionLog 用本地内存消息缓冲模拟一个分区日志（类似 Kafka 分区）。
//
// 语义：
//   - 偏移从 0 开始单调递增，事件一旦写入不会消失；
//   - ReadAt 可按任意偏移重复读取，从而模拟“重复消费”；
//   - 偏移尚未写入时 ReadAt 阻塞等待（这就是背压：消费者不前进，
//     数据留在日志里，绝不悄悄丢弃），直到新事件 Append、分区 Close 或 ctx 取消。
type PartitionLog struct {
	mu     sync.Mutex
	cond   *sync.Cond
	id     int
	name   string
	events []Event
	closed bool
}

func NewPartitionLog(id int, name string, events []Event) *PartitionLog {
	p := &PartitionLog{
		id:     id,
		name:   name,
		events: append([]Event(nil), events...),
	}
	p.cond = sync.NewCond(&p.mu)
	return p
}

// ID 返回分区编号。
func (p *PartitionLog) ID() int { return p.id }

// Name 返回分区名。
func (p *PartitionLog) Name() string { return p.name }

// Length 返回当前已写入事件数（下一条偏移）。
func (p *PartitionLog) Length() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return int64(len(p.events))
}

// Append 向分区追加事件并返回其偏移。已关闭分区上追加以 SourceGone 拒绝。
func (p *PartitionLog) Append(e Event) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return 0, newError(ReasonSourceGone, "partition "+p.name+" already closed")
	}
	p.events = append(p.events, e)
	off := int64(len(p.events) - 1)
	p.cond.Broadcast()
	return off, nil
}

// ReadAt 读取指定偏移的事件。
//
//   - offset 越界但分区未关闭：阻塞等待（背压，不丢数据）；
//   - offset 越界且分区已关闭：返回 ReasonSourceGone（正常读完的结束信号）；
//   - ctx 取消：返回 ctx.Err()。
func (p *PartitionLog) ReadAt(ctx context.Context, offset int64) (Envelope, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// ctx 取消时唤醒等待者。
	stop := context.AfterFunc(ctx, func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		p.cond.Broadcast()
	})
	defer stop()

	for offset >= int64(len(p.events)) {
		if p.closed {
			return Envelope{}, newError(ReasonSourceGone,
				"partition "+p.name+" closed at offset "+itoa(offset))
		}
		if err := ctx.Err(); err != nil {
			return Envelope{}, err
		}
		p.cond.Wait()
	}
	if err := ctx.Err(); err != nil {
		return Envelope{}, err
	}
	return Envelope{
		Partition: p.id,
		Offset:    offset,
		Event:     p.events[offset],
	}, nil
}

// TryReadAt 非阻塞读取指定偏移：
//   - 偏移已有数据：返回事件；
//   - 偏移越界但分区未关闭（数据暂时不可用，即背压）：返回 ReasonBackpressure，
//     调用方应稍后重试，数据不会丢失；
//   - 偏移越界且分区已关闭：返回 ReasonSourceGone（正常结束）。
func (p *PartitionLog) TryReadAt(offset int64) (Envelope, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if offset >= int64(len(p.events)) {
		if p.closed {
			return Envelope{}, newError(ReasonSourceGone,
				"partition "+p.name+" closed at offset "+itoa(offset))
		}
		return Envelope{}, newError(ReasonBackpressure,
			"partition "+p.name+" offset "+itoa(offset)+" not ready yet")
	}
	return Envelope{
		Partition: p.id,
		Offset:    offset,
		Event:     p.events[offset],
	}, nil
}

// Close 标记分区不再追加新事件，已写入数据仍可重复读取。
func (p *PartitionLog) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	p.cond.Broadcast()
}

// Closed 返回分区是否已关闭。
func (p *PartitionLog) Closed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closed
}

// Source 聚合多个分区，按分区编号查找。
type Source struct {
	byID map[int]*PartitionLog
}

func NewSource(partitions ...*PartitionLog) *Source {
	s := &Source{byID: make(map[int]*PartitionLog, len(partitions))}
	for _, p := range partitions {
		s.byID[p.id] = p
	}
	return s
}

func (s *Source) Partition(i int) *PartitionLog { return s.byID[i] }

func (s *Source) NumPartitions() int { return len(s.byID) }

// IDs 返回所有分区编号（升序）。
func (s *Source) IDs() []int {
	ids := make([]int, 0, len(s.byID))
	for id := range s.byID {
		ids = append(ids, id)
	}
	for i := 1; i < len(ids); i++ {
		for j := i; j > 0 && ids[j-1] > ids[j]; j-- {
			ids[j-1], ids[j] = ids[j], ids[j-1]
		}
	}
	return ids
}

func itoa(i int64) string {
	// 避免仅为一处字符串拼接引入 strconv 的间接性，保持错误信息零依赖。
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		b[pos] = '-'
	}
	return string(b[pos:])
}
