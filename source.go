package streamengine

import (
	"context"
	"errors"
	"sync"
)

// MemorySource 用本地有界消息缓冲模拟分区数据源。
//
// 每个分区维护一条按下标即 offset 排列的 FIFO 记录切片：
//   - Produce 时分配 offset（分区内单调递增）；
//   - 缓冲按"未读记录数"计容量，满时非阻塞生产返回 ErrBackpressure，
//     阻塞生产则等待空位 —— 任何情况下都不静默丢数据；
//   - Poll 从读取游标顺序读取并推进游标；
//   - Commit/Seek 以已提交位点为下界，回退明确返回 ErrOffsetRollback。
type MemorySource struct {
	mu         sync.Mutex
	partitions map[int][]Record
	capacity   int
	cond       *sync.Cond
	closed     bool
	readCursor map[int]int64 // 当前读取游标（恢复时由 Seek 重置）
	committed  map[int]int64 // 已提交位点：下一个待处理 offset
}

// 源相关的可区分失败原因。
var (
	// ErrSourceClosed 数据源已关闭。
	ErrSourceClosed = errors.New("source closed")
	// ErrUnknownPartition 访问了不存在的分区。
	ErrUnknownPartition = errors.New("unknown partition")
)

func NewMemorySource(partitions, capacityPerPartition int) *MemorySource {
	if partitions <= 0 || capacityPerPartition <= 0 {
		panic("partitions and capacity must be positive")
	}
	s := &MemorySource{
		partitions: make(map[int][]Record, partitions),
		capacity:   capacityPerPartition,
		readCursor: make(map[int]int64, partitions),
		committed:  make(map[int]int64, partitions),
	}
	for i := 0; i < partitions; i++ {
		s.partitions[i] = make([]Record, 0, capacityPerPartition)
	}
	s.cond = sync.NewCond(&s.mu)
	return s
}

func (s *MemorySource) known(p int) bool {
	_, ok := s.partitions[p]
	return ok
}

// pendingLocked 返回未读记录数（下标即 offset）。
func (s *MemorySource) pendingLocked(p int) int {
	return len(s.partitions[p]) - int(s.readCursor[p])
}

// ProduceNonBlocking 生产一条记录；缓冲满返回 ErrBackpressure，调用方可重试。
// 记录的 Offset 由源统一分配，入参 r.Offset 会被覆盖。
func (s *MemorySource) ProduceNonBlocking(r Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrSourceClosed
	}
	if !s.known(r.Partition) {
		return ErrUnknownPartition
	}
	if s.pendingLocked(r.Partition) >= s.capacity {
		return ErrBackpressure
	}
	r.Offset = int64(len(s.partitions[r.Partition]))
	s.partitions[r.Partition] = append(s.partitions[r.Partition], r)
	s.cond.Broadcast()
	return nil
}

// Produce 阻塞式生产：缓冲满时等待空位或 ctx 取消，不丢数据。
func (s *MemorySource) Produce(ctx context.Context, r Record) error {
	if err := s.ProduceNonBlocking(r); err == nil || !errors.Is(err, ErrBackpressure) {
		return err
	}
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			s.mu.Lock()
			s.cond.Broadcast()
			s.mu.Unlock()
		case <-done:
		}
	}()

	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		if s.closed {
			return ErrSourceClosed
		}
		if !s.known(r.Partition) {
			return ErrUnknownPartition
		}
		if s.pendingLocked(r.Partition) < s.capacity {
			r.Offset = int64(len(s.partitions[r.Partition]))
			s.partitions[r.Partition] = append(s.partitions[r.Partition], r)
			s.cond.Broadcast()
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		s.cond.Wait()
	}
}

// Poll 拉取读取游标处的一条记录并推进游标；无记录时阻塞等待。
// 重启后若调用 Seek 回退游标，Poll 会重新投递旧记录（重复消费由处理器幂等去重）。
func (s *MemorySource) Poll(ctx context.Context, partition int) (Record, error) {
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			s.mu.Lock()
			s.cond.Broadcast()
			s.mu.Unlock()
		case <-done:
		}
	}()

	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		if !s.known(partition) {
			return Record{}, ErrUnknownPartition
		}
		if cur := s.readCursor[partition]; cur < int64(len(s.partitions[partition])) {
			r := s.partitions[partition][cur]
			s.readCursor[partition] = cur + 1
			return r, nil
		}
		if s.closed {
			return Record{}, ErrSourceClosed
		}
		if err := ctx.Err(); err != nil {
			return Record{}, err
		}
		s.cond.Wait()
	}
}

// Seek 将分区读取游标重置为 offset，用于断点恢复后回放。
// offset 不得小于已提交位点，否则返回 ErrOffsetRollback（断点回退被拒绝）。
func (s *MemorySource) Seek(partition int, offset int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.known(partition) {
		return ErrUnknownPartition
	}
	if offset < s.committed[partition] {
		return ErrOffsetRollback
	}
	s.readCursor[partition] = offset
	s.cond.Broadcast()
	return nil
}

// Commit 提交位点（语义：下一个待处理 offset），单调不减；严格回退被拒绝。
// 位点提交与处理器快照在同一把处理器/运行器协调顺序下成对发生。
func (s *MemorySource) Commit(partition int, offset int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.known(partition) {
		return ErrUnknownPartition
	}
	if offset < s.committed[partition] {
		return ErrOffsetRollback
	}
	s.committed[partition] = offset
	s.cond.Broadcast()
	return nil
}

func (s *MemorySource) Committed(partition int) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.committed[partition]
}

// Len 返回某分区当前未读记录数。
func (s *MemorySource) Len(partition int) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.known(partition) {
		return -1
	}
	return s.pendingLocked(partition)
}

func (s *MemorySource) Close() {
	s.mu.Lock()
	s.closed = true
	s.cond.Broadcast()
	s.mu.Unlock()
}
