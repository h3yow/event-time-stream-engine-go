package streamengine

import (
	"context"
	"sync"
)

// GroupCheckpointStore 包装一个底层存储，使消费组内多个 Runner 的并发
// 检查点保存变成“读最新 -> 合并 -> 写回”的串行操作。
//
// 没有它，两个 Runner 各自递增本地代际号会被底层存储当作代际冲突拒绝；
// 有了它，每次保存都与已持久化的最新快照合并（分区位置取最大、窗口记录
// 求并集），代际严格递增，因此再平衡/并发摄入后状态仍然完整、不重复。
type GroupCheckpointStore struct {
	mu                sync.Mutex
	inner             CheckpointStore
	maxOutOfOrderness int64
}

// NewGroupCheckpointStore 包装底层存储。
func NewGroupCheckpointStore(inner CheckpointStore, maxOutOfOrderness int64) *GroupCheckpointStore {
	return &GroupCheckpointStore{inner: inner, maxOutOfOrderness: maxOutOfOrderness}
}

func (g *GroupCheckpointStore) Load(ctx context.Context) (*Checkpoint, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.inner.Load(ctx)
}

func (g *GroupCheckpointStore) Save(ctx context.Context, cp *Checkpoint) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	latest, err := g.inner.Load(ctx)
	if err != nil {
		return err
	}
	merged := MergeCheckpoints(g.maxOutOfOrderness, latest, cp)
	return g.inner.Save(ctx, merged)
}

// Runner 表示消费组中的一个消费者实例，负责消费被分配到的分区集合。
//
// 消费组调整（再平衡）模型：同一个 groupID 的所有 Runner 共享同一个
// CheckpointStore（建议用 GroupCheckpointStore 包装）。再平衡时，旧 Runner
// 停止（其已处理偏移与窗口状态已通过检查点持久化），新 Runner 带着新的分区
// 分配启动，从共享检查点恢复：
//
//   - 窗口状态（含已处理记录的去重集合）随检查点一起迁移到新 Runner；
//   - 任何早于已提交位置的启动偏移都被 ReasonCheckpointRollback 拒绝；
//   - 正常重放的记录由记录级去重抵消，已触发窗口不会再次 INITIAL；
//
// 因而同一个窗口在再平衡前后保持恰好一次输出。
type Runner struct {
	groupID string
	engine  *Engine
}

// NewRunner 创建消费组消费者。
func NewRunner(groupID string, cfg Config) *Runner {
	return &Runner{
		groupID: groupID,
		engine:  NewEngine(cfg),
	}
}

// GroupID 返回消费组名。
func (r *Runner) GroupID() string { return r.groupID }

// Engine 返回底层引擎（用于读取输出通道、触发检查点、打印快照）。
func (r *Runner) Engine() *Engine { return r.engine }

// Run 消费 assigned 分区直到全部读完或 ctx 取消。
// store 为消费组共享检查点存储；startOffsets 非 nil 时显式指定起始位置
// （可用于构造重复消费/断点回退场景）。
func (r *Runner) Run(ctx context.Context, src *Source, assigned []int, store CheckpointStore, startOffsets map[int]int64) error {
	r.engine.BindSource(src)
	if store == nil {
		store = NewMemoryCheckpointStore()
	}
	r.engine.SetCheckpointStore(store)
	r.engine.setAssigned(assigned)
	r.engine.SetStartOffsets(startOffsets)
	return r.engine.Run(ctx)
}

// Checkpoint 触发共享检查点提交。
func (r *Runner) Checkpoint(ctx context.Context) error { return r.engine.Checkpoint(ctx) }

// AssignedOffsets 返回共享检查点中已提交的消费位置（不启动引擎，便于再平衡核对）。
func (r *Runner) AssignedOffsets(ctx context.Context, store CheckpointStore) (map[int]int64, error) {
	cp, err := store.Load(ctx)
	if err != nil {
		return nil, err
	}
	if cp == nil {
		return map[int]int64{}, nil
	}
	return cp.PartitionNext, nil
}
