package streamengine

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Assignment 是某一世代下消费组对分区的分配方案。
type Assignment struct {
	Generation int64
	Members    []string
	Partitions map[string][]int // memberID -> partitions
}

func (a *Assignment) owns(partition int) bool {
	for _, ps := range a.Partitions {
		for _, p := range ps {
			if p == partition {
				return true
			}
		}
	}
	return false
}

func (a *Assignment) clone() *Assignment {
	c := &Assignment{Generation: a.Generation, Partitions: make(map[string][]int, len(a.Partitions))}
	c.Members = append(c.Members, a.Members...)
	for m, ps := range a.Partitions {
		c.Partitions[m] = append([]int(nil), ps...)
	}
	return c
}

// GroupCoordinator 模拟消费组协调器：成员变化即推进世代并确定性地重新均分分区。
// 世代单调递增，旧世代的提交/摄入由 Runner 以 ErrStaleGeneration 明确拒绝。
type GroupCoordinator struct {
	mu         sync.Mutex
	groupID    string
	generation int64
	members    map[string]struct{}
	partitions []int
}

func NewGroupCoordinator(groupID string, partitions []int) *GroupCoordinator {
	ps := append([]int(nil), partitions...)
	sort.Ints(ps)
	return &GroupCoordinator{
		groupID:    groupID,
		members:    make(map[string]struct{}),
		partitions: ps,
	}
}

// assignLocked 按排序后成员轮询分配分区，结果对所有人都确定一致。
func (g *GroupCoordinator) assignLocked() *Assignment {
	members := make([]string, 0, len(g.members))
	for m := range g.members {
		members = append(members, m)
	}
	sort.Strings(members)
	ass := &Assignment{Generation: g.generation, Members: members, Partitions: map[string][]int{}}
	for _, m := range members {
		ass.Partitions[m] = nil
	}
	for i, p := range g.partitions {
		if len(members) == 0 {
			break
		}
		m := members[i%len(members)]
		ass.Partitions[m] = append(ass.Partitions[m], p)
	}
	return ass
}

// Join 成员加入；若为新成员则推进世代并再均衡。
func (g *GroupCoordinator) Join(memberID string) *Assignment {
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, ok := g.members[memberID]; !ok {
		g.members[memberID] = struct{}{}
		g.generation++
	}
	return g.assignLocked().clone()
}

// Leave 成员退出；若成员存在则推进世代并再均衡。
func (g *GroupCoordinator) Leave(memberID string) *Assignment {
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, ok := g.members[memberID]; ok {
		delete(g.members, memberID)
		g.generation++
	}
	return g.assignLocked().clone()
}

// Current 返回当前世代分配的拷贝；无成员时返回 nil。
func (g *GroupCoordinator) Current() *Assignment {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.generation == 0 {
		return nil
	}
	return g.assignLocked().clone()
}

func (g *GroupCoordinator) Generation() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.generation
}

// Runner 是并发摄入运行器。
//
// 它为"当前世代"分配到的每个分区运行一个消费 goroutine，多个摄入 goroutine
// 共享同一个并发安全的 Processor。再均衡时先优雅停止旧世代全部 goroutine，
// 做一致性断点，再把各分区读取位点对齐到处理器状态（Seek），然后才启动新世代。
// 配合处理器的 (partition,offset) 幂等去重，窗口结果在任意并发/再均衡下
// 都保持恰好一次。
type Runner struct {
	source          *MemorySource
	proc            *Processor
	store           CheckpointStore
	coord           *GroupCoordinator
	checkpointEvery time.Duration

	mu           sync.Mutex
	gen          int64
	assignment   *Assignment
	offsets      map[int]int64 // partition -> 下一个待处理 offset（镜像，用于再均衡 Seek 对齐与日志）
	lastCp       *Checkpoint   // 上次成功保存的断点（无进展时跳过高频打点）
	nextCpID     int64
	cancel       context.CancelFunc
	tickerCancel context.CancelFunc
	wg           sync.WaitGroup
	running      bool
}

func NewRunner(source *MemorySource, proc *Processor, store CheckpointStore, coord *GroupCoordinator, checkpointEvery time.Duration) *Runner {
	return &Runner{
		source:          source,
		proc:            proc,
		store:           store,
		coord:           coord,
		checkpointEvery: checkpointEvery,
		offsets:         make(map[int]int64),
		nextCpID:        store.LatestID() + 1,
	}
}

// Recover 从断点存储恢复：处理器聚合/水位线 + 源消费位点 + 幂等防线一起重建。
// 状态与位点来自同一份快照，恢复后回放旧记录只会得到 DUPLICATE。
func (r *Runner) Recover() error {
	cp, err := r.store.Load()
	if err != nil {
		return fmt.Errorf("recover load: %w", err)
	}
	if cp == nil {
		return nil
	}
	r.mu.Lock()
	offsets := r.proc.Restore(cp)
	for p, off := range offsets {
		r.offsets[p] = off
	}
	if cp.ID >= r.nextCpID {
		r.nextCpID = cp.ID + 1
	}
	r.mu.Unlock()

	// 重建去重表必须在 Seek/消费之前完成。
	r.proc.RecoverSeen(offsets)
	for p, off := range offsets {
		// Seek 拒绝回退到已提交位点之前 —— 不允许从更旧的中间位置接着算。
		if err := r.source.Seek(p, off); err != nil {
			return fmt.Errorf("recover seek partition %d: %w", p, err)
		}
	}
	return nil
}

// Start 按给定分配启动消费；世代必须与协调器当前世代一致。
func (r *Runner) Start(ass *Assignment) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.running {
		return fmt.Errorf("runner already running at generation %d", r.gen)
	}
	cur := r.coord.Current()
	if cur == nil {
		return fmt.Errorf("%w: coordinator has no active generation", ErrStaleGeneration)
	}
	if ass.Generation != cur.Generation {
		return fmt.Errorf("%w: assignment gen=%d coordinator gen=%d",
			ErrStaleGeneration, ass.Generation, cur.Generation)
	}
	r.startGenerationLocked(ass)
	return nil
}

func (r *Runner) startGenerationLocked(ass *Assignment) {
	ctx, cancel := context.WithCancel(context.Background())
	r.gen = ass.Generation
	r.assignment = ass.clone()
	r.cancel = cancel
	r.running = true

	// 注册本世代涉及的全部分区：暂无数据的分区会拖住水位线，直到来数或被判空闲。
	var all []int
	for member, parts := range ass.Partitions {
		all = append(all, parts...)
		for _, p := range parts {
			r.wg.Add(1)
			go r.consume(ctx, ass.Generation, member, p)
		}
	}
	r.proc.TrackPartitions(all...)

	tctx, tcancel := context.WithCancel(context.Background())
	r.tickerCancel = tcancel
	go r.tickLoop(tctx)
}

func (r *Runner) consume(ctx context.Context, gen int64, member string, partition int) {
	defer r.wg.Done()
	for {
		rec, err := r.source.Poll(ctx, partition)
		if err != nil {
			// ctx 取消（再均衡/停止）或源关闭：退出，不做任何状态提交。
			return
		}
		if _, err := r.Submit(gen, rec); err != nil {
			switch {
			case errors.Is(err, ErrStaleGeneration), errors.Is(err, ErrPartitionNotAssigned):
				return // 旧世代或分区已被抢走，立即退出
			default:
				return // 本地源不预期其它错误，发生即停止该消费者
			}
		}
	}
}

// Submit 是唯一的记录入口，校验世代与分区归属后再进入处理器。
// 旧世代提交 -> ErrStaleGeneration；非本世代分配的分区 -> ErrPartitionNotAssigned。
func (r *Runner) Submit(gen int64, rec Record) (ProcessResult, error) {
	r.mu.Lock()
	if gen != r.gen || !r.running {
		r.mu.Unlock()
		return ProcessResult{}, fmt.Errorf("%w: submit gen=%d current gen=%d",
			ErrStaleGeneration, gen, r.gen)
	}
	if r.assignment == nil || !r.assignment.owns(rec.Partition) {
		r.mu.Unlock()
		return ProcessResult{}, fmt.Errorf("%w: partition=%d gen=%d",
			ErrPartitionNotAssigned, rec.Partition, gen)
	}
	r.mu.Unlock()

	res := r.proc.Process(rec)

	r.mu.Lock()
	if next := rec.Offset + 1; next > r.offsets[rec.Partition] {
		r.offsets[rec.Partition] = next
	}
	r.mu.Unlock()
	return res, nil
}

// tickLoop 是运行器的周期循环：每次 tick 先做空闲分区检测（水位线可能因此
// 推进并触发窗口），再按 checkpointEvery 的节奏做断点。
// 即使未启用周期断点（checkpointEvery<=0），空闲检测也会以默认节奏运行，
// 保证"长时间无数据的分区不会把水位线永远拖住"。
func (r *Runner) tickLoop(ctx context.Context) {
	interval := r.checkpointEvery
	if interval <= 0 {
		interval = 50 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.proc.CheckIdle()
			if r.checkpointEvery > 0 {
				_ = r.Checkpoint()
			}
		}
	}
}

// Checkpoint 触发一致性断点：处理器在同一瞬间原子捕获"聚合状态+位点+水位线"，
// 先保存快照，成功后再向源提交位点。顺序保证恢复后最多重放、绝不丢失，且重放幂等。
// 若自上次断点以来位点与水位线都没有变化，则跳过本次保存，避免高频打点产生无效写。
func (r *Runner) Checkpoint() error {
	r.mu.Lock()
	if !r.running && r.gen == 0 {
		r.mu.Unlock()
		return nil
	}
	id := r.nextCpID
	r.nextCpID++
	r.mu.Unlock()

	cp := r.proc.Snapshot(id)

	r.mu.Lock()
	if r.lastCp != nil && r.lastCp.Watermark.Equal(cp.Watermark) && offsetsEqual(r.lastCp.Offsets, cp.Offsets) {
		r.mu.Unlock()
		return nil // 无进展：跳过，不消耗存储 IO
	}
	r.mu.Unlock()

	if err := r.store.Save(cp); err != nil { // 回退断点在此被明确拒绝
		return err
	}
	r.mu.Lock()
	r.lastCp = cloneCheckpoint(cp)
	r.mu.Unlock()
	for p, off := range cp.Offsets {
		if err := r.source.Commit(p, off); err != nil { // 位点回退在此被明确拒绝
			return err
		}
	}
	return nil
}

func offsetsEqual(a, b map[int]int64) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}

// Rebalance 切换到新世代分配：优雅停止旧 goroutine -> 断点 -> 位点对齐 -> 启动。
func (r *Runner) Rebalance(next *Assignment) error {
	r.mu.Lock()
	if !r.running {
		r.mu.Unlock()
		return r.Start(next)
	}
	if next.Generation <= r.gen {
		return fmt.Errorf("%w: next gen=%d current gen=%d",
			ErrStaleGeneration, next.Generation, r.gen)
	}
	oldCancel := r.cancel
	r.running = false // 阻止旧世代 goroutine 在退出前提交新记录
	r.mu.Unlock()

	oldCancel()
	r.wg.Wait()
	if r.tickerCancel != nil {
		r.tickerCancel()
	}

	// 旧世代完全静止后做断点，保证状态与位点一致。
	if err := r.Checkpoint(); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	// 将读取位点对齐到处理器已处理位置：
	// 旧 goroutine 可能"已 Poll 未处理"，对齐后这些记录会被重新投递并幂等去重。
	for p, off := range r.offsets {
		if err := r.source.Seek(p, off); err != nil {
			return err
		}
	}
	r.startGenerationLocked(next)
	return nil
}

// Stop 优雅停止全部消费并落最后一次断点。
func (r *Runner) Stop() error {
	r.mu.Lock()
	if !r.running {
		r.mu.Unlock()
		return nil
	}
	cancel := r.cancel
	r.running = false
	r.mu.Unlock()

	cancel()
	r.wg.Wait()
	if r.tickerCancel != nil {
		r.tickerCancel()
	}
	return r.Checkpoint()
}

// Offsets 返回当前消费位点快照（测试/日志用）。
func (r *Runner) Offsets() map[int]int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[int]int64, len(r.offsets))
	for k, v := range r.offsets {
		out[k] = v
	}
	return out
}
