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

// ReplaceAll 模拟"整批发版"：老成员全部下线、一批全新成员整体顶上。
// 与逐个 Join/Leave 的区别是整个更换只推进一次世代（中间没有"零成员"世代）。
// 返回新世代分配。
func (g *GroupCoordinator) ReplaceAll(newMembers []string) *Assignment {
	g.mu.Lock()
	defer g.mu.Unlock()
	next := make(map[string]struct{}, len(newMembers))
	for _, m := range newMembers {
		next[m] = struct{}{}
	}
	g.members = next
	g.generation++
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

// Partitions 返回消费组管辖的全部分区（排序拷贝）。
func (g *GroupCoordinator) Partitions() []int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]int(nil), g.partitions...)
}

// Runner 是并发摄入运行器。
//
// 它为"当前世代"分配到的每个分区运行一个消费 goroutine，多个摄入 goroutine
// 共享同一个并发安全的 Processor。再均衡时先优雅停止旧世代全部 goroutine，
// 做一致性断点，再把各分区读取位点对齐到处理器状态（Seek），然后才启动新世代。
// 配合处理器的 (partition,offset) 幂等去重，窗口结果在任意并发/再均衡下
// 都保持恰好一次。
type Runner struct {
	source             *MemorySource
	proc               *Processor
	store              CheckpointStore
	coord              *GroupCoordinator
	checkpointEvery    time.Duration
	watermarkTickEvery time.Duration
	logger             func(format string, args ...interface{})

	mu           sync.Mutex
	gen          int64
	assignment   *Assignment
	offsets      map[int]int64 // partition -> 下一个待处理 offset
	nextCpID     int64
	cancel       context.CancelFunc
	tickerCancel context.CancelFunc
	wmCancel     context.CancelFunc
	wg           sync.WaitGroup
	running      bool
}

func NewRunner(source *MemorySource, proc *Processor, store CheckpointStore, coord *GroupCoordinator, checkpointEvery time.Duration) *Runner {
	return &Runner{
		source:             source,
		proc:               proc,
		store:              store,
		coord:              coord,
		checkpointEvery:    checkpointEvery,
		watermarkTickEvery: 10 * time.Millisecond, // 默认高频驱动空闲检测
		offsets:            make(map[int]int64),
		nextCpID:           store.LatestID() + 1,
	}
}

// SetLogger 注入日志钩子（测试用它把分区状态变化写进 t.Log）。
func (r *Runner) SetLogger(f func(format string, args ...interface{})) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.logger = f
}

// SetWatermarkTickInterval 设置空闲水位线检测的驱动周期；必须在 Start 前调用。
// <=0 表示关闭周期性驱动（空闲检测将不生效，需手动调用 proc.TickWatermarks）。
func (r *Runner) SetWatermarkTickInterval(d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.watermarkTickEvery = d
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
	// 注册消费组管辖的全部分区：断点里有水位线明细的分区按明细恢复，
	// 其余分区以 WAITING 注册，避免新分区/未活动分区被漏出全局水位线计算。
	r.proc.RegisterPartitions(r.coord.Partitions())
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

	// 注册全部已知分区（WAITING 拖住全局水位线，直到有数据或被判定空闲）。
	r.proc.RegisterPartitions(r.coord.Partitions())

	for member, parts := range ass.Partitions {
		for _, p := range parts {
			r.wg.Add(1)
			go r.consume(ctx, ass.Generation, member, p)
		}
	}

	tctx, tcancel := context.WithCancel(context.Background())
	r.tickerCancel = tcancel
	go r.checkpointLoop(tctx)

	if r.watermarkTickEvery > 0 {
		wctx, wcancel := context.WithCancel(context.Background())
		r.wmCancel = wcancel
		r.wg.Add(1)
		go r.watermarkLoop(wctx)
	}
}

// watermarkLoop 周期性驱动空闲检测：把超时无数据的分区转为 IDLE 并重算水位线，
// 触发因此到点的窗口。这让"某分区彻底空闲"不会把窗口永远拖住。
func (r *Runner) watermarkLoop(ctx context.Context) {
	defer r.wg.Done()
	ticker := time.NewTicker(r.watermarkTickEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tr, fired := r.proc.TickWatermarks()
			for _, x := range tr {
				r.logf("partition %d transition %s -> %s at %s",
					x.Partition, x.From, x.To, x.At.Format(time.RFC3339Nano))
			}
			if len(fired) > 0 {
				r.logf("watermark tick fired %d window(s)", len(fired))
			}
		}
	}
}

// logf 是运行器的轻量日志钩子，默认写标准 logger；测试可替换。
func (r *Runner) logf(format string, args ...interface{}) {
	if r.logger != nil {
		r.logger(format, args...)
	}
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
//
// 关键一致性保证：r.proc.Process(rec) 与 r.offsets 的推进在同一把 r.mu 临界区内
// 完成，且不持有处理器锁跨越 I/O。Checkpoint 也在同一锁序（先 r.mu 后 proc 锁）下
// 成对捕获"处理器状态 + 位点"。因此任何快照里：
//   - 已计入聚合的记录，其位点必然已包含在快照的 offsets 中；
//   - offsets 指向的位点之前不存在"状态里没有"的记录。
//
// 恢复时 RecoverSeen([0,offset)) 与聚合状态严格对应，既不重复计数也不遗漏。
func (r *Runner) Submit(gen int64, rec Record) (ProcessResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if gen != r.gen || !r.running {
		return ProcessResult{}, fmt.Errorf("%w: submit gen=%d current gen=%d",
			ErrStaleGeneration, gen, r.gen)
	}
	if r.assignment == nil || !r.assignment.owns(rec.Partition) {
		return ProcessResult{}, fmt.Errorf("%w: partition=%d gen=%d",
			ErrPartitionNotAssigned, rec.Partition, gen)
	}

	// 在 r.mu 内完成处理，保证与位点推进是同一个原子步骤。
	// Processor 内部另有自己的互斥锁；锁序恒为 r.mu -> proc.mu，不存在反向获取。
	res := r.processAndAdvanceLocked(rec)
	return res, nil
}

// processAndAdvanceLocked 处理一条记录并原子地推进对应分区位点。调用时持有 r.mu。
func (r *Runner) processAndAdvanceLocked(rec Record) ProcessResult {
	res := r.proc.Process(rec)
	if next := rec.Offset + 1; next > r.offsets[rec.Partition] {
		r.offsets[rec.Partition] = next
	}
	return res
}

func (r *Runner) checkpointLoop(ctx context.Context) {
	if r.checkpointEvery <= 0 {
		<-ctx.Done()
		return
	}
	ticker := time.NewTicker(r.checkpointEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = r.Checkpoint()
		}
	}
}

// Checkpoint 触发一致性断点：在同一把 r.mu 下把"位点 + 处理器状态"作为同一瞬间
// 原子捕获，然后保存快照；保存成功后再向源提交位点。
//
// 顺序保证：
//  1. 持 r.mu 拷贝 offsets 并持处理器锁生成快照 —— 二者严格对应同一处理点，
//     高频并发处理不可能插在两者之间；
//  2. 释放锁后做持久化 I/O，不阻塞正常摄入；
//  3. Save 成功后 Commit 位点。失败时只会"快照领先"，恢复后最多重放、绝不丢失，
//     且重放命中幂等防线不会重复计数。
func (r *Runner) Checkpoint() error {
	r.mu.Lock()
	if !r.running && r.gen == 0 {
		r.mu.Unlock()
		return nil
	}
	id := r.nextCpID
	r.nextCpID++
	offsets := make(map[int]int64, len(r.offsets))
	for p, off := range r.offsets {
		offsets[p] = off
	}
	// r.mu 持有期间不会有 Submit 能改动状态或位点；SnapshotLocked 与位点拷贝
	// 因此对应同一个时间点。
	cp := r.proc.SnapshotLocked(id, offsets)
	r.mu.Unlock()

	if err := r.store.Save(cp); err != nil { // 回退断点在此被明确拒绝
		r.mu.Lock()
		r.nextCpID--
		r.mu.Unlock()
		return err
	}
	for p, off := range offsets {
		if err := r.source.Commit(p, off); err != nil { // 位点回退在此被明确拒绝
			return err
		}
	}
	return nil
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
	if r.wmCancel != nil {
		r.wmCancel()
	}
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
	if r.wmCancel != nil {
		r.wmCancel()
	}
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
