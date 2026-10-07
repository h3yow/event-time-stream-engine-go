package streamengine

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"
)

var t0 = time.Date(2026, 9, 28, 10, 0, 0, 0, time.UTC)

// dumpState 在单测日志中打印水位线、消费位点与聚合状态快照。
func dumpState(t *testing.T, tag string, p *Processor, r *Runner) {
	t.Helper()
	var offs map[int]int64
	if r != nil {
		offs = r.Offsets()
	} else {
		offs = map[int]int64{}
	}
	offKeys := make([]int, 0, len(offs))
	for k := range offs {
		offKeys = append(offKeys, k)
	}
	sort.Ints(offKeys)
	t.Logf("[%s] watermark=%s", tag, p.Watermark().Format(time.RFC3339Nano))
	for _, ps := range p.PartitionWatermarks() {
		t.Logf("[%s] partition-watermark partition=%d hasData=%v maxEventTime=%s contribution=%s idle=%v holdsWatermark=%v",
			tag, ps.Partition, ps.HasData, ps.MaxEventTime.Format("15:04:05"),
			ps.Contribution.Format("15:04:05"), ps.Idle, ps.HoldsWatermark)
	}
	for _, k := range offKeys {
		t.Logf("[%s] consumed-position partition=%d next-offset=%d", tag, k, offs[k])
	}
	ags := p.Aggregates()
	wks := make([]WindowKey, 0, len(ags))
	for k := range ags {
		wks = append(wks, k)
	}
	sort.Slice(wks, func(i, j int) bool {
		if !wks[i].Start.Equal(wks[j].Start) {
			return wks[i].Start.Before(wks[j].Start)
		}
		return wks[i].Key < wks[j].Key
	})
	for _, k := range wks {
		a := ags[k]
		t.Logf("[%s] state key=%s window=[%s,%s) count=%d sum=%d fired=%v revision=%d",
			tag, k.Key, a.Window.Start.Format("15:04:05"), a.Window.End.Format("15:04:05"),
			a.Count, a.Sum, a.Fired, a.Revision)
	}
	for _, s := range p.SideOutput() {
		t.Logf("[%s] side-output partition=%d offset=%d eventTime=%s reason=%v",
			tag, s.Record.Partition, s.Record.Offset,
			s.Record.Event.EventTime.Format("15:04:05"), s.Reason)
	}
}

func eventually(timeout time.Duration, fn func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return fn()
}

// 场景1：乱序到达 + 两级迟到规则。
// 结论必须可区分：ON_TIME / LATE_ACCEPTED / LATE_SIDE_OUTPUT，且计数不重复、不静默丢失。
func TestOutOfOrderAndLateness(t *testing.T) {
	p := NewProcessor(NewTumblingAssigner(10*time.Second), 2*time.Second, 5*time.Second)

	feed := func(offset int64, ts time.Time, v int64, want Disposition) {
		res := p.Process(Record{Partition: 0, Offset: offset, Event: Event{Key: "k", EventTime: ts, Value: v}})
		t.Logf("offset=%d eventTime=%s -> disposition=%s watermark=%s fired=%d",
			offset, ts.Format("15:04:05"), res.Disposition,
			res.Watermark.Format("15:04:05"), len(res.NewResults))
		if res.Disposition != want {
			t.Fatalf("offset=%d want %s got %s", offset, want, res.Disposition)
		}
	}

	feed(0, t0, 1, DispositionOnTime)
	feed(1, t0.Add(5*time.Second), 2, DispositionOnTime)
	// t=12s 推进水位线到 t0+10s，窗口 [0,10) 触发最终结果 count=2 sum=3。
	feed(2, t0.Add(12*time.Second), 3, DispositionOnTime)
	// t=3s 晚到：水位线 t0+10s，容忍下界 t0+5s，3<5 => 侧输出，绝不计入。
	feed(3, t0.Add(3*time.Second), 100, DispositionLateSideOutput)
	// t=6s 晚到但在 allowedLateness=5s 内 => 接受并产生 revision=1 的修订结果。
	feed(4, t0.Add(6*time.Second), 6, DispositionLateAccepted)

	dumpState(t, "out-of-order", p, nil)

	results := p.Results()
	if len(results) != 2 {
		t.Fatalf("want 2 emitted results (final+revision), got %d", len(results))
	}
	if results[0].Revision != 0 || results[0].Count != 2 || results[0].Sum != 3 {
		t.Fatalf("final result mismatch: %+v", results[0])
	}
	if results[1].Revision != 1 || results[1].Count != 3 || results[1].Sum != 9 {
		t.Fatalf("revised result mismatch: %+v", results[1])
	}
	side := p.SideOutput()
	if len(side) != 1 || !errors.Is(side[0].Reason, ErrLateBeyondTolerance) {
		t.Fatalf("want 1 side output with ErrLateBeyondTolerance, got %+v", side)
	}
	// 超出容忍的 100 绝不能出现在聚合中。
	if got := p.Aggregates()[WindowKey{Key: "k", Start: t0}].Sum; got != 9 {
		t.Fatalf("side-output event leaked into aggregate sum=%d", got)
	}
}

// 场景2：重复消费（同分区+位点再来一次）幂等跳过，不重复计数。
func TestDuplicateConsumption(t *testing.T) {
	p := NewProcessor(NewTumblingAssigner(10*time.Second), 2*time.Second, 5*time.Second)
	r := Record{Partition: 0, Offset: 7, Event: Event{Key: "k", EventTime: t0, Value: 5}}

	first := p.Process(r)
	second := p.Process(r)
	third := p.Process(r)
	if first.Disposition != DispositionOnTime {
		t.Fatalf("first want ON_TIME, got %s", first.Disposition)
	}
	if second.Disposition != DispositionDuplicate || third.Disposition != DispositionDuplicate {
		t.Fatalf("repeats want DUPLICATE, got %s/%s", second.Disposition, third.Disposition)
	}
	ag := p.Aggregates()[WindowKey{Key: "k", Start: t0}]
	if ag.Count != 1 || ag.Sum != 5 {
		t.Fatalf("duplicate was counted: %+v", ag)
	}
	dumpState(t, "duplicate", p, nil)
}

// 场景3：背压 —— 缓冲满返回 ErrBackpressure，不丢数据，腾出空位后可重试成功。
func TestBackpressureNoLoss(t *testing.T) {
	src := NewMemorySource(1, 2)
	rec := func(off int64) Record {
		return Record{Partition: 0, Offset: off, Event: Event{Key: "k", EventTime: t0, Value: off}}
	}
	if err := src.ProduceNonBlocking(rec(0)); err != nil {
		t.Fatal(err)
	}
	if err := src.ProduceNonBlocking(rec(1)); err != nil {
		t.Fatal(err)
	}
	if err := src.ProduceNonBlocking(rec(2)); !errors.Is(err, ErrBackpressure) {
		t.Fatalf("want ErrBackpressure, got %v", err)
	}
	t.Logf("buffer full as expected, Len=%d, producer told to retry (no silent drop)", src.Len(0))

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	got, err := src.Poll(ctx, 0) // 消费一条腾出空位
	if err != nil {
		t.Fatal(err)
	}
	if got.Offset != 0 {
		t.Fatalf("want offset 0, got %d", got.Offset)
	}
	if err := src.ProduceNonBlocking(rec(2)); err != nil {
		t.Fatalf("retry after drain should succeed: %v", err)
	}
	if src.Len(0) != 2 {
		t.Fatalf("want Len=2 after retry, got %d", src.Len(0))
	}

	// 阻塞式生产在 ctx 取消时返回 ctx 错误，数据仍未丢失。
	src.ProduceNonBlocking(rec(3)) // 再次填满
	cctx, ccancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer ccancel()
	if err := src.Produce(cctx, rec(4)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocking produce want deadline, got %v", err)
	}
}

// 场景4：断点回退被明确拒绝（断点 ID 与消费位点两个层面）。
func TestRollbackRejected(t *testing.T) {
	store := NewMemoryCheckpointStore()
	cp := func(id int64) *Checkpoint {
		return &Checkpoint{ID: id, CreatedAt: time.Now(), Offsets: map[int]int64{0: id}, Aggregates: map[WindowKey]*Aggregate{}}
	}
	if err := store.Save(cp(5)); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(cp(4)); !errors.Is(err, ErrCheckpointRollback) {
		t.Fatalf("want ErrCheckpointRollback, got %v", err)
	}
	loaded, _ := store.Load()
	if loaded.Offsets[0] != 5 {
		t.Fatalf("checkpoint must stay at 5 after rejected rollback, got %d", loaded.Offsets[0])
	}

	src := NewMemorySource(1, 8)
	if err := src.Commit(0, 10); err != nil {
		t.Fatal(err)
	}
	if err := src.Commit(0, 9); !errors.Is(err, ErrOffsetRollback) {
		t.Fatalf("want ErrOffsetRollback, got %v", err)
	}
	if err := src.Seek(0, 8); !errors.Is(err, ErrOffsetRollback) {
		t.Fatalf("Seek below committed want ErrOffsetRollback, got %v", err)
	}
	if got := src.Committed(0); got != 10 {
		t.Fatalf("committed offset must stay 10, got %d", got)
	}
}

// 场景5：重启恢复 —— 新处理器/新运行器从文件断点恢复，状态与位点对得上，
// 重启后迟到事件继续产生修订结果，且不会重发最终结果（不重复、不缺失）。
func TestRestartRecoveryFromFile(t *testing.T) {
	path := t.TempDir() + "/checkpoint.json"
	src := NewMemorySource(1, 64) // 源代表可保留数据的本地消息日志
	produce := func(off int64, ts time.Time, v int64) {
		t.Helper()
		if err := src.ProduceNonBlocking(Record{Partition: 0, Offset: 0, Event: Event{Key: "k", EventTime: ts, Value: v}}); err != nil {
			t.Fatal(err)
		}
	}
	produce(0, t0, 1)
	produce(1, t0.Add(5*time.Second), 2)
	produce(2, t0.Add(12*time.Second), 3)

	newProc := func() *Processor {
		return NewProcessor(NewTumblingAssigner(10*time.Second), 2*time.Second, 5*time.Second)
	}
	coord := NewGroupCoordinator("g", []int{0})
	ass := coord.Join("a")

	// 第一次运行：消费并触发窗口最终结果，然后停止（落盘断点）。
	store1 := NewFileCheckpointStore(path)
	proc1 := newProc()
	run1 := NewRunner(src, proc1, store1, coord, 0)
	if err := run1.Start(ass); err != nil {
		t.Fatal(err)
	}
	if !eventually(2*time.Second, func() bool { return len(proc1.Results()) == 1 }) {
		t.Fatal("window never fired before restart")
	}
	dumpState(t, "before-stop", proc1, run1)
	if err := run1.Stop(); err != nil {
		t.Fatal(err)
	}
	t.Logf("checkpoint persisted to %s id=%d", path, store1.LatestID())

	// 模拟重启：全新处理器、全新运行器、全新存储实例，但同一个本地消息日志。
	store2 := NewFileCheckpointStore(path)
	proc2 := newProc()
	run2 := NewRunner(src, proc2, store2, coord, 0)
	if err := run2.Recover(); err != nil {
		t.Fatalf("recover: %v", err)
	}
	dumpState(t, "after-recover", proc2, run2)

	if want, got := t0.Add(10*time.Second), proc2.Watermark(); !got.Equal(want) {
		t.Fatalf("watermark restored want %s got %s", want, got)
	}
	ag := proc2.Aggregates()[WindowKey{Key: "k", Start: t0}]
	if ag == nil || !ag.Fired || ag.Count != 2 || ag.Sum != 3 {
		t.Fatalf("aggregate not restored correctly: %+v", ag)
	}
	// 恢复不会重放已发出的最终结果，避免下游重复计数。
	if len(proc2.Results()) != 0 {
		t.Fatalf("final result must not be re-emitted after recover, got %d", len(proc2.Results()))
	}
	if pos := run2.Offsets()[0]; pos != 3 {
		t.Fatalf("consume position restored want 3 got %d", pos)
	}

	// 重启后继续：迟到但可容忍的事件 -> 修订；超出容忍 -> 侧输出。
	produce(0, t0.Add(6*time.Second), 6)
	produce(0, t0.Add(3*time.Second), 100)
	if err := run2.Start(coord.Current()); err != nil {
		t.Fatal(err)
	}
	if !eventually(2*time.Second, func() bool {
		return len(proc2.Results()) == 1 && len(proc2.SideOutput()) == 1
	}) {
		t.Fatalf("post-restart processing incomplete: results=%d side=%d",
			len(proc2.Results()), len(proc2.SideOutput()))
	}
	dumpState(t, "after-restart-processing", proc2, run2)
	if err := run2.Stop(); err != nil {
		t.Fatal(err)
	}
	ag = proc2.Aggregates()[WindowKey{Key: "k", Start: t0}]
	if ag.Count != 3 || ag.Sum != 9 || ag.Revision != 1 {
		t.Fatalf("post-restart aggregate mismatch: %+v", ag)
	}
	res := proc2.Results()
	if len(res) != 1 || res[0].Revision != 1 || res[0].Sum != 9 {
		t.Fatalf("want exactly one revised result after restart, got %+v", res)
	}
}

// 场景6：并发摄入 + 消费组多次再均衡，窗口结果恰好一次，失败原因可区分。
func TestConcurrentRebalanceExactlyOnce(t *testing.T) {
	const n = 150
	src := NewMemorySource(2, 256)
	var expectSum int64
	for i := 0; i < n; i++ {
		v := int64(i + 1)
		expectSum += v
		// 所有事件都落在窗口 [t0,t0+10s)，保证并发处理顺序下结论一致。
		err := src.ProduceNonBlocking(Record{
			Partition: i % 2,
			Event:     Event{Key: "k", EventTime: t0.Add(time.Duration(i%10) * time.Second), Value: v},
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	proc := NewProcessor(NewTumblingAssigner(10*time.Second), 0, 0)
	store := NewMemoryCheckpointStore()
	coord := NewGroupCoordinator("g", []int{0, 1})
	coord.Join("a")
	ass := coord.Join("b") // gen2：a->[0] b->[1]

	run := NewRunner(src, proc, store, coord, 50*time.Millisecond) // 周期性断点同时施压
	if err := run.Start(ass); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)

	// 再均衡1：加入 c（gen3）。
	if err := run.Rebalance(coord.Join("c")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	// 再均衡2：b 退出（gen4），分区 1 被 c 接管。
	if err := run.Rebalance(coord.Leave("b")); err != nil {
		t.Fatal(err)
	}

	// 等待全部记录恰好处理一次。
	if !eventuallyCount(proc, n) {
		t.Fatalf("aggregate count never reached %d", n)
	}
	dumpState(t, "drained-before-close", proc, run)

	// 失败原因必须可区分：旧世代提交 / 非本世代分区。
	curGen := coord.Generation()
	if _, err := run.Submit(curGen-1, Record{Partition: 0, Offset: 99999, Event: Event{Key: "k", EventTime: t0}}); !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("want ErrStaleGeneration, got %v", err)
	}
	if _, err := run.Submit(curGen, Record{Partition: 99, Offset: 1, Event: Event{Key: "k", EventTime: t0}}); !errors.Is(err, ErrPartitionNotAssigned) {
		t.Fatalf("want ErrPartitionNotAssigned, got %v", err)
	}

	// 一条 t=15s 的关闭事件推进水位线，触发窗口最终结果，且只触发一次。
	if err := src.ProduceNonBlocking(Record{Partition: 0, Event: Event{Key: "k", EventTime: t0.Add(15 * time.Second), Value: 0}}); err != nil {
		t.Fatal(err)
	}
	if !eventually(2*time.Second, func() bool { return len(proc.Results()) == 1 }) {
		t.Fatalf("want exactly 1 final result under concurrency/rebalance, got %d", len(proc.Results()))
	}
	if err := run.Stop(); err != nil {
		t.Fatal(err)
	}
	dumpState(t, "final", proc, run)

	res := proc.Results()
	if res[0].Count != n || res[0].Sum != expectSum {
		t.Fatalf("exactly-once violated: count=%d sum=%d want count=%d sum=%d",
			res[0].Count, res[0].Sum, n, expectSum)
	}
	if len(proc.SideOutput()) != 0 {
		t.Fatalf("unexpected side outputs: %d", len(proc.SideOutput()))
	}
	t.Logf("EXACTLY-ONCE OK: count=%d sum=%d checkpoints=%d offsets=%v",
		res[0].Count, res[0].Sum, store.LatestID(), run.Offsets())
}

func eventuallyCount(p *Processor, want int) bool {
	return eventually(3*time.Second, func() bool {
		var total int64
		for _, a := range p.Aggregates() {
			total += a.Count
		}
		return int(total) == want
	})
}

// 兜底：确保测试辅助输出格式本身可编译且包含关键信息。
func TestDumpStateFormat(t *testing.T) {
	p := NewProcessor(NewTumblingAssigner(time.Minute), time.Second, time.Second)
	p.Process(Record{Partition: 0, Offset: 0, Event: Event{Key: "k", EventTime: t0, Value: 1}})
	dumpState(t, fmt.Sprintf("snapshot at %s", t0.Format(time.RFC3339)), p, nil)
}
