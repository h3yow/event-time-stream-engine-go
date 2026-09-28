package streamengine

import (
	"context"
	"testing"
	"time"
)

// 两分区并发摄入同一批乱序数据，多次运行结果必须一致（确定性）。
func TestEngine_ConcurrentPartitions_Deterministic(t *testing.T) {
	cfg := Config{WindowSize: 10, MaxOutOfOrderness: 1, AllowedLateness: 0}

	mk := func() (*Source, []Event) {
		a := []Event{
			{Key: "k", EventTime: 1, Value: 1},
			{Key: "k", EventTime: 11, Value: 10},
			{Key: "k", EventTime: 21, Value: 100},
		}
		b := []Event{
			{Key: "k", EventTime: 2, Value: 2},
			{Key: "k", EventTime: 12, Value: 20},
			{Key: "k", EventTime: 22, Value: 200},
		}
		p0 := NewPartitionLog(0, "p0", a)
		p1 := NewPartitionLog(1, "p1", b)
		p0.Close()
		p1.Close()
		return NewSource(p0, p1), append(a, b...)
	}

	var first []WindowResult
	for iter := 0; iter < 20; iter++ {
		src, _ := mk()
		eng := NewEngine(cfg)
		eng.BindSource(src)
		c := collect(eng)
		if err := eng.Run(context.Background()); err != nil {
			t.Fatal(err)
		}
		c.wait()
		results, _ := c.snapshot()
		// 只保留每个窗口的最终版本（INITIAL 之后可能有 CORRECTED，多分区下
		// 触发时点可能不同）；比较窗口最终聚合值。
		final := finalizeByWindow(results)
		if iter == 0 {
			first = final
			t.Logf("iter0 final windows: %v", final)
		} else if !resultsEqual(first, final) {
			t.Fatalf("iteration %d nondeterministic:\n first=%v\n now  =%v", iter, first, final)
		}
	}
}

// finalizeByWindow 保留每个窗口的最后一个版本（版本号最高者）。
func finalizeByWindow(rs []WindowResult) []WindowResult {
	idx := map[string]int{}
	var out []WindowResult
	for _, r := range rs {
		k := r.Key + "@" + itoa(r.Window.Start)
		if i, ok := idx[k]; ok {
			out[i] = r
			continue
		}
		idx[k] = len(out)
		out = append(out, r)
	}
	return out
}

// 消费组再平衡：阶段 A 两个 Runner 各消费一个分区并落检查点；
// 阶段 B 分区全部交给一个新 Runner 从共享检查点恢复。
// 同一窗口的最终结果必须恰好一次（不重复、不缺失）。
func TestRunner_GroupRebalance_ExactlyOnce(t *testing.T) {
	ctx := context.Background()
	cfg := Config{WindowSize: 10, MaxOutOfOrderness: 1, AllowedLateness: 0}

	p0 := NewPartitionLog(0, "p0", []Event{
		{Key: "k", EventTime: 1, Value: 1},
		{Key: "k", EventTime: 11, Value: 10},
	})
	p1 := NewPartitionLog(1, "p1", []Event{
		{Key: "k", EventTime: 2, Value: 2},
		{Key: "k", EventTime: 12, Value: 20},
	})
	src := NewSource(p0, p1)
	inner := NewMemoryCheckpointStore()
	store := NewGroupCheckpointStore(inner, cfg.MaxOutOfOrderness)

	// 阶段 A：组内两个 Runner 分别消费 p0 / p1（并发摄入）。
	r0 := NewRunner("g1", cfg)
	r1 := NewRunner("g1", cfg)
	c0 := collect(r0.Engine())
	c1 := collect(r1.Engine())
	done0 := make(chan error, 1)
	done1 := make(chan error, 1)
	p0.Close()
	p1.Close()
	go func() {
		err := r0.Run(ctx, src, []int{0}, store, nil)
		if err == nil {
			err = r0.Checkpoint(ctx)
		}
		done0 <- err
	}()
	go func() {
		err := r1.Run(ctx, src, []int{1}, store, nil)
		if err == nil {
			err = r1.Checkpoint(ctx)
		}
		done1 <- err
	}()
	if err := <-done0; err != nil {
		t.Fatalf("r0: %v", err)
	}
	if err := <-done1; err != nil {
		t.Fatalf("r1: %v", err)
	}
	c0.wait()
	c1.wait()

	// 合并两个 Runner 的检查点到一个最终快照（再平衡前的协调点）。
	cp0, err := inner.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if cp0 == nil {
		t.Fatal("expected merged checkpoint from group")
	}
	t.Logf("group checkpoint: %s", DescribeSnapshot(cp0))

	// 阶段 B：新 Runner 接管两个分区，从共享检查点恢复。
	// 源已关闭，所有事件在阶段 A 已提交 => 不应再有任何窗口输出（不重复）。
	r2 := NewRunner("g1", cfg)
	c2 := collect(r2.Engine())
	if err := r2.Run(ctx, src, []int{0, 1}, store, nil); err != nil {
		t.Fatalf("r2: %v", err)
	}
	c2.wait()
	replayed, late := c2.snapshot()
	if len(replayed) != 0 {
		t.Fatalf("rebalance replay must not re-emit windows, got %v", replayed)
	}
	if len(late) != 0 {
		t.Fatalf("no late rejections expected after rebalance, got %v", late)
	}

	// 校验合并检查点本身：窗口聚合含两个分区各一次，引用 4 条且去重。
	cpFinal, _ := inner.Load(ctx)
	t.Logf("after rebalance: %s", DescribeSnapshot(cpFinal))
	if cpFinal.PartitionNext[0] != 2 || cpFinal.PartitionNext[1] != 2 {
		t.Fatalf("committed offsets mismatch: %+v", cpFinal.PartitionNext)
	}
}

// 背压：消费跟不上时引擎停顿（偏移不前进），绝不丢数据；接上消费者后
// 全部事件被精确处理一次。
func TestEngine_Backpressure_NoLoss(t *testing.T) {
	ctx := context.Background()
	cfg := Config{WindowSize: 1, MaxOutOfOrderness: 0, AllowedLateness: 0}
	// 100 条事件分布在 100 个不同窗口：每条事件最终都产出一条窗口结果，
	// 使投递路径在无消费者时必然背压。
	var evs0, evs1 []Event
	for i := int64(0); i < 50; i++ {
		evs0 = append(evs0, Event{Key: "a", EventTime: 2 * i, Value: i})
		evs1 = append(evs1, Event{Key: "a", EventTime: 2*i + 1, Value: i * 1000})
	}
	p0 := NewPartitionLog(0, "p0", evs0)
	p1 := NewPartitionLog(1, "p1", evs1)
	p0.Close()
	p1.Close()
	src := NewSource(p0, p1)

	eng := newEngineWithBuffer(cfg, 4) // 极小结果缓冲：很快进入背压
	eng.BindSource(src)
	// 无任何消费者运行：结果在“有界待发队列(256)+结果通道(4)”中积压，
	// 投递器随后阻塞在通道发送，finish 阶段阻塞——Run 不返回，也不报错、不丢数据。
	runErr := make(chan error, 1)
	go func() { runErr <- eng.Run(ctx) }()

	// 100 条结果远超 260 容量中的前 4+256 上限：等到事件全部处理、投递器停顿。
	if err := eng.AwaitOffsets(ctx, map[int]int64{0: 50, 1: 50}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-runErr:
		t.Fatalf("Run must not finish without consumer (backpressure), got err=%v", err)
	case <-time.After(30 * time.Millisecond):
		// 预期：Run 仍卡在交付背压上。
	}
	t.Logf("backpressure stalled: %s", eng.Snapshot())

	// 接上消费者：背压解除，Run 正常收尾，结果完整且恰好一次。
	c := collect(eng)
	if err := <-runErr; err != nil {
		t.Fatalf("run: %v", err)
	}
	c.wait()
	results, late := c.snapshot()
	if len(late) != 0 {
		t.Fatalf("backpressure must not cause late drops: %v", late)
	}
	final := finalizeByWindow(results)
	if len(final) != 100 {
		t.Fatalf("expect 100 distinct windows after drain, got %d", len(final))
	}
	if s := eng.Stats(); s.Accepted != 100 || s.Duplicates != 0 || s.LateReject != 0 {
		t.Fatalf("stats indicate loss/dup: %+v", s)
	}
}
