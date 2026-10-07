package streamengine

import (
	"errors"
	"testing"
	"time"
)

// 场景D：消费组整批换人（同一进程、同一个 Processor 与共享断点）。
// 老实例 a,b 全下线，新实例 x,y,z 整批顶上（只跨一次世代）；
// 最终窗口结果与全程单消费者一致：不重不漏，最终结果只发一次。
func TestWholeGroupReplacementInProcess(t *testing.T) {
	const n = 300
	src := NewMemorySource(2, 512)
	var expectSum int64
	for i := 0; i < n; i++ {
		v := int64(i + 1)
		expectSum += v
		// 全部落在窗口 [t0,t0+10s)，事件时间随 offset 单调，保证并发/重放下结论一致。
		if err := src.ProduceNonBlocking(Record{
			Partition: i % 2,
			Event:     Event{Key: "k", EventTime: t0.Add(time.Duration(i%10) * time.Second), Value: v},
		}); err != nil {
			t.Fatal(err)
		}
	}

	proc := NewProcessor(NewTumblingAssigner(10*time.Second), 0, 0)
	store := NewMemoryCheckpointStore()
	coord := NewGroupCoordinator("g", []int{0, 1})
	oldAss := coord.ReplaceAll([]string{"a", "b"}) // gen1：a->[0] b->[1]

	run := NewRunner(src, proc, store, coord, 20*time.Millisecond)
	run.SetLogger(func(format string, args ...interface{}) { t.Logf("[runner] "+format, args...) })
	if err := run.Start(oldAss); err != nil {
		t.Fatal(err)
	}

	// 消费一阵，确保老成员确实处理了一部分记录，然后整批换人。
	if !eventually(2*time.Second, func() bool {
		off := run.Offsets()
		return off[0] > 30 && off[1] > 30
	}) {
		t.Fatalf("old generation did not make progress: %v", run.Offsets())
	}
	dumpState(t, "before-whole-replace", proc, run)
	t.Logf("whole generation replacement: {a,b} -> {x,y,z}")
	newAss := coord.ReplaceAll([]string{"x", "y", "z"}) // 一次世代跳变
	if newAss.Generation != oldAss.Generation+1 {
		t.Fatalf("whole replacement must advance exactly one generation: %d -> %d",
			oldAss.Generation, newAss.Generation)
	}
	if err := run.Rebalance(newAss); err != nil {
		t.Fatal(err)
	}
	dumpState(t, "after-whole-replace", proc, run)

	if !eventuallyCount(proc, n) {
		t.Fatalf("not all %d records processed after whole group replacement, offsets=%v", n, run.Offsets())
	}

	// 旧世代提交必须被拒。
	if _, err := run.Submit(oldAss.Generation, rec(0, 99999, t0, 0)); !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("old generation submit after whole replacement want ErrStaleGeneration, got %v", err)
	}

	// 两个分区各发关闭事件推进水位线，触发唯一一次最终结果。
	for _, part := range []int{0, 1} {
		if err := src.ProduceNonBlocking(rec(part, int64(100000+part), t0.Add(15*time.Second), 0)); err != nil {
			t.Fatal(err)
		}
	}
	if !eventually(2*time.Second, func() bool { return len(proc.Results()) == 1 }) {
		t.Fatalf("want exactly 1 final result after whole group replacement, got %d", len(proc.Results()))
	}
	if err := run.Stop(); err != nil {
		t.Fatal(err)
	}
	dumpState(t, "whole-replace-final", proc, run)

	res := proc.Results()
	if res[0].Count != n || res[0].Sum != expectSum {
		t.Fatalf("whole replacement exactly-once violated: count=%d sum=%d want count=%d sum=%d",
			res[0].Count, res[0].Sum, n, expectSum)
	}
	if len(proc.SideOutput()) != 0 {
		t.Fatalf("unexpected side outputs: %d", len(proc.SideOutput()))
	}
	t.Logf("WHOLE-REPLACE OK: count=%d sum=%d checkpoints=%d", res[0].Count, res[0].Sum, store.LatestID())
}

// 场景E：整批换人 + 全新 Runner/Processor 从共享文件断点接手（模拟新实例整体上线）。
// 老实例运行到窗口触发后整批下线；新实例 Recover 后续跑：
// 已发过的窗口最终结果不重发，之后到达的迟到修订仍能正常产生。
func TestWholeGroupReplacementWithFreshRunners(t *testing.T) {
	path := t.TempDir() + "/group-checkpoint.json"
	src := NewMemorySource(2, 512)
	produce := func(part int, off int64, ts time.Time, v int64) {
		t.Helper()
		if err := src.ProduceNonBlocking(rec(part, off, ts, v)); err != nil {
			t.Fatal(err)
		}
	}
	// 窗口 [0,10)：分区0 三条、分区1 两条；分区1 再发 t=12s 推进全局水位线到 10s。
	produce(0, 0, t0, 1)
	produce(0, 1, t0.Add(5*time.Second), 2)
	produce(1, 0, t0.Add(1*time.Second), 4)
	produce(1, 1, t0.Add(12*time.Second), 8)  // p1 水位线 12s
	produce(0, 2, t0.Add(12*time.Second), 16) // p0 水位线 12s -> 全局 12s，窗口触发

	newProc := func() *Processor {
		return NewProcessor(NewTumblingAssigner(10*time.Second), 2*time.Second, 5*time.Second)
	}
	coord := NewGroupCoordinator("g", []int{0, 1})
	oldAss := coord.ReplaceAll([]string{"old-a", "old-b"})

	// —— 老实例批次 ——
	store1 := NewFileCheckpointStore(path)
	proc1 := newProc()
	run1 := NewRunner(src, proc1, store1, coord, 0)
	if err := run1.Start(oldAss); err != nil {
		t.Fatal(err)
	}
	if !eventually(2*time.Second, func() bool { return len(proc1.Results()) == 1 }) {
		t.Fatal("old generation never fired window")
	}
	dumpState(t, "old-batch-final", proc1, run1)
	if err := run1.Stop(); err != nil {
		t.Fatal(err)
	}

	// —— 整批发版：老实例全下线，新实例整批顶上，共享同一份文件断点 ——
	newAss := coord.ReplaceAll([]string{"new-x", "new-y"})
	store2 := NewFileCheckpointStore(path)
	proc2 := newProc()
	run2 := NewRunner(src, proc2, store2, coord, 0)
	if err := run2.Recover(); err != nil {
		t.Fatalf("new batch recover: %v", err)
	}
	dumpState(t, "new-batch-recovered", proc2, run2)

	// 已发过的最终结果不得重发。
	if len(proc2.Results()) != 0 {
		t.Fatalf("new batch must not re-emit final result, got %d", len(proc2.Results()))
	}
	ag := proc2.Aggregates()[WindowKey{Key: "k", Start: t0}]
	if ag == nil || !ag.Fired || ag.Count != 3 || ag.Sum != 7 {
		t.Fatalf("new batch aggregate restore mismatch: %+v", ag)
	}
	// 两分区位点都要从共享断点接上。
	if off := run2.Offsets(); off[0] != 3 || off[1] != 2 {
		t.Fatalf("new batch offsets must resume from shared checkpoint, got %v", off)
	}

	// 新批次继续处理：迟到但可容忍（6s >= WM-L=7s? 乱序容忍2s + 迟到5s => 下界 5s）
	// t=6s 落在允许迟到范围内 -> revision=1；t=3s 超出 -> 侧输出。
	produce(1, 2, t0.Add(6*time.Second), 32)
	produce(0, 3, t0.Add(3*time.Second), 64)
	if err := run2.Start(newAss); err != nil {
		t.Fatal(err)
	}
	if !eventually(2*time.Second, func() bool {
		return len(proc2.Results()) == 1 && len(proc2.SideOutput()) == 1
	}) {
		t.Fatalf("new batch late processing incomplete: results=%d side=%d",
			len(proc2.Results()), len(proc2.SideOutput()))
	}
	if err := run2.Stop(); err != nil {
		t.Fatal(err)
	}
	dumpState(t, "new-batch-final", proc2, run2)
	ag = proc2.Aggregates()[WindowKey{Key: "k", Start: t0}]
	if ag.Count != 4 || ag.Sum != 39 || ag.Revision != 1 {
		t.Fatalf("new batch late revision mismatch: %+v", ag)
	}
	res := proc2.Results()
	if len(res) != 1 || res[0].Revision != 1 || res[0].Sum != 39 {
		t.Fatalf("want exactly one revision after batch replacement, got %+v", res)
	}
	t.Logf("FRESH-BATCH RECOVERY OK: final not re-emitted, revision=%d sum=%d", res[0].Revision, res[0].Sum)
}
