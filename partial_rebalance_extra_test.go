package streamengine

import (
	"testing"
	"time"
)

// 场景G：只增减部分成员（混合再均衡，与"整批换人"区分）。
// 3 分区上 a,b,c 各持一个分区 -> b 离开 -> 再加入 d，
// 全过程结果与全程单消费者一致，最终结果只发一次。
func TestPartialMembershipChanges(t *testing.T) {
	const n = 240
	src := NewMemorySource(3, 512)
	var expectSum int64
	for i := 0; i < n; i++ {
		v := int64(i + 1)
		expectSum += v
		if err := src.ProduceNonBlocking(Record{
			Partition: i % 3,
			Event:     Event{Key: "k", EventTime: t0.Add(time.Duration(i%10) * time.Second), Value: v},
		}); err != nil {
			t.Fatal(err)
		}
	}
	proc := NewProcessor(NewTumblingAssigner(10*time.Second), 0, 0)
	store := NewMemoryCheckpointStore()
	coord := NewGroupCoordinator("g", []int{0, 1, 2})
	ass := coord.ReplaceAll([]string{"a", "b", "c"}) // 每成员一个分区
	run := NewRunner(src, proc, store, coord, 20*time.Millisecond)
	run.SetLogger(func(format string, args ...interface{}) { t.Logf("[runner] "+format, args...) })
	if err := run.Start(ass); err != nil {
		t.Fatal(err)
	}
	time.Sleep(15 * time.Millisecond)

	// 部分变更1：b 离开，其分区由 a/c 接管。
	if err := run.Rebalance(coord.Leave("b")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(15 * time.Millisecond)
	// 部分变更2：新成员 d 加入。
	if err := run.Rebalance(coord.Join("d")); err != nil {
		t.Fatal(err)
	}

	if !eventuallyCount(proc, n) {
		t.Fatalf("not all records processed after partial changes: %v", run.Offsets())
	}
	dumpState(t, "partial-drained", proc, run)

	// 全部 3 个分区各发关闭事件推进各自水位线。
	for _, part := range []int{0, 1, 2} {
		if err := src.ProduceNonBlocking(rec(part, 100000, t0.Add(15*time.Second), 0)); err != nil {
			t.Fatal(err)
		}
	}
	if !eventually(2*time.Second, func() bool { return len(proc.Results()) == 1 }) {
		t.Fatalf("want exactly 1 final result after partial changes, got %d", len(proc.Results()))
	}
	if err := run.Stop(); err != nil {
		t.Fatal(err)
	}
	res := proc.Results()
	if res[0].Count != n || res[0].Sum != expectSum {
		t.Fatalf("partial rebalance exactly-once violated: count=%d sum=%d want %d/%d",
			res[0].Count, res[0].Sum, n, expectSum)
	}
	t.Logf("PARTIAL CHANGES OK: count=%d sum=%d gen=%d", res[0].Count, res[0].Sum, coord.Generation())
}
