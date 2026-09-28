package streamengine

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

func TestValidateStartOffsets_RollbackRejected(t *testing.T) {
	committed := map[int]int64{0: 10, 1: 5}

	// 回退必须被明确拒绝，且原因可区分。
	err := ValidateStartOffsets(committed, map[int]int64{0: 9, 1: 5})
	if err == nil {
		t.Fatal("rollback must be rejected")
	}
	var ee *EngineError
	if !errors.As(err, &ee) || ee.Reason != ReasonCheckpointRollback {
		t.Fatalf("want ReasonCheckpointRollback, got %v", err)
	}
	t.Logf("rollback rejected as expected: %v", err)

	// 从已提交位置重启（重复消费）：允许，由去重保证不双算。
	if err := ValidateStartOffsets(committed, map[int]int64{0: 10, 1: 5}); err != nil {
		t.Fatalf("restart at committed must be allowed: %v", err)
	}
	// 向前跳：允许；新分区：允许。
	if err := ValidateStartOffsets(committed, map[int]int64{0: 11, 1: 5, 2: 0}); err != nil {
		t.Fatalf("forward skip/new partition must be allowed: %v", err)
	}
}

func TestMemoryStore_GenerationMonotonic(t *testing.T) {
	ctx := context.Background()
	st := NewMemoryCheckpointStore()
	cp1 := &Checkpoint{Generation: 1, PartitionNext: map[int]int64{0: 1}}
	if err := st.Save(ctx, cp1); err != nil {
		t.Fatal(err)
	}
	// 旧代际覆盖 => 拒绝。
	if err := st.Save(ctx, &Checkpoint{Generation: 1}); err == nil {
		t.Fatal("equal generation must be rejected")
	}
	got, err := st.Load(ctx)
	if err != nil || got.Generation != 1 {
		t.Fatalf("load = %+v, %v", got, err)
	}
}

func TestFileStore_RoundTripAndRollbackGuard(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "cp.json")
	st := NewFileCheckpointStore(path)

	if cp, err := st.Load(ctx); err != nil || cp != nil {
		t.Fatalf("empty store: cp=%v err=%v", cp, err)
	}
	cp := &Checkpoint{
		Generation:    3,
		PartitionNext: map[int]int64{0: 42},
		Windows: []WindowSnapshot{{
			Key: "a", Start: 0, End: 10, Sum: 7, Count: 1, Fired: true, Version: 1,
			Records: []RecordRef{{Partition: 0, Offset: 41, Value: 7}},
		}},
		Watermark:    8,
		PartitionMax: map[int]int64{0: 10},
		Registered:   []int{0},
	}
	if err := st.Save(ctx, cp); err != nil {
		t.Fatal(err)
	}
	got, err := st.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(DescribeSnapshot(got))
	if got.Generation != 3 || got.PartitionNext[0] != 42 || len(got.Windows) != 1 {
		t.Fatalf("round trip mismatch: %+v", got)
	}
	// 用新 store 对象（模拟重启后的进程）保存旧代际也必须被拒绝。
	st2 := NewFileCheckpointStore(path)
	if err := st2.Save(ctx, &Checkpoint{Generation: 2}); err == nil {
		t.Fatal("stale generation after restart must be rejected")
	}
}

func TestMergeCheckpoints_GroupStateUnion(t *testing.T) {
	// runnerA 拥有 p0，runnerB 拥有 p1，二者各把同一窗口的不同记录写入共享检查点。
	a := &Checkpoint{
		Generation: 1, PartitionNext: map[int]int64{0: 2},
		PartitionMax: map[int]int64{0: 20}, Registered: []int{0},
		Windows: []WindowSnapshot{{
			Key: "a", Start: 0, End: 10, Fired: true, Version: 1,
			Records: []RecordRef{{0, 0, 3}, {0, 1, 4}},
		}},
	}
	b := &Checkpoint{
		Generation: 1, PartitionNext: map[int]int64{1: 1},
		PartitionMax: map[int]int64{1: 18}, Registered: []int{1},
		Windows: []WindowSnapshot{{
			Key: "a", Start: 0, End: 10, Fired: false,
			Records: []RecordRef{{1, 0, 5}},
		}},
	}
	m := MergeCheckpoints(2, a, b)

	if m.Generation != 2 {
		t.Fatalf("merged gen = %d, want 2", m.Generation)
	}
	if m.PartitionNext[0] != 2 || m.PartitionNext[1] != 1 {
		t.Fatalf("offsets not merged: %+v", m.PartitionNext)
	}
	// min(20,18)-2 = 16
	if m.Watermark != 16 {
		t.Fatalf("merged watermark = %d, want 16", m.Watermark)
	}
	if len(m.Windows) != 1 {
		t.Fatalf("want one merged window, got %d", len(m.Windows))
	}
	w := m.Windows[0]
	if w.Sum != 12 || w.Count != 3 || len(w.Records) != 3 || !w.Fired || w.Version != 1 {
		t.Fatalf("merged window wrong: sum=%d count=%d refs=%d fired=%v v=%d",
			w.Sum, w.Count, len(w.Records), w.Fired, w.Version)
	}

	// 再次合并：同一条记录不能被双算，偏移只进不退。
	m2 := MergeCheckpoints(2, m, a)
	w2 := m2.Windows[0]
	if w2.Sum != 12 || w2.Count != 3 || len(w2.Records) != 3 {
		t.Fatalf("re-merge double-counted: sum=%d count=%d refs=%d", w2.Sum, w2.Count, len(w2.Records))
	}
	if m2.PartitionNext[0] != 2 {
		t.Fatal("offset regressed after merge")
	}
	t.Log(DescribeSnapshot(m2))
}
