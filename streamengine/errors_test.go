package streamengine

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestFailureReasons_Distinguishable(t *testing.T) {
	// 收集所有失败原因，确认它们彼此不同、字符串可辨识。
	reasons := []FailureReason{
		ReasonLateRejected, ReasonDuplicate, ReasonCheckpointRollback,
		ReasonBackpressure, ReasonSourceGone, ReasonClosed,
	}
	seen := map[FailureReason]string{}
	for _, r := range reasons {
		s := r.String()
		if s == "" || strings.Contains(s, "unknown") {
			t.Fatalf("reason %v has no readable name", r)
		}
		if prev, dup := seen[r]; dup {
			t.Fatalf("duplicate reason encoding %v (%s,%s)", r, prev, s)
		}
		seen[r] = s
	}
	t.Logf("failure reasons: %v", seen)
}

// 端到端区分：超容忍迟到 = LateRejected；重复 = Duplicate（计数可观测）；
// 回退 = CheckpointRollback；关闭分区末尾读取 = SourceGone。
func TestFailureReasons_EndToEnd(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryCheckpointStore()

	// 1) 回退原因。
	cp := &Checkpoint{Generation: 1, PartitionNext: map[int]int64{0: 5}}
	if err := store.Save(ctx, cp); err != nil {
		t.Fatal(err)
	}
	err := ValidateStartOffsets(cp.PartitionNext, map[int]int64{0: 1})
	var ee *EngineError
	if !errors.As(err, &ee) || ee.Reason != ReasonCheckpointRollback {
		t.Fatalf("want rollback, got %v", err)
	}

	// 2) SourceGone：关闭分区后读取越界偏移。
	p := NewPartitionLog(0, "p0", []Event{{Key: "a", EventTime: 0, Value: 1}})
	p.Close()
	if _, err := p.ReadAt(ctx, 10); err == nil {
		t.Fatal("expected SourceGone reading past closed log")
	} else if !errors.As(err, &ee) || ee.Reason != ReasonSourceGone {
		t.Fatalf("want source-gone, got %v", err)
	}

	// 3) SourceGone：向已关闭分区追加。
	if _, err := p.Append(Event{}); err == nil || !errors.As(err, &ee) || ee.Reason != ReasonSourceGone {
		t.Fatalf("want source-gone on append closed, got %v", err)
	}

	// 4) LateRejected + Duplicate：经 WindowStore 直接区分（结果枚举不同），
	//    引擎层面分别体现在侧输出与 Stats.Duplicates。
	s := NewWindowStore(10, 0)
	e := Envelope{Partition: 0, Offset: 0, Event: Event{Key: "a", EventTime: 1, Value: 1}}
	s.Add(e, MinWatermark)
	if oc, _, _ := s.Add(e, MinWatermark); oc != outcomeDuplicate {
		t.Fatalf("want duplicate, got %v", oc)
	}
	s.Advance(10)
	s.Advance(11) // 清理窗口（deadline=10，wm=11）
	if oc, _, dl := s.Add(
		Envelope{Partition: 0, Offset: 1, Event: Event{Key: "a", EventTime: 1, Value: 2}}, 11); oc != outcomeRejected || dl != 10 {
		t.Fatalf("want rejected deadline=10, got %v/%d", oc, dl)
	}

	// 5) Closed：未运行就检查点。
	eng := NewEngine(Config{WindowSize: 10})
	if err := eng.Checkpoint(ctx); err == nil || !errors.As(err, &ee) || ee.Reason != ReasonClosed {
		t.Fatalf("want closed for checkpoint-before-run, got %v", err)
	}
}
