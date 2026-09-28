package streamengine

import (
	"testing"
)

func TestAssignWindow(t *testing.T) {
	cases := []struct {
		tt, size int64
		start    int64
	}{
		{0, 10, 0}, {9, 10, 0}, {10, 10, 10}, {-1, 10, -10}, {-10, 10, -10},
	}
	for _, c := range cases {
		w := AssignWindow(c.tt, c.size)
		if w.Start != c.start || w.End != c.start+c.size {
			t.Fatalf("AssignWindow(%d,%d)=%s, want start %d", c.tt, c.size, w, c.start)
		}
	}
}

func TestWindowStore_FireCorrectReject(t *testing.T) {
	// size=10, allowedLateness=3：窗口 [0,10) deadline=13。
	s := NewWindowStore(10, 3)
	e := Envelope{Partition: 0, Offset: 0, Event: Event{Key: "a", EventTime: 1, Value: 100}}
	oc, corr, dl := s.Add(e, MinWatermark)
	if oc != outcomeAccepted || corr != nil || dl != 13 {
		t.Fatalf("add1 = %v,%v,%d", oc, corr, dl)
	}

	// 重复记录（同分区同偏移）：去重，不双算。
	oc, _, _ = s.Add(e, MinWatermark)
	if oc != outcomeDuplicate {
		t.Fatalf("duplicate = %v, want outcomeDuplicate", oc)
	}

	// 水位线 10：首次触发。
	fired := s.Advance(10)
	if len(fired) != 1 || fired[0].Kind != RecordInitial || fired[0].Sum != 100 || fired[0].Version != 1 {
		t.Fatalf("initial fire unexpected: %+v", fired)
	}

	// 容忍内迟到 t=2：CORRECTED v2，sum 增加一次。
	oc, corr, _ = s.Add(Envelope{Partition: 0, Offset: 1, Event: Event{Key: "a", EventTime: 2, Value: 5}}, 10)
	if oc != outcomeAccepted || corr == nil || corr.Version != 2 || corr.Sum != 105 {
		t.Fatalf("late-within = %v,%v", oc, corr)
	}

	// 水位线 13：仍可接受（deadline 边界 watermark<=13）。
	oc, corr, _ = s.Add(Envelope{Partition: 0, Offset: 2, Event: Event{Key: "a", EventTime: 3, Value: 7}}, 13)
	if oc != outcomeAccepted || corr == nil || corr.Version != 3 || corr.Sum != 112 {
		t.Fatalf("late-boundary = %v,%v", oc, corr)
	}

	// 推进到 14：窗口过期清理。
	if got := s.Advance(14); len(got) != 0 {
		t.Fatalf("advance past deadline should not refire, got %+v", got)
	}
	if s.WindowCount() != 0 {
		t.Fatalf("expired window must be GC'd, count=%d", s.WindowCount())
	}

	// 超容忍迟到：明确拒绝。
	oc, _, dl = s.Add(Envelope{Partition: 0, Offset: 3, Event: Event{Key: "a", EventTime: 1, Value: 9000}}, 14)
	if oc != outcomeRejected || dl != 13 {
		t.Fatalf("over-lateness = %v deadline=%d, want rejected/13", oc, dl)
	}
}

func TestWindowStore_SnapshotRestoreDedup(t *testing.T) {
	s := NewWindowStore(10, 3)
	s.Add(Envelope{Partition: 0, Offset: 0, Event: Event{Key: "a", EventTime: 1, Value: 10}}, MinWatermark)
	s.Add(Envelope{Partition: 1, Offset: 7, Event: Event{Key: "a", EventTime: 2, Value: 20}}, MinWatermark)
	s.Advance(10)

	snaps := s.Snapshot()
	s2 := NewWindowStore(10, 3)
	s2.Restore(snaps)

	// 恢复后重放同一批记录必须判为重复，聚合不变。
	oc, _, _ := s2.Add(Envelope{Partition: 0, Offset: 0, Event: Event{Key: "a", EventTime: 1, Value: 10}}, 10)
	if oc != outcomeDuplicate {
		t.Fatalf("replayed record must dedup, got %v", oc)
	}
	oc, corr, _ := s2.Add(Envelope{Partition: 1, Offset: 7, Event: Event{Key: "a", EventTime: 2, Value: 20}}, 10)
	if oc != outcomeDuplicate || corr != nil {
		t.Fatalf("replayed cross-partition record must dedup, got %v %v", oc, corr)
	}
	got := s2.Snapshot()
	if len(got) != 1 || got[0].Sum != 30 || got[0].Count != 2 || !got[0].Fired {
		t.Fatalf("restored state mismatch: %+v", got)
	}
}

func TestWatermark_RegisteredIdleAndFinish(t *testing.T) {
	w := NewWatermarkGenerator(2)
	w.Register([]int{0, 1})

	// 只有 p0 有数据：空闲 p1 拖住水位线。
	if wm := w.Observe(0, 100); wm != MinWatermark {
		t.Fatalf("idle p1 must hold watermark, got %d", wm)
	}
	// p1 到达：min(100,50)-2 = 48。
	if wm := w.Observe(1, 50); wm != 48 {
		t.Fatalf("watermark = %d, want 48", wm)
	}
	// 较早事件不回退。
	if wm := w.Observe(1, 30); wm != 48 {
		t.Fatalf("watermark must not regress, got %d", wm)
	}
	// p1 关闭退出：只剩 p0 => 98。
	if wm := w.Finish(1); wm != 98 {
		t.Fatalf("after finish watermark = %d, want 98", wm)
	}
	// p0 也关闭：保持最后水位线，不回落 -∞。
	if wm := w.Finish(0); wm != 98 {
		t.Fatalf("watermark after all finished = %d, want 98", wm)
	}
}
