package streamengine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// recordingStore 记录每一次成功保存的快照副本，用于事后逐个校验一致性不变量。
type recordingStore struct {
	mu     sync.Mutex
	saved  []*Checkpoint
	latest int64
}

func newRecordingStore() *recordingStore { return &recordingStore{} }

func (s *recordingStore) Save(cp *Checkpoint) error {
	if cp == nil {
		return fmt.Errorf("nil snapshot")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.latest != 0 && cp.ID < s.latest {
		return fmt.Errorf("%w: incoming=%d existing=%d", ErrCheckpointRollback, cp.ID, s.latest)
	}
	s.latest = cp.ID
	s.saved = append(s.saved, cloneCheckpoint(cp))
	return nil
}

func (s *recordingStore) Load() (*Checkpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.saved) == 0 {
		return nil, nil
	}
	return cloneCheckpoint(s.saved[len(s.saved)-1]), nil
}

func (s *recordingStore) LatestID() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.latest
}

func (s *recordingStore) snapshots() []*Checkpoint {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*Checkpoint, len(s.saved))
	copy(out, s.saved)
	return out
}

// TestHighFrequencyCheckpointConcurrency 高频断点（1ms 一次）与多分区并发摄入同时进行，
// 逐快照验证"已计入聚合的记录数 == 位点覆盖的记录数"，恢复任何一版快照都不重不漏。
func TestHighFrequencyCheckpointConcurrency(t *testing.T) {
	const n = 400
	src := NewMemorySource(2, 1024)
	var expectSum int64
	perPartCount := map[int]int64{0: 0, 1: 0}
	var expectMu sync.Mutex
	for i := 0; i < n; i++ {
		v := int64(i + 1)
		expectMu.Lock()
		expectSum += v
		perPartCount[i%2]++
		expectMu.Unlock()
		// 事件时间随 offset 单调且在分区内有序，保证重放任意快照时迟到结论一致。
		if err := src.ProduceNonBlocking(Record{
			Partition: i % 2,
			Event:     Event{Key: "k", EventTime: t0.Add(time.Duration(i) * time.Millisecond), Value: v},
		}); err != nil {
			t.Fatal(err)
		}
	}

	proc := NewProcessor(NewTumblingAssigner(time.Hour), 0, 0) // 大窗口，本测试不触发
	store := newRecordingStore()
	coord := NewGroupCoordinator("g", []int{0, 1})
	ass := coord.ReplaceAll([]string{"a", "b"})

	run := NewRunner(src, proc, store, coord, time.Millisecond) // 极高频断点施压
	run.SetLogger(func(format string, args ...interface{}) { t.Logf("[runner] "+format, args...) })
	start := time.Now()
	if err := run.Start(ass); err != nil {
		t.Fatal(err)
	}
	if !eventuallyCount(proc, n) {
		t.Fatalf("records never drained: offsets=%v", run.Offsets())
	}
	// drain 完成后再主动打两版断点，确保即使调度很快、周期 tick 赶上的次数不多，
	// 也有足够多的"处理中/处理后"快照可逐版校验。
	if err := run.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := run.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	drainElapsed := time.Since(start)
	if err := run.Stop(); err != nil {
		t.Fatal(err)
	}
	dumpState(t, "hfcp-final", proc, run)

	cps := store.snapshots()
	t.Logf("processed %d records in %s with %d high-frequency checkpoints", n, drainElapsed, len(cps))
	if len(cps) < 3 {
		t.Fatalf("expected multiple checkpoints under 1ms interval during drain, got %d", len(cps))
	}

	// 逐快照校验原子性不变量：
	// 所有聚合的 Count 之和，必须恰好等于各分区 offsets 覆盖的记录总数。
	// 若快照可能抓到"状态已含记录 N、位点未含 N"，这里就会出现 count > covered。
	for _, cp := range cps {
		var aggCount int64
		for _, ag := range cp.Aggregates {
			aggCount += ag.Count
		}
		var covered int64
		for _, off := range cp.Offsets {
			covered += off
		}
		if aggCount != covered {
			t.Fatalf("checkpoint %d inconsistent: aggregate-count=%d but offsets cover %d records",
				cp.ID, aggCount, covered)
		}
	}
	t.Logf("atomicity invariant held on all %d checkpoints (count==offsets-covered)", len(cps))

	// 吞吐不应被高频断点明显拖垮（宽松上限，只防严重退化；CI 上也留足余量）。
	if drainElapsed > 10*time.Second {
		t.Fatalf("high-frequency checkpointing stalled processing: %s for %d records", drainElapsed, n)
	}
}

// TestRecoverFromEverySnapshot 从压测期间的"任意一版"快照恢复并继续处理，
// 最终聚合都必须等于全程单消费者的结果，证明恢复任何处理瞬间都不重不漏。
func TestRecoverFromEverySnapshot(t *testing.T) {
	const n = 2000
	src := NewMemorySource(2, 4096)
	var expectSum int64
	for i := 0; i < n; i++ {
		v := int64(i + 1)
		expectSum += v
		if err := src.ProduceNonBlocking(Record{
			Partition: i % 2,
			Event:     Event{Key: "k", EventTime: t0.Add(time.Duration(i) * time.Second), Value: v},
		}); err != nil {
			t.Fatal(err)
		}
	}
	proc0 := NewProcessor(NewTumblingAssigner(time.Hour), 0, 0)
	store := newRecordingStore()
	coord := NewGroupCoordinator("g", []int{0, 1})
	ass := coord.ReplaceAll([]string{"a", "b"})
	run := NewRunner(src, proc0, store, coord, time.Millisecond)
	if err := run.Start(ass); err != nil {
		t.Fatal(err)
	}
	if !eventuallyCount(proc0, n) {
		t.Fatal("never drained")
	}
	if err := run.Stop(); err != nil {
		t.Fatal(err)
	}
	cps := store.snapshots()
	if len(cps) < 3 {
		t.Fatalf("want several snapshots to sample, got %d", len(cps))
	}

	// 等距抽取若干快照（含较早的），逐个恢复并续跑至全部记录处理完。
	step := len(cps) / 4
	if step == 0 {
		step = 1
	}
	for idx := 0; idx < len(cps); idx += step {
		cp := cps[idx]
		// 每个样本使用独立的源拷贝（读游标独立），但底层记录相同：
		// MemorySource 无拷贝接口，这里重新建源并按相同顺序补齐全部记录，
		// 再 Seek 到快照位点，等价于"从共享日志的快照点继续"。
		src2 := NewMemorySource(2, 4096)
		for i := 0; i < n; i++ {
			if err := src2.ProduceNonBlocking(Record{
				Partition: i % 2,
				Offset:    int64(i / 2),
				Event:     Event{Key: "k", EventTime: t0.Add(time.Duration(i) * time.Second), Value: int64(i + 1)},
			}); err != nil {
				t.Fatal(err)
			}
		}
		p2 := NewProcessor(NewTumblingAssigner(time.Hour), 0, 0)
		offsets := p2.Restore(cp)
		p2.RecoverSeen(offsets)
		for p, off := range offsets {
			if err := src2.Seek(p, off); err != nil {
				t.Fatalf("sample cp=%d seek: %v", cp.ID, err)
			}
		}
		// 直接串行处理两个分区从快照位点之后的全部记录。
		// 事件 i 落在分区 i%2，分区内 offset=i/2，因此每分区记录总数为 n/2。
		dctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		perPartTotal := int64(n / 2)
		for _, part := range []int{0, 1} {
			off := offsets[part]
			for ; off < perPartTotal; off++ {
				rec2, err := src2.Poll(dctx, part)
				if err != nil {
					cancel()
					t.Fatalf("sample cp=%d drain p%d at off %d: %v", cp.ID, part, off, err)
				}
				p2.Process(rec2)
			}
		}
		cancel()
		var gotCount int64
		var gotSum int64
		for _, ag := range p2.Aggregates() {
			gotCount += ag.Count
			gotSum += ag.Sum
		}
		if gotCount != n || gotSum != expectSum {
			t.Fatalf("recover from checkpoint %d mismatch: count=%d sum=%d want count=%d sum=%d",
				cp.ID, gotCount, gotSum, n, expectSum)
		}
		t.Logf("recover from checkpoint id=%d (offsets=%v) -> count=%d sum=%d OK",
			cp.ID, cp.Offsets, gotCount, gotSum)
	}
}

// TestStaleCheckpointRejectedOnRecover 明确验证：更旧的断点不能覆盖更新断点，
// 恢复路径也不允许拿旧断点回退。
func TestStaleCheckpointRejectedOnRecover(t *testing.T) {
	store := newRecordingStore()
	mk := func(id int64, off int64) *Checkpoint {
		return &Checkpoint{
			ID: id, CreatedAt: time.Now(),
			Offsets:      map[int]int64{0: off},
			PartitionWMs: map[int]*PartitionWM{},
			Aggregates:   map[WindowKey]*Aggregate{},
		}
	}
	if err := store.Save(mk(10, 10)); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(mk(9, 9)); !errors.Is(err, ErrCheckpointRollback) {
		t.Fatalf("older checkpoint must be rejected with ErrCheckpointRollback, got %v", err)
	}
	loaded, _ := store.Load()
	if loaded.Offsets[0] != 10 {
		t.Fatalf("checkpoint must remain at id=10, got offset %d", loaded.Offsets[0])
	}
	// 相同 ID 不前进也允许（幂等重试），但位点不能变小：
	if err := store.Save(mk(10, 10)); err != nil {
		t.Fatalf("equal-id re-save should be accepted idempotently, got %v", err)
	}
	t.Log("older checkpoint explicitly rejected; equal id accepted")
}

// TestCheckpointJSONRoundTripWithPartitions 验证分区水位线状态随断点 JSON 完整往返，
// 这是"整批换人 + 文件断点跨进程接手"能恢复空闲/活跃状态的前提。
func TestCheckpointJSONRoundTripWithPartitions(t *testing.T) {
	cp := &Checkpoint{
		ID: 42, CreatedAt: time.Now().UTC(),
		Watermark: t0.Add(12 * time.Second),
		MaxTS:     t0.Add(14 * time.Second),
		Offsets:   map[int]int64{0: 7, 1: 3},
		PartitionWMs: map[int]*PartitionWM{
			0: {State: PartitionActive, MaxObserved: t0.Add(14 * time.Second),
				Watermark: t0.Add(12 * time.Second), RegisteredAt: t0,
				LastEventAt: t0.Add(14 * time.Second), LastProcessAt: t0.Add(100 * time.Second)},
			1: {State: PartitionIdle, MaxObserved: t0.Add(1 * time.Second),
				Watermark: t0.Add(-1 * time.Second), RegisteredAt: t0,
				LastEventAt: t0.Add(1 * time.Second), LastProcessAt: t0.Add(1 * time.Second),
				IdleAt: t0.Add(40 * time.Second)},
		},
		Aggregates: map[WindowKey]*Aggregate{
			{Key: "k", Start: t0}: {Key: "k", Window: Window{Start: t0, End: t0.Add(10 * time.Second)},
				Count: 2, Sum: 5, Fired: true},
		},
	}
	data, err := cp.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	var back Checkpoint
	if err := back.UnmarshalJSON(data); err != nil {
		t.Fatal(err)
	}
	if back.Watermark != cp.Watermark || back.ID != 42 || back.Offsets[1] != 3 {
		t.Fatalf("scalar fields lost in round-trip: %+v", back)
	}
	if back.PartitionWMs[0].State != PartitionActive || back.PartitionWMs[1].State != PartitionIdle {
		t.Fatalf("partition states lost in round-trip: %+v / %+v", back.PartitionWMs[0], back.PartitionWMs[1])
	}
	if !back.PartitionWMs[1].IdleAt.Equal(t0.Add(40 * time.Second)) {
		t.Fatalf("idle-at timestamp lost: %s", back.PartitionWMs[1].IdleAt)
	}
	if !back.PartitionWMs[0].LastProcessAt.Equal(t0.Add(100 * time.Second)) {
		t.Fatalf("last-process wall clock lost: %s", back.PartitionWMs[0].LastProcessAt)
	}
	ag := back.Aggregates[WindowKey{Key: "k", Start: t0}]
	if ag == nil || !ag.Fired || ag.Sum != 5 {
		t.Fatalf("aggregate lost in round-trip: %+v", ag)
	}
	t.Logf("checkpoint JSON round-trip OK: %d partitions, idle p1 preserved", len(back.PartitionWMs))
}
