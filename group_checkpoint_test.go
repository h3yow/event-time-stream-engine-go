package streamengine

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// 新场景3：消费组整批换人（发版全量替换实例）—— 老实例全部下线、新实例全部新起，
// 新实例从共享断点恢复接着算：已发的最终结果不重发、迟到修订继续产生、
// 最终每个窗口的结果与"全程只用一个消费者"完全一致（不重不漏）。
func TestFullConsumerGroupReplacementViaRecovery(t *testing.T) {
	type ev struct {
		part int
		ts   time.Time
		v    int64
	}
	// 第一批（老实例处理）：W0 的 4 条事件 + 两个分区各自的推进事件。
	phase1 := []ev{
		{0, t0.Add(1 * time.Second), 1}, {1, t0.Add(3 * time.Second), 4},
		{0, t0.Add(2 * time.Second), 2}, {1, t0.Add(4 * time.Second), 8},
		{0, t0.Add(15 * time.Second), 16}, {1, t0.Add(16 * time.Second), 32},
	}
	// 第二批（新实例处理）：W0 的迟到修订、W1 的两条、两个分区的关闭事件。
	phase2 := []ev{
		{0, t0.Add(11 * time.Second), 1}, {1, t0.Add(9 * time.Second), 64},
		{1, t0.Add(12 * time.Second), 2}, {0, t0.Add(25 * time.Second), 0},
		{1, t0.Add(26 * time.Second), 0},
	}

	src := NewMemorySource(2, 64)
	store := NewMemoryCheckpointStore()
	coord := NewGroupCoordinator("g", []int{0, 1})
	newProc := func() *Processor {
		return NewProcessor(NewTumblingAssigner(10*time.Second), 2*time.Second, 5*time.Second)
	}
	produce := func(events []ev) {
		t.Helper()
		for _, e := range events {
			if err := src.ProduceNonBlocking(Record{Partition: e.part, Event: Event{Key: "k", EventTime: e.ts, Value: e.v}}); err != nil {
				t.Fatal(err)
			}
		}
	}

	// ---- 老实例：成员 a,b，消费第一批后整体停止（落断点）。
	coord.Join("a")
	ass1 := coord.Join("b")
	proc1 := newProc()
	run1 := NewRunner(src, proc1, store, coord, 0)
	produce(phase1)
	if err := run1.Start(ass1); err != nil {
		t.Fatal(err)
	}
	if !eventually(2*time.Second, func() bool { return len(proc1.Results()) == 1 }) {
		t.Fatal("W0 final never fired on old instances")
	}
	dumpState(t, "old-instances-before-shutdown", proc1, run1)
	if err := run1.Stop(); err != nil {
		t.Fatal(err)
	}

	// ---- 整批换人：a,b 全部下线，全新实例 c,d 上线；新处理器/新运行器从共享断点恢复。
	coord.Leave("a")
	coord.Leave("b")
	coord.Join("c")
	ass2 := coord.Join("d")
	proc2 := newProc()
	run2 := NewRunner(src, proc2, store, coord, 0)
	if err := run2.Recover(); err != nil {
		t.Fatalf("new instances recover: %v", err)
	}
	dumpState(t, "new-instances-after-recover", proc2, run2)
	// 恢复绝不重发已发出的最终结果。
	if n := len(proc2.Results()); n != 0 {
		t.Fatalf("recovered processor must not re-emit final results, got %d", n)
	}

	// ---- 新实例继续消费第二批。
	produce(phase2)
	if err := run2.Start(ass2); err != nil {
		t.Fatal(err)
	}
	// 期望新实例发出 2 个结果：W0 的迟到修订（rev1）+ W1 的最终结果。
	if !eventually(2*time.Second, func() bool { return len(proc2.Results()) == 2 }) {
		t.Fatalf("new instances should emit W0 revision + W1 final, got %+v", proc2.Results())
	}
	dumpState(t, "new-instances-after-processing", proc2, run2)
	if err := run2.Stop(); err != nil {
		t.Fatal(err)
	}

	// ---- 对照组：同一事件流全程只用一个处理器算一遍。
	baseline := newProc()
	baseline.TrackPartitions(0, 1)
	offs := map[int]int64{}
	for _, e := range append(append([]ev{}, phase1...), phase2...) {
		baseline.Process(Record{Partition: e.part, Offset: offs[e.part], Event: Event{Key: "k", EventTime: e.ts, Value: e.v}})
		offs[e.part]++
	}
	dumpState(t, "single-consumer-baseline", baseline, nil)

	// 新老实例的结果合并后，必须与对照组完全一致：每个窗口最终结果恰好一次、
	// 修订链一致；且 W0 的最终结果只由老实例发出、新实例没有重发。
	combined := append(append([]Result{}, proc1.Results()...), proc2.Results()...)
	assertSameResults(t, "final", baseline.Results(), combined, 0)
	assertSameResults(t, "revision", baseline.Results(), combined, 1)
	for _, r := range proc2.Results() {
		if r.Window.Start.Equal(t0) && r.Revision == 0 {
			t.Fatalf("new instances must not re-emit W0 final result: %+v", r)
		}
	}
	// 新实例上的 W0 聚合 = 老实例的 4 条（1+4+2+8=15）+ 恢复后 1 条迟到修订（+64）。
	ag := proc2.Aggregates()[WindowKey{Key: "k", Start: t0}]
	if ag == nil || ag.Count != 5 || ag.Sum != 79 || ag.Revision != 1 || !ag.Fired {
		t.Fatalf("W0 aggregate after replacement mismatch: %+v", ag)
	}
}

// assertSameResults 比较两组结果中指定 revision 档位的部分：
// 每个窗口在该档位必须恰好一条，且 count/sum 一致。
func assertSameResults(t *testing.T, label string, want, got []Result, revision int64) {
	t.Helper()
	collect := func(rs []Result) map[time.Time]Result {
		m := map[time.Time]Result{}
		for _, r := range rs {
			if r.Revision != revision {
				continue
			}
			if _, dup := m[r.Window.Start]; dup {
				t.Fatalf("%s: duplicate result for window %s revision=%d", label, r.Window.Start, revision)
			}
			m[r.Window.Start] = r
		}
		return m
	}
	wm, gm := collect(want), collect(got)
	if len(wm) != len(gm) {
		t.Fatalf("%s: want %d windows got %d (want=%v got=%v)", label, len(wm), len(gm), wm, gm)
	}
	for start, wr := range wm {
		gr, ok := gm[start]
		if !ok || gr.Count != wr.Count || gr.Sum != wr.Sum {
			t.Fatalf("%s: window %s want %+v got %+v", label, start, wr, gr)
		}
	}
}

// 新场景4：同一运行器内整批换成员（a,b 全下、c,d 全上，无交集），
// 窗口结果仍恰好一次，换人后迟到修订继续工作。
func TestFullMemberSwapRebalance(t *testing.T) {
	const n = 120
	src := NewMemorySource(2, 256)
	var expectSum int64
	for i := 0; i < n; i++ {
		v := int64(i + 1)
		expectSum += v
		// 全部落在窗口 [t0, t0+10s)，换人前不触发。
		if err := src.ProduceNonBlocking(Record{
			Partition: i % 2,
			Event:     Event{Key: "k", EventTime: t0.Add(time.Duration(i%10) * time.Second), Value: v},
		}); err != nil {
			t.Fatal(err)
		}
	}
	produce := func(part int, ts time.Time, v int64) {
		t.Helper()
		if err := src.ProduceNonBlocking(Record{Partition: part, Event: Event{Key: "k", EventTime: ts, Value: v}}); err != nil {
			t.Fatal(err)
		}
	}

	proc := NewProcessor(NewTumblingAssigner(10*time.Second), 2*time.Second, 5*time.Second)
	store := NewMemoryCheckpointStore()
	coord := NewGroupCoordinator("g", []int{0, 1})
	coord.Join("a")
	ass := coord.Join("b") // gen2：a->[0] b->[1]

	run := NewRunner(src, proc, store, coord, 20*time.Millisecond)
	if err := run.Start(ass); err != nil {
		t.Fatal(err)
	}
	if !eventuallyCount(proc, n) {
		t.Fatalf("aggregate count never reached %d before member swap", n)
	}
	dumpState(t, "before-member-swap", proc, run)

	// 整批换人：老成员 a,b 全部退出，全新成员 c,d 顶上（世代 3->6）。
	coord.Leave("a")
	coord.Leave("b")
	coord.Join("c")
	next := coord.Join("d")
	if err := run.Rebalance(next); err != nil {
		t.Fatal(err)
	}
	t.Logf("swapped all members, now at generation %d", coord.Generation())

	// 换人后两个分区各来一条关闭事件，触发 W0 最终结果（恰好一次）。
	produce(0, t0.Add(15*time.Second), 0)
	produce(1, t0.Add(16*time.Second), 0)
	if !eventually(2*time.Second, func() bool { return len(proc.Results()) == 1 }) {
		t.Fatalf("W0 final should fire exactly once after full member swap, got %+v", proc.Results())
	}
	// 换人后迟到修订继续：t0+9 在允许迟到范围内（水位线 t0+13，容忍 5s）。
	produce(1, t0.Add(9*time.Second), 7)
	if !eventually(2*time.Second, func() bool { return len(proc.Results()) == 2 }) {
		t.Fatalf("late revision after swap should be emitted, got %+v", proc.Results())
	}
	if err := run.Stop(); err != nil {
		t.Fatal(err)
	}
	dumpState(t, "after-member-swap", proc, run)

	res := proc.Results()
	if res[0].Revision != 0 || res[0].Count != n || res[0].Sum != expectSum {
		t.Fatalf("W0 final mismatch after swap: %+v (want count=%d sum=%d)", res[0], n, expectSum)
	}
	if res[1].Revision != 1 || res[1].Count != n+1 || res[1].Sum != expectSum+7 {
		t.Fatalf("W0 revision after swap mismatch: %+v", res[1])
	}
	if len(proc.SideOutput()) != 0 {
		t.Fatalf("unexpected side outputs: %d", len(proc.SideOutput()))
	}
	t.Logf("FULL SWAP OK: final count=%d sum=%d revision count=%d sum=%d checkpoints=%d",
		res[0].Count, res[0].Sum, res[1].Count, res[1].Sum, store.LatestID())
}

// recordingStore 包装内存断点存储，记录每一次成功保存的断点，
// 用于事后验证"任意一个断点都是自洽的"。
type recordingStore struct {
	inner *MemoryCheckpointStore
	mu    sync.Mutex
	all   []*Checkpoint
}

func newRecordingStore() *recordingStore {
	return &recordingStore{inner: NewMemoryCheckpointStore()}
}

func (s *recordingStore) Save(cp *Checkpoint) error {
	if err := s.inner.Save(cp); err != nil {
		return err
	}
	s.mu.Lock()
	s.all = append(s.all, cloneCheckpoint(cp))
	s.mu.Unlock()
	return nil
}

func (s *recordingStore) Load() (*Checkpoint, error) { return s.inner.Load() }
func (s *recordingStore) LatestID() int64            { return s.inner.LatestID() }

func (s *recordingStore) saved() []*Checkpoint {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*Checkpoint(nil), s.all...)
}

// 新场景5：高频断点（1ms）与并发处理同时发生 —— 每一个被保存的断点，
// 其"聚合状态 + 消费位点 + 水位线"都必须对应同一个瞬间：
// 从任意采样断点恢复并重放其余记录，最终结果都必须等于全量单跑，不重不漏。
func TestFrequentCheckpointsConcurrentConsistency(t *testing.T) {
	const n = 900
	const parts = 3
	src := NewMemorySource(parts, 1024)

	// 预先构造确定性记录流：每个分区内事件时间单调不减（全部 ON_TIME），
	// offset 即分区内下标。
	recordsByPart := make([][]Record, parts)
	var expectSum int64
	for i := 0; i < n; i++ {
		part := i % parts
		off := int64(len(recordsByPart[part]))
		rec := Record{
			Partition: part,
			Offset:    off,
			Event: Event{
				Key:       "k",
				EventTime: t0.Add(time.Duration(off/10) * time.Second),
				Value:     off + 1,
			},
		}
		recordsByPart[part] = append(recordsByPart[part], rec)
		expectSum += off + 1
	}

	store := newRecordingStore()
	coord := NewGroupCoordinator("g", []int{0, 1, 2})
	coord.Join("a")
	coord.Join("b")
	ass := coord.Join("c")
	proc := NewProcessor(NewTumblingAssigner(10*time.Second), 2*time.Second, 5*time.Second)
	run := NewRunner(src, proc, store, coord, time.Millisecond) // 1ms 高频断点
	if err := run.Start(ass); err != nil {
		t.Fatal(err)
	}
	// 分三批生产，批间留 25ms，保证有断点落在处理中途。
	for batch := 0; batch < 3; batch++ {
		for i := batch * n / 3; i < (batch+1)*n/3; i++ {
			rec := recordsByPart[i%parts][i/parts]
			if err := src.ProduceNonBlocking(rec); err != nil {
				t.Fatal(err)
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !eventuallyCount(proc, n) {
		t.Fatalf("aggregate count never reached %d", n)
	}
	if err := run.Stop(); err != nil {
		t.Fatal(err)
	}
	dumpState(t, "high-freq-checkpoints", proc, run)

	saved := store.saved()
	if len(saved) < 3 {
		t.Fatalf("expected several mid-flight checkpoints, got %d", len(saved))
	}
	for i := 1; i < len(saved); i++ {
		if saved[i].ID <= saved[i-1].ID {
			t.Fatalf("checkpoint IDs must be strictly increasing: %d -> %d", saved[i-1].ID, saved[i].ID)
		}
	}
	t.Logf("saved %d checkpoints during concurrent processing", len(saved))

	// 从采样断点（含处理中途的断点）恢复 + 重放其余记录，
	// 最终结果必须等于全量单跑：任何"状态与位点不在同一瞬间"的断点
	// 都会在这里表现为重复计数或漏记录。
	sampleIdx := map[int]bool{0: true, len(saved) / 4: true, len(saved) / 2: true, 3 * len(saved) / 4: true, len(saved) - 1: true}
	for idx, cp := range saved {
		if !sampleIdx[idx] {
			continue
		}
		proc2 := NewProcessor(NewTumblingAssigner(10*time.Second), 2*time.Second, 5*time.Second)
		proc2.TrackPartitions(0, 1, 2)
		offsets := proc2.Restore(cp)
		proc2.RecoverSeen(offsets)
		for part := 0; part < parts; part++ {
			for off := offsets[part]; off < int64(len(recordsByPart[part])); off++ {
				proc2.Process(recordsByPart[part][off])
			}
		}
		var count, sum int64
		for _, ag := range proc2.Aggregates() {
			count += ag.Count
			sum += ag.Sum
		}
		t.Logf("restore checkpoint id=%d offsets=%v watermark=%s -> replayed totals count=%d sum=%d",
			cp.ID, cp.Offsets, cp.Watermark.Format("15:04:05"), count, sum)
		if count != n || sum != expectSum {
			t.Fatalf("checkpoint id=%d inconsistent: after restore+replay count=%d sum=%d, want count=%d sum=%d",
				cp.ID, count, sum, n, expectSum)
		}
	}
}

// 新场景6：恢复时断点中的位点比源已提交位点更旧 —— 明确拒绝（ErrOffsetRollback），
// 不允许拿旧断点往回退。
func TestRecoverRejectsCheckpointOlderThanCommitted(t *testing.T) {
	src := NewMemorySource(1, 8)
	if err := src.Commit(0, 10); err != nil {
		t.Fatal(err)
	}
	store := NewMemoryCheckpointStore()
	if err := store.Save(&Checkpoint{
		ID:         1,
		CreatedAt:  time.Now(),
		Offsets:    map[int]int64{0: 5}, // 比已提交的 10 更旧
		Partitions: map[int]time.Time{0: t0},
		Aggregates: map[WindowKey]*Aggregate{},
	}); err != nil {
		t.Fatal(err)
	}
	proc := NewProcessor(NewTumblingAssigner(10*time.Second), 2*time.Second, 5*time.Second)
	coord := NewGroupCoordinator("g", []int{0})
	run := NewRunner(src, proc, store, coord, 0)
	err := run.Recover()
	if !errors.Is(err, ErrOffsetRollback) {
		t.Fatalf("recover from stale checkpoint should be rejected with ErrOffsetRollback, got %v", err)
	}
	t.Logf("recover from stale checkpoint rejected as expected: %v", err)
}
