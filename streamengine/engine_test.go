package streamengine

import (
	"context"
	"errors"
	"sync"
	"testing"
)

// collector 收集结果通道与侧输出。
type collector struct {
	mu      sync.Mutex
	results []WindowResult
	late    []LateRejection
	done    chan struct{}
}

func collect(e *Engine) *collector {
	c := &collector{done: make(chan struct{})}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for r := range e.Outputs() {
			c.mu.Lock()
			c.results = append(c.results, r)
			c.mu.Unlock()
		}
	}()
	go func() {
		defer wg.Done()
		for l := range e.LateEvents() {
			c.mu.Lock()
			c.late = append(c.late, l)
			c.mu.Unlock()
		}
	}()
	go func() { wg.Wait(); close(c.done) }()
	return c
}

func (c *collector) snapshot() ([]WindowResult, []LateRejection) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]WindowResult(nil), c.results...), append([]LateRejection(nil), c.late...)
}

func (c *collector) wait() { <-c.done }

// baselineEvents：乱序但都在容忍内的数据，单分区，窗口 size=10。
func baselineEvents() []Event {
	return []Event{
		{Key: "a", EventTime: 1, Value: 1},
		{Key: "a", EventTime: 12, Value: 2},
		{Key: "a", EventTime: 3, Value: 3}, // 乱序迟到（仍在同一窗口、触发前）
		{Key: "b", EventTime: 2, Value: 10},
		{Key: "b", EventTime: 22, Value: 10}, // 推进水位线
	}
}

// baselineResult 是一次性跑完基线数据得到的结果快照。
type baselineResult struct {
	results []WindowResult
	late    []LateRejection
}

func runBaseline(t *testing.T) (*Engine, baselineResult) {
	t.Helper()
	p := NewPartitionLog(0, "p0", baselineEvents())
	p.Close() // 有界基线数据：关闭后读循环在末尾正常结束
	src := NewSource(p)
	eng := NewEngine(Config{WindowSize: 10, MaxOutOfOrderness: 2, AllowedLateness: 0})
	eng.BindSource(src)
	c := collect(eng)
	if err := eng.Run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	c.wait()
	results, late := c.snapshot()
	return eng, baselineResult{results: results, late: late}
}

func TestEngine_OutOfOrder_Baseline(t *testing.T) {
	eng, base := runBaseline(t)

	t.Logf("watermark=%d", eng.CurrentWatermark())
	t.Logf("offsets=%v", eng.CommittedOffsets())
	t.Log(eng.Snapshot())
	for _, r := range base.results {
		t.Logf("RESULT %s", r)
	}
	if len(base.late) != 0 {
		t.Fatalf("baseline should have no late rejections: %+v", base.late)
	}

	// 期望窗口最终值（finish 用 +∞ 冲发）：
	want := map[string][2]int64{
		"a@0":  {4, 2},  // [0,10): t=1,3 sum=4 count=2
		"a@10": {2, 1},  // [10,20): t=12
		"b@0":  {10, 1}, // [0,10): t=2
		"b@20": {10, 1}, // [20,30): t=22
	}
	got := map[string][2]int64{}
	for _, r := range base.results {
		got[r.Key+"@"+itoa(r.Window.Start)] = [2]int64{r.Sum, r.Count}
	}
	for k, w := range want {
		if got[k] != w {
			t.Fatalf("window %s = %v, want %v; all=%v", k, got[k], w, got)
		}
	}
}

func TestEngine_DuplicateConsumption_Dedup(t *testing.T) {
	// 语义：
	//  1) 检查点提交的位置与窗口状态是一个原子边界；从该边界重启，
	//     未提交区域可能被源重放，窗口记录级去重保证不双算；
	//  2) 显式从早于已提交的位置启动（断点回退）必须被明确拒绝。
	_, baseline := runBaseline(t)

	cfg := Config{WindowSize: 10, MaxOutOfOrderness: 2, AllowedLateness: 0}
	all := baselineEvents()
	// 只预载前两条，确保检查点确定地落在 offset=2。
	p := NewPartitionLog(0, "p0", all[:2])
	src := NewSource(p)
	store := NewMemoryCheckpointStore()

	eng := NewEngine(cfg)
	eng.BindSource(src)
	eng.SetCheckpointStore(store)
	c1 := collect(eng) // 记录检查点边界前/后的输出
	runErr := make(chan error, 1)
	go func() { runErr <- eng.Run(context.Background()) }()
	if err := eng.AwaitOffsets(context.Background(), map[int]int64{0: 2}); err != nil {
		t.Fatal(err)
	}
	if err := eng.Checkpoint(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Log("cp-1:", eng.Snapshot())
	// 检查点边界之前已发出并实际交付的窗口结果条数（确定性等待，无轮询）。
	before := int(eng.Stats().Emitted)
	if err := eng.AwaitDelivered(context.Background(), before); err != nil {
		t.Fatal(err)
	}

	// 在检查点之后、进程结束之前，源可能把“已处理但未提交”的记录再投递：
	// 同一 (分区,偏移) 再来一次必须判重。
	dupEnv := Envelope{Partition: 0, Offset: 1, Event: all[1]}
	wmNow := eng.wm.Current()
	eng.mu.Lock()
	oc, _, _ := eng.windows.Add(dupEnv, wmNow)
	eng.mu.Unlock()
	if oc != outcomeDuplicate {
		t.Fatalf("redelivery of processed-but-uncommitted record must dedup, got %v", oc)
	}

	// 追加剩余事件并关闭，让 run1 跑完（含 +∞ 冲发）。
	for _, ev := range all[2:] {
		if _, err := p.Append(ev); err != nil {
			t.Fatal(err)
		}
	}
	if err := eng.AwaitOffsets(context.Background(), map[int]int64{0: 5}); err != nil {
		t.Fatal(err)
	}
	p.Close()
	if err := <-runErr; err != nil {
		t.Fatalf("run1: %v", err)
	}
	c1.wait()
	run1Results, _ := c1.snapshot()
	for _, r := range run1Results {
		t.Logf("run1 RESULT %s", r)
	}

	// 显式回退到 0（早于已提交位置 2）：必须被明确拒绝。
	eng2 := NewEngine(cfg)
	eng2.BindSource(src)
	eng2.SetCheckpointStore(store)
	eng2.SetStartOffsets(map[int]int64{0: 0})
	err := eng2.Run(context.Background())
	var ee *EngineError
	if err == nil || !errors.As(err, &ee) || ee.Reason != ReasonCheckpointRollback {
		t.Fatalf("restart from rolled-back offset must fail with rollback, got %v", err)
	}
	t.Logf("rollback explicitly rejected: %v", err)

	// 正常从检查点（位置 2）继续重启：偏移 2..4 由源重放。
	// 已在检查点前触发的 a[0,10) INITIAL 不能重发；其余结果必须与
	// 不中断运行时“检查点边界之后”的输出逐条一致。
	eng3 := NewEngine(cfg)
	eng3.BindSource(src)
	eng3.SetCheckpointStore(store)
	c3 := collect(eng3)
	if err := eng3.Run(context.Background()); err != nil {
		t.Fatalf("run3: %v", err)
	}
	c3.wait()
	restartResults, _ := c3.snapshot()
	t.Log("restored:", eng3.Snapshot())
	for _, r := range restartResults {
		t.Logf("restart RESULT %s", r)
	}
	wantTail := baseline.results[before:]
	if !resultsEqual(wantTail, restartResults) {
		t.Fatalf("post-checkpoint output differs:\n want=%v\n got =%v", wantTail, restartResults)
	}
	// 重启输出与 run1 检查点后的输出也必须一致（两种路径结论相同）。
	if !resultsEqual(run1Results[before:], restartResults) {
		t.Fatalf("restart tail differs from uninterrupted run tail:\n run1=%v\n rest=%v",
			run1Results[before:], restartResults)
	}
	if s := eng3.Stats(); s.Duplicates != 0 || s.Accepted != 3 {
		t.Fatalf("restart should process exactly the 3 replayed events once: %+v", s)
	}
}

func resultsEqual(a, b []WindowResult) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
