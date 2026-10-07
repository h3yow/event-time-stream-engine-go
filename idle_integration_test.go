package streamengine

import (
	"testing"
	"time"
)

// 场景F（Runner 集成）：两分区消费，其中一个分区长期空闲；
// 空闲超时后由 Runner 的 watermarkLoop 自动释放水位线，窗口自动触发，
// 不需要向空闲分区补发任何"推进事件"。
func TestRunnerIdlePartitionAutoFiresWindows(t *testing.T) {
	src := NewMemorySource(2, 64)
	idleTimeout := 120 * time.Millisecond
	proc := NewProcessorWithIdle(
		NewTumblingAssigner(10*time.Second), 0, 0, idleTimeout, []int{0, 1})
	store := NewMemoryCheckpointStore()
	coord := NewGroupCoordinator("g", []int{0, 1})
	ass := coord.ReplaceAll([]string{"a", "b"})

	// 初始：两分区都只有窗口 [0,10) 内的事件，分区1 此后彻底空闲。
	produce := func(p int, off int64, ts time.Duration, v int64) {
		if err := src.ProduceNonBlocking(rec(p, off, t0.Add(ts), v)); err != nil {
			t.Fatal(err)
		}
	}
	produce(1, 0, 1*time.Second, 4)
	produce(0, 0, 0, 1)
	produce(0, 1, 5*time.Second, 2)

	run := NewRunner(src, proc, store, coord, 0)
	run.SetWatermarkTickInterval(10 * time.Millisecond)
	run.SetLogger(func(format string, args ...interface{}) { t.Logf("[runner] "+format, args...) })
	if err := run.Start(ass); err != nil {
		t.Fatal(err)
	}

	// 等分区1 被判定空闲（此时分区0 也暂无新数据，但接下来的推进会重新激活它）。
	if !eventually(2*time.Second, func() bool {
		for _, st := range proc.PartitionStatuses() {
			if st.Partition == 1 {
				return st.State == PartitionIdle
			}
		}
		return false
	}) {
		t.Fatal("partition 1 never marked idle")
	}
	// 再多等一个 tick 周期，避免与"恰好到龄"的空闲翻转落在同一检测周期里。
	time.Sleep(30 * time.Millisecond)
	t.Logf("partition 1 idle; now hot partition 0 advances to t=15s")

	// 分区1 已空闲后，热分区0 到达 t=15s：分区0 重新 ACTIVE 且独占水位线 -> 窗口自动触发。
	produce(0, 2, 15*time.Second, 3)

	if !eventually(2*time.Second, func() bool { return len(proc.Results()) == 1 }) {
		t.Fatalf("window did not auto-fire after idle partition release, results=%d", len(proc.Results()))
	}
	if err := run.Stop(); err != nil {
		t.Fatal(err)
	}
	dumpState(t, "idle-runner-final", proc, run)

	res := proc.Results()
	if res[0].Count != 3 || res[0].Sum != 7 { // p0 的 1+2 与 p1 的 4 都计入 [0,10)
		t.Fatalf("idle auto-fire result mismatch: %+v", res[0])
	}
	states := map[int]PartitionState{}
	for _, st := range proc.PartitionStatuses() {
		states[st.Partition] = st.State
	}
	if states[1] != PartitionIdle {
		t.Fatalf("partition 1 must be IDLE, got %s", states[1])
	}
	if states[0] != PartitionActive {
		t.Fatalf("partition 0 must remain ACTIVE, got %s", states[0])
	}
	t.Logf("IDLE RUNNER OK: window auto-fired count=%d sum=%d p1=IDLE", res[0].Count, res[0].Sum)
}
