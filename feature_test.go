package streamengine

import (
	"sync"
	"testing"
	"time"
)

// fakeClock 是可手动推进的时钟，让空闲判定在单测中确定性发生。
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// 新场景1：分区冷热不均 —— 热分区跑得快，不能单方面推高水位线，
// 冷分区正常到达的事件必须仍判 ON_TIME；水位线由最慢的活跃分区决定。
func TestUnevenPartitionProgress(t *testing.T) {
	p := NewProcessor(NewTumblingAssigner(10*time.Second), 2*time.Second, 5*time.Second)
	clock := &fakeClock{now: time.Now()}
	p.SetClock(clock.Now)
	p.TrackPartitions(0, 1)

	feed := func(part int, off int64, ts time.Time, v int64) ProcessResult {
		res := p.Process(Record{Partition: part, Offset: off, Event: Event{Key: "k", EventTime: ts, Value: v}})
		t.Logf("feed partition=%d offset=%d eventTime=%s -> %s watermark=%s",
			part, off, ts.Format("15:04:05"), res.Disposition, res.Watermark.Format("15:04:05"))
		return res
	}

	// 热分区 0 一路冲到 t0+100s。
	feed(0, 0, t0, 1)
	feed(0, 1, t0.Add(50*time.Second), 2)
	feed(0, 2, t0.Add(100*time.Second), 3)
	// 分区 1 还一条没来：水位线必须被拖在零点，热分区不能单方面推进。
	if !p.Watermark().IsZero() {
		t.Fatalf("hot partition must not advance watermark alone, got %s", p.Watermark())
	}

	// 冷分区 1 正常到达的事件：在旧的全局最大水位线语义下会被误判成"太迟"，
	// 现在必须是 ON_TIME。
	if res := feed(1, 0, t0.Add(5*time.Second), 10); res.Disposition != DispositionOnTime {
		t.Fatalf("cold partition event misjudged: want ON_TIME got %s", res.Disposition)
	}
	// 水位线 = min(100-2, 5-2) = t0+3s，由最慢的活跃分区决定。
	if want := t0.Add(3 * time.Second); !p.Watermark().Equal(want) {
		t.Fatalf("watermark want %s got %s", want, p.Watermark())
	}

	// 冷分区追上来到 t0+102s：水位线 = min(98, 100) = t0+98s，
	// 触发 [0,10) 与 [50,60) 两个窗口。
	feed(1, 1, t0.Add(102*time.Second), 4)
	dumpState(t, "uneven-progress", p, nil)

	results := p.Results()
	if len(results) != 2 {
		t.Fatalf("want 2 fired windows, got %d: %+v", len(results), results)
	}
	byWindow := map[time.Time]Result{}
	for _, r := range results {
		byWindow[r.Window.Start] = r
	}
	if r := byWindow[t0]; r.Count != 2 || r.Sum != 11 || r.Revision != 0 {
		t.Fatalf("window [0,10) must include cold-partition event: %+v", r)
	}
	if r := byWindow[t0.Add(50*time.Second)]; r.Count != 1 || r.Sum != 2 {
		t.Fatalf("window [50,60) mismatch: %+v", r)
	}
	if len(p.SideOutput()) != 0 {
		t.Fatalf("no event should be side-output, got %d", len(p.SideOutput()))
	}
}

// 新场景2：分区彻底空闲 —— 长时间无数据的分区不能永远拖住水位线；
// 空闲判定后窗口得以触发；空闲分区恢复来数时，其"补交"事件按既有迟到规则处理
// （可容忍则修订，超容忍则侧输出，且侧输出带判定时的水位线便于排查）。
func TestIdlePartitionDoesNotBlockWatermark(t *testing.T) {
	p := NewProcessor(NewTumblingAssigner(10*time.Second), 2*time.Second, 5*time.Second)
	clock := &fakeClock{now: time.Now()}
	p.SetClock(clock.Now)
	p.SetPartitionIdleTimeout(time.Minute)
	p.TrackPartitions(0, 1)

	feed := func(part int, off int64, ts time.Time, v int64) ProcessResult {
		res := p.Process(Record{Partition: part, Offset: off, Event: Event{Key: "k", EventTime: ts, Value: v}})
		t.Logf("feed partition=%d offset=%d eventTime=%s -> %s watermark=%s",
			part, off, ts.Format("15:04:05"), res.Disposition, res.Watermark.Format("15:04:05"))
		return res
	}

	feed(0, 0, t0, 1)
	feed(0, 1, t0.Add(12*time.Second), 2)
	// 分区 1 一直无数据：水位线被拖在零点，窗口发不出。
	if !p.Watermark().IsZero() {
		t.Fatalf("watermark should be held back by data-less partition 1, got %s", p.Watermark())
	}
	if len(p.Results()) != 0 {
		t.Fatalf("no window should fire while partition 1 holds the watermark")
	}

	// 分区 1 空闲超过 1 分钟；期间分区 0 又来了新数据（保持自身活跃）。
	clock.Advance(2 * time.Minute)
	feed(0, 2, t0.Add(25*time.Second), 3)
	// 空闲检测：分区 1 被判空闲并退出 min 计算，水位线推进到 25-2=t0+23s，
	// 窗口 [0,10) 与 [10,20) 得以触发。
	fired := p.CheckIdle()
	if want := t0.Add(23 * time.Second); !p.Watermark().Equal(want) {
		t.Fatalf("watermark after idle detection want %s got %s", want, p.Watermark())
	}
	if len(fired) != 2 {
		t.Fatalf("idle detection should fire 2 windows, got %d: %+v", len(fired), fired)
	}

	// 空闲分区 1 恢复来数：t0+19 在允许迟到范围内（>= 23-5）→ 修订结果；
	if res := feed(1, 0, t0.Add(19*time.Second), 10); res.Disposition != DispositionLateAccepted {
		t.Fatalf("reactivated partition event want LATE_ACCEPTED got %s", res.Disposition)
	}
	// t0+15 超出容忍（< 23-5）→ 侧输出，且带判定时的水位线。
	if res := feed(1, 1, t0.Add(15*time.Second), 100); res.Disposition != DispositionLateSideOutput {
		t.Fatalf("event beyond tolerance want LATE_SIDE_OUTPUT got %s", res.Disposition)
	}
	dumpState(t, "idle-partition", p, nil)

	side := p.SideOutput()
	if len(side) != 1 || !side[0].Watermark.Equal(t0.Add(23*time.Second)) {
		t.Fatalf("side output should carry judgment-time watermark t0+23s: %+v", side)
	}
	// 水位线单调：空闲分区恢复来数不会把水位线拉回去。
	if want := t0.Add(23 * time.Second); !p.Watermark().Equal(want) {
		t.Fatalf("watermark must stay monotonic, want %s got %s", want, p.Watermark())
	}
	// 窗口 [10,20)：1 个准时事件 + 1 个可容忍迟到修订；超容忍的 100 不计入。
	ag := p.Aggregates()[WindowKey{Key: "k", Start: t0.Add(10 * time.Second)}]
	if ag == nil || ag.Count != 2 || ag.Sum != 12 || ag.Revision != 1 || !ag.Fired {
		t.Fatalf("window [10,20) aggregate mismatch: %+v", ag)
	}
}

// 新场景2b：全部分区都空闲（整体输入静默）—— 水位线追平最远观测进度，
// 窗口得以关闭，不会"数据停了窗口永远发不出"。
func TestAllPartitionsIdleWatermarkCatchesUp(t *testing.T) {
	p := NewProcessor(NewTumblingAssigner(10*time.Second), 2*time.Second, 5*time.Second)
	clock := &fakeClock{now: time.Now()}
	p.SetClock(clock.Now)
	p.SetPartitionIdleTimeout(time.Minute)
	p.TrackPartitions(0, 1)

	feed := func(part int, off int64, ts time.Time, v int64) {
		res := p.Process(Record{Partition: part, Offset: off, Event: Event{Key: "k", EventTime: ts, Value: v}})
		t.Logf("feed partition=%d offset=%d eventTime=%s -> %s watermark=%s",
			part, off, ts.Format("15:04:05"), res.Disposition, res.Watermark.Format("15:04:05"))
	}

	feed(0, 0, t0.Add(2*time.Second), 1)
	feed(0, 1, t0.Add(12*time.Second), 10)
	feed(1, 0, t0.Add(3*time.Second), 2)
	feed(1, 1, t0.Add(11*time.Second), 20)
	// 水位线 = min(12-2, 11-2) = t0+9s，窗口 [0,10) 还差 1 秒不能触发。
	if want := t0.Add(9 * time.Second); !p.Watermark().Equal(want) {
		t.Fatalf("watermark want %s got %s", want, p.Watermark())
	}
	if len(p.Results()) != 0 {
		t.Fatalf("window [0,10) must not fire at watermark t0+9s")
	}

	// 两个分区都静默超过空闲超时：水位线追平最远观测进度 max(10, 9) = t0+10s，
	// 窗口 [0,10) 关闭。
	clock.Advance(2 * time.Minute)
	fired := p.CheckIdle()
	if want := t0.Add(10 * time.Second); !p.Watermark().Equal(want) {
		t.Fatalf("all-idle watermark should catch up to %s, got %s", want, p.Watermark())
	}
	if len(fired) != 1 || fired[0].Count != 2 || fired[0].Sum != 3 || fired[0].Revision != 0 {
		t.Fatalf("all-idle catch-up should fire window [0,10) once: %+v", fired)
	}
	dumpState(t, "all-idle-catch-up", p, nil)
}
