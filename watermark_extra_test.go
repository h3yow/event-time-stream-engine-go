package streamengine

import (
	"testing"
	"time"
)

// controlledClock 是可控墙钟，空闲判定测试用它代替真实时间。
type controlledClock struct{ t time.Time }

func (c *controlledClock) now() time.Time { return c.t }
func (c *controlledClock) advance(d time.Duration) time.Time {
	c.t = c.t.Add(d)
	return c.t
}

// newProcessorWithTestClock 构造使用可控墙钟的处理器（分区在当前时钟点注册）。
func newProcessorWithTestClock(assigner *TumblingAssigner, maxOoo, allowedLate, idleTimeout time.Duration, parts []int, clk *controlledClock) *Processor {
	p := newProcessor(assigner, maxOoo, allowedLate, idleTimeout, clk.now)
	p.wm.RegisterAll(parts)
	return p
}
func rec(partition int, offset int64, ts time.Time, v int64) Record {
	return Record{Partition: partition, Offset: offset, Event: Event{Key: "k", EventTime: ts, Value: v}}
}

// 场景A：分区冷热不均 —— 快分区不能把慢分区的正常事件判迟到；
// 慢分区持续拖住全局水位线（在空闲超时之内），窗口按最慢的活跃分区推进。
func TestPartitionSkewHoldsBackWatermark(t *testing.T) {
	clk := &controlledClock{t: t0}
	// 窗口 10s，乱序容忍 0，空闲超时 1 分钟。
	p := newProcessorWithTestClock(
		NewTumblingAssigner(10*time.Second), 0, 5*time.Second, time.Minute, []int{0, 1}, clk)

	// 分区0 很热：一路跑到 t=30s。
	for i, ts := range []time.Duration{0, 10, 20, 30} {
		r := p.Process(rec(0, int64(i), t0.Add(ts*time.Second), 1))
		t.Logf("hot p0 offset=%d eventTime=%s -> disp=%s globalWM=%s",
			i, t0.Add(ts*time.Second).Format("15:04:05"), r.Disposition, r.Watermark.Format("15:04:05"))
	}
	// 分区1 只在 t=2s 来过一条；墙钟仅前进 5s（远小于空闲超时），仍属活跃（虽冷）。
	clk.advance(5 * time.Second)
	r := p.Process(rec(1, 0, t0.Add(2*time.Second), 1))
	t.Logf("cold p1 eventTime=00:00:02 -> disp=%s globalWM=%s", r.Disposition, r.Watermark.Format("15:04:05"))
	if r.Disposition != DispositionOnTime {
		t.Fatalf("slow partition's normal event must be ON_TIME, got %s", r.Disposition)
	}
	// 全局水位线被分区1 拖在 t=2s，窗口 [0,10) 不能因为分区0 快就提前触发。
	if wm := p.Watermark(); !wm.Equal(t0.Add(2 * time.Second)) {
		t.Fatalf("global watermark must follow slow active partition (want %s), got %s",
			t0.Add(2*time.Second).Format("15:04:05"), wm.Format("15:04:05"))
	}
	if ag := p.Aggregates()[WindowKey{Key: "k", Start: t0}]; ag != nil && ag.Fired {
		t.Fatal("window must not fire while a slow active partition still holds the watermark")
	}
	dumpState(t, "skew-held", p, nil)

	// 慢分区继续正常推进到 t=12s：它自己的事件依旧全部准时，窗口随之触发。
	r = p.Process(rec(1, 1, t0.Add(12*time.Second), 1))
	t.Logf("cold p1 eventTime=00:00:12 -> disp=%s globalWM=%s fired=%d",
		r.Disposition, r.Watermark.Format("15:04:05"), len(r.NewResults))
	if r.Disposition != DispositionOnTime {
		t.Fatalf("slow partition event still ON_TIME when it advances, got %s", r.Disposition)
	}
	dumpState(t, "skew-advanced", p, nil)
}

// 场景B：分区彻底空闲 —— 超过 idleTimeout 没有数据的分区转为 IDLE，
// 不再拖住全局水位线；热分区可以正常触发窗口。状态变化可解释、可观测。
func TestIdlePartitionReleasesWatermark(t *testing.T) {
	clk := &controlledClock{t: t0}
	p := newProcessorWithTestClock(
		NewTumblingAssigner(10*time.Second), 0, 0, 30*time.Second, []int{0, 1}, clk)

	// 两分区各来一条 t=1s 的事件（墙钟 t0），全局水位线 1s。
	p.Process(rec(0, 0, t0.Add(1*time.Second), 1))
	p.Process(rec(1, 0, t0.Add(1*time.Second), 1))
	dumpState(t, "both-active", p, nil)

	// 热分区0 在墙钟 +10s 时推进到 t=20s（中间补一条 [10,20) 的事件）；
	// 分区1 距上次处理才 10s < idleTimeout=30s，仍 ACTIVE，全局水位线被拖住。
	clk.advance(10 * time.Second)
	p.Process(rec(0, 1, t0.Add(11*time.Second), 1))
	r := p.Process(rec(0, 2, t0.Add(20*time.Second), 1))
	t.Logf("hot p0 ->20s at wall+10s, globalWM=%s (p1 still active)", r.Watermark.Format("15:04:05"))
	if !r.Watermark.Equal(t0.Add(1 * time.Second)) {
		t.Fatalf("watermark still held by non-idle p1 at 1s, got %s", r.Watermark.Format("15:04:05"))
	}

	// 墙钟再走 21s（分区1 已 31s 无数据 > idleTimeout），然后热分区0 立刻再来一条，
	// 保证分区0 在本次 Tick 时是"刚刚活动"的 ACTIVE，只有分区1 转为 IDLE。
	clk.advance(21 * time.Second)
	p.Process(rec(0, 3, t0.Add(20*time.Second), 0))
	tr, fired := p.TickWatermarks()
	if len(tr) != 1 || tr[0].Partition != 1 || tr[0].To != PartitionIdle {
		t.Fatalf("want p1 ACTIVE->IDLE transition only, got %+v", tr)
	}
	t.Logf("partition %d marked IDLE at %s, fired=%d", tr[0].Partition, tr[0].At.Format("15:04:05"), len(fired))
	wm := p.Watermark()
	if !wm.Equal(t0.Add(20 * time.Second)) {
		t.Fatalf("idle p1 must release watermark to hot p0's 20s, got %s", wm.Format("15:04:05"))
	}
	dumpState(t, "p1-idle", p, nil)

	// 窗口 [0,10)、[10,20) 都应已随水位线触发。
	for _, start := range []time.Duration{0, 10} {
		ag := p.Aggregates()[WindowKey{Key: "k", Start: t0.Add(start * time.Second)}]
		if ag == nil || !ag.Fired {
			t.Fatalf("window at +%ds must fire once idle partition releases watermark", start)
		}
	}

	// 空闲分区"复活"：带来的更旧事件不能把水位线拉回去，按既有迟到规则处理。
	r = p.Process(rec(1, 1, t0.Add(2*time.Second), 99))
	t.Logf("revived p1 old event -> disp=%s globalWM=%s", r.Disposition, r.Watermark.Format("15:04:05"))
	if r.Watermark.Before(t0.Add(20 * time.Second)) {
		t.Fatalf("idle partition recovery must not move watermark backwards, got %s", r.Watermark.Format("15:04:05"))
	}
	if r.Disposition != DispositionLateSideOutput {
		t.Fatalf("stale event from revived idle partition follows existing lateness rules, want LATE_SIDE_OUTPUT, got %s", r.Disposition)
	}
	dumpState(t, "p1-revived", p, nil)
}

// 场景C：已注册但从未来过数据的分区，在空闲超时内拖住水位线；
// 超时后同样转 IDLE 释放（冷启动/新分区场景）。
func TestWaitingPartitionTimesOutToIdle(t *testing.T) {
	clk := &controlledClock{t: t0}
	p := newProcessorWithTestClock(
		NewTumblingAssigner(10*time.Second), 0, 0, 30*time.Second, []int{0, 1}, clk)

	// 墙钟先走过 31s（分区1 自注册起一直没有任何事件），此时分区0 才来数据。
	clk.advance(31 * time.Second)
	p.Process(rec(0, 0, t0.Add(15*time.Second), 1))
	// 在分区0 保持活跃的同一刻做 Tick：只有空等超时的分区1 应转 IDLE。
	tr, _ := p.TickWatermarks()
	if len(tr) != 1 || tr[0].Partition != 1 || tr[0].From != PartitionWaiting || tr[0].To != PartitionIdle {
		t.Fatalf("want WAITING->IDLE for p1, got %+v", tr)
	}
	if wm := p.Watermark(); !wm.Equal(t0.Add(15 * time.Second)) {
		t.Fatalf("after waiting timeout watermark advances to 15s, got %s", wm.Format("15:04:05"))
	}
	dumpState(t, "waiting-timeout", p, nil)
}
