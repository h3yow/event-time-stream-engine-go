package streamengine

import (
	"context"
	"errors"
	"testing"
)

func TestPartitionLog_TryReadAt_BackpressureVsGone(t *testing.T) {
	ctx := context.Background()
	p := NewPartitionLog(0, "p0", []Event{{Key: "a", EventTime: 1, Value: 1}})
	var ee *EngineError

	// 已有数据：正常读取，可重复读取（模拟重复消费）。
	env, err := p.TryReadAt(0)
	if err != nil || env.Offset != 0 || env.Event.Value != 1 {
		t.Fatalf("tryread ready = %+v,%v", env, err)
	}
	if env, err = p.TryReadAt(0); err != nil {
		t.Fatalf("repeated read must work: %v", err)
	}

	// 越界且未关闭：背压，可重试，原因与“读完”明确区分。
	if _, err = p.TryReadAt(1); err == nil || !errors.As(err, &ee) || ee.Reason != ReasonBackpressure {
		t.Fatalf("want backpressure, got %v", err)
	}
	t.Logf("backpressure as expected: %v", err)

	// 阻塞式 ReadAt 在未关闭时等待；追加后立刻拿到同一偏移（不丢数据）。
	got := make(chan Envelope, 1)
	go func() {
		e, rerr := p.ReadAt(ctx, 1)
		if rerr == nil {
			got <- e
		}
	}()
	if _, err := p.Append(Event{Key: "a", EventTime: 2, Value: 2}); err != nil {
		t.Fatal(err)
	}
	if e := <-got; e.Event.Value != 2 {
		t.Fatalf("blocked read got %+v", e)
	}

	// 关闭后越界：source-gone（与背压明确区分）。
	p.Close()
	if _, err = p.TryReadAt(2); err == nil || !errors.As(err, &ee) || ee.Reason != ReasonSourceGone {
		t.Fatalf("want source-gone after close, got %v", err)
	}
}
