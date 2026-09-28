// 本地可跑的端到端演示：乱序到达、容忍内迟到修正、超容忍迟到拒绝、
// 中途检查点 + 重启恢复。
//
// 运行：go run ./cmd/demo
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	se "github.com/h3yow/event-time-stream-engine-go/streamengine"
)

func main() {
	ctx := context.Background()
	cpPath := filepath.Join(".", "demo-checkpoint.json")
	_ = os.Remove(cpPath)
	defer os.Remove(cpPath)

	// 窗口 10ms；水位线容忍乱序 2ms；窗口触发后再允许迟到 3ms。
	cfg := se.Config{WindowSize: 10, MaxOutOfOrderness: 2, AllowedLateness: 3}

	p0 := se.NewPartitionLog(0, "p0", []se.Event{
		{Key: "a", EventTime: 1, Value: 1},
		{Key: "a", EventTime: 5, Value: 10},
		{Key: "a", EventTime: 3, Value: 100}, // 乱序：3 在 5 之后到达
		{Key: "a", EventTime: 12, Value: 5},  // p0 自身先推进到 12
	})
	p1 := se.NewPartitionLog(1, "p1", []se.Event{
		{Key: "b", EventTime: 2, Value: 2},
		{Key: "b", EventTime: 9, Value: 20},
	})
	src := se.NewSource(p0, p1)
	store := se.NewFileCheckpointStore(cpPath)

	// ---------- run-1 ----------
	eng := se.NewEngine(cfg)
	eng.BindSource(src)
	eng.SetCheckpointStore(store)
	go drain("run-1", eng)

	run1 := make(chan error, 1)
	go func() { run1 <- eng.Run(ctx) }()

	// 等首批 6 条处理完：水位线 = min(p0:12,p1:9)-2 = 7，窗口尚未触发。
	if err := eng.AwaitOffsets(ctx, map[int]int64{0: 4, 1: 2}); err != nil {
		panic(err)
	}
	if err := eng.Checkpoint(ctx); err != nil {
		panic(err)
	}
	fmt.Println(">>> mid:", eng.Snapshot())

	// p1 推进到 12：水位线 = 10，a[0,10) INITIAL(sum=111)。
	if _, err := p1.Append(se.Event{Key: "b", EventTime: 12, Value: 200}); err != nil {
		panic(err)
	}
	if err := eng.AwaitOffsets(ctx, map[int]int64{1: 3}); err != nil {
		panic(err)
	}
	// 容忍范围内迟到：p0 自身已到 12，t=6 迟到但 deadline=13 => CORRECTED。
	if _, err := p0.Append(se.Event{Key: "a", EventTime: 6, Value: 1000}); err != nil {
		panic(err)
	}
	if err := eng.AwaitOffsets(ctx, map[int]int64{0: 5}); err != nil {
		panic(err)
	}
	// p0 自己继续推进到 17（水位线取各分区 min，必须由慢分区自己越过）。
	if _, err := p0.Append(se.Event{Key: "a", EventTime: 17, Value: 7}); err != nil {
		panic(err)
	}
	if err := eng.AwaitOffsets(ctx, map[int]int64{0: 6}); err != nil {
		panic(err)
	}
	// 此时全局水位线 = min(p0:17,p1:12)-2 = 10；p1 再推到 18 => 水位线 15 > 13，
	// 窗口 [0,10) 过期清理。
	if _, err := p1.Append(se.Event{Key: "b", EventTime: 18, Value: 30}); err != nil {
		panic(err)
	}
	if err := eng.AwaitOffsets(ctx, map[int]int64{1: 4}); err != nil {
		panic(err)
	}
	// 超容忍迟到：t=1 属于已过期窗口 => 侧输出明确拒绝（9999 不进任何聚合）。
	if _, err := p0.Append(se.Event{Key: "a", EventTime: 1, Value: 9999}); err != nil {
		panic(err)
	}
	if err := eng.AwaitOffsets(ctx, map[int]int64{0: 7}); err != nil {
		panic(err)
	}

	p0.Close()
	p1.Close()
	if err := <-run1; err != nil {
		panic(err)
	}
	if err := eng.Checkpoint(ctx); err != nil {
		panic(err)
	}
	st := eng.Stats()
	fmt.Println(">>> final:", eng.Snapshot())
	fmt.Printf(">>> stats: accepted=%d duplicates=%d lateRejected=%d corrections=%d\n",
		st.Accepted, st.Duplicates, st.LateReject, st.Corrections)

	// ---------- run-2：全新引擎从最终检查点恢复 ----------
	fmt.Println("--- restart: no duplicate output, no missing state ---")
	eng2 := se.NewEngine(cfg)
	eng2.BindSource(src)
	eng2.SetCheckpointStore(store)
	go drain("run-2", eng2)
	if err := eng2.Run(ctx); err != nil {
		panic(err)
	}
	s2 := eng2.Stats()
	fmt.Println(">>> restored:", eng2.Snapshot())
	fmt.Printf(">>> restart stats: accepted=%d duplicates=%d lateRejected=%d\n",
		s2.Accepted, s2.Duplicates, s2.LateReject)
}

func drain(tag string, e *se.Engine) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for r := range e.Outputs() {
			fmt.Printf("[%s] RESULT %s\n", tag, r)
		}
	}()
	go func() {
		defer wg.Done()
		for l := range e.LateEvents() {
			fmt.Printf("[%s] %s\n", tag, l)
		}
	}()
	wg.Wait()
}
