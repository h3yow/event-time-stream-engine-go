package streamengine

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// Checkpoint 是一次原子快照：消费位置 + 窗口状态 + 水位线 + 代际号。
// 四者必须一起提交，恢复时一起读回，才能保证“状态与位置对得上”。
type Checkpoint struct {
	Generation    int              `json:"generation"`
	PartitionNext map[int]int64    `json:"partitionNext"` // 各分区下一条待处理偏移
	Windows       []WindowSnapshot `json:"windows"`
	Watermark     int64            `json:"watermark"`
	PartitionMax  map[int]int64    `json:"partitionMax"` // 水位线各分区观测最大值
	Registered    []int            `json:"registered"`   // 参与水位线计算的分区
	Finished      []int            `json:"finished"`     // 已关闭退出水位线计算的分区
}

// CheckpointStore 是检查点持久化存储。
type CheckpointStore interface {
	// Load 读取最新检查点；尚不存在时返回 (nil, nil)。
	Load(ctx context.Context) (*Checkpoint, error)
	// Save 原子保存检查点；generation 必须严格大于已保存的代际号，
	// 否则返回 ReasonCheckpointRollback（防止旧快照覆盖新快照）。
	Save(ctx context.Context, cp *Checkpoint) error
}

// MemoryCheckpointStore 是进程内检查点存储（单测用）。
type MemoryCheckpointStore struct {
	saved *Checkpoint
}

func NewMemoryCheckpointStore() *MemoryCheckpointStore { return &MemoryCheckpointStore{} }

func (m *MemoryCheckpointStore) Load(ctx context.Context) (*Checkpoint, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return m.saved, nil
}

func (m *MemoryCheckpointStore) Save(ctx context.Context, cp *Checkpoint) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if m.saved != nil && cp.Generation <= m.saved.Generation {
		return newError(ReasonCheckpointRollback, fmt.Sprintf(
			"refuse to save generation %d, latest is %d", cp.Generation, m.saved.Generation))
	}
	m.saved = cp
	return nil
}

// FileCheckpointStore 用单个本地文件做原子检查点持久化（重启恢复用）。
// 写入采用“临时文件 + fsync + rename”，保证文件要么是旧快照、要么是新快照。
type FileCheckpointStore struct {
	path string
}

func NewFileCheckpointStore(path string) *FileCheckpointStore {
	return &FileCheckpointStore{path: path}
}

func (f *FileCheckpointStore) Load(ctx context.Context) (*Checkpoint, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(f.path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var cp Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, fmt.Errorf("corrupt checkpoint %s: %w", f.path, err)
	}
	return &cp, nil
}

func (f *FileCheckpointStore) Save(ctx context.Context, cp *Checkpoint) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// 代际单调检查（即使重启后 store 是新对象，也先读磁盘上的旧快照比对）。
	if old, err := f.Load(ctx); err != nil {
		return err
	} else if old != nil && cp.Generation <= old.Generation {
		return newError(ReasonCheckpointRollback, fmt.Sprintf(
			"refuse to save generation %d, persisted latest is %d", cp.Generation, old.Generation))
	}

	data, err := json.MarshalIndent(cp, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(f.path)
	tmp, err := os.CreateTemp(dir, ".checkpoint-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		cleanup()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		cleanup()
		return err
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return err
	}
	if err := os.Rename(tmpName, f.path); err != nil {
		cleanup()
		return err
	}
	// 尽力 fsync 目录，使 rename 落盘。
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

// ValidateStartOffsets 校验启动消费位置。
//
//   - 对任何已提交过位置的分区，若启动位置 < 已提交位置 => 断点回退，
//     明确拒绝（ReasonCheckpointRollback），而不是从中间重新计算；
//   - 启动位置 == 已提交位置 => 正常重启（该位置事件会被重放，由记录级
//     去重保证不重复计数）；
//   - 启动位置 > 已提交位置 => 允许（跳过已确认不需要的历史），但不常见；
//   - 新分区没有已提交位置 => 允许。
func ValidateStartOffsets(committed map[int]int64, start map[int]int64) error {
	parts := make([]int, 0, len(committed))
	for p, st := range start {
		if c, ok := committed[p]; ok && st < c {
			parts = append(parts, p)
		}
	}
	if len(parts) == 0 {
		return nil
	}
	sort.Ints(parts)
	var b strings.Builder
	for i, p := range parts {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "partition %d start=%d < committed=%d", p, start[p], committed[p])
	}
	return newError(ReasonCheckpointRollback,
		"checkpoint rollback refused: "+b.String())
}

// DescribeSnapshot 返回检查点的可读描述（测试日志打印水位线/消费位置/状态）。
func DescribeSnapshot(cp *Checkpoint) string {
	if cp == nil {
		return "checkpoint=<nil>"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "checkpoint gen=%d %s offsets={",
		cp.Generation, describeWatermark(cp.Watermark, cp.PartitionMax))
	parts := make([]int, 0, len(cp.PartitionNext))
	for p := range cp.PartitionNext {
		parts = append(parts, p)
	}
	sort.Ints(parts)
	for i, p := range parts {
		if i > 0 {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, "p%d:%d", p, cp.PartitionNext[p])
	}
	fmt.Fprintf(&b, "} windows=%d[", len(cp.Windows))
	for i, w := range cp.Windows {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "%s %s sum=%d count=%d fired=%v v%d refs=%d",
			w.Key,
			Window{Start: w.Start, End: w.End},
			w.Sum, w.Count, w.Fired, w.Version, len(w.Records))
	}
	b.WriteString("]")
	return b.String()
}

// MergeCheckpoints 合并多个消费者各自保存的检查点（消费组并发摄入/再平衡）。
//
// 合并规则（保证同一窗口恰好一次）：
//   - 消费位置：按分区取最大 next（不回退）；
//   - 水位线：各分区观测最大值取 max；已关闭取并集；已注册取并集；
//   - 窗口：同一 keyed 窗口内按 (partition,offset) 对记录求并集去重，
//     并由记录值重算 sum/count；fired 取 OR，version 取 max。
//
// 任意参与快照的 Generation 高于返回值不会发生：返回代际取最大值 +1。
func MergeCheckpoints(maxOutOfOrderness int64, cps ...*Checkpoint) *Checkpoint {
	merged := &Checkpoint{
		PartitionNext: make(map[int]int64),
		PartitionMax:  make(map[int]int64),
	}
	regSet := make(map[int]bool)
	finSet := make(map[int]bool)
	winIdx := make(map[string]int) // key|start -> merged.Windows 下标
	genMax := 0

	for _, cp := range cps {
		if cp == nil {
			continue
		}
		if cp.Generation > genMax {
			genMax = cp.Generation
		}
		for p, off := range cp.PartitionNext {
			if cur, ok := merged.PartitionNext[p]; !ok || off > cur {
				merged.PartitionNext[p] = off
			}
		}
		for p, m := range cp.PartitionMax {
			if cur, ok := merged.PartitionMax[p]; !ok || m > cur {
				merged.PartitionMax[p] = m
			}
		}
		for _, p := range cp.Registered {
			regSet[p] = true
		}
		for _, p := range cp.Finished {
			finSet[p] = true
		}
		for _, w := range cp.Windows {
			k := w.Key + "|" + strconv.FormatInt(w.Start, 10)
			idx, ok := winIdx[k]
			if !ok {
				merged.Windows = append(merged.Windows, WindowSnapshot{
					Key: w.Key, Start: w.Start, End: w.End,
					Records: nil,
				})
				idx = len(merged.Windows) - 1
				winIdx[k] = idx
			}
			mergeWindowInto(&merged.Windows[idx], w)
		}
	}

	merged.Generation = genMax + 1
	merged.Registered = sortedKeys(regSet)
	merged.Finished = sortedKeys(finSet)
	merged.Watermark = minWatermarkOf(maxOutOfOrderness, merged.PartitionMax, regSet, finSet)
	return merged
}

type refKey struct {
	p int
	o int64
}

func mergeWindowInto(dst *WindowSnapshot, src WindowSnapshot) {
	if dst.End == 0 {
		dst.End = src.End
	}
	seen := make(map[refKey]bool, len(dst.Records))
	for _, r := range dst.Records {
		seen[refKey{r.Partition, r.Offset}] = true
	}
	for _, r := range src.Records {
		k := refKey{r.Partition, r.Offset}
		if !seen[k] {
			dst.Records = append(dst.Records, r)
			seen[k] = true
		}
	}
	dst.Fired = dst.Fired || src.Fired
	if src.Version > dst.Version {
		dst.Version = src.Version
	}
	// 由记录并集重算聚合（sum/count 绝不双算）。
	var sum, count int64
	for _, r := range dst.Records {
		sum += r.Value
		count++
	}
	dst.Sum, dst.Count = sum, count
	sort.Slice(dst.Records, func(i, j int) bool {
		if dst.Records[i].Partition != dst.Records[j].Partition {
			return dst.Records[i].Partition < dst.Records[j].Partition
		}
		return dst.Records[i].Offset < dst.Records[j].Offset
	})
}

func sortedKeys(set map[int]bool) []int {
	out := make([]int, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Ints(out)
	return out
}

// minWatermarkOf 依据合并后的分区视图重算水位线；
// 任一已注册但既无观测又未关闭的分区存在时返回 MinWatermark。
func minWatermarkOf(tolerance int64, maxes map[int]int64, reg, fin map[int]bool) int64 {
	min := int64(^uint64(0) >> 1) // MaxInt64
	active := 0
	for p := range reg {
		if fin[p] {
			continue
		}
		active++
		m, ok := maxes[p]
		if !ok {
			return MinWatermark
		}
		if m < min {
			min = m
		}
	}
	if active == 0 {
		return MinWatermark
	}
	return min - tolerance
}
