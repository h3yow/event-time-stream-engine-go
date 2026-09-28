package streamengine

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// Checkpoint 是一致性快照：窗口聚合状态 + 各分区消费位点 + 水位线。
// 三者原子地一起保存，恢复后聚合状态与消费位置严格对得上：
// 位点语义为"下一个待处理 offset"，与快照中已计入的记录一一对应。
type Checkpoint struct {
	ID         int64
	CreatedAt  time.Time
	Watermark  time.Time
	MaxTS      time.Time
	Offsets    map[int]int64 // partition -> 下一个待处理 offset
	Aggregates map[WindowKey]*Aggregate
}

// CheckpointStore 断点存储。
// Save 必须单调：以更小 ID 覆盖已存在的更新断点返回 ErrCheckpointRollback，
// 绝不允许断点回退，更不能"从中间接着算"。
type CheckpointStore interface {
	Save(cp *Checkpoint) error
	Load() (*Checkpoint, error)
	LatestID() int64
}

// jsonAggEntry 是断点 JSON 中聚合条目的表示：map 键被拍平成字段，
// 因为 encoding/json 不支持 struct 作为 map 键。
type jsonAggEntry struct {
	Key   string     `json:"key"`
	Start time.Time  `json:"start"`
	Agg   *Aggregate `json:"agg"`
}

// jsonCheckpoint 是 Checkpoint 的 JSON 表示。
type jsonCheckpoint struct {
	ID         int64          `json:"id"`
	CreatedAt  time.Time      `json:"createdAt"`
	Watermark  time.Time      `json:"watermark"`
	MaxTS      time.Time      `json:"maxTs"`
	Offsets    map[int]int64  `json:"offsets"`
	Aggregates []jsonAggEntry `json:"aggregates"`
}

// MarshalJSON 自定义序列化，拍平 WindowKey 键。
func (cp *Checkpoint) MarshalJSON() ([]byte, error) {
	keys := make([]WindowKey, 0, len(cp.Aggregates))
	for k := range cp.Aggregates {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if !keys[i].Start.Equal(keys[j].Start) {
			return keys[i].Start.Before(keys[j].Start)
		}
		return keys[i].Key < keys[j].Key
	})
	jc := jsonCheckpoint{
		ID:         cp.ID,
		CreatedAt:  cp.CreatedAt,
		Watermark:  cp.Watermark,
		MaxTS:      cp.MaxTS,
		Offsets:    cp.Offsets,
		Aggregates: make([]jsonAggEntry, 0, len(keys)),
	}
	for _, k := range keys {
		jc.Aggregates = append(jc.Aggregates, jsonAggEntry{Key: k.Key, Start: k.Start, Agg: cp.Aggregates[k]})
	}
	return json.Marshal(&jc)
}

// UnmarshalJSON 自定义反序列化，重建 WindowKey 键。
func (cp *Checkpoint) UnmarshalJSON(data []byte) error {
	var jc jsonCheckpoint
	if err := json.Unmarshal(data, &jc); err != nil {
		return err
	}
	cp.ID = jc.ID
	cp.CreatedAt = jc.CreatedAt
	cp.Watermark = jc.Watermark
	cp.MaxTS = jc.MaxTS
	cp.Offsets = jc.Offsets
	if cp.Offsets == nil {
		cp.Offsets = map[int]int64{}
	}
	cp.Aggregates = make(map[WindowKey]*Aggregate, len(jc.Aggregates))
	for _, e := range jc.Aggregates {
		cp.Aggregates[WindowKey{Key: e.Key, Start: e.Start}] = e.Agg
	}
	return nil
}

// cloneCheckpoint 深拷贝，避免调用方在保存后修改快照内容。
func cloneCheckpoint(cp *Checkpoint) *Checkpoint {
	cp2 := &Checkpoint{
		ID:         cp.ID,
		CreatedAt:  cp.CreatedAt,
		Watermark:  cp.Watermark,
		MaxTS:      cp.MaxTS,
		Offsets:    make(map[int]int64, len(cp.Offsets)),
		Aggregates: make(map[WindowKey]*Aggregate, len(cp.Aggregates)),
	}
	for k, v := range cp.Offsets {
		cp2.Offsets[k] = v
	}
	for k, v := range cp.Aggregates {
		cp2.Aggregates[k] = v.clone()
	}
	return cp2
}

// MemoryCheckpointStore 本地内存断点存储（模拟持久化，同进程重启场景用）。
type MemoryCheckpointStore struct {
	mu     sync.Mutex
	latest *Checkpoint
}

func NewMemoryCheckpointStore() *MemoryCheckpointStore {
	return &MemoryCheckpointStore{}
}

func (m *MemoryCheckpointStore) Save(cp *Checkpoint) error {
	if cp == nil {
		return fmt.Errorf("checkpoint: nil snapshot")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.latest != nil && cp.ID < m.latest.ID {
		return fmt.Errorf("%w: incoming id=%d existing id=%d",
			ErrCheckpointRollback, cp.ID, m.latest.ID)
	}
	m.latest = cloneCheckpoint(cp)
	return nil
}

func (m *MemoryCheckpointStore) Load() (*Checkpoint, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.latest == nil {
		return nil, nil
	}
	return cloneCheckpoint(m.latest), nil
}

func (m *MemoryCheckpointStore) LatestID() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.latest == nil {
		return 0
	}
	return m.latest.ID
}

// FileCheckpointStore 基于本地 JSON 文件的断点存储，模拟真实持久化，
// 可跨进程"重启恢复"。写入用临时文件 + 原子 rename，避免半写文件。
type FileCheckpointStore struct {
	mu       sync.Mutex
	path     string
	latestID int64
}

func NewFileCheckpointStore(path string) *FileCheckpointStore {
	return &FileCheckpointStore{path: path}
}

func (f *FileCheckpointStore) Save(cp *Checkpoint) error {
	if cp == nil {
		return fmt.Errorf("checkpoint: nil snapshot")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if cp.ID < f.latestID {
		return fmt.Errorf("%w: incoming id=%d existing id=%d",
			ErrCheckpointRollback, cp.ID, f.latestID)
	}
	data, err := json.MarshalIndent(cp, "", "  ")
	if err != nil {
		return fmt.Errorf("checkpoint marshal: %w", err)
	}
	dir := filepath.Dir(f.path)
	tmp, err := os.CreateTemp(dir, ".cp-*.tmp")
	if err != nil {
		return fmt.Errorf("checkpoint temp file: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("checkpoint write: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("checkpoint sync: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("checkpoint close: %w", err)
	}
	if err := os.Rename(tmpName, f.path); err != nil {
		return fmt.Errorf("checkpoint rename: %w", err)
	}
	f.latestID = cp.ID
	return nil
}

func (f *FileCheckpointStore) Load() (*Checkpoint, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	data, err := os.ReadFile(f.path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("checkpoint read: %w", err)
	}
	var cp Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, fmt.Errorf("checkpoint unmarshal: %w", err)
	}
	f.latestID = cp.ID
	return &cp, nil
}

func (f *FileCheckpointStore) LatestID() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.latestID
}
