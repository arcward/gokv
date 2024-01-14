package server

import (
	"hash/fnv"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// KeyValueInfo is the internal representation of a key-value pair and
// associated metadata.
type KeyValueInfo struct {
	Key         string       `json:"key"`
	Value       []byte       `json:"value"`
	ContentType string       `json:"content_type,omitempty"`
	Size        uint64       `json:"-"`
	Hash        uint64       `json:"hash,omitempty"`
	Created     time.Time    `json:"created,omitempty"`
	Updated     time.Time    `json:"updated,omitempty"`
	Version     uint64       `json:"version,omitempty"`
	History     *KVHistory   `json:"-"`
	mu          sync.RWMutex `json:"-"`
	CreatedBy   string       `json:"created_by"`
}

func NewKeyValue(
	srv *KeyValueStore,
	key string,
	value []byte,
	contentType string,
	clientID string,
	revisionLimit int64,
) *KeyValueInfo {
	now := time.Now()
	hashChannel := make(chan uint64, 1)

	go func() {
		hashFunc := fnv.New64a()
		_, he := hashFunc.Write(value)
		if he != nil {
			srv.logger.Error(
				"error hashing value",
				"error", he,
			)
		}
		hashChannel <- hashFunc.Sum64()
	}()

	kvInfo := &KeyValueInfo{
		Key:       key,
		Value:     value,
		Created:   now,
		Size:      uint64(len(value)),
		Version:   1,
		CreatedBy: clientID,
	}

	if revisionLimit > 0 {
		kvInfo.History = NewKVHistory(revisionLimit)
	}

	if contentType == "" && value != nil {
		kvInfo.ContentType = http.DetectContentType(kvInfo.Value)
	} else {
		kvInfo.ContentType = contentType
	}
	kvInfo.Hash = <-hashChannel
	return kvInfo
}

func (kv *KeyValueInfo) LogValue() slog.Value {
	attrs := make([]slog.Attr, 0, 8)
	attrs = append(attrs, slog.String("key", kv.Key))
	attrs = append(attrs, slog.Uint64("version", kv.Version))
	attrs = append(attrs, slog.Uint64("hash", kv.Hash))
	if !kv.Created.IsZero() {
		attrs = append(attrs, slog.Time("created", kv.Created))
	}
	if !kv.Updated.IsZero() {
		attrs = append(attrs, slog.Time("updated", kv.Updated))
	}
	return slog.GroupValue(attrs...)
}

// KeyValueSnapshot is a snapshot of a key-value pair and associated info
type KeyValueSnapshot struct {
	Key         string    `json:"key"`
	Version     uint64    `json:"version"`
	Value       []byte    `json:"value"`
	ContentType string    `json:"content_type,omitempty"`
	Timestamp   time.Time `json:"timestamp"`
	Size        uint64    `json:"size"`
	Hash        uint64    `json:"hash,omitempty"`
}

type KVHistory struct {
	snapshots []*KeyValueSnapshot
	maxLen    int64
}

// Version returns the snapshot for the given revision number
func (h *KVHistory) Version(v uint64) (*KeyValueSnapshot, bool) {
	for i := 0; i < len(h.snapshots); i++ {
		s := h.snapshots[i]
		if s.Version == v {
			return s, true
		}
	}
	return nil, false
}

// RelativeVersion returns the snapshot for the given revision number. If
// the number of positive, returns that revision, if it exists. If negative,
// returns the revision that many from the end, if it exists (ex: if the current
// version is 10, -1 would return version 9). The second boolean will return
// false if the version does not exist.
func (h *KVHistory) RelativeVersion(v int64) (*KeyValueSnapshot, bool) {
	if v < 0 {
		v = int64(len(h.snapshots)) + v
	}
	targetVersion := uint64(v)
	for i := 0; i < len(h.snapshots); i++ {
		keySnapshot := h.snapshots[i]
		if keySnapshot.Version == targetVersion {
			return keySnapshot, true
		}
	}
	return nil, false
}

func (h *KVHistory) Append(snapshot *KeyValueSnapshot) {
	if h.maxLen > 0 && int64(len(h.snapshots)) == h.maxLen {
		for i := 0; i < int(h.maxLen)-1; i++ {
			h.snapshots[i] = h.snapshots[i+1]
		}
		h.snapshots[h.maxLen-1] = snapshot
		return
	}
	h.snapshots = append(h.snapshots, snapshot)
}

func (h *KVHistory) Add(kv *KeyValueInfo) {
	size := len(kv.Value)
	value := make([]byte, size)
	copy(value, kv.Value)
	snapshot := &KeyValueSnapshot{
		Timestamp:   time.Now(),
		Key:         kv.Key,
		Size:        uint64(size),
		Version:     kv.Version,
		ContentType: kv.ContentType,
		Hash:        kv.Hash,
		Value:       value,
	}
	h.Append(snapshot)
}

func (h *KVHistory) Clear() int64 {
	items := len(h.snapshots)
	if h.maxLen > 0 {
		h.snapshots = make([]*KeyValueSnapshot, 0, h.maxLen)
	} else {
		h.snapshots = make([]*KeyValueSnapshot, 0)
	}
	return int64(items)
}

func (h *KVHistory) List() []*KeyValueSnapshot {
	return h.snapshots[:]
}

func (h *KVHistory) Length() int {
	return len(h.snapshots)
}

type KeyMetrics struct {
	AccessCount   uint64     `json:"access_count"`
	FirstAccessed *time.Time `json:"first_accessed,omitempty"`
	LastAccessed  *time.Time `json:"last_accessed,omitempty"`

	SetCount uint64     `json:"set_count"`
	FirstSet *time.Time `json:"first_set,omitempty"`
	LastSet  *time.Time `json:"last_set,omitempty"`

	LockCount   uint64     `json:"lock_count"`
	FirstLocked *time.Time `json:"first_locked,omitempty"`
	LastLocked  *time.Time `json:"last_locked,omitempty"`

	mu sync.RWMutex
}

func (k *KeyMetrics) StaleScore(t time.Time) float64 {
	var accessScore float64
	var updateScore float64
	var createScore float64
	var lockScore float64

	accessWeight := 0.5
	updateWeight := 0.2
	createWeight := 0.2
	lockWeight := 0.1

	if k.LastAccessed != nil {
		accHours := t.Sub(*k.LastAccessed).Hours()
		if accHours > 0 {
			accessScore = 1 / accHours
		}
	}
	if k.LastSet != nil {
		setHours := t.Sub(*k.LastSet).Hours()
		if setHours > 0 {
			updateScore = 1 / setHours
		}
	}
	if k.FirstSet != nil {
		createHours := t.Sub(*k.FirstSet).Hours()
		if createHours > 0 {
			createScore = 1 / createHours
		}
	}
	if k.LastLocked != nil {
		lockHours := t.Sub(*k.LastLocked).Hours()
		if lockHours > 0 {
			lockScore = 1 / lockHours
		}
	}
	accessResult := accessScore * accessWeight
	updateResult := updateScore * updateWeight
	createResult := createScore * createWeight
	lockResult := lockScore * lockWeight
	total := accessResult + updateResult + createResult + lockResult
	return total
}

func (k *KeyMetrics) LogValue() slog.Value {
	attrs := []slog.Attr{
		slog.Uint64("access_count", k.AccessCount),
		slog.Uint64("set_count", k.SetCount),
		slog.Uint64("lock_count", k.LockCount),
	}
	if k.FirstAccessed != nil {
		attrs = append(attrs, slog.Time("first_accessed", *k.FirstAccessed))
	}
	if k.LastAccessed != nil {
		attrs = append(attrs, slog.Time("last_accessed", *k.LastAccessed))
	}
	if k.FirstSet != nil {
		attrs = append(attrs, slog.Time("first_set", *k.FirstSet))
	}
	if k.LastSet != nil {
		attrs = append(attrs, slog.Time("last_set", *k.LastSet))
	}
	if k.FirstLocked != nil {
		attrs = append(attrs, slog.Time("first_locked", *k.FirstLocked))
	}
	if k.LastLocked != nil {
		attrs = append(attrs, slog.Time("last_locked", *k.LastLocked))
	}
	return slog.GroupValue(attrs...)
}

func (k *KeyMetrics) LocksPerHour() float64 {
	var lockRate float64
	if k.FirstLocked == nil {
		return lockRate
	}
	td := k.LastLocked.Sub(*k.FirstLocked).Hours()
	if td > 0 {
		lockRate = float64(k.LockCount) / td
	}
	return lockRate
}

func (k *KeyMetrics) AccessPerHour() float64 {
	var accessRate float64
	if k.FirstAccessed == nil {
		return accessRate
	}
	td := k.LastAccessed.Sub(*k.FirstAccessed).Hours()
	if td > 0 {
		accessRate = float64(k.AccessCount) / td
	}
	return accessRate
}

func (k *KeyMetrics) SetPerHour() float64 {
	var setRate float64

	td := k.LastSet.Sub(*k.FirstSet).Hours()
	if td > 0 {
		setRate = float64(k.SetCount) / td
	}
	return setRate
}

func (k *KeyMetrics) AccessToSetRatio() float64 {
	var ratio float64
	if k.SetCount > 0 {
		ratio = float64(k.AccessCount) / float64(k.SetCount)
	}
	return ratio
}

func NewKVHistory(maxLen int64) *KVHistory {
	if maxLen == 0 {
		return nil
	}

	if maxLen == -1 {
		return &KVHistory{
			snapshots: make([]*KeyValueSnapshot, 0),
			maxLen:    maxLen,
		}
	}
	return &KVHistory{
		snapshots: make([]*KeyValueSnapshot, 0, maxLen),
		maxLen:    maxLen,
	}
}
