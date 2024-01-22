package server

import (
	"hash/fnv"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// keyValue is the internal representation of a key-value pair and
// associated metadata.
type keyValue struct {
	Key         string    `json:"key"`
	Value       []byte    `json:"value"`
	ContentType string    `json:"content_type,omitempty"`
	Size        uint64    `json:"-"`
	Hash        uint64    `json:"hash,omitempty"`
	Created     time.Time `json:"created,omitempty"`
	Updated     time.Time `json:"updated,omitempty"`
	Version     uint64    `json:"version,omitempty"`
	// History     *keyValueHistory `json:"-"`
	CreatedBy string `json:"created_by"`
	mu        sync.RWMutex
}

// newKeyValue initializes a new keyValue
func newKeyValue(
	key string,
	value []byte,
	contentType string,
	clientID string,
) *keyValue {
	now := time.Now()
	hashChannel := make(chan uint64, 1)

	go func() {
		hashFunc := fnv.New64a()
		if _, he := hashFunc.Write(value); he != nil {
			slog.Warn("error hashing value", "error", he, "key", key)
		}
		hashChannel <- hashFunc.Sum64()
	}()

	kvInfo := &keyValue{
		Key:       key,
		Value:     value,
		Created:   now,
		Size:      uint64(len(value)),
		Version:   1,
		CreatedBy: clientID,
	}

	switch {
	case contentType == "" && value != nil:
		kvInfo.ContentType = http.DetectContentType(kvInfo.Value)
	default:
		kvInfo.ContentType = contentType
	}

	kvInfo.Hash = <-hashChannel
	return kvInfo
}

func (kv *keyValue) LogValue() slog.Value {
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

// keyValueSnapshot is a snapshot of a key-value pair and associated info
type keyValueSnapshot struct {
	Key         string    `json:"key"`
	Value       []byte    `json:"value"`
	ContentType string    `json:"content_type,omitempty"`
	Size        uint64    `json:"size"`
	Hash        uint64    `json:"hash,omitempty"`
	Version     uint64    `json:"version"`
	Timestamp   time.Time `json:"timestamp"`
}

func newKeySnapshot(kv *keyValue) *keyValueSnapshot {
	size := len(kv.Value)
	value := make([]byte, size)
	copy(value, kv.Value)
	return &keyValueSnapshot{
		Timestamp:   time.Now(),
		Key:         kv.Key,
		Size:        uint64(size),
		Version:     kv.Version,
		ContentType: kv.ContentType,
		Hash:        kv.Hash,
		Value:       value,
	}
}

// keyLifetimeMetric tracks some basic metadata for a key, persisting
// between deletes, expunges, etc. If a key `foo` is created with a
// lifespan of 1 hour and is eventually deleted, then a client creates
// a new key `foo` 2 hours later, the new key will have a `first_set`
// time of 2 hours ago, and a `first_accessed` time of 2 hours ago, as
// this metadata isn't deleted along with the key. This is primarily used
// to help determine which keys to [Expunge] first - a key may have a
// relatively short lifespan, but be recreated/accessed frequently, in which
// case we may want to keep it around.
type keyLifetimeMetric struct {
	// AccessCount is the number of times the key has been accessed, either
	// via [Server.Get], [Server.Inspect] (with include_value=true),
	// [Server.Pop], or pushed via [Server.WatchKeyValue]
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

// StaleScore returns a score for how "stale" the key is. The higher the score,
// the more stale the key is. The score is calculated by taking the inverse of
// the number of hours since the key was last accessed, set, created, or locked.
// This score is used by [pruner] to determine which keys to [Expunge] first.
func (k *keyLifetimeMetric) StaleScore(t time.Time) float64 {
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

func (k *keyLifetimeMetric) LogValue() slog.Value {
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
