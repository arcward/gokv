package server

import (
	"crypto"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	pb "github.com/arcward/gokv/api"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sync"
	"time"
)

import (
	"log/slog"
	"sync/atomic"
)

import (
	"context"
)

const (
	DefaultMaxKeySize   uint64 = 1000    // 1,000 bytes
	DefaultMaxValueSize uint64 = 1000000 // 1,000,000 bytes
)

var (
	ErrKeyTooLong    = errors.New("key length greater than maximum")
	ErrValueTooLarge = fmt.Errorf(
		"value size greater than maximum %d",
		DefaultMaxValueSize,
	)
	ErrKeyNotFound       = fmt.Errorf("key not found")
	ErrMaxKeysReached    = fmt.Errorf("maximum number of keys reached")
	ErrInvalidKeyPattern = fmt.Errorf("invalid key pattern")
	ErrAlreadyLocked     = fmt.Errorf("lock already exists")
	ErrLocked            = fmt.Errorf("key is Locked")
	ErrRevisionNotFound  = fmt.Errorf("revision not found")
)

type Config struct {
	MaxNumberOfKeys uint64       `json:"max_number_of_keys" yaml:"max_number_of_keys" mapstructure:"max_number_of_keys"`
	MaxValueSize    uint64       `json:"max_value_size" yaml:"max_value_size" mapstructure:"max_value_size"`
	MaxKeySize      uint64       `json:"max_key_size" yaml:"max_key_size" mapstructure:"max_key_size"`
	KeepExpiredKeys bool         `json:"keep_expired_keys" yaml:"keep_expired_keys" mapstructure:"keep_expired_keys"`
	RevisionLimit   int64        `json:"revision_limit" yaml:"revision_limit" mapstructure:"revision_limit"`
	Hashing         HashConfig   `json:"hashing" yaml:"hashing" mapstructure:"hashing"`
	Logger          *slog.Logger `json:"-" yaml:"-" mapstructure:"-"`
}

type HashConfig struct {
	// Enables hashing of values
	Enabled   bool        `json:"enabled" yaml:"enabled" mapstructure:"enabled"`
	Algorithm crypto.Hash `json:"algorithm" yaml:"algorithm" mapstructure:"algorithm"`
}

// NewServer returns a new KeyValueStore server
func NewServer(cfg Config) *KeyValueStore {
	if cfg.Hashing.Algorithm == 0 {
		cfg.Hashing.Algorithm = crypto.MD5
	}
	if cfg.MaxValueSize == 0 {
		cfg.MaxValueSize = DefaultMaxValueSize
	}
	if cfg.MaxKeySize == 0 {
		cfg.MaxKeySize = DefaultMaxKeySize
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default().WithGroup("gokv_server")
	}
	return &KeyValueStore{
		store:  make(map[string]*KeyValueInfo),
		logger: cfg.Logger,
		cfg:    cfg,
	}
}

type KeyValueStore struct {
	store                 map[string]*KeyValueInfo
	logger                *slog.Logger `json:"-"`
	cfg                   Config       `json:"-"`
	numKeys               atomic.Uint64
	totalSize             atomic.Uint64
	numGetRequests        atomic.Uint64
	numSetRequests        atomic.Uint64
	numDelRequests        atomic.Uint64
	numNewKeysSet         atomic.Uint64
	numKeysUpdated        atomic.Uint64
	numGetKeyInfoRequests atomic.Uint64
	numListKeysRequests   atomic.Uint64
	numPopRequests        atomic.Uint64
	numExistsRequests     atomic.Uint64
	numLockRequests       atomic.Uint64
	numUnlockRequests     atomic.Uint64
	mu                    sync.RWMutex
	pb.UnimplementedKeyValueStoreServer
}

func (s *KeyValueStore) Config() Config {
	return s.cfg
}

func (s *KeyValueStore) Restore(data []byte) error {
	tmpStore := NewServer(Config{})
	err := json.Unmarshal(data, &tmpStore)
	if err != nil {
		return err
	}

	*s = *tmpStore
	return nil
}

func (s *KeyValueStore) GetRevision(
	ctx context.Context,
	in *pb.GetRevisionRequest,
) (
	*pb.RevisionResponse,
	error,
) {
	p, _ := peer.FromContext(ctx)
	s.logger.Info(
		"getting revision",
		slog.String("address", p.Addr.String()),
		slog.String("key", in.Key),
		slog.Uint64("version", in.Version),
	)
	s.mu.RLock()
	defer s.mu.RUnlock()

	kv, ok := s.store[in.Key]
	if !ok {
		return nil, status.Errorf(
			codes.NotFound,
			ErrKeyNotFound.Error(),
		)
	}

	kv.mu.RLock()
	defer kv.mu.RUnlock()

	if kv.Version == in.Version {
		rv := &pb.RevisionResponse{
			Value: kv.Value,
		}
		if kv.Updated.IsZero() {
			rv.Timestamp = kv.Created.Unix()
		} else {
			rv.Timestamp = kv.Updated.Unix()
		}
		return rv, nil
	}

	if kv.History == nil {
		return nil, status.Errorf(codes.NotFound, ErrRevisionNotFound.Error())
	}
	for _, h := range kv.History {
		if h.Version == in.Version {
			return &pb.RevisionResponse{
				Value:     h.Value,
				Timestamp: h.Timestamp.Unix(),
			}, nil
		}
	}
	return nil, status.Errorf(codes.NotFound, ErrRevisionNotFound.Error())
}

func (s *KeyValueStore) MarshalJSON() (data []byte, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	keys := make([]*KeyValueInfo, 0, len(s.store))
	for _, kvInfo := range s.store {
		kvInfo.mu.RLock()
		keys = append(keys, kvInfo)
	}
	defer func() {
		for _, kvInfo := range keys {
			kvInfo.mu.RUnlock()
		}
	}()
	return json.Marshal(keys)
}

func (s *KeyValueStore) UnmarshalJSON(data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.resetStats()
	kvDump := []*KeyValueInfo{}
	err := json.Unmarshal(data, &kvDump)
	if err != nil {
		return err
	}
	if s.store == nil {
		s.store = make(map[string]*KeyValueInfo)
	}
	s.resetStats()
	var hashValue bool
	if s.cfg.Hashing.Enabled {
		hashValue = true
	}

	hashChannel := make(chan *KeyValueInfo)
	wg := sync.WaitGroup{}
	doneChannel := make(chan *KeyValueInfo)

	hashAlgorithm := s.cfg.Hashing.Algorithm
	hashFunc := hashAlgorithm.HashFunc()

	ctx, cancel := context.WithCancel(context.Background())
	workers := runtime.GOMAXPROCS(0)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for kvInfo := range hashChannel {
				if ctx.Err() != nil {
					return
				}
				kvCopy := kvInfo
				kvCopy.mu.Lock()
				kvCopy.Hash = ""
				kvCopy.Size = uint64(len(kvCopy.Value))
				if kvCopy.Created.IsZero() {
					kvCopy.Created = time.Now().UTC()
				}
				setExpiredLocked(s, kvCopy)

				s.numKeys.Add(1)
				s.totalSize.Add(kvCopy.Size)

				if hashValue {
					setHash(kvCopy, hashFunc)
				}
				kvCopy.mu.Unlock()
				doneChannel <- kvCopy
			}
		}()
	}

	go func() {
		for i := 0; i < len(kvDump); i++ {
			if ctx.Err() != nil {
				return
			}
			hashChannel <- kvDump[i]
		}
		close(hashChannel)
	}()

	go func() {
		wg.Wait()
		close(doneChannel)
	}()

	for kv := range doneChannel {
		if ctx.Err() != nil {
			break
		}
		kc := kv
		s.logger.Debug("loading key", slog.String("key", kc.Key))
		_, exists := s.store[kc.Key]
		if exists {
			s.logger.Error(
				"duplicate key found",
				slog.String("key", kc.Key),
			)
			cancel()
		} else {
			s.store[kc.Key] = kc
		}
	}
	return nil
}

func (s *KeyValueStore) resetStats() {
	s.numNewKeysSet.Store(0)
	s.numKeysUpdated.Store(0)
	s.numGetRequests.Store(0)
	s.numSetRequests.Store(0)
	s.numDelRequests.Store(0)
	s.numGetKeyInfoRequests.Store(0)
	s.numListKeysRequests.Store(0)
	s.numPopRequests.Store(0)
	s.numExistsRequests.Store(0)
	s.numLockRequests.Store(0)
	s.numUnlockRequests.Store(0)
}

// Stats returns server statistics
func (s *KeyValueStore) Stats(
	ctx context.Context,
	_ *pb.EmptyRequest,
) (*pb.ServerMetrics, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var numKeys uint64 = s.numKeys.Load()
	var totalSize uint64 = s.totalSize.Load()
	var numGetRequests uint64 = s.numGetRequests.Load()
	var numSetRequests uint64 = s.numSetRequests.Load()
	var numDelRequests uint64 = s.numDelRequests.Load()
	var numNewKeysSet uint64 = s.numNewKeysSet.Load()
	var numKeysUpdated uint64 = s.numKeysUpdated.Load()
	var numGetKeyInfoRequests uint64 = s.numGetKeyInfoRequests.Load()
	var numListKeysRequests uint64 = s.numListKeysRequests.Load()
	var numPopRequests uint64 = s.numPopRequests.Load()
	var numExistsRequests uint64 = s.numExistsRequests.Load()
	return &pb.ServerMetrics{
		Keys:               &numKeys,
		TotalSize:          &totalSize,
		GetRequests:        &numGetRequests,
		SetRequests:        &numSetRequests,
		DeleteRequests:     &numDelRequests,
		NewKeysSet:         &numNewKeysSet,
		KeysUpdated:        &numKeysUpdated,
		GetKeyInfoRequests: &numGetKeyInfoRequests,
		ListKeysRequests:   &numListKeysRequests,
		PopRequests:        &numPopRequests,
		ExistsRequests:     &numExistsRequests,
	}, nil

	//return &pb.ServerMetrics{
	//	Keys:               &numKeys,
	//	TotalSize:          s.totalSize.Load(),
	//	GetRequests:        s.numGetRequests.Load(),
	//	SetRequests:        s.numSetRequests.Load(),
	//	DeleteRequests:     s.numDelRequests.Load(),
	//	NewKeysSet:         s.numNewKeysSet.Load(),
	//	KeysUpdated:        s.numKeysUpdated.Load(),
	//	GetKeyInfoRequests: s.numGetKeyInfoRequests.Load(),
	//	ListKeysRequests:   s.numListKeysRequests.Load(),
	//	PopRequests:        s.numPopRequests.Load(),
	//	ExistsRequests:     s.numExistsRequests.Load(),
	//}, nil
}

func (s *KeyValueStore) Unlock(
	ctx context.Context,
	in *pb.UnlockRequest,
) (*pb.UnlockResponse, error) {
	p, _ := peer.FromContext(ctx)

	addr := p.Addr.String()
	s.logger.Info(
		"unlock request",
		slog.String("address", addr),
		slog.String("key", in.Key),
	)
	s.numUnlockRequests.Add(1)
	s.mu.RLock()
	defer s.mu.RUnlock()
	kvInfo, ok := s.store[in.Key]
	if !ok {
		return nil, status.Errorf(
			codes.NotFound,
			ErrKeyNotFound.Error(),
		)
	}
	kvInfo.mu.Lock()
	defer kvInfo.mu.Unlock()
	if kvInfo.Locked {
		if kvInfo.unlockTimer != nil {
			_ = kvInfo.unlockTimer.Stop()
		}
		if kvInfo.expired {
			s.deleteKey(kvInfo.Key)
		}
	}
	kvInfo.Locked = false
	kvInfo.LockedAt = time.Time{}

	return &pb.UnlockResponse{Success: true}, nil
}

func (s *KeyValueStore) clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, keyInfo := range s.store {
		keyInfo.mu.RLock()
		if keyInfo.expirationTimer != nil {
			_ = keyInfo.expirationTimer.Stop()
		}
		if keyInfo.unlockTimer != nil {
			_ = keyInfo.unlockTimer.Stop()
		}
		keyInfo.mu.RUnlock()
	}
	s.store = make(map[string]*KeyValueInfo)
	s.numKeys.Store(0)
	s.totalSize.Store(0)
}

func (s *KeyValueStore) ClearHistory(
	ctx context.Context,
	_ *pb.EmptyRequest,
) (*pb.ClearHistoryResponse, error) {
	p, _ := peer.FromContext(ctx)

	addr := p.Addr.String()
	s.logger.Info(
		"clearing history",
		slog.String("address", addr),
	)

	revisionLimit := s.cfg.RevisionLimit
	data := &pb.ClearHistoryResponse{}

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, kvInfo := range s.store {
		if e := ctx.Err(); e != nil {
			s.logger.Warn(
				"interrupted clearing history",
				slog.String("error", e.Error()),
			)
			return data, e
		}
		data.Keys++

		kvInfo.mu.Lock()
		data.Cleared = data.Cleared + uint64(len(kvInfo.History))
		if revisionLimit == 0 {
			kvInfo.History = nil
		} else if revisionLimit == -1 {
			kvInfo.History = make([]*KeyValueSnapshot, 0)
		} else {
			kvInfo.History = make([]*KeyValueSnapshot, 0, revisionLimit)
		}
		kvInfo.mu.Unlock()
	}
	s.logger.Info(
		"finished clearing history",
		"keys_cleared",
		data.Keys,
		"total_cleared",
		data.Cleared,
	)
	return data, nil
}

// Clear clears all key-value pairs from the store
func (s *KeyValueStore) Clear(
	ctx context.Context,
	_ *pb.EmptyRequest,
) (*pb.ClearResponse, error) {
	p, _ := peer.FromContext(ctx)
	s.logger.Info(
		"clearing all keys",
		slog.String("address", p.Addr.String()),
	)
	s.mu.Lock()
	defer s.mu.Unlock()

	keyCount := uint64(len(s.store))

	var newTotalSize uint64

	clearResponse := &pb.ClearResponse{}

	for key, kvInfo := range s.store {
		if e := ctx.Err(); e != nil {
			s.logger.Warn(
				"interrupted clearing keys",
				slog.String("error", e.Error()),
			)
			clearResponse.KeysDeleted = keyCount - uint64(len(s.store))
			return clearResponse, e
		}
		if kvInfo.IsLocked() {
			kvInfo.mu.RLock()
			newTotalSize += kvInfo.Size
			kvInfo.mu.RUnlock()
		} else {
			kvInfo.mu.Lock()
			s.deleteKey(key)
			clearResponse.KeysDeleted++
			kvInfo.mu.Unlock()
		}
	}
	s.totalSize.Store(newTotalSize)
	s.numKeys.Store(uint64(len(s.store)))

	clearResponse.Success = true
	clearResponse.KeysDeleted = keyCount - uint64(len(s.store))
	s.logger.Info(
		fmt.Sprintf("cleared %d/%d keys", clearResponse.KeysDeleted, keyCount),
		slog.String("address", p.Addr.String()),
	)
	return clearResponse, nil
}

// ListKeys returns a list of keys in the store matching the provided
// pattern, limited to the provided count. If limit=0, all matching keys are
// returned. If a pattern nor a limit are provided, all keys currently in the
// store will be returned. This may be expensive, depending on the number of keys.
func (s *KeyValueStore) ListKeys(
	ctx context.Context,
	req *pb.ListKeysRequest,
) (*pb.ListKeysResponse, error) {
	p, _ := peer.FromContext(ctx)
	s.logger.Info(
		"listing keys",
		slog.String("address", p.Addr.String()),
		slog.String("pattern", req.Pattern),
		slog.Uint64("limit", req.Limit),
	)
	var keys []string
	if req.Limit == 0 {
		keys = make([]string, 0, len(s.store))
	} else {
		keys = make([]string, 0, req.Limit)
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	limit := int(req.Limit)
	if req.Pattern == "" {
		for key := range s.store {
			if e := ctx.Err(); e != nil {
				return &pb.ListKeysResponse{Keys: keys}, e
			}
			keys = append(keys, key)
			if limit > 0 && len(keys) >= limit {
				break
			}
		}
		return &pb.ListKeysResponse{Keys: keys}, nil
	}

	pattern, err := regexp.Compile(req.Pattern)
	if err != nil {
		return nil, status.Errorf(
			codes.InvalidArgument,
			ErrInvalidKeyPattern.Error(),
		)
	}

	for key := range s.store {
		if e := ctx.Err(); e != nil {
			return &pb.ListKeysResponse{Keys: keys}, e
		}

		if pattern.MatchString(key) {
			keys = append(keys, key)
		}
		if limit > 0 && len(keys) >= limit {
			break
		}
	}
	return &pb.ListKeysResponse{Keys: keys}, nil
}

func (s *KeyValueStore) Lock(
	ctx context.Context,
	in *pb.LockRequest,
) (*pb.LockResponse, error) {
	p, _ := peer.FromContext(ctx)

	addr := p.Addr.String()
	s.logger.Info(
		"requesting lock",
		slog.String("key", in.Key),
		slog.String("address", addr),
	)
	s.numLockRequests.Add(1)

	s.mu.RLock()
	defer s.mu.RUnlock()
	kvInfo, ok := s.store[in.Key]

	if !ok {
		return nil, status.Errorf(
			codes.NotFound,
			ErrKeyNotFound.Error(),
		)
	}
	kvInfo.mu.Lock()
	defer kvInfo.mu.Unlock()
	if kvInfo.IsLocked() {
		return nil, status.Errorf(
			codes.FailedPrecondition,
			ErrAlreadyLocked.Error(),
		)
	}

	kvInfo.Locked = true
	lockedAt := time.Now()
	if in.Duration > 0 {
		kvInfo.lockDuration = time.Duration(in.Duration) * time.Second
		if kvInfo.unlockTimer == nil {
			kvInfo.unlockTimer = time.AfterFunc(
				kvInfo.lockDuration,
				unlockFunc(s, kvInfo),
			)
		} else {
			_ = kvInfo.unlockTimer.Reset(kvInfo.lockDuration)
		}
	} else {
		if kvInfo.unlockTimer != nil {
			_ = kvInfo.unlockTimer.Stop()
			kvInfo.unlockTimer = nil
		}
	}

	s.logger.Info(
		"lock granted",
		slog.String("key", in.Key),
		slog.String("address", addr),
		slog.Duration("duration", kvInfo.lockDuration),
	)
	kvInfo.LockedAt = lockedAt
	return &pb.LockResponse{Success: true}, nil
}

// Set sets the value for the given key. If the key doesn't exist,
// it will be created. If it does, the value will be updated.
func (s *KeyValueStore) Set(ctx context.Context, in *pb.KeyValue) (
	*pb.SetResponse,
	error,
) {
	p, _ := peer.FromContext(ctx)

	addr := p.Addr.String()
	s.logger.Info(
		"setting value",
		slog.Group(
			"request",
			slog.String("key", in.Key),
			slog.String("address", addr),
			slog.Bool("lock", in.Lock),
			slog.Uint64("lock_duration", uint64(in.LockDuration)),
			slog.Uint64("expire_in", uint64(in.ExpireIn)),
		),
	)

	s.numSetRequests.Add(1)
	if uint64(len(in.Key)) > s.cfg.MaxKeySize {
		return nil, status.Errorf(
			codes.FailedPrecondition,
			fmt.Sprintf(
				"%s (max %d)",
				ErrKeyTooLong.Error(),
				s.cfg.MaxKeySize,
			),
		)
	}
	size := uint64(len(in.Value))
	if size > s.cfg.MaxValueSize {
		return nil, status.Errorf(
			codes.FailedPrecondition,
			ErrValueTooLarge.Error(),
		)
	}

	var hashValue bool
	if s.cfg.Hashing.Enabled {
		hashValue = true
	}
	hashChannel := make(chan string, 1)

	if hashValue {
		go func() {
			hashFunc := s.cfg.Hashing.Algorithm.HashFunc()
			hash := hashFunc.New().Sum(in.Value)
			hashChannel <- hex.EncodeToString(hash[:])
		}()
	}
	now := time.Now().UTC()

	s.mu.Lock()
	defer s.mu.Unlock()

	kvInfo, exists := s.store[in.Key]

	if exists {
		kvInfo.mu.Lock()
		defer kvInfo.mu.Unlock()
		if kvInfo.IsLocked() {
			return nil, status.Errorf(
				codes.PermissionDenied,
				ErrLocked.Error(),
			)
		}

		s.numKeysUpdated.Add(1)
		kvInfo.addSnapshotToHistory(s.cfg.RevisionLimit)
		s.totalSize.Add(^(kvInfo.Size - 1))
		s.totalSize.Add(size)
		kvInfo.Size = size
		kvInfo.Updated = now
		kvInfo.Version++

		if in.Lock {
			kvInfo.Locked = true
			kvInfo.LockedAt = now
			if kvInfo.unlockTimer != nil {
				_ = kvInfo.unlockTimer.Stop()
			}

			if in.LockDuration > 0 {
				lockDuration := time.Duration(in.LockDuration) * time.Second
				kvInfo.lockDuration = lockDuration
				if kvInfo.unlockTimer == nil {
					kvInfo.unlockTimer = time.AfterFunc(
						lockDuration, unlockFunc(s, kvInfo),
					)
				} else {
					kvInfo.unlockTimer.Reset(lockDuration)
				}
				slog.Info(
					"locking key",
					slog.String("key", kvInfo.Key),
					slog.Duration("duration", lockDuration),
				)
			}
		}

		if hashValue {
			kvInfo.Hash = <-hashChannel
		}

		kvInfo.Value = in.Value
		slog.Info(
			"updated value",
			slog.String("key", kvInfo.Key),
			slog.Uint64("version", kvInfo.Version),
			slog.String("hash", kvInfo.Hash),
			slog.Time("updated", now),
			slog.Time("created", now),
			slog.String("address", addr),
			slog.Bool("Locked", kvInfo.Locked),
		)
		return &pb.SetResponse{Success: true}, nil
	}

	if s.cfg.MaxNumberOfKeys > 0 && s.numKeys.Load() >= s.cfg.MaxNumberOfKeys {
		return nil, status.Errorf(
			codes.ResourceExhausted,
			ErrMaxKeysReached.Error(),
		)
	}

	kvInfo = &KeyValueInfo{
		Key:     in.Key,
		Value:   in.Value,
		Created: now,
		Size:    size,
	}
	kvInfo.mu.Lock()
	defer kvInfo.mu.Unlock()
	if in.ExpireIn > 0 {
		kvInfo.ExpireAfter = in.ExpireIn
		kvInfo.expiresAfter = time.Duration(in.ExpireIn) * time.Second
		kvInfo.expirationTimer = time.AfterFunc(
			kvInfo.expiresAfter,
			expireFunc(s, kvInfo),
		)
		s.logger.Info(
			"setting expiration",
			slog.String("key", kvInfo.Key),
			slog.Duration("duration", kvInfo.expiresAfter),
		)
	}
	if in.Lock {
		kvInfo.Locked = true
		kvInfo.LockedAt = now
		if in.LockDuration > 0 {
			kvInfo.LockDuration = in.LockDuration
			kvInfo.lockDuration = time.Duration(in.LockDuration) * time.Second
			kvInfo.unlockTimer = time.AfterFunc(
				kvInfo.lockDuration,
				unlockFunc(s, kvInfo),
			)
			s.logger.Info(
				"locking key",
				slog.String("key", kvInfo.Key),
				slog.Duration("duration", kvInfo.lockDuration),
			)
		}
	}
	if s.cfg.RevisionLimit == -1 {
		kvInfo.History = make([]*KeyValueSnapshot, 0)
	} else if s.cfg.RevisionLimit > 0 {
		kvInfo.History = make(
			[]*KeyValueSnapshot,
			0,
			s.cfg.RevisionLimit,
		)
	}

	if hashValue {
		kvInfo.Hash = <-hashChannel
	}
	if in.ContentType != "" {
		kvInfo.ContentType = in.ContentType
	}
	if kvInfo.ContentType == "" && kvInfo.Value != nil {
		kvInfo.ContentType = http.DetectContentType(kvInfo.Value)
	}

	s.store[in.Key] = kvInfo
	s.numKeys.Add(1)
	s.totalSize.Add(size)
	s.numNewKeysSet.Add(1)
	slog.Info(
		"created key",
		slog.String("key", kvInfo.Key),
		slog.Uint64("version", kvInfo.Version),
		slog.Time("created", now),
		slog.String("address", addr),
	)
	return &pb.SetResponse{Success: true, IsNew: true}, nil
}

// TotalSize returns the total size of all values
func (s *KeyValueStore) TotalSize() uint64 {
	return s.totalSize.Load()
}

// Exists returns true if a key exists
func (s *KeyValueStore) Exists(ctx context.Context, in *pb.Key) (
	*pb.ExistsResponse,
	error,
) {
	s.numExistsRequests.Add(1)
	p, _ := peer.FromContext(ctx)

	s.logger.Info(
		"checking if key exists",
		slog.String("key", in.Key),
		slog.String("address", p.Addr.String()),
	)

	s.mu.RLock()
	defer s.mu.RUnlock()

	kvInfo := s.getKey(in.Key)
	if kvInfo == nil {
		return &pb.ExistsResponse{Exists: false}, nil
	}

	return &pb.ExistsResponse{Exists: true}, nil
}

// Pop returns a key-value pair and deletes the key
func (s *KeyValueStore) Pop(ctx context.Context, in *pb.Key) (
	*pb.GetResponse,
	error,
) {
	p, _ := peer.FromContext(ctx)
	s.logger.Info(
		"popping key",
		slog.String("key", in.Key),
		slog.String("address", p.Addr.String()),
	)
	s.numPopRequests.Add(1)

	s.mu.Lock()
	defer s.mu.Unlock()

	kvInfo := s.getKey(in.Key)
	if kvInfo == nil {
		return nil, status.Errorf(
			codes.NotFound,
			ErrKeyNotFound.Error(),
		)
	}
	kvInfo.mu.Lock()
	defer kvInfo.mu.Unlock()
	if kvInfo.IsLocked() {
		return nil, status.Errorf(
			codes.PermissionDenied,
			ErrLocked.Error(),
		)
	}

	getResponse := &pb.GetResponse{Value: kvInfo.Value}
	s.deleteKey(in.Key)
	return getResponse, nil
}

// Get returns the value of a key
func (s *KeyValueStore) Get(ctx context.Context, in *pb.Key) (
	*pb.GetResponse,
	error,
) {

	s.numGetRequests.Add(1)
	p, _ := peer.FromContext(ctx)

	s.logger.Info(
		"getting key",
		slog.String("key", in.Key),
		slog.String("address", p.Addr.String()),
	)

	s.mu.RLock()
	defer s.mu.RUnlock()

	kvInfo := s.getKey(in.Key)
	if kvInfo == nil {
		return nil, status.Errorf(
			codes.NotFound,
			ErrKeyNotFound.Error(),
		)
	}

	kvInfo.mu.RLock()
	defer kvInfo.mu.RUnlock()
	go kvInfo.setAccessed(time.Now().UTC())
	return &pb.GetResponse{Value: kvInfo.Value}, nil
}

func (kv *KeyValueInfo) setAccessed(ts time.Time) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.Accessed = ts
}

// Delete deletes a key from the store
func (s *KeyValueStore) Delete(ctx context.Context, in *pb.Key) (
	*pb.DeleteResponse,
	error,
) {
	s.numDelRequests.Add(1)
	p, _ := peer.FromContext(ctx)

	s.logger.Info(
		"deleting key",
		slog.String("key", in.Key),
		slog.String("address", p.Addr.String()),
	)
	s.mu.Lock()
	defer s.mu.Unlock()

	kvInfo := s.getKey(in.Key)
	if kvInfo == nil {
		return &pb.DeleteResponse{Deleted: false}, status.Errorf(
			codes.NotFound,
			ErrKeyNotFound.Error(),
		)
	}
	kvInfo.mu.Lock()
	defer kvInfo.mu.Unlock()
	if kvInfo.IsLocked() {
		return &pb.DeleteResponse{Deleted: false}, status.Errorf(
			codes.PermissionDenied,
			ErrLocked.Error(),
		)
	}
	s.deleteKey(in.Key)
	return &pb.DeleteResponse{Deleted: true}, nil
}

func (s *KeyValueStore) deleteKey(key string) {
	kvInfo, ok := s.store[key]
	if !ok {
		return
	}
	s.logger.Info(
		"deleting key",
		slog.String("key", kvInfo.Key),
		slog.Uint64("version", kvInfo.Version),
		slog.String("hash", kvInfo.Hash),
		slog.Time("updated", kvInfo.Updated),
		slog.Time("created", kvInfo.Created),
	)
	delete(s.store, key)
	s.totalSize.Add(^(kvInfo.Size - 1))
	s.numKeys.Add(^uint64(0))
	if kvInfo.expirationTimer != nil {
		_ = kvInfo.expirationTimer.Stop()
	}
	if kvInfo.unlockTimer != nil {
		_ = kvInfo.unlockTimer.Stop()
	}
}

// getKey returns the KeyValueInfo associated with the given
// key name. If KeyValueInfo is expired (but not Locked), it
// will be deleted.
// nil will be returned for any key that isn't found, or is
// deleted.
func (s *KeyValueStore) getKey(key string) *KeyValueInfo {
	keyInfo, ok := s.store[key]
	if !ok {
		return nil
	}
	return keyInfo
}

// GetKeyInfo returns information about a key
func (s *KeyValueStore) GetKeyInfo(
	ctx context.Context,
	in *pb.Key,
) (*pb.GetKeyValueInfoResponse, error) {
	s.numGetKeyInfoRequests.Add(1)
	p, _ := peer.FromContext(ctx)

	s.logger.Info(
		"getting key info",
		slog.String("key", in.Key),
		slog.String("address", p.Addr.String()),
	)

	s.mu.RLock()
	defer s.mu.RUnlock()

	kvInfo := s.getKey(in.Key)
	if kvInfo == nil {
		return nil, status.Errorf(
			codes.NotFound,
			ErrKeyNotFound.Error(),
		)
	}
	kvInfo.mu.RLock()
	defer kvInfo.mu.RUnlock()
	resp := &pb.GetKeyValueInfoResponse{
		Key:         kvInfo.Key,
		Hash:        kvInfo.Hash,
		Created:     kvInfo.Created.UnixNano(),
		Version:     kvInfo.Version,
		Size:        kvInfo.Size,
		ContentType: kvInfo.ContentType,
	}
	if !kvInfo.Updated.IsZero() {
		resp.Updated = kvInfo.Updated.UnixNano()
	}
	if !kvInfo.Accessed.IsZero() {
		resp.Accessed = kvInfo.Accessed.UnixNano()
	}
	if kvInfo.IsLocked() {
		resp.Locked = true
	}
	return resp, nil
}

// KeyValueSnapshot is a snapshot of a key-value pair and associated info
type KeyValueSnapshot struct {
	Timestamp    time.Time
	historyTimer *time.Timer
	KeyValueInfo
}

// KeyValueInfo is the internal representation of a key-value pair and
// associated metadata.
type KeyValueInfo struct {
	Key             string    `json:"key"`
	Value           []byte    `json:"value"`
	ContentType     string    `json:"content_type,omitempty"`
	Size            uint64    `json:"size"`
	Hash            string    `json:"hash,omitempty"`
	Created         time.Time `json:"created"`
	Updated         time.Time `json:"updated,omitempty"`
	Accessed        time.Time `json:"accessed,omitempty"`
	ExpireAfter     uint32    `json:"expires_after,omitempty"`
	expiresAfter    time.Duration
	Locked          bool   `json:"locked"`
	LockDuration    uint32 `json:"unlock_after,omitempty"`
	lockDuration    time.Duration
	LockedAt        time.Time           `json:"locked_at,omitempty"`
	Version         uint64              `json:"version"`
	History         []*KeyValueSnapshot `json:"-"`
	expired         bool
	expirationTimer *time.Timer
	unlockTimer     *time.Timer
	mu              sync.RWMutex
}

// expireFunc returns a function to be used with time.AfterFunc to
// set a key as expired after the timer has passed. If the key is
// locked, it won't be deleted until unlocked.
func expireFunc(s *KeyValueStore, kvInfo *KeyValueInfo) func() {
	return func() {
		kvInfo.mu.Lock()
		defer kvInfo.mu.Unlock()

		s.logger.Info("expiring key on trigger", slog.String("key", kvInfo.Key))
		kvInfo.expired = true

		if kvInfo.Locked {
			s.logger.Info(
				"key is locked, skipping delete",
				slog.String("key", kvInfo.Key),
			)
			return
		}

		if !s.cfg.KeepExpiredKeys {
			s.logger.Info(
				"key is not locked, deleting",
				slog.String("key", kvInfo.Key),
			)
			s.mu.Lock()
			defer s.mu.Unlock()
			currentKV, ok := s.store[kvInfo.Key]
			if ok && currentKV == kvInfo {
				s.logger.Info(
					"deleting expired key on trigger",
					slog.String("key", kvInfo.Key),
				)
				if currentKV == kvInfo {
					s.deleteKey(kvInfo.Key)
				}
			}
		}
	}
}

func setHash(kv *KeyValueInfo, h crypto.Hash) {
	hash := h.New().Sum(kv.Value)
	kv.Hash = hex.EncodeToString(hash[:])
	if kv.ContentType == "" && kv.Value != nil {
		kv.ContentType = http.DetectContentType(kv.Value)
	}
}

func setExpiredLocked(s *KeyValueStore, kvInfo *KeyValueInfo) {
	if kvInfo.Locked {
		if kvInfo.LockDuration > 0 {
			kvInfo.lockDuration = time.Duration(kvInfo.LockDuration) * time.Second
			kvInfo.unlockTimer = time.AfterFunc(
				kvInfo.lockDuration, unlockFunc(s, kvInfo),
			)
		}
	}
	if kvInfo.ExpireAfter > 0 {
		kvInfo.expiresAfter = time.Duration(kvInfo.ExpireAfter) * time.Second
		kvInfo.expirationTimer = time.AfterFunc(
			kvInfo.expiresAfter, expireFunc(s, kvInfo),
		)
	}

}

// unlockFunc returns a function to be used with time.AfterFunc to unlock a
// key after the timer has passed. If the key is expired, it will be deleted.
func unlockFunc(s *KeyValueStore, kvInfo *KeyValueInfo) func() {
	return func() {
		kvInfo.mu.Lock()
		defer kvInfo.mu.Unlock()
		kvInfo.Locked = false
		s.logger.Info(
			"unlocking key on trigger",
			slog.String("key", kvInfo.Key),
		)
		if kvInfo.unlockTimer != nil {
			_ = kvInfo.unlockTimer.Stop()
		}

		if kvInfo.expired && !s.cfg.KeepExpiredKeys {
			s.mu.Lock()
			defer s.mu.Unlock()
			currentKV, ok := s.store[kvInfo.Key]
			if ok && currentKV == kvInfo {
				s.logger.Info(
					"deleting expired key on trigger",
					slog.String("key", kvInfo.Key),
				)
				s.deleteKey(kvInfo.Key)
			}
		}
	}
}

func (kv *KeyValueInfo) IsLocked() bool {
	return kv.Locked
}

func (kv *KeyValueInfo) IsExpired() bool {
	return kv.expired
}

// snapshot returns a snapshot of the current key-value pair
func (kv *KeyValueInfo) snapshot() *KeyValueSnapshot {
	size := len(kv.Value)
	value := make([]byte, size, size)
	copy(value, kv.Value)
	snapshot := &KeyValueSnapshot{Timestamp: time.Now()}
	snapshot.KeyValueInfo = *kv
	snapshot.Value = value
	return snapshot
}

// addSnapshotToHistory adds a snapshot of the current key-value pair to the
// history. If the history is full, the oldest snapshot is removed.
func (kv *KeyValueInfo) addSnapshotToHistory(limit int64) {

	if kv.History == nil || limit == 0 {
		return
	}
	if limit == -1 {
		kv.History = append(kv.History, kv.snapshot())
		return
	}
	if len(kv.History) >= cap(kv.History) {
		tmp := kv.History[1:]
		for ind := 0; ind < len(tmp); ind++ {
			kv.History[ind] = tmp[ind]
		}
		kv.History[len(kv.History)-1] = kv.snapshot()
		return
	}
	kv.History = append(kv.History, kv.snapshot())
}

// Dump writes a JSON dump of the key-value store's current state to
// a temporary file, then renames the temp file to the given path.
func (s *KeyValueStore) Dump(filename string) error {
	s.logger.Info(
		"backing up store", slog.String("bacup_file", filename),
	)
	data, err := json.Marshal(s)
	if err != nil {
		return fmt.Errorf("unable to marshal json: %w", err)
	}
	basename := filepath.Base(filename)
	tmp, err := os.CreateTemp("", fmt.Sprintf("%s.*", basename))
	if err != nil {
		return fmt.Errorf("error creating temp file '%s': %w", basename, err)
	}
	tmpFilename := tmp.Name()
	_, err = tmp.Write(data)
	tmp.Close()
	if err != nil {
		return fmt.Errorf(
			"error writing to temp file '%s': %w",
			tmpFilename,
			err,
		)
	}
	err = os.Rename(tmpFilename, filename)
	if err != nil {
		return fmt.Errorf(
			"error renaming temp file '%s' to '%s': %w",
			tmpFilename,
			filename,
			err,
		)
	}
	s.logger.Info(
		"backup complete", slog.String("bacup_file", filename),
	)
	return nil
}
