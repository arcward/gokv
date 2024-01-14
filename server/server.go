package server

import (
	"context"
	"encoding/json"
	"errors"
	"expvar"
	"fmt"
	pb "github.com/arcward/keyquarry/api"
	"github.com/arcward/keyquarry/build"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"hash/fnv"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	_ "net/http/pprof"
	"net/url"
	"os"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	LevelEvent                           = slog.Level(100)
	DefaultGracefulStopTimeout           = 30 * time.Second
	DefaultKeepaliveTime                 = 10 * time.Second
	DefaultKeepaliveTimeout              = 60 * time.Second
	DefaultEventStreamBufferSize         = 10000
	DefaultEventStreamSendTimeout        = time.Second
	DefaultMaxKeyLength           uint64 = 1000 // 1,000 bytes
	MinKeyLength                         = 1
	DefaultMaxValueSize           uint64 = 1000000 // 1,000,000 bytes
	DefaultRevisionLimit          int64  = 0
	DefaultMaxKeys                uint64 = 0
	DefaultMaxLockDuration               = 24 * time.Hour
	DefaultMinLockDuration               = 5 * time.Second
	DefaultMinExpiry                     = 5 * time.Second
	DefaultSnapshotExpireAfter           = 24 * time.Hour
	DefaultMaxPruneHistory               = 100
	// reservedPrefix is the prefix used for internal keys - these keys can't
	// be modified with normal methods, and are excluded from dumps
	reservedPrefix        = "keyquarry"
	clientIDKey           = "client_id"
	MinPruneInterval      = 30 * time.Second
	DefaultPruneThreshold = 0.95
	DefaultPruneTarget    = 0.90
	kvLogKey              = "kv"
	loggerKey             = "logger"
	reservedClientID      = "keyquarry"
	DefaultNetwork        = "tcp"
	DefaultHost           = "localhost"
	DefaultPort           = 33969
	DefaultMonitorPort    = 33970
)

var (
	DefaultAddress        = fmt.Sprintf("%s:%d", DefaultHost, DefaultPort)
	DefaultMonitorAddress = fmt.Sprintf(
		"%s:%d",
		DefaultHost,
		DefaultMonitorPort,
	)
	startupKeyStarted         = fmt.Sprintf("%s/Started", reservedPrefix)
	startupKeyMaxLockDuration = fmt.Sprintf(
		"%s/MaxLockDuration",
		reservedPrefix,
	)
	startupKeyMinLockDuration = fmt.Sprintf(
		"%s/MinLockDuration",
		reservedPrefix,
	)
	startupKeyMinLifespan      = fmt.Sprintf("%s/MinLifespan", reservedPrefix)
	startupKeyMaxKeys          = fmt.Sprintf("%s/MaxKeys", reservedPrefix)
	startupKeyMinPruneInterval = fmt.Sprintf(
		"%s/MinPruneInterval",
		reservedPrefix,
	)
	startupKeyPruneInterval  = fmt.Sprintf("%s/PruneInterval", reservedPrefix)
	startupKeyPruneThreshold = fmt.Sprintf(
		"%s/PruneThreshold",
		reservedPrefix,
	)
	startupKeyPruneTarget     = fmt.Sprintf("%s/PruneTarget", reservedPrefix)
	startupKeyEagerPrune      = fmt.Sprintf("%s/EagerPrune", reservedPrefix)
	startupKeyMaxValueSize    = fmt.Sprintf("%s/MaxValueSize", reservedPrefix)
	startupKeyMaxKeySize      = fmt.Sprintf("%s/MaxKeyLength", reservedPrefix)
	startupKeyRevisionLimit   = fmt.Sprintf("%s/RevisionLimit", reservedPrefix)
	startupKeySnapshotEnabled = fmt.Sprintf(
		"%s/Snapshot/Enabled",
		reservedPrefix,
	)
	startupKeySnapshotInterval = fmt.Sprintf(
		"%s/Snapshot/Interval",
		reservedPrefix,
	)
	startupKeyEventStreamSubscriberLimit = fmt.Sprintf(
		"%s/EventStreamSubscriberLimit",
		reservedPrefix,
	)
	ErrKeyTooLong = &GOKVError{
		Message: "Key length greater than maximum",
		Code:    codes.FailedPrecondition,
	}
	ErrValueTooLarge = GOKVError{
		Message: "value size is greater than maximum",
		Code:    codes.FailedPrecondition,
	}
	ErrKeyNotFound = GOKVError{
		Message: "Key not found",
		Code:    codes.NotFound,
	}
	ErrEmptyKey = GOKVError{
		Message: "Empty key",
		Code:    codes.InvalidArgument,
	}
	ErrMaxKeysReached = GOKVError{
		Message: "maximum number of keys reached",
		Code:    codes.ResourceExhausted,
	}
	ErrInvalidKeyPattern = GOKVError{
		Message: "invalid key pattern",
		Code:    codes.InvalidArgument,
	}
	ErrLocked = GOKVError{
		Message: "Key is locked",
		Code:    codes.PermissionDenied,
	}
	ErrServerPendingInit = GOKVError{
		Message: "server is pending initialization",
		Code:    codes.Unavailable,
	}
	ErrVersioningDisabled = GOKVError{
		Message: "versioning is not enabled",
		Code:    codes.FailedPrecondition,
	}
	ErrRevisionNotFound = GOKVError{
		Message: "revision not found",
		Code:    codes.NotFound,
	}
	ErrWrongUnlockToken = GOKVError{
		Message: "unlock token provided does not match",
		Code:    codes.PermissionDenied,
	}
	ErrInvalidLockDuration = GOKVError{
		Message: "invalid lock Duration",
		Code:    codes.OutOfRange,
	}
	ErrLockDurationTooLong = GOKVError{
		Message: "lock Duration too long",
		Code:    codes.OutOfRange,
	}
	ErrReadOnlyServer = GOKVError{
		Message: "server in readonly mode",
		Code:    codes.FailedPrecondition,
	}
	ErrReservedKeyPrefix = GOKVError{
		Message: fmt.Sprintf("Key cannot begin with '%s'", reservedPrefix),
		Code:    codes.InvalidArgument,
	}
)

type KeyValueStore struct {
	store     map[string]*KeyValueInfo
	logger    *slog.Logger
	cfg       *Config
	cfgMu     sync.RWMutex
	numKeys   atomic.Uint64
	totalSize atomic.Uint64
	Tracer    trace.Tracer

	numEventValueAccessed atomic.Uint64
	numEventCreated       atomic.Uint64
	numEventUpdated       atomic.Uint64
	numEventDeleted       atomic.Uint64
	numEventLocked        atomic.Uint64
	numEventUnlocked      atomic.Uint64
	numEventExpired       atomic.Uint64
	numEventExpunged      atomic.Uint64
	numEventLifespanSet   atomic.Uint64
	numEventSubscribers   atomic.Uint64
	numSnapshotsCreated   atomic.Uint64

	clientIDs atomic.Uint64

	keyStats  map[string]*KeyMetrics
	keyStatMu sync.RWMutex

	// mu is the main mutex for the server
	mu sync.RWMutex

	// clientInfo is a map of client IDs to ClientInfo
	clientInfo map[string]*ClientInfo
	// clientInfo sync.Map
	// cmu is a mutex specific to clientInfo
	cmu sync.RWMutex

	// eventStream handles publishing of events to subscribers other
	// than snapshotter and the event logger
	eventStream *eventStream
	// snapshotter handles snapshotting of the server, which can either
	// happen on regular intervals, or only on shutdown
	snapshotter *Snapshotter
	started     bool
	// stopped indicates the Stop function has started
	stopped bool
	// locks is a map of key to KeyLock values
	locks map[string]*KeyLock
	// lockMu is a mutex specific to locks
	lockMu sync.RWMutex
	// reapers track keys that are set to expire after a certain
	// amount of time
	reapers  map[string]*KeyReaper
	reaperMu sync.RWMutex
	// pruner handles the scheduled pruning of keys over the configured threshold
	pruner *Pruner
	// pruneHistory is a log of pruning events, up to the 100 most recent
	pruneHistory []PruneLog
	// events is the main channel for emitting events. This channel is
	// closed as the final step of Stop
	events     chan Event
	MonitorMux *http.ServeMux
	pb.UnimplementedKeyValueStoreServer
}

func (s *KeyValueStore) ServeMonitor() {
	s.logger.Info("serving monitor", "address", s.cfg.MonitorAddress)
	srv := &http.Server{
		Addr:    s.cfg.MonitorAddress,
		Handler: s.MonitorMux,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			s.logger.Error("error serving monitor", "error", err)
		}
	}()
}

// Set sets the value for the given key. If the key doesn't exist,
// it will be created. If it does, the value will be updated.
func (s *KeyValueStore) Set(ctx context.Context, in *pb.KeyValue) (
	*pb.SetResponse,
	error,
) {
	span := trace.SpanFromContext(ctx)

	span.SetAttributes(attribute.String("key", in.Key))

	logger := s.requestLogger(ctx)
	logger.Info(
		"request to set/update value",
		slog.Group(
			"request",
			slog.String("key", in.Key),
			slog.Any("lock_duration", in.LockDuration),
			slog.Any("lifespan", in.Lifespan),
		),
	)
	if len(in.Key) < MinKeyLength {
		return nil, ErrEmptyKey
	}
	if strings.HasPrefix(strings.ToLower(in.Key), reservedPrefix) {
		return nil, ErrReservedKeyPrefix
	}

	s.cfgMu.RLock()
	cfg := *s.cfg
	s.cfgMu.RUnlock()
	if cfg.Readonly {
		return nil, ErrReadOnlyServer
	}

	if cfg.MaxKeyLength > 0 && uint64(len(in.Key)) > cfg.MaxKeyLength {
		slog.Info(
			"key too long",
			slog.String("key", in.Key),
			slog.Uint64("max", cfg.MaxKeyLength),
		)
		return nil, ErrKeyTooLong
	}

	size := uint64(len(in.Value))
	if cfg.MaxValueSize > 0 && size > cfg.MaxValueSize {
		return nil, ErrValueTooLarge
	}

	var lockDuration *time.Duration
	var expireAfter *time.Duration

	// Validate any explicitly provided lock Duration
	if in.LockDuration != nil {
		d := in.LockDuration.AsDuration()
		if d > cfg.MaxLockDuration {
			return nil, ErrLockDurationTooLong
		}
		if d == 0 || d < cfg.MinLockDuration {
			return nil, ErrInvalidLockDuration
		}
		lockDuration = &d
	}
	if in.Lifespan != nil {
		e := in.Lifespan.AsDuration()
		if e == 0 || e < cfg.MinLifespan {
			return nil, GOKVError{
				Message: "invalid expire_after",
				Code:    codes.InvalidArgument,
			}
		}
		expireAfter = &e
	}

	var kvInfo *KeyValueInfo

	s.mu.Lock()
	kvInfo, exists := s.store[in.Key]

	clientID := s.ClientID(ctx)

	// Update an existing key
	if exists {
		s.lockMu.Lock()
		defer s.lockMu.Unlock()

		keyLock, alreadyLocked := s.locks[in.Key]

		if alreadyLocked && clientID != keyLock.ClientID {
			s.mu.Unlock()
			return nil, ErrWrongUnlockToken
		}

		// When updating the value, stop any current reaper.
		// If a lifespan was provided, create a new reaper using
		// that duration. Otherwise, create a new reaper with
		// the existing lifespan.
		s.reaperMu.Lock()
		defer s.reaperMu.Unlock()

		keyReaper, reaperExists := s.reapers[in.Key]
		if reaperExists {
			_ = keyReaper.t.Stop()
			if expireAfter == nil {
				expireAfter = &keyReaper.Lifespan
				logger.Debug(
					"reaper will be reset with existing lifespan",
					"lifespan",
					expireAfter,
				)
			}
			delete(s.reapers, in.Key)
		}

		if expireAfter != nil {
			reaper := NewKeyReaper(
				s,
				*expireAfter,
				in.Key,
				clientID,
			)
			s.reapers[in.Key] = reaper
		}

		hashChannel := make(chan uint64, 1)
		go func() {
			hashFunc := fnv.New64a()
			_, he := hashFunc.Write(in.Value)
			if he != nil {
				s.logger.Error(
					"error hashing value",
					"error", he,
				)
			}
			hashChannel <- hashFunc.Sum64()
		}()

		if lockDuration != nil {
			// If a lock Duration was provided, update the existing
			// key, and reset any existing unlock timer if necessary
			if keyLock != nil {
				delete(s.locks, in.Key)
				_ = keyLock.t.Stop()

			}
			keyLock = NewKeyLock(s, *lockDuration, in.Key, clientID)
			s.locks[in.Key] = keyLock
		}
		kvInfo.mu.Lock()
		s.mu.Unlock()
		defer kvInfo.mu.Unlock()

		// Check back in on our hash function, and only consider it
		// an updated value if the hash has changed, or there's no
		// hash function set
		newHash := <-hashChannel
		if newHash != kvInfo.Hash {
			s.logger.Info(
				"updated value",
				kvLogKey, kvInfo,
			)
			now := time.Now()

			if cfg.RevisionLimit > 0 {
				if kvInfo.History == nil {
					kvInfo.History = NewKVHistory(cfg.RevisionLimit)
				}
				kvInfo.History.Add(kvInfo)
			}
			s.totalSize.Add(^(kvInfo.Size - 1))
			s.totalSize.Add(size)
			kvInfo.Size = size
			kvInfo.Updated = now
			kvInfo.Value = in.Value
			kvInfo.Version++
			kvInfo.Hash = newHash
			defer s.emit(in.Key, Updated, clientID, &now)
		} else {
			s.logger.Info(
				"no change to value",
				kvLogKey, kvInfo,
			)
		}
		return &pb.SetResponse{
			Success: true,
		}, nil
	}

	defer s.mu.Unlock()
	currentCt := len(s.store)
	maxKeys := int(cfg.MaxNumberOfKeys)
	if maxKeys > 0 && currentCt >= maxKeys {
		overLimit := currentCt - maxKeys
		s.logger.Warn(
			"too many keys",
			"current", currentCt,
			"max", maxKeys,
			"over_limit", overLimit,
		)
		if s.cfg.EagerPrune {
			s.keyStatMu.Lock()
			pruned := s.pruneNumKeys(ctx, overLimit+1, true, in.Key)
			s.keyStatMu.Unlock()
			s.logger.Debug(
				"pruned keys",
				"count",
				len(pruned),
				"keys",
				pruned,
			)
			newCount := s.numKeys.Load()
			if int(newCount) >= maxKeys {
				s.logger.Warn(
					"pruned keys, but still at limit",
					"before",
					currentCt,
					"after",
					newCount,
				)
				return nil, ErrMaxKeysReached
			}
		} else {
			return nil, ErrMaxKeysReached
		}
	} else {
		s.logger.Info(
			"key counts",
			"current",
			currentCt,
			"max",
			cfg.MaxNumberOfKeys,
		)
	}

	kvInfo = NewKeyValue(
		s,
		in.Key,
		in.Value,
		in.ContentType,
		clientID,
		cfg.RevisionLimit,
	)
	kvInfo.mu.RLock()

	s.addKeyValueInfo(kvInfo, size)

	if in.LockDuration != nil {
		s.lockMu.Lock()
		s.locks[in.Key] = NewKeyLock(s, *lockDuration, in.Key, clientID)
		s.lockMu.Unlock()
	}

	if expireAfter != nil {
		s.reaperMu.Lock()
		s.reapers[in.Key] = NewKeyReaper(
			s,
			*expireAfter,
			in.Key,
			clientID,
		)
		s.reaperMu.Unlock()
	}

	logger.Info(
		"created key",
		kvLogKey, kvInfo,
		slog.Group(
			"metrics",
			"keys", s.numKeys.Load(),
			"total_size", s.totalSize.Load(),
		),
	)
	kvInfo.mu.RUnlock()
	return &pb.SetResponse{
		Success: true,
		IsNew:   true,
	}, nil
}

// Get returns the value of a key
func (s *KeyValueStore) Get(ctx context.Context, in *pb.Key) (
	*pb.GetResponse,
	error,
) {
	span := trace.SpanFromContext(ctx)
	span.SetAttributes(attribute.String("key", in.Key))

	logger := s.requestLogger(ctx)
	logger.Info(
		"getting key",
		slog.String("key", in.Key),
	)

	s.mu.RLock()
	kvInfo := s.getKey(in.Key)
	if kvInfo == nil {
		s.mu.RUnlock()
		return nil, ErrKeyNotFound
	}

	s.lockMu.RLock()
	keyLock, locked := s.locks[in.Key]
	s.lockMu.RUnlock()
	clientID := s.ClientID(ctx)
	if locked && clientID != keyLock.ClientID {
		s.mu.RUnlock()
		logger.Info(
			"cannot get key locked by another client", kvLogKey, kvInfo,
		)
		return nil, ErrLocked
	}

	kvInfo.mu.RLock()
	s.mu.RUnlock()
	defer kvInfo.mu.RUnlock()
	t := time.Now()
	defer s.emit(in.Key, Accessed, clientID, &t)
	return &pb.GetResponse{Value: kvInfo.Value}, nil
}

func (s *KeyValueStore) Lock(
	ctx context.Context,
	in *pb.LockRequest,
) (*pb.LockResponse, error) {
	span := trace.SpanFromContext(ctx)
	span.SetAttributes(attribute.String("key", in.Key))

	logger := s.requestLogger(ctx)
	clientID := s.ClientID(ctx)
	logger.Info(
		"requesting lock",
		slog.String("key", in.Key),
	)

	if in.CreateIfMissing && len(in.Key) < MinKeyLength {
		return nil, ErrEmptyKey
	}

	if strings.HasPrefix(strings.ToLower(in.Key), reservedPrefix) {
		return nil, ErrReservedKeyPrefix
	}

	s.cfgMu.RLock()
	cfg := *s.cfg
	s.cfgMu.RUnlock()
	if cfg.Readonly {
		return nil, ErrReadOnlyServer
	}

	if in.CreateIfMissing && cfg.MaxKeyLength > 0 && uint64(len(in.Key)) > cfg.MaxKeyLength {
		return nil, ErrKeyTooLong
	}

	if in.Duration == nil {
		return nil, ErrInvalidLockDuration
	}
	var lockDuration time.Duration
	lockDuration = in.Duration.AsDuration()

	if lockDuration > s.cfg.MaxLockDuration {
		return nil, ErrLockDurationTooLong
	}
	if lockDuration == 0 || lockDuration < s.cfg.MinLockDuration {
		return nil, ErrInvalidLockDuration
	}

	// Only need a write lock if we're potentially adding a new key from
	// this request
	s.mu.Lock()

	var kvInfo *KeyValueInfo

	kvInfo, ok := s.store[in.Key]

	// Already exists - if the client owns the lock, then replace the
	// current lock with a new one
	if ok {
		s.mu.Unlock()
		s.lockMu.Lock()
		defer s.lockMu.Unlock()

		keyLock, alreadyLocked := s.locks[in.Key]
		if alreadyLocked {
			if clientID != keyLock.ClientID {
				return nil, GOKVError{
					Message: "locked by another client",
					Code:    codes.PermissionDenied,
				}
			}
			logger.Debug("renewing lock", slog.String("key", in.Key))
			delete(s.locks, in.Key)
			_ = keyLock.t.Stop()
		}
		s.locks[in.Key] = NewKeyLock(s, lockDuration, in.Key, clientID)
		logger.Info(
			"lock granted",
			"key_lock", s.locks[in.Key],
		)
		return &pb.LockResponse{Success: true}, nil
	}
	defer s.mu.Unlock()
	if !in.CreateIfMissing {
		return nil, ErrKeyNotFound
	}

	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	maxKeys := int(cfg.MaxNumberOfKeys)
	if in.CreateIfMissing && maxKeys > 0 {
		currentCount := int(s.numKeys.Load())

		if currentCount >= maxKeys {
			s.logger.Warn(
				"max number of keys reached",
				slog.Group(
					"key_count",
					"current", currentCount,
					"max", cfg.MaxNumberOfKeys,
				),
			)
			if cfg.EagerPrune {
				overLimit := currentCount - maxKeys
				s.logger.Info("pruning keys", "over_limit", overLimit)
				s.keyStatMu.RLock()
				pruned := s.pruneNumKeys(ctx, overLimit+1, true, in.Key)
				s.keyStatMu.RUnlock()
				s.logger.Info(
					"pruned keys",
					"count",
					len(pruned),
					"keys",
					pruned,
				)
				newCount := s.numKeys.Load()
				if int(newCount) >= maxKeys {
					s.logger.Warn(
						"pruned keys, but still at limit",
						"before",
						currentCount,
						"after",
						newCount,
					)
					return nil, ErrMaxKeysReached
				}
			} else {
				return nil, ErrMaxKeysReached
			}

		} else {
			s.logger.Debug(
				"key counts",
				"current",
				currentCount,
				"max",
				cfg.MaxNumberOfKeys,
			)
		}
	}

	d := in.Duration.AsDuration()
	kvInfo = NewKeyValue(
		s,
		in.Key,
		nil,
		"",
		clientID,
		cfg.RevisionLimit,
	)

	s.addKeyValueInfo(kvInfo, 0)
	s.locks[in.Key] = NewKeyLock(s, d, in.Key, clientID)
	logger.Info(
		"created key",
		kvLogKey, kvInfo,
	)
	return &pb.LockResponse{
		Success: true,
	}, nil
}

func (s *KeyValueStore) Unlock(
	ctx context.Context,
	in *pb.UnlockRequest,
) (*pb.UnlockResponse, error) {
	span := trace.SpanFromContext(ctx)
	span.SetAttributes(attribute.String("key", in.Key))

	clientID := s.ClientID(ctx)
	logger := s.requestLogger(ctx)

	logger.Info(
		"unlock request",
		slog.String("key", in.Key),
	)
	if strings.HasPrefix(strings.ToLower(in.Key), reservedPrefix) {
		return nil, ErrReservedKeyPrefix
	}

	s.cfgMu.RLock()
	cfg := *s.cfg
	s.cfgMu.RUnlock()
	if cfg.Readonly {
		return nil, ErrReadOnlyServer
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.started {
		return nil, ErrServerPendingInit
	}

	_, ok := s.store[in.Key]
	if !ok {
		return nil, ErrKeyNotFound
	}

	s.lockMu.Lock()
	defer s.lockMu.Unlock()

	keyLock, exists := s.locks[in.Key]
	if !exists {
		return &pb.UnlockResponse{Success: true}, nil
	}

	if clientID != keyLock.ClientID {
		return nil, ErrWrongUnlockToken
	}
	delete(s.locks, in.Key)
	_ = keyLock.t.Stop()

	now := time.Now()
	s.emit(in.Key, Unlocked, clientID, &now)

	return &pb.UnlockResponse{Success: true}, nil
}

// Delete deletes a key from the store
func (s *KeyValueStore) Delete(ctx context.Context, in *pb.DeleteRequest) (
	*pb.DeleteResponse,
	error,
) {
	span := trace.SpanFromContext(ctx)
	span.SetAttributes(attribute.String("key", in.Key))

	clientID := s.ClientID(ctx)
	logger := s.requestLogger(ctx)
	logger.Info(
		"request to delete key",
		slog.String("key", in.Key),
	)
	s.cfgMu.RLock()
	cfg := *s.cfg
	s.cfgMu.RUnlock()
	if cfg.Readonly {
		return nil, ErrReadOnlyServer
	}
	if strings.HasPrefix(strings.ToLower(in.Key), reservedPrefix) {
		return nil, ErrReservedKeyPrefix
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.started {
		return nil, ErrServerPendingInit
	}

	kvInfo := s.getKey(in.Key)
	if kvInfo == nil {
		return &pb.DeleteResponse{Deleted: false}, ErrKeyNotFound
	}

	s.lockMu.Lock()
	defer s.lockMu.Unlock()

	keyLock := s.locks[in.Key]
	if keyLock != nil && clientID != keyLock.ClientID {
		logger.Info(
			"cannot delete locked key", "key", in.Key,
		)
		return &pb.DeleteResponse{Deleted: false}, ErrWrongUnlockToken
	}

	s.reaperMu.Lock()
	defer s.reaperMu.Unlock()

	kvInfo.mu.Lock()
	defer kvInfo.mu.Unlock()

	s.deleteKey(in.Key, clientID)
	return &pb.DeleteResponse{Deleted: true}, nil
}

// Inspect returns information about a key
func (s *KeyValueStore) Inspect(
	ctx context.Context,
	in *pb.InspectRequest,
) (*pb.InspectResponse, error) {
	span := trace.SpanFromContext(ctx)
	span.SetAttributes(attribute.String("key", in.Key))

	logger := s.requestLogger(ctx)

	logger.Info(
		"getting key info",
		slog.String("key", in.Key),
	)

	s.mu.RLock()

	kvInfo := s.getKey(in.Key)
	if kvInfo == nil {
		s.mu.RUnlock()
		return nil, ErrKeyNotFound
	}

	s.lockMu.RLock()
	keyLock, locked := s.locks[in.Key]
	if locked && in.IncludeValue {
		clientID := s.ClientID(ctx)
		if clientID != keyLock.ClientID {
			logger.Info(
				"cannot get key locked by another client",
			)
			s.lockMu.RUnlock()
			s.mu.RUnlock()
			return nil, ErrLocked
		}
	}
	s.reaperMu.RLock()
	s.lockMu.RUnlock()
	lifespan, hasExpiry := s.reapers[in.Key]

	resp := &pb.InspectResponse{}
	if hasExpiry {
		resp.Lifespan = durationpb.New(lifespan.Lifespan)
		resp.LifespanSet = timestamppb.New(lifespan.LifespanSet)
	}
	s.reaperMu.RUnlock()
	kvInfo.mu.RLock()

	resp.Key = kvInfo.Key
	resp.Hash = kvInfo.Hash
	resp.Version = kvInfo.Version
	resp.Size = kvInfo.Size
	resp.ContentType = kvInfo.ContentType
	resp.Locked = &locked

	if !kvInfo.Created.IsZero() {
		resp.Created = timestamppb.New(kvInfo.Created)
	}
	if !kvInfo.Updated.IsZero() {
		resp.Updated = timestamppb.New(kvInfo.Updated)
	}
	if in.IncludeValue {
		resp.Value = kvInfo.Value
		t := time.Now()
		defer s.emit(in.Key, Accessed, s.ClientID(ctx), &t)
	}
	kvInfo.mu.RUnlock()
	s.mu.RUnlock()

	if in.IncludeMetrics {
		s.keyStatMu.RLock()
		keyStat := s.keyStats[in.Key]
		keyStat.mu.RLock()
		s.keyStatMu.RUnlock()
		m := &pb.KeyMetric{
			AccessCount: keyStat.AccessCount,
			LockCount:   keyStat.LockCount,
			SetCount:    keyStat.SetCount,
		}
		if keyStat.FirstAccessed != nil {
			m.FirstAccessed = timestamppb.New(*keyStat.FirstAccessed)
		}
		if keyStat.LastAccessed != nil {
			m.LastAccessed = timestamppb.New(*keyStat.LastAccessed)

		}

		if keyStat.FirstLocked != nil {
			m.FirstLocked = timestamppb.New(*keyStat.FirstLocked)

		}
		if keyStat.LastLocked != nil {
			m.LastLocked = timestamppb.New(*keyStat.LastLocked)
		}
		if keyStat.FirstSet != nil {
			m.FirstSet = timestamppb.New(*keyStat.FirstSet)

		}
		if keyStat.LastSet != nil {
			m.LastSet = timestamppb.New(*keyStat.LastSet)
		}
		keyStat.mu.RUnlock()
		resp.Metrics = m
	}
	return resp, nil
}

// Pop returns a Key-value pair and deletes the key
func (s *KeyValueStore) Pop(ctx context.Context, in *pb.PopRequest) (
	*pb.GetResponse,
	error,
) {
	span := trace.SpanFromContext(ctx)
	span.SetAttributes(attribute.String("key", in.Key))

	clientID := s.ClientID(ctx)
	logger := s.requestLogger(ctx)
	logger.Info(
		"request to pop key",
		slog.String("key", in.Key),
	)
	s.cfgMu.RLock()
	cfg := *s.cfg
	s.cfgMu.RUnlock()
	if cfg.Readonly {
		return nil, ErrReadOnlyServer
	}

	if strings.HasPrefix(strings.ToLower(in.Key), reservedPrefix) {
		return nil, ErrReservedKeyPrefix
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	kvInfo := s.getKey(in.Key)
	if kvInfo == nil {
		return nil, ErrKeyNotFound
	}

	s.lockMu.RLock()
	keyLock, lockExists := s.locks[in.Key]
	defer s.lockMu.RUnlock()
	if lockExists && clientID != keyLock.ClientID {
		logger.Info(
			"cannot pop locked key", kvLogKey, kvInfo,
		)
		return nil, ErrLocked
	}

	s.reaperMu.Lock()
	defer s.reaperMu.Unlock()

	kvInfo.mu.Lock()
	defer kvInfo.mu.Unlock()

	getResponse := &pb.GetResponse{Value: kvInfo.Value}

	s.deleteKey(in.Key, clientID)
	return getResponse, nil
}

// Exists returns true if a key exists
func (s *KeyValueStore) Exists(ctx context.Context, in *pb.Key) (
	*pb.ExistsResponse,
	error,
) {
	span := trace.SpanFromContext(ctx)
	span.SetAttributes(attribute.String("key", in.Key))

	logger := s.requestLogger(ctx)

	logger.Debug(
		"checking if key exists",
		slog.String("key", in.Key),
	)

	s.mu.RLock()
	kvInfo := s.getKey(in.Key)
	s.mu.RUnlock()

	if kvInfo == nil {
		return &pb.ExistsResponse{Exists: false}, nil
	}

	return &pb.ExistsResponse{Exists: true}, nil
}

// ListKeys returns a list of keys in the store matching the provided
// pattern, limited to the provided count. If limit=0, all matching keys are
// returned. If a pattern nor a limit are provided, all keys currently in the
// store will be returned. This may be expensive, depending on the number of keys.
func (s *KeyValueStore) ListKeys(
	ctx context.Context,
	req *pb.ListKeysRequest,
) (*pb.ListKeysResponse, error) {
	logger := s.requestLogger(ctx)
	logger.Info(
		"listing keys",
		slog.String("pattern", req.Pattern),
		slog.Uint64("limit", req.Limit),
		slog.Bool("include_reserved", req.IncludeReserved),
	)

	limit := int(req.Limit)

	s.mu.RLock()
	if limit == 0 {
		limit = len(s.store)
	}

	allKeys := make([]string, 0, len(s.store))
	for k := range s.store {
		if !req.IncludeReserved && strings.HasPrefix(
			strings.ToLower(k),
			reservedPrefix,
		) {
			logger.Debug("excluding reserved key", "key", k)
			continue
		}
		allKeys = append(allKeys, k)
	}

	if req.Pattern == "" {
		s.mu.RUnlock()
		rt := make([]string, 0, len(allKeys))
		for _, k := range allKeys[:limit] {
			if k == "" {
				break
			}
			rt = append(rt, k)
		}
		return &pb.ListKeysResponse{Keys: rt}, nil
	}

	s.mu.RUnlock()

	keys := make([]string, 0, limit)
	pattern, err := regexp.Compile(req.Pattern)
	if err != nil {
		return nil, ErrInvalidKeyPattern
	}

	for _, key := range allKeys {
		if pattern.MatchString(key) {
			keys = append(keys, key)
		}
		if len(keys) >= limit {
			break
		}
	}
	return &pb.ListKeysResponse{Keys: keys}, nil
}

// Stats returns server statistics
func (s *KeyValueStore) Stats(
	ctx context.Context,
	_ *pb.EmptyRequest,
) (*pb.ServerMetrics, error) {
	return s.GetStats(), nil
}

func (s *KeyValueStore) ClearHistory(
	ctx context.Context,
	_ *pb.EmptyRequest,
) (*pb.ClearHistoryResponse, error) {
	logger := s.requestLogger(ctx)

	s.cfgMu.RLock()
	readonly := s.cfg.Readonly
	s.cfgMu.RUnlock()
	if readonly {
		return nil, ErrReadOnlyServer
	}

	data := &pb.ClearHistoryResponse{}

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, kvInfo := range s.store {
		if e := ctx.Err(); e != nil {
			return data, e
		}
		data.Keys++

		kvInfo.mu.Lock()
		if kvInfo.History != nil {
			data.Cleared = kvInfo.History.Clear()
		}
		kvInfo.mu.Unlock()
	}
	logger.Info(
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
	req *pb.ClearRequest,
) (*pb.ClearResponse, error) {
	s.cfgMu.RLock()
	cfg := *s.cfg
	s.cfgMu.RUnlock()

	if cfg.Readonly {
		return nil, ErrReadOnlyServer
	}
	logger := s.requestLogger(ctx)
	logger.Info(
		"clearing all keys",
	)
	clientID := s.ClientID(ctx)

	s.mu.Lock()
	defer s.mu.Unlock()

	s.lockMu.Lock()
	defer s.lockMu.Unlock()

	s.reaperMu.Lock()
	defer s.reaperMu.Unlock()

	keyCount := uint64(len(s.store))

	var newTotalSize uint64

	clearResponse := &pb.ClearResponse{}

	for key, kvInfo := range s.store {
		if e := ctx.Err(); e != nil {
			clearResponse.KeysDeleted = keyCount - uint64(len(s.store))
			return clearResponse, e
		}
		// The only time we keep a key when clearing the store
		// is when it has a reserved prefix, or when the
		// key is locked, and we aren't forcing it to clear
		if strings.HasPrefix(strings.ToLower(key), reservedPrefix) {
			continue
		}
		_, isLocked := s.locks[key]
		kvInfo.mu.Lock()
		if req.Force || !isLocked {
			if isLocked && s.locks != nil {
				delete(s.locks, key)
			}
			s.deleteKey(key, clientID)
			clearResponse.KeysDeleted++
		}
		kvInfo.mu.Unlock()
	}
	s.totalSize.Store(newTotalSize)
	s.numKeys.Store(uint64(len(s.store)))

	clearResponse.Success = true
	clearResponse.KeysDeleted = keyCount - uint64(len(s.store))
	logger.Info(
		fmt.Sprintf("cleared %d/%d keys", clearResponse.KeysDeleted, keyCount),
	)
	return clearResponse, nil
}

func (s *KeyValueStore) GetRevision(
	ctx context.Context,
	in *pb.GetRevisionRequest,
) (
	*pb.RevisionResponse,
	error,
) {
	span := trace.SpanFromContext(ctx)
	span.SetAttributes(attribute.String("key", in.Key))

	logger := s.requestLogger(ctx)
	logger.Info(
		"getting revision",
		slog.String("key", in.Key),
		slog.Int64("version", in.Version),
	)

	s.cfgMu.RLock()
	cfg := *s.cfg
	s.cfgMu.RUnlock()

	if cfg.RevisionLimit == 0 {
		return nil, ErrVersioningDisabled
	}

	if in.Version == 0 {
		return nil, GOKVError{
			Message: "revision must be greater or less than zero",
			Code:    codes.InvalidArgument,
		}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	kv, ok := s.store[in.Key]
	if !ok {
		return nil, ErrKeyNotFound
	}

	kv.mu.RLock()
	defer kv.mu.RUnlock()

	if kv.Version == uint64(in.Version) {
		return &pb.RevisionResponse{
			Value:     kv.Value,
			Timestamp: timestamppb.New(kv.Updated),
		}, nil
	}
	logger.Info(
		"getting revision",
		slog.String("key", in.Key),
		slog.Group(
			"version",
			slog.Uint64("current", kv.Version),
			slog.Int64("requested", in.Version),
		),
	)
	h, found := kv.History.RelativeVersion(in.Version)
	if !found {
		return nil, ErrRevisionNotFound
	}
	return &pb.RevisionResponse{
		Value:     h.Value,
		Timestamp: timestamppb.New(h.Timestamp),
	}, nil
}

func (s *KeyValueStore) Register(
	ctx context.Context,
	_ *pb.RegisterRequest,
) (*pb.RegisterResponse, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Errorf(
			codes.InvalidArgument,
			"missing metadata (at least client_id is required)",
		)
	}
	var clientIDs []string
	clientIDs, ok = md[clientIDKey]
	if !ok || len(clientIDs) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "missing client_id")
	}
	clientID := clientIDs[0]
	if clientID == "" {
		return nil, status.Errorf(codes.InvalidArgument, "empty client_id")
	}

	p, ok := peer.FromContext(ctx)
	s.cmu.Lock()
	defer s.cmu.Unlock()

	if s.clientInfo == nil {
		s.clientInfo = make(map[string]*ClientInfo)
	}

	ci, ok := s.clientInfo[clientID]
	if ok {
		return nil, GOKVError{
			Message: "already registered",
			Code:    codes.AlreadyExists,
		}
	}

	var addr string
	if p != nil && p.Addr != nil {
		addr = p.Addr.String()
	}
	ci = &ClientInfo{
		ClientID:  clientID,
		FirstSeen: time.Now(),
	}
	s.clientIDs.Add(1)
	ci.mu.RLock()
	defer ci.mu.RUnlock()
	s.clientInfo[clientID] = ci
	s.logger.Info("new client seen", "client", ci, "address", addr)
	return &pb.RegisterResponse{Success: true}, nil
}

func (s *KeyValueStore) SetReadOnly(
	ctx context.Context,
	in *pb.ReadOnlyRequest,
) (*pb.ReadOnlyResponse, error) {
	logger := s.requestLogger(ctx)
	clientID := s.ClientID(ctx)
	if in.Enable {
		logger.Info("attempting to set read-only mode")
	} else {
		logger.Info("attempt to disable read-only mode")
	}
	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()

	if s.cfg.PrivilegedClientID == "" {
		return nil, GOKVError{
			Message: "privileged client ID not set",
			Code:    codes.FailedPrecondition,
		}
	}
	if clientID != s.cfg.PrivilegedClientID {
		return nil, GOKVError{
			Message: "not authorized",
			Code:    codes.PermissionDenied,
		}
	}

	s.cfg.Readonly = in.Enable
	if s.cfg.Readonly {
		logger.Warn("read-only mode enabled")
	} else {
		logger.Warn("read-only mode disabled")
	}
	return &pb.ReadOnlyResponse{Success: true}, nil
}

// Subscribe returns a channel of events.
func (s *KeyValueStore) Subscribe(
	ctx context.Context,
	name string,
	keys []string,
	events []KeyEvent,
) (chan Event, error) {
	return s.eventStream.Subscribe(ctx, name, keys, events)
}

func (s *KeyValueStore) GetKeyMetric(
	ctx context.Context,
	in *pb.KeyMetricRequest,
) (*pb.KeyMetric, error) {
	span := trace.SpanFromContext(ctx)
	span.SetAttributes(attribute.String("key", in.Key))

	logger := s.requestLogger(ctx)
	logger.Info("request for key metric", "key", in.Key)
	s.keyStatMu.RLock()
	keyStat := s.keyStats[in.Key]
	if keyStat == nil {
		s.keyStatMu.RUnlock()
		return nil, GOKVError{
			Message: "key not found",
			Code:    codes.NotFound,
		}
	}

	keyStat.mu.RLock()
	s.keyStatMu.RUnlock()
	defer keyStat.mu.RUnlock()

	m := &pb.KeyMetric{
		AccessCount: keyStat.AccessCount,
		LockCount:   keyStat.LockCount,
		SetCount:    keyStat.SetCount,
	}

	if keyStat.FirstAccessed != nil {
		m.FirstAccessed = timestamppb.New(*keyStat.FirstAccessed)
	}
	if keyStat.LastAccessed != nil {
		m.LastAccessed = timestamppb.New(*keyStat.LastAccessed)
	}
	if keyStat.FirstLocked != nil {
		m.FirstLocked = timestamppb.New(*keyStat.FirstLocked)
	}
	if keyStat.LastLocked != nil {
		m.LastLocked = timestamppb.New(*keyStat.LastLocked)
	}
	if keyStat.FirstSet != nil {
		m.FirstSet = timestamppb.New(*keyStat.FirstSet)
	}
	if keyStat.LastSet != nil {
		m.LastSet = timestamppb.New(*keyStat.LastSet)
	}
	logger.Debug("got metric", "key", in.Key, "metric", m)

	return m, nil
}

func (s *KeyValueStore) getStats() *pb.ServerMetrics {
	s.logger.Info("compiling stats")

	currentLocks := uint64(len(s.locks))
	currentReapers := uint64(len(s.reapers))

	var numKeys = s.numKeys.Load()
	var totalSize = s.totalSize.Load()
	var numNewKeysSet = s.numEventCreated.Load()
	var numKeysUpdated = s.numEventUpdated.Load()
	var deleted = s.numEventDeleted.Load()
	var numKeysLocked = s.numEventLocked.Load()
	var numKeysUnlocked = s.numEventUnlocked.Load()
	var numKeysExpired = s.numEventExpired.Load()
	var numKeysExpunged = s.numEventExpunged.Load()
	var clientsSeen = s.clientIDs.Load()
	var accesses = s.numEventValueAccessed.Load()
	var numLifespanSet = s.numEventLifespanSet.Load()
	var subscribers = s.numEventSubscribers.Load()
	var snapshots = s.numSnapshotsCreated.Load()

	p := s.pressure()
	var used float32
	var threshold float32

	used = float32(p.Used)
	threshold = float32(p.PruneThreshold)

	eventMetrics := &pb.EventMetrics{
		New:         &numNewKeysSet,
		Updated:     &numKeysUpdated,
		Deleted:     &deleted,
		Locked:      &numKeysLocked,
		Unlocked:    &numKeysUnlocked,
		Expired:     &numKeysExpired,
		Expunged:    &numKeysExpunged,
		Accessed:    &accesses,
		LifespanSet: &numLifespanSet,
	}

	m := &pb.ServerMetrics{
		Keys:             &numKeys,
		TotalSize:        &totalSize,
		Events:           eventMetrics,
		CurrentLocks:     &currentLocks,
		ClientIds:        &clientsSeen,
		Reapers:          &currentReapers,
		SnapshotsCreated: &snapshots,
		EventSubscribers: &subscribers,
		Pressure: &pb.KeyPressure{
			Keys:              &p.Keys,
			Max:               &p.Max,
			Used:              &used,
			PruneThreshold:    &threshold,
			KeysOverThreshold: &p.KeysOverThreshold,
		},
	}
	s.logger.Info(
		"metrics compiled",
		slog.Group(
			"metric",
			slog.Uint64("keys", *m.Keys),
			slog.Uint64("total_size", *m.TotalSize),
			slog.Uint64("current_locks", *m.CurrentLocks),
			slog.Uint64("reapers", *m.Reapers),
			slog.Uint64("client_ids", *m.ClientIds),
			slog.Uint64("event_subscribers", *m.EventSubscribers),
			slog.Uint64("snapshots_created", *m.SnapshotsCreated),
			slog.Group(
				"pressure",
				slog.Uint64("keys", *m.Pressure.Keys),
				slog.Uint64("max", *m.Pressure.Max),
				slog.Float64("used", float64(*m.Pressure.Used)),
				slog.Float64(
					"prune_threshold",
					float64(*m.Pressure.PruneThreshold),
				),
				slog.Int64(
					"keys_over_threshold",
					*m.Pressure.KeysOverThreshold,
				),
			),
			slog.Group(
				"events",
				slog.Uint64(strings.ToLower(Created.String()), *m.Events.New),
				slog.Uint64(
					strings.ToLower(Updated.String()),
					*m.Events.Updated,
				),
				slog.Uint64(
					strings.ToLower(Deleted.String()),
					*m.Events.Deleted,
				),
				slog.Uint64(strings.ToLower(Locked.String()), *m.Events.Locked),
				slog.Uint64(
					strings.ToLower(Unlocked.String()),
					*m.Events.Unlocked,
				),
				slog.Uint64(
					strings.ToLower(Expired.String()),
					*m.Events.Expired,
				),
				slog.Uint64(
					strings.ToLower(Expunged.String()),
					*m.Events.Expunged,
				),
				slog.Uint64(
					strings.ToLower(Accessed.String()),
					*m.Events.Accessed,
				),
				slog.Uint64(
					strings.ToLower(LifespanSet.String()),
					*m.Events.LifespanSet,
				),
			),
		),
	)
	return m
}

func (s *KeyValueStore) GetStats() *pb.ServerMetrics {
	s.cfgMu.RLock()
	defer s.cfgMu.RUnlock()

	s.mu.RLock()
	defer s.mu.RUnlock()

	s.lockMu.RLock()
	defer s.lockMu.RUnlock()

	s.reaperMu.RLock()
	defer s.reaperMu.RUnlock()

	return s.getStats()
}

// Stop cleans up timers/goroutines/channels, after the
// context given to Start has been finished, and its goroutines have
// finished.
func (s *KeyValueStore) Stop() error {
	s.logger.Info("cleaning up")

	_, span := s.Tracer.Start(
		context.Background(),
		"keyquarry.stop",
		trace.WithNewRoot(),
	)
	defer span.End()

	s.cmu.Lock()
	defer s.cmu.Unlock()

	s.cfgMu.RLock()
	cfg := *s.cfg
	defer s.cfgMu.RUnlock()

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.stopped {
		return errors.New("server previously stopped")
	}

	s.lockMu.Lock()
	defer s.lockMu.Unlock()

	s.reaperMu.Lock()
	defer s.reaperMu.Unlock()

	wg := sync.WaitGroup{}
	s.stopped = true

	// Stop expiration and unlock timers, gather final server
	// metrics
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.stopExpirationTimers()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		s.stopUnlockTimers()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		s.eventStream.mu.Lock()
		defer s.eventStream.mu.Unlock()
		s.eventStream.Stop()
	}()

	var err error
	var snapshot int64

	if cfg.Snapshot.Enabled {
		wg.Add(1)
		s.logger.Info(
			"saving final snapshot",
			"config", s.cfg.Snapshot,
		)

		go func() {
			defer wg.Done()
			s.logger.Info("starting snapshot")
			snapshot, err = s.snapshotter.DBSnapshot(context.Background())
			s.logger.Info(
				"final snapshot created",
				slog.Int64("snapshot_id", snapshot),
			)
		}()
	}

	wg.Wait()
	s.logger.Info("getting final stats")
	stats := s.getStats()
	s.logger.Info("final metrics", "metrics", stats)
	if err != nil {
		return fmt.Errorf("error creating final snapshot: %w", err)
	}
	close(s.events)
	return err
}

func (s *KeyValueStore) Start(ctx context.Context) error {

	s.cfgMu.Lock()
	s.mu.Lock()
	_, span := s.Tracer.Start(
		ctx, "keyquarry.start", trace.WithAttributes(
			attribute.String("keyquarry.config.log_level", s.cfg.LogLevel),
			attribute.Bool("keyquarry.config.readonly", s.cfg.Readonly),
		),
	)

	if s.started {
		s.cfgMu.Unlock()
		s.mu.Unlock()
		span.End()
		return errors.New("server previously started")
	}

	s.logger.Info("starting", "config", s.cfg)

	wg := &sync.WaitGroup{}

	snapshotCh := make(chan Event, DefaultEventStreamBufferSize)
	logCh := make(chan Event, DefaultEventStreamBufferSize)
	eventStreamCh := make(chan Event, DefaultEventStreamBufferSize)
	s.eventStream.events = eventStreamCh

	var logEvents bool
	if s.cfg.LogEvents || s.cfg.LogLevel == "EVENT" {
		logEvents = true
	}

	if !logEvents {
		logCh = nil
	}

	go s.eventLoop(ctx, snapshotCh, logCh, eventStreamCh)

	wg.Add(1)
	go func() {
		defer wg.Done()
		s.logger.Info("running event stream")
		s.eventStream.Run(ctx)
	}()
	if logEvents {
		wg.Add(1)
		eventLogger := s.cfg.EventLogger

		go func() {
			defer wg.Done()
			s.logger.Info("starting event watcher")
			for ev := range logCh {
				var msg string
				switch ev.Event {
				case Created:
					msg = "created new key"
				case Updated:
					msg = "updated key"
				case Deleted:
					msg = "deleted key"
				case Expired:
					msg = "expired key"
				case Locked:
					msg = "locked key"
				case Unlocked:
					msg = "unlocked key"
				case Expunged:
					msg = "expunged key"
				case Accessed:
					msg = "accessed key"
				case LifespanSet:
					msg = "lifespan set"
				case NoEvent:
					if ev.Key != "" {
						panic(ev)
					}
				}
				eventLogger.LogAttrs(
					ctx,
					LevelEvent,
					msg,
					slog.Any("event", ev),
				)
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()

		if s.cfg.Snapshot.Enabled {
			dialect := GetDialect(s.cfg.Snapshot.Database)
			if dialect == nil {
				panic("invalid or unsupported dialect in database connection string")
			}
			initErr := dialect.InitDB(ctx, s.cfg.Snapshot.Database)
			if initErr != nil {
				panic(
					fmt.Sprintf(
						"error initializing database: %s",
						initErr.Error(),
					),
				)
			}
		} else {
			s.logger.Warn("snapshotter disabled, data will be lost on shutdown/restart")
		}
		s.snapshotter.Run(ctx, snapshotCh)
		s.logger.Debug("snapshotter finished")

	}()

	// If a ceiling is configured for the number of keys, start a goroutine
	// which, at the configured interval, will delete the oldest keys
	// (determined by lock date if locked, then updated, then created) until
	// the number of keys is below the ceiling and configured threshold
	if s.cfg.PruneInterval > 0 {
		if s.cfg.PruneThreshold == 0 {
			s.cfg.PruneThreshold = DefaultPruneThreshold
		}
		if s.cfg.PruneTarget == 0 {
			s.cfg.PruneTarget = DefaultPruneThreshold
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = s.pruner.Run(ctx)
			s.logger.Info("pruner finished")
		}()
	} else {
		s.logger.Warn("key pruner disabled, set prune_interval to enable")
	}

	s.started = true

	s.addStartupKeys()
	s.cfgMu.Unlock()
	s.mu.Unlock()
	s.logger.Info("setup complete")
	wg.Wait()

	defer func() {
		// event stream is stopped here in addition to Stop, because a
		// client watching events can block the grpc server from
		// stopping gracefully, causing us to get stuck here
		s.eventStream.mu.Lock()
		defer s.eventStream.mu.Unlock()
		s.eventStream.Stop()
	}()

	return nil
}

func (s *KeyValueStore) WatchKeyValue(
	in *pb.WatchKeyValueRequest,
	stream pb.KeyValueStore_WatchKeyValueServer,
) error {
	ctx := stream.Context()
	span := trace.SpanFromContext(ctx)
	span.SetAttributes(attribute.String("key", in.Key))
	clientID, err := s.ClientIDFromContext(ctx)
	if err != nil {
		return err
	}
	if clientID == "" {
		return GOKVError{
			Message: "client ID not found",
			Code:    codes.Unauthenticated,
		}
	}
	logger := s.requestLogger(ctx)
	logger.Info("got new event watcher")

	var currentKV *KeyValueInfo
	var exists bool
	var streamErr error
	var kv *pb.WatchKeyValueResponse

	s.mu.RLock()

	currentKV, exists = s.store[in.Key]

	if exists {
		currentKV.mu.RLock()
		s.mu.RUnlock()
		kv = &pb.WatchKeyValueResponse{
			Key:           currentKV.Key,
			Value:         currentKV.Value,
			Hash:          currentKV.Hash,
			ContentType:   currentKV.ContentType,
			Version:       currentKV.Version,
			EventClientId: reservedClientID,
		}
		currentKV.mu.RUnlock()

		streamErr = stream.Send(kv)
		if streamErr != nil {
			return streamErr
		}
	} else {
		s.mu.RUnlock()
	}
	streamClientID := fmt.Sprintf("%s/WatchKeyValue", clientID)
	sctx, cancel := context.WithCancel(ctx)
	defer func() {
		logger.Debug("unsubscribing client")
		logger.Debug("cancelling context for value watch")
		cancel()
		unsubErr := s.Unsubscribe(streamClientID)
		if unsubErr != nil {
			s.logger.Warn("unsubscribe failed", "error", unsubErr)
		}
	}()
	events, err := s.Subscribe(
		sctx,
		streamClientID,
		[]string{in.Key},
		[]KeyEvent{
			Deleted,
			Expired,
			Expunged,
			Created,
			Updated,
		},
	)

	if err != nil {
		return GOKVError{Message: err.Error(), Code: codes.Internal}
	}

	for ev := range events {
		if sctx.Err() != nil {
			break
		}
		switch ev.Event {
		case Deleted, Expired, Expunged:
			// logger.Info("ending client stream due to key event", "event", ev)
			kv = &pb.WatchKeyValueResponse{
				Key:            ev.Key,
				KeyEvent:       pb.KeyEvent(ev.Event),
				EventClientId:  ev.ClientID,
				EventTimestamp: timestamppb.New(ev.Time),
			}
			streamErr = stream.Send(kv)
			if streamErr != nil {
				logger.Error(
					"error sending key value to subscribed client",
					"error",
					streamErr,
				)
				return streamErr
			}
		case Created, Updated:
			s.mu.RLock()
			currentKV, exists = s.store[in.Key]
			if !exists {
				s.mu.RUnlock()
				return GOKVError{
					Message: "key no longer found",
					Code:    codes.NotFound,
				}
			}
			currentKV.mu.RLock()
			s.mu.RUnlock()
			kv = &pb.WatchKeyValueResponse{
				Key:            ev.Key,
				Value:          currentKV.Value,
				Hash:           currentKV.Hash,
				ContentType:    currentKV.ContentType,
				Version:        currentKV.Version,
				KeyEvent:       pb.KeyEvent(ev.Event),
				EventClientId:  ev.ClientID,
				EventTimestamp: timestamppb.New(ev.Time),
			}
			currentKV.mu.RUnlock()
			logger.Info(
				"sending key value to subscribed client",
				slog.Group(
					"key_value",
					slog.String("key", in.Key),
					slog.Uint64("hash", kv.Hash),
					slog.String("content_type", kv.ContentType),
					slog.Uint64("version", kv.Version),
				),
				slog.String("client_id", clientID),
			)
			streamErr = stream.Send(kv)
			if streamErr != nil {
				logger.Error(
					"error sending key value to subscribed client",
					"error",
					streamErr,
				)
				return streamErr
			}
		}
	}
	logger.Debug("stream finished", "client_id", clientID)
	return nil
}

func (s *KeyValueStore) WatchStream(
	in *pb.WatchRequest,
	stream pb.KeyValueStore_WatchStreamServer,
) error {
	ctx := stream.Context()
	clientID, err := s.ClientIDFromContext(ctx)
	if err != nil {
		return err
	}
	if clientID == "" {
		return GOKVError{
			Message: "client ID not found",
			Code:    codes.Unauthenticated,
		}
	}
	logger := s.requestLogger(ctx)
	logger.Info("got new event watcher")
	sctx, cancel := context.WithCancel(ctx)
	streamClientID := fmt.Sprintf("%s/WatchStream", clientID)
	events, err := s.Subscribe(sctx, streamClientID, in.Keys, nil)
	if err != nil {
		cancel()
		return err
	}
	defer func() {
		logger.Debug("unsubscribing client")
		cancel()
		_ = s.Unsubscribe(streamClientID)
	}()
	var targetEvents = make([]KeyEvent, 0, len(in.Events))
	for _, ev := range in.Events {
		targetEvents = append(targetEvents, KeyEvent(ev))
	}

	for ev := range events {
		switch {
		case len(targetEvents) > 0 && !sliceContains(targetEvents, ev.Event):
			continue
		case len(in.Keys) > 0 && !sliceContains(in.Keys, ev.Key):
			continue
		case len(in.ClientIds) > 0 && !sliceContains(in.ClientIds, ev.ClientID):
			continue
		}

		logger.Debug("saw client stream event", "event", ev)
		if e := stream.Send(
			&pb.Event{
				Key:      ev.Key,
				Event:    pb.KeyEvent(ev.Event),
				Time:     timestamppb.New(ev.Time),
				ClientId: ev.ClientID,
			},
		); e != nil {
			logger.Warn(
				"error sending event",
				"error",
				e,
				"event",
				ev,
				"client_id",
				clientID,
			)
			return err
		}
		logger.Debug("sent client event", "event", ev)
	}
	logger.Debug("stream finished", "client_id", clientID)
	return nil
}

// Unsubscribe removes the given subscriber name from the event stream, stops
// the sending worker and closes the outbound channel
func (s *KeyValueStore) Unsubscribe(name string) error {
	s.logger.Info("request to unsubscribe", "name", name)
	r := s.eventStream.Unsubscribe(name)
	return r
}

func (s *KeyValueStore) Config() Config {
	s.cfgMu.RLock()
	cfg := *s.cfg
	s.cfgMu.RUnlock()
	return cfg
}

// ClientIDFromContext extracts the client ID from the provided context
// and returns it. It also returns an error if it's missing.
func (s *KeyValueStore) ClientIDFromContext(ctx context.Context) (
	string,
	error,
) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", status.Errorf(
			codes.InvalidArgument,
			"missing metadata (at least client_id is required)",
		)
	}
	var clientIDs []string
	clientIDs, ok = md[clientIDKey]
	if !ok || len(clientIDs) == 0 {
		s.logger.Warn("missing client_id")
		return "", &GOKVError{
			Message: "reserved client_id used",
			Code:    codes.InvalidArgument,
		}
	}
	clientID := clientIDs[0]
	if clientID == "" {
		s.logger.Warn("missing client_id")
		return "", status.Errorf(codes.InvalidArgument, "empty client_id")
	} else if clientID == reservedClientID {
		return "", status.Errorf(
			codes.InvalidArgument,
			"reserved client_id used",
		)
	}
	span := trace.SpanFromContext(ctx)
	span.SetAttributes(attribute.String("client_id", clientID))
	return clientID, nil
}

// ClientID returns the client ID from the given context
func (s *KeyValueStore) ClientID(ctx context.Context) string {
	clientID, _ := s.ClientIDFromContext(ctx)
	return clientID
}

// ClientInfo returns the ClientInfo for the given context
func (s *KeyValueStore) ClientInfo(ctx context.Context) *ClientInfo {
	clientID, err := s.ClientIDFromContext(ctx)
	if err != nil {
		return nil
	}
	s.cmu.RLock()
	defer s.cmu.RUnlock()
	return s.clientInfo[clientID]
}

func (s *KeyValueStore) MarshalJSON() (data []byte, err error) {
	keys := make([]*KeyValueInfo, 0, len(s.store))
	for _, kvInfo := range s.store {
		if strings.HasPrefix(strings.ToLower(kvInfo.Key), reservedPrefix) {
			continue
		}
		kvInfo.mu.RLock()
		keys = append(keys, kvInfo)
	}
	defer func() {
		for _, kvInfo := range keys {
			kvInfo.mu.RUnlock()
		}
	}()

	clients := make([]*ClientInfo, 0, len(s.clientInfo))

	for _, c := range s.clientInfo {
		clients = append(clients, c)
	}

	locks := make([]*KeyLock, 0, len(s.locks))
	for _, keyLock := range s.locks {
		locks = append(locks, keyLock)
	}

	reapers := make([]*KeyReaper, 0, len(s.reapers))
	for _, keyReaper := range s.reapers {
		reapers = append(reapers, keyReaper)
	}

	stats := map[string]*KeyMetrics{}
	for key, keyStat := range s.keyStats {
		stats[key] = keyStat
		keyStat.mu.RLock()
	}
	defer func() {
		for _, keyStat := range stats {
			keyStat.mu.RUnlock()
		}
	}()

	state := KeyValueStoreState{
		Keys:    keys,
		Clients: clients,
		Locks:   locks,
		Reapers: reapers,
		Version: build.Version,
	}

	return json.Marshal(state)
}

// UnmarshalJSON reads the provided data and populates the server
// with the key-value data, client IDs and locks. This will
// overwrite any existing data.
func (s *KeyValueStore) UnmarshalJSON(data []byte) error {
	// s.resetStats()

	state := KeyValueStoreState{}
	err := json.Unmarshal(data, &state)
	if err != nil {
		return err
	}
	if state.Version != build.Version {
		s.logger.Warn(
			"snapshot version does not match build version",
			"build_version", build.Version,
			"snapshot_version", state.Version,
		)
	}
	if s.store == nil {
		s.store = make(map[string]*KeyValueInfo)
	}

	if s.keyStats == nil {
		s.keyStats = make(map[string]*KeyMetrics)
	}

	// channel to send KeyValueInfo for value hashing
	hashChannel := make(chan *KeyValueInfo)
	// receive KeyValueInfo when hashing is complete
	doneChannel := make(chan *KeyValueInfo)
	wg := sync.WaitGroup{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	workers := runtime.GOMAXPROCS(0)

	now := time.Now()

	ignoreKeys := []string{}

	if state.Metrics != nil {
		for metricKey, keyStat := range state.Metrics {
			s.keyStats[metricKey] = keyStat
		}
	}

	if state.Reapers != nil {
		for _, keyReaper := range state.Reapers {
			reapTime := keyReaper.LifespanSet.Add(keyReaper.Lifespan)
			if reapTime.Before(now) {
				s.logger.Warn(
					"anticipated key expiration time has passed",
					"key_reaper",
					keyReaper,
					"reap_time", reapTime,
					"now", now,
				)
				ignoreKeys = append(ignoreKeys, keyReaper.Key)
				continue
			}
			s.reapers[keyReaper.Key] = keyReaper
			keyReaper.t = time.AfterFunc(
				keyReaper.Lifespan,
				keyReaper.ExpireFunc(),
			)
		}
	}

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for kvInfo := range hashChannel {
				if ctx.Err() != nil {
					return
				}

				kvInfo.mu.Lock()
				kvInfo.Size = uint64(len(kvInfo.Value))

				s.numKeys.Add(1)
				s.totalSize.Add(kvInfo.Size)

				if kvInfo.Value != nil {
					hashFunc := fnv.New64a()
					_, he := hashFunc.Write(kvInfo.Value)
					if he != nil {
						s.logger.Error(
							"error hashing value",
							"error",
							he,
							"key",
							kvInfo.Key,
						)
					}
					kvInfo.Hash = hashFunc.Sum64()
					kvInfo.ContentType = http.DetectContentType(kvInfo.Value)
				}

				if kvInfo.Created.IsZero() {
					kvInfo.Created = time.Now()
				}
				kvInfo.mu.Unlock()
				doneChannel <- kvInfo
			}
		}()
	}

	go func() {
		for i := 0; i < len(state.Keys); i++ {
			if ctx.Err() != nil {
				return
			}
			ignore := sliceContains(ignoreKeys, state.Keys[i].Key)
			if !ignore {
				hashChannel <- state.Keys[i]
			}
		}
		close(hashChannel)
	}()
	go func() {
		wg.Wait()
		close(doneChannel)
	}()

	s.keyStatMu.Lock()

	for kv := range doneChannel {
		if ctx.Err() != nil {
			break
		}
		s.logger.Debug("loading key", slog.String("key", kv.Key))
		_, exists := s.store[kv.Key]
		if exists {
			s.logger.Error(
				"duplicate key found",
				slog.String("key", kv.Key),
			)
			cancel()
		} else {
			s.store[kv.Key] = kv
			_, statExists := s.keyStats[kv.Key]
			if !statExists {
				createdAt := kv.Created
				updated := kv.Updated
				if updated.IsZero() {
					updated = createdAt
				}
				s.keyStats[kv.Key] = &KeyMetrics{
					FirstSet: &createdAt,
					LastSet:  &updated,
					SetCount: 1,
				}
			}
		}
	}
	s.keyStatMu.Unlock()

	if s.clientInfo == nil {
		s.clientInfo = make(map[string]*ClientInfo)
	}
	for _, clientInfo := range state.Clients {
		s.clientInfo[clientInfo.ClientID] = clientInfo
		s.logger.Info("loaded client", "client", clientInfo)
	}

	if state.Locks != nil {
		s.keyStatMu.RLock()
		for _, keyLock := range state.Locks {
			unlockTime := keyLock.Created.Add(keyLock.Duration)
			if unlockTime.Before(now) {
				s.logger.Warn(
					"key was locked, but anticipated unlock time has passed",
					"key_lock",
					keyLock,
					"unlock_time", unlockTime,
				)
				continue
			}
			_, stillExists := s.store[keyLock.Key]
			if stillExists {
				s.locks[keyLock.Key] = keyLock
				keyLock.t = time.AfterFunc(
					keyLock.Duration,
					keyLock.UnlockFunc(),
				)
				keyStat := s.keyStats[keyLock.Key]
				keyStat.mu.Lock()
				lockCreated := keyLock.Created
				keyStat.FirstLocked = &lockCreated
				keyStat.LastLocked = &lockCreated
				keyStat.LockCount = 1
				keyStat.mu.Unlock()
			}
		}
		s.keyStatMu.RUnlock()
	}
	return nil
}

func (s *KeyValueStore) pressure() keyPressure {
	k := keyPressure{
		Keys:           s.numKeys.Load(),
		Max:            s.cfg.MaxNumberOfKeys,
		PruneThreshold: s.cfg.PruneThreshold,
	}
	var used float64
	if k.Max > 0 {
		used = float64(k.Keys) / float64(k.Max)
	}
	rounded := strconv.FormatFloat(used, 'f', 2, 64)
	used, _ = strconv.ParseFloat(rounded, 64)
	k.Used = used
	if k.Max > 0 {
		thresholdCount := uint64(float64(k.Max) * k.PruneThreshold)
		k.KeysOverThreshold = int64(k.Keys) - int64(thresholdCount)
	}
	return k
}

// emit sends the provided event details to the event stream as an Event
func (s *KeyValueStore) emit(
	key string,
	event KeyEvent,
	clientID string,
	eventTime *time.Time,
) {
	if eventTime == nil {
		t := time.Now()
		eventTime = &t
	}
	ev := Event{
		Key:      key,
		Event:    event,
		Time:     *eventTime,
		ClientID: clientID,
	}
	s.logger.Debug("broadcasting event", "event", ev)
	if s.events == nil {
		panic("events channel not initialized")
	}
	if s.stopped {
		s.logger.Warn("server stopped, dropping event", "event", ev)
		return
	}

	s.events <- ev
	s.logger.Debug("event sent", "event", ev)
}

// broadcast sends the given event to the provided snapshot, event logger and
// event stream channels. If any of the channels are nil, they are skipped.
func (s *KeyValueStore) broadcast(
	ctx context.Context,
	event Event,
	snapshotCh chan<- Event,
	logCh chan<- Event,
	streamCh chan<- Event,
) {
	broadcastLogger := s.logger.With("logger", "broadcast")
	target := 0
	if snapshotCh != nil {
		target++
	}
	if logCh != nil {
		target++
	}
	if streamCh != nil {
		target++
	}

	for i := 0; i < target; i++ {
		if ctx.Err() != nil {
			break
		}
		select {
		case snapshotCh <- event:
			snapshotCh = nil
			broadcastLogger.Debug(
				"event sent to snapshotter",
				"event",
				event,
			)
		case logCh <- event:
			logCh = nil
			broadcastLogger.Debug(
				"event sent to logger",
				"event",
				event,
			)
		case streamCh <- event:
			streamCh = nil
			broadcastLogger.Debug(
				"event sent to stream",
				"event",
				event,
			)
		}
	}
}

func (s *KeyValueStore) addKeyValueInfo(kvInfo *KeyValueInfo, size uint64) {
	s.store[kvInfo.Key] = kvInfo
	s.numKeys.Add(1)
	s.totalSize.Add(size)
	t := kvInfo.Created
	defer s.emit(kvInfo.Key, Created, kvInfo.CreatedBy, &t)
	s.keyStatMu.Lock()
	_, exists := s.keyStats[kvInfo.Key]
	if !exists {
		s.keyStats[kvInfo.Key] = &KeyMetrics{}
	}
	s.keyStatMu.Unlock()
}

func (s *KeyValueStore) addStartupKeys() {
	revisionLimit := s.cfg.RevisionLimit
	startKv := NewKeyValue(
		s,
		startupKeyStarted,
		[]byte(time.Now().String()),
		"",
		reservedClientID,
		revisionLimit,
	)
	s.addKeyValueInfo(startKv, uint64(len(startKv.Value)))

	lockDurationKey := NewKeyValue(
		s,
		startupKeyMaxLockDuration,
		[]byte(fmt.Sprintf("%d", s.cfg.MaxLockDuration)),
		"",
		reservedClientID,
		revisionLimit,
	)
	s.addKeyValueInfo(lockDurationKey, uint64(len(lockDurationKey.Value)))

	minLockDurationKey := NewKeyValue(
		s,
		startupKeyMinLockDuration,
		[]byte(fmt.Sprintf("%d", s.cfg.MinLockDuration)),
		"",
		reservedClientID,
		revisionLimit,
	)
	s.addKeyValueInfo(minLockDurationKey, uint64(len(minLockDurationKey.Value)))

	keyMinLifespan := NewKeyValue(
		s,
		startupKeyMinLifespan,
		[]byte(fmt.Sprintf("%d", s.cfg.MinLifespan)),
		"",
		reservedClientID,
		revisionLimit,
	)
	s.addKeyValueInfo(keyMinLifespan, uint64(len(keyMinLifespan.Value)))

	keyMaxKeys := NewKeyValue(
		s,
		startupKeyMaxKeys,
		[]byte(fmt.Sprintf("%d", s.cfg.MaxNumberOfKeys)),
		"",
		reservedClientID,
		revisionLimit,
	)
	s.addKeyValueInfo(keyMaxKeys, uint64(len(keyMaxKeys.Value)))

	keyMinPruneInterval := NewKeyValue(
		s,
		startupKeyMinPruneInterval,
		[]byte(fmt.Sprintf("%d", s.cfg.MinPruneInterval)),
		"",
		reservedClientID,
		revisionLimit,
	)
	s.addKeyValueInfo(
		keyMinPruneInterval,
		uint64(len(keyMinPruneInterval.Value)),
	)

	keyPruneInterval := NewKeyValue(
		s,
		startupKeyPruneInterval,
		[]byte(fmt.Sprintf("%d", s.cfg.PruneInterval)),
		"",
		reservedClientID,
		revisionLimit,
	)
	s.addKeyValueInfo(keyPruneInterval, uint64(len(keyPruneInterval.Value)))

	keyPruneThreshold := NewKeyValue(
		s,
		startupKeyPruneThreshold,
		[]byte(fmt.Sprintf("%f", s.cfg.PruneThreshold)),
		"",
		reservedClientID,
		revisionLimit,
	)
	s.addKeyValueInfo(keyPruneThreshold, uint64(len(keyPruneThreshold.Value)))

	keyPruneTarget := NewKeyValue(
		s,
		startupKeyPruneTarget,
		[]byte(fmt.Sprintf("%f", s.cfg.PruneTarget)),
		"",
		reservedClientID,
		revisionLimit,
	)
	s.addKeyValueInfo(keyPruneTarget, uint64(len(keyPruneTarget.Value)))

	keyEagerPrune := NewKeyValue(
		s,
		startupKeyEagerPrune,
		[]byte(fmt.Sprintf("%t", s.cfg.EagerPrune)),
		"",
		reservedClientID,
		revisionLimit,
	)
	s.addKeyValueInfo(keyEagerPrune, uint64(len(keyEagerPrune.Value)))

	keyMaxValueSize := NewKeyValue(
		s,
		startupKeyMaxValueSize,
		[]byte(fmt.Sprintf("%d", s.cfg.MaxValueSize)),
		"",
		reservedClientID,
		revisionLimit,
	)
	s.addKeyValueInfo(keyMaxValueSize, uint64(len(keyMaxValueSize.Value)))

	keyMaxKeySize := NewKeyValue(
		s,
		startupKeyMaxKeySize,
		[]byte(fmt.Sprintf("%d", s.cfg.MaxKeyLength)),
		"",
		reservedClientID,
		revisionLimit,
	)
	s.addKeyValueInfo(keyMaxKeySize, uint64(len(keyMaxKeySize.Value)))

	keyRevisionLimit := NewKeyValue(
		s,
		startupKeyRevisionLimit,
		[]byte(fmt.Sprintf("%d", s.cfg.RevisionLimit)),
		"",
		reservedClientID,
		revisionLimit,
	)
	s.addKeyValueInfo(keyRevisionLimit, uint64(len(keyRevisionLimit.Value)))

	keySnapshotsEnabled := NewKeyValue(
		s,
		startupKeySnapshotEnabled,
		[]byte(fmt.Sprintf("%t", s.cfg.Snapshot.Enabled)),
		"",
		reservedClientID,
		revisionLimit,
	)
	s.addKeyValueInfo(
		keySnapshotsEnabled,
		uint64(len(keySnapshotsEnabled.Value)),
	)

	keySnapshotInterval := NewKeyValue(
		s,
		startupKeySnapshotInterval,
		[]byte(fmt.Sprintf("%d", s.cfg.Snapshot.Interval)),
		"",
		reservedClientID,
		revisionLimit,
	)
	s.addKeyValueInfo(
		keySnapshotInterval,
		uint64(len(keySnapshotInterval.Value)),
	)

	keySubscriberLimit := NewKeyValue(
		s,
		startupKeyEventStreamSubscriberLimit,
		[]byte(fmt.Sprintf("%d", s.cfg.EventStreamSubscriberLimit)),
		"",
		reservedClientID,
		revisionLimit,
	)
	s.addKeyValueInfo(
		keySubscriberLimit,
		uint64(len(keySubscriberLimit.Value)),
	)
}

func (s *KeyValueStore) requestLogger(
	ctx context.Context,
) *slog.Logger {
	if clientInfo := s.ClientInfo(ctx); clientInfo != nil {
		return s.logger.With("client", clientInfo)
	}
	return s.logger
}

// stopUnlockTimers runs with Stop to prevent any existing unlock
// timers from firing after the server has stopped
func (s *KeyValueStore) stopUnlockTimers() {
	s.logger.Info("stopping any lock timers")
	for key, keyLock := range s.locks {
		if keyLock.t != nil {
			stopped := keyLock.t.Stop()
			s.logger.Debug(
				"stopped unlock timer",
				"key",
				key,
				"stopped",
				stopped,
			)
			keyLock.t = nil
		}
	}
}

// stopExpirationTimers runs with Stop to prevent any existing expiration
// timers from firing after the server has stopped
func (s *KeyValueStore) stopExpirationTimers() {
	s.logger.Info("stopping any expiration timers")

	for key, keyReaper := range s.reapers {
		if keyReaper.t != nil {
			stopped := keyReaper.t.Stop()
			s.logger.Debug(
				"stopped unlock timer",
				"key",
				key,
				"stopped",
				stopped,
			)
			keyReaper.t = nil
		}
	}
}

func (s *KeyValueStore) staleKeys(
	ctx context.Context,
	includeLocked bool,
	includeReserved bool,
	ignoreKey ...string,
) []*KeyValueInfo {
	targetKeys := make([]keyWithMetric, 0, len(s.store))

	defer func() {
		for _, kv := range targetKeys {
			kv.kv.mu.RUnlock()
		}
	}()

	for k, kvInfo := range s.store {
		if ctx.Err() != nil {
			break
		}
		if !includeReserved && strings.HasPrefix(k, reservedPrefix) {
			continue
		}
		if sliceContains(ignoreKey, k) {
			continue
		}
		kvInfo.mu.RLock()
		keyLock := s.locks[kvInfo.Key]
		if !includeLocked && keyLock != nil {
			continue
		}

		ks := s.keyStats[kvInfo.Key]
		if ks == nil {
			panic(fmt.Sprintf("key %s has no stats", kvInfo.Key))
		}
		targetKeys = append(
			targetKeys,
			keyWithMetric{
				kv:     kvInfo,
				metric: ks,
			},
		)
	}

	toUnlock := []*KeyMetrics{}
	for _, tk := range targetKeys {
		if ctx.Err() != nil {
			break
		}
		tk.metric.mu.RLock()
		toUnlock = append(toUnlock, tk.metric)
	}
	sortKeyValueInfoByDates(targetKeys)
	for _, tk := range toUnlock {
		tk.mu.RUnlock()
	}
	result := make([]*KeyValueInfo, 0, len(targetKeys))
	for _, kv := range targetKeys {
		result = append(result, kv.kv)
	}
	return result

}

func (s *KeyValueStore) pruneNumKeys(
	ctx context.Context,
	limit int,
	eager bool,
	ignoreKey ...string,
) []string {
	if limit < 1 {
		return nil
	}
	s.logger.Info("pruning keys", "limit", limit)
	reportedCt := int(s.numKeys.Load())
	actualCt := len(s.store)
	started := time.Now()
	if reportedCt != actualCt {
		s.logger.Warn(
			"reported key count mismatch",
			"reported",
			reportedCt,
			"actual",
			actualCt,
		)
		s.numKeys.Store(uint64(actualCt))
	}

	toRemove := s.staleKeys(ctx, false, false, ignoreKey...)
	if limit > 0 {
		if limit <= len(toRemove) {
			toRemove = toRemove[:limit]
		}
	}

	removed := make([]string, 0)
	kvLocked := make([]*KeyValueInfo, 0, len(toRemove))
	for _, kvInfo := range toRemove {
		if ctx.Err() != nil {
			return removed
		}
		kvInfo.mu.Lock()
		kvLocked = append(kvLocked, kvInfo)
	}
	defer func() {
		for _, kv := range kvLocked {
			kv.mu.Unlock()
		}
	}()

	s.logger.Debug(
		"pending removal",
		"pending_ct",
		len(toRemove),
	)

	for _, tr := range toRemove {
		s.deleteKey(tr.Key, reservedClientID)
		removed = append(removed, tr.Key)
		s.emit(tr.Key, Expunged, reservedClientID, nil)
	}
	s.numKeys.Store(uint64(len(s.store)))
	s.addPruneLog(PruneLog{Time: started, KeysPruned: removed, Eager: eager})
	return removed
}

func (s *KeyValueStore) addPruneLog(p PruneLog) {
	if len(s.pruneHistory) >= DefaultMaxPruneHistory {
		for pi := 0; pi < len(s.pruneHistory)-1; pi++ {
			s.pruneHistory[pi] = s.pruneHistory[pi+1]
		}
		s.pruneHistory[len(s.pruneHistory)-1] = p
	} else {
		s.pruneHistory = append(s.pruneHistory, p)
	}
	s.logger.Debug("added to prune log", "prune_history", s.pruneHistory)
}

func (s *KeyValueStore) deleteKey(key string, clientID string) {
	kvInfo, ok := s.store[key]
	if !ok {
		return
	}
	keyReaper := s.reapers[key]

	if keyReaper != nil {
		s.logger.Debug("stopping expiration timer", "key", key)
		if keyReaper.t != nil {
			_ = keyReaper.t.Stop()
		}
	}
	keyLock, exists := s.locks[key]
	if exists {
		if keyLock.t != nil {
			s.logger.Debug("stopping lock timer", "key", key)
			_ = keyLock.t.Stop()
		}
		delete(s.locks, key)
	}
	s.logger.Info(
		"deleting key",
		kvLogKey, kvInfo,
	)
	delete(s.store, key)
	t := time.Now()
	defer s.emit(key, Deleted, clientID, &t)
	s.totalSize.Add(^(kvInfo.Size - 1))
	s.numKeys.Add(^uint64(0))
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

// eventLoop is the main handler for emitted Event structs. When the context
// passed to Start is cancelled, the provided channels (snapshotCh, logCh and
// eventStreamCh) are closed. When Stop is called, the main event channel
// is then closed, to avoid blocking sends to the channels, and to accommodate
// events being emitted between the grpc server stopping and the full
// shutdown process completing.
func (s *KeyValueStore) eventLoop(
	ctx context.Context,
	snapshotCh chan<- Event,
	logCh chan<- Event,
	eventStreamCh chan Event,
) {
	// evLock is used to ensure that the channels are closed when the
	// context is closed, so we don't block on sending to a closed channel
	evLock := sync.Mutex{}
	go func() {
		for {
			select {
			case <-ctx.Done():
				evLock.Lock()
				if snapshotCh != nil {
					close(snapshotCh)
					snapshotCh = nil
				}
				if logCh != nil {
					close(logCh)
					logCh = nil
				}
				if eventStreamCh != nil {
					close(eventStreamCh)
					eventStreamCh = nil
				}
				evLock.Unlock()
				return
			}
		}
	}()

	for event := range s.events {
		evLock.Lock()
		switch event.Event {
		case Created:
			s.numEventCreated.Add(1)
			s.keyStatMu.Lock()
			eventTime := event.Time
			_, exists := s.keyStats[event.Key]

			if !exists {
				s.keyStats[event.Key] = &KeyMetrics{}
			}
			keyStat := s.keyStats[event.Key]
			keyStat.mu.Lock()
			s.keyStatMu.Unlock()

			if keyStat.FirstSet == nil {
				keyStat.FirstSet = &eventTime
				keyStat.LastSet = &eventTime

			} else if keyStat.LastSet == nil || (keyStat.LastSet != nil && keyStat.LastSet.Before(eventTime)) {
				keyStat.LastSet = &eventTime
			}

			keyStat.SetCount++
			keyStat.mu.Unlock()
		case Updated:
			s.numEventUpdated.Add(1)
			s.keyStatMu.RLock()
			keyStat := s.keyStats[event.Key]
			keyStat.mu.Lock()
			s.keyStatMu.RUnlock()
			eventTime := event.Time
			if keyStat.LastSet == nil || (keyStat.LastSet != nil && keyStat.LastSet.Before(eventTime)) {
				keyStat.LastSet = &eventTime
			}
			keyStat.SetCount++
			keyStat.mu.Unlock()
		case Deleted:
			s.numEventDeleted.Add(1)
		case Expired:
			s.numEventExpired.Add(1)
		case Locked:
			s.numEventLocked.Add(1)
			s.keyStatMu.RLock()
			keyStat := s.keyStats[event.Key]
			keyStat.mu.Lock()
			s.keyStatMu.RUnlock()
			eventTime := event.Time
			if keyStat.FirstLocked == nil {
				keyStat.FirstLocked = &eventTime
				keyStat.LastLocked = &eventTime
			}
			if keyStat.LastLocked != nil && keyStat.LastLocked.Before(eventTime) {
				keyStat.LastLocked = &eventTime
			}

			keyStat.LockCount++
			keyStat.mu.Unlock()
		case Unlocked:
			s.numEventUnlocked.Add(1)
		case Expunged:
			s.numEventExpunged.Add(1)
		case Accessed:
			s.numEventValueAccessed.Add(1)
			s.keyStatMu.RLock()
			keyStat := s.keyStats[event.Key]
			keyStat.mu.Lock()
			s.keyStatMu.RUnlock()
			eventTime := event.Time
			keyStat.AccessCount++
			if keyStat.FirstAccessed == nil {
				keyStat.FirstAccessed = &eventTime
				keyStat.LastAccessed = &eventTime
			} else if keyStat.LastAccessed != nil && keyStat.LastAccessed.Before(eventTime) {
				keyStat.LastAccessed = &eventTime
			}
			keyStat.mu.Unlock()
		case LifespanSet:
			s.numEventLifespanSet.Add(1)
		}
		s.broadcast(ctx, event, snapshotCh, logCh, eventStreamCh)
		evLock.Unlock()
	}
}

type Config struct {
	ServiceName string `json:"service_name" yaml:"service_name" mapstructure:"service_name"`
	// Name is the server name to use when storing snapshots. When loading
	// snapshots, this will be used to find the latest.
	Name                string        `json:"name" yaml:"name" mapstructure:"name"`
	GracefulStopTimeout time.Duration `json:"graceful_stop_timeout" yaml:"graceful_stop_timeout" mapstructure:"graceful_stop_timeout"`
	// MaxLockDuration is the maximum duration a lock can be held for
	MaxLockDuration time.Duration `json:"max_lock_duration" yaml:"max_lock_duration" mapstructure:"max_lock_duration"`
	// MinLockDuration is the minimum duration a lock can be held for
	MinLockDuration time.Duration `json:"min_lock_duration" yaml:"min_lock_duration" mapstructure:"min_lock_duration"`
	// MinLifespan is the minimum lifespan of a key, if set.
	MinLifespan time.Duration `json:"min_lifespan" yaml:"min_lifespan" mapstructure:"min_lifespan"`
	// MaxNumberOfKeys is the maximum number of keys that can be stored
	MaxNumberOfKeys  uint64        `json:"max_keys" yaml:"max_keys" mapstructure:"max_keys"`
	MinPruneInterval time.Duration `json:"min_prune_interval" yaml:"min_prune_interval" mapstructure:"min_prune_interval"`
	// PruneInterval is the interval at which the server will attempt to
	// prune any keys over the current threshold
	PruneInterval time.Duration `json:"prune_interval" yaml:"prune_interval" mapstructure:"prune_interval"`
	// If PruneInterval > 0, when the number of keys reaches
	// PruneThreshold * MaxNumberOfKeys, the server will attempt to delete
	// keys until the number of keys is PruneTarget * MaxNumberOfKeys. Locked
	// keys will be excluded from this process. Keys will be deleted in order
	// of their last updated time, oldest first (if a key hasn't been updated,
	// its creation date will be used).
	// This does not apply for EagerPrune
	PruneThreshold float64 `json:"prune_threshold" yaml:"prune_threshold" mapstructure:"prune_threshold"`
	// When pruning keys, up to PruneTarget * MaxNumberOfKeys will be deleted.
	// This does not apply for EagerPrune.
	PruneTarget float64 `json:"prune_target" yaml:"prune_target" mapstructure:"prune_target"`
	// If enabled, the server will attempt to prune keys when new keys are
	// being set, and the number of keys has reached MaxNumberOfKeys.
	// Only enough keys will be pruned to allow the new key to be set.
	// This excludes locked keys.
	EagerPrune bool `json:"eager_prune" yaml:"eager_prune" mapstructure:"eager_prune"`
	// MaxValueSize is the maximum size of a value in bytes
	MaxValueSize uint64 `json:"max_value_size" yaml:"max_value_size" mapstructure:"max_value_size"`
	// MaxKeyLength is the maximum size of a key's value in bytes
	MaxKeyLength uint64 `json:"max_key_length" yaml:"max_key_length" mapstructure:"max_key_length"`
	// RevisionLimit is the maximum number of revisions to keep for each key.
	RevisionLimit int64          `json:"revision_limit" yaml:"revision_limit" mapstructure:"revision_limit"`
	ListenAddress string         `json:"listen_address" yaml:"listen_address" mapstructure:"listen_address"`
	SSLKeyfile    string         `json:"ssl_keyfile" yaml:"ssl_keyfile" mapstructure:"ssl_keyfile"`
	SSLCertfile   string         `json:"ssl_certfile" yaml:"ssl_certfile" mapstructure:"ssl_certfile"`
	Snapshot      SnapshotConfig // `json:"snapshot" yaml:"snapshot" mapstructure:"snapshot"`
	// LogEvents enables logging of events (e.g. Key created, updated, deleted)
	// as emitted by the event stream
	LogEvents bool   `json:"log_events" yaml:"log_events" mapstructure:"log_events"`
	LogLevel  string `json:"log_level" yaml:"log_level" mapstructure:"log_level"`
	// LogJSON (if true) will set the logging handler to JSON
	LogJSON     bool         `json:"log_json" yaml:"log_json" mapstructure:"log_json"`
	Logger      *slog.Logger `json:"-" yaml:"-" mapstructure:"-"`
	EventLogger *slog.Logger `json:"-" yaml:"-" mapstructure:"-"`
	// PrivilegedClientID is the client ID that is allowed to set the server
	// into readonly mode
	PrivilegedClientID string `json:"privileged_client_id" yaml:"privileged_client_id" mapstructure:"privileged_client_id"`
	// Readonly sets the server into readonly mode, where only
	// Get, Inspect, ListKeys, Exists, Stats and Register are allowed
	Readonly bool `json:"readonly" yaml:"readonly" mapstructure:"readonly"`
	// EventStreamSendTimeout is the timeout for sending events to subscribers
	EventStreamSendTimeout time.Duration
	// Limits the number of subscribers to the event stream (and thus the
	// number of concurrent goroutines). The event logger, snapshotter and
	// pruners do not count against this number. (0 = unlimited)
	EventStreamSubscriberLimit uint64
	// MonitorAddress is the listen address for non-RPC HTTP endpoints
	// (this includes /debug/pprof, /debug/vars and /metrics)
	MonitorAddress string `json:"monitor_address" yaml:"monitor_address" mapstructure:"monitor_address"`
	// PPROF enables the /debug/pprof endpoint
	PPROF bool `json:"pprof" yaml:"pprof" mapstructure:"pprof"`
	// ExpVar enables the /debug/vars endpoint
	ExpVar bool `json:"expvar" yaml:"expvar" mapstructure:"expvar"`
	// Metrics enables the /metrics endpoint and enables telemetry
	Metrics bool `json:"metrics" yaml:"metrics" mapstructure:"metrics"`
	// Trace enables application tracing telemetry
	Trace bool `json:"trace" yaml:"trace" mapstructure:"trace"`
}

func (c Config) LogValue() slog.Value {
	return slog.GroupValue(
		slog.Int64("revision_limit", c.RevisionLimit),
		slog.Uint64("max_keys", c.MaxNumberOfKeys),
		slog.Uint64("max_value_size", c.MaxValueSize),
		slog.Uint64("max_key_size", c.MaxKeyLength),
		slog.Duration("max_lock_duration", c.MaxLockDuration),
		slog.Duration("min_lock_duration", c.MinLockDuration),
		slog.Duration("min_lifespan", c.MinLifespan),
		slog.Duration("min_prune_interval", c.MinPruneInterval),
		slog.Duration("prune_interval", c.PruneInterval),
		slog.Float64("prune_threshold", c.PruneThreshold),
		slog.Float64("prune_target", c.PruneTarget),
		slog.Bool("eager_prune", c.EagerPrune),
		slog.String("listen_address", c.ListenAddress),
		slog.String("ssl_keyfile", c.SSLKeyfile),
		slog.String("ssl_certfile", c.SSLCertfile),
		slog.Bool("log_events", c.LogEvents),
		slog.String("log_level", c.LogLevel),
		slog.Bool("log_json", c.LogJSON),
		slog.Bool("readonly", c.Readonly),
		slog.Bool("pprof", c.PPROF),
		slog.Bool("metrics", c.Metrics),
		slog.Bool("expvar", c.ExpVar),
		slog.Bool("trace", c.Trace),
		slog.String("monitor_address", c.MonitorAddress),
		slog.Group(
			"snapshot",
			slog.Duration("interval", c.Snapshot.Interval),
			slog.Bool("enabled", c.Snapshot.Enabled),
			slog.String("database", c.Snapshot.Database),
		),
		slog.Duration("event_stream_send_timeout", c.EventStreamSendTimeout),
		slog.Uint64(
			"event_stream_subscriber_limit",
			c.EventStreamSubscriberLimit,
		),
	)
}

func RunServer(
	ctx context.Context,
	grpcServer *grpc.Server,
	lis net.Listener,
	srv *KeyValueStore,
) error {
	pb.RegisterKeyValueStoreServer(grpcServer, srv)
	var socketFile string

	if lis == nil {
		u := parseURL(srv.cfg.ListenAddress)
		if u.Scheme == "unix" {
			_, err := os.Stat(u.Host)
			if err == nil || !os.IsNotExist(err) {
				return fmt.Errorf(
					"socket file '%s' already exists",
					u.Host,
				)
			}
			socketFile = u.Host
		}
	}

	srvDone := make(chan struct{}, 1)
	sctx, scancel := context.WithCancel(ctx)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		if e := srv.Start(sctx); e != nil {
			panic(fmt.Errorf("failed to start server: %w", e))
		}
		srvDone <- struct{}{}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case <-ctx.Done():
			srv.logger.Info("shutting down server")

			grpcServerStopped := make(chan struct{}, 1)
			if srv.cfg.GracefulStopTimeout == 0 {
				grpcServer.GracefulStop()
				grpcServerStopped <- struct{}{}
				close(grpcServerStopped)
			} else {
				gsTimer := time.NewTimer(srv.cfg.GracefulStopTimeout)
				go func() {
					grpcServer.GracefulStop()
					grpcServerStopped <- struct{}{}
					close(grpcServerStopped)
				}()
				select {
				case <-gsTimer.C:
					srv.logger.Warn(
						"graceful stop timed out, forcing stop",
						"timeout",
						srv.cfg.GracefulStopTimeout,
					)
					grpcServer.Stop()
				case <-grpcServerStopped:
					srv.logger.Info("grpc server stopped gracefully")
				}
			}

			socketCleanupDone := make(chan struct{}, 1)
			if socketFile != "" {
				go func() {
					socketErr := os.Remove(socketFile)
					if socketErr != nil && !os.IsNotExist(socketErr) {
						srv.logger.Error(
							"failed to remove socket file",
							slog.String("error", socketErr.Error()),
						)
					}
					socketCleanupDone <- struct{}{}
				}()
			} else {
				socketCleanupDone <- struct{}{}
			}

			scancel()
			<-srvDone
			stopErr := srv.Stop()
			if stopErr != nil {
				srv.logger.Error(
					"failed to stop server",
					slog.String("error", stopErr.Error()),
				)
			}
			srv.logger.Debug("waiting for socket cleanup, if needed")
			<-socketCleanupDone
			return
		}
	}()

	if err := grpcServer.Serve(lis); err != nil {
		srv.logger.Error(
			"failed to serve",
			slog.String("error", err.Error()),
		)
	}
	wg.Wait()
	return nil
}

type KeyValueStoreState struct {
	Keys    []*KeyValueInfo        `json:"keys"`
	Clients []*ClientInfo          `json:"clients"`
	Locks   []*KeyLock             `json:"locks"`
	Reapers []*KeyReaper           `json:"reapers"`
	Version string                 `json:"version"`
	Metrics map[string]*KeyMetrics `json:"metrics"`
}

type KeyReaper struct {
	Key         string        `json:"key"`
	Lifespan    time.Duration `json:"lifespan"`
	LifespanSet time.Time     `json:"lifespan_set"`
	t           *time.Timer
	srv         *KeyValueStore
}

func (r *KeyReaper) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("key", r.Key),
		slog.Duration("lifespan", r.Lifespan),
	)
}

func NewKeyReaper(
	srv *KeyValueStore,
	d time.Duration,
	key string,
	clientID string,
) *KeyReaper {
	now := time.Now()
	r := &KeyReaper{
		Key:         key,
		Lifespan:    d,
		srv:         srv,
		LifespanSet: now,
	}
	t := time.AfterFunc(d, r.ExpireFunc())
	r.t = t
	defer srv.emit(key, LifespanSet, clientID, &now)
	return r
}

func (r *KeyReaper) ExpireFunc() func() {
	return func() {
		if r.srv.snapshotter != nil {
			r.srv.snapshotter.mu.RLock()
			defer r.srv.snapshotter.mu.RUnlock()
		}
		r.srv.mu.Lock()
		defer r.srv.mu.Unlock()

		logger := r.srv.logger.With(loggerKey, "reaper", "key_reaper", r)

		if r.srv.stopped {
			logger.Debug("server stopped, skipping expire func")
			return
		}

		r.srv.lockMu.Lock()
		defer r.srv.lockMu.Unlock()

		r.srv.reaperMu.Lock()
		defer r.srv.reaperMu.Unlock()

		reaper, ok := r.srv.reapers[r.Key]

		if !ok {
			return
		}
		if reaper != r {
			return
		}
		keyLock, locked := r.srv.locks[r.Key]
		events := []Event{}

		if locked {
			_ = keyLock.t.Stop()
			delete(r.srv.locks, r.Key)
			events = append(
				events,
				Event{
					Key:      r.Key,
					Event:    Unlocked,
					ClientID: reservedClientID,
					Time:     time.Now(),
				},
			)
		}

		delete(r.srv.reapers, r.Key)
		events = append(
			events,
			Event{
				Key:      r.Key,
				Event:    Expired,
				ClientID: reservedClientID,
				Time:     time.Now(),
			},
		)

		kvInfo, exists := r.srv.store[r.Key]
		if !exists {
			return
		}

		delete(r.srv.store, r.Key)
		events = append(
			events,
			Event{
				Key:      r.Key,
				Event:    Deleted,
				ClientID: reservedClientID,
				Time:     time.Now(),
			},
		)
		defer func() {
			for _, e := range events {
				r.srv.emit(e.Key, e.Event, e.ClientID, &e.Time)
			}
		}()
		r.srv.totalSize.Add(^(kvInfo.Size - 1))
		r.srv.numKeys.Add(^uint64(0))
	}
}

// KeyLock maintains a record of a lock on a key. KeyValueStore maintains
// a registry of current locks (mapped by key) to each KeyLock
// in KeyValueStore.locks. When a KeyLock expires, it will be removed from
// that registry, and KeyValueStore.store. If the key isn't registered,
// nothing happens. If it is, but the current lock isn't the same as the
// one associated with UnlockFunc, nothing happens.
type KeyLock struct {
	// Key is the name of the key to hold a lock on
	Key string `json:"key"`
	// Duration is the amount of time to hold the lock before releasing
	Duration time.Duration `json:"duration"`
	// ClientID is the client that owns this lock
	ClientID string `json:"client_id"`
	// Created is the time the lock was created
	Created time.Time `json:"created"`
	t       *time.Timer
	srv     *KeyValueStore
}

// NewKeyLock creates a new lock for a key, and starts the timer
func NewKeyLock(
	srv *KeyValueStore,
	d time.Duration,
	key string,
	clientID string,
) *KeyLock {
	now := time.Now()
	k := &KeyLock{
		Key:      key,
		ClientID: clientID,
		Duration: d,
		srv:      srv,
		Created:  now,
	}
	t := time.AfterFunc(d, k.UnlockFunc())
	k.t = t
	defer k.srv.emit(key, Locked, clientID, &now)

	return k
}

// UnlockFunc returns a function to be used with time.AfterFunc to
// unlock a key after the timer has passed. If the key is expired, it
// will be deleted upon unlocking.
func (k *KeyLock) UnlockFunc() func() {
	return func() {
		if k.srv.snapshotter != nil {
			k.srv.snapshotter.mu.RLock()
			defer k.srv.snapshotter.mu.RUnlock()
		}

		k.srv.mu.Lock()
		defer k.srv.mu.Unlock()

		k.srv.lockMu.Lock()
		defer k.srv.lockMu.Unlock()

		if k.srv.stopped {
			return
		}
		lock, ok := k.srv.locks[k.Key]

		if !ok {
			return
		}
		if lock != k {
			return
		}
		logger := k.srv.logger.With(loggerKey, "lock", "key_lock", k)

		logger.Info("unlocking key on trigger")

		delete(k.srv.locks, k.Key)
		now := time.Now()
		k.srv.emit(k.Key, Unlocked, reservedClientID, &now)
	}
}

func (k *KeyLock) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("key", k.Key),
		slog.String(clientIDKey, k.ClientID),
		slog.Duration("duration", k.Duration),
	)
}

type GOKVError struct {
	Message string `json:"message" yaml:"message"`
	Code    codes.Code
}

func (e GOKVError) Error() string {
	return e.Message
}

func (e GOKVError) GRPCStatus() *status.Status {
	return status.New(e.Code, e.Message)
}

type ClientInfo struct {
	ClientID  string    `json:"client_id" yaml:"client_id"`
	FirstSeen time.Time `json:"-" yaml:"-"`
	mu        sync.RWMutex
}

func (c *ClientInfo) LogValue() slog.Value {
	attrs := []slog.Attr{
		slog.String("id", c.ClientID),
	}
	return slog.GroupValue(attrs...)
}

type keyWithMetric struct {
	kv     *KeyValueInfo
	metric *KeyMetrics
}

type keyPressure struct {
	Keys              uint64  `json:"keys"`
	Max               uint64  `json:"max"`
	Used              float64 `json:"percent_used"`
	PruneThreshold    float64 `json:"prune_threshold"`
	KeysOverThreshold int64   `json:"keys_over_threshold"`
}

func (k *keyPressure) LogValue() slog.Value {
	return slog.GroupValue(
		slog.Uint64("keys", k.Keys),
		slog.Uint64("max", k.Max),
		slog.Any("used", k.Used),
		slog.Float64("prune_threshold", k.PruneThreshold),
		slog.Int64("keys_over_threshold", k.KeysOverThreshold),
	)
}

// NewServer returns a new KeyValueStore server
func NewServer(cfg *Config) (*KeyValueStore, error) {
	if cfg == nil {
		cfg = DefaultConfig()
	}
	if cfg.ServiceName == "" {
		cfg.ServiceName = "keyquarry"
	}
	if cfg.MaxValueSize == 0 {
		cfg.MaxValueSize = DefaultMaxValueSize
	}
	if cfg.MaxKeyLength == 0 {
		cfg.MaxKeyLength = DefaultMaxKeyLength
	}
	if cfg.MaxLockDuration == 0 {
		cfg.MaxLockDuration = DefaultMaxLockDuration
	}
	if cfg.MinLockDuration == 0 {
		cfg.MinLockDuration = DefaultMinLockDuration
	}
	if cfg.PruneThreshold == 0 {
		cfg.PruneThreshold = DefaultPruneThreshold
	}
	if cfg.PruneTarget == 0 {
		cfg.PruneTarget = DefaultPruneTarget
	}
	if cfg.MinLifespan == 0 {
		cfg.MinLifespan = DefaultMinExpiry
	}
	if cfg.EventStreamSendTimeout == 0 {
		cfg.EventStreamSendTimeout = DefaultEventStreamSendTimeout
	}
	if cfg.MinPruneInterval == 0 {
		cfg.MinPruneInterval = MinPruneInterval
	}
	if cfg.Logger == nil {
		var logger *slog.Logger
		var handler slog.Handler
		handlerOptions := &slog.HandlerOptions{
			AddSource: true,
		}

		if cfg.LogJSON {
			handler = slog.NewJSONHandler(os.Stdout, handlerOptions)
		} else {
			handler = slog.NewTextHandler(os.Stdout, handlerOptions)
		}
		logger = slog.New(handler).With(
			loggerKey,
			"server",
		)
		//goland:noinspection GoBoolExpressions
		if build.Version != "" {
			logger = logger.With(
				slog.String(
					"version",
					build.Version,
				),
			)
		}
		cfg.Logger = logger
	}

	if cfg.EventLogger == nil {
		eventHandlerOpts := &slog.HandlerOptions{
			Level: LevelEvent,
			ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
				if a.Key == slog.LevelKey {
					a.Value = slog.StringValue("EVENT")
				}
				return a
			},
		}
		var eventHandler slog.Handler
		if cfg.LogJSON {
			eventHandler = slog.NewJSONHandler(
				os.Stdout,
				eventHandlerOpts,
			)
		} else {
			eventHandler = slog.NewTextHandler(
				os.Stdout,
				eventHandlerOpts,
			)
		}
		cfg.EventLogger = slog.New(eventHandler)
	}

	srv := &KeyValueStore{
		store:        make(map[string]*KeyValueInfo),
		logger:       cfg.Logger,
		cfg:          cfg,
		locks:        make(map[string]*KeyLock),
		reapers:      make(map[string]*KeyReaper),
		pruneHistory: make([]PruneLog, 0, DefaultMaxPruneHistory),
		events:       make(chan Event),
		keyStats:     map[string]*KeyMetrics{},
		clientInfo:   map[string]*ClientInfo{},
		Tracer:       otel.Tracer("keyquarry_server"),
	}
	srv.cfgMu.Lock()
	defer srv.cfgMu.Unlock()

	debugServerPath := "/debug"
	expvarPath := debugServerPath + "/vars"
	pprofPrefix := debugServerPath + "/pprof"
	metricsPath := "/metrics"

	if cfg.ExpVar || cfg.PPROF || cfg.Metrics {
		mux := http.NewServeMux()
		mux.HandleFunc(
			"/", func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("OK"))
			},
		)
		cfg.Logger.Info(
			"registering monitor endpoints",
			"address",
			cfg.MonitorAddress,
		)

		if cfg.ExpVar {
			cfg.Logger.Info(
				"registering expvar handler",
				"address",
				cfg.MonitorAddress,
				"path",
				expvarPath,
			)
			mux.Handle(
				expvarPath,
				otelhttp.WithRouteTag(expvarPath, expvar.Handler()),
			)
		}

		if cfg.PPROF {
			runtime.SetMutexProfileFraction(1)
			runtime.SetBlockProfileRate(1)
			cfg.Logger.Info(
				"registering ppprof handlers",
				"address",
				cfg.MonitorAddress,
				"path",
				pprofPrefix,
			)
			mux.Handle(
				pprofPrefix+"/",
				otelhttp.WithRouteTag(
					pprofPrefix+"/",
					http.HandlerFunc(pprof.Index),
				),
			)
			mux.Handle(
				pprofPrefix+"/profile",
				otelhttp.WithRouteTag(
					pprofPrefix+"/profile",
					http.HandlerFunc(pprof.Profile),
				),
			)
			mux.Handle(
				pprofPrefix+"/symbol",
				otelhttp.WithRouteTag(
					pprofPrefix+"/symbol",
					http.HandlerFunc(pprof.Symbol),
				),
			)
			mux.Handle(
				pprofPrefix+"/cmdline",
				otelhttp.WithRouteTag(
					pprofPrefix+"/cmdline",
					http.HandlerFunc(pprof.Cmdline),
				),
			)
			mux.Handle(
				pprofPrefix+"/trace",
				otelhttp.WithRouteTag(
					pprofPrefix+"/trace",
					http.HandlerFunc(pprof.Trace),
				),
			)

			mux.Handle(
				pprofPrefix+"/heap",
				otelhttp.WithRouteTag(
					pprofPrefix+"/heap",
					pprof.Handler("heap"),
				),
			)
			mux.Handle(
				pprofPrefix+"/goroutine",
				otelhttp.WithRouteTag(
					pprofPrefix+"/goroutine",
					pprof.Handler("goroutine"),
				),
			)
			mux.Handle(
				pprofPrefix+"/threadcreate",
				otelhttp.WithRouteTag(
					pprofPrefix+"/threadcreate",
					pprof.Handler("threadcreate"),
				),
			)
			mux.Handle(
				pprofPrefix+"/block",
				otelhttp.WithRouteTag(
					pprofPrefix+"/block",
					pprof.Handler("block"),
				),
			)
			mux.Handle(
				pprofPrefix+"/mutex",
				otelhttp.WithRouteTag(
					pprofPrefix+"/mutex",
					pprof.Handler("mutex"),
				),
			)
			mux.Handle(
				pprofPrefix+"/allocs",
				otelhttp.WithRouteTag(
					pprofPrefix+"/allocs",
					pprof.Handler("allocs"),
				),
			)
		}

		if cfg.Metrics {
			cfg.Logger.Info(
				"registering prometheus handler",
				"address",
				cfg.MonitorAddress,
				"path",
				metricsPath,
			)

			mux.Handle(
				metricsPath,
				otelhttp.WithRouteTag(
					metricsPath,
					promhttp.Handler(),
				),
			)
		}
		cfg.Logger.Info("registered all handlers")
		srv.MonitorMux = mux
	}

	srv.mu.Lock()
	defer srv.mu.Unlock()

	srv.logger.Info("creating event stream")
	srv.eventStream = NewEventStream(srv)

	srv.logger.Info("creating snapshotter")
	snapshotter, err := NewSnapshotter(
		srv,
	)
	if err != nil {
		return nil, err
	}
	srv.snapshotter = snapshotter

	srv.logger.Info("creating pruner")
	srv.pruner = NewPruner(srv)

	return srv, nil
}

func NewServerFromLatestSnapshot(
	ctx context.Context,
	cfg *Config,
) (*KeyValueStore, error) {
	if cfg == nil {
		cfg = DefaultConfig()
	}
	srv, err := NewServer(cfg)
	if err != nil {
		return nil, err
	}

	connStr := srv.cfg.Snapshot.Database
	if connStr == "" {
		return nil, fmt.Errorf("no database connection string provided")
	}

	dialect := GetDialect(srv.cfg.Snapshot.Database)
	if dialect == nil {
		return nil, fmt.Errorf(
			"unknown database type '%s'",
			srv.cfg.Snapshot.Database,
		)
	}

	db, err := dialect.DBConn(connStr)
	if err != nil {
		return nil, err
	}
	latestSnapshot, err := dialect.GetLatestSnapshot(ctx, db, cfg.Name)
	if err != nil {
		return nil, err
	}
	srv.logger.Info(
		"loading state from snapshot",
		slog.Group(
			"snapshot",
			slog.Int64("id", latestSnapshot.ID),
			slog.String("server_name", latestSnapshot.ServerName),
			slog.Time("created", latestSnapshot.Created),
		),
	)
	err = json.Unmarshal(latestSnapshot.Data, srv)
	if err != nil {
		return nil, fmt.Errorf("unable to unmarshal snapshot: %w", err)
	}
	return srv, nil
}

func DefaultConfig() *Config {
	cfg := &Config{
		GracefulStopTimeout: DefaultGracefulStopTimeout,
		MaxValueSize:        DefaultMaxValueSize,
		MaxKeyLength:        DefaultMaxKeyLength,
		RevisionLimit:       DefaultRevisionLimit,
		MaxNumberOfKeys:     DefaultMaxKeys,
		MaxLockDuration:     DefaultMaxLockDuration,
		MinLockDuration:     DefaultMinLockDuration,
		MinPruneInterval:    MinPruneInterval,
		PruneThreshold:      DefaultPruneThreshold,
		PruneInterval:       0,
		EagerPrune:          false,
		MinLifespan:         DefaultMinExpiry,
		LogEvents:           false,
		LogLevel:            slog.LevelInfo.String(),
		Snapshot: SnapshotConfig{
			Enabled: false,
		},
	}
	handler := slog.NewTextHandler(
		os.Stdout, &slog.HandlerOptions{
			AddSource: true,
		},
	)
	logger := slog.New(handler).With(
		loggerKey,
		"server",
	)
	//goland:noinspection GoBoolExpressions
	if build.Version != "" {
		logger = logger.With(
			slog.String(
				"version",
				build.Version,
			),
		)
	}
	cfg.Logger = logger
	cfg.MonitorAddress = DefaultMonitorAddress
	return cfg
}

func ClientIDInterceptor(srv *KeyValueStore) grpc.UnaryServerInterceptor {
	f := func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (resp any, err error) {
		_, err = srv.ClientIDFromContext(ctx)
		if err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
	return f
}

// parseURL takes the given string, and attempts to parse it to
// a URL with scheme 'tcp' or 'unix', setting a default port if
// one isn't specified (TCP only)
func parseURL(s string) *url.URL {
	var network string
	var host string

	n, h, found := strings.Cut(s, "://")
	if found {
		host = h
		network = n
	} else {
		host = n
	}
	u := &url.URL{Scheme: network, Host: host}
	return u
}

func sliceContains[V comparable](slice []V, value V) bool {
	for _, v := range slice {
		if v == value {
			return true
		}
	}
	return false
}

func sortKeyValueInfoByDates(keys []keyWithMetric) []keyWithMetric {
	now := time.Now()
	sort.Slice(
		keys, func(i, j int) bool {
			ki := keys[i]
			kiMetric := ki.metric

			kj := keys[j]
			kjMetric := kj.metric
			kiScore := kiMetric.StaleScore(now)
			kjScore := kjMetric.StaleScore(now)
			return kiScore < kjScore
		},
	)
	return keys
}

type UnaryInterceptorChain []grpc.UnaryServerInterceptor
