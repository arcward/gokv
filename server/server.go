package server

import (
	"bytes"
	"compress/gzip"
	"crypto"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	pb "github.com/arcward/keyquarry/api"
	"github.com/arcward/keyquarry/build"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
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
	LevelEvent                           = slog.Level(100)
	DefaultEventStreamBufferSize         = 10000
	DefaultEventStreamSendTimeout        = time.Second
	DefaultMaxKeySize             uint64 = 1000 // 1,000 bytes
	MinKeyLength                         = 1
	DefaultMaxValueSize           uint64 = 1000000 // 1,000,000 bytes
	DefaultRevisionLimit          int64  = 0
	DefaultMaxKeys                uint64 = math.MaxUint64
	DefaultMaxLockDuration               = 24 * time.Hour
	DefaultMinLockDuration               = 5 * time.Second
	DefaultMinExpiry                     = 5 * time.Second
	DefaultSnapshotLimit                 = 5
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
)

var (
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
	startupKeyMaxKeySize      = fmt.Sprintf("%s/MaxKeySize", reservedPrefix)
	startupKeyRevisionLimit   = fmt.Sprintf("%s/RevisionLimit", reservedPrefix)
	startupKeyHashAlgorithm   = fmt.Sprintf("%s/HashAlgorithm", reservedPrefix)
	startupKeySnapshotEnabled = fmt.Sprintf(
		"%s/Snapshot/Enabled",
		reservedPrefix,
	)
	startupKeySnapshotInterval = fmt.Sprintf(
		"%s/Snapshot/Interval",
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

type KeyEvent uint

const (
	NoEvent KeyEvent = iota
	Created
	Updated
	Deleted
	Expired
	Locked
	Unlocked
	Expunged
)

func (ke KeyEvent) GoString() string {
	return ke.String()
}

func (ke KeyEvent) String() string {
	switch ke {
	case Created:
		return "CREATED"
	case Updated:
		return "UPDATED"
	case Deleted:
		return "DELETED"
	case Expired:
		return "EXPIRED"
	case Locked:
		return "LOCKED"
	case Unlocked:
		return "UNLOCKED"
	case Expunged:
		return "EXPUNGED"
	default:
		return "NO_EVENT"
	}
}

func (ke KeyEvent) LogValue() slog.Value {
	return slog.StringValue(ke.String())
}

type Event struct {
	Key      string
	Event    KeyEvent
	Time     time.Time
	ClientID string
}

func (e Event) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("key", e.Key),
		slog.String("event", e.Event.String()),
		slog.Time("time", e.Time),
		slog.String("client_id", e.ClientID),
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

type SnapshotConfig struct {
	LoadFile string
	// SnapshotDir is the directory where snapshots are stored. This directory
	// is also checked on startup for snapshots to load.
	Dir      string        `json:"dir" yaml:"dir" mapstructure:"dir"`
	Interval time.Duration `json:"interval" yaml:"interval" mapstructure:"interval"`
	// SnapshotLimit is the maximum number of snapshots to keep for each run.
	// Once this limit is reached, the oldest recorded snapshot will be deleted
	// on each new snapshot. Snapshots from other runs are not affected.
	Limit int `json:"limit" yaml:"limit" mapstructure:"limit"`
	// SnapshotEncryption enables/disables encryption of snapshot files. Requires SecretKey
	Encrypt bool `json:"encrypt" yaml:"encrypt" mapstructure:"encrypt"`
	// SecretKey is used to encrypt/decrypt snapshots
	SecretKey string `json:"secret_key" yaml:"secret_key" mapstructure:"secret_key"`
	Enabled   bool   `json:"enabled" yaml:"enabled" mapstructure:"enabled"`
}

func (s SnapshotConfig) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("dir", s.Dir),
		slog.Duration("interval", s.Interval),
		slog.Int("limit", s.Limit),
		slog.Bool("encrypt", s.Encrypt),
		slog.Bool("enabled", s.Enabled),
	)
}

type Config struct {
	// MaxLockDuration is the maximum duration a lock can be held for
	MaxLockDuration time.Duration `json:"max_lock_duration" yaml:"max_lock_duration" mapstructure:"max_lock_duration"`
	// MinLockDuration is the minimum duration a lock can be held for
	MinLockDuration time.Duration `json:"min_lock_duration" yaml:"min_lock_duration" mapstructure:"min_lock_duration"`
	// MinLifespan is the minimum lifespan of a key, if set.
	MinLifespan time.Duration `json:"min_lifespan" yaml:"min_lifespan" mapstructure:"min_lifespan"`
	// MaxNumberOfKeys is the maximum number of keys that can be stored
	MaxNumberOfKeys  uint64        `json:"max_keys" yaml:"max_keys" mapstructure:"max_keys"`
	MinPruneInterval time.Duration `json:"min_prune_interval" yaml:"min_prune_interval" mapstructure:"min_prune_interval"`
	PruneInterval    time.Duration `json:"prune_interval" yaml:"prune_interval" mapstructure:"prune_interval"`
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
	// MaxKeySize is the maximum size of a key's value in bytes
	MaxKeySize uint64 `json:"max_key_size" yaml:"max_key_size" mapstructure:"max_key_size"`
	// RevisionLimit is the maximum number of revisions to keep for each key.
	RevisionLimit int64          `json:"revision_limit" yaml:"revision_limit" mapstructure:"revision_limit"`
	HashAlgorithm crypto.Hash    `json:"hash_algorithm" yaml:"hash_algorithm" mapstructure:"hash_algorithm"`
	ListenAddress string         `json:"listen_address" yaml:"listen_address" mapstructure:"listen_address"`
	SSLKeyfile    string         `json:"ssl_keyfile" yaml:"ssl_keyfile" mapstructure:"ssl_keyfile"`
	SSLCertfile   string         `json:"ssl_certfile" yaml:"ssl_certfile" mapstructure:"ssl_certfile"`
	Snapshot      SnapshotConfig //`json:"snapshot" yaml:"snapshot" mapstructure:"snapshot"`
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
	// Get, GetKeyInfo, ListKeys, Exists, Stats and Register are allowed
	Readonly bool `json:"readonly" yaml:"readonly" mapstructure:"readonly"`
	// EventStreamSendTimeout is the timeout for sending events to subscribers
	EventStreamSendTimeout time.Duration
}

func (c Config) LogValue() slog.Value {
	var hashAlgorithmName string
	if c.HashAlgorithm > 0 {
		hashAlgorithmName = c.HashAlgorithm.String()
	}
	return slog.GroupValue(
		slog.String("hash_algorithm", hashAlgorithmName),
		slog.Int64("revision_limit", c.RevisionLimit),
		slog.Uint64("max_keys", c.MaxNumberOfKeys),
		slog.Uint64("max_value_size", c.MaxValueSize),
		slog.Uint64("max_key_size", c.MaxKeySize),
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
		slog.Group(
			"snapshot", slog.String("dir", c.Snapshot.Dir),
			slog.Duration("interval", c.Snapshot.Interval),
			slog.Int("limit", c.Snapshot.Limit),
			slog.Bool("encrypt", c.Snapshot.Encrypt),
			slog.Bool("enabled", c.Snapshot.Enabled),
		),
	)
}

func DefaultConfig() *Config {
	cfg := &Config{
		MaxValueSize:     DefaultMaxValueSize,
		MaxKeySize:       DefaultMaxKeySize,
		RevisionLimit:    DefaultRevisionLimit,
		MaxNumberOfKeys:  DefaultMaxKeys,
		MaxLockDuration:  DefaultMaxLockDuration,
		MinLockDuration:  DefaultMinLockDuration,
		MinPruneInterval: MinPruneInterval,
		PruneThreshold:   DefaultPruneThreshold,
		PruneInterval:    0,
		EagerPrune:       false,
		MinLifespan:      DefaultMinExpiry,
		LogEvents:        true,
		LogLevel:         slog.LevelInfo.String(),
		Snapshot: SnapshotConfig{
			Enabled: false,
			Limit:   DefaultSnapshotLimit,
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
	if build.Version != "" {
		logger = logger.With(
			slog.String(
				"version",
				build.Version,
			),
		)
	}
	cfg.Logger = logger
	return cfg
}

// NewServer returns a new KeyValueStore server
func NewServer(cfg *Config) (*KeyValueStore, error) {
	if cfg == nil {
		cfg = DefaultConfig()
	}
	if cfg.MaxValueSize == 0 {
		cfg.MaxValueSize = DefaultMaxValueSize
	}
	if cfg.MaxKeySize == 0 {
		cfg.MaxKeySize = DefaultMaxKeySize
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
		pruneHistory: make([]PruneLog, 0, DefaultMaxPruneHistory),
		events:       make(chan Event),
	}
	srv.cfgMu.Lock()
	defer srv.cfgMu.Unlock()
	srv.mu.Lock()
	defer srv.mu.Unlock()

	ev := &eventStream{
		SendTimeout: cfg.EventStreamSendTimeout,
		logger:      srv.logger.With(loggerKey, "events"),
	}
	srv.eventStream = ev

	snapshotter, err := NewSnapshotter(
		srv,
	)
	if err != nil {
		return nil, err
	}
	srv.snapshotter = snapshotter

	srv.pruner = NewPruner(srv)

	return srv, nil
}

func NewServerFromSnapshot(data []byte, cfg *Config) (*KeyValueStore, error) {
	contentType := http.DetectContentType(data)

	if strings.Contains(contentType, "gzip") {
		gz, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, fmt.Errorf("gzip decompression failed: %w", err)
		}
		defer func() {
			_ = gz.Close()
		}()
		decompressedData, err := io.ReadAll(gz)
		if err != nil {
			return nil, fmt.Errorf("gzip decompression failed: %w", err)
		}
		data = decompressedData
	}

	srv, err := NewServer(cfg)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(data, srv)
	if err != nil {
		return nil, fmt.Errorf("unable to unmarshal snapshot: %w", err)
	}
	return srv, nil
}

func NewServerFromEncryptedSnapshot(
	key string,
	data []byte,

	cfg *Config,
) (*KeyValueStore, error) {
	contentType := http.DetectContentType(data)

	if strings.Contains(contentType, "gzip") {
		gz, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, fmt.Errorf("gzip decompression failed: %w", err)
		}
		defer func() {
			_ = gz.Close()
		}()
		decompressedData, err := io.ReadAll(gz)
		if err != nil {
			return nil, fmt.Errorf("gzip decompression failed: %w", err)
		}
		data = decompressedData
	}

	data, err := decrypt(key, data)
	if err != nil {
		return nil, err
	}

	srv, err := NewServer(cfg)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(data, srv)
	if err != nil {
		return nil, fmt.Errorf("unable to unmarshal snapshot: %w", err)
	}
	return srv, nil
}

type ClientInfo struct {
	ClientID  string    `json:"client_id" yaml:"client_id"`
	Address   string    `json:"address" yaml:"address"`
	FirstSeen time.Time `json:"first_seen" yaml:"first_seen"`
	mu        sync.RWMutex
}

func (c *ClientInfo) LogValue() slog.Value {
	attrs := []slog.Attr{
		slog.String("id", c.ClientID),
		slog.String("address", c.Address),
	}
	return slog.GroupValue(attrs...)
}

type keyInfoWithLock struct {
	kv   *KeyValueInfo
	lock *KeyLock
}

func sortKeyValueInfoByDates(keys []keyInfoWithLock) []keyInfoWithLock {
	sort.Slice(
		keys, func(i, j int) bool {
			ki := keys[i]
			kj := keys[j]

			var kiCompare time.Time

			if ki.kv.Updated.IsZero() {
				kiCompare = ki.kv.Created
			} else {
				kiCompare = ki.kv.Updated
			}
			if ki.lock != nil && ki.lock.Created.After(kiCompare) {
				kiCompare = ki.lock.Created
			}

			var kjCompare time.Time

			if kj.kv.Updated.IsZero() {
				kjCompare = kj.kv.Created
			} else {
				kjCompare = kj.kv.Updated
			}

			if !kj.kv.Created.IsZero() && kj.kv.Created.After(kjCompare) {
				kjCompare = kj.kv.Created
			}

			if kj.lock != nil && kj.lock.Created.After(kjCompare) {
				kjCompare = kj.lock.Created
			}
			return kiCompare.Before(kjCompare)
		},
	)
	return keys
}

type KeyValueStore struct {
	store     map[string]*KeyValueInfo
	logger    *slog.Logger
	cfg       *Config
	cfgMu     sync.RWMutex
	numKeys   atomic.Uint64
	totalSize atomic.Uint64

	numNewKeysSet   atomic.Uint64
	numKeysUpdated  atomic.Uint64
	numKeysDeleted  atomic.Uint64
	numKeysLocked   atomic.Uint64
	numKeysUnlocked atomic.Uint64
	numKeysExpired  atomic.Uint64
	numSnapshots    atomic.Uint64
	numKeysExpunged atomic.Uint64
	numReservedKeys atomic.Uint64
	clientIDs       atomic.Uint64

	// mu is the main mutex for the server
	mu sync.RWMutex
	// clientInfo is a map of client IDs to ClientInfo
	clientInfo map[string]*ClientInfo
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
	// pruner handles the scheduled pruning of keys over the configured threshold
	pruner *Pruner
	// pruneHistory is a log of pruning events, up to the 100 most recent
	pruneHistory []PruneLog
	// events is the main channel for emitting events. This channel is
	// closed as the final step of Stop
	events chan Event

	pb.UnimplementedKeyValueStoreServer
}

// Subscribe returns a channel of events.
func (s *KeyValueStore) Subscribe(name string) (chan Event, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.started {
		return nil, errors.New("server not started")
	}
	return s.eventStream.Subscribe(name)
}

// Dump writes a JSON dump of the key-value store's current state to
// a temporary file, then renames the temp file to the given path.
func (s *KeyValueStore) Dump(filename string) error {
	s.logger.Info(
		"backing up store", slog.String("backup", filename),
	)
	data, err := json.Marshal(s)
	if err != nil {
		return fmt.Errorf("unable to marshal json: %w", err)
	}
	basename := filepath.Base(filename)
	tmp, err := os.CreateTemp("", fmt.Sprintf("%s.*", basename))
	if err != nil && os.IsNotExist(err) {
		tmp, err = os.CreateTemp(
			filepath.Dir(filename),
			fmt.Sprintf("%s.*", basename),
		)
	}
	if err != nil {
		return fmt.Errorf(
			"error creating temp file '%s': %w",
			basename,
			err,
		)
	}

	tmpFilename := tmp.Name()
	_, err = tmp.Write(data)
	_ = tmp.Close()
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
		"backup complete", slog.String("backup", filename),
	)
	return nil
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
	for key, kvInfo := range s.store {
		if kvInfo.expirationTimer != nil {
			stopped := kvInfo.expirationTimer.Stop()
			kvInfo.expirationTimer = nil
			s.logger.Debug(
				"stopped expiration timer",
				"key",
				key,
				"stopped",
				stopped,
			)
		}
	}
}

// Stop cleans up timers/goroutines/channels, after the
// context given to Start has been finished, and its goroutines have
// finished.
func (s *KeyValueStore) Stop() error {
	s.logger.Info("cleaning up")

	s.cmu.Lock()
	defer s.cmu.Unlock()

	s.mu.Lock()
	defer s.mu.Unlock()

	s.lockMu.Lock()
	defer s.lockMu.Unlock()

	if s.stopped {
		return errors.New("server previously stopped")
	}

	wg := sync.WaitGroup{}
	s.stopped = true

	metricAttrs := make([]any, 0)

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
		s.logger.Info("gathering final metrics")
		metricAttrs = append(
			metricAttrs,
			"num_keys", s.numKeys.Load(),
			"total_size", s.totalSize.Load(),
			"new", s.numNewKeysSet.Load(),
			"updated", s.numKeysUpdated.Load(),
			"deleted", s.numKeysDeleted.Load(),
			"locked", s.numKeysLocked.Load(),
			"unlocked", s.numKeysUnlocked.Load(),
			"expired", s.numKeysExpired.Load(),
			"expunged", s.numKeysExpunged.Load(),
		)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		s.eventStream.mu.Lock()
		defer s.eventStream.mu.Unlock()
		s.eventStream.Stop()
	}()

	var err error

	var snapshot string

	s.cfgMu.RLock()
	defer s.cfgMu.RUnlock()

	if s.cfg.Snapshot.Enabled {
		wg.Add(1)
		s.logger.Info(
			"saving final snapshot",
			"config", s.cfg.Snapshot,
		)

		go func() {
			defer wg.Done()
			snapshot, err = s.snapshotter.Snapshot()
			if snapshot != "" {
				s.logger.Info(
					"final snapshot created",
					slog.String("snapshot", snapshot),
				)
			}
		}()
	}

	wg.Wait()
	metricAttrs = append(metricAttrs, "snapshots", s.numSnapshots.Load())

	s.logger.Info("final metrics", slog.Group("metric", metricAttrs...))
	if err != nil {
		return fmt.Errorf("error creating final snapshot: %w", err)
	}
	close(s.events)
	return err
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

func (s *KeyValueStore) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return errors.New("server previously started")
	}
	s.cfgMu.RLock()
	s.logger.Info("starting", "config", s.cfg)

	wg := &sync.WaitGroup{}

	snapshotCh := make(chan Event, DefaultEventStreamBufferSize)
	logCh := make(chan Event, DefaultEventStreamBufferSize)
	eventStreamCh := make(chan Event, DefaultEventStreamBufferSize)
	s.eventStream.events = eventStreamCh

	if !s.cfg.LogEvents {
		logCh = nil
	}

	go s.eventLoop(ctx, snapshotCh, logCh, eventStreamCh)

	wg.Add(1)
	go func() {
		defer wg.Done()
		s.logger.Info("running event stream")
		s.eventStream.Run(ctx)
	}()

	if s.cfg.LogEvents || s.cfg.LogLevel == "EVENT" {
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
		if !s.cfg.Snapshot.Enabled {
			s.logger.Warn("snapshotter disabled")
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
		if s.cfg.MaxNumberOfKeys == 0 {
			s.cfg.MaxNumberOfKeys = DefaultMaxKeys
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.logger.Info("starting pruner")
			_ = s.pruner.Run(ctx)
			s.logger.Info("pruner finished")
		}()
	} else {
		s.logger.Warn("key pruner disabled, set prune_interval to enable")
	}

	s.started = true

	s.addStartupKeys()
	s.cfgMu.RUnlock()
	s.mu.Unlock()

	wg.Wait()
	return nil
}

func (s *KeyValueStore) addStartupKeys() {
	startKv := NewKeyValue(
		s,
		startupKeyStarted,
		[]byte(time.Now().UTC().String()),
		"",
		nil,
		reservedClientID,
	)
	s.addKeyValueInfo(startKv, uint64(len(startKv.Value)))

	lockDurationKey := NewKeyValue(
		s,
		startupKeyMaxLockDuration,
		[]byte(fmt.Sprintf("%d", s.cfg.MaxLockDuration)),
		"",
		nil,
		reservedClientID,
	)
	s.addKeyValueInfo(lockDurationKey, uint64(len(lockDurationKey.Value)))

	minLockDurationKey := NewKeyValue(
		s,
		startupKeyMinLockDuration,
		[]byte(fmt.Sprintf("%d", s.cfg.MinLockDuration)),
		"",
		nil,
		reservedClientID,
	)
	s.addKeyValueInfo(minLockDurationKey, uint64(len(minLockDurationKey.Value)))

	keyMinLifespan := NewKeyValue(
		s,
		startupKeyMinLifespan,
		[]byte(fmt.Sprintf("%d", s.cfg.MinLifespan)),
		"",
		nil,
		reservedClientID,
	)
	s.addKeyValueInfo(keyMinLifespan, uint64(len(keyMinLifespan.Value)))

	keyMaxKeys := NewKeyValue(
		s,
		startupKeyMaxKeys,
		[]byte(fmt.Sprintf("%d", s.cfg.MaxNumberOfKeys)),
		"",
		nil,
		reservedClientID,
	)
	s.addKeyValueInfo(keyMaxKeys, uint64(len(keyMaxKeys.Value)))

	keyMinPruneInterval := NewKeyValue(
		s,
		startupKeyMinPruneInterval,
		[]byte(fmt.Sprintf("%d", s.cfg.MinPruneInterval)),
		"",
		nil,
		reservedClientID,
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
		nil,
		reservedClientID,
	)
	s.addKeyValueInfo(keyPruneInterval, uint64(len(keyPruneInterval.Value)))

	keyPruneThreshold := NewKeyValue(
		s,
		startupKeyPruneThreshold,
		[]byte(fmt.Sprintf("%f", s.cfg.PruneThreshold)),
		"",
		nil,
		reservedClientID,
	)
	s.addKeyValueInfo(keyPruneThreshold, uint64(len(keyPruneThreshold.Value)))

	keyPruneTarget := NewKeyValue(
		s,
		startupKeyPruneTarget,
		[]byte(fmt.Sprintf("%f", s.cfg.PruneTarget)),
		"",
		nil,
		reservedClientID,
	)
	s.addKeyValueInfo(keyPruneTarget, uint64(len(keyPruneTarget.Value)))

	keyEagerPrune := NewKeyValue(
		s,
		startupKeyEagerPrune,
		[]byte(fmt.Sprintf("%t", s.cfg.EagerPrune)),
		"",
		nil,
		reservedClientID,
	)
	s.addKeyValueInfo(keyEagerPrune, uint64(len(keyEagerPrune.Value)))

	keyMaxValueSize := NewKeyValue(
		s,
		startupKeyMaxValueSize,
		[]byte(fmt.Sprintf("%d", s.cfg.MaxValueSize)),
		"",
		nil,
		reservedClientID,
	)
	s.addKeyValueInfo(keyMaxValueSize, uint64(len(keyMaxValueSize.Value)))

	keyMaxKeySize := NewKeyValue(
		s,
		startupKeyMaxKeySize,
		[]byte(fmt.Sprintf("%d", s.cfg.MaxKeySize)),
		"",
		nil,
		reservedClientID,
	)
	s.addKeyValueInfo(keyMaxKeySize, uint64(len(keyMaxKeySize.Value)))

	keyRevisionLimit := NewKeyValue(
		s,
		startupKeyRevisionLimit,
		[]byte(fmt.Sprintf("%d", s.cfg.RevisionLimit)),
		"",
		nil,
		reservedClientID,
	)
	s.addKeyValueInfo(keyRevisionLimit, uint64(len(keyRevisionLimit.Value)))

	keyHashAlgorithm := NewKeyValue(
		s,
		startupKeyHashAlgorithm,
		[]byte(fmt.Sprintf("%s", s.cfg.HashAlgorithm)),
		"",
		nil,
		reservedClientID,
	)
	s.addKeyValueInfo(keyHashAlgorithm, uint64(len(keyHashAlgorithm.Value)))

	keySnapshotsEnabled := NewKeyValue(
		s,
		startupKeySnapshotEnabled,
		[]byte(fmt.Sprintf("%t", s.cfg.Snapshot.Enabled)),
		"",
		nil,
		reservedClientID,
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
		nil,
		reservedClientID,
	)
	s.addKeyValueInfo(
		keySnapshotInterval,
		uint64(len(keySnapshotInterval.Value)),
	)

	for k, _ := range s.store {
		if strings.HasPrefix(strings.ToLower(k), reservedPrefix) {
			s.numReservedKeys.Add(1)
		}
	}
}

func (s *KeyValueStore) addKeyValueInfo(kvInfo *KeyValueInfo, size uint64) {
	s.store[kvInfo.Key] = kvInfo
	s.numKeys.Add(1)
	s.totalSize.Add(size)
	s.emit(kvInfo.Key, Created, kvInfo.CreatedBy)
}

func (s *KeyValueStore) keyCapacityUsed() float64 {
	maxKeys := s.cfg.MaxNumberOfKeys
	if maxKeys == 0 {
		return 0
	}
	currentCount := s.numKeys.Load()
	capacityUsed := float64(currentCount) / float64(maxKeys)
	s.logger.Info(
		"key counts",
		slog.Uint64("current", currentCount),
		slog.Uint64("max", maxKeys),
		slog.Any("used", capacityUsed),
	)

	return capacityUsed
}

func (s *KeyValueStore) PruneHistory() []PruneLog {
	s.mu.RLock()
	defer s.mu.RUnlock()
	h := make([]PruneLog, len(s.pruneHistory))
	copy(h, s.pruneHistory)
	return h
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
	closeSignal := make(chan struct{}, 1)
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
				<-closeSignal
				return
			}
		}
	}()

	for event := range s.events {
		evLock.Lock()
		switch event.Event {
		case Created:
			s.numNewKeysSet.Add(1)
		case Updated:
			s.numKeysUpdated.Add(1)
		case Deleted:
			s.numKeysDeleted.Add(1)
		case Expired:
			s.numKeysExpired.Add(1)
		case Locked:
			s.numKeysLocked.Add(1)
		case Unlocked:
			s.numKeysUnlocked.Add(1)
		case Expunged:
			s.numKeysExpunged.Add(1)
		}
		s.broadcast(ctx, event, snapshotCh, logCh, eventStreamCh)
		evLock.Unlock()
	}
	<-closeSignal
}

// Unsubscribe removes the given subscriber name from the event stream, stops
// the sending worker and closes the outbound channel
func (s *KeyValueStore) Unsubscribe(name string) error {
	s.logger.Info("request to unsubscribe", "name", name)
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.started {
		return errors.New("server not started")
	}
	return s.eventStream.Unsubscribe(name)
}

func (s *KeyValueStore) Config() Config {
	s.cfgMu.RLock()
	cfg := *s.cfg
	s.cfgMu.RUnlock()
	return cfg
}

// emit sends the provided event details to the event stream as an Event
func (s *KeyValueStore) emit(key string, event KeyEvent, clientID string) {
	ev := Event{
		Key:      key,
		Event:    event,
		Time:     time.Now().UTC(),
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
		return "", status.Errorf(codes.InvalidArgument, "missing client_id")
	}
	clientID := clientIDs[0]
	if clientID == "" {
		s.logger.Warn("missing client_id")
		return "", status.Errorf(codes.InvalidArgument, "empty client_id")
	}
	if clientID == reservedClientID {
		s.logger.Warn("reserved client_id used")
		return "", status.Errorf(
			codes.InvalidArgument,
			"reserved client_id used",
		)
	}
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
		Address:   addr,
		FirstSeen: time.Now().UTC(),
	}
	s.clientIDs.Add(1)
	ci.mu.RLock()
	defer ci.mu.RUnlock()
	s.clientInfo[clientID] = ci
	s.logger.Info("new client seen", "client", ci)
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

// Restore unmarshalls the provided data (the output of Dump), resets
// the current store, and populates it with the data from the dump.
// The number of keys restored is returned, along with any error.
func (s *KeyValueStore) Restore(data []byte) (n int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.cmu.Lock()
	defer s.cmu.Unlock()

	s.lockMu.Lock()
	defer s.lockMu.Unlock()

	cfg := s.Config()
	tmpStore, err := NewServer(&cfg)
	if err != nil {
		return n, err
	}
	err = json.Unmarshal(data, &tmpStore)
	if err != nil {
		return n, err
	}

	s.clear()

	for key, kvInfo := range tmpStore.store {
		s.store[key] = kvInfo
	}
	s.numKeys.Store(tmpStore.numKeys.Load())
	s.totalSize.Store(tmpStore.totalSize.Load())
	n = len(s.store)
	return n, err
}

func (s *KeyValueStore) requestLogger(
	ctx context.Context,
) *slog.Logger {
	if clientInfo := s.ClientInfo(ctx); clientInfo != nil {
		return s.logger.With("client", clientInfo)
	}
	return s.logger
}

func (s *KeyValueStore) GetRevision(
	ctx context.Context,
	in *pb.GetRevisionRequest,
) (
	*pb.RevisionResponse,
	error,
) {
	logger := s.requestLogger(ctx)
	logger.Info(
		"getting revision",
		slog.String("key", in.Key),
		slog.Uint64("version", in.Version),
	)
	if in.Version == 0 {
		return nil, GOKVError{
			Message: "revision must be greater than 0",
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

	if kv.Version == in.Version {
		rv := &pb.RevisionResponse{
			Value: kv.Value,
		}
		if kv.Updated.IsZero() {
			rv.Timestamp = timestamppb.New(kv.Created)
		} else {
			rv.Timestamp = timestamppb.New(kv.Updated)
		}
		return rv, nil
	}

	if kv.History == nil {
		return nil, ErrRevisionNotFound
	}
	h, found := kv.History.Version(in.Version)
	if !found {
		return nil, ErrRevisionNotFound
	}
	return &pb.RevisionResponse{
		Value:     h.Value,
		Timestamp: timestamppb.New(h.Timestamp),
	}, nil
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

	state := KeyValueStoreState{
		Keys:    keys,
		Clients: clients,
		Locks:   locks,
		Version: build.Version,
	}

	return json.Marshal(state)
}

// UnmarshalJSON reads the provided data and populates the server
// with the key-value data, client IDs and locks. This will
// overwrite any existing data.
func (s *KeyValueStore) UnmarshalJSON(data []byte) error {
	s.resetStats()

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

	var hashValue bool
	if s.cfg.HashAlgorithm > 0 {
		hashValue = true
	}

	// channel to send KeyValueInfo for value hashing
	hashChannel := make(chan *KeyValueInfo)
	// receive KeyValueInfo when hashing is complete
	doneChannel := make(chan *KeyValueInfo)
	wg := sync.WaitGroup{}

	hashAlgorithm := s.cfg.HashAlgorithm
	hashFunc := hashAlgorithm.HashFunc()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	workers := runtime.GOMAXPROCS(0)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for kvInfo := range hashChannel {
				if ctx.Err() != nil {
					return
				}

				kvInfo.mu.Lock()
				kvInfo.Hash = ""
				kvInfo.Size = uint64(len(kvInfo.Value))

				s.numKeys.Add(1)
				s.totalSize.Add(kvInfo.Size)

				if hashValue {
					setHash(kvInfo, hashFunc)
				}

				if kvInfo.Created.IsZero() {
					kvInfo.Created = time.Now().UTC()
				}
				now := time.Now()
				if kvInfo.Lifespan != nil && *kvInfo.Lifespan > 0 && kvInfo.LifespanSet != nil {
					// Set an expiration timer as needed
					expectedExpiry := kvInfo.LifespanSet.Add(*kvInfo.Lifespan)
					newExpiry := expectedExpiry.Sub(now)
					if newExpiry > 0 {
						s.logger.Warn(
							"setting new expiration",
							"new_exp",
							newExpiry,
						)
						kvInfo.expirationTimer = time.AfterFunc(
							newExpiry,
							expireFunc(s, kvInfo),
						)
					} else {
						s.logger.Warn(
							"key was loaded with an expiration time in the past",
							slog.String("key", kvInfo.Key),
							slog.Time(
								"expiration_time",
								kvInfo.LifespanSet.Add(*kvInfo.Lifespan),
							),
						)
						return
					}
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
			hashChannel <- state.Keys[i]
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
		}
	}

	if s.clientInfo == nil {
		s.clientInfo = make(map[string]*ClientInfo)
	}
	for _, clientInfo := range state.Clients {
		s.clientInfo[clientInfo.ClientID] = clientInfo
		s.logger.Info("loaded client", "client", clientInfo)
	}

	if state.Locks != nil {
		now := time.Now().UTC()
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
			s.locks[keyLock.Key] = keyLock
			keyLock.t = time.AfterFunc(keyLock.Duration, keyLock.UnlockFunc())
		}
	}
	return nil
}

func (s *KeyValueStore) resetStats() {
	s.numNewKeysSet.Store(0)
	s.numKeysUpdated.Store(0)
	s.numKeysLocked.Store(0)
	s.numKeysUnlocked.Store(0)
	s.numKeysExpired.Store(0)
	s.numKeysDeleted.Store(0)
	s.numKeysExpunged.Store(0)
}

func (s *KeyValueStore) GetStats() *pb.ServerMetrics {
	s.mu.RLock()
	s.lockMu.RLock()
	defer s.lockMu.RUnlock()
	defer s.mu.RUnlock()
	var numKeys = s.numKeys.Load()
	var totalSize = s.totalSize.Load()
	var numNewKeysSet = s.numNewKeysSet.Load()
	var numKeysUpdated = s.numKeysUpdated.Load()
	var deleted = s.numKeysDeleted.Load()
	var numKeysLocked = s.numKeysLocked.Load()
	var numKeysUnlocked = s.numKeysUnlocked.Load()
	var numKeysExpired = s.numKeysExpired.Load()
	var numKeysExpunged = s.numKeysExpunged.Load()
	var clientsSeen = s.clientIDs.Load()
	currentLocks := uint64(len(s.locks))

	return &pb.ServerMetrics{
		Keys:         &numKeys,
		TotalSize:    &totalSize,
		New:          &numNewKeysSet,
		Updated:      &numKeysUpdated,
		Deleted:      &deleted,
		Locked:       &numKeysLocked,
		Unlocked:     &numKeysUnlocked,
		Expired:      &numKeysExpired,
		Expunged:     &numKeysExpunged,
		CurrentLocks: &currentLocks,
		ClientIds:    &clientsSeen,
	}
}

// Stats returns server statistics
func (s *KeyValueStore) Stats(
	ctx context.Context,
	_ *pb.EmptyRequest,
) (*pb.ServerMetrics, error) {
	return s.GetStats(), nil
}

func (s *KeyValueStore) Unlock(
	ctx context.Context,
	in *pb.UnlockRequest,
) (*pb.UnlockResponse, error) {
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
	defer s.cfgMu.RUnlock()
	if s.cfg.Readonly {
		return nil, ErrReadOnlyServer
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.started {
		return nil, ErrServerPendingInit
	}

	kvInfo, ok := s.store[in.Key]
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

	kvInfo.mu.Lock()
	defer kvInfo.mu.Unlock()
	s.emit(in.Key, Unlocked, clientID)
	if kvInfo.expired {
		s.deleteKey(kvInfo.Key, clientID)
	}
	return &pb.UnlockResponse{Success: true}, nil
}

func (s *KeyValueStore) clear() {
	for key, _ := range s.store {
		s.deleteKey(key, "")
	}
	s.numKeys.Store(0)
	s.totalSize.Store(0)
	s.resetStats()
}

func (s *KeyValueStore) ClearHistory(
	ctx context.Context,
	_ *pb.EmptyRequest,
) (*pb.ClearHistoryResponse, error) {
	logger := s.requestLogger(ctx)

	s.cfgMu.RLock()
	defer s.cfgMu.RUnlock()
	if s.cfg.Readonly {
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
		data.Cleared = kvInfo.History.Clear()
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
	defer s.cfgMu.RUnlock()
	if s.cfg.Readonly {
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

func (s *KeyValueStore) Lock(
	ctx context.Context,
	in *pb.LockRequest,
) (*pb.LockResponse, error) {
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
	defer s.cfgMu.RUnlock()
	if s.cfg.Readonly {
		return nil, ErrReadOnlyServer
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
	defer s.mu.Unlock()

	s.lockMu.Lock()
	defer s.lockMu.Unlock()

	if !s.started {
		return nil, ErrServerPendingInit
	}

	var kvInfo *KeyValueInfo

	kvInfo, ok := s.store[in.Key]

	// Already exists - if the client owns the lock, then replace the
	// current lock with a new one
	if ok {
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
	if in.CreateIfMissing && s.cfg.MaxKeySize > 0 && uint64(len(in.Key)) > s.cfg.MaxKeySize {
		return nil, ErrKeyTooLong
	}

	maxKeys := int(s.cfg.MaxNumberOfKeys)
	if in.CreateIfMissing && maxKeys > 0 {
		currentCount := int(s.numKeys.Load())

		if currentCount >= maxKeys {
			s.logger.Warn(
				"max number of keys reached",
				slog.Group(
					"key_count",
					"current", currentCount,
					"max", s.cfg.MaxNumberOfKeys,
				),
			)
			if s.cfg.EagerPrune {
				overLimit := currentCount - maxKeys
				s.logger.Info("pruning keys", "over_limit", overLimit)
				pruned := s.pruneNumKeys(ctx, overLimit+1, true, in.Key)
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
			s.logger.Info(
				"key counts",
				"current",
				currentCount,
				"max",
				s.cfg.MaxNumberOfKeys,
			)
		}
	}
	if !in.CreateIfMissing {
		return nil, ErrKeyNotFound
	}

	d := in.Duration.AsDuration()
	kvInfo = NewKeyValue(
		s,
		in.Key,
		nil,
		"",
		nil,
		clientID,
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

// Set sets the value for the given key. If the key doesn't exist,
// it will be created. If it does, the value will be updated.
func (s *KeyValueStore) Set(ctx context.Context, in *pb.KeyValue) (
	*pb.SetResponse,
	error,
) {
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
	defer s.cfgMu.RUnlock()
	if s.cfg.Readonly {
		return nil, ErrReadOnlyServer
	}

	if s.cfg.MaxKeySize > 0 && uint64(len(in.Key)) > s.cfg.MaxKeySize {
		slog.Info(
			"key too long",
			slog.String("key", in.Key),
			slog.Uint64("max", s.cfg.MaxKeySize),
		)
		return nil, ErrKeyTooLong
	}

	size := uint64(len(in.Value))
	if s.cfg.MaxValueSize > 0 && size > s.cfg.MaxValueSize {
		return nil, ErrValueTooLarge
	}

	now := time.Now().UTC()

	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.started {
		return nil, ErrServerPendingInit
	}

	var lockDuration *time.Duration
	var expireAfter *time.Duration

	// Validate any explicitly provided lock Duration
	if in.LockDuration != nil {
		d := in.LockDuration.AsDuration()
		if d > s.cfg.MaxLockDuration {
			return nil, ErrLockDurationTooLong
		}
		if d == 0 || d < s.cfg.MinLockDuration {
			return nil, ErrInvalidLockDuration
		}
		lockDuration = &d
	}
	if in.Lifespan != nil {
		e := in.Lifespan.AsDuration()
		if e == 0 || e < s.cfg.MinLifespan {
			return nil, GOKVError{
				Message: "invalid expire_after",
				Code:    codes.InvalidArgument,
			}
		}
		expireAfter = &e
	}

	var kvInfo *KeyValueInfo
	kvInfo, exists := s.store[in.Key]

	clientID := s.ClientID(ctx)

	// Update an existing key
	if exists {
		kvInfo.mu.Lock()
		defer kvInfo.mu.Unlock()
		s.lockMu.Lock()
		defer s.lockMu.Unlock()
		keyLock, alreadyLocked := s.locks[in.Key]

		if alreadyLocked && clientID != keyLock.ClientID {
			return nil, ErrWrongUnlockToken
		}

		var hashValue bool
		if s.cfg.HashAlgorithm > 0 {
			hashValue = true
		}
		hashChannel := make(chan string, 1)

		if hashValue {
			go func() {
				hashFunc := s.cfg.HashAlgorithm.HashFunc()
				hash := hashFunc.New().Sum(in.Value)
				hashChannel <- hex.EncodeToString(hash[:])
			}()
		} else {
			hashChannel <- ""
		}

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

		if expireAfter != nil {
			if kvInfo.expirationTimer != nil {
				_ = kvInfo.expirationTimer.Stop()
				kvInfo.expirationTimer = nil
			}
			kvInfo.expired = false
			kvInfo.Lifespan = expireAfter
			kvInfo.LifespanSet = &now
			kvInfo.expirationTimer = time.AfterFunc(
				*expireAfter,
				expireFunc(s, kvInfo),
			)
		}

		// Check back in on our hash function, and only consider it
		// an updated value if the hash has changed, or there's no
		// hash function set
		newHash := <-hashChannel
		if newHash == "" || newHash != kvInfo.Hash {
			s.logger.Info(
				"updated value",
				kvLogKey, kvInfo,
			)
			s.emit(in.Key, Updated, clientID)
			if s.cfg.RevisionLimit > 0 {
				if kvInfo.History == nil {
					kvInfo.History = NewKVHistory(s.cfg.RevisionLimit)
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
	currentCt := len(s.store)
	maxKeys := int(s.cfg.MaxNumberOfKeys)
	if maxKeys > 0 && currentCt >= maxKeys {
		overLimit := currentCt - maxKeys
		s.logger.Warn(
			"too many keys",
			"current", currentCt,
			"max", maxKeys,
			"over_limit", overLimit,
		)
		if s.cfg.EagerPrune {
			pruned := s.pruneNumKeys(ctx, overLimit+1, true, in.Key)
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
			s.cfg.MaxNumberOfKeys,
		)
	}

	kvInfo = NewKeyValue(
		s,
		in.Key,
		in.Value,
		in.ContentType,
		expireAfter,
		clientID,
	)

	kvInfo.mu.RLock()
	defer kvInfo.mu.RUnlock()
	s.addKeyValueInfo(kvInfo, size)

	if in.LockDuration != nil {
		s.lockMu.Lock()
		s.locks[in.Key] = NewKeyLock(s, *lockDuration, in.Key, clientID)
		s.lockMu.Unlock()
	}

	logger.Info(
		"created key",
		kvLogKey, kvInfo,
	)
	return &pb.SetResponse{
		Success: true,
		IsNew:   true,
	}, nil
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
	logger := s.requestLogger(ctx)

	logger.Debug(
		"checking if key exists",
		slog.String("key", in.Key),
	)

	s.mu.RLock()
	defer s.mu.RUnlock()

	kvInfo := s.getKey(in.Key)
	if kvInfo == nil {
		return &pb.ExistsResponse{Exists: false}, nil
	}

	return &pb.ExistsResponse{Exists: true}, nil
}

func (s *KeyValueStore) staleKeys(
	ctx context.Context,
	includeLocked bool,
	includeReserved bool,
	ignoreKey ...string,
) []*KeyValueInfo {
	targetKeys := make([]keyInfoWithLock, 0, len(s.store))

	defer func() {
		for _, kv := range targetKeys {
			kv.kv.mu.RUnlock()
		}
	}()

	for _, kvInfo := range s.store {
		if ctx.Err() != nil {
			break
		}
		kvInfo.mu.RLock()
		keyLock := s.locks[kvInfo.Key]
		if !includeLocked && keyLock != nil {
			continue
		}
		if !includeReserved && strings.HasPrefix(kvInfo.Key, reservedPrefix) {
			continue
		}
		if sliceContains(ignoreKey, kvInfo.Key) {
			continue
		}
		targetKeys = append(
			targetKeys,
			keyInfoWithLock{kv: kvInfo, lock: keyLock},
		)
	}
	sortKeyValueInfoByDates(targetKeys)
	result := make([]*KeyValueInfo, 0, len(targetKeys))
	for _, kv := range targetKeys {
		result = append(result, kv.kv)
	}
	return result

}

func sliceContains[V comparable](slice []V, value V) bool {
	for _, v := range slice {
		if v == value {
			return true
		}
	}
	return false
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
	started := time.Now().UTC()
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
	defer func() {
		for _, kv := range kvLocked {
			kv.mu.Unlock()
		}
	}()
	for _, kvInfo := range toRemove {
		if ctx.Err() != nil {
			return removed
		}
		kvInfo.mu.Lock()
		kvLocked = append(kvLocked, kvInfo)
	}

	s.logger.Debug(
		"pending removal",
		"pending_ct",
		len(toRemove),
	)

	for _, tr := range toRemove {
		s.deleteKey(tr.Key, "")
		removed = append(removed, tr.Key)
		s.emit(tr.Key, Expunged, "")
	}

	s.numKeys.Store(uint64(len(s.store)))
	s.addPruneLog(PruneLog{Time: started, KeysPruned: removed, Eager: eager})
	return removed
}

// pruneKeys deletes all unlocked keys past the configured threshold, to
// the target lower threshold, if possible
func (s *KeyValueStore) pruneKeys(ctx context.Context) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.lockMu.Lock()
	defer s.lockMu.Unlock()

	s.logger.Info("starting prune")

	reportedCount := s.numKeys.Load()
	actualCount := uint64(len(s.store))
	started := time.Now()
	if reportedCount != actualCount {
		s.logger.Warn(
			"Key count mismatch",
			slog.Group(
				"key_count",
				"reported_count", reportedCount,
				"actual_count", actualCount,
			),
		)
		s.numKeys.Store(actualCount)
	}

	reportedCount = actualCount
	maxKeys := s.cfg.MaxNumberOfKeys
	threshold := s.cfg.PruneThreshold
	thresholdCount := uint64(float64(maxKeys) * threshold)

	if actualCount < thresholdCount {
		s.logger.Info("actual count below threshold, skipping")
		s.addPruneLog(PruneLog{Time: started, Eager: false})
		return nil, nil
	}

	targetCount := uint64(float64(maxKeys) * s.cfg.PruneTarget)

	pruned := s.pruneNumKeys(ctx, int(targetCount), false)
	if len(pruned) == 0 {
		s.addPruneLog(PruneLog{Time: started, Eager: false})
	}
	return pruned, nil

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

// Pop returns a Key-value pair and deletes the key
func (s *KeyValueStore) Pop(ctx context.Context, in *pb.PopRequest) (
	*pb.GetResponse,
	error,
) {
	clientID := s.ClientID(ctx)
	logger := s.requestLogger(ctx)
	logger.Info(
		"request to pop key",
		slog.String("key", in.Key),
	)
	s.cfgMu.RLock()
	defer s.cfgMu.RUnlock()
	if s.cfg.Readonly {
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
	kvInfo.mu.Lock()
	defer kvInfo.mu.Unlock()
	s.lockMu.RLock()
	keyLock := s.locks[in.Key]
	s.lockMu.RUnlock()
	if keyLock != nil && clientID != keyLock.ClientID {
		logger.Info(
			"cannot pop locked key", kvLogKey, kvInfo,
		)
		return nil, ErrLocked
	}

	getResponse := &pb.GetResponse{Value: kvInfo.Value}
	s.deleteKey(in.Key, clientID)
	return getResponse, nil
}

// Get returns the value of a key
func (s *KeyValueStore) Get(ctx context.Context, in *pb.Key) (
	*pb.GetResponse,
	error,
) {
	logger := s.requestLogger(ctx)
	logger.Info(
		"getting key",
		slog.String("key", in.Key),
	)
	clientID := s.ClientID(ctx)
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.started {
		return nil, ErrServerPendingInit
	}

	kvInfo := s.getKey(in.Key)
	if kvInfo == nil {
		return nil, ErrKeyNotFound
	}
	s.lockMu.RLock()
	defer s.lockMu.RUnlock()
	keyLock, locked := s.locks[in.Key]
	if locked && clientID != keyLock.ClientID {
		logger.Info(
			"cannot get key locked by another client", kvLogKey, kvInfo,
		)
		return nil, ErrLocked
	}

	kvInfo.mu.RLock()
	defer kvInfo.mu.RUnlock()
	return &pb.GetResponse{Value: kvInfo.Value}, nil
}

// Delete deletes a key from the store
func (s *KeyValueStore) Delete(ctx context.Context, in *pb.DeleteRequest) (
	*pb.DeleteResponse,
	error,
) {
	clientID := s.ClientID(ctx)
	logger := s.requestLogger(ctx)
	logger.Info(
		"request to delete key",
		slog.String("key", in.Key),
	)
	s.cfgMu.RLock()
	defer s.cfgMu.RUnlock()
	if s.cfg.Readonly {
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
	kvInfo.mu.Lock()
	defer kvInfo.mu.Unlock()
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	keyLock := s.locks[in.Key]
	if keyLock != nil && clientID != keyLock.ClientID {
		logger.Info(
			"cannot delete locked key", kvLogKey, kvInfo,
		)
		return &pb.DeleteResponse{Deleted: false}, ErrWrongUnlockToken
	}
	s.deleteKey(in.Key, clientID)
	return &pb.DeleteResponse{Deleted: true}, nil
}

func (s *KeyValueStore) deleteKey(key string, clientID string) {
	kvInfo, ok := s.store[key]
	if !ok {
		return
	}
	if kvInfo.expirationTimer != nil {
		s.logger.Debug("stopping expiration timer", "key", key)
		_ = kvInfo.expirationTimer.Stop()
		kvInfo.expirationTimer = nil
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
	s.emit(key, Deleted, clientID)
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

// GetKeyInfo returns information about a key
func (s *KeyValueStore) GetKeyInfo(
	ctx context.Context,
	in *pb.Key,
) (*pb.GetKeyValueInfoResponse, error) {
	logger := s.requestLogger(ctx)

	logger.Info(
		"getting key info",
		slog.String("key", in.Key),
	)

	s.mu.RLock()
	defer s.mu.RUnlock()

	kvInfo := s.getKey(in.Key)
	if kvInfo == nil {
		return nil, ErrKeyNotFound
	}
	kvInfo.mu.RLock()
	defer kvInfo.mu.RUnlock()

	s.lockMu.RLock()
	_, locked := s.locks[in.Key]
	s.lockMu.RUnlock()
	resp := &pb.GetKeyValueInfoResponse{
		Key:         kvInfo.Key,
		Hash:        kvInfo.Hash,
		Version:     kvInfo.Version,
		Size:        kvInfo.Size,
		ContentType: kvInfo.ContentType,
		Expired:     kvInfo.expired,
		Locked:      locked,
	}
	if !kvInfo.Created.IsZero() {
		resp.Created = timestamppb.New(kvInfo.Created)
	}
	if !kvInfo.Updated.IsZero() {
		resp.Updated = timestamppb.New(kvInfo.Updated)
	}
	return resp, nil
}

// KeyValueSnapshot is a snapshot of a key-value pair and associated info
type KeyValueSnapshot struct {
	Timestamp   time.Time `json:"timestamp"`
	Key         string    `json:"key"`
	Value       []byte    `json:"value"`
	ContentType string    `json:"content_type,omitempty"`
	Size        uint64    `json:"size"`
	Hash        string    `json:"hash,omitempty"`
	Version     uint64    `json:"version"`
}

// KeyValueInfo is the internal representation of a key-value pair and
// associated metadata.
type KeyValueInfo struct {
	Key             string     `json:"key"`
	Value           []byte     `json:"value"`
	ContentType     string     `json:"content_type,omitempty"`
	Size            uint64     `json:"-"`
	Hash            string     `json:"hash,omitempty"`
	Created         time.Time  `json:"created,omitempty"`
	Updated         time.Time  `json:"updated,omitempty"`
	Version         uint64     `json:"version,omitempty"`
	History         *KVHistory `json:"-"`
	expired         bool
	expirationTimer *time.Timer
	mu              sync.RWMutex
	Lifespan        *time.Duration `json:"lifespan,omitempty"`
	LifespanSet     *time.Time     `json:"lifespan_set,omitempty"`
	CreatedBy       string         `json:"created_by"` // Client ID of the client that created the key
}

func NewKeyValue(
	srv *KeyValueStore,
	key string,
	value []byte,
	contentType string,
	expireAfter *time.Duration,
	clientID string,
) *KeyValueInfo {
	var hashAlgorithm crypto.Hash
	var hashFunc crypto.Hash
	var revisionLimit int64

	if srv != nil {
		hashAlgorithm = srv.cfg.HashAlgorithm
		revisionLimit = srv.cfg.RevisionLimit
	}

	now := time.Now()
	var hashValue bool
	if hashAlgorithm > 0 {
		hashValue = true
		hashFunc = hashAlgorithm.HashFunc()
	}

	hashChannel := make(chan string, 1)

	if hashValue {
		go func() {
			hash := hashFunc.New().Sum(value)
			hashChannel <- hex.EncodeToString(hash[:])
		}()
	} else {
		hashChannel <- ""
	}

	kvInfo := &KeyValueInfo{
		Key:       key,
		Value:     value,
		Created:   now,
		Size:      uint64(len(value)),
		Lifespan:  expireAfter,
		Version:   1,
		CreatedBy: clientID,
	}

	if expireAfter != nil {
		if srv != nil {
			kvInfo.LifespanSet = &now
			kvInfo.expirationTimer = time.AfterFunc(
				*expireAfter,
				expireFunc(srv, kvInfo),
			)
		}
	}
	kvInfo.History = NewKVHistory(revisionLimit)

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
	attrs = append(attrs, slog.String("hash", kv.Hash))
	if !kv.Created.IsZero() {
		attrs = append(attrs, slog.Time("created", kv.Created))
	}
	if !kv.Updated.IsZero() {
		attrs = append(attrs, slog.Time("updated", kv.Updated))
	}
	attrs = append(attrs, slog.Bool("expired", kv.expired))
	return slog.GroupValue(attrs...)
}

// expireFunc returns a function to be used with time.AfterFunc to
// set a key as expired after the timer has passed. If the key is
// locked, it won't be deleted until unlocked.
func expireFunc(s *KeyValueStore, kvInfo *KeyValueInfo) func() {
	return func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.stopped {
			s.logger.Debug("server stopped, skipping expire func")
			return
		}
		kvInfo.mu.Lock()
		defer kvInfo.mu.Unlock()

		if kvInfo.Lifespan == nil || kvInfo.expirationTimer == nil {
			s.logger.Info(
				"expiration removed waiting for lock, skipping expire",
				kvLogKey,
				kvInfo,
			)
			return
		}
		kvInfo.expirationTimer = nil

		kvInfo.expired = true
		kvInfo.LifespanSet = nil
		s.lockMu.RLock()
		_, isLocked := s.locks[kvInfo.Key]
		s.lockMu.RUnlock()
		if isLocked {
			if s != nil {
				s.logger.Info(
					"Key is locked, skipping delete",
					kvLogKey, kvInfo,
				)
			}
			return
		}
		if s == nil {
			return
		}
		s.emit(kvInfo.Key, Expired, "")

		s.logger.Info(
			"Key is not locked, deleting",
			kvLogKey, kvInfo,
		)
		currentKV, ok := s.store[kvInfo.Key]
		if ok && currentKV == kvInfo {
			s.logger.Info(
				"deleting expired key on trigger",
				kvLogKey, kvInfo,
			)
			if currentKV == kvInfo {
				s.deleteKey(kvInfo.Key, "")
			}
		}

	}
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
			grpcServer.GracefulStop()

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

func setHash(kv *KeyValueInfo, h crypto.Hash) {
	hash := h.New().Sum(kv.Value)
	kv.Hash = hex.EncodeToString(hash[:])
	if kv.ContentType == "" && kv.Value != nil {
		kv.ContentType = http.DetectContentType(kv.Value)
	}
}

type KeyValueStoreState struct {
	Keys    []*KeyValueInfo `json:"keys"`
	Clients []*ClientInfo   `json:"clients"`
	Locks   []*KeyLock      `json:"locks"`
	Version string          `json:"version"`
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
	k := &KeyLock{
		Key:      key,
		ClientID: clientID,
		Duration: d,
		srv:      srv,
		Created:  time.Now(),
	}
	t := time.AfterFunc(d, k.UnlockFunc())
	k.t = t
	k.srv.emit(key, Locked, clientID)
	return k
}

// UnlockFunc returns a function to be used with time.AfterFunc to
// unlock a key after the timer has passed. If the key is expired, it
// will be deleted upon unlocking.
func (k *KeyLock) UnlockFunc() func() {
	return func() {
		k.srv.lockMu.Lock()
		defer k.srv.lockMu.Unlock()

		k.srv.mu.Lock()
		defer k.srv.mu.Unlock()

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
		kvInfo, exists := k.srv.store[k.Key]
		if exists {
			kvInfo.mu.Lock()
			defer kvInfo.mu.Unlock()
			if kvInfo.expired {
				logger.Debug(
					"deleting expired key on trigger",
				)
				if kvInfo.expirationTimer != nil {
					_ = kvInfo.expirationTimer.Stop()
				}
				kvInfo.expirationTimer = nil
				delete(k.srv.store, kvInfo.Key)
				k.srv.emit(kvInfo.Key, Deleted, "")
				k.srv.totalSize.Add(^(kvInfo.Size - 1))
				k.srv.numKeys.Add(^uint64(0))
			}
		}
	}
}

func (k *KeyLock) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("key", k.Key),
		slog.String(clientIDKey, k.ClientID),
		slog.Duration("duration", k.Duration),
	)
}

type KVHistory struct {
	snapshots []*KeyValueSnapshot
	maxLen    int64
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

func (h *KVHistory) Version(v uint64) (*KeyValueSnapshot, bool) {
	for i := 0; i < len(h.snapshots); i++ {
		s := h.snapshots[i]
		if s.Version == v {
			return s, true
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

type eventWorker struct {
	name string
	// out is the channel the worker will re-broadcast events to
	out chan Event
	// in is the channel the worker will receive events from, which are
	// then re-broadcast to the out channel
	in chan Event
	// done is the channel the worker will receive a signal on when it
	// should stop broadcasting events on out
	done chan struct{}
	// stopped is the channel the worker will send a signal on when its
	// in channel has been closed or a signal has been received on done
	stopped chan struct{}
	// sendTimeout is the amount of time to wait for an event to be sent, so
	// messages on out don't block forever
	sendTimeout time.Duration
	logger      *slog.Logger
	// published is the number of events published by this worker
	published atomic.Uint64
	// timeouts is the number of events that timed out when sending
	timeouts atomic.Uint64
	running  bool
	mu       sync.RWMutex
	once     sync.Once
}

func (w *eventWorker) Run() {
	w.once.Do(w.run)
}

func (w *eventWorker) run() {
	w.logger.Info("starting subscriber worker")
	w.mu.Lock()
	if w.running {
		panic(fmt.Sprintf("worker already running %s", w.name))
	}
	ctx, cancel := context.WithCancel(context.Background())

	w.running = true
	w.mu.Unlock()

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-w.done
		cancel()
	}()

	for event := range w.in {
		if ctx.Err() != nil {
			break
		}
		w.logger.Debug("saw event", "event", event)
		//w.mu.Lock()
		t := time.NewTimer(w.sendTimeout)
		select {
		case w.out <- event:
			// sent
			w.logger.Debug("worker sent event", "event", event)
			w.published.Add(1)
		case <-t.C:
			// timeout
			w.timeouts.Add(1)
			w.logger.Warn("event send timeout", "event", event)
		}
		//w.mu.Unlock()
	}
	wg.Wait()

	close(w.out)
	w.stopped <- struct{}{}
	close(w.stopped)
	w.logger.Info(
		"subscriber metrics",
		"published", w.published.Load(),
		"timeouts", w.timeouts.Load(),
	)
}

type eventStream struct {
	// SendTimeout is the amount of time to wait for an event to be sent from
	// a worker to a subscriber, to avoid blocking.
	SendTimeout time.Duration
	// events is a channel receiving events from the main event channel
	events chan Event
	// workers maps the name provided in Subscribe, to a worker goroutine
	// responsible for forwarding events to the subscriber
	workers         map[string]*eventWorker
	mu              sync.RWMutex
	logger          *slog.Logger
	running         bool
	subscriberCount atomic.Uint64
}

func (e *eventStream) Unsubscribe(name string) error {
	e.logger.Info("attempting to unsubscribe", "subscriber", name)
	e.mu.Lock()
	defer e.mu.Unlock()
	w, exists := e.workers[name]
	if !exists {
		return fmt.Errorf("subscriber '%s' does not exist", name)
	}

	delete(e.workers, name)
	e.logger.Info("stopping worker", "subscriber", name)

	w.done <- struct{}{}
	w.mu.Lock()
	close(w.in)

	e.subscriberCount.Store(uint64(len(e.workers)))
	e.logger.Info(
		"removed subscriber",
		"subscriber",
		name,
		"subscriber_count",
		e.subscriberCount.Load(),
	)
	e.logger.Info("waiting for stop signal")
	<-w.stopped
	w.mu.Unlock()
	e.logger.Info("got stop signal")
	return nil
}

func (e *eventStream) Publish(event Event) {
	e.mu.RLock()
	running := e.running
	e.mu.RUnlock()
	if running {
		e.events <- event
	}
}

func (e *eventStream) Subscribe(name string) (chan Event, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.workers == nil {
		e.workers = make(map[string]*eventWorker)
	}
	_, exists := e.workers[name]
	if exists {
		return nil, fmt.Errorf("subscriber '%s' already exists", name)
	}

	w := &eventWorker{
		name:        name,
		out:         make(chan Event, DefaultEventStreamBufferSize),
		in:          make(chan Event, DefaultEventStreamBufferSize),
		sendTimeout: e.SendTimeout,
		done:        make(chan struct{}, 1),
		stopped:     make(chan struct{}, 1),
		logger: e.logger.With(
			"subscriber",
			name,
			"logger",
			"event_worker",
		),
	}

	e.subscriberCount.Add(1)
	e.logger.Info(
		"added subscriber to event stream",
		"subscriber",
		name,
		"subscriber_count",
		e.subscriberCount.Load(),
	)
	e.workers[name] = w
	go w.Run()
	return w.out, nil
}

func (e *eventStream) Stop() {
	e.logger.Info(
		"stopping event stream",
		"subscriber_count",
		e.subscriberCount.Load(),
	)
	e.running = false

	workersStopped := []string{}
	for name, w := range e.workers {
		e.logger.Debug("removing worker entry", "name", name)
		w.mu.Lock()
		delete(e.workers, w.name)
		e.logger.Debug("sending stop signal to worker", "worker", w.name)
		w.done <- struct{}{}
		close(w.in)
		<-w.stopped
		e.logger.Info("worker stopped", "name", w.name)
		workersStopped = append(workersStopped, w.name)
		w.mu.Unlock()
	}

	e.subscriberCount.Store(0)
}

func (e *eventStream) Run(ctx context.Context) {
	e.logger.Info("starting event stream")
	e.mu.Lock()
	if e.running {
		e.logger.Error("already running")
		panic("event stream already running")
	}
	e.running = true
	for _, w := range e.workers {
		w.mu.RLock()
		if !w.running {
			go w.Run()
		}
		w.mu.RUnlock()
	}
	e.mu.Unlock()

	for event := range e.events {
		if ctx.Err() != nil {
			continue
		}
		e.logger.Debug("event seen", "event", event)
		e.mu.RLock()
		wg := &sync.WaitGroup{}
		for name, w := range e.workers {
			if ctx.Err() != nil {
				break
			}

			e.logger.Debug("sending to worker", "name", name, "event", event)
			wg.Add(1)
			ww := w
			go func(worker *eventWorker) {
				defer wg.Done()
				worker.mu.RLock()
				defer worker.mu.RUnlock()
				select {
				case <-ctx.Done():
					//
					return
				case worker.in <- event:
					e.logger.Info(
						"sent to worker",
						"name",
						worker.name,
						"event",
						event,
					)
					// sent
				}
			}(ww)
		}
		wg.Wait()
		e.mu.RUnlock()
	}

}

type PruneLog struct {
	Time       time.Time `json:"time"`
	KeysPruned []string  `json:"keys_pruned"`
	Eager      bool      `json:"eager"`
}

// Pruner is used to prune keys from the store when the number of keys
// exceeds the configured threshold. Pruner does it specifically on a
// configured interval. Set Config.EagerPrune to true to prune keys
// when they hit their limit, rather than waiting for the interval.
type Pruner struct {
	srv     *KeyValueStore
	logger  *slog.Logger
	t       *time.Ticker
	started bool
}

func NewPruner(srv *KeyValueStore) *Pruner {
	p := &Pruner{
		srv: srv,
		logger: srv.logger.With(
			slog.String(loggerKey, "pruner"),
		).WithGroup("pruner"),
	}
	if srv.pruner != nil {
		if srv.pruner.t != nil {
			srv.pruner.t.Stop()
		}
	}
	srv.pruner = p
	return p
}

func (p *Pruner) Run(ctx context.Context) error {
	p.logger = p.logger.With(
		slog.Uint64("max_keys", p.srv.cfg.MaxNumberOfKeys),
		slog.Float64("threshold", p.srv.cfg.PruneThreshold),
		slog.Float64("target", p.srv.cfg.PruneTarget),
		slog.Duration("interval", p.srv.cfg.PruneInterval),
		slog.Bool("eager", p.srv.cfg.EagerPrune),
	)
	if p.started {
		panic(errors.New("pruner already started"))
	}
	p.logger.Info("starting pruner")
	if p.t != nil {
		p.t.Stop()
	}
	if p.srv.cfg.PruneInterval == 0 {
		p.t = &time.Ticker{}
		p.t.Stop()
	} else {
		p.t = time.NewTicker(p.srv.cfg.PruneInterval)
	}
	defer p.t.Stop()

	p.started = true
	for {
		select {
		case <-ctx.Done():
			p.t.Stop()
			return nil
		case <-p.t.C:
			p.logger.Info("pruning keys")
			p.srv.cfgMu.RLock()
			keysPruned, pruneErr := p.srv.pruneKeys(ctx)
			p.srv.cfgMu.RUnlock()
			p.logger.Debug(
				"finished pruning keys",
				slog.Any("pruned", keysPruned),
			)
			if pruneErr != nil {
				p.logger.Error(
					"error pruning keys",
					"error",
					pruneErr,
				)
			}
		}
	}
}

// Snapshotter is used to create snapshots of the current state of the
// server at a given interval.
type Snapshotter struct {
	server       *KeyValueStore
	ticker       *time.Ticker
	logger       *slog.Logger
	lastSnapshot string
	mu           sync.RWMutex
	Snapshots    []string
}

func (s *Snapshotter) Append(snapshot string) {
	cfg := s.server.cfg.Snapshot
	if cfg.Limit > 0 && len(s.Snapshots) == cfg.Limit {
		oldest := s.Snapshots[0]
		s.logger.Info("removing oldest snapshot", "snapshot", oldest)

		err := os.Remove(oldest)
		if err != nil {
			if !os.IsNotExist(err) {
				s.logger.Warn(
					"error removing oldest snapshot",
					"snapshot",
					oldest,
					"error",
					err,
				)
			}
		}

		for i := 0; i < int(cfg.Limit)-1; i++ {
			s.Snapshots[i] = s.Snapshots[i+1]
		}
		s.Snapshots[cfg.Limit-1] = snapshot
		return
	}
	s.Snapshots = append(s.Snapshots, snapshot)
	s.lastSnapshot = snapshot
}

func NewSnapshotter(
	s *KeyValueStore,
) (*Snapshotter, error) {
	config := s.cfg.Snapshot
	logger := s.logger.With(
		loggerKey,
		"snapshotter",
	).WithGroup("snapshotter").With("config", config)

	if config.Dir != "" {
		info, err := os.Stat(config.Dir)
		if os.IsNotExist(err) {
			return nil, fmt.Errorf(
				"snapshot directory '%s' does not exist: %w",
				config.Dir,
				err,
			)
		}
		if !info.IsDir() {
			return nil, fmt.Errorf(
				"snapshot directory is not a directory: %w",
				err,
			)
		}
	}

	if config.Dir != "" && config.Interval > 0 {
		if config.Encrypt {
			keyLength := len([]byte(config.SecretKey))
			if keyLength != 32 && keyLength != 16 {
				return nil, errors.New("secret key must be 16 bytes (AES-128) or 32 (AES-256) when snapshot encryption is enabled")
			}
		}
	}

	var ticker *time.Ticker
	if config.Interval == 0 {
		ticker = &time.Ticker{}
	} else {
		ticker = time.NewTicker(config.Interval)
	}

	return &Snapshotter{
		logger: logger,
		server: s,
		ticker: ticker,
	}, nil
}

func (s *Snapshotter) Logger() *slog.Logger {
	return s.logger
}

// EncryptedSnapshot creates a snapshot of the current state of the
// server, encrypts it, and writes it to the specified filename.
func (s *Snapshotter) EncryptedSnapshot() (filename string, err error) {
	if !s.server.cfg.Snapshot.Encrypt {
		return filename, fmt.Errorf("encryption is not enabled")
	}
	if s.server.cfg.Snapshot.SecretKey == "" {
		return filename, fmt.Errorf("secret key is not set")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	begin := time.Now()

	filename = fmt.Sprintf("%d.json.aes.gz", begin.UnixMilli())
	s.logger.Info(
		"saving to",
		slog.String("filename", filename),
	)
	jsonData, err := json.Marshal(s.server)
	if err != nil {
		return filename, fmt.Errorf(
			"unable to marshal to %s: %w",
			filename,
			err,
		)
	}
	encryptedData, err := encrypt(s.server.cfg.Snapshot.SecretKey, jsonData)
	if err != nil {
		return filename, fmt.Errorf("unable to encrypt %s: %w", filename, err)
	}

	tempFile, err := os.CreateTemp("", "*.json.aes.gz")
	if err != nil {
		return "", err
	}
	defer func() {
		if te := tempFile.Close(); te != nil {
			//
			//s.logger.Warn(
			//	"unable to close enc tempfile",
			//	"filename",
			//	tempFile.Name(),
			//)
		}
	}()
	tempFilename := tempFile.Name()

	zw := gzip.NewWriter(tempFile)
	zw.Name = filename
	zw.Comment = "keyquarry snapshot (encrypted)"
	zw.ModTime = begin

	_, err = zw.Write(encryptedData)
	if err = zw.Close(); err != nil {
		return "", fmt.Errorf("error closing gzip writer: %w", err)
	}

	if err != nil {
		s.logger.Error(
			"error writing snapshot",
			slog.String("error", err.Error()),
		)
		return filename, fmt.Errorf(
			"unable to write archive %s: %w",
			filename,
			err,
		)
	}
	if ze := tempFile.Close(); ze != nil {
		return "", fmt.Errorf("error closing temp file: %w", ze)
	}
	filename = filepath.Join(s.server.cfg.Snapshot.Dir, filename)
	s.logger.Info(fmt.Sprintf("renaming '%s' to '%s'", tempFilename, filename))
	err = os.Rename(tempFilename, filename)
	if err != nil {
		return tempFilename, fmt.Errorf(
			"error renaming %s to %s: %w",
			tempFilename,
			filename,
			err,
		)
	}

	end := time.Now()
	elapsed := end.Sub(begin)
	s.logger.Info(
		"snapshot complete",
		slog.String("file", filename),
		slog.Time("start_at", begin),
		slog.Time("end_at", end),
		slog.Duration("elapsed", elapsed),
	)
	s.Append(filename)

	return filename, nil
}

func (s *Snapshotter) Snapshot() (filename string, err error) {
	if s.server.cfg.Snapshot.Encrypt {
		return s.EncryptedSnapshot()
	}
	s.logger.Warn("encryption is not enabled")
	s.mu.Lock()
	defer s.mu.Unlock()

	begin := time.Now()
	filename = fmt.Sprintf("%d.json.gz", begin.UnixMilli())

	s.logger.Info(
		"snapshotting",
		slog.String("file", filename),
	)

	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	zw.Name = filename
	zw.Comment = "keyquarry snapshot"
	zw.ModTime = begin
	encoder := json.NewEncoder(zw)
	err = encoder.Encode(s.server)
	if err != nil {
		return filename, err
	}
	tempFile, err := os.CreateTemp("", "*.json.gz")
	if err != nil {
		return "", err
	}
	defer func() {
		if te := tempFile.Close(); te != nil {
			//
			//s.logger.Warn(
			//	"unable to close tempfile",
			//	"filename",
			//	tempFile.Name(),
			//)
		}
	}()
	tempFilename := tempFile.Name()
	filename = filepath.Join(s.server.cfg.Snapshot.Dir, filename)

	if ze := zw.Close(); ze != nil {
		return "", fmt.Errorf("error closing gzip writer: %w", ze)
	}

	_, err = tempFile.Write(buf.Bytes())

	if err != nil {
		return tempFilename, err
	}
	if ze := tempFile.Close(); ze != nil {
		return "", fmt.Errorf("error closing temp file: %w", ze)
	}
	err = os.Rename(tempFilename, filename)
	if err != nil {
		return tempFilename, fmt.Errorf(
			"error renaming %s to %s: %w",
			tempFilename,
			filename,
			err,
		)
	}
	end := time.Now()
	elapsed := end.Sub(begin)
	if err != nil {
		return filename, err
	}

	s.logger.Info(
		"snapshot complete",
		slog.String("file", filename),
		slog.Time("start_at", begin),
		slog.Time("end_at", end),
		slog.Duration("elapsed", elapsed),
	)
	s.Append(filename)

	return filename, nil
}

func (s *Snapshotter) Run(ctx context.Context, events <-chan Event) {
	var changeDetected bool
	wg := sync.WaitGroup{}

	cdlock := sync.Mutex{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for ev := range events {
			s.logger.Debug("snapshotter saw event", "event", ev)
			switch ev.Event {
			case Created, Updated, Deleted:
				cdlock.Lock()
				if !changeDetected {
					changeDetected = true
					s.logger.Debug("change detected", "event", ev)
				}
				cdlock.Unlock()
			}
		}
	}()

	s.server.mu.RLock()
	enabled := s.server.cfg.Snapshot.Enabled
	s.server.mu.RUnlock()
	if enabled {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var ticker *time.Ticker
			s.server.cfgMu.RLock()
			if s.ticker == nil {
				s.ticker = time.NewTicker(s.server.cfg.Snapshot.Interval)
			}
			ticker = s.ticker

			defer ticker.Stop()
			s.logger.Info("starting snapshotter")
			var tickCounter int
			interval := s.server.cfg.Snapshot.Interval
			s.server.cfgMu.RUnlock()
			for {
				select {
				case <-ctx.Done():
					s.logger.Info(
						"stopping snapshotter",
						slog.String("last_snapshot", s.lastSnapshot),
					)
					return
				case <-ticker.C:
					tickCounter++
					cdlock.Lock()
					if changeDetected {
						s.logger.Debug(
							"change detected, snapshotting",
							"tick",
							tickCounter,
						)
						s.server.mu.RLock()
						s.server.lockMu.RLock()
						s.server.cmu.RLock()
						filename, e := s.Snapshot()
						changeDetected = false
						cdlock.Unlock()
						s.server.mu.RUnlock()
						s.server.lockMu.RUnlock()
						s.server.cmu.RUnlock()
						if e != nil {
							s.logger.Error(
								"snapshot failed",
								slog.String("filename", filename),
								slog.String("error", e.Error()),
								slog.Int("tick", tickCounter),
							)
							continue
						}
						s.server.numSnapshots.Add(1)
						s.logger.Info(
							"created snapshot",
							slog.String("filename", filename),
							slog.Time(
								"next_snapshot",
								time.Now().Add(interval),
							),
							slog.Int("tick", tickCounter),
						)
					} else {
						cdlock.Unlock()
						s.logger.Info(
							"no change detected, skipping snapshot",
							"tick",
							tickCounter,
						)
					}
				}
			}
		}()
	}
	wg.Wait()
}

func encrypt(key string, data []byte) ([]byte, error) {
	block, err := aes.NewCipher([]byte(key))
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	nonceData := make([]byte, gcm.NonceSize())
	_, err = io.ReadFull(rand.Reader, nonceData)
	if err != nil {
		return nil, err
	}

	encryptedData := gcm.Seal(nil, nonceData, data, nil)
	return append(nonceData, encryptedData...), nil

}

func ReadSnapshot(filename string, secretKey string) (
	*KeyValueStoreState,
	error,
) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("snapshot file '%s' is empty", filename)
	}
	gz, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("gzip decompression failed: %w", err)
	}
	defer func() {
		_ = gz.Close()
	}()
	decompressedData, err := io.ReadAll(gz)
	if err != nil {
		return nil, fmt.Errorf("failed to read gzip data: %w", err)
	}
	data = decompressedData

	data, err = decrypt(secretKey, data)
	if err != nil {
		return nil, fmt.Errorf("decryption failed for '%s': %w", filename, err)
	}

	var kv KeyValueStoreState
	err = json.Unmarshal(data, &kv)
	if err != nil {
		return nil, err
	}
	return &kv, nil
}

func decrypt(key string, data []byte) ([]byte, error) {
	block, err := aes.NewCipher([]byte(key))
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	if len(data) < gcm.NonceSize() {
		return nil, fmt.Errorf("ciphertext too short")
	}

	nonce := data[:gcm.NonceSize()]
	ciphertext := data[gcm.NonceSize():]

	return gcm.Open(nil, nonce, ciphertext, nil)
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
