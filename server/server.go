package server

import (
	"bytes"
	"compress/gzip"
	"crypto"
	"encoding/hex"
	"encoding/json"
	"fmt"
	pb "github.com/arcward/gokv/api"
	"github.com/arcward/gokv/build"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
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
	DefaultMaxKeySize    uint64 = 1000    // 1,000 bytes
	DefaultMaxValueSize  uint64 = 1000000 // 1,000,000 bytes
	DefaultRevisionLimit int64  = 0
	DefaultMaxKeys       uint64 = 0
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
)

func (ke KeyEvent) GoString() string {
	return ke.String()
}

func (ke KeyEvent) String() string {
	switch ke {
	case Created:
		return "Created"
	case Updated:
		return "Updated"
	case Deleted:
		return "Deleted"
	case Expired:
		return "Expired"
	case Locked:
		return "Locked"
	case Unlocked:
		return "Unlocked"
	default:
		return "NoEvent"
	}
}

type Event struct {
	Key   string
	Event KeyEvent
	Time  time.Time
}

func (e Event) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("key", e.Key),
		slog.String("event", e.Event.String()),
		slog.Time("time", e.Time),
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
	//return status.Convert(e)
	return status.New(e.Code, e.Message)
}

var (
	ErrKeyTooLong = &GOKVError{
		Message: "key length greater than maximum",
		Code:    codes.FailedPrecondition,
	}

	ErrValueTooLarge = GOKVError{
		Message: "value size is greater than maximum",
		Code:    codes.FailedPrecondition,
	}
	ErrKeyNotFound = GOKVError{
		Message: "key not found",
		Code:    codes.NotFound,
	}
	ErrMaxKeysReached = GOKVError{
		Message: "maximum number of keys reached",
		Code:    codes.ResourceExhausted,
	}
	ErrInvalidKeyPattern = GOKVError{
		Message: "invalid key pattern",
		Code:    codes.InvalidArgument,
	}
	ErrAlreadyLocked = GOKVError{
		Message: "lock already exists",
		Code:    codes.FailedPrecondition,
	}
	ErrLocked = GOKVError{
		Message: "key is locked",
		Code:    codes.PermissionDenied,
	}
	ErrRevisionNotFound = GOKVError{
		Message: "revision not found",
		Code:    codes.NotFound,
	}
)

type Config struct {
	MaxNumberOfKeys uint64       `json:"max_keys" yaml:"max_keys" mapstructure:"max_keys"`
	MaxValueSize    uint64       `json:"max_value_size" yaml:"max_value_size" mapstructure:"max_value_size"`
	MaxKeySize      uint64       `json:"max_key_size" yaml:"max_key_size" mapstructure:"max_key_size"`
	KeepExpiredKeys bool         `json:"keep_expired_keys" yaml:"keep_expired_keys" mapstructure:"keep_expired_keys"`
	RevisionLimit   int64        `json:"revision_limit" yaml:"revision_limit" mapstructure:"revision_limit"`
	HashAlgorithm   crypto.Hash  `json:"hash_algorithm" yaml:"hash_algorithm" mapstructure:"hash_algorithm"`
	Logger          *slog.Logger `json:"-" yaml:"-" mapstructure:"-"`
}

//type Snapshotter interface {
//	Snapshot(filename string) error
//	Run(ctx context.Context) string
//	Stop()
//	Dir() string
//	Logger() *slog.Logger
//}

//func NewGZIPSnapshotter(dir string, interval time.Duration) Snapshotter {
//	return &Snapshotter{
//		dir:      dir,
//		interval: interval,
//		ticker:   time.NewTicker(interval),
//	}
//}

type Snapshotter struct {
	dir          string
	interval     time.Duration
	server       *KeyValueStore
	ticker       *time.Ticker
	logger       *slog.Logger
	lastSnapshot string
	mu           sync.RWMutex
	Snapshots    []string
	limit        int
}

func (s *Snapshotter) Stop() {
	//return
	//s.mu.Lock()
	//s.ticker.Stop()
	finalSnapshotFilename := fmt.Sprintf(
		"gokv-snapshot-%d.json.gz",
		time.Now().Unix(),
	)
	snapshotDir := s.Dir()
	finalSnapshot := filepath.Join(
		snapshotDir,
		finalSnapshotFilename,
	)
	//s.mu.Unlock()
	err := s.Snapshot(finalSnapshot)
	if err == nil {
		s.Logger().Info(
			"saved snapshot on shutdown",
			slog.String("file", finalSnapshot),
		)
	} else {
		s.Logger().Error(
			"failed to save final snapshot",
			slog.String("file", finalSnapshot),
			slog.String("error", err.Error()),
		)
	}
}

func (s *Snapshotter) Dir() string {
	return s.dir
}

func (s *Snapshotter) Logger() *slog.Logger {
	return s.logger
}

func (s *Snapshotter) Snapshot(filename string) error {
	filename, _ = filepath.Abs(filename)
	s.mu.Lock()
	defer s.mu.Unlock()
	begin := time.Now()
	s.logger.Info(
		"snapshotting",
		slog.String("file", filename),
	)

	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	zw.Name = filename
	zw.Comment = "gokv snapshot"
	zw.ModTime = begin
	encoder := json.NewEncoder(zw)
	err := encoder.Encode(s.server)
	if err != nil {
		return err
	}

	zw.Close()
	err = os.WriteFile(filename, buf.Bytes(), 0644)
	end := time.Now()
	elapsed := end.Sub(begin)
	if err != nil {
		return err
	}

	s.logger.Info(
		"snapshot complete",
		slog.String("file", filename),
		slog.Time("start_at", begin),
		slog.Time("end_at", end),
		slog.Duration("elapsed", elapsed),
	)
	s.lastSnapshot = filename
	snapshots := make([]string, len(s.Snapshots), len(s.Snapshots)+1)
	copy(snapshots, s.Snapshots)
	snapshots = append(snapshots, filename)

	s.logger.Info(fmt.Sprintf("current snapshots: %#v", snapshots))
	if s.limit == 0 {
		s.Snapshots = snapshots
		return nil
	}
	leftoverSnapshots := make([]string, 0, len(snapshots))

	//s.logger.Info(fmt.Sprintf("snapshots: %#v", snapshots))
	snapshotsOverLimit := 0
	if len(snapshots) > s.limit {
		snapshotsOverLimit = len(snapshots) - s.limit
	} else {
		s.Snapshots = snapshots
		return nil
	}
	oldestSnapshots := snapshots[:snapshotsOverLimit]
	remainingSnapshots := snapshots[snapshotsOverLimit:]
	s.logger.Info(fmt.Sprintf("removing %d snapshots", len(oldestSnapshots)))
	for _, snap := range oldestSnapshots {
		s.logger.Info("removing snapshot", slog.String("filename", snap))
		err = os.Remove(snap)
		if err != nil {
			if os.IsNotExist(err) {
				//
			} else {
				s.logger.Error(
					"error removing snapshot",
					slog.String("filename", snap),
					slog.String("error", err.Error()),
				)
				leftoverSnapshots = append(leftoverSnapshots, snap)
			}
			continue
		}
		leftoverSnapshots = append(leftoverSnapshots, snap)
	}
	leftoverSnapshots = append(leftoverSnapshots, remainingSnapshots...)
	s.Snapshots = leftoverSnapshots
	return nil
}

func (s *Snapshotter) Run(ctx context.Context) (lastSnapshot string) {
	ch := s.server.Subscribe()
	var changeDetected bool

	cdlock := sync.Mutex{}
	wg := &sync.WaitGroup{}
	go func() {
		wg.Add(1)
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case ev := <-ch:
				cdlock.Lock()
				s.logger.Info("event seen", "event", ev)
				switch ev.Event {
				case Created, Updated, Deleted:
					changeDetected = true
				}
				cdlock.Unlock()
			}
		}
	}()

	var ticker *time.Ticker
	if s.ticker == nil {
		s.ticker = time.NewTicker(s.interval)
	}
	ticker = s.ticker

	defer ticker.Stop()
	s.logger.Info("starting snapshotter")
	for {
		select {
		case <-ctx.Done():
			s.logger.Info(
				"stopping snapshotter",
				slog.String("last_snapshot", s.lastSnapshot),
			)
			//finalSnapshotFilename := fmt.Sprintf(
			//	"gokv-snapshot-%d.json.gz",
			//	time.Now().Unix(),
			//)
			//snapshotDir := s.Dir()
			//finalSnapshot := filepath.Join(
			//	snapshotDir,
			//	finalSnapshotFilename,
			//)
			//err := s.Snapshot(finalSnapshot)
			//if err == nil {
			//	s.Logger().Info(
			//		"saved snapshot on shutdown",
			//		slog.String("file", finalSnapshot),
			//	)
			//} else {
			//	s.Logger().Error(
			//		"failed to save final snapshot",
			//		slog.String("file", finalSnapshot),
			//		slog.String("error", err.Error()),
			//	)
			//}
			return lastSnapshot
		case <-ticker.C:
			cdlock.Lock()
			if !changeDetected {
				s.logger.Info("no change detected since last snapshot")
				cdlock.Unlock()
				continue
			}
			s.logger.Info("change detected, snapshotting")
			baseFilename := fmt.Sprintf(
				"gokv-snapshot-%d.json.gz",
				time.Now().Unix(),
			)
			filename := filepath.Join(s.dir, baseFilename)

			err := s.Snapshot(filename)
			changeDetected = false
			cdlock.Unlock()
			if err != nil {
				s.logger.Error(
					"snapshot failed",
					slog.String("filename", filename),
					slog.String("error", err.Error()),
				)
				continue
			}
			lastSnapshot = filename
			s.logger.Info("created snapshot", slog.String("filename", filename))
		}
	}
	wg.Wait()
	return lastSnapshot
}

//
//func (s *Snapshotter) LastSnapshot() string {
//	s.mu.RLock()
//	defer s.mu.RUnlock()
//	return s.lastSnapshot
//}

func (s *Snapshotter) Reset(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ticker != nil {
		s.ticker.Reset(d)
	}
	s.interval = d
}

func (c Config) LogValue() slog.Value {
	var hashAlgorithmName string
	if c.HashAlgorithm > 0 {
		hashAlgorithmName = c.HashAlgorithm.String()
	}
	return slog.GroupValue(
		slog.String("hash_algorithm", hashAlgorithmName),
		slog.Bool("keep_expired_keys", c.KeepExpiredKeys),
		slog.Int64("revision_limit", c.RevisionLimit),
		slog.Uint64("max_keys", c.MaxNumberOfKeys),
		slog.Uint64("max_value_size", c.MaxValueSize),
		slog.Uint64("max_key_size", c.MaxKeySize),
	)
}

// NewServer returns a new KeyValueStore server
func NewServer(cfg *Config) *KeyValueStore {
	if cfg == nil {
		cfg = &Config{}
	}
	if cfg.MaxValueSize == 0 {
		cfg.MaxValueSize = DefaultMaxValueSize
	}
	if cfg.MaxKeySize == 0 {
		cfg.MaxKeySize = DefaultMaxKeySize
	}
	if cfg.Logger == nil {
		logger := slog.Default().WithGroup("gokv")
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

	kvs := &KeyValueStore{
		store:  make(map[string]*KeyValueInfo),
		logger: cfg.Logger,
		cfg:    *cfg,
	}
	return kvs
}

func (s *KeyValueStore) WithEventStream(
	ctx context.Context,
) {
	ev := &eventStream{
		BufferSize:  250,
		SendTimeout: 1 * time.Second,
		events:      make(chan Event),
		logger:      s.logger.WithGroup("events"),
	}
	s.eventStream = ev
	go ev.Run(ctx)
}

func NewServerFromSnapshot(data []byte, cfg *Config) (*KeyValueStore, error) {
	contentType := http.DetectContentType(data)

	if strings.Contains(contentType, "gzip") {
		gz, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, fmt.Errorf("gzip decompression failed: %w", err)
		}
		defer gz.Close()
		decompressedData, err := io.ReadAll(gz)
		if err != nil {
			return nil, fmt.Errorf("gzip decompression failed: %w", err)
		}
		data = decompressedData
	}

	srv := NewServer(cfg)
	err := json.Unmarshal(data, srv)
	if err != nil {
		return nil, fmt.Errorf("unable to unmarshal snapshot: %w", err)
	}
	return srv, nil
}

func NewServerFromFile(filename string, cfg *Config) (*KeyValueStore, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	return NewServerFromSnapshot(data, cfg)
}

type KeyValueStore struct {
	store                 map[string]*KeyValueInfo
	logger                *slog.Logger
	cfg                   Config
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
	eventStream           *eventStream
	snapshotter           Snapshotter
	pb.UnimplementedKeyValueStoreServer
}

func (s *KeyValueStore) Subscribe() chan Event {
	return s.eventStream.Subscribe()
}

func (s *KeyValueStore) Config() Config {
	return s.cfg
}

func (s *KeyValueStore) emit(key string, event KeyEvent) {
	if s.eventStream != nil {
		s.eventStream.Publish(
			Event{
				Key:   key,
				Event: event,
				Time:  time.Now().UTC(),
			},
		)
	}
}

func NewSnapshotter(
	s *KeyValueStore,
	dir string,
	interval time.Duration,
	limit int,
) (*Snapshotter, error) {
	logger := s.logger.WithGroup("snapshot").With(
		slog.Duration("interval", interval),
		slog.String("dir", dir),
		slog.Int("limit", limit),
	)
	var err error
	if dir == "" {
		dir, err = os.Getwd()
		if err != nil {
			return nil, err
		}
	}
	info, err := os.Stat(dir)
	if os.IsNotExist(err) {
		return nil, fmt.Errorf(
			"snapshot directory '%s' does not exist: %w",
			dir,
			err,
		)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf(
			"snapshot directory is not a directory: %w",
			err,
		)
	}

	return &Snapshotter{
		logger:   logger,
		server:   s,
		dir:      dir,
		interval: interval,
		limit:    limit,
		ticker:   time.NewTicker(interval),
	}, nil
	//s.WithSnapshotter(snapshotter)
	//return nil
}

// Restore unmarshalls the provided data (the output of Dump), resets
// the current store, and populates it with the data from the dump.
// The number of keys restored is returned, along with any error.
func (s *KeyValueStore) Restore(data []byte) (n int, err error) {
	cfg := s.Config()
	tmpStore := NewServer(&cfg)
	err = json.Unmarshal(data, &tmpStore)
	if err != nil {
		return n, err
	}
	if err != nil {
		return n, fmt.Errorf(
			"unable to clear existing data prior to restore: %w",
			err,
		)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.clear()

	for key, kvInfo := range tmpStore.store {
		s.store[key] = kvInfo
	}
	s.numKeys.Store(tmpStore.numKeys.Load())
	s.totalSize.Store(tmpStore.totalSize.Load())
	n = len(s.store)
	return n, err
}

func (s *KeyValueStore) Logger() *slog.Logger {
	return s.logger
}

func (s *KeyValueStore) requestLogger(ctx context.Context) *slog.Logger {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return s.logger
	}
	return s.logger.With(
		slog.Group(
			"peer",
			slog.String("address", p.Addr.String()),
		),
	)
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
		slog.String("peer.address", p.Addr.String()),
		slog.String("key", in.Key),
		slog.Uint64("version", in.Version),
	)
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
	for _, h := range kv.History {
		if h.Version == in.Version {
			return &pb.RevisionResponse{
				Value:     h.Value,
				Timestamp: timestamppb.New(h.Timestamp),
			}, nil
		}
	}
	return nil, ErrRevisionNotFound
}

type eventStream struct {
	BufferSize  int
	SendTimeout time.Duration
	events      chan Event
	subscribers []chan Event
	mu          sync.RWMutex
	logger      *slog.Logger
	running     bool
}

func (e *eventStream) Publish(event Event) {
	if e.running {
		e.logger.Debug("publishing event", "event", event)
		e.events <- event
	} else {
		e.logger.Debug("not publishing event", "event", event)
	}

}

func (e *eventStream) Subscribe() chan Event {
	e.mu.Lock()
	defer e.mu.Unlock()
	ch := make(chan Event, e.BufferSize)
	e.subscribers = append(e.subscribers, ch)
	e.logger.Info("added subscriber to event stream")
	return ch
}

func (e *eventStream) Stop() {
	e.running = false
	e.mu.Lock()
	defer e.mu.Unlock()
	subs := e.subscribers
	e.subscribers = nil

	for _, ch := range subs {
		close(ch)
	}

	e.logger.Info("stopping event stream")
	close(e.events)
}

func (e *eventStream) Run(ctx context.Context) {
	e.logger.Info("starting event stream")
	defer e.Stop()
	e.running = true

	for {
		select {
		case <-ctx.Done():
			e.logger.Info("stopping event stream")
			return
		case event := <-e.events:
			e.mu.RLock()
			for _, ch := range e.subscribers {
				ch := ch
				go func(c chan<- Event) {
					select {
					case ch <- event:
						// sent
					case <-time.After(e.SendTimeout):
						// timeout
					}
				}(ch)
			}
			e.mu.RUnlock()
		default:
			// no events
		}

	}
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
	if s.cfg.HashAlgorithm > 0 {
		hashValue = true
	}

	hashChannel := make(chan *KeyValueInfo)
	wg := sync.WaitGroup{}
	doneChannel := make(chan *KeyValueInfo)

	hashAlgorithm := s.cfg.HashAlgorithm
	if hashAlgorithm > 0 {

	}
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
	p, ok := peer.FromContext(ctx)
	if ok {
		s.logger.Info(
			"retrieving stats",
			slog.String("peer.address", p.Addr.String()),
		)
	}
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
}

func (s *KeyValueStore) Unlock(
	ctx context.Context,
	in *pb.UnlockRequest,
) (*pb.UnlockResponse, error) {
	p, _ := peer.FromContext(ctx)

	addr := p.Addr.String()
	s.logger.Info(
		"unlock request",
		slog.String("peer.address", addr),
		slog.String("key", in.Key),
	)
	s.numUnlockRequests.Add(1)
	s.mu.RLock()
	defer s.mu.RUnlock()
	kvInfo, ok := s.store[in.Key]
	if !ok {
		return nil, ErrKeyNotFound
	}
	kvInfo.mu.Lock()
	defer kvInfo.mu.Unlock()
	if kvInfo.Locked {
		if kvInfo.unlockTimer != nil {
			_ = kvInfo.unlockTimer.Stop()
		}
		if kvInfo.expired {
			s.emit(kvInfo.Key, Expired)
			s.deleteKey(kvInfo.Key)
		}
	}
	kvInfo.Locked = false
	kvInfo.LockedAt = time.Time{}
	kvInfo.LockDuration = 0

	return &pb.UnlockResponse{Success: true}, nil
}

func (s *KeyValueStore) clear() {
	var keysDeleted int

	keys := make([]string, 0, len(s.store))
	keyValues := make([]*KeyValueInfo, 0, len(s.store))

	for key, keyInfo := range s.store {
		keys = append(keys, key)
		s.logger.Debug("deleting key", slog.String("key", keyInfo.Key))
		delete(s.store, keyInfo.Key)
		keysDeleted++
		keyValues = append(keyValues, keyInfo)
	}
	s.logger.Info("cleared all keys", slog.Int("keys_deleted", keysDeleted))

	stopTimers := make(chan *KeyValueInfo, len(keyValues))
	wg := &sync.WaitGroup{}
	workers := runtime.GOMAXPROCS(0)
	s.numKeys.Store(0)
	s.totalSize.Store(0)
	s.resetStats()

	s.logger.Info("stopping any existing expiration/unlock timers")
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for keyInfo := range stopTimers {
				keyInfo.mu.Lock()
				if keyInfo.expirationTimer != nil {
					_ = keyInfo.expirationTimer.Stop()
				}
				if keyInfo.unlockTimer != nil {
					_ = keyInfo.unlockTimer.Stop()
				}
				keyInfo.mu.Unlock()
			}
		}()
	}

	for _, keyInfo := range keyValues {
		stopTimers <- keyInfo
	}
	close(stopTimers)
	wg.Wait()
	s.logger.Info("stopped all expiration/unlock timers")

}

func (s *KeyValueStore) ClearHistory(
	ctx context.Context,
	_ *pb.EmptyRequest,
) (*pb.ClearHistoryResponse, error) {
	p, _ := peer.FromContext(ctx)

	addr := p.Addr.String()
	s.logger.Info(
		"clearing history",
		slog.String("peer.address", addr),
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
		slog.String("peer.address", p.Addr.String()),
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
				"key_value", kvInfo,
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
		slog.String("peer.address", p.Addr.String()),
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
		return nil, ErrInvalidKeyPattern
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
	logger := s.requestLogger(ctx)
	logger.Info(
		"requesting lock",
		slog.String("key", in.Key),
	)
	s.numLockRequests.Add(1)

	s.mu.RLock()
	defer s.mu.RUnlock()
	kvInfo, ok := s.store[in.Key]

	if !ok {
		return nil, ErrKeyNotFound
	}
	kvInfo.mu.Lock()
	defer kvInfo.mu.Unlock()
	if kvInfo.IsLocked() {
		return nil, ErrAlreadyLocked
	}
	var lockDuration time.Duration
	kvInfo.Locked = true
	kvInfo.LockDuration = in.Duration
	lockedAt := time.Now()
	if in.Duration > 0 {
		lockDuration = time.Duration(in.Duration) * time.Second
		if kvInfo.unlockTimer == nil {
			kvInfo.unlockTimer = time.AfterFunc(
				lockDuration,
				unlockFunc(s, kvInfo),
			)
		} else {
			_ = kvInfo.unlockTimer.Reset(lockDuration)
		}
	} else {
		if kvInfo.unlockTimer != nil {
			_ = kvInfo.unlockTimer.Stop()
			kvInfo.unlockTimer = nil
		}
	}

	logger.Info(
		"lock granted",
		slog.String("key", in.Key),
		slog.Duration("duration", lockDuration),
	)
	kvInfo.LockedAt = lockedAt
	s.emit(in.Key, Locked)
	return &pb.LockResponse{Success: true}, nil
}

// Set sets the value for the given key. If the key doesn't exist,
// it will be created. If it does, the value will be updated.
func (s *KeyValueStore) Set(ctx context.Context, in *pb.KeyValue) (
	*pb.SetResponse,
	error,
) {
	logger := s.requestLogger(ctx)
	logger.Info(
		"setting value",
		slog.Group(
			"request",
			slog.String("key", in.Key),
			slog.Bool("lock", in.Lock),
			slog.Uint64("lock_duration", uint64(in.LockDuration)),
			slog.Uint64("expire_in", uint64(in.ExpireIn)),
		),
	)

	s.numSetRequests.Add(1)
	if uint64(len(in.Key)) > s.cfg.MaxKeySize {
		return nil, ErrKeyTooLong

	}
	size := uint64(len(in.Value))
	if size > s.cfg.MaxValueSize {
		return nil, ErrValueTooLarge
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
	}
	now := time.Now().UTC()

	s.mu.Lock()
	defer s.mu.Unlock()

	kvInfo, exists := s.store[in.Key]

	if exists {
		kvInfo.mu.Lock()
		defer kvInfo.mu.Unlock()
		if kvInfo.IsLocked() {
			return nil, ErrLocked
		}

		s.numKeysUpdated.Add(1)

		if in.Lock {
			kvInfo.Locked = true
			kvInfo.LockedAt = now
			if kvInfo.unlockTimer != nil {
				_ = kvInfo.unlockTimer.Stop()
			}

			if in.LockDuration > 0 {
				kvInfo.LockDuration = in.LockDuration
				lockDuration := time.Duration(in.LockDuration) * time.Second
				if kvInfo.unlockTimer == nil {
					kvInfo.unlockTimer = time.AfterFunc(
						lockDuration, unlockFunc(s, kvInfo),
					)
				} else {
					kvInfo.unlockTimer.Reset(lockDuration)
				}
				logger.Info(
					"locking key",
					"key_value", kvInfo,
					slog.Duration("duration", lockDuration),
				)
			}
		}
		var incrementVersion bool

		if hashValue {
			newHash := <-hashChannel
			if newHash != kvInfo.Hash {
				kvInfo.Hash = newHash
				incrementVersion = true
			}
		} else {
			incrementVersion = true
		}

		if incrementVersion {
			s.emit(in.Key, Updated)
			kvInfo.addSnapshotToHistory(s.cfg.RevisionLimit)
			s.totalSize.Add(^(kvInfo.Size - 1))
			s.totalSize.Add(size)
			kvInfo.Size = size
			kvInfo.Updated = now
			kvInfo.Value = in.Value
			kvInfo.Version++
		}

		s.logger.Info(
			"updated value",
			"key_value", kvInfo,
		)
		return &pb.SetResponse{Success: true}, nil
	}

	if s.cfg.MaxNumberOfKeys > 0 && s.numKeys.Load() >= s.cfg.MaxNumberOfKeys {
		return nil, ErrMaxKeysReached
	}
	s.emit(in.Key, Created)

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
		expireDuration := time.Duration(in.ExpireIn) * time.Second
		kvInfo.expirationTimer = time.AfterFunc(
			expireDuration,
			expireFunc(s, kvInfo),
		)
		logger.Info(
			"setting expiration",
			"key_value", kvInfo,
			slog.Duration("duration", expireDuration),
		)
	}
	if in.Lock {
		kvInfo.Locked = true
		kvInfo.LockedAt = now
		if in.LockDuration > 0 {
			kvInfo.LockDuration = in.LockDuration
			lockDuration := time.Duration(in.LockDuration) * time.Second
			kvInfo.unlockTimer = time.AfterFunc(
				lockDuration,
				unlockFunc(s, kvInfo),
			)
			s.logger.Info(
				"locking key",
				"key_value", kvInfo,
				slog.Duration("duration", lockDuration),
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
	logger.Info(
		"created key",
		"key_value", kvInfo,
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
	logger := s.requestLogger(ctx)

	logger.Info(
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

// Pop returns a key-value pair and deletes the key
func (s *KeyValueStore) Pop(ctx context.Context, in *pb.Key) (
	*pb.GetResponse,
	error,
) {
	logger := s.requestLogger(ctx)
	logger.Info(
		"popping key",
		slog.String("key", in.Key),
	)
	s.numPopRequests.Add(1)

	s.mu.Lock()
	defer s.mu.Unlock()

	kvInfo := s.getKey(in.Key)
	if kvInfo == nil {
		return nil, ErrKeyNotFound
	}
	kvInfo.mu.Lock()
	defer kvInfo.mu.Unlock()
	if kvInfo.IsLocked() {
		logger.Info(
			"cannot pop locked key", "key_value", kvInfo,
		)
		return nil, ErrLocked
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
	logger := s.requestLogger(ctx)
	logger.Info(
		"getting key",
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
	now := time.Now().UTC()
	go func(t time.Time) {
		kvInfo.mu.Lock()
		defer kvInfo.mu.Unlock()
		kvInfo.Accessed = t
	}(now)
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
	logger := s.requestLogger(ctx)

	logger.Info(
		"deleting key",
		slog.String("key", in.Key),
	)
	s.mu.Lock()
	defer s.mu.Unlock()

	kvInfo := s.getKey(in.Key)
	if kvInfo == nil {
		return &pb.DeleteResponse{Deleted: false}, ErrKeyNotFound
	}
	kvInfo.mu.Lock()
	defer kvInfo.mu.Unlock()
	if kvInfo.IsLocked() {
		logger.Info(
			"cannot delete locked key", "key_value", kvInfo,
		)
		return &pb.DeleteResponse{Deleted: false}, ErrLocked
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
		"key_value", kvInfo,
	)
	delete(s.store, key)
	s.emit(key, Deleted)
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

	resp := &pb.GetKeyValueInfoResponse{
		Key:         kvInfo.Key,
		Hash:        kvInfo.Hash,
		Version:     kvInfo.Version,
		Size:        kvInfo.Size,
		ContentType: kvInfo.ContentType,
		Expired:     kvInfo.expired,
	}
	if !kvInfo.Created.IsZero() {
		resp.Created = timestamppb.New(kvInfo.Created)
	}
	if !kvInfo.Updated.IsZero() {
		resp.Updated = timestamppb.New(kvInfo.Updated)
	}
	if !kvInfo.Accessed.IsZero() {
		resp.Accessed = timestamppb.New(kvInfo.Accessed)
	}
	if kvInfo.IsLocked() {
		resp.Locked = true
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
	Key             string              `json:"key"`
	Value           []byte              `json:"value"`
	ContentType     string              `json:"content_type,omitempty"`
	Size            uint64              `json:"size"`
	Hash            string              `json:"hash,omitempty"`
	Created         time.Time           `json:"created"`
	Updated         time.Time           `json:"updated,omitempty"`
	Accessed        time.Time           `json:"accessed,omitempty"`
	ExpireAfter     uint32              `json:"expires_after,omitempty"`
	Locked          bool                `json:"locked"`
	LockDuration    uint32              `json:"unlock_after,omitempty"`
	LockedAt        time.Time           `json:"locked_at,omitempty"`
	Version         uint64              `json:"version"`
	History         []*KeyValueSnapshot `json:"-"`
	expired         bool
	expirationTimer *time.Timer
	unlockTimer     *time.Timer
	mu              sync.RWMutex
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
	if !kv.Accessed.IsZero() {
		attrs = append(attrs, slog.Time("accessed", kv.Accessed))
	}
	attrs = append(attrs, slog.Bool("locked", kv.Locked))
	attrs = append(attrs, slog.Bool("expired", kv.expired))
	return slog.GroupValue(attrs...)
}

// expireFunc returns a function to be used with time.AfterFunc to
// set a key as expired after the timer has passed. If the key is
// locked, it won't be deleted until unlocked.
func expireFunc(s *KeyValueStore, kvInfo *KeyValueInfo) func() {
	return func() {
		kvInfo.mu.Lock()
		defer kvInfo.mu.Unlock()

		kvInfo.expired = true
		if kvInfo.Locked {
			s.logger.Info(
				"key is locked, skipping delete",
				"key_value", kvInfo,
			)
			return
		}
		s.emit(kvInfo.Key, Expired)

		if !s.cfg.KeepExpiredKeys {
			s.logger.Info(
				"key is not locked, deleting",
				"key_value", kvInfo,
			)
			s.mu.Lock()
			defer s.mu.Unlock()
			currentKV, ok := s.store[kvInfo.Key]
			if ok && currentKV == kvInfo {
				s.logger.Info(
					"deleting expired key on trigger",
					"key_value", kvInfo,
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
			lockDuration := time.Duration(kvInfo.LockDuration) * time.Second
			kvInfo.unlockTimer = time.AfterFunc(
				lockDuration, unlockFunc(s, kvInfo),
			)
		}
	}
	if kvInfo.ExpireAfter > 0 {
		expiresAfter := time.Duration(kvInfo.ExpireAfter) * time.Second
		kvInfo.expirationTimer = time.AfterFunc(
			expiresAfter, expireFunc(s, kvInfo),
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
		s.emit(kvInfo.Key, Unlocked)
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
	snapshot := &KeyValueSnapshot{
		Timestamp:   time.Now(),
		Key:         kv.Key,
		Size:        uint64(size),
		Version:     kv.Version,
		ContentType: kv.ContentType,
		Hash:        kv.Hash,
		Value:       value,
	}
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
