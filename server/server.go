package server

import (
	"context"
	"encoding/json"
	"errors"
	"expvar"
	"fmt"
	pb "github.com/arcward/keyquarry/api"
	"github.com/arcward/keyquarry/build"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	mnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/propagation"
	msdk "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	tsdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
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
	LevelEventName                       = "EVENT"
	LevelNotice                          = slog.Level(2)
	LevelNoticeName                      = "NOTICE"
	DefaultGracefulStopTimeout           = 30 * time.Second
	DefaultKeepaliveTime                 = 10 * time.Second
	DefaultKeepaliveTimeout              = 60 * time.Second
	DefaultEventStreamBufferSize         = 10000
	DefaultEventStreamSendTimeout        = time.Second
	DefaultMaxKeyLength           uint64 = 1024
	DefaultMaxValueSize           uint64 = 1000000
	DefaultRevisionLimit          int64  = 5
	DefaultMaxLockDuration               = 15 * time.Minute
	DefaultMinLockDuration               = 5 * time.Second
	DefaultMinLifespan                   = 5 * time.Second
	DefaultTracerName                    = "keyquarry_server"
	// ReservedKeyPrefix is the prefix used for internal keys - these keys can't
	// be modified with normal methods, and are excluded from dumps
	ReservedKeyPrefix = "keyquarry"
	clientIDKey       = "client_id"
	kvLogKey          = "kv"
	loggerKey         = "logger"
	// InternalClientID is used for events from internal processes, like
	// Expunged, Expired, ...
	InternalClientID   = "keyquarry"
	DefaultNetwork     = "tcp"
	DefaultPort        = 33969
	DefaultMonitorPort = 33970
	decrementUint64    = ^uint64(0)
	ServerStartTimeout = 30 * time.Second
)

var (
	DefaultAddress        = fmt.Sprintf(":%d", DefaultPort)
	DefaultMonitorAddress = fmt.Sprintf(
		":%d",
		DefaultMonitorPort,
	)
	startupKeyStarted = fmt.Sprintf(
		"%s/Started",
		ReservedKeyPrefix,
	)
	startupKeyMaxLockDuration = fmt.Sprintf(
		"%s/MaxLockDuration",
		ReservedKeyPrefix,
	)
	startupKeyMinLockDuration = fmt.Sprintf(
		"%s/MinLockDuration",
		ReservedKeyPrefix,
	)
	startupKeyMinLifespan = fmt.Sprintf(
		"%s/MinLifespan",
		ReservedKeyPrefix,
	)
	startupKeyMaxKeys = fmt.Sprintf(
		"%s/MaxKeys",
		ReservedKeyPrefix,
	)
	startupKeyPruneInterval = fmt.Sprintf(
		"%s/PruneInterval",
		ReservedKeyPrefix,
	)
	startupKeyPruneAt = fmt.Sprintf(
		"%s/PruneAt",
		ReservedKeyPrefix,
	)
	startupKeyPruneTo = fmt.Sprintf(
		"%s/PruneTo",
		ReservedKeyPrefix,
	)
	startupKeyEagerPruneAt = fmt.Sprintf(
		"%s/EagerPruneAt",
		ReservedKeyPrefix,
	)
	startupKeyEagerPruneTo = fmt.Sprintf(
		"%s/EagerPruneTo",
		ReservedKeyPrefix,
	)
	startupKeyMaxValueSize = fmt.Sprintf(
		"%s/MaxValueSize",
		ReservedKeyPrefix,
	)
	startupKeyMaxKeySize = fmt.Sprintf(
		"%s/MaxKeyLength",
		ReservedKeyPrefix,
	)
	startupKeyRevisionLimit = fmt.Sprintf(
		"%s/RevisionLimit",
		ReservedKeyPrefix,
	)
	startupKeySnapshotEnabled = fmt.Sprintf(
		"%s/snapshot/Enabled",
		ReservedKeyPrefix,
	)
	startupKeySnapshotInterval = fmt.Sprintf(
		"%s/snapshot/Interval",
		ReservedKeyPrefix,
	)
	startupKeyEventStreamSubscriberLimit = fmt.Sprintf(
		"%s/EventStreamSubscriberLimit",
		ReservedKeyPrefix,
	)
)

var (
	ErrKeyTooLong = &KQError{
		Message: "Key length greater than maximum",
		Code:    codes.FailedPrecondition,
	}
	ErrValueTooLarge = KQError{
		Message: "value size is greater than maximum",
		Code:    codes.FailedPrecondition,
	}
	ErrKeyNotFound = KQError{
		Message: "Key not found",
		Code:    codes.NotFound,
	}
	ErrEmptyKey = KQError{
		Message: "Empty key",
		Code:    codes.InvalidArgument,
	}
	ErrMaxKeysReached = KQError{
		Message: "maximum number of keys reached",
		Code:    codes.ResourceExhausted,
	}
	ErrInvalidKeyPattern = KQError{
		Message: "invalid key pattern",
		Code:    codes.InvalidArgument,
	}
	ErrLocked = KQError{
		Message: "Key is locked",
		Code:    codes.PermissionDenied,
	}
	ErrVersioningDisabled = KQError{
		Message: "versioning is not enabled",
		Code:    codes.FailedPrecondition,
	}
	ErrRevisionNotFound = KQError{
		Message: "revision not found",
		Code:    codes.NotFound,
	}
	ErrWrongUnlockToken = KQError{
		Message: "client_id provided does not match key",
		Code:    codes.PermissionDenied,
	}
	ErrInvalidLockDuration = KQError{
		Message: "invalid lock Duration",
		Code:    codes.OutOfRange,
	}
	ErrLockDurationTooLong = KQError{
		Message: "lock Duration too long",
		Code:    codes.OutOfRange,
	}
	ErrReadOnlyServer = KQError{
		Message: "server in readonly mode",
		Code:    codes.FailedPrecondition,
	}
	ErrReservedKeyPrefix = KQError{
		Message: fmt.Sprintf("Key cannot begin with '%s'", ReservedKeyPrefix),
		Code:    codes.InvalidArgument,
	}
)

// Server is the main server struct. It should be initialized
// with New or NewFromLatestSnapshot.
// Configuration is handled via the Config struct.
type Server struct {
	// cfg is the current server Config
	cfg *Config

	// store is the main map of key to keyValue
	store map[string]*keyValue

	// locks is a map of key to kvLock values
	locks map[string]*kvLock

	// reapers track keys that are set to expire after a certain
	// amount of time
	reapers map[string]*reaper

	// history maps keys to their history/snapshots
	history map[string][]*keyValueSnapshot

	// clientInfo is a map of client IDs to ClientInfo
	clientInfo map[string]*ClientInfo

	// keyStats tracks keyLifetimeMetric for keys, persisting across restarts
	// and key deletions
	keyStats map[string]*keyLifetimeMetric

	// The mutexes here are used to protect the maps above. They should generally
	// be locked in the order below, starting from whichever mutex is being
	// used. Doing any of these backwards may result in a deadlock.
	cfgMu     sync.RWMutex // cfgMu is a mutex for cfg
	mu        sync.RWMutex // mu is the main mutex for the server
	keyStatMu sync.RWMutex // keyStatMu is a mutex for keyStats
	lockMu    sync.RWMutex // lockMu is a mutex specific to locks
	reaperMu  sync.RWMutex // reaperMu is a mutex for reapers
	cmu       sync.RWMutex // cmu is a mutex specific to clientInfo
	hmu       sync.RWMutex // hmu is a mutex specific to history

	// logger is the main logger for the server. Set this via Config
	logger *slog.Logger

	// tracer is the OpenTelemetry tracer for the server
	tracer trace.Tracer

	// events is the main channel for emitting events. This channel is
	// closed as the final step of Stop
	events chan Event

	// eventStream handles publishing of events to subscribers other
	// than snapshotter and the event logger
	eventStream *eventStream

	// snapshotter handles snapshotting of the server, which can either
	// happen on regular intervals, or only on shutdown
	snapshotter *snapshotter

	// pruner handles the scheduled pruning of keys over the configured pruneAt
	pruner *pruner

	// eagerPruneCh is a channel used to trigger an eager prune
	eagerPruneCh chan string

	// monitorMux is the ServeMux used for the metric/pprof/expvar endpoints
	monitorMux *http.ServeMux

	monitorServer *http.Server

	// grpcServer is the GRPC server this server is attached to
	grpcServer *grpc.Server

	// started is set true by Start once some initial setup has been done, just
	// prior to unlocking cfgMu and mu
	started bool

	onStart chan struct{}

	// stopped indicates the Stop function has started
	stopped bool

	// Metrics

	// numKeys is the current count of non-expunged keys
	numKeys atomic.Uint64

	// totalSize is the current total size of key values (does not include
	// history)
	totalSize atomic.Uint64

	// metrics for KeyEvent types seen by the event loop
	numEventValueAccessed   atomic.Uint64 // numEventValueAccessed tracks Accessed
	numEventCreated         atomic.Uint64 // numEventCreated tracks Created
	numEventUpdated         atomic.Uint64 // numEventUpdated tracks Updated
	numEventDeleted         atomic.Uint64 // numEventDeleted tracks Deleted
	numEventLocked          atomic.Uint64 // numEventLocked tracks Locked
	numEventUnlocked        atomic.Uint64 // numEventUnlocked tracks Unlocked
	numEventExpired         atomic.Uint64 // numEventExpired tracks Expired
	numEventExpunged        atomic.Uint64 // numEventExpunged tracks Expunged
	numEventLifespanSet     atomic.Uint64 // numEventLifespanSet tracks LifespanSet
	numEventLifespanRenewed atomic.Uint64 // numEventLifespanRenewed tracks LifespanRenewed

	// numEventSubscribers tracks the number of subscribers to the event
	// (this reflects the eventWorker count, generally)
	numEventSubscribers atomic.Uint64

	// numSnapshotsCreated tracks the number of snapshots created for the
	// current runtime of the server
	numSnapshotsCreated atomic.Uint64

	// numEagerPruneTriggered is the number of times an eager prune
	// was performed
	numEagerPruneTriggered atomic.Uint64

	// numPruneCompleted is the number of times the pruning process ran
	numPruneCompleted atomic.Uint64

	// numReapers is the number of registered reaper instances
	numReapers atomic.Uint64

	// numLocks tracks the number of locks currently held
	numLocks atomic.Uint64

	// clientIDs tracks the number of client IDs tracked by clientInfo
	clientIDs atomic.Uint64

	adminServer *Admin
	shutdown    chan struct{}

	lis             net.Listener
	monitorListener net.Listener

	pb.UnimplementedKeyQuarryServer
}

// New initializes a new [Server] with the
// given [Config] and returns it. If cfg is nil, a default [Config] will be used.
func New(cfg *Config) (*Server, error) {
	if cfg == nil {
		cfg = NewConfig()
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
	if cfg.MinLifespan == 0 {
		cfg.MinLifespan = DefaultMinLifespan
	}

	if cfg.PruneInterval > 0 && cfg.PruneTo > 0 && cfg.PruneTo > cfg.PruneAt {
		return nil, fmt.Errorf("prune_to must be less than prune_at")
	}

	if cfg.EagerPruneAt > 0 && cfg.EagerPruneAt <= cfg.EagerPruneTo {
		return nil, fmt.Errorf("eager_prune_to must be less than eager_prune_at")
	}

	if cfg.Logger == nil {
		var logger *slog.Logger
		var handler slog.Handler
		logLevel, _ := getLogLevel(cfg.LogLevel)
		handlerOptions := &slog.HandlerOptions{
			Level:     logLevel,
			AddSource: true,
			ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
				if a.Key == slog.LevelKey {
					level := a.Value.Any().(slog.Level)
					switch level {
					case LevelNotice:
						a.Value = slog.StringValue(LevelNoticeName)
					case LevelEvent:
						a.Value = slog.StringValue(LevelEventName)
					}
				}
				return a
			},
		}
		switch {
		case cfg.LogJSON:
			handler = slog.NewJSONHandler(os.Stdout, handlerOptions)
		default:
			handler = slog.NewTextHandler(os.Stdout, handlerOptions)
		}

		logger = slog.New(handler).With(
			loggerKey,
			"server",
		)
		cfg.Logger = logger
	}

	cfg.Logger = cfg.Logger.With(slog.String("version", build.Version))

	eventHandlerOpts := &slog.HandlerOptions{
		Level: LevelEvent,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == slog.LevelKey {
				a.Value = slog.StringValue("EVENT")
			}
			return a
		},
	}
	switch {
	case cfg.LogJSON:
		cfg.EventLogger = slog.New(
			slog.NewJSONHandler(
				os.Stdout,
				eventHandlerOpts,
			),
		)
	default:
		cfg.EventLogger = slog.New(
			slog.NewTextHandler(
				os.Stdout,
				eventHandlerOpts,
			),
		)
	}

	srv := &Server{
		store:        make(map[string]*keyValue),
		logger:       cfg.Logger,
		cfg:          cfg,
		locks:        make(map[string]*kvLock),
		reapers:      make(map[string]*reaper),
		events:       make(chan Event),
		keyStats:     map[string]*keyLifetimeMetric{},
		clientInfo:   map[string]*ClientInfo{},
		eagerPruneCh: make(chan string, 1),
		tracer:       otel.Tracer(cfg.TracerName),
		onStart:      make(chan struct{}, 1),
		shutdown:     make(chan struct{}, 1),
	}
	srv.cfgMu.Lock()
	defer srv.cfgMu.Unlock()

	if cfg.ExpVar || cfg.PPROF || cfg.Prometheus {
		mux := srv.newMonitorMux("/debug", "/metrics")
		srv.monitorMux = mux
		srv.monitorServer = &http.Server{Handler: mux}
	}

	srv.mu.Lock()
	defer srv.mu.Unlock()

	if cfg.RevisionLimit > 0 || cfg.RevisionLimit == -1 {
		srv.hmu.Lock()
		defer srv.hmu.Unlock()
		srv.history = map[string][]*keyValueSnapshot{}
	}

	srv.eventStream = newEventStream(srv)

	if cfg.Snapshot.Enabled {
		snapper, err := newSnapshotter(srv, cfg.Snapshot)
		if err != nil {
			return nil, err
		}
		srv.snapshotter = snapper
	}

	srv.pruner = newPruner(srv)

	srv.adminServer = NewAdminServer(srv)

	if srv.grpcServer == nil {
		// sets up the GRPC server options
		var serverOpts []grpc.ServerOption

		if cfg.OTLPTrace {
			statsHandler := otelgrpc.NewServerHandler()
			serverOpts = append(serverOpts, grpc.StatsHandler(statsHandler))
		}

		serverOpts = append(
			serverOpts,
			grpc.UnaryInterceptor(ClientIDInterceptor(srv)),
			grpc.KeepaliveParams(
				keepalive.ServerParameters{
					Time:    DefaultKeepaliveTime,
					Timeout: DefaultKeepaliveTimeout,
				},
			),
		)

		if cfg.SSLCertfile != "" && cfg.SSLKeyfile != "" {
			creds, tlsErr := credentials.NewServerTLSFromFile(
				cfg.SSLCertfile,
				cfg.SSLKeyfile,
			)
			if tlsErr != nil {
				return nil, fmt.Errorf(
					"failed to load TLS keys: %s",
					tlsErr.Error(),
				)
			}
			serverOpts = append(serverOpts, grpc.Creds(creds))
		}

		grpcServer := grpc.NewServer(serverOpts...)
		srv.grpcServer = grpcServer
		pb.RegisterKeyQuarryServer(grpcServer, srv)
		pb.RegisterAdminServer(grpcServer, srv.adminServer)
	}

	return srv, nil
}

// Snapshot calls [snapshotter.snapshot] if snapshotting is enabled,
// and returns the rowID of the snapshot, or an error if snapshotting
// failed
func (s *Server) Snapshot(ctx context.Context) (rowID int64, err error) {
	if s.snapshotter == nil {
		return 0, fmt.Errorf("snapshotting is not enabled")
	}
	return s.snapshotter.snapshot(ctx)
}

// newMonitorMux creates a new http.ServeMux with handlers
// added, based on the configuration. These include otelhttp
// for tracing.
func (s *Server) newMonitorMux(
	debugServerPath string,
	metricsPath string,
) *http.ServeMux {
	expvarPath := debugServerPath + "/vars"
	pprofPrefix := debugServerPath + "/pprof"
	mux := http.NewServeMux()
	mux.HandleFunc(
		"/", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("OK"))
		},
	)
	cfg := s.cfg
	cfg.Logger.Debug(
		"registering monitor endpoints",
		"address",
		cfg.MonitorAddress,
	)

	if cfg.ExpVar {
		cfg.Logger.Debug(
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
		cfg.Logger.Debug(
			"registering pprof handlers",
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

	if cfg.Prometheus {
		cfg.Logger.Debug(
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
	cfg.Logger.Debug("registered all handlers")

	return mux
}

// Serve finishes initializing Server, creates a GRPC
// server and begins to accept requests
func (s *Server) Serve(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-s.shutdown:
			cancel()
			return
		}
	}()

	s.cfgMu.RLock()
	serverConfig := s.cfg

	traceProvider, meterProvider, setupErr := setupOTelSDK(
		ctx,
		serverConfig.ServiceName,
		build.Version,
		serverConfig.OTLPTrace,
		false,
	)
	if setupErr != nil {
		return setupErr
	}

	switch {
	case traceProvider == nil:
		otel.SetTracerProvider(noop.TracerProvider{})
	default:
		otel.SetTracerProvider(traceProvider)
		defer func() {
			tpCtx, tpCancel := context.WithTimeout(
				context.Background(),
				30*time.Second,
			)
			_ = traceProvider.Shutdown(tpCtx)
			tpCancel()
		}()
	}

	switch {
	case meterProvider == nil:
		otel.SetMeterProvider(mnoop.MeterProvider{})
	default:
		otel.SetMeterProvider(meterProvider)
		defer func() {
			mpCtx, mpCancel := context.WithTimeout(
				context.Background(),
				30*time.Second,
			)
			_ = meterProvider.Shutdown(mpCtx)
			mpCancel()
		}()

	}

	u, err := serverConfig.URL()
	if u.Scheme == "unix" {
		_, err = os.Stat(u.Host)
		if err == nil || !os.IsNotExist(err) {
			return fmt.Errorf(
				"socket file '%s' already exists",
				u.Host,
			)
		}
	}

	if serverConfig.SSLKeyfile == "" && serverConfig.SSLCertfile == "" {
		s.logger.Warn("no SSL key or certificate file specified, using insecure connection")
	}

	serverConfig.ListenAddress = u.String()
	var lis = s.lis
	if lis == nil {
		lis, err = net.Listen(
			u.Scheme,
			u.Host,
		)
		if err != nil {
			return fmt.Errorf("failed to listen: %w", err)
		}
		s.lis = lis
	}

	// sets up the HTTP monitoring endpoint(s)
	var monitorURL *url.URL
	var monitorListener = s.monitorListener
	if s.monitorServer != nil {
		monitorURL, err = serverConfig.MonitorURL()
		if err != nil {
			return fmt.Errorf("failed to parse monitor address: %w", err)
		}
		if monitorURL.Scheme == "unix" {
			if _, err = os.Stat(monitorURL.Host); err == nil || !os.IsNotExist(err) {
				return fmt.Errorf(
					"socket file '%s' already exists",
					monitorURL.Host,
				)
			}
		}

		serverConfig.MonitorAddress = monitorURL.String()
		if monitorListener == nil {
			monitorListener, err = net.Listen(
				monitorURL.Scheme,
				monitorURL.Host,
			)
			if err != nil {
				return fmt.Errorf(
					"failed to listen with monitor address %s: %w",
					monitorURL.String(),
					err,
				)
			}
			s.monitorListener = monitorListener
		}
	}

	s.logger.Log(
		ctx,
		LevelNotice,
		"starting server",
		"config",
		serverConfig,
	)

	// the command context closing triggers the grpc server to shut down,
	// but we don't want to shut down the keyquarry server until the grpc
	// server has shut down gracefully, or just stopped handling requests.
	// so this separate context will be closed after the grpc server has
	// stopped
	keyQuarryCtx, keyQuarryCancel := context.WithCancel(context.Background())

	// Start starting things
	// 1. Start the keyquarry server, panic if it fails
	// 2. Start the HTTP monitor server, panic if it fails
	// 3. Start a goroutine to wait for the command context to close,
	//    after which it stops the servers and performs cleanup
	// 3. Start the GRPC server
	wg := &sync.WaitGroup{}

	// Start the event stream and snapshotter - when the context is
	// cancelled, that propagates to both, and the Start() function
	// should return.
	srvDone := make(chan struct{})
	wg.Add(1)
	s.cfgMu.RUnlock()
	go func() {
		defer wg.Done()
		if e := s.Start(keyQuarryCtx); e != nil {
			panic(fmt.Errorf("failed to start server: %w", e))
		}
		srvDone <- struct{}{}
	}()

	// start the HTTP monitor server, if configured
	if s.monitorServer != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()

			var httpErr error
			switch {
			case serverConfig.SSLCertfile != "" && serverConfig.SSLKeyfile != "":
				s.logger.Log(
					ctx,
					LevelNotice,
					"starting monitor server (TLS)",
					"address",
					serverConfig.MonitorAddress,
				)
				httpErr = s.monitorServer.ServeTLS(
					monitorListener,
					serverConfig.SSLCertfile,
					serverConfig.SSLKeyfile,
				)
			default:
				s.logger.Log(
					ctx,
					LevelNotice,
					"starting monitor server",
					"address",
					serverConfig.MonitorAddress,
				)
				httpErr = s.monitorServer.Serve(monitorListener)
			}

			if httpErr != nil && !errors.Is(httpErr, http.ErrServerClosed) {
				panic(
					fmt.Sprintf(
						"failed to start HTTP monitor server: %s",
						httpErr.Error(),
					),
				)

			}
		}()
	}

	gracefulStopTimeout := serverConfig.GracefulStopTimeout
	// start the goroutine responsible for shutting things
	// down in order and cleaning up
	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case <-ctx.Done():
			// When the main context is cancelled, gracefully stop
			// the GRPC server. When that's done, we cancel the
			// Server context that was passed to Start().
			// We don't use the same context or a child context for
			// that, because it might be cancelled before the server
			// is done shutting down.
			// Then, we wait for the signal that Start() has returned,
			// so we know it's safe to call Stop(), which will stop the
			// event stream, close its channels, and take a final
			// snapshot.
			s.logger.Log(
				ctx,
				LevelNotice,
				"shutting down server",
				"timeout",
				gracefulStopTimeout,
			)
			grpcServerStopped := make(chan struct{}, 1)
			switch gracefulStopTimeout {
			case 0:
				s.grpcServer.GracefulStop()
				grpcServerStopped <- struct{}{}
				close(grpcServerStopped)
			default:
				gsTimer := time.NewTimer(gracefulStopTimeout)
				go func() {
					s.grpcServer.GracefulStop()
					grpcServerStopped <- struct{}{}
					close(grpcServerStopped)
				}()

				select {
				case <-gsTimer.C:
					s.logger.Warn(
						"graceful stop timed out, forcing stop",
						"timeout",
						gracefulStopTimeout,
					)
					s.grpcServer.Stop()
				case <-grpcServerStopped:
					s.logger.Info("grpc server stopped gracefully")
				}
			}

			socketCleanupDone := make(chan struct{}, 1)
			switch u.Scheme {
			case "unix":
				go func() {
					if sErr := os.Remove(u.Host); sErr != nil && !os.IsNotExist(sErr) {
						s.logger.Error(
							"failed to remove socket file",
							slog.String("error", sErr.Error()),
						)
					}
					socketCleanupDone <- struct{}{}
				}()
			default:
				socketCleanupDone <- struct{}{}
			}

			// cancel Server context, wait for Start() to return,
			// then stop. Finally, wait for the socket cleanup to finish.
			keyQuarryCancel()
			<-srvDone
			s.logger.Debug("got done signal, waiting for server to stop")

			if stopErr := s.Stop(ctx); stopErr != nil {
				s.logger.Error(
					"failed to stop server",
					slog.String("error", stopErr.Error()),
				)
			}

			s.logger.Debug("waiting for socket cleanup, if needed")
			<-socketCleanupDone

			if s.monitorServer != nil {
				monitorCtx, monitorCancel := context.WithTimeout(
					context.Background(),
					30*time.Second,
				)
				shutErr := s.monitorServer.Shutdown(monitorCtx)
				monitorCancel()
				s.logger.Warn(
					"error shutting down monitor endpoints",
					"error",
					shutErr,
				)
				if monitorURL != nil && monitorURL.Scheme == "unix" {
					if msErr := os.Remove(monitorURL.Host); msErr != nil && !os.IsNotExist(msErr) {
						s.logger.Error(
							"failed to remove monitor socket file",
							slog.String("file", monitorURL.Host),
							slog.String("error", msErr.Error()),
						)
					}
				}
			}
		}
	}()

	// give the server 30 seconds to start
	startCtx, startCancel := context.WithTimeout(
		ctx,
		ServerStartTimeout,
	)
	defer startCancel()

	select {
	case <-s.onStart:
		startCancel()
		s.logger.Info("server started")
	case <-startCtx.Done():
		panic("server failed to start")
	}

	go func() {
		sanityTicker := time.NewTicker(120 * time.Second)
		for {
			select {
			case <-ctx.Done():
				return
			case <-sanityTicker.C:
				if sane := s.sanityCheck(ctx); sane != nil {
					s.logger.Error("failed sanity check", "errors", sane)
				}
			}
		}
	}()

	if err = s.grpcServer.Serve(lis); err != nil {
		s.logger.Error(
			"failed to serve",
			slog.String("error", err.Error()),
		)
	}

	// Waits for Server.Start() to return, for the GRPC
	// server to shut down gracefully, for Server.Stop() to
	// finish, and possibly for a unix socket file to be removed.
	wg.Wait()
	s.logger.Log(ctx, LevelNotice, "server stopped")
	if sanityErr := s.sanityCheck(ctx); sanityErr != nil {
		return sanityErr
	}
	return nil
}

// NewFromLatestSnapshot loads the most recent snapshot from the
// configured SnapshotConfig database, matching Config.Name, and returns
// a new Server initialized with snapshot data.
func NewFromLatestSnapshot(
	ctx context.Context,
	cfg *Config,
) (*Server, error) {
	if cfg == nil {
		cfg = NewConfig()
	}
	srv, err := New(cfg)
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

	db, err := dialect.DB(connStr)
	if err != nil {
		return nil, err
	}
	defer func() {
		if e := db.Close(); e != nil {
			srv.logger.Error(
				"error closing database connection",
				slog.String("error", e.Error()),
			)
		}
	}()
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

	if err = json.Unmarshal(latestSnapshot.Data, srv); err != nil {
		return nil, fmt.Errorf("unable to unmarshal snapshot: %w", err)
	}
	return srv, nil
}

// Set sets the value for the given key. If the key doesn't exist,
// it will be created. If it does, the value will be updated.
func (s *Server) Set(ctx context.Context, in *pb.KeyValue) (
	*pb.SetResponse,
	error,
) {
	if in.Key == "" {
		return nil, ErrEmptyKey
	}
	if strings.HasPrefix(strings.ToLower(in.Key), ReservedKeyPrefix) {
		return nil, ErrReservedKeyPrefix
	}

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
			return nil, KQError{
				Message: "invalid expire_after",
				Code:    codes.InvalidArgument,
			}
		}
		expireAfter = &e
	}

	var kvInfo *keyValue

	s.mu.Lock()
	defer s.mu.Unlock()
	kvInfo, exists := s.store[in.Key]

	clientID := s.ClientID(ctx)

	// Update an existing key
	if exists {
		s.lockMu.Lock()
		defer s.lockMu.Unlock()

		keyLock, alreadyLocked := s.locks[in.Key]

		if alreadyLocked && clientID != keyLock.ClientID {
			return nil, ErrWrongUnlockToken
		}

		// When updating the value, stop any current reaper.
		// If a lifespan was provided, create a new reaper using
		// that duration. Otherwise, create a new reaper with
		// the existing lifespan.
		s.reaperMu.Lock()
		defer s.reaperMu.Unlock()

		keyReaper, reaperExists := s.reapers[in.Key]
		switch {
		case reaperExists:
			reapTime := keyReaper.renew(expireAfter, clientID)
			logger.Info(
				"key lifespan renewed",
				"reap_after",
				reapTime,
				"reaper",
				keyReaper,
			)
		case expireAfter != nil:
			keyReaper = newKeyReaper(
				s,
				*expireAfter,
				in.Key,
				clientID,
			)
			logger.Info("created new reaper", "reaper", keyReaper)
		}

		hashChannel := make(chan uint64, 1)
		go func() {
			hashFunc := fnv.New64a()
			if _, he := hashFunc.Write(in.Value); he != nil {
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
			switch {
			case alreadyLocked:
				_ = keyLock.renew(*lockDuration, clientID)
			default:
				keyLock = newKeyLock(s, *lockDuration, in.Key, clientID)
			}
		}
		kvInfo.mu.Lock()
		defer kvInfo.mu.Unlock()

		// Check back in on our hash function, and only consider it
		// an updated value if the hash has changed, or there's no
		// hash function set
		newHash := <-hashChannel
		switch newHash {
		case kvInfo.Hash:
			s.logger.Info("no change to value", kvLogKey, kvInfo)
		default:
			s.logger.Info("updated value", kvLogKey, kvInfo)
			now := time.Now()
			defer s.emit(in.Key, Updated, clientID, &now)

			s.totalSize.Add(^(kvInfo.Size - 1))
			s.totalSize.Add(size)
			kvInfo.Size = size
			kvInfo.Updated = now
			kvInfo.Value = in.Value
			kvInfo.Version++
			kvInfo.Hash = newHash
			if cfg.RevisionLimit != 0 {
				s.hmu.Lock()
				defer s.hmu.Unlock()
				_ = s.addKeyValueSnapshot(kvInfo, cfg.RevisionLimit)
			}
		}

		return &pb.SetResponse{Success: true}, nil
	}

	// Create a new key
	currentCt := s.numKeys.Load()
	eagerPruneAt := cfg.EagerPruneAt
	maxKeys := cfg.MaxNumberOfKeys

	switch {
	case maxKeys > 0 && currentCt >= maxKeys:
		overLimit := currentCt - eagerPruneAt
		s.logger.Warn(
			"number of keys >= maximum, enable eager pruning to automatically make space",
			"current", currentCt,
			"max", maxKeys,
			"over_limit", overLimit,
		)
		return nil, ErrMaxKeysReached
	case eagerPruneAt > 0 && currentCt >= eagerPruneAt:
		overLimit := currentCt - eagerPruneAt
		s.logger.Warn(
			"number of keys over eager prune limit",
			"current", currentCt,
			"eager_prune_at", eagerPruneAt,
			"over_limit", overLimit,
		)
		select {
		case s.eagerPruneCh <- in.Key:
			s.logger.Debug("sent eager prune signal")
		default: // don't wait up
		}
	default:
		s.logger.Info(
			"key counts",
			"current",
			currentCt,
			"max",
			cfg.MaxNumberOfKeys,
		)
	}

	var version uint64 = 1

	switch cfg.RevisionLimit {
	case 0:
		// nothing
	case -1:
		s.hmu.Lock()
		defer s.hmu.Unlock()

		_, historyExists := s.history[in.Key]
		if historyExists {
			kh := s.history[in.Key]
			if len(kh) > 0 {
				version = kh[len(kh)-1].Version + 1
			}
		} else {
			s.history[in.Key] = []*keyValueSnapshot{}
		}
	default:
		s.hmu.Lock()
		defer s.hmu.Unlock()

		_, historyExists := s.history[in.Key]
		if historyExists {
			kh := s.history[in.Key]
			if len(kh) > 0 {
				version = kh[len(kh)-1].Version + 1
			}
		} else {
			s.history[in.Key] = make([]*keyValueSnapshot, 0, cfg.RevisionLimit)
		}
	}

	s.logger.Debug("most recent version seen", "key_version", version)

	kvInfo = newKeyValue(in.Key, in.Value, in.ContentType, clientID)
	kvInfo.Version = version

	_ = s.addKeyValueSnapshot(kvInfo, cfg.RevisionLimit)
	s.addKeyValueInfo(kvInfo, size)

	if in.LockDuration != nil {
		s.lockMu.Lock()
		lock := newKeyLock(s, *lockDuration, in.Key, clientID)
		logger.Info("created new lock", "key_lock", lock)
		s.lockMu.Unlock()
	}

	if expireAfter != nil {
		s.reaperMu.Lock()
		keyReaper := newKeyReaper(
			s,
			*expireAfter,
			in.Key,
			clientID,
		)
		logger.Info("created new reaper", "reaper", keyReaper)
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
	return &pb.SetResponse{
		Success: true,
		IsNew:   true,
	}, nil
}

// Get returns the value of a key
func (s *Server) Get(ctx context.Context, in *pb.Key) (
	*pb.GetResponse,
	error,
) {
	span := trace.SpanFromContext(ctx)
	span.SetAttributes(attribute.String("key", in.Key))

	logger := s.requestLogger(ctx)
	logger.Info("getting key", slog.String("key", in.Key))

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
	s.emit(in.Key, Accessed, clientID, &t)
	return &pb.GetResponse{Value: kvInfo.Value}, nil
}

// Lock locks a key, preventing other clients from setting/reading it
func (s *Server) Lock(
	ctx context.Context,
	in *pb.LockRequest,
) (*pb.LockResponse, error) {
	if in.Key == "" {
		return nil, ErrEmptyKey
	}
	if strings.HasPrefix(strings.ToLower(in.Key), ReservedKeyPrefix) {
		return nil, ErrReservedKeyPrefix
	}

	span := trace.SpanFromContext(ctx)
	span.SetAttributes(attribute.String("key", in.Key))

	logger := s.requestLogger(ctx)
	clientID := s.ClientID(ctx)
	logger.Info("requesting lock", slog.String("key", in.Key))

	s.cfgMu.RLock()
	defer s.cfgMu.RUnlock()
	cfg := *s.cfg

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
	defer s.mu.Unlock()
	var kvInfo *keyValue

	kvInfo, ok := s.store[in.Key]

	// Already exists - if the client owns the lock, then replace the
	// current lock with a new one
	if ok {
		s.lockMu.Lock()
		defer s.lockMu.Unlock()

		keyLock, alreadyLocked := s.locks[in.Key]
		switch {
		case alreadyLocked:
			if clientID != keyLock.ClientID {
				return nil, KQError{
					Message: "locked by another client",
					Code:    codes.PermissionDenied,
				}
			}
			logger.Debug("renewing lock", slog.String("key", in.Key))
			_ = keyLock.renew(lockDuration, clientID)
		default:
			newLock := newKeyLock(s, lockDuration, in.Key, clientID)
			logger.Info(
				"lock granted",
				"key_lock", newLock,
			)
		}
		return &pb.LockResponse{Success: true}, nil
	}

	if !in.CreateIfMissing {
		return nil, ErrKeyNotFound
	}

	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	maxKeys := cfg.MaxNumberOfKeys
	eagerPruneAt := cfg.EagerPruneAt
	currentCt := s.numKeys.Load()

	if in.CreateIfMissing && (maxKeys > 0 || eagerPruneAt > 0) {
		switch {
		case maxKeys > 0 && currentCt >= maxKeys:
			overLimit := currentCt - maxKeys
			s.logger.Warn(
				"number of keys >= maximum, enable eager pruning to automatically make space",
				"current", currentCt,
				"max", maxKeys,
				"over_limit", overLimit,
			)
			return nil, ErrMaxKeysReached
		case eagerPruneAt > 0 && currentCt >= eagerPruneAt:
			overLimit := currentCt - eagerPruneAt
			s.logger.Warn(
				"number of keys over eager prune limit",
				"current", currentCt,
				"eager_prune_at", eagerPruneAt,
				"over_limit", overLimit,
			)
			select {
			case s.eagerPruneCh <- in.Key:
				s.logger.Debug("sent eager prune signal")
			default:
				// don't wait up
			}
		default:
			s.logger.Debug(
				"key counts",
				"current",
				currentCt,
				"max",
				cfg.MaxNumberOfKeys,
			)

		}
	}

	d := in.Duration.AsDuration()
	kvInfo = newKeyValue(in.Key, nil, "", clientID)

	s.hmu.Lock()
	defer s.hmu.Unlock()
	_ = s.addKeyValueSnapshot(kvInfo, cfg.RevisionLimit)

	s.addKeyValueInfo(kvInfo, 0)
	lock := newKeyLock(s, d, in.Key, clientID)
	logger.Info(
		"created key",
		kvLogKey, kvInfo,
		"key_lock", lock,
	)
	return &pb.LockResponse{
		Success: true,
	}, nil
}

func (s *Server) Unlock(
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
	if strings.HasPrefix(strings.ToLower(in.Key), ReservedKeyPrefix) {
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
	s.numLocks.Add(decrementUint64)

	now := time.Now()
	s.emit(in.Key, Unlocked, clientID, &now)

	return &pb.UnlockResponse{Success: true}, nil
}

// Delete deletes a key from the store
func (s *Server) Delete(ctx context.Context, in *pb.DeleteRequest) (
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

	if strings.HasPrefix(strings.ToLower(in.Key), ReservedKeyPrefix) {
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

	s.deleteKey(in.Key, clientID, false)
	if !cfg.KeepKeyHistoryAfterDelete {
		s.hmu.Lock()
		defer s.hmu.Unlock()
		delete(s.history, in.Key)
	}
	return &pb.DeleteResponse{Deleted: true}, nil
}

// Inspect returns information about a key
func (s *Server) Inspect(
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
func (s *Server) Pop(ctx context.Context, in *pb.PopRequest) (
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

	if strings.HasPrefix(strings.ToLower(in.Key), ReservedKeyPrefix) {
		return nil, ErrReservedKeyPrefix
	}

	s.cfgMu.RLock()
	cfg := *s.cfg
	defer s.cfgMu.RUnlock()
	if cfg.Readonly {
		return nil, ErrReadOnlyServer
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	kvInfo := s.getKey(in.Key)
	if kvInfo == nil {
		return nil, ErrKeyNotFound
	}

	s.lockMu.RLock()
	defer s.lockMu.RUnlock()

	keyLock, lockExists := s.locks[in.Key]
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

	s.deleteKey(in.Key, clientID, false)
	s.emit(in.Key, Accessed, clientID, nil)
	if !cfg.KeepKeyHistoryAfterDelete {
		s.hmu.Lock()
		defer s.hmu.Unlock()
		delete(s.history, in.Key)
	}
	return getResponse, nil
}

// Exists returns true if a key exists
func (s *Server) Exists(ctx context.Context, in *pb.Key) (
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
func (s *Server) ListKeys(
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
			ReservedKeyPrefix,
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
func (s *Server) Stats(
	ctx context.Context, _ *pb.EmptyRequest,
) (*pb.ServerMetrics, error) {
	return s.GetStats(), nil
}

func (s *Server) ClearHistory(
	ctx context.Context,
	_ *pb.EmptyRequest,
) (*pb.ClearHistoryResponse, error) {
	logger := s.requestLogger(ctx)

	s.cfgMu.RLock()
	defer s.cfgMu.RUnlock()

	revisionLimit := s.cfg.RevisionLimit
	readonly := s.cfg.Readonly

	if readonly {
		return nil, ErrReadOnlyServer
	}
	if revisionLimit == 0 {
		return nil, ErrVersioningDisabled
	}

	data := &pb.ClearHistoryResponse{}

	s.mu.RLock()
	defer s.mu.RUnlock()

	s.hmu.Lock()
	defer s.hmu.Unlock()

	for key, keyHistory := range s.history {
		if e := ctx.Err(); e != nil {
			return data, e
		}
		_, exists := s.store[key]
		data.Keys++
		if !exists {
			delete(s.history, key)
			continue
		}
		if keyHistory == nil {
			continue
		}
		data.Cleared = int64(len(keyHistory))
		switch revisionLimit {
		case -1:
			s.history[key] = make([]*keyValueSnapshot, 0)
		default:
			s.history[key] = make([]*keyValueSnapshot, 0, revisionLimit)
		}
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
func (s *Server) Clear(
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

	s.hmu.Lock()
	defer s.hmu.Unlock()

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
		if strings.HasPrefix(strings.ToLower(key), ReservedKeyPrefix) {
			continue
		}
		_, isLocked := s.locks[key]
		kvInfo.mu.Lock()
		if req.Force || !isLocked {
			if isLocked && s.locks != nil {
				delete(s.locks, key)
			}
			s.deleteKey(key, clientID, false)
			delete(s.history, key)
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

func (s *Server) GetRevision(
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
		return nil, KQError{
			Message: "revision must be greater or less than zero",
			Code:    codes.InvalidArgument,
		}
	}

	s.hmu.RLock()
	defer s.hmu.RUnlock()

	var h *keyValueSnapshot
	var found bool

	if in.Version > 0 {
		h, found = s.getVersion(in.Key, in.Version)
	} else if in.Version < 0 {
		h, found = s.getRelativeVersion(in.Key, in.Version)
	}

	if !found {
		return nil, ErrRevisionNotFound
	}
	return &pb.RevisionResponse{
		Value:     h.Value,
		Timestamp: timestamppb.New(h.Timestamp),
	}, nil
}

// Logger returns the server's logger
func (s *Server) Logger() *slog.Logger {
	return s.logger
}

func (s *Server) getVersion(k string, v int64) (*keyValueSnapshot, bool) {
	versions := s.history[k]
	if versions == nil {
		return nil, false
	}

	for i := 0; i < len(versions); i++ {
		kvs := versions[i]
		if kvs.Version == uint64(v) {
			return kvs, true
		}
	}
	return nil, false
}

func (s *Server) getRelativeVersion(k string, v int64) (
	*keyValueSnapshot,
	bool,
) {
	kh, exists := s.history[k]
	if !exists {
		return nil, false
	}
	if v < 0 {
		v = int64(len(kh)) + v
	}
	targetVersion := uint64(v)
	for i := 0; i < len(kh); i++ {
		keySnapshot := kh[i]
		if keySnapshot.Version == targetVersion {
			return keySnapshot, true
		}
	}
	return nil, false
}

func (s *Server) Register(
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
		return nil, KQError{
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

func (s *Server) SetReadOnly(
	ctx context.Context,
	in *pb.ReadOnlyRequest,
) (*pb.ReadOnlyResponse, error) {
	logger := s.requestLogger(ctx)
	clientID := s.ClientID(ctx)

	switch in.Enable {
	case in.Enable:
		logger.Info("attempting to set read-only mode")
	default:
		logger.Info("attempt to disable read-only mode")
	}

	s.cfgMu.Lock()
	defer s.cfgMu.Unlock()

	if s.cfg.PrivilegedClientID == "" {
		return nil, KQError{
			Message: "privileged client ID not set",
			Code:    codes.FailedPrecondition,
		}
	}

	if clientID != s.cfg.PrivilegedClientID {
		return nil, KQError{
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
func (s *Server) Subscribe(
	ctx context.Context,
	name string,
	keys []string,
	events []KeyEvent,
) (<-chan Event, error) {
	return s.eventStream.Subscribe(ctx, name, keys, events)
}

func (s *Server) GetKeyMetric(
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
		return nil, KQError{
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

func (s *Server) getStats() *pb.ServerMetrics {
	s.logger.Info("compiling stats")

	var numKeys = s.numKeys.Load()
	var totalSize = s.totalSize.Load()

	var numNewKeysSet = s.numEventCreated.Load()
	var numKeysUpdated = s.numEventUpdated.Load()
	var deleted = s.numEventDeleted.Load()
	var numKeysLocked = s.numEventLocked.Load()
	var numKeysUnlocked = s.numEventUnlocked.Load()
	var numKeysExpired = s.numEventExpired.Load()
	var numKeysExpunged = s.numEventExpunged.Load()
	var numLifespanSet = s.numEventLifespanSet.Load()
	var numLifespanRenewed = s.numEventLifespanRenewed.Load()
	var clientsSeen = s.clientIDs.Load()
	var accesses = s.numEventValueAccessed.Load()

	var subscribers = s.numEventSubscribers.Load()
	var snapshots = s.numSnapshotsCreated.Load()
	var eagerPruneTriggered = s.numEagerPruneTriggered.Load()
	var pruneCompleted = s.numPruneCompleted.Load()
	var currentReapers = s.numReapers.Load()
	var currentLocks = s.numLocks.Load()

	p := s.pressure()
	var used float32

	used = float32(p.Used)
	// threshold = float32(p.PruneThreshold)

	eventMetrics := &pb.EventMetrics{
		New:             &numNewKeysSet,
		Updated:         &numKeysUpdated,
		Deleted:         &deleted,
		Locked:          &numKeysLocked,
		Unlocked:        &numKeysUnlocked,
		Expired:         &numKeysExpired,
		Expunged:        &numKeysExpunged,
		Accessed:        &accesses,
		LifespanSet:     &numLifespanSet,
		LifespanRenewed: &numLifespanRenewed,
	}
	hkCount := uint64(len(s.history))
	var revisionCount uint64

	for _, kh := range s.history {
		revisionCount = revisionCount + uint64(len(kh))
	}

	h := &pb.HistoryMetrics{Keys: &hkCount, Revisions: &revisionCount}

	m := &pb.ServerMetrics{
		Keys:                &numKeys,
		TotalSize:           &totalSize,
		Events:              eventMetrics,
		CurrentLocks:        &currentLocks,
		ClientIds:           &clientsSeen,
		Reapers:             &currentReapers,
		SnapshotsCreated:    &snapshots,
		EventSubscribers:    &subscribers,
		EagerPruneTriggered: &eagerPruneTriggered,
		PruneCompleted:      &pruneCompleted,
		History:             h,
		Pressure: &pb.KeyPressure{
			Keys: &p.Keys,
			Max:  &p.Max,
			Used: &used,
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
				slog.Uint64(
					strings.ToLower(LifespanRenewed.String()),
					*m.Events.LifespanRenewed,
				),
			),
		),
	)
	return m
}

func (s *Server) GetStats() *pb.ServerMetrics {
	s.cfgMu.RLock()
	defer s.cfgMu.RUnlock()

	s.mu.RLock()
	defer s.mu.RUnlock()

	s.lockMu.RLock()
	defer s.lockMu.RUnlock()

	s.reaperMu.RLock()
	defer s.reaperMu.RUnlock()

	s.hmu.RLock()
	defer s.hmu.RUnlock()

	stats := s.getStats()
	return stats
}

// Stop cleans up timers/goroutines/channels, after the
// context given to Start has been finished, and its goroutines have
// finished.
// This should be called after both Start has finished, and
// the GRPC server has been stopped. Requests received after
// Stop has finished are likely to crash/block the server, or
// have changes that aren't reflected in the final snapshot.
func (s *Server) Stop(ctx context.Context) error {
	s.logger.Info("stopping server, cleaning up")

	_, span := s.tracer.Start(
		context.Background(),
		"keyquarry.stop",
		trace.WithNewRoot(),
	)
	defer span.End()

	s.cmu.Lock()
	defer s.cmu.Unlock()

	s.cfgMu.RLock()
	defer s.cfgMu.RUnlock()

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.stopped {
		return errors.New("server previously stopped")
	}
	s.stopped = true

	wg := sync.WaitGroup{}

	// Stop unlock timers
	s.lockMu.Lock()
	defer s.lockMu.Unlock()

	wg.Add(1)
	go func() {
		defer wg.Done()
		s.stopUnlockTimers()
	}()

	// Stop expiration timers
	s.reaperMu.Lock()
	defer s.reaperMu.Unlock()
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.stopExpirationTimers()
	}()

	var err error
	var snapshot int64

	// Take a final snapshot
	if s.snapshotter != nil {
		s.logger.Info(
			"saving final snapshot",
			"config", s.cfg.Snapshot,
		)
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.logger.Info("starting snapshot")
			var sfErr error
			snapshot, sfErr = s.Snapshot(context.Background())
			if sfErr != nil {
				s.logger.Error(
					"error creating final snapshot",
					"error",
					sfErr,
				)
				return
			}
			s.logger.Log(
				ctx,
				LevelNotice,
				"final snapshot created",
				slog.Int64("snapshot_id", snapshot),
			)
			if s.snapshotter.db != nil {
				if dbErr := s.snapshotter.db.Close(); dbErr != nil {
					s.snapshotter.logger.Error(
						"error closing snapshot database",
						"error",
						dbErr,
					)
				}
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		s.eventStream.mu.Lock()
		defer s.eventStream.mu.Unlock()
		s.eventStream.Stop(ctx)
	}()

	wg.Wait()

	s.logger.Info("getting final stats")
	stats := s.getStats()
	s.logger.Log(ctx, LevelNotice, "server stopped", "metrics", stats)
	return err
}

// Start initializes snapshotting, pruning, and starts the event loop.
func (s *Server) Start(ctx context.Context) error {
	s.cfgMu.Lock()
	s.logger.LogAttrs(
		ctx,
		LevelNotice,
		"starting server",
		slog.Any("config", s.cfg),
	)
	s.mu.Lock()
	s.lockMu.Lock()
	s.reaperMu.Lock()
	s.cmu.Lock()

	_, span := s.tracer.Start(
		ctx, "keyquarry.start", trace.WithAttributes(
			attribute.String("keyquarry.config.log_level", s.cfg.LogLevel),
			attribute.Bool("keyquarry.config.readonly", s.cfg.Readonly),
		),
	)

	if s.started {
		s.cfgMu.Unlock()
		s.mu.Unlock()
		s.lockMu.Unlock()
		s.reaperMu.Unlock()
		s.cmu.Unlock()
		span.End()
		return errors.New("server previously started")
	}

	wg := &sync.WaitGroup{}

	snapshotCh := make(chan Event, s.cfg.EventStreamBufferSize)
	logCh := make(chan Event, s.cfg.EventStreamBufferSize)
	eventStreamCh := make(chan Event, s.cfg.EventStreamBufferSize)
	s.eventStream.events = eventStreamCh

	var logEvents bool
	if s.cfg.LogEvents || s.cfg.LogLevel == "EVENT" {
		logEvents = true
	}
	if !logEvents {
		logCh = nil
	}
	if s.snapshotter == nil {
		snapshotCh = nil
	}

	go s.eventLoop(ctx, snapshotCh, logCh, eventStreamCh)

	// starts the event stream - it'll exit when the context is finished
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.eventStream.Run(ctx)
	}()

	// start the event logger
	if logEvents {
		wg.Add(1)
		eventLogger := s.cfg.EventLogger
		go func() {
			defer wg.Done()
			s.logger.Log(ctx, LevelNotice, "started event logger")
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
				case LifespanRenewed:
					msg = "lifespan renewed"
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

	switch {
	case s.snapshotter == nil:
		s.logger.Warn("snapshotter disabled, data will be lost on shutdown/restart")
	default: // start the snapshotter
		if initErr := s.snapshotter.dialect.InitDB(
			ctx,
			s.cfg.Snapshot.Database,
		); initErr != nil {
			s.logger.Debug(
				"error initializing database",
				"error",
				initErr,
				"database",
				s.cfg.Snapshot.Database,
			)
			return fmt.Errorf(
				"error initializing database: %s",
				initErr.Error(),
			)
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			if runErr := s.snapshotter.Run(ctx, snapshotCh); runErr != nil {
				panic(
					fmt.Errorf(
						"error running snapshotter: %s",
						runErr.Error(),
					),
				)
			}
		}()
	}

	// If a ceiling is configured for the number of keys, start a goroutine
	// which, at the configured interval, will delete the oldest keys
	// (determined by lock date if locked, then updated, then created) until
	// the number of keys is below the ceiling and configured pruneAt
	switch {
	case (s.cfg.PruneInterval > 0 && s.cfg.PruneAt > 0) || s.cfg.EagerPruneAt > 0:
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = s.pruner.Run(ctx)
			s.logger.Log(ctx, LevelNotice, "pruner finished")
			close(s.eagerPruneCh)
		}()
	default:
		s.logger.Warn("key pruner disabled, set prune_interval to enable")
	}

	s.addStartupKeys()
	s.started = true
	s.cfgMu.Unlock()
	s.mu.Unlock()
	s.lockMu.Unlock()
	s.reaperMu.Unlock()
	s.cmu.Unlock()

	s.logger.Log(ctx, LevelNotice, "setup complete")
	s.onStart <- struct{}{}
	close(s.onStart)

	// Wait for the snapshotter, pruner, event logger and event stream
	// to finish
	wg.Wait()
	return nil
}

// WatchKeyValue watches a key for events Deleted, Expired, Expunged,
// Created, Updated and Unlocked, and forwards the value of the key
// with some additional metadata, to the client. If the key is Locked by
// another client, changes will not be pushed until unlocked. If available,
// the current value is immediately pushed prior to watching for changes.
func (s *Server) WatchKeyValue(
	in *pb.WatchKeyValueRequest,
	stream pb.KeyQuarry_WatchKeyValueServer,
) error {
	ctx := stream.Context()
	span := trace.SpanFromContext(ctx)
	span.SetAttributes(attribute.String("key", in.Key))
	clientID, err := s.ClientIDFromContext(ctx)
	if err != nil {
		return err
	}
	if clientID == "" {
		return KQError{
			Message: "client ID not found",
			Code:    codes.Unauthenticated,
		}
	}
	logger := s.requestLogger(ctx)
	logger.Info("got new event watcher")

	var currentKV *keyValue
	var exists bool
	var streamErr error
	var kv *pb.WatchKeyValueResponse
	var keyLock *kvLock
	var keyIsLocked bool
	var lockClientID string
	s.mu.RLock()

	currentKV, exists = s.store[in.Key]

	switch {
	case exists:
		s.lockMu.RLock()
		keyLock, keyIsLocked = s.locks[in.Key]
		if keyIsLocked {
			lockClientID = keyLock.ClientID
		}
		s.lockMu.RUnlock()
		s.mu.RUnlock()
		if !keyIsLocked || (lockClientID == clientID) {
			currentKV.mu.RLock()

			kv = &pb.WatchKeyValueResponse{
				Key:           currentKV.Key,
				Value:         currentKV.Value,
				Hash:          currentKV.Hash,
				ContentType:   currentKV.ContentType,
				Version:       currentKV.Version,
				EventClientId: InternalClientID,
			}
			currentKV.mu.RUnlock()

			if streamErr = stream.Send(kv); streamErr != nil {
				return streamErr
			}
			s.emit(in.Key, Accessed, clientID, nil)
		}
	default:
		s.mu.RUnlock()
	}

	streamClientID := fmt.Sprintf("%s/WatchKeyValue", clientID)
	sctx, cancel := context.WithCancel(ctx)
	defer func() {
		logger.Debug("unsubscribing client")
		logger.Debug("cancelling context for value watch")
		cancel()
		if unsubErr := s.Unsubscribe(streamClientID); unsubErr != nil {
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
			Unlocked,
		},
	)

	if err != nil {
		return KQError{Message: err.Error(), Code: codes.Internal}
	}

	for ev := range events {
		if sctx.Err() != nil {
			break
		}
		//goland:noinspection GoSwitchMissingCasesForIotaConsts
		switch ev.Event {
		case Deleted, Expired, Expunged:
			kv = &pb.WatchKeyValueResponse{
				Key:            ev.Key,
				KeyEvent:       pb.KeyEvent(ev.Event),
				EventClientId:  ev.ClientID,
				EventTimestamp: timestamppb.New(ev.Time),
			}
			if streamErr = stream.Send(kv); streamErr != nil {
				logger.Error(
					"error sending key value to subscribed client",
					"error",
					streamErr,
				)
				return streamErr
			}
		case Created, Updated, Unlocked:
			s.mu.RLock()
			currentKV, exists = s.store[in.Key]
			if !exists {
				s.mu.RUnlock()
				return KQError{
					Message: "key no longer found",
					Code:    codes.NotFound,
				}
			}

			if ev.Event != Unlocked {
				s.lockMu.RLock()
				keyLock, keyIsLocked = s.locks[in.Key]
				if keyIsLocked {
					lockClientID = keyLock.ClientID
				}
				s.lockMu.RUnlock()

				if keyIsLocked && (lockClientID != clientID) {
					s.mu.RUnlock()
					continue
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

			if streamErr = stream.Send(kv); streamErr != nil {
				logger.Error(
					"error sending key value to subscribed client",
					"error",
					streamErr,
				)
				return streamErr
			}
			s.emit(in.Key, Accessed, clientID, nil)
		}
	}
	logger.Debug("stream finished", "client_id", clientID)
	return nil
}

// WatchStream subscribes to the event stream
func (s *Server) WatchStream(
	in *pb.WatchRequest,
	stream pb.KeyQuarry_WatchStreamServer,
) error {
	ctx := stream.Context()
	clientID, err := s.ClientIDFromContext(ctx)
	if err != nil {
		return err
	}
	if clientID == "" {
		return KQError{
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
		if unsubErr := s.Unsubscribe(streamClientID); unsubErr != nil {
			logger.Error("unsubscribe failed", "error", unsubErr)
		}
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
func (s *Server) Unsubscribe(name string) error {
	s.logger.Info("request to unsubscribe", "name", name)
	r := s.eventStream.Unsubscribe(name)
	return r
}

// Config returns a copy of the current server configuration
func (s *Server) Config() Config {
	s.cfgMu.RLock()
	cfg := *s.cfg
	s.cfgMu.RUnlock()
	return cfg
}

// ClientIDFromContext extracts the client ID from the provided context
// and returns it. It also returns an error if it's missing.
func (s *Server) ClientIDFromContext(ctx context.Context) (
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
		return "", &KQError{
			Message: "reserved client_id used",
			Code:    codes.InvalidArgument,
		}
	}

	clientID := clientIDs[0]
	switch clientID {
	case "":
		s.logger.Warn("missing client_id")
		return "", status.Errorf(codes.InvalidArgument, "empty client_id")
	case InternalClientID:
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
func (s *Server) ClientID(ctx context.Context) string {
	clientID, _ := s.ClientIDFromContext(ctx)
	return clientID
}

// ClientInfo returns the ClientInfo for the given context
func (s *Server) ClientInfo(ctx context.Context) *ClientInfo {
	clientID, err := s.ClientIDFromContext(ctx)
	if err != nil {
		return nil
	}
	s.cmu.RLock()
	defer s.cmu.RUnlock()
	return s.clientInfo[clientID]
}

func (s *Server) MarshalJSON() (data []byte, err error) {
	keys := make([]*keyValue, 0, len(s.store))
	for _, kvInfo := range s.store {
		if strings.HasPrefix(strings.ToLower(kvInfo.Key), ReservedKeyPrefix) {
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

	locks := make([]*kvLock, 0, len(s.locks))
	for _, keyLock := range s.locks {
		locks = append(locks, keyLock)
	}

	reapers := make([]*reaper, 0, len(s.reapers))
	for _, keyReaper := range s.reapers {
		reapers = append(reapers, keyReaper)
	}

	stats := map[string]*keyLifetimeMetric{}
	for key, keyStat := range s.keyStats {
		stats[key] = keyStat
		keyStat.mu.RLock()
	}
	defer func() {
		for _, keyStat := range stats {
			keyStat.mu.RUnlock()
		}
	}()

	state := kvStoreState{
		Keys:    keys,
		Clients: clients,
		Locks:   locks,
		Reapers: reapers,
		Version: build.Version,
	}
	if s.cfg.PersistentRevisions {
		state.History = s.history
	}

	return json.Marshal(state)
}

// UnmarshalJSON reads the provided data and populates the server
// with the key-value data, client IDs and locks. This will
// overwrite any existing data.
func (s *Server) UnmarshalJSON(data []byte) error {
	state := kvStoreState{}
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
		s.store = make(map[string]*keyValue)
	}

	if s.keyStats == nil {
		s.keyStats = make(map[string]*keyLifetimeMetric)
	}

	// channel to send keyValue for value hashing
	hashChannel := make(chan *keyValue)
	// receive keyValue when hashing is complete
	doneChannel := make(chan *keyValue)
	wg := sync.WaitGroup{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	workers := runtime.GOMAXPROCS(0)

	now := time.Now()

	var ignoreKeys []string

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
			keyReaper.srv = s
			s.reapers[keyReaper.Key] = keyReaper
			s.numReapers.Add(1)
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
					if _, he := hashFunc.Write(kvInfo.Value); he != nil {
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
		switch {
		case exists:
			s.logger.Error(
				"duplicate key found",
				slog.String("key", kv.Key),
			)
			cancel()
		default:
			s.store[kv.Key] = kv
			_, statExists := s.keyStats[kv.Key]
			if !statExists {
				createdAt := kv.Created
				updated := kv.Updated
				if updated.IsZero() {
					updated = createdAt
				}
				s.keyStats[kv.Key] = &keyLifetimeMetric{
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
			keyLock.srv = s
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

	if state.History != nil {
		if s.history == nil {
			s.history = make(map[string][]*keyValueSnapshot)
		}
		for k, kh := range state.History {
			s.history[k] = kh
		}
	}

	return nil
}

func (s *Server) pressure() keyPressure {
	return newKeyPressure(
		s.numKeys.Load(),
		s.cfg.MaxNumberOfKeys,
	)
}

func (s *Server) addKeyValueSnapshot(
	kv *keyValue,
	revisionLimit int64,
) (kvs *keyValueSnapshot) {

	switch revisionLimit {
	case 0: // no history retained
		return nil
	case -1:
		kvs = newKeySnapshot(kv)
		s.history[kv.Key] = append(s.history[kv.Key], kvs)
	default:
		kvs = newKeySnapshot(kv)
		// if we're at our revision limit, shift all the snapshots down
		// by one, and replace the last one with the new snapshot
		if int64(len(s.history[kv.Key])) == revisionLimit {
			for i := 0; i < int(revisionLimit)-1; i++ {
				s.history[kv.Key][i] = s.history[kv.Key][i+1]
			}
			s.history[kv.Key][int(revisionLimit)-1] = kvs
		} else {
			// otherwise, we just append
			s.history[kv.Key] = append(s.history[kv.Key], kvs)
		}
	}
	s.logger.Info("added new snapshot", kvLogKey, kv, "kv_snapshot", kvs)
	return kvs
}

// newKeyPressure creates+calculates a new keyPressure and returns it
func newKeyPressure(
	numKeys uint64,
	maxKeys uint64,
) keyPressure {
	k := keyPressure{
		Keys: numKeys,
		Max:  maxKeys,
	}
	var used float64
	if k.Max > 0 {
		used = float64(k.Keys) / float64(k.Max)
	}
	rounded := strconv.FormatFloat(used, 'f', 2, 64)
	used, _ = strconv.ParseFloat(rounded, 64)
	k.Used = used
	return k
}

// emit sends the provided event details to the event stream as an Event
func (s *Server) emit(
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

	go func() {
		defer func() {
			if r := recover(); r != nil {
				s.logger.Error(
					"recovered from panic while emitting event",
					"panic", r,
				)
			}
		}()
		s.logger.Debug("broadcasting event", "event", ev)
		s.events <- ev
		s.logger.Debug("event sent", "event", ev)
	}()
}

func (s *Server) sanityCheck(ctx context.Context) error {
	s.mu.RLock()
	reportedKeyCount := s.numKeys.Load()
	keyCount := uint64(len(s.store))
	defer s.mu.RUnlock()

	s.lockMu.RLock()
	reportedLockCount := s.numLocks.Load()
	lockCount := uint64(len(s.locks))
	defer s.lockMu.RUnlock()

	s.reaperMu.RLock()
	reportedReaperCount := s.numReapers.Load()
	reaperCount := uint64(len(s.reapers))
	defer s.reaperMu.RUnlock()
	var errs []error

	s.logger.Log(
		ctx,
		LevelNotice,
		"sanity check",
		slog.Group("keys", "reported", reportedKeyCount, "actual", keyCount),
		slog.Group("locks", "reported", reportedLockCount, "actual", lockCount),
		slog.Group(
			"reapers",
			"reported",
			reportedReaperCount,
			"actual",
			reaperCount,
		),
	)

	if reportedKeyCount != keyCount {
		errs = append(
			errs,
			fmt.Errorf(
				"mismatch between reported and actual key count (%d/%d)",
				reportedKeyCount,
				keyCount,
			),
		)
	}

	if reportedLockCount != lockCount {
		errs = append(
			errs,
			fmt.Errorf(
				"mismatch between reported and actual lock count (%d/%d)",
				reportedLockCount,
				lockCount,
			),
		)
	}
	if reportedLockCount > keyCount {
		errs = append(
			errs,
			fmt.Errorf(
				"more locks than keys (%d/%d)",
				reportedLockCount,
				keyCount,
			),
		)
	}

	if reportedReaperCount != reaperCount {
		errs = append(
			errs,
			fmt.Errorf(
				"mismatch between reported and actual reaper count (%d/%d)",
				reportedReaperCount,
				reaperCount,
			),
		)
	}
	if reportedReaperCount > keyCount {
		errs = append(
			errs,
			fmt.Errorf(
				"more reapers than keys (%d/%d)",
				reportedReaperCount,
				keyCount,
			),
		)
	}

	return errors.Join(errs...)
}

// broadcast sends the given event to the provided snapshot, event logger and
// event stream channels. If any of the channels are nil, they are skipped.
func (s *Server) broadcast(
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

func (s *Server) addKeyValueInfo(kvInfo *keyValue, size uint64) {
	t := kvInfo.Created
	defer s.emit(kvInfo.Key, Created, kvInfo.CreatedBy, &t)
	s.store[kvInfo.Key] = kvInfo
	s.numKeys.Add(1)
	s.totalSize.Add(size)

	s.keyStatMu.Lock()
	_, exists := s.keyStats[kvInfo.Key]
	if !exists {
		s.keyStats[kvInfo.Key] = &keyLifetimeMetric{}
	}
	s.keyStatMu.Unlock()
}

func (s *Server) addStartupKeys() {
	startKv := newKeyValue(
		startupKeyStarted,
		[]byte(time.Now().String()),
		"",
		InternalClientID,
	)
	s.addKeyValueInfo(startKv, uint64(len(startKv.Value)))

	lockDurationKey := newKeyValue(
		startupKeyMaxLockDuration,
		[]byte(fmt.Sprintf("%d", s.cfg.MaxLockDuration)),
		"",
		InternalClientID,
	)
	s.addKeyValueInfo(lockDurationKey, uint64(len(lockDurationKey.Value)))

	minLockDurationKey := newKeyValue(
		startupKeyMinLockDuration,
		[]byte(fmt.Sprintf("%d", s.cfg.MinLockDuration)),
		"",
		InternalClientID,
	)
	s.addKeyValueInfo(minLockDurationKey, uint64(len(minLockDurationKey.Value)))

	keyMinLifespan := newKeyValue(
		startupKeyMinLifespan,
		[]byte(fmt.Sprintf("%d", s.cfg.MinLifespan)),
		"",
		InternalClientID,
	)
	s.addKeyValueInfo(keyMinLifespan, uint64(len(keyMinLifespan.Value)))

	keyMaxKeys := newKeyValue(
		startupKeyMaxKeys,
		[]byte(fmt.Sprintf("%d", s.cfg.MaxNumberOfKeys)),
		"",
		InternalClientID,
	)
	s.addKeyValueInfo(keyMaxKeys, uint64(len(keyMaxKeys.Value)))

	keyPruneInterval := newKeyValue(
		startupKeyPruneInterval,
		[]byte(fmt.Sprintf("%d", s.cfg.PruneInterval)),
		"",
		InternalClientID,
	)
	s.addKeyValueInfo(keyPruneInterval, uint64(len(keyPruneInterval.Value)))

	keyPruneAt := newKeyValue(
		startupKeyPruneAt,
		[]byte(fmt.Sprintf("%d", s.cfg.PruneAt)),
		"",
		InternalClientID,
	)
	s.addKeyValueInfo(keyPruneAt, uint64(len(keyPruneAt.Value)))

	keyPruneTo := newKeyValue(
		startupKeyPruneTo,
		[]byte(fmt.Sprintf("%d", s.cfg.PruneTo)),
		"",
		InternalClientID,
	)
	s.addKeyValueInfo(keyPruneTo, uint64(len(keyPruneTo.Value)))

	keyEagerPruneAt := newKeyValue(
		startupKeyEagerPruneAt,
		[]byte(fmt.Sprintf("%d", s.cfg.EagerPruneAt)),
		"",
		InternalClientID,
	)
	s.addKeyValueInfo(keyEagerPruneAt, uint64(len(keyEagerPruneAt.Value)))

	keyEagerPruneTo := newKeyValue(
		startupKeyEagerPruneTo,
		[]byte(fmt.Sprintf("%d", s.cfg.EagerPruneTo)),
		"",
		InternalClientID,
	)
	s.addKeyValueInfo(keyEagerPruneTo, uint64(len(keyEagerPruneTo.Value)))

	keyMaxValueSize := newKeyValue(
		startupKeyMaxValueSize,
		[]byte(fmt.Sprintf("%d", s.cfg.MaxValueSize)),
		"",
		InternalClientID,
	)
	s.addKeyValueInfo(keyMaxValueSize, uint64(len(keyMaxValueSize.Value)))

	keyMaxKeySize := newKeyValue(
		startupKeyMaxKeySize,
		[]byte(fmt.Sprintf("%d", s.cfg.MaxKeyLength)),
		"",
		InternalClientID,
	)
	s.addKeyValueInfo(keyMaxKeySize, uint64(len(keyMaxKeySize.Value)))

	keyRevisionLimit := newKeyValue(
		startupKeyRevisionLimit,
		[]byte(fmt.Sprintf("%d", s.cfg.RevisionLimit)),
		"",
		InternalClientID,
	)
	s.addKeyValueInfo(keyRevisionLimit, uint64(len(keyRevisionLimit.Value)))

	keySnapshotsEnabled := newKeyValue(
		startupKeySnapshotEnabled,
		[]byte(fmt.Sprintf("%t", s.cfg.Snapshot.Enabled)),
		"",
		InternalClientID,
	)
	s.addKeyValueInfo(
		keySnapshotsEnabled,
		uint64(len(keySnapshotsEnabled.Value)),
	)

	keySnapshotInterval := newKeyValue(
		startupKeySnapshotInterval,
		[]byte(fmt.Sprintf("%d", s.cfg.Snapshot.Interval)),
		"",
		InternalClientID,
	)
	s.addKeyValueInfo(
		keySnapshotInterval,
		uint64(len(keySnapshotInterval.Value)),
	)

	keySubscriberLimit := newKeyValue(
		startupKeyEventStreamSubscriberLimit,
		[]byte(fmt.Sprintf("%d", s.cfg.EventStreamSubscriberLimit)),
		"",
		InternalClientID,
	)
	s.addKeyValueInfo(
		keySubscriberLimit,
		uint64(len(keySubscriberLimit.Value)),
	)
}

func (s *Server) requestLogger(
	ctx context.Context,
) *slog.Logger {
	if clientInfo := s.ClientInfo(ctx); clientInfo != nil {
		return s.logger.With("client", clientInfo)
	}
	return s.logger
}

// stopUnlockTimers runs with Stop to prevent any existing unlock
// timers from firing after the server has stopped.
// This does not delete the lock, because it potentially still
// needs to be saved to a snapshot.
func (s *Server) stopUnlockTimers() {
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
// This does not delete the reaper, because it potentially still
// needs to be saved to a snapshot.
func (s *Server) stopExpirationTimers() {
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

func (s *Server) staleKeys(
	ctx context.Context,
	ignoreKey ...string,
) []*keyValue {
	targetKeys := make([]*keyMetricWithName, 0, len(s.store))
	defer func() {
		for _, km := range targetKeys {
			km.Metric.mu.RUnlock()
		}
	}()
	for k := range s.store {
		if ctx.Err() != nil {
			break
		}
		if strings.HasPrefix(k, ReservedKeyPrefix) {
			continue
		}
		if sliceContains(ignoreKey, k) {
			continue
		}

		ks := s.keyStats[k]
		if ks == nil {
			panic(fmt.Sprintf("key %s has no stats", k))
		}
		ks.mu.RLock()
		targetKeys = append(targetKeys, &keyMetricWithName{Key: k, Metric: ks})
	}

	ordered := sortKeyValueInfoByDates(targetKeys)

	result := make([]*keyValue, 0, len(ordered))
	for _, keyName := range ordered {
		kvInfo, exists := s.store[keyName]
		if !exists {
			s.logger.Warn("key disappeared while pruning", "key", keyName)
		}
		result = append(result, kvInfo)
	}
	return result

}

func (s *Server) deleteKey(key string, clientID string, expunged bool) {
	kvInfo, ok := s.store[key]
	if !ok {
		return
	}

	keyReaper, reaperExists := s.reapers[key]
	if reaperExists {
		s.logger.Debug("stopping expiration timer", "key", key)
		if keyReaper.t != nil {
			_ = keyReaper.t.Stop()
		}
		delete(s.reapers, key)
		s.numReapers.Add(decrementUint64)
	}

	keyLock, exists := s.locks[key]
	if exists {
		if keyLock.t != nil {
			s.logger.Debug("stopping lock timer", "key", key)
			_ = keyLock.t.Stop()
		}
		delete(s.locks, key)
		s.numLocks.Add(decrementUint64)
	}

	delete(s.store, key)
	s.logger.Info(
		"deleted key",
		kvLogKey, kvInfo,
	)

	t := time.Now()
	switch {
	case expunged:
		s.emit(key, Expunged, clientID, &t)
	default:
		s.emit(key, Deleted, clientID, &t)
	}

	s.totalSize.Add(^(kvInfo.Size - 1))
	s.numKeys.Add(decrementUint64)

}

func (s *Server) expungeKey(kvInfo *keyValue) (string, bool) {
	kvInfo.mu.RLock()
	defer kvInfo.mu.RUnlock()
	kvInfo, exists := s.store[kvInfo.Key]
	if !exists {
		return kvInfo.Key, false
	}

	s.deleteKey(kvInfo.Key, InternalClientID, true)
	delete(s.history, kvInfo.Key)

	return kvInfo.Key, true
}

// getKey returns the keyValue associated with the given
// key name. If keyValue is expired (but not Locked), it
// will be deleted.
// nil will be returned for any key that isn't found, or is
// deleted.
func (s *Server) getKey(key string) *keyValue {
	keyInfo, ok := s.store[key]
	if !ok {
		return nil
	}
	return keyInfo
}

func (s *Server) evLoopHandleCreated(event Event) {
	s.keyStatMu.RLock()
	defer s.keyStatMu.RUnlock()

	s.numEventCreated.Add(1)

	keyStat := s.keyStats[event.Key]
	keyStat.mu.Lock()
	defer keyStat.mu.Unlock()

	keyStat.SetCount++

	eventTime := event.Time

	if keyStat.FirstSet == nil {
		keyStat.FirstSet = &eventTime
		keyStat.LastSet = &eventTime
		return
	}

	if keyStat.LastSet == nil || (keyStat.LastSet != nil && keyStat.LastSet.Before(eventTime)) {
		keyStat.LastSet = &eventTime
	}
}

func (s *Server) evLoopHandleUpdated(event Event) {
	s.numEventUpdated.Add(1)
	s.keyStatMu.RLock()
	defer s.keyStatMu.RUnlock()

	keyStat := s.keyStats[event.Key]
	keyStat.mu.Lock()
	defer keyStat.mu.Unlock()

	keyStat.SetCount++
	eventTime := event.Time
	if keyStat.LastSet == nil || (keyStat.LastSet != nil && keyStat.LastSet.Before(eventTime)) {
		keyStat.LastSet = &eventTime
	}
}

func (s *Server) evLoopHandleLocked(event Event) {
	s.numEventLocked.Add(1)
	s.keyStatMu.RLock()
	defer s.keyStatMu.RUnlock()

	keyStat := s.keyStats[event.Key]
	keyStat.mu.Lock()
	defer keyStat.mu.Unlock()

	keyStat.LockCount++

	eventTime := event.Time
	if keyStat.FirstLocked == nil {
		keyStat.FirstLocked = &eventTime
		keyStat.LastLocked = &eventTime
	}
	if keyStat.LastLocked != nil && keyStat.LastLocked.Before(eventTime) {
		keyStat.LastLocked = &eventTime
	}
}

func (s *Server) evLoopHandleAccessed(event Event) {
	s.numEventValueAccessed.Add(1)
	s.keyStatMu.RLock()
	defer s.keyStatMu.RUnlock()

	keyStat := s.keyStats[event.Key]
	keyStat.mu.Lock()
	defer keyStat.mu.Unlock()

	keyStat.AccessCount++

	eventTime := event.Time

	if keyStat.FirstAccessed == nil {
		keyStat.FirstAccessed = &eventTime
		keyStat.LastAccessed = &eventTime
	} else if keyStat.LastAccessed != nil && keyStat.LastAccessed.Before(eventTime) {
		keyStat.LastAccessed = &eventTime
	}
}

// eventLoop is the main handler for emitted Event structs. When the context
// passed to Start is cancelled, the provided channels (snapshotCh, logCh and
// eventStreamCh) are closed. When Stop is called, the main event channel
// is then closed, to avoid blocking sends to the channels, and to accommodate
// events being emitted between the grpc server stopping and the full
// shutdown process completing.
func (s *Server) eventLoop(
	ctx context.Context,
	snapshotCh chan<- Event,
	logCh chan<- Event,
	eventStreamCh chan Event,
) {
	s.logger.Log(ctx, LevelNotice, "started event loop")
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
			s.evLoopHandleCreated(event)
		case Updated:
			s.evLoopHandleUpdated(event)
		case Deleted:
			s.numEventDeleted.Add(1)
		case Expired:
			s.numEventExpired.Add(1)
		case Locked:
			s.evLoopHandleLocked(event)
		case Unlocked:
			s.numEventUnlocked.Add(1)
		case Expunged:
			s.numEventExpunged.Add(1)
		case Accessed:
			s.evLoopHandleAccessed(event)
		case LifespanSet:
			s.numEventLifespanSet.Add(1)
		case LifespanRenewed:
			s.numEventLifespanRenewed.Add(1)
		case NoEvent:
			//
		}
		s.broadcast(ctx, event, snapshotCh, logCh, eventStreamCh)
		evLock.Unlock()
	}
}

// Config specifies server configuration
//
// # Environment Variables
//
// Each of these options can be set as an environment variable, or in a YAML
// configuration file. The environment variable name is the same as the option,
// prefixed with KEYQUARRY_, uppercased, with any period replaced by an underscore.
// For example, ListenAddress can be set as KEYQUARRY_LISTEN_ADDRESS in the
// environment, while [SnapshotConfig].Database would be KEYQUARRY_SNAPSHOT_DATABASE
// in the environment.
//
// # Example YAML configuration
//
//		listen_address: localhost:33969
//		name: foo@bar
//		graceful_shutdown_timeout: 30s
//
//		log_level: INFO
//		log_json: false
//		log_events: false
//
//		event_stream_buffer_size: 1000
//		event_stream_send_timeout: 1s
//		event_stream_subscriber_limit: 0
//
//		snapshot:
//		  database: postgres://keyquarry:keyquarry@localhost:5432/keyquarry
//		  enabled: true
//		  interval: 5m
//
//		monitor_address: localhost:33970
//		prometheus: false
//		expvar: false
//		pprof: false
//		trace: false
//		service_name: keyquarry
//
//		revision_limit: 5
//		max_key_length: 1024
//		max_value_size: 1000000
//		min_lock_duration: 5s
//		max_lock_duration: 5m
//		min_lifespan: 5s
//
//		max_keys: 10000
//		prune_interval: 1h
//		prune_at: 10000
//		prune_tp: 9000
//		eager_prune_at: 20000
//	 eager_prune_to: 15000
//
// # Traces/OpenTelemetry
//
// To enable tracing, set Config.trace=true, and environment variables:
//   - OTEL_EXPORTER_OTLP_ENDPOINT (ex: https://jaeger:4317)
//   - OTEL_EXPORTER_OTLP_TRACES_ENDPOINT (ex: https://jaeger:4317)
//   - OTEL_EXPORTER_OTLP_INSECURE (ex: false)
//
// # Pruning
//
// The [pruner] process is configured by the following settings:
//   - MaxNumberOfKeys
//   - PruneInterval
//   - PruneAt
//   - PruneTo
//   - EagerPruneAt
//   - EagerPruneTo
//
// Pruning will occur only if EagerPruneAt > 0, and/or both
// PruneAt > 0 with PruneInterval set.
// For example, with these settings:
//   - MaxNumberOfKeys=1000
//   - PruneInterval=1h
//   - PruneAt=10000
//   - PruneTo=9000
//   - EagerPruneAt=20000
//   - EagerPruneTo=15000
//
// The number of keys for PruneAt is 10000, and the
// pruneTo number of keys is 9000. The pruner will check the number of keys every hour.
// If the number of keys exceeds 10000, the pruner will attempt to delete
// keys until the number of keys is 9000.
//
// If the number of keys exceeds 20000, the pruner will (almost) immediately attempt
// to delete keys until the number of keys is 15000. Eager pruning is checked when
// a request is made to create a new key. This is helpful for cases where you either
// want to aggressively prune, or to avoid running into MaxNumberOfKeys before
// a scheduled prune is run.
//
// [Locked] keys will not be deleted, nor will keys beginning with
// the reserved prefix (keyquarry).
//
// Keys are weighted by their last access time, last set time, first
// set time and last locked time, oldest first. See [keyLifetimeMetric.StaleScore].
//
// If EagerPrune is true and a client attempts to create a new key, and
// MaxNumberOfKeys has been reached, [pruner.Prune] will be triggered immediately,
// and [ErrMaxKeysReached] will be returned to the client.
type Config struct {
	// ListenAddress is the address to listen on for the gRPC server
	ListenAddress string `json:"listen_address" yaml:"listen_address" mapstructure:"listen_address"`
	url           *url.URL
	// LogEvents enables logging of events (e.g. Key created, updated, deleted)
	// as emitted by the event stream
	LogEvents bool `json:"log_events" yaml:"log_events" mapstructure:"log_events"`

	// LogLevel specifies the log level to use for the server
	LogLevel string `json:"log_level" yaml:"log_level" mapstructure:"log_level"`

	// LogJSON (if true) will set the logging handler to JSON
	LogJSON bool `json:"log_json" yaml:"log_json" mapstructure:"log_json"`

	// Name specifies the `server_name` value to insert into the `snapshots`
	// table when creating snapshots, and is used to filter for existing snapshots
	// to load. This allows multiple servers to save/load snapshots using the
	// same database table.
	Name string `json:"name" yaml:"name" mapstructure:"name"`

	// GracefulStopTimeout is the duration to wait when stopping the
	// GRPC server, before calling `grpc.Server.Stop()`
	GracefulStopTimeout time.Duration `json:"graceful_stop_timeout" yaml:"graceful_stop_timeout" mapstructure:"graceful_stop_timeout"`

	// MinLockDuration sets a floor for the lock duration of a key, when set.
	// If 0, defaults to 5 seconds.
	MinLockDuration time.Duration `json:"min_lock_duration" yaml:"min_lock_duration" mapstructure:"min_lock_duration"`

	// MaxLockDuration sets a ceiling for the lock duration of a key, when set.
	// If 0, defaults to 5 minutes.
	MaxLockDuration time.Duration `json:"max_lock_duration" yaml:"max_lock_duration" mapstructure:"max_lock_duration"`

	// MinLifespan sets a floor for the lifespan of a key, when set.
	// If 0, defaults to 5 seconds.
	MinLifespan time.Duration `json:"min_lifespan" yaml:"min_lifespan" mapstructure:"min_lifespan"`

	// MaxNumberOfKeys limits the number of keys. Set to 0 for no limit.
	// When pruning is enabled, this will be used to determine
	// when to prune, and how many keys to prune.
	MaxNumberOfKeys uint64 `json:"max_keys" yaml:"max_keys" mapstructure:"max_keys"`

	// PruneInterval is the interval at which the server will attempt to
	// prune any keys over the current pruneAt.
	PruneInterval time.Duration `json:"prune_interval" yaml:"prune_interval" mapstructure:"prune_interval"`

	// PruneAt defines the number of keys which, at each PruneInterval, if exceeded, will
	// trigger pruning. For example, if MaxNumberOfKeys is 1000, and PruneAt is 900, and on
	// the next PruneInterval there are 899 keys, nothing will happen. If there are >=900 keys,
	// the pruner will attempt to delete keys until the number of keys remaining is PruneTo.
	PruneAt uint64 `json:"prune_at" yaml:"prune_at" mapstructure:"prune_at"`

	// PruneTo should be between 0 and 1, and smaller than PruneAt (if set).
	// If 0, this will default to PruneAt * 0.9.
	PruneTo uint64 `json:"prune_to" yaml:"prune_to" mapstructure:"prune_to"`

	// EagerPruneAt, if greater than 0, will immediately trigger pruning upon receiving
	// a request to set a new key, if the number of keys has reached or exceeded EagerPruneAt.
	// It will attempt to delete keys until the number of keys remaining is EagerPruneTo.
	// Locked keys will not be pruned.
	EagerPruneAt uint64 `json:"eager_prune_at" yaml:"eager_prune_at" mapstructure:"eager_prune_at"`

	// EagerPruneTo defines a number of keys to prune to, if EagerPruneAt is > 0.
	EagerPruneTo uint64 `json:"eager_prune_to" yaml:"eager_prune_to" mapstructure:"eager_prune_to"`

	// MaxValueSize is the maximum size of a value in bytes. Default: 1000000
	MaxValueSize uint64 `json:"max_value_size" yaml:"max_value_size" mapstructure:"max_value_size"`

	// MaxKeyLength is the maximum length of a key name. Default: 1024
	MaxKeyLength uint64 `json:"max_key_length" yaml:"max_key_length" mapstructure:"max_key_length"`

	// RevisionLimit is the maximum number of revisions to keep for each key.
	RevisionLimit int64 `json:"revision_limit" yaml:"revision_limit" mapstructure:"revision_limit"`

	// PersistentRevisions, if true, will include key version histories in
	// snapshots. When loading from snapshots, any existing history will
	// be loaded as well. If KeepKeyHistoryAfterDelete is also true, the
	// history of deleted keys will be included in snapshots and reloaded.
	// If you have a large number of unique keys and/or a high RevisionLimit,
	// this may cause excessive memory usage.
	// If false, only the history of current keys will be saved.
	PersistentRevisions bool `json:"persistent_revisions" yaml:"persistent_revisions" mapstructure:"persistent_revisions"`

	// KeepKeyHistoryAfterDelete, if true, will not delete a key's revision
	// history after the key is deleted. If PersistentRevisions is also true, these
	// revisions will be included in snapshots and re-loaded. This means that,
	// if key `foo` is deleted, the next time the key is created, its version
	// will be the version of the previous `foo`+1, even across restarts.
	// This may cause excessive memory usage, especially with a high RevisionLimit,
	// and/or with a large number of unique keys.
	// If false, a key's history will be deleted along with the key.
	// Neither option affects the lifetime key metrics.
	// If pruning is enabled, that will take precedence over this setting, and
	// the history of pruned keys will be removed.
	KeepKeyHistoryAfterDelete bool `json:"keep_deleted_key_history" yaml:"keep_deleted_key_history" mapstructure:"keep_deleted_key_history"`

	// DeletedKeyHistoryMaxAge deletes revisions after the given duration, for
	// keys which no longer exist.
	DeletedKeyHistoryMaxAge time.Duration `json:"deleted_key_history_max_age" yaml:"deleted_key_history_max_age" mapstructure:"deleted_key_history_max_age"`

	// SSLKeyfile is the path to the SSL key file to use for the gRPC server
	SSLKeyfile string `json:"ssl_keyfile" yaml:"ssl_keyfile" mapstructure:"ssl_keyfile"`

	// SSLCertfile is the path to the SSL certificate file to use for the gRPC server
	SSLCertfile string `json:"ssl_certfile" yaml:"ssl_certfile" mapstructure:"ssl_certfile"`

	// Snapshot configures the snapshotting process
	Snapshot SnapshotConfig // `json:"snapshot" yaml:"snapshot" mapstructure:"snapshot"`

	// StartFresh will ignore any existing snapshots and start with a clean slate.
	// If snapshots are enabled, they will still be created.
	StartFresh bool `json:"start_fresh" yaml:"start_fresh" mapstructure:"start_fresh"`

	// Logger is the logger to use for the server. If nil, a default
	// logger will be set.
	Logger *slog.Logger `json:"-" yaml:"-" mapstructure:"-"`

	// EventLogger is the logger to use for the event stream. If nil and LogEvents
	// is true, a logger will be created from Config.Logger.
	EventLogger *slog.Logger `json:"-" yaml:"-" mapstructure:"-"`

	// PrivilegedClientID is the client ID that is allowed to set the server
	// into readonly mode
	PrivilegedClientID string `json:"privileged_client_id" yaml:"privileged_client_id" mapstructure:"privileged_client_id"`

	// Readonly sets the server into readonly mode, where only
	// Get, Inspect, ListKeys, Exists, Stats and Register are allowed
	Readonly bool `json:"readonly" yaml:"readonly" mapstructure:"readonly"`

	// EventStreamSendTimeout is the timeout for sending events to subscribers
	EventStreamSendTimeout time.Duration `json:"event_stream_send_timeout" yaml:"event_stream_send_timeout" mapstructure:"event_stream_send_timeout"`

	// EventStreamSubscriberLimit limits the number of subscribers to the event stream (and thus the
	// number of concurrent goroutines). The event logger, snapshotter and
	// pruners do not count against this number. (0 = unlimited)
	EventStreamSubscriberLimit uint64 `json:"event_stream_subscriber_limit" yaml:"event_stream_subscriber_limit" mapstructure:"event_stream_subscriber_limit"`

	// EventStreamBufferSize sets the channel buffer size for the event logger,
	// snapshotter, and event worker in/out channels.
	// If EventStreamSendTimeout is too high, or a client is consistently timing
	// out, the event worker channel may become full, and messages going to the
	// worker will be dropped, so the client won't receive all events. This can be
	// increased to help avoid that.
	// Default: 1000
	EventStreamBufferSize uint64 `json:"event_stream_buffer_size" yaml:"event_stream_buffer_size" mapstructure:"event_stream_buffer_size"`

	// MonitorAddress is the listen address for non-RPC HTTP endpoints
	// when either PPROF, ExpVar and/or Metrics are enabled.
	// This includes /debug/pprof, /debug/vars and /metrics.
	MonitorAddress string `json:"monitor_address" yaml:"monitor_address" mapstructure:"monitor_address"`

	// PPROF enables the /debug/pprof endpoint
	PPROF bool `json:"pprof" yaml:"pprof" mapstructure:"pprof"`

	// ExpVar enables the /debug/vars endpoint
	ExpVar bool `json:"expvar" yaml:"expvar" mapstructure:"expvar"`

	// Prometheus enables the prometheus HTTP /metrics endpoint
	Prometheus bool `json:"prometheus" yaml:"prometheus" mapsstructure:"prometheus"`

	// OTLPTrace enables application tracing telemetry
	OTLPTrace bool `json:"trace" yaml:"trace" mapstructure:"trace"`

	// TracerName sets the name of the OpenTelemetry tracer.
	// Default: keyquarry_server
	TracerName string `json:"tracer_name" yaml:"tracer_name" mapstructure:"tracer_name"`

	// ServiceName specifies the service name to use with
	// `go.opentelemetry.io/otel/semconv` when `Config.trace` is enabled.
	ServiceName string `json:"service_name" yaml:"service_name" mapstructure:"service_name"`
}

// NewConfig returns a Config with default settings
func NewConfig() *Config {
	cfg := &Config{
		GracefulStopTimeout:    DefaultGracefulStopTimeout,
		MaxValueSize:           DefaultMaxValueSize,
		MaxKeyLength:           DefaultMaxKeyLength,
		RevisionLimit:          DefaultRevisionLimit,
		MaxLockDuration:        DefaultMaxLockDuration,
		MinLockDuration:        DefaultMinLockDuration,
		MinLifespan:            DefaultMinLifespan,
		LogEvents:              false,
		LogLevel:               LevelNoticeName,
		EventStreamBufferSize:  DefaultEventStreamBufferSize,
		EventStreamSendTimeout: DefaultEventStreamSendTimeout,
		TracerName:             DefaultTracerName,
		Snapshot: SnapshotConfig{
			Enabled: false,
		},
	}
	cfg.MonitorAddress = DefaultMonitorAddress
	return cfg
}

func (c Config) Validate() error {
	errs := []error{}
	if c.PruneInterval > 0 && c.PruneTo > 0 && c.PruneTo > c.PruneAt {
		errs = append(errs, fmt.Errorf("prune_to must be less than prune_at"))
	}

	if c.EagerPruneAt > 0 && c.EagerPruneAt <= c.EagerPruneTo {
		errs = append(
			errs,
			fmt.Errorf("eager_prune_to must be less than eager_prune_at"),
		)
	}

	return errors.Join(errs...)
}

// URL returns a url.URL for the configured ListenAddress
func (c Config) URL() (*url.URL, error) {
	return parseURL(c.ListenAddress)
}

// MonitorURL returns a url.URL for the configured MonitorAddress
func (c Config) MonitorURL() (*url.URL, error) {
	return parseURL(c.MonitorAddress)
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
		slog.Duration("prune_interval", c.PruneInterval),
		slog.Uint64("prune_at", c.PruneAt),
		slog.Uint64("prune_to", c.PruneTo),
		slog.Uint64("eager_prune_at", c.EagerPruneAt),
		slog.Uint64("eager_prune_to", c.EagerPruneTo),
		slog.String("listen_address", c.ListenAddress),
		slog.String("ssl_keyfile", c.SSLKeyfile),
		slog.String("ssl_certfile", c.SSLCertfile),
		slog.Bool("log_events", c.LogEvents),
		slog.String("log_level", c.LogLevel),
		slog.Bool("log_json", c.LogJSON),
		slog.Bool("readonly", c.Readonly),
		slog.Bool("pprof", c.PPROF),
		slog.Bool("prometheus", c.Prometheus),
		slog.Bool("expvar", c.ExpVar),
		slog.Bool("trace", c.OTLPTrace),
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

// kvStoreState is the state of the Server, used
// for snapshots.
type kvStoreState struct {
	Keys    []*keyValue                    `json:"keys"`
	Clients []*ClientInfo                  `json:"clients"`
	Locks   []*kvLock                      `json:"locks"`
	Reapers []*reaper                      `json:"reapers"`
	Version string                         `json:"version"`
	Metrics map[string]*keyLifetimeMetric  `json:"metrics"`
	History map[string][]*keyValueSnapshot `json:"history"`
}

// reaper manages the lifespan of a key. When the lifespan has
// passed, the reaper deletes the key.
type reaper struct {
	// Key is the name of the key to expire
	Key string `json:"key"`

	// Lifespan is a duration, after which the key will be deleted
	Lifespan time.Duration `json:"lifespan"`

	// LifespanSet is a timestamp for the last time the lifespan was set
	LifespanSet time.Time `json:"lifespan_set"`

	// ID is a UUID for the current reaper. When the reaper function
	// triggers, this ID is checked against the current reaper ID to
	// determine if the key should be deleted. (If the reaper func
	// triggers, but is waiting to unlock a mutex, during which time
	// the lifespan is renewed, the key should not be deleted)
	ID string `json:"id"`

	// renewals is the count of lifespan renewals seen by the current reaper
	renewals atomic.Uint64

	t   *time.Timer
	srv *Server
}

// newKeyReaper initializes a new reaper and associates it
// to the given Server. A [LifespanSet] event will be emitted.
func newKeyReaper(
	srv *Server,
	d time.Duration,
	key string,
	clientID string,
) *reaper {
	now := time.Now()
	r := &reaper{
		Key:         key,
		Lifespan:    d,
		srv:         srv,
		LifespanSet: now,
		ID:          uuid.NewString(),
	}
	srv.reapers[key] = r
	srv.numReapers.Add(1)
	t := time.AfterFunc(d, r.ExpireFunc())
	r.t = t
	srv.emit(key, LifespanSet, clientID, &now)
	return r
}

func (r *reaper) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("key", r.Key),
		slog.Duration("lifespan", r.Lifespan),
		slog.String("id", r.ID),
	)
}

// renew stops the current timer, and starts a new one with the given
// duration. If the duration is nil, the current lifespan is used.
func (r *reaper) renew(d *time.Duration, clientID string) time.Time {
	if r.t != nil {
		_ = r.t.Stop()
	}

	if d != nil {
		r.Lifespan = *d
	}
	ts := time.Now()
	r.LifespanSet = ts

	r.ID = uuid.NewString()
	t := time.AfterFunc(r.Lifespan, r.ExpireFunc())
	r.t = t
	r.renewals.Add(1)
	switch {
	case d == nil:
		r.srv.emit(r.Key, LifespanRenewed, clientID, &ts)
	default:
		r.srv.emit(r.Key, LifespanSet, clientID, &ts)
	}

	return ts.Add(r.Lifespan)
}

// ExpireFunc returns a function to be used with time.AfterFunc to
// expire the key after a given duration
func (r *reaper) ExpireFunc() func() {
	reaperID := r.ID
	return func() {
		// events to emit after expiration is complete
		var events []Event
		defer func() {
			for _, e := range events {
				r.srv.emit(e.Key, e.Event, e.ClientID, &e.Time)
			}
		}()

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

		keyReaper, ok := r.srv.reapers[r.Key]

		if !ok {
			logger.Info("reaper triggered, but not found on server")
			return
		}
		if keyReaper.ID != reaperID {
			logger.Info(
				"reaper triggered, but ID does not match current reaper",
				"current_id",
				reaperID,
				"found_id",
				keyReaper.ID,
			)
			return
		}

		keyLock, locked := r.srv.locks[r.Key]

		if locked {
			logger.Debug("removing lock for expired key")
			delete(r.srv.locks, r.Key)
			_ = keyLock.t.Stop()
			events = append(
				events,
				Event{
					Key:      r.Key,
					Event:    Unlocked,
					ClientID: InternalClientID,
					Time:     time.Now(),
				},
			)
			r.srv.numLocks.Add(decrementUint64)
		}

		delete(r.srv.reapers, r.Key)
		r.srv.numReapers.Add(decrementUint64)

		kvInfo, exists := r.srv.store[r.Key]
		if !exists {
			logger.Info("reaper triggered, but key no longer exists")
			return
		}
		delete(r.srv.store, r.Key)
		r.srv.numKeys.Add(decrementUint64)
		r.srv.totalSize.Add(^(kvInfo.Size - 1))

		events = append(
			events,
			Event{
				Key:      r.Key,
				Event:    Expired,
				ClientID: InternalClientID,
				Time:     time.Now(),
			},
		)
	}
}

// kvLock maintains a record of a lock on a key. Server maintains
// a registry of current locks (mapped by key) to each kvLock
// in Server.locks. When a kvLock expires, it will be removed from
// that registry, and Server.store. If the key isn't registered,
// nothing happens. If it is, but the current lock isn't the same as the
// one associated with UnlockFunc, nothing happens.
type kvLock struct {
	// Key is the name of the key to hold a lock on
	Key string `json:"key"`
	// Duration is the amount of time to hold the lock before releasing
	Duration time.Duration `json:"duration"`
	// ClientID is the client that owns this lock
	ClientID string `json:"client_id"`
	// Created is the time the lock was created
	Created time.Time `json:"created"`
	// UUID to identify the current lock + lockfunc
	ID  string `json:"id"`
	t   *time.Timer
	srv *Server
}

// newKeyLock creates a new lock for a key, and starts the timer
func newKeyLock(
	srv *Server,
	d time.Duration,
	key string,
	clientID string,
) *kvLock {
	now := time.Now()
	k := &kvLock{
		Key:      key,
		ClientID: clientID,
		Duration: d,
		srv:      srv,
		Created:  now,
		ID:       uuid.NewString(),
	}
	t := time.AfterFunc(d, k.UnlockFunc())
	k.t = t
	srv.locks[key] = k
	srv.numLocks.Add(1)
	k.srv.emit(key, Locked, clientID, &now)
	return k
}

// renew stops the current timer (if there is one), and starts a new timer
// with the given duration
func (k *kvLock) renew(d time.Duration, clientID string) time.Time {
	if k.t != nil {
		_ = k.t.Stop()
	}
	ts := time.Now()
	k.Duration = d
	k.ID = uuid.NewString()
	k.t = time.AfterFunc(d, k.UnlockFunc())
	k.srv.emit(k.Key, Locked, clientID, &ts)
	return ts.Add(d)
}

// UnlockFunc returns a function to be used with time.AfterFunc to
// unlock a key after the timer has passed. If the key is expired, it
// will be deleted upon unlocking.
func (k *kvLock) UnlockFunc() func() {
	lockID := k.ID
	return func() {
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
		if lock.ID != lockID {
			return
		}
		logger := k.srv.logger.With(loggerKey, "lock", "key_lock", k)

		logger.Info("unlocking key on trigger")

		delete(k.srv.locks, k.Key)
		k.srv.numLocks.Add(decrementUint64)
		now := time.Now()
		k.srv.emit(k.Key, Unlocked, InternalClientID, &now)
	}
}

func (k *kvLock) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("key", k.Key),
		slog.String(clientIDKey, k.ClientID),
		slog.Duration("duration", k.Duration),
		slog.String("id", k.ID),
	)
}

// KQError is a KeyQuarry error type that implements the GRPCStatus interface
type KQError struct {
	Message string `json:"message" yaml:"message"`
	Code    codes.Code
}

func (e KQError) Error() string {
	return e.Message
}

func (e KQError) GRPCStatus() *status.Status {
	return status.New(e.Code, e.Message)
}

// ClientInfo describes when a client ID was first seen
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

// keyPressure provides insight on the current key capacity
type keyPressure struct {
	// Keys is the total number of keys
	Keys uint64 `json:"keys"`
	// Max is the currently configured max number of keys allowed
	Max uint64 `json:"max"`
	// Used is a percentage of the key capacity used.
	// 0 = none, 1 = full, 0.5 = 50%, ...
	Used float64 `json:"percent_used"`
}

func (k *keyPressure) LogValue() slog.Value {
	return slog.GroupValue(
		slog.Uint64("keys", k.Keys),
		slog.Uint64("max", k.Max),
		slog.Any("used", k.Used),
	)
}

func ClientIDInterceptor(srv *Server) grpc.UnaryServerInterceptor {
	f := func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (resp any, err error) {
		if _, err = srv.ClientIDFromContext(ctx); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
	return f
}

func sliceContains[V comparable](slice []V, value V) bool {
	for _, v := range slice {
		if v == value {
			return true
		}
	}
	return false
}

type keyMetricWithName struct {
	Key    string
	Metric *keyLifetimeMetric
}

func sortKeyValueInfoByDates(keys []*keyMetricWithName) []string {
	now := time.Now()
	ordered := make([]string, 0, len(keys))
	sort.Slice(
		keys, func(i, j int) bool {
			ki := keys[i]
			kj := keys[j]
			kiScore := ki.Metric.StaleScore(now)
			kjScore := kj.Metric.StaleScore(now)
			return kiScore < kjScore
		},
	)
	for _, k := range keys {
		ordered = append(ordered, k.Key)
	}
	return ordered
}

// parseURL takes the given string, and attempts to parse it to
// a URL with scheme 'tcp' or 'unix', setting a default port if
// one isn't specified (TCP only)
func parseURL(s string) (*url.URL, error) {
	var network string
	var host string

	n, h, found := strings.Cut(s, "://")
	switch {
	case found:
		host = h
		if n == "" {
			network = DefaultNetwork
		} else {
			network = n
		}
	default:
		host = n
		network = DefaultNetwork
	}

	u := &url.URL{Scheme: network, Host: host}
	if u.Port() == "" && u.Scheme != "unix" {
		u.Host = fmt.Sprintf("%s:%d", u.Host, DefaultPort)
	}
	return u, nil
}

func setupOTelSDK(
	ctx context.Context,
	serviceName,
	serviceVersion string,
	traces bool,
	metrics bool,
) (
	traceProvider *tsdk.TracerProvider,
	metricProvider *msdk.MeterProvider,
	err error,
) {
	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion(serviceVersion),
		),
	)
	if err != nil {
		return nil, nil, err
	}

	// Set up propagator.
	prop := propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)
	otel.SetTextMapPropagator(prop)
	if traces {
		traceExporter, te := otlptracegrpc.New(ctx)
		if te != nil {
			return nil, nil, te
		}

		traceProvider = tsdk.NewTracerProvider(
			tsdk.WithBatcher(
				traceExporter,
				// Default is 5s. Set to 1s for demonstrative purposes.
				tsdk.WithBatchTimeout(time.Second),
			),
			tsdk.WithResource(res),
		)
	}

	if metrics {
		metricExporter, merr := otlpmetricgrpc.New(ctx)
		if merr != nil {
			return nil, nil, err
		}

		metricProvider = msdk.NewMeterProvider(
			msdk.WithResource(res),
			msdk.WithReader(
				msdk.NewPeriodicReader(
					metricExporter,
					// Default is 1m. Set to 3s for demonstrative purposes.
					msdk.WithInterval(3*time.Second),
				),
			),
		)
	}

	return traceProvider, metricProvider, nil
}

func getLogLevel(levelName string) (slog.Level, bool) {
	levelName = strings.ToUpper(levelName)
	switch levelName {
	case slog.LevelDebug.String():
		return slog.LevelDebug, true
	case slog.LevelInfo.String():
		return slog.LevelInfo, true
	case slog.LevelWarn.String():
		return slog.LevelWarn, true
	case slog.LevelError.String():
		return slog.LevelError, true
	case LevelEventName:
		return LevelEvent, true
	case LevelNoticeName:
		return LevelNotice, true
	default:
		return LevelNotice, false
	}
}
