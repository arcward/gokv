package server

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"
)

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

func (s *Snapshotter) Logger() *slog.Logger {
	return s.logger
}

func (s *Snapshotter) DBSnapshot(ctx context.Context) (rowID int64, err error) {
	begin := time.Now()
	spanCtx, span := s.server.Tracer.Start(ctx, "snapshotter")
	defer span.End()
	span.SetName("db_snapshot")
	// driverName, connStr := DBConnStr(s.server.cfg.Snapshot.Database)
	// db, err := DBConn(ctx, driverName, connStr)
	connStr := s.server.cfg.Snapshot.Database
	dialect := GetDialect(connStr)
	if dialect == nil {
		s.logger.Error("error getting dialect", "error", err)
	}

	s.logger.Info("snapshotting")

	snapshotData, err := json.Marshal(s.server)
	if err != nil {
		return rowID, fmt.Errorf("unable to marshal snapshot: %w", err)
	}

	db, err := dialect.DBConn(connStr)
	if err != nil {
		s.logger.Error("error connecting to db", "error", err)
		return rowID, err
	}
	defer func() {
		err = db.Close()
	}()

	tx, e := db.Begin()
	if e != nil {
		s.logger.Error("error opening db transaction", "error", err)
		return rowID, fmt.Errorf("error starting transaction: %w", e)
	}

	slog.Info("DB stats", "stats", db.Stats())

	var elapsed time.Duration
	var end time.Time

	rowID, err = dialect.AddSnapshot(
		spanCtx,
		db,
		s.server.cfg.Name,
		snapshotData,
	)
	if err != nil {
		s.logger.Error("error inserting snapshot", "error", err)
		_ = tx.Rollback()
		return rowID, err
	}
	err = tx.Commit()
	if err != nil {
		s.logger.Error("error committing snapshot", "error", err)
		return rowID, err
	}

	end = time.Now()
	elapsed = end.Sub(begin)
	s.logger.Info(
		"snapshot complete",
		slog.Time("start_at", begin),
		slog.Time("end_at", end),
		slog.Duration("elapsed", elapsed),
		slog.Int64("snapshots.id", rowID),
	)
	s.server.numSnapshotsCreated.Add(1)
	return rowID, nil

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
			case NoEvent, Accessed:
				//
			default:
				if strings.HasPrefix(ev.Key, reservedPrefix) {
					continue
				}
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
			snapshotInterval := s.server.cfg.Snapshot.Interval
			if s.ticker == nil {
				s.ticker = time.NewTicker(snapshotInterval)
			}
			ticker = s.ticker

			defer ticker.Stop()
			s.logger.Info("starting snapshotter")
			var tickCounter int
			interval := s.server.cfg.Snapshot.Interval
			s.server.cfgMu.RUnlock()
			s.logger.Info("snapshotter ready")

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
						s.logger.Info(
							"change detected, snapshotting",
							"tick",
							tickCounter,
						)

						s.server.cfgMu.RLock()
						s.server.mu.RLock()
						s.server.lockMu.RLock()
						s.server.reaperMu.RLock()
						s.server.cmu.RLock()

						snapshotID, e := s.DBSnapshot(ctx)

						changeDetected = false
						cdlock.Unlock()
						s.server.cfgMu.RUnlock()
						s.server.cmu.RUnlock()
						s.server.reaperMu.RUnlock()
						s.server.lockMu.RUnlock()
						s.server.mu.RUnlock()

						if e != nil {
							s.logger.Error(
								"snapshot failed",
								slog.String("error", e.Error()),
								slog.Int("tick", tickCounter),
							)
							continue
						}
						s.logger.Info(
							"created snapshot",
							slog.Time(
								"next_snapshot",
								time.Now().Add(interval),
							),
							slog.Int("tick", tickCounter),
							slog.Int64("snapshots.id", snapshotID),
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

type SnapshotConfig struct {
	// Interval defines how often to take snapshots. If Interval is 0, snapshots,
	// if enabled, will only be created on shutdown.
	Interval time.Duration `json:"interval" yaml:"interval" mapstructure:"interval"`
	// Enabled enables/disables snapshotting
	Enabled bool `json:"enabled" yaml:"enabled" mapstructure:"enabled"`
	// Database is a connection string (sqlite or postgres) to use for storing
	// and loading snapshots
	Database string `json:"database" yaml:"database" mapstructure:"database"`
	// ExpireAfter is the amount of time to keep snapshots before expiring them
	// (does not apply to the most recent snapshot)
	ExpireAfter time.Duration `json:"expire_after" yaml:"expire_after" mapstructure:"expire_after"`
}

func (s SnapshotConfig) LogValue() slog.Value {
	return slog.GroupValue(
		slog.Duration("interval", s.Interval),
		slog.Bool("enabled", s.Enabled),
	)
}

func NewSnapshotter(
	s *KeyValueStore,
) (*Snapshotter, error) {
	config := s.cfg.Snapshot
	logger := s.logger.With(
		loggerKey,
		"snapshotter",
	).WithGroup("snapshotter").With("config", config)

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
