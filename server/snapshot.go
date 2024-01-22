package server

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"
)

// snapshotter is used to create snapshots of the current state of the
// server at a given interval.
type snapshotter struct {
	server  *Server
	ticker  *time.Ticker
	logger  *slog.Logger
	mu      sync.RWMutex
	db      *sql.DB
	dialect *SQLDialect
	cfg     SnapshotConfig
}

// snapshot marshals Server to JSON and writes it to the configured
// database, returning the row ID of the snapshot and any errors.
func (s *snapshotter) snapshot(ctx context.Context) (rowID int64, err error) {
	cfg := s.cfg
	begin := time.Now()
	if s.db == nil {
		s.logger.Debug("connecting to db")
		db, e := s.dialect.DB(cfg.Database)
		if e != nil {
			return 0, e
		}

		s.db = db
		defer s.db.Close()

	}
	spanCtx, span := s.server.tracer.Start(ctx, "snapshotter")
	defer span.End()
	span.SetName("snapshot")

	s.logger.Info("snapshotting", "begin", begin)

	snapshotData, err := json.Marshal(s.server)
	if err != nil {
		return rowID, fmt.Errorf("unable to marshal snapshot: %w", err)
	}

	tx, e := s.db.Begin()
	if e != nil {
		s.logger.Error("error opening db transaction", "error", err)
		return rowID, fmt.Errorf("error starting transaction: %w", e)
	}

	slog.Info("DB stats", "stats", s.db.Stats())

	var elapsed time.Duration
	var end time.Time

	rowID, err = s.dialect.addSnapshot(
		spanCtx,
		s.db,
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

// Run starts the snapshotter, triggering every SnapshotConfig.Interval.
// If the interval has passed and no change has been detected, the snapshot
// is skipped. A change is identified if any event other than NoEvent
// and Accessed has been seen since the last snapshot.
func (s *snapshotter) Run(ctx context.Context, events <-chan Event) error {
	s.logger.Log(ctx, LevelNotice, "started snapshotter")
	cfg := s.cfg
	db, err := s.dialect.DB(cfg.Database)
	if err != nil {
		s.logger.Error("error connecting to db", "error", err)
		return err
	}
	s.db = db
	defer db.Close()

	var changeDetected bool
	wg := sync.WaitGroup{}

	cdlock := sync.Mutex{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for ev := range events {
			s.logger.Debug("snapshotter saw event", "event", ev)

			if strings.HasPrefix(ev.Key, ReservedKeyPrefix) {
				continue
			}
			switch ev.Event {
			case NoEvent, Accessed:
			default:
				cdlock.Lock()
				if !changeDetected {
					changeDetected = true
					s.logger.Debug("change detected", "event", ev)
				}
				cdlock.Unlock()
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		s.server.cfgMu.RLock()

		var ticker *time.Ticker
		snapshotInterval := cfg.Interval
		if s.ticker == nil {
			s.ticker = time.NewTicker(snapshotInterval)
		}
		ticker = s.ticker

		defer ticker.Stop()
		s.logger.Info("starting snapshotter")

		var tickCounter int
		interval := cfg.Interval

		s.server.cfgMu.RUnlock()
		s.logger.Info("snapshotter ready")

		snapshotResult := make(
			chan struct {
				ID    int64
				Error error
			}, 1,
		)
		defer close(snapshotResult)

		for {
			select {
			case <-ctx.Done():
				s.logger.Info("stopping snapshotter")
				return
			case <-ticker.C:
				tickCounter++
				cdlock.Lock()
				if !changeDetected {
					s.logger.Info(
						"no change detected, skipping snapshot",
						"tick",
						tickCounter,
					)
					cdlock.Unlock()
					continue
				}

				s.logger.Info(
					"change detected, snapshotting",
					"tick",
					tickCounter,
				)

				go func() {
					s.server.cfgMu.RLock()
					defer s.server.cfgMu.RUnlock()

					s.server.mu.RLock()
					defer s.server.mu.RUnlock()

					s.server.lockMu.RLock()
					defer s.server.lockMu.RUnlock()

					s.server.reaperMu.RLock()
					defer s.server.reaperMu.RUnlock()

					s.server.cmu.RLock()
					defer s.server.cmu.RUnlock()

					snapshotID, snapshotErr := s.snapshot(ctx)
					snapshotResult <- struct {
						ID    int64
						Error error
					}{
						ID:    snapshotID,
						Error: snapshotErr,
					}
				}()

				sr := <-snapshotResult
				switch {
				case sr.Error == nil:
					changeDetected = false
					s.logger.LogAttrs(
						ctx,
						LevelNotice,
						"created snapshot",
						slog.Time("next_snapshot", time.Now().Add(interval)),
						slog.Uint64(
							"count",
							s.server.numSnapshotsCreated.Load(),
						),
						slog.Int64("snapshot_id", sr.ID),
					)
				default:
					s.logger.Error(
						"snapshot failed",
						slog.String("error", sr.Error.Error()),
						slog.Int("tick", tickCounter),
					)
				}
				cdlock.Unlock()
			}
		}
	}()

	wg.Wait()
	s.logger.Log(ctx, LevelNotice, "snapshotter finished")
	return nil
}

// SnapshotConfig defines the configuration for the snapshotter process.
type SnapshotConfig struct {
	// Enabled enables/disables snapshotting
	Enabled bool `json:"enabled" yaml:"enabled" mapstructure:"enabled"`

	// Interval defines how often to take snapshots. If Interval is 0, snapshots,
	// if enabled, will only be created on shutdown.
	Interval time.Duration `json:"interval" yaml:"interval" mapstructure:"interval"`

	// Database is the connection string for the database to use for snapshots
	Database string `json:"database" yaml:"database" mapstructure:"database"`
}

func (s SnapshotConfig) LogValue() slog.Value {
	return slog.GroupValue(
		slog.Duration("interval", s.Interval),
		slog.Bool("enabled", s.Enabled),
	)
}

func newSnapshotter(s *Server, config SnapshotConfig) (*snapshotter, error) {
	logger := s.logger.With(
		loggerKey,
		"snapshotter",
	).WithGroup("snapshotter").With("config", config)

	var ticker *time.Ticker
	switch config.Interval {
	case 0:
		ticker = &time.Ticker{}
	default:
		ticker = time.NewTicker(config.Interval)
	}

	connStr := config.Database
	dialect := GetDialect(connStr)
	if dialect == nil {
		return nil, fmt.Errorf("unsupported or invalid database driver in connection string")
	}

	return &snapshotter{
		logger:  logger,
		server:  s,
		ticker:  ticker,
		dialect: dialect,
		cfg:     config,
	}, nil
}

func NewServerFromSnapshot(data []byte, cfg *Config) (*Server, error) {
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

	srv, err := New(cfg)
	if err != nil {
		return nil, err
	}

	if err = json.Unmarshal(data, srv); err != nil {
		return nil, fmt.Errorf("unable to unmarshal snapshot: %w", err)
	}
	return srv, nil
}
