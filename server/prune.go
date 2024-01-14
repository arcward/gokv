package server

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

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
			keysPruned, pruneErr := p.Prune(ctx)
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

// Prune deletes all unlocked keys past the configured threshold, to
// the target lower threshold, if possible
func (p *Pruner) Prune(ctx context.Context) ([]string, error) {
	s := p.srv

	p.srv.cfgMu.RLock()
	defer p.srv.cfgMu.RUnlock()

	s.mu.Lock()
	defer s.mu.Unlock()

	s.lockMu.Lock()
	defer s.lockMu.Unlock()

	s.reaperMu.Lock()
	defer s.reaperMu.Unlock()

	s.keyStatMu.RLock()
	defer s.keyStatMu.RUnlock()

	pressure := s.pressure()
	s.logger.Info("starting prune", "pressure", pressure)
	started := time.Now()

	if pressure.KeysOverThreshold < 0 {
		s.logger.Info("actual count below threshold, skipping")
		s.addPruneLog(PruneLog{Time: started, Eager: false})
		return nil, nil
	}

	targetCount := uint64(float64(pressure.Max) * s.cfg.PruneTarget)

	pruned := s.pruneNumKeys(ctx, int(targetCount), false)

	if len(pruned) == 0 {
		s.addPruneLog(PruneLog{Time: started, Eager: false})
	}
	return pruned, nil
}

type PruneLog struct {
	Time       time.Time `json:"time"`
	KeysPruned []string  `json:"keys_pruned"`
	Eager      bool      `json:"eager"`
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
