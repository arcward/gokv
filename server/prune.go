package server

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// pruner is used to prune keys from the store when the number of keys
// exceeds the configured pruneAt. pruner does it specifically on a
// configured interval. Set Config.EagerPrune to true to prune keys
// when they hit their limit, rather than waiting for the interval.
type pruner struct {
	pruneAt      uint64 // pruneAt is copied from Config.PruneAt
	pruneTo      uint64 // pruneTo is copied from Config.PruneTo
	eagerPruneAt uint64
	eagerPruneTo uint64
	interval     time.Duration // interval is copied from Config.PruneInterval
	srv          *Server
	logger       *slog.Logger
	t            *time.Ticker
	mu           sync.Mutex
}

func (p *pruner) Run(ctx context.Context) error {
	p.logger.Info("starting pruner")

	switch p.interval {
	case 0:
		p.t = &time.Ticker{}
		p.t.Stop()
	default:
		p.t = time.NewTicker(p.interval)
	}
	defer p.t.Stop()

	for {
		select {
		case <-ctx.Done():
			p.t.Stop()
			return nil
		case <-p.t.C:
			if p.pruneAt == 0 {
				p.logger.Debug("no key maximum, skipping scheduled prune")
				continue
			}
			if p.srv.numKeys.Load() < p.pruneAt {
				continue
			}
			go func() {
				p.logger.Info("pruning keys")
				keysPruned := p.Prune(ctx, p.pruneTo)
				p.logger.Debug(
					"finished pruning keys",
					slog.Any("pruned", keysPruned),
					slog.Uint64("new_count", p.srv.numKeys.Load()),
				)
			}()
		case key := <-p.srv.eagerPruneCh:
			if p.eagerPruneAt == 0 {
				continue
			}

			currentCt := p.srv.numKeys.Load()
			if currentCt < p.eagerPruneAt {
				p.logger.Info(
					"eager prune signal received, but key count is below eager prune threshold",
					"current_count", currentCt,
					"eager_prune_at", p.eagerPruneAt,
				)
				continue
			}

			p.logger.Info("received eager prune signal")
			p.srv.numEagerPruneTriggered.Add(1)
			keysPruned := p.Prune(ctx, p.eagerPruneTo, key)
			p.logger.Debug(
				"finished pruning keys",
				slog.Any("pruned", keysPruned),
				slog.Uint64("beginning_count", currentCt),
				slog.Uint64("new_count", p.srv.numKeys.Load()),
			)
		}
	}
}

// Prune deletes all unlocked keys past the configured pruneAt, to
// the pruneTo lower pruneAt, if possible
func (p *pruner) Prune(
	ctx context.Context,
	targetCount uint64,
	ignoreKey ...string,
) []string {
	reportedCt := p.srv.numKeys.Load()
	if reportedCt < targetCount {
		p.logger.Info(
			"reported key count is below target",
			"reported", reportedCt,
			"target", targetCount,
		)
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	p.srv.mu.Lock()
	defer p.srv.mu.Unlock()

	pressure := p.srv.pressure()
	p.logger.Info(
		"starting prune",
		"pressure", pressure,
		"prune_to", targetCount,
	)

	p.srv.keyStatMu.RLock()

	p.srv.lockMu.Lock()
	defer p.srv.lockMu.Unlock()

	p.srv.reaperMu.Lock()
	defer p.srv.reaperMu.Unlock()

	p.srv.hmu.Lock()
	defer p.srv.hmu.Unlock()

	for lockedKey := range p.srv.locks {
		ignoreKey = append(ignoreKey, lockedKey)
	}

	defer p.srv.numPruneCompleted.Add(1)

	p.logger.Info(
		"pruning keys",
		"target_count",
		targetCount,
	)

	toRemove := p.srv.staleKeys(ctx, ignoreKey...)
	removeLimit := reportedCt - targetCount
	if int(removeLimit) > len(toRemove) {
		removeLimit = uint64(len(toRemove))
	}
	p.logger.Log(
		ctx,
		LevelNotice,
		"identifying keys",
		"limit",
		removeLimit,
		"reported",
		reportedCt,
	)
	toRemove = toRemove[:removeLimit]
	p.logger.Warn("targeting keys", "count", len(toRemove))

	p.srv.keyStatMu.RUnlock()
	removed := make([]string, 0)
	for _, kvInfo := range toRemove {
		key, removedKey := p.srv.expungeKey(kvInfo)
		if removedKey {
			removed = append(removed, key)
		}
	}
	return removed
}

func newPruner(srv *Server) *pruner {
	p := &pruner{
		srv:          srv,
		pruneAt:      srv.cfg.PruneAt,
		pruneTo:      srv.cfg.PruneTo,
		interval:     srv.cfg.PruneInterval,
		eagerPruneAt: srv.cfg.EagerPruneAt,
		eagerPruneTo: srv.cfg.EagerPruneTo,
	}
	p.logger = srv.logger.With(
		slog.String(loggerKey, "pruner"),
		slog.Uint64("prune_at", p.pruneAt),
		slog.Uint64("prune_to", p.pruneTo),
		slog.Duration("interval", p.interval),
		slog.Uint64("eager_prune_at", p.eagerPruneAt),
		slog.Uint64("eager_prune_to", p.eagerPruneTo),
	).WithGroup("pruner")

	srv.pruner = p
	return p
}
