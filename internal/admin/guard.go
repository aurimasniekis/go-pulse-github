// Package admin serves health, readiness, metrics and optional profiling, and owns the storage admission guard.
package admin

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aurimasniekis/go-pulse-github/internal/observability"
	"github.com/aurimasniekis/go-pulse-github/internal/storage"
)

// ErrShuttingDown is returned by Admit during shutdown.
var ErrShuttingDown = errors.New("shutting down")

// GuardConfig sets storage thresholds.
type GuardConfig struct {
	MinFreeBytes    int64
	MaxDBBytes      int64
	MaxInboxPending int
}

// Guard tracks whether the state database can safely accept new data. Webhook admission, readiness and pollers use
// it; a Pulse outage does not affect it because captured data waits durably in the outbox.
type Guard struct {
	db      *storage.DB
	cfg     GuardConfig
	metrics *observability.Metrics
	log     *slog.Logger
	now     func() time.Time
	// FreeBytes is replaceable in tests.
	FreeBytes func(path string) (uint64, error)

	shutting atomic.Bool
	mu       sync.RWMutex
	err      error
	checked  time.Time
}

// NewGuard builds a guard. Call Check before serving.
func NewGuard(db *storage.DB, cfg GuardConfig, m *observability.Metrics, log *slog.Logger, now func() time.Time) *Guard {
	return &Guard{db: db, cfg: cfg, metrics: m, log: log.With(observability.FieldComponent, "storage_guard"), now: now,
		FreeBytes: storage.FreeBytes, err: errors.New("not checked yet")}
}

// Check probes the database (ping, write heartbeat), free space, size and inbox depth.
func (g *Guard) Check(ctx context.Context) error {
	err := g.check(ctx)
	g.mu.Lock()
	prev := g.err
	g.err, g.checked = err, g.now()
	g.mu.Unlock()
	if g.metrics != nil {
		if err == nil {
			g.metrics.AdmissionOpen.Set(1)
		} else {
			g.metrics.AdmissionOpen.Set(0)
		}
	}
	switch {
	case err != nil && (prev == nil || prev.Error() != err.Error()):
		g.log.Error("storage admission closed", observability.Err(err))
	case err == nil && prev != nil:
		g.log.Info("storage admission open")
	}
	return err
}

func (g *Guard) check(ctx context.Context) error {
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := g.db.Ping(cctx); err != nil {
		g.sqlErr("ping")
		return fmt.Errorf("database unavailable: %w", err)
	}
	if err := g.db.Heartbeat(cctx, g.now()); err != nil {
		g.sqlErr("heartbeat")
		return fmt.Errorf("database not writable: %w", err)
	}
	free, err := g.FreeBytes(filepath.Dir(g.db.Path()))
	if err != nil {
		return fmt.Errorf("free space unknown: %w", err)
	}
	size := g.db.SizeBytes()
	if g.metrics != nil {
		g.metrics.StorageFreeBytes.Set(float64(free))
		g.metrics.StorageDBBytes.Set(float64(size))
	}
	if g.cfg.MinFreeBytes > 0 && free < uint64(g.cfg.MinFreeBytes) { //nolint:gosec // validated non-negative
		return fmt.Errorf("free space %d bytes is below storage.min_free_bytes %d", free, g.cfg.MinFreeBytes)
	}
	if g.cfg.MaxDBBytes > 0 && size > g.cfg.MaxDBBytes {
		return fmt.Errorf("database size %d bytes exceeds storage.max_db_bytes %d", size, g.cfg.MaxDBBytes)
	}
	n, err := g.db.InboxPending(cctx)
	if err != nil {
		g.sqlErr("inbox_pending")
		return fmt.Errorf("database unavailable: %w", err)
	}
	if n >= g.cfg.MaxInboxPending {
		return fmt.Errorf("webhook inbox has %d pending deliveries (storage.max_inbox_pending %d)", n, g.cfg.MaxInboxPending)
	}
	return nil
}

func (g *Guard) sqlErr(op string) {
	if g.metrics != nil {
		g.metrics.SQLiteErrors.WithLabelValues(op).Inc()
	}
}

// Run re-checks periodically until ctx ends.
func (g *Guard) Run(ctx context.Context, every time.Duration) error {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			_ = g.Check(ctx)
		}
	}
}

// Err returns the current admission problem (nil when open).
func (g *Guard) Err() error {
	if g.shutting.Load() {
		return ErrShuttingDown
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.err
}

// Admit implements the webhook admission check.
func (g *Guard) Admit(context.Context) error { return g.Err() }

// Pressure reports whether collectors should pause fetching new data.
func (g *Guard) Pressure() bool { return g.Err() != nil }

// SetShuttingDown closes admission for shutdown.
func (g *Guard) SetShuttingDown() {
	g.shutting.Store(true)
	if g.metrics != nil {
		g.metrics.AdmissionOpen.Set(0)
	}
}
