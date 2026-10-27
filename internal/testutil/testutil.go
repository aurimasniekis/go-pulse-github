// Package testutil holds shared test helpers.
package testutil

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/aurimasniekis/go-pulse-github/internal/config"
	"github.com/aurimasniekis/go-pulse-github/internal/storage"
)

// OpenDB opens a fresh state database in a temp dir.
func OpenDB(t testing.TB) *storage.DB {
	t.Helper()
	db, err := storage.Open(context.Background(), filepath.Join(t.TempDir(), "state.db"), storage.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// Logger discards output unless PULSE_GITHUB_TEST_LOG is set.
func Logger() *slog.Logger {
	if os.Getenv("PULSE_GITHUB_TEST_LOG") != "" {
		return slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

// WriteSecret writes a secret file and returns its path.
func WriteSecret(t testing.TB, value string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(p, []byte(value), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// Clock is a manually advanced clock.
type Clock struct {
	mu sync.Mutex
	t  time.Time
}

// NewClock starts a clock at t.
func NewClock(t time.Time) *Clock { return &Clock{t: t} }

// Now returns the current fake time.
func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

// Advance moves the clock forward.
func (c *Clock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// Config parses a YAML configuration without environment overrides, failing the test on error.
func Config(t testing.TB, yaml string) *config.Config {
	t.Helper()
	c, err := config.Parse([]byte(yaml), nil)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	return c
}
