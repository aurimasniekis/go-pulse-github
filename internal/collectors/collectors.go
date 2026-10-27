// Package collectors holds helpers shared by the polling collectors.
package collectors

import (
	"context"
	"math/rand/v2"
	"time"

	"github.com/aurimasniekis/go-pulse-github/internal/githubapi"
)

// Pressure reports storage pressure; collectors pause fetching while it is true.
type Pressure interface {
	Pressure() bool
}

// Jitter returns d ± 10 %, so accounts polling at the same interval spread out.
func Jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	spread := int64(d) / 5
	return d - time.Duration(spread/2) + time.Duration(rand.Int64N(spread+1)) //nolint:gosec // jitter
}

// Sleep waits for d or until ctx ends.
func Sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// AccessLost reports whether an error means the credential or its permissions stopped working (as opposed to
// transient failures or rate limits).
func AccessLost(err error) bool {
	switch githubapi.ClassOf(err) {
	case githubapi.ClassUnauthorized, githubapi.ClassForbidden, githubapi.ClassCredential:
		return true
	}
	return false
}

// NextDelay picks the wait before the next poll: the configured interval, never below GitHub's X-Poll-Interval, and
// at least the rate-limit retry time after a limited request.
func NextDelay(interval, pollInterval time.Duration, err error) time.Duration {
	d := max(interval, pollInterval)
	if ra := githubapi.RetryAfter(err); ra > d {
		d = ra
	}
	if AccessLost(err) {
		d = max(d, 5*time.Minute)
	}
	return Jitter(d)
}
