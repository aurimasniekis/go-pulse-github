// Package enrich performs optional, rate-budgeted fetches of resources referenced by observations (for example the
// issue a notification points at). Enrichment never delays capture of the original observation: jobs are queued in
// the same transaction and processed later. Results are published as separate enrichment events that reference the
// source observation and describe the resource's state at fetch time, not at the time of the original event.
package enrich

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"maps"
	"slices"
	"time"

	"golang.org/x/time/rate"

	"github.com/aurimasniekis/go-pulse-github/internal/capture"
	"github.com/aurimasniekis/go-pulse-github/internal/collectors"
	"github.com/aurimasniekis/go-pulse-github/internal/events"
	"github.com/aurimasniekis/go-pulse-github/internal/githubapi"
	"github.com/aurimasniekis/go-pulse-github/internal/observability"
	"github.com/aurimasniekis/go-pulse-github/internal/routing"
	"github.com/aurimasniekis/go-pulse-github/internal/storage"
)

// Account is one account allowed to enrich.
type Account struct {
	Cred       githubapi.Credential
	MaxPerHour int
}

// Worker processes enrichment jobs.
type Worker struct {
	DB        *storage.DB
	Router    *routing.Router
	Capturer  *capture.Capturer
	GitHub    *githubapi.Client
	Accounts  map[string]Account
	Pressure  collectors.Pressure
	Log       *slog.Logger
	Metrics   *observability.Metrics
	Now       func() time.Time
	Published func()
	// Interval between queue scans.
	Interval time.Duration

	limiters map[string]*rate.Limiter
}

// Run processes jobs until ctx ends.
func (w *Worker) Run(ctx context.Context) error {
	if w.Interval == 0 {
		w.Interval = 10 * time.Second
	}
	for {
		w.RunOnce(ctx)
		_ = collectors.Sleep(ctx, w.Interval)
		if err := ctx.Err(); err != nil {
			return err
		}
	}
}

// RunOnce processes the pending jobs that fit the budgets.
func (w *Worker) RunOnce(ctx context.Context) {
	if w.limiters == nil {
		w.limiters = map[string]*rate.Limiter{}
		for k, a := range w.Accounts {
			w.limiters[k] = rate.NewLimiter(rate.Limit(float64(a.MaxPerHour)/3600), max(1, a.MaxPerHour/10))
		}
	}
	if w.Pressure != nil && w.Pressure.Pressure() {
		return
	}
	keys := slices.Sorted(maps.Keys(w.Accounts))
	_, _ = w.DB.SkipEnrichmentExcept(ctx, keys, "enrichment disabled for account", w.Now())
	// Each account reads its own queue, so one account's backlog never hides another account's jobs.
	for _, key := range keys {
		lim := w.limiters[key]
		if lim.Tokens() < 1 {
			continue // budget spent: the jobs wait
		}
		jobs, err := w.DB.PendingEnrichment(ctx, key, 20)
		if err != nil {
			return
		}
		for _, j := range jobs {
			if ctx.Err() != nil {
				return
			}
			if !lim.AllowN(w.Now(), 1) {
				break
			}
			w.fetch(ctx, w.Accounts[key], j)
		}
	}
}

func (w *Worker) result(acct, r string) {
	if w.Metrics != nil {
		w.Metrics.EnrichmentFetches.WithLabelValues(acct, r).Inc()
	}
}

func (w *Worker) finish(ctx context.Context, j storage.EnrichmentJob, state, msg string) {
	_ = w.DB.Tx(ctx, func(tx *storage.Tx) error { return tx.FinishEnrichment(ctx, j.ID, state, msg, w.Now()) })
	w.result(j.AccountKey, state)
}

func (w *Worker) fetch(ctx context.Context, acct Account, j storage.EnrichmentJob) {
	// The URL came from GitHub content: only the configured API origin is ever contacted with credentials.
	if _, err := w.GitHub.ResolveURL(j.URL); err != nil {
		w.finish(ctx, j, "skipped", "untrusted URL: "+err.Error())
		return
	}
	resp, err := w.GitHub.Do(ctx, acct.Cred, githubapi.Request{URL: j.URL})
	if err != nil {
		switch githubapi.ClassOf(err) {
		case githubapi.ClassNotFound, githubapi.ClassGone, githubapi.ClassForbidden:
			// Deleted, never visible, or access lost: ambiguous, so nothing is inferred and no event is published.
			w.finish(ctx, j, "failed", err.Error())
		default:
			_ = w.DB.EnrichmentFailed(ctx, j.ID, err.Error(), j.Attempts+1 >= 5, w.Now())
			w.result(j.AccountKey, "retry")
		}
		return
	}
	now := w.Now()
	sum := sha256.Sum256(resp.Body)
	kind := j.Kind
	if kind == "" {
		kind = "resource"
	}
	d := &events.Draft{
		Source:     events.SourceEnrichment,
		DedupKey:   "enrich:" + j.AccountKey + ":" + j.URL + ":" + hex.EncodeToString(sum[:8]),
		Event:      kind,
		Action:     "fetched",
		ObservedAt: now,
		Mode:       events.ModeEnrichment,
		GitHub:     events.GitHubRef{Event: kind, PayloadSHA256: hex.EncodeToString(sum[:])},
		Resource:   events.ResourceFrom(kind, resp.Body),
		Provenance: events.Provenance{Snapshot: true, SourceObservationID: j.ObservationID,
			Qualifiers: []string{"current_state_at_fetch_time"}},
		Raw: resp.Body,
	}
	if d.Resource == nil {
		d.Resource = &events.Resource{Kind: kind}
	}
	d.Resource.URL = j.URL
	err = w.DB.Tx(ctx, func(tx *storage.Tx) error {
		if _, err := w.Capturer.Store(ctx, tx, d, []routing.Target{{Account: w.Router.Ref(j.AccountKey),
			Routing: events.Routing{Match: events.MatchCredential}}}, 0); err != nil {
			return err
		}
		return tx.FinishEnrichment(ctx, j.ID, "done", "", now)
	})
	if err != nil {
		w.Log.Error("enrichment store failed", observability.FieldAccount, j.AccountKey, observability.Err(err))
		return
	}
	w.result(j.AccountKey, "done")
	if w.Published != nil {
		w.Published()
	}
}
