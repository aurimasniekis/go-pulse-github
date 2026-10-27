// Package capture writes normalized observations and their routed publications into the state database. Every
// collector calls it inside the transaction that also advances its own progress, so a crash can never leave progress
// ahead of stored observations.
package capture

import (
	"context"
	"fmt"
	"time"

	"github.com/aurimasniekis/go-pulse-github/internal/events"
	"github.com/aurimasniekis/go-pulse-github/internal/observability"
	"github.com/aurimasniekis/go-pulse-github/internal/routing"
	"github.com/aurimasniekis/go-pulse-github/internal/storage"
)

// Capturer stores drafts.
type Capturer struct {
	Domain  string
	Metrics *observability.Metrics
	Now     func() time.Time
}

// Result reports what Store did.
type Result struct {
	ObservationID   string
	NewObservation  bool
	NewPublications int
}

// Store inserts the observation (or reuses it when its dedup key exists) and queues one publication per target that
// has not received this observation before. Re-storing the same draft is a no-op for as long as the observation is
// kept, which makes collector retries and re-reads safe; a new target (for example a second account's feed seeing
// the same event) still gets its publication.
func (c *Capturer) Store(ctx context.Context, tx *storage.Tx, d *events.Draft, targets []routing.Target, inboxID int64) (Result, error) {
	now := c.Now()
	obs := storage.Observation{
		ID:         d.ObservationID(),
		Source:     d.Source,
		DedupKey:   d.DedupKey,
		Event:      d.Event,
		Action:     d.Action,
		OccurredAt: d.OccurredAt,
		ObservedAt: d.ObservedAt,
		Mode:       d.Mode,
		InboxID:    inboxID,
	}
	inserted, existing, err := tx.InsertObservation(ctx, obs, now)
	if err != nil {
		return Result{}, fmt.Errorf("store observation: %w", err)
	}
	res := Result{ObservationID: existing, NewObservation: inserted}
	if existing != obs.ID {
		return res, fmt.Errorf("observation id mismatch for dedup key %q", d.DedupKey)
	}
	if !inserted && c.Metrics != nil {
		c.Metrics.ObservationDuplicate.WithLabelValues(d.Source).Inc()
	}
	for _, t := range targets {
		r, err := events.Render(d, c.Domain, t.Account, t.Routing)
		if err != nil {
			return res, fmt.Errorf("render for %s: %w", t.Account.Key, err)
		}
		ok, err := tx.InsertPublication(ctx, storage.Publication{
			PublicationID: r.PublicationID,
			ObservationID: obs.ID,
			AccountKey:    t.Account.Key,
			Subject:       r.Subject,
			CEType:        r.CEType,
			CETime:        r.CETime,
			TimeBasis:     r.TimeBasis,
			Payload:       r.Body,
		}, now)
		if err != nil {
			return res, fmt.Errorf("queue publication: %w", err)
		}
		if ok {
			res.NewPublications++
			if c.Metrics != nil {
				c.Metrics.Observations.WithLabelValues(t.Account.Key, d.Source, events.Category(d.Event)).Inc()
			}
		}
	}
	return res, nil
}
