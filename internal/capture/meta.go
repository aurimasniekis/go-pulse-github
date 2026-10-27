package capture

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aurimasniekis/go-pulse-github/internal/events"
	"github.com/aurimasniekis/go-pulse-github/internal/routing"
	"github.com/aurimasniekis/go-pulse-github/internal/storage"
)

// Gap kinds.
const (
	GapPageBudget    = "page_budget_exhausted" // more data existed than the configured page budget allowed
	GapCursorMissing = "cursor_not_found"      // the last seen item was not reached: activity may be missing
	GapRetention     = "outside_retention"     // downtime longer than the source retains data
	GapAccessLost    = "access_lost"           // credentials or permissions stopped working
	GapRequestBudget = "request_budget_exhausted"
)

// RecordGap stores a coverage gap and publishes it as collector.coverage.gap_detected to the affected account, in
// the caller's transaction.
func (c *Capturer) RecordGap(ctx context.Context, tx *storage.Tx, r *routing.Router, g storage.Gap) error {
	now := c.Now()
	if err := tx.InsertGap(ctx, g, now); err != nil {
		return err
	}
	if c.Metrics != nil {
		c.Metrics.CoverageGaps.WithLabelValues(g.AccountKey, g.Collector, g.Kind).Inc()
	}
	body, err := json.Marshal(map[string]any{"collector": g.Collector, "feed": g.Feed, "kind": g.Kind,
		"window_start": g.WindowStart, "window_end": g.WindowEnd, "detail": g.Detail})
	if err != nil {
		return err
	}
	d := &events.Draft{
		Source: events.SourceCollector, Event: "coverage", Action: "gap_detected",
		DedupKey:   fmt.Sprintf("gap:%s:%s:%s:%s:%d", g.AccountKey, g.Collector, g.Feed, g.Kind, now.UnixNano()),
		ObservedAt: now, Mode: events.ModeLive, SourceRef: events.SourceRef{Feed: g.Feed},
		GitHub:     events.GitHubRef{Event: "coverage"},
		Provenance: events.Provenance{Inferred: true, Qualifiers: []string{"service_generated"}}, Raw: body,
	}
	_, err = c.Store(ctx, tx, d, []routing.Target{{Account: r.Ref(g.AccountKey), Routing: events.Routing{Match: events.MatchService}}}, 0)
	return err
}

// AccessChange publishes collector.access.lost or collector.access.restored for a collector of an account.
func (c *Capturer) AccessChange(ctx context.Context, tx *storage.Tx, r *routing.Router, account, collector, feed string, lost bool, class, detail string) error {
	now := c.Now()
	action := "restored"
	if lost {
		action = "lost"
	}
	body, err := json.Marshal(map[string]any{"collector": collector, "feed": feed, "error_class": class, "detail": detail,
		"at": now.UTC().Format(time.RFC3339Nano)})
	if err != nil {
		return err
	}
	d := &events.Draft{
		Source: events.SourceCollector, Event: "access", Action: action,
		DedupKey:   fmt.Sprintf("access:%s:%s:%s:%s:%d", account, collector, feed, action, now.UnixNano()),
		ObservedAt: now, Mode: events.ModeLive, SourceRef: events.SourceRef{Feed: feed}, GitHub: events.GitHubRef{Event: "access"},
		Provenance: events.Provenance{Inferred: true, Qualifiers: []string{"service_generated"}}, Raw: body,
	}
	_, err = c.Store(ctx, tx, d, []routing.Target{{Account: r.Ref(account), Routing: events.Routing{Match: events.MatchService}}}, 0)
	return err
}
