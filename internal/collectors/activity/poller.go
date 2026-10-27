// Package activity polls GitHub's Events API feeds (user, received, user-org, organization and repository events)
// and, optionally, the organization audit log.
//
// The Events API is a bounded, supplementary feed: at most 300 events, only the last 30 days, with latency from
// 30 seconds up to 6 hours, and PushEvent payloads without commit lists. It is not an audit trail. The poller stores
// GitHub event IDs, re-reads with overlap (it pages until it reaches the newest event it already has), and reports a
// coverage gap whenever that event is no longer reachable.
package activity

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/aurimasniekis/go-pulse-github/internal/capture"
	"github.com/aurimasniekis/go-pulse-github/internal/collectors"
	"github.com/aurimasniekis/go-pulse-github/internal/config"
	"github.com/aurimasniekis/go-pulse-github/internal/events"
	"github.com/aurimasniekis/go-pulse-github/internal/githubapi"
	"github.com/aurimasniekis/go-pulse-github/internal/observability"
	"github.com/aurimasniekis/go-pulse-github/internal/routing"
	"github.com/aurimasniekis/go-pulse-github/internal/storage"
)

// Collector is the checkpoint and metric name.
const Collector = "activity"

// Retention is the Events API retention window (30 days since 2025-01-30).
const Retention = 30 * 24 * time.Hour

// Feed is one configured feed with its credential.
type Feed struct {
	Config config.FeedConfig
	Cred   githubapi.Credential
}

// Poller polls the feeds of one account.
type Poller struct {
	Account   *config.Account
	Feeds     []Feed
	DB        *storage.DB
	Router    *routing.Router
	Capturer  *capture.Capturer
	GitHub    *githubapi.Client
	Pressure  collectors.Pressure
	Log       *slog.Logger
	Metrics   *observability.Metrics
	Now       func() time.Time
	Published func()

	lost map[string]bool
}

type cursor struct {
	LastID        string `json:"last_id,omitempty"`
	LastCreatedAt string `json:"last_created_at,omitempty"`
	// Audit log: newest @timestamp (ms) processed and the document IDs at that timestamp.
	AuditTS   int64    `json:"audit_ts,omitempty"`
	AuditDocs []string `json:"audit_docs,omitempty"`
}

// Run polls every feed each interval until ctx ends.
func (p *Poller) Run(ctx context.Context) error {
	for {
		var maxPoll time.Duration
		var lastErr error
		for _, f := range p.Feeds {
			pi, err := p.PollFeed(ctx, f)
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err != nil {
				lastErr = err
				p.Log.Warn("activity poll failed", observability.FieldComponent, "activity", observability.FieldAccount, p.Account.Key,
					observability.FieldFeed, f.Config.ID(), observability.FieldErrorClass, githubapi.ClassOf(err), observability.Err(err))
			}
			maxPoll = max(maxPoll, pi)
		}
		_ = collectors.Sleep(ctx, collectors.NextDelay(p.Account.Activity.Interval.D(), maxPoll, lastErr))
		if err := ctx.Err(); err != nil {
			return err
		}
	}
}

// URL returns the first-page URL of a feed.
func URL(login string, f config.FeedConfig) string {
	pe := url.PathEscape
	switch f.Kind {
	case config.FeedUserEvents:
		return "/users/" + pe(login) + "/events?per_page=100"
	case config.FeedReceivedEvents:
		return "/users/" + pe(login) + "/received_events?per_page=100"
	case config.FeedUserOrgEvents:
		return "/users/" + pe(login) + "/events/orgs/" + pe(f.Org) + "?per_page=100"
	case config.FeedOrgEvents:
		return "/orgs/" + pe(f.Org) + "/events?per_page=100"
	case config.FeedRepoEvents:
		owner, name, _ := strings.Cut(f.Repo, "/")
		return "/repos/" + pe(owner) + "/" + pe(name) + "/events?per_page=100"
	}
	return ""
}

func (p *Poller) metric(result string) {
	if p.Metrics != nil {
		p.Metrics.Polls.WithLabelValues(p.Account.Key, Collector, result).Inc()
	}
}

// PollFeed polls one feed once.
func (p *Poller) PollFeed(ctx context.Context, f Feed) (time.Duration, error) {
	if p.Pressure != nil && p.Pressure.Pressure() {
		p.metric("paused")
		return 0, nil
	}
	if f.Config.Kind == config.FeedAuditLog {
		return 0, p.pollAudit(ctx, f)
	}
	key, feedID := p.Account.Key, f.Config.ID()
	cp, err := p.DB.Checkpoint(ctx, key, Collector, feedID)
	if err != nil {
		return 0, err
	}
	var cur cursor
	if cp.Cursor != "" {
		_ = json.Unmarshal([]byte(cp.Cursor), &cur)
	}
	lastID, _ := strconv.ParseInt(cur.LastID, 10, 64)
	now := p.Now()
	next := URL(p.Account.Login, f.Config)
	etag := cp.ETag
	var newETag string
	var pollInterval time.Duration
	reached := lastID == 0
	newest := cur
	newestID := lastID
	var oldestSeen string
	pages, stored := 0, 0
	// Stop once the newest known event is reached (or after the page budget).
	for next != "" && pages < p.Account.Activity.MaxPages && (!reached || lastID == 0) {
		resp, err := p.GitHub.Do(ctx, f.Cred, githubapi.Request{URL: next, ETag: etag})
		if err != nil {
			p.fail(ctx, feedID, err)
			return pollInterval, err
		}
		if pages == 0 {
			newETag, pollInterval = resp.ETag, resp.PollInterval
		}
		pages++
		etag = ""
		if resp.NotModified {
			reached = true
			break
		}
		items, err := githubapi.SplitArray(resp.Body)
		if err != nil {
			return pollInterval, fmt.Errorf("decode events: %w", err)
		}
		var drafts []*events.Draft
		var repos []string
		for _, raw := range items {
			d, id, repo, err := Normalize(raw, feedID, now)
			if err != nil {
				return pollInterval, err
			}
			if lastID != 0 && id <= lastID {
				reached = true
				continue
			}
			if id > newestID {
				newestID, newest.LastID = id, d.GitHub.EventID
				if d.OccurredAt != nil {
					newest.LastCreatedAt = d.OccurredAt.Format(time.RFC3339)
				}
			}
			if d.OccurredAt != nil {
				oldestSeen = d.OccurredAt.Format(time.RFC3339)
			}
			drafts = append(drafts, d)
			repos = append(repos, repo)
		}
		n, err := p.store(ctx, drafts, repos)
		if err != nil {
			return pollInterval, err
		}
		stored += n
		next = resp.Next
	}
	// A previously seen event that is no longer reachable means events in between may have been missed: the feed
	// keeps at most 300 events and 30 days, or the page budget ran out.
	gap := lastID != 0 && !reached
	if newETag == "" {
		newETag = cp.ETag
	}
	b, _ := json.Marshal(newest)
	err = p.DB.Tx(ctx, func(tx *storage.Tx) error {
		if gap {
			g := storage.Gap{AccountKey: key, Collector: Collector, Feed: feedID, Kind: capture.GapCursorMissing,
				WindowStart: events.ParseTime(cur.LastCreatedAt), WindowEnd: events.ParseTime(oldestSeen),
				Detail: "the last seen event was not reached within the Events API window or the page budget"}
			if cp.LastSuccessAt != nil && now.Sub(*cp.LastSuccessAt) > Retention {
				g.Kind = capture.GapRetention
				g.Detail = "collector was not polling for longer than the 30-day Events API retention"
			}
			if err := p.Capturer.RecordGap(ctx, tx, p.Router, g); err != nil {
				return err
			}
		}
		if p.lost[feedID] {
			if err := p.Capturer.AccessChange(ctx, tx, p.Router, key, Collector, feedID, false, "", ""); err != nil {
				return err
			}
		}
		return tx.PutCheckpoint(ctx, storage.Checkpoint{AccountKey: key, Collector: Collector, Feed: feedID, Cursor: string(b),
			ETag: newETag, PollInterval: pollInterval}, now)
	})
	if err != nil {
		return pollInterval, err
	}
	p.restored(feedID)
	p.metric("ok")
	if (stored > 0 || gap) && p.Published != nil {
		p.Published()
	}
	return pollInterval, nil
}

func (p *Poller) store(ctx context.Context, drafts []*events.Draft, repos []string) (int, error) {
	if len(drafts) == 0 {
		return 0, nil
	}
	n := 0
	err := p.DB.Tx(ctx, func(tx *storage.Tx) error {
		for i, d := range drafts {
			res, err := p.Capturer.Store(ctx, tx, d, p.Router.Route(p.Account.Key, events.MatchCredential, d.Source, repos[i]), 0)
			if err != nil {
				return err
			}
			n += res.NewPublications
		}
		return nil
	})
	return n, err
}

func (p *Poller) restored(feed string) {
	if p.lost[feed] {
		delete(p.lost, feed)
		if p.Metrics != nil {
			p.Metrics.CoverageDegraded.WithLabelValues(p.Account.Key, Collector).Set(0)
		}
	}
}

func (p *Poller) fail(ctx context.Context, feed string, err error) {
	if ctx.Err() != nil {
		return
	}
	class := githubapi.ClassOf(err)
	if class == "" {
		class = "error"
	}
	_ = p.DB.RecordPollError(ctx, p.Account.Key, Collector, feed, class, err.Error(), p.Now())
	p.metric(class)
	// Lost access to a feed is surfaced; a 404 here means the org/repo/user is gone or no longer visible, which is
	// ambiguous and reported as access lost rather than as a deletion.
	if !collectors.AccessLost(err) && class != githubapi.ClassNotFound {
		return
	}
	if p.lost == nil {
		p.lost = map[string]bool{}
	}
	if p.lost[feed] {
		return
	}
	p.lost[feed] = true
	if p.Metrics != nil {
		p.Metrics.CoverageDegraded.WithLabelValues(p.Account.Key, Collector).Set(1)
	}
	_ = p.DB.Tx(ctx, func(tx *storage.Tx) error {
		return p.Capturer.AccessChange(ctx, tx, p.Router, p.Account.Key, Collector, feed, true, class, err.Error())
	})
	if p.Published != nil {
		p.Published()
	}
}

type ghEvent struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Public    *bool           `json:"public"`
	CreatedAt string          `json:"created_at"`
	Payload   json.RawMessage `json:"payload"`
	Actor     *struct {
		ID    int64  `json:"id"`
		Login string `json:"login"`
	} `json:"actor"`
	Repo *struct {
		ID   int64  `json:"id"`
		Name string `json:"name"`
		URL  string `json:"url"`
	} `json:"repo"`
	Org *struct {
		ID    int64  `json:"id"`
		Login string `json:"login"`
	} `json:"org"`
}

// EventName turns an Events API type into the webhook-style name: PullRequestReviewEvent -> pull_request_review.
func EventName(typ string) string {
	t := strings.TrimSuffix(typ, "Event")
	var b strings.Builder
	for i, r := range t {
		if unicode.IsUpper(r) {
			if i > 0 {
				b.WriteByte('_')
			}
			r = unicode.ToLower(r)
		}
		b.WriteRune(r)
	}
	if b.Len() == 0 {
		return "unknown"
	}
	return b.String()
}

// Normalize builds the draft of one Events API event. The dedup key is the GitHub event ID, shared by every feed and
// account that sees the event.
func Normalize(raw json.RawMessage, feed string, now time.Time) (*events.Draft, int64, string, error) {
	var e ghEvent
	if err := json.Unmarshal(raw, &e); err != nil {
		return nil, 0, "", fmt.Errorf("decode event: %w", err)
	}
	id, err := strconv.ParseInt(e.ID, 10, 64)
	if err != nil {
		return nil, 0, "", fmt.Errorf("event id %q is not numeric", e.ID)
	}
	var payload struct {
		Action string `json:"action"`
	}
	_ = json.Unmarshal(e.Payload, &payload)
	d := &events.Draft{
		Source:     events.SourceActivity,
		DedupKey:   "activity:event:" + e.ID,
		Event:      EventName(e.Type),
		Action:     "observed",
		ObservedAt: now,
		Mode:       events.ModeLive,
		SourceRef:  events.SourceRef{Feed: feed},
		GitHub:     events.GitHubRef{Event: e.Type, Action: payload.Action, EventID: e.ID},
		Provenance: events.Provenance{Authoritative: true, Qualifiers: []string{"events_api_bounded"}},
		Raw:        raw,
	}
	if e.Type == "PushEvent" {
		d.Provenance.Qualifiers = append(d.Provenance.Qualifiers, "push_without_commit_list")
	}
	if e.Public != nil && !*e.Public {
		d.Provenance.Qualifiers = append(d.Provenance.Qualifiers, "private_event")
	}
	if t := events.ParseTime(e.CreatedAt); t != nil {
		d.OccurredAt, d.OccurredAtBasis = t, "event.created_at"
	}
	if e.Actor != nil {
		d.Actor = &events.User{ID: e.Actor.ID, Login: e.Actor.Login}
	}
	repo := ""
	if e.Repo != nil {
		repo = e.Repo.Name
		d.Repository = &events.Repository{ID: e.Repo.ID, FullName: e.Repo.Name}
	}
	if e.Org != nil {
		d.Organization = &events.User{ID: e.Org.ID, Login: e.Org.Login, Type: "Organization"}
	}
	return d, id, repo, nil
}
