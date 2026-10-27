// Package notifications polls the authenticated user's notifications (GET /notifications) for personal accounts.
//
// It needs a classic personal access token with the notifications or repo scope: GitHub supports no other token type
// on this endpoint. It never marks notifications read, subscribes, or changes any other state. Each observation is a
// notification thread state (reason, unread, timestamps, subject), not the underlying issue, comment or review.
package notifications

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/aurimasniekis/go-pulse-github/internal/capture"
	"github.com/aurimasniekis/go-pulse-github/internal/collectors"
	"github.com/aurimasniekis/go-pulse-github/internal/config"
	"github.com/aurimasniekis/go-pulse-github/internal/events"
	"github.com/aurimasniekis/go-pulse-github/internal/githubapi"
	"github.com/aurimasniekis/go-pulse-github/internal/observability"
	"github.com/aurimasniekis/go-pulse-github/internal/routing"
	"github.com/aurimasniekis/go-pulse-github/internal/storage"
)

// Collector and feed names in checkpoints and metrics.
const (
	Collector = "notifications"
	Feed      = "threads"
)

// ErrIdentity means the token does not belong to the configured account or is not a classic token with the needed scope.
var ErrIdentity = errors.New("notifications credential check failed")

// Poller polls one account.
type Poller struct {
	Account  *config.Account
	DB       *storage.DB
	Router   *routing.Router
	Capturer *capture.Capturer
	GitHub   *githubapi.Client
	Cred     githubapi.Credential
	Pressure collectors.Pressure
	Log      *slog.Logger
	Metrics  *observability.Metrics
	Now      func() time.Time
	// Bind pins the account's GitHub ID after the identity check.
	Bind func(ctx context.Context, key string, id int64, login, typ string) error
	// Published wakes the outbox.
	Published func()

	verified bool
	lost     bool
}

type cursor struct {
	Since         string `json:"since,omitempty"`
	LastFullSweep string `json:"last_full_sweep,omitempty"`
}

// Run polls until ctx ends.
func (p *Poller) Run(ctx context.Context) error {
	log := p.log()
	for {
		pollInterval, err := p.PollOnce(ctx)
		if err != nil && ctx.Err() == nil {
			log.Warn("notifications poll failed", observability.FieldErrorClass, errClass(err), observability.Err(err))
		}
		_ = collectors.Sleep(ctx, collectors.NextDelay(p.Account.Notifications.Interval.D(), pollInterval, err))
		if err := ctx.Err(); err != nil {
			return err
		}
	}
}

func (p *Poller) log() *slog.Logger {
	return p.Log.With(observability.FieldComponent, "notifications", observability.FieldAccount, p.Account.Key,
		observability.FieldCollector, Collector)
}

func errClass(err error) string {
	if c := githubapi.ClassOf(err); c != "" {
		return c
	}
	if errors.Is(err, ErrIdentity) {
		return "identity"
	}
	return "error"
}

// PollOnce runs one poll and returns GitHub's requested poll interval.
func (p *Poller) PollOnce(ctx context.Context) (time.Duration, error) {
	key := p.Account.Key
	if p.Pressure != nil && p.Pressure.Pressure() {
		p.metric("paused")
		return 0, nil
	}
	if !p.verified {
		if err := p.verify(ctx); err != nil {
			p.fail(ctx, err)
			return 0, err
		}
		p.verified = true
	}
	cp, err := p.DB.Checkpoint(ctx, key, Collector, Feed)
	if err != nil {
		return 0, err
	}
	var cur cursor
	if cp.Cursor != "" {
		_ = json.Unmarshal([]byte(cp.Cursor), &cur)
	}
	now := p.Now()
	cfg := p.Account.Notifications
	// The first poll reads initial_lookback incrementally and starts the full-sweep schedule; a full sweep of every
	// listed thread runs only once per full_sweep_interval after that.
	lastSweep := events.ParseTime(cur.LastFullSweep)
	sweep := lastSweep != nil && now.Sub(*lastSweep) >= cfg.FullSweepInterval.D()
	q := url.Values{"all": {"true"}, "per_page": {"50"}}
	ims := ""
	var since time.Time
	if !sweep {
		since = now.Add(-cfg.InitialLookback.D())
		if t := events.ParseTime(cur.Since); t != nil {
			since = t.Add(-cfg.Overlap.D())
		}
		q.Set("since", since.UTC().Format(time.RFC3339))
		ims = cp.LastModified
	}
	next := "/notifications?" + q.Encode()
	var maxUpdated, minUpdated string
	var lastModified string
	var pollInterval time.Duration
	pages, stored := 0, 0
	for next != "" && pages < cfg.MaxPages {
		resp, err := p.GitHub.Do(ctx, p.Cred, githubapi.Request{URL: next, IfModifiedSince: ims})
		if err != nil {
			p.fail(ctx, err)
			return pollInterval, err
		}
		if pages == 0 {
			lastModified, pollInterval = resp.LastModified, resp.PollInterval
		}
		pages++
		ims = ""
		if resp.NotModified {
			next = ""
			break
		}
		n, mu, oldest, err := p.storePage(ctx, resp.Body, sweep)
		if err != nil {
			return pollInterval, err
		}
		stored += n
		if mu > maxUpdated {
			maxUpdated = mu
		}
		if oldest != "" && (minUpdated == "" || oldest < minUpdated) {
			minUpdated = oldest
		}
		next = resp.Next
	}
	if lastModified == "" {
		lastModified = cp.LastModified
	}
	// Notifications arrive newest first: a remaining next page means older changes were not read in this run. The
	// cursor still moves past what was read, and the unread window is published once as a gap. Holding the cursor
	// back instead would make every later poll exhaust the same, growing window and report the same gap again.
	exhausted := next != ""
	newCur := cur
	if maxUpdated > newCur.Since {
		newCur.Since = maxUpdated
	}
	if sweep || lastSweep == nil {
		newCur.LastFullSweep = now.UTC().Format(time.RFC3339)
	}
	b, _ := json.Marshal(newCur)
	err = p.DB.Tx(ctx, func(tx *storage.Tx) error {
		if exhausted {
			g := storage.Gap{AccountKey: key, Collector: Collector, Feed: Feed, Kind: capture.GapPageBudget,
				WindowEnd: events.ParseTime(minUpdated)}
			if sweep {
				g.Detail = fmt.Sprintf("full sweep stopped after %d pages; older threads were not re-checked for read-state changes; raise notifications.max_pages", cfg.MaxPages)
			} else {
				g.WindowStart = &since
				g.Detail = fmt.Sprintf("more than %d pages of notifications changed; changes before window_end were not read; raise notifications.max_pages", cfg.MaxPages)
			}
			if err := p.Capturer.RecordGap(ctx, tx, p.Router, g); err != nil {
				return err
			}
		}
		if p.lost {
			if err := p.Capturer.AccessChange(ctx, tx, p.Router, key, Collector, Feed, false, "", ""); err != nil {
				return err
			}
		}
		return tx.PutCheckpoint(ctx, storage.Checkpoint{AccountKey: key, Collector: Collector, Feed: Feed, Cursor: string(b),
			LastModified: lastModified, PollInterval: pollInterval}, now)
	})
	if err != nil {
		return pollInterval, err
	}
	if p.lost {
		p.lost = false
		p.setDegraded(false)
	}
	p.metric("ok")
	if stored > 0 && p.Published != nil {
		p.Published()
	}
	return pollInterval, nil
}

func (p *Poller) metric(result string) {
	if p.Metrics != nil {
		p.Metrics.Polls.WithLabelValues(p.Account.Key, Collector, result).Inc()
	}
}

func (p *Poller) setDegraded(v bool) {
	if p.Metrics != nil {
		p.Metrics.CoverageDegraded.WithLabelValues(p.Account.Key, Collector).Set(map[bool]float64{true: 1}[v])
	}
}

// verify checks that the token is a classic token with the notifications or repo scope and authenticates as the
// configured account. Payload or configuration logins alone never establish identity.
func (p *Poller) verify(ctx context.Context) error {
	id, err := p.GitHub.AuthenticatedUser(ctx, p.Cred)
	if err != nil {
		return err
	}
	if !id.Classic {
		return fmt.Errorf("%w: GET /notifications only supports classic personal access tokens (no X-OAuth-Scopes reported)", ErrIdentity)
	}
	if !id.HasScope("notifications") && !id.HasScope("repo") {
		return fmt.Errorf("%w: token lacks the notifications or repo scope (has %v)", ErrIdentity, id.Scopes)
	}
	ref := p.Router.Ref(p.Account.Key)
	switch {
	case ref.GitHubID != 0 && ref.GitHubID != id.ID:
		return fmt.Errorf("%w: token belongs to %s (id %d), account %s is id %d", ErrIdentity, id.Login, id.ID, p.Account.Key, ref.GitHubID)
	case ref.GitHubID == 0 && !strings.EqualFold(id.Login, p.Account.Login):
		return fmt.Errorf("%w: token belongs to %s, account %s is configured as %s", ErrIdentity, id.Login, p.Account.Key, p.Account.Login)
	}
	if p.Bind != nil {
		if err := p.Bind(ctx, p.Account.Key, id.ID, id.Login, id.Type); err != nil {
			return err
		}
	}
	if !p.Router.Bind(p.Account.Key, id.ID, id.Login) {
		return fmt.Errorf("%w: github id %d conflicts with another account", ErrIdentity, id.ID)
	}
	return nil
}

// fail records a failed poll; credential and permission failures are surfaced once as collector.access.lost.
func (p *Poller) fail(ctx context.Context, err error) {
	if ctx.Err() != nil {
		return
	}
	now := p.Now()
	class := errClass(err)
	_ = p.DB.RecordPollError(ctx, p.Account.Key, Collector, Feed, class, err.Error(), now)
	p.metric(class)
	lost := collectors.AccessLost(err) || errors.Is(err, ErrIdentity)
	if githubapi.IsClass(err, githubapi.ClassUnauthorized) {
		p.verified = false
	}
	if lost && !p.lost {
		p.lost = true
		p.setDegraded(true)
		p.log().Error("notifications access lost", observability.FieldErrorClass, class, observability.Err(err))
		_ = p.DB.Tx(ctx, func(tx *storage.Tx) error {
			return p.Capturer.AccessChange(ctx, tx, p.Router, p.Account.Key, Collector, Feed, true, class, err.Error())
		})
		if p.Published != nil {
			p.Published()
		}
	}
}

type thread struct {
	ID         string `json:"id"`
	Reason     string `json:"reason"`
	Unread     bool   `json:"unread"`
	UpdatedAt  string `json:"updated_at"`
	LastReadAt string `json:"last_read_at"`
	URL        string `json:"url"`
	Subject    struct {
		Title            string `json:"title"`
		URL              string `json:"url"`
		LatestCommentURL string `json:"latest_comment_url"`
		Type             string `json:"type"`
	} `json:"subject"`
	Repository *struct {
		ID       int64  `json:"id"`
		FullName string `json:"full_name"`
		Private  *bool  `json:"private"`
		Owner    *struct {
			ID    int64  `json:"id"`
			Login string `json:"login"`
			Type  string `json:"type"`
		} `json:"owner"`
	} `json:"repository"`
}

// Fingerprint is the meaningful state of a thread: a change of any of these fields is published as
// notifications.thread.updated. Fields outside it (e.g. subscription_url) do not produce events.
func (t *thread) Fingerprint() string {
	return events.Fingerprint(t.Reason, strconv.FormatBool(t.Unread), t.UpdatedAt, t.LastReadAt, t.Subject.Title,
		t.Subject.Type, t.Subject.URL, t.Subject.LatestCommentURL)
}

// storePage stores one page in one transaction and returns the stored count and the newest and oldest updated_at.
func (p *Poller) storePage(ctx context.Context, body []byte, sweep bool) (int, string, string, error) {
	items, err := githubapi.SplitArray(body)
	if err != nil {
		return 0, "", "", fmt.Errorf("decode notifications: %w", err)
	}
	key := p.Account.Key
	now := p.Now()
	stored := 0
	maxUpdated, minUpdated := "", ""
	err = p.DB.Tx(ctx, func(tx *storage.Tx) error {
		for _, raw := range items {
			var t thread
			if err := json.Unmarshal(raw, &t); err != nil || t.ID == "" {
				return fmt.Errorf("decode notification thread %q: %w", t.ID, err)
			}
			if t.UpdatedAt > maxUpdated {
				maxUpdated = t.UpdatedAt
			}
			if minUpdated == "" || t.UpdatedAt < minUpdated {
				minUpdated = t.UpdatedAt
			}
			fp := t.Fingerprint()
			prev, prevUpdated, err := tx.NotificationState(ctx, key, t.ID)
			if err != nil {
				return err
			}
			if prev == fp {
				continue
			}
			action := "observed"
			if prev != "" {
				action = "updated"
			}
			d := draft(key, &t, raw, fp, prev, action, now, sweep, t.UpdatedAt > prevUpdated)
			repo := ""
			if t.Repository != nil {
				repo = t.Repository.FullName
			}
			res, err := p.Capturer.Store(ctx, tx, d, p.Router.Route(key, events.MatchCredential, events.SourceNotifications, repo), 0)
			if err != nil {
				return err
			}
			if err := tx.PutNotificationState(ctx, key, t.ID, fp, t.UpdatedAt, now); err != nil {
				return err
			}
			if p.Account.Notifications.Enrichment.Enabled && res.NewPublications > 0 {
				for _, u := range []string{t.Subject.URL, t.Subject.LatestCommentURL} {
					if u == "" {
						continue
					}
					if _, err := p.GitHub.ResolveURL(u); err != nil {
						continue // never fetch URLs outside the configured API origin
					}
					if err := tx.EnqueueEnrichment(ctx, storage.EnrichmentJob{ObservationID: res.ObservationID, AccountKey: key,
						Kind: strings.ToLower(t.Subject.Type), URL: u}, now); err != nil {
						return err
					}
				}
			}
			stored += res.NewPublications
		}
		return nil
	})
	return stored, maxUpdated, minUpdated, err
}

func draft(key string, t *thread, raw json.RawMessage, fp, prev, action string, now time.Time, sweep, advanced bool) *events.Draft {
	mode := events.ModeLive
	if sweep {
		mode = events.ModeReconciliation
	}
	d := &events.Draft{
		Source:     events.SourceNotifications,
		DedupKey:   "notif:" + key + ":" + t.ID + ":" + fp,
		Event:      "thread",
		Action:     action,
		ObservedAt: now,
		Mode:       mode,
		SourceRef:  events.SourceRef{Feed: Feed},
		GitHub:     events.GitHubRef{Event: "notification", Action: t.Reason, NotificationID: t.ID},
		Provenance: events.Provenance{Authoritative: true, Fingerprint: fp, PreviousFingerprint: prev,
			Qualifiers: []string{"notification_state", "not_the_underlying_event"}},
		Raw: raw,
	}
	// updated_at is GitHub's time of the latest change to the thread. A change that did not move it (for example the
	// thread being read elsewhere) has no known time, so occurred_at stays null.
	if ts := events.ParseTime(t.UpdatedAt); ts != nil && advanced {
		d.OccurredAt, d.OccurredAtBasis = ts, "notification.updated_at"
	}
	if t.Subject.Type != "" || t.Subject.URL != "" {
		d.Resource = &events.Resource{Kind: strings.ToLower(t.Subject.Type), URL: t.Subject.URL}
	}
	if r := t.Repository; r != nil {
		d.Repository = &events.Repository{ID: r.ID, FullName: r.FullName, Private: r.Private}
		if r.Owner != nil {
			d.Repository.Owner = &events.User{ID: r.Owner.ID, Login: r.Owner.Login, Type: r.Owner.Type}
		}
	}
	return d
}
