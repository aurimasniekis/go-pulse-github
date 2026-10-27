package activity

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/aurimasniekis/go-pulse-github/internal/events"
	"github.com/aurimasniekis/go-pulse-github/internal/githubapi"
	"github.com/aurimasniekis/go-pulse-github/internal/storage"
)

// auditInitialLookback bounds the first audit-log read.
const auditInitialLookback = 7 * 24 * time.Hour

// auditOverlap re-reads the last minute so entries with equal timestamps are not skipped (dedup absorbs repeats).
const auditOverlap = time.Minute

// pollAudit reads GET /orgs/{org}/audit-log in ascending time order. It is optional: the endpoint exists only for
// organizations on GitHub Enterprise Cloud, needs an organization owner's classic token with read:audit_log, and
// GitHub's documentation gives differing retention figures (3 to 7 months; Git events 7 days).
func (p *Poller) pollAudit(ctx context.Context, f Feed) error {
	key, feedID := p.Account.Key, f.Config.ID()
	cp, err := p.DB.Checkpoint(ctx, key, Collector, feedID)
	if err != nil {
		return err
	}
	var cur cursor
	if cp.Cursor != "" {
		_ = json.Unmarshal([]byte(cp.Cursor), &cur)
	}
	now := p.Now()
	since := now.Add(-auditInitialLookback)
	if cur.AuditTS > 0 {
		since = time.UnixMilli(cur.AuditTS).Add(-auditOverlap)
	}
	q := url.Values{"include": {"all"}, "order": {"asc"}, "per_page": {"100"},
		"phrase": {"created:>=" + since.UTC().Format("2006-01-02T15:04:05Z")}}
	next := "/orgs/" + url.PathEscape(f.Config.Org) + "/audit-log?" + q.Encode()
	maxTS, ascending := cur.AuditTS, true
	var prevTS int64
	pages, stored := 0, 0
	for next != "" && pages < p.Account.Activity.MaxPages {
		resp, err := p.GitHub.Do(ctx, f.Cred, githubapi.Request{URL: next})
		if err != nil {
			p.fail(ctx, feedID, err)
			return err
		}
		pages++
		items, err := githubapi.SplitArray(resp.Body)
		if err != nil {
			return fmt.Errorf("decode audit log: %w", err)
		}
		var drafts []*events.Draft
		var repos []string
		for _, raw := range items {
			d, ts, repo, err := normalizeAudit(raw, f.Config.Org, feedID, now)
			if err != nil {
				return err
			}
			if ts < prevTS {
				ascending = false
			}
			prevTS = ts
			maxTS = max(maxTS, ts)
			drafts = append(drafts, d)
			repos = append(repos, repo)
		}
		n, err := p.store(ctx, drafts, repos)
		if err != nil {
			return err
		}
		stored += n
		next = resp.Next
	}
	// With ascending results a partial read can resume from the newest processed entry. If the order was not
	// ascending, the cursor only moves after a complete read.
	newCur := cur
	if next == "" || ascending {
		newCur.AuditTS = maxTS
	}
	b, _ := json.Marshal(newCur)
	err = p.DB.Tx(ctx, func(tx *storage.Tx) error {
		if p.lost[feedID] {
			if err := p.Capturer.AccessChange(ctx, tx, p.Router, key, Collector, feedID, false, "", ""); err != nil {
				return err
			}
		}
		return tx.PutCheckpoint(ctx, storage.Checkpoint{AccountKey: key, Collector: Collector, Feed: feedID, Cursor: string(b)}, now)
	})
	if err != nil {
		return err
	}
	p.restored(feedID)
	p.metric("ok")
	if stored > 0 && p.Published != nil {
		p.Published()
	}
	return nil
}

func normalizeAudit(raw json.RawMessage, org, feed string, now time.Time) (*events.Draft, int64, string, error) {
	var e struct {
		DocumentID string          `json:"_document_id"`
		Timestamp  json.Number     `json:"@timestamp"`
		Action     string          `json:"action"`
		Actor      string          `json:"actor"`
		ActorID    int64           `json:"actor_id"`
		Repo       string          `json:"repo"`
		CreatedAt  json.RawMessage `json:"created_at"`
	}
	if err := json.Unmarshal(raw, &e); err != nil {
		return nil, 0, "", fmt.Errorf("decode audit entry: %w", err)
	}
	if e.DocumentID == "" || e.Action == "" {
		return nil, 0, "", fmt.Errorf("audit entry without _document_id or action")
	}
	ts, _ := strconv.ParseInt(e.Timestamp.String(), 10, 64)
	category, action, _ := strings.Cut(e.Action, ".")
	d := &events.Draft{
		Source:     events.SourceAudit,
		DedupKey:   "audit:" + strings.ToLower(org) + ":" + e.DocumentID,
		Event:      category,
		Action:     action,
		ObservedAt: now,
		Mode:       events.ModeLive,
		SourceRef:  events.SourceRef{Feed: feed},
		GitHub:     events.GitHubRef{Event: "audit_log", Action: e.Action, EventID: e.DocumentID},
		Provenance: events.Provenance{Authoritative: true, Qualifiers: []string{"audit_log"}},
		Raw:        raw,
	}
	if ts > 0 {
		t := time.UnixMilli(ts).UTC()
		d.OccurredAt, d.OccurredAtBasis = &t, "audit.@timestamp"
	}
	if e.Actor != "" {
		d.Actor = &events.User{ID: e.ActorID, Login: e.Actor}
	}
	if e.Repo != "" {
		d.Repository = &events.Repository{FullName: e.Repo}
	}
	return d, ts, e.Repo, nil
}
