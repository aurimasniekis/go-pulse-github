package activity

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aurimasniekis/go-pulse-github/internal/capture"
	"github.com/aurimasniekis/go-pulse-github/internal/config"
	"github.com/aurimasniekis/go-pulse-github/internal/githubapi"
	"github.com/aurimasniekis/go-pulse-github/internal/githubauth"
	"github.com/aurimasniekis/go-pulse-github/internal/githubtest"
	"github.com/aurimasniekis/go-pulse-github/internal/routing"
	"github.com/aurimasniekis/go-pulse-github/internal/storage"
	"github.com/aurimasniekis/go-pulse-github/internal/testutil"
)

const cfgYAML = `
version: 1
pulse: {url: x, creds_file: y}
accounts:
  - login: alice
    type: user
    token: {env: PG_TEST_PAT}
    activity: {enabled: true, max_pages: 2, feeds: [{kind: received_events}]}
  - login: bob
    type: user
    token: {env: PG_TEST_PAT}
    activity: {enabled: true, max_pages: 2, feeds: [{kind: received_events}]}
  - login: acme
    type: organization
    token: {env: PG_TEST_PAT}
    activity: {enabled: true, feeds: [{kind: audit_log, org: acme}]}
`

// feed serves an Events API listing (newest first, 2 per page) with an ETag.
type feed struct {
	mu    sync.Mutex
	ids   []int
	etag  string
	hits  int
	etags []string
}

func (f *feed) set(ids ...int) {
	f.mu.Lock()
	f.ids, f.etag = ids, fmt.Sprintf(`"%d"`, len(ids)*1000+ids[0])
	f.mu.Unlock()
}

func event(id int) map[string]any {
	return map[string]any{"id": fmt.Sprint(id), "type": "IssueCommentEvent", "public": false,
		"created_at": time.Date(2026, 10, 27, 0, id%60, 0, 0, time.UTC).Format(time.RFC3339),
		"actor":      map[string]any{"id": 1, "login": "someone"}, "repo": map[string]any{"id": 9, "name": "acme/tool"},
		"payload": map[string]any{"action": "created"}}
}

func (f *feed) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hits++
	f.etags = append(f.etags, r.Header.Get("If-None-Match"))
	if r.Header.Get("If-None-Match") == f.etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	page := 1
	_, _ = fmt.Sscan(r.URL.Query().Get("page"), &page)
	start, end := (page-1)*2, min(page*2, len(f.ids))
	items := []map[string]any{}
	for _, id := range f.ids[min(start, len(f.ids)):end] {
		items = append(items, event(id))
	}
	if end < len(f.ids) {
		w.Header().Set("Link", fmt.Sprintf(`<https://%s%s?per_page=100&page=%d>; rel="next"`, r.Host, r.URL.Path, page+1))
	}
	w.Header().Set("ETag", f.etag)
	w.Header().Set("X-Poll-Interval", "60")
	b, _ := json.Marshal(items)
	_, _ = w.Write(b)
}

type world struct {
	gh  *githubtest.Server
	db  *storage.DB
	cfg *config.Config
	gc  *githubapi.Client
	r   *routing.Router
	clk *testutil.Clock
}

func newWorld(t *testing.T) *world {
	t.Helper()
	t.Setenv("PG_TEST_PAT", "ghp_x")
	w := &world{gh: githubtest.New(t), db: testutil.OpenDB(t), clk: testutil.NewClock(time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC))}
	w.cfg = testutil.Config(t, cfgYAML)
	w.r = routing.New(w.cfg)
	w.gc, _ = githubapi.New(githubapi.Options{BaseURL: w.gh.URL, HTTPClient: w.gh.Client(), Logger: testutil.Logger(),
		Sleep: func(context.Context, time.Duration) error { return nil }})
	return w
}

func (w *world) poller(i int) *Poller {
	a := &w.cfg.Accounts[i]
	var feeds []Feed
	for _, f := range a.Activity.Feeds {
		feeds = append(feeds, Feed{Config: f, Cred: githubauth.NewPAT("token:"+a.Key, a.Token)})
	}
	return &Poller{Account: a, Feeds: feeds, DB: w.db, Router: w.r, Capturer: &capture.Capturer{Domain: "github", Now: w.clk.Now},
		GitHub: w.gc, Log: testutil.Logger(), Now: w.clk.Now}
}

func (w *world) subjects(t *testing.T, acct string) []string {
	t.Helper()
	rows, _ := w.db.OutboxRows(context.Background(), storage.OutboxPending, 1000)
	var out []string
	for _, r := range rows {
		if r.AccountKey == acct {
			out = append(out, strings.TrimPrefix(r.Subject, "evt.github."+acct+"."))
		}
	}
	return out
}

func TestEventName(t *testing.T) {
	for in, want := range map[string]string{"PushEvent": "push", "PullRequestReviewCommentEvent": "pull_request_review_comment",
		"IssuesEvent": "issues", "Event": "unknown", "NewThingEvent": "new_thing"} {
		if got := EventName(in); got != want {
			t.Errorf("%s: %s", in, got)
		}
	}
}

func TestPollOverlapAndConditional(t *testing.T) {
	w := newWorld(t)
	ctx := context.Background()
	f := &feed{}
	w.gh.Handle("GET /users/alice/received_events", f.serve)
	p := w.poller(0)
	f.set(103, 102, 101)
	if _, err := p.PollFeed(ctx, p.Feeds[0]); err != nil {
		t.Fatal(err)
	}
	// The first run is a baseline bounded by max_pages (2 pages = 4 events here, all 3 fit).
	if got := w.subjects(t, "alice"); len(got) != 3 || got[0] != "activity.issue_comment.observed" {
		t.Fatalf("%v", got)
	}
	// Unchanged: the ETag is sent and a 304 changes nothing.
	pi, err := p.PollFeed(ctx, p.Feeds[0])
	if err != nil || pi != 0 || f.etags[len(f.etags)-1] == "" || len(w.subjects(t, "alice")) != 3 {
		t.Fatalf("conditional poll: %v %v", pi, err)
	}
	// Two new events: reading stops at the newest known event; nothing is published twice.
	f.set(105, 104, 103, 102, 101)
	if _, err := p.PollFeed(ctx, p.Feeds[0]); err != nil {
		t.Fatal(err)
	}
	if got := w.subjects(t, "alice"); len(got) != 5 {
		t.Fatalf("%v", got)
	}
	cp, _ := w.db.Checkpoint(ctx, "alice", Collector, "received_events")
	if !strings.Contains(cp.Cursor, `"last_id":"105"`) {
		t.Fatalf("cursor %s", cp.Cursor)
	}
}

// When the newest known event has fallen out of the bounded feed, a gap is published with its time window.
func TestPollGapDetection(t *testing.T) {
	w := newWorld(t)
	ctx := context.Background()
	f := &feed{}
	w.gh.Handle("GET /users/alice/received_events", f.serve)
	p := w.poller(0)
	f.set(10)
	_, _ = p.PollFeed(ctx, p.Feeds[0])
	f.set(20, 19, 18, 17, 16, 15) // 10 is gone and max_pages (2) stops at 17
	if _, err := p.PollFeed(ctx, p.Feeds[0]); err != nil {
		t.Fatal(err)
	}
	got := w.subjects(t, "alice")
	if got[len(got)-1] != "collector.coverage.gap_detected" {
		t.Fatalf("%v", got)
	}
	if n, _ := w.db.GapCount(ctx, time.Time{}); n != 1 {
		t.Fatalf("gaps %d", n)
	}
}

// The same GitHub event seen by two accounts is one observation with one publication per account.
func TestSharedObservationAcrossAccounts(t *testing.T) {
	w := newWorld(t)
	ctx := context.Background()
	f := &feed{}
	f.set(7)
	w.gh.Handle("GET /users/alice/received_events", f.serve)
	w.gh.Handle("GET /users/bob/received_events", f.serve)
	pa, pb := w.poller(0), w.poller(1)
	_, _ = pa.PollFeed(ctx, pa.Feeds[0])
	_, _ = pb.PollFeed(ctx, pb.Feeds[0])
	rows, _ := w.db.OutboxRows(ctx, storage.OutboxPending, 10)
	if len(rows) != 2 || rows[0].ObservationID != rows[1].ObservationID || rows[0].PublicationID == rows[1].PublicationID {
		t.Fatalf("rows %+v", rows)
	}
}

func TestPollAccessLost(t *testing.T) {
	w := newWorld(t)
	ctx := context.Background()
	w.gh.JSON("GET /users/alice/received_events", 403, `{"message":"Resource not accessible"}`)
	p := w.poller(0)
	for range 2 {
		if _, err := p.PollFeed(ctx, p.Feeds[0]); !githubapi.IsClass(err, githubapi.ClassForbidden) {
			t.Fatal(err)
		}
	}
	if got := w.subjects(t, "alice"); len(got) != 1 || got[0] != "collector.access.lost" {
		t.Fatalf("want one access.lost, got %v", got)
	}
}

func TestAuditLog(t *testing.T) {
	w := newWorld(t)
	ctx := context.Background()
	var phrases []string
	w.gh.Handle("GET /orgs/acme/audit-log", func(rw http.ResponseWriter, r *http.Request) {
		phrases = append(phrases, r.URL.Query().Get("phrase"))
		if r.URL.Query().Get("order") != "asc" {
			rw.WriteHeader(400)
			return
		}
		_, _ = rw.Write([]byte(`[{"_document_id":"d1","@timestamp":1761562800000,"action":"repo.create","actor":"alice","repo":"acme/new"},
			{"_document_id":"d2","@timestamp":1761562900000,"action":"protected_branch.policy_override","actor":"bob"}]`))
	})
	p := w.poller(2)
	for range 2 {
		if _, err := p.PollFeed(ctx, p.Feeds[0]); err != nil {
			t.Fatal(err)
		}
	}
	got := w.subjects(t, "acme")
	if len(got) != 2 || got[0] != "audit.repo.create" || got[1] != "audit.protected_branch.policy_override" {
		t.Fatalf("%v", got)
	}
	if phrases[1] != "created:>=2025-10-27T11:00:40Z" {
		t.Fatalf("second read must resume from the newest entry minus overlap: %q", phrases[1])
	}
}
