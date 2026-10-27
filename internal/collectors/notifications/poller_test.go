package notifications

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aurimasniekis/go-pulse-github/internal/capture"
	"github.com/aurimasniekis/go-pulse-github/internal/config"
	"github.com/aurimasniekis/go-pulse-github/internal/githubapi"
	"github.com/aurimasniekis/go-pulse-github/internal/githubauth"
	"github.com/aurimasniekis/go-pulse-github/internal/githubtest"
	"github.com/aurimasniekis/go-pulse-github/internal/observability"
	"github.com/aurimasniekis/go-pulse-github/internal/routing"
	"github.com/aurimasniekis/go-pulse-github/internal/storage"
	"github.com/aurimasniekis/go-pulse-github/internal/testutil"
)

const cfgYAML = `
version: 1
pulse: {url: x, creds_file: y}
accounts:
  - login: aurimasniekis
    type: user
    token: {file: %s}
    notifications: {enabled: true, max_pages: %d, enrichment: {enabled: true}}
`

// fakeNotifications serves /notifications from a mutable thread list, newest first, 2 per page, with Last-Modified.
type fakeNotifications struct {
	mu           sync.Mutex
	threads      []map[string]any
	lastModified string
	pollInterval string
	queries      []string
	ims          []string
}

func (f *fakeNotifications) set(threads ...map[string]any) {
	f.mu.Lock()
	f.threads = threads
	f.lastModified = time.Now().UTC().Add(time.Duration(len(threads)) * time.Second).Format(http.TimeFormat)
	f.mu.Unlock()
}

func (f *fakeNotifications) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queries = append(f.queries, r.URL.RawQuery)
	f.ims = append(f.ims, r.Header.Get("If-Modified-Since"))
	if r.URL.Query().Get("all") != "true" {
		http.Error(w, "want all=true", 400)
		return
	}
	if ims := r.Header.Get("If-Modified-Since"); ims != "" && ims == f.lastModified {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	page := 1
	_, _ = fmt.Sscan(r.URL.Query().Get("page"), &page)
	start, end := (page-1)*2, min(page*2, len(f.threads))
	var items []map[string]any
	if start < len(f.threads) {
		items = f.threads[start:end]
	}
	if end < len(f.threads) {
		q := r.URL.Query()
		q.Set("page", fmt.Sprint(page+1))
		w.Header().Set("Link", fmt.Sprintf(`<https://%s/notifications?%s>; rel="next"`, r.Host, q.Encode()))
	}
	w.Header().Set("Last-Modified", f.lastModified)
	if f.pollInterval != "" {
		w.Header().Set("X-Poll-Interval", f.pollInterval)
	}
	b, _ := json.Marshal(items)
	_, _ = w.Write(b)
}

func mkThread(id, reason string, unread bool, updated string) map[string]any {
	return map[string]any{"id": id, "reason": reason, "unread": unread, "updated_at": updated, "last_read_at": nil,
		"url": "https://api.github.com/notifications/threads/" + id,
		"subject": map[string]any{"title": "T" + id, "type": "Issue", "url": "/repos/example-org/tool/issues/" + id,
			"latest_comment_url": nil},
		"repository": map[string]any{"id": 9001, "full_name": "example-org/tool", "private": true,
			"owner": map[string]any{"id": 7001, "login": "example-org", "type": "Organization"}}}
}

type world struct {
	gh     *githubtest.Server
	fake   *fakeNotifications
	db     *storage.DB
	clock  *testutil.Clock
	poller *Poller
	token  string
}

func newWorld(t *testing.T, maxPages int, scopes string) *world {
	t.Helper()
	w := &world{gh: githubtest.New(t), fake: &fakeNotifications{}, db: testutil.OpenDB(t),
		clock: testutil.NewClock(time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)), token: testutil.WriteSecret(t, "ghp_first")}
	w.gh.Handle("GET /user", func(rw http.ResponseWriter, r *http.Request) {
		if scopes != "-" {
			rw.Header().Set("X-OAuth-Scopes", scopes)
		}
		_, _ = rw.Write([]byte(`{"id":15481,"login":"aurimasniekis","type":"User"}`))
	})
	w.gh.Handle("GET /notifications", w.fake.serve)
	cfg := testutil.Config(t, fmt.Sprintf(cfgYAML, w.token, maxPages))
	w.poller = w.newPoller(t, cfg)
	return w
}

func (w *world) newPoller(t *testing.T, cfg *config.Config) *Poller {
	t.Helper()
	m := observability.NewMetrics()
	gh, err := githubapi.New(githubapi.Options{BaseURL: w.gh.URL, HTTPClient: w.gh.Client(), Metrics: m, Now: w.clock.Now,
		Sleep: func(context.Context, time.Duration) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	r := routing.New(cfg)
	return &Poller{Account: &cfg.Accounts[0], DB: w.db, Router: r, Capturer: &capture.Capturer{Domain: "github", Metrics: m, Now: w.clock.Now},
		GitHub: gh, Cred: githubauth.NewPAT("token:aurimasniekis", cfg.Accounts[0].Token), Log: testutil.Logger(), Metrics: m, Now: w.clock.Now}
}

func (w *world) subjects(t *testing.T) []string {
	t.Helper()
	rows, err := w.db.OutboxRows(context.Background(), storage.OutboxPending, 1000)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, r := range rows {
		out = append(out, strings.TrimPrefix(r.Subject, "evt.github.aurimasniekis."))
	}
	return out
}

func TestPollStateChanges(t *testing.T) {
	w := newWorld(t, 10, "notifications, read:user")
	ctx := context.Background()
	w.fake.set(mkThread("3", "mention", true, "2026-10-27T11:00:00Z"), mkThread("2", "subscribed", true, "2026-10-27T10:00:00Z"),
		mkThread("1", "author", false, "2026-10-26T10:00:00Z"))
	if _, err := w.poller.PollOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if got := w.subjects(t); len(got) != 3 || got[0] != "notifications.thread.observed" {
		t.Fatalf("first poll %v", got)
	}
	// Same data again (overlap window): no new observations, and the conditional request is answered with 304.
	w.clock.Advance(time.Minute)
	if _, err := w.poller.PollOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if got := w.subjects(t); len(got) != 3 {
		t.Fatalf("re-poll created %d", len(got))
	}
	if ims := w.fake.ims[len(w.fake.ims)-1]; ims == "" {
		t.Fatal("If-Modified-Since not sent")
	}
	// Thread 2 is read elsewhere: a meaningful change. Its updated_at did not move, so occurred_at stays null.
	t2 := mkThread("2", "subscribed", false, "2026-10-27T10:00:00Z")
	t2["last_read_at"] = "2026-10-27T11:30:00Z"
	w.fake.set(mkThread("3", "mention", true, "2026-10-27T11:00:00Z"), t2)
	w.clock.Advance(time.Minute)
	if _, err := w.poller.PollOnce(ctx); err != nil {
		t.Fatal(err)
	}
	got := w.subjects(t)
	if len(got) != 4 || got[3] != "notifications.thread.updated" {
		t.Fatalf("after state change %v", got)
	}
	rows, _ := w.db.OutboxRows(ctx, storage.OutboxPending, 10)
	var p struct {
		OccurredAt *time.Time `json:"occurred_at"`
		GitHub     struct {
			NotificationID string `json:"notification_id"`
		} `json:"github"`
		Provenance struct {
			PreviousFingerprint string `json:"previous_fingerprint"`
		} `json:"provenance"`
	}
	_ = json.Unmarshal(rows[3].Payload, &p)
	if p.OccurredAt != nil || p.GitHub.NotificationID != "2" || p.Provenance.PreviousFingerprint == "" || rows[3].TimeBasis != "observed" {
		t.Fatalf("read-state change: %+v basis %s", p, rows[3].TimeBasis)
	}
	// Enrichment jobs were queued for new threads; never for URLs outside the API origin.
	jobs, _ := w.db.PendingEnrichment(ctx, 10)
	if len(jobs) != 4 {
		t.Fatalf("enrichment jobs %d", len(jobs))
	}
}

// Checkpoints survive a restart: the new poller asks only for changes since the stored cursor minus the overlap.
func TestPollResumesAfterRestart(t *testing.T) {
	w := newWorld(t, 10, "repo")
	ctx := context.Background()
	w.fake.set(mkThread("1", "mention", true, "2026-10-27T11:00:00Z"))
	if _, err := w.poller.PollOnce(ctx); err != nil {
		t.Fatal(err)
	}
	cfg := testutil.Config(t, fmt.Sprintf(cfgYAML, w.token, 10))
	w.clock.Advance(time.Hour)
	if _, err := w.newPoller(t, cfg).PollOnce(ctx); err != nil {
		t.Fatal(err)
	}
	q := w.fake.queries[len(w.fake.queries)-1]
	if !strings.Contains(q, "since=2026-10-27T10%3A50%3A00Z") {
		t.Fatalf("restart query %q: want since = newest updated_at minus 10m overlap", q)
	}
	if got := w.subjects(t); len(got) != 1 {
		t.Fatalf("restart duplicated observations: %v", got)
	}
}

// A page budget that ends before the newest-first listing does leaves older changes unread: the cursor does not
// move and a coverage gap is published.
func TestPollPageBudgetGap(t *testing.T) {
	w := newWorld(t, 1, "notifications")
	ctx := context.Background()
	w.fake.set(mkThread("3", "mention", true, "2026-10-27T11:00:00Z"), mkThread("2", "mention", true, "2026-10-27T10:00:00Z"),
		mkThread("1", "mention", true, "2026-10-26T10:00:00Z"))
	if _, err := w.poller.PollOnce(ctx); err != nil {
		t.Fatal(err)
	}
	got := w.subjects(t)
	if len(got) != 3 || got[2] != "collector.coverage.gap_detected" {
		t.Fatalf("%v", got)
	}
	cp, _ := w.db.Checkpoint(ctx, "aurimasniekis", Collector, Feed)
	if strings.Contains(cp.Cursor, "since") {
		t.Fatalf("cursor advanced past unread pages: %s", cp.Cursor)
	}
}

func TestPollIdentityChecks(t *testing.T) {
	for scopes, want := range map[string]string{
		"-":         "classic personal access tokens", // fine-grained tokens report no scopes
		"read:user": "lacks the notifications or repo scope",
	} {
		w := newWorld(t, 10, scopes)
		_, err := w.poller.PollOnce(context.Background())
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("scopes %q: %v", scopes, err)
		}
		if got := w.subjects(t); len(got) != 1 || got[0] != "collector.access.lost" {
			t.Errorf("scopes %q: want access.lost, got %v", scopes, got)
		}
		if w.gh.Count("/notifications") != 0 {
			t.Error("notifications must not be read before the identity check passes")
		}
	}
	// A token of another user is refused even when the scopes are right.
	w := newWorld(t, 10, "notifications")
	w.gh.JSON("GET /user", 200, `{"id":1,"login":"someone-else","type":"User"}`, "X-OAuth-Scopes", "notifications")
	if _, err := w.poller.PollOnce(context.Background()); err == nil || !strings.Contains(err.Error(), "belongs to someone-else") {
		t.Fatalf("other user: %v", err)
	}
}

// Expired or rotated tokens: a 401 re-reads the mounted secret once and retries; a revoked token surfaces as
// access.lost and recovers as access.restored.
func TestPollTokenRotationAndRevocation(t *testing.T) {
	w := newWorld(t, 10, "notifications")
	ctx := context.Background()
	w.fake.set(mkThread("1", "mention", true, "2026-10-27T11:00:00Z"))
	valid := "Bearer ghp_second"
	w.gh.Handle("GET /notifications", func(rw http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != valid {
			rw.WriteHeader(http.StatusUnauthorized)
			_, _ = rw.Write([]byte(`{"message":"Bad credentials"}`))
			return
		}
		w.fake.serve(rw, r)
	})
	if err := os.WriteFile(w.token, []byte("ghp_second\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := w.poller.PollOnce(ctx); err != nil {
		t.Fatalf("rotation: %v", err)
	}
	valid = "revoked"
	w.clock.Advance(time.Minute)
	if _, err := w.poller.PollOnce(ctx); !githubapi.IsClass(err, githubapi.ClassUnauthorized) {
		t.Fatalf("revoked: %v", err)
	}
	valid = "Bearer ghp_second"
	w.clock.Advance(time.Minute)
	if _, err := w.poller.PollOnce(ctx); err != nil {
		t.Fatal(err)
	}
	got := w.subjects(t)
	if len(got) != 3 || got[1] != "collector.access.lost" || got[2] != "collector.access.restored" {
		t.Fatalf("%v", got)
	}
}

func TestPollRateLimitAndPollInterval(t *testing.T) {
	w := newWorld(t, 10, "notifications")
	ctx := context.Background()
	w.fake.set(mkThread("1", "mention", true, "2026-10-27T11:00:00Z"))
	w.fake.pollInterval = "120"
	calls := 0
	w.gh.Handle("GET /notifications", func(rw http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			rw.Header().Set("Retry-After", "30")
			rw.WriteHeader(http.StatusTooManyRequests)
			_, _ = rw.Write([]byte(`{"message":"You have exceeded a secondary rate limit"}`))
			return
		}
		w.fake.serve(rw, r)
	})
	interval, err := w.poller.PollOnce(ctx)
	if err != nil || calls != 2 {
		t.Fatalf("secondary limit not retried after Retry-After: %d calls, %v", calls, err)
	}
	if interval != 120*time.Second {
		t.Fatalf("X-Poll-Interval %v", interval)
	}
}
