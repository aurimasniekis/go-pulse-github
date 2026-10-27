package reconcile

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aurimasniekis/go-pulse-github/internal/capture"
	"github.com/aurimasniekis/go-pulse-github/internal/githubapi"
	"github.com/aurimasniekis/go-pulse-github/internal/githubtest"
	"github.com/aurimasniekis/go-pulse-github/internal/routing"
	"github.com/aurimasniekis/go-pulse-github/internal/storage"
	"github.com/aurimasniekis/go-pulse-github/internal/testutil"
)

const cfgYAML = `
version: 1
pulse: {url: x, creds_file: y}
webhook: {enabled: true, secrets: [{env: S}]}
github: {app: {enabled: true, app_id: 1, private_key: {env: K}}}
accounts:
  - login: acme
    type: organization
    webhook: {enabled: true}
    repositories: {deny: [acme/secret]}
    reconcile: {enabled: true, kinds: [issues, releases], max_requests_per_run: 3}
`

type staticCred struct{ name string }

func (c staticCred) Name() string                                { return c.name }
func (staticCred) Authorization(context.Context) (string, error) { return "Bearer t", nil }
func (staticCred) Invalidate()                                   {}

type tokens struct{}

func (tokens) Credential(_ int64, name string) githubapi.Credential { return staticCred{name} }

type world struct {
	gh    *githubtest.Server
	db    *storage.DB
	r     *routing.Router
	clock *testutil.Clock
	inst  *Installations
	snap  *Snapshots
}

func newWorld(t *testing.T) *world {
	t.Helper()
	w := &world{gh: githubtest.New(t), db: testutil.OpenDB(t), clock: testutil.NewClock(time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC))}
	w.r = routing.New(testutil.Config(t, cfgYAML))
	gc, _ := githubapi.New(githubapi.Options{BaseURL: w.gh.URL, HTTPClient: w.gh.Client(), Logger: testutil.Logger(),
		Sleep: func(context.Context, time.Duration) error { return nil }})
	capt := &capture.Capturer{Domain: "github", Now: w.clock.Now}
	w.inst = &Installations{App: &AppAPI{Client: gc, Cred: staticCred{"app"}}, Tokens: tokens{}, DB: w.db, Router: w.r,
		Capturer: capt, Log: testutil.Logger(), Now: w.clock.Now}
	w.snap = &Snapshots{DB: w.db, Router: w.r, Capturer: capt, GitHub: gc, Tokens: tokens{}, Log: testutil.Logger(), Now: w.clock.Now,
		Installation: func(context.Context, string) (int64, error) { return 5, nil }}
	return w
}

func (w *world) subjects(t *testing.T) []string {
	t.Helper()
	rows, _ := w.db.OutboxRows(context.Background(), storage.OutboxPending, 1000)
	var out []string
	for _, r := range rows {
		out = append(out, strings.TrimPrefix(r.Subject, "evt.github.acme."))
	}
	return out
}

const installations = `[{"id":5,"app_id":1,"account":{"id":7001,"login":"acme","type":"Organization"},"repository_selection":"selected"},
	{"id":6,"app_id":1,"account":{"id":9,"login":"stranger","type":"User"}}]`

func TestInstallationReconciliation(t *testing.T) {
	w := newWorld(t)
	ctx := context.Background()
	w.gh.JSON("GET /app/installations", 200, installations)
	w.gh.JSON("GET /installation/repositories", 200, `{"total_count":2,"repositories":[{"id":1,"full_name":"acme/a"},{"id":2,"full_name":"acme/b"}]}`)
	if err := w.inst.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if k, ok := w.r.AccountForGitHubID(7001); !ok || k != "acme" {
		t.Fatal("the account ID is pinned from the App's installation listing")
	}
	got := w.subjects(t)
	if len(got) != 3 || got[0] != "reconcile.installation.snapshot" || got[1] != "reconcile.installation_repository.snapshot" {
		t.Fatalf("%v", got)
	}
	// Repository b is no longer accessible: published as access_removed (not deleted). Then the App is uninstalled.
	w.gh.JSON("GET /installation/repositories", 200, `{"total_count":1,"repositories":[{"id":1,"full_name":"acme/a"}]}`)
	_ = w.inst.RunOnce(ctx)
	w.gh.JSON("GET /app/installations", 200, `[]`)
	_ = w.inst.RunOnce(ctx)
	got = w.subjects(t)
	want := []string{"reconcile.installation_repository.access_removed", "collector.access.lost", "reconcile.installation.removed"}
	if len(got) != 6 || strings.Join(got[3:], ",") != strings.Join(want, ",") {
		t.Fatalf("%v", got)
	}
	in, _ := w.db.Installation(ctx, 5)
	if in.Status != storage.InstallRemoved {
		t.Fatalf("status %s", in.Status)
	}
}

func issue(id int, updated string) string {
	return fmt.Sprintf(`{"id":%d,"number":%d,"state":"open","updated_at":%q,"created_at":"2026-10-01T00:00:00Z"}`, id, id, updated)
}

// A backfill interrupted mid-way (process restart) resumes from its stored cursor without re-reading finished pages,
// and a later reconciliation publishes only changed resources, as inferred changes.
func TestSnapshotsResumeAndChange(t *testing.T) {
	w := newWorld(t)
	ctx := context.Background()
	_ = w.db.Tx(ctx, func(tx *storage.Tx) error {
		for i, n := range []string{"acme/a", "acme/secret"} {
			_ = tx.SetInstallationRepo(ctx, storage.InstallationRepo{InstallationID: 5, RepoID: int64(i + 1), FullName: n}, w.clock.Now())
		}
		return nil
	})
	page2 := false
	w.gh.Handle("GET /repos/acme/a/issues", func(rw http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("direction") != "asc" || r.URL.Query().Get("since") == "" {
			rw.WriteHeader(400)
			return
		}
		if r.URL.Query().Get("page") == "2" {
			page2 = true
			_, _ = rw.Write([]byte(`[` + issue(3, "2026-10-26T00:00:00Z") + `]`))
			return
		}
		rw.Header().Set("Link", fmt.Sprintf(`<https://%s/repos/acme/a/issues?page=2&direction=asc&since=x>; rel="next"`, r.Host))
		_, _ = rw.Write([]byte(`[` + issue(1, "2026-10-20T00:00:00Z") + `,` + issue(2, "2026-10-21T00:00:00Z") + `]`))
	})
	w.gh.JSON("GET /repos/acme/a/releases", 200, `[{"id":50,"name":"v1","created_at":"2026-10-25T00:00:00Z","published_at":"2026-10-25T00:00:00Z"},
		{"id":49,"name":"v0","created_at":"2025-01-01T00:00:00Z"}]`)
	a, _ := w.r.Account("acme")
	id, _, err := EnqueueBackfill(ctx, w.db, a, w.clock.Now().Add(-30*24*time.Hour), nil, 50, w.clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	w.snap.Round(ctx) // first page only, then the "process" stops
	if page2 {
		t.Fatal("one request per round")
	}
	restarted := *w.snap
	for restarted.Round(ctx) {
	}
	j, _ := w.db.JobByID(ctx, id)
	if j.Status != storage.JobDone || j.RequestsUsed != 3 {
		t.Fatalf("job %+v", j)
	}
	if w.gh.Count("/repos/acme/secret/issues") != 0 {
		t.Fatal("denied repositories are never read")
	}
	got := w.subjects(t)
	// v0 is older than the window and stops the newest-first release listing.
	if strings.Join(got, ",") != "reconcile.issue.snapshot,reconcile.issue.snapshot,reconcile.issue.snapshot,reconcile.release.snapshot" {
		t.Fatalf("%v", got)
	}
	// Reconciliation: issue 2 changed; everything else is unchanged and not republished.
	w.gh.Handle("GET /repos/acme/a/issues", func(rw http.ResponseWriter, r *http.Request) {
		_, _ = rw.Write([]byte(`[` + issue(1, "2026-10-20T00:00:00Z") + `,` + issue(2, "2026-10-27T00:00:00Z") + `]`))
	})
	_, _, _ = w.db.EnqueueJob(ctx, storage.Job{AccountKey: "acme", Mode: ModeReconciliation, Kinds: []string{"issues"},
		Since: w.clock.Now().Add(-48 * time.Hour), MaxRequests: 10}, w.clock.Now())
	for w.snap.Round(ctx) {
	}
	got = w.subjects(t)
	if len(got) != 5 || got[4] != "reconcile.issue.changed" {
		t.Fatalf("%v", got)
	}
	rows, _ := w.db.OutboxRows(ctx, storage.OutboxPending, 10)
	if !strings.Contains(string(rows[4].Payload), `"inferred":true`) || !strings.Contains(string(rows[4].Payload), `"occurred_at":null`) ||
		!strings.Contains(string(rows[4].Payload), `"collection_mode":"reconciliation"`) {
		t.Fatalf("changed snapshot provenance: %s", rows[4].Payload[:300])
	}
}

// An inaccessible repository is skipped (never inferred as deleted); an exhausted budget ends the job with a gap.
func TestSnapshotsUnavailableAndBudget(t *testing.T) {
	w := newWorld(t)
	ctx := context.Background()
	_ = w.db.Tx(ctx, func(tx *storage.Tx) error {
		for i, n := range []string{"acme/a", "acme/b", "acme/c"} {
			_ = tx.SetInstallationRepo(ctx, storage.InstallationRepo{InstallationID: 5, RepoID: int64(i + 1), FullName: n}, w.clock.Now())
		}
		return nil
	})
	w.gh.JSON("GET /repos/acme/a/issues", 200, `[`+issue(1, "2026-10-26T00:00:00Z")+`]`)
	w.gh.JSON("GET /repos/acme/a/releases", 200, `[]`)
	// acme/b answers 404 (deleted, or no longer visible: ambiguous)
	_, _, _ = w.db.EnqueueJob(ctx, storage.Job{AccountKey: "acme", Mode: ModeReconciliation, Kinds: []string{"issues", "releases"},
		Since: w.clock.Now().Add(-48 * time.Hour), MaxRequests: 3}, w.clock.Now())
	for w.snap.Round(ctx) {
	}
	got := w.subjects(t)
	if len(got) != 2 || got[0] != "reconcile.issue.snapshot" || got[1] != "collector.coverage.gap_detected" {
		t.Fatalf("%v", got)
	}
	for _, s := range got {
		if strings.Contains(s, "deleted") {
			t.Fatal("a 404 must not be published as a deletion")
		}
	}
}
