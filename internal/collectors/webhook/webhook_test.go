package webhook

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aurimasniekis/go-pulse-github/internal/capture"
	"github.com/aurimasniekis/go-pulse-github/internal/githubauth"
	"github.com/aurimasniekis/go-pulse-github/internal/observability"
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
  - {login: aurimasniekis, type: user, webhook: {enabled: true}}
  - {login: example-org, type: organization, webhook: {enabled: true}}
routing:
  shares: [{from: example-org, to: aurimasniekis, sources: [webhook], repositories: {allow: [example-org/shared]}}]
`

type admission struct{ err error }

func (a *admission) Admit(context.Context) error { return a.err }

type installations struct {
	mu    sync.Mutex
	byID  map[int64]*storage.Installation
	err   error
	calls int
}

func (f *installations) Installation(_ context.Context, id int64) (*storage.Installation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	in, ok := f.byID[id]
	if !ok {
		return nil, ErrInstallationGone
	}
	return in, nil
}

type world struct {
	db    *storage.DB
	recv  *Receiver
	proc  *Processor
	adm   *admission
	insts *installations
	m     *observability.Metrics
}

var secret = []byte("It's a Secret to Everybody")

func newWorld(t *testing.T) *world {
	t.Helper()
	db := testutil.OpenDB(t)
	cfg := testutil.Config(t, cfgYAML)
	m := observability.NewMetrics()
	clock := testutil.NewClock(time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC))
	ver, err := githubauth.NewWebhookVerifier([]byte("old secret"), secret)
	if err != nil {
		t.Fatal(err)
	}
	w := &world{db: db, adm: &admission{}, m: m, insts: &installations{byID: map[int64]*storage.Installation{
		555: {ID: 555, AccountGitHubID: 7001, AccountLogin: "example-org", AccountType: "Organization"},
		777: {ID: 777, AccountGitHubID: 4242, AccountLogin: "stranger", AccountType: "User"},
	}}}
	router := routing.New(cfg)
	w.proc = &Processor{DB: db, Router: router, Capturer: &capture.Capturer{Domain: "github", Metrics: m, Now: clock.Now},
		Installations: w.insts, PoisonAttempts: 3, BackoffInitial: time.Nanosecond, BackoffMax: time.Nanosecond,
		Log: testutil.Logger(), Metrics: m, Now: clock.Now,
		OnAccountBound: func(ctx context.Context, key string, id int64, login, typ string) error {
			_, err := db.SyncAccount(ctx, storage.AccountRecord{Key: key, GitHubID: id, Login: login, Type: typ}, clock.Now())
			return err
		}}
	w.proc.Init()
	w.recv = &Receiver{DB: db, Verifier: ver, MaxBody: 1 << 20, Admission: w.adm, Metrics: m, Log: testutil.Logger(),
		Now: clock.Now, Notify: w.proc.Notify}
	return w
}

func (w *world) post(t *testing.T, event, delivery string, body []byte, mutate func(*http.Request)) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GitHub-Event", event)
	req.Header.Set("X-GitHub-Delivery", delivery)
	req.Header.Set("X-GitHub-Hook-ID", "123")
	req.Header.Set("X-Hub-Signature-256", githubauth.Sign(secret, body))
	if mutate != nil {
		mutate(req)
	}
	rec := httptest.NewRecorder()
	w.recv.ServeHTTP(rec, req)
	return rec
}

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile("../../../testdata/webhooks/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return bytes.TrimSpace(b)
}

// GitHub's documented example ("Validating webhook deliveries"): secret "It's a Secret to Everybody", payload
// "Hello, World!", signature sha256=757107ea0eb2509fc211221cce984b8a37570b6d7586c22c46f4379c8b043e17.
func TestVerifierGitHubVector(t *testing.T) {
	v, _ := githubauth.NewWebhookVerifier(secret)
	if _, err := v.Verify([]byte("Hello, World!"), "sha256=757107ea0eb2509fc211221cce984b8a37570b6d7586c22c46f4379c8b043e17"); err != nil {
		t.Fatal(err)
	}
	for name, h := range map[string]string{
		"no prefix": "757107ea0eb2509fc211221cce984b8a37570b6d7586c22c46f4379c8b043e17",
		"not hex":   "sha256=zz",
		"short":     "sha256=7571",
		"other":     "sha256=" + strings.Repeat("0", 64),
		"empty":     "",
	} {
		if _, err := v.Verify([]byte("Hello, World!"), h); !errors.Is(err, githubauth.ErrSignature) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// During rotation both the old and the new secret are accepted.
func TestVerifierRotation(t *testing.T) {
	v, _ := githubauth.NewWebhookVerifier([]byte("old"), []byte("new"))
	for i, s := range []string{"old", "new"} {
		if idx, err := v.Verify([]byte("b"), githubauth.Sign([]byte(s), []byte("b"))); err != nil || idx != i {
			t.Errorf("%s: %d %v", s, idx, err)
		}
	}
	if _, err := v.Verify([]byte("b"), githubauth.Sign([]byte("retired"), []byte("b"))); err == nil {
		t.Error("removed secret must be refused")
	}
}

func TestReceiverRejects(t *testing.T) {
	w := newWorld(t)
	body := fixture(t, "issues_opened.json")
	for name, c := range map[string]struct {
		code   int
		mutate func(*http.Request)
		body   []byte
	}{
		"bad signature":     {http.StatusUnauthorized, func(r *http.Request) { r.Header.Set("X-Hub-Signature-256", githubauth.Sign([]byte("x"), body)) }, body},
		"missing signature": {http.StatusUnauthorized, func(r *http.Request) { r.Header.Del("X-Hub-Signature-256") }, body},
		"tampered body": {http.StatusUnauthorized, func(r *http.Request) {
			r.Body = http.NoBody
			r.Header.Set("X-Hub-Signature-256", githubauth.Sign(secret, body))
		}, body},
		"form encoded":    {http.StatusUnsupportedMediaType, func(r *http.Request) { r.Header.Set("Content-Type", "application/x-www-form-urlencoded") }, body},
		"wrong method":    {http.StatusMethodNotAllowed, func(r *http.Request) { r.Method = http.MethodGet }, body},
		"bad delivery id": {http.StatusBadRequest, func(r *http.Request) { r.Header.Set("X-GitHub-Delivery", "../../x") }, body},
		"bad hook id":     {http.StatusBadRequest, func(r *http.Request) { r.Header.Set("X-GitHub-Hook-ID", "1;drop") }, body},
		"malformed json":  {http.StatusBadRequest, nil, []byte(`{"action":`)},
		"json array":      {http.StatusBadRequest, nil, []byte(`[1,2]`)},
		"oversized":       {http.StatusRequestEntityTooLarge, nil, bytes.Repeat([]byte("a"), 2<<20)},
	} {
		rec := w.post(t, "issues", "d-"+strings.ReplaceAll(name, " ", "-"), c.body, c.mutate)
		if rec.Code != c.code {
			t.Errorf("%s: %d, want %d", name, rec.Code, c.code)
		}
	}
	if st, _ := w.db.InboxStats(context.Background()); st.Pending != 0 {
		t.Fatalf("rejected requests must store nothing, %d stored", st.Pending)
	}
}

func TestReceiverDurableBeforeSuccess(t *testing.T) {
	w := newWorld(t)
	ctx := context.Background()
	body := fixture(t, "issues_opened.json")
	w.adm.err = errors.New("disk full")
	if rec := w.post(t, "issues", "d1", body, nil); rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") == "" {
		t.Fatalf("closed admission: %d", rec.Code)
	}
	if n, _ := w.db.InboxPending(ctx); n != 0 {
		t.Fatal("nothing may be stored while admission is closed")
	}
	w.adm.err = nil
	if rec := w.post(t, "issues", "d1", body, nil); rec.Code != http.StatusAccepted {
		t.Fatalf("accept: %d", rec.Code)
	}
	if rec := w.post(t, "issues", "d1", body, nil); rec.Code != http.StatusOK {
		t.Fatalf("redelivery: %d", rec.Code)
	}
	rows, _ := w.db.DueInbox(ctx, time.Now().Add(time.Hour), 10)
	if len(rows) != 1 || !bytes.Equal(rows[0].Body, body) || rows[0].HookID != "123" {
		t.Fatalf("stored %d rows", len(rows))
	}
	// A broken database answers 503, never 2xx.
	_ = w.db.Close()
	if rec := w.post(t, "issues", "d2", body, nil); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("storage failure: %d", rec.Code)
	}
}

func TestProcessRoutesThroughInstallation(t *testing.T) {
	w := newWorld(t)
	ctx := context.Background()
	w.post(t, "issues", "d1", fixture(t, "issues_opened.json"), nil)
	shared := bytes.Replace(fixture(t, "issues_opened.json"), []byte(`"example-org/tool"`), []byte(`"example-org/shared"`), 1)
	w.post(t, "issues", "d2", shared, nil)
	w.post(t, "ping", "d3", []byte(`{"zen":"x","hook_id":1}`), nil)
	w.post(t, "issues", "d4", []byte(`{"action":"opened","installation":{"id":777}}`), nil)
	w.post(t, "issues", "d5", []byte(`{"action":"opened","installation":{"id":999}}`), nil)
	if n, err := w.proc.ProcessDue(ctx, 50); err != nil || n != 5 {
		t.Fatalf("processed %d %v", n, err)
	}
	rows, _ := w.db.OutboxRows(ctx, storage.OutboxPending, 50)
	got := map[string]int{}
	for _, r := range rows {
		got[r.AccountKey+" "+r.Subject]++
	}
	want := map[string]int{
		"example-org evt.github.example-org.webhook.issues.opened":     2,
		"aurimasniekis evt.github.aurimasniekis.webhook.issues.opened": 1, // only the shared repository
	}
	if len(got) != len(want) || got["example-org evt.github.example-org.webhook.issues.opened"] != 2 ||
		got["aurimasniekis evt.github.aurimasniekis.webhook.issues.opened"] != 1 {
		t.Fatalf("publications %v", got)
	}
	// ping (no installation), a stranger's installation and an unknown installation are kept but unrouted.
	if st, _ := w.db.InboxStats(ctx); st.Unrouted != 3 || st.Pending != 0 {
		t.Fatalf("inbox %+v", st)
	}
	// The installation account's ID was pinned from the App API, not from payload logins.
	accts, _ := w.db.Accounts(ctx)
	for _, a := range accts {
		if a.Key == "example-org" && a.GitHubID != 7001 {
			t.Fatalf("pinned id %d", a.GitHubID)
		}
	}
	// The shared observation has one observation ID and two publication IDs.
	obs := map[string][]string{}
	for _, r := range rows {
		obs[r.ObservationID] = append(obs[r.ObservationID], r.PublicationID)
	}
	shares := 0
	for _, pubs := range obs {
		if len(pubs) == 2 && pubs[0] != pubs[1] {
			shares++
		}
	}
	if shares != 1 {
		t.Fatalf("want one shared observation, got %v", obs)
	}
}

func TestProcessInstallationLifecycle(t *testing.T) {
	w := newWorld(t)
	ctx := context.Background()
	created := []byte(`{"action":"created","installation":{"id":888,"app_id":1,"account":{"id":7001,"login":"example-org","type":"Organization"},
		"repository_selection":"selected","permissions":{"issues":"read"},"events":["issues"]},
		"repositories":[{"id":1,"full_name":"example-org/a","private":true}]}`)
	removed := []byte(`{"action":"removed","installation":{"id":888,"account":{"id":7001,"login":"example-org","type":"Organization"}},
		"repositories_added":[],"repositories_removed":[{"id":1,"full_name":"example-org/a","private":true}]}`)
	deleted := []byte(`{"action":"deleted","installation":{"id":888,"account":{"id":7001,"login":"example-org","type":"Organization"}}}`)
	w.post(t, "installation", "i1", created, nil)
	w.post(t, "installation_repositories", "i2", removed, nil)
	w.post(t, "installation", "i3", deleted, nil)
	if _, err := w.proc.ProcessDue(ctx, 50); err != nil {
		t.Fatal(err)
	}
	in, err := w.db.Installation(ctx, 888)
	if err != nil || in.Status != storage.InstallRemoved || in.RepositorySelection != "selected" {
		t.Fatalf("installation %+v %v", in, err)
	}
	repos, _ := w.db.InstallationRepos(ctx, 888)
	if len(repos) != 1 || repos[0].Status != storage.InstallRemoved {
		t.Fatalf("repos %+v", repos)
	}
	if w.insts.calls != 0 {
		t.Fatal("lifecycle events carry the installation; no API lookup needed")
	}
	rows, _ := w.db.OutboxRows(ctx, storage.OutboxPending, 10)
	if len(rows) != 3 || rows[2].Subject != "evt.github.example-org.webhook.installation.deleted" {
		t.Fatalf("lifecycle events are published too: %d", len(rows))
	}
}

func TestProcessRetriesAndQuarantines(t *testing.T) {
	w := newWorld(t)
	ctx := context.Background()
	// The App API is down: the delivery stays pending (not unrouted, not lost) and is retried.
	w.insts.err = errors.New("connection refused")
	w.post(t, "issues", "d1", fixture(t, "issues_opened.json"), nil)
	_, _ = w.proc.ProcessDue(ctx, 10)
	if st, _ := w.db.InboxStats(ctx); st.Pending != 1 {
		t.Fatalf("inbox %+v", st)
	}
	w.insts.err = nil
	time.Sleep(time.Millisecond)
	_, _ = w.proc.ProcessDue(ctx, 10)
	if st, _ := w.db.InboxStats(ctx); st.Pending != 0 || st.Unrouted != 0 {
		t.Fatalf("after recovery %+v", st)
	}
	// A body that is a JSON object but cannot be decoded as installation metadata is poison after 3 attempts.
	w.post(t, "installation", "bad", []byte(`{"installation":"nope"}`), nil)
	for range 3 {
		_, _ = w.proc.ProcessDue(ctx, 10)
	}
	q, _ := w.db.ListQuarantine(ctx, 10)
	if len(q) != 1 || q[0].ID != "bad" || q[0].ErrorClass != "malformed" {
		t.Fatalf("quarantine %+v", q)
	}
	if n, _ := w.db.ReplayQuarantine(ctx, storage.QuarantineInbox, "bad"); n != 1 {
		t.Fatal("replay")
	}
}
