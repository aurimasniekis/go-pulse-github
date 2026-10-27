package app

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aurimasniekis/go-pulse-github/internal/githubauth"
	"github.com/aurimasniekis/go-pulse-github/internal/githubtest"
	"github.com/aurimasniekis/go-pulse-github/internal/observability"
	"github.com/aurimasniekis/go-pulse-github/internal/pulsetest"
	"github.com/aurimasniekis/go-pulse-github/internal/storage"
	"github.com/aurimasniekis/go-pulse-github/internal/testutil"
)

func freeAddr(t *testing.T) string {
	t.Helper()
	var lc net.ListenConfig
	l, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

type e2e struct {
	env      *pulsetest.Env
	gh       *githubtest.Server
	hookAddr string
	admAddr  string
	db       *storage.DB
	cancel   context.CancelFunc
	done     chan error
}

// startService runs the whole service: real Pulse SDK against a Pulse-shaped NATS, fake GitHub App API, real
// listeners.
func startService(t *testing.T) *e2e {
	t.Helper()
	e := &e2e{env: pulsetest.Start(t, pulsetest.Options{}), gh: githubtest.New(t), hookAddr: freeAddr(t), admAddr: freeAddr(t)}
	e.gh.JSON("GET /app/installations/555", 200,
		`{"id":555,"app_id":1,"account":{"id":7001,"login":"example-org","type":"Organization"},"repository_selection":"all","permissions":{"issues":"read"},"events":["issues"]}`)
	creds, _ := e.env.Creds("pghe2e01", pulsetest.Grants{Publish: []string{"evt.github.>"}})
	t.Setenv("PG_E2E_HOOK_SECRET", "s3cret")
	dbPath := t.TempDir() + "/state.db"
	cfg := testutil.Config(t, fmt.Sprintf(`
version: 1
storage: {path: %s, min_free_bytes: 1}
pulse: {url: %s, creds_file: %s, publish_timeout: 2s}
outbox: {poll_interval: 50ms, backoff_initial: 50ms, backoff_max: 200ms}
webhook: {enabled: true, listen: %s, secrets: [{env: PG_E2E_HOOK_SECRET}]}
admin: {listen: %s}
github:
  api_url: %s
  app: {enabled: true, app_id: 1, private_key: {file: %s}}
accounts:
  - {login: example-org, type: organization, webhook: {enabled: true}}
`, dbPath, e.env.URL, creds, e.hookAddr, e.admAddr, e.gh.URL, githubtest.AppKey(t)))
	ctx, cancel := context.WithCancel(context.Background())
	db, err := storage.Open(ctx, dbPath, storage.Options{})
	if err != nil {
		t.Fatal(err)
	}
	e.db = db
	svc, err := New(ctx, cfg, db, testutil.Logger(), observability.NewMetrics(), Deps{HTTPClient: e.gh.Client()})
	if err != nil {
		t.Fatal(err)
	}
	e.cancel, e.done = cancel, make(chan error, 1)
	go func() { e.done <- svc.Run(ctx) }()
	t.Cleanup(func() { e.stop(t); _ = db.Close() })
	waitFor(t, "ready", func() bool {
		resp, err := http.Get("http://" + e.admAddr + "/readyz")
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	})
	return e
}

func (e *e2e) stop(t *testing.T) {
	if e.cancel == nil {
		return
	}
	e.cancel()
	e.cancel = nil
	select {
	case err := <-e.done:
		if err != nil {
			t.Errorf("run: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Error("shutdown hung")
	}
}

func waitFor(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (e *e2e) post(t *testing.T, delivery string, body []byte) int {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, "http://"+e.hookAddr+"/webhook", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GitHub-Event", "issues")
	req.Header.Set("X-GitHub-Delivery", delivery)
	req.Header.Set("X-Hub-Signature-256", githubauth.Sign([]byte("s3cret"), body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

func fixture(t *testing.T) []byte {
	t.Helper()
	b, err := os.ReadFile("../../testdata/webhooks/issues_opened.json")
	if err != nil {
		t.Fatal(err)
	}
	return bytes.TrimSpace(b)
}

// A signed webhook is durably stored before the 2xx and eventually published through the real Pulse SDK. During a
// NATS outage deliveries are still accepted and wait in the outbox; after recovery they are published.
func TestWebhookToPulse(t *testing.T) {
	e := startService(t)
	body := fixture(t)
	if code := e.post(t, "11111111-1111-1111-1111-111111111111", body); code != http.StatusAccepted {
		t.Fatalf("status %d", code)
	}
	waitFor(t, "first publication", func() bool { return len(e.env.Stored(">")) == 1 })
	m := e.env.Stored(">")[0]
	if m.Subject != "evt.github.example-org.webhook.issues.opened" || m.Header.Get("ce-type") != "github.webhook.issues.opened.v1" ||
		!bytes.HasSuffix(m.Data, append(append([]byte(`"payload":`), body...), '}')) {
		t.Fatalf("stored %s %v", m.Subject, m.Header)
	}
	if e.gh.Count("/app/installations/555") != 1 {
		t.Fatal("installation resolved through the App API once")
	}
	auth := e.gh.Requests()[0].Header.Get("Authorization")
	if !strings.HasPrefix(auth, "Bearer ey") {
		t.Fatalf("App JWT expected, got %q", auth[:min(len(auth), 12)])
	}

	e.env.Shutdown()
	if code := e.post(t, "22222222-2222-2222-2222-222222222222", body); code != http.StatusAccepted {
		t.Fatalf("webhook admission must stay open during a NATS outage: %d", code)
	}
	waitFor(t, "queued while NATS is down", func() bool {
		st, _ := e.db.OutboxStats(context.Background())
		return st.Pending == 1
	})
	e.env.Restart()
	waitFor(t, "publication after recovery", func() bool { return len(e.env.Stored(">")) == 2 })
	e.stop(t)
}
