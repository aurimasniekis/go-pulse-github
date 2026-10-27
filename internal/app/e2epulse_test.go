//go:build e2epulse

package app

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/aurimasniekis/go-pulse/pkg/pulse"

	"github.com/aurimasniekis/go-pulse-github/internal/githubtest"
	"github.com/aurimasniekis/go-pulse-github/internal/observability"
	"github.com/aurimasniekis/go-pulse-github/internal/storage"
	"github.com/aurimasniekis/go-pulse-github/internal/testutil"
)

// TestRealPulse runs against a real pulsed (scripts/e2e-pulse.sh): least-privilege client creds issued by Pulse's
// admin API, Pulse's own NATS configuration and EVENTS stream.
func TestRealPulse(t *testing.T) {
	url, creds, ca := os.Getenv("PULSE_E2E_URL"), os.Getenv("PULSE_E2E_CREDS"), os.Getenv("PULSE_E2E_CA")
	if url == "" {
		t.Skip("run scripts/e2e-pulse.sh")
	}
	gh := githubtest.New(t)
	gh.JSON("GET /app/installations/555", 200,
		`{"id":555,"app_id":1,"account":{"id":7001,"login":"example-org","type":"Organization"}}`)
	t.Setenv("PG_E2E_HOOK_SECRET", "s3cret")
	hookAddr, admAddr := freeAddr(t), freeAddr(t)
	dbPath := t.TempDir() + "/state.db"
	cfg := testutil.Config(t, fmt.Sprintf(`
version: 1
storage: {path: %s, min_free_bytes: 1}
pulse: {url: %s, creds_file: %s, ca_file: %s}
webhook: {enabled: true, listen: %s, secrets: [{env: PG_E2E_HOOK_SECRET}]}
admin: {listen: %s}
github:
  api_url: %s
  app: {enabled: true, app_id: 1, private_key: {file: %s}}
accounts:
  - {login: example-org, type: organization, webhook: {enabled: true}}
`, dbPath, url, creds, ca, hookAddr, admAddr, gh.URL, githubtest.AppKey(t)))
	ctx, cancel := context.WithCancel(context.Background())
	db, err := storage.Open(ctx, dbPath, storage.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	svc, err := New(ctx, cfg, db, testutil.Logger(), observability.NewMetrics(), Deps{HTTPClient: gh.Client()})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()
	defer func() { cancel(); <-done }()

	e := &e2e{hookAddr: hookAddr, admAddr: admAddr}
	waitFor(t, "ready", func() bool {
		resp, err := http.Get("http://" + admAddr + "/readyz")
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	})
	start := time.Now()
	body := fixture(t)
	if code := e.post(t, "33333333-3333-3333-3333-333333333333", body); code != http.StatusAccepted {
		t.Fatalf("status %d", code)
	}

	c, err := pulse.Connect(url, pulse.WithCredsFile(creds), pulse.WithCAFile(ca))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	got := make(chan pulse.Event, 1)
	sub, err := c.Subscribe(ctx, "evt.github.example-org.>", func(_ context.Context, ev pulse.Event) error {
		select {
		case got <- ev:
		default:
		}
		return nil
	}, pulse.Since(start.Add(-time.Minute)))
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Stop()
	select {
	case ev := <-got:
		if ev.Subject != "evt.github.example-org.webhook.issues.opened" || ev.Type != "github.webhook.issues.opened.v1" ||
			ev.Source != "pulse-github" || !bytes.HasSuffix(ev.Data, append(append([]byte(`"payload":`), body...), '}')) {
			t.Fatalf("event %s %s %s", ev.Subject, ev.Type, ev.Source)
		}
		t.Logf("replayed %s (id %s, stream seq %d) from real Pulse", ev.Subject, ev.ID, ev.Sequence)
	case <-time.After(30 * time.Second):
		t.Fatal("event not replayed from Pulse")
	}
}
