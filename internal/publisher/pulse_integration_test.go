package publisher_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/aurimasniekis/go-pulse/pkg/pulse/envelope"

	"github.com/aurimasniekis/go-pulse-github/internal/publisher"
	"github.com/aurimasniekis/go-pulse-github/internal/pulsetest"
	"github.com/aurimasniekis/go-pulse-github/internal/testutil"
)

func connect(t *testing.T, env *pulsetest.Env, g pulsetest.Grants) *publisher.Pulse {
	t.Helper()
	creds, _ := env.Creds("pghtest01", g)
	p, err := publisher.Connect(publisher.Options{URL: env.URL, CredsFile: creds, Source: "pulse-github", Name: "pulse-github", Logger: testutil.Logger(),
		ExtraNATS: []nats.Option{nats.ReconnectWait(50 * time.Millisecond)}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	deadline := time.Now().Add(5 * time.Second)
	for !p.Connected() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	return p
}

func msg(id string) publisher.Message {
	return publisher.Message{ID: id, Subject: "evt.github.alice.webhook.issues.opened", Type: "github.webhook.issues.opened.v1",
		Time: time.Date(2026, 10, 20, 10, 0, 0, 0, time.UTC), Body: []byte(`{"schema_version":1,"n":12345678901234567890}`),
		Headers: map[string]string{"github-time-basis": "occurred"}}
}

// The real SDK, least-privilege creds (publish:evt.github.>) and a Pulse-shaped EVENTS stream: the publish is
// acknowledged, stored with the Pulse envelope, and the client's own inbox prefix carries the ack.
func TestPulsePublishEnvelope(t *testing.T) {
	env := pulsetest.Start(t, pulsetest.Options{})
	p := connect(t, env, pulsetest.Grants{Publish: []string{"evt.github.>"}})
	if p.Client().ClientID() != "pghtest01" || p.MaxPayload() != 1<<20 {
		t.Fatalf("client id %q max payload %d", p.Client().ClientID(), p.MaxPayload())
	}
	ack, err := p.Publish(context.Background(), msg("p1"))
	if err != nil || ack.Stream != "EVENTS" || ack.Duplicate {
		t.Fatalf("%+v %v", ack, err)
	}
	stored := env.Stored(">")
	if len(stored) != 1 {
		t.Fatalf("%d stored", len(stored))
	}
	m := stored[0]
	for k, want := range map[string]string{"Nats-Msg-Id": "p1", "ce-type": "github.webhook.issues.opened.v1",
		"ce-source": "pulse-github", "ce-time": "2026-10-20T10:00:00Z", "content-type": "application/json",
		"github-time-basis": "occurred"} {
		if got := m.Header.Get(k); got != want {
			t.Errorf("%s = %q, want %q", k, got, want)
		}
	}
	if !bytes.Equal(m.Data, msg("p1").Body) {
		t.Errorf("payload changed: %s", m.Data)
	}
}

func TestPulseDuplicates(t *testing.T) {
	// Within the duplicate window a retry with the same ID is acknowledged as a duplicate and stored once.
	env := pulsetest.Start(t, pulsetest.Options{})
	p := connect(t, env, pulsetest.Grants{Publish: []string{"evt.github.>"}})
	ctx := context.Background()
	_, _ = p.Publish(ctx, msg("p1"))
	ack, err := p.Publish(ctx, msg("p1"))
	if err != nil || !ack.Duplicate || len(env.Stored(">")) != 1 {
		t.Fatalf("within window: %+v %v", ack, err)
	}

	// Outside the window (shortened here) the broker stores a second copy with the same event ID; consumers must
	// deduplicate by event ID.
	short := pulsetest.Start(t, pulsetest.Options{DuplicateWindow: 100 * time.Millisecond})
	p2 := connect(t, short, pulsetest.Grants{Publish: []string{"evt.github.>"}})
	_, _ = p2.Publish(ctx, msg("p1"))
	time.Sleep(300 * time.Millisecond)
	ack, err = p2.Publish(ctx, msg("p1"))
	stored := short.Stored(">")
	if err != nil || ack.Duplicate || len(stored) != 2 || stored[0].Header.Get("Nats-Msg-Id") != stored[1].Header.Get("Nats-Msg-Id") {
		t.Fatalf("outside window: %+v %v, %d stored", ack, err, len(stored))
	}
}

// Grants are enforced by NATS: publishing outside evt.github.> fails (Pulse reports denials as timeouts), and blobs
// need the blobs grant.
func TestPulseLeastPrivilege(t *testing.T) {
	env := pulsetest.Start(t, pulsetest.Options{})
	p := connect(t, env, pulsetest.Grants{Publish: []string{"evt.github.>"}})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	m := msg("p2")
	m.Subject = "evt.other.thing.happened"
	if _, err := p.Publish(ctx, m); err == nil {
		t.Fatal("publish outside the grant must fail")
	} else if class, permanent := publisher.Classify(err); class != publisher.ClassTimeout || permanent {
		t.Fatalf("denied publish classified as %s (permanent %v): %v", class, permanent, err)
	}
	bctx, bcancel := context.WithTimeout(context.Background(), time.Second)
	defer bcancel()
	if _, err := p.PutBlob(bctx, []byte("x")); err == nil {
		t.Fatal("blob without blobs grant must fail")
	}
	m.Subject = "evt.GitHub.Bad"
	if _, err := p.Publish(context.Background(), m); err == nil {
		t.Fatal("invalid subject must be rejected by the SDK")
	} else if class, permanent := publisher.Classify(err); class != publisher.ClassInvalidSubject || !permanent {
		t.Fatalf("classified %s %v", class, permanent)
	}
}

func TestPulseBlobRoundTrip(t *testing.T) {
	env := pulsetest.Start(t, pulsetest.Options{})
	p := connect(t, env, pulsetest.Grants{Publish: []string{"evt.github.>"}, Blobs: true})
	ctx := context.Background()
	body := bytes.Repeat([]byte("y"), 2<<20)
	ref, err := p.PutBlob(ctx, body)
	if err != nil {
		t.Fatal(err)
	}
	m := msg("p3")
	m.Blob = ref
	if _, err := p.Publish(ctx, m); err != nil {
		t.Fatal(err)
	}
	stored := env.Stored(">")
	if stored[0].Header.Get("pulse-blob") != "blobs/"+ref.Name || stored[0].Header.Get("pulse-blob-sha256") != ref.SHA256 {
		t.Fatalf("blob headers %v", stored[0].Header)
	}
	got, err := p.Client().GetBlob(ctx, envelope.BlobRef{Bucket: ref.Bucket, Name: ref.Name, Size: ref.Size, SHA256: ref.SHA256})
	if err != nil || !bytes.Equal(got, body) {
		t.Fatalf("round trip: %v", err)
	}
}

// A NATS outage makes publishes fail with a transient class; after the server returns the client reconnects on
// its own and publishing resumes.
func TestPulseOutageAndRecovery(t *testing.T) {
	env := pulsetest.Start(t, pulsetest.Options{})
	p := connect(t, env, pulsetest.Grants{Publish: []string{"evt.github.>"}})
	env.Shutdown()
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	_, err := p.Publish(ctx, msg("p4"))
	cancel()
	if err == nil {
		t.Fatal("publish during outage must fail")
	}
	if _, permanent := publisher.Classify(err); permanent {
		t.Fatalf("outage errors are transient: %v", err)
	}
	env.Restart()
	deadline := time.Now().Add(10 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err = p.Publish(ctx, msg("p4"))
		cancel()
		if err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("no recovery: %v", err)
	}
	if !p.Connected() || len(env.Stored(">")) != 1 {
		t.Fatal("stored after recovery")
	}
}
