package events

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aurimasniekis/go-pulse/pkg/pulse/envelope"
)

func TestToken(t *testing.T) {
	for in, want := range map[string]string{
		"issues":                     "issues",
		"pull_request_review_thread": "pull_request_review_thread",
		"none":                       "x-",
		"Opened":                     "x-",
		"evil.>":                     "x-",
		"a*b":                        "x-",
		"with space":                 "x-",
		strings.Repeat("a", 49):      "h-",
		strings.Repeat("b", 200):     "h-",
	} {
		got := Token(in)
		if got != want && (!strings.HasSuffix(want, "-") || !strings.HasPrefix(got, want)) {
			t.Errorf("Token(%q) = %q, want %q…", in, got, want)
		}
		if len(got) > 64 {
			t.Errorf("Token(%q) is %d bytes", in, len(got))
		}
		if err := envelope.ValidateSubject("evt.github.acct.webhook." + got + ".none"); err != nil {
			t.Errorf("Token(%q) = %q is not a valid Pulse token: %v", in, got, err)
		}
	}
}

// Encoded tokens always contain '-' and verbatim ones never do, so no two inputs share a token.
func TestTokenCollisionFree(t *testing.T) {
	seen := map[string]string{}
	for _, in := range []string{"none", "", "x", "x-", "X", "a.b", "a_b", "a-b", "x-f0", "opened", "Opened", "h-abc",
		strings.Repeat("z", 100), strings.Repeat("z", 101)} {
		tok := Token(in)
		if prev, ok := seen[tok]; ok {
			t.Fatalf("Token(%q) and Token(%q) collide on %q", prev, in, tok)
		}
		seen[tok] = in
	}
	if ActionToken("") != NoAction || ActionToken("none") == NoAction {
		t.Fatal("a missing action maps to the reserved token; a literal 'none' action must not")
	}
}

func TestIDs(t *testing.T) {
	a := ObservationID("webhook", "webhook:d1")
	if a != ObservationID("webhook", "webhook:d1") || a == ObservationID("webhook", "webhook:d2") {
		t.Fatal("observation IDs must be deterministic and distinct")
	}
	p1, p2 := PublicationID(a, "acct1"), PublicationID(a, "acct2")
	if p1 == p2 || p1 != PublicationID(a, "acct1") || len(p1) != 27 {
		t.Fatalf("publication IDs: %q %q", p1, p2)
	}
}

// The source JSON is embedded byte-for-byte: no number conversion (big integers, exponents), no HTML escaping, no
// whitespace changes.
func TestMarshalPreservesSource(t *testing.T) {
	raw := []byte(`{"id": 12345678901234567890123, "big": 1e400, "html": "<a&b>", "nested": {"x": [1, 2.50]}}`)
	out, err := Marshal(&Payload{SchemaVersion: 1, ObservationID: "o1", Payload: raw})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasSuffix(out, append(append([]byte(`"payload":`), raw...), '}')) || !json.Valid(out) {
		t.Fatalf("source not embedded verbatim:\n%s", out)
	}
	stub, err := BlobStub(out, BlobRef{Bucket: "blobs", Name: "n.json", Size: 10, SHA256: "aa"})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(stub, []byte(`"payload_blob":{"bucket":"blobs"`)) || !bytes.HasSuffix(stub, []byte(`"payload":null}`)) ||
		!bytes.Contains(stub, []byte(`"observation_id":"o1"`)) {
		t.Fatalf("bad blob stub: %s", stub)
	}
	if _, err := Marshal(&Payload{Payload: []byte(`{bad`)}); err == nil {
		t.Fatal("invalid source JSON must be refused, not altered")
	}
}

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile("../../testdata/webhooks/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return bytes.TrimSpace(b)
}

func TestNormalizeWebhook(t *testing.T) {
	recv := time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)
	for _, c := range []struct {
		name, event string
		body        []byte
		install     int64
		repo        string
		actor       string
		resource    string
		occurred    string
		subject     string
	}{
		{"issues opened", "issues", fixture(t, "issues_opened.json"), 555, "example-org/tool", "aurimasniekis", "issue",
			"2026-10-20T10:00:00Z", "issues.opened"},
		// push has no action; repository.pushed_at (Unix seconds in push payloads) is the push time.
		{"push", "push", fixture(t, "push.json"), 555, "example-org/tool", "someone", "ref", "2025-10-20T22:40:00Z", "push.none"},
		{"unknown event and action stay collectable", "future_thing", []byte(`{"action":"Did.Something","installation":{"id":1}}`),
			1, "", "", "", "", "future_thing." + Token("Did.Something")},
		{"missing optional fields", "issues", []byte(`{"action":"edited"}`), 0, "", "", "", "", "issues.edited"},
		{"non-string action", "weird", []byte(`{"action":{"x":1}}`), 0, "", "", "", "", "weird.none"},
		// edited carries no unambiguous event time: occurred_at stays null rather than guessing from updated_at.
		{"no guessed time", "issue_comment", []byte(`{"action":"edited","comment":{"id":5,"updated_at":"2026-01-01T00:00:00Z"}}`),
			0, "", "", "issue_comment", "", "issue_comment.edited"},
	} {
		d, meta, err := NormalizeWebhook(WebhookInput{DeliveryID: "d-1", HookID: "7", Event: c.event, Body: c.body, ReceivedAt: recv}, ModeLive)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if meta.InstallationID != c.install || meta.RepositoryFullName != c.repo || login(d.Actor) != c.actor || kind(d.Resource) != c.resource {
			t.Errorf("%s: install %d repo %q actor %q resource %q", c.name, meta.InstallationID, meta.RepositoryFullName,
				login(d.Actor), kind(d.Resource))
		}
		occ := ""
		if d.OccurredAt != nil {
			occ = d.OccurredAt.Format(time.RFC3339)
		}
		if occ != c.occurred {
			t.Errorf("%s: occurred %q, want %q", c.name, occ, c.occurred)
		}
		r, err := Render(d, "github", AccountRef{Key: "example-org", Login: "example-org", Type: "organization"},
			Routing{Match: MatchInstallation})
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if want := "evt.github.example-org.webhook." + c.subject; r.Subject != want {
			t.Errorf("%s: subject %q, want %q", c.name, r.Subject, want)
		}
		if !bytes.HasSuffix(r.Body, append(append([]byte(`"payload":`), c.body...), '}')) {
			t.Errorf("%s: original payload not preserved", c.name)
		}
		if basis := map[bool]string{true: TimeOccurred, false: TimeObserved}[c.occurred != ""]; r.TimeBasis != basis {
			t.Errorf("%s: time basis %q", c.name, r.TimeBasis)
		}
		if c.occurred == "" && !r.CETime.Equal(recv) {
			t.Errorf("%s: ce-time must fall back to observed_at", c.name)
		}
	}
}

// The monitored account, the actor and the repository owner are distinct fields; the routed account comes only from
// routing.
func TestRenderKeepsIdentitiesApart(t *testing.T) {
	d, _, err := NormalizeWebhook(WebhookInput{DeliveryID: "d", Event: "issues", Body: fixture(t, "issues_opened.json"),
		ReceivedAt: time.Now()}, ModeLive)
	if err != nil {
		t.Fatal(err)
	}
	r, err := Render(d, "github", AccountRef{Key: "personal", GitHubID: 15481, Login: "aurimasniekis", Type: "user"},
		Routing{Match: MatchShare, SharedFrom: "example-org"})
	if err != nil {
		t.Fatal(err)
	}
	var p Payload
	if err := json.Unmarshal(r.Body, &p); err != nil {
		t.Fatal(err)
	}
	if p.Account.Key != "personal" || p.Actor.Login != "aurimasniekis" || p.Repository.Owner.Login != "example-org" ||
		p.Routing.SharedFrom != "example-org" || p.Source.InstallationID != 555 || p.GitHub.DeliveryID != "d" {
		t.Fatalf("identities mixed up: %+v", p)
	}
	if p.SchemaVersion != 1 || p.CollectionMode != ModeLive || !p.Provenance.Authoritative || p.Provenance.Snapshot {
		t.Fatalf("metadata: %+v", p)
	}
}

func login(u *User) string {
	if u == nil {
		return ""
	}
	return u.Login
}

func kind(r *Resource) string {
	if r == nil {
		return ""
	}
	return r.Kind
}
