package enrich

import (
	"context"
	"encoding/json"
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

func TestEnrichment(t *testing.T) {
	gh := githubtest.New(t)
	gh.JSON("GET /repos/a/b/issues/1", 200, `{"id":11,"number":1,"node_id":"I_1","state":"open"}`)
	db := testutil.OpenDB(t)
	clock := testutil.NewClock(time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC))
	cfg := testutil.Config(t, "version: 1\npulse: {url: x, creds_file: y}\naccounts:\n  - {login: alice, type: user}\n")
	client, _ := githubapi.New(githubapi.Options{BaseURL: gh.URL, HTTPClient: gh.Client()})
	ctx := context.Background()
	_ = db.Tx(ctx, func(tx *storage.Tx) error {
		for _, u := range []string{gh.URL + "/repos/a/b/issues/1", "https://evil.example.com/repos/a/b/issues/1", "/repos/a/b/issues/404", "/repos/a/b/issues/1#2"} {
			_ = tx.EnqueueEnrichment(ctx, storage.EnrichmentJob{ObservationID: "osrc", AccountKey: "alice", Kind: "issue", URL: u}, clock.Now())
		}
		return nil
	})
	w := &Worker{DB: db, Router: routing.New(cfg), Capturer: &capture.Capturer{Domain: "github", Now: clock.Now}, GitHub: client,
		Accounts: map[string]Account{"alice": {Cred: githubauth.NewPAT("token:alice", config.SecretRef{Env: "PG_TEST_PAT"}), MaxPerHour: 3600}},
		Log:      testutil.Logger(), Now: clock.Now}
	t.Setenv("PG_TEST_PAT", "ghp_x")
	w.RunOnce(ctx)
	if gh.Count("/repos/a/b/issues/1") != 1 {
		t.Fatalf("fetched %d times", gh.Count("/repos/a/b/issues/1"))
	}
	if left, _ := db.PendingEnrichment(ctx, 10); len(left) != 0 {
		t.Fatalf("%d jobs left", len(left))
	}
	rows, _ := db.OutboxRows(ctx, storage.OutboxPending, 10)
	if len(rows) != 1 || rows[0].Subject != "evt.github.alice.enrichment.issue.fetched" {
		t.Fatalf("published %d", len(rows))
	}
	var p struct {
		CollectionMode string `json:"collection_mode"`
		OccurredAt     *time.Time
		Provenance     struct {
			Snapshot            bool     `json:"snapshot"`
			SourceObservationID string   `json:"source_observation_id"`
			Qualifiers          []string `json:"qualifiers"`
		} `json:"provenance"`
	}
	_ = json.Unmarshal(rows[0].Payload, &p)
	if p.CollectionMode != "enrichment" || !p.Provenance.Snapshot || p.Provenance.SourceObservationID != "osrc" || p.OccurredAt != nil {
		t.Fatalf("%+v", p)
	}
}
