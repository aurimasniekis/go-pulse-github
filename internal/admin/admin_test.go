package admin

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aurimasniekis/go-pulse-github/internal/observability"
	"github.com/aurimasniekis/go-pulse-github/internal/storage"
	"github.com/aurimasniekis/go-pulse-github/internal/testutil"
)

type nats struct{ up bool }

func (n nats) Connected() bool { return n.up }

func readyz(t *testing.T, g *Guard) (int, map[string]any) {
	t.Helper()
	rec := httptest.NewRecorder()
	Handler(g, observability.NewMetrics(), nats{false}, nil).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	var body map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	return rec.Code, body
}

// Readiness follows storage, not NATS: a NATS outage keeps admission open; low disk, a full inbox, a broken
// database or shutdown close it.
func TestGuard(t *testing.T) {
	db := testutil.OpenDB(t)
	ctx := context.Background()
	_ = db.WriteInstance(ctx, "test", "dev", time.Now())
	g := NewGuard(db, GuardConfig{MinFreeBytes: 1 << 20, MaxDBBytes: 1 << 30, MaxInboxPending: 2}, observability.NewMetrics(),
		testutil.Logger(), time.Now)
	free := uint64(1 << 30)
	g.FreeBytes = func(string) (uint64, error) { return free, nil }

	if err := g.Check(ctx); err != nil {
		t.Fatal(err)
	}
	if code, body := readyz(t, g); code != 200 || body["nats_connected"] != false {
		t.Fatalf("NATS down must not affect readiness: %d %v", code, body)
	}
	free = 1 << 10
	if err := g.Check(ctx); err == nil || !strings.Contains(err.Error(), "min_free_bytes") || !g.Pressure() {
		t.Fatalf("low disk: %v", err)
	}
	if code, _ := readyz(t, g); code != http.StatusServiceUnavailable {
		t.Fatal("not ready while storage is low")
	}
	free = 1 << 30
	for _, id := range []string{"a", "b"} {
		_, _ = db.InsertDelivery(ctx, storage.Delivery{DeliveryID: id, Event: "e", Body: []byte(`{}`), ReceivedAt: time.Now()})
	}
	if err := g.Check(ctx); err == nil || !strings.Contains(err.Error(), "max_inbox_pending") {
		t.Fatalf("inbox limit: %v", err)
	}
	g.FreeBytes = func(string) (uint64, error) { return 0, errors.New("statfs failed") }
	if err := g.Check(ctx); err == nil {
		t.Fatal("unknown free space must close admission")
	}
	g.FreeBytes = func(string) (uint64, error) { return free, nil }
	_ = db.Close()
	if err := g.Check(ctx); err == nil || !strings.Contains(err.Error(), "database") {
		t.Fatalf("broken database: %v", err)
	}
}

// The retention sweep frees pages but never shrinks the file. Only live data may count against max_db_bytes, or
// admission would stay closed for good once the file had grown past the cap.
func TestGuardSizeCapCountsLiveData(t *testing.T) {
	db := testutil.OpenDB(t)
	ctx := context.Background()
	_ = db.WriteInstance(ctx, "test", "dev", time.Now())
	g := NewGuard(db, GuardConfig{MaxDBBytes: 2 << 20, MaxInboxPending: 100}, nil, testutil.Logger(), time.Now)
	g.FreeBytes = func(string) (uint64, error) { return 1 << 40, nil }
	body := []byte(`{"x":"` + strings.Repeat("a", 1<<20) + `"}`)
	for _, d := range []string{"a", "b", "c"} {
		_, _ = db.InsertDelivery(ctx, storage.Delivery{DeliveryID: d, Event: "e", Body: body, ReceivedAt: time.Now()})
	}
	if err := g.Check(ctx); err == nil || !strings.Contains(err.Error(), "max_db_bytes") {
		t.Fatalf("3 MiB of live data under a 2 MiB cap: %v", err)
	}
	old := time.Now().Add(-time.Hour)
	rows, _ := db.DueInbox(ctx, time.Now(), 10)
	if len(rows) != 3 {
		t.Fatalf("inbox rows: %d", len(rows))
	}
	_ = db.Tx(ctx, func(tx *storage.Tx) error {
		for _, r := range rows {
			if err := tx.MarkInbox(ctx, r.ID, storage.InboxProcessed, old); err != nil {
				return err
			}
		}
		return nil
	})
	if _, err := db.Sweep(ctx, time.Now(), storage.RetentionPolicy{Published: time.Minute, InboxProcessed: time.Minute,
		Observations: 24 * time.Hour}); err != nil {
		t.Fatal(err)
	}
	if db.SizeBytes() <= 2<<20 {
		t.Fatalf("test needs a file larger than the cap after the sweep, got %d", db.SizeBytes())
	}
	if err := g.Check(ctx); err != nil {
		t.Fatalf("swept bodies must not keep admission closed: %v", err)
	}
}

func TestShutdownClosesAdmission(t *testing.T) {
	db := testutil.OpenDB(t)
	_ = db.WriteInstance(context.Background(), "test", "dev", time.Now())
	g := NewGuard(db, GuardConfig{MaxInboxPending: 10}, nil, testutil.Logger(), time.Now)
	g.FreeBytes = func(string) (uint64, error) { return 1 << 40, nil }
	_ = g.Check(context.Background())
	g.SetShuttingDown()
	if !errors.Is(g.Admit(context.Background()), ErrShuttingDown) {
		t.Fatal("admission must close during shutdown")
	}
}

func TestPprofNeedsToken(t *testing.T) {
	db := testutil.OpenDB(t)
	g := NewGuard(db, GuardConfig{}, nil, testutil.Logger(), time.Now)
	for token, want := range map[string]int{"": http.StatusNotFound, "wrong": http.StatusUnauthorized, "s3cret": http.StatusOK} {
		var tok []byte
		if token != "" {
			tok = []byte("s3cret")
		}
		req := httptest.NewRequest(http.MethodGet, "/debug/pprof/cmdline", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		Handler(g, observability.NewMetrics(), nil, tok).ServeHTTP(rec, req)
		if rec.Code != want {
			t.Errorf("token %q: %d, want %d", token, rec.Code, want)
		}
	}
}
