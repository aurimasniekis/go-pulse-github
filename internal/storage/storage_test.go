package storage

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func open(t *testing.T) *DB {
	t.Helper()
	db, err := Open(context.Background(), filepath.Join(t.TempDir(), "state.db"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestMigrationsAndPragmas(t *testing.T) {
	db := open(t)
	ctx := context.Background()
	v, err := db.SchemaVersion(ctx)
	if err != nil || v != "0002_observation_routes" {
		t.Fatalf("schema %q %v", v, err)
	}
	var mode string
	var sync int
	if err := db.r.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&mode); err != nil || mode != "wal" {
		t.Fatalf("journal_mode %q %v", mode, err)
	}
	if err := db.w.QueryRowContext(ctx, "PRAGMA synchronous").Scan(&sync); err != nil || sync != 2 {
		t.Fatalf("synchronous %d %v (want FULL=2)", sync, err)
	}
	// Re-opening applies nothing twice.
	path := db.Path()
	_ = db.Close()
	db2, err := Open(ctx, path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	_ = db2.Close()
}

func TestLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	l, err := AcquireLock(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := AcquireLock(path); !errors.Is(err, ErrLocked) {
		t.Fatalf("second lock: %v", err)
	}
	if err := l.Release(); err != nil {
		t.Fatal(err)
	}
	l2, err := AcquireLock(path)
	if err != nil {
		t.Fatal(err)
	}
	_ = l2.Release()
}

func TestInsertDeliveryDeduplicates(t *testing.T) {
	db := open(t)
	ctx := context.Background()
	d := Delivery{DeliveryID: "abc", Event: "issues", Body: []byte(`{}`), ReceivedAt: time.Now()}
	if ok, err := db.InsertDelivery(ctx, d); err != nil || !ok {
		t.Fatalf("first insert %v %v", ok, err)
	}
	if ok, err := db.InsertDelivery(ctx, d); err != nil || ok {
		t.Fatalf("redelivery must be a no-op: %v %v", ok, err)
	}
	rows, _ := db.DueInbox(ctx, time.Now(), 10)
	if len(rows) != 1 {
		t.Fatalf("%d rows", len(rows))
	}
}

func queue(t *testing.T, db *DB, obsID, acct string) {
	t.Helper()
	ctx := context.Background()
	err := db.Tx(ctx, func(tx *Tx) error {
		if _, _, err := tx.InsertObservation(ctx, Observation{ID: obsID, Source: "webhook", DedupKey: obsID, Event: "e",
			Action: "a", ObservedAt: time.Now(), Mode: "live"}, time.Now()); err != nil {
			return err
		}
		_, err := tx.InsertPublication(ctx, Publication{PublicationID: "p" + obsID + acct, ObservationID: obsID,
			AccountKey: acct, Subject: "evt.github.x.webhook.e.a", CEType: "t", CETime: time.Now(), TimeBasis: "observed",
			Payload: []byte(`{}`)}, time.Now())
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestOutboxFIFOAndQuarantine(t *testing.T) {
	db := open(t)
	ctx := context.Background()
	queue(t, db, "o1", "a")
	queue(t, db, "o2", "a")
	queue(t, db, "o3", "b")
	parts, _ := db.PendingPartitions(ctx)
	if len(parts) != 2 {
		t.Fatalf("partitions %v", parts)
	}
	head, _ := db.HeadOfPartition(ctx, "a")
	if head.ObservationID != "o1" {
		t.Fatalf("head %s", head.ObservationID)
	}
	if err := db.PublishFailed(ctx, head.Seq, time.Now(), "invalid_subject", "bad", true); err != nil {
		t.Fatal(err)
	}
	head, _ = db.HeadOfPartition(ctx, "a")
	if head.ObservationID != "o2" {
		t.Fatalf("a quarantined head must not block the partition, head %s", head.ObservationID)
	}
	q, _ := db.ListQuarantine(ctx, 10)
	if len(q) != 1 || q[0].ID != "po1a" || q[0].ErrorClass != "invalid_subject" {
		t.Fatalf("quarantine %+v", q)
	}
	if n, err := db.ReplayQuarantine(ctx, QuarantineOutbox, "po1a"); err != nil || n != 1 {
		t.Fatalf("replay %d %v", n, err)
	}
	head, _ = db.HeadOfPartition(ctx, "a")
	if head.PublicationID != "po1a" || head.Attempts != 0 {
		t.Fatalf("replayed row keeps its publication ID and position: %+v", head)
	}
	if err := db.MarkPublished(ctx, head.Seq, 7, false, time.Now()); err != nil {
		t.Fatal(err)
	}
	st, _ := db.OutboxStats(ctx)
	if st.Pending != 2 || st.Quarantined != 0 {
		t.Fatalf("stats %+v", st)
	}
}

func TestObservationSharedAcrossAccounts(t *testing.T) {
	db := open(t)
	queue(t, db, "o1", "a")
	queue(t, db, "o1", "b")
	queue(t, db, "o1", "b") // same (observation, account): no second publication
	rows, _ := db.OutboxByObservation(context.Background(), "o1")
	if len(rows) != 2 || rows[0].PublicationID == rows[1].PublicationID {
		t.Fatalf("want one publication per account, got %d", len(rows))
	}
}

// Published outbox rows are swept after 7 days but observations are kept for 35. Re-reading an item in between (a
// new activity feed reads 30 days back) must not queue it for the same account again; a new account still gets it.
func TestRouteOutlivesSweptPublication(t *testing.T) {
	db := open(t)
	ctx := context.Background()
	queue(t, db, "o1", "a")
	head, _ := db.HeadOfPartition(ctx, "a")
	_ = db.MarkPublished(ctx, head.Seq, 1, false, time.Now().Add(-10*24*time.Hour))
	if r, err := db.Sweep(ctx, time.Now(), RetentionPolicy{Published: 7 * 24 * time.Hour, InboxProcessed: time.Hour,
		Observations: 35 * 24 * time.Hour}); err != nil || r.Outbox != 1 {
		t.Fatalf("sweep %+v %v", r, err)
	}
	queue(t, db, "o1", "a")
	queue(t, db, "o1", "b")
	rows, _ := db.OutboxByObservation(ctx, "o1")
	if len(rows) != 1 || rows[0].AccountKey != "b" {
		t.Fatalf("want only the new account queued, got %+v", rows)
	}
}

func TestSyncAccount(t *testing.T) {
	db := open(t)
	ctx := context.Background()
	now := time.Now()
	if _, err := db.SyncAccount(ctx, AccountRecord{Key: "alice", Login: "alice", Type: "user"}, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SyncAccount(ctx, AccountRecord{Key: "alice", GitHubID: 1, Login: "alice", Type: "user"}, now); err != nil {
		t.Fatal(err)
	}
	if from, err := db.SyncAccount(ctx, AccountRecord{Key: "alice", GitHubID: 1, Login: "alice2", Type: "user"}, now); err != nil || from != "alice" {
		t.Fatalf("rename %q %v", from, err)
	}
	if _, err := db.SyncAccount(ctx, AccountRecord{Key: "alice", GitHubID: 2, Login: "x", Type: "user"}, now); !errors.Is(err, ErrAccountConflict) {
		t.Fatalf("different id for a pinned key: %v", err)
	}
	if _, err := db.SyncAccount(ctx, AccountRecord{Key: "other", GitHubID: 1, Login: "x", Type: "user"}, now); !errors.Is(err, ErrAccountConflict) {
		t.Fatalf("pinned id under another key: %v", err)
	}
}

// Unrouted deliveries follow processed-delivery retention, and a requeue takes only the newest that still have a body,
// bounded by its limit.
func TestUnroutedRetentionAndRequeue(t *testing.T) {
	db := open(t)
	ctx := context.Background()
	for _, id := range []string{"a", "b", "c"} {
		_, _ = db.InsertDelivery(ctx, Delivery{DeliveryID: id, Event: "e", Body: []byte(`{}`), ReceivedAt: time.Now()})
	}
	rows, _ := db.DueInbox(ctx, time.Now(), 10)
	ages := []time.Duration{10 * 24 * time.Hour, time.Hour, time.Minute} // a: past retention
	_ = db.Tx(ctx, func(tx *Tx) error {
		for i, r := range rows {
			if err := tx.MarkInbox(ctx, r.ID, InboxUnrouted, time.Now().Add(-ages[i])); err != nil {
				return err
			}
		}
		return nil
	})
	if _, err := db.Sweep(ctx, time.Now(), RetentionPolicy{Published: time.Hour, InboxProcessed: 7 * 24 * time.Hour,
		Observations: 35 * 24 * time.Hour}); err != nil {
		t.Fatal(err)
	}
	if r, _ := db.InboxByID(ctx, rows[0].ID); r == nil || r.Body != nil {
		t.Fatalf("unrouted body past retention must be dropped: %+v", r)
	}
	n, err := db.RequeueUnrouted(ctx, 1)
	if err != nil || n != 1 {
		t.Fatalf("requeue %d %v", n, err)
	}
	due, _ := db.DueInbox(ctx, time.Now(), 10)
	if len(due) != 1 || due[0].DeliveryID != "c" {
		t.Fatalf("requeue must take the newest first: %+v", due)
	}
	if n, _ := db.RequeueUnrouted(ctx, 10); n != 1 {
		t.Fatalf("only b still has a body: %d", n)
	}
}

func TestSweepKeepsUnfinishedWork(t *testing.T) {
	db := open(t)
	ctx := context.Background()
	queue(t, db, "o1", "a")
	queue(t, db, "o2", "a")
	head, _ := db.HeadOfPartition(ctx, "a")
	_ = db.MarkPublished(ctx, head.Seq, 1, false, time.Now().Add(-10*24*time.Hour))
	r, err := db.Sweep(ctx, time.Now(), RetentionPolicy{Published: 7 * 24 * time.Hour, InboxProcessed: time.Hour, Observations: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	st, _ := db.OutboxStats(ctx)
	if r.Outbox != 1 || st.Pending != 1 {
		t.Fatalf("sweep %+v pending %d", r, st.Pending)
	}
}
