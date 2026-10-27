package outbox

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/aurimasniekis/go-pulse-github/internal/events"
	"github.com/aurimasniekis/go-pulse-github/internal/observability"
	"github.com/aurimasniekis/go-pulse-github/internal/publisher"
	"github.com/aurimasniekis/go-pulse-github/internal/storage"
	"github.com/aurimasniekis/go-pulse-github/internal/testutil"
)

type world struct {
	db    *storage.DB
	pub   *testutil.FakePublisher
	clock *testutil.Clock
}

func newWorld(t *testing.T) *world {
	t.Helper()
	return &world{db: testutil.OpenDB(t), pub: testutil.NewFakePublisher(),
		clock: testutil.NewClock(time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC))}
}

func (w *world) dispatcher(blobs bool) *Dispatcher {
	return New(w.db, w.pub, Config{Workers: 2, BackoffInitial: time.Second, BackoffMax: time.Minute, PollInterval: time.Hour,
		PublishTimeout: 5 * time.Second, BlobsEnabled: blobs}, testutil.Logger(), observability.NewMetrics(), w.clock.Now)
}

// queue stores an observation for each account the way collectors do.
func (w *world) queue(t *testing.T, acct, dedup string, raw []byte) string {
	t.Helper()
	ctx := context.Background()
	d := &events.Draft{Source: events.SourceWebhook, DedupKey: dedup, Event: "issues", Action: "opened",
		ObservedAt: w.clock.Now(), Mode: events.ModeLive, Raw: raw}
	r, err := events.Render(d, "github", events.AccountRef{Key: acct, Login: acct, Type: "user"}, events.Routing{Match: events.MatchCredential})
	if err != nil {
		t.Fatal(err)
	}
	err = w.db.Tx(ctx, func(tx *storage.Tx) error {
		if _, _, err := tx.InsertObservation(ctx, storage.Observation{ID: d.ObservationID(), Source: d.Source, DedupKey: dedup,
			Event: d.Event, Action: d.Action, ObservedAt: d.ObservedAt, Mode: d.Mode}, w.clock.Now()); err != nil {
			return err
		}
		_, err := tx.InsertPublication(ctx, storage.Publication{PublicationID: r.PublicationID, ObservationID: d.ObservationID(),
			AccountKey: acct, Subject: r.Subject, CEType: r.CEType, CETime: r.CETime, TimeBasis: r.TimeBasis, Payload: r.Body}, w.clock.Now())
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return r.PublicationID
}

func (w *world) stats(t *testing.T) storage.OutboxStats {
	t.Helper()
	st, err := w.db.OutboxStats(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func TestPublishFIFOWithStableIDs(t *testing.T) {
	w := newWorld(t)
	var ids []string
	for i := range 3 {
		ids = append(ids, w.queue(t, "alice", fmt.Sprint("d", i), []byte(`{"n":1}`)))
	}
	w.dispatcher(false).drain(context.Background(), "alice")
	msgs := w.pub.Messages()
	if len(msgs) != 3 {
		t.Fatalf("published %d", len(msgs))
	}
	for i, m := range msgs {
		if m.ID != ids[i] || m.Subject != "evt.github.alice.webhook.issues.opened" || m.Type != "github.webhook.issues.opened.v1" ||
			m.Headers["github-time-basis"] != events.TimeObserved || !m.Time.Equal(w.clock.Now()) {
			t.Errorf("message %d: %+v", i, m)
		}
	}
	if st := w.stats(t); st.Pending != 0 {
		t.Fatalf("pending %d", st.Pending)
	}
}

// A Pulse outage leaves publications queued (never quarantined); after recovery they go out in order with the same IDs.
func TestNATSOutageAndRecovery(t *testing.T) {
	w := newWorld(t)
	ctx := context.Background()
	a := w.queue(t, "alice", "d1", []byte(`{}`))
	b := w.queue(t, "alice", "d2", []byte(`{}`))
	d := w.dispatcher(false)
	for _, err := range []error{nats.ErrConnectionReconnecting, context.DeadlineExceeded, nats.ErrNoResponders} {
		w.pub.SetErr(err)
		d.drain(ctx, "alice")
		w.clock.Advance(2 * time.Minute)
	}
	st := w.stats(t)
	if st.Pending != 2 || st.Quarantined != 0 || len(w.pub.Messages()) != 0 {
		t.Fatalf("during outage: %+v", st)
	}
	head, _ := w.db.HeadOfPartition(ctx, "alice")
	if head.Attempts != 3 || head.ErrorClass != publisher.ClassNoStream {
		t.Fatalf("head %+v", head)
	}
	w.pub.SetErr(nil)
	d.drain(ctx, "alice")
	msgs := w.pub.Messages()
	if len(msgs) != 2 || msgs[0].ID != a || msgs[1].ID != b {
		t.Fatalf("after recovery: %+v", msgs)
	}
}

// Backoff is honoured: a failed head is not retried before its next attempt time.
func TestBackoffDelaysRetry(t *testing.T) {
	w := newWorld(t)
	w.queue(t, "alice", "d1", []byte(`{}`))
	d := w.dispatcher(false)
	w.pub.SetErr(nats.ErrDisconnected)
	d.drain(context.Background(), "alice")
	w.pub.SetErr(nil)
	d.drain(context.Background(), "alice")
	if w.pub.Calls != 1 {
		t.Fatalf("retried before backoff elapsed: %d calls", w.pub.Calls)
	}
	w.clock.Advance(time.Minute)
	d.drain(context.Background(), "alice")
	if len(w.pub.Messages()) != 1 {
		t.Fatal("not retried after backoff")
	}
}

// Crash after the JetStream ack but before the local completion: on restart the publication is sent again with the
// same ID. Within the broker's duplicate window it is acknowledged as a duplicate; outside it, JetStream stores a
// second copy with the same event ID, which consumers must deduplicate.
func TestCrashAfterAckBeforeCompletion(t *testing.T) {
	for _, outsideWindow := range []bool{false, true} {
		w := newWorld(t)
		ctx := context.Background()
		id := w.queue(t, "alice", "d1", []byte(`{}`))
		d := w.dispatcher(false)
		d.SetHooks(Hooks{AfterAck: func(*storage.OutboxRow, *publisher.Ack) error { return errors.New("crash") }})
		d.drain(ctx, "alice")
		if st := w.stats(t); st.Pending != 1 {
			t.Fatal("must stay pending when the completion was not stored")
		}
		if outsideWindow {
			w.pub.Forget()
		}
		w.dispatcher(false).drain(ctx, "alice") // the restarted process
		msgs := w.pub.Messages()
		wantCopies := map[bool]int{false: 1, true: 2}[outsideWindow]
		if len(msgs) != wantCopies {
			t.Fatalf("outside window %v: %d stored copies, want %d", outsideWindow, len(msgs), wantCopies)
		}
		for _, m := range msgs {
			if m.ID != id {
				t.Fatalf("retry changed the event ID: %s != %s", m.ID, id)
			}
		}
		rows, _ := w.db.OutboxRows(ctx, storage.OutboxPublished, 10)
		if len(rows) != 1 {
			t.Fatal("not completed after the retry")
		}
	}
}

func TestPayloadTooLarge(t *testing.T) {
	w := newWorld(t)
	ctx := context.Background()
	w.pub.Max = 64 << 10
	big := []byte(`{"body":"` + strings.Repeat("x", 100<<10) + `"}`)
	id := w.queue(t, "alice", "big", big)
	small := w.queue(t, "alice", "small", []byte(`{}`))

	// Without blobs the oversized publication is quarantined with an actionable error and does not block others.
	w.dispatcher(false).drain(ctx, "alice")
	q, _ := w.db.ListQuarantine(ctx, 10)
	if len(q) != 1 || q[0].ID != id || q[0].ErrorClass != publisher.ClassPayloadTooLarge || !strings.Contains(q[0].Error, "enable pulse.blobs") {
		t.Fatalf("quarantine %+v", q)
	}
	if msgs := w.pub.Messages(); len(msgs) != 1 || msgs[0].ID != small {
		t.Fatalf("published %+v", msgs)
	}

	// After enabling blobs and replaying, the full document goes to the blob store and a stub is published.
	_, _ = w.db.ReplayQuarantine(ctx, storage.QuarantineOutbox, id)
	w.pub.SetErr(nats.ErrDisconnected)
	d := w.dispatcher(true)
	d.drain(ctx, "alice")
	row, _ := w.db.OutboxByPublicationID(ctx, id)
	if row.Blob == nil || len(w.pub.Blobs) != 1 {
		t.Fatal("blob reference must be persisted before publishing")
	}
	w.pub.SetErr(nil)
	w.clock.Advance(time.Hour)
	d.drain(ctx, "alice")
	if len(w.pub.Blobs) != 1 {
		t.Fatalf("a retry must reuse the stored blob, %d blobs uploaded", len(w.pub.Blobs))
	}
	msgs := w.pub.Messages()
	last := msgs[len(msgs)-1]
	if last.ID != id || last.Blob == nil || !bytes.Contains(last.Body, []byte(`"payload":null}`)) ||
		!bytes.Equal(w.pub.Blobs[last.Blob.Name], row.Payload) {
		t.Fatalf("blob publication %+v", last)
	}
}

func TestInvalidSubjectQuarantined(t *testing.T) {
	w := newWorld(t)
	ctx := context.Background()
	w.queue(t, "alice", "d1", []byte(`{}`))
	w.pub.SetErr(fmt.Errorf("publish: %w", errSubject))
	w.dispatcher(false).drain(ctx, "alice")
	if st := w.stats(t); st.Quarantined != 1 {
		t.Fatalf("%+v", st)
	}
}

// Shutdown waits for the publication in flight instead of abandoning its acknowledgement.
func TestShutdownFinishesInFlight(t *testing.T) {
	w := newWorld(t)
	w.queue(t, "alice", "d1", []byte(`{}`))
	slow := &slowPublisher{FakePublisher: w.pub, started: make(chan struct{}), release: make(chan struct{})}
	d := New(w.db, slow, Config{Workers: 1, BackoffInitial: time.Second, BackoffMax: time.Minute, PollInterval: time.Hour,
		PublishTimeout: 5 * time.Second}, testutil.Logger(), observability.NewMetrics(), w.clock.Now)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = d.Run(ctx); close(done) }()
	<-slow.started
	cancel()
	close(slow.release)
	<-done
	if st := w.stats(t); st.Pending != 0 {
		t.Fatal("in-flight publication was not completed on shutdown")
	}
}

type slowPublisher struct {
	*testutil.FakePublisher
	started chan struct{}
	release chan struct{}
}

func (s *slowPublisher) Publish(ctx context.Context, m publisher.Message) (*publisher.Ack, error) {
	close(s.started)
	<-s.release
	return s.FakePublisher.Publish(ctx, m)
}

func TestBackoff(t *testing.T) {
	for attempt, want := range map[int]time.Duration{1: time.Second, 2: 2 * time.Second, 5: 16 * time.Second, 30: time.Minute} {
		for range 20 {
			if got := Backoff(attempt, time.Second, time.Minute); got < want/2 || got > want {
				t.Fatalf("attempt %d: %v not in [%v, %v]", attempt, got, want/2, want)
			}
		}
	}
}
