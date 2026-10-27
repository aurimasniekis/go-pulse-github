// Package outbox publishes queued publications to Pulse. Each account is a FIFO partition with at most one publication
// in flight; partitions run concurrently up to the worker limit. A publication is marked complete only after the
// JetStream acknowledgement, and every retry reuses its publication ID as the Pulse event ID.
package outbox

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/aurimasniekis/go-pulse-github/internal/events"
	"github.com/aurimasniekis/go-pulse-github/internal/observability"
	"github.com/aurimasniekis/go-pulse-github/internal/publisher"
	"github.com/aurimasniekis/go-pulse-github/internal/storage"
)

// HeaderAllowance is reserved for envelope headers when comparing a body with the server's max_payload.
const HeaderAllowance = 8 << 10

// Config tunes the dispatcher.
type Config struct {
	Workers        int
	BackoffInitial time.Duration
	BackoffMax     time.Duration
	PollInterval   time.Duration
	PublishTimeout time.Duration
	BlobsEnabled   bool
	// PoisonAttempts quarantines a publication the server keeps rejecting (ClassRejected) after this many attempts.
	// Outages and timeouts are retried without limit.
	PoisonAttempts int
	// MaxMessageBytes overrides the server max_payload when positive (and smaller).
	MaxMessageBytes int64
}

// Hooks inject faults in tests. AfterAck runs after the acknowledgement and before the completion is stored; an error
// simulates a crash in that window.
type Hooks struct {
	AfterAck func(row *storage.OutboxRow, ack *publisher.Ack) error
}

// Dispatcher publishes pending publications.
type Dispatcher struct {
	db      *storage.DB
	pub     publisher.Publisher
	cfg     Config
	log     *slog.Logger
	metrics *observability.Metrics
	now     func() time.Time
	hooks   Hooks

	wake chan struct{}
	sem  chan struct{}

	mu       sync.Mutex
	inflight map[string]bool
	wg       sync.WaitGroup
}

// New builds a dispatcher.
func New(db *storage.DB, pub publisher.Publisher, cfg Config, log *slog.Logger, m *observability.Metrics, now func() time.Time) *Dispatcher {
	if now == nil {
		now = time.Now
	}
	if cfg.Workers < 1 {
		cfg.Workers = 1
	}
	if cfg.PoisonAttempts < 1 {
		cfg.PoisonAttempts = 5
	}
	return &Dispatcher{
		db: db, pub: pub, cfg: cfg, log: log.With(observability.FieldComponent, "outbox"), metrics: m, now: now,
		wake:     make(chan struct{}, 1),
		sem:      make(chan struct{}, cfg.Workers),
		inflight: map[string]bool{},
	}
}

// SetHooks installs test hooks.
func (d *Dispatcher) SetHooks(h Hooks) { d.hooks = h }

// Notify wakes the dispatcher after new publications were committed.
func (d *Dispatcher) Notify() {
	select {
	case d.wake <- struct{}{}:
	default:
	}
}

// Run dispatches until ctx is cancelled, then waits for in-flight publications (each bounded by PublishTimeout).
func (d *Dispatcher) Run(ctx context.Context) error {
	t := time.NewTicker(d.cfg.PollInterval)
	defer t.Stop()
	defer d.wg.Wait()
	for {
		d.schedule(ctx)
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		case <-d.wake:
		}
	}
}

func (d *Dispatcher) schedule(ctx context.Context) {
	parts, err := d.db.PendingPartitions(ctx)
	if err != nil {
		if ctx.Err() == nil {
			d.sqliteError("pending_partitions", err)
		}
		return
	}
	for _, acct := range parts {
		d.mu.Lock()
		busy := d.inflight[acct]
		d.mu.Unlock()
		if busy {
			continue
		}
		select {
		case d.sem <- struct{}{}:
		default:
			return // all workers busy; the next tick continues fairly from the start of the sorted list
		}
		d.mu.Lock()
		d.inflight[acct] = true
		d.mu.Unlock()
		d.wg.Add(1)
		go func(acct string) {
			defer d.wg.Done()
			defer func() {
				d.mu.Lock()
				delete(d.inflight, acct)
				d.mu.Unlock()
				<-d.sem
			}()
			d.drain(ctx, acct)
		}(acct)
	}
}

// drain publishes an account's FIFO until it is empty, the head is not yet due, an attempt fails, or ctx ends. It
// publishes a bounded batch so one busy account yields its worker to others.
func (d *Dispatcher) drain(ctx context.Context, acct string) {
	for range 100 {
		if ctx.Err() != nil {
			return
		}
		row, err := d.db.HeadOfPartition(ctx, acct)
		if errors.Is(err, storage.ErrNotFound) {
			return
		}
		if err != nil {
			if ctx.Err() == nil {
				d.sqliteError("head_of_partition", err)
			}
			return
		}
		if row.NextAt.After(d.now()) {
			return
		}
		if !d.publishOne(ctx, row) {
			return
		}
	}
	d.Notify()
}

// PublishOne publishes a single row and reports whether the partition may continue.
func (d *Dispatcher) publishOne(ctx context.Context, row *storage.OutboxRow) bool {
	log := d.log.With(observability.FieldAccount, row.AccountKey, observability.FieldPublication, row.PublicationID,
		observability.FieldObservation, row.ObservationID, observability.FieldEventType, row.CEType,
		observability.FieldAttempt, row.Attempts+1)
	// Publishing outlives cancellation so shutdown finishes the attempt in flight instead of abandoning an ack.
	pctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), d.cfg.PublishTimeout)
	defer cancel()

	body, blob, class, err := d.prepare(pctx, row)
	if err != nil {
		return d.fail(ctx, row, log, class, err, class == publisher.ClassPayloadTooLarge)
	}
	start := d.now()
	ack, err := d.pub.Publish(pctx, publisher.Message{
		ID:      row.PublicationID,
		Subject: row.Subject,
		Type:    row.CEType,
		Time:    row.CETime,
		Body:    body,
		Headers: map[string]string{"github-time-basis": row.TimeBasis},
		Blob:    blob,
	})
	if err != nil {
		class, permanent := publisher.Classify(err)
		if class == publisher.ClassPayloadTooLarge && d.cfg.BlobsEnabled && row.Blob == nil {
			// The server limit is lower than the local estimate: retry through a blob.
			if _, uerr := d.uploadBlob(pctx, row); uerr == nil {
				return true
			}
		}
		return d.fail(ctx, row, log, class, err, permanent)
	}
	if d.metrics != nil {
		d.metrics.PublishDuration.Observe(d.now().Sub(start).Seconds())
	}
	if d.hooks.AfterAck != nil {
		if herr := d.hooks.AfterAck(row, ack); herr != nil {
			log.Warn("simulated crash after acknowledgement", observability.Err(herr))
			return false
		}
	}
	if err := d.db.MarkPublished(context.WithoutCancel(ctx), row.Seq, ack.Sequence, ack.Duplicate, d.now()); err != nil {
		// The event is in Pulse but not marked: it will be republished with the same ID (a duplicate outside the
		// broker's 2-minute window). Consumers deduplicate by event ID.
		d.sqliteError("mark_published", err)
		log.Error("publication acknowledged but completion not stored", observability.Err(err))
		return false
	}
	result := "published"
	if ack.Duplicate {
		result = "duplicate_ack"
	}
	if d.metrics != nil {
		d.metrics.Publications.WithLabelValues(row.AccountKey, result).Inc()
	}
	log.Debug("published", observability.FieldSubject, row.Subject, "stream_seq", ack.Sequence, "duplicate", ack.Duplicate)
	return true
}

// prepare returns the body to publish: the full document, or a blob stub when the document is too large.
func (d *Dispatcher) prepare(ctx context.Context, row *storage.OutboxRow) ([]byte, *storage.Blob, string, error) {
	if row.Blob != nil {
		stub, err := events.BlobStub(row.Payload, toRef(row.Blob))
		if err != nil {
			return nil, nil, publisher.ClassBlob, err
		}
		return stub, row.Blob, "", nil
	}
	if int64(len(row.Payload))+HeaderAllowance <= d.limit() {
		return row.Payload, nil, "", nil
	}
	if !d.cfg.BlobsEnabled {
		return nil, nil, publisher.ClassPayloadTooLarge, fmt.Errorf("payload is %d bytes, above the %d byte message limit, and blobs are disabled (enable pulse.blobs with a blobs grant, then replay)",
			len(row.Payload), d.limit())
	}
	blob, err := d.uploadBlob(ctx, row)
	if err != nil {
		return nil, nil, publisher.ClassBlob, err
	}
	stub, err := events.BlobStub(row.Payload, toRef(blob))
	if err != nil {
		return nil, nil, publisher.ClassBlob, err
	}
	return stub, blob, "", nil
}

// uploadBlob stores the full document and persists the reference before any publish, so a retry reuses the same blob
// instead of uploading another copy. A crash between upload and persist leaves an orphaned blob (documented).
func (d *Dispatcher) uploadBlob(ctx context.Context, row *storage.OutboxRow) (*storage.Blob, error) {
	blob, err := d.pub.PutBlob(ctx, row.Payload)
	if err != nil {
		return nil, fmt.Errorf("put blob: %w", err)
	}
	if err := d.db.SetOutboxBlob(context.WithoutCancel(ctx), row.Seq, *blob); err != nil {
		d.sqliteError("set_blob", err)
		return nil, err
	}
	row.Blob = blob
	return blob, nil
}

func (d *Dispatcher) limit() int64 {
	l := d.pub.MaxPayload()
	if d.cfg.MaxMessageBytes > 0 && d.cfg.MaxMessageBytes < l {
		l = d.cfg.MaxMessageBytes
	}
	return l
}

func (d *Dispatcher) fail(ctx context.Context, row *storage.OutboxRow, log *slog.Logger, class string, err error, permanent bool) bool {
	attempts := row.Attempts + 1
	rejected := class == publisher.ClassRejected
	if rejected && row.Rejections+1 >= d.cfg.PoisonAttempts {
		permanent = true
		err = fmt.Errorf("rejected %d times in a row: %w", row.Rejections+1, err)
	}
	next := d.now().Add(Backoff(attempts, d.cfg.BackoffInitial, d.cfg.BackoffMax))
	result := "failed"
	if permanent {
		result = "quarantined"
		log.Error("publication quarantined", observability.FieldErrorClass, class, observability.Err(err))
	} else {
		log.Warn("publish failed; will retry", observability.FieldErrorClass, class, observability.Err(err),
			"next_attempt_at", next.UTC().Format(time.RFC3339))
	}
	if d.metrics != nil {
		d.metrics.Publications.WithLabelValues(row.AccountKey, result).Inc()
	}
	if serr := d.db.PublishFailed(context.WithoutCancel(ctx), row.Seq, next, class, err.Error(), rejected, permanent); serr != nil {
		d.sqliteError("publish_failed", serr)
		return false
	}
	// A quarantined head no longer blocks its partition.
	return permanent
}

func (d *Dispatcher) sqliteError(op string, err error) {
	if d.metrics != nil {
		d.metrics.SQLiteErrors.WithLabelValues(op).Inc()
	}
	d.log.Error("storage error", "op", op, observability.Err(err))
}

func toRef(b *storage.Blob) events.BlobRef {
	return events.BlobRef{Bucket: b.Bucket, Name: b.Name, Size: b.Size, SHA256: b.SHA256}
}

// Backoff is exponential backoff with jitter: a random duration in [d/2, d], where d = initial·2^(attempt-1) capped at max.
func Backoff(attempt int, initial, maxDelay time.Duration) time.Duration {
	d := initial
	for i := 1; i < attempt && d < maxDelay; i++ {
		d *= 2
	}
	if d > maxDelay {
		d = maxDelay
	}
	half := d / 2
	if half <= 0 {
		return d
	}
	return half + rand.N(half+1) //nolint:gosec // jitter, not security
}
