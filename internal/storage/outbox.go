package storage

import (
	"context"
	"database/sql"
	"time"
)

// Outbox states.
const (
	OutboxPending     = "pending"
	OutboxPublished   = "published"
	OutboxQuarantined = "quarantined"
)

// Observation is one distinct thing a collector saw.
type Observation struct {
	ID         string
	Source     string
	DedupKey   string
	Event      string
	Action     string
	OccurredAt *time.Time
	ObservedAt time.Time
	Mode       string
	InboxID    int64
}

// Publication is one routed, rendered Pulse event.
type Publication struct {
	PublicationID string
	ObservationID string
	AccountKey    string
	Subject       string
	CEType        string
	CETime        time.Time
	TimeBasis     string
	Payload       []byte
}

// Blob is a persisted Pulse blob reference.
type Blob struct {
	Bucket string
	Name   string
	Size   int64
	SHA256 string
}

// OutboxRow is a stored publication.
type OutboxRow struct {
	Seq int64
	Publication
	Blob       *Blob
	State      string
	Attempts   int
	NextAt     time.Time
	LastError  string
	ErrorClass string
	CreatedAt  time.Time
}

// InsertObservation stores an observation. It reports false when the dedup key already exists; existingID is then the
// stored observation's ID.
func (t *Tx) InsertObservation(ctx context.Context, o Observation, now time.Time) (inserted bool, existingID string, err error) {
	var inbox any
	if o.InboxID != 0 {
		inbox = o.InboxID
	}
	res, err := t.exec(ctx, `INSERT INTO observations (observation_id, source, dedup_key, event, action, occurred_at,
		observed_at, mode, inbox_id, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING`,
		o.ID, o.Source, o.DedupKey, o.Event, o.Action, nullMS(o.OccurredAt), ms(o.ObservedAt), o.Mode, inbox, ms(now))
	if err != nil {
		return false, "", err
	}
	if n, _ := res.RowsAffected(); n == 1 {
		return true, o.ID, nil
	}
	err = t.tx.QueryRowContext(ctx, `SELECT observation_id FROM observations WHERE dedup_key = ?`, o.DedupKey).Scan(&existingID)
	return false, existingID, err
}

// ObservationExists reports whether a dedup key was already stored.
func (t *Tx) ObservationExists(ctx context.Context, dedupKey string) (bool, error) {
	var n int
	err := t.tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM observations WHERE dedup_key = ?`, dedupKey).Scan(&n)
	return n > 0, err
}

// InsertPublication queues a publication. It reports false when the (observation, account) pair is already queued.
func (t *Tx) InsertPublication(ctx context.Context, p Publication, now time.Time) (bool, error) {
	res, err := t.exec(ctx, `INSERT INTO outbox (publication_id, observation_id, account_key, subject, ce_type, ce_time,
		time_basis, payload, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING`,
		p.PublicationID, p.ObservationID, p.AccountKey, p.Subject, p.CEType, p.CETime.UnixNano(), p.TimeBasis, p.Payload, ms(now))
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}

// PendingPartitions returns the accounts that have pending publications.
func (d *DB) PendingPartitions(ctx context.Context) ([]string, error) {
	rows, err := d.r.QueryContext(ctx, `SELECT DISTINCT account_key FROM outbox WHERE state = 'pending' ORDER BY account_key`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

const outboxCols = `seq, publication_id, observation_id, account_key, subject, ce_type, ce_time, time_basis, payload,
	blob_bucket, blob_name, blob_size, blob_sha256, state, attempts, next_attempt_at, COALESCE(last_error, ''),
	COALESCE(error_class, ''), created_at`

// HeadOfPartition returns the oldest pending publication of an account (FIFO head), or ErrNotFound.
func (d *DB) HeadOfPartition(ctx context.Context, account string) (*OutboxRow, error) {
	rows, err := d.r.QueryContext(ctx, `SELECT `+outboxCols+` FROM outbox WHERE state = 'pending' AND account_key = ?
		ORDER BY seq LIMIT 1`, account)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out, err := scanOutbox(rows)
	if err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, ErrNotFound
	}
	return &out[0], nil
}

// OutboxByPublicationID returns one publication.
func (d *DB) OutboxByPublicationID(ctx context.Context, id string) (*OutboxRow, error) {
	rows, err := d.r.QueryContext(ctx, `SELECT `+outboxCols+` FROM outbox WHERE publication_id = ?`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out, err := scanOutbox(rows)
	if err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, ErrNotFound
	}
	return &out[0], nil
}

// OutboxByObservation lists the publications of one observation.
func (d *DB) OutboxByObservation(ctx context.Context, observationID string) ([]OutboxRow, error) {
	rows, err := d.r.QueryContext(ctx, `SELECT `+outboxCols+` FROM outbox WHERE observation_id = ? ORDER BY seq`, observationID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanOutbox(rows)
}

// OutboxRows lists publications in a state, oldest first (tests and inspection).
func (d *DB) OutboxRows(ctx context.Context, state string, limit int) ([]OutboxRow, error) {
	rows, err := d.r.QueryContext(ctx, `SELECT `+outboxCols+` FROM outbox WHERE state = ? ORDER BY seq LIMIT ?`, state, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanOutbox(rows)
}

func scanOutbox(rows *sql.Rows) ([]OutboxRow, error) {
	var out []OutboxRow
	for rows.Next() {
		var r OutboxRow
		var ceTime, nextAt, created int64
		var bb, bn, bs sql.NullString
		var bsize sql.NullInt64
		if err := rows.Scan(&r.Seq, &r.PublicationID, &r.ObservationID, &r.AccountKey, &r.Subject, &r.CEType, &ceTime,
			&r.TimeBasis, &r.Payload, &bb, &bn, &bsize, &bs, &r.State, &r.Attempts, &nextAt, &r.LastError, &r.ErrorClass,
			&created); err != nil {
			return nil, err
		}
		r.CETime = time.Unix(0, ceTime).UTC()
		r.NextAt = time.UnixMilli(nextAt).UTC()
		r.CreatedAt = time.UnixMilli(created).UTC()
		if bb.Valid && bn.Valid {
			r.Blob = &Blob{Bucket: bb.String, Name: bn.String, Size: bsize.Int64, SHA256: bs.String}
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// SetOutboxBlob persists an uploaded blob reference before the event is published, so retries reuse the blob.
func (d *DB) SetOutboxBlob(ctx context.Context, seq int64, b Blob) error {
	return d.Tx(ctx, func(tx *Tx) error {
		_, err := tx.exec(ctx, `UPDATE outbox SET blob_bucket = ?, blob_name = ?, blob_size = ?, blob_sha256 = ? WHERE seq = ?`,
			b.Bucket, b.Name, b.Size, b.SHA256, seq)
		return err
	})
}

// MarkPublished completes a publication after the JetStream acknowledgement.
func (d *DB) MarkPublished(ctx context.Context, seq int64, streamSeq uint64, duplicate bool, now time.Time) error {
	return d.Tx(ctx, func(tx *Tx) error {
		_, err := tx.exec(ctx, `UPDATE outbox SET state = 'published', stream_seq = ?, duplicate = ?, published_at = ?,
			attempts = attempts + 1, last_error = NULL, error_class = NULL WHERE seq = ?`,
			int64(streamSeq), duplicate, ms(now), seq) //nolint:gosec // stream sequences fit int64
		return err
	})
}

// PublishFailed records a failed attempt. With quarantine the publication stops being retried.
func (d *DB) PublishFailed(ctx context.Context, seq int64, nextAt time.Time, class, msg string, quarantine bool) error {
	state := OutboxPending
	if quarantine {
		state = OutboxQuarantined
	}
	return d.Tx(ctx, func(tx *Tx) error {
		_, err := tx.exec(ctx, `UPDATE outbox SET attempts = attempts + 1, next_attempt_at = ?, error_class = ?,
			last_error = ?, state = ? WHERE seq = ?`, ms(nextAt), class, truncate(msg, 1000), state, seq)
		return err
	})
}

// OutboxStats summarizes the outbox.
type OutboxStats struct {
	Pending       int
	OldestPending *time.Time
	Quarantined   int
}

// OutboxStats returns outbox counters.
func (d *DB) OutboxStats(ctx context.Context) (OutboxStats, error) {
	var s OutboxStats
	var oldest sql.NullInt64
	err := d.r.QueryRowContext(ctx, `SELECT COALESCE(SUM(state = 'pending'), 0),
		MIN(CASE WHEN state = 'pending' THEN created_at END), COALESCE(SUM(state = 'quarantined'), 0) FROM outbox`).
		Scan(&s.Pending, &oldest, &s.Quarantined)
	s.OldestPending = fromMS(oldest)
	return s, err
}

// PendingByAccount counts pending publications per account.
func (d *DB) PendingByAccount(ctx context.Context) (map[string]int, error) {
	rows, err := d.r.QueryContext(ctx, `SELECT account_key, COUNT(*) FROM outbox WHERE state = 'pending' GROUP BY account_key`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var k string
		var n int
		if err := rows.Scan(&k, &n); err != nil {
			return nil, err
		}
		out[k] = n
	}
	return out, rows.Err()
}
