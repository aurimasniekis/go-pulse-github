package storage

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// Checkpoint is the polling progress of one account feed.
type Checkpoint struct {
	AccountKey    string
	Collector     string
	Feed          string
	Cursor        string
	ETag          string
	LastModified  string
	PollInterval  time.Duration
	LastSuccessAt *time.Time
	LastAttemptAt *time.Time
	LastError     string
	ErrorClass    string
}

// Checkpoint returns the stored checkpoint, or a zero checkpoint for a new feed.
func (d *DB) Checkpoint(ctx context.Context, account, collector, feed string) (Checkpoint, error) {
	cp := Checkpoint{AccountKey: account, Collector: collector, Feed: feed}
	var cursor, etag, lm, lastErr, class sql.NullString
	var poll, succ, att sql.NullInt64
	err := d.r.QueryRowContext(ctx, `SELECT cursor, etag, last_modified, poll_interval_s, last_success_at,
		last_attempt_at, last_error, error_class FROM checkpoints WHERE account_key = ? AND collector = ? AND feed = ?`,
		account, collector, feed).Scan(&cursor, &etag, &lm, &poll, &succ, &att, &lastErr, &class)
	if errors.Is(err, sql.ErrNoRows) {
		return cp, nil
	}
	if err != nil {
		return cp, err
	}
	cp.Cursor, cp.ETag, cp.LastModified = cursor.String, etag.String, lm.String
	cp.PollInterval = time.Duration(poll.Int64) * time.Second
	cp.LastSuccessAt, cp.LastAttemptAt = fromMS(succ), fromMS(att)
	cp.LastError, cp.ErrorClass = lastErr.String, class.String
	return cp, nil
}

// PutCheckpoint stores a successful poll's progress in the transaction that stored its observations.
func (t *Tx) PutCheckpoint(ctx context.Context, cp Checkpoint, now time.Time) error {
	_, err := t.exec(ctx, `INSERT INTO checkpoints (account_key, collector, feed, cursor, etag, last_modified,
		poll_interval_s, last_success_at, last_attempt_at, last_error, error_class, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?)
		ON CONFLICT (account_key, collector, feed) DO UPDATE SET cursor = excluded.cursor, etag = excluded.etag,
			last_modified = excluded.last_modified, poll_interval_s = excluded.poll_interval_s,
			last_success_at = excluded.last_success_at, last_attempt_at = excluded.last_attempt_at, last_error = NULL,
			error_class = NULL, updated_at = excluded.updated_at`,
		cp.AccountKey, cp.Collector, cp.Feed, nullStr(cp.Cursor), nullStr(cp.ETag), nullStr(cp.LastModified),
		int64(cp.PollInterval/time.Second), ms(now), ms(now), ms(now))
	return err
}

// RecordPollError stores a failed poll without moving the checkpoint.
func (d *DB) RecordPollError(ctx context.Context, account, collector, feed, class, msg string, now time.Time) error {
	return d.Tx(ctx, func(tx *Tx) error {
		_, err := tx.exec(ctx, `INSERT INTO checkpoints (account_key, collector, feed, last_attempt_at, last_error,
			error_class, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT (account_key, collector, feed) DO UPDATE SET last_attempt_at = excluded.last_attempt_at,
				last_error = excluded.last_error, error_class = excluded.error_class, updated_at = excluded.updated_at`,
			account, collector, feed, ms(now), truncate(msg, 1000), class, ms(now))
		return err
	})
}

// Checkpoints lists every checkpoint (status and metrics).
func (d *DB) Checkpoints(ctx context.Context) ([]Checkpoint, error) {
	rows, err := d.r.QueryContext(ctx, `SELECT account_key, collector, feed, COALESCE(cursor, ''), COALESCE(etag, ''),
		COALESCE(last_modified, ''), COALESCE(poll_interval_s, 0), last_success_at, last_attempt_at,
		COALESCE(last_error, ''), COALESCE(error_class, '') FROM checkpoints ORDER BY 1, 2, 3`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Checkpoint
	for rows.Next() {
		var cp Checkpoint
		var poll int64
		var succ, att sql.NullInt64
		if err := rows.Scan(&cp.AccountKey, &cp.Collector, &cp.Feed, &cp.Cursor, &cp.ETag, &cp.LastModified, &poll,
			&succ, &att, &cp.LastError, &cp.ErrorClass); err != nil {
			return nil, err
		}
		cp.PollInterval = time.Duration(poll) * time.Second
		cp.LastSuccessAt, cp.LastAttemptAt = fromMS(succ), fromMS(att)
		out = append(out, cp)
	}
	return out, rows.Err()
}

// NotificationFingerprint returns the stored fingerprint of a thread ("" when unseen).
func (t *Tx) NotificationFingerprint(ctx context.Context, account, threadID string) (string, error) {
	var fp string
	err := t.tx.QueryRowContext(ctx, `SELECT fingerprint FROM notification_state WHERE account_key = ? AND thread_id = ?`,
		account, threadID).Scan(&fp)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return fp, err
}

// PutNotificationState stores the latest meaningful state of a thread.
func (t *Tx) PutNotificationState(ctx context.Context, account, threadID, fingerprint, updatedAt string, now time.Time) error {
	_, err := t.exec(ctx, `INSERT INTO notification_state (account_key, thread_id, fingerprint, updated_at, last_seen_at)
		VALUES (?, ?, ?, ?, ?) ON CONFLICT (account_key, thread_id) DO UPDATE SET fingerprint = excluded.fingerprint,
		updated_at = excluded.updated_at, last_seen_at = excluded.last_seen_at`,
		account, threadID, fingerprint, updatedAt, ms(now))
	return err
}

// ResourceFingerprint returns the stored fingerprint of a reconciled resource ("" when unseen).
func (t *Tx) ResourceFingerprint(ctx context.Context, account, kind, id string) (string, error) {
	var fp string
	err := t.tx.QueryRowContext(ctx, `SELECT fingerprint FROM resource_fingerprints WHERE account_key = ? AND kind = ?
		AND resource_id = ?`, account, kind, id).Scan(&fp)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return fp, err
}

// PutResourceFingerprint stores the latest fingerprint of a reconciled resource.
func (t *Tx) PutResourceFingerprint(ctx context.Context, account, kind, id, fp string, now time.Time) error {
	_, err := t.exec(ctx, `INSERT INTO resource_fingerprints (account_key, kind, resource_id, fingerprint, seen_at)
		VALUES (?, ?, ?, ?, ?) ON CONFLICT (account_key, kind, resource_id) DO UPDATE SET fingerprint = excluded.fingerprint,
		seen_at = excluded.seen_at`, account, kind, id, fp, ms(now))
	return err
}

// Gap is a detected or suspected collection gap.
type Gap struct {
	AccountKey  string
	Collector   string
	Feed        string
	Kind        string
	WindowStart *time.Time
	WindowEnd   *time.Time
	Detail      string
}

// InsertGap records a gap.
func (t *Tx) InsertGap(ctx context.Context, g Gap, now time.Time) error {
	_, err := t.exec(ctx, `INSERT INTO coverage_gaps (account_key, collector, feed, kind, window_start, window_end,
		detail, detected_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		g.AccountKey, g.Collector, g.Feed, g.Kind, nullMS(g.WindowStart), nullMS(g.WindowEnd), g.Detail, ms(now))
	return err
}

// GapCount counts gaps detected since a time.
func (d *DB) GapCount(ctx context.Context, since time.Time) (int, error) {
	var n int
	err := d.r.QueryRowContext(ctx, `SELECT COUNT(*) FROM coverage_gaps WHERE detected_at >= ?`, ms(since)).Scan(&n)
	return n, err
}
