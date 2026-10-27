package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"time"
)

// Inbox states.
const (
	InboxPending     = "pending"
	InboxProcessed   = "processed"
	InboxUnrouted    = "unrouted"
	InboxQuarantined = "quarantined"
)

// Delivery is a verified webhook delivery to store.
type Delivery struct {
	DeliveryID string
	HookID     string
	Event      string
	TargetType string
	TargetID   string
	Body       []byte
	ReceivedAt time.Time
}

// InboxRow is a stored webhook delivery.
type InboxRow struct {
	ID         int64
	DeliveryID string
	HookID     string
	Event      string
	TargetType string
	TargetID   string
	Body       []byte
	ReceivedAt time.Time
	State      string
	Attempts   int
	LastError  string
	ErrorClass string
}

// InsertDelivery durably stores a delivery. It reports false when the delivery ID was already stored (a GitHub
// redelivery keeps the delivery ID); the duplicate is counted and nothing else changes.
func (d *DB) InsertDelivery(ctx context.Context, del Delivery) (bool, error) {
	sum := sha256.Sum256(del.Body)
	inserted := false
	err := d.Tx(ctx, func(tx *Tx) error {
		res, err := tx.exec(ctx, `INSERT INTO webhook_inbox (delivery_id, hook_id, event, installation_target_type,
			installation_target_id, body, body_size, body_sha256, received_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT (delivery_id) DO NOTHING`,
			del.DeliveryID, nullStr(del.HookID), del.Event, nullStr(del.TargetType), nullStr(del.TargetID), del.Body,
			len(del.Body), hex.EncodeToString(sum[:]), ms(del.ReceivedAt))
		if err != nil {
			return err
		}
		n, _ := res.RowsAffected()
		if n == 1 {
			inserted = true
			return nil
		}
		_, err = tx.exec(ctx, `UPDATE webhook_inbox SET duplicates = duplicates + 1 WHERE delivery_id = ?`, del.DeliveryID)
		return err
	})
	return inserted, err
}

// DueInbox returns pending deliveries whose retry time has come, oldest first.
func (d *DB) DueInbox(ctx context.Context, now time.Time, limit int) ([]InboxRow, error) {
	rows, err := d.r.QueryContext(ctx, `SELECT id, delivery_id, COALESCE(hook_id, ''), event,
		COALESCE(installation_target_type, ''), COALESCE(installation_target_id, ''), body, received_at, state, attempts,
		COALESCE(last_error, ''), COALESCE(error_class, '')
		FROM webhook_inbox WHERE state = 'pending' AND next_attempt_at <= ? ORDER BY id LIMIT ?`, ms(now), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanInbox(rows)
}

// InboxByID returns one delivery.
func (d *DB) InboxByID(ctx context.Context, id int64) (*InboxRow, error) {
	rows, err := d.r.QueryContext(ctx, `SELECT id, delivery_id, COALESCE(hook_id, ''), event,
		COALESCE(installation_target_type, ''), COALESCE(installation_target_id, ''), body, received_at, state, attempts,
		COALESCE(last_error, ''), COALESCE(error_class, '') FROM webhook_inbox WHERE id = ?`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out, err := scanInbox(rows)
	if err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, ErrNotFound
	}
	return &out[0], nil
}

func scanInbox(rows *sql.Rows) ([]InboxRow, error) {
	var out []InboxRow
	for rows.Next() {
		var r InboxRow
		var recv int64
		if err := rows.Scan(&r.ID, &r.DeliveryID, &r.HookID, &r.Event, &r.TargetType, &r.TargetID, &r.Body, &recv,
			&r.State, &r.Attempts, &r.LastError, &r.ErrorClass); err != nil {
			return nil, err
		}
		r.ReceivedAt = time.UnixMilli(recv).UTC()
		out = append(out, r)
	}
	return out, rows.Err()
}

// MarkInbox sets the final state of a delivery inside the transaction that wrote its observations.
func (t *Tx) MarkInbox(ctx context.Context, id int64, state string, now time.Time) error {
	_, err := t.exec(ctx, `UPDATE webhook_inbox SET state = ?, processed_at = ?, last_error = NULL, error_class = NULL
		WHERE id = ?`, state, ms(now), id)
	return err
}

// InboxFailed records a processing failure. With quarantine the delivery stops being retried.
func (d *DB) InboxFailed(ctx context.Context, id int64, nextAt time.Time, class, msg string, quarantine bool) error {
	state := InboxPending
	if quarantine {
		state = InboxQuarantined
	}
	return d.Tx(ctx, func(tx *Tx) error {
		_, err := tx.exec(ctx, `UPDATE webhook_inbox SET attempts = attempts + 1, next_attempt_at = ?, error_class = ?,
			last_error = ?, state = ? WHERE id = ?`, ms(nextAt), class, truncate(msg, 1000), state, id)
		return err
	})
}

// RequeueUnrouted puts unrouted deliveries back into processing (after installations or configuration changed).
func (d *DB) RequeueUnrouted(ctx context.Context) (int64, error) {
	var n int64
	err := d.Tx(ctx, func(tx *Tx) error {
		res, err := tx.exec(ctx, `UPDATE webhook_inbox SET state = 'pending', next_attempt_at = 0
			WHERE state = 'unrouted' AND body IS NOT NULL`)
		if err != nil {
			return err
		}
		n, _ = res.RowsAffected()
		return nil
	})
	return n, err
}

// InboxStats summarizes the inbox.
type InboxStats struct {
	Pending       int
	OldestPending *time.Time
	Unrouted      int
	Quarantined   int
}

// InboxStats returns inbox counters.
func (d *DB) InboxStats(ctx context.Context) (InboxStats, error) {
	var s InboxStats
	var oldest sql.NullInt64
	err := d.r.QueryRowContext(ctx, `SELECT
		COALESCE(SUM(state = 'pending'), 0), MIN(CASE WHEN state = 'pending' THEN received_at END),
		COALESCE(SUM(state = 'unrouted'), 0), COALESCE(SUM(state = 'quarantined'), 0) FROM webhook_inbox`).
		Scan(&s.Pending, &oldest, &s.Unrouted, &s.Quarantined)
	s.OldestPending = fromMS(oldest)
	return s, err
}

// InboxPending counts deliveries not yet normalized (admission control).
func (d *DB) InboxPending(ctx context.Context) (int, error) {
	var n int
	err := d.r.QueryRowContext(ctx, `SELECT COUNT(*) FROM webhook_inbox WHERE state = 'pending'`).Scan(&n)
	return n, err
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
