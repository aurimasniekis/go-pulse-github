package storage

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// Quarantine kinds.
const (
	QuarantineInbox  = "inbox"
	QuarantineOutbox = "outbox"
)

// QuarantineItem is a record that stopped being retried and needs operator attention.
type QuarantineItem struct {
	Kind       string    `json:"kind"`
	ID         string    `json:"id"`
	Account    string    `json:"account_key,omitempty"`
	Subject    string    `json:"subject,omitempty"`
	Event      string    `json:"event,omitempty"`
	ErrorClass string    `json:"error_class"`
	Error      string    `json:"error"`
	Attempts   int       `json:"attempts"`
	CreatedAt  time.Time `json:"created_at"`
}

// ListQuarantine lists quarantined inbox deliveries and publications.
func (d *DB) ListQuarantine(ctx context.Context, limit int) ([]QuarantineItem, error) {
	rows, err := d.r.QueryContext(ctx, `
		SELECT 'inbox', delivery_id, '', '', event, COALESCE(error_class, ''), COALESCE(last_error, ''), attempts, received_at
		FROM webhook_inbox WHERE state = 'quarantined'
		UNION ALL
		SELECT 'outbox', publication_id, account_key, subject, '', COALESCE(error_class, ''), COALESCE(last_error, ''),
			attempts, created_at
		FROM outbox WHERE state = 'quarantined'
		ORDER BY 9 LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []QuarantineItem
	for rows.Next() {
		var q QuarantineItem
		var created int64
		if err := rows.Scan(&q.Kind, &q.ID, &q.Account, &q.Subject, &q.Event, &q.ErrorClass, &q.Error, &q.Attempts, &created); err != nil {
			return nil, err
		}
		q.CreatedAt = time.UnixMilli(created).UTC()
		out = append(out, q)
	}
	return out, rows.Err()
}

// ReplayQuarantine returns a quarantined record to processing. Publications keep their publication ID, so consumers
// can still deduplicate a replay. An empty id replays every quarantined record of the kind ("" kind: both kinds).
func (d *DB) ReplayQuarantine(ctx context.Context, kind, id string) (int64, error) {
	var total int64
	err := d.Tx(ctx, func(tx *Tx) error {
		run := func(q string, args ...any) error {
			res, err := tx.exec(ctx, q, args...)
			if err != nil {
				return err
			}
			n, _ := res.RowsAffected()
			total += n
			return nil
		}
		if kind == "" || kind == QuarantineInbox {
			q := `UPDATE webhook_inbox SET state = 'pending', attempts = 0, next_attempt_at = 0 WHERE state = 'quarantined'`
			args := []any{}
			if id != "" {
				q += ` AND delivery_id = ?`
				args = append(args, id)
			}
			if err := run(q, args...); err != nil {
				return err
			}
		}
		if kind == "" || kind == QuarantineOutbox {
			q := `UPDATE outbox SET state = 'pending', attempts = 0, next_attempt_at = 0 WHERE state = 'quarantined'`
			args := []any{}
			if id != "" {
				q += ` AND publication_id = ?`
				args = append(args, id)
			}
			if err := run(q, args...); err != nil {
				return err
			}
		}
		if kind != "" && kind != QuarantineInbox && kind != QuarantineOutbox {
			return fmt.Errorf("unknown quarantine kind %q", kind)
		}
		return nil
	})
	return total, err
}

// PurgeQuarantine deletes quarantined records older than before. Their content is lost.
func (d *DB) PurgeQuarantine(ctx context.Context, before time.Time) (int64, error) {
	var total int64
	err := d.Tx(ctx, func(tx *Tx) error {
		for _, q := range []string{
			`DELETE FROM outbox WHERE state = 'quarantined' AND created_at < ?`,
			`DELETE FROM webhook_inbox WHERE state = 'quarantined' AND received_at < ?`,
		} {
			res, err := tx.exec(ctx, q, ms(before))
			if err != nil {
				return err
			}
			n, _ := res.RowsAffected()
			total += n
		}
		return nil
	})
	return total, err
}

// QuarantineCount counts quarantined records.
func (d *DB) QuarantineCount(ctx context.Context) (int, error) {
	var n sql.NullInt64
	err := d.r.QueryRowContext(ctx, `SELECT (SELECT COUNT(*) FROM outbox WHERE state = 'quarantined') +
		(SELECT COUNT(*) FROM webhook_inbox WHERE state = 'quarantined')`).Scan(&n)
	return int(n.Int64), err
}
