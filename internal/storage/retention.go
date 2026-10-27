package storage

import (
	"context"
	"os"
	"time"
)

// RetentionPolicy bounds how long completed records are kept.
type RetentionPolicy struct {
	Published      time.Duration
	InboxProcessed time.Duration
	Observations   time.Duration
}

// SweepResult counts deleted rows.
type SweepResult struct {
	Outbox       int64
	InboxBodies  int64
	Observations int64
	Enrichment   int64
}

// Sweep deletes completed records past retention. Pending and quarantined records are never touched. Observation
// dedup keys must outlive the longest source re-read window (the Events API keeps 30 days), or old activity could be
// re-published as new after a long outage.
func (d *DB) Sweep(ctx context.Context, now time.Time, p RetentionPolicy) (SweepResult, error) {
	var r SweepResult
	err := d.Tx(ctx, func(tx *Tx) error {
		res, err := tx.exec(ctx, `DELETE FROM outbox WHERE state = 'published' AND published_at < ?`, ms(now.Add(-p.Published)))
		if err != nil {
			return err
		}
		r.Outbox, _ = res.RowsAffected()
		// Unrouted deliveries follow the same retention as processed ones: they keep their body (for a requeue after
		// routing changes) only as long as a processed delivery would.
		res, err = tx.exec(ctx, `UPDATE webhook_inbox SET body = NULL WHERE state IN ('processed', 'unrouted')
			AND body IS NOT NULL AND processed_at < ?`, ms(now.Add(-p.InboxProcessed)))
		if err != nil {
			return err
		}
		r.InboxBodies, _ = res.RowsAffected()
		_, err = tx.exec(ctx, `DELETE FROM webhook_inbox WHERE state IN ('processed', 'unrouted') AND body IS NULL
			AND processed_at < ?`, ms(now.Add(-p.Observations)))
		if err != nil {
			return err
		}
		res, err = tx.exec(ctx, `DELETE FROM observations WHERE created_at < ? AND NOT EXISTS
			(SELECT 1 FROM outbox o WHERE o.observation_id = observations.observation_id)`, ms(now.Add(-p.Observations)))
		if err != nil {
			return err
		}
		r.Observations, _ = res.RowsAffected()
		res, err = tx.exec(ctx, `DELETE FROM enrichment_jobs WHERE state != 'pending' AND updated_at < ?`, ms(now.Add(-p.Published)))
		if err != nil {
			return err
		}
		r.Enrichment, _ = res.RowsAffected()
		_, err = tx.exec(ctx, `DELETE FROM coverage_gaps WHERE detected_at < ?`, ms(now.Add(-p.Observations)))
		return err
	})
	if err == nil && d.w != nil {
		_, _ = d.w.ExecContext(ctx, "PRAGMA wal_checkpoint(PASSIVE)")
	}
	return r, err
}

// WriteInstance records the running process for diagnostics.
func (d *DB) WriteInstance(ctx context.Context, holder, version string, now time.Time) error {
	host, _ := os.Hostname()
	return d.Tx(ctx, func(tx *Tx) error {
		_, err := tx.exec(ctx, `INSERT INTO instance (id, holder, pid, hostname, version, started_at, heartbeat_at)
			VALUES (1, ?, ?, ?, ?, ?, ?) ON CONFLICT (id) DO UPDATE SET holder = excluded.holder, pid = excluded.pid,
			hostname = excluded.hostname, version = excluded.version, started_at = excluded.started_at,
			heartbeat_at = excluded.heartbeat_at`, holder, os.Getpid(), host, version, ms(now), ms(now))
		return err
	})
}

// Heartbeat updates the instance heartbeat. It doubles as a periodic write probe for readiness.
func (d *DB) Heartbeat(ctx context.Context, now time.Time) error {
	return d.Tx(ctx, func(tx *Tx) error {
		_, err := tx.exec(ctx, `UPDATE instance SET heartbeat_at = ? WHERE id = 1`, ms(now))
		return err
	})
}
