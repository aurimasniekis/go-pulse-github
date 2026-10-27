package storage

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"
)

// Job statuses.
const (
	JobQueued  = "queued"
	JobRunning = "running"
	JobDone    = "done"
	JobFailed  = "failed"
)

// Job is a bounded, resumable backfill or reconciliation run.
type Job struct {
	ID           int64
	AccountKey   string
	Mode         string
	Kinds        []string
	Since        time.Time
	MaxRequests  int
	RequestsUsed int
	Cursor       string
	Status       string
	LastError    string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// EnqueueJob adds a job unless an unfinished job with the same account and mode exists. It returns the job ID.
func (d *DB) EnqueueJob(ctx context.Context, j Job, now time.Time) (int64, bool, error) {
	var id int64
	created := false
	err := d.Tx(ctx, func(tx *Tx) error {
		e := tx.tx.QueryRowContext(ctx, `SELECT id FROM backfill_jobs WHERE account_key = ? AND mode = ?
			AND status IN ('queued', 'running') ORDER BY id LIMIT 1`, j.AccountKey, j.Mode).Scan(&id)
		if e == nil {
			return nil
		}
		if !errors.Is(e, sql.ErrNoRows) {
			return e
		}
		res, e := tx.exec(ctx, `INSERT INTO backfill_jobs (account_key, mode, kinds, since, max_requests, status,
			created_at, updated_at) VALUES (?, ?, ?, ?, ?, 'queued', ?, ?)`,
			j.AccountKey, j.Mode, strings.Join(j.Kinds, ","), ms(j.Since), j.MaxRequests, ms(now), ms(now))
		if e != nil {
			return e
		}
		id, e = res.LastInsertId()
		created = true
		return e
	})
	return id, created, err
}

const jobCols = `id, account_key, mode, kinds, since, max_requests, requests_used, COALESCE(cursor, ''), status,
	COALESCE(last_error, ''), created_at, updated_at`

func scanJob(sc interface{ Scan(...any) error }) (*Job, error) {
	var j Job
	var kinds string
	var since, created, updated int64
	if err := sc.Scan(&j.ID, &j.AccountKey, &j.Mode, &kinds, &since, &j.MaxRequests, &j.RequestsUsed, &j.Cursor,
		&j.Status, &j.LastError, &created, &updated); err != nil {
		return nil, err
	}
	if kinds != "" {
		j.Kinds = strings.Split(kinds, ",")
	}
	j.Since = time.UnixMilli(since).UTC()
	j.CreatedAt, j.UpdatedAt = time.UnixMilli(created).UTC(), time.UnixMilli(updated).UTC()
	return &j, nil
}

// ActiveJobs returns queued and running jobs, oldest first (running jobs resume after a restart).
func (d *DB) ActiveJobs(ctx context.Context) ([]Job, error) {
	rows, err := d.r.QueryContext(ctx, `SELECT `+jobCols+` FROM backfill_jobs WHERE status IN ('queued', 'running') ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *j)
	}
	return out, rows.Err()
}

// JobByID returns one job.
func (d *DB) JobByID(ctx context.Context, id int64) (*Job, error) {
	j, err := scanJob(d.r.QueryRowContext(ctx, `SELECT `+jobCols+` FROM backfill_jobs WHERE id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return j, err
}

// UpdateJobProgress stores a job's cursor and request usage inside the transaction that stored its observations.
func (t *Tx) UpdateJobProgress(ctx context.Context, id int64, cursor string, used int, status string, now time.Time) error {
	_, err := t.exec(ctx, `UPDATE backfill_jobs SET cursor = ?, requests_used = ?, status = ?, updated_at = ? WHERE id = ?`,
		nullStr(cursor), used, status, ms(now), id)
	return err
}

// FinishJob sets a terminal status.
func (d *DB) FinishJob(ctx context.Context, id int64, status, lastErr string, now time.Time) error {
	return d.Tx(ctx, func(tx *Tx) error {
		_, err := tx.exec(ctx, `UPDATE backfill_jobs SET status = ?, last_error = ?, updated_at = ? WHERE id = ?`,
			status, nullStr(truncate(lastErr, 1000)), ms(now), id)
		return err
	})
}

// EnrichmentJob is a queued optional fetch.
type EnrichmentJob struct {
	ID            int64
	ObservationID string
	AccountKey    string
	Kind          string
	URL           string
	Attempts      int
}

// EnqueueEnrichment queues an enrichment fetch (deduplicated per observation, account and URL).
func (t *Tx) EnqueueEnrichment(ctx context.Context, j EnrichmentJob, now time.Time) error {
	_, err := t.exec(ctx, `INSERT INTO enrichment_jobs (observation_id, account_key, kind, url, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING`, j.ObservationID, j.AccountKey, j.Kind, j.URL, ms(now), ms(now))
	return err
}

// PendingEnrichment returns pending enrichment jobs, oldest first.
func (d *DB) PendingEnrichment(ctx context.Context, limit int) ([]EnrichmentJob, error) {
	rows, err := d.r.QueryContext(ctx, `SELECT id, observation_id, account_key, kind, url, attempts FROM enrichment_jobs
		WHERE state = 'pending' ORDER BY id LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []EnrichmentJob
	for rows.Next() {
		var j EnrichmentJob
		if err := rows.Scan(&j.ID, &j.ObservationID, &j.AccountKey, &j.Kind, &j.URL, &j.Attempts); err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// FinishEnrichment sets an enrichment job's state.
func (t *Tx) FinishEnrichment(ctx context.Context, id int64, state, lastErr string, now time.Time) error {
	_, err := t.exec(ctx, `UPDATE enrichment_jobs SET state = ?, attempts = attempts + 1, last_error = ?, updated_at = ?
		WHERE id = ?`, state, nullStr(truncate(lastErr, 1000)), ms(now), id)
	return err
}

// EnrichmentFailed records a retryable enrichment failure.
func (d *DB) EnrichmentFailed(ctx context.Context, id int64, lastErr string, giveUp bool, now time.Time) error {
	state := "pending"
	if giveUp {
		state = "failed"
	}
	return d.Tx(ctx, func(tx *Tx) error { return tx.FinishEnrichment(ctx, id, state, lastErr, now) })
}

// LastJob returns the newest job of an account and mode, or ErrNotFound.
func (d *DB) LastJob(ctx context.Context, account, mode string) (*Job, error) {
	j, err := scanJob(d.r.QueryRowContext(ctx, `SELECT `+jobCols+` FROM backfill_jobs WHERE account_key = ? AND mode = ?
		ORDER BY id DESC LIMIT 1`, account, mode))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return j, err
}
