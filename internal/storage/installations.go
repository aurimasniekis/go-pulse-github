package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// Installation statuses.
const (
	InstallActive       = "active"
	InstallSuspended    = "suspended"
	InstallRemoved      = "removed"
	InstallInaccessible = "inaccessible"
)

// Installation is a GitHub App installation.
type Installation struct {
	ID                  int64
	AppID               int64
	AccountGitHubID     int64
	AccountLogin        string
	AccountType         string
	RepositorySelection string
	PermissionsJSON     string
	EventsJSON          string
	Status              string
	SuspendedAt         *time.Time
	UpdatedAt           time.Time
	LastReconciledAt    *time.Time
}

const instCols = `installation_id, COALESCE(app_id, 0), account_github_id, account_login, account_type,
	COALESCE(repository_selection, ''), COALESCE(permissions_json, ''), COALESCE(events_json, ''), status, suspended_at,
	updated_at, last_reconciled_at`

func scanInstallation(sc interface{ Scan(...any) error }) (*Installation, error) {
	var in Installation
	var susp, rec sql.NullInt64
	var upd int64
	if err := sc.Scan(&in.ID, &in.AppID, &in.AccountGitHubID, &in.AccountLogin, &in.AccountType, &in.RepositorySelection,
		&in.PermissionsJSON, &in.EventsJSON, &in.Status, &susp, &upd, &rec); err != nil {
		return nil, err
	}
	in.SuspendedAt, in.LastReconciledAt = fromMS(susp), fromMS(rec)
	in.UpdatedAt = time.UnixMilli(upd).UTC()
	return &in, nil
}

// Installation returns a stored installation or ErrNotFound.
func (d *DB) Installation(ctx context.Context, id int64) (*Installation, error) {
	in, err := scanInstallation(d.r.QueryRowContext(ctx, `SELECT `+instCols+` FROM installations WHERE installation_id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	return in, err
}

// Installations lists stored installations.
func (d *DB) Installations(ctx context.Context) ([]Installation, error) {
	rows, err := d.r.QueryContext(ctx, `SELECT `+instCols+` FROM installations ORDER BY installation_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Installation
	for rows.Next() {
		in, err := scanInstallation(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *in)
	}
	return out, rows.Err()
}

// UpsertInstallation stores installation metadata.
func (t *Tx) UpsertInstallation(ctx context.Context, in Installation, now time.Time) error {
	if in.Status == "" {
		in.Status = InstallActive
	}
	_, err := t.exec(ctx, `INSERT INTO installations (installation_id, app_id, account_github_id, account_login,
		account_type, repository_selection, permissions_json, events_json, status, suspended_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (installation_id) DO UPDATE SET app_id = COALESCE(excluded.app_id, installations.app_id),
			account_github_id = excluded.account_github_id, account_login = excluded.account_login,
			account_type = excluded.account_type,
			repository_selection = COALESCE(excluded.repository_selection, installations.repository_selection),
			permissions_json = COALESCE(excluded.permissions_json, installations.permissions_json),
			events_json = COALESCE(excluded.events_json, installations.events_json), status = excluded.status,
			suspended_at = excluded.suspended_at, updated_at = excluded.updated_at`,
		in.ID, nullID(in.AppID), in.AccountGitHubID, in.AccountLogin, in.AccountType, nullStr(in.RepositorySelection),
		nullStr(in.PermissionsJSON), nullStr(in.EventsJSON), in.Status, nullMS(in.SuspendedAt), ms(now))
	return err
}

// SetInstallationStatus changes the status of a known installation.
func (t *Tx) SetInstallationStatus(ctx context.Context, id int64, status string, now time.Time) error {
	_, err := t.exec(ctx, `UPDATE installations SET status = ?, updated_at = ? WHERE installation_id = ?`, status, ms(now), id)
	return err
}

// MarkInstallationReconciled records a completed access reconciliation.
func (t *Tx) MarkInstallationReconciled(ctx context.Context, id int64, now time.Time) error {
	_, err := t.exec(ctx, `UPDATE installations SET last_reconciled_at = ? WHERE installation_id = ?`, ms(now), id)
	return err
}

// InstallationRepo is a repository an installation can access.
type InstallationRepo struct {
	InstallationID int64
	RepoID         int64
	FullName       string
	Private        bool
	Status         string
}

// InstallationRepos lists the repositories stored for an installation (any status).
func (d *DB) InstallationRepos(ctx context.Context, installationID int64) ([]InstallationRepo, error) {
	rows, err := d.r.QueryContext(ctx, `SELECT installation_id, repo_id, full_name, private, status
		FROM installation_repositories WHERE installation_id = ? ORDER BY full_name`, installationID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []InstallationRepo
	for rows.Next() {
		var r InstallationRepo
		if err := rows.Scan(&r.InstallationID, &r.RepoID, &r.FullName, &r.Private, &r.Status); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// SetInstallationRepo upserts a repository's access status for an installation.
func (t *Tx) SetInstallationRepo(ctx context.Context, r InstallationRepo, now time.Time) error {
	if r.Status == "" {
		r.Status = InstallActive
	}
	_, err := t.exec(ctx, `INSERT INTO installation_repositories (installation_id, repo_id, full_name, private, status,
		updated_at) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (installation_id, repo_id) DO UPDATE SET
		full_name = excluded.full_name, private = excluded.private, status = excluded.status, updated_at = excluded.updated_at`,
		r.InstallationID, r.RepoID, r.FullName, r.Private, r.Status, ms(now))
	return err
}

// AccountRecord is the persisted identity of a monitored account.
type AccountRecord struct {
	Key         string
	GitHubID    int64
	Login       string
	Type        string
	RenamedFrom string
}

// ErrAccountConflict means a configured account contradicts the stored identity.
var ErrAccountConflict = errors.New("account identity conflict")

// SyncAccount pins an account key to an immutable GitHub ID. A login change for the same ID is recorded as a rename
// and keeps the routing key; a different ID for the same key, or the same ID under another key, is a conflict.
func (d *DB) SyncAccount(ctx context.Context, rec AccountRecord, now time.Time) (renamedFrom string, err error) {
	err = d.Tx(ctx, func(tx *Tx) error {
		var id sql.NullInt64
		var login string
		e := tx.tx.QueryRowContext(ctx, `SELECT github_id, login FROM accounts WHERE account_key = ?`, rec.Key).Scan(&id, &login)
		switch {
		case errors.Is(e, sql.ErrNoRows):
			if rec.GitHubID != 0 {
				var other string
				e2 := tx.tx.QueryRowContext(ctx, `SELECT account_key FROM accounts WHERE github_id = ?`, rec.GitHubID).Scan(&other)
				if e2 == nil {
					return fmt.Errorf("%w: github id %d is already bound to account key %q", ErrAccountConflict, rec.GitHubID, other)
				}
			}
			_, e = tx.exec(ctx, `INSERT INTO accounts (account_key, github_id, login, account_type, first_seen, updated_at)
				VALUES (?, ?, ?, ?, ?, ?)`, rec.Key, nullID(rec.GitHubID), rec.Login, rec.Type, ms(now), ms(now))
			return e
		case e != nil:
			return e
		}
		if id.Valid && rec.GitHubID != 0 && id.Int64 != rec.GitHubID {
			return fmt.Errorf("%w: account key %q is bound to github id %d, configuration resolves to %d",
				ErrAccountConflict, rec.Key, id.Int64, rec.GitHubID)
		}
		gid := id.Int64
		if gid == 0 {
			gid = rec.GitHubID
		}
		if login != rec.Login {
			renamedFrom = login
		}
		_, e = tx.exec(ctx, `UPDATE accounts SET github_id = ?, login = ?, account_type = ?, updated_at = ?,
			renamed_from = COALESCE(?, renamed_from) WHERE account_key = ?`,
			nullID(gid), rec.Login, rec.Type, ms(now), nullStr(renamedFrom), rec.Key)
		return e
	})
	return renamedFrom, err
}

// Accounts lists persisted account identities.
func (d *DB) Accounts(ctx context.Context) ([]AccountRecord, error) {
	rows, err := d.r.QueryContext(ctx, `SELECT account_key, COALESCE(github_id, 0), login, account_type,
		COALESCE(renamed_from, '') FROM accounts ORDER BY account_key`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []AccountRecord
	for rows.Next() {
		var a AccountRecord
		if err := rows.Scan(&a.Key, &a.GitHubID, &a.Login, &a.Type, &a.RenamedFrom); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func nullID(v int64) any {
	if v == 0 {
		return nil
	}
	return v
}
