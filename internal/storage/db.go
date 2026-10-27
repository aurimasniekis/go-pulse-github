// Package storage is the SQLite state database: webhook inbox, observations, publication outbox, checkpoints and
// collector state. It runs in WAL mode with synchronous=FULL so a committed transaction survives power loss.
package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	_ "modernc.org/sqlite" // database/sql driver "sqlite"

	"github.com/aurimasniekis/go-pulse-github/migrations"
)

// ErrNotFound is returned when a requested row does not exist.
var ErrNotFound = errors.New("not found")

// DB is the state database. Writes go through a single connection (SQLite has one writer); reads use a small pool.
type DB struct {
	path string
	w    *sql.DB
	r    *sql.DB
}

// Options configure Open.
type Options struct {
	// ReadOnly opens only the read pool (inspection commands).
	ReadOnly bool
}

func dsn(path string, readOnly bool) string {
	q := url.Values{}
	q.Add("_pragma", "busy_timeout(10000)")
	q.Add("_pragma", "foreign_keys(1)")
	q.Add("_pragma", "journal_mode(WAL)")
	q.Add("_pragma", "synchronous(FULL)")
	// Truncate the WAL file back to this size after checkpoints instead of keeping its high-water mark.
	q.Add("_pragma", "journal_size_limit(67108864)")
	if readOnly {
		q.Add("_pragma", "query_only(1)")
	} else {
		q.Add("_txlock", "immediate")
	}
	return "file:" + path + "?" + q.Encode()
}

// Open opens (creating if needed) the database and applies pending migrations.
func Open(ctx context.Context, path string, opts Options) (*DB, error) {
	if dir := filepath.Dir(path); dir != "" {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return nil, fmt.Errorf("create state directory: %w", err)
		}
	}
	db := &DB{path: path}
	var err error
	if !opts.ReadOnly {
		db.w, err = sql.Open("sqlite", dsn(path, false))
		if err != nil {
			return nil, err
		}
		db.w.SetMaxOpenConns(1)
		db.w.SetConnMaxIdleTime(0)
		if err := db.w.PingContext(ctx); err != nil {
			_ = db.w.Close()
			return nil, fmt.Errorf("open %s: %w", path, err)
		}
		if err := migrate(ctx, db.w); err != nil {
			_ = db.w.Close()
			return nil, err
		}
	}
	db.r, err = sql.Open("sqlite", dsn(path, true))
	if err != nil {
		db.closeWriter()
		return nil, err
	}
	db.r.SetMaxOpenConns(4)
	if err := db.r.PingContext(ctx); err != nil {
		db.closeWriter()
		_ = db.r.Close()
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	if opts.ReadOnly {
		db.w = nil
	}
	return db, nil
}

func (d *DB) closeWriter() {
	if d.w != nil {
		_ = d.w.Close()
	}
}

// Path is the database file path.
func (d *DB) Path() string { return d.path }

// Close checkpoints the WAL and closes the database.
func (d *DB) Close() error {
	var errs []error
	if d.w != nil {
		_, _ = d.w.ExecContext(context.Background(), "PRAGMA wal_checkpoint(TRUNCATE)")
		errs = append(errs, d.w.Close())
	}
	errs = append(errs, d.r.Close())
	return errors.Join(errs...)
}

// Ping checks that the database answers on both pools.
func (d *DB) Ping(ctx context.Context) error {
	if d.w != nil {
		if err := d.w.PingContext(ctx); err != nil {
			return err
		}
	}
	return d.r.PingContext(ctx)
}

// Tx runs fn in one write transaction (BEGIN IMMEDIATE). fn's error rolls it back.
func (d *DB) Tx(ctx context.Context, fn func(*Tx) error) error {
	if d.w == nil {
		return errors.New("storage: database opened read-only")
	}
	tx, err := d.w.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	if err := fn(&Tx{tx: tx}); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// Tx is a write transaction.
type Tx struct{ tx *sql.Tx }

func (t *Tx) exec(ctx context.Context, q string, args ...any) (sql.Result, error) {
	return t.tx.ExecContext(ctx, q, args...)
}

// UsedBytes returns the bytes held by live data: the pages in use, without SQLite's free list. Pages freed by the
// retention sweep are reused for new rows but never shrink the file, so a size cap must not count them. The WAL is
// left out: it is transient and truncated to journal_size_limit when it resets.
func (d *DB) UsedBytes(ctx context.Context) (int64, error) {
	var pageSize, pages, free int64
	if err := d.r.QueryRowContext(ctx, `SELECT page_size, page_count, freelist_count FROM pragma_page_size, pragma_page_count, pragma_freelist_count`).
		Scan(&pageSize, &pages, &free); err != nil {
		return 0, err
	}
	return pageSize * (pages - free), nil
}

// SizeBytes returns the database plus WAL size on disk.
func (d *DB) SizeBytes() int64 {
	var total int64
	for _, p := range []string{d.path, d.path + "-wal", d.path + "-shm"} {
		if st, err := os.Stat(p); err == nil {
			total += st.Size()
		}
	}
	return total
}

func migrate(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (version TEXT PRIMARY KEY, applied_at INTEGER NOT NULL)`); err != nil {
		return fmt.Errorf("migrations table: %w", err)
	}
	entries, err := fs.Glob(migrations.FS, "*.sql")
	if err != nil {
		return err
	}
	slices.Sort(entries)
	for _, name := range entries {
		version := strings.TrimSuffix(name, ".sql")
		var n int
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM schema_migrations WHERE version = ?`, version).Scan(&n); err != nil {
			return err
		}
		if n > 0 {
			continue
		}
		body, err := migrations.FS.ReadFile(name)
		if err != nil {
			return err
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, string(body)); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("migration %s: %w", name, err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)`, version, time.Now().UnixMilli()); err != nil {
			_ = tx.Rollback()
			return err
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("migration %s: %w", name, err)
		}
	}
	return nil
}

// SchemaVersion returns the newest applied migration.
func (d *DB) SchemaVersion(ctx context.Context) (string, error) {
	var v sql.NullString
	err := d.r.QueryRowContext(ctx, `SELECT MAX(version) FROM schema_migrations`).Scan(&v)
	return v.String, err
}

func ms(t time.Time) int64 { return t.UnixMilli() }

func nullMS(t *time.Time) any {
	if t == nil || t.IsZero() {
		return nil
	}
	return t.UnixMilli()
}

func fromMS(v sql.NullInt64) *time.Time {
	if !v.Valid {
		return nil
	}
	t := time.UnixMilli(v.Int64).UTC()
	return &t
}

func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}
