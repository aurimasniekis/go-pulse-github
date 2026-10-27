-- pulse-github state database, schema 1.
-- Timestamps are Unix milliseconds (UTC) unless stated otherwise. Payload columns hold exact bytes.

-- Diagnostic record of the process holding the run lock (the lock itself is flock on <db>.lock).
CREATE TABLE instance (
    id           INTEGER PRIMARY KEY CHECK (id = 1),
    holder       TEXT    NOT NULL,
    pid          INTEGER NOT NULL,
    hostname     TEXT    NOT NULL,
    version      TEXT    NOT NULL,
    started_at   INTEGER NOT NULL,
    heartbeat_at INTEGER NOT NULL
);

-- Monitored accounts. account_key is the stable routing identity; github_id is immutable on GitHub.
CREATE TABLE accounts (
    account_key  TEXT PRIMARY KEY,
    github_id    INTEGER UNIQUE,
    login        TEXT    NOT NULL,
    account_type TEXT    NOT NULL,
    first_seen   INTEGER NOT NULL,
    updated_at   INTEGER NOT NULL,
    renamed_from TEXT
);

-- GitHub App installations known to this service.
CREATE TABLE installations (
    installation_id      INTEGER PRIMARY KEY,
    app_id               INTEGER,
    account_github_id    INTEGER NOT NULL,
    account_login        TEXT    NOT NULL,
    account_type         TEXT    NOT NULL,
    repository_selection TEXT,
    permissions_json     TEXT,
    events_json          TEXT,
    status               TEXT    NOT NULL DEFAULT 'active', -- active | suspended | removed | inaccessible
    suspended_at         INTEGER,
    updated_at           INTEGER NOT NULL,
    last_reconciled_at   INTEGER
);
CREATE INDEX installations_account ON installations (account_github_id);

CREATE TABLE installation_repositories (
    installation_id INTEGER NOT NULL,
    repo_id         INTEGER NOT NULL,
    full_name       TEXT    NOT NULL,
    private         INTEGER NOT NULL DEFAULT 0,
    status          TEXT    NOT NULL DEFAULT 'active', -- active | removed
    updated_at      INTEGER NOT NULL,
    PRIMARY KEY (installation_id, repo_id)
);

-- Durable webhook inbox: a row is committed before GitHub receives a 2xx.
CREATE TABLE webhook_inbox (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    delivery_id         TEXT    NOT NULL UNIQUE,
    hook_id             TEXT,
    event               TEXT    NOT NULL,
    installation_target_type TEXT,
    installation_target_id   TEXT,
    body                BLOB,
    body_size           INTEGER NOT NULL,
    body_sha256         TEXT    NOT NULL,
    received_at         INTEGER NOT NULL,
    state               TEXT    NOT NULL DEFAULT 'pending', -- pending | processed | unrouted | quarantined
    attempts            INTEGER NOT NULL DEFAULT 0,
    next_attempt_at     INTEGER NOT NULL DEFAULT 0,
    last_error          TEXT,
    error_class         TEXT,
    duplicates          INTEGER NOT NULL DEFAULT 0,
    processed_at        INTEGER
);
CREATE INDEX webhook_inbox_pending ON webhook_inbox (state, next_attempt_at, id);

-- One row per distinct observation. dedup_key is unique across sources and scoped as each source documents.
CREATE TABLE observations (
    observation_id TEXT PRIMARY KEY,
    source         TEXT    NOT NULL,
    dedup_key      TEXT    NOT NULL UNIQUE,
    event          TEXT    NOT NULL,
    action         TEXT    NOT NULL,
    occurred_at    INTEGER,
    observed_at    INTEGER NOT NULL,
    mode           TEXT    NOT NULL,
    inbox_id       INTEGER REFERENCES webhook_inbox (id) ON DELETE SET NULL,
    created_at     INTEGER NOT NULL
);
CREATE INDEX observations_created ON observations (created_at);

-- Publication outbox: one row per (observation, account). publication_id is the Pulse event ID on every retry.
CREATE TABLE outbox (
    seq             INTEGER PRIMARY KEY AUTOINCREMENT,
    publication_id  TEXT    NOT NULL UNIQUE,
    observation_id  TEXT    NOT NULL,
    account_key     TEXT    NOT NULL,
    subject         TEXT    NOT NULL,
    ce_type         TEXT    NOT NULL,
    ce_time         INTEGER NOT NULL, -- Unix nanoseconds
    time_basis      TEXT    NOT NULL, -- occurred | observed
    payload         BLOB    NOT NULL,
    blob_bucket     TEXT,
    blob_name       TEXT,
    blob_size       INTEGER,
    blob_sha256     TEXT,
    state           TEXT    NOT NULL DEFAULT 'pending', -- pending | published | quarantined
    attempts        INTEGER NOT NULL DEFAULT 0,
    next_attempt_at INTEGER NOT NULL DEFAULT 0,
    last_error      TEXT,
    error_class     TEXT,
    stream_seq      INTEGER,
    duplicate       INTEGER,
    created_at      INTEGER NOT NULL,
    published_at    INTEGER,
    UNIQUE (observation_id, account_key)
);
CREATE INDEX outbox_pending ON outbox (state, account_key, seq);
CREATE INDEX outbox_published ON outbox (state, published_at);

-- Polling progress per account, collector and feed.
CREATE TABLE checkpoints (
    account_key     TEXT    NOT NULL,
    collector       TEXT    NOT NULL,
    feed            TEXT    NOT NULL,
    cursor          TEXT,
    etag            TEXT,
    last_modified   TEXT,
    poll_interval_s INTEGER,
    last_success_at INTEGER,
    last_attempt_at INTEGER,
    last_error      TEXT,
    error_class     TEXT,
    updated_at      INTEGER NOT NULL,
    PRIMARY KEY (account_key, collector, feed)
);

-- Last meaningful state of each notification thread per account.
CREATE TABLE notification_state (
    account_key  TEXT    NOT NULL,
    thread_id    TEXT    NOT NULL,
    fingerprint  TEXT    NOT NULL,
    updated_at   TEXT,
    last_seen_at INTEGER NOT NULL,
    PRIMARY KEY (account_key, thread_id)
);

-- Last seen fingerprint of reconciled resources.
CREATE TABLE resource_fingerprints (
    account_key TEXT    NOT NULL,
    kind        TEXT    NOT NULL,
    resource_id TEXT    NOT NULL,
    fingerprint TEXT    NOT NULL,
    seen_at     INTEGER NOT NULL,
    PRIMARY KEY (account_key, kind, resource_id)
);

-- Bounded, resumable backfill and reconciliation runs.
CREATE TABLE backfill_jobs (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    account_key   TEXT    NOT NULL,
    mode          TEXT    NOT NULL, -- backfill | reconciliation
    kinds         TEXT    NOT NULL, -- comma-separated
    since         INTEGER NOT NULL,
    max_requests  INTEGER NOT NULL,
    requests_used INTEGER NOT NULL DEFAULT 0,
    cursor        TEXT,
    status        TEXT    NOT NULL DEFAULT 'queued', -- queued | running | paused | done | failed
    last_error    TEXT,
    created_at    INTEGER NOT NULL,
    updated_at    INTEGER NOT NULL
);
CREATE INDEX backfill_jobs_status ON backfill_jobs (status, id);

-- Optional enrichment fetches for an observation.
CREATE TABLE enrichment_jobs (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    observation_id TEXT    NOT NULL,
    account_key    TEXT    NOT NULL,
    kind           TEXT    NOT NULL,
    url            TEXT    NOT NULL,
    state          TEXT    NOT NULL DEFAULT 'pending', -- pending | done | failed | skipped
    attempts       INTEGER NOT NULL DEFAULT 0,
    last_error     TEXT,
    created_at     INTEGER NOT NULL,
    updated_at     INTEGER NOT NULL,
    UNIQUE (observation_id, account_key, url)
);
CREATE INDEX enrichment_jobs_state ON enrichment_jobs (state, id);

-- Detected or suspected collection gaps.
CREATE TABLE coverage_gaps (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    account_key  TEXT    NOT NULL,
    collector    TEXT    NOT NULL,
    feed         TEXT    NOT NULL,
    kind         TEXT    NOT NULL,
    window_start INTEGER,
    window_end   INTEGER,
    detail       TEXT,
    detected_at  INTEGER NOT NULL
);
CREATE INDEX coverage_gaps_detected ON coverage_gaps (detected_at);
