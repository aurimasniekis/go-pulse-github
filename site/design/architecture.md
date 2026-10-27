# Architecture

pulse-github is a single Go process that collects GitHub activity for several accounts and publishes it to an
existing Pulse deployment. It is headless: there is no UI, but it can listen for GitHub webhooks.

```
                 GitHub App webhooks (signed)                 GitHub REST API (polling)
                            │                                            │
                            ▼                                            ▼
                 ┌─────────────────────┐      ┌──────────────────────────────────────────────────┐
                 │ webhook receiver    │      │ notifications · activity · audit log ·            │
                 │ size/time limits,   │      │ installation reconcile · snapshot jobs ·          │
                 │ HMAC, admission     │      │ enrichment  (budgets, ETag/IMS, fair semaphore)   │
                 └─────────┬───────────┘      └──────────────────────┬───────────────────────────┘
                 commit, then 2xx                 observations + checkpoint in one transaction
                           ▼                                         ▼
                 ┌──────────────────────────────────────────────────────────────────────────────┐
                 │ SQLite (WAL, synchronous=FULL): webhook_inbox → observations → outbox (FIFO per │
                 │ account), checkpoints, notification/resource state, jobs, gaps, quarantine      │
                 └──────────────────────────────────────┬───────────────────────────────────────┘
                                                        ▼
                                      outbox dispatcher (Pulse Go SDK v0.1.3)
                                      complete only after the JetStream ack
                                                        ▼
                                      Pulse NATS JetStream, stream EVENTS, evt.github.>
```

## Upstream contracts (verified)

All of these were read in `github.com/aurimasniekis/go-pulse` **v0.1.3** (commit `9aba53b5c8cf`). That version is
pinned in `go.mod`. The Pulse module requires Go 1.27.3, so this module does too.

| Contract | Source | What pulse-github relies on |
|---|---|---|
| Subject grammar | `pkg/pulse/envelope.ValidateSubject` | `evt.` prefix, ≥ 4 tokens, each `[a-z0-9_-]+`, ≤ 256 bytes. Every subject is validated with this function before it is stored. |
| Envelope | `pkg/pulse/envelope.Encode` | `Nats-Msg-Id` (the ID), `ce-type`, `ce-source`, `ce-time` (RFC 3339 UTC), `content-type`, `pulse-blob*`. Any other header passes through (we add `github-time-basis`). |
| Publish | `pulse.Client.Publish` + `WithID`, `WithTime`, `WithHeader`, `WithBlob` | Waits for the JetStream ack and returns `PubAck{Stream, Sequence, Duplicate}`. |
| Duplicate window | `internal/streams/events.go` (`DuplicateWindow = 2m`) | A retry with the same ID within 2 minutes is dropped and the ack says `Duplicate`. |
| Max payload | `MaxMsgSize = 1<<20`, NATS `max_payload` | The body plus headers must fit. The dispatcher reads `nc.MaxPayload()` and keeps 8 KiB for headers. |
| Blobs | `pulse.Client.PutBlob` | Every call stores a **new** object name. pulse-github persists the reference before publishing, so a retry reuses the same blob. |
| Credentials | `pulse.Connect(WithCredsFile)` | The client ID comes from the JWT tag `pulse-client:<id>`. The SDK sets the inbox prefix `_INBOX_<id>`. |
| Grants | `internal/natsauth/grants.go` | `publish:evt.github.>` is enough. `blobs` is needed only for blob offload. NATS enforces denials, and they surface as **timeouts** (go-pulse `docs/ENVELOPE.md`). |

pulse-github does not create or modify streams, buckets, users or accounts. It is an ordinary Pulse client.

## Packages

| Package | Responsibility |
|---|---|
| `cmd/pulse-github` | CLI: `run`, `version`, `config validate`, `doctor`, `backfill`, `quarantine list/replay/purge`, `status`, `healthcheck`. |
| `internal/config` | Versioned YAML, defaults, `PULSE_GITHUB_*` overrides, secret references, validation. |
| `internal/storage`, `migrations/` | SQLite: open, pragmas, embedded transactional migrations, `flock` run lock, every table's queries. |
| `internal/events` | Payload schema v1, subject tokens, deterministic IDs, webhook normalization. |
| `internal/routing` | Maps verified scopes to configured accounts, applies shares and repository filters. |
| `internal/capture` | Writes observations and their routed publications. Publishes gap and access events. |
| `internal/collectors/webhook` | HTTP receiver into the inbox; inbox processor including installation lifecycle. |
| `internal/collectors/notifications` | `GET /notifications` poller. |
| `internal/collectors/activity` | Events API feeds and the optional audit log. |
| `internal/reconcile` | Installation and repository-access reconciliation; snapshot and backfill jobs. |
| `internal/enrich` | Optional, budgeted fetches of referenced resources. |
| `internal/outbox` | Publication dispatcher. |
| `internal/publisher` | Pulse SDK wrapper and error classification. |
| `internal/githubapi`, `internal/githubauth` | GitHub REST client; App JWT, installation tokens, PATs, webhook HMAC. |
| `internal/admin`, `internal/observability` | Health, readiness, metrics, pprof, storage guard, slog setup. |
| `internal/app` | Wiring, lifecycle, graceful shutdown. |
| `internal/pulsetest`, `internal/githubtest`, `internal/testutil` | Test harnesses: Pulse-shaped embedded NATS, fake GitHub API, helpers. |

## Data flow and durability

### Webhooks

1. **Limits.** `http.MaxBytesReader` caps the body at 26 MiB by default (GitHub caps payloads at 25 MB). The server
   has header, read and write timeouts, and only `POST` with `application/json` is accepted.
2. **Signature.** `X-Hub-Signature-256` is checked over the unmodified body against every configured secret with
   `hmac.Equal`, so rotation works. An invalid signature gets 401 before anything else happens.
3. **Metadata.** The delivery ID, event name, hook ID and target ID headers must have the expected format. The body
   must be a JSON object.
4. **Admission.** The storage guard has to be open: database writable, enough free space, database size and inbox
   depth under their limits. Otherwise the answer is **503** with `Retry-After`.
5. **Commit.** `INSERT … ON CONFLICT(delivery_id) DO NOTHING` runs in a `synchronous=FULL` transaction. Only after
   the commit does the receiver answer **202**, or **200** for a redelivery of a stored delivery ID.
6. **Normalize.** The inbox processor resolves the installation (stored, or `GET /app/installations/{id}` with the
   App JWT) and applies installation and repository-access changes. In **one transaction** it writes the
   observation, one outbox row per routed account, and the inbox state.
7. **Publish.** The dispatcher publishes each account's outbox in FIFO order. A row is marked `published` only after
   the JetStream ack.

### Polling

Each page of results is stored in one transaction: observations, outbox rows, and state rows such as notification
fingerprints and resource fingerprints. The feed's checkpoint (`since` cursor, newest event ID, ETag or
`Last-Modified`) is written in the same transaction as the run's last page, or after it. It never moves past data
that was not stored. A crash at any point re-reads from the old checkpoint, and dedup keys absorb the overlap.

### Identifiers

- `observation_id = "o" + base32(sha256(source ‖ dedup_key))[:26]`
- `publication_id = "p" + base32(sha256(observation_id ‖ account_key))[:26]`

The publication ID is the Pulse event ID (`Nats-Msg-Id`). Retries, process restarts, webhook redeliveries and
quarantine replays all reuse the same publication ID.

| Source | Dedup key | Scope |
|---|---|---|
| webhook | `webhook:<X-GitHub-Delivery>` | global (a delivery belongs to one installation) |
| notifications | `notif:<account>:<thread id>:<state fingerprint>` | per account |
| activity | `activity:event:<GitHub event id>` | global: the same event seen by two accounts is **one** observation with two publications |
| audit | `audit:<org>:<_document_id>` | per org |
| reconcile | `reconcile:<account>:<kind>:<id>:<fingerprint>` | per account |
| enrichment | `enrich:<account>:<url>:<sha256 prefix of body>` | per account |

### Delivery guarantees

- **At least once after durable capture.** Every published event was committed to SQLite first.
- **No exactly-once.** The broker drops duplicates for 2 minutes. If the process crashes after the ack but before
  marking the row complete, and restarts later than that, JetStream stores a second copy with the **same event ID**
  (tested in `internal/outbox` and `internal/publisher`). Consumers must deduplicate by event ID (`Nats-Msg-Id`, also
  `publication_id` in the payload).
- **Ordering.** Within one account the outbox is FIFO with at most one publication in flight, so events of one
  account are published in the order they were captured. A quarantined row is skipped and keeps its position for
  replay; a replayed row is published out of order. There is **no** ordering across accounts or across sources:
  a notification and the webhook it relates to come from independent systems with independent latency.
- **Cross-source observations are not merged.** A webhook, a notification and an Events API record of the same
  activity carry different information and stay separate events.

### Failure handling

| Failure | Behaviour |
|---|---|
| Pulse/NATS down | The SDK reconnects forever. Publishes fail with transient classes (`nats_unavailable`, `publish_timeout`, `no_stream`), rows stay pending with jittered exponential backoff (1 s up to 5 min), and webhook admission stays open. |
| Missing publish grant | Shows up as `publish_timeout` while connected. It is retried, never quarantined, and alerts fire on publication lag. `doctor --test-publish` diagnoses it. |
| Invalid subject or oversized payload without blobs | Permanent. The row is quarantined with an actionable error and the account's partition continues. |
| Server rejects a message (JetStream API error) | `publish_rejected`, retried with backoff, then quarantined after `outbox.poison_attempts` consecutive rejections so the account's partition continues. Replay it after fixing the cause. |
| Webhook body cannot be normalized | Retried, then quarantined after `processor_poison_attempts`. |
| Installation lookup fails | Retried with backoff. The delivery stays `pending`. |
| Delivery matches no configured account | Stored as `unrouted`. It is requeued after installation reconciliation finds changes. |
| Storage full or broken | Admission closes (503 and not ready), pollers pause, and the dispatcher keeps draining what was already stored. |
| SIGTERM | Readiness closes and pollers pause. The webhook listener stops accepting connections; requests already being read are still admitted and committed while storage is healthy. Workers stop: publishes in flight complete, poll transactions roll back. NATS is drained and the WAL checkpointed. Everything unfinished resumes from SQLite on the next start. |

### Single active process

`run` takes an exclusive, non-blocking `flock` on `<db>.lock`. A second `run` on the same database exits with
"another pulse-github process holds the database lock". `flock` is reliable only on local filesystems, so do not put
the state volume on NFS. Multi-replica high availability on shared SQLite is not supported. Inspection and control
commands (`status`, `quarantine`, `backfill`) open the database without the lock: SQLite WAL allows them next to the
running service, and they use short transactions.

## GitHub API client

- Every request sends `Accept: application/vnd.github+json` and `X-GitHub-Api-Version`. The default is
  `2022-11-28`, which GitHub supports until 2028-03-10. `2026-03-10` exists and contains breaking changes, and this
  service has not been validated against it.
- Timeouts are bounded and the TLS 1.2+ client verifies certificates against system roots plus `github.ca_files`.
  Redirects are never followed.
- Pagination follows `Link: rel="next"` only when the URL has the configured API scheme, host and path prefix. The
  same check applies to every URL taken from GitHub content before enrichment, which prevents SSRF and credential
  leakage.
- Conditional requests use `ETag`/`If-None-Match` (Events API) and `Last-Modified`/`If-Modified-Since`
  (notifications). GitHub documents that a 304 does not count against the rate limit.
- **Rate limits.** If `retry-after` is present, it wins. Otherwise, with `x-ratelimit-remaining: 0`, the client waits
  for `x-ratelimit-reset`. Otherwise (a secondary limit without headers) it waits at least one minute. A
  per-credential block stops further requests until that time. Waits of up to 2 minutes happen inline; longer ones
  return to the collector, which reschedules. A reserve (`rate_limit_reserve`) is kept per credential.
- **Fairness.** A FIFO-fair weighted semaphore (`max_concurrent_requests`) is shared by every collector. Snapshot
  jobs take one request per account per turn.
- **Error classes:** `unauthorized` (expired, revoked or wrong credential: the credential is invalidated and
  retried once), `forbidden` (missing permission, suspended installation, SSO), `not_found` (ambiguous: deleted or
  not visible), `gone`, `rate_limited`, `secondary_limited`, `invalid_request`, `server_error`, `network_error`,
  `credential_error`.
- **Installation tokens** are cached in memory only and refreshed 5 minutes before expiry. Concurrent refreshes are
  coalesced with `singleflight`. They are treated as opaque strings: the 2026 format is about 520 characters.
- **PATs** are read from the referenced file and re-read when the file's modification time changes or after a 401,
  so rotation needs no restart.

## Observability

- **Logs.** `log/slog` JSON, one line per entry, UTC RFC 3339 timestamps, stable keys (`component`, `account_key`,
  `installation_id`, `collector`, `feed`, `delivery_id`, `observation_id`, `publication_id`, `event_type`, `attempt`,
  `duration_ms`, `error_class`, `error`). Payloads, comment bodies and tokens are never logged.
- **Metrics.** `pulse_github_*` on the admin listener. Labels are bounded: account labels only take configured keys,
  event names map to a fixed category list or `other`, credential labels are `app`, `token:<account>` and
  `installation:<account>`. No delivery IDs, URLs, repositories or actors appear as labels.
- **`/healthz`** answers 200 while the process runs. **`/readyz`** answers 200 while durable admission is safe and
  503 when storage is broken, full or over its limits, or during shutdown. NATS state is reported in the body but
  does not affect readiness.

## GitHub Enterprise Server

`github.api_url` and `github.ca_files` make GHES usable in principle (`https://ghe.example.com/api/v3`). It is
**not tested** against a real GHES. Endpoint availability, the audit-log API and API versions differ by GHES version.
