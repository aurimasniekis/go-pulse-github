# Decision records

Short architectural decision records for the trade-offs that shape pulse-github. A change to one of these trade-offs
adds a record here or marks an existing one as superseded; records are never renumbered.

## ADR-0001: SQLite inbox/outbox with a single active process

**Status:** accepted

**Context.** Webhooks must be durable before GitHub gets a 2xx, polling progress must never pass unstored data, and
publication must survive Pulse outages. The service is personal-scale and Docker-first.

**Decision.** Use one SQLite database (`modernc.org/sqlite`, pure Go) in WAL mode with `synchronous=FULL` on a local
volume. It holds the webhook inbox, observations, a per-account publication outbox, checkpoints and collector state.
Each collector writes its observations and progress in one transaction. `flock` on `<db>.lock` allows one running
process per database.

**Consequences.**
- No external database or queue to run, and `CGO_ENABLED=0` builds for amd64 and arm64.
- No multi-replica high availability. A shared SQLite volume is explicitly unsupported, and `flock` needs a local
  filesystem.
- Throughput is bounded by one writer. That is ample for webhook rates of individual accounts and organizations.

## ADR-0002: Deterministic IDs and at-least-once publication

**Status:** accepted

**Context.** Pulse drops duplicate `Nats-Msg-Id`s for 2 minutes only. A crash after the JetStream ack and before the
local completion re-publishes later, possibly outside that window.

**Decision.**
- `observation_id` is derived from the source and dedup key.
- `publication_id` is derived from the observation ID and the account key, and it is the Pulse event ID.
- A row is marked published only after the ack.

Redeliveries, retries and replays all reuse the same ID. Exactly-once is not claimed.

**Consequences.** Consumers deduplicate by event ID. Duplicates outside the window are possible and documented, and
tested against real JetStream with a shortened window.

## ADR-0003: Account-scoped subjects with collision-free token encoding

**Status:** accepted

**Context.** Consumers filter by account and by event. GitHub event and action names are open-ended, and Pulse
tokens must match `[a-z0-9_-]+`.

**Decision.** Subjects are `evt.github.<account_key>.<source>.<event>.<action>`:

- The account key is configured and stable. It defaults to the lowercased login and is kept when the login changes.
- Values that are already plain `[a-z0-9_]` are used verbatim. Anything else is encoded `x-<base32hex>` or
  `h-<sha256 prefix>`.
- `none` marks a missing action.
- No user content appears in subjects.

**Consequences.**
- `evt.github.<account>.>` and `evt.github.>` work as expected, and new GitHub events need no release.
- Encoded tokens are opaque in subjects; the original values are in the payload.

## ADR-0004: ce-time is the occurrence time only when GitHub reports one

**Status:** accepted

**Context.** Pulse's `ce-time` is "when it happened". Receipt time is not occurrence time, and many GitHub payloads
have no unambiguous event time.

**Decision.**
- `occurred_at` is set only from an explicit list of unambiguous fields; it is otherwise null.
- `ce-time` equals `occurred_at` when it is set, otherwise `observed_at`.
- The header `github-time-basis` and the field `occurred_at_basis` say which.
- Snapshots and enrichment never set `occurred_at`.

**Consequences.** Time-ordered views must check the basis. `observed_at` and the stream timestamp are always
available.

## ADR-0005: A small REST client instead of go-github

**Status:** accepted

**Context.** The service needs:
- original JSON kept byte for byte, including fields unknown to SDK types;
- precise control over conditional requests, `X-Poll-Interval`, rate-limit headers and pagination;
- refusal of any URL outside the configured API origin (SSRF and credential safety).

**Decision.** Use a small `net/http` client (`internal/githubapi`) that returns raw bodies, plus stdlib RS256 for App
JWTs. Payloads are never decoded into typed SDK structs.

**Consequences.** Less code to trust and full header control. Each endpoint used is explicit and covered by tests
against a fake API. The cost is maintaining that endpoint list.

## ADR-0006: Sources are not merged; snapshots are not history

**Status:** accepted

**Context.** A webhook, a notification, an Events API record and a snapshot of the same activity carry different
information and have different trust levels.

**Decision.**
- Each source publishes its own events with provenance.
- Correlation is left to consumers through GitHub identifiers.
- Snapshots are labelled current state, and computed differences are labelled `inferred`.
- A 404 or lost access never produces a deletion event.

**Consequences.** More events and no false certainty. Consumers can choose which sources to trust.

## ADR-0007: Route by verified scope plus explicit shares

**Status:** accepted

**Context.** One process monitors several accounts whose activity overlaps: a user is a member of an organization,
and two users receive the same event.

**Decision.**
- An observation goes to the account that owns the installation (webhooks, reconciliation) or the credential
  (polling), resolved through GitHub IDs pinned from authenticated sources.
- Cross-account delivery happens only through configured `routing.shares` (by source and repository filter).
- Actor logins are never used for routing.
- Shared observations keep one `observation_id` and get one `publication_id` per account.

**Consequences.** Intentional, documented duplication across accounts; no accidental exposure across accounts.

## ADR-0008: Readiness and webhook admission depend on storage, not NATS

**Status:** accepted

**Context.** GitHub does not retry failed webhook deliveries, so rejecting deliveries during a Pulse outage would
lose data. Accepting them without durable storage would be worse.

**Decision.**
- Admission and `/readyz` close only when SQLite is broken or full, or the inbox is over its limit.
- A NATS outage leaves admission open: data queues in the outbox.
- When admission closes, webhooks get 503 explicitly; acceptance is never claimed without a commit.

**Consequences.** Pulse outages are absorbed up to the disk budget. Storage alerts are critical, because a closed
admission means lost webhooks unless they are redelivered.
