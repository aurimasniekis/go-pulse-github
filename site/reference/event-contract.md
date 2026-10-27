# Event contract

pulse-github publishes Pulse events. It does not wrap them in a second envelope: the transport is the Pulse envelope
(NATS headers), and the body is a versioned JSON document.

## Subjects

```
evt.<domain>.<account_key>.<source>.<event>.<action>
```

| Token | Values |
|---|---|
| `domain` | `pulse.subject_domain`, default `github` |
| `account_key` | the configured, stable routing key (`^[a-z0-9][a-z0-9_-]{0,38}$`). It defaults to the lowercased login and stays the same when the GitHub login changes. |
| `source` | `webhook`, `notifications`, `activity`, `audit`, `reconcile`, `enrichment`, `collector` |
| `event` | webhook: `X-GitHub-Event`; activity: the Events API type in webhook style (`PushEvent` → `push`); notifications: `thread`; reconcile/enrichment: the resource kind; audit: the action category; collector: `coverage`, `access`, `doctor` |
| `action` | webhook: the payload's `action`, or `none` when absent; notifications: `observed`, `updated`; activity: `observed`; reconcile: `snapshot`, `changed`, `added`, `access_removed`, `removed`, `suspended`, `reactivated`, `renamed`; enrichment: `fetched`; collector: `gap_detected`, `lost`, `restored`, `test` |

Repository names, issue numbers and other user content never appear in subjects. They are in the payload.

### Token encoding

Every `event` and `action` value goes through one deterministic, collision-free function (`events.Token`):

1. A value matching `[a-z0-9_]{1,48}` that is not reserved is used **verbatim** (`issues`, `pull_request_review`).
2. Otherwise it becomes `x-` + lowercase unpadded base32hex of the value, if the result fits in 64 bytes
   (`Opened` → `x-9to6arj5cg`).
3. Otherwise it becomes `h-` + the first 26 base32 characters of its SHA-256.

Verbatim tokens never contain `-`, so they cannot collide with encoded ones. `none` is reserved for "no action": an
action literally named `none` is encoded. Dots, wildcards (`*`, `>`), spaces and uppercase can never reach a subject.
Every subject is also validated with Pulse's `envelope.ValidateSubject` before it is stored. The original values are
always in `github.event` and `github.action`.

### Filters

| Filter | Matches |
|---|---|
| `evt.github.>` | all GitHub activity |
| `evt.github.aurimasniekis.>` | one monitored account |
| `evt.github.*.webhook.>` | webhook activity of every account |
| `evt.github.example-org.webhook.pull_request.*` | every pull request action of one organization |
| `evt.github.*.webhook.issues.opened` | opened issues, all accounts |
| `evt.github.aurimasniekis.notifications.>` | one user's notification changes |
| `evt.github.*.collector.>` | coverage gaps and access changes |
| `evt.github.*.reconcile.*.changed` | inferred changes found by reconciliation |

## Envelope (Pulse headers)

| Header | Value |
|---|---|
| `Nats-Msg-Id` | `publication_id`: stable for the (observation, account) pair, reused on every retry and replay |
| `ce-type` | `github.<source>.<event>.<action>.v1`, e.g. `github.webhook.issues.opened.v1` |
| `ce-source` | `service.source` (default `pulse-github`) |
| `ce-time` | see below |
| `content-type` | `application/json` |
| `github-time-basis` | `occurred` or `observed`: which time `ce-time` carries |
| `pulse-blob`, `pulse-blob-size`, `pulse-blob-sha256` | only when the full document was stored as a blob |

### Times

- **`ce-time`** is `occurred_at` when GitHub reported when the thing happened. Otherwise it is `observed_at`.
  `github-time-basis` says which one was used.
- **`occurred_at`** in the payload is `null` unless a source field unambiguously records the time. Examples:
  - `issues.opened` → `issue.created_at`
  - `issue_comment.created` → `comment.created_at`
  - `push` → `repository.pushed_at`
  - Events API → `created_at`
  - notifications → `updated_at`, only when it advanced
  - audit log → `@timestamp`
  - snapshots and enrichment: always `null`

  `occurred_at_basis` names the field.
- **`observed_at`** is when pulse-github received or fetched the data. It is never presented as the occurrence time.
- The **stream timestamp** (Pulse replay `--since`) is when JetStream stored the event. It can be much later than
  both of the above after an outage.

## Payload (schema_version 1)

Rendered by the code from `testdata/webhooks/issues_opened.json` (abridged only by indentation):

```json
{
  "schema_version": 1,
  "observation_id": "oirhse7h5m4phjlrhryhqgvgdtt",
  "publication_id": "poiopw6f5zgtc5mqmjaj7w6xfpe",
  "account": {"key": "example-org", "github_id": 7001, "login": "example-org", "type": "organization"},
  "source": {"collector": "webhook", "installation_id": 555, "hook_id": "512345"},
  "github": {
    "event": "issues",
    "action": "opened",
    "delivery_id": "8f2c7e10-b3a1-11f0-9c6d-1a2b3c4d5e6f",
    "payload_sha256": "0beb3690dade8e576e7c117c0defff074f21a7abd54fa11cc0caa4adcbd11e15"
  },
  "actor": {"id": 15481, "login": "aurimasniekis", "type": "User"},
  "repository": {
    "id": 9001, "full_name": "example-org/tool",
    "owner": {"id": 7001, "login": "example-org", "type": "Organization"},
    "private": true, "visibility": "private"
  },
  "organization": {"id": 7001, "login": "example-org", "type": "Organization"},
  "resource": {
    "kind": "issue", "id": 3001, "node_id": "I_kw1", "number": 42,
    "url": "https://api.github.com/repos/example-org/tool/issues/42",
    "html_url": "https://github.com/example-org/tool/issues/42"
  },
  "occurred_at": "2026-10-20T10:00:00Z",
  "occurred_at_basis": "payload:issue.created_at",
  "observed_at": "2026-10-20T10:00:01.25Z",
  "collection_mode": "live",
  "provenance": {"authoritative": true, "snapshot": false, "inferred": false},
  "routing": {"match": "installation_account", "also_routed_to": ["aurimasniekis"]},
  "payload": {"action": "opened", "issue": {"id": 3001, "…": "…"}, "sender": {"…": "…"}, "installation": {"id": 555}}
}
```

| Field | Meaning |
|---|---|
| `observation_id` | Shared by every account's copy of the same observation. |
| `publication_id` | Equals `Nats-Msg-Id`. **Deduplicate by this.** |
| `account` | The monitored account the event is routed to. This is **not** the actor and **not** the repository owner. |
| `source` | Collector, App installation (webhooks, reconcile), feed (polling). |
| `github` | GitHub's own identifiers: event name and action, `delivery_id`, `event_id` (Events API, audit `_document_id`), `notification_id`, SHA-256 of the original webhook body. |
| `actor` | Who did it (`sender` / `actor`), when the source says so. |
| `repository`, `organization`, `resource` | Extracted identifiers and URLs. They are null when absent. |
| `collection_mode` | `live`, `backfill`, `reconciliation` (also notification full sweeps), `enrichment`. |
| `provenance` | `authoritative` (GitHub reported it as activity); `snapshot` (current state, not an event); `inferred` (a difference computed by this service); `source_observation_id` (enrichment); `fingerprint` / `previous_fingerprint`; `qualifiers`. |
| `routing` | `match`: `installation_account`, `credential_account`, `share`, `service`. `shared_from` for shares; `also_routed_to` lists other accounts that received the same observation. |
| `payload_blob` | Present instead of `payload` when the document exceeded the message limit (see below). |
| `payload` | The **original source JSON, byte for byte**. Nothing is re-encoded, so big integers and formatting are kept and unknown fields survive. |

Qualifiers in use: `push_commits_max_2048`, `no_events_for_more_than_3_tags`, `events_api_bounded`,
`push_without_commit_list`, `private_event`, `notification_state`, `not_the_underlying_event`, `current_state`,
`no_event_time`, `current_state_at_fetch_time`, `audit_log`, `service_generated`, `non_string_action`.

### Other sources

- **Notification:** `payload` is the thread object from `GET /notifications`.
  `github = {"event": "notification", "action": "<reason>", "notification_id": "…"}`, and the provenance carries the
  fingerprints.
- **Activity:** `payload` is the Events API event object, with `github.event_id` set.
- **Snapshot:** `payload` is the REST resource as fetched, with `provenance.snapshot = true` and `occurred_at: null`.
  `changed` adds `inferred: true` and `previous_fingerprint`.
- **Enrichment:** `payload` is the fetched resource, with `provenance.source_observation_id` pointing at the
  notification it came from.
- **`collector.coverage.gap_detected`:** `payload = {"collector", "feed", "kind", "window_start", "window_end", "detail"}`.
  Kinds: `page_budget_exhausted`, `cursor_not_found`, `outside_retention`, `request_budget_exhausted`.
- **`collector.access.lost` / `restored`:** `payload = {"collector", "feed", "error_class", "detail", "at"}`.

### Large payloads

When a document is larger than the server's `max_payload` (1 MiB on Pulse) minus 8 KiB for headers:

- with `pulse.blobs.enabled` (the client needs the `blobs` grant), the complete document is stored with `PutBlob`.
  The event body is the same document with `"payload": null` and
  `"payload_blob": {"bucket", "name", "size", "sha256"}`, and the Pulse `pulse-blob*` headers are set. Read the
  original with `GetBlob`.
- without blobs, the publication is quarantined with error class `payload_too_large`. It is never truncated. Enable
  blobs and run `pulse-github quarantine replay <publication_id>`.

## Duplicates and consumer obligations

- The same `publication_id` can be delivered more than once (at-least-once; see [delivery guarantees](/design/architecture#delivery-guarantees)). Consumers must be
  idempotent on the event ID.
- The same GitHub activity can arrive through several sources (webhook, notification, Events API, snapshot) as
  **different** events. They are intentionally not merged. Correlate them through `github.*` and `resource.*`
  identifiers if needed.
- With `routing.shares`, an observation is intentionally published once per matching account: same
  `observation_id`, different `publication_id`, different subjects.

## Versioning

The type carries the version (`….v1`), not the subject. Additive fields do not change the version. A breaking change
to the document would publish `….v2` types with `schema_version: 2`.
