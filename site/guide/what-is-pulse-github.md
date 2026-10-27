# What is pulse-github?

pulse-github is a headless, Docker-first Go service that collects GitHub activity for one or many personal and
organization accounts and publishes it into [Pulse](https://aurimasniekis.github.io/go-pulse/), a durable event bus on
NATS JetStream. Once the activity is in Pulse, any tool can replay it from a timestamp and then follow it live.

<Flow />

## Where the data comes from

GitHub has no single, complete activity feed. pulse-github combines every supported source the configured
credentials are allowed to read, and labels each event with where it came from:

| Source | What it gives you | Subject |
|---|---|---|
| **GitHub App webhooks** (primary) | Real-time events for the repositories and organizations the App is installed on: issues, pull requests, reviews, comments, pushes, releases, discussions, workflows, checks, deployments, members and more. Unknown events are kept too. | `evt.github.<account>.webhook.<event>.<action>` |
| **Notifications** | A user's notification threads, from any repository they get notifications for, including ones they do not own. | `evt.github.<account>.notifications.thread.*` |
| **Events API** (supplementary) | User, received, organization and repository event feeds: bounded to the newest 300 events and 30 days. | `evt.github.<account>.activity.<event>.observed` |
| **Audit log** (optional) | Organization audit events on GitHub Enterprise Cloud. | `evt.github.<account>.audit.<category>.<action>` |
| **Reconciliation and backfill** | Installation and repository access, and current-state snapshots of repositories, issues, pull requests, releases and workflow runs. | `evt.github.<account>.reconcile.<kind>.*` |
| **Collector events** | Coverage gaps and lost or restored access, so missing data is visible. | `evt.github.<account>.collector.>` |

What each source needs, and what none of them can see, is on the [coverage](/reference/coverage) page.

## Guarantees

- **Nothing is acknowledged before it is durable.** A webhook gets its 2xx only after it is committed to SQLite.
  Polled data and its checkpoint are committed together, so a crash re-reads instead of skipping.
- **At least once, with stable IDs.** Every publication has a deterministic event ID (`Nats-Msg-Id`). Retries,
  restarts, webhook redeliveries and quarantine replays reuse it, so consumers can deduplicate.
- **Pulse outages only queue.** While NATS is unreachable, captured data waits in a per-account FIFO outbox and is
  published in order after reconnecting.
- **Original data survives.** The payload is GitHub's JSON byte for byte, next to normalized identifiers (account,
  actor, repository, resource) and provenance (live, snapshot, inferred).
- **Gaps are visible.** When a feed outruns its window, a credential is revoked or a budget runs out, pulse-github
  publishes a collector event and exports a metric instead of failing silently.

## What it is not

- **Not a complete history of GitHub.** It sees only what its credentials can see, from the moment it runs (plus what
  the Events API and snapshots can recover).
- **Not exactly once.** JetStream drops duplicates for 2 minutes; after that, a retry can store a second copy with the
  same event ID. Consumers must be idempotent.
- **Not a GitHub client that writes.** It never marks notifications read, redelivers webhooks or changes anything.
- **Not highly available.** One active process per state database. Downtime is recovered from checkpoints,
  redeliveries and backfill.
- **Not a UI.** It is headless, but it does need an inbound HTTPS endpoint for webhooks unless you run it polling-only.

## Next steps

- [Quick start](/guide/quick-start): run it next to an existing Pulse.
- [GitHub App and tokens](/guide/github): which credentials collect what.
- [Event contract](/reference/event-contract): subjects, headers and the payload.
