---
layout: home

hero:
  name: pulse-github
  text: Your GitHub activity, replayable in Pulse
  tagline: >-
    Signed webhooks, notifications, Events API feeds and reconciliation for every personal and organization account
    you monitor, captured durably and published to Pulse with stable event IDs.
  image:
    src: /logo.svg
    alt: pulse-github
  actions:
    - theme: brand
      text: Get started
      link: /guide/what-is-pulse-github
    - theme: alt
      text: Quick start
      link: /guide/quick-start
    - theme: alt
      text: What is captured
      link: /reference/coverage

features:
  - title: Webhooks, acknowledged only when durable
    details: >-
      Every GitHub App delivery is signature-checked and committed to SQLite before GitHub gets its 2xx. Unknown event
      types and actions are kept and published too, so a new GitHub event needs no release.
  - title: Everything your credentials can see
    details: >-
      Notifications from any repository, Events API feeds, the organization audit log, and bounded, resumable
      snapshots of repositories, issues, pull requests, releases and workflow runs.
  - title: At least once, with stable IDs
    details: >-
      Deterministic event IDs survive retries, restarts, redeliveries and replays. Each account's outbox is published
      in order, and a Pulse outage only queues events on disk.
  - title: Subjects made for replay
    details: >-
      evt.github.<account>.<source>.<event>.<action>, validated with Pulse's own rules. Subscribe to one account, one
      organization's pull requests or all coverage gaps.
  - title: Honest about coverage
    details: >-
      GitHub's original JSON travels byte for byte with provenance: live, snapshot or inferred. Gaps, lost access and
      rate limits become events and metrics instead of silent holes.
  - title: Small and headless
    details: >-
      One static Go binary in a distroless, non-root, read-only container. SQLite state, Prometheus metrics,
      structured logs, a read-only doctor and graceful shutdown.
---

<div class="pg-home">

## How it works

<Flow />

1. A **GitHub App** installed on your accounts sends signed webhooks; pulse-github stores each delivery before
   answering, then normalizes and routes it to the configured account.
2. **Pollers** read notifications, Events API feeds and, optionally, the audit log with your personal access tokens,
   using conditional requests and the poll intervals GitHub asks for.
3. **Reconciliation** keeps installations and repository access current and takes budgeted snapshots, for periodic
   checks and after downtime.
4. A per-account **outbox** publishes every observation to Pulse through the Go SDK and marks it done only after the
   JetStream acknowledgement.
5. Your tools **replay** `evt.github.>` from any point in time, then follow live.

## Where to go next

- New here? Read [What is pulse-github?](/guide/what-is-pulse-github).
- Ready to run it? The [quick start](/guide/quick-start) and [GitHub App and tokens](/guide/github).
- Writing a consumer? [Consuming events](/guide/consuming) and the [event contract](/reference/event-contract).
- Wondering what you will and will not see? [Coverage](/reference/coverage).
- Running it for real? [Operator guides](/operators/) and [troubleshooting](/operators/troubleshooting).

</div>
