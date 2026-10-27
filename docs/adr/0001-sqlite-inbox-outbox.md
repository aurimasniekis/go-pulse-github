# ADR-0001: SQLite inbox/outbox with a single active process

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
