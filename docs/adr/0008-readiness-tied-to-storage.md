# ADR-0008: Readiness and webhook admission depend on storage, not NATS

**Status:** accepted

**Context.** GitHub does not retry failed webhook deliveries, so rejecting deliveries during a Pulse outage would
lose data. Accepting them without durable storage would be worse.

**Decision.**
- Admission and `/readyz` close only when SQLite is broken or full, or the inbox is over its limit.
- A NATS outage leaves admission open: data queues in the outbox.
- When admission closes, webhooks get 503 explicitly; acceptance is never claimed without a commit.

**Consequences.** Pulse outages are absorbed up to the disk budget. Storage alerts are critical, because a closed
admission means lost webhooks unless they are redelivered.
