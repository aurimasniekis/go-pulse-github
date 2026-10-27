# ADR-0002: Deterministic IDs and at-least-once publication

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
