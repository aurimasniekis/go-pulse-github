# ADR-0004: ce-time is the occurrence time only when GitHub reports one

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
