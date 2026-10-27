# ADR-0006: Sources are not merged; snapshots are not history

**Status:** accepted

**Context.** A webhook, a notification, an Events API record and a snapshot of the same activity carry different
information and have different trust levels.

**Decision.**
- Each source publishes its own events with provenance.
- Correlation is left to consumers through GitHub identifiers.
- Snapshots are labelled current state, and computed differences are labelled `inferred`.
- A 404 or lost access never produces a deletion event.

**Consequences.** More events and no false certainty. Consumers can choose which sources to trust.
