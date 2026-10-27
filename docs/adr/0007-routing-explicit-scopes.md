# ADR-0007: Route by verified scope plus explicit shares

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
