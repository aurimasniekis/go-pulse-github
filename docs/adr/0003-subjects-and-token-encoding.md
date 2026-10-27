# ADR-0003: Account-scoped subjects with collision-free token encoding

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
