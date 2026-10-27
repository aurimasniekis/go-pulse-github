# ADR-0005: A small REST client instead of go-github

**Status:** accepted

**Context.** The service needs:
- original JSON kept byte for byte, including fields unknown to SDK types;
- precise control over conditional requests, `X-Poll-Interval`, rate-limit headers and pagination;
- refusal of any URL outside the configured API origin (SSRF and credential safety).

**Decision.** Use a small `net/http` client (`internal/githubapi`) that returns raw bodies, plus stdlib RS256 for App
JWTs. Payloads are never decoded into typed SDK structs.

**Consequences.** Less code to trust and full header control. Each endpoint used is explicit and covered by tests
against a fake API. The cost is maintaining that endpoint list.
