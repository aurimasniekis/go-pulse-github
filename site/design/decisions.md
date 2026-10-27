# Decision records

Short architectural decision records for the trade-offs that shape pulse-github. The sources live in
[`docs/adr/`](https://github.com/aurimasniekis/go-pulse-github/tree/main/docs/adr); a change to one of these trade-offs adds
or supersedes a record there.

## ADR-0001: SQLite inbox/outbox with a single active process

<!--@include: ../../docs/adr/0001-sqlite-inbox-outbox.md{2,}-->

## ADR-0002: Deterministic IDs and at-least-once publication

<!--@include: ../../docs/adr/0002-deterministic-ids-at-least-once.md{2,}-->

## ADR-0003: Account-scoped subjects with collision-free token encoding

<!--@include: ../../docs/adr/0003-subjects-and-token-encoding.md{2,}-->

## ADR-0004: ce-time is the occurrence time only when GitHub reports one

<!--@include: ../../docs/adr/0004-time-semantics.md{2,}-->

## ADR-0005: A small REST client instead of go-github

<!--@include: ../../docs/adr/0005-no-go-github.md{2,}-->

## ADR-0006: Sources are not merged; snapshots are not history

<!--@include: ../../docs/adr/0006-sources-not-merged.md{2,}-->

## ADR-0007: Route by verified scope plus explicit shares

<!--@include: ../../docs/adr/0007-routing-explicit-scopes.md{2,}-->

## ADR-0008: Readiness and webhook admission depend on storage, not NATS

<!--@include: ../../docs/adr/0008-readiness-tied-to-storage.md{2,}-->
