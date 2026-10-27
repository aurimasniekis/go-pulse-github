# AGENTS.md — rules for agents working on pulse-github

pulse-github is a headless Go service that collects GitHub activity and publishes it to Pulse (NATS JetStream). These
rules apply to every agent session here.

## Before working

1. Read the documentation website sources in `site/`, in particular `site/reference/coverage.md`,
   `site/reference/event-contract.md` and `site/design/architecture.md`, and the decision records in
   `site/design/decisions.md`.
2. Follow [CONTRIBUTING.md](CONTRIBUTING.md) for commands, test style and rules.

## Contract rules

- **Do not fabricate GitHub or Pulse capabilities.** Verify GitHub behaviour against docs.github.com and Pulse
  behaviour against the pinned go-pulse version. Record what was verified, and what was not, in the docs.
- **Never silently weaken security or delivery guarantees** (ack after commit, stable publication IDs, origin checks,
  least-privilege grants). Changes need a decision record in `site/design/decisions.md`.
- **Never mark unfinished work done.** Distinguish implemented, tested and externally verified.
- Subjects, `ce-type` and the payload schema are a public contract (`site/reference/event-contract.md`). Breaking
  changes need a new `…v2` type.
- Never commit secrets, tokens, private keys, `.creds` or private infrastructure details. Use `example.com` hosts.

## Git workflow

- Never commit, push, tag or open pull requests unless explicitly asked.
- When asked to commit: stage only relevant files by explicit path after reviewing `git status` and the diff, and
  write a Conventional Commits message (the `/cc` skill).
