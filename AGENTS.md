# AGENTS.md

## Repo Purpose

`ai-assist-orchestration-service` coordinates user commands across context, policy, provider adapters, proposed actions, write-back, and session event publishing.

## Agent Instructions

- Read `README.md`, `ai-assist-platform-context.md`, `../ai-assist-architecture/lld-actions-writeback.md`, `../ai-assist-architecture/lld-session-events-transport.md`, and `../ai-assist-architecture/lld-operations-safety.md` before changing behavior.
- Use injected dependencies for context, provider adapters, action storage, event publishing, clocks, ID generation, and policy decisions.
- Keep provider-specific API details out of orchestration.
- Keep connector-specific mutation behavior out of orchestration; call connector abstractions.
- Proposed actions must be durable, TTL-bound, encrypted at storage boundaries, and scoped to tenant/user/session/resource.
- Apply-action must be authenticated, authorized, idempotent, and conflict-safe.
- Add tests for command validation, policy deny, provider failures, action lifecycle, duplicate apply, stale conflict no-mutation behavior, and event publication.

## Commands

- Run tests with `node --test`.
- `npm` may not be available in this environment; prefer the direct Node command.

## Review Notes

Before committing, review for partial state transitions, duplicate writes, prompt/content logging, missing event emission, and stale-data overwrites.

## Commit Messages

All commits in this repo must use this format:

```text
docs/feat/fix/(or another appropriate type): title of change

problem: <description of problem>
solution: <description of solution>
impact: <impact of this change>
reference: <reference to this change in the docs if applicable>
```
