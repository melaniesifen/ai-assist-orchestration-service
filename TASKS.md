# Task Breakdown

Update this file as implementation progresses. Check off completed tasks in the same change that implements or verifies them.

Canonical source: `../ai-assist-architecture/implementation-task-breakdown.md`.

Relevant LLDs:

- `../ai-assist-architecture/lld-actions-writeback.md`
- `../ai-assist-architecture/lld-session-events-transport.md`
- `../ai-assist-architecture/lld-operations-safety.md`

## Completed Local Bootstrap

- [x] Create dependency-light Node.js ESM package for orchestration domain code.
- [x] Implement injected command coordination across policy, context, provider, prompt, and event dependencies.
- [x] Implement typed orchestration errors for validation, authorization, policy, conflict, and dependency failures.
- [x] ACTION-001: Implement local `ProposedActions` model fields, server-owned action IDs, 24-hour TTL cap, and payload-vault encryption boundary.
- [x] ACTION-002: Implement proposed action status lifecycle for `PROPOSED`, `APPROVED`, `APPLIED`, `REJECTED`, `EXPIRED`, `CONFLICTED`, and `FAILED`.
- [x] ACTION-003: Implement domain approve/reject behavior with ownership checks, deterministic repeat handling, and action status event publication.
- [x] ACTION-004: Implement domain apply-action validation, idempotency reservation, duplicate-apply replay, and stale-conflict no-mutation behavior.
- [x] EVT-004: Use an injected event publisher boundary for progress, assistant, action proposed, and action status events.
- [x] SAFE-002: Call an injected policy decision boundary before provider generation.
- [x] PROVIDER-004: Require explicit provider selection through the provider registry; unsupported providers fail with a typed validation error.
- [x] Add unit tests for command coordination, policy deny, unsupported providers, action lifecycle, idempotent apply, stale conflict, concurrent apply reservation, typed validation, and safe conflict metadata.
- [x] Document test and coverage commands in `README.md`.
- [x] Ignore local prompts, feedback, coverage output, dependencies, and build artifacts.

## Architecture Tasks Pending

- [ ] REPO-001: Decide final language, runtime, package manager, framework, package layout, migration cost, deployment target, and test strategy for this repo.
- [ ] EVT-001: Add the HTTP command API adapter for command creation, action approval/rejection, and apply-action with server-derived identity, request IDs, correlation IDs, and idempotency keys.
- [ ] EVT-004: Wire the injected event publisher to the session events service `SessionEvent` contract and categorize publisher failures without corrupting durable action state.
- [ ] ACTION-001: Replace in-memory action storage with durable encrypted `ProposedActions` persistence scoped by tenant/user/session/resource.
- [ ] ACTION-003: Expose authenticated HTTP approve/reject endpoints and contract tests against shared action schemas.
- [ ] ACTION-004: Expose authenticated HTTP apply-action endpoint and wire real context consent, connector target verification, connector write-back, and idempotency persistence.
- [ ] ACTION-006: Add explicit handling and tests for revoked Google OAuth, KMS decrypt failure, provider write uncertainty, expired action recovery, and missing consent behavior.
- [ ] OPS-003: Add metadata-only logging around orchestration commands, provider calls, action transitions, and dependency failures.
- [ ] OPS-004: Emit metrics for provider failures, token usage, KMS failures, action conflicts, apply failures, and dependency latency.
- [ ] SAFE-002: Preserve the policy extension point when adding production provider and proposed-action workflows; do not bypass policy before provider calls or action creation.
- [ ] Add integration tests for context adapter, policy service, provider adapter, action storage, and session event publisher coordination.
- [ ] E2E-002: Validate read/context/generate path through real context and provider adapters with SSE output.
- [ ] E2E-003: Validate proposed action creation, review, approval/rejection, ownership protection, encrypted payload storage, and action proposed events.
- [ ] E2E-004: Validate safe apply-action against Google Docs adapter with consent, revision/range/hash checks, idempotency, and conflict handling.
- [ ] E2E-005: Verify orchestration logs exclude raw prompts, document text, model responses, secrets, and action payload plaintext.
- [ ] Define deployment pipeline checks for contract compatibility, HTTP command routes, environment config, and required dependency endpoints.
- [ ] Add failure-mode validation for event publisher outage, policy dependency failure, provider timeout, KMS decrypt failure, stale action conflicts, and missing consent.

## Future Production Tasks

- [ ] Add richer workflow selection after MVP command/action contracts are stable.
- [ ] Add background or long-running workflow support only when product requirements need it.
- [ ] Add human review queue support only if product scope requires it.
