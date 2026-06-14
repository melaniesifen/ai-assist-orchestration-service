# Task Breakdown

Update this file as implementation progresses. Check off completed tasks in the same change that implements or verifies them.

Canonical source: `../ai-assist-architecture/implementation-task-breakdown.md`.

Workspace-level tasks listed below are tracked here only as orchestration-service implementation or wiring slices. They do not mark the full cross-repo task complete unless the canonical workspace task list says so.

Relevant LLDs:

- `../ai-assist-architecture/lld-actions-writeback.md`
- `../ai-assist-architecture/lld-session-events-transport.md`
- `../ai-assist-architecture/lld-operations-safety.md`

## Completed Local Bootstrap And Migration

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
- [x] PROVIDER-004: Default provider handoff to platform-owned provider access metadata and use BYO secret references only when explicitly configured.
- [x] Add unit tests for command coordination, policy deny, unsupported providers, action lifecycle, idempotent apply, stale conflict, concurrent apply reservation, typed validation, and safe conflict metadata.
- [x] Document test and coverage commands in `README.md`.
- [x] Ignore local prompts, feedback, coverage output, dependencies, and build artifacts.
- [x] REPO-002: Migrate orchestration service to a Python package layout with equivalent command coordination, proposed-action lifecycle, policy/provider/context/event boundaries, tests, coverage workflow, and repo docs.
- [x] Repo layout: standardize the Python package under `src/ai_assist_orchestration/` and document `PYTHONPATH=src` unittest and compile checks.

## Completed M6 Local Tasks

- [x] M6-T2: Convert provider proposal output into server-owned proposed actions with server-generated IDs.
- [x] M6-T2: Verify approve, reject, duplicate decision, and stale expiry lifecycle transitions.
- [x] M6-T2: Verify tenant, user, session, and resource scoped action authorization.
- [x] M6-T2: Verify the fake encryption boundary and metadata-only action event payloads.
- [x] M6-T2: Publish `action.proposed` and `action.status_changed` calls from orchestration action lifecycle paths.

## Completed M7 Local Tasks

- [x] M7-T3: Verify apply-action requires server-derived identity, scoped session/resource/action references, request metadata, and an idempotency key.
- [x] M7-T3: Reserve apply idempotency before consent, token, target verification, decrypt, or connector mutation dependency calls.
- [x] M7-T3: Verify revoked consent, reconnect-required token status, stale target, payload decrypt failure, and provider write failure paths skip connector mutation where safe.
- [x] M7-T3: Verify successful apply, conflicted apply, failed apply, expired apply, same-key replay, different-key terminal replay, and `action.status_changed` publishing.

## Architecture Tasks Pending

- Approved direction: migrate from the temporary Node.js ESM bootstrap to Python initially. Revisit Java only if long-running workflow throughput or typed domain needs justify it.
- Migration gate: Do not continue broad new feature work until the Python migration is completed or explicitly deferred.
- [ ] REPO-001: Decide final language, runtime, package manager, framework, package layout, migration cost, deployment target, and test strategy for this repo.
- [ ] EVT-001: Add the HTTP command API adapter for command creation, action approval/rejection, and apply-action with server-derived identity, request IDs, correlation IDs, and idempotency keys.
  - [x] Add repo-local, framework-neutral Python HTTP boundary DTOs/adapters for command creation, action approval, action rejection, and apply-action.
  - [x] Propagate request IDs and correlation IDs from the HTTP boundary into action status events.
  - [x] Require an idempotency key for command creation and apply-action at the HTTP boundary.
  - [ ] Align route names, shared request/response schemas, and contract tests with `ai-assist-contracts` once the cross-service HTTP API contracts are available.
- [ ] EVT-004: Wire the injected event publisher to the session events service `SessionEvent` contract and categorize publisher failures without corrupting durable action state.
  - [x] M5-T3: Convert generic provider stream chunks into progress, assistant delta, assistant final, and safe error publish calls.
  - [x] M5-T3: Categorize command event publisher failures as dependency errors with metadata-only details.
  - [ ] Wire published events to the full session-events `SessionEvent` envelope once the cross-repo event bridge is available.
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
- [x] M8-T4.6: Provider handoff tests verify default platform access and optional BYO secret-reference access without raw credentials.
- [ ] Define deployment pipeline checks for contract compatibility, HTTP command routes, environment config, and required dependency endpoints.
- [ ] Add failure-mode validation for event publisher outage, policy dependency failure, provider timeout, KMS decrypt failure, stale action conflicts, and missing consent.

## Future Production Tasks

- [ ] Add richer workflow selection after MVP command/action contracts are stable.
- [ ] Add background or long-running workflow support only when product requirements need it.
- [ ] Add human review queue support only if product scope requires it.
