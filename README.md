# ai-assist-orchestration-service

Domain-layer orchestration primitives for the AI Assist Platform.

## Boundary

This service owns:

- Command coordination across context, policy, provider, action, and event dependencies.
- Proposed-action lifecycle rules.
- Approval, rejection, apply, conflict, and idempotency domain behavior.
- Typed domain errors that adapters can map to HTTP responses.

This service does not own:

- Product authentication.
- Provider SDK calls.
- Google Docs API calls.
- SSE transport mechanics.
- KMS encryption implementation.

## Current Implementation

The package is dependency-light Python and exports pure domain services with injected collaborators. Runtime code uses only the Python standard library. The included in-memory action repository is for local tests and future adapter development only.

Future HTTP adapters should derive `tenantId` and `userId` from authenticated identity before calling these services. Future provider, context, connector, event, and encryption adapters should implement the injected dependency contracts without logging raw prompts, document text, model responses, secrets, or decrypted action payloads.

Proposed action IDs and expirations are server-owned. Callers may request a shorter TTL with `ttlMs`, but actions are capped at the 24-hour MVP maximum.

## Package Layout

- `ai_assist_orchestration/`: orchestration domain package.
- `tests/`: stdlib `unittest` coverage for command coordination and proposed-action lifecycle behavior.
- `pyproject.toml`: Python packaging metadata with no runtime dependencies.

## Task Breakdown

Implementation tasks are tracked in [TASKS.md](TASKS.md). Update the checkboxes there in the same change that implements or verifies a task.

## Testing And Coverage

Run the unit tests:

```sh
python3 -m unittest discover -s tests
```

This repo currently has no coverage dependency. If coverage tooling is added later, keep generated reports out of git; `.gitignore` already excludes common Python and JavaScript coverage/build outputs.
