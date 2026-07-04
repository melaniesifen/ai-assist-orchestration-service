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

`src/ai_assist_orchestration/http_adapter.py` provides a framework-neutral command/action boundary. `src/ai_assist_orchestration/http_runtime.py` wraps that boundary in a stdlib HTTP runtime for the canonical command, approve, reject, and apply-action route shapes. The runtime requires a server-derived auth context, defaulting to trusted upstream headers `X-AI-Assist-Tenant-Id` and `X-AI-Assist-User-Id`, and rejects path/body session or action mismatches before invoking domain services.

Production provider, context, connector, event, persistence, and encryption adapters should implement the injected dependency contracts without logging raw prompts, document text, model responses, secrets, or decrypted action payloads. Durable AWS storage and real cross-service clients are still supplied outside this package.

Proposed action IDs and expirations are server-owned. Callers may request a shorter TTL with `ttlMs`, but actions are capped at the 24-hour MVP maximum.

## Package Layout

- `src/ai_assist_orchestration/`: orchestration domain package.
- `src/ai_assist_orchestration/http_adapter.py`: framework-neutral HTTP command boundary.
- `src/ai_assist_orchestration/http_runtime.py`: dependency-free stdlib HTTP runtime for deployed service wrapping.
- `tests/`: stdlib `unittest` coverage for command coordination and proposed-action lifecycle behavior.
- `pyproject.toml`: Python packaging metadata with no runtime dependencies.

## Task Breakdown

Implementation tasks are tracked in [TASKS.md](TASKS.md). Update the checkboxes there in the same change that implements or verifies a task.

## Testing And Coverage

Run the unit tests:

```sh
PYTHONPATH=src python3 -m unittest discover -s tests
PYTHONPATH=src python3 -m compileall src tests
```

This repo currently has no coverage dependency. If coverage tooling is added later, keep generated reports out of git; `.gitignore` already excludes common Python and JavaScript coverage/build outputs.
