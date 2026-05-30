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

The package is dependency-light Node.js ESM and exports pure domain services with injected collaborators. The included in-memory action repository is for local tests and future adapter development only.

Future HTTP adapters should derive `tenantId` and `userId` from authenticated identity before calling these services. Future provider, context, connector, event, and encryption adapters should implement the injected dependency contracts without logging raw prompts, document text, model responses, secrets, or decrypted action payloads.

Proposed action IDs and expirations are server-owned. Callers may request a shorter TTL with `ttlMs`, but actions are capped at the 24-hour MVP maximum.

## Tests

Run:

```sh
node --test
```

The test suite uses `node:test` and installs no packages.
