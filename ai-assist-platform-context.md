# AI Assist Platform Context

## Project Summary

This project defines a browser-native AI workflow assistant platform. The platform lets users bring an AI assistant into an existing browser-based workflow, choose how much context the assistant can access, discuss that context, and explicitly approve actions back into the source system.

Google Docs is the first connector and MVP proof point, but the product is not a Google Docs add-on. The core architecture is connector-first, provider-agnostic, and designed to support future workflows such as Notion, GitHub pull requests, Gmail, Obsidian, local Markdown, Word/OneDrive, browser extensions, and screen-aware clients.

## Product Direction

The preferred MVP direction is hosted convenience mode with user-provided credentials:

- The product hosts the app infrastructure.
- Users authorize access to their own Google account.
- Users provide their own OpenAI or Anthropic API key, stored as a short-lived encrypted session secret.
- Amazon Bedrock can be added as an optional hosted or self-hosted provider path.
- Self-hosted deployment remains a future-supported path through IaC such as CDK.

The platform should feel like "ChatGPT inside the workflow" for the first Google Docs use case, while keeping reusable platform boundaries intact.

## MVP Scope

The MVP should support:

- User login and server-derived `tenantId` / `userId`.
- Google OAuth connection for the first connector.
- User-provided OpenAI or Anthropic key validation.
- KMS-encrypted `SessionSecrets` with an 8-hour TTL.
- Google Docs resource selection.
- Context modes `SELECTION` and `ACTIVE_RESOURCE`.
- Assistant command creation through authenticated HTTP APIs.
- Assistant response streaming over SSE.
- Generic, short-lived, encrypted `ProposedActions`.
- User approval or rejection of proposed actions.
- Safe Google Docs replace/insert write-back through idempotent HTTP apply-action.
- Conflict handling for stale document ranges, revision mismatches, and original-text hash mismatches.
- API Gateway/WAF rate limits where practical.
- Metadata-only logs that exclude secrets and raw user content.

Deferred until later:

- WebSocket transport.
- `VISIBLE_REGION`, `WORKSPACE`, and `SCREEN` context modes.
- Public/untrusted user launch.
- Automated illicit-content classification.
- Persistent remembered provider keys.
- Long-term conversation or content storage.
- App-level DynamoDB rate-limit counters.

## Core Architecture Decisions

- Use separate service repos from day one.
- Put shared schemas and contracts in `ai-assist-contracts`.
- Use HTTP command APIs for setup, commands, approvals, and durable mutations.
- Use SSE for MVP one-way assistant/session event streaming.
- Treat WebSocket as a future transport adapter over the same `SessionEvent` contract.
- Keep durable writes on authenticated, idempotent HTTP even after WebSocket exists.
- Keep Google Docs behavior isolated to the Google Docs connector and first writing workflow.
- Keep provider-specific model behavior isolated to provider adapters.
- Keep use-case workflow decisions in orchestration, not in generic platform services.
- Treat `tenantId` as first-class from day one. Do not make `tenantId = userId` a permanent contract.
- Enforce context consent and context mode server-side.
- Require connector-verified provenance for write-back. Client-supplied text may inform model context, but cannot authorize mutation.
- Do not log or retain raw prompts, document text, selected text, model responses, screenshots, OCR, accessibility-tree captures, provider keys, OAuth tokens, or decrypted action payloads by default.

## Service Boundaries

Expected repos/services:

- `ai-assist-web`: onboarding, dashboard, chat/session UI, provider setup, resource picker, SSE client, proposed-action review UI.
- `ai-assist-auth-service`: product auth, tenant/user identity, Google OAuth lifecycle, token encryption/storage coordination, authZ helpers.
- `ai-assist-secrets-service`: short-lived provider API keys, KMS encryption/decryption, fingerprints, TTL, validation status.
- `ai-assist-session-events-service`: transport-neutral `SessionEvent` delivery, SSE stream lifecycle, future WebSocket adapter boundary.
- `ai-assist-orchestration-service`: command handling, workflow selection, context coordination, prompt construction, provider calls, proposed actions, apply orchestration, event publishing.
- `ai-assist-context-service`: context modes, consent grants, normalized context, provenance, truncation/redaction hooks.
- `ai-assist-google-docs-adapter`: Google Docs/Drive API integration, resource listing, context reads, verified anchors/ranges/revisions, safe replace/insert.
- `ai-assist-openai-adapter`: OpenAI credential validation, generation, streaming normalization, usage/error normalization.
- `ai-assist-anthropic-adapter`: Anthropic credential validation, Claude generation, streaming normalization, usage/error normalization.
- `ai-assist-policy-service`: MVP allow/stub policy boundary; future abuse checks and blocked-request metadata.
- `ai-assist-infra`: AWS IaC, HTTP APIs, SSE-capable routes, DynamoDB, KMS, IAM, WAF/API Gateway throttling, CloudWatch.
- `ai-assist-contracts`: versioned schemas for APIs, events, errors, context, consent, secrets, proposed actions, providers, connectors, and policy decisions.

## Main Data Concepts

- `Tenants`: personal or organization tenant records.
- `TenantUsers`: tenant membership and role records.
- `Users`: user profile and default tenant metadata.
- `OAuthTokens`: encrypted Google OAuth access/refresh tokens scoped by tenant, user, provider.
- `SessionSecrets`: encrypted short-lived provider API keys, default TTL 8 hours.
- `ContextConsentGrants`: consent for provider, resource/workspace, context mode, scopes, status, and expiry.
- `ResourceSessions`: active sessions tied to a provider resource.
- `ProposedActions`: short-lived server-authoritative proposed mutations, default TTL 24 hours, sensitive payload encrypted.
- `SessionEvents`: optional short-lived event metadata/encrypted payloads if replay becomes necessary.
- `ConversationEvents`: optional only; avoid by default for MVP.

## Main Flows

First-run flow:

1. User logs in.
2. Backend derives `tenantId` and `userId`.
3. User connects Google through OAuth.
4. Auth service encrypts and stores Google tokens.
5. User enters OpenAI or Anthropic key.
6. Secrets service validates and stores it as an encrypted `SessionSecrets` record with an 8-hour TTL.
7. User selects a Google Doc and starts a resource session.

Ask flow:

1. User chooses `SELECTION` or `ACTIVE_RESOURCE`.
2. Frontend sends an authenticated HTTP command.
3. Orchestration validates ownership, consent, and effective context mode.
4. Context service and Google Docs adapter return normalized, provenance-tagged context.
5. Orchestration builds the prompt without logging raw content.
6. Provider adapter calls the selected model provider using a decrypted secret reference.
7. Orchestration publishes `progress`, `assistant.delta`, and `assistant.final` events through the session events service.
8. Frontend renders the stream over SSE.

Write-back flow:

1. Orchestration creates an encrypted `ProposedActions` record.
2. User reviews and approves or rejects the action over HTTP.
3. Apply-action requires an idempotency key.
4. Backend validates ownership, consent, action status, expiry, resource, revision/range, original-text hash, OAuth token, and decrypted payload.
5. Google Docs adapter applies only safe replace/insert operations.
6. Stale or unverifiable targets become `CONFLICTED` with no mutation.
7. Session events report action status changes.

## Security and Privacy Invariants

- Backend derives identity; client-supplied identity fields are never trusted.
- Services authorize every reference such as session ID, resource ID, action ID, and grant ID.
- OAuth tokens and provider keys are encrypted with KMS.
- Provider keys never appear in frontend code, browser extensions, Tampermonkey scripts, logs, or API responses.
- Session secrets expire at read time even if database TTL cleanup has not run.
- Write-back is impossible without connector-verified target metadata.
- Apply-action is idempotent and must not duplicate provider writes.
- Logs are metadata-only.
- Public launch requires policy checks, app-level tenant-aware rate limits, suspension/abuse workflow, and stronger operational controls.

## Implementation Sequence

Recommended order:

1. Contracts and repo/service boundaries.
2. Auth, tenancy, and secrets.
3. Consent and context model.
4. Google Docs read flow.
5. Provider adapter validation and generation.
6. HTTP command API and SSE stream.
7. Proposed actions.
8. Safe apply-action.
9. Rate limits and logging.
10. End-to-end MVP validation.

The task breakdown currently groups work as:

- Architecture/spec cleanup.
- Auth, tenancy, and secrets.
- Context and connector contracts.
- Session events and transport.
- Proposed actions and write-back.
- Google Docs adapter.
- AI provider adapters.
- Rate limiting and operations.
- Safety/privacy.
- Infra.
- Documentation/LLDs.
- End-to-end MVP validation.

## Current Documentation Set

Repo docs read for this context:

- `ai-assist-architecture/ai-workflow-assistant-platform-architecture-spec.md`
- `ai-assist-architecture/microservices-responsibilities.md`
- `ai-assist-architecture/implementation-task-breakdown.md`
- `ai-assist-architecture/lld-auth-secrets-tenancy.md`
- `ai-assist-architecture/lld-context-connectors.md`
- `ai-assist-architecture/lld-session-events-transport.md`
- `ai-assist-architecture/lld-actions-writeback.md`
- `ai-assist-architecture/lld-operations-safety.md`

## Open Questions

- Should the first product surface be standalone web app, Chrome extension, or Google Docs sidebar?
- What exact Google OAuth scopes are required for the first MVP?
- How much document context should `ACTIVE_RESOURCE` fetch automatically?
- What consent UX should back `ContextConsentGrants`, especially future workspace and screen modes?
- Which client should implement `SCREEN` first: desktop companion or browser extension?
- Should conversation history be stored at all?
- What metadata retention period should logs use?
- Which second connector should prove extensibility first: local Markdown, Notion, GitHub PRs, Gmail, or Obsidian?
- After OpenAI and Anthropic direct adapters, should the next provider be Bedrock-hosted Claude, Gemini, or local models?

