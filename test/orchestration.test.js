import assert from "node:assert/strict";
import test from "node:test";

import {
  ACTION_STATUS,
  InMemoryActionStore,
  OrchestrationError,
  createActionService,
  createCommandService
} from "../src/index.js";

const IDENTITY = Object.freeze({ tenantId: "tenant_001", userId: "user_001" });
const NOW = new Date("2026-05-29T00:00:00.000Z");

test("coordinates assistant commands through injected context, provider, policy, and event dependencies", async () => {
  const events = [];
  const commandService = createCommandService({
    clock: () => NOW,
    policyService: {
      evaluate: async () => ({ decision: "ALLOW", decisionId: "pol_001" })
    },
    contextService: {
      resolveContext: async (request) => ({
        authorized: true,
        request,
        provenance: { source: "connector_verified" },
        metadata: { resourceId: request.resourceId }
      })
    },
    providerRegistry: new Map([
      ["openai", {
        generate: async ({ prompt }) => ({
          messageId: "msg_001",
          deltas: [`answer to ${prompt.promptId}`, " done"],
          finishReason: "stop",
          usage: { inputTokens: 4, outputTokens: 5 }
        })
      }]
    ]),
    promptBuilder: {
      buildPrompt: ({ command }) => ({ promptId: command.commandId })
    },
    eventPublisher: {
      publish: async (event) => events.push(event)
    }
  });

  const result = await commandService.runAssistantCommand(IDENTITY, {
    commandId: "cmd_001",
    requestId: "req_001",
    correlationId: "corr_001",
    sessionId: "session_001",
    provider: "openai",
    resourceId: "doc_001",
    contextMode: "SELECTION",
    secretRef: "secret_ref_001"
  });

  assert.deepEqual(result, {
    messageId: "msg_001",
    finishReason: "stop",
    provider: "openai"
  });
  assert.deepEqual(events.map((event) => event.type), [
    "progress",
    "progress",
    "assistant.delta",
    "assistant.delta",
    "assistant.final"
  ]);
});

test("blocks commands when the policy dependency denies them", async () => {
  const commandService = createCommandService({
    policyService: {
      evaluate: async () => ({ decision: "BLOCK", decisionId: "pol_002", reasonCode: "PUBLIC_POLICY_NOT_CONFIGURED" })
    },
    contextService: { resolveContext: async () => ({ authorized: true }) },
    providerRegistry: new Map(),
    promptBuilder: { buildPrompt: () => ({}) },
    eventPublisher: { publish: async () => {} }
  });

  await assert.rejects(
    () => commandService.runAssistantCommand(IDENTITY, {
      requestId: "req_001",
      correlationId: "corr_001",
      sessionId: "session_001",
      provider: "openai"
    }),
    (error) => error instanceof OrchestrationError && error.category === "POLICY"
  );
});

test("rejects unsupported providers before resolving context", async () => {
  let contextCallCount = 0;
  const events = [];
  const commandService = createCommandService({
    policyService: {
      evaluate: async () => ({ decision: "ALLOW", decisionId: "pol_003" })
    },
    contextService: {
      resolveContext: async () => {
        contextCallCount += 1;
        return { authorized: true };
      }
    },
    providerRegistry: new Map(),
    promptBuilder: { buildPrompt: () => ({}) },
    eventPublisher: { publish: async (event) => events.push(event) }
  });

  await assert.rejects(
    () => commandService.runAssistantCommand(IDENTITY, {
      requestId: "req_unsupported",
      correlationId: "corr_unsupported",
      sessionId: "session_001",
      provider: "unknown"
    }),
    (error) => error instanceof OrchestrationError
      && error.category === "VALIDATION"
      && error.code === "PROVIDER_UNSUPPORTED"
  );
  assert.equal(contextCallCount, 0);
  assert.deepEqual(events, []);
});

test("returns typed validation errors for malformed public inputs", async () => {
  const commandService = createCommandService({
    policyService: { evaluate: async () => ({ decision: "ALLOW" }) },
    contextService: { resolveContext: async () => ({ authorized: true }) },
    providerRegistry: new Map(),
    promptBuilder: { buildPrompt: () => ({}) },
    eventPublisher: { publish: async () => {} }
  });

  await assert.rejects(
    () => commandService.runAssistantCommand(IDENTITY, {
      requestId: " ",
      correlationId: "corr_001",
      sessionId: "session_001",
      provider: "openai"
    }),
    (error) => error instanceof OrchestrationError
      && error.category === "VALIDATION"
      && error.statusCode === 400
      && error.metadata.fieldName === "command.requestId"
  );
});

test("creates, approves, applies, and idempotently replays a proposed action", async () => {
  const events = [];
  let writeCount = 0;
  const actionStore = new InMemoryActionStore();
  const service = createTestActionService({
    actionStore,
    events,
    connector: {
      validateTarget: async () => ({ valid: true, verifiedTarget: { revision: "rev_001" } }),
      applyAction: async () => {
        writeCount += 1;
        return { providerOperationId: "google_op_001" };
      }
    }
  });

  const proposed = await service.createProposedAction(IDENTITY, baseActionInput());
  assert.equal(proposed.status, ACTION_STATUS.PROPOSED);
  assert.equal(proposed.actionId, "action_001");

  const approved = await service.approveAction(IDENTITY, { actionId: proposed.actionId });
  assert.equal(approved.status, ACTION_STATUS.APPROVED);

  const firstApply = await service.applyAction(IDENTITY, {
    actionId: proposed.actionId,
    idempotencyKey: "idem_001"
  });
  const replayedApply = await service.applyAction(IDENTITY, {
    actionId: proposed.actionId,
    idempotencyKey: "idem_001"
  });
  const terminalApply = await service.applyAction(IDENTITY, {
    actionId: proposed.actionId,
    idempotencyKey: "idem_002"
  });

  assert.equal(writeCount, 1);
  assert.deepEqual(replayedApply, firstApply);
  assert.equal(terminalApply.status, ACTION_STATUS.APPLIED);
  assert.equal(actionStore.get(proposed.actionId).status, ACTION_STATUS.APPLIED);
  assert(events.some((event) => event.type === "action.status_changed" && event.status === ACTION_STATUS.APPLIED));
});

test("uses server-owned action IDs and clamps caller-supplied TTLs to the maximum", async () => {
  const actionStore = new InMemoryActionStore();
  const service = createTestActionService({
    actionStore,
    connector: {
      validateTarget: async () => ({ valid: true, verifiedTarget: {} }),
      applyAction: async () => ({ providerOperationId: "google_op_001" })
    }
  });

  const proposed = await service.createProposedAction(IDENTITY, {
    ...baseActionInput(),
    actionId: "caller_chosen",
    expiresAt: "2099-01-01T00:00:00.000Z",
    ttlMs: 99 * 24 * 60 * 60 * 1000
  });

  assert.equal(proposed.actionId, "action_001");
  assert.equal(proposed.expiresAt, "2026-05-30T00:00:00.000Z");
});

test("marks stale action state as conflicted and performs no provider mutation", async () => {
  let writeCount = 0;
  const actionStore = new InMemoryActionStore();
  const service = createTestActionService({
    actionStore,
    connector: {
      validateTarget: async () => ({
        valid: false,
        reasonCode: "ORIGINAL_TEXT_HASH_MISMATCH",
        conflictDetails: {
          resourceId: "doc_001",
          expectedRevision: "rev_001",
          currentRevision: "rev_002",
          currentText: "must not be retained",
          expectedText: "must not be retained either"
        }
      }),
      applyAction: async () => {
        writeCount += 1;
        return { providerOperationId: "should_not_happen" };
      }
    }
  });

  const proposed = await service.createProposedAction(IDENTITY, baseActionInput());
  await service.approveAction(IDENTITY, { actionId: proposed.actionId });

  const result = await service.applyAction(IDENTITY, {
    actionId: proposed.actionId,
    idempotencyKey: "idem_conflict"
  });

  assert.equal(writeCount, 0);
  assert.equal(result.status, ACTION_STATUS.CONFLICTED);
  assert.equal(result.reasonCode, "ORIGINAL_TEXT_HASH_MISMATCH");
  assert.equal(JSON.stringify(result).includes("must not be retained"), false);
  assert.equal(JSON.stringify(actionStore.get(proposed.actionId)).includes("must not be retained"), false);
  assert.equal(actionStore.get(proposed.actionId).status, ACTION_STATUS.CONFLICTED);
});

test("reserves apply before provider mutation so concurrent applies do not double-write", async () => {
  let writeCount = 0;
  const gate = createDeferred();
  const actionStore = new InMemoryActionStore();
  const service = createTestActionService({
    actionStore,
    connector: {
      validateTarget: async () => ({ valid: true, verifiedTarget: { revision: "rev_001" } }),
      applyAction: async () => {
        writeCount += 1;
        await gate.promise;
        return { providerOperationId: "google_op_001" };
      }
    }
  });

  const proposed = await service.createProposedAction(IDENTITY, baseActionInput());
  await service.approveAction(IDENTITY, { actionId: proposed.actionId });

  const first = service.applyAction(IDENTITY, {
    actionId: proposed.actionId,
    idempotencyKey: "idem_concurrent"
  });
  const second = service.applyAction(IDENTITY, {
    actionId: proposed.actionId,
    idempotencyKey: "idem_concurrent"
  });

  await assert.rejects(second, (error) => error instanceof OrchestrationError
    && error.code === "ACTION_APPLY_IN_PROGRESS");
  gate.resolve();
  await first;

  assert.equal(writeCount, 1);
});

function createTestActionService({ actionStore, connector, events = [] }) {
  return createActionService({
    actionStore,
    connector,
    clock: () => NOW,
    idGenerator: () => "action_001",
    eventPublisher: {
      publish: async (event) => events.push(event)
    },
    consentService: {
      validateApplyConsent: async () => ({ allowed: true })
    },
    payloadVault: {
      encrypt: async (payload) => ({ ciphertextRef: "encrypted_payload_001", payload }),
      decrypt: async (encryptedPayload) => encryptedPayload.payload
    }
  });
}

function baseActionInput() {
  return {
    sessionId: "session_001",
    provider: "google_docs",
    resourceId: "doc_001",
    resourceRevision: "rev_001",
    targetAnchor: { kind: "range_name", id: "anchor_001" },
    targetRange: { start: 1, end: 4 },
    originalTextHash: "sha256:abc",
    actionType: "replace_text",
    payload: { replacementText: "new text" },
    summary: "Replace selected text"
  };
}

function createDeferred() {
  let resolve;
  let reject;
  const promise = new Promise((innerResolve, innerReject) => {
    resolve = innerResolve;
    reject = innerReject;
  });
  return { promise, resolve, reject };
}
