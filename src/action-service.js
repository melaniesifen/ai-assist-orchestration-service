import {
  ACTION_STATUS,
  DEFAULT_ACTION_TTL_MS,
  isExpired,
  isTerminalActionStatus
} from "./action-model.js";
import {
  authorizationError,
  conflictError,
  dependencyError,
  validationError
} from "./errors.js";
import {
  assertIdentity,
  assertOwnership,
  requireNonBlankString,
  requireObject
} from "./validation.js";

export function createActionService({
  actionStore,
  connector,
  eventPublisher,
  consentService,
  payloadVault,
  clock = () => new Date(),
  idGenerator = () => crypto.randomUUID()
}) {
  if (!actionStore || !connector || !eventPublisher || !consentService || !payloadVault) {
    throw new TypeError("actionStore, connector, eventPublisher, consentService, and payloadVault are required");
  }

  async function createProposedAction(identity, input) {
    assertIdentity(identity);
    requireObject(input, "input");
    const now = clock();
    const actionId = idGenerator("action");
    const expiresAt = new Date(now.getTime() + resolveActionTtlMs(input.ttlMs)).toISOString();
    const encryptedPayload = await payloadVault.encrypt(input.payload);
    const action = {
      actionId,
      tenantId: identity.tenantId,
      userId: identity.userId,
      sessionId: requireNonBlankString(input.sessionId, "input.sessionId"),
      provider: requireNonBlankString(input.provider, "input.provider"),
      resourceId: requireNonBlankString(input.resourceId, "input.resourceId"),
      resourceRevision: requireNonBlankString(input.resourceRevision, "input.resourceRevision"),
      targetAnchor: input.targetAnchor ?? null,
      targetRange: input.targetRange ?? null,
      originalTextHash: requireNonBlankString(input.originalTextHash, "input.originalTextHash"),
      actionType: requireNonBlankString(input.actionType, "input.actionType"),
      encryptedPayload,
      status: ACTION_STATUS.PROPOSED,
      idempotencyKey: null,
      createdAt: now.toISOString(),
      updatedAt: now.toISOString(),
      expiresAt,
      summary: requireNonBlankString(input.summary, "input.summary")
    };

    if (actionStore.get(actionId)) {
      throw conflictError("ACTION_ID_COLLISION", "Generated action ID already exists", { actionId });
    }
    const created = actionStore.create(action);
    await publishStatus(created, null, ACTION_STATUS.PROPOSED, "ACTION_PROPOSED");
    await eventPublisher.publish({
      type: "action.proposed",
      actionId: created.actionId,
      sessionId: created.sessionId,
      resourceId: created.resourceId,
      actionType: created.actionType,
      summary: created.summary,
      expiresAt: created.expiresAt
    });
    return created;
  }

  async function approveAction(identity, { actionId }) {
    const action = getOwnedAction(identity, actionId);
    if (action.status === ACTION_STATUS.APPROVED) {
      return action;
    }
    if (action.status !== ACTION_STATUS.PROPOSED) {
      throw conflictError("ACTION_NOT_APPROVABLE", "Action cannot be approved from its current status", {
        actionId,
        status: action.status
      });
    }

    const now = clock().toISOString();
    const updated = actionStore.update(actionId, (current) => ({
      ...current,
      status: ACTION_STATUS.APPROVED,
      approvedAt: now,
      updatedAt: now
    }));
    await publishStatus(updated, action.status, updated.status, "USER_APPROVED");
    return updated;
  }

  async function rejectAction(identity, { actionId, reasonCode = "USER_REJECTED" }) {
    const action = getOwnedAction(identity, actionId);
    if (isTerminalActionStatus(action.status)) {
      return action;
    }
    const now = clock().toISOString();
    const updated = actionStore.update(actionId, (current) => ({
      ...current,
      status: ACTION_STATUS.REJECTED,
      rejectedAt: now,
      updatedAt: now,
      reasonCode
    }));
    await publishStatus(updated, action.status, updated.status, reasonCode);
    return updated;
  }

  async function applyAction(identity, { actionId, idempotencyKey }) {
    requireNonBlankString(idempotencyKey, "idempotencyKey");
    const action = getOwnedAction(identity, actionId);

    if (action.applyResult && action.idempotencyKey === idempotencyKey) {
      return action.applyResult;
    }
    if (isTerminalActionStatus(action.status)) {
      return terminalResult(action);
    }
    if (action.status !== ACTION_STATUS.APPROVED) {
      throw conflictError("ACTION_NOT_APPROVED", "Action must be approved before apply", {
        actionId,
        status: action.status
      });
    }

    const nowMs = clock().getTime();
    if (isExpired(action, nowMs)) {
      const expired = transition(action, ACTION_STATUS.EXPIRED, {
        expiredAt: clock().toISOString(),
        reasonCode: "ACTION_EXPIRED",
        idempotencyKey
      });
      await publishStatus(expired, action.status, expired.status, "ACTION_EXPIRED");
      return terminalResult(expired);
    }

    const consent = await consentService.validateApplyConsent({
      tenantId: action.tenantId,
      userId: action.userId,
      sessionId: action.sessionId,
      resourceId: action.resourceId,
      actionType: action.actionType
    });
    if (!consent?.allowed) {
      throw conflictError("CONSENT_REQUIRED", "Consent is required before applying this action", {
        actionId,
        reasonCode: consent?.reasonCode ?? "CONSENT_REQUIRED"
      });
    }

    const validation = await connector.validateTarget(action);
    if (!validation?.valid) {
      const conflicted = transition(action, ACTION_STATUS.CONFLICTED, {
        conflictedAt: clock().toISOString(),
        conflictDetails: toSafeConflictDetails(validation?.conflictDetails ?? {}),
        reasonCode: validation?.reasonCode ?? "TARGET_CONFLICT",
        idempotencyKey
      });
      await publishStatus(conflicted, action.status, conflicted.status, conflicted.reasonCode);
      return terminalResult(conflicted);
    }

    const reservation = actionStore.reserveApply(actionId, idempotencyKey, clock().toISOString());
    if (reservation.kind === "REPLAY") {
      return reservation.applyResult;
    }
    if (reservation.kind === "IN_PROGRESS" || reservation.kind === "IN_PROGRESS_DIFFERENT_KEY") {
      throw conflictError("ACTION_APPLY_IN_PROGRESS", "Action apply is already in progress", {
        actionId,
        status: action.status
      });
    }
    if (reservation.kind !== "RESERVED") {
      throw conflictError("ACTION_NOT_APPLIABLE", "Action could not be reserved for apply", {
        actionId,
        status: action.status
      });
    }

    const reservedAction = reservation.action;
    const payload = await payloadVault.decrypt(reservedAction.encryptedPayload);
    let applyResult;
    try {
      applyResult = await connector.applyAction({
        action: reservedAction,
        verifiedTarget: validation.verifiedTarget,
        payload,
        idempotencyKey
      });
    } catch (error) {
      const failed = actionStore.completeApply(actionId, idempotencyKey, {
        status: ACTION_STATUS.FAILED,
        failedAt: clock().toISOString(),
        reasonCode: "PROVIDER_WRITE_FAILED",
        failureCode: "PROVIDER_WRITE_FAILED",
        updatedAt: clock().toISOString()
      });
      await publishStatus(failed, action.status, failed.status, "PROVIDER_WRITE_FAILED");
      throw dependencyError("PROVIDER_WRITE_FAILED", "Provider write failed", { actionId });
    }

    const applied = actionStore.completeApply(actionId, idempotencyKey, {
      status: ACTION_STATUS.APPLIED,
      appliedAt: clock().toISOString(),
      providerOperationId: applyResult.providerOperationId,
      reasonCode: "APPLY_SUCCEEDED",
      idempotencyKey,
      applyResult: {
        status: ACTION_STATUS.APPLIED,
        actionId,
        providerOperationId: applyResult.providerOperationId
      },
      updatedAt: clock().toISOString()
    });
    await publishStatus(applied, action.status, applied.status, "APPLY_SUCCEEDED");
    return applied.applyResult;
  }

  function getOwnedAction(identity, actionId) {
    assertIdentity(identity);
    requireNonBlankString(actionId, "actionId");
    const action = actionStore.get(actionId);
    if (!action) {
      throw validationError("ACTION_NOT_FOUND", "Action was not found", { actionId });
    }
    if (!assertOwnership(identity, action)) {
      throw authorizationError("ACTION_FORBIDDEN", "Action is not accessible to this identity", { actionId });
    }
    return action;
  }

  function transition(action, status, patch) {
    const updated = actionStore.update(action.actionId, (current) => ({
      ...current,
      ...patch,
      status,
      updatedAt: clock().toISOString()
    }));
    return updated;
  }

  async function publishStatus(action, previousStatus, status, reasonCode) {
    await eventPublisher.publish({
      type: "action.status_changed",
      actionId: action.actionId,
      sessionId: action.sessionId,
      previousStatus,
      status,
      reasonCode
    });
  }

  return {
    createProposedAction,
    approveAction,
    rejectAction,
    applyAction
  };
}

function terminalResult(action) {
  if (action.applyResult) {
    return action.applyResult;
  }
  return {
    status: action.status,
    actionId: action.actionId,
    reasonCode: action.reasonCode,
    conflictDetails: action.conflictDetails,
    providerOperationId: action.providerOperationId
  };
}

function resolveActionTtlMs(ttlMs) {
  if (ttlMs === undefined) {
    return DEFAULT_ACTION_TTL_MS;
  }
  if (!Number.isFinite(ttlMs) || ttlMs <= 0) {
    throw validationError("INVALID_ACTION_TTL", "Action TTL must be positive", {
      maxTtlMs: DEFAULT_ACTION_TTL_MS
    });
  }
  return Math.min(ttlMs, DEFAULT_ACTION_TTL_MS);
}

function toSafeConflictDetails(details) {
  const allowed = [
    "connectorCode",
    "currentRevision",
    "expectedRevision",
    "reasonCode",
    "resourceId",
    "targetAnchorId",
    "targetRangeId"
  ];
  return Object.fromEntries(
    Object.entries(details)
      .filter(([key, value]) => allowed.includes(key) && isSafeMetadataValue(value))
  );
}

function isSafeMetadataValue(value) {
  return value === null
    || typeof value === "string"
    || typeof value === "number"
    || typeof value === "boolean";
}
