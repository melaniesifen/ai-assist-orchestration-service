export class InMemoryActionStore {
  #actions = new Map();

  create(action) {
    if (this.#actions.has(action.actionId)) {
      throw new Error(`action already exists: ${action.actionId}`);
    }
    this.#actions.set(action.actionId, clone(action));
    return clone(action);
  }

  get(actionId) {
    const action = this.#actions.get(actionId);
    return action ? clone(action) : undefined;
  }

  update(actionId, updater) {
    const current = this.get(actionId);
    if (!current) {
      return undefined;
    }
    const next = updater(current);
    this.#actions.set(actionId, clone(next));
    return clone(next);
  }

  reserveApply(actionId, idempotencyKey, startedAt) {
    const current = this.get(actionId);
    if (!current) {
      return { kind: "NOT_FOUND" };
    }
    if (current.applyResult && current.idempotencyKey === idempotencyKey) {
      return { kind: "REPLAY", applyResult: current.applyResult };
    }
    if (current.applyLock) {
      return {
        kind: current.applyLock.idempotencyKey === idempotencyKey
          ? "IN_PROGRESS"
          : "IN_PROGRESS_DIFFERENT_KEY",
        action: current
      };
    }
    if (current.status !== "APPROVED") {
      return { kind: "NOT_APPROVED", action: current };
    }
    const reserved = {
      ...current,
      idempotencyKey,
      applyLock: { idempotencyKey, startedAt },
      updatedAt: startedAt
    };
    this.#actions.set(actionId, clone(reserved));
    return { kind: "RESERVED", action: clone(reserved) };
  }

  completeApply(actionId, idempotencyKey, patch) {
    return this.update(actionId, (current) => {
      if (current.applyLock?.idempotencyKey !== idempotencyKey) {
        return current;
      }
      const next = {
        ...current,
        ...patch
      };
      delete next.applyLock;
      return next;
    });
  }
}

function clone(value) {
  return structuredClone(value);
}
