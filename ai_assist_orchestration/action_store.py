from __future__ import annotations

from copy import deepcopy
from threading import RLock
from typing import Callable


class InMemoryActionStore:
    def __init__(self) -> None:
        self._actions: dict[str, dict] = {}
        self._lock = RLock()

    def create(self, action: dict) -> dict:
        with self._lock:
            action_id = action["actionId"]
            if action_id in self._actions:
                raise ValueError(f"action already exists: {action_id}")
            self._actions[action_id] = deepcopy(action)
            return deepcopy(action)

    def get(self, action_id: str) -> dict | None:
        with self._lock:
            action = self._actions.get(action_id)
            return deepcopy(action) if action is not None else None

    def update(self, action_id: str, updater: Callable[[dict], dict]) -> dict | None:
        with self._lock:
            current = self.get(action_id)
            if current is None:
                return None
            next_action = updater(current)
            self._actions[action_id] = deepcopy(next_action)
            return deepcopy(next_action)

    def transition(
        self,
        action_id: str,
        *,
        allowed_statuses: set[str],
        patch: dict,
        reject_if_apply_locked: bool = False,
    ) -> dict:
        with self._lock:
            current = self.get(action_id)
            if current is None:
                return {"kind": "NOT_FOUND"}
            if reject_if_apply_locked and current.get("applyLock"):
                return {"kind": "APPLY_IN_PROGRESS", "action": current}
            if current.get("status") not in allowed_statuses:
                return {"kind": "STATUS_MISMATCH", "action": current}
            next_action = {**current, **patch}
            self._actions[action_id] = deepcopy(next_action)
            return {"kind": "UPDATED", "action": deepcopy(next_action)}

    def reserve_apply(self, action_id: str, idempotency_key: str, started_at: str) -> dict:
        with self._lock:
            current = self.get(action_id)
            if current is None:
                return {"kind": "NOT_FOUND"}
            if current.get("applyResult") and current.get("idempotencyKey") == idempotency_key:
                return {"kind": "REPLAY", "applyResult": current["applyResult"]}
            if current.get("applyLock"):
                return {
                    "kind": "IN_PROGRESS" if current["applyLock"]["idempotencyKey"] == idempotency_key else "IN_PROGRESS_DIFFERENT_KEY",
                    "action": current,
                }
            if current.get("status") != "APPROVED":
                return {"kind": "NOT_APPROVED", "action": current}
            reserved = {
                **current,
                "idempotencyKey": idempotency_key,
                "applyLock": {"idempotencyKey": idempotency_key, "startedAt": started_at},
                "updatedAt": started_at,
            }
            self._actions[action_id] = deepcopy(reserved)
            return {"kind": "RESERVED", "action": deepcopy(reserved)}

    def complete_apply(self, action_id: str, idempotency_key: str, patch: dict) -> dict | None:
        def updater(current: dict) -> dict:
            if current.get("applyLock", {}).get("idempotencyKey") != idempotency_key:
                return current
            next_action = {**current, **patch}
            next_action.pop("applyLock", None)
            return next_action

        return self.update(action_id, updater)
