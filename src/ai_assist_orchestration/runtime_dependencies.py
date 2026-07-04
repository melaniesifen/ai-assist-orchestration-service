from __future__ import annotations

import json
import os
import tempfile
from copy import deepcopy
from pathlib import Path
from threading import RLock
from typing import Any, Callable

from .errors import dependency_error

DEFAULT_ACTION_STORE_PATH = "/tmp/ai-assist-orchestration/actions.json"


class JsonFileActionStore:
    """Small durable action-store adapter for the dependency-light dogfood runtime."""

    def __init__(self, path: str | os.PathLike[str] = DEFAULT_ACTION_STORE_PATH) -> None:
        self.path = Path(path)
        self._lock = RLock()

    def create(self, action: dict) -> dict:
        with self._lock:
            actions = self._read_actions()
            action_id = action["actionId"]
            if action_id in actions:
                raise ValueError(f"action already exists: {action_id}")
            actions[action_id] = deepcopy(action)
            self._write_actions(actions)
            return deepcopy(action)

    def get(self, action_id: str) -> dict | None:
        with self._lock:
            action = self._read_actions().get(action_id)
            return deepcopy(action) if action is not None else None

    def list_for_session(self, *, tenant_id: str, user_id: str, session_id: str) -> list[dict]:
        with self._lock:
            actions = [
                deepcopy(action)
                for action in self._read_actions().values()
                if action.get("tenantId") == tenant_id
                and action.get("userId") == user_id
                and action.get("sessionId") == session_id
            ]
        return sorted(actions, key=lambda action: (action.get("createdAt", ""), action.get("actionId", "")))

    def update(self, action_id: str, updater: Callable[[dict], dict]) -> dict | None:
        with self._lock:
            actions = self._read_actions()
            current = actions.get(action_id)
            if current is None:
                return None
            next_action = updater(deepcopy(current))
            actions[action_id] = deepcopy(next_action)
            self._write_actions(actions)
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
            actions = self._read_actions()
            current = actions.get(action_id)
            if current is None:
                return {"kind": "NOT_FOUND"}
            current = deepcopy(current)
            if reject_if_apply_locked and current.get("applyLock"):
                return {"kind": "APPLY_IN_PROGRESS", "action": current}
            if current.get("status") not in allowed_statuses:
                return {"kind": "STATUS_MISMATCH", "action": current}
            next_action = {**current, **patch}
            actions[action_id] = deepcopy(next_action)
            self._write_actions(actions)
            return {"kind": "UPDATED", "action": deepcopy(next_action)}

    def reserve_apply(self, action_id: str, idempotency_key: str, started_at: str) -> dict:
        with self._lock:
            actions = self._read_actions()
            current = actions.get(action_id)
            if current is None:
                return {"kind": "NOT_FOUND"}
            current = deepcopy(current)
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
            actions[action_id] = deepcopy(reserved)
            self._write_actions(actions)
            return {"kind": "RESERVED", "action": deepcopy(reserved)}

    def complete_apply(self, action_id: str, idempotency_key: str, patch: dict) -> dict | None:
        def updater(current: dict) -> dict:
            if current.get("applyLock", {}).get("idempotencyKey") != idempotency_key:
                return current
            next_action = {**current, **patch}
            next_action.pop("applyLock", None)
            return next_action

        return self.update(action_id, updater)

    def _read_actions(self) -> dict[str, dict]:
        if not self.path.exists():
            return {}
        try:
            with self.path.open("r", encoding="utf-8") as handle:
                data = json.load(handle)
        except json.JSONDecodeError as error:
            raise dependency_error(
                "ACTION_STORE_MALFORMED",
                "Action store could not be read",
                {"operation": "action_store_read", "dependencyStatus": "malformed"},
            ) from error
        if not isinstance(data, dict):
            raise dependency_error(
                "ACTION_STORE_MALFORMED",
                "Action store could not be read",
                {"operation": "action_store_read", "dependencyStatus": "malformed"},
            )
        return {key: value for key, value in data.items() if isinstance(key, str) and isinstance(value, dict)}

    def _write_actions(self, actions: dict[str, dict]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, temp_name = tempfile.mkstemp(prefix=f".{self.path.name}.", dir=str(self.path.parent), text=True)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(actions, handle, sort_keys=True, separators=(",", ":"))
            os.replace(temp_name, self.path)
        finally:
            if os.path.exists(temp_name):
                os.unlink(temp_name)


class UnconfiguredPayloadVault:
    def encrypt(self, _payload: Any) -> dict:
        raise dependency_error(
            "ACTION_PAYLOAD_VAULT_UNAVAILABLE",
            "Encrypted action payload storage is not configured",
            {"operation": "payload_encrypt", "dependencyStatus": "not_configured"},
        )

    def decrypt(self, _encrypted_payload: dict) -> Any:
        raise dependency_error(
            "ACTION_PAYLOAD_VAULT_UNAVAILABLE",
            "Encrypted action payload storage is not configured",
            {"operation": "payload_decrypt", "dependencyStatus": "not_configured"},
        )


class AllowingConsentService:
    async def validate_apply_consent(self, _request: dict) -> dict:
        return {"allowed": True}


class NoopEventPublisher:
    async def publish(self, _event: dict) -> None:
        return None


class UnconfiguredConnector:
    async def validate_target(self, action: dict) -> dict:
        raise dependency_error(
            "CONNECTOR_TARGET_VALIDATION_UNAVAILABLE",
            "Connector target validation is not configured",
            {
                "operation": "connector_validate_target",
                "dependencyStatus": "not_configured",
                "actionId": action.get("actionId"),
                "resourceId": action.get("resourceId"),
            },
        )

    async def apply_action(self, request: dict) -> dict:
        action = request.get("action") if isinstance(request, dict) else {}
        raise dependency_error(
            "CONNECTOR_APPLY_UNAVAILABLE",
            "Connector apply is not configured",
            {
                "operation": "connector_apply",
                "dependencyStatus": "not_configured",
                "actionId": action.get("actionId") if isinstance(action, dict) else None,
            },
        )
