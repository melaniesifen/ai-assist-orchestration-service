from __future__ import annotations

from datetime import timedelta
from typing import Any, Callable
from uuid import uuid4

from .action_model import ACTION_STATUS, DEFAULT_ACTION_TTL_MS, is_expired, is_known_action_status, is_terminal_action_status
from .async_utils import maybe_await
from .errors import authorization_error, conflict_error, dependency_error, validation_error
from .time_utils import epoch_ms, isoformat_z, utc_now
from .validation import assert_identity, assert_ownership, require_non_blank_string, require_object

SAFE_CONFLICT_DETAIL_KEYS = frozenset(
    {
        "connectorCode",
        "currentRevision",
        "expectedRevision",
        "reasonCode",
        "resourceId",
        "targetAnchorId",
        "targetRangeId",
    }
)
ACTION_EVENT_PROPOSED_REASON = "ACTION_PROPOSED"
ACTION_EVENT_EXPIRED_REASON = "ACTION_EXPIRED"
DEFAULT_ACTION_SUMMARY_BY_TYPE = {
    "replace_text": "Replace text proposal",
    "insert_text": "Insert text proposal",
}
SUPPORTED_ACTION_TYPES = frozenset(DEFAULT_ACTION_SUMMARY_BY_TYPE)


class ActionService:
    def __init__(
        self,
        *,
        action_store: Any,
        connector: Any,
        event_publisher: Any,
        consent_service: Any,
        payload_vault: Any,
        token_service: Any | None = None,
        clock: Callable[[], Any] = utc_now,
        id_generator: Callable[[str], str] | None = None,
    ) -> None:
        if not action_store or not connector or not event_publisher or not consent_service or not payload_vault:
            raise TypeError("action_store, connector, event_publisher, consent_service, and payload_vault are required")
        self.action_store = action_store
        self.connector = connector
        self.event_publisher = event_publisher
        self.consent_service = consent_service
        self.payload_vault = payload_vault
        self.token_service = token_service
        self.clock = clock
        self.id_generator = id_generator or (lambda prefix: f"{prefix}_{uuid4().hex}")

    async def create_proposed_action(self, identity: dict, input_data: dict) -> dict:
        assert_identity(identity)
        require_object(input_data, "input")
        now = self.clock()
        action_id = self.id_generator("action")
        expires_at = isoformat_z(now + timedelta(milliseconds=resolve_action_ttl_ms(input_data.get("ttlMs"))))
        encrypted_payload = await maybe_await(self.payload_vault.encrypt(input_data.get("payload")))
        action = {
            "actionId": action_id,
            "tenantId": identity["tenantId"],
            "userId": identity["userId"],
            "sessionId": require_non_blank_string(input_data.get("sessionId"), "input.sessionId"),
            "provider": require_non_blank_string(input_data.get("provider"), "input.provider"),
            "resourceId": require_non_blank_string(input_data.get("resourceId"), "input.resourceId"),
            "resourceRevision": require_non_blank_string(input_data.get("resourceRevision"), "input.resourceRevision"),
            "targetAnchor": input_data.get("targetAnchor"),
            "targetRange": input_data.get("targetRange"),
            "originalTextHash": require_non_blank_string(input_data.get("originalTextHash"), "input.originalTextHash"),
            "actionType": require_non_blank_string(input_data.get("actionType"), "input.actionType"),
            "encryptedPayload": encrypted_payload,
            "status": ACTION_STATUS.PROPOSED.value,
            "idempotencyKey": None,
            "createdAt": isoformat_z(now),
            "updatedAt": isoformat_z(now),
            "expiresAt": expires_at,
            "summary": require_non_blank_string(
                input_data.get("summary") or DEFAULT_ACTION_SUMMARY_BY_TYPE.get(input_data.get("actionType")),
                "input.summary",
            ),
        }
        require_supported_action_type(action["actionType"])
        require_action_target(action)
        if self.action_store.get(action_id):
            raise conflict_error("ACTION_ID_COLLISION", "Generated action ID already exists", {"actionId": action_id})
        created = self.action_store.create(action)
        await maybe_await(
            self.event_publisher.publish(
                {
                    "type": "action.proposed",
                    "actionId": created["actionId"],
                    "sessionId": created["sessionId"],
                    "actionType": created["actionType"],
                    "resourceRef": {"provider": created["provider"], "resourceId": created["resourceId"]},
                    "summary": created["summary"],
                    "expiresAt": created["expiresAt"],
                    **request_event_context(input_data),
                }
            )
        )
        return created

    async def approve_action(self, identity: dict, input_data: dict) -> dict:
        input_data = require_object(input_data, "input")
        event_context = request_event_context(input_data)
        action = self._get_authorized_action(identity, input_data)
        require_action_status(action)
        expired = await self._expire_action_if_needed(action, event_context)
        if expired["expired"]:
            return expired["action"]
        action = expired["action"]
        if action["status"] == ACTION_STATUS.APPROVED.value:
            return action
        if action["status"] != ACTION_STATUS.PROPOSED.value:
            raise conflict_error(
                "ACTION_NOT_APPROVABLE",
                "Action cannot be approved from its current status",
                {"actionId": input_data.get("actionId"), "status": action["status"]},
            )
        now = isoformat_z(self.clock())
        result = self.action_store.transition(
            action["actionId"],
            allowed_statuses={ACTION_STATUS.PROPOSED.value},
            patch={"status": ACTION_STATUS.APPROVED.value, "approvedAt": now, "updatedAt": now},
        )
        updated = require_transition_updated(result, "ACTION_NOT_APPROVABLE", "Action cannot be approved from its current status")
        await self._publish_status(updated, action["status"], updated["status"], "USER_APPROVED", event_context)
        return updated

    async def reject_action(self, identity: dict, input_data: dict) -> dict:
        input_data = require_object(input_data, "input")
        event_context = request_event_context(input_data)
        action = self._get_authorized_action(identity, input_data)
        require_action_status(action)
        expired = await self._expire_action_if_needed(action, event_context)
        if expired["expired"]:
            return expired["action"]
        action = expired["action"]
        if is_terminal_action_status(action["status"]):
            return action
        reason_code = input_data.get("reasonCode", "USER_REJECTED")
        now = isoformat_z(self.clock())
        result = self.action_store.transition(
            action["actionId"],
            allowed_statuses={ACTION_STATUS.PROPOSED.value, ACTION_STATUS.APPROVED.value},
            reject_if_apply_locked=True,
            patch={
                "status": ACTION_STATUS.REJECTED.value,
                "rejectedAt": now,
                "updatedAt": now,
                "reasonCode": reason_code,
            },
        )
        if result["kind"] == "APPLY_IN_PROGRESS":
            raise conflict_error(
                "ACTION_APPLY_IN_PROGRESS",
                "Action apply is already in progress",
                {"actionId": action["actionId"], "status": result["action"]["status"]},
            )
        if result["kind"] == "STATUS_MISMATCH" and is_terminal_or_raise(result["action"]):
            return result["action"]
        updated = require_transition_updated(result, "ACTION_NOT_REJECTABLE", "Action cannot be rejected from its current status")
        await self._publish_status(updated, action["status"], updated["status"], reason_code, event_context)
        return updated

    async def apply_action(self, identity: dict, input_data: dict) -> dict:
        input_data = require_object(input_data, "input")
        event_context = request_event_context(input_data)
        idempotency_key = require_non_blank_string(input_data.get("idempotencyKey"), "idempotencyKey")
        action = self._get_authorized_action(identity, input_data)
        require_action_status(action)
        if action.get("applyResult") and action.get("idempotencyKey") == idempotency_key:
            return action["applyResult"]
        if is_terminal_action_status(action["status"]):
            return terminal_result(action)
        if action["status"] != ACTION_STATUS.APPROVED.value:
            raise conflict_error(
                "ACTION_NOT_APPROVED",
                "Action must be approved before apply",
                {"actionId": action["actionId"], "status": action["status"]},
            )
        expired = await self._expire_action_if_needed(action, event_context, idempotency_key=idempotency_key)
        if expired["expired"]:
            return terminal_result(expired["action"])
        action = expired["action"]

        reservation = self.action_store.reserve_apply(action["actionId"], idempotency_key, isoformat_z(self.clock()))
        if reservation["kind"] == "REPLAY":
            return reservation["applyResult"]
        if reservation["kind"] in {"IN_PROGRESS", "IN_PROGRESS_DIFFERENT_KEY"}:
            raise conflict_error(
                "ACTION_APPLY_IN_PROGRESS",
                "Action apply is already in progress",
                {"actionId": action["actionId"], "status": action["status"]},
            )
        if reservation["kind"] != "RESERVED":
            raise conflict_error(
                "ACTION_NOT_APPLIABLE",
                "Action could not be reserved for apply",
                {"actionId": action["actionId"], "status": action["status"]},
            )
        reserved_action = reservation["action"]

        try:
            consent = await maybe_await(
                self.consent_service.validate_apply_consent(
                    {
                        "tenantId": reserved_action["tenantId"],
                        "userId": reserved_action["userId"],
                        "sessionId": reserved_action["sessionId"],
                        "resourceId": reserved_action["resourceId"],
                        "actionType": reserved_action["actionType"],
                    }
                )
            )
        except Exception as error:
            failed = self._complete_reserved_apply_failure(reserved_action, idempotency_key, "CONSENT_CHECK_FAILED")
            await self._publish_status(failed, action["status"], failed["status"], failed["reasonCode"], event_context)
            raise dependency_error("CONSENT_CHECK_FAILED", "Consent status could not be validated", {"actionId": action["actionId"]}) from error
        if not consent or not consent.get("allowed"):
            conflicted = self._complete_reserved_apply(
                reserved_action,
                idempotency_key,
                ACTION_STATUS.CONFLICTED.value,
                {
                    "conflictedAt": isoformat_z(self.clock()),
                    "conflictDetails": to_safe_conflict_details((consent or {}).get("conflictDetails", {})),
                    "reasonCode": (consent or {}).get("reasonCode", "CONSENT_REQUIRED"),
                },
            )
            await self._publish_status(conflicted, action["status"], conflicted["status"], conflicted["reasonCode"], event_context)
            return terminal_result(conflicted)

        try:
            token_status = await self._validate_token_status(reserved_action)
        except Exception as error:
            failed = self._complete_reserved_apply_failure(reserved_action, idempotency_key, "TOKEN_STATUS_CHECK_FAILED")
            await self._publish_status(failed, action["status"], failed["status"], failed["reasonCode"], event_context)
            raise dependency_error("TOKEN_STATUS_CHECK_FAILED", "Token status could not be validated", {"actionId": action["actionId"]}) from error
        if not token_status.get("valid"):
            failed = self._complete_reserved_apply(
                reserved_action,
                idempotency_key,
                ACTION_STATUS.FAILED.value,
                {
                    "failedAt": isoformat_z(self.clock()),
                    "reasonCode": token_status.get("reasonCode", "RECONNECT_REQUIRED"),
                    "failureCode": token_status.get("reasonCode", "RECONNECT_REQUIRED"),
                },
            )
            await self._publish_status(failed, action["status"], failed["status"], failed["reasonCode"], event_context)
            return terminal_result(failed)

        try:
            validation = await maybe_await(self.connector.validate_target(reserved_action))
        except Exception as error:
            failed = self._complete_reserved_apply_failure(reserved_action, idempotency_key, "TARGET_VALIDATION_FAILED")
            await self._publish_status(failed, action["status"], failed["status"], failed["reasonCode"], event_context)
            raise dependency_error("TARGET_VALIDATION_FAILED", "Mutation target could not be validated", {"actionId": action["actionId"]}) from error
        if not validation or not validation.get("valid"):
            conflicted = self._complete_reserved_apply(
                reserved_action,
                idempotency_key,
                ACTION_STATUS.CONFLICTED.value,
                {
                    "conflictedAt": isoformat_z(self.clock()),
                    "conflictDetails": to_safe_conflict_details((validation or {}).get("conflictDetails", {})),
                    "reasonCode": (validation or {}).get("reasonCode", "TARGET_CONFLICT"),
                },
            )
            await self._publish_status(conflicted, action["status"], conflicted["status"], conflicted["reasonCode"], event_context)
            return terminal_result(conflicted)

        try:
            payload = await maybe_await(self.payload_vault.decrypt(reserved_action["encryptedPayload"]))
        except Exception as error:
            failed = self._complete_reserved_apply(
                reserved_action,
                idempotency_key,
                ACTION_STATUS.FAILED.value,
                {
                    "failedAt": isoformat_z(self.clock()),
                    "reasonCode": "ACTION_PAYLOAD_DECRYPT_FAILED",
                    "failureCode": "ACTION_PAYLOAD_DECRYPT_FAILED",
                },
            )
            await self._publish_status(failed, action["status"], failed["status"], failed["reasonCode"], event_context)
            raise dependency_error("ACTION_PAYLOAD_DECRYPT_FAILED", "Action payload could not be decrypted", {"actionId": action["actionId"]}) from error
        try:
            apply_result = await maybe_await(
                self.connector.apply_action(
                    {
                        "action": reserved_action,
                        "verifiedTarget": validation.get("verifiedTarget"),
                        "payload": payload,
                        "idempotencyKey": idempotency_key,
                    }
                )
            )
        except Exception as error:
            failed = self.action_store.complete_apply(
                action["actionId"],
                idempotency_key,
                {
                    "status": ACTION_STATUS.FAILED.value,
                    "failedAt": isoformat_z(self.clock()),
                    "reasonCode": "PROVIDER_WRITE_FAILED",
                    "failureCode": "PROVIDER_WRITE_FAILED",
                    "updatedAt": isoformat_z(self.clock()),
                },
            )
            await self._publish_status(failed, action["status"], failed["status"], "PROVIDER_WRITE_FAILED", event_context)
            raise dependency_error("PROVIDER_WRITE_FAILED", "Provider write failed", {"actionId": action["actionId"]}) from error

        applied = self.action_store.complete_apply(
            action["actionId"],
            idempotency_key,
            {
                "status": ACTION_STATUS.APPLIED.value,
                "appliedAt": isoformat_z(self.clock()),
                "providerOperationId": apply_result.get("providerOperationId"),
                "reasonCode": "APPLY_SUCCEEDED",
                "idempotencyKey": idempotency_key,
                "applyResult": {
                    "status": ACTION_STATUS.APPLIED.value,
                    "actionId": action["actionId"],
                    "providerOperationId": apply_result.get("providerOperationId"),
                },
                "updatedAt": isoformat_z(self.clock()),
            },
        )
        await self._publish_status(applied, action["status"], applied["status"], "APPLY_SUCCEEDED", event_context)
        return applied["applyResult"]

    def _complete_reserved_apply_failure(self, action: dict, idempotency_key: str, reason_code: str) -> dict:
        return self._complete_reserved_apply(
            action,
            idempotency_key,
            ACTION_STATUS.FAILED.value,
            {
                "failedAt": isoformat_z(self.clock()),
                "reasonCode": reason_code,
                "failureCode": reason_code,
            },
        )

    async def _validate_token_status(self, action: dict) -> dict:
        if not self.token_service:
            return {"valid": True}
        result = await maybe_await(
            self.token_service.validate_apply_token(
                {
                    "tenantId": action["tenantId"],
                    "userId": action["userId"],
                    "provider": action["provider"],
                    "resourceId": action["resourceId"],
                    "sessionId": action["sessionId"],
                }
            )
        )
        return result if isinstance(result, dict) else {"valid": False, "reasonCode": "RECONNECT_REQUIRED"}

    def _complete_reserved_apply(self, action: dict, idempotency_key: str, status: str, patch: dict) -> dict:
        apply_result = {
            "status": status,
            "actionId": action["actionId"],
            "reasonCode": patch.get("reasonCode"),
            "conflictDetails": patch.get("conflictDetails"),
            "providerOperationId": patch.get("providerOperationId"),
        }
        completed = self.action_store.complete_apply(
            action["actionId"],
            idempotency_key,
            {
                **patch,
                "status": status,
                "idempotencyKey": idempotency_key,
                "applyResult": apply_result,
                "updatedAt": isoformat_z(self.clock()),
            },
        )
        if completed and completed.get("status") == status:
            return completed
        current = self.action_store.get(action["actionId"])
        if current and is_terminal_action_status(current["status"]):
            return current
        raise conflict_error(
            "ACTION_NOT_APPLIABLE",
            "Action could not be completed for apply",
            {"actionId": action["actionId"], "status": (current or action).get("status")},
        )

    def _get_authorized_action(self, identity: dict, input_data: dict) -> dict:
        assert_identity(identity)
        action_id = require_non_blank_string(input_data.get("actionId"), "actionId")
        action = self.action_store.get(action_id)
        if not action:
            raise validation_error("ACTION_NOT_FOUND", "Action was not found", {"actionId": action_id})
        if not assert_ownership(identity, action):
            raise authorization_error("ACTION_FORBIDDEN", "Action is not accessible to this identity", {"actionId": action_id})
        expected_session_id = require_non_blank_string(input_data.get("sessionId"), "input.sessionId")
        expected_resource_id = require_non_blank_string(input_data.get("resourceId"), "input.resourceId")
        if action.get("sessionId") != expected_session_id:
            raise authorization_error("ACTION_FORBIDDEN", "Action is not accessible for this session", {"actionId": action_id})
        if action.get("resourceId") != expected_resource_id:
            raise authorization_error("ACTION_FORBIDDEN", "Action is not accessible for this resource", {"actionId": action_id})
        return action

    async def _expire_action_if_needed(
        self,
        action: dict,
        event_context: dict | None = None,
        *,
        idempotency_key: str | None = None,
    ) -> dict:
        if action["status"] == ACTION_STATUS.EXPIRED.value:
            return {"expired": True, "action": action}
        if is_terminal_action_status(action["status"]) or not is_expired(action, epoch_ms(self.clock())):
            return {"expired": False, "action": action}
        now = isoformat_z(self.clock())
        result = self.action_store.transition(
            action["actionId"],
            allowed_statuses={ACTION_STATUS.PROPOSED.value, ACTION_STATUS.APPROVED.value},
            patch={
                "status": ACTION_STATUS.EXPIRED.value,
                "expiredAt": now,
                "updatedAt": now,
                "reasonCode": ACTION_EVENT_EXPIRED_REASON,
                **({"idempotencyKey": idempotency_key} if idempotency_key is not None else {}),
            },
        )
        if result["kind"] == "UPDATED":
            await self._publish_status(result["action"], action["status"], result["action"]["status"], ACTION_EVENT_EXPIRED_REASON, event_context)
            return {"expired": True, "action": result["action"]}
        if result["kind"] == "STATUS_MISMATCH":
            require_action_status(result["action"])
            return {"expired": is_terminal_action_status(result["action"]["status"]), "action": result["action"]}
        raise validation_error("ACTION_NOT_FOUND", "Action was not found", {"actionId": action["actionId"]})

    def _transition(self, action: dict, status: str, patch: dict) -> dict:
        result = self.action_store.transition(
            action["actionId"],
            allowed_statuses={ACTION_STATUS.APPROVED.value},
            patch={**patch, "status": status, "updatedAt": isoformat_z(self.clock())},
        )
        if result["kind"] == "UPDATED":
            return {"transitioned": True, "action": result["action"]}
        if result["kind"] == "STATUS_MISMATCH":
            require_action_status(result["action"])
            if is_terminal_action_status(result["action"]["status"]):
                return {"transitioned": False, "action": result["action"]}
            raise conflict_error(
                "ACTION_NOT_APPLIABLE",
                "Action could not be transitioned for apply",
                {"actionId": action["actionId"], "status": result["action"]["status"]},
            )
        raise validation_error("ACTION_NOT_FOUND", "Action was not found", {"actionId": action["actionId"]})

    async def _publish_status(
        self,
        action: dict,
        previous_status: str | None,
        status: str,
        reason_code: str,
        event_context: dict | None = None,
    ) -> None:
        await maybe_await(
            self.event_publisher.publish(
                {
                    "type": "action.status_changed",
                    "actionId": action["actionId"],
                    "sessionId": action["sessionId"],
                    "previousStatus": previous_status,
                    "status": status,
                    "reasonCode": reason_code,
                    **(event_context or {}),
                }
            )
        )


def create_action_service(**kwargs: Any) -> ActionService:
    return ActionService(**kwargs)


def request_event_context(input_data: dict) -> dict:
    event_context: dict[str, str] = {}
    if isinstance(input_data.get("requestId"), str) and input_data["requestId"].strip():
        event_context["requestId"] = input_data["requestId"]
    if isinstance(input_data.get("correlationId"), str) and input_data["correlationId"].strip():
        event_context["correlationId"] = input_data["correlationId"]
    return event_context


def require_action_target(action: dict) -> None:
    has_anchor = is_valid_target_anchor(action.get("targetAnchor"))
    has_range = is_valid_target_range(action.get("targetRange"))
    if not has_anchor and not has_range:
        raise validation_error(
            "INVALID_ACTION_TARGET",
            "Action targetAnchor or targetRange is required and must be well formed",
            {"fieldName": "input.targetAnchor"},
        )


def require_supported_action_type(action_type: str) -> None:
    if action_type not in SUPPORTED_ACTION_TYPES:
        raise validation_error(
            "UNSUPPORTED_ACTION_TYPE",
            "Action type is not supported for proposed actions",
            {"actionType": action_type},
        )


def is_valid_target_anchor(value: Any) -> bool:
    return isinstance(value, dict) and any(isinstance(item, str) and item.strip() for item in value.values())


def is_valid_target_range(value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    start = value.get("start")
    end = value.get("end")
    return isinstance(start, int) and isinstance(end, int) and start >= 0 and end > start


def terminal_result(action: dict) -> dict:
    if action.get("applyResult"):
        return action["applyResult"]
    return {
        "status": action["status"],
        "actionId": action["actionId"],
        "reasonCode": action.get("reasonCode"),
        "conflictDetails": action.get("conflictDetails"),
        "providerOperationId": action.get("providerOperationId"),
    }


def resolve_action_ttl_ms(ttl_ms: Any) -> int:
    if ttl_ms is None:
        return DEFAULT_ACTION_TTL_MS
    if not isinstance(ttl_ms, (int, float)) or ttl_ms <= 0:
        raise validation_error("INVALID_ACTION_TTL", "Action TTL must be positive", {"maxTtlMs": DEFAULT_ACTION_TTL_MS})
    return min(int(ttl_ms), DEFAULT_ACTION_TTL_MS)


def require_transition_updated(result: dict, code: str, message: str) -> dict:
    if result["kind"] == "UPDATED":
        return result["action"]
    if result["kind"] == "STATUS_MISMATCH":
        require_action_status(result["action"])
        raise conflict_error(code, message, {"actionId": result["action"]["actionId"], "status": result["action"]["status"]})
    raise validation_error("ACTION_NOT_FOUND", "Action was not found")


def require_action_status(action: dict) -> str:
    status = action.get("status")
    if not is_known_action_status(status):
        raise validation_error(
            "INVALID_ACTION_STATUS",
            "Action has an invalid status",
            {"actionId": action.get("actionId"), "status": status},
        )
    return status


def is_terminal_or_raise(action: dict) -> bool:
    require_action_status(action)
    return is_terminal_action_status(action["status"])


def to_safe_conflict_details(details: dict) -> dict:
    return {key: value for key, value in details.items() if key in SAFE_CONFLICT_DETAIL_KEYS and is_safe_metadata_value(value)}


def is_safe_metadata_value(value: Any) -> bool:
    return value is None or isinstance(value, (str, int, float, bool))
