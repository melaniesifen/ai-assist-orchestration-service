from __future__ import annotations

import asyncio
import unittest
from datetime import datetime, timezone

from ai_assist_orchestration import (
    ACTION_STATUS,
    InMemoryActionStore,
    OrchestrationError,
    create_action_service,
    create_command_service,
    create_http_command_boundary,
)

IDENTITY = {"tenantId": "tenant_001", "userId": "user_001"}
NOW = datetime(2026, 5, 29, tzinfo=timezone.utc)


class OrchestrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_http_create_command_derives_identity_and_request_metadata_server_side(self) -> None:
        command_service = RecordingCommandService({"messageId": "msg_001", "finishReason": "stop", "provider": "openai"})
        boundary = create_http_command_boundary(
            command_service=command_service,
            action_service=RecordingActionService(),
            request_id_generator=lambda: "req_generated",
            correlation_id_generator=lambda: "corr_generated",
        )

        response = await boundary.create_command(
            {
                "auth": IDENTITY,
                "headers": {"Idempotency-Key": "idem_command_001", "X-Request-Id": "req_http_001"},
                "body": {
                    "tenantId": "attacker_tenant",
                    "userId": "attacker_user",
                    "sessionId": "session_001",
                    "provider": "openai",
                    "resourceId": "doc_001",
                    "contextMode": "ACTIVE_RESOURCE",
                },
            }
        )

        self.assertEqual(response["statusCode"], 202)
        self.assertEqual(command_service.identity, IDENTITY)
        self.assertNotIn("tenantId", command_service.command)
        self.assertNotIn("userId", command_service.command)
        self.assertEqual(command_service.command["requestId"], "req_http_001")
        self.assertEqual(command_service.command["correlationId"], "corr_generated")
        self.assertEqual(command_service.command["idempotencyKey"], "idem_command_001")
        self.assertEqual(response["headers"]["X-Request-Id"], "req_http_001")
        self.assertEqual(response["headers"]["X-Correlation-Id"], "corr_generated")

    async def test_http_create_command_maps_missing_idempotency_to_safe_error_response(self) -> None:
        boundary = create_http_command_boundary(
            command_service=RecordingCommandService({}),
            action_service=RecordingActionService(),
            request_id_generator=lambda: "req_generated",
            correlation_id_generator=lambda: "corr_generated",
        )

        response = await boundary.create_command({"auth": IDENTITY, "headers": {}, "body": {"sessionId": "session_001"}})

        self.assertEqual(response["statusCode"], 400)
        self.assertEqual(response["body"]["error"]["category"], "VALIDATION")
        self.assertEqual(response["body"]["error"]["metadata"]["fieldName"], "headers.idempotency-key")
        self.assertEqual(response["body"]["requestId"], "req_generated")
        self.assertEqual(response["body"]["correlationId"], "corr_generated")

    async def test_http_action_routes_delegate_with_server_identity_and_event_correlation(self) -> None:
        events = []
        action_store = InMemoryActionStore()
        action_service = create_test_action_service(
            action_store=action_store,
            events=events,
            connector=RecordingConnector({"valid": True, "verifiedTarget": {"revision": "rev_001"}}, {"providerOperationId": "google_op_001"}),
        )
        proposed = await action_service.create_proposed_action(IDENTITY, base_action_input())
        boundary = create_http_command_boundary(command_service=RecordingCommandService({}), action_service=action_service)

        approved = await boundary.approve_action(
            {
                "auth": IDENTITY,
                "headers": {"X-Request-Id": "req_approve", "X-Correlation-Id": "corr_action"},
                "body": {"tenantId": "ignored", "userId": "ignored", "actionId": proposed["actionId"]},
            }
        )
        applied = await boundary.apply_action(
            {
                "auth": IDENTITY,
                "headers": {"Idempotency-Key": "idem_apply", "X-Request-Id": "req_apply", "X-Correlation-Id": "corr_action"},
                "body": {"actionId": proposed["actionId"]},
            }
        )

        self.assertEqual(approved["statusCode"], 200)
        self.assertEqual(applied["statusCode"], 200)
        self.assertEqual(applied["body"]["data"]["status"], ACTION_STATUS.APPLIED.value)
        approved_event = next(event for event in events if event.get("status") == ACTION_STATUS.APPROVED.value)
        applied_event = next(event for event in events if event.get("status") == ACTION_STATUS.APPLIED.value)
        self.assertEqual(approved_event["requestId"], "req_approve")
        self.assertEqual(approved_event["correlationId"], "corr_action")
        self.assertEqual(applied_event["requestId"], "req_apply")
        self.assertEqual(applied_event["correlationId"], "corr_action")

    async def test_http_reject_route_maps_terminal_status_without_client_identity(self) -> None:
        action_service = RecordingActionService()
        boundary = create_http_command_boundary(command_service=RecordingCommandService({}), action_service=action_service)

        response = await boundary.reject_action(
            {
                "auth": IDENTITY,
                "headers": {"X-Request-Id": "req_reject", "X-Correlation-Id": "corr_reject"},
                "body": {"tenantId": "ignored", "userId": "ignored", "actionId": "action_001", "reasonCode": "USER_CANCELLED"},
            }
        )

        self.assertEqual(response["statusCode"], 200)
        self.assertEqual(action_service.identity, IDENTITY)
        self.assertNotIn("tenantId", action_service.input_data)
        self.assertNotIn("userId", action_service.input_data)
        self.assertEqual(action_service.input_data["requestId"], "req_reject")
        self.assertEqual(action_service.input_data["correlationId"], "corr_reject")
        self.assertEqual(action_service.input_data["reasonCode"], "USER_CANCELLED")

    async def test_coordinates_assistant_commands_through_injected_dependencies(self) -> None:
        events = []

        class PolicyService:
            async def evaluate(self, request):
                return {"decision": "ALLOW", "decisionId": "pol_001"}

        class ContextService:
            async def resolve_context(self, request):
                return {
                    "authorized": True,
                    "request": request,
                    "provenance": {"source": "connector_verified"},
                    "metadata": {"resourceId": request["resourceId"]},
                }

        class Provider:
            async def generate(self, request):
                return {
                    "messageId": "msg_001",
                    "deltas": [f"answer to {request['prompt']['promptId']}", " done"],
                    "finishReason": "stop",
                    "usage": {"inputTokens": 4, "outputTokens": 5},
                }

        class PromptBuilder:
            def build_prompt(self, request):
                return {"promptId": request["command"]["commandId"]}

        service = create_command_service(
            clock=lambda: NOW,
            policy_service=PolicyService(),
            context_service=ContextService(),
            provider_registry={"openai": Provider()},
            prompt_builder=PromptBuilder(),
            event_publisher=Publisher(events),
        )

        result = await service.run_assistant_command(
            IDENTITY,
            {
                "commandId": "cmd_001",
                "requestId": "req_001",
                "correlationId": "corr_001",
                "sessionId": "session_001",
                "provider": "openai",
                "resourceId": "doc_001",
                "contextMode": "SELECTION",
                "secretRef": "secret_ref_001",
            },
        )

        self.assertEqual(result, {"messageId": "msg_001", "finishReason": "stop", "provider": "openai"})
        self.assertEqual([event["type"] for event in events], ["progress", "progress", "assistant.delta", "assistant.delta", "assistant.final"])

    async def test_blocks_commands_when_policy_dependency_denies_them(self) -> None:
        service = create_command_service(
            policy_service=SimplePolicy({"decision": "BLOCK", "decisionId": "pol_002", "reasonCode": "PUBLIC_POLICY_NOT_CONFIGURED"}),
            context_service=SimpleContext({"authorized": True}),
            provider_registry={},
            prompt_builder=SimplePromptBuilder(),
            event_publisher=Publisher([]),
        )

        with self.assertRaises(OrchestrationError) as caught:
            await service.run_assistant_command(
                IDENTITY,
                {
                    "requestId": "req_001",
                    "correlationId": "corr_001",
                    "sessionId": "session_001",
                    "provider": "openai",
                },
            )
        self.assertEqual(caught.exception.category, "POLICY")

    async def test_rejects_unsupported_providers_before_resolving_context(self) -> None:
        context = CountingContext({"authorized": True})
        events = []
        service = create_command_service(
            policy_service=SimplePolicy({"decision": "ALLOW", "decisionId": "pol_003"}),
            context_service=context,
            provider_registry={},
            prompt_builder=SimplePromptBuilder(),
            event_publisher=Publisher(events),
        )

        with self.assertRaises(OrchestrationError) as caught:
            await service.run_assistant_command(
                IDENTITY,
                {
                    "requestId": "req_unsupported",
                    "correlationId": "corr_unsupported",
                    "sessionId": "session_001",
                    "provider": "unknown",
                },
            )
        self.assertEqual(caught.exception.category, "VALIDATION")
        self.assertEqual(caught.exception.code, "PROVIDER_UNSUPPORTED")
        self.assertEqual(context.call_count, 0)
        self.assertEqual(events, [])

    async def test_returns_typed_validation_errors_for_malformed_public_inputs(self) -> None:
        service = create_command_service(
            policy_service=SimplePolicy({"decision": "ALLOW"}),
            context_service=SimpleContext({"authorized": True}),
            provider_registry={},
            prompt_builder=SimplePromptBuilder(),
            event_publisher=Publisher([]),
        )

        with self.assertRaises(OrchestrationError) as caught:
            await service.run_assistant_command(
                IDENTITY,
                {
                    "requestId": " ",
                    "correlationId": "corr_001",
                    "sessionId": "session_001",
                    "provider": "openai",
                },
            )
        self.assertEqual(caught.exception.category, "VALIDATION")
        self.assertEqual(caught.exception.status_code, 400)
        self.assertEqual(caught.exception.metadata["fieldName"], "command.requestId")

    async def test_creates_approves_applies_and_idempotently_replays_proposed_action(self) -> None:
        events = []
        connector = RecordingConnector({"valid": True, "verifiedTarget": {"revision": "rev_001"}}, {"providerOperationId": "google_op_001"})
        action_store = InMemoryActionStore()
        service = create_test_action_service(action_store=action_store, events=events, connector=connector)

        proposed = await service.create_proposed_action(IDENTITY, base_action_input())
        self.assertEqual(proposed["status"], ACTION_STATUS.PROPOSED.value)
        self.assertEqual(proposed["actionId"], "action_001")

        approved = await service.approve_action(IDENTITY, {"actionId": proposed["actionId"]})
        self.assertEqual(approved["status"], ACTION_STATUS.APPROVED.value)

        first_apply = await service.apply_action(IDENTITY, {"actionId": proposed["actionId"], "idempotencyKey": "idem_001"})
        replayed_apply = await service.apply_action(IDENTITY, {"actionId": proposed["actionId"], "idempotencyKey": "idem_001"})
        terminal_apply = await service.apply_action(IDENTITY, {"actionId": proposed["actionId"], "idempotencyKey": "idem_002"})

        self.assertEqual(connector.write_count, 1)
        self.assertEqual(replayed_apply, first_apply)
        self.assertEqual(terminal_apply["status"], ACTION_STATUS.APPLIED.value)
        self.assertEqual(action_store.get(proposed["actionId"])["status"], ACTION_STATUS.APPLIED.value)
        self.assertTrue(any(event["type"] == "action.status_changed" and event["status"] == ACTION_STATUS.APPLIED.value for event in events))

    async def test_uses_server_owned_action_ids_and_clamps_caller_supplied_ttls(self) -> None:
        service = create_test_action_service(
            action_store=InMemoryActionStore(),
            connector=RecordingConnector({"valid": True, "verifiedTarget": {}}, {"providerOperationId": "google_op_001"}),
        )

        proposed = await service.create_proposed_action(
            IDENTITY,
            {**base_action_input(), "actionId": "caller_chosen", "expiresAt": "2099-01-01T00:00:00.000Z", "ttlMs": 99 * 24 * 60 * 60 * 1000},
        )

        self.assertEqual(proposed["actionId"], "action_001")
        self.assertEqual(proposed["expiresAt"], "2026-05-30T00:00:00.000Z")

    async def test_marks_stale_action_state_conflicted_and_performs_no_provider_mutation(self) -> None:
        action_store = InMemoryActionStore()
        service = create_test_action_service(
            action_store=action_store,
            connector=RecordingConnector(
                {
                    "valid": False,
                    "reasonCode": "ORIGINAL_TEXT_HASH_MISMATCH",
                    "conflictDetails": {
                        "resourceId": "doc_001",
                        "expectedRevision": "rev_001",
                        "currentRevision": "rev_002",
                        "currentText": "must not be retained",
                        "expectedText": "must not be retained either",
                    },
                },
                {"providerOperationId": "should_not_happen"},
            ),
        )

        proposed = await service.create_proposed_action(IDENTITY, base_action_input())
        await service.approve_action(IDENTITY, {"actionId": proposed["actionId"]})
        result = await service.apply_action(IDENTITY, {"actionId": proposed["actionId"], "idempotencyKey": "idem_conflict"})

        self.assertEqual(result["status"], ACTION_STATUS.CONFLICTED.value)
        self.assertEqual(result["reasonCode"], "ORIGINAL_TEXT_HASH_MISMATCH")
        self.assertNotIn("must not be retained", str(result))
        self.assertNotIn("must not be retained", str(action_store.get(proposed["actionId"])))
        self.assertEqual(action_store.get(proposed["actionId"])["status"], ACTION_STATUS.CONFLICTED.value)

    async def test_reserves_apply_before_provider_mutation_so_concurrent_applies_do_not_double_write(self) -> None:
        gate = asyncio.Event()
        connector = BlockingConnector(gate)
        action_store = InMemoryActionStore()
        service = create_test_action_service(action_store=action_store, connector=connector)
        proposed = await service.create_proposed_action(IDENTITY, base_action_input())
        await service.approve_action(IDENTITY, {"actionId": proposed["actionId"]})

        first = asyncio.create_task(service.apply_action(IDENTITY, {"actionId": proposed["actionId"], "idempotencyKey": "idem_concurrent"}))
        await asyncio.sleep(0)
        with self.assertRaises(OrchestrationError) as caught:
            await service.apply_action(IDENTITY, {"actionId": proposed["actionId"], "idempotencyKey": "idem_concurrent"})
        self.assertEqual(caught.exception.code, "ACTION_APPLY_IN_PROGRESS")
        gate.set()
        await first
        self.assertEqual(connector.write_count, 1)

    async def test_action_methods_return_typed_validation_errors_for_malformed_inputs(self) -> None:
        service = create_test_action_service(
            action_store=InMemoryActionStore(),
            connector=RecordingConnector({"valid": True, "verifiedTarget": {}}, {"providerOperationId": "google_op_001"}),
        )

        for method, bad_input in (
            (service.approve_action, None),
            (service.reject_action, []),
            (service.apply_action, None),
        ):
            with self.assertRaises(OrchestrationError) as caught:
                await method(IDENTITY, bad_input)
            self.assertEqual(caught.exception.category, "VALIDATION")
            self.assertEqual(caught.exception.code, "INVALID_FIELD")

    async def test_unknown_persisted_action_status_returns_typed_error(self) -> None:
        action_store = InMemoryActionStore()
        service = create_test_action_service(
            action_store=action_store,
            connector=RecordingConnector({"valid": True, "verifiedTarget": {}}, {"providerOperationId": "google_op_001"}),
        )
        proposed = await service.create_proposed_action(IDENTITY, base_action_input())
        action_store.update(proposed["actionId"], lambda current: {**current, "status": "UNKNOWN"})

        with self.assertRaises(OrchestrationError) as caught:
            await service.reject_action(IDENTITY, {"actionId": proposed["actionId"]})
        self.assertEqual(caught.exception.category, "VALIDATION")
        self.assertEqual(caught.exception.code, "INVALID_ACTION_STATUS")
        self.assertEqual(caught.exception.metadata["actionId"], proposed["actionId"])

    async def test_conditional_approve_does_not_overwrite_rejected_state_after_stale_read(self) -> None:
        action_store = StaleApproveRaceStore()
        service = create_test_action_service(
            action_store=action_store,
            connector=RecordingConnector({"valid": True, "verifiedTarget": {}}, {"providerOperationId": "google_op_001"}),
        )
        proposed = await service.create_proposed_action(IDENTITY, base_action_input())

        with self.assertRaises(OrchestrationError) as caught:
            await service.approve_action(IDENTITY, {"actionId": proposed["actionId"]})
        self.assertEqual(caught.exception.code, "ACTION_NOT_APPROVABLE")
        self.assertEqual(action_store.get(proposed["actionId"])["status"], ACTION_STATUS.REJECTED.value)

    async def test_reject_during_apply_in_progress_does_not_overwrite_apply_lock(self) -> None:
        gate = asyncio.Event()
        action_store = InMemoryActionStore()
        service = create_test_action_service(action_store=action_store, connector=BlockingConnector(gate))
        proposed = await service.create_proposed_action(IDENTITY, base_action_input())
        await service.approve_action(IDENTITY, {"actionId": proposed["actionId"]})

        first = asyncio.create_task(service.apply_action(IDENTITY, {"actionId": proposed["actionId"], "idempotencyKey": "idem_reject_race"}))
        await asyncio.sleep(0)
        with self.assertRaises(OrchestrationError) as caught:
            await service.reject_action(IDENTITY, {"actionId": proposed["actionId"]})
        self.assertEqual(caught.exception.code, "ACTION_APPLY_IN_PROGRESS")
        gate.set()
        await first
        self.assertEqual(action_store.get(proposed["actionId"])["status"], ACTION_STATUS.APPLIED.value)

    async def test_duplicate_conflict_apply_preserves_first_terminal_transition(self) -> None:
        events = []
        action_store = InMemoryActionStore()
        service = create_test_action_service(
            action_store=action_store,
            events=events,
            connector=RecordingConnector(
                {
                    "valid": False,
                    "reasonCode": "ORIGINAL_TEXT_HASH_MISMATCH",
                    "conflictDetails": {"currentRevision": "rev_002"},
                },
                {"providerOperationId": "should_not_happen"},
            ),
        )
        proposed = await service.create_proposed_action(IDENTITY, base_action_input())
        await service.approve_action(IDENTITY, {"actionId": proposed["actionId"]})

        first = await service.apply_action(IDENTITY, {"actionId": proposed["actionId"], "idempotencyKey": "idem_conflict_first"})
        second = await service.apply_action(IDENTITY, {"actionId": proposed["actionId"], "idempotencyKey": "idem_conflict_second"})

        self.assertEqual(first, second)
        conflict_events = [
            event
            for event in events
            if event["type"] == "action.status_changed" and event["status"] == ACTION_STATUS.CONFLICTED.value
        ]
        self.assertEqual(len(conflict_events), 1)


def create_test_action_service(*, action_store, connector, events=None):
    return create_action_service(
        action_store=action_store,
        connector=connector,
        clock=lambda: NOW,
        id_generator=lambda _prefix: "action_001",
        event_publisher=Publisher(events if events is not None else []),
        consent_service=ConsentService(),
        payload_vault=PayloadVault(),
    )


def base_action_input():
    return {
        "sessionId": "session_001",
        "provider": "google_docs",
        "resourceId": "doc_001",
        "resourceRevision": "rev_001",
        "targetAnchor": {"kind": "range_name", "id": "anchor_001"},
        "targetRange": {"start": 1, "end": 4},
        "originalTextHash": "sha256:abc",
        "actionType": "replace_text",
        "payload": {"replacementText": "new text"},
        "summary": "Replace selected text",
    }


class Publisher:
    def __init__(self, events):
        self.events = events

    async def publish(self, event):
        self.events.append(event)


class SimplePolicy:
    def __init__(self, decision):
        self.decision = decision

    async def evaluate(self, _request):
        return self.decision


class SimpleContext:
    def __init__(self, response):
        self.response = response

    async def resolve_context(self, _request):
        return self.response


class CountingContext(SimpleContext):
    def __init__(self, response):
        super().__init__(response)
        self.call_count = 0

    async def resolve_context(self, request):
        self.call_count += 1
        return await super().resolve_context(request)


class SimplePromptBuilder:
    def build_prompt(self, _request):
        return {}


class ConsentService:
    async def validate_apply_consent(self, _request):
        return {"allowed": True}


class PayloadVault:
    async def encrypt(self, payload):
        return {"ciphertextRef": "encrypted_payload_001", "payload": payload}

    async def decrypt(self, encrypted_payload):
        return encrypted_payload["payload"]


class RecordingConnector:
    def __init__(self, validation, apply_result):
        self.validation = validation
        self.apply_result = apply_result
        self.write_count = 0

    async def validate_target(self, _action):
        return self.validation

    async def apply_action(self, _request):
        self.write_count += 1
        return self.apply_result


class BlockingConnector(RecordingConnector):
    def __init__(self, gate):
        super().__init__({"valid": True, "verifiedTarget": {"revision": "rev_001"}}, {"providerOperationId": "google_op_001"})
        self.gate = gate

    async def apply_action(self, _request):
        self.write_count += 1
        await self.gate.wait()
        return self.apply_result


class StaleApproveRaceStore(InMemoryActionStore):
    def transition(self, action_id, *, allowed_statuses, patch, reject_if_apply_locked=False):
        if patch.get("status") == ACTION_STATUS.APPROVED.value:
            self.update(action_id, lambda current: {**current, "status": ACTION_STATUS.REJECTED.value, "reasonCode": "USER_REJECTED"})
        return super().transition(
            action_id,
            allowed_statuses=allowed_statuses,
            patch=patch,
            reject_if_apply_locked=reject_if_apply_locked,
        )


class RecordingCommandService:
    def __init__(self, result):
        self.result = result
        self.identity = None
        self.command = None

    async def run_assistant_command(self, identity, command):
        self.identity = identity
        self.command = command
        return self.result


class RecordingActionService:
    def __init__(self):
        self.identity = None
        self.input_data = None

    async def approve_action(self, identity, input_data):
        self.identity = identity
        self.input_data = input_data
        return {"actionId": input_data["actionId"], "status": ACTION_STATUS.APPROVED.value}

    async def reject_action(self, identity, input_data):
        self.identity = identity
        self.input_data = input_data
        return {"actionId": input_data["actionId"], "status": ACTION_STATUS.REJECTED.value}

    async def apply_action(self, identity, input_data):
        self.identity = identity
        self.input_data = input_data
        return {"actionId": input_data["actionId"], "status": ACTION_STATUS.APPLIED.value}
