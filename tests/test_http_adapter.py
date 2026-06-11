from __future__ import annotations

import unittest

from ai_assist_orchestration import ACTION_STATUS, InMemoryActionStore, create_http_command_boundary

from common import (
    IDENTITY,
    RecordingActionService,
    RecordingCommandService,
    RecordingConnector,
    action_decision_input,
    base_action_input,
    create_test_action_service,
)


class HttpAdapterTests(unittest.IsolatedAsyncioTestCase):
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
                "body": {
                    "tenantId": "ignored",
                    "userId": "ignored",
                    "actionId": proposed["actionId"],
                    "sessionId": "session_001",
                    "resourceId": "doc_001",
                },
            }
        )
        applied = await boundary.apply_action(
            {
                "auth": IDENTITY,
                "headers": {"Idempotency-Key": "idem_apply", "X-Request-Id": "req_apply", "X-Correlation-Id": "corr_action"},
                "body": {"actionId": proposed["actionId"], "sessionId": "session_001", "resourceId": "doc_001"},
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
                "body": {
                    "tenantId": "ignored",
                    "userId": "ignored",
                    "actionId": "action_001",
                    "sessionId": "session_001",
                    "resourceId": "doc_001",
                    "reasonCode": "USER_CANCELLED",
                },
            }
        )

        self.assertEqual(response["statusCode"], 200)
        self.assertEqual(action_service.identity, IDENTITY)
        self.assertNotIn("tenantId", action_service.input_data)
        self.assertNotIn("userId", action_service.input_data)
        self.assertEqual(action_service.input_data["requestId"], "req_reject")
        self.assertEqual(action_service.input_data["correlationId"], "corr_reject")
        self.assertEqual(action_service.input_data["reasonCode"], "USER_CANCELLED")

