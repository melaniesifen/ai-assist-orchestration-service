from __future__ import annotations

import json
import unittest

from ai_assist_orchestration import OrchestrationHttpRuntime, create_http_command_boundary

from common import RecordingActionService, RecordingCommandService


AUTH_HEADERS = {
    "X-AI-Assist-Tenant-Id": "tenant_001",
    "X-AI-Assist-User-Id": "user_001",
}


class HttpRuntimeTests(unittest.TestCase):
    def test_command_route_maps_path_session_and_trusted_auth_headers(self) -> None:
        command_service = RecordingCommandService({"messageId": "msg_001", "finishReason": "stop", "provider": "openai"})
        runtime = OrchestrationHttpRuntime(
            boundary=create_http_command_boundary(
                command_service=command_service,
                action_service=RecordingActionService(),
                request_id_generator=lambda: "req_generated",
                correlation_id_generator=lambda: "corr_generated",
            )
        )

        response = runtime.handle_request(
            {
                "method": "POST",
                "path": "/resource-sessions/session_001/commands",
                "headers": {**AUTH_HEADERS, "Idempotency-Key": "idem_001"},
                "body": json.dumps({"provider": "openai", "resourceId": "doc_001", "contextMode": "ACTIVE_RESOURCE"}),
            }
        )

        self.assertEqual(response["statusCode"], 202)
        self.assertEqual(command_service.identity, {"tenantId": "tenant_001", "userId": "user_001"})
        self.assertEqual(command_service.command["sessionId"], "session_001")
        self.assertEqual(command_service.command["idempotencyKey"], "idem_001")

    def test_action_route_maps_action_id_from_path_and_rejects_spoofed_body_identity(self) -> None:
        action_service = RecordingActionService()
        runtime = OrchestrationHttpRuntime(
            boundary=create_http_command_boundary(
                command_service=RecordingCommandService({}),
                action_service=action_service,
            )
        )

        response = runtime.handle_request(
            {
                "method": "POST",
                "path": "/resource-sessions/session_001/actions/action_001/approve",
                "headers": AUTH_HEADERS,
                "body": json.dumps(
                    {
                        "tenantId": "attacker_tenant",
                        "userId": "attacker_user",
                        "resourceId": "doc_001",
                    }
                ),
            }
        )

        self.assertEqual(response["statusCode"], 200)
        self.assertEqual(action_service.identity, {"tenantId": "tenant_001", "userId": "user_001"})
        self.assertEqual(action_service.input_data["sessionId"], "session_001")
        self.assertEqual(action_service.input_data["actionId"], "action_001")
        self.assertNotIn("tenantId", action_service.input_data)
        self.assertNotIn("userId", action_service.input_data)

    def test_rejects_body_session_mismatch_before_action_boundary(self) -> None:
        runtime = OrchestrationHttpRuntime(
            boundary=create_http_command_boundary(
                command_service=RecordingCommandService({}),
                action_service=RecordingActionService(),
            )
        )

        response = runtime.handle_request(
            {
                "method": "POST",
                "path": "/resource-sessions/session_001/actions/action_001/reject",
                "headers": AUTH_HEADERS,
                "body": json.dumps({"sessionId": "session_other", "resourceId": "doc_001"}),
            }
        )

        self.assertEqual(response["statusCode"], 403)
        self.assertEqual(response["body"]["error"]["code"], "SESSION_ROUTE_BODY_MISMATCH")

    def test_missing_trusted_auth_context_is_authentication_error(self) -> None:
        runtime = OrchestrationHttpRuntime(
            boundary=create_http_command_boundary(
                command_service=RecordingCommandService({}),
                action_service=RecordingActionService(),
            )
        )

        response = runtime.handle_request(
            {
                "method": "POST",
                "path": "/resource-sessions/session_001/commands",
                "headers": {"Idempotency-Key": "idem_001"},
                "body": "{}",
            }
        )

        self.assertEqual(response["statusCode"], 401)
        self.assertEqual(response["body"]["error"]["category"], "AUTHENTICATION")

    def test_invalid_json_returns_safe_validation_error(self) -> None:
        runtime = OrchestrationHttpRuntime(
            boundary=create_http_command_boundary(
                command_service=RecordingCommandService({}),
                action_service=RecordingActionService(),
            )
        )

        response = runtime.handle_request(
            {
                "method": "POST",
                "path": "/resource-sessions/session_001/apply-action",
                "headers": {**AUTH_HEADERS, "Idempotency-Key": "idem_apply"},
                "body": "{not-json",
            }
        )

        self.assertEqual(response["statusCode"], 400)
        self.assertEqual(response["body"]["error"]["code"], "INVALID_JSON")


if __name__ == "__main__":
    unittest.main()
