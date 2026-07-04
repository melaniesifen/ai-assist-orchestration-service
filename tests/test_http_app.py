from __future__ import annotations

import json
import unittest

from ai_assist_orchestration import OrchestrationHttpRuntime, configure_http_runtime, create_http_command_boundary, handle_http_request

from common import RecordingActionService, RecordingCommandService


AUTH_HEADERS = {
    "X-AI-Assist-Tenant-Id": "tenant_001",
    "X-AI-Assist-User-Id": "user_001",
}


class PackageHttpAppTests(unittest.TestCase):
    def tearDown(self) -> None:
        configure_http_runtime(None)

    def test_injected_runtime_returns_supported_action_response_with_no_store(self) -> None:
        action_service = RecordingActionService()
        configure_http_runtime(
            OrchestrationHttpRuntime(
                boundary=create_http_command_boundary(
                    command_service=RecordingCommandService({}),
                    action_service=action_service,
                    request_id_generator=lambda: "req_generated",
                    correlation_id_generator=lambda: "corr_generated",
                )
            )
        )

        response = handle_http_request(
            "POST",
            "/resource-sessions/session_001/actions/action_001/approve",
            headers={**AUTH_HEADERS, "X-Request-Id": "req_001"},
            body=json.dumps({"resourceId": "doc_001"}),
        )

        self.assertEqual(response["statusCode"], 200)
        self.assertEqual(response["headers"]["Cache-Control"], "no-store")
        self.assertEqual(response["headers"]["Content-Type"], "application/json; charset=utf-8")
        self.assertEqual(action_service.input_data["sessionId"], "session_001")
        self.assertEqual(action_service.input_data["actionId"], "action_001")
        self.assertEqual(response["body"]["data"]["status"], "APPROVED")

    def test_injected_runtime_returns_command_response_with_no_store(self) -> None:
        command_service = RecordingCommandService({"messageId": "msg_001", "finishReason": "stop", "provider": "openai"})
        configure_http_runtime(
            OrchestrationHttpRuntime(
                boundary=create_http_command_boundary(
                    command_service=command_service,
                    action_service=RecordingActionService(),
                )
            )
        )

        response = handle_http_request(
            "POST",
            "/resource-sessions/session_001/commands",
            headers={**AUTH_HEADERS, "Idempotency-Key": "idem_001"},
            body=json.dumps({"provider": "openai", "resourceId": "doc_001", "contextMode": "ACTIVE_RESOURCE"}),
        )

        self.assertEqual(response["statusCode"], 202)
        self.assertEqual(response["headers"]["Cache-Control"], "no-store")
        self.assertEqual(command_service.command["sessionId"], "session_001")
        self.assertEqual(command_service.command["idempotencyKey"], "idem_001")
        self.assertEqual(response["body"]["data"]["messageId"], "msg_001")

    def test_injected_runtime_returns_reject_response_with_no_store(self) -> None:
        action_service = RecordingActionService()
        configure_http_runtime(
            OrchestrationHttpRuntime(
                boundary=create_http_command_boundary(
                    command_service=RecordingCommandService({}),
                    action_service=action_service,
                )
            )
        )

        response = handle_http_request(
            "POST",
            "/resource-sessions/session_001/actions/action_001/reject",
            headers=AUTH_HEADERS,
            body=json.dumps({"resourceId": "doc_001", "reasonCode": "USER_CANCELLED"}),
        )

        self.assertEqual(response["statusCode"], 200)
        self.assertEqual(response["headers"]["Cache-Control"], "no-store")
        self.assertEqual(action_service.input_data["sessionId"], "session_001")
        self.assertEqual(action_service.input_data["actionId"], "action_001")
        self.assertEqual(action_service.input_data["reasonCode"], "USER_CANCELLED")
        self.assertEqual(response["body"]["data"]["status"], "REJECTED")

    def test_injected_runtime_returns_apply_response_with_no_store(self) -> None:
        action_service = RecordingActionService()
        configure_http_runtime(
            OrchestrationHttpRuntime(
                boundary=create_http_command_boundary(
                    command_service=RecordingCommandService({}),
                    action_service=action_service,
                )
            )
        )

        response = handle_http_request(
            "POST",
            "/resource-sessions/session_001/apply-action",
            headers={**AUTH_HEADERS, "Idempotency-Key": "idem_apply"},
            body=json.dumps({"actionId": "action_001", "resourceId": "doc_001"}),
        )

        self.assertEqual(response["statusCode"], 200)
        self.assertEqual(response["headers"]["Cache-Control"], "no-store")
        self.assertEqual(action_service.input_data["sessionId"], "session_001")
        self.assertEqual(action_service.input_data["actionId"], "action_001")
        self.assertEqual(action_service.input_data["idempotencyKey"], "idem_apply")
        self.assertEqual(response["body"]["data"]["status"], "APPLIED")

    def test_default_runtime_returns_501_for_owned_command_route_without_dependency_leakage(self) -> None:
        raw_prompt = "raw prompt that must not leak"
        raw_document = "raw document text that must not leak"

        response = handle_http_request(
            "POST",
            "/resource-sessions/session_001/commands",
            headers={**AUTH_HEADERS, "Idempotency-Key": "idem_001"},
            body=json.dumps(
                {
                    "provider": "openai",
                    "resourceId": "doc_001",
                    "contextMode": "ACTIVE_RESOURCE",
                    "prompt": raw_prompt,
                    "documentText": raw_document,
                }
            ),
        )

        serialized = json.dumps(response, sort_keys=True)
        self.assertEqual(response["statusCode"], 501)
        self.assertEqual(response["headers"]["Cache-Control"], "no-store")
        self.assertEqual(response["body"]["error"]["code"], "ORCHESTRATION_DEPENDENCIES_NOT_CONFIGURED")
        self.assertEqual(response["body"]["error"]["category"], "DEPENDENCY")
        self.assertNotIn(raw_prompt, serialized)
        self.assertNotIn(raw_document, serialized)

    def test_missing_auth_maps_to_401(self) -> None:
        response = handle_http_request(
            "POST",
            "/resource-sessions/session_001/commands",
            headers={"Idempotency-Key": "idem_001"},
            body=json.dumps({"provider": "openai", "resourceId": "doc_001"}),
        )

        self.assertEqual(response["statusCode"], 401)
        self.assertEqual(response["headers"]["Cache-Control"], "no-store")
        self.assertEqual(response["body"]["error"]["category"], "AUTHENTICATION")

    def test_invalid_json_maps_to_400(self) -> None:
        response = handle_http_request(
            "POST",
            "/resource-sessions/session_001/apply-action",
            headers={**AUTH_HEADERS, "Idempotency-Key": "idem_apply"},
            body="{not-json",
        )

        self.assertEqual(response["statusCode"], 400)
        self.assertEqual(response["headers"]["Cache-Control"], "no-store")
        self.assertEqual(response["body"]["error"]["code"], "INVALID_JSON")

    def test_unknown_route_maps_to_404(self) -> None:
        response = handle_http_request(
            "GET",
            "/health",
            headers=AUTH_HEADERS,
            body=None,
        )

        self.assertEqual(response["statusCode"], 404)
        self.assertEqual(response["headers"]["Cache-Control"], "no-store")
        self.assertEqual(response["body"]["error"]["code"], "ROUTE_NOT_FOUND")

    def test_unexpected_service_failure_maps_to_safe_500_without_payload_leakage(self) -> None:
        raw_prompt = "private prompt"
        configure_http_runtime(
            OrchestrationHttpRuntime(
                boundary=create_http_command_boundary(
                    command_service=FailingCommandService(),
                    action_service=RecordingActionService(),
                )
            )
        )

        response = handle_http_request(
            "POST",
            "/resource-sessions/session_001/commands",
            headers={**AUTH_HEADERS, "Idempotency-Key": "idem_001"},
            body=json.dumps({"provider": "openai", "resourceId": "doc_001", "prompt": raw_prompt}),
        )

        serialized = json.dumps(response, sort_keys=True)
        self.assertEqual(response["statusCode"], 500)
        self.assertEqual(response["headers"]["Cache-Control"], "no-store")
        self.assertEqual(response["body"]["error"]["code"], "HTTP_RUNTIME_FAILED")
        self.assertNotIn(raw_prompt, serialized)
        self.assertNotIn("database unavailable", serialized)


class FailingCommandService:
    async def run_assistant_command(self, _identity, _command):
        raise RuntimeError("database unavailable")


if __name__ == "__main__":
    unittest.main()
