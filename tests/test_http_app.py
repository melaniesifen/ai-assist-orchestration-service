from __future__ import annotations

import json
import os
import tempfile
import unittest

from ai_assist_orchestration import (
    OrchestrationHttpRuntime,
    configure_http_dependencies,
    configure_http_runtime,
    create_http_command_boundary,
    handle_http_request,
)

from common import RecordingActionService, RecordingCommandService, RecordingProviderStatusService


AUTH_HEADERS = {
    "X-AI-Assist-Tenant-Id": "tenant_001",
    "X-AI-Assist-User-Id": "user_001",
}


class PackageHttpAppTests(unittest.TestCase):
    def tearDown(self) -> None:
        configure_http_runtime(None)

    def test_default_runtime_fails_closed_without_encrypted_action_payload_vault(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            self._configure_default_store(temp_dir)
            raw_payload_text = "private replacement text"

            response = handle_http_request(
                "POST",
                "/resource-sessions/session_001/actions",
                headers={**AUTH_HEADERS, "X-Request-Id": "req_create", "X-Correlation-Id": "corr_action"},
                body=json.dumps(
                    {
                        "provider": "google_docs",
                        "resourceId": "doc_001",
                        "resourceRevision": "rev_001",
                        "targetRange": {"start": 1, "end": 4},
                        "originalTextHash": "sha256:abc",
                        "actionType": "replace_text",
                        "payload": {"replacementText": raw_payload_text},
                        "summary": "Replace selected text",
                    }
                ),
            )

            serialized = json.dumps(response, sort_keys=True)
            self.assertEqual(response["statusCode"], 502)
            self.assertEqual(response["body"]["error"]["code"], "ACTION_PAYLOAD_VAULT_UNAVAILABLE")
            self.assertEqual(response["body"]["error"]["metadata"]["operation"], "payload_encrypt")
            self.assertNotIn(raw_payload_text, serialized)

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

    def test_configured_dependencies_return_command_response_without_replacing_runtime(self) -> None:
        command_service = RecordingCommandService({"messageId": "msg_001", "finishReason": "stop", "provider": "openai"})
        configure_http_dependencies(
            command_service=command_service,
            action_service=RecordingActionService(),
            provider_status_service=RecordingProviderStatusService({"providers": []}),
        )

        response = handle_http_request(
            "POST",
            "/resource-sessions/session_001/commands",
            headers={**AUTH_HEADERS, "Idempotency-Key": "idem_001", "X-Request-Id": "req_001"},
            body=json.dumps({"provider": "openai", "resourceId": "doc_001", "contextMode": "ACTIVE_RESOURCE"}),
        )

        self.assertEqual(response["statusCode"], 202)
        self.assertEqual(response["headers"]["Cache-Control"], "no-store")
        self.assertEqual(command_service.identity, {"tenantId": "tenant_001", "userId": "user_001"})
        self.assertEqual(command_service.command["sessionId"], "session_001")
        self.assertEqual(command_service.command["requestId"], "req_001")
        self.assertEqual(response["body"]["data"]["finishReason"], "stop")

    def test_configured_provider_status_returns_safe_no_store_response(self) -> None:
        provider_status = RecordingProviderStatusService(
            {
                "providers": [
                    {
                        "provider": "openai",
                        "available": True,
                        "status": "available",
                        "credentialSource": "platform",
                        "secretRef": "platform/openai",
                        "secretArn": "arn:aws:secretsmanager:us-west-2:123456789012:secret:openai",
                        "secretValue": "sk-must-not-leak",
                        "model": "internal-model-name",
                        "messages": [{"role": "user", "content": "private message"}],
                        "input": "private input",
                        "output": "private output",
                        "rawResponse": "raw provider response must not leak",
                    }
                ],
                "prompt": "raw prompt must not leak",
            }
        )
        configure_http_dependencies(provider_status_service=provider_status)

        response = handle_http_request(
            "GET",
            "/providers",
            headers={**AUTH_HEADERS, "X-Request-Id": "req_providers", "X-Correlation-Id": "corr_providers"},
        )

        serialized = json.dumps(response, sort_keys=True)
        self.assertEqual(response["statusCode"], 200)
        self.assertEqual(response["headers"]["Cache-Control"], "no-store")
        self.assertEqual(provider_status.identity, {"tenantId": "tenant_001", "userId": "user_001"})
        self.assertEqual(provider_status.request["requestId"], "req_providers")
        self.assertEqual(response["body"]["requestId"], "req_providers")
        self.assertEqual(response["body"]["correlationId"], "corr_providers")
        self.assertEqual(response["body"]["data"]["providers"][0]["provider"], "openai")
        self.assertNotIn("secretRef", response["body"]["data"]["providers"][0])
        self.assertNotIn("secretArn", response["body"]["data"]["providers"][0])
        self.assertNotIn("model", response["body"]["data"]["providers"][0])
        self.assertNotIn("messages", response["body"]["data"]["providers"][0])
        self.assertNotIn("input", response["body"]["data"]["providers"][0])
        self.assertNotIn("output", response["body"]["data"]["providers"][0])
        self.assertNotIn("platform/openai", serialized)
        self.assertNotIn("arn:aws:secretsmanager", serialized)
        self.assertNotIn("internal-model-name", serialized)
        self.assertNotIn("private message", serialized)
        self.assertNotIn("private input", serialized)
        self.assertNotIn("private output", serialized)
        self.assertNotIn("sk-must-not-leak", serialized)
        self.assertNotIn("raw provider response", serialized)
        self.assertNotIn("raw prompt", serialized)

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

    def test_default_runtime_returns_501_for_provider_status_without_dependency_leakage(self) -> None:
        response = handle_http_request(
            "GET",
            "/providers",
            headers=AUTH_HEADERS,
        )

        self.assertEqual(response["statusCode"], 501)
        self.assertEqual(response["headers"]["Cache-Control"], "no-store")
        self.assertEqual(response["body"]["error"]["code"], "ORCHESTRATION_DEPENDENCIES_NOT_CONFIGURED")
        self.assertEqual(response["body"]["error"]["category"], "DEPENDENCY")
        self.assertEqual(response["body"]["error"]["metadata"], {"operation": "provider_status"})

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

    def _configure_default_store(self, temp_dir: str) -> None:
        old_action_store = os.environ.get("ORCHESTRATION_ACTION_STORE_PATH")
        os.environ["ORCHESTRATION_ACTION_STORE_PATH"] = os.path.join(temp_dir, "actions.json")
        self.addCleanup(self._restore_env, "ORCHESTRATION_ACTION_STORE_PATH", old_action_store)
        configure_http_runtime(None)

    def _restore_env(self, key: str, old_value: str | None) -> None:
        if old_value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = old_value


class FailingCommandService:
    async def run_assistant_command(self, _identity, _command):
        raise RuntimeError("database unavailable")


if __name__ == "__main__":
    unittest.main()
