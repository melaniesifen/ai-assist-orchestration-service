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
        self.assertEqual(
            [(event["stage"], event["status"], event["messageCode"]) for event in events[:2]],
            [
                ("context.loading", "started", "CONTEXT.LOADING.STARTED"),
                ("provider.generating", "in_progress", "PROVIDER.GENERATING.IN_PROGRESS"),
            ],
        )

    async def test_converts_provider_stream_chunks_to_assistant_events(self) -> None:
        events = []
        observed_provider_request = {}

        class StreamProvider:
            async def stream(self, request):
                observed_provider_request.update(request)
                yield {"type": "assistant.delta", "provider": "openai", "model": "test-model", "delta": "first "}
                yield {"type": "assistant.delta", "provider": "openai", "model": "test-model", "delta": "second"}
                yield {
                    "type": "assistant.final",
                    "provider": "openai",
                    "model": "test-model",
                    "finishReason": "stop",
                    "usage": {"inputTokens": 2, "outputTokens": 3, "totalTokens": 5},
                }

        service = create_command_service(
            clock=lambda: NOW,
            policy_service=SimplePolicy({"decision": "ALLOW", "decisionId": "pol_stream"}),
            context_service=SimpleContext(
                {
                    "authorized": True,
                    "contextMode": "SELECTION",
                    "resourceRef": {"provider": "google_docs", "resourceId": "doc_001"},
                    "provenance": {"connectorVerified": True, "resourceVersion": "rev_001"},
                }
            ),
            provider_registry={"openai": StreamProvider()},
            prompt_builder=SimplePromptBuilder(),
            event_publisher=Publisher(events),
        )

        result = await service.run_assistant_command(IDENTITY, base_command_input())

        self.assertEqual(result, {"messageId": "msg_req_001", "finishReason": "stop", "provider": "openai"})
        self.assertEqual([event["type"] for event in events], ["progress", "progress", "assistant.delta", "assistant.delta", "assistant.final"])
        self.assertEqual(events[2]["messageId"], "msg_req_001")
        self.assertEqual(events[2]["index"], 0)
        self.assertEqual(events[3]["index"], 1)
        self.assertEqual(events[4]["usage"], {"inputTokens": 2, "outputTokens": 3, "totalTokens": 5})
        self.assertEqual(observed_provider_request["context"]["contextMode"], "SELECTION")
        self.assertTrue(observed_provider_request["context"]["provenance"]["connectorVerified"])

    async def test_provider_proposal_output_creates_server_owned_actions_and_safe_events(self) -> None:
        events = []
        action_store = InMemoryActionStore()
        action_service = create_test_action_service(
            action_store=action_store,
            events=events,
            connector=RecordingConnector({"valid": True, "verifiedTarget": {}}, {"providerOperationId": "google_op_001"}),
            id_generator=lambda _prefix: "action_from_provider",
        )

        class ProposalProvider:
            async def stream(self, _request):
                yield {"type": "assistant.delta", "delta": "Proposal ready."}
                yield {
                    "type": "assistant.final",
                    "finishReason": "stop",
                    "proposals": [
                        {
                            "proposalId": "proposal_001",
                            "actionId": "caller_must_not_win",
                            "actionType": "replace_text",
                            "currentText": "plain old text",
                            "proposedText": "plain new text",
                            "surroundingText": "plain surrounding text",
                            "rationale": "Improve clarity",
                            "targetHint": {
                                "originalTextHash": "sha256:from-provider",
                                "targetRange": {"start": 5, "end": 9},
                            },
                        }
                    ],
                }

        service = create_command_service(
            clock=lambda: NOW,
            policy_service=SimplePolicy({"decision": "ALLOW"}),
            context_service=SimpleContext(
                {
                    "authorized": True,
                    "resourceRef": {"provider": "google_docs", "resourceId": "doc_from_context"},
                    "provenance": {"resourceVersion": "rev_from_context"},
                }
            ),
            provider_registry={"openai": ProposalProvider()},
            prompt_builder=SimplePromptBuilder(),
            event_publisher=Publisher(events),
            action_service=action_service,
        )

        result = await service.run_assistant_command(IDENTITY, base_command_input())

        self.assertEqual(result["proposedActionIds"], ["action_from_provider"])
        persisted = action_store.get("action_from_provider")
        self.assertEqual(persisted["tenantId"], IDENTITY["tenantId"])
        self.assertEqual(persisted["userId"], IDENTITY["userId"])
        self.assertEqual(persisted["sessionId"], "session_001")
        self.assertEqual(persisted["provider"], "google_docs")
        self.assertEqual(persisted["resourceId"], "doc_from_context")
        self.assertEqual(persisted["resourceRevision"], "rev_from_context")
        self.assertEqual(persisted["status"], ACTION_STATUS.PROPOSED.value)
        self.assertEqual(persisted["actionId"], "action_from_provider")
        self.assertNotIn("plain old text", str(persisted["encryptedPayload"]))
        self.assertNotIn("plain new text", str(persisted["encryptedPayload"]))

        proposed_event = next(event for event in events if event["type"] == "action.proposed")
        self.assertEqual(proposed_event["resourceRef"], {"provider": "google_docs", "resourceId": "doc_from_context"})
        self.assertEqual(proposed_event["requestId"], "req_001")
        self.assertEqual(proposed_event["correlationId"], "corr_001")
        self.assertNotIn("plain old text", str(proposed_event))
        self.assertNotIn("plain new text", str(proposed_event))

    async def test_provider_proposal_batch_validation_rejects_unsafe_batches_before_persistence(self) -> None:
        cases = [
            (
                "unsupported action",
                [
                    {
                        "actionType": "rewrite_document",
                        "targetRange": {"start": 1, "end": 3},
                        "originalTextHash": "sha256:one",
                    }
                ],
                "UNSUPPORTED_ACTION_TYPE",
            ),
            (
                "malformed target",
                [
                    {
                        "actionType": "replace_text",
                        "targetRange": {"start": 5, "end": 5},
                        "originalTextHash": "sha256:one",
                    }
                ],
                "INVALID_ACTION_TARGET",
            ),
            (
                "overlapping ranges",
                [
                    {
                        "proposalId": "proposal_001",
                        "actionType": "replace_text",
                        "targetRange": {"start": 1, "end": 5},
                        "originalTextHash": "sha256:one",
                    },
                    {
                        "proposalId": "proposal_002",
                        "actionType": "replace_text",
                        "targetRange": {"start": 4, "end": 8},
                        "originalTextHash": "sha256:two",
                    },
                ],
                "OVERLAPPING_ACTION_TARGETS",
            ),
            (
                "valid then invalid",
                [
                    {
                        "proposalId": "proposal_001",
                        "actionType": "replace_text",
                        "targetRange": {"start": 1, "end": 3},
                        "originalTextHash": "sha256:one",
                    },
                    {
                        "proposalId": "proposal_002",
                        "actionType": "replace_text",
                        "targetRange": {"start": 9, "end": 12},
                    },
                ],
                "INVALID_FIELD",
            ),
        ]

        for name, proposals, error_code in cases:
            with self.subTest(name=name):
                events = []
                action_store = InMemoryActionStore()
                service = create_command_with_proposal_provider(action_store=action_store, events=events, proposals=proposals)

                with self.assertRaises(OrchestrationError) as caught:
                    await service.run_assistant_command(IDENTITY, base_command_input())

                self.assertEqual(caught.exception.code, error_code)
                self.assertIsNone(action_store.get("action_001"))
                self.assertFalse(any(event["type"] == "action.proposed" for event in events))

    async def test_multiple_non_overlapping_provider_proposals_create_actions_after_batch_validation(self) -> None:
        events = []
        action_store = InMemoryActionStore()
        service = create_command_with_proposal_provider(
            action_store=action_store,
            events=events,
            proposals=[
                {
                    "proposalId": "proposal_001",
                    "actionType": "replace_text",
                    "targetRange": {"start": 1, "end": 3},
                    "originalTextHash": "sha256:one",
                },
                {
                    "proposalId": "proposal_002",
                    "actionType": "replace_text",
                    "targetRange": {"start": 6, "end": 9},
                    "originalTextHash": "sha256:two",
                },
            ],
            id_generator=CountingIdGenerator(),
        )

        result = await service.run_assistant_command(IDENTITY, base_command_input())

        self.assertEqual(result["proposedActionIds"], ["action_001", "action_002"])
        self.assertEqual(action_store.get("action_001")["status"], ACTION_STATUS.PROPOSED.value)
        self.assertEqual(action_store.get("action_002")["status"], ACTION_STATUS.PROPOSED.value)
        self.assertEqual(len([event for event in events if event["type"] == "action.proposed"]), 2)

    async def test_context_failures_publish_safe_error_event(self) -> None:
        events = []
        service = create_command_service(
            policy_service=SimplePolicy({"decision": "ALLOW"}),
            context_service=SimpleContext(
                {
                    "authorized": False,
                    "reasonCode": "CONSENT_REQUIRED",
                    "content": "raw document text must not be published",
                }
            ),
            provider_registry={"openai": object()},
            prompt_builder=SimplePromptBuilder(),
            event_publisher=Publisher(events),
        )

        with self.assertRaises(OrchestrationError) as caught:
            await service.run_assistant_command(IDENTITY, base_command_input())

        self.assertEqual(caught.exception.code, "CONTEXT_UNAVAILABLE")
        self.assertEqual([event["type"] for event in events], ["progress", "error"])
        self.assertEqual(events[-1]["errorCode"], "CONTEXT_UNAVAILABLE")
        self.assertEqual(events[-1]["metadata"], {"sessionId": "session_001", "reasonCode": "CONSENT_REQUIRED"})
        self.assertNotIn("raw document text", str(events[-1]))

    async def test_provider_stream_error_publishes_safe_error_event(self) -> None:
        events = []

        class ErrorProvider:
            async def stream(self, _request):
                yield {
                    "type": "error",
                    "provider": "openai",
                    "model": "test-model",
                    "error": {
                        "code": "PROVIDER_RATE_LIMITED",
                        "category": "rate_limited",
                        "message": "Provider asked the service to retry later.",
                        "dependencyStatus": "rate_limited",
                        "retryAfterSeconds": 30,
                        "rawResponse": "raw provider body must not be published",
                    },
                }

        service = create_command_service(
            policy_service=SimplePolicy({"decision": "ALLOW"}),
            context_service=SimpleContext({"authorized": True}),
            provider_registry={"openai": ErrorProvider()},
            prompt_builder=SimplePromptBuilder(),
            event_publisher=Publisher(events),
        )

        with self.assertRaises(OrchestrationError) as caught:
            await service.run_assistant_command(IDENTITY, base_command_input())

        self.assertEqual(caught.exception.code, "PROVIDER_RATE_LIMITED")
        self.assertEqual(caught.exception.category, "RATE_LIMITED")
        self.assertTrue(caught.exception.retryable)
        self.assertEqual(caught.exception.status_code, 429)
        self.assertEqual([event["type"] for event in events], ["progress", "progress", "error"])
        self.assertEqual(events[-1]["category"], "RATE_LIMITED")
        self.assertEqual(events[-1]["retryable"], True)
        self.assertEqual(events[-1]["metadata"]["dependencyStatus"], "rate_limited")
        self.assertEqual(events[-1]["metadata"]["retryAfterSeconds"], 30)
        self.assertNotIn("raw provider body", str(events[-1]))

    async def test_publisher_failures_are_categorized_as_dependency_errors(self) -> None:
        service = create_command_service(
            policy_service=SimplePolicy({"decision": "ALLOW"}),
            context_service=SimpleContext({"authorized": True}),
            provider_registry={"openai": LegacyProvider()},
            prompt_builder=SimplePromptBuilder(),
            event_publisher=FailingPublisher(fail_on_type="assistant.final"),
        )

        with self.assertRaises(OrchestrationError) as caught:
            await service.run_assistant_command(IDENTITY, base_command_input())

        self.assertEqual(caught.exception.code, "EVENT_PUBLISH_FAILED")
        self.assertEqual(caught.exception.category, "DEPENDENCY")
        self.assertEqual(caught.exception.metadata["eventType"], "assistant.final")
        self.assertEqual(caught.exception.metadata["sessionId"], "session_001")

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

        approved = await service.approve_action(IDENTITY, action_decision_input(proposed["actionId"]))
        self.assertEqual(approved["status"], ACTION_STATUS.APPROVED.value)

        first_apply = await service.apply_action(IDENTITY, action_apply_input(proposed["actionId"], "idem_001"))
        replayed_apply = await service.apply_action(IDENTITY, action_apply_input(proposed["actionId"], "idem_001"))
        terminal_apply = await service.apply_action(IDENTITY, action_apply_input(proposed["actionId"], "idem_002"))

        self.assertEqual(connector.write_count, 1)
        self.assertEqual(replayed_apply, first_apply)
        self.assertEqual(terminal_apply["status"], ACTION_STATUS.APPLIED.value)
        self.assertEqual(action_store.get(proposed["actionId"])["status"], ACTION_STATUS.APPLIED.value)
        self.assertTrue(any(event["type"] == "action.status_changed" and event["status"] == ACTION_STATUS.APPLIED.value for event in events))

    async def test_approve_and_reject_lifecycle_decisions_are_deterministic(self) -> None:
        events = []
        action_store = InMemoryActionStore()
        service = create_test_action_service(
            action_store=action_store,
            events=events,
            connector=RecordingConnector({"valid": True, "verifiedTarget": {}}, {"providerOperationId": "google_op_001"}),
        )

        proposed = await service.create_proposed_action(IDENTITY, base_action_input())
        first_approve = await service.approve_action(IDENTITY, action_decision_input(proposed["actionId"]))
        second_approve = await service.approve_action(IDENTITY, action_decision_input(proposed["actionId"]))
        first_reject = await service.reject_action(IDENTITY, action_decision_input(proposed["actionId"], reasonCode="USER_REJECTED"))
        second_reject = await service.reject_action(IDENTITY, action_decision_input(proposed["actionId"], reasonCode="USER_REJECTED"))

        self.assertEqual(first_approve["status"], ACTION_STATUS.APPROVED.value)
        self.assertEqual(second_approve, first_approve)
        self.assertEqual(first_reject["status"], ACTION_STATUS.REJECTED.value)
        self.assertEqual(second_reject, first_reject)
        approved_events = [event for event in events if event.get("status") == ACTION_STATUS.APPROVED.value]
        rejected_events = [event for event in events if event.get("status") == ACTION_STATUS.REJECTED.value]
        self.assertEqual(len(approved_events), 1)
        self.assertEqual(len(rejected_events), 1)

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

    async def test_decision_commands_deny_cross_tenant_user_session_and_resource_scope(self) -> None:
        action_store = InMemoryActionStore()
        service = create_test_action_service(
            action_store=action_store,
            connector=RecordingConnector({"valid": True, "verifiedTarget": {}}, {"providerOperationId": "google_op_001"}),
        )
        proposed = await service.create_proposed_action(IDENTITY, base_action_input())

        denial_cases = [
            ({"tenantId": "tenant_other", "userId": IDENTITY["userId"]}, action_decision_input(proposed["actionId"])),
            ({"tenantId": IDENTITY["tenantId"], "userId": "user_other"}, action_decision_input(proposed["actionId"])),
            (IDENTITY, action_decision_input(proposed["actionId"], sessionId="session_other")),
            (IDENTITY, action_decision_input(proposed["actionId"], resourceId="doc_other")),
        ]
        for denied_identity, denied_input in denial_cases:
            with self.subTest(denied_identity=denied_identity, denied_input=denied_input):
                with self.assertRaises(OrchestrationError) as caught:
                    await service.approve_action(denied_identity, denied_input)
                self.assertEqual(caught.exception.category, "AUTHORIZATION")

        self.assertEqual(action_store.get(proposed["actionId"])["status"], ACTION_STATUS.PROPOSED.value)

    async def test_decision_commands_require_session_and_resource_scope(self) -> None:
        action_store = InMemoryActionStore()
        service = create_test_action_service(
            action_store=action_store,
            connector=RecordingConnector({"valid": True, "verifiedTarget": {}}, {"providerOperationId": "google_op_001"}),
        )
        proposed = await service.create_proposed_action(IDENTITY, base_action_input())

        for method, scoped_input in (
            (service.approve_action, {"actionId": proposed["actionId"], "resourceId": "doc_001"}),
            (service.reject_action, {"actionId": proposed["actionId"], "sessionId": "session_001"}),
            (service.apply_action, {"actionId": proposed["actionId"], "idempotencyKey": "idem_missing_scope", "sessionId": "session_001"}),
        ):
            with self.subTest(method=method.__name__):
                with self.assertRaises(OrchestrationError) as caught:
                    await method(IDENTITY, scoped_input)
                self.assertEqual(caught.exception.category, "VALIDATION")

        self.assertEqual(action_store.get(proposed["actionId"])["status"], ACTION_STATUS.PROPOSED.value)

    async def test_stale_approval_or_rejection_expires_action_without_connector_mutation(self) -> None:
        events = []
        action_store = InMemoryActionStore()
        connector = RecordingConnector({"valid": True, "verifiedTarget": {}}, {"providerOperationId": "should_not_happen"})
        service = create_test_action_service(
            action_store=action_store,
            events=events,
            connector=connector,
        )
        proposed = await service.create_proposed_action(IDENTITY, {**base_action_input(), "ttlMs": 1})
        action_store.update(proposed["actionId"], lambda current: {**current, "expiresAt": "2026-05-28T23:59:59.000Z"})

        approved = await service.approve_action(IDENTITY, action_decision_input(proposed["actionId"]))
        rejected = await service.reject_action(IDENTITY, action_decision_input(proposed["actionId"]))

        self.assertEqual(approved["status"], ACTION_STATUS.EXPIRED.value)
        self.assertEqual(rejected["status"], ACTION_STATUS.EXPIRED.value)
        self.assertEqual(connector.write_count, 0)
        expired_events = [event for event in events if event.get("status") == ACTION_STATUS.EXPIRED.value]
        self.assertEqual(len(expired_events), 1)

    async def test_action_events_and_fake_encryption_boundary_exclude_payload_plaintext(self) -> None:
        events = []
        action_store = InMemoryActionStore()
        service = create_test_action_service(
            action_store=action_store,
            events=events,
            connector=RecordingConnector({"valid": True, "verifiedTarget": {}}, {"providerOperationId": "google_op_001"}),
        )

        proposed = await service.create_proposed_action(
            IDENTITY,
            {
                **base_action_input(),
                "payload": {
                    "currentText": "sensitive current document text",
                    "proposedText": "sensitive proposed document text",
                },
            },
        )
        await service.approve_action(IDENTITY, action_decision_input(proposed["actionId"]))

        persisted = action_store.get(proposed["actionId"])
        self.assertEqual(persisted["encryptedPayload"], {"ciphertextRef": "encrypted_payload_1"})
        self.assertNotIn("sensitive current document text", str(persisted))
        self.assertNotIn("sensitive proposed document text", str(persisted))
        self.assertNotIn("sensitive current document text", str(events))
        self.assertNotIn("sensitive proposed document text", str(events))

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
        await service.approve_action(IDENTITY, action_decision_input(proposed["actionId"]))
        result = await service.apply_action(IDENTITY, action_apply_input(proposed["actionId"], "idem_conflict"))

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
        await service.approve_action(IDENTITY, action_decision_input(proposed["actionId"]))

        first = asyncio.create_task(service.apply_action(IDENTITY, action_apply_input(proposed["actionId"], "idem_concurrent")))
        await asyncio.sleep(0)
        with self.assertRaises(OrchestrationError) as caught:
            await service.apply_action(IDENTITY, action_apply_input(proposed["actionId"], "idem_concurrent"))
        self.assertEqual(caught.exception.code, "ACTION_APPLY_IN_PROGRESS")
        gate.set()
        await first
        self.assertEqual(connector.write_count, 1)

    async def test_reserves_apply_before_target_validation_so_duplicate_requests_wait(self) -> None:
        validation_gate = asyncio.Event()
        connector = BlockingValidationConnector(validation_gate)
        action_store = InMemoryActionStore()
        service = create_test_action_service(action_store=action_store, connector=connector)
        proposed = await service.create_proposed_action(IDENTITY, base_action_input())
        await service.approve_action(IDENTITY, action_decision_input(proposed["actionId"]))

        first = asyncio.create_task(service.apply_action(IDENTITY, action_apply_input(proposed["actionId"], "idem_validation_race")))
        await asyncio.sleep(0)
        with self.assertRaises(OrchestrationError) as caught:
            await service.apply_action(IDENTITY, action_apply_input(proposed["actionId"], "idem_validation_race"))
        self.assertEqual(caught.exception.code, "ACTION_APPLY_IN_PROGRESS")
        validation_gate.set()
        await first
        self.assertEqual(connector.validation_count, 1)
        self.assertEqual(connector.write_count, 1)

    async def test_revoked_consent_conflicts_and_skips_connector_mutation(self) -> None:
        events = []
        connector = RecordingConnector({"valid": True, "verifiedTarget": {"revision": "rev_001"}}, {"providerOperationId": "should_not_happen"})
        action_store = InMemoryActionStore()
        service = create_test_action_service(
            action_store=action_store,
            events=events,
            connector=connector,
            consent_service=ConsentService({"allowed": False, "reasonCode": "CONSENT_REVOKED"}),
        )
        proposed = await service.create_proposed_action(IDENTITY, base_action_input())
        await service.approve_action(IDENTITY, action_decision_input(proposed["actionId"]))

        result = await service.apply_action(IDENTITY, action_apply_input(proposed["actionId"], "idem_consent_revoked"))

        self.assertEqual(result["status"], ACTION_STATUS.CONFLICTED.value)
        self.assertEqual(result["reasonCode"], "CONSENT_REVOKED")
        self.assertEqual(connector.validation_count, 0)
        self.assertEqual(connector.write_count, 0)
        self.assertEqual(action_store.get(proposed["actionId"])["status"], ACTION_STATUS.CONFLICTED.value)
        self.assertTrue(any(event.get("status") == ACTION_STATUS.CONFLICTED.value and event.get("reasonCode") == "CONSENT_REVOKED" for event in events))

    async def test_reconnect_required_token_status_fails_safely_before_target_validation(self) -> None:
        events = []
        connector = RecordingConnector({"valid": True, "verifiedTarget": {"revision": "rev_001"}}, {"providerOperationId": "should_not_happen"})
        action_store = InMemoryActionStore()
        service = create_test_action_service(
            action_store=action_store,
            events=events,
            connector=connector,
            token_service=TokenService({"valid": False, "reasonCode": "RECONNECT_REQUIRED"}),
        )
        proposed = await service.create_proposed_action(IDENTITY, base_action_input())
        await service.approve_action(IDENTITY, action_decision_input(proposed["actionId"]))

        result = await service.apply_action(IDENTITY, action_apply_input(proposed["actionId"], "idem_reconnect"))

        self.assertEqual(result["status"], ACTION_STATUS.FAILED.value)
        self.assertEqual(result["reasonCode"], "RECONNECT_REQUIRED")
        self.assertEqual(connector.validation_count, 0)
        self.assertEqual(connector.write_count, 0)
        self.assertEqual(action_store.get(proposed["actionId"])["failureCode"], "RECONNECT_REQUIRED")
        self.assertTrue(any(event.get("status") == ACTION_STATUS.FAILED.value and event.get("reasonCode") == "RECONNECT_REQUIRED" for event in events))

    async def test_payload_decrypt_failure_fails_safely_before_connector_mutation(self) -> None:
        events = []
        connector = RecordingConnector({"valid": True, "verifiedTarget": {"revision": "rev_001"}}, {"providerOperationId": "should_not_happen"})
        action_store = InMemoryActionStore()
        service = create_test_action_service(
            action_store=action_store,
            events=events,
            connector=connector,
            payload_vault=FailingPayloadVault(),
        )
        proposed = await service.create_proposed_action(IDENTITY, base_action_input())
        await service.approve_action(IDENTITY, action_decision_input(proposed["actionId"]))

        with self.assertRaises(OrchestrationError) as caught:
            await service.apply_action(IDENTITY, action_apply_input(proposed["actionId"], "idem_decrypt"))

        self.assertEqual(caught.exception.code, "ACTION_PAYLOAD_DECRYPT_FAILED")
        self.assertEqual(connector.validation_count, 1)
        self.assertEqual(connector.write_count, 0)
        persisted = action_store.get(proposed["actionId"])
        self.assertEqual(persisted["status"], ACTION_STATUS.FAILED.value)
        self.assertEqual(persisted["failureCode"], "ACTION_PAYLOAD_DECRYPT_FAILED")
        self.assertTrue(any(event.get("status") == ACTION_STATUS.FAILED.value and event.get("reasonCode") == "ACTION_PAYLOAD_DECRYPT_FAILED" for event in events))

    async def test_target_validation_dependency_failure_clears_apply_lock_and_publishes_failure(self) -> None:
        events = []
        connector = FailingValidationConnector()
        action_store = InMemoryActionStore()
        service = create_test_action_service(action_store=action_store, events=events, connector=connector)
        proposed = await service.create_proposed_action(IDENTITY, base_action_input())
        await service.approve_action(IDENTITY, action_decision_input(proposed["actionId"]))

        with self.assertRaises(OrchestrationError) as caught:
            await service.apply_action(IDENTITY, action_apply_input(proposed["actionId"], "idem_target_dependency"))
        replay = await service.apply_action(IDENTITY, action_apply_input(proposed["actionId"], "idem_target_dependency"))

        self.assertEqual(caught.exception.code, "TARGET_VALIDATION_FAILED")
        self.assertEqual(replay["status"], ACTION_STATUS.FAILED.value)
        self.assertEqual(replay["reasonCode"], "TARGET_VALIDATION_FAILED")
        self.assertEqual(connector.write_count, 0)
        persisted = action_store.get(proposed["actionId"])
        self.assertNotIn("applyLock", persisted)
        self.assertEqual(persisted["failureCode"], "TARGET_VALIDATION_FAILED")
        self.assertTrue(any(event.get("status") == ACTION_STATUS.FAILED.value and event.get("reasonCode") == "TARGET_VALIDATION_FAILED" for event in events))

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
            await service.reject_action(IDENTITY, action_decision_input(proposed["actionId"]))
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
            await service.approve_action(IDENTITY, action_decision_input(proposed["actionId"]))
        self.assertEqual(caught.exception.code, "ACTION_NOT_APPROVABLE")
        self.assertEqual(action_store.get(proposed["actionId"])["status"], ACTION_STATUS.REJECTED.value)

    async def test_reject_during_apply_in_progress_does_not_overwrite_apply_lock(self) -> None:
        gate = asyncio.Event()
        action_store = InMemoryActionStore()
        service = create_test_action_service(action_store=action_store, connector=BlockingConnector(gate))
        proposed = await service.create_proposed_action(IDENTITY, base_action_input())
        await service.approve_action(IDENTITY, action_decision_input(proposed["actionId"]))

        first = asyncio.create_task(service.apply_action(IDENTITY, action_apply_input(proposed["actionId"], "idem_reject_race")))
        await asyncio.sleep(0)
        with self.assertRaises(OrchestrationError) as caught:
            await service.reject_action(IDENTITY, action_decision_input(proposed["actionId"]))
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
        await service.approve_action(IDENTITY, action_decision_input(proposed["actionId"]))

        first = await service.apply_action(IDENTITY, action_apply_input(proposed["actionId"], "idem_conflict_first"))
        second = await service.apply_action(IDENTITY, action_apply_input(proposed["actionId"], "idem_conflict_second"))

        self.assertEqual(first, second)
        conflict_events = [
            event
            for event in events
            if event["type"] == "action.status_changed" and event["status"] == ACTION_STATUS.CONFLICTED.value
        ]
        self.assertEqual(len(conflict_events), 1)


def create_test_action_service(*, action_store, connector, events=None, id_generator=None, consent_service=None, payload_vault=None, token_service=None):
    return create_action_service(
        action_store=action_store,
        connector=connector,
        clock=lambda: NOW,
        id_generator=id_generator or (lambda _prefix: "action_001"),
        event_publisher=Publisher(events if events is not None else []),
        consent_service=consent_service or ConsentService(),
        payload_vault=payload_vault or PayloadVault(),
        token_service=token_service,
    )


def create_command_with_proposal_provider(*, action_store, events, proposals, id_generator=None):
    action_service = create_test_action_service(
        action_store=action_store,
        events=events,
        connector=RecordingConnector({"valid": True, "verifiedTarget": {}}, {"providerOperationId": "google_op_001"}),
        id_generator=id_generator,
    )
    return create_command_service(
        clock=lambda: NOW,
        policy_service=SimplePolicy({"decision": "ALLOW"}),
        context_service=SimpleContext(
            {
                "authorized": True,
                "resourceRef": {"provider": "google_docs", "resourceId": "doc_from_context"},
                "provenance": {"resourceVersion": "rev_from_context"},
            }
        ),
        provider_registry={"openai": ProposalProvider(proposals)},
        prompt_builder=SimplePromptBuilder(),
        event_publisher=Publisher(events),
        action_service=action_service,
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


def action_decision_input(action_id, **overrides):
    return {
        "actionId": action_id,
        "sessionId": "session_001",
        "resourceId": "doc_001",
        **overrides,
    }


def action_apply_input(action_id, idempotency_key, **overrides):
    return {**action_decision_input(action_id), "idempotencyKey": idempotency_key, **overrides}


def base_command_input():
    return {
        "commandId": "cmd_001",
        "requestId": "req_001",
        "correlationId": "corr_001",
        "sessionId": "session_001",
        "provider": "openai",
        "resourceId": "doc_001",
        "contextMode": "SELECTION",
        "secretRef": "secret_ref_001",
    }


class Publisher:
    def __init__(self, events):
        self.events = events

    async def publish(self, event):
        self.events.append(event)


class FailingPublisher:
    def __init__(self, *, fail_on_type):
        self.fail_on_type = fail_on_type

    async def publish(self, event):
        if event.get("type") == self.fail_on_type:
            raise RuntimeError("publisher unavailable")


class LegacyProvider:
    async def generate(self, _request):
        return {"messageId": "msg_001", "deltas": ["ok"], "finishReason": "stop", "usage": {}}


class ProposalProvider:
    def __init__(self, proposals):
        self.proposals = proposals

    async def stream(self, _request):
        yield {"type": "assistant.delta", "delta": "Proposal ready."}
        yield {"type": "assistant.final", "finishReason": "stop", "proposals": self.proposals}


class CountingIdGenerator:
    def __init__(self):
        self.next_id = 1

    def __call__(self, _prefix):
        action_id = f"action_{self.next_id:03d}"
        self.next_id += 1
        return action_id


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
    def __init__(self, response=None):
        self.response = response or {"allowed": True}

    async def validate_apply_consent(self, _request):
        return self.response


class TokenService:
    def __init__(self, response):
        self.response = response

    async def validate_apply_token(self, _request):
        return self.response


class PayloadVault:
    def __init__(self):
        self.payloads = {}
        self.next_id = 1

    async def encrypt(self, payload):
        ciphertext_ref = f"encrypted_payload_{self.next_id}"
        self.next_id += 1
        self.payloads[ciphertext_ref] = payload
        return {"ciphertextRef": ciphertext_ref}

    async def decrypt(self, encrypted_payload):
        return self.payloads[encrypted_payload["ciphertextRef"]]


class FailingPayloadVault(PayloadVault):
    async def decrypt(self, _encrypted_payload):
        raise RuntimeError("kms decrypt unavailable")


class RecordingConnector:
    def __init__(self, validation, apply_result):
        self.validation = validation
        self.apply_result = apply_result
        self.validation_count = 0
        self.write_count = 0

    async def validate_target(self, _action):
        self.validation_count += 1
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


class BlockingValidationConnector(RecordingConnector):
    def __init__(self, validation_gate):
        super().__init__({"valid": True, "verifiedTarget": {"revision": "rev_001"}}, {"providerOperationId": "google_op_001"})
        self.validation_gate = validation_gate

    async def validate_target(self, action):
        self.validation_count += 1
        await self.validation_gate.wait()
        return self.validation


class FailingValidationConnector(RecordingConnector):
    def __init__(self):
        super().__init__({"valid": True}, {"providerOperationId": "should_not_happen"})

    async def validate_target(self, _action):
        self.validation_count += 1
        raise RuntimeError("target validation unavailable")


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
