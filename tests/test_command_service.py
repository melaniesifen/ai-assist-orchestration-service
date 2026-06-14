from __future__ import annotations

import unittest

from ai_assist_orchestration import ACTION_STATUS, InMemoryActionStore, OrchestrationError, create_command_service

from common import (
    IDENTITY,
    NOW,
    CountingContext,
    CountingIdGenerator,
    FailingPublisher,
    LegacyProvider,
    Publisher,
    RecordingConnector,
    SimpleContext,
    SimplePolicy,
    SimplePromptBuilder,
    base_command_input,
    create_command_with_proposal_provider,
    create_test_action_service,
)


class CommandServiceTests(unittest.IsolatedAsyncioTestCase):
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

        command = base_command_input()
        command.pop("secretRef", None)

        result = await service.run_assistant_command(IDENTITY, command)

        self.assertEqual(result, {"messageId": "msg_req_001", "finishReason": "stop", "provider": "openai"})
        self.assertEqual([event["type"] for event in events], ["progress", "progress", "assistant.delta", "assistant.delta", "assistant.final"])
        self.assertEqual(events[2]["messageId"], "msg_req_001")
        self.assertEqual(events[2]["index"], 0)
        self.assertEqual(events[3]["index"], 1)
        self.assertEqual(events[4]["usage"], {"inputTokens": 2, "outputTokens": 3, "totalTokens": 5})
        self.assertEqual(observed_provider_request["context"]["contextMode"], "SELECTION")
        self.assertTrue(observed_provider_request["context"]["provenance"]["connectorVerified"])
        self.assertEqual(observed_provider_request["providerAccess"], {"source": "platform", "reference": None})
        self.assertNotIn("secretRef", observed_provider_request)

    async def test_published_command_events_include_session_event_envelope_fields(self) -> None:
        events = []
        event_ids = iter(["evt_001", "evt_002", "evt_003", "evt_004"])
        service = create_command_service(
            clock=lambda: NOW,
            event_id_generator=lambda: next(event_ids),
            policy_service=SimplePolicy({"decision": "ALLOW", "decisionId": "pol_envelope"}),
            context_service=SimpleContext({"authorized": True}),
            provider_registry={"openai": LegacyProvider()},
            prompt_builder=SimplePromptBuilder(),
            event_publisher=Publisher(events),
        )

        await service.run_assistant_command(IDENTITY, base_command_input())

        self.assertEqual([event["eventId"] for event in events], ["evt_001", "evt_002", "evt_003", "evt_004"])
        self.assertEqual([event["sequence"] for event in events], [1, 2, 3, 4])
        for event in events:
            self.assertEqual(event["tenantId"], IDENTITY["tenantId"])
            self.assertEqual(event["userId"], IDENTITY["userId"])
            self.assertEqual(event["sessionId"], "session_001")
            self.assertEqual(event["requestId"], "req_001")
            self.assertEqual(event["correlationId"], "corr_001")
            self.assertIn("createdAt", event)
            self.assertIsInstance(event["payload"], dict)
            self.assertNotIn("tenantId", event["payload"])
            self.assertNotIn("userId", event["payload"])

    async def test_provider_handoff_uses_optional_byo_secret_reference_only_when_explicit(self) -> None:
        observed_provider_request = {}

        class StreamProvider:
            async def stream(self, request):
                observed_provider_request.update(request)
                yield {"type": "assistant.final", "finishReason": "stop"}

        service = create_command_service(
            clock=lambda: NOW,
            policy_service=SimplePolicy({"decision": "ALLOW", "decisionId": "pol_byo"}),
            context_service=SimpleContext({"authorized": True}),
            provider_registry={"openai": StreamProvider()},
            prompt_builder=SimplePromptBuilder(),
            event_publisher=Publisher([]),
        )

        await service.run_assistant_command(
            IDENTITY,
            {
                **base_command_input(),
                "secretRef": "secret_001",
            },
        )

        self.assertEqual(observed_provider_request["providerAccess"], {"source": "byo", "secretRef": "secret_001"})
        self.assertNotIn("credential", observed_provider_request)

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
