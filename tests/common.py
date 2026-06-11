from __future__ import annotations

from datetime import datetime, timezone

from ai_assist_orchestration import (
    ACTION_STATUS,
    InMemoryActionStore,
    create_action_service,
    create_command_service,
)

IDENTITY = {"tenantId": "tenant_001", "userId": "user_001"}
NOW = datetime(2026, 5, 29, tzinfo=timezone.utc)


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
