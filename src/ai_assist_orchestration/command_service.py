from __future__ import annotations

from typing import Any, Callable

from .async_utils import maybe_await
from .errors import policy_error, validation_error
from .time_utils import isoformat_z, utc_now
from .validation import assert_identity, require_non_blank_string, require_object


class CommandService:
    def __init__(
        self,
        *,
        context_service: Any,
        provider_registry: Any,
        event_publisher: Any,
        policy_service: Any,
        prompt_builder: Any,
        clock: Callable[[], Any] = utc_now,
    ) -> None:
        if not context_service or provider_registry is None or not event_publisher or not policy_service or not prompt_builder:
            raise TypeError("context_service, provider_registry, event_publisher, policy_service, and prompt_builder are required")
        self.context_service = context_service
        self.provider_registry = provider_registry
        self.event_publisher = event_publisher
        self.policy_service = policy_service
        self.prompt_builder = prompt_builder
        self.clock = clock

    async def run_assistant_command(self, identity: dict, command: dict) -> dict:
        assert_identity(identity)
        require_object(command, "command")
        request_id = require_non_blank_string(command.get("requestId"), "command.requestId")
        correlation_id = require_non_blank_string(command.get("correlationId"), "command.correlationId")
        session_id = require_non_blank_string(command.get("sessionId"), "command.sessionId")
        provider_name = require_non_blank_string(command.get("provider"), "command.provider")

        policy = await maybe_await(
            self.policy_service.evaluate(
                {
                    "tenantId": identity["tenantId"],
                    "userId": identity["userId"],
                    "requestId": request_id,
                    "correlationId": correlation_id,
                    "subjectKind": "assistant_command",
                    "metadata": {
                        "sessionId": session_id,
                        "provider": provider_name,
                        "contextMode": command.get("contextMode"),
                    },
                }
            )
        )
        if policy.get("decision") != "ALLOW":
            raise policy_error(
                "POLICY_BLOCKED",
                "Policy blocked the command",
                {"policyDecisionId": policy.get("decisionId"), "reasonCode": policy.get("reasonCode")},
            )

        provider = self._get_provider(provider_name)
        if provider is None:
            raise validation_error("PROVIDER_UNSUPPORTED", "Provider is not configured", {"provider": provider_name})

        await self._publish_progress(session_id, request_id, correlation_id, "context.loading", "STARTED")
        context = await maybe_await(
            self.context_service.resolve_context(
                {
                    "tenantId": identity["tenantId"],
                    "userId": identity["userId"],
                    "sessionId": session_id,
                    "resourceId": command.get("resourceId"),
                    "contextMode": command.get("contextMode"),
                }
            )
        )
        if not context or not context.get("authorized"):
            raise validation_error(
                "CONTEXT_UNAVAILABLE",
                "Context could not be resolved for this command",
                {"sessionId": session_id, "reasonCode": (context or {}).get("reasonCode", "CONTEXT_UNAVAILABLE")},
            )

        await self._publish_progress(session_id, request_id, correlation_id, "provider.generating", "STARTED")
        prompt = await maybe_await(self.prompt_builder.build_prompt({"command": command, "context": context}))
        response = await maybe_await(
            provider.generate(
                {
                    "prompt": prompt,
                    "context": context,
                    "secretRef": command.get("secretRef"),
                    "requestId": request_id,
                    "correlationId": correlation_id,
                }
            )
        )

        for index, delta in enumerate(response.get("deltas", [])):
            await maybe_await(
                self.event_publisher.publish(
                    {
                        "type": "assistant.delta",
                        "sessionId": session_id,
                        "requestId": request_id,
                        "correlationId": correlation_id,
                        "messageId": response.get("messageId"),
                        "delta": delta,
                        "index": index,
                    }
                )
            )

        await maybe_await(
            self.event_publisher.publish(
                {
                    "type": "assistant.final",
                    "sessionId": session_id,
                    "requestId": request_id,
                    "correlationId": correlation_id,
                    "messageId": response.get("messageId"),
                    "finishReason": response.get("finishReason"),
                    "usage": response.get("usage"),
                    "createdAt": isoformat_z(self.clock()),
                }
            )
        )

        return {
            "messageId": response.get("messageId"),
            "finishReason": response.get("finishReason"),
            "provider": provider_name,
        }

    async def _publish_progress(self, session_id: str, request_id: str, correlation_id: str, stage: str, status: str) -> None:
        await maybe_await(
            self.event_publisher.publish(
                {
                    "type": "progress",
                    "sessionId": session_id,
                    "requestId": request_id,
                    "correlationId": correlation_id,
                    "stage": stage,
                    "status": status,
                    "messageCode": f"{stage}.{status}".upper(),
                }
            )
        )

    def _get_provider(self, provider_name: str) -> Any:
        if hasattr(self.provider_registry, "get"):
            return self.provider_registry.get(provider_name)
        return None


def create_command_service(**kwargs: Any) -> CommandService:
    return CommandService(**kwargs)
