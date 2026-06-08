from __future__ import annotations

from collections.abc import AsyncIterable, Iterable, Mapping
from typing import Any, Callable

from .async_utils import maybe_await
from .errors import OrchestrationError, dependency_error, policy_error, validation_error
from .time_utils import isoformat_z, utc_now
from .validation import assert_identity, require_non_blank_string, require_object

EVENT_TYPE_ASSISTANT_DELTA = "assistant.delta"
EVENT_TYPE_ASSISTANT_FINAL = "assistant.final"
EVENT_TYPE_ERROR = "error"
EVENT_TYPE_PROGRESS = "progress"
EVENT_TYPE_ACTION_CREATING_STAGE = "action.creating"
PROPOSAL_BATCH_KEY = "proposalBatch"
PROPOSALS_KEY = "proposals"
ACTION_TYPE_INSERT_TEXT = "insert_text"
ACTION_TYPE_REPLACE_TEXT = "replace_text"
SUPPORTED_PROPOSAL_ACTION_TYPES = frozenset({ACTION_TYPE_INSERT_TEXT, ACTION_TYPE_REPLACE_TEXT})

ERROR_CODE_CONTEXT_UNAVAILABLE = "CONTEXT_UNAVAILABLE"
ERROR_CODE_EVENT_PUBLISH_FAILED = "EVENT_PUBLISH_FAILED"
ERROR_CODE_PROVIDER_STREAM_FAILED = "PROVIDER_STREAM_FAILED"

PROVIDER_ERROR_CATEGORY_MAP = {
    "authentication": "AUTHENTICATION",
    "authorization": "AUTHORIZATION",
    "content_filtered": "POLICY",
    "internal": "INTERNAL",
    "invalid_request": "VALIDATION",
    "model_unavailable": "DEPENDENCY",
    "quota": "PROVIDER_QUOTA",
    "rate_limited": "RATE_LIMITED",
    "timeout": "DEPENDENCY",
    "unavailable": "DEPENDENCY",
}
RETRYABLE_PROVIDER_CATEGORIES = {"DEPENDENCY", "PROVIDER_QUOTA", "RATE_LIMITED"}
PROVIDER_ERROR_STATUS_CODES = {
    "AUTHENTICATION": 401,
    "AUTHORIZATION": 403,
    "POLICY": 403,
    "PROVIDER_QUOTA": 429,
    "RATE_LIMITED": 429,
    "VALIDATION": 400,
}


class CommandService:
    def __init__(
        self,
        *,
        context_service: Any,
        provider_registry: Any,
        event_publisher: Any,
        policy_service: Any,
        prompt_builder: Any,
        action_service: Any | None = None,
        clock: Callable[[], Any] = utc_now,
    ) -> None:
        if not context_service or provider_registry is None or not event_publisher or not policy_service or not prompt_builder:
            raise TypeError("context_service, provider_registry, event_publisher, policy_service, and prompt_builder are required")
        self.context_service = context_service
        self.provider_registry = provider_registry
        self.event_publisher = event_publisher
        self.policy_service = policy_service
        self.prompt_builder = prompt_builder
        self.action_service = action_service
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

        await self._publish_progress(session_id, request_id, correlation_id, "context.loading", "started")
        try:
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
                    ERROR_CODE_CONTEXT_UNAVAILABLE,
                    "Context could not be resolved for this command",
                    {"sessionId": session_id, "reasonCode": (context or {}).get("reasonCode", ERROR_CODE_CONTEXT_UNAVAILABLE)},
                )
        except OrchestrationError as error:
            await self._publish_error(session_id, request_id, correlation_id, error)
            raise
        except Exception as error:
            safe_error = dependency_error(
                ERROR_CODE_CONTEXT_UNAVAILABLE,
                "Context could not be resolved for this command",
                {"sessionId": session_id, "dependencyStatus": "failed", "dependency": "context"},
            )
            await self._publish_error(session_id, request_id, correlation_id, safe_error)
            raise safe_error from error

        await self._publish_progress(session_id, request_id, correlation_id, "provider.generating", "in_progress")
        prompt = await maybe_await(self.prompt_builder.build_prompt({"command": command, "context": context}))
        provider_request = {
            "prompt": prompt,
            "context": context,
            "secretRef": command.get("secretRef"),
            "requestId": request_id,
            "correlationId": correlation_id,
        }
        try:
            final_event = None
            delta_index = 0
            message_id = command.get("messageId") or f"msg_{request_id}"
            async for provider_event in self._provider_stream_events(provider, provider_request):
                if provider_event.get("type") == EVENT_TYPE_ASSISTANT_DELTA:
                    provider_event.setdefault("messageId", message_id)
                    provider_event.setdefault("index", delta_index)
                    await self._publish_assistant_delta(session_id, request_id, correlation_id, provider_event)
                    delta_index += 1
                    continue
                if provider_event.get("type") == EVENT_TYPE_ASSISTANT_FINAL:
                    provider_event.setdefault("messageId", message_id)
                    final_event = provider_event
                    await self._publish_assistant_final(session_id, request_id, correlation_id, provider_event)
                    continue
                if provider_event.get("type") == EVENT_TYPE_ERROR:
                    error = self._provider_event_error(provider_event, session_id)
                    raise error

            if final_event is None:
                raise dependency_error(
                    ERROR_CODE_PROVIDER_STREAM_FAILED,
                    "Provider stream did not produce a final response",
                    {"sessionId": session_id, "provider": provider_name, "dependencyStatus": "malformed"},
                )
            created_actions = await self._create_proposed_actions_from_provider_output(
                identity,
                command,
                context,
                final_event,
            )
        except OrchestrationError as error:
            if error.code != ERROR_CODE_EVENT_PUBLISH_FAILED:
                await self._publish_error(session_id, request_id, correlation_id, error)
            raise
        except Exception as error:
            safe_error = dependency_error(
                ERROR_CODE_PROVIDER_STREAM_FAILED,
                "Provider stream failed for this command",
                {"sessionId": session_id, "provider": provider_name, "dependencyStatus": "failed"},
            )
            await self._publish_error(session_id, request_id, correlation_id, safe_error)
            raise safe_error from error

        return {
            "messageId": final_event.get("messageId"),
            "finishReason": final_event.get("finishReason"),
            "provider": provider_name,
            **({"proposedActionIds": [action["actionId"] for action in created_actions]} if created_actions else {}),
        }

    async def _create_proposed_actions_from_provider_output(
        self,
        identity: dict,
        command: dict,
        context: dict,
        final_event: dict,
    ) -> list[dict]:
        if self.action_service is None:
            return []
        proposals = provider_proposals(final_event)
        if not proposals:
            return []

        session_id = require_non_blank_string(command.get("sessionId"), "command.sessionId")
        request_id = require_non_blank_string(command.get("requestId"), "command.requestId")
        correlation_id = require_non_blank_string(command.get("correlationId"), "command.correlationId")
        resource_ref = resolve_resource_ref(command, context)
        resource_revision = resolve_resource_revision(context)
        action_inputs = normalize_provider_proposal_batch(
            proposals,
            session_id=session_id,
            request_id=request_id,
            correlation_id=correlation_id,
            resource_ref=resource_ref,
            resource_revision=resource_revision,
        )

        await self._publish_progress(session_id, request_id, correlation_id, EVENT_TYPE_ACTION_CREATING_STAGE, "in_progress")
        created_actions = []
        for action_input in action_inputs:
            action = await maybe_await(
                self.action_service.create_proposed_action(
                    identity,
                    action_input,
                )
            )
            created_actions.append(action)
        return created_actions

    async def _provider_stream_events(self, provider: Any, request: dict) -> Any:
        if hasattr(provider, "stream") and callable(provider.stream):
            stream = await maybe_await(provider.stream(request))
            async for event in _aiter(stream):
                if isinstance(event, Mapping):
                    yield dict(event)
            return

        response = await maybe_await(provider.generate(request))
        for index, delta in enumerate(response.get("deltas", [])):
            yield {
                "type": EVENT_TYPE_ASSISTANT_DELTA,
                "messageId": response.get("messageId"),
                "delta": delta,
                "index": index,
            }
        yield {
            "type": EVENT_TYPE_ASSISTANT_FINAL,
            "messageId": response.get("messageId"),
            "finishReason": response.get("finishReason"),
            "usage": response.get("usage"),
        }

    async def _publish_assistant_delta(self, session_id: str, request_id: str, correlation_id: str, event: dict) -> None:
        await self._publish_event(
            {
                "type": EVENT_TYPE_ASSISTANT_DELTA,
                "sessionId": session_id,
                "requestId": request_id,
                "correlationId": correlation_id,
                "messageId": event.get("messageId"),
                "delta": event.get("delta", ""),
                "index": event.get("index", 0),
            }
        )

    async def _publish_assistant_final(self, session_id: str, request_id: str, correlation_id: str, event: dict) -> None:
        await self._publish_event(
            {
                "type": EVENT_TYPE_ASSISTANT_FINAL,
                "sessionId": session_id,
                "requestId": request_id,
                "correlationId": correlation_id,
                "messageId": event.get("messageId"),
                "finishReason": event.get("finishReason"),
                "usage": event.get("usage"),
                "createdAt": isoformat_z(self.clock()),
            }
        )

    async def _publish_progress(self, session_id: str, request_id: str, correlation_id: str, stage: str, status: str) -> None:
        await self._publish_event(
            {
                "type": EVENT_TYPE_PROGRESS,
                "sessionId": session_id,
                "requestId": request_id,
                "correlationId": correlation_id,
                "stage": stage,
                "status": status,
                "messageCode": f"{stage}.{status}".upper(),
            }
        )

    async def _publish_error(self, session_id: str, request_id: str, correlation_id: str, error: OrchestrationError) -> None:
        await self._publish_event(
            {
                "type": EVENT_TYPE_ERROR,
                "sessionId": session_id,
                "requestId": request_id,
                "correlationId": correlation_id,
                "errorCode": error.code,
                "category": error.category,
                "retryable": error.retryable,
                "message": error.message,
                "metadata": _safe_error_metadata(error.metadata),
            }
        )

    async def _publish_event(self, event: dict) -> None:
        try:
            await maybe_await(self.event_publisher.publish(event))
        except OrchestrationError as error:
            raise dependency_error(
                ERROR_CODE_EVENT_PUBLISH_FAILED,
                "Session event could not be published",
                {
                    "eventType": event.get("type"),
                    "sessionId": event.get("sessionId"),
                    "publisherErrorCode": error.code,
                    "publisherErrorCategory": error.category,
                },
            ) from error
        except Exception as error:
            raise dependency_error(
                ERROR_CODE_EVENT_PUBLISH_FAILED,
                "Session event could not be published",
                {"eventType": event.get("type"), "sessionId": event.get("sessionId")},
            ) from error

    def _provider_event_error(self, event: dict, session_id: str) -> OrchestrationError:
        raw_error = event.get("error") if isinstance(event.get("error"), Mapping) else {}
        category = _provider_error_category(raw_error.get("category"))
        return OrchestrationError(
            code=raw_error.get("code") or ERROR_CODE_PROVIDER_STREAM_FAILED,
            category=category,
            message=raw_error.get("message") or "Provider stream failed for this command",
            retryable=category in RETRYABLE_PROVIDER_CATEGORIES,
            status_code=PROVIDER_ERROR_STATUS_CODES.get(category, 502),
            metadata={
                "sessionId": session_id,
                "provider": event.get("provider"),
                "model": event.get("model"),
                "dependencyStatus": raw_error.get("dependencyStatus", "failed"),
                "retryAfterSeconds": raw_error.get("retryAfterSeconds"),
            },
        )

    def _get_provider(self, provider_name: str) -> Any:
        if hasattr(self.provider_registry, "get"):
            return self.provider_registry.get(provider_name)
        return None


def create_command_service(**kwargs: Any) -> CommandService:
    return CommandService(**kwargs)


def provider_proposals(final_event: dict) -> list:
    batch = final_event.get(PROPOSAL_BATCH_KEY)
    if isinstance(batch, Mapping) and isinstance(batch.get(PROPOSALS_KEY), list):
        return batch[PROPOSALS_KEY]
    proposals = final_event.get(PROPOSALS_KEY)
    return proposals if isinstance(proposals, list) else []


def normalize_provider_proposal_batch(
    proposals: list,
    *,
    session_id: str,
    request_id: str,
    correlation_id: str,
    resource_ref: dict,
    resource_revision: str,
) -> list[dict]:
    normalized = [
        normalize_provider_proposal(
            proposal,
            index=index,
            session_id=session_id,
            request_id=request_id,
            correlation_id=correlation_id,
            resource_ref=resource_ref,
            resource_revision=resource_revision,
        )
        for index, proposal in enumerate(proposals)
    ]
    reject_overlapping_replace_ranges(normalized)
    return normalized


def normalize_provider_proposal(
    proposal: Any,
    *,
    index: int,
    session_id: str,
    request_id: str,
    correlation_id: str,
    resource_ref: dict,
    resource_revision: str,
) -> dict:
    proposal = require_object(proposal, f"providerProposal[{index}]")
    target_hint = proposal.get("targetHint") if isinstance(proposal.get("targetHint"), Mapping) else {}
    action_type = proposal.get("actionType", ACTION_TYPE_REPLACE_TEXT)
    if action_type not in SUPPORTED_PROPOSAL_ACTION_TYPES:
        raise validation_error(
            "UNSUPPORTED_ACTION_TYPE",
            "Provider proposal action type is not supported",
            {"actionType": action_type, "proposalIndex": index},
        )

    target_range = target_hint.get("targetRange") or proposal.get("targetRange")
    target_anchor = target_hint.get("targetAnchor") or proposal.get("targetAnchor")
    if target_range is not None and not is_valid_target_range(target_range):
        raise validation_error("INVALID_ACTION_TARGET", "Provider proposal targetRange is malformed", {"proposalIndex": index})
    if target_anchor is not None and not is_valid_target_anchor(target_anchor):
        raise validation_error("INVALID_ACTION_TARGET", "Provider proposal targetAnchor is malformed", {"proposalIndex": index})
    if target_range is None and target_anchor is None:
        raise validation_error("INVALID_ACTION_TARGET", "Provider proposal target is required", {"proposalIndex": index})

    original_text_hash = target_hint.get("originalTextHash") or proposal.get("originalTextHash")
    if action_type == ACTION_TYPE_REPLACE_TEXT:
        original_text_hash = require_non_blank_string(original_text_hash, "providerProposal.originalTextHash")

    return {
        "requestId": request_id,
        "correlationId": correlation_id,
        "sessionId": session_id,
        "provider": resource_ref["provider"],
        "resourceId": resource_ref["resourceId"],
        "resourceRevision": proposal.get("resourceRevision") or resource_revision,
        "targetAnchor": target_anchor,
        "targetRange": target_range,
        "originalTextHash": original_text_hash or "sha256:not-required-for-insert",
        "actionType": action_type,
        "summary": proposal_safe_summary(proposal),
        "payload": {
            "proposalId": proposal.get("proposalId"),
            "currentText": proposal.get("currentText"),
            "proposedText": proposal.get("proposedText"),
            "surroundingText": proposal.get("surroundingText"),
            "rationale": proposal.get("rationale"),
        },
    }


def reject_overlapping_replace_ranges(action_inputs: list[dict]) -> None:
    ranges = []
    for index, action_input in enumerate(action_inputs):
        if action_input["actionType"] != ACTION_TYPE_REPLACE_TEXT:
            continue
        target_range = action_input.get("targetRange")
        if target_range is None:
            continue
        ranges.append((target_range["start"], target_range["end"], index))
    sorted_ranges = sorted(ranges)
    for previous, current in zip(sorted_ranges, sorted_ranges[1:]):
        if current[0] < previous[1]:
            raise validation_error(
                "OVERLAPPING_ACTION_TARGETS",
                "Provider proposal target ranges overlap",
                {"proposalIndex": current[2], "overlapsProposalIndex": previous[2]},
            )


def is_valid_target_anchor(value: Any) -> bool:
    return isinstance(value, Mapping) and any(isinstance(item, str) and item.strip() for item in value.values())


def is_valid_target_range(value: Any) -> bool:
    if not isinstance(value, Mapping):
        return False
    start = value.get("start")
    end = value.get("end")
    return isinstance(start, int) and isinstance(end, int) and start >= 0 and end > start


def resolve_resource_ref(command: dict, context: dict) -> dict:
    resource_ref = context.get("resourceRef") if isinstance(context.get("resourceRef"), Mapping) else {}
    provider = resource_ref.get("provider") or command.get("resourceProvider")
    resource_id = resource_ref.get("resourceId") or command.get("resourceId")
    return {
        "provider": require_non_blank_string(provider, "context.resourceRef.provider"),
        "resourceId": require_non_blank_string(resource_id, "context.resourceRef.resourceId"),
    }


def resolve_resource_revision(context: dict) -> str:
    provenance = context.get("provenance") if isinstance(context.get("provenance"), Mapping) else {}
    return require_non_blank_string(
        context.get("resourceRevision") or provenance.get("resourceRevision") or provenance.get("resourceVersion"),
        "context.resourceRevision",
    )


def proposal_safe_summary(proposal: dict) -> str:
    action_type = proposal.get("actionType")
    if action_type == "insert_text":
        return "Insert text proposal"
    return "Replace text proposal"


async def _aiter(value: Any) -> Any:
    if isinstance(value, AsyncIterable) or hasattr(value, "__aiter__"):
        async for item in value:
            yield item
        return
    if isinstance(value, Iterable) and not isinstance(value, (str, bytes, Mapping)):
        for item in value:
            yield item
        return
    raise TypeError("Provider stream must return an iterable.")


def _safe_error_metadata(metadata: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in metadata.items()
        if key
        in {
            "dependency",
            "dependencyStatus",
            "model",
            "provider",
            "reasonCode",
            "retryAfterSeconds",
            "sessionId",
        }
    }


def _provider_error_category(category: Any) -> str:
    if not isinstance(category, str) or not category.strip():
        return "DEPENDENCY"
    normalized = category.strip()
    return PROVIDER_ERROR_CATEGORY_MAP.get(normalized.lower(), normalized.upper())
