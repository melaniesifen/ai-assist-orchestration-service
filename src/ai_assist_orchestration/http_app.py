from __future__ import annotations

import os
from typing import Any

from .action_service import create_action_service
from .errors import OrchestrationError
from .http_adapter import create_http_command_boundary
from .http_runtime import JSON_CONTENT_TYPE, OrchestrationHttpRuntime
from .runtime_dependencies import (
    AllowingConsentService,
    JsonFileActionStore,
    NoopEventPublisher,
    UnconfiguredConnector,
    UnconfiguredPayloadVault,
)

NO_STORE_HEADERS = {
    "Cache-Control": "no-store",
    "Content-Type": JSON_CONTENT_TYPE,
}

ERROR_CODE_DEPLOYED_DEPENDENCIES_MISSING = "ORCHESTRATION_DEPENDENCIES_NOT_CONFIGURED"

_runtime: OrchestrationHttpRuntime | None = None


def handle_http_request(
    method: str,
    path: str,
    headers: dict[str, str] | None = None,
    query_string: str = "",
    body: str | bytes | None = None,
) -> dict[str, Any]:
    """Package-level dogfood runtime entrypoint for orchestration-owned HTTP routes."""
    del query_string
    response = _get_runtime().handle_request(
        {
            "method": method,
            "path": path,
            "headers": headers or {},
            "body": body,
        }
    )
    return with_no_store(response)


def configure_http_runtime(runtime: OrchestrationHttpRuntime | None) -> None:
    """Set or reset the process-local runtime used by the package-level handler."""
    global _runtime
    if runtime is not None and not isinstance(runtime, OrchestrationHttpRuntime):
        raise TypeError("runtime must be an OrchestrationHttpRuntime")
    _runtime = runtime


def configure_http_dependencies(
    *,
    command_service: Any | None = None,
    action_service: Any | None = None,
    provider_status_service: Any | None = None,
) -> None:
    """Configure package-level dogfood dependencies without replacing the runtime."""
    missing_dependencies = MissingDependencyService()
    runtime_action_service = action_service or create_default_action_service()
    configure_http_runtime(
        OrchestrationHttpRuntime(
            boundary=create_http_command_boundary(
                command_service=command_service or missing_dependencies,
                action_service=runtime_action_service,
            ),
            provider_status_service=provider_status_service or missing_dependencies,
        )
    )


def create_default_http_runtime() -> OrchestrationHttpRuntime:
    missing_dependencies = MissingDependencyService()
    return OrchestrationHttpRuntime(
        boundary=create_http_command_boundary(
            command_service=missing_dependencies,
            action_service=create_default_action_service(),
        ),
        provider_status_service=missing_dependencies,
    )


def create_default_action_service() -> Any:
    return create_action_service(
        action_store=JsonFileActionStore(os.environ.get("ORCHESTRATION_ACTION_STORE_PATH", "/tmp/ai-assist-orchestration/actions.json")),
        connector=UnconfiguredConnector(),
        event_publisher=NoopEventPublisher(),
        consent_service=AllowingConsentService(),
        payload_vault=UnconfiguredPayloadVault(),
    )


def _get_runtime() -> OrchestrationHttpRuntime:
    global _runtime
    if _runtime is None:
        _runtime = create_default_http_runtime()
    return _runtime


def with_no_store(response: dict[str, Any]) -> dict[str, Any]:
    response_headers = response.get("headers", {})
    if not isinstance(response_headers, dict):
        response_headers = {}
    return {
        "statusCode": response.get("statusCode", 500),
        "headers": {**NO_STORE_HEADERS, **response_headers, "Cache-Control": "no-store"},
        "body": response.get("body", {}),
    }


class MissingDependencyService:
    async def list_provider_status(self, _identity: dict, request: dict) -> dict:
        raise deployed_dependencies_missing("provider_status", request)

    async def run_assistant_command(self, _identity: dict, command: dict) -> dict:
        raise deployed_dependencies_missing("create_command", command)

    async def create_proposed_action(self, _identity: dict, input_data: dict) -> dict:
        raise deployed_dependencies_missing("create_action", input_data)

    async def list_actions(self, _identity: dict, input_data: dict) -> dict:
        raise deployed_dependencies_missing("list_actions", input_data)

    async def get_action(self, _identity: dict, input_data: dict) -> dict:
        raise deployed_dependencies_missing("get_action", input_data)

    async def approve_action(self, _identity: dict, input_data: dict) -> dict:
        raise deployed_dependencies_missing("approve_action", input_data)

    async def reject_action(self, _identity: dict, input_data: dict) -> dict:
        raise deployed_dependencies_missing("reject_action", input_data)

    async def apply_action(self, _identity: dict, input_data: dict) -> dict:
        raise deployed_dependencies_missing("apply_action", input_data)


def deployed_dependencies_missing(operation: str, payload: dict[str, Any]) -> OrchestrationError:
    metadata = {"operation": operation}
    session_id = payload.get("sessionId")
    action_id = payload.get("actionId")
    provider = payload.get("provider")
    resource_id = payload.get("resourceId")
    if isinstance(session_id, str) and session_id:
        metadata["sessionId"] = session_id
    if isinstance(action_id, str) and action_id:
        metadata["actionId"] = action_id
    if isinstance(provider, str) and provider:
        metadata["provider"] = provider
    if isinstance(resource_id, str) and resource_id:
        metadata["resourceId"] = resource_id
    return OrchestrationError(
        code=ERROR_CODE_DEPLOYED_DEPENDENCIES_MISSING,
        category="DEPENDENCY",
        message="Orchestration runtime dependencies are not configured",
        status_code=501,
        retryable=False,
        metadata=metadata,
    )
