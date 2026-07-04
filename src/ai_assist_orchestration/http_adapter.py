from __future__ import annotations

from typing import Any, Callable
from uuid import uuid4

from .action_service import public_action_view
from .errors import OrchestrationError
from .validation import assert_identity, require_non_blank_string, require_object

HEADER_CORRELATION_ID = "x-correlation-id"
HEADER_IDEMPOTENCY_KEY = "idempotency-key"
HEADER_REQUEST_ID = "x-request-id"


class HttpCommandBoundary:
    """Framework-neutral HTTP command adapter for orchestration routes."""

    def __init__(
        self,
        *,
        command_service: Any,
        action_service: Any,
        request_id_generator: Callable[[], str] | None = None,
        correlation_id_generator: Callable[[], str] | None = None,
    ) -> None:
        if not command_service or not action_service:
            raise TypeError("command_service and action_service are required")
        self.command_service = command_service
        self.action_service = action_service
        self.request_id_generator = request_id_generator or (lambda: f"req_{uuid4().hex}")
        self.correlation_id_generator = correlation_id_generator or (lambda: f"corr_{uuid4().hex}")

    async def create_command(self, request: dict) -> dict:
        context = None
        try:
            context = self.request_context(request)
            body = require_object(request.get("body"), "body")
            idempotency_key = require_idempotency_key(context.headers)
            command = without_client_identity(
                {
                    **body,
                    "requestId": context.request_id,
                    "correlationId": context.correlation_id,
                    "idempotencyKey": idempotency_key,
                }
            )
            result = await self.command_service.run_assistant_command(context.identity, command)
            return success_response(202, result, context)
        except OrchestrationError as error:
            return error_response(error, context or self._error_context(request))

    async def approve_action(self, request: dict) -> dict:
        return await self._action_response(
            request,
            operation=lambda identity, body, context: self.action_service.approve_action(
                identity,
                {
                    "actionId": body.get("actionId"),
                    "sessionId": body.get("sessionId"),
                    "resourceId": body.get("resourceId"),
                    "requestId": context.request_id,
                    "correlationId": context.correlation_id,
                },
            ),
        )

    async def create_action(self, request: dict) -> dict:
        context = None
        try:
            context = self.request_context(request)
            body = without_client_identity(require_object(request.get("body"), "body"))
            result = await self.action_service.create_proposed_action(
                context.identity,
                {
                    **body,
                    "requestId": context.request_id,
                    "correlationId": context.correlation_id,
                },
            )
            return success_response(201, public_action_view(result), context)
        except OrchestrationError as error:
            return error_response(error, context or self._error_context(request))

    async def list_actions(self, request: dict) -> dict:
        context = None
        try:
            context = self.request_context(request)
            body = without_client_identity(require_object(request.get("body"), "body"))
            result = await self.action_service.list_actions(
                context.identity,
                {
                    "sessionId": body.get("sessionId"),
                    "requestId": context.request_id,
                    "correlationId": context.correlation_id,
                },
            )
            return success_response(200, result, context)
        except OrchestrationError as error:
            return error_response(error, context or self._error_context(request))

    async def get_action(self, request: dict) -> dict:
        context = None
        try:
            context = self.request_context(request)
            body = without_client_identity(require_object(request.get("body"), "body"))
            result = await self.action_service.get_action(
                context.identity,
                {
                    "actionId": body.get("actionId"),
                    "sessionId": body.get("sessionId"),
                    "resourceId": body.get("resourceId"),
                    "requestId": context.request_id,
                    "correlationId": context.correlation_id,
                },
            )
            return success_response(200, result, context)
        except OrchestrationError as error:
            return error_response(error, context or self._error_context(request))

    async def reject_action(self, request: dict) -> dict:
        return await self._action_response(
            request,
            operation=lambda identity, body, context: self.action_service.reject_action(
                identity,
                {
                    "actionId": body.get("actionId"),
                    "sessionId": body.get("sessionId"),
                    "resourceId": body.get("resourceId"),
                    "reasonCode": body.get("reasonCode", "USER_REJECTED"),
                    "requestId": context.request_id,
                    "correlationId": context.correlation_id,
                },
            ),
        )

    async def apply_action(self, request: dict) -> dict:
        context = None
        try:
            context = self.request_context(request)
            body = require_object(request.get("body"), "body")
            result = await self.action_service.apply_action(
                context.identity,
                {
                    "actionId": body.get("actionId"),
                    "sessionId": body.get("sessionId"),
                    "resourceId": body.get("resourceId"),
                    "idempotencyKey": require_idempotency_key(context.headers),
                    "requestId": context.request_id,
                    "correlationId": context.correlation_id,
                },
            )
            return success_response(200, result, context)
        except OrchestrationError as error:
            return error_response(error, context or self._error_context(request))

    def request_context(self, request: dict) -> "HttpRequestContext":
        return self._request_context(request)

    async def _action_response(self, request: dict, *, operation: Callable[[dict, dict, HttpRequestContext], Any]) -> dict:
        context = None
        try:
            context = self.request_context(request)
            body = require_object(request.get("body"), "body")
            result = await operation(context.identity, without_client_identity(body), context)
            return success_response(200, public_action_view(result), context)
        except OrchestrationError as error:
            return error_response(error, context or self._error_context(request))

    def _request_context(self, request: dict) -> "HttpRequestContext":
        request = require_object(request, "request")
        headers = normalize_headers(request.get("headers", {}))
        identity = assert_identity(require_object(request.get("auth"), "auth"))
        return HttpRequestContext(
            identity=identity,
            headers=headers,
            request_id=header_or_generated(headers, HEADER_REQUEST_ID, self.request_id_generator),
            correlation_id=header_or_generated(headers, HEADER_CORRELATION_ID, self.correlation_id_generator),
        )

    def _error_context(self, request: Any) -> "HttpRequestContext":
        try:
            headers = normalize_headers(request.get("headers", {}) if isinstance(request, dict) else {})
        except OrchestrationError:
            headers = {}
        return HttpRequestContext(
            identity={},
            headers=headers,
            request_id=header_or_generated(headers, HEADER_REQUEST_ID, self.request_id_generator),
            correlation_id=header_or_generated(headers, HEADER_CORRELATION_ID, self.correlation_id_generator),
        )


class HttpRequestContext:
    def __init__(self, *, identity: dict, headers: dict[str, str], request_id: str, correlation_id: str) -> None:
        self.identity = identity
        self.headers = headers
        self.request_id = request_id
        self.correlation_id = correlation_id


def create_http_command_boundary(**kwargs: Any) -> HttpCommandBoundary:
    return HttpCommandBoundary(**kwargs)


def normalize_headers(headers: Any) -> dict[str, str]:
    if headers is None:
        return {}
    raw_headers = require_object(headers, "headers")
    normalized: dict[str, str] = {}
    for key, value in raw_headers.items():
        if isinstance(key, str) and isinstance(value, str):
            normalized[key.lower()] = value
    return normalized


def header_or_generated(headers: dict[str, str], header_name: str, generator: Callable[[], str]) -> str:
    value = headers.get(header_name)
    if isinstance(value, str) and value.strip():
        return value
    generated = generator()
    return require_non_blank_string(generated, header_name)


def require_idempotency_key(headers: dict[str, str]) -> str:
    return require_non_blank_string(headers.get(HEADER_IDEMPOTENCY_KEY), "headers.idempotency-key")


def without_client_identity(payload: dict) -> dict:
    clean = dict(payload)
    clean.pop("tenantId", None)
    clean.pop("userId", None)
    return clean


def success_response(status_code: int, body: dict, context: HttpRequestContext) -> dict:
    return {
        "statusCode": status_code,
        "headers": response_headers(context),
        "body": {
            "data": body,
            "requestId": context.request_id,
            "correlationId": context.correlation_id,
        },
    }


def error_response(error: OrchestrationError, context: HttpRequestContext) -> dict:
    return {
        "statusCode": error.status_code,
        "headers": response_headers(context),
        "body": {
            "error": {
                "code": error.code,
                "category": error.category,
                "message": error.message,
                "retryable": error.retryable,
                "metadata": error.metadata,
            },
            "requestId": context.request_id,
            "correlationId": context.correlation_id,
        },
    }


def response_headers(context: HttpRequestContext) -> dict[str, str]:
    return {
        "X-Request-Id": context.request_id,
        "X-Correlation-Id": context.correlation_id,
    }
