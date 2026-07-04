from __future__ import annotations

import asyncio
import json
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable

from .errors import OrchestrationError, validation_error
from .http_adapter import HttpCommandBoundary, normalize_headers
from .validation import require_non_blank_string

HEADER_TENANT_ID = "x-ai-assist-tenant-id"
HEADER_USER_ID = "x-ai-assist-user-id"
JSON_CONTENT_TYPE = "application/json; charset=utf-8"
MAX_BODY_BYTES = 256 * 1024

COMMAND_ROUTE = re.compile(r"^/resource-sessions/(?P<session_id>[^/]+)/commands$")
APPROVE_ROUTE = re.compile(r"^/resource-sessions/(?P<session_id>[^/]+)/actions/(?P<action_id>[^/]+)/approve$")
REJECT_ROUTE = re.compile(r"^/resource-sessions/(?P<session_id>[^/]+)/actions/(?P<action_id>[^/]+)/reject$")
APPLY_ROUTE = re.compile(r"^/resource-sessions/(?P<session_id>[^/]+)/apply-action$")


class OrchestrationHttpRuntime:
    """Stdlib HTTP runtime adapter over the framework-neutral command boundary."""

    def __init__(
        self,
        *,
        boundary: HttpCommandBoundary,
        resolve_auth: Callable[[dict[str, Any]], dict[str, str]] | None = None,
        max_body_bytes: int = MAX_BODY_BYTES,
    ) -> None:
        if not isinstance(boundary, HttpCommandBoundary):
            raise TypeError("boundary must be an HttpCommandBoundary")
        if not isinstance(max_body_bytes, int) or max_body_bytes < 1:
            raise TypeError("max_body_bytes must be a positive integer")
        self.boundary = boundary
        self.resolve_auth = resolve_auth or trusted_header_identity
        self.max_body_bytes = max_body_bytes

    def handle_request(self, request: dict[str, Any]) -> dict[str, Any]:
        try:
            routed = self._route_request(request)
            return asyncio.run(routed["operation"](routed["boundaryRequest"]))
        except OrchestrationError as error:
            return runtime_error_response(error)
        except Exception:
            return runtime_error_response(
                OrchestrationError(
                    code="HTTP_RUNTIME_FAILED",
                    category="INTERNAL",
                    message="Request could not be processed",
                    status_code=500,
                    retryable=False,
                )
            )

    def _route_request(self, request: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(request, dict):
            raise validation_error("INVALID_HTTP_REQUEST", "HTTP request must be an object")
        method = require_non_blank_string(request.get("method"), "method").upper()
        path = require_non_blank_string(request.get("path"), "path")
        headers = normalize_headers(request.get("headers", {}))
        auth = self.resolve_auth({"method": method, "path": path, "headers": headers})
        body = parse_json_body(request.get("body", b""), max_body_bytes=self.max_body_bytes)

        if method == "POST" and (match := COMMAND_ROUTE.match(path)):
            return {
                "operation": self.boundary.create_command,
                "boundaryRequest": boundary_request(auth, headers, body, session_id=match.group("session_id")),
            }
        if method == "POST" and (match := APPROVE_ROUTE.match(path)):
            return {
                "operation": self.boundary.approve_action,
                "boundaryRequest": boundary_request(
                    auth,
                    headers,
                    body,
                    session_id=match.group("session_id"),
                    action_id=match.group("action_id"),
                ),
            }
        if method == "POST" and (match := REJECT_ROUTE.match(path)):
            return {
                "operation": self.boundary.reject_action,
                "boundaryRequest": boundary_request(
                    auth,
                    headers,
                    body,
                    session_id=match.group("session_id"),
                    action_id=match.group("action_id"),
                ),
            }
        if method == "POST" and (match := APPLY_ROUTE.match(path)):
            return {
                "operation": self.boundary.apply_action,
                "boundaryRequest": boundary_request(auth, headers, body, session_id=match.group("session_id")),
            }
        raise OrchestrationError(
            code="ROUTE_NOT_FOUND",
            category="VALIDATION",
            message="Route is not handled by orchestration",
            status_code=404,
            metadata={"route": path},
        )


def trusted_header_identity(request: dict[str, Any]) -> dict[str, str]:
    headers = normalize_headers(request.get("headers", {}))
    tenant_id = headers.get(HEADER_TENANT_ID)
    user_id = headers.get(HEADER_USER_ID)
    if not tenant_id or not tenant_id.strip() or not user_id or not user_id.strip():
        raise OrchestrationError(
            code="AUTH_CONTEXT_REQUIRED",
            category="AUTHENTICATION",
            message="Authenticated tenant and user context is required",
            status_code=401,
        )
    return {"tenantId": tenant_id, "userId": user_id}


def boundary_request(
    auth: dict[str, str],
    headers: dict[str, str],
    body: dict[str, Any],
    *,
    session_id: str,
    action_id: str | None = None,
) -> dict[str, Any]:
    merged_body = dict(body)
    if "sessionId" in merged_body and merged_body["sessionId"] != session_id:
        raise OrchestrationError(
            code="SESSION_ROUTE_BODY_MISMATCH",
            category="AUTHORIZATION",
            message="Request body session does not match route session",
            status_code=403,
            metadata={"sessionId": session_id},
        )
    merged_body["sessionId"] = session_id
    if action_id is not None:
        if "actionId" in merged_body and merged_body["actionId"] != action_id:
            raise OrchestrationError(
                code="ACTION_ROUTE_BODY_MISMATCH",
                category="AUTHORIZATION",
                message="Request body action does not match route action",
                status_code=403,
                metadata={"actionId": action_id},
            )
        merged_body["actionId"] = action_id
    return {"auth": auth, "headers": headers, "body": merged_body}


def parse_json_body(body: Any, *, max_body_bytes: int) -> dict[str, Any]:
    if body is None or body == b"" or body == "":
        return {}
    if isinstance(body, str):
        raw = body.encode("utf-8")
    elif isinstance(body, bytes):
        raw = body
    else:
        raise validation_error("INVALID_HTTP_BODY", "HTTP request body must be JSON bytes or string")
    if len(raw) > max_body_bytes:
        raise OrchestrationError(
            code="HTTP_BODY_TOO_LARGE",
            category="VALIDATION",
            message="HTTP request body is too large",
            status_code=413,
            metadata={"maxBodyBytes": max_body_bytes},
        )
    try:
        parsed = json.loads(raw.decode("utf-8"))
    except Exception as error:
        raise validation_error("INVALID_JSON", "HTTP request body must be valid JSON") from error
    if not isinstance(parsed, dict):
        raise validation_error("INVALID_JSON_BODY", "HTTP request body must be a JSON object")
    return parsed


def runtime_error_response(error: OrchestrationError) -> dict[str, Any]:
    return {
        "statusCode": error.status_code,
        "headers": {"Content-Type": JSON_CONTENT_TYPE},
        "body": {
            "error": {
                "code": error.code,
                "category": error.category,
                "message": error.message,
                "retryable": error.retryable,
                "metadata": error.metadata,
            }
        },
    }


def create_http_handler(runtime: OrchestrationHttpRuntime) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802 - stdlib handler method name
            length_header = self.headers.get("content-length", "0")
            try:
                body = self.rfile.read(int(length_header))
            except Exception:
                body = b""
            response = runtime.handle_request(
                {
                    "method": self.command,
                    "path": self.path.split("?", 1)[0],
                    "headers": dict(self.headers.items()),
                    "body": body,
                }
            )
            write_json_response(self, response)

        def do_GET(self) -> None:  # noqa: N802
            write_json_response(
                self,
                runtime_error_response(
                    OrchestrationError(
                        code="ROUTE_NOT_FOUND",
                        category="VALIDATION",
                        message="Route is not handled by orchestration",
                        status_code=404,
                    )
                ),
            )

        def log_message(self, _format: str, *_args: Any) -> None:
            return

    return Handler


def write_json_response(handler: BaseHTTPRequestHandler, response: dict[str, Any]) -> None:
    status_code = int(response.get("statusCode", 500))
    body_bytes = json.dumps(response.get("body", {}), separators=(",", ":")).encode("utf-8")
    handler.send_response(status_code)
    headers = {"Content-Type": JSON_CONTENT_TYPE, **response.get("headers", {})}
    for key, value in headers.items():
        handler.send_header(key, value)
    handler.send_header("Content-Length", str(len(body_bytes)))
    handler.end_headers()
    handler.wfile.write(body_bytes)


def serve_http(runtime: OrchestrationHttpRuntime, *, host: str = "127.0.0.1", port: int = 8080) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer((host, port), create_http_handler(runtime))
    server.serve_forever()
    return server
