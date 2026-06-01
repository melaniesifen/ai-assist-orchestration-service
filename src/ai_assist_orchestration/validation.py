from __future__ import annotations

from typing import Any

from .errors import validation_error


def require_non_blank_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or len(value.strip()) == 0:
        raise validation_error("INVALID_FIELD", f"{field_name} must be a non-empty string", {"fieldName": field_name})
    return value


def require_object(value: Any, field_name: str) -> dict:
    if not isinstance(value, dict):
        raise validation_error("INVALID_FIELD", f"{field_name} must be an object", {"fieldName": field_name})
    return value


def assert_identity(identity: Any) -> dict:
    record = require_object(identity, "identity")
    require_non_blank_string(record.get("tenantId"), "identity.tenantId")
    require_non_blank_string(record.get("userId"), "identity.userId")
    return record


def assert_ownership(identity: dict, record: dict) -> bool:
    return record.get("tenantId") == identity.get("tenantId") and record.get("userId") == identity.get("userId")
