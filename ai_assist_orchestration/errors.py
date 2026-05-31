from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class OrchestrationError(Exception):
    code: str
    category: str
    message: str
    retryable: bool = False
    status_code: int = 400
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        Exception.__init__(self, self.message)


def validation_error(code: str, message: str, metadata: dict[str, Any] | None = None) -> OrchestrationError:
    return OrchestrationError(code=code, category="VALIDATION", message=message, status_code=400, metadata=metadata or {})


def authorization_error(code: str, message: str, metadata: dict[str, Any] | None = None) -> OrchestrationError:
    return OrchestrationError(code=code, category="AUTHORIZATION", message=message, status_code=403, metadata=metadata or {})


def conflict_error(code: str, message: str, metadata: dict[str, Any] | None = None) -> OrchestrationError:
    return OrchestrationError(code=code, category="CONFLICT", message=message, status_code=409, metadata=metadata or {})


def policy_error(code: str, message: str, metadata: dict[str, Any] | None = None) -> OrchestrationError:
    return OrchestrationError(code=code, category="POLICY", message=message, status_code=403, metadata=metadata or {})


def dependency_error(code: str, message: str, metadata: dict[str, Any] | None = None) -> OrchestrationError:
    return OrchestrationError(code=code, category="DEPENDENCY", message=message, status_code=502, metadata=metadata or {})
