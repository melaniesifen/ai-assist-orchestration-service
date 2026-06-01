from __future__ import annotations

from enum import StrEnum


class ActionStatus(StrEnum):
    PROPOSED = "PROPOSED"
    APPROVED = "APPROVED"
    APPLIED = "APPLIED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"
    CONFLICTED = "CONFLICTED"
    FAILED = "FAILED"


ACTION_STATUS = ActionStatus
TERMINAL_ACTION_STATUSES = frozenset(
    {
        ActionStatus.APPLIED,
        ActionStatus.REJECTED,
        ActionStatus.EXPIRED,
        ActionStatus.CONFLICTED,
        ActionStatus.FAILED,
    }
)
DEFAULT_ACTION_TTL_MS = 24 * 60 * 60 * 1000


def is_terminal_action_status(status: str | ActionStatus) -> bool:
    return ActionStatus(status) in TERMINAL_ACTION_STATUSES


def is_known_action_status(status: str | ActionStatus) -> bool:
    try:
        ActionStatus(status)
    except ValueError:
        return False
    return True


def is_expired(action: dict, now_ms: int) -> bool:
    return parse_iso_ms(action["expiresAt"]) <= now_ms


def parse_iso_ms(value: str) -> int:
    from datetime import datetime, timezone

    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)
