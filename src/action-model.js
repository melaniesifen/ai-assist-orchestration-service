export const ACTION_STATUS = Object.freeze({
  PROPOSED: "PROPOSED",
  APPROVED: "APPROVED",
  APPLIED: "APPLIED",
  REJECTED: "REJECTED",
  EXPIRED: "EXPIRED",
  CONFLICTED: "CONFLICTED",
  FAILED: "FAILED"
});

export const TERMINAL_ACTION_STATUSES = Object.freeze([
  ACTION_STATUS.APPLIED,
  ACTION_STATUS.REJECTED,
  ACTION_STATUS.EXPIRED,
  ACTION_STATUS.CONFLICTED,
  ACTION_STATUS.FAILED
]);

export const DEFAULT_ACTION_TTL_MS = 24 * 60 * 60 * 1000;

export function isTerminalActionStatus(status) {
  return TERMINAL_ACTION_STATUSES.includes(status);
}

export function isExpired(action, nowMs) {
  return Date.parse(action.expiresAt) <= nowMs;
}
