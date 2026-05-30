import { validationError } from "./errors.js";

export function requireNonBlankString(value, fieldName) {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw validationError("INVALID_FIELD", `${fieldName} must be a non-empty string`, { fieldName });
  }
  return value;
}

export function requireObject(value, fieldName) {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw validationError("INVALID_FIELD", `${fieldName} must be an object`, { fieldName });
  }
  return value;
}

export function assertIdentity(identity) {
  requireObject(identity, "identity");
  requireNonBlankString(identity.tenantId, "identity.tenantId");
  requireNonBlankString(identity.userId, "identity.userId");
  return identity;
}

export function assertOwnership(identity, record) {
  return record.tenantId === identity.tenantId && record.userId === identity.userId;
}
