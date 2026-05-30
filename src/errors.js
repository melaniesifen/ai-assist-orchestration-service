export class OrchestrationError extends Error {
  constructor({
    code,
    category,
    message,
    retryable = false,
    statusCode = 400,
    metadata = {}
  }) {
    super(message);
    this.name = "OrchestrationError";
    this.code = code;
    this.category = category;
    this.retryable = retryable;
    this.statusCode = statusCode;
    this.metadata = metadata;
  }
}

export function validationError(code, message, metadata = {}) {
  return new OrchestrationError({
    code,
    category: "VALIDATION",
    message,
    statusCode: 400,
    metadata
  });
}

export function authorizationError(code, message, metadata = {}) {
  return new OrchestrationError({
    code,
    category: "AUTHORIZATION",
    message,
    statusCode: 403,
    metadata
  });
}

export function conflictError(code, message, metadata = {}) {
  return new OrchestrationError({
    code,
    category: "CONFLICT",
    message,
    statusCode: 409,
    metadata
  });
}

export function policyError(code, message, metadata = {}) {
  return new OrchestrationError({
    code,
    category: "POLICY",
    message,
    statusCode: 403,
    metadata
  });
}

export function dependencyError(code, message, metadata = {}) {
  return new OrchestrationError({
    code,
    category: "DEPENDENCY",
    message,
    statusCode: 502,
    metadata
  });
}
