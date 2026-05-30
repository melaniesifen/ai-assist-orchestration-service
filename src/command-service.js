import { policyError, validationError } from "./errors.js";
import { assertIdentity, requireNonBlankString, requireObject } from "./validation.js";

export function createCommandService({
  contextService,
  providerRegistry,
  eventPublisher,
  policyService,
  promptBuilder,
  clock = () => new Date()
}) {
  if (!contextService || !providerRegistry || !eventPublisher || !policyService || !promptBuilder) {
    throw new TypeError("contextService, providerRegistry, eventPublisher, policyService, and promptBuilder are required");
  }

  async function runAssistantCommand(identity, command) {
    assertIdentity(identity);
    requireObject(command, "command");
    const requestId = requireNonBlankString(command.requestId, "command.requestId");
    const correlationId = requireNonBlankString(command.correlationId, "command.correlationId");
    const sessionId = requireNonBlankString(command.sessionId, "command.sessionId");
    const providerName = requireNonBlankString(command.provider, "command.provider");

    const policy = await policyService.evaluate({
      tenantId: identity.tenantId,
      userId: identity.userId,
      requestId,
      correlationId,
      subjectKind: "assistant_command",
      metadata: {
        sessionId,
        provider: providerName,
        contextMode: command.contextMode
      }
    });
    if (policy.decision !== "ALLOW") {
      throw policyError("POLICY_BLOCKED", "Policy blocked the command", {
        policyDecisionId: policy.decisionId,
        reasonCode: policy.reasonCode
      });
    }

    const provider = providerRegistry.get(providerName);
    if (!provider) {
      throw validationError("PROVIDER_UNSUPPORTED", "Provider is not configured", { provider: providerName });
    }

    await publishProgress({ sessionId, requestId, correlationId, stage: "context.loading", status: "STARTED" });
    const context = await contextService.resolveContext({
      tenantId: identity.tenantId,
      userId: identity.userId,
      sessionId,
      resourceId: command.resourceId,
      contextMode: command.contextMode
    });
    if (!context?.authorized) {
      throw validationError("CONTEXT_UNAVAILABLE", "Context could not be resolved for this command", {
        sessionId,
        reasonCode: context?.reasonCode ?? "CONTEXT_UNAVAILABLE"
      });
    }

    await publishProgress({ sessionId, requestId, correlationId, stage: "provider.generating", status: "STARTED" });
    const prompt = promptBuilder.buildPrompt({ command, context });
    const response = await provider.generate({
      prompt,
      context,
      secretRef: command.secretRef,
      requestId,
      correlationId
    });

    let index = 0;
    for (const delta of response.deltas ?? []) {
      await eventPublisher.publish({
        type: "assistant.delta",
        sessionId,
        requestId,
        correlationId,
        messageId: response.messageId,
        delta,
        index
      });
      index += 1;
    }

    await eventPublisher.publish({
      type: "assistant.final",
      sessionId,
      requestId,
      correlationId,
      messageId: response.messageId,
      finishReason: response.finishReason,
      usage: response.usage ?? null,
      createdAt: clock().toISOString()
    });

    return {
      messageId: response.messageId,
      finishReason: response.finishReason,
      provider: providerName
    };
  }

  async function publishProgress({ sessionId, requestId, correlationId, stage, status }) {
    await eventPublisher.publish({
      type: "progress",
      sessionId,
      requestId,
      correlationId,
      stage,
      status,
      messageCode: `${stage}.${status}`.toUpperCase()
    });
  }

  return { runAssistantCommand };
}
