import type {
  ClaudeApiFormat,
  ClaudeCodexBridgeMode,
  ProviderMeta,
} from "@/types";

type BridgeScope = {
  appId: string;
  providerType?: string;
  apiFormat: ClaudeApiFormat;
};

export function isClaudeCodexBridgeEligible(
  appId: string,
  providerType: string | undefined,
  apiFormat: ClaudeApiFormat,
): boolean {
  return (
    appId === "claude" &&
    providerType === "codex_oauth" &&
    apiFormat === "openai_responses"
  );
}

export function resolveClaudeCodexBridgeMode(
  meta: ProviderMeta | undefined,
): ClaudeCodexBridgeMode {
  return meta?.bridgeMode ?? "legacy";
}

export function bridgeModeForProviderSave(
  scope: BridgeScope & { mode: ClaudeCodexBridgeMode },
): ClaudeCodexBridgeMode | undefined {
  return isClaudeCodexBridgeEligible(
    scope.appId,
    scope.providerType,
    scope.apiFormat,
  )
    ? scope.mode
    : undefined;
}
