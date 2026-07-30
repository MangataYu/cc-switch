import { describe, expect, it } from "vitest";
import {
  bridgeModeForProviderSave,
  isClaudeCodexBridgeEligible,
  resolveClaudeCodexBridgeMode,
} from "./claudeCodexBridge";

describe("Claude Codex bridge provider mode", () => {
  it("defaults missing and old metadata to legacy", () => {
    expect(resolveClaudeCodexBridgeMode(undefined)).toBe("legacy");
    expect(resolveClaudeCodexBridgeMode({ providerType: "codex_oauth" })).toBe(
      "legacy",
    );
  });

  it.each(["legacy", "shadow", "enabled"] as const)(
    "round trips explicit %s",
    (mode) => {
      expect(resolveClaudeCodexBridgeMode({ bridgeMode: mode })).toBe(mode);
      expect(
        bridgeModeForProviderSave({
          appId: "claude",
          providerType: "codex_oauth",
          apiFormat: "openai_responses",
          mode,
        }),
      ).toBe(mode);
    },
  );

  it("limits the setting to Claude Codex OAuth Responses", () => {
    expect(
      isClaudeCodexBridgeEligible("claude", "codex_oauth", "openai_responses"),
    ).toBe(true);
    expect(
      isClaudeCodexBridgeEligible("codex", "codex_oauth", "openai_responses"),
    ).toBe(false);
    expect(
      isClaudeCodexBridgeEligible("claude", "xai_oauth", "openai_responses"),
    ).toBe(false);
    expect(
      isClaudeCodexBridgeEligible("claude", "codex_oauth", "openai_chat"),
    ).toBe(false);
  });

  it("persists explicit legacy rollback and removes out-of-scope values", () => {
    expect(
      bridgeModeForProviderSave({
        appId: "claude",
        providerType: "codex_oauth",
        apiFormat: "openai_responses",
        mode: "legacy",
      }),
    ).toBe("legacy");
    expect(
      bridgeModeForProviderSave({
        appId: "claude",
        providerType: "codex_oauth",
        apiFormat: "openai_chat",
        mode: "enabled",
      }),
    ).toBeUndefined();
  });
});
