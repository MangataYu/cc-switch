# Claude Code Codex OAuth Shadow Tool-Fragment Normalization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans` for inline execution. Use `superpowers:subagent-driven-development` only when the user explicitly requests delegated agents.

**Goal:** Make shadow comparison treat one-versus-many `input_json_delta` fragments for the same content block as equivalent, while preserving strict lifecycle, index, text, reasoning, signature, terminal, and leak-free diagnostic checks.

**Architecture:** Keep the existing raw `ShadowStreamShape` counters and structural hash unchanged for evidence. Add a second, comparison-only semantic event sequence to each `ShadowStreamAccumulator`; collapse only consecutive tool-argument deltas with the same block index, and compare those sequences in `finish_stream`. Expose only ordered, deduplicated `ShadowReasonCode` values in the debug log so future blockers are diagnosable without logging request paths or content.

**Tech Stack:** Rust 2021, Tokio/futures streams, serde/serde_json, the existing strict Responses-to-Claude state machine, and the production legacy SSE converter.

## Global Constraints

- Scope is only Claude Code + built-in `providerType=codex_oauth` + `apiFormat=openai_responses` shadow observation.
- Do not change yielded legacy bytes, strict bridge decoding, tool schemas, registry identities, ledger behavior, retry behavior, mode defaults, or rollout policy.
- Preserve raw event counts and raw structural hashes exactly as today; semantic normalization affects only `shape_matches`.
- Collapse only consecutive `content_block_delta` events classified as tool arguments and carrying the same block index.
- Never collapse text, thinking, signature, block start/stop, message lifecycle, error, or terminal events. Never join different indexes or reorder events.
- Logs may contain ordered reason-code enums, counts, and booleans only. Do not log difference paths, hashes, call IDs, tool names, arguments, prompt text, reasoning, results, credentials, or raw SSE.
- Keep observation bounded by the existing `MAX_SHADOW_STREAM_EVENTS`; do not add unbounded buffers, queues, tasks, files, or network calls.
- Do not touch `.codegraph/`, upgrade dependencies, run live OAuth traffic, push, or create a PR. A live visible-tool rerun still needs separate explicit authorization.

---

## File and Interface Map

- Modify `src-tauri/src/proxy/claude_codex_bridge/shadow.rs`: add comparison-only semantic tokens, reason-code projection, focused positive and negative tests, and leak checks.
- Modify `src-tauri/src/proxy/providers/streaming_responses.rs`: append safe unexplained reason codes to the existing shadow completion debug log.
- Modify `docs/superpowers/runbooks/2026-07-30-claude-codex-oauth-stage-5-live-smoke.md`: record the confirmed offline cause and fix while leaving `LiveSmokeStatus::Failed` until an authorized rerun passes.

## Core Interfaces

Add private comparison state without changing serialized reports:

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
struct ShadowSemanticEvent {
    kind: String,
    index: Option<u32>,
    classification: ShadowEventClass,
}

#[derive(Debug, Default)]
struct ShadowStreamAccumulator {
    // Existing raw fields stay unchanged.
    semantic_events: Vec<ShadowSemanticEvent>,
}
```

Extract indexes from typed bridge events and legacy Anthropic payloads, then record both raw and semantic state:

```rust
fn record_classified(
    &mut self,
    kind: &str,
    index: Option<u32>,
    classification: ShadowEventClass,
) {
    // Existing bounded raw counters/event_kinds first.
    let semantic = ShadowSemanticEvent {
        kind: kind.to_string(),
        index,
        classification,
    };
    let collapsible = kind == "content_block_delta"
        && classification == ShadowEventClass::Tool
        && self.semantic_events.last() == Some(&semantic);
    if !collapsible {
        self.semantic_events.push(semantic);
    }
}
```

`finish_stream` compares `legacy.semantic_events == bridge.semantic_events`; completion remains a separate comparison. `ShadowStreamShape` continues to expose raw counts and its current raw `event_kinds` hash.

Add a safe report projection used by logging:

```rust
impl ShadowComparisonReport {
    pub fn unexplained_reason_codes(&self) -> Vec<ShadowReasonCode> {
        let mut codes = Vec::new();
        for difference in &self.differences {
            if difference.disposition == ShadowDifferenceDisposition::Unexplained
                && !codes.contains(&difference.reason_code)
            {
                codes.push(difference.reason_code);
            }
        }
        codes
    }
}
```

---

## Task 1: Lock the Live Glob/Grep Failure into an Offline RED Test

**Files:**

- Modify: `src-tauri/src/proxy/claude_codex_bridge/shadow.rs`

- [ ] Add a test-only request fixture for built-in `Glob` (expected bridge alias `find_files`) with a required `pattern` string and strict object schema.
- [ ] Add async test `shadow_fragmented_tool_arguments_are_semantically_equivalent` beside `shadow_tool_stream_reprojects_legacy_tool_name_before_strict_observation`.
- [ ] Construct one upstream SSE stream containing `response.created`, a `Glob` function-call start, two consecutive `response.function_call_arguments.delta` fragments that form one valid JSON object, arguments/output-item completion, and `response.completed`.
- [ ] Feed that exact upstream through production `create_anthropic_sse_stream_from_responses`, feed the upstream bytes to `ShadowComparisonSession`, feed the converted bytes as the legacy observation, and call `finish_stream`.
- [ ] Assert the intended contract: bounded stream, zero comparison failures, zero unexplained differences, `shape_matches == true`, and unequal raw legacy/bridge `tool_events` so the fixture proves one-versus-many fragmentation.
- [ ] Run the exact test before production changes and preserve the RED evidence: it must fail because the current raw-count comparison reports `ResponseEventMismatch` and `shape_matches == false`.

Run:

```powershell
cargo test --manifest-path src-tauri/Cargo.toml --lib proxy::claude_codex_bridge::shadow::tests::shadow_fragmented_tool_arguments_are_semantically_equivalent -- --exact
```

Expected before implementation: one failed test with one unexplained response-event mismatch, not a strict-decoder failure.

## Task 2: Implement the Narrow Semantic Comparison and Safety Guards

**Files:**

- Modify: `src-tauri/src/proxy/claude_codex_bridge/shadow.rs`

- [ ] Add `ShadowSemanticEvent` and bounded `semantic_events` state. Do not serialize it or replace raw `event_kinds`/counters.
- [ ] In `record_bridge_event`, pass `Some(index)` for `ContentBlockStart`, `ContentBlockDelta`, and `ContentBlockStop`; pass `None` for message/error events.
- [ ] In `record_legacy_event`, read the top-level Anthropic `index` with `as_u64()` plus checked `u32::try_from`; missing or out-of-range indexes stay `None` and therefore cannot equal a typed indexed bridge event.
- [ ] Change `record_classified` to preserve the existing raw accounting and append comparison tokens, collapsing only a consecutive duplicate whose kind is `content_block_delta`, class is `Tool`, and index is identical.
- [ ] Change only `finish_stream`'s `shape_matches` predicate to semantic sequence equality. Keep the separate `legacy.complete != bridge.complete` terminal check and all raw report fields unchanged.
- [ ] Add negative test `shadow_tool_fragment_normalization_keeps_block_indexes_distinct`: otherwise-identical tool deltas at different indexes must produce different semantic sequences / a mismatch.
- [ ] Extend `stream_shape_classifies_tool_and_reasoning_payloads_without_content` (or add an adjacent test) to prove consecutive thinking, signature, and text deltas are not collapsed.
- [ ] Run the positive regression again and confirm GREEN, then run all shadow module tests.

Run:

```powershell
cargo test --manifest-path src-tauri/Cargo.toml --lib proxy::claude_codex_bridge::shadow::tests::shadow_fragmented_tool_arguments_are_semantically_equivalent -- --exact
cargo test --manifest-path src-tauri/Cargo.toml --lib proxy::claude_codex_bridge::shadow::tests
```

- [ ] Commit the tested semantic change.

```powershell
git add -- src-tauri/src/proxy/claude_codex_bridge/shadow.rs
git commit -m "fix(proxy): normalize shadow tool argument fragments"
```

## Task 3: Add Content-Free Reason-Code Diagnostics

**Files:**

- Modify: `src-tauri/src/proxy/claude_codex_bridge/shadow.rs`
- Modify: `src-tauri/src/proxy/providers/streaming_responses.rs`

- [ ] First add a failing test for `ShadowComparisonReport::unexplained_reason_codes`: include duplicate unexplained differences, an accepted/equivalent difference, and caller-controlled path strings; expect ordered unique unexplained enums only.
- [ ] Implement `unexplained_reason_codes` without deriving ordering or exposing whole `ShadowDifference` values.
- [ ] Extend the existing shadow completion `log::debug!` with `unexplained_reason_codes={:?}` using only that projection. Do not print `path`, structural hashes, payloads, aliases, call IDs, or tool names.
- [ ] Serialize/format the projected value in the test and assert it does not contain fixture path, prompt, call ID, legacy tool name, bridge alias, or argument values.
- [ ] Run the shadow and streaming converter tests.

Run:

```powershell
cargo test --manifest-path src-tauri/Cargo.toml --lib proxy::claude_codex_bridge::shadow::tests
cargo test --manifest-path src-tauri/Cargo.toml --lib streaming_responses
```

- [ ] Commit the safe diagnostic change.

```powershell
git add -- src-tauri/src/proxy/claude_codex_bridge/shadow.rs src-tauri/src/proxy/providers/streaming_responses.rs
git commit -m "fix(proxy): log safe shadow reason codes"
```

## Task 4: Run Offline Regression and Static Verification

**Files:**

- Verify only; no planned source changes.

- [ ] Run the full bridge suite and confirm all tests pass.
- [ ] Run the streaming converter suite and confirm all tests pass.
- [ ] Run the forwarder suite to protect rollout selection and legacy/shadow routing.
- [ ] Run `cargo check`, formatting verification, and whitespace validation.
- [ ] If any command fails, stop completion claims, diagnose the specific failure, add the smallest regression, and return to the relevant task.

Run:

```powershell
cargo test --manifest-path src-tauri/Cargo.toml --lib claude_codex_bridge
cargo test --manifest-path src-tauri/Cargo.toml --lib streaming_responses
cargo test --manifest-path src-tauri/Cargo.toml --lib forwarder
cargo check --manifest-path src-tauri/Cargo.toml
cargo fmt --manifest-path src-tauri/Cargo.toml -- --check
git diff --check -- src-tauri/src/proxy/claude_codex_bridge/shadow.rs src-tauri/src/proxy/providers/streaming_responses.rs
```

Expected: every command exits 0; the exact pass counts are recorded from actual output rather than predicted in advance.

## Task 5: Record the Offline Fix Without Overstating Live Readiness

**Files:**

- Modify: `docs/superpowers/runbooks/2026-07-30-claude-codex-oauth-stage-5-live-smoke.md`

- [ ] Amend the latest 3.19.0 result to state that offline replay confirmed argument-fragment count as the `ResponseEventMismatch` cause and that the narrow semantic comparison regression now passes.
- [ ] Keep the top-level status `Failed`, blocker `UnexplainedDifferences`, and prohibition on `enabled`: no live result changes until a separately authorized visible-tool rerun completes.
- [ ] State explicitly that raw event counts/hashes remain evidence and text/reasoning/signature/index/lifecycle/terminal mismatches still fail closed.
- [ ] Check the final diff for accidental secrets, temporary paths, raw SSE, OAuth material, unrelated edits, or `.codegraph/` interaction.

Run:

```powershell
git diff --check -- docs/superpowers/runbooks/2026-07-30-claude-codex-oauth-stage-5-live-smoke.md
git status --short --untracked-files=all -- . ':(exclude).codegraph'
```

- [ ] Commit the documentation update.

```powershell
git add -- docs/superpowers/runbooks/2026-07-30-claude-codex-oauth-stage-5-live-smoke.md
git commit -m "docs(proxy): record shadow fragment fix"
```

## Final Handoff Gate

- [ ] Re-run `git status --short --untracked-files=all -- . ':(exclude).codegraph'` and report only the scoped worktree state.
- [ ] Report the RED failure evidence, GREEN focused test, exact suite/check results, commits created, and remaining live-smoke blocker.
- [ ] Do not claim rollout readiness and do not run live OAuth/tool traffic without a new explicit authorization.
