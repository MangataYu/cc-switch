# Claude Codex OAuth Stage 3 Conversation Ledger Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the minimum reliable in-process conversation state needed for Claude Code requests served by the enabled built-in Codex OAuth Responses bridge, including retry identity, tool-call closure, reasoning binding, compaction epochs, safe snapshots, and zero-network replay.

**Architecture:** A concurrency-safe `ConversationLedger` owns bounded, TTL-governed sessions keyed by a hashed stable Claude Code identity. `ClaudeCodexBridge` registers or reuses a turn before request emission; the frozen `PreparedCodexTurn` carries the ledger binding used to observe returned calls and later `tool_result` history. Legacy bypasses the ledger, while shadow uses an isolated throwaway ledger so local comparison cannot mutate enabled state.

**Tech Stack:** Rust 1.85, `Arc`, `std::sync::{Mutex, OnceLock}`, `serde`/`serde_json`, `sha2`, existing canonical JSON, bridge forensic capture, and offline replay infrastructure; no new dependency or persistence.

## Global Constraints

- Scope is only Claude Code -> built-in `providerType=codex_oauth` with `apiFormat=openai_responses`; other providers, clients, and directions remain unchanged.
- Store only hashes, identifiers, lifecycle states, timestamps, counts, and bounded safe summaries; never retain prompt, source code, tool arguments/results, credentials, or plaintext reasoning.
- Tool transitions are monotonic and idempotent for identical repeated events; conflicts return typed `ConversationStateConflict` errors.
- Same session identity plus canonical request fingerprint reuses the exact `TurnId`, `Arc<ToolRegistry>`, and capability snapshot.
- Active calls, `ReturnedToClaude` calls, and incomplete reasoning items are never evicted; state is in-process only.
- Shadow compilation makes no second upstream request and cannot mutate the shared enabled ledger; legacy neither reads nor creates ledger state.
- Reuse the Stage 2 codecs. Do not add the Stage 4 typed SSE model/state machine, codec rewrite, online probes, persistent/cross-process state, tool execution, or legacy removal.
- Every core behavior follows RED -> observed expected failure -> minimal GREEN -> focused regression.

## File Structure and Data Structures

- Create `src-tauri/src/proxy/claude_codex_bridge/conversation_ledger.rs`: `ConversationLedger`, `SessionState`, `TurnState`, `ToolCallState`, reasoning bindings, request/history fingerprints, TTL/capacity cleanup, snapshots, and typed transition methods.
- Modify `src-tauri/src/proxy/claude_codex_bridge/mod.rs`: shared enabled ledger, isolated shadow bridge, request registration/reuse, history observation, and prepared-turn response state updates.
- Modify `src-tauri/src/proxy/claude_codex_bridge/error.rs`: `ConversationStateConflict { kind, summary }` and safe forensic mapping.
- Modify `src-tauri/src/proxy/forwarder.rs`: mode-specific ledger ownership, safe `LedgerSnapshot` capture, and retry/fallback boundary propagation.
- Modify `src-tauri/src/proxy/providers/streaming_responses.rs` and `src-tauri/src/proxy/handlers.rs`: report validated complete tool calls as returned without introducing Stage 4 event ownership.
- Modify `src-tauri/src/proxy/bridge_forensics/replay.rs`: replay request -> returned tool call -> observed tool result -> completed with `network_requests = 0`.
- Modify `docs/superpowers/specs/2026-07-29-claude-code-codex-oauth-agent-bridge-design.md`: record Stages 0-3 implemented and Stage 4 pending after acceptance.

Core shapes:

```rust
pub struct ConversationLedger {
    inner: Arc<Mutex<LedgerInner>>,
    limits: LedgerLimits,
}

pub struct SessionState {
    pub session_identity_hash: String,
    pub generation: u64,
    pub capability_profile_version: String,
    pub turns: VecDeque<TurnState>,
    pub compaction_epoch: u64,
    pub last_access: SystemTime,
    pub expires_at: SystemTime,
}

pub struct TurnState {
    pub turn_id: String,
    pub request_fingerprint: String,
    pub tool_registry: Arc<ToolRegistry>,
    pub capability_snapshot: Arc<CodexOAuthCapabilities>,
    pub calls: HashMap<String, ToolCallRecord>,
    pub reasoning_items: HashMap<String, ReasoningIdentityState>,
    pub safe_summary: TurnSafeSummary,
    pub compaction_epoch: u64,
}

pub enum ToolCallState {
    Declared,
    ArgumentsStreaming,
    Ready,
    ReturnedToClaude,
    ResultObserved,
    Completed,
    Aborted,
}
```

---

### Task 1: Core ledger, bounded sessions, and lifecycle

**Files:** Create `conversation_ledger.rs`; modify `mod.rs` and `error.rs`; tests inline in the ledger module.

**Interfaces:** `ConversationLedger::with_limits`, `register_turn`, `transition_call`, `snapshot`, `cleanup`; `LedgerLimits { max_sessions, max_turns_per_session, ttl }`; `ConversationConflictKind` enumerating unknown identity, call-ID conflict, state regression, argument conflict, result conflict, orphan result, and reasoning-binding conflict.

- [x] Add failing tests proving bounded sessions/turns, TTL metadata, no content-bearing fields, parallel calls, the full legal lifecycle, identical-event idempotency, and every forbidden conflict.
- [x] Run `cargo test conversation_ledger --lib -- --nocapture`; confirm RED is caused by missing ledger APIs.
- [x] Implement the minimal mutex-protected state and monotonic transition table. Store only SHA-256 hashes for arguments/results and reject unknown tool identities through the frozen registry.
- [x] Re-run `cargo test conversation_ledger --lib -- --nocapture` and `cargo check`; commit as `feat(proxy): add conversation ledger lifecycle`.

### Task 2: Canonical retry identity and tool-result observation

**Files:** Modify `conversation_ledger.rs` and `mod.rs`; tests in both modules.

**Interfaces:** `canonical_request_fingerprint(&Value) -> String`, `history_fingerprints(&Value) -> Vec<String>`, `observe_tool_results(&TurnBinding, &Value)`, and `TurnBinding { session_identity_hash, generation, compaction_epoch, turn_id }`.

- [x] Add failing tests showing JSON key order produces one fingerprint; exact retry reuses turn ID, registry `Arc`, and capability `Arc`; a changed request creates a new turn; matching later `tool_result` completes a returned call; duplicate identical results are idempotent; conflicting or orphan results fail closed and are never converted to text.
- [x] Run the focused ledger/bridge tests and observe expected RED failures.
- [x] Hash canonical request/history values without storing them. Scan Claude history before codec conversion, match `tool_use_id` only to ledger-known returned calls, and keep the original prepared registry/profile on retry.
- [x] Run `cargo test conversation_ledger --lib -- --nocapture` and `cargo test proxy::claude_codex_bridge --lib -- --nocapture`; commit as `feat(proxy): reuse safe Claude Codex turns`.

### Task 3: Reasoning identity, compaction, and child-session isolation

**Files:** Modify `conversation_ledger.rs` and `mod.rs`; focused tests inline.

**Interfaces:** `ReasoningBinding { item_id, content_hash, identity_hash, state, provider_id_hash, model_hash, capability_profile_version }`, `observe_reasoning_item`, and history-prefix observation that advances `compaction_epoch` only when the previously observed hash sequence is not a prefix of the new sequence.

- [x] Add failing tests for same-binding idempotency, cross-session/turn/model/provider/profile rejection, normal incremental history without epoch change, discontinuous stable-session history incrementing exactly once, retained active/returned calls and incomplete reasoning across cleanup, and independent child session identities.
- [x] Run focused tests and observe RED.
- [x] Implement hashed reasoning bindings and prefix-based compaction detection. Evict only closed prior-epoch records and expired sessions without protected active state.
- [x] Run `cargo test conversation_ledger --lib -- --nocapture` and bridge regressions; commit as `feat(proxy): bind reasoning and compaction epochs`.

### Task 4: Routing, visibility, retry/failover safety, and safe forensic snapshot

**Files:** Modify `mod.rs`, `forwarder.rs`, `handlers.rs`, `streaming_responses.rs`, and `bridge_forensics/model.rs` only if the existing artifact enum needs serialization coverage.

**Interfaces:** enabled uses the process-shared ledger; shadow creates an isolated ledger for one local comparison; legacy has no ledger binding. `LedgerSnapshot` serializes only session hash, generation, epoch, turn ID, request/registry fingerprints, call ID, binding identity, state, and optional error kind.

- [x] Add failing routing tests proving enabled registers once and reuses on retry, legacy performs zero ledger operations, shadow performs local isolated operations with one upstream request total, and a call marked `ReturnedToClaude` disables bridge automatic retry/failover.
- [x] Add failing snapshot tests that serialize a real ledger and assert forbidden prompt/code/argument/result/credential/reasoning strings are absent.
- [x] Implement mode ownership, record the snapshot through existing forensic capture, and report completed calls from non-stream and existing validated stream tool completion hooks. Do not add typed SSE events.
- [x] Run `cargo test bridge_forensics --lib -- --nocapture`, `cargo test proxy::claude_codex_bridge --lib -- --nocapture`, `cargo test proxy::forwarder --lib -- --nocapture`, and relevant streaming/handler tests; commit as `feat(proxy): route conversation ledger safely`.

### Task 5: Minimal offline lifecycle replay

**Files:** Modify `bridge_forensics/replay.rs`; add or update only the checked-in Stage 3 structural fixture needed for lifecycle replay.

**Interfaces:** replay prepares one enabled turn against an isolated ledger, observes one function call through `ReturnedToClaude`, feeds a matching next-history `tool_result`, and asserts `Completed`; `ReplayReport.network_requests` remains literal zero.

- [x] Add a failing replay test for request -> tool call -> `ReturnedToClaude` -> tool result -> `Completed`, including snapshot structural comparison and `network_requests == 0`.
- [x] Run the focused replay test and observe RED.
- [x] Implement only the minimal ledger replay path and safe artifact comparison.
- [x] Run `cargo test bridge_forensics --lib -- --nocapture` and the replay example; commit as `test(proxy): replay conversation ledger lifecycle`.

### Task 6: Acceptance, scope audit, and design status

**Files:** Modify the design document and mark this plan complete; make behavioral fixes only with a new failing regression test.

- [x] Run from `src-tauri`: `cargo fmt --all -- --check`, `cargo test --lib`, `cargo test conversation_ledger --lib`, `cargo test bridge_forensics --lib`, `cargo test proxy::claude_codex_bridge --lib`, and `cargo test proxy::forwarder --lib`.
- [x] Run `git diff --check`; inspect `git status --short`, all Stage 3 diffs, and untracked files while treating `.codegraph/` as untouched user-owned local data.
- [x] Scan changed files for credentials and forbidden stored content; confirm no temporary bundle, persistent ledger, Stage 4 event model, online probe, other-provider change, or `.codegraph/` modification exists.
- [x] Update the design status to exactly `Stages 0–3 implemented; Stage 4 pending`, recording scope, retry safety, compaction support, and known limitations.
- [x] Re-run the complete acceptance commands after documentation changes, then commit as `docs: record Claude Codex bridge stage 3`.

## Acceptance Criteria

- Same stable session plus canonical fingerprint reuses turn ID, registry, and capability snapshot; changed requests create bounded new turns.
- Legal parallel lifecycle transitions and identical duplicates succeed; regressions, conflicts, unknown identities, and orphan results return typed safe errors.
- Matching subsequent `tool_result` closes exactly its returned call, and no orphan becomes ordinary text.
- Reasoning identity cannot cross session, turn, provider, model, or capability profile.
- Incremental history does not compact; discontinuity increments the epoch; cleanup never removes protected active state.
- Enabled alone uses shared state, legacy bypasses it, and shadow cannot pollute it or add network traffic.
- Ledger snapshots contain only the approved structural fields.
- Minimal replay ends at `Completed` with `network_requests = 0`.
- `.codegraph/` is unchanged and uncommitted; final diff has no credentials, temporary bundle, Stage 4 implementation, or out-of-scope provider/client changes.

## Test Commands

```text
cd src-tauri
cargo check
cargo test conversation_ledger --lib -- --nocapture
cargo test bridge_forensics --lib -- --nocapture
cargo test proxy::claude_codex_bridge --lib -- --nocapture
cargo test proxy::forwarder --lib -- --nocapture
cargo fmt --all -- --check
cargo test --lib
git diff --check
```
