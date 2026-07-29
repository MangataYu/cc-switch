# Claude Code Codex OAuth Stage 4 Strict Streaming State Machine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Route only Claude Code → built-in Codex OAuth → `openai_responses` → `bridgeMode=enabled` streaming responses through a request-scoped typed decoder, strict state machine, validated Claude event encoder, and deterministic offline replay.

**Architecture:** Add a focused `claude_codex_bridge::streaming` module owned by `PreparedCodexTurn`. The existing `providers::streaming_responses` implementation remains the legacy codec and SSE framing helper; its prepared-turn entry points delegate semantic decisions to the bridge module, while legacy and other providers retain their current behavior. The bridge decoder converts protocol JSON into typed events, the turn-bound machine validates monotonic item/tool/terminal state and advances `ConversationLedger`, and only validated `ClaudeStreamEvent` values reach the Anthropic SSE encoder.

**Tech Stack:** Rust 2021, Tokio/futures streams, serde/serde_json, SHA-256 structural identities, existing bridge forensics and unit-test infrastructure.

## Global Constraints

- Scope is exactly Claude Code + built-in `providerType=codex_oauth` + `apiFormat=openai_responses` + `bridgeMode=enabled`.
- Preserve legacy, shadow, other-provider, and other-client behavior; shadow performs no second upstream request.
- Use TDD for every core behavior: RED, minimal GREEN, refactor only after green.
- Do not store prompt, source code, plaintext tool arguments/results, credentials, or plaintext reasoning in ledger, logs, errors, traces, or replay artifacts.
- Do not modify, rebuild, delete, stage, or commit `.codegraph/`.
- Do not add online capability probes, persistent/cross-process state, tool/MCP execution, Stage 5 rollout machinery, Stage 6 defaults/removal, or a general codec rewrite.
- Do not push or create a pull request.

---

## Module and File Map

- Create `src-tauri/src/proxy/claude_codex_bridge/streaming.rs`: typed event/value types, Responses event decoder, request-scoped stream state, legal transition table, validated Claude events, visibility/retry state, structural decisions, and focused tests.
- Modify `src-tauri/src/proxy/claude_codex_bridge/mod.rs`: export streaming types and let `PreparedCodexTurn` create a stream machine bound to its frozen registry, capabilities, turn ID, reasoning identity, and ledger binding.
- Modify `src-tauri/src/proxy/claude_codex_bridge/error.rs`: add `InvalidUpstreamEvent` and `IncompleteStream` variants with safe structural metadata and typed `ProxyError` mapping.
- Modify `src-tauri/src/proxy/providers/streaming_responses.rs`: keep the legacy converter unchanged; replace only prepared-turn entry points with SSE framing → bridge decoder/machine → Anthropic SSE encoding and evidence notifications.
- Modify `src-tauri/src/proxy/handlers.rs`: require a prepared turn for enabled scoped streaming and propagate the strict stream visibility/failure boundary without changing legacy selection.
- Modify `src-tauri/src/proxy/forwarder.rs`: keep existing pre-output stream priming/failover and add only a pure retry decision boundary for output-visible/tool-visible strict stream failures.
- Modify `src-tauri/src/proxy/bridge_forensics/{model.rs,store.rs,replay.rs,mod.rs}`: add structural stream decisions/replay reports, typed failure capture, deterministic fixtures, and redaction assertions.
- Modify `docs/superpowers/specs/2026-07-29-claude-code-codex-oauth-agent-bridge-design.md`: record Stage 4 implementation status, event/state policy, retry visibility boundary, tests, replay, and limitations.
- Modify this plan: check off each completed task and acceptance item.

## Typed Event and State Interfaces

The bridge module will expose opaque identity newtypes (`ItemId`, `CallId`) and these core values:

```rust
pub enum CodexResponseEvent {
    ResponseStarted { response_id: String, model: String, usage: Option<CodexUsage> },
    ReasoningStarted { item_id: ItemId },
    ReasoningDelta { item_id: ItemId, text: String },
    ReasoningDone { item_id: ItemId, encrypted_content: Option<String> },
    ToolCallStarted { item_id: ItemId, call_id: CallId, codex_name: String },
    ToolArgumentsDelta { item_id: ItemId, call_id: CallId, bytes: Vec<u8> },
    ToolCallDone { item_id: ItemId, call_id: CallId, arguments: Option<Vec<u8>> },
    TextStarted { item_id: ItemId },
    TextDelta { item_id: ItemId, text: String },
    TextDone { item_id: ItemId },
    UsageUpdated { usage: CodexUsage },
    ResponseCompleted { status: String, stop_reason: Option<String> },
    ResponseFailed { error_type: String, safe_message: String },
}

pub enum ClaudeStreamEvent {
    MessageStart { id: String, model: String, usage: ClaudeUsage },
    ContentBlockStart { index: u32, block: ClaudeContentBlock },
    ContentBlockDelta { index: u32, delta: ClaudeContentDelta },
    ContentBlockStop { index: u32 },
    MessageDelta { stop_reason: String, usage: ClaudeUsage },
    MessageStop,
    Error { error_type: String, safe_message: String },
}

pub struct StreamDecision {
    pub sequence: u64,
    pub event_kind: CodexResponseEventKind,
    pub item_identity_hash: Option<String>,
    pub call_identity_hash: Option<String>,
    pub state_before: StreamStateKind,
    pub state_after: StreamStateKind,
    pub output_visible: bool,
    pub tool_visible: bool,
}
```

The decoder may accept `serde_json::Value` only at the protocol boundary. `PreparedCodexStream` consumes only `CodexResponseEvent`, and the encoder consumes only `ClaudeStreamEvent`.

## Request-Scoped State and Legal Transitions

Each `PreparedCodexStream` owns response state, stable content indices, per-item state, usage, sequence number, output visibility, and tool visibility. It borrows authority from the prepared turn's frozen registry/capabilities and shared ledger; it does not create a second authoritative tool lifecycle.

| State | Legal input | Next state / effect |
|---|---|---|
| `AwaitingResponse` | `ResponseStarted` | `Streaming`; emit one `MessageStart` |
| `Streaming` | new reasoning/text/tool start | create one typed item with a stable content index |
| open reasoning | reasoning delta | append visible thinking; emit thinking delta |
| open reasoning | matching reasoning done | bind encrypted identity through Stage 3 ledger; emit signature then block stop |
| open text | text delta | emit text delta |
| open text | matching text done | emit block stop |
| open tool | matching argument delta | append request-local bytes only; advance ledger to `ArgumentsStreaming` using hashes |
| open tool | matching tool done with valid object JSON | restore exact Claude identity, advance ledger to `Ready`, emit complete `tool_use`, mark visibility, then advance ledger to `ReturnedToClaude` |
| `Streaming` | usage update | replace latest monotonic usage snapshot without emitting terminal output |
| `Streaming` with all items closed | response completed/incomplete | emit final usage/stop reason and `MessageStop`; enter `Completed` |
| non-terminal | response failed/error | emit safe error only; enter `Failed` |
| `Completed`/`Failed` | exact duplicate terminal event | deterministic no-op |
| `Completed`/`Failed` | EOF | success |

Exact duplicate starts/done/terminal events are idempotent only when identity and complete content hashes match. Delta duplication is accepted only when the upstream sequence/index identity proves it is the same event; otherwise deltas append normally, and conflicting reuse of a sequence identity fails closed.

## Illegal Sequence Policy

Return `BridgeError::InvalidUpstreamEvent` for malformed/unknown semantic events, missing identity, item type mutation, identity conflicts, delta after done, conflicting duplicate completion, malformed/non-object tool JSON, capability mismatch, events after terminal, or completion with open items. Return `BridgeError::IncompleteStream` for EOF without a terminal response event or with truncated UTF-8/SSE/JSON/item state. Map unknown lifecycle-only metadata events through an explicit ignore allowlist; all unknown semantic event types fail closed.

## Tasks

### Task 1: Typed Responses decoder and safe errors

**Files:** create `claude_codex_bridge/streaming.rs`; modify `claude_codex_bridge/{mod.rs,error.rs}`.

**Interfaces:** `decode_codex_response_event(event_name: Option<&str>, payload: Value) -> Result<Vec<CodexResponseEvent>, BridgeError>`; `CodexResponseEvent::kind()`; safe `BridgeError::{InvalidUpstreamEvent,IncompleteStream}`.

- [ ] Add failing table tests for every typed event, official compatibility alias, explicit ignorable lifecycle event, malformed JSON shape, unknown semantic event, missing item/call/tool identity, and safe summaries that exclude supplied secrets.
- [ ] Run `cargo test typed_responses_event --lib -- --nocapture` and confirm RED is caused by missing decoder/types.
- [ ] Implement the typed values and decoder with explicit field extraction/allowlists; retain no raw payload after decoding.
- [ ] Re-run focused tests plus `cargo check`; refactor extraction helpers only while green.
- [ ] Commit as `feat(proxy): add typed Responses stream events`.

### Task 2: Prepared-turn strict state machine

**Files:** modify `claude_codex_bridge/{streaming.rs,mod.rs,conversation_ledger.rs}` only where an existing hashed transition needs a stream-safe entry point.

**Interfaces:** `PreparedCodexTurn::stream() -> PreparedCodexStream`; `PreparedCodexStream::{apply,finish,visibility,decisions}`; `StreamVisibility { output_emitted, tool_visible }`.

- [ ] Add failing tests for legal text, reasoning, single tool, parallel tool, and interleaved multi-item streams; exact duplicate idempotency; every forbidden sequence in the policy; frozen capability/registry enforcement; and monotonic ledger transitions.
- [ ] Run `cargo test strict_stream_state --lib -- --nocapture` and confirm expected RED failures.
- [ ] Implement minimal response/item maps and transition methods. Keep argument/reasoning plaintext request-scoped only, hash before ledger/decision output, validate completed tool JSON through `ToolRegistry`, and call the ledger for authoritative lifecycle changes.
- [ ] Re-run strict state, ledger, and bridge tests plus `cargo check`; split helpers if needed while green.
- [ ] Commit as `feat(proxy): validate prepared turn streams`.

### Task 3: Validated Claude SSE encoder and routing isolation

**Files:** modify `providers/streaming_responses.rs`, `handlers.rs`, and bridge streaming tests.

**Interfaces:** `encode_claude_stream_event(&ClaudeStreamEvent) -> Bytes`; prepared-turn converter uses the typed pipeline; legacy converter remains the existing core.

- [ ] Add failing end-to-end tests proving exact Claude event shapes/order for text, reasoning signature, tool use, parallel tools, usage, and stop reason; prove enabled cannot bypass a prepared turn and legacy/non-scoped callers preserve current output.
- [ ] Run `cargo test strict_streaming_responses --lib -- --nocapture` and confirm RED.
- [ ] Implement a separate strict prepared-turn stream adapter using existing UTF-8-safe SSE framing. Encode only validated events, mark output visibility on emitted Claude events, and mark tool calls `ReturnedToClaude` only after their Claude events become visible.
- [ ] Re-run strict adapter, legacy `streaming_responses`, bridge, and handler tests plus `cargo check`.
- [ ] Commit as `feat(proxy): route enabled streams through prepared turns`.

### Task 4: Retry/failover boundary and structural forensics

**Files:** modify `forwarder.rs`, `bridge_forensics/{model.rs,store.rs}`, `handlers.rs`, and tests.

**Interfaces:** `bridge_stream_retry_allowed(StreamVisibility) -> bool`; structural stream artifact rows based on `StreamDecision`; typed evidence errors include event kind, output/tool visibility, session hash, turn ID, and bundle ID without content.

- [ ] Add failing tests proving pre-output failure remains retryable, any emitted output forbids unconditional legacy fallback, visible tools forbid automatic retry, retry reuses Stage 3 identities, and failure artifacts contain only hashes/enums/IDs.
- [ ] Add failing redaction tests with sentinel prompt, arguments, result, credential, and reasoning strings; assert none appear in logs/errors/artifacts.
- [ ] Run focused forwarder/forensics tests and confirm RED.
- [ ] Implement the pure visibility gate and structural capture. Preserve the existing first-semantic-event priming path and do not redesign the retry loop.
- [ ] Re-run `cargo test proxy::forwarder --lib`, `cargo test bridge_forensics --lib`, and `cargo check`.
- [ ] Commit as `feat(proxy): enforce visible stream retry boundary`.

### Task 5: Fragmentation properties and complete offline replay

**Files:** modify bridge streaming tests and `bridge_forensics/{replay.rs,mod.rs}`; add compact in-module fixtures if no standalone fixture directory is required.

**Interfaces:** `replay_stream_events(...) -> StreamingReplayReport` returning decisions, Claude event shapes, ledger transitions, terminal/error state, and `network_requests: 0`.

- [ ] Add systematic split-point tests across every byte boundary for SSE frames, multi-byte UTF-8, and multi-chunk tool JSON; compare Claude output and final ledger snapshots with the unsplit fixture.
- [ ] Add illegal-sequence matrices for duplicate/conflicting delta, truncation, no terminal, parallel/interleaved items, duplicate completion, post-done delta, malformed JSON, unknown event, and unknown tool; assert rejection is split-invariant and leak-free.
- [ ] Add ten offline replay fixtures: text-only; reasoning+text; single tool; parallel tools; multi-chunk arguments; tool→`ReturnedToClaude`→later result→`Completed`; incomplete; invalid order; unknown tool; conflicting duplicate.
- [ ] Run new tests and confirm RED, then implement deterministic replay through the same decoder/machine as enabled routing with no HTTP/Tauri dependencies.
- [ ] Re-run `cargo test strict_stream_fragmentation --lib`, `cargo test streaming_event_replay --lib`, `cargo test bridge_forensics --lib`, and `cargo check`; assert every report says `network_requests = 0`.
- [ ] Commit as `test(proxy): replay strict streaming events`.

### Task 6: Documentation, acceptance, and scope audit

**Files:** modify the approved design document and this plan.

- [ ] Update the design status to exactly `Stages 0–4 implemented; Stage 5 pending`; summarize typed events, transitions/rejections, visibility boundary, fragmentation/replay coverage, and limitations.
- [ ] Mark all completed tasks and acceptance checks in this plan; scan for placeholders and interface-name drift.
- [ ] Run every command in Test Commands, the dedicated property/replay tests, and `git diff --check`.
- [ ] Inspect status/diff and changed-file content for warnings, credentials, plaintext reasoning, tool argument/result leakage, temporary bundles, `.codegraph/` changes, other-provider changes, legacy behavior changes, shadow network duplication, and prepared-turn bypass.
- [ ] Commit as `docs: record Claude Codex bridge stage 4`.

## Fragmentation / Property Strategy

- Build one canonical logical event stream for each legal fixture.
- Generate deterministic chunkings at every byte split and selected three-way split pairs; include splits inside `\r\n\r\n`, JSON escapes, numeric tokens, and every byte of a multi-byte UTF-8 scalar.
- Feed every chunking through the same strict adapter, normalize parsed Claude SSE values, and compare literal event vectors plus a safe final ledger snapshot.
- Feed each illegal stream through the same chunk matrix and compare only typed error kind/safe structural metadata, never secret-bearing text.
- Use existing dependencies and deterministic loops rather than adding/upgrading a property-testing crate.

## Replay Strategy

- Replay accepts in-memory structural fixture events/chunks and a prepared turn built with the same scoped provider helper as production enabled routing.
- Replay calls the production typed decoder and state machine, parses its Claude SSE output, and records typed decisions plus safe ledger state.
- It never constructs a network client, never invokes Tauri commands, and hard-codes/increments no network action; reports assert `network_requests == 0`.
- Success fixtures compare complete decisions/output/ledger/terminal state; failure fixtures compare error kind and pre-failure decisions.

## Acceptance Criteria

- [ ] Enabled scoped Responses SSE always requires and consumes its creating `PreparedCodexTurn`.
- [ ] All listed typed events exist and no raw JSON is the state-machine interface.
- [ ] Text, reasoning, tool, usage, parallel calls, interleaving, legal duplicates, and arbitrary legal fragmentation are deterministic.
- [ ] Every listed illegal sequence fails closed with `InvalidUpstreamEvent` or `IncompleteStream` (or the existing typed registry/conversation conflict where authoritative).
- [ ] Tool identity restores exactly from the frozen registry; valid object JSON is required before tool visibility; ledger reaches `ReturnedToClaude` only after visibility.
- [ ] Reasoning encrypted identity follows Stage 3 bindings; plaintext reasoning is not persisted.
- [ ] Usage/stop reason match terminal state and the encoder performs no semantic repair.
- [ ] Output-visible failures cannot fall back unconditionally; tool-visible failures cannot automatically retry.
- [ ] Forensics/replay artifacts are structural and contain none of the forbidden sentinel content.
- [ ] Legacy behavior and other providers remain unchanged; shadow sends exactly one upstream request and does not mutate enabled state.
- [ ] Offline replay covers all ten required fixtures and reports `network_requests = 0`.
- [ ] `.codegraph/` remains untouched and uncommitted.
- [ ] Stage 5 and Stage 6 remain unimplemented.

## Test Commands

Run from `src-tauri` unless noted:

```text
cargo fmt --all -- --check
cargo check
cargo test --lib
cargo test conversation_ledger --lib
cargo test streaming_responses --lib
cargo test proxy::claude_codex_bridge --lib
cargo test bridge_forensics --lib
cargo test proxy::forwarder --lib
cargo test typed_responses_event --lib -- --nocapture
cargo test strict_stream_state --lib -- --nocapture
cargo test strict_streaming_responses --lib -- --nocapture
cargo test strict_stream_fragmentation --lib -- --nocapture
cargo test streaming_event_replay --lib -- --nocapture
git diff --check
```

If the same verification fails more than twice, stop broadening changes and report the current error, attempted fixes, root-cause assessment, and recommended next step.

## Explicitly Out of Scope

- Stage 5 full shadow comparison/opt-in live rollout.
- Stage 6 default enablement or legacy removal.
- Live Codex OAuth smoke tests or online capability probes.
- Persistent ledger, cross-process state, tool execution, or MCP execution.
- Other providers, clients, or protocol directions.
- General-purpose codec rewrite, unrelated warning/lint cleanup, dependency upgrades, `.codegraph/` changes, automatic push, or PR creation.
