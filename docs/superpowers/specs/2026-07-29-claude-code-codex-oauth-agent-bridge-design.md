# Claude Code → Codex OAuth Agent Bridge Design

**Date:** 2026-07-29
**Status:** Stage 5 implementation complete; live rollout blocked pending an authorized smoke rerun
**Scope:** Claude Code client to the built-in Codex OAuth backend only

## 1. Summary

CC Switch currently translates Claude Code's Anthropic Messages traffic into OpenAI Responses traffic and translates the response back. This makes the protocol shape compatible, but it does not preserve enough Agent semantics. Tool definitions are mostly rewrapped, session state is partial, unsupported fields can be dropped silently, and streaming conversion is not governed by one request-scoped state machine. The result can be syntactically valid traffic with poor or unsafe Agent behavior.

This design introduces a dedicated `ClaudeCodexBridge` at the Claude provider seam. The bridge owns Codex OAuth capability negotiation, request-scoped tool bindings, conversation and tool-call state, strict streaming validation, diagnostic capture, and offline replay. Existing JSON/SSE transformation logic is initially reused as an internal codec, then reduced or removed after behavioral parity is proven.

The bridge never executes tools. Claude Code remains the sole owner of local file access, shell execution, MCP calls, permission prompts, hooks, and tool-result production.

## 2. Goals

1. Make Claude Code's local Agent tools understandable to Codex-oriented GPT models without changing who executes them.
2. Restore every upstream tool call to the exact Claude Code tool identity and contract registered for that turn.
3. Make Codex OAuth capabilities explicit and versioned instead of inferred through scattered conditional logic.
4. Track tool calls, reasoning items, retries, stream progress, and compaction epochs as session state.
5. Reject unsafe or ambiguous conversions instead of silently dropping or guessing.
6. Automatically capture failed protocol conversions in a local forensic bundle suitable for manual comparison and future bridge improvements.
7. Provide deterministic, offline replay tests for real Claude Code ↔ Codex OAuth traffic.
8. Introduce the bridge behind a provider-level experiment switch, compare it with the legacy path, and remove the legacy Claude→Codex OAuth semantic path only after explicit exit criteria are met.

## 3. Non-goals

- Supporting Codex → Claude, Gemini, OpenAI Chat Completions, Claude Desktop, or generic third-party Responses gateways.
- Executing shell commands, reading files, applying patches, or invoking MCP from CC Switch.
- Mapping Claude `Read` to OpenAI `file_search`. These tools have different execution and data semantics.
- Persisting prompts, source code, tool output, or reasoning content in the application database.
- Achieving identical model behavior between Claude and GPT. The target is semantic protocol correctness and safe execution, not identical planning style.
- Performing an online capability probe on every user request.
- Rewriting every existing proxy protocol in one migration.

## 4. Confirmed Product Decisions

- The first stable backend is `https://chatgpt.com/backend-api/codex` through the built-in Codex OAuth provider.
- All tools continue to be executed by Claude Code.
- The new bridge is introduced behind a provider-level experimental switch.
- The legacy path remains available for fallback during shadow and rollout stages.
- Failed conversions automatically create local protocol forensic bundles.
- Successful requests record only a redacted structural trace.
- Failed protocol bundles are retained for seven days or until their aggregate size reaches 200 MB, whichever limit is reached first.
- The old Claude→Codex OAuth semantic conversion may be deleted after the new bridge passes the defined rollout gates. Pure codecs still used elsewhere are not removed.

## 5. Architectural Shape

### 5.1 External seam

The Claude provider adapter calls one bridge interface:

```rust
pub struct ClaudeCodexBridge {
    capabilities: Arc<CodexOAuthCapabilities>,
    ledger: Arc<ConversationLedger>,
    trace_store: Arc<BridgeTraceStore>,
}

impl ClaudeCodexBridge {
    pub fn prepare_turn(
        &self,
        request: ClaudeRequest,
        provider: &Provider,
        session: SessionIdentity,
    ) -> Result<PreparedCodexTurn, BridgeError>;
}
```

The result contains the upstream request and all request-scoped state required to interpret its response:

```rust
pub struct PreparedCodexTurn {
    pub request: CodexRequest,
    pub turn_id: TurnId,
    pub tool_registry: Arc<ToolRegistry>,
    pub capability_snapshot: Arc<CodexOAuthCapabilities>,
    pub trace_context: TraceContext,
    stream: BridgeStreamState,
}

impl PreparedCodexTurn {
    pub fn consume_event(
        &mut self,
        event: CodexResponseEvent,
    ) -> Result<Vec<ClaudeStreamEvent>, BridgeError>;
}
```

A response cannot be converted without the `PreparedCodexTurn` that created the request. This prevents response conversion from guessing tool mappings, call identities, and capability decisions.

### 5.2 Internal modules

```text
Claude provider adapter
        |
        v
ClaudeCodexBridge
  +-- CodexOAuthCapabilities
  +-- ToolRegistry
  +-- ConversationLedger
  +-- ResponsesCodec
  +-- BridgeTraceStore / ReplayRunner
```

`ClaudeCodexBridge` is the deep module. Its interface remains small while its implementation hides capability decisions, tool adaptation, state validation, and diagnostic capture. `ResponsesCodec` is an internal adapter responsible only for protocol encoding and decoding.

### 5.3 Placement

The implementation should live under a focused module such as:

```text
src-tauri/src/proxy/claude_codex_bridge/
  mod.rs
  capabilities.rs
  tools.rs
  schema.rs
  ledger.rs
  events.rs
  error.rs
  trace.rs
  replay.rs
```

The exact file split may be adjusted during the implementation plan, but semantic policy must not return to `transform_responses.rs`, `streaming_responses.rs`, or the provider adapter.

## 6. Invariants

1. CC Switch never executes a registered Agent tool.
2. GPT may call only a tool present in the current turn's `ToolRegistry`.
3. Every accepted upstream tool call maps to exactly one Claude tool binding.
4. Tool aliases and capability decisions are frozen for the lifetime of a turn.
5. Tool-call state advances monotonically; conflicting repeated events fail.
6. A tool call returned to Claude Code is not considered executed until a matching later `tool_result` is observed.
7. Automatic provider retry is disabled after any tool call becomes visible to Claude Code.
8. Reasoning state cannot cross session, model, provider, or capability-profile identity.
9. OAuth-private request fields are injected only by the Codex OAuth capability adapter.
10. Unsupported or lossy conversions that can affect execution correctness are rejected explicitly.
11. Authentication material never enters ordinary logs or forensic bundles.
12. The legacy and new paths cannot both send the same user request upstream in shadow mode.

## 7. Tool Semantic Adaptation

### 7.1 Request-scoped registry

Each turn compiles the Claude Code tool directory into bidirectional bindings:

```rust
pub struct ToolBinding {
    pub claude_name: String,
    pub codex_name: String,
    pub claude_schema: serde_json::Value,
    pub codex_schema: serde_json::Value,
    pub execution_owner: ExecutionOwner,
    pub semantics: ToolSemantics,
}

pub enum ExecutionOwner {
    ClaudeCode,
}
```

The registry supports:

```rust
impl ToolRegistry {
    pub fn compile(
        tools: &[ClaudeToolDefinition],
        capabilities: &CodexOAuthCapabilities,
    ) -> Result<(Self, NegotiationReport), BridgeError>;

    pub fn codex_tools(&self) -> &[CodexToolDefinition];

    pub fn restore_call(
        &self,
        call: CodexToolCall,
    ) -> Result<ClaudeToolUse, BridgeError>;
}
```

### 7.2 Built-in aliases

The first profile uses GPT/Codex-oriented function names while retaining Claude Code execution:

| Claude tool | Codex-visible function | Execution owner |
|---|---|---|
| `Read` | `read_file` | Claude Code |
| `Glob` | `find_files` | Claude Code |
| `Grep` | `search_text` | Claude Code |
| `Bash` | `shell_command` | Claude Code |
| `Edit` | `edit_file` | Claude Code |
| `Write` | `write_file` | Claude Code |
| `NotebookEdit` | `edit_notebook` | Claude Code |
| `Task` | `spawn_agent` | Claude Code |

Aliases change only what the model sees. The bridge restores the original Claude tool name and original parameter contract before emitting `tool_use`.

`Read` must never map to `file_search`. OpenAI `file_search` is a hosted vector-store retrieval tool, not a local exact-path read operation.

### 7.3 MCP and dynamic tools

MCP, plugin, and dynamically loaded tools receive deterministic names such as `mcp__<namespace>__<tool>`. If sanitization creates a collision, a short stable hash is appended. The complete forward and reverse mapping is frozen in the turn registry.

Unknown upstream tool names are rejected with `ToolRegistryViolation`; they are never forwarded to Claude Code as invented names.

### 7.4 Tool descriptions

The bridge may replace a built-in tool's model-facing name and description with a Codex-oriented description, but it must preserve operational constraints from the Claude definition. Descriptions must explicitly state:

- The tool acts on the local Claude Code workspace.
- Paths are exact local paths, not vector-store identifiers.
- The tool result arrives in a later user turn.
- The model must not claim the operation succeeded before receiving that result.

For third-party tools, the bridge preserves the supplied description unless only non-semantic formatting changes are required.

### 7.5 Schema adaptation

Schema conversion returns both a schema and a loss report:

```rust
pub struct SchemaAdaptation {
    pub schema: serde_json::Value,
    pub losses: Vec<SchemaLoss>,
}

pub struct SchemaLoss {
    pub source_path: String,
    pub reason: SchemaLossReason,
    pub affects_correctness: bool,
}
```

Changes to required fields, unions, numeric bounds, media-bearing values, or discriminators are recorded. A correctness-affecting loss rejects the request. Non-semantic normalization may proceed but appears in the `NegotiationReport` and trace.

`BatchTool` is unsupported in the first bridge version and produces an explicit capability result. It is not silently filtered. Claude tool search is represented as a Claude-executed function tool; it does not impersonate an OpenAI hosted tool.

## 8. Codex OAuth Capability Negotiation

### 8.1 Versioned static profile

The first implementation uses a versioned static profile:

```rust
pub struct CodexOAuthCapabilities {
    pub profile_version: String,
    pub function_tools: SupportLevel,
    pub parallel_tool_calls: SupportLevel,
    pub encrypted_reasoning: SupportLevel,
    pub image_input: SupportLevel,
    pub strict_json_schema: SupportLevel,
    pub hosted_tools: SupportLevel,
}

pub enum SupportLevel {
    Native,
    Emulated,
    Unsupported,
}
```

The profile is selected once when a turn is prepared and stored in the turn. Configuration changes affect new turns only.

Anthropic requests do not carry the Responses-only `parallel_tool_calls` field. When at least one Claude tool is present, the Codex OAuth request projection therefore enables parallel tool calls by default. An explicit Anthropic `tool_choice.disable_parallel_tool_use: true` projects to `parallel_tool_calls: false`; the converter must not silently disable parallel execution merely because the Responses field was absent at the Claude boundary.

### 8.2 Negotiation report

Request preparation produces a report:

```rust
pub struct NegotiationReport {
    pub profile_version: String,
    pub decisions: Vec<CapabilityDecision>,
    pub schema_losses: Vec<SchemaLoss>,
}
```

Each decision is `Native`, `Emulated`, `Rejected`, or `Degraded`. A degradation that can change which tool executes, the arguments it receives, or whether a tool executes rejects the turn. Harmless representation changes may continue and are traced.

### 8.3 Probes

Existing shell and previous-response probes are development evidence. They do not run on the normal request path. A future profile refresh process may run probes explicitly and use their results to update a checked-in profile version.

## 9. Conversation Ledger

### 9.1 Lifetime and storage

The first implementation is in-memory and partitioned by stable Claude Code session identity. It does not persist prompt, code, tool output, or reasoning content. Claude Code supplies sufficient history to rebuild state after a CC Switch restart; restart recovery may lose optimization state but must not invent tool completion.

```rust
pub struct SessionState {
    pub session_id: SessionId,
    pub generation: u64,
    pub capability_profile_version: String,
    pub turns: BoundedMap<TurnId, TurnState>,
    pub compaction_epoch: u64,
}

pub struct TurnState {
    pub request_fingerprint: RequestFingerprint,
    pub tool_registry: Arc<ToolRegistry>,
    pub calls: HashMap<CallId, ToolCallState>,
    pub reasoning_items: HashMap<ItemId, ReasoningState>,
    pub stream_state: StreamState,
}
```

### 9.2 Tool-call lifecycle

```rust
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

Transitions are monotonic. Duplicate identical deltas may be ignored deterministically; conflicting duplicates fail. Parallel calls are tracked independently and do not rely on completion order.

### 9.3 Retry identity

A retry with the same session identity and request fingerprint reuses the original `TurnId` and tool registry. It must not produce new aliases. Once a tool call has been emitted to Claude Code, automatic upstream retry is disabled because the proxy cannot prove whether the local tool already executed.

### 9.4 Reasoning state

`reasoning.encrypted_content` is bound to the session, turn, item, provider, model, and capability-profile version that produced it. A mismatch rejects replay. The ledger does not expose or persist plaintext reasoning.

### 9.5 Context compaction

The bridge detects a stable session identity with a discontinuous history prefix and starts a new `compaction_epoch`.

- Closed calls from prior epochs may be evicted.
- Active calls remain until resolved or explicitly aborted.
- A `tool_result` without a provable matching call becomes `OrphanToolResult`.
- During the experiment period, a provider option may preserve the legacy text downgrade for comparison, but the downgrade is traced and cannot become the new default silently.
- A new epoch cannot reference an incomplete reasoning item or tool call from an older epoch.

### 9.6 Memory limits

Sessions have a TTL and bounded turn count. Active calls are not evicted. Expiry records only identifiers and state summaries in the trace; content is not retained.

## 10. Streaming State Machine

### 10.1 Internal events

OpenAI Responses SSE is decoded into a finite event model:

```rust
pub enum CodexResponseEvent {
    ResponseStarted,
    ReasoningStarted { item_id: ItemId },
    ReasoningDelta { item_id: ItemId, text: String },
    ReasoningDone { item_id: ItemId, encrypted_content: Option<String> },
    ToolCallStarted { item_id: ItemId, call_id: CallId, codex_name: String },
    ToolArgumentsDelta { call_id: CallId, bytes: Vec<u8> },
    ToolCallDone { call_id: CallId },
    TextStarted { item_id: ItemId },
    TextDelta { item_id: ItemId, text: String },
    TextDone { item_id: ItemId },
    UsageUpdated { usage: CodexUsage },
    ResponseCompleted,
    ResponseFailed { error: UpstreamError },
}
```

The Anthropic SSE encoder accepts only events validated by `PreparedCodexTurn`. It does not repair semantic state.

### 10.2 Invalid sequences

The bridge rejects:

- A tool not registered for the turn.
- Missing or conflicting call IDs.
- Argument bytes after `ToolCallDone`.
- An item changing between reasoning, tool, and text types.
- `ResponseCompleted` with incomplete tool arguments.
- End-of-stream without a terminal response event.
- Conflicting duplicate completion events.
- Tool-result references that cannot be proven against the ledger.

## 11. Error Model

```rust
pub enum BridgeErrorKind {
    CapabilityMismatch,
    ToolRegistryViolation,
    SchemaAdaptationLoss,
    ConversationStateConflict,
    InvalidUpstreamEvent,
    IncompleteStream,
    UpstreamRejected,
}

pub struct BridgeError {
    pub kind: BridgeErrorKind,
    pub stage: BridgeStage,
    pub retryable: bool,
    pub safe_summary: String,
    pub session_id_hash: String,
    pub turn_id: Option<TurnId>,
    pub evidence_bundle_id: Option<String>,
}
```

Every error is logged with provider, model, stage, tool name or event type when available, whether output was already emitted, and the forensic bundle ID. The ordinary log never includes tokens, authorization headers, raw prompts, tool parameters, or tool output.

## 12. Protocol Forensics

### 12.1 Two recording levels

Successful requests produce a redacted structural trace. Failed semantic or protocol conversion automatically produces a local quarantine bundle.

Failures that trigger capture include:

- `CapabilityMismatch`
- `SchemaAdaptationLoss`
- `ToolRegistryViolation`
- `ConversationStateConflict`
- `InvalidUpstreamEvent`
- `IncompleteStream`

### 12.2 Bundle contents

```text
<bundle-id>/
  error.json
  claude-request.json
  codex-request.json
  codex-response.ndjson | codex-response.json
  claude-response.ndjson | claude-response.json
  tool-registry.json
  capability-report.json
  ledger-snapshot.json
  transform-decisions.ndjson
```

The bundle retains the protocol body, tool arguments, paths, prompt, and tool results when they are required to determine whether an alternative conversion is possible. It is stored only in the local application data directory, outside ordinary logs and database exports.

### 12.3 Mandatory credential removal

Before writing a bundle, the trace store removes:

- Authorization and proxy authorization headers.
- API keys, OAuth access tokens, refresh tokens, cookies, and device codes.
- ChatGPT account identifiers and authentication responses.
- Network proxy credentials.
- Configuration fields explicitly marked secret.

Credential redaction is fail-closed. If the recorder cannot prove that authentication material was removed, it records only the structural trace and notes that full capture was suppressed.

### 12.4 Transformation decisions

Every semantic conversion may emit:

```rust
pub struct TransformDecision {
    pub source_path: String,
    pub source_value_type: String,
    pub target_path: Option<String>,
    pub action: TransformAction,
    pub reason_code: String,
    pub capability_reference: Option<String>,
}

pub enum TransformAction {
    Preserved,
    Renamed,
    Normalized,
    Dropped,
    Rejected,
}
```

This allows a person to compare original and converted traffic and understand why each changed or missing field was treated that way.

### 12.5 Retention and user controls

- Retention age: seven days.
- Aggregate limit: 200 MB.
- Cleanup runs when either limit is exceeded.
- Bundles can be listed, exported, and deleted explicitly.
- Export preserves credential redaction.
- No bundle is uploaded automatically.

## 13. Offline Replay

`ReplayRunner` loads a structural trace or quarantine bundle and drives the bridge without network access or a Tauri UI. It must reproduce tool registry compilation, capability decisions, event ordering, ledger transitions, and Claude output shape.

Replay tests are the primary regression seam for real failures. A corrected conversion is accepted only when the original captured failure becomes a deterministic passing replay without weakening invariants.

## 14. Shadow Mode and Rollout

### 14.1 Provider-level switch

The Codex OAuth Claude provider receives a bridge mode:

```text
legacy
shadow
enabled
```

- `legacy`: current production path only.
- `shadow`: legacy path serves the request; the new bridge compiles and compares locally without making a second upstream request.
- `enabled`: the new bridge serves the request; eligible failures may fall back only before upstream output or tool visibility makes fallback unsafe.

### 14.2 Shadow comparison

Shadow mode compares:

- Tool catalog identities and schema hashes.
- Preserved, normalized, dropped, and rejected fields.
- Capability decisions.
- Request structural hashes.
- Recorded response-event restoration when a response is available to both in-process decoders.

It must not double Codex OAuth traffic or quota consumption and must not block the legacy response path.

## 15. Testing Strategy

### 15.1 Test layers

1. Property tests for arbitrary SSE fragmentation, duplication, truncation, and legal event ordering.
2. Unit tests for capability selection, schema adaptation, tool registry restoration, and ledger transitions.
3. Replay tests using redacted real protocol artifacts.
4. Live smoke tests using Claude Code and the Codex OAuth backend.

### 15.2 Required matrix

Tools:

- `Read`, `Glob`, `Grep`, `Bash`, `Edit`, `Write`, `NotebookEdit`, and `Task`.
- MCP and dynamically loaded tools.
- Sanitized-name collisions.
- Unsupported `BatchTool`.

Calls:

- Single and parallel calls.
- Fragmented, empty, and invalid JSON arguments.
- Duplicate call IDs.
- Unknown tool names.
- A call visible before a network interruption.

History and state:

- Normal tool-call/result closure.
- Orphan tool results.
- Context compaction.
- Same-request retry.
- Model or capability-profile changes.
- Child Agent sessions.

Reasoning:

- With and without encrypted content.
- Reasoning around tool calls.
- Rejection of cross-session replay.

Media:

- Image input.
- Image-bearing tool results.
- Unsupported or oversized media.

Security and operations:

- Credential redaction.
- Fail-closed full-capture suppression.
- File permissions.
- Retention age and size.
- Export and delete.

## 16. Delivery Stages

### Stage 0: Forensic capture and replay

Add safe structural tracing, failed-conversion quarantine bundles, retention, export/delete primitives, and an offline replay runner. This stage creates the feedback loop for every following change.

### Stage 1: Bridge seam and Codex OAuth profile

Add `ClaudeCodexBridge`, `PreparedCodexTurn`, the static capability profile, and the provider-level `legacy/shadow/enabled` switch. Initially delegate protocol encoding to the current codec.

Implemented on 2026-07-29. Providers without `bridgeMode` remain on `legacy`; `shadow` compiles and compares locally without an additional upstream request; `enabled` carries the finalized outbound request and capability snapshot in its prepared turn. Non-stream responses are consumed through that turn, while strict per-event streaming ownership remains Stage 4.

### Stage 2: Tool registry and semantic aliases

Compile request-scoped tool bindings, adapt schemas with loss reports, restore calls by registry identity, and reject unknown tools. Cover the built-in Claude tool matrix before MCP and dynamic tools.

Implemented on 2026-07-29. Each prepared turn freezes a request-scoped registry covering the current Claude Code built-ins plus deterministic MCP/dynamic bindings; schema adaptation is loss-reported and fail-closed, non-streaming and enabled strict SSE restore exact registered identities, IDs, and validated arguments, and forensic replay verifies registry, capability, and transform-decision evidence without network access. `BatchTool` remains explicitly unsupported. Conversation-ledger behavior is provided by Stage 3, and strict typed streaming ownership is provided by Stage 4.

### Stage 3: Conversation ledger

Track session/turn identity, calls, reasoning items, retries, and compaction epochs. Replace post-hoc orphan cleanup in the new path with explicit ledger decisions.

Implemented on 2026-07-29. The enabled bridge now uses a concurrency-safe, process-local ledger with hashed session/request/history identities, frozen turn registries and capability snapshots, monotonic parallel tool-call state, matching `tool_result` closure, provider/model/profile-bound encrypted-reasoning identity, prefix-based compaction epochs, protected active-state retention, safe structural snapshots, and zero-network lifecycle replay. Exact same-session retries reuse the original turn, registry, schema-loss report, and capability snapshot; orphan or conflicting results fail closed. Legacy never creates or reads ledger state, while shadow uses an isolated ledger and still makes only the served legacy upstream request.

The default limits are 128 sessions, 32 evictable turns per session, and a 30-minute idle TTL. Active calls, calls already returned to Claude Code, and incomplete reasoning items can temporarily exceed those bounds rather than being evicted. State is intentionally lost on restart and is never shared across processes. The Stage 4 enabled stream path now reports validated tool/reasoning completion into this ledger; legacy remains outside the ledger.

### Stage 4: Strict streaming state machine

Decode Responses SSE into typed events, validate them through the prepared turn, and encode validated Claude SSE. Add property and replay coverage for fragmentation and failure cases.

Implemented on 2026-07-29. Only the scoped Claude Code → built-in Codex OAuth → `openai_responses` path with `bridgeMode=enabled` requires and consumes its creating `PreparedCodexTurn`. A protocol-boundary decoder converts the supported Responses lifecycle into typed response, reasoning, text, tool, usage, completion, and failure events. The turn-bound state machine assigns stable content indices, enforces item identity and type, validates terminal ordering and exact duplicate semantics, restores tool calls through the frozen registry, and advances the Stage 3 ledger. Unknown semantic events, malformed or missing identity, conflicting sequence reuse, post-completion deltas, invalid tool JSON, completion with open items, and terminal-free EOF fail closed. Only validated typed Claude events reach the SSE encoder; it performs no semantic repair.

Output and tool visibility are acknowledged only after the corresponding Claude SSE chunk is yielded. Failures before visibility remain eligible for the existing pre-output retry decision, while any emitted output forbids unconditional legacy fallback and any visible tool forbids automatic retry. Forensic failures record structural event/state decisions, hashed item and call identities, frozen registry/capability fingerprints, terminal state, and visibility without retaining prompt, reasoning, tool arguments/results, or credentials.

Deterministic tests split canonical streams at every SSE byte boundary, including inside multibyte UTF-8, and split completed tool JSON at every logical delta boundary. Offline replay exercises text-only, reasoning plus text, one tool, parallel tools, chunked arguments, the tool-result lifecycle, incomplete streams, invalid ordering, unknown tools, and conflicting duplicates through the production decoder and state machine with `network_requests = 0`.

The limits remain deliberate: ledger and stream buffers are process-local and request-scoped, completed plaintext reasoning and tool arguments are discarded after validation, and legacy and other providers keep their prior codec behavior. Stage 5 adds shadow observation without changing these Stage 4 ownership rules. Stage 6 default enablement and legacy removal are not implemented.

### Stage 5: Shadow comparison and opt-in live use

Run local shadow compilation without duplicate upstream requests. Resolve unexplained differences, then enable the new path for explicit provider opt-in and live smoke testing.

Implemented on 2026-07-30, with live validation still pending explicit authorization. `ShadowComparisonSession` owns a request-scoped isolated prepared turn and emits a typed `ShadowComparisonReport`. Differences have a stable kind, disposition, reason code, safe structural path, and optional structural hashes. Request summaries include capability profile, registry/schema identity, tool/model/stream structure, and transform decisions. Buffered responses are decoded twice locally from the same already-received JSON value. Streaming responses are subscribed once: the existing legacy converter remains authoritative while a synchronous observer consumes the same byte chunks through the strict bridge state machine with a 256 KiB framing limit and a 4096-event limit.

Shadow never replaces the served request or response. Compile, decode, observation, and report failures detach or record the comparison and fail open to the unchanged legacy path. No second HTTP request, OAuth lookup, response-body subscription, spawned task, or unbounded channel is introduced. Shadow ledger state is isolated from the enabled ledger. Stream summaries recognize text, reasoning, tool, usage, visibility, and terminal structure without retaining their plaintext. The older full-content forensic capture path is disabled in shadow mode; shadow diagnostics and replay contain only enums, booleans, counts, stable paths, profile/version values, opaque identifiers, and hashes.

`replay_shadow_comparison` drives the production comparison logic offline and returns `ShadowComparisonReplayReport { comparison, network_requests: 0 }`. The current deterministic coverage includes the supported built-in registry plus MCP/dynamic identity, strict rejection cases, request/non-stream/stream structure, tool visibility, ledger isolation, readiness blockers, and sentinel leak assertions. The readiness reducer is pure and report-only: incomplete fixture coverage, unexplained differences, comparison or forensic failures, unsafe visible-tool retry, unavailable rollback, or any live status other than `passed` blocks readiness.

Provider metadata now supports explicit `bridgeMode = legacy | shadow | enabled` only for Claude + built-in Codex OAuth + `openai_responses`. Missing and old configuration remains `legacy`. The advanced provider form exposes the three modes only in that scope, persists explicit rollback to `legacy`, and never promotes a provider automatically. The next request reads the selected mode, so rollback is immediate and requires no migration.

Offline Rust verification after the live-smoke fixes passed all 2400 library tests (2398 passed, 2 ignored), including 62/62 bridge tests, 46/46 streaming Responses tests, and 84/84 forwarder tests. The `mcp_commands` integration target previously passed 23/23 when rerun with access to its test configuration path. The repository-wide Rust integration run remains affected by Windows symlink privilege error 1314 in `skill_sync`; its first failure poisons the shared test mutex and causes one secondary failure. Focused shadow, bridge-mode, rollout, replay, ledger, streaming, bridge, forensics, and forwarder filters matched and passed. Frontend typecheck, 12 focused bridge-mode/UI tests, formatting, and renderer build passed. The repository-wide Vitest command remains affected by pre-existing test discovery of `.claude/worktrees` and an existing concurrent `App.test.tsx` state leak; the current `App.test.tsx` passes 4/4 when isolated.

An explicitly authorized live smoke was run on 2026-07-30 in an isolated temporary home and disposable work directory. Claude Code text and reasoning streams completed with HTTP 200, client exit 0, request/response `unexplained=0`, `comparison_failures=0`, and a bounded stream comparison. A `Read` flow also completed successfully on the served legacy path and continued after the tool result, but its first tool-call stream produced `comparison_failures=1` and `bounded=false` in the shadow observer. The following continuation turn returned to a bounded, zero-failure comparison. Because any comparison failure blocks rollout, remaining tool flows and `enabled` mode were not run. The provider was immediately changed to explicit `legacy`; the next Claude Code text request completed with no new shadow diagnostic, proving next-request rollback. No visible tool was retried.

`LiveSmokeStatus` is therefore `failed`, and readiness is false with the safe blocker `ComparisonFailures` (the recorded request-local observer reported an incomplete shadow observation). The disposable application/database/OAuth copy, logs, fixtures, and processes were removed after recording only these structural results. The real CC Switch database and real Claude configuration were not changed. A separately authorized live rerun is required before the status can become `passed`.

Post-run offline tracing found two defects. First, after a stream-observer failure discarded its state, every later upstream chunk attempted to initialize from the already-consumed prepared turn and appended another internal failure difference. A test first reproduced four records from one failure plus three later chunks; the request-local observer now enters a detached failure state and records only the original incomplete observation. Second, shadow sends the authoritative legacy request but originally decoded the shared upstream response against the isolated bridge tool registry. A legacy `Read` response therefore conflicted with the bridge registry's `read_file` projection. A production-path regression reproduced that exact boundary using the legacy request transformer, official Responses function-call events, and the legacy SSE converter. Shadow now carries a bounded request-local legacy-to-bridge alias map and reprojects only the observation copy before strict decoding; it does not alter the upstream request, served SSE, enabled-mode registry, or serialized report. The fixture now has zero comparison failures, zero unexplained differences, identical legacy/bridge stream structural hashes, and a bounded completed stream. The historical live status remains failed until a newly authorized rerun passes.

Stage 5 does not make the bridge the default, remove legacy codecs, persist ledger state, execute tools in CC Switch, or relax visible-tool retry safety. Those default/removal decisions remain Stage 6 work and require every rollout gate, including live smoke, to pass.

### Stage 6: Default and legacy removal

Make the bridge the Codex OAuth default after exit criteria pass. Keep an immediate provider rollback for one release cycle, then remove the legacy Claude→Codex OAuth semantic entry points and their obsolete tests.

## 17. Rollout Gates

The bridge may become the default only when:

- Every core built-in tool passes bidirectional unit and replay tests.
- Real `Read`, search, edit, shell, test, MCP, and child-Agent flows pass live smoke tests.
- Every semantic failure generates either a valid quarantine bundle or an explicit fail-closed suppression record.
- No code path executes tools in CC Switch.
- No automatic retry can repeat a possibly visible tool call.
- All unexplained shadow differences are resolved or explicitly accepted as harmless.
- The provider switch can return immediately to the legacy path.

The legacy path may be removed only when:

- The bridge has been the default for one release cycle.
- No known trace requires the legacy path to complete successfully.
- Legacy tests have been replaced with bridge, replay, or codec tests.
- Claude→Codex OAuth semantic logic in the old transformers is covered by the new modules.
- Removing old entry points does not remove codecs used by other traffic directions.

## 18. Worktree Baseline

Before implementation planning, the earlier Read tracing, capability probes,
generated probe targets, and related uncommitted proxy experiments were discarded
at the user's request because they did not establish the required Agent semantics.
Implementation therefore starts from commit `5ad2fafc` with a clean worktree and
must build its own failing tests and replay fixtures rather than depending on those
experiments.

## 19. Success Criteria

The project succeeds when Claude Code can use the Codex OAuth GPT backend while retaining Claude Code as the Agent harness, and when every tool decision is registered, reversible, state-validated, observable, and replayable. A syntactically valid conversion is not sufficient: unsafe ambiguity fails explicitly and leaves evidence that can be inspected to improve the bridge.
