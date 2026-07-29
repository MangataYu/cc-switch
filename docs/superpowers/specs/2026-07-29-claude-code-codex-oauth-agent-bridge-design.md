# Claude Code → Codex OAuth Agent Bridge Design

**Date:** 2026-07-29
**Status:** Stages 0–2 implemented; Stage 3 pending
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

Implemented on 2026-07-29. Each prepared turn freezes a request-scoped registry covering the current Claude Code built-ins plus deterministic MCP/dynamic bindings; schema adaptation is loss-reported and fail-closed, non-streaming and existing SSE codecs restore exact registered identities, IDs, and validated arguments, and forensic replay verifies registry, capability, and transform-decision evidence without network access. `BatchTool` remains explicitly unsupported. Conversation-ledger behavior remains Stage 3, and the strict typed streaming state machine remains Stage 4.

### Stage 3: Conversation ledger

Track session/turn identity, calls, reasoning items, retries, and compaction epochs. Replace post-hoc orphan cleanup in the new path with explicit ledger decisions.

### Stage 4: Strict streaming state machine

Decode Responses SSE into typed events, validate them through the prepared turn, and encode validated Claude SSE. Add property and replay coverage for fragmentation and failure cases.

### Stage 5: Shadow comparison and opt-in live use

Run local shadow compilation without duplicate upstream requests. Resolve unexplained differences, then enable the new path for explicit provider opt-in and live smoke testing.

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
