# Claude Codex OAuth Stage 2 Tool Registry Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a request-scoped, immutable tool registry to the Claude Code -> built-in Codex OAuth bridge so every model-visible function is safely reversible to the exact Claude tool identity and schema registered for that turn.

**Architecture:** `ClaudeCodexBridge::prepare_turn` compiles Claude request tools before the existing Responses codec runs, replaces only the codec-produced tool directory and forced tool selector with registry output, and freezes the registry in `PreparedCodexTurn`. The registry owns aliases, canonical schema hashes, schema adaptation decisions, argument validation, and reverse restoration; the existing JSON/SSE codecs remain protocol encoders and Stage 3 ledger/Stage 4 stream-state work is not introduced.

**Tech Stack:** Rust 1.85, serde/serde_json, sha2, existing canonical JSON helpers, existing Responses JSON/SSE codecs, existing Stage 0 forensic bundle/replay infrastructure.

## Global Constraints

- Scope is only `AppType::Claude` targeting built-in `providerType=codex_oauth` with `apiFormat=openai_responses`; Claude Desktop, Codex -> Claude, third-party Responses gateways, and other providers remain unchanged.
- CC Switch never executes tools; every emitted tool remains a Claude Code-executed function tool, including Claude tool search.
- `legacy` behavior is unchanged, including its current codec quirks; `shadow` compiles and compares locally without another upstream request and cannot affect the served legacy result; `enabled` uses the frozen `PreparedCodexTurn` registry.
- Unknown, ambiguous, conflicting, malformed, correctness-affecting lossy, and unsupported BatchTool cases fail closed with `ToolRegistryViolation` or `SchemaAdaptationLoss`.
- Tool aliases change only model-facing identity. Claude name, tool-use ID, and arguments returned to Claude Code come from the current turn's registry and upstream call; no name or contract guessing is allowed.
- Reuse the current JSON/SSE codecs. Do not add the Stage 3 conversation ledger, retry/compaction semantics, or Stage 4 typed strict stream state machine.
- Each behavior follows RED -> verify expected failure -> minimal GREEN -> focused regression -> commit.

## File Structure

- Create `src-tauri/src/proxy/claude_codex_bridge/schema.rs`: schema decision types, loss classification, canonical hash, safe adaptation, and returned-argument validation.
- Create `src-tauri/src/proxy/claude_codex_bridge/tools.rs`: immutable `ToolRegistry`, `ToolBinding`, built-in aliases/descriptions, dynamic naming, collision rejection, request tool compilation, and response restoration.
- Modify `src-tauri/src/proxy/claude_codex_bridge/capabilities.rs`: use the richer Stage 2 `SchemaLoss` model in `NegotiationReport`.
- Modify `src-tauri/src/proxy/claude_codex_bridge/error.rs`: typed fail-closed registry/schema errors and evidence-kind mapping.
- Modify `src-tauri/src/proxy/claude_codex_bridge/mod.rs`: compile/freeze registry, install Codex tools/tool choice, and restore non-stream responses.
- Modify `src-tauri/src/proxy/providers/streaming_responses.rs`: add an optional request-scoped tool-name/argument restoration hook without adding new event-state semantics.
- Modify `src-tauri/src/proxy/handlers.rs`: pass the prepared registry to the existing Responses stream codec.
- Modify `src-tauri/src/proxy/forwarder.rs`: record registry/report/transform-decision evidence and compare Stage 2 shadow fingerprints locally.
- Modify `src-tauri/src/proxy/bridge_forensics/replay.rs`: rebuild a prepared turn from the captured Claude request and use its registry for request/response replay.
- Modify `src-tauri/tests/fixtures/bridge-forensics/non-stream-tool-call/{codex-request.json,codex-response.json,expected-claude-response.json,manifest.json}`: upgrade the Stage 0 fixture from legacy identity to aliased Stage 2 bridge identity and artifact hashes.
- Modify `docs/superpowers/specs/2026-07-29-claude-code-codex-oauth-agent-bridge-design.md`: mark Stages 0-2 implemented only after final acceptance.

---

### Task 1: Schema adaptation, hashes, and correctness classification

**Files:**
- Create: `src-tauri/src/proxy/claude_codex_bridge/schema.rs`
- Modify: `src-tauri/src/proxy/claude_codex_bridge/mod.rs`
- Modify: `src-tauri/src/proxy/claude_codex_bridge/capabilities.rs`
- Test: inline `schema.rs` unit tests

**Interfaces:**
- Produces: `SchemaAction::{Preserve,Normalize,Drop,Reject}`, `SchemaLossReason`, `SchemaLoss { source_path, action, reason, affects_correctness }`, `SchemaAdaptation { schema, schema_hash, decisions }`, `adapt_schema(&Value) -> Result<SchemaAdaptation, BridgeError>`, and `validate_arguments(&Value, &Value) -> Result<(), BridgeError>`.
- Hash contract: SHA-256 over `proxy::json_canonical::canonical_json_string`, encoded as lowercase hex; property insertion order must not change the hash.

- [ ] **Step 1: Write failing schema unit tests**

  Add literal tests proving: object schemas are preserved; missing root `type` normalizes to `object`; root `$schema` is dropped as annotation-only; property order produces the same hash; malformed `required`, non-object roots, and a transformation that would remove `required`/union/bounds/discriminator/media semantics are rejected with `SchemaAdaptationLoss`; returned arguments reject non-objects, missing required keys, wrong primitive types, enum violations, and forbidden additional properties.

- [ ] **Step 2: Run RED**

  Run `cargo test proxy::claude_codex_bridge::schema --lib -- --nocapture` from `src-tauri` and confirm compilation/test failure is caused by the missing Stage 2 schema API.

- [ ] **Step 3: Implement the minimal schema adapter**

  Walk JSON objects recursively. Preserve supported validation keywords and record a `Preserve` root decision; normalize only a missing root type to `"object"`; drop only `$schema` while recording `affects_correctness=false`; reject malformed schema shapes instead of repairing them. Validate the argument subset needed for Claude tool contracts (`type`, `required`, `properties`, `additionalProperties`, `enum`, `const`, `oneOf`/`anyOf`, numeric minimum/maximum, array items) without mutating arguments.

- [ ] **Step 4: Run GREEN and regress capability tests**

  Run `cargo test proxy::claude_codex_bridge --lib -- --nocapture` and confirm all schema and Stage 1 capability tests pass.

- [ ] **Step 5: Commit**

  Commit only this task as `feat(proxy): classify Claude tool schema adaptation`.

### Task 2: Built-in tool registry and fail-closed identity compilation

**Files:**
- Create: `src-tauri/src/proxy/claude_codex_bridge/tools.rs`
- Modify: `src-tauri/src/proxy/claude_codex_bridge/mod.rs`
- Modify: `src-tauri/src/proxy/claude_codex_bridge/error.rs`
- Test: inline `tools.rs` unit tests

**Interfaces:**
- Produces: serializable `ToolBinding { claude_name, codex_name, claude_schema, codex_schema, schema_hash, execution_owner, semantics }`, `ExecutionOwner::ClaudeCode`, `ToolSemantics`, and immutable `ToolRegistry` backed by private ordered bindings plus private forward/reverse maps.
- Produces: `ToolRegistry::compile(&[Value], &CodexOAuthCapabilities) -> Result<(Self, Vec<SchemaLoss>), BridgeError>`, `codex_tools() -> &[Value]`, `bindings() -> &[ToolBinding]`, `schema_fingerprint() -> String`, `restore_call(name, call_id, arguments) -> Result<RestoredToolCall, BridgeError>`, and `restore_response(&Value) -> Result<Value, BridgeError>`.
- Error contract: `BridgeError::ToolRegistryViolation { summary }` for missing names, duplicate Claude names, alias collisions, unknown calls, duplicate reverse mappings, or invalid tool definitions; `BridgeError::SchemaAdaptationLoss { summary }` for schema loss.

- [ ] **Step 1: Write failing registry compilation tests**

  Table-test exact aliases for current Claude forms: `Read->read_file`, `Glob->find_files`, `Grep|Search->search_text`, `Edit->edit_file`, `Write->write_file`, `Bash|Shell->shell_command`, `WebFetch->fetch_url`, `WebSearch->search_web`, `NotebookEdit|Notebook->edit_notebook`, and `Task|Agent->spawn_agent`. Assert descriptions say execution is in the local Claude Code workspace and results arrive later. Assert Claude tool-search spellings remain ordinary `{type:"function"}` tools. Assert `BatchTool` is rejected, not filtered.

- [ ] **Step 2: Run RED**

  Run `cargo test proxy::claude_codex_bridge::tools --lib -- --nocapture` and confirm failure is caused by absent registry types/aliases.

- [ ] **Step 3: Implement built-in compilation and immutable indexes**

  Parse each Claude definition without modifying the request. Require a non-empty unique `name` and object `input_schema`; apply the exact alias table; use adapted schema and canonical hash; generate Responses function objects; build both indexes once; reject any Codex-visible name claimed by more than one Claude binding. Tool search is classified as `ToolSemantics::ClaudeToolSearch` but emitted as a function. BatchTool returns an explicit unsupported error.

- [ ] **Step 4: Add RED tests for stability and collisions**

  Assert identical input produces identical names/hashes regardless of object key order; `Grep` plus `Search`, `Bash` plus `Shell`, `Task` plus `Agent`, duplicate Claude names, a custom tool named `read_file`, empty names, and conflicting sanitized names are rejected before request emission.

- [ ] **Step 5: Implement minimal collision/stability behavior and run GREEN**

  Use ordered compilation and canonical hashes only; never resolve a built-in collision by picking one binding. Run `cargo test proxy::claude_codex_bridge --lib -- --nocapture`.

- [ ] **Step 6: Commit**

  Commit as `feat(proxy): compile request scoped Claude tool registry`.

### Task 3: Built-in bidirectional restoration and exact argument contract

**Files:**
- Modify: `src-tauri/src/proxy/claude_codex_bridge/tools.rs`
- Modify: `src-tauri/src/proxy/claude_codex_bridge/mod.rs`
- Test: inline bridge/tool matrix tests

**Interfaces:**
- Consumes: the Task 2 registry and Task 1 argument validator.
- Produces: exact response restoration where upstream `call_id` becomes Claude `tool_use.id`, registry `claude_name` becomes `tool_use.name`, and the parsed unmodified object becomes `tool_use.input` only after validation against `claude_schema`.

- [ ] **Step 1: Write failing full built-in round-trip matrix**

  For every alias family, construct a literal Claude request with a unique required argument, compile the turn, assert the Codex function name/schema, feed a literal Responses `function_call`, and assert exact original Claude name, call ID, and input object. Include Read, Glob, Grep, Search, Edit, Write, Bash, Shell, WebFetch, WebSearch, NotebookEdit, Notebook, Task, and Agent as independent cases.

- [ ] **Step 2: Run RED**

  Run `cargo test proxy::claude_codex_bridge --lib -- --nocapture` and confirm calls still return Codex aliases or lack registry validation.

- [ ] **Step 3: Implement non-stream restoration**

  Clone the response, visit only `output[]` entries of type `function_call`, require non-empty registered name and call ID, parse `arguments` as an object, validate it against the original schema, replace only the name with the registry's exact Claude name, then delegate to the existing `transform_responses` codec. Do not synthesize IDs, infer names, sanitize arguments, or execute anything.

- [ ] **Step 4: Add RED rejection tests**

  Cover unknown Codex name, empty/missing call ID, malformed JSON, non-object JSON, missing required input, wrong type, extra forbidden argument, and a response name that would be ambiguous in a deliberately invalid registry constructor fixture. Each must return `ToolRegistryViolation`, never a Claude `tool_use`.

- [ ] **Step 5: Implement minimal rejections and run GREEN**

  Run `cargo test proxy::claude_codex_bridge --lib -- --nocapture` and `cargo test proxy::providers::transform_responses --lib`.

- [ ] **Step 6: Commit**

  Commit as `feat(proxy): restore Claude tool calls by registry identity`.

### Task 4: MCP and dynamic tools after built-in acceptance

**Files:**
- Modify: `src-tauri/src/proxy/claude_codex_bridge/tools.rs`
- Test: inline dynamic registry tests

**Interfaces:**
- Acceptance boundary: Tasks 1-3 completely cover built-ins first. This task separately admits MCP/plugin/dynamic Claude function tools without changing built-in behavior.
- Produces: deterministic model-facing dynamic names. Existing valid `mcp__namespace__tool` identities are retained when collision-free; other names are sanitized to `[A-Za-z0-9_-]`, prefixed when necessary, and suffixed with the first 8 hex characters of the binding's canonical identity hash when a sanitized name could collide.

- [ ] **Step 1: Write failing dynamic-tool tests**

  Assert stable names and exact reverse restoration for `mcp__filesystem__stat`, a Unicode/space/punctuation plugin name, two names with the same sanitized base, and a dynamic name colliding with `read_file`. Assert every resulting name has exactly one reverse binding.

- [ ] **Step 2: Run RED**

  Run `cargo test proxy::claude_codex_bridge::tools --lib -- --nocapture` and confirm unsupported dynamic naming/collision tests fail.

- [ ] **Step 3: Implement deterministic dynamic naming**

  Precompute all candidate names, reserve the complete built-in alias namespace, append stable identity hashes where needed, and build the registry only after global uniqueness is proven. Preserve supplied third-party descriptions except non-semantic whitespace normalization recorded in schema/transform decisions.

- [ ] **Step 4: Run GREEN and commit**

  Run `cargo test proxy::claude_codex_bridge --lib -- --nocapture`; commit as `feat(proxy): bind MCP and dynamic Claude tools`.

### Task 5: Prepare-turn integration, tool choice, and mode isolation

**Files:**
- Modify: `src-tauri/src/proxy/claude_codex_bridge/mod.rs`
- Modify: `src-tauri/src/proxy/forwarder.rs`
- Test: bridge and forwarder inline tests

**Interfaces:**
- `PreparedCodexTurn` gains `pub tool_registry: Arc<ToolRegistry>` and retains it unchanged through `finalize_request`.
- `prepare_turn` compiles from the original Claude `tools`, runs the existing request codec, replaces `request.tools` with `registry.codex_tools()`, translates a forced Claude tool choice through the same registry, and appends schema decisions to `NegotiationReport`.
- Shadow comparison includes registry identity fingerprint, schema fingerprint, loss decisions, and canonical request structure; it remains local and diagnostic.

- [ ] **Step 1: Write failing preparation/mode tests**

  Prove enabled request aliases tools and forced tool choice, `Arc::ptr_eq`/fingerprints stay unchanged after final request synchronization, a post-prepare mutation of the original JSON cannot affect the registry, and BatchTool/schema/alias failures abort before an upstream request can be produced. Extend dispatcher counters to prove legacy never compiles a registry, shadow compiles once plus one legacy conversion but exposes no prepared turn/network operation, and enabled compiles exactly once.

- [ ] **Step 2: Run RED**

  Run `cargo test proxy::claude_codex_bridge --lib -- --nocapture` and `cargo test proxy::forwarder --lib -- --nocapture`.

- [ ] **Step 3: Implement request integration and richer shadow comparison**

  Compile before codec invocation, merge report losses deterministically, map forced tool choice by exact Claude forward binding, and compare only local fingerprints in shadow logging. Do not alter `ClaudeCodexBridgeMode::Legacy` or send code.

- [ ] **Step 4: Run GREEN and commit**

  Run the two focused suites plus `cargo test proxy::handlers --lib`; commit as `feat(proxy): route frozen tool registries through bridge modes`.

### Task 6: Existing SSE codec restoration without Stage 4 state machine

**Files:**
- Modify: `src-tauri/src/proxy/providers/streaming_responses.rs`
- Modify: `src-tauri/src/proxy/handlers.rs`
- Modify: `src-tauri/src/proxy/claude_codex_bridge/mod.rs`
- Test: streaming Responses and handler inline tests

**Interfaces:**
- Add a registry-aware variant of the existing stream adapter accepting `Option<Arc<ToolRegistry>>`; legacy callers pass `None` and retain byte behavior.
- For prepared turns, restore tool names on `response.output_item.added`, function argument events carrying names, and complete JSON-response fallback; validate complete argument JSON before the existing codec emits the final Claude tool block.

- [ ] **Step 1: Write failing stream restoration tests**

  Feed existing SSE event shapes for every model-visible alias family and assert emitted `content_block_start` uses the original Claude name and original call ID. Add unknown-name, malformed/final invalid arguments, and non-stream JSON fallback rejection tests. Add a legacy `None` registry byte snapshot proving unchanged output.

- [ ] **Step 2: Run RED**

  Run `cargo test proxy::providers::streaming_responses --lib -- --nocapture` and confirm aliased names are not restored or unknown names are accepted.

- [ ] **Step 3: Implement the smallest restoration hook**

  Resolve names and validate completed arguments through `ToolRegistry` immediately before existing codec emission. Preserve the current fragmentation buffers, stop-reason mapping, EOF behavior, read-offset compatibility for legacy, and evidence observer. Do not introduce typed event ownership or new transition rules.

- [ ] **Step 4: Wire handlers and run GREEN**

  Pass `prepared_codex_turn.tool_registry.clone()` only for enabled scoped Responses streams. Run `cargo test proxy::providers::streaming_responses --lib`, `cargo test proxy::handlers --lib`, and `cargo test proxy::claude_codex_bridge --lib -- --nocapture`.

- [ ] **Step 5: Commit**

  Commit as `feat(proxy): restore streamed Claude tools from turn registry`.

### Task 7: Forensic evidence and fail-closed error capture

**Files:**
- Modify: `src-tauri/src/proxy/forwarder.rs`
- Modify: `src-tauri/src/proxy/handlers.rs`
- Modify: `src-tauri/src/proxy/claude_codex_bridge/error.rs`
- Modify: `src-tauri/src/proxy/bridge_forensics/model.rs` only if serialization needs a Stage 2 field shape adjustment
- Test: forensics, forwarder, and handler inline tests

**Interfaces:**
- Evidence artifacts: `ToolRegistry` serializes bindings/fingerprints; `CapabilityReport` serializes capability decisions plus schema losses; `TransformDecisions` is NDJSON with preserve/rename/normalize/drop/reject actions.
- Registry/schema errors map to `EvidenceErrorKind::{ToolRegistryViolation,SchemaAdaptationLoss}` with safe summaries that exclude tool arguments and credentials.

- [ ] **Step 1: Write failing evidence tests**

  Begin a real temporary capture, prepare a tool turn, force an unknown-call or schema failure, and assert the committed bundle contains ToolRegistry, CapabilityReport, and TransformDecisions artifacts with valid manifest hashes. Assert decisions include alias rename plus schema preserve/normalize/drop/reject as applicable, and raw credentials/arguments do not appear in safe summaries.

- [ ] **Step 2: Run RED**

  Run `cargo test bridge_forensics --lib -- --nocapture` and the focused forwarder/handler bridge evidence tests.

- [ ] **Step 3: Implement evidence recording**

  Record registry/report/decisions when an enabled capture exists, before sending. On preparation or response restoration error, commit the existing capture with the typed evidence kind; retain Stage 0 fail-closed redaction and successful-capture discard behavior.

- [ ] **Step 4: Run GREEN and commit**

  Run `cargo test bridge_forensics --lib -- --nocapture`, `cargo test proxy::forwarder --lib`, and `cargo test proxy::handlers --lib`; commit as `feat(proxy): record tool registry negotiation evidence`.

### Task 8: Offline Stage 0 replay through the Stage 2 bridge

**Files:**
- Modify: `src-tauri/src/proxy/bridge_forensics/replay.rs`
- Modify: `src-tauri/tests/fixtures/bridge-forensics/non-stream-tool-call/claude-request.json`
- Modify: `src-tauri/tests/fixtures/bridge-forensics/non-stream-tool-call/codex-request.json`
- Modify: `src-tauri/tests/fixtures/bridge-forensics/non-stream-tool-call/codex-response.json`
- Modify: `src-tauri/tests/fixtures/bridge-forensics/non-stream-tool-call/expected-claude-response.json`
- Modify: `src-tauri/tests/fixtures/bridge-forensics/non-stream-tool-call/manifest.json`
- Test: replay unit and example execution

**Interfaces:**
- Non-stream replay builds the scoped built-in provider, invokes `ClaudeCodexBridge::prepare_turn`, compares its Codex request, and consumes the response through that same prepared turn.
- `ReplayReport.network_requests` remains literal zero.

- [ ] **Step 1: Upgrade fixture expectations and run RED**

  Change the captured request function from `Read` to `read_file`, change the captured upstream response accordingly, keep expected Claude response `name:"Read"`, update artifact hashes/lengths, and run `cargo test proxy::bridge_forensics::replay::tests::replays_non_stream_tool_call_without_network --lib -- --nocapture`. Confirm the legacy replay path fails identity restoration.

- [ ] **Step 2: Implement bridge replay**

  Construct only the in-memory scoped provider, prepare once, consume once, perform no HTTP calls, and include registry/capability/decision structural comparisons when those artifacts exist. Keep streaming replay on the Stage 2 registry-aware existing codec, not a Stage 4 state machine.

- [ ] **Step 3: Run GREEN**

  Run `cargo test bridge_forensics --lib -- --nocapture` and `cargo run --example replay_bridge_bundle -- tests/fixtures/bridge-forensics/non-stream-tool-call`; assert both request/response matches are true, differences are empty, and `network_requests` is 0.

- [ ] **Step 4: Commit**

  Commit as `test(proxy): replay tool aliases through frozen registry`.

### Task 9: Independent review, acceptance, and status documentation

**Files:**
- Modify: all Critical/Important review findings with a new failing regression test per behavioral fix
- Modify: `docs/superpowers/specs/2026-07-29-claude-code-codex-oauth-agent-bridge-design.md`
- Modify: this plan to mark completed checkboxes

**Interfaces:**
- Review range starts at `e7ec00fe` and ends at the current Stage 2 implementation HEAD.
- Final design status is exactly `Stages 0-2 implemented; Stage 3 pending`.

- [ ] **Step 1: Dispatch independent code review**

  Give a fresh reviewer the design sections 6-8 and 11-17, this plan, base SHA `e7ec00fe`, and implementation HEAD. Require severity-ranked findings for correctness, fail-closed behavior, registry immutability, legacy/shadow isolation, evidence leakage, and scope expansion.

- [ ] **Step 2: Fix every Critical and Important finding with TDD**

  For each valid finding, add a focused failing test, run it to observe the expected failure, implement the minimal fix, rerun the focused and adjacent suites, and commit with a finding-specific message. If a finding is invalid, document the exact code/test evidence rather than changing behavior.

- [ ] **Step 3: Re-dispatch independent verification review**

  Ask the reviewer to verify all prior Critical/Important findings against the new HEAD and report any remaining issues. Repeat Step 2 until none remain.

- [ ] **Step 4: Run final acceptance exactly**

  From `src-tauri` where applicable, run:

  ```text
  cargo fmt --all -- --check
  cargo test --lib
  cargo check --all-targets
  cargo test bridge_forensics --lib -- --nocapture
  cargo test proxy::claude_codex_bridge --lib -- --nocapture
  cargo test proxy::providers::transform_responses --lib
  cargo test proxy::providers::streaming_responses --lib
  cargo test proxy::forwarder --lib
  cargo test proxy::handlers --lib
  cargo run --example replay_bridge_bundle -- tests/fixtures/bridge-forensics/non-stream-tool-call
  git diff --check
  ```

  Capture passed/failed/ignored counts and confirm no new warnings.

- [ ] **Step 5: Audit artifacts and scope**

  Run `git status --short`, inspect all untracked files, scan the diff for access tokens/API keys/cookies/device codes, and confirm no online probe, generated temporary forensic bundle, tool executor, Stage 3 ledger, or Stage 4 state machine was added.

- [ ] **Step 6: Update status and commit documentation**

  Set the design status to `Stages 0-2 implemented; Stage 3 pending`, add a concise implemented note under Stage 2, mark every plan checkbox complete, run `git diff --check`, and commit as `docs: record Claude Codex bridge stage 2`.

- [ ] **Step 7: Verify clean final state**

  Run `git status --short --branch`, `git log --oneline e7ec00fe..HEAD`, and a final `git diff --check`. The worktree must be clean and the final report must list commits, test counts, review conclusion, known Stage 2 limitations, and explicit Stage 3 omissions.
