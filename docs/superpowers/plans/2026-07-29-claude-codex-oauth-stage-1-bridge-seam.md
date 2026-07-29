# Claude Code to Codex OAuth Stage 1 Bridge Seam Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Introduce a request-scoped `ClaudeCodexBridge` and versioned Codex OAuth capability snapshot behind a provider-level `legacy`/`shadow`/`enabled` switch, while keeping the current codec and upstream traffic behavior unchanged.

**Architecture:** Add a focused `proxy::claude_codex_bridge` deep module that owns mode selection, the checked-in static capability profile, negotiation reporting, request preparation, and response conversion delegation. The forwarder invokes it only for Claude Code (`AppType::Claude`) targeting the built-in `codex_oauth` + `openai_responses` provider; `legacy` remains byte-for-byte current behavior, `shadow` compiles locally and compares the bridge request without a second network call, and `enabled` sends the bridge-prepared request and carries its `PreparedCodexTurn` into response handling.

**Tech Stack:** Rust 2021, serde/serde_json, uuid, existing Anthropic↔Responses codecs, existing bridge forensic capture, Cargo test tooling.

## Global Constraints

- Scope is Claude Code (`AppType::Claude`) → built-in Codex OAuth (`providerType=codex_oauth`, `apiFormat=openai_responses`) only; Claude Desktop and every other provider/direction stay on existing paths.
- `legacy` is the default when `bridgeMode` is absent, preserving rollback and existing provider JSON compatibility.
- `shadow` performs no HTTP request beyond the existing legacy request and cannot fail or block the served legacy conversion.
- `enabled` initially delegates protocol encoding/decoding to the current codecs; Stage 2 tool aliases and tool restoration are explicitly out of scope.
- Capability selection is static and versioned per prepared turn; no live probe, database persistence, or network lookup is added.
- CC Switch does not execute tools, retry visible tool calls, or persist successful full-content traces.
- Rust version floor remains `1.85.0`; add no dependencies.

---

### Task 1: Provider bridge-mode configuration

**Files:**
- Modify: `src-tauri/src/provider.rs`

**Interfaces:**
- Produces: `ClaudeCodexBridgeMode::{Legacy, Shadow, Enabled}`, `ProviderMeta.bridge_mode: Option<ClaudeCodexBridgeMode>`, and `Provider::claude_codex_bridge_mode() -> ClaudeCodexBridgeMode`.
- Consumes: Existing `Provider.meta` serde layout.

- [x] **Step 1: Write failing serde/default tests**

Add tests proving omitted mode resolves to legacy, camel-case `bridgeMode` accepts each snake-case value, and serialization preserves `enabled`:

```rust
#[test]
fn claude_codex_bridge_mode_defaults_to_legacy() {
    let provider = create_provider_with_meta(ProviderMeta::default());
    assert_eq!(provider.claude_codex_bridge_mode(), ClaudeCodexBridgeMode::Legacy);
}

#[test]
fn claude_codex_bridge_mode_round_trips_provider_json() {
    for (raw, expected) in [
        ("legacy", ClaudeCodexBridgeMode::Legacy),
        ("shadow", ClaudeCodexBridgeMode::Shadow),
        ("enabled", ClaudeCodexBridgeMode::Enabled),
    ] {
        let meta: ProviderMeta = serde_json::from_value(json!({"bridgeMode": raw})).unwrap();
        assert_eq!(meta.bridge_mode, Some(expected));
    }
}
```

- [x] **Step 2: Run tests and verify red**

Run: `cargo test claude_codex_bridge_mode --lib -- --nocapture`

Expected: compilation fails because the enum, field, and accessor do not exist.

- [x] **Step 3: Implement the minimal configuration model**

Define the enum with `Clone + Copy + Debug + Default + Serialize + Deserialize + PartialEq + Eq`, `#[serde(rename_all = "snake_case")]`, and `Legacy` as `#[default]`. Add `#[serde(rename = "bridgeMode", skip_serializing_if = "Option::is_none")]` to `ProviderMeta`, and make the provider accessor return `Legacy` when metadata or the field is absent.

- [x] **Step 4: Run focused tests and commit**

Run: `cargo test claude_codex_bridge_mode --lib -- --nocapture`

Commit: `feat(provider): configure Claude Codex bridge mode`

---

### Task 2: Static Codex OAuth capability profile and negotiation report

**Files:**
- Create: `src-tauri/src/proxy/claude_codex_bridge/capabilities.rs`
- Create: `src-tauri/src/proxy/claude_codex_bridge/error.rs`
- Create: `src-tauri/src/proxy/claude_codex_bridge/mod.rs`
- Modify: `src-tauri/src/proxy/mod.rs`

**Interfaces:**
- Produces: `CodexOAuthCapabilities::builtin() -> Arc<Self>`, `SupportLevel`, `CapabilityDecisionKind`, `CapabilityDecision`, `SchemaLoss`, `NegotiationReport`, and `BridgeError`.
- Consumes: serde and `ProxyError`; makes no network or database calls.

- [x] **Step 1: Write failing profile tests**

Add module tests asserting one stable profile version and the first profile matrix:

```rust
#[test]
fn builtin_profile_is_versioned_and_explicit() {
    let profile = CodexOAuthCapabilities::builtin();
    assert_eq!(profile.profile_version, "codex-oauth-2026-07-29.v1");
    assert_eq!(profile.function_tools, SupportLevel::Native);
    assert_eq!(profile.parallel_tool_calls, SupportLevel::Native);
    assert_eq!(profile.encrypted_reasoning, SupportLevel::Native);
    assert_eq!(profile.image_input, SupportLevel::Native);
    assert_eq!(profile.strict_json_schema, SupportLevel::Emulated);
    assert_eq!(profile.hosted_tools, SupportLevel::Unsupported);
}

#[test]
fn negotiation_report_covers_every_profile_capability() {
    let report = CodexOAuthCapabilities::builtin().negotiation_report();
    assert_eq!(report.decisions.len(), 6);
    assert!(report.schema_losses.is_empty());
    assert!(report.decisions.iter().any(|d| d.capability == "hosted_tools" && d.decision == CapabilityDecisionKind::Rejected));
}
```

- [x] **Step 2: Run tests and verify red**

Run: `cargo test proxy::claude_codex_bridge::capabilities --lib -- --nocapture`

Expected: the module and types are missing.

- [x] **Step 3: Implement immutable profile/report types**

Implement the six-field profile from the design. Map `Native -> Native`, `Emulated -> Emulated`, and `Unsupported -> Rejected` in a deterministic six-entry report. Keep `SchemaLoss { path, reason }` available but empty in Stage 1 because schema inspection belongs to Stage 2. Define `BridgeError::OutOfScope` and `BridgeError::Codec(ProxyError)` with a `From<ProxyError>` implementation.

- [x] **Step 4: Verify module tests and commit**

Run: `cargo test proxy::claude_codex_bridge --lib -- --nocapture`

Commit: `feat(proxy): define Codex OAuth capability profile`

---

### Task 3: Request-scoped bridge and prepared turn

**Files:**
- Modify: `src-tauri/src/proxy/claude_codex_bridge/mod.rs`

**Interfaces:**
- Produces: `ClaudeCodexBridge::prepare_turn`, `PreparedCodexTurn::{request, turn_id, capability_snapshot, negotiation_report, consume_response}`, and `bridge_scope_matches`.
- Consumes: `transform_claude_request_for_api_format`, `transform_responses::responses_to_anthropic`, `Provider`, `AppType`, optional client session ID, and the static profile.

- [x] **Step 1: Write failing scope and prepared-turn tests**

Test exact scoping across Claude, Claude Desktop, Codex, Codex OAuth, and non-Codex Responses providers. Prepare a literal Claude request and assert the prepared request equals a separately invoked legacy codec result, has a non-empty turn ID, stores an `Arc` capability snapshot with the fixed version, and converts a literal Responses text response to the same Anthropic JSON as the existing response codec.

- [x] **Step 2: Run tests and verify red**

Run: `cargo test proxy::claude_codex_bridge::tests --lib -- --nocapture`

Expected: bridge types and preparation methods are missing.

- [x] **Step 3: Implement the bridge seam using existing codecs**

Use these signatures:

```rust
pub struct ClaudeCodexBridge {
    capabilities: Arc<CodexOAuthCapabilities>,
}

pub struct PreparedCodexTurn {
    pub request: Value,
    pub turn_id: String,
    pub capability_snapshot: Arc<CodexOAuthCapabilities>,
    pub negotiation_report: NegotiationReport,
}

impl ClaudeCodexBridge {
    pub fn prepare_turn(
        &self,
        app_type: &AppType,
        request: Value,
        provider: &Provider,
        session_id: Option<&str>,
    ) -> Result<PreparedCodexTurn, BridgeError>;
}

impl PreparedCodexTurn {
    pub fn consume_response(&self, response: Value) -> Result<Value, BridgeError>;
}
```

Reject calls outside the exact scope before invoking any codec. Generate `turn_id` with UUID v4. Freeze a cloned `Arc` profile and its report into the prepared turn. Do not add tool registry, ledger, stream state, retries, probes, or hosted-tool adaptation.

- [x] **Step 4: Run bridge and legacy codec regression tests and commit**

Run:

```powershell
cargo test proxy::claude_codex_bridge --lib -- --nocapture
cargo test proxy::providers::transform_responses --lib
```

Commit: `feat(proxy): prepare Codex OAuth bridge turns`

---

### Task 4: Route legacy, shadow, and enabled request preparation

**Files:**
- Modify: `src-tauri/src/proxy/forwarder.rs`

**Interfaces:**
- Consumes: Tasks 1–3 bridge mode, scope predicate, bridge preparation, existing forensic capture, and `short_value_hash`.
- Produces: `ForwardResult.prepared_codex_turn: Option<PreparedCodexTurn>` and a pure `prepare_claude_codex_request` dispatcher used only in the scoped request-transform branch.

- [x] **Step 1: Write failing dispatcher tests**

For a literal scoped request, assert:

- `legacy` returns the legacy body and no prepared turn.
- `shadow` returns the exact legacy body and no prepared turn even when a test-only bridge compiler returns an error.
- `enabled` returns the bridge body and a prepared turn.
- non-Claude and non-Codex providers bypass the dispatcher and never create a prepared turn.
- shadow compilation is invoked exactly once and does not expose a second HTTP/upstream operation.

- [x] **Step 2: Run tests and verify red**

Run: `cargo test claude_codex_bridge_dispatch --lib -- --nocapture`

Expected: dispatcher/result field are missing.

- [x] **Step 3: Implement request routing**

Replace only the existing Claude `openai_responses` transform call for exact bridge scope:

- `Legacy`: call the current transform unchanged.
- `Shadow`: prepare the bridge from a clone, always serve the separately computed legacy result, and log only provider ID, profile version, and equal/different structural hash status. A bridge error becomes a safe warning and does not change the legacy result.
- `Enabled`: prepare once, send `prepared.request.clone()`, retain the `PreparedCodexTurn`, and record its serialized `NegotiationReport` as `CapabilityReport` when a Stage 0 evidence capture is active.

Thread the optional turn through every `ForwardResult` construction and retry success tuple without changing other modes or provider behavior.

- [x] **Step 4: Verify dispatcher, forwarder, and codec parity tests and commit**

Run:

```powershell
cargo test claude_codex_bridge_dispatch --lib -- --nocapture
cargo test proxy::forwarder --lib
cargo test proxy::providers::claude --lib
```

Commit: `feat(proxy): route Claude Codex bridge modes`

---

### Task 5: Consume enabled non-stream responses through the prepared turn

**Files:**
- Modify: `src-tauri/src/proxy/handlers.rs`
- Modify: `src-tauri/src/proxy/forwarder.rs`

**Interfaces:**
- Consumes: `ForwardResult.prepared_codex_turn` and `PreparedCodexTurn::consume_response`.
- Produces: enabled-mode non-stream response conversion that cannot run without its request-scoped turn; legacy/shadow and streaming bytes remain on the existing codec path in Stage 1.

- [x] **Step 1: Write failing response-dispatch tests**

Add a pure conversion helper test showing that an enabled prepared turn is consumed for a literal Responses response and produces the same Anthropic JSON as the legacy codec. Add a legacy/no-turn case proving the existing converter remains selected. Add a scope regression proving Claude Desktop cannot carry an enabled prepared Codex turn.

- [x] **Step 2: Run tests and verify red**

Run: `cargo test claude_codex_bridge_response_dispatch --lib -- --nocapture`

Expected: the handler helper and prepared-turn handoff are missing.

- [x] **Step 3: Thread the turn into `handle_claude_transform`**

Take `result.prepared_codex_turn` beside evidence. In the non-stream `openai_responses` branch, call `PreparedCodexTurn::consume_response` when present and map `BridgeError` back to its original `ProxyError`; otherwise call the legacy transformer unchanged. Keep the existing Responses SSE converter in streaming mode while retaining the prepared turn for the lifetime of stream setup; strict per-event turn consumption is Stage 4.

- [x] **Step 4: Verify handler behavior and commit**

Run:

```powershell
cargo test claude_codex_bridge_response_dispatch --lib -- --nocapture
cargo test proxy::handlers --lib
cargo test proxy::providers::streaming_responses --lib
```

Commit: `feat(proxy): consume prepared bridge responses`

---

### Task 6: Stage 1 documentation and full acceptance

**Files:**
- Modify: `docs/superpowers/specs/2026-07-29-claude-code-codex-oauth-agent-bridge-design.md`
- Modify: `docs/superpowers/plans/2026-07-29-claude-codex-oauth-stage-1-bridge-seam.md`

**Interfaces:**
- Consumes: completed implementation and fresh verification output.
- Produces: design status `Stage 0–1 implemented; Stage 2 pending`, checked plan boxes, and final Stage 1 commit.

- [x] **Step 1: Run formatting and complete Rust verification**

Run:

```powershell
cargo fmt --all -- --check
cargo test --lib
cargo check --all-targets
git diff --check
```

Expected: all commands exit 0. Existing dead-code warnings may remain; no new warning may originate from Stage 1 files.

- [x] **Step 2: Re-run Stage 0 forensic acceptance**

Run:

```powershell
cargo test bridge_forensics --lib -- --nocapture
cargo run --example replay_bridge_bundle -- tests/fixtures/bridge-forensics/non-stream-tool-call
cargo test proxy::providers::streaming_responses --lib
cargo test proxy::handlers --lib
```

Expected: replay reports both matches true and `network_requests: 0`; successful SSE/evidence behavior remains unchanged.

- [x] **Step 3: Re-read the design and inspect the final diff**

Verify every Stage 1 deliverable is present and Stage 2–4 semantics are absent. Confirm `git status --short`, `git diff --stat`, and `git diff --check` show only intended changes and no credential/probe artifacts.

- [x] **Step 4: Update docs and commit Stage 1 acceptance**

Update the design status only after Step 1–3 pass, check every completed plan item, then commit:

```powershell
git add docs/superpowers/specs/2026-07-29-claude-code-codex-oauth-agent-bridge-design.md docs/superpowers/plans/2026-07-29-claude-codex-oauth-stage-1-bridge-seam.md
git commit -m "docs: record Claude Codex bridge stage 1"
```

## Stage 1 Exit Criteria

- Existing provider JSON without `bridgeMode` remains `legacy`.
- Mode selection is read only for Claude Code → built-in Codex OAuth Responses traffic.
- The capability profile is static, versioned, serialized, and frozen in every prepared turn.
- `legacy` preserves the Stage 0 request and response paths.
- `shadow` makes no additional upstream call and cannot block the legacy path.
- `enabled` sends the bridge-prepared request and consumes non-stream responses through the same prepared turn while delegating codec behavior.
- Stage 0 evidence can include the capability report on enabled failures without persisting successful full content.
- No Stage 2 tool alias/registry behavior, Stage 3 ledger, or Stage 4 strict stream state machine is introduced early.
- Full Rust tests, all-target checks, formatting, diff checks, Stage 0 replay, and focused Stage 1 tests pass immediately before the final commit.
