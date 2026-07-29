# Claude Code → Codex OAuth Stage 0 Forensics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a fail-closed local forensic capture and offline replay loop for Claude Code → Codex OAuth conversion failures before changing Agent semantics.

**Architecture:** Add a focused `bridge_forensics` module that redacts credentials, stages request/response artifacts during a turn, commits a quarantine bundle only on failure, enforces retention, and replays captured non-stream or SSE responses without network access. Existing Claude/Responses codecs remain behaviorally unchanged in Stage 0; the only proxy integration is evidence collection around the Codex OAuth request and response conversion seams.

**Tech Stack:** Rust 1.85, serde/serde_json, chrono, uuid, sha2, tempfile, zip, Tokio, Tauri 2, existing CC Switch proxy codecs.

## Global Constraints

- Scope is only Claude Code traffic whose selected provider has `providerType == "codex_oauth"` and `apiFormat == "openai_responses"`.
- CC Switch must never execute a Claude Code tool.
- Ordinary logs must never contain raw prompts, tool parameters, tool results, access tokens, refresh tokens, cookies, account identifiers, or proxy credentials.
- Full bundles are local-only and must remove credentials before any artifact becomes durable.
- Credential redaction is fail-closed: uncertain redaction writes a structural-only suppression bundle.
- Failure bundles are retained for seven days or until aggregate size reaches 200 MB, whichever limit is reached first.
- Successful turns delete their staging data and retain only an in-memory structural summary for logging.
- Shadow/replay work must not issue a second Codex OAuth request.
- Existing Claude/Responses conversion behavior must not change in Stage 0.
- Every task uses test-first red/green steps and ends in its own commit.

---

## File Structure

Create the following focused module:

```text
src-tauri/src/proxy/bridge_forensics/
  mod.rs       public interface and constants
  model.rs     serializable manifest, artifact, error, and listing types
  redact.rs    fail-closed header/JSON credential redaction
  store.rs     staging, atomic commit, retention, list/delete/export
  replay.rs    offline bundle loader and codec replay
```

Integrate through:

```text
src-tauri/src/proxy/mod.rs
src-tauri/src/proxy/forwarder.rs
src-tauri/src/proxy/handlers.rs
src-tauri/src/proxy/providers/streaming_responses.rs
src-tauri/src/commands/bridge_forensics.rs
src-tauri/src/commands/mod.rs
src-tauri/src/lib.rs
src-tauri/examples/replay_bridge_bundle.rs
```

Do not add forensic policy to `transform_responses.rs`; it remains a codec.

---

### Task 1: Forensic domain model and fail-closed redaction

**Files:**
- Create: `src-tauri/src/proxy/bridge_forensics/mod.rs`
- Create: `src-tauri/src/proxy/bridge_forensics/model.rs`
- Create: `src-tauri/src/proxy/bridge_forensics/redact.rs`
- Modify: `src-tauri/src/proxy/mod.rs`
- Test: inline `#[cfg(test)]` modules in `model.rs` and `redact.rs`

**Interfaces:**
- Produces: `EvidenceBundleId`, `EvidenceStage`, `EvidenceArtifactKind`, `EvidenceError`, `EvidenceManifest`, `RedactionOutcome`, and `redact_protocol_value(&Value) -> RedactionOutcome`.
- Consumes: `serde_json::Value`, `chrono::DateTime<Utc>`, and UUID v4.

- [ ] **Step 1: Add the module declaration and write failing serialization tests**

In `proxy/mod.rs` add:

```rust
pub mod bridge_forensics;
```

Create `model.rs` with tests that require stable snake_case wire values:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_serializes_stable_stage_and_error_kind() {
        let manifest = EvidenceManifest::new_for_test(
            EvidenceStage::ResponseTransform,
            EvidenceErrorKind::InvalidUpstreamEvent,
        );
        let value = serde_json::to_value(manifest).unwrap();
        assert_eq!(value["stage"], "response_transform");
        assert_eq!(value["error"]["kind"], "invalid_upstream_event");
        assert_eq!(value["format_version"], 1);
    }
}
```

Create `redact.rs` with tests for recursive redaction and fail-closed ambiguity:

```rust
#[test]
fn redacts_credentials_recursively_without_removing_prompt_content() {
    let input = serde_json::json!({
        "headers": {"Authorization": "Bearer secret", "x-api-key": "sk-test"},
        "auth": {"access_token": "a", "refresh_token": "r"},
        "input": [{"role": "user", "content": "inspect src/main.rs"}]
    });
    let outcome = redact_protocol_value(&input);
    assert!(outcome.safe_for_full_capture);
    assert_eq!(outcome.value["headers"]["Authorization"], "[REDACTED]");
    assert_eq!(outcome.value["auth"]["refresh_token"], "[REDACTED]");
    assert_eq!(outcome.value["input"][0]["content"], "inspect src/main.rs");
}

#[test]
fn unknown_credential_shaped_key_suppresses_full_capture() {
    let input = serde_json::json!({"custom_super_secret_credential": "value"});
    let outcome = redact_protocol_value(&input);
    assert!(!outcome.safe_for_full_capture);
    assert!(outcome.uncertain_paths.contains(&"$.custom_super_secret_credential".to_string()));
}
```

- [ ] **Step 2: Run the tests and verify the module is red**

Run:

```powershell
cargo test bridge_forensics --lib -- --nocapture
```

Expected: compilation fails because the forensic types and redactor do not exist.

- [ ] **Step 3: Implement the serializable types**

Use these exact public shapes in `model.rs`:

```rust
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct EvidenceBundleId(pub String);

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceStage {
    RequestTransform,
    UpstreamResponse,
    ResponseTransform,
    StreamTransform,
    Replay,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceErrorKind {
    CapabilityMismatch,
    SchemaAdaptationLoss,
    ToolRegistryViolation,
    ConversationStateConflict,
    InvalidUpstreamEvent,
    IncompleteStream,
    UpstreamRejected,
    LegacyTransformFailure,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EvidenceError {
    pub kind: EvidenceErrorKind,
    pub safe_summary: String,
    pub retryable: bool,
    pub output_already_visible: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EvidenceManifest {
    pub format_version: u32,
    pub bundle_id: EvidenceBundleId,
    pub created_at: DateTime<Utc>,
    pub provider_id: String,
    pub model: String,
    pub session_id_hash: String,
    pub stage: EvidenceStage,
    pub error: EvidenceError,
    pub full_capture: bool,
    pub suppression_reason: Option<String>,
    pub artifacts: Vec<EvidenceArtifact>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EvidenceArtifact {
    pub kind: EvidenceArtifactKind,
    pub file_name: String,
    pub byte_len: u64,
    pub sha256: String,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceArtifactKind {
    ClaudeRequest,
    CodexRequest,
    CodexResponse,
    ClaudeResponse,
    ToolRegistry,
    CapabilityReport,
    LedgerSnapshot,
    TransformDecisions,
}
```

Define `EvidenceArtifactKind` with the exact variants from the design bundle and serialize them as snake_case. Add `EvidenceBundleId::new()` using `Uuid::new_v4()`.

- [ ] **Step 4: Implement recursive redaction**

Implement:

```rust
pub struct RedactionOutcome {
    pub value: Value,
    pub safe_for_full_capture: bool,
    pub redacted_paths: Vec<String>,
    pub uncertain_paths: Vec<String>,
}

pub fn redact_protocol_value(value: &Value) -> RedactionOutcome;
```

Match keys case-insensitively after replacing `-` with `_`. Always redact `authorization`, `proxy_authorization`, `x_api_key`, `api_key`, `access_token`, `refresh_token`, `id_token`, `cookie`, `set_cookie`, `device_code`, `client_secret`, `password`, `chatgpt_account_id`, `account_id`, and `organization_id`. Mark credential-like unknown keys containing `secret`, `credential`, or ending in `_token` as uncertain unless they are on the explicit list. Traverse arrays and objects and record JSONPath-like locations.

- [ ] **Step 5: Run focused tests and the proxy library check**

Run:

```powershell
cargo test bridge_forensics --lib -- --nocapture
cargo check --lib
```

Expected: all forensic tests pass and the library compiles.

- [ ] **Step 6: Commit the domain model**

```powershell
git add src-tauri/src/proxy/mod.rs src-tauri/src/proxy/bridge_forensics
git commit -m "feat(proxy): add bridge forensic model and redaction"
```

---

### Task 2: Atomic staging and failure bundle commit

**Files:**
- Create: `src-tauri/src/proxy/bridge_forensics/store.rs`
- Modify: `src-tauri/src/proxy/bridge_forensics/mod.rs`
- Test: inline tests in `store.rs`

**Interfaces:**
- Consumes: Task 1 `EvidenceManifest`, `EvidenceArtifactKind`, `EvidenceError`, and `redact_protocol_value`.
- Produces: `BridgeForensicStore`, `ForensicTurnCapture`, `CaptureMetadata`, `commit_failure`, and `discard_success`.

- [ ] **Step 1: Write failing store tests using a temporary root**

Add tests that require atomic visibility and fail-closed suppression:

```rust
#[test]
fn committed_bundle_is_atomic_and_contains_redacted_protocol_artifacts() {
    let temp = tempfile::tempdir().unwrap();
    let store = BridgeForensicStore::new(temp.path().to_path_buf());
    let mut capture = store.begin_turn(CaptureMetadata::test_fixture()).unwrap();
    capture.record_json(EvidenceArtifactKind::ClaudeRequest, &json!({
        "headers": {"authorization": "Bearer hidden"},
        "messages": [{"role": "user", "content": "read file"}]
    })).unwrap();
    let info = capture.commit_failure(EvidenceError::test_fixture()).unwrap();
    assert!(info.path.join("manifest.json").is_file());
    let request: Value = serde_json::from_slice(
        &std::fs::read(info.path.join("claude-request.json")).unwrap()
    ).unwrap();
    assert_eq!(request["headers"]["authorization"], "[REDACTED]");
    assert!(std::fs::read_dir(temp.path().join("staging")).unwrap().next().is_none());
}

#[test]
fn uncertain_secret_writes_structural_suppression_bundle_only() {
    let temp = tempfile::tempdir().unwrap();
    let store = BridgeForensicStore::new(temp.path().to_path_buf());
    let mut capture = store.begin_turn(CaptureMetadata::test_fixture()).unwrap();
    capture.record_json(
        EvidenceArtifactKind::CodexRequest,
        &json!({"vendor_credential": "cannot-persist"}),
    ).unwrap();
    let info = capture.commit_failure(EvidenceError::test_fixture()).unwrap();
    let manifest: EvidenceManifest = read_manifest(&info.path).unwrap();
    assert!(!manifest.full_capture);
    assert!(!info.path.join("codex-request.json").exists());
    assert!(manifest.suppression_reason.unwrap().contains("uncertain credential"));
}
```

- [ ] **Step 2: Run the tests and verify failure**

Run:

```powershell
cargo test bridge_forensics::store --lib -- --nocapture
```

Expected: compilation fails because `BridgeForensicStore` does not exist.

- [ ] **Step 3: Implement store and capture interfaces**

Use:

```rust
pub const FORENSIC_FORMAT_VERSION: u32 = 1;
pub const RETENTION_DAYS: i64 = 7;
pub const RETENTION_MAX_BYTES: u64 = 200 * 1024 * 1024;

#[derive(Clone)]
pub struct BridgeForensicStore {
    root: PathBuf,
}

pub struct CaptureMetadata {
    pub provider_id: String,
    pub model: String,
    pub session_id_hash: String,
}

pub struct ForensicTurnCapture {
    store: BridgeForensicStore,
    bundle_id: EvidenceBundleId,
    staging_dir: PathBuf,
    metadata: CaptureMetadata,
    artifacts: Vec<PendingArtifact>,
    full_capture_allowed: bool,
    suppression_reasons: Vec<String>,
}

struct PendingArtifact {
    kind: EvidenceArtifactKind,
    file_name: String,
    path: PathBuf,
    byte_len: u64,
    sha256: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EvidenceBundleInfo {
    pub bundle_id: EvidenceBundleId,
    pub path: PathBuf,
    pub full_capture: bool,
}
```

`record_json` must redact before writing. Write staged artifacts using `crate::config::atomic_write`. `commit_failure` writes `manifest.json` last inside staging, then renames the entire staging directory to `bundles/<bundle-id>`. `discard_success(self)` recursively removes only its validated staging directory.

On Unix, set directories to `0o700` and files to `0o600`. On Windows, rely on the per-user application config directory ACL and document that choice in the module comment; do not shell out to `icacls`.

- [ ] **Step 4: Add NDJSON streaming artifact support**

Implement:

```rust
impl ForensicTurnCapture {
    pub fn record_json(&mut self, kind: EvidenceArtifactKind, value: &Value) -> Result<(), AppError>;
    pub fn append_ndjson(&mut self, kind: EvidenceArtifactKind, value: &Value) -> Result<(), AppError>;
    pub fn commit_failure(self, error: EvidenceError) -> Result<EvidenceBundleInfo, AppError>;
    pub fn discard_success(self) -> Result<(), AppError>;
}
```

Each NDJSON line must be independently redacted and newline terminated. If any line is uncertain, delete all full staged artifacts before committing the structural-only manifest.

Under `#[cfg(test)]`, implement `CaptureMetadata::test_fixture()`,
`EvidenceError::test_fixture()`, and `read_manifest()` exactly as local test
helpers; production code must not expose fixture constructors.

- [ ] **Step 5: Run store tests twice to check deterministic cleanup**

```powershell
cargo test bridge_forensics::store --lib -- --nocapture
cargo test bridge_forensics::store --lib -- --nocapture
```

Expected: both runs pass with no staging directories left behind.

- [ ] **Step 6: Commit atomic bundle storage**

```powershell
git add src-tauri/src/proxy/bridge_forensics
git commit -m "feat(proxy): persist failed bridge evidence bundles"
```

---

### Task 3: Retention, listing, deletion, and ZIP export

**Files:**
- Modify: `src-tauri/src/proxy/bridge_forensics/model.rs`
- Modify: `src-tauri/src/proxy/bridge_forensics/store.rs`
- Test: inline tests in `store.rs`

**Interfaces:**
- Consumes: Task 2 `BridgeForensicStore` and committed bundle layout.
- Produces: `list_bundles`, `delete_bundle`, `export_bundle`, and `enforce_retention`.

- [ ] **Step 1: Write failing lifecycle tests**

Add tests for path safety and deterministic eviction:

```rust
#[test]
fn retention_removes_expired_then_oldest_until_under_size_limit() {
    let fixture = RetentionFixture::new();
    fixture.bundle("expired", days_ago(8), 10);
    fixture.bundle("old", days_ago(2), 120);
    fixture.bundle("new", days_ago(1), 120);
    fixture.store.enforce_retention_at(now(), 200).unwrap();
    assert!(!fixture.bundle_path("expired").exists());
    assert!(!fixture.bundle_path("old").exists());
    assert!(fixture.bundle_path("new").exists());
}

#[test]
fn delete_rejects_path_traversal_bundle_id() {
    let temp = tempfile::tempdir().unwrap();
    let store = BridgeForensicStore::new(temp.path().to_path_buf());
    let error = store.delete_bundle(&EvidenceBundleId("../config".into())).unwrap_err();
    assert!(error.to_string().contains("invalid evidence bundle id"));
}
```

- [ ] **Step 2: Run lifecycle tests and verify red**

```powershell
cargo test bridge_forensics::store::tests::retention --lib -- --nocapture
cargo test bridge_forensics::store::tests::delete_rejects --lib -- --nocapture
```

Expected: compilation fails because lifecycle methods do not exist.

- [ ] **Step 3: Implement safe lifecycle methods**

Add:

```rust
impl BridgeForensicStore {
    pub fn list_bundles(&self) -> Result<Vec<EvidenceBundleSummary>, AppError>;
    pub fn delete_bundle(&self, id: &EvidenceBundleId) -> Result<(), AppError>;
    pub fn export_bundle(&self, id: &EvidenceBundleId, destination: &Path) -> Result<(), AppError>;
    pub fn enforce_retention(&self) -> Result<RetentionReport, AppError>;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EvidenceBundleSummary {
    pub bundle_id: EvidenceBundleId,
    pub created_at: DateTime<Utc>,
    pub provider_id: String,
    pub model: String,
    pub stage: EvidenceStage,
    pub error_kind: EvidenceErrorKind,
    pub full_capture: bool,
    pub byte_len: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct RetentionReport {
    pub removed_expired: u64,
    pub removed_over_limit: u64,
    pub remaining_bundles: u64,
    pub remaining_bytes: u64,
}
```

Accept bundle IDs only when every byte is ASCII alphanumeric, `-`, or `_`. Resolve children through `root.join("bundles").join(id)` only after validation. Sort summaries newest first. Retention first removes bundles older than seven days, then removes oldest remaining bundles until aggregate bytes are at or below 200 MB.

ZIP export must include only files enumerated by the manifest, plus `manifest.json`; never recursively include unexpected files. Write to a temporary sibling and rename on success.

Implement `RetentionFixture`, `days_ago`, and `now` inside the `#[cfg(test)]`
module. The fixture writes minimal valid manifests through store helpers rather
than creating arbitrary directories that production listing would reject.

- [ ] **Step 4: Verify lifecycle and ZIP contents**

```powershell
cargo test bridge_forensics::store --lib -- --nocapture
```

Expected: lifecycle, traversal, ordering, and ZIP whitelist tests pass.

- [ ] **Step 5: Commit lifecycle operations**

```powershell
git add src-tauri/src/proxy/bridge_forensics
git commit -m "feat(proxy): manage bridge evidence retention"
```

---

### Task 4: Expose evidence management through Tauri commands

**Files:**
- Create: `src-tauri/src/commands/bridge_forensics.rs`
- Modify: `src-tauri/src/commands/mod.rs`
- Modify: `src-tauri/src/lib.rs`
- Test: inline tests in `commands/bridge_forensics.rs`

**Interfaces:**
- Consumes: Task 3 store lifecycle methods and `crate::config::get_app_config_dir()`.
- Produces Tauri commands: `list_bridge_evidence`, `delete_bridge_evidence`, `export_bridge_evidence`, and `cleanup_bridge_evidence`.

- [ ] **Step 1: Write command-helper tests without launching Tauri**

Extract plain helpers and test them:

```rust
#[test]
fn evidence_root_is_below_app_config_dir() {
    let root = evidence_root_from(Path::new("C:/tmp/cc-switch"));
    assert_eq!(root, Path::new("C:/tmp/cc-switch/bridge-evidence"));
}

#[test]
fn export_rejects_destination_inside_bundle_root() {
    let root = Path::new("C:/tmp/cc-switch/bridge-evidence");
    let destination = root.join("bundles/out.zip");
    assert!(validate_export_destination(root, &destination).is_err());
}
```

- [ ] **Step 2: Run tests and verify red**

```powershell
cargo test commands::bridge_forensics --lib -- --nocapture
```

Expected: module or helper symbols are missing.

- [ ] **Step 3: Implement commands**

Use these signatures:

```rust
#[tauri::command]
pub fn list_bridge_evidence() -> Result<Vec<EvidenceBundleSummary>, String>;

#[tauri::command]
pub fn delete_bridge_evidence(bundle_id: String) -> Result<(), String>;

#[tauri::command]
pub fn export_bridge_evidence(bundle_id: String, destination: String) -> Result<(), String>;

#[tauri::command]
pub fn cleanup_bridge_evidence() -> Result<RetentionReport, String>;
```

Construct the store at `get_app_config_dir().join("bridge-evidence")`. Validate export destinations are outside the evidence root and do not already point at a directory. Do not add upload or open-in-shell behavior.

- [ ] **Step 4: Register commands and verify compilation**

Add `mod bridge_forensics;` and `pub use bridge_forensics::*;` in `commands/mod.rs`. Add all four command functions to `tauri::generate_handler!` in `lib.rs`.

Run:

```powershell
cargo test commands::bridge_forensics --lib -- --nocapture
cargo check --lib
```

Expected: command helper tests pass and Tauri command registration compiles.

- [ ] **Step 5: Commit management commands**

```powershell
git add src-tauri/src/commands/bridge_forensics.rs src-tauri/src/commands/mod.rs src-tauri/src/lib.rs
git commit -m "feat(commands): manage bridge evidence bundles"
```

---

### Task 5: Capture Codex OAuth request-conversion failures and outbound request bodies

**Files:**
- Modify: `src-tauri/src/proxy/forwarder.rs`
- Test: existing forwarder test module plus new focused tests

**Interfaces:**
- Consumes: Task 2 `BridgeForensicStore`, `ForensicTurnCapture`, and `CaptureMetadata`.
- Produces: a request-scoped `ForensicTurnCapture` carried in the successful forward result for Codex OAuth only.

- [ ] **Step 1: Write failing scope tests**

Add a pure predicate and tests:

```rust
#[test]
fn evidence_capture_is_scoped_to_claude_codex_oauth_responses() {
    assert!(should_capture_bridge_evidence(
        AppType::Claude,
        &provider("codex_oauth", "openai_responses")
    ));
    assert!(!should_capture_bridge_evidence(
        AppType::Codex,
        &provider("codex_oauth", "openai_responses")
    ));
    assert!(!should_capture_bridge_evidence(
        AppType::Claude,
        &provider("github_copilot", "openai_chat")
    ));
}
```

Add a conversion-failure test using an Anthropic request fixture with an invalid tool schema. Assert one bundle is committed with `stage == request_transform`, a Claude request artifact, no Codex request artifact, and a safe error summary.

- [ ] **Step 2: Run focused tests and verify red**

```powershell
cargo test evidence_capture_is_scoped --lib -- --nocapture
cargo test captures_codex_oauth_request_transform_failure --lib -- --nocapture
```

Expected: missing predicate/capture plumbing failures.

- [ ] **Step 3: Create capture before request transformation**

When all scope conditions match, create a capture using:

```rust
let mut evidence = BridgeForensicStore::at_default_location()
    .begin_turn(CaptureMetadata {
        provider_id: provider.id.clone(),
        model: mapped_body.get("model").and_then(Value::as_str).unwrap_or("unknown").to_string(),
        session_id_hash: short_sha256_hex(self.session_id.as_bytes()),
    })?;
evidence.record_json(EvidenceArtifactKind::ClaudeRequest, &mapped_body)?;
```

Wrap only `transform_claude_request_for_api_format` on the scoped path. On error, commit `LegacyTransformFailure` at `RequestTransform`, log the bundle ID, and return the original `ProxyError`. Do not change error text returned to Claude Code in Stage 0.

- [ ] **Step 4: Record the exact finalized outbound body**

After `prepare_upstream_request_body` and local overrides have completed, record `CodexRequest` from `filtered_body`. Carry the live capture in `ForwardResult` as:

```rust
pub bridge_evidence: Option<ForensicTurnCapture>,
```

Initialize it to `None` on all non-scoped paths and in test constructors.

- [ ] **Step 5: Run forwarder and proxy tests**

```powershell
cargo test evidence_capture_is_scoped --lib -- --nocapture
cargo test captures_codex_oauth_request_transform_failure --lib -- --nocapture
cargo test proxy::forwarder --lib
```

Expected: scoped tests pass; other providers create no staging directory.

- [ ] **Step 6: Commit request capture integration**

```powershell
git add src-tauri/src/proxy/forwarder.rs
git commit -m "feat(proxy): capture Codex OAuth request conversion evidence"
```

---

### Task 6: Capture non-streaming response-conversion failures

**Files:**
- Modify: `src-tauri/src/proxy/handlers.rs`
- Modify: `src-tauri/src/proxy/forwarder.rs` if the result handoff needs ownership adjustments
- Test: handler tests in `handlers.rs`

**Interfaces:**
- Consumes: Task 5 `ForwardResult.bridge_evidence` with Claude and Codex request artifacts already staged.
- Produces: committed failure bundles containing the upstream response and any produced Claude response; successful requests discard staging.

- [ ] **Step 1: Write a failing non-stream conversion replay fixture test**

Construct a Codex OAuth response with an unsupported output item:

```rust
let upstream = json!({
    "id": "resp_1",
    "model": "gpt-test",
    "status": "completed",
    "output": [{"type": "unknown_agent_item", "id": "item_1"}],
    "usage": {"input_tokens": 1, "output_tokens": 1, "total_tokens": 2}
});
```

Drive the non-stream handler helper and assert the resulting bundle contains `codex-response.json`, has `stage == response_transform`, and ordinary captured log metadata contains only bundle ID and safe summary.

- [ ] **Step 2: Run the test and verify red**

```powershell
cargo test captures_non_stream_response_transform_failure --lib -- --nocapture
```

Expected: no bundle is produced.

- [ ] **Step 3: Pass capture ownership into `handle_claude_transform`**

Take `result.bridge_evidence` after forwarding and add:

```rust
evidence: Option<ForensicTurnCapture>
```

to `handle_claude_transform`. Limit all behavior to the already-scoped capture; do not recalculate provider scope in the response layer.

- [ ] **Step 4: Record upstream and converted response artifacts**

Before calling `responses_to_anthropic...`, record the parsed/aggregated upstream `Value` as `CodexResponse`. On successful conversion, record `ClaudeResponse` then call `discard_success`. On conversion error, commit the bundle before returning the original error.

When the scoped upstream returns a non-success status, record its parsed JSON or
text envelope as `CodexResponse`, commit `UpstreamRejected` at
`UpstreamResponse`, and then continue through the existing client-facing error
normalization. The evidence path must not replace or reshape the existing error.

If evidence persistence itself fails, log `[BridgeEvidence] capture_failed` with only the safe IO error and continue returning the original conversion error. Evidence failure must not replace the protocol failure.

- [ ] **Step 5: Verify non-stream behavior and usage accounting**

```powershell
cargo test captures_non_stream_response_transform_failure --lib -- --nocapture
cargo test proxy::handlers --lib
```

Expected: capture test passes; existing usage-on-transform-error tests still pass.

- [ ] **Step 6: Commit non-stream capture**

```powershell
git add src-tauri/src/proxy/handlers.rs src-tauri/src/proxy/forwarder.rs
git commit -m "feat(proxy): capture failed Codex OAuth response transforms"
```

---

### Task 7: Capture streaming evidence without changing SSE behavior

**Files:**
- Modify: `src-tauri/src/proxy/bridge_forensics/store.rs`
- Modify: `src-tauri/src/proxy/providers/streaming_responses.rs`
- Modify: `src-tauri/src/proxy/handlers.rs`
- Test: streaming tests in `streaming_responses.rs`

**Interfaces:**
- Consumes: live `ForensicTurnCapture` and existing Responses SSE converter.
- Produces: `ForensicStreamObserver` that records upstream and Claude event lines and commits only on stream failure/incomplete termination.

- [ ] **Step 1: Write failing observer tests**

Test two cases:

```rust
#[tokio::test]
async fn successful_stream_discards_staging_capture() {
    // response.created → output_text.delta → response.completed
    // Assert converted Anthropic bytes are unchanged and no bundle exists.
}

#[tokio::test]
async fn truncated_stream_commits_raw_event_evidence() {
    // response.created → function_call arguments delta → EOF
    // Assert a bundle exists with codex-response.ndjson and stage stream_transform.
}
```

Snapshot the converted bytes in both the observer-disabled and observer-enabled runs and assert byte equality for the successful stream.

- [ ] **Step 2: Run streaming tests and verify red**

```powershell
cargo test successful_stream_discards_staging_capture --lib -- --nocapture
cargo test truncated_stream_commits_raw_event_evidence --lib -- --nocapture
```

Expected: observer API is missing.

- [ ] **Step 3: Implement the observer**

Add:

```rust
pub struct ForensicStreamObserver {
    capture: Option<ForensicTurnCapture>,
    saw_terminal_event: bool,
    output_visible: bool,
}

impl ForensicStreamObserver {
    pub fn upstream_event(&mut self, event: &Value) -> Result<(), AppError>;
    pub fn claude_event(&mut self, event: &Value) -> Result<(), AppError>;
    pub fn mark_output_visible(&mut self);
    pub fn mark_terminal(&mut self);
    pub fn finish(self, stream_error: Option<&str>) -> Result<Option<EvidenceBundleInfo>, AppError>;
}
```

`finish` discards on a terminal success. It commits `IncompleteStream` when EOF arrives without a terminal event and `InvalidUpstreamEvent` when the converter yields an error. It records whether Claude output was already visible.

- [ ] **Step 4: Thread the observer through the existing converter**

Add a new internal function rather than changing the public one:

```rust
pub(crate) fn create_anthropic_sse_stream_from_responses_with_evidence<E>(
    stream: impl Stream<Item = Result<Bytes, E>> + Send + 'static,
    read_offset_protection: Option<ReadOffsetProtection>,
    read_trace: Option<ReadTrace>,
    evidence: Option<ForensicStreamObserver>,
) -> impl Stream<Item = Result<Bytes, std::io::Error>> + Send;
```

Existing public wrappers call it with `None`, preserving their interface. Parse each complete upstream SSE `data` JSON once and offer that `Value` to the observer. Offer each emitted Anthropic event `Value` before serialization. Observer errors are logged safely and disable further capture; they do not change stream bytes.

- [ ] **Step 5: Pass the scoped observer from the handler**

For Codex OAuth streaming only, convert `ForensicTurnCapture` into `ForensicStreamObserver` and call the new internal function. All other providers use the existing path.

- [ ] **Step 6: Run streaming regression suites**

```powershell
cargo test successful_stream_discards_staging_capture --lib -- --nocapture
cargo test truncated_stream_commits_raw_event_evidence --lib -- --nocapture
cargo test proxy::providers::streaming_responses --lib
cargo test proxy::handlers --lib
```

Expected: observer tests pass and existing SSE snapshots remain byte-identical.

- [ ] **Step 7: Commit streaming capture**

```powershell
git add src-tauri/src/proxy/bridge_forensics/store.rs src-tauri/src/proxy/providers/streaming_responses.rs src-tauri/src/proxy/handlers.rs
git commit -m "feat(proxy): capture incomplete bridge streams"
```

---

### Task 8: Offline replay runner and real fixture

**Files:**
- Create: `src-tauri/src/proxy/bridge_forensics/replay.rs`
- Create: `src-tauri/examples/replay_bridge_bundle.rs`
- Create: `src-tauri/tests/fixtures/bridge-forensics/non-stream-tool-call/manifest.json`
- Create: `src-tauri/tests/fixtures/bridge-forensics/non-stream-tool-call/claude-request.json`
- Create: `src-tauri/tests/fixtures/bridge-forensics/non-stream-tool-call/codex-request.json`
- Create: `src-tauri/tests/fixtures/bridge-forensics/non-stream-tool-call/codex-response.json`
- Create: `src-tauri/tests/fixtures/bridge-forensics/non-stream-tool-call/expected-claude-response.json`
- Modify: `src-tauri/src/proxy/bridge_forensics/mod.rs`
- Modify: `src-tauri/src/lib.rs`
- Test: inline replay tests in `replay.rs`

**Interfaces:**
- Consumes: Task 2 bundle layout and existing `transform_responses::{anthropic_to_responses, responses_to_anthropic}` codecs.
- Produces: `replay_bundle(path: &Path) -> Result<ReplayReport, AppError>` and a no-network CLI example.

- [ ] **Step 1: Add a red replay test**

Use the checked-in fixture and assert:

```rust
#[test]
fn replays_non_stream_tool_call_without_network() {
    let fixture = fixture_path("non-stream-tool-call");
    let report = replay_bundle(&fixture).unwrap();
    assert_eq!(report.mode, ReplayMode::NonStreaming);
    assert!(report.codex_request_matches);
    assert!(report.claude_response_matches);
    assert_eq!(report.network_requests, 0);
}
```

The fixture must contain a Claude `Read` definition and a Responses `function_call`; Stage 0 expects the legacy name behavior and records the mismatch for Stage 2 rather than silently rewriting it.

- [ ] **Step 2: Run replay test and verify red**

```powershell
cargo test replays_non_stream_tool_call_without_network --lib -- --nocapture
```

Expected: `replay_bundle` is missing.

- [ ] **Step 3: Implement deterministic non-stream replay**

Define:

```rust
#[derive(Debug, Serialize)]
pub struct ReplayReport {
    pub mode: ReplayMode,
    pub codex_request_matches: bool,
    pub claude_response_matches: bool,
    pub structural_differences: Vec<StructuralDifference>,
    pub network_requests: u32,
}

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReplayMode {
    NonStreaming,
    Streaming,
}

#[derive(Clone, Debug, Serialize)]
pub struct StructuralDifference {
    pub path: String,
    pub expected_type: String,
    pub actual_type: String,
    pub reason: String,
}

pub fn replay_bundle(path: &Path) -> Result<ReplayReport, AppError>;
```

Load only files listed by the manifest. Re-run request and response codecs using manifest metadata, compare canonical JSON values, and report JSON-pointer differences. Never initialize an HTTP client, OAuth manager, database, or Tauri runtime.

- [ ] **Step 4: Add SSE replay mode**

When `codex-response.ndjson` exists, feed recorded event objects through the existing Responses SSE converter using an in-memory stream. Compare parsed Anthropic event objects rather than raw whitespace. Set `ReplayMode::Streaming` and keep `network_requests == 0`.

- [ ] **Step 5: Implement the CLI example**

Re-export the replay entry point from `lib.rs`:

```rust
pub use proxy::bridge_forensics::{replay_bundle, ReplayMode, ReplayReport};
```

The example accepts exactly one bundle path and prints JSON:

```rust
fn main() {
    let path = std::env::args_os().nth(1).expect("usage: replay_bridge_bundle <bundle-dir>");
    let report = cc_switch_lib::replay_bundle(Path::new(&path))
        .expect("replay failed");
    println!("{}", serde_json::to_string_pretty(&report).unwrap());
}
```

Expose only the replay entry point and report types publicly; keep storage internals crate-visible.

- [ ] **Step 6: Run fixture replay and full Stage 0 verification**

```powershell
cargo test bridge_forensics --lib -- --nocapture
cargo run --example replay_bridge_bundle -- tests/fixtures/bridge-forensics/non-stream-tool-call
cargo test proxy::providers::streaming_responses --lib
cargo test proxy::handlers --lib
cargo check --all-targets
```

Expected: replay JSON reports both comparisons true and `network_requests: 0`; all focused suites and all-target compilation pass.

- [ ] **Step 7: Commit replay support**

```powershell
git add src-tauri/src/proxy/bridge_forensics/replay.rs src-tauri/src/proxy/bridge_forensics/mod.rs src-tauri/src/lib.rs src-tauri/examples/replay_bridge_bundle.rs src-tauri/tests/fixtures/bridge-forensics
git commit -m "test(proxy): replay bridge conversion evidence offline"
```

---

### Task 9: Documentation consistency and Stage 0 acceptance

**Files:**
- Modify: `docs/superpowers/specs/2026-07-29-claude-code-codex-oauth-agent-bridge-design.md`
- Create: `docs/guides/claude-codex-bridge-evidence-zh.md`
- Test: repository checks and manual acceptance commands

**Interfaces:**
- Consumes: Tasks 1–8 commands, paths, limits, and replay invocation.
- Produces: operator instructions for locating, exporting, deleting, and replaying evidence.

- [ ] **Step 1: Update the design status and baseline**

Change design status to `Stage 0 implemented; Stage 1 pending` only after all automated checks pass. Ensure the worktree-baseline section states the discarded experiments are not dependencies.

- [ ] **Step 2: Write the operator guide with exact commands**

Document:

```text
Evidence root: <app-config-dir>/bridge-evidence
Retention: 7 days / 200 MB
Replay: cargo run --example replay_bridge_bundle -- <bundle-dir>
Security: local-only, credential-redacted, no automatic upload
```

Include the exact Tauri command names and explain that full prompt/tool content may exist in failure bundles after credential removal.

- [ ] **Step 3: Run the complete acceptance suite**

```powershell
cargo fmt --all -- --check
cargo test bridge_forensics --lib -- --nocapture
cargo test proxy::providers::streaming_responses --lib
cargo test proxy::handlers --lib
cargo check --all-targets
git diff --check
```

Expected: every command exits 0.

- [ ] **Step 4: Inspect a generated failure bundle manually**

Generate the invalid non-stream fixture through the focused test, then verify:

```text
manifest.json exists
authorization/access_token/refresh_token/cookie values do not exist anywhere
claude-request.json and codex-response.json retain non-secret protocol content
the ordinary application log contains only bundle ID and safe summary
replay completes with network_requests == 0
```

- [ ] **Step 5: Commit Stage 0 documentation**

```powershell
git add docs/superpowers/specs/2026-07-29-claude-code-codex-oauth-agent-bridge-design.md docs/guides/claude-codex-bridge-evidence-zh.md
git commit -m "docs: document bridge evidence workflow"
```

---

## Stage 0 Exit Criteria

Stage 1 planning may begin only when all statements are true:

- A request-transform failure produces a local bundle without changing the client-facing error.
- A non-stream response-transform failure records original Claude request, finalized Codex request, upstream response, and safe error metadata.
- A truncated or invalid SSE stream records upstream and emitted event sequences.
- Successful turns leave no durable full-content capture.
- Credential-like ambiguity suppresses full capture rather than risking leakage.
- Retention deletes expired and oldest-over-limit bundles deterministically.
- List, export, delete, and cleanup commands reject traversal and unsafe destinations.
- A checked-in fixture replays request and response conversion with zero network access.
- Existing Claude/Responses tests remain green and successful SSE bytes are unchanged when observation is enabled.
