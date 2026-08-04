# Safe Unknown Responses Event Diagnostics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Preserve a safe identifier for an unrecognized Responses SSE event in the bridge evidence manifest without persisting raw event data or changing client-visible errors.

**Architecture:** Add one private event-label sanitizer at the strict SSE conversion boundary. Derive the label before the payload is moved into the existing decoder, then include only the sanitized label in `StreamingFailureContext.event_kind` when typed decoding fails; leave decoding, redaction, suppression, and client error behavior unchanged.

**Tech Stack:** Rust, serde_json, sha2, Tokio async tests, existing bridge-forensics store.

## Global Constraints

- Store verbatim labels only when the entire value matches `[A-Za-z0-9._-]{1,128}` and passes the existing protocol credential-shape scan.
- Represent every other non-empty label as lowercase SHA-256 with the prefix `sha256:`.
- Represent an absent or empty label as `missing`.
- Never add payload content or a raw unsafe label to logs, manifests, or client errors.
- Do not accept, ignore, translate, or retry new Responses events.

---

### Task 1: Persist a safe unknown-event diagnostic label

**Files:**
- Modify: `src-tauri/src/proxy/providers/streaming_responses.rs:20-35`
- Modify: `src-tauri/src/proxy/providers/streaming_responses.rs:600-625`
- Test: `src-tauri/src/proxy/providers/streaming_responses.rs` test module near the existing evidence tests

**Interfaces:**
- Consumes: `named_event: Option<&str>` and `payload: &serde_json::Value` from `strict_sse_payload`.
- Produces: private `safe_responses_event_label(named_event: Option<&str>, payload: &Value) -> String` and manifest context values formatted as `typed_decode_error:<label>`.

- [x] **Step 1: Write failing integration tests**

Add two Tokio tests using `create_anthropic_sse_stream_from_responses_with_evidence` and the existing `evidence_capture` helper. The first sends `response.future_semantic.delta` with a payload containing `sk-secret-payload`, then asserts:

```rust
assert_eq!(
    manifest.error.streaming.unwrap().event_kind,
    "typed_decode_error:response.future_semantic.delta"
);
assert!(!manifest_json.contains("sk-secret-payload"));
assert!(!client_output.contains("sk-secret-payload"));
```

The second uses the same credential-shaped but grammar-valid value for the named event and payload type, such as `response.sk-secret-label`, then asserts that the manifest context starts with `typed_decode_error:sha256:`, has exactly 64 lowercase hexadecimal digest characters, and contains neither the unsafe label nor payload.

- [x] **Step 2: Run the focused tests and verify RED**

Run:

```powershell
cargo test --manifest-path src-tauri/Cargo.toml unknown_event_diagnostic --lib -- --nocapture
```

Expected: both new tests fail because the manifest currently records only `typed_decode_error`.

- [x] **Step 3: Implement the minimal sanitizer and failure-context wiring**

Import `sha2::{Digest, Sha256}` and reuse `redact_protocol_value`. Add a private helper that prefers a non-empty named event, falls back to a non-empty payload `type`, returns the value verbatim only when it passes the protocol credential-shape scan, its length is at most 128 bytes, and every byte is ASCII alphanumeric or `.`, `_`, `-`; otherwise return `sha256:{:x}` over the original label bytes. Return `missing` when neither source contains a non-empty string.

Immediately after `strict_sse_payload` succeeds, derive the label without consuming `named_event` or `data`:

```rust
let diagnostic_event_label =
    safe_responses_event_label(named_event.as_deref(), &data);
```

On `decode_codex_response_event` failure, replace the fixed context kind with:

```rust
&format!("typed_decode_error:{diagnostic_event_label}")
```

Do not alter the returned `BridgeError`.

- [x] **Step 4: Run focused tests and verify GREEN**

Run:

```powershell
cargo test --manifest-path src-tauri/Cargo.toml unknown_event_diagnostic --lib -- --nocapture
```

Expected: 2 passed, 0 failed.

- [x] **Step 5: Run surrounding bridge and forensic tests**

Run:

```powershell
cargo test --manifest-path src-tauri/Cargo.toml proxy::claude_codex_bridge::streaming::tests --lib
cargo test --manifest-path src-tauri/Cargo.toml proxy::bridge_forensics --lib
cargo test --manifest-path src-tauri/Cargo.toml proxy::providers::streaming_responses::tests --lib
```

Expected: all selected tests pass with no failures.

- [x] **Step 6: Run formatting and static checks**

Run:

```powershell
cargo fmt --manifest-path src-tauri/Cargo.toml -- --check
cargo clippy --manifest-path src-tauri/Cargo.toml --lib -- -D warnings
git diff --check
```

Expected: all commands exit 0.

Verification note: the repository-wide `cargo clippy --lib -- -D warnings` is
currently blocked by eight pre-existing warnings in unrelated files. The same command
passes for this change when only the five existing lint categories are allowed. The
full Rust test suite passes with the single fixed-port proxy test skipped because the
installed CC Switch process is actively listening on port 15721.

- [x] **Step 7: Commit the implementation**

```powershell
git add src-tauri/src/proxy/providers/streaming_responses.rs docs/superpowers/plans/2026-08-04-safe-unknown-responses-event-diagnostics.md
git commit -m "fix(proxy): preserve safe unknown Responses event diagnostics"
```
