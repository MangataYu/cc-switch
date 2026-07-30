# Claude Code Codex OAuth Stage 5 Shadow Rollout Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add local, single-upstream shadow comparison and an explicit per-provider `legacy`/`shadow`/`enabled` rollout control, together with deterministic readiness and offline replay, while keeping legacy as the default and sole served path in shadow mode.

**Architecture:** Move shadow policy out of `forwarder.rs` into a focused `claude_codex_bridge::shadow` module. A request-scoped report compares safe structural summaries of the legacy and bridge request paths, then observes the one already-received upstream response through both local decoders: buffered responses are cloned in memory and streaming responses are incrementally tee-observed with bounded structural state. Readiness is a pure reduction over safe report counters and an explicit live-smoke state; provider UI only persists an explicit mode and never changes it automatically.

**Tech Stack:** Rust 2021, Tokio/futures streams, serde/serde_json, SHA-256 structural identities, React 18, TypeScript, Vitest, i18next, existing bridge forensics/replay infrastructure.

## Global Constraints

- Scope is exactly Claude Code + built-in `providerType=codex_oauth` + `apiFormat=openai_responses`.
- `legacy` remains the default for missing and old configuration; Stage 5 never infers or automatically enables `shadow` or `enabled`.
- Shadow makes exactly one upstream HTTP/OAuth request, legacy remains the only served path, and all comparison/capture failures fail open to the unchanged legacy status, headers, body, SSE ordering, and terminal event.
- Shadow state and ledger are request-scoped and isolated from the enabled ledger; no persistent or cross-process conversation ledger is added.
- Reports, logs, replay fixtures, and readiness contain only enums, booleans, counts, stable paths, profile/version, opaque identifiers, and structural hashes—never prompt/source/reasoning/tool argument/result plaintext or credentials.
- Request/stream buffers and observer queues are bounded; cancellation/drop releases all state and no unbounded task or channel is created.
- Visible tools remain non-retryable; Stage 2 registry/schema, Stage 3 ledger/reasoning, and Stage 4 strict-stream invariants remain unchanged.
- Do not execute tools, add online capability probes, upgrade dependencies, redesign general proxy/failover, implement Stage 6, remove legacy code/tests, touch `.codegraph/`, push, or create a PR.
- Live Codex OAuth traffic requires explicit user authorization after all code and offline verification are complete.

---

## Baseline Evidence

- [x] Branch is `internal`, HEAD is `65cdb03f9cde001f71328fc158ea25c4309dfe08`, and the worktree excluding `.codegraph/` is clean.
- [x] No repository `AGENTS.md` was present; `.codegraph/` was not read.
- [x] `cargo fmt --all -- --check` and `cargo check` passed from `src-tauri`.
- [x] `cargo test --lib` passed: 2388 passed, 0 failed.
- [x] Stage 4 focused suites (`conversation_ledger`, `streaming_responses`, `proxy::claude_codex_bridge`, `bridge_forensics`, `proxy::forwarder`, typed/strict stream/fragmentation/replay filters) all matched real tests and passed.

## File and Interface Map

- Create `src-tauri/src/proxy/claude_codex_bridge/shadow.rs`: difference taxonomy, structural summaries and hashes, request/non-stream/stream comparison, bounded observer, readiness calculation, and focused tests.
- Modify `src-tauri/src/proxy/claude_codex_bridge/mod.rs`: export Stage 5 shadow interfaces and expose only the prepared-turn summaries needed by the deep module.
- Modify `src-tauri/src/proxy/forwarder.rs`: replace `ClaudeCodexShadowComparison`/`compare_shadow_turn` with `ShadowComparisonSession`; retain one existing request dispatch and attach request-scoped shadow state to `ForwardResult`.
- Modify `src-tauri/src/proxy/handlers.rs`: feed the same buffered upstream value or the same streaming byte chunks to the shadow comparison before/alongside legacy conversion; never select shadow output for the response.
- Modify `src-tauri/src/proxy/providers/streaming_responses.rs`: add a bounded local observer seam around the existing legacy stream, without another HTTP request and without changing yielded bytes.
- Modify `src-tauri/src/proxy/bridge_forensics/{model.rs,replay.rs,mod.rs}`: safe shadow report artifact/replay and deterministic `network_requests = 0` reports; capture failure/suppression only as counters/reason codes.
- Modify `src-tauri/src/provider.rs`: keep serde/default behavior and add scoped-mode/rollback tests where needed.
- Modify `src/types.ts`: add `ClaudeCodexBridgeMode` and `ProviderMeta.bridgeMode`.
- Modify `src/components/providers/forms/{ProviderForm.tsx,ClaudeFormFields.tsx}`: state, save round-trip, scope-gated advanced selector, and risk/rollback copy.
- Add or modify focused frontend tests beside provider form/type utilities following existing Vitest patterns.
- Modify `src/i18n/locales/{en,zh,zh-TW,ja}.json`: selector labels and concise experimental warning.
- Modify the approved design and this plan for the final implementation record and live-validation truth.

## Core Interfaces

```rust
pub enum ShadowDifferenceDisposition { Equivalent, Expected, Accepted, Unexplained }

pub enum ShadowDifferenceKind {
    CapabilityDriven, SafeNormalization, LegacyOnlyRepair, BridgeStrictRejection,
    RegistrySchemaMismatch, RequestFieldMismatch, ResponseEventMismatch,
    ToolIdentityMismatch, UsageStopMismatch, TerminalMismatch,
    IncompleteObservation, InternalComparisonFailure, Unexplained,
}

pub struct ShadowDifference {
    pub kind: ShadowDifferenceKind,
    pub disposition: ShadowDifferenceDisposition,
    pub reason_code: ShadowReasonCode,
    pub path: String,
    pub legacy_hash: Option<String>,
    pub bridge_hash: Option<String>,
}

pub struct ShadowComparisonReport {
    pub request: ShadowRequestComparison,
    pub response: Option<ShadowResponseComparison>,
    pub state: ShadowStateComparison,
    pub differences: Vec<ShadowDifference>,
    pub readiness: ShadowReadinessSummary,
}

pub struct ShadowComparisonSession { /* isolated prepared turn + bounded observation */ }

impl ShadowComparisonSession {
    pub fn compare_request(prepared: PreparedCodexTurn, legacy: &Value) -> Self;
    pub fn compare_non_streaming(&mut self, upstream: &Value, legacy: &Value);
    pub fn observe_stream_chunk(&mut self, chunk: &[u8]);
    pub fn finish_stream(&mut self, legacy_shape: &ShadowStreamShape);
    pub fn fail_open(&mut self, reason: ShadowReasonCode);
    pub fn report(&self) -> ShadowComparisonReport;
}

pub enum LiveSmokeStatus { NotRun, Pending, Passed, Failed, Blocked }

pub fn calculate_shadow_readiness(input: &ShadowReadinessInput) -> ShadowReadinessSummary;
```

Exact field visibility may be reduced during implementation, but names recorded in the final design must match shipped interfaces.

## Task 1: Typed Difference Taxonomy and Request Comparison

**Files:** create `claude_codex_bridge/shadow.rs`; modify `claude_codex_bridge/mod.rs` and `forwarder.rs`.

**Produces:** typed report/difference/reason-code values; safe canonical structural summaries for catalog identity, exact Claude identity, schema, transforms, capabilities, tool choice, model/stream/reasoning/usage decisions, and request hashes.

- [x] Write failing `shadow_request_comparison_*` tests with literal expected reason codes/dispositions for equivalent input, safe normalization, capability-driven difference, registry/schema mismatch, request-field mismatch, bridge rejection, and unexplained input. Assert sentinel prompt, arguments, result, reasoning, token, cookie, and authorization strings are absent from serialized reports.
- [x] Run `cargo test shadow_request_comparison --lib -- --nocapture`; confirm RED is caused by the missing typed module/interfaces.
- [x] Implement minimal safe structural projection/hashing and deterministic ordering. Every `Expected`/`Accepted` row must carry a non-generic stable reason code; every unknown mismatch must become `Unexplained`.
- [x] Replace the boolean `compare_shadow_turn` path with the typed session while preserving the legacy request as the dispatch result and retaining `prepared_turn: None` for served shadow routing.
- [x] Re-run focused tests, bridge tests, forwarder tests, and `cargo check`; refactor only while green.
- [x] Commit the typed request/response comparison as `df9f4bf3` (`feat(proxy): compare structured shadow turns`).

## Task 2: Same-Response Non-Streaming Shadow Recovery

**Files:** modify `shadow.rs`, `forwarder.rs`, `handlers.rs`, and bridge response tests.

**Consumes:** `ShadowComparisonSession`; the already buffered upstream `Value`; existing legacy codec and isolated shadow prepared turn.

**Produces:** structural Claude-visible response summaries for content/event shape, exact tool identity, opaque call ID hashes, argument validity, reasoning binding, usage, stop reason, terminal state, and isolated ledger transitions.

- [ ] Write failing `shadow_non_streaming_*` tests proving one upstream result is locally decoded twice, bridge failure is classified and fail-open, legacy status/headers/body are byte-identical, and shadow ledger transitions never enter the enabled ledger.
- [ ] Run the focused tests and confirm RED for missing response comparison plumbing.
- [ ] Implement the minimal handler/session seam: clone only the already-buffered JSON value, run legacy serving first/authoritatively, run isolated bridge recovery for comparison, and suppress all comparison/forensics errors from the served response.
- [ ] Add structured comparisons for visible content kinds/counts, restored identities/call IDs, argument validation state, reasoning binding state, usage, stop reason, terminal state, and ledger summary without retaining plaintext content.
- [ ] Re-run focused, handler, bridge, ledger, and forwarder tests plus `cargo check`.
- [ ] Commit as `feat(proxy): compare shadow response recovery`.

## Task 3: Bounded Same-Stream Shadow Observation

**Files:** modify `shadow.rs`, `providers/streaming_responses.rs`, `handlers.rs`, and strict streaming tests.

**Produces:** request-scoped incremental observer with explicit byte/event/item limits, typed event/Claude-shape/usage/tool/terminal/ledger summaries, and deterministic cleanup on EOF/error/drop/cancellation.

- [ ] Write failing `shadow_stream_*` tests for text, reasoning, one tool, parallel tools, malformed/unknown/incomplete events, observer failure, upstream error, forensics failure, cancellation, and limit exhaustion. Assert legacy SSE bytes/order/terminal event are identical with observer success/failure and there is one upstream subscription/request.
- [ ] Run `cargo test shadow_stream --lib -- --nocapture`; confirm RED for the missing observer seam.
- [ ] Implement a synchronous/incremental observer call inside the existing legacy chunk-consumption path. It may retain only bounded framing remainder, typed decisions, counts, hashes, and isolated ledger state; limit overflow becomes `IncompleteObservation` and detaches comparison without blocking/yield replacement.
- [ ] Ensure no spawned task, unbounded channel, second response-body subscription, second OAuth lookup, or second HTTP request is introduced; Drop clears request-local buffers.
- [ ] Re-run shadow stream, legacy streaming, strict streaming, fragmentation, handler, and forwarder suites plus `cargo check`.
- [ ] Commit as `feat(proxy): observe one upstream shadow stream`.

## Task 4: Safe Forensics, Offline Replay, and Full Matrix

**Files:** modify `bridge_forensics/{model.rs,replay.rs,mod.rs}`, `shadow.rs`, bridge tool/ledger/stream tests.

**Produces:** `ShadowComparisonReplayReport { comparison, network_requests: 0 }` through production comparison logic and safe structural evidence.

- [ ] Write failing replay/matrix tests for `Read`, `Glob`, `Grep`, `Bash`, `Edit`, `Write`, `NotebookEdit`, `Task`, MCP/dynamic tools, sanitized collisions, and rejected `BatchTool`; single/parallel calls; fragmented/empty/invalid JSON; duplicate call IDs; unknown tools; visible-tool interruption; result closure; orphan result; compaction; same-request retry; model/profile change; child session; encrypted/no-encrypted reasoning; reasoning around tools; and cross-session rejection.
- [ ] Add failure-path tests for shadow compile/compare failure, forensic suppression/write failure, cancellation, and `network_requests == 0`. Use only structured literal assertions and sentinel leak checks—no sensitive golden payloads.
- [ ] Run `cargo test shadow_replay --lib -- --nocapture` and the matrix filters; confirm RED for missing replay/report behavior.
- [ ] Implement replay by invoking the production request/non-stream/stream comparison APIs directly without constructing any network client or Tauri state.
- [ ] Persist only the safe report artifact (or structural suppression/failure record); evidence-store errors remain fail-open to legacy.
- [ ] Re-run shadow/replay/forensics/tool/ledger/stream suites plus `cargo check`.
- [ ] Commit as `test(proxy): replay structured shadow comparisons`.

## Task 5: Pure Readiness and Rollout Gate

**Files:** modify `shadow.rs`, `bridge_forensics/model.rs` if shared serializable status is needed, and focused tests.

**Produces:** `LiveSmokeStatus`, `ShadowReadinessInput`, `ShadowReadinessSummary`, stable blocking reason codes, and no automatic provider mutation.

- [ ] Write failing `rollout_readiness_*` tests for sample count, supported fixture coverage, expected/accepted/unexplained counts, comparison failure, forensic suppression/failure, visible-tool retry safety, rollback availability, and each live-smoke state.
- [ ] Prove with literal assertions that unexplained differences, missing rollback, unsafe retry, incomplete fixtures, comparison/forensic failures, and `NotRun`/`Pending`/`Failed`/`Blocked` smoke prevent `ready`; local passing tests cannot synthesize `Passed`.
- [ ] Run `cargo test rollout_readiness --lib -- --nocapture`; confirm RED.
- [ ] Implement the pure reduction and safe serialization. It reports only; it has no `Provider` mutator and no automatic mode transition.
- [ ] Re-run readiness, shadow, replay, and provider tests plus `cargo check`.
- [ ] Commit as `feat(proxy): calculate shadow rollout readiness`.

## Task 6: Explicit Provider Opt-In and Immediate Rollback UI

**Files:** modify `src/types.ts`, `ProviderForm.tsx`, `ClaudeFormFields.tsx`, focused frontend tests, four locale files, and backend provider tests if coverage gaps remain.

**Produces:** `ClaudeCodexBridgeMode = "legacy" | "shadow" | "enabled"`; `ProviderMeta.bridgeMode`; scope-gated advanced selector; save/load/import/export round-trip through existing provider serialization.

- [ ] Write failing frontend tests proving missing/old data renders and saves `legacy`, all three values round-trip, the selector appears only for Claude + Codex OAuth + Responses, `enabled` requires an explicit selection, switching to `legacy` is persisted for the next request, and other providers/formats retain no bridge field.
- [ ] Run the exact Vitest files and confirm RED for missing type/state/UI behavior.
- [ ] Add state initialized with `initialData?.meta?.bridgeMode ?? "legacy"`; render the selector in the existing advanced area only for the scoped provider/format; add concise warning that shadow serves legacy, enabled is experimental, and legacy is immediate rollback.
- [ ] Save `bridgeMode` only for the eligible scope while preserving an explicit `legacy` rollback value; delete it when scope changes. Keep backend default and serde compatibility unchanged.
- [ ] Add English, Simplified Chinese, Traditional Chinese, and Japanese strings; do not refactor unrelated UI.
- [ ] Run focused unit tests, `pnpm typecheck`, `pnpm format:check`, full `pnpm test:unit`, and `pnpm build:renderer`.
- [ ] Commit as `feat(ui): add Claude Codex bridge opt-in`.

## Task 7: Documentation, Runbook, Verification, and Scope Audit

**Files:** modify the approved design and this plan; create `docs/superpowers/runbooks/2026-07-30-claude-codex-oauth-stage-5-live-smoke.md`.

- [x] Write the manual smoke runbook before any live call: disposable directory setup/cleanup; no real-project Edit/Write/Bash; ordinary text/reasoning; `Read`; `Glob`/`Grep`; safe `Write`/`Edit`; safe Bash/test; parallel tools; result continuation; optional MCP/dynamic tool; optional child `Task`; immediate rollback; safe structured result recording.
- [x] Check local OAuth login availability without reading or printing token/cookie/Authorization values.
- [ ] If live validation is feasible, report exact flows, quota/network impact, and temporary directory, then stop for explicit authorization before the first network request. Without authorization, record `LiveSmokeStatus::NotRun` and exact status `Stage 5 implementation complete; live validation pending`.
- [x] Update the design with the actual interfaces, taxonomy, single-request proof, legacy-output guarantee, opt-in/rollback semantics, readiness result, replay coverage, live status, limitations, and Stage 6 boundary. Live validation remains explicitly pending.
- [ ] Mark only genuinely completed checkboxes in this plan; scan for placeholders and interface drift.
- [ ] Run the complete verification matrix below and record counts/zero-match filters accurately.
- [ ] Perform the Scope / Leak Audit below, inspect `git diff --check`, `git status --short`, and `.codegraph/` status without reading its content.
- [ ] Commit as `docs: record Claude Codex bridge stage 5`.

## Complete Verification Matrix

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
cargo test shadow --lib -- --nocapture
cargo test bridge_mode --lib -- --nocapture
cargo test rollout --lib -- --nocapture
cargo test replay --lib -- --nocapture
```

Run from the repository root after frontend changes:

```text
pnpm typecheck
pnpm test:unit
pnpm format:check
pnpm build:renderer
git diff --check
git status --short
```

Any broad filter that runs zero tests must be reported as zero-match and followed by the precise module/test filter that proves the behavior.

## Rollback Strategy

- Runtime rollback is a provider edit to explicit `bridgeMode: "legacy"`; the next scoped request reads provider metadata anew and uses only the legacy request/response codec.
- Each implementation commit is responsibility-scoped. If a Stage 5 subsystem must be reverted, revert its commit without reverting Stage 0–4 history or deleting legacy code.
- Shadow observer/report failure is an in-request logical rollback: detach/suppress comparison and continue yielding the exact legacy response.
- No migration writes `enabled`; old and absent fields remain legacy, so downgrading before Stage 6 does not require data migration.

## Acceptance Criteria

- [ ] Every scoped shadow request performs exactly one upstream HTTP request and one OAuth acquisition; offline replay reports `network_requests = 0`.
- [ ] Shadow uses the legacy request/response as the only served path and cannot change status, headers, body, SSE ordering, or terminal events even when compile, comparison, observer, or evidence writes fail.
- [ ] Request, non-stream response, stream events/Claude shape, capabilities/registry/schema/transforms, ledger/terminal/visibility, and safe structural hashes are represented in `ShadowComparisonReport`.
- [ ] Every difference has a stable kind, disposition, reason code, and safe structural path; `Expected`/`Accepted` are explained and `Unexplained` prevents readiness.
- [ ] No report/log/forensics/replay/test snapshot contains plaintext prompt, source, reasoning, tool arguments/results, OAuth token, cookie, Authorization, API key, or other credential.
- [ ] Shadow ledger/state is isolated, bounded, and released on completion/cancellation; enabled/legacy state is not contaminated and there are no unbounded buffers/tasks/channels.
- [ ] The full offline tool/call/history/reasoning/failure matrix uses structured assertions through production comparison logic.
- [ ] Provider mode is visible and effective only in the exact scope, defaults to legacy, requires explicit opt-in, round-trips through existing storage/import/export, and can be switched back immediately.
- [ ] Readiness is pure/report-only, accurately represents smoke `not_run`/`pending`, and cannot become ready without passed live smoke, rollback, retry safety, fixture coverage, zero unexplained differences, and healthy comparison/forensics.
- [ ] Stage 2–4 invariants and all legacy/other-provider/client behaviors remain intact.
- [ ] `.codegraph/` remains untouched and uncommitted; no temporary evidence/test files, warnings, TODOs, placeholders, or interface drift remain.
- [ ] No push, PR, dependency upgrade, Stage 6 default, legacy removal, second shadow request, online probe, persistent ledger, or tool execution occurs.
