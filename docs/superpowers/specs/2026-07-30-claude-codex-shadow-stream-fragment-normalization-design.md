# Claude Codex Shadow Stream Fragment Normalization Design

## Context

The Stage 5 live smoke on CC Switch 3.19.0 passed text, reasoning, `Read`, tool-result continuation, OAuth, and rollback. The Glob/Grep flow executed one `Glob` and one `Grep` successfully, but shadow comparison reported one unexplained stream difference for each tool-producing turn.

The legacy Responses-to-Anthropic stream forwards non-`Read` function argument fragments as multiple `input_json_delta` events. The strict bridge buffers those fragments, validates the complete arguments against the request-scoped tool registry, and emits one validated `input_json_delta`. The final tool identity and arguments are equivalent, but `ShadowComparisonSession::finish_stream` currently compares raw event and tool-event counts, so harmless transport fragmentation becomes `ResponseEventMismatch`.

## Goals

- Treat different fragmentation of the same logical tool-argument delta stream as structurally equivalent.
- Keep tool identity, call identity, content-block ordering, lifecycle, strict argument validation, completion, and terminal-state checks fail-closed.
- Preserve the raw event counts and structural hashes already exposed in safe shadow reports.
- Emit enough content-free diagnostics to identify any future unexplained difference by reason code.
- Lock the live Glob/Grep pattern down with an offline regression before changing production behavior.

## Non-goals

- Do not change the legacy stream served to Claude Code.
- Do not weaken strict bridge decoding, registry validation, conversation-ledger checks, or visible-tool retry rules.
- Do not compare or log prompt text, reasoning text, tool arguments, tool results, OAuth material, or raw SSE payloads.
- Do not opt into `enabled` mode or rerun a visible live tool as part of this code change.

## Design

### Semantic stream tokens

`ShadowStreamAccumulator` will continue recording its existing raw event kinds and counters. In parallel, it will record a comparison-only sequence of semantic tokens derived from:

- Anthropic event kind;
- content-block index when present;
- structural class: text, reasoning, tool, or other.

Consecutive `input_json_delta` events with the same block index collapse into one semantic token. Text, thinking, signature, starts, stops, message lifecycle events, tool blocks, and terminal events never collapse. Tool deltas for different indexes remain distinct.

Both the legacy accumulator and the strict bridge accumulator use the same token builder. This makes one versus many JSON argument fragments equivalent while preserving the surrounding lifecycle.

### Shape comparison

`finish_stream` will determine `shape_matches` from the semantic token sequence instead of raw event counts. It will still require:

- equivalent semantic event order;
- equivalent logical text, reasoning, and tool blocks;
- equivalent terminal-event structure;
- successful strict-machine completion;
- matching legacy and bridge completion state.

Raw `event_count`, per-class event counts, and `structural_hash` remain unchanged in `ShadowStreamShape` for diagnostics. A fragmented stream may therefore have different raw counts while `shape_matches=true`.

### Safe diagnostics

The existing shadow log line will add an ordered, deduplicated list of unexplained reason codes. Paths are intentionally omitted because request-field paths can contain caller-controlled keys. Hashes and payload content remain excluded.

## Error handling

- Invalid or incomplete strict decoding remains a comparison failure and detaches the observer as today.
- Missing starts/stops, changed block ordering, changed block index, different structural class, or a terminal mismatch remains unexplained and blocks rollout.
- Semantic normalization is applied only to consecutive `input_json_delta` events within one logical block; it cannot join separate tool calls, collapse reasoning/text/signature events, or reorder events.

## Test strategy

1. Add a regression using a built-in Glob or Grep tool whose upstream function arguments arrive in multiple delta events. Feed the same upstream SSE through the production legacy converter and shadow observer. Before the fix it must fail with one unexplained `ResponseEventMismatch`.
2. After the fix, require zero unexplained differences, zero comparison failures, bounded observation, matching semantic shape, and different raw event counts to prove the normalization is doing real work.
3. Add a negative test where a lifecycle event or block index differs; it must remain `ResponseEventMismatch`.
4. Assert serialized reports and safe reason-code diagnostics contain none of the fixture path, argument text, call ID, legacy tool name, projected tool name, or caller-controlled request-field key.
5. Run the focused bridge tests, streaming Responses tests, forwarder tests, `cargo check`, formatting, and diff checks.

## Rollout

This change affects shadow comparison only. Legacy remains served and `enabled` remains blocked. After offline verification and review, a new explicitly authorized live smoke must repeat the required shadow matrix before any `enabled` flow.
