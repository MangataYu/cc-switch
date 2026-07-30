# Claude Code Codex OAuth Stage 5 Live Smoke Runbook

**Status:** Failed on 2026-07-30 with blocker `FixtureCoverageIncomplete`; rollout remains blocked. All required `shadow` tool fixtures pass. The latest `enabled` run produced a Read closure and identified a post-validation execution-projection defect, now fixed offline, but that live Read result was an error. The later required enabled fixtures therefore remain unverified. Optional MCP/Task remain unverified.

## 2026-07-30 enabled Read execution-projection diagnosis

- Refresh boundary: used an isolated application/database/provider and an access-token-only binary whose refresh path failed closed. The installed CC Switch remained stopped. The real CC Switch OAuth store and real Codex auth file retained their pre-run SHA-256 hashes; no refresh or credential handback occurred.
- Enabled Read evidence: Claude Code emitted exactly one Read and one matching tool-result closure, with no retry. Safe structural inspection confirmed that the requested fixture identity and the supported numeric arguments were correct, but the generated input also contained `pages` as an empty string. Claude Code classified the result as `invalid_arguments`.
- Enabled Write/Edit evidence: the runner's initial safe stdout counter missed the Read call, so the next Write/Edit fixture was started before the Read failure was recovered from the Claude session record. That fixture emitted exactly one Write and one Edit with two unique IDs and two matching closures, with no retry. Write created its disposable file. Edit's input matched the fixture, but Claude Code returned the safe reason `read_precondition`; the original marker remained and the replacement was absent. This is a fixture precondition failure, not evidence of argument corruption.
- Fail-fast boundary: after the session-record evidence exposed both tool errors, the matrix stopped. Enabled Bash, parallel tools, and optional MCP/Task were not run.
- Confirmed production path: prepared streaming strictly validated the original upstream Read arguments through the per-turn registry, then emitted the unprojected raw JSON to Claude Code. The legacy path already removed the semantically empty Read `pages` field. The focused production-converter regression was RED only because prepared output retained that field.
- Minimal offline fix: after strict schema validation, the registry now projects only an exactly empty string `pages` field from Read execution input. Both prepared streaming completion paths serialize the validated projected input. Tool identity, call identity, strict argument validation, retry safety, block order/lifecycle, reasoning/signature, and terminal checks are unchanged; non-empty `pages` and every other argument remain intact.
- Verification: the focused regression is GREEN. `claude_codex_bridge` 76 passed, `streaming_responses` 47 passed, and `forwarder` 84 passed. `cargo check`, `cargo fmt -- --check`, and `git diff --check` passed.
- Rollback and cleanup: returned the isolated provider to explicit `legacy`, completed a no-tool rollback probe with HTTP 200, stopped the isolated application, and deleted the exact temporary directory containing raw prompts, traffic, tool arguments/results, logs, fixtures, and the credential copy. The real credential hashes remained unchanged and no CC Switch process remained.
- Readiness remains `LiveSmokeStatus::Failed` with blocker `FixtureCoverageIncomplete`. A later explicitly authorized live run must verify the fixed Read closure, correct the Edit fixture by reading before editing, and complete enabled Bash, parallel tools, and required continuations before rollout status can change.

## 2026-07-30 current Claude Code schema compatibility fix

- Fail-closed live signal: before any tool became visible, a fresh isolated `enabled` Claude Code Read fixture returned the safe reason code `schema_adaptation_loss`. No visible tool was executed or retried, and all credential hashes remained unchanged.
- Offline structural diagnosis: the current Claude Code request advertised the complete built-in tool directory even when execution permission was restricted to Read. Read's own schema contained no unsupported keyword. Across the full directory, three tool schemas used previously unsupported strict-schema features: two `propertyNames` occurrences and one `format: uri` occurrence. Only keyword enums and counts were retained; raw requests, schemas, prompts, tool arguments, and results were deleted.
- TDD evidence: focused tests first failed because the strict adapter rejected `propertyNames` and `format`. The minimal implementation then passed 3/3 focused tests. `propertyNames` now recursively validates every object key against its child schema; `format` accepts and enforces only `uri` through the existing URL parser. Unknown formats and all other unsupported correctness-affecting keywords still fail closed.
- Live post-fix evidence: an access-token-only, fail-closed no-refresh binary completed an `enabled` no-tool probe with HTTP 200. Two separate Claude Code enabled Read prompts then exited 0 without a top-level error, proving that the complete current tool directory compiled and reached a successful terminal response, but neither prompt emitted a visible tool call or tool-result closure. Per the fail-fast boundary, no further prompt retry or later enabled fixture was run.
- Credential and cleanup boundary: the installed CC Switch remained stopped. The real CC Switch OAuth store, real Codex auth file, and isolated OAuth copy retained their pre-run SHA-256 hashes. The isolated provider was returned to `legacy`, proxy/failover were disabled, and the exact live/offline temporary directories containing raw events, logs, fixtures, schema sampling, and the credential copy were deleted.
- Verification: `claude_codex_bridge` 76 passed, `streaming_responses` 46 passed, and `forwarder` 84 passed. `cargo check`, `cargo fmt -- --check`, and `git diff --check` passed.
- Readiness remains `LiveSmokeStatus::Failed` with blocker `FixtureCoverageIncomplete`. The schema blocker is cleared, but successful enabled Read execution/closure and the remaining enabled matrix still require a later explicit live run.

## 2026-07-30 remaining Stage 5 fixture run

- Refresh boundary: used a fresh isolated application/database/provider with the current access token held only in process memory and a fail-closed refresh guard. The installed CC Switch remained stopped. Safe logs contained zero refresh and zero HTTP 401/403 signals; the real Codex auth file, real CC Switch OAuth store, and isolated OAuth-store copy retained their pre-run SHA-256 hashes.
- Claude Code Write/Edit passed in `shadow`: exactly one `Write` and one `Edit`, two unique tool-use IDs, two matching successful result closures, no other tool, exit 0, and no top-level result error. The disposable file was created, the original marker was absent afterward, and the replacement marker was present. Its three stream reports were bounded and failure-free.
- Claude Code Bash passed in `shadow`: exactly one `Bash`, one matching successful closure, no other tool, exit 0, and no top-level result error. The command only validated a disposable local marker.
- Parallel root cause and TDD fix: the first dedicated Glob/Grep request returned both tools sequentially because the Codex OAuth converter defaulted `parallel_tool_calls` to `false` whenever the Anthropic request omitted that Responses-only field. A focused RED proved tools must default to parallel-enabled; a second RED proved `tool_choice.disable_parallel_tool_use=true` must still force sequential execution. The minimal converter fix passed seven focused Codex OAuth transformation tests and the full 82-test `transform_responses` suite.
- Parallel live GREEN: after rebuilding the isolated no-refresh binary with the fix, Claude Code returned exactly one Glob and one Grep with two unique IDs and two matching successful closures in two upstream turns: the shared initial assistant turn and the terminal continuation. The post-fix stream reports were bounded, failure-free, and had no unexplained differences.
- Aggregate `shadow` evidence: all 14 stream reports in the attempt had `unexplained=0`, `comparison_failures=0`, `bounded=true`, and empty safe reason-code lists.
- `enabled` fail-fast: the ordinary no-tool probe exited 0 without error. The next fixture returned exactly one Read and one matching closure, but the Claude Code tool result was marked as an error. The visible Read was not retried; enabled Write/Edit, Bash, and parallel tools were not run.
- Rollback and cleanup: switched immediately to explicit `legacy`, where a no-tool request exited 0 without error. Across the complete attempt, all 18 recorded upstream requests returned HTTP 200. Proxy/takeover/failover were disabled, the isolated application was stopped, and the exact temporary directory containing raw prompts, SSE, tool arguments/results, logs, fixtures, and the credential copy was deleted.
- Readiness remains `LiveSmokeStatus::Failed` with blocker `FixtureCoverageIncomplete`. Required `shadow` coverage is complete; the `enabled` gate remains incomplete after the Read tool-result error. Optional MCP/Task are still not required for the base gate.

## 2026-07-30 deterministic Write/Edit continuation rerun

- Scope: used the production `/v1/messages` handler in isolated `shadow` mode with a deterministic Claude-protocol client. Explicit tool selection forced the exact Write → result → Edit → error result → terminal sequence that the earlier Claude Code prompts did not produce. This verifies the bridge converter, alias restoration, strict observer, continuation ledger, error-result semantics, and terminal lifecycle; it does not substitute for Claude Code filesystem-execution or permission UX coverage.
- Tool identity and closure: exactly one `Write` and one `Edit` were returned, with two unique tool-use IDs and two matching result closures. The final turn returned no tool and completed normally. No visible tool was retried.
- Upstream and shadow evidence: all three upstream requests returned HTTP 200. The three stream reports recorded `stream_differences` of 2, 2, and 1 respectively; every report had `unexplained=0`, `comparison_failures=0`, `bounded=true`, and an empty safe reason-code list. All request differences were explained alias/tool-choice projections with zero unexplained request differences.
- Refresh and credential boundary: the isolated binary rejected the refresh path and accepted only the current access token in process memory. Safe logs contained zero refresh and zero HTTP 401/403 signals. The real Codex auth file, real CC Switch OAuth store, and isolated OAuth-store copy retained their pre-run SHA-256 hashes. The installed CC Switch remained stopped and was not restarted.
- Rollback and cleanup: returned the isolated provider to explicit `legacy`, disabled proxy/takeover/failover, stopped the isolated application, and deleted the exact temporary directory containing raw prompts, responses, SSE, tool arguments/results, logs, fixtures, and the credential copy.
- Readiness remains `LiveSmokeStatus::Failed`, now with blocker `FixtureCoverageIncomplete`. The earlier Write/Edit `ComparisonFailures` blocker is cleared for the deterministic production bridge chain, but the full Stage 5 live gate cannot pass until the remaining required fixtures and `enabled` opt-in complete under separate authorization.

## 2026-07-30 access-token-only Write/Edit rerun attempt

- Refresh boundary during the isolated run: stopped the installed CC Switch and launched an isolated debug application with the current Codex access token injected only into process memory. A temporary fail-closed guard rejected the refresh path; repository source was restored before the run. Isolated safe logs contained zero refresh or HTTP 401/403 signals.
- Credential integrity before application restart: the real Codex auth file, real CC Switch OAuth store, isolated OAuth-store copy, and real CC Switch database all retained their pre-run SHA-256 hashes. The isolated run did not write either real credential store.
- Non-tool probe: Claude Code exited 0 and reached the isolated `shadow` proxy. Across the complete attempt, four recorded upstream requests returned HTTP 200 and all four stream reports had `comparison_failures=0`, `bounded=true`, and no unexplained differences.
- Inconclusive Write/Edit coverage: two separately restricted Claude Code invocations allowed only `Write` and `Edit`, but each exited 0 without emitting any tool use or tool result. The disposable file was not created. Because no visible tool call, continuation, or result closure occurred, this attempt does not verify the Write/Edit incomplete-observation fix live.
- Retry boundary: no visible tool was retried. After the second no-tool outcome, the run stopped instead of issuing another model request.
- Rollback and cleanup: the isolated provider was returned to explicit `legacy`, proxy/takeover and failover were disabled, the isolated application was stopped, and raw prompts, responses, logs, temporary credentials, and fixtures were deleted.
- Post-cleanup restart incident: starting the installed CC Switch triggered one background access-token refresh before any proxy request. The refresh succeeded and rotated the persisted refresh token, changing the OAuth-store hash while preserving its size; there was no HTTP 401/403. The application was stopped immediately, the newly rotated store was retained, and the now-stale pre-refresh value was not restored. The installed application remains stopped to avoid another refresh without explicit authorization.
- Readiness remains `LiveSmokeStatus::Failed` with blocker `ComparisonFailures`. A later explicitly authorized live run must produce the complete Write → result → Edit → result → terminal sequence before the blocker can be cleared.

## 2026-07-30 Write/Edit incomplete-observation offline fix

- Exact offline reproduction: a three-turn production-converter replay issued `Write`, continued with its matching result, issued `Edit`, continued with an error result, and ended with reasoning, text, and `response.completed`. Before the fix, the final shadow report reproduced `comparison_failures=1`, `bounded=false`, and `unexplained=0` without plaintext or raw-payload diagnostics.
- Confirmed offline cause: the legacy production converter explicitly accepts the identity-bearing legacy lifecycle event `response.reasoning.done`, but the strict shadow typed decoder classified it as an unknown semantic Responses event. That decode failure detached the strict observer and entered the `IncompleteShadowObservation` fail-open path. The preceding Write/Edit identity and schema validation, legacy-name re-projection, unique call identities, both tool-result closures, the Edit error result, and the final terminal event all passed in the same regression.
- Minimal fix: the strict decoder now maps only an identity-bearing `response.reasoning.done` into the existing `ReasoningDone` state-machine event and preserves optional encrypted content through the existing signature path. An anonymous completion still fails closed. Tool identity, call identity, argument validation, visible-tool retry safety, content-block ordering/lifecycle, reasoning/signature handling, and terminal completeness were not relaxed.
- TDD evidence: the focused production-chain test first failed with the exact tuple `(comparison_failures=1, bounded=false, unexplained=0)` and then passed after the typed mapping. A focused decoder test also proves encrypted content is preserved and missing reasoning identity is rejected. Serialized comparison assertions exclude fixture paths, tool-result text, call IDs, and reasoning text.
- Attribution boundary: raw traffic from the prior live run was deleted as required, so this offline-equivalent failure cannot retroactively prove the exact historical SSE payload. A separately authorized live rerun is still required to close the Write/Edit blocker.
- Readiness remains `LiveSmokeStatus::Failed` with blocker `ComparisonFailures`; do not run `enabled` or mark the live gate passed from offline evidence alone.

## 2026-07-30 credential-handoff live rerun

- Credential continuity: stopped the installed CC Switch before testing, copied its freshly authorized OAuth store without inspecting values, and used a fresh isolated home/database/provider. The first no-tool request returned HTTP 200 with `request_match=true`, `unexplained=0`, `comparison_failures=0`, and `bounded=true`. The isolated manager rotated its refresh token; the updated store was atomically handed back immediately and again during final cleanup. The installed application was restarted successfully, so no additional login is required.
- Glob/Grep passed: Claude Code executed exactly one `Glob` and one `Grep`, produced two unique tool-use IDs and two matching results, used no other tool, exited 0, and emitted no result error. All three flow requests returned HTTP 200. Every shadow stream report was bounded with zero unexplained differences and zero comparison failures.
- Blocking Write/Edit result: Claude Code emitted exactly one `Write` and one `Edit`, with two unique IDs and matching tool-result closures, no other tool, exit 0, and no top-level result error. The new disposable file was created, but the independent fixture check found the original Edit marker still present and the replacement absent. Across the rerun, all seven upstream requests returned HTTP 200; six stream reports were bounded and failure-free, while the final Write/Edit report recorded `stream_differences=2`, `unexplained=0`, `comparison_failures=1`, and `bounded=false`.
- Not run after the blocker: Bash/test, the dedicated parallel-tools flow, optional MCP/Task, and `enabled` mode. No visible tool was retried.
- Rollback and cleanup: stopped only the isolated debug application, changed its provider to explicit `legacy`, disabled its proxy/takeover state, performed the final OAuth handback, deleted the exact temporary directory and obsolete recovery backup, and restarted the original installed CC Switch. Raw prompts, traffic, tool arguments/results, logs, and temporary credentials were not retained.
- Readiness: `LiveSmokeStatus::Failed` with blocker `ComparisonFailures`. Diagnose and fix the Write/Edit shadow observation offline before another live rerun.

## 2026-07-30 tool-fragment-fix verification attempt

- Environment: rebuilt the current 3.19.0 `internal` debug binary, then used a new isolated temporary home, fresh 3.19 database, copied CC Switch OAuth store without inspecting values, disposable fixtures, loopback-only random port, explicit `shadow`, one provider, no failover, and scoped Claude tool permissions.
- Blocking result: the restricted Glob/Grep invocation never exposed a tool or reached strict stream comparison. Claude Code retried automatically until the 180-second outer timeout; the isolated proxy recorded 11 streaming requests, all HTTP 401. Safe application diagnostics identified an expired/invalid refresh token for the CC Switch-managed Codex OAuth account.
- Additional structural signal: every attempted request logged `mode=shadow ... compile_failed` before the authentication failure. No raw request was retained, so this run does not infer a cause. Recheck this signal after reauthentication; if it persists on an authenticated request, diagnose it offline before continuing the live matrix.
- Not run after the blocker: Write/Edit, Bash/test, parallel tools, result continuation, optional MCP/Task, and `enabled` mode. No visible tool was retried.
- Rollback and cleanup: stopped only the scoped application process, changed the isolated provider to explicit `legacy`, disabled isolated takeover/proxy state, verified the exact target remained beneath the OS temporary directory, and deleted the entire disposable directory including copied OAuth data, database, fixtures, and raw logs.
- Readiness: `LiveSmokeStatus::Blocked` with blocker `CodexOAuthRefreshExpired`. The prior offline fragment regression remains green but cannot substitute for the required live matrix.

## 2026-07-30 post-reauthentication rerun on 3.19.0

- Environment: rebuilt the 3.19.0 debug binary, then used a new isolated temporary home, isolated 3.19 database, copied OAuth store without inspecting values, disposable fixtures, loopback-only random port, explicit `shadow`, and persisted debug-level structural logging.
- Passed: ordinary text, reasoning with separate thinking/text structure, one legacy-served `Read`, the matching tool-result continuation, and immediate rollback to explicit `legacy`. Every upstream request in these flows returned HTTP 200; no comparison failure or incomplete observation was recorded.
- Blocking result: the Glob/Grep flow executed exactly one `Glob` and one `Grep`, with two unique tool-use IDs and matching results, but its stream comparison recorded one unexplained difference while remaining bounded. Request comparison remained explained and strict observation did not fail. The retained safe evidence narrows the blocker to a stream event-shape or terminal-state mismatch; raw traffic was deleted, so this run does not speculate further.
- Offline root cause and fix: a production-converter replay reproduced the blocker as `ResponseEventMismatch`. The legacy converter forwarded two non-`Read` `input_json_delta` fragments while the strict observer emitted one validated logical tool-argument delta. Shadow comparison now normalizes only consecutive tool-argument deltas at the same content-block index. The regression passes with unequal raw tool-event counts, zero unexplained differences, and zero comparison failures.
- Preserved fail-closed evidence: raw stream event counts and structural hashes remain unchanged in the report. Text, thinking, signature, content-block index, start/stop lifecycle, ordering, error, and terminal differences are not collapsed; safe debug diagnostics expose only ordered, deduplicated reason-code enums.
- Not run after the blocker: Write/Edit, Bash/test, the dedicated parallel-tools flow, optional MCP/Task, and `enabled` mode. The visible tool flow was not retried.
- Rollback and cleanup: the next no-tool Claude Code request completed with HTTP 200 and no new shadow diagnostic. The isolated provider remained explicit `legacy`; proxy/application/launcher processes stopped; the exact temporary directory, copied OAuth store, database, fixtures, evidence staging directories, and raw logs were deleted.
- 3.19 setup note: `proxy_config.live_takeover_active` no longer exists. The isolated setup used the current schema and did not recreate the earlier literal `%SystemDrive%` cache artifact.
- Readiness: `LiveSmokeStatus::Failed` with blocker `UnexplainedDifferences`. Do not opt into `enabled` until an offline regression explains and fixes the Glob/Grep stream mismatch and a separately authorized live rerun completes the matrix.

## 2026-07-30 authorized rerun

- Environment: rebuilt the debug binary from the fixed source, then used an isolated temporary home, isolated CC Switch database, copied OAuth store without inspecting values, disposable tool directory, loopback-only random port, and explicit `shadow` mode.
- Blocking result: the first no-tool Claude Code text flow never entered bridge comparison. Claude Code emitted one initialization event followed by nine API retries; the isolated proxy recorded HTTP 401 with safe reason code `CodexOAuthRefreshExpired`. No tool became visible and no `enabled` request was sent.
- Not run after the blocker: reasoning, Read, continuation, Glob/Grep, Write/Edit, Bash/test, parallel tools, optional MCP/Task, and `enabled` mode.
- Rollback and cleanup: changed the isolated provider back to explicit `legacy`, disabled isolated takeover, stopped only the scoped debug application and launcher, then deleted the exact disposable directory containing the copied OAuth store, database, fixtures, and logs.
- Isolation note: two earlier launcher attempts started the debug executable without the intended environment because Windows PowerShell rejected the process environment dictionary. Both were stopped before any Claude/model request; no intentional real-user configuration mutation was performed. The successful launcher used an independently created hidden process with the verified isolation environment.
- Readiness: `LiveSmokeStatus::Blocked` with blocker `CodexOAuthRefreshExpired`. Reauthenticate Codex OAuth before requesting another live rerun; offline verification remains green but cannot substitute for the required live matrix.

## 2026-07-30 earlier result

- Environment: isolated temporary home, isolated CC Switch database, copied OAuth store without inspecting values, disposable tool directory, and a debug-only local proxy startup hook removed immediately after the run.
- Passed: direct Anthropic-compatible text, Claude Code streaming text, Claude Code reasoning, legacy-served `Read`, tool-result continuation, and immediate rollback to explicit `legacy`.
- Shadow evidence for text/reasoning: request matched structurally, zero unexplained differences, zero comparison failures, bounded stream observation, HTTP 200, and Claude Code exit 0.
- Blocking result: the first `Read` tool-call stream was served successfully by legacy, but shadow recorded one comparison failure and an unbounded/incomplete observation. The continuation turn was bounded and failure-free. No visible tool was retried. Post-run offline diagnosis first fixed repeated failure accounting, then reproduced the initial trigger with the production legacy request/SSE path: the served request used the legacy `Read` identity while the isolated bridge registry expected its `read_file` projection. Shadow now reprojects only its local observation copy before strict decoding. The regression reports zero comparison failures, zero unexplained differences, matching structural hashes, and a bounded terminal stream.
- Not run after the blocker: Glob/Grep, Write/Edit, Bash/test, parallel tools, optional MCP/Task, and `enabled` mode.
- Rollback: the next Claude Code text request completed with no new shadow diagnostic.
- Cleanup: application/proxy/dev processes stopped; the exact disposable directory, copied OAuth store, isolated database, fixtures, and logs were deleted. Real user configuration was unchanged.
- Readiness at the time: `LiveSmokeStatus::Failed` with blocker `ComparisonFailures`. That offline root cause was subsequently fixed; the newer authorized rerun above is now blocked earlier by expired Codex OAuth credentials.

## Safety boundary

- Use only a newly created disposable directory under the operating-system temporary directory. Record its resolved absolute path before starting and verify that every file operation stays beneath it.
- Do not open or modify a real repository. Do not run destructive shell commands, installers, network commands, credential commands, or commands outside the disposable directory.
- CC Switch only bridges protocol traffic; it must never execute a tool itself. Tool execution remains Claude Code's responsibility and requires its normal approval boundary.
- Do not print, copy, log, or inspect OAuth tokens, cookies, authorization headers, API keys, prompt text, reasoning text, tool arguments, or tool results. Record only mode, flow name, pass/fail, counts, reason codes, hashes, terminal state, and rollback result.
- Start in `shadow`. `legacy` must remain the served path. Use `enabled` only after shadow flows have no unexplained difference or comparison/forensic failure.

## Expected impact

- Shadow uses one normal Codex OAuth request per Claude Code turn; it does not duplicate the upstream request. Each flow still consumes normal network bandwidth and Codex quota/tokens.
- The sequence below can make multiple model turns because tool results and continuations are separate turns. Stop immediately on unexpected cost, authentication, rate-limit, visibility, or filesystem behavior.
- The optional MCP and child-Task flows may add turns and tool processes. Skip them unless a known-safe local fixture is already available.

## Setup

1. Confirm all offline verification is green or that every unrelated pre-existing failure is recorded.
2. Confirm a local Codex OAuth login and `claude` command exist by presence only; never read credential contents.
3. Create a unique disposable directory, resolve its absolute path, and verify it is beneath the OS temporary directory.
4. Add a scoped Claude provider with built-in `codex_oauth`, `openai_responses`, and explicit `bridgeMode: shadow`.
5. Put two harmless text fixtures in the disposable directory: one searchable source file and one file reserved for Write/Edit. Do not use secrets or production content.
6. Start the local proxy/application using the repository's documented development command, then point Claude Code at the disposable directory and scoped provider.

## Shadow flows

Run one flow at a time and record only the safe result fields described above.

1. Ordinary text: request a short answer with no tool use; verify one response and a valid terminal event.
2. Reasoning: request a small planning problem; verify reasoning/text structure is accepted without recording reasoning content.
3. Read: ask Claude Code to read the harmless fixture; approve only the exact file beneath the disposable directory.
4. Glob and Grep: find the fixture and a known marker within it.
5. Write and Edit: create one new harmless file, then replace one known marker. Verify paths remain inside the disposable directory.
6. Bash/test: run a harmless local command that reads or validates the fixture, such as the repository-independent test command prepared in the disposable directory. No network or destructive flags.
7. Parallel tools: request two independent reads/searches and verify both call identities and results close exactly once.
8. Result continuation: after a visible tool result, request a textual continuation and verify no automatic retry repeats the tool.
9. Optional MCP/dynamic tool: invoke only a preconfigured, local, read-only fixture tool.
10. Optional child Task: invoke one bounded child task that reads only the disposable fixtures.

For each flow verify: one upstream request for that turn, legacy output served, expected terminal state, isolated shadow ledger, no unexplained differences, no comparison/forensic failure, and no plaintext in structured diagnostics.

## Enabled opt-in and rollback

1. Only after all required shadow flows pass, explicitly select `enabled` for the same provider.
2. Repeat ordinary text, Read, one safe Write/Edit, one safe Bash/test, parallel tools, and result continuation.
3. On any failure, stop. Do not retry a turn after a tool may have become visible.
4. Set the provider immediately back to explicit `legacy`.
5. Send one ordinary text turn and verify the next request uses the legacy path without restarting or migrating data.

## Readiness record

Mark live smoke `passed` only when every required flow above passes, rollback succeeds, visible-tool retry remains safe, fixture coverage is complete, and there are zero unexplained differences, comparison failures, forensic suppressions, or forensic failures. Otherwise record `failed` or `blocked` with safe reason codes. Offline success alone must leave the state `not_run`.

## Cleanup

1. Stop Claude Code and the local proxy/application.
2. Resolve and re-verify the disposable directory is beneath the OS temporary directory.
3. Remove only that exact disposable directory using a single-shell, literal-path operation.
4. Keep the provider at explicit `legacy` unless a later, separately approved rollout decision says otherwise.
5. Do not retain raw traffic or prompt/tool content. Retain only the safe structured readiness record.
