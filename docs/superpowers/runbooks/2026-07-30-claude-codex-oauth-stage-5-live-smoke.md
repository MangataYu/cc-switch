# Claude Code Codex OAuth Stage 5 Live Smoke Runbook

**Status:** Blocked on 2026-07-30 with blocker `CodexOAuthRefreshExpired`; rollout remains blocked. The tool-fragment fix is green offline but has not passed a live rerun. Reauthenticate the CC Switch-managed Codex OAuth account before requesting another live attempt.

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
