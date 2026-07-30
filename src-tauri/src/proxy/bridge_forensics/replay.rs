use std::collections::BTreeSet;
use std::fs;
use std::path::{Component, Path};

use bytes::Bytes;
use futures::{stream, StreamExt};
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use super::{
    EvidenceArtifact, EvidenceArtifactKind, EvidenceErrorKind, EvidenceManifest,
    FORENSIC_FORMAT_VERSION,
};
use crate::app_config::AppType;
use crate::error::AppError;
use crate::provider::{Provider, ProviderMeta};
use crate::proxy::claude_codex_bridge::{
    shadow::{ShadowComparisonReport, ShadowComparisonSession},
    streaming::{
        claude_stream_event_kind, decode_codex_response_event, StreamDecision, StreamTerminalState,
    },
    BridgeError, ClaudeCodexBridge, ConversationLedger, PreparedCodexTurn, ToolCallState,
};
use crate::proxy::json_canonical::canonicalize_value;
use crate::proxy::providers::streaming_responses::create_anthropic_sse_stream_from_responses_with_prepared_turn;
use crate::proxy::sse::{strip_sse_field, take_sse_block};

#[derive(Debug, Serialize)]
pub struct ReplayReport {
    pub mode: ReplayMode,
    pub codex_request_matches: bool,
    pub claude_response_matches: bool,
    pub tool_registry_matches: bool,
    pub capability_report_matches: bool,
    pub transform_decisions_match: bool,
    pub structural_differences: Vec<StructuralDifference>,
    pub network_requests: u32,
}

#[derive(Clone, Copy)]
struct Stage2EvidenceMatches {
    tool_registry: bool,
    capability_report: bool,
    transform_decisions: bool,
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

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct ConversationLedgerReplayReport {
    pub final_state: ToolCallState,
    pub network_requests: u32,
}

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StrictStreamReplayFixture {
    TextOnly,
    ReasoningAndText,
    SingleTool,
    ParallelTools,
    ToolArgumentsChunks,
    ToolLifecycle,
    Incomplete,
    InvalidSequence,
    UnknownTool,
    ConflictingDuplicate,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct StreamingEventReplayReport {
    pub fixture: StrictStreamReplayFixture,
    pub typed_event_decisions: Vec<StreamDecision>,
    pub claude_sse_shape: Vec<String>,
    pub ledger_transitions: Vec<String>,
    pub terminal_state: String,
    pub error_kind: Option<EvidenceErrorKind>,
    pub network_requests: u32,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
#[allow(dead_code)]
pub struct ShadowComparisonReplayReport {
    pub comparison: ShadowComparisonReport,
    pub network_requests: u32,
}

#[allow(dead_code)]
pub fn replay_shadow_comparison(request: Value) -> Result<ShadowComparisonReplayReport, AppError> {
    let provider = replay_provider();
    let bridge = ClaudeCodexBridge::with_ledger(ConversationLedger::default());
    let prepared = bridge
        .prepare_turn_with_session_identity(
            &AppType::Claude,
            request.clone(),
            &provider,
            "shadow-offline-replay-session",
            None,
        )
        .map_err(|error| AppError::Message(format!("shadow replay preparation failed: {error}")))?;
    let legacy = crate::proxy::providers::transform_claude_request_for_api_format(
        request,
        &provider,
        "openai_responses",
        None,
        None,
    )
    .map_err(|error| AppError::Message(format!("shadow legacy replay failed: {error}")))?;
    let comparison = ShadowComparisonSession::compare_request(prepared, &legacy).report();
    Ok(ShadowComparisonReplayReport {
        comparison,
        network_requests: 0,
    })
}

pub fn replay_conversation_lifecycle(
    request: Value,
    response: Value,
    followup_request: Value,
) -> Result<ConversationLedgerReplayReport, AppError> {
    let bridge = ClaudeCodexBridge::with_ledger(ConversationLedger::default());
    let provider = replay_provider();
    let prepared = bridge
        .prepare_turn_with_session_identity(
            &AppType::Claude,
            request,
            &provider,
            "offline-replay-session",
            None,
        )
        .map_err(|error| AppError::Message(format!("request replay failed: {error}")))?;
    let binding = prepared.ledger_binding().clone();
    prepared
        .consume_response(response, None, None)
        .map_err(|error| AppError::Message(format!("response replay failed: {error}")))?;
    bridge
        .prepare_turn_with_session_identity(
            &AppType::Claude,
            followup_request,
            &provider,
            "offline-replay-session",
            None,
        )
        .map_err(|error| AppError::Message(format!("tool_result replay failed: {error}")))?;
    let final_state = bridge
        .ledger()
        .call_state(&binding, "call-1")
        .ok_or_else(|| {
            AppError::Message("replayed tool call did not retain a terminal state".to_string())
        })?;
    Ok(ConversationLedgerReplayReport {
        final_state,
        network_requests: 0,
    })
}

pub fn replay_strict_stream_fixture(
    fixture: StrictStreamReplayFixture,
) -> Result<StreamingEventReplayReport, AppError> {
    let bridge = ClaudeCodexBridge::with_ledger(ConversationLedger::default());
    let provider = replay_provider();
    let prepared = bridge
        .prepare_turn_with_session_identity(
            &AppType::Claude,
            strict_replay_request(),
            &provider,
            "strict-offline-replay-session",
            None,
        )
        .map_err(|error| AppError::Message(format!("strict replay preparation failed: {error}")))?;
    let binding = prepared.ledger_binding().clone();
    let lookup = prepared
        .tool_registry
        .codex_name_for_claude("lookup")
        .map_err(|error| AppError::Message(error.to_string()))?
        .to_string();
    let fetch = prepared
        .tool_registry
        .codex_name_for_claude("fetch")
        .map_err(|error| AppError::Message(error.to_string()))?
        .to_string();
    let mut machine = prepared.start_stream();
    let mut claude_sse_shape = Vec::new();
    let mut ledger_transitions = Vec::new();
    let mut error_kind = None;

    'events: for (event_name, payload) in strict_fixture_events(fixture, &lookup, &fetch) {
        let decoded = match decode_codex_response_event(Some(event_name), payload) {
            Ok(events) => events,
            Err(error) => {
                error_kind = Some(bridge_error_kind(&error));
                break;
            }
        };
        for event in decoded {
            let outputs = match machine.apply(event) {
                Ok(outputs) => outputs,
                Err(error) => {
                    error_kind = Some(bridge_error_kind(&error));
                    break 'events;
                }
            };
            for output in outputs {
                claude_sse_shape.push(claude_stream_event_kind(&output).to_string());
                if let Err(error) = machine.acknowledge_emitted(&output) {
                    error_kind = Some(bridge_error_kind(&error));
                    break 'events;
                }
                record_call_state(&bridge, &binding, "call-1", &mut ledger_transitions);
                record_call_state(&bridge, &binding, "call-2", &mut ledger_transitions);
            }
        }
    }

    if error_kind.is_none() {
        if let Err(error) = machine.finish() {
            error_kind = Some(bridge_error_kind(&error));
        }
    }
    let typed_event_decisions = machine.decisions().to_vec();
    let terminal_state = stream_state_name(machine.terminal_state());

    if fixture == StrictStreamReplayFixture::ToolLifecycle && error_kind.is_none() {
        bridge
            .prepare_turn_with_session_identity(
                &AppType::Claude,
                strict_replay_followup_request(),
                &provider,
                "strict-offline-replay-session",
                None,
            )
            .map_err(|error| {
                AppError::Message(format!("strict replay followup failed: {error}"))
            })?;
        record_call_state(&bridge, &binding, "call-1", &mut ledger_transitions);
    }

    Ok(StreamingEventReplayReport {
        fixture,
        typed_event_decisions,
        claude_sse_shape,
        ledger_transitions,
        terminal_state,
        error_kind,
        network_requests: 0,
    })
}

fn strict_replay_request() -> Value {
    json!({
        "model":"gpt-5.6",
        "max_tokens":128,
        "messages":[{"role":"user","content":"offline fixture"}],
        "tools":[
            {"name":"lookup","input_schema":strict_tool_schema()},
            {"name":"fetch","input_schema":strict_tool_schema()}
        ]
    })
}

fn strict_replay_followup_request() -> Value {
    json!({
        "model":"gpt-5.6",
        "max_tokens":128,
        "messages":[
            {"role":"user","content":"offline fixture"},
            {"role":"assistant","content":[{
                "type":"tool_use","id":"call-1","name":"lookup","input":{"q":"one"}
            }]},
            {"role":"user","content":[{
                "type":"tool_result","tool_use_id":"call-1","content":"fixture result"
            }]}
        ],
        "tools":[
            {"name":"lookup","input_schema":strict_tool_schema()},
            {"name":"fetch","input_schema":strict_tool_schema()}
        ]
    })
}

fn strict_tool_schema() -> Value {
    json!({
        "type":"object",
        "properties":{"q":{"type":"string"}},
        "required":["q"],
        "additionalProperties":false
    })
}

fn strict_fixture_events(
    fixture: StrictStreamReplayFixture,
    lookup: &str,
    fetch: &str,
) -> Vec<(&'static str, Value)> {
    let created = || {
        (
            "response.created",
            json!({"type":"response.created","response":{"id":"resp-replay","model":"gpt-5.6","usage":{"input_tokens":2,"output_tokens":0}}}),
        )
    };
    let completed = || {
        (
            "response.completed",
            json!({"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":2,"output_tokens":2}}}),
        )
    };
    let text = || {
        vec![
            (
                "response.content_part.added",
                json!({"type":"response.content_part.added","item_id":"msg-1","part":{"type":"output_text","text":""}}),
            ),
            (
                "response.output_text.delta",
                json!({"type":"response.output_text.delta","item_id":"msg-1","sequence_number":20,"delta":"hello"}),
            ),
            (
                "response.output_text.done",
                json!({"type":"response.output_text.done","item_id":"msg-1","text":"hello"}),
            ),
        ]
    };
    let tool_start = |item: &str, call: &str, name: &str| {
        (
            "response.output_item.added",
            json!({"type":"response.output_item.added","item":{"id":item,"type":"function_call","call_id":call,"name":name}}),
        )
    };

    match fixture {
        StrictStreamReplayFixture::TextOnly => {
            let mut events = vec![created()];
            events.extend(text());
            events.push(completed());
            events
        }
        StrictStreamReplayFixture::ReasoningAndText => {
            let mut events = vec![
                created(),
                (
                    "response.output_item.added",
                    json!({"type":"response.output_item.added","item":{"id":"rs-1","type":"reasoning"}}),
                ),
                (
                    "response.reasoning_summary_text.delta",
                    json!({"type":"response.reasoning_summary_text.delta","item_id":"rs-1","sequence_number":3,"delta":"think"}),
                ),
                (
                    "response.output_item.done",
                    json!({"type":"response.output_item.done","item":{"id":"rs-1","type":"reasoning","encrypted_content":"opaque"}}),
                ),
            ];
            events.extend(text());
            events.push(completed());
            events
        }
        StrictStreamReplayFixture::SingleTool | StrictStreamReplayFixture::ToolLifecycle => vec![
            created(),
            tool_start("fc-1", "call-1", lookup),
            (
                "response.function_call_arguments.done",
                json!({"type":"response.function_call_arguments.done","item_id":"fc-1","call_id":"call-1","arguments":"{\"q\":\"one\"}"}),
            ),
            completed(),
        ],
        StrictStreamReplayFixture::ParallelTools => vec![
            created(),
            tool_start("fc-1", "call-1", lookup),
            tool_start("fc-2", "call-2", fetch),
            (
                "response.function_call_arguments.delta",
                json!({"type":"response.function_call_arguments.delta","item_id":"fc-2","sequence_number":7,"delta":"{\"q\":\"two\"}"}),
            ),
            (
                "response.function_call_arguments.delta",
                json!({"type":"response.function_call_arguments.delta","item_id":"fc-1","sequence_number":8,"delta":"{\"q\":\"one\"}"}),
            ),
            (
                "response.function_call_arguments.done",
                json!({"type":"response.function_call_arguments.done","item_id":"fc-1"}),
            ),
            (
                "response.function_call_arguments.done",
                json!({"type":"response.function_call_arguments.done","item_id":"fc-2"}),
            ),
            completed(),
        ],
        StrictStreamReplayFixture::ToolArgumentsChunks => vec![
            created(),
            tool_start("fc-1", "call-1", lookup),
            (
                "response.function_call_arguments.delta",
                json!({"type":"response.function_call_arguments.delta","item_id":"fc-1","sequence_number":4,"delta":"{\"q\":\""}),
            ),
            (
                "response.function_call_arguments.delta",
                json!({"type":"response.function_call_arguments.delta","item_id":"fc-1","sequence_number":5,"delta":"chunked\"}"}),
            ),
            (
                "response.function_call_arguments.done",
                json!({"type":"response.function_call_arguments.done","item_id":"fc-1"}),
            ),
            completed(),
        ],
        StrictStreamReplayFixture::Incomplete => vec![created()],
        StrictStreamReplayFixture::InvalidSequence => vec![
            created(),
            (
                "response.output_text.delta",
                json!({"type":"response.output_text.delta","item_id":"msg-1","delta":"orphan"}),
            ),
        ],
        StrictStreamReplayFixture::UnknownTool => {
            vec![created(), tool_start("fc-1", "call-1", "unknown-tool")]
        }
        StrictStreamReplayFixture::ConflictingDuplicate => vec![
            created(),
            (
                "response.content_part.added",
                json!({"type":"response.content_part.added","item_id":"msg-1","part":{"type":"output_text"}}),
            ),
            (
                "response.output_text.delta",
                json!({"type":"response.output_text.delta","item_id":"msg-1","sequence_number":9,"delta":"one"}),
            ),
            (
                "response.output_text.delta",
                json!({"type":"response.output_text.delta","item_id":"msg-1","sequence_number":9,"delta":"two"}),
            ),
        ],
    }
}

fn bridge_error_kind(error: &BridgeError) -> EvidenceErrorKind {
    match error {
        BridgeError::ToolRegistryViolation { .. } => EvidenceErrorKind::ToolRegistryViolation,
        BridgeError::ConversationStateConflict { .. } => {
            EvidenceErrorKind::ConversationStateConflict
        }
        BridgeError::IncompleteStream { .. } => EvidenceErrorKind::IncompleteStream,
        _ => EvidenceErrorKind::InvalidUpstreamEvent,
    }
}

fn record_call_state(
    bridge: &ClaudeCodexBridge,
    binding: &crate::proxy::claude_codex_bridge::TurnBinding,
    call_id: &str,
    transitions: &mut Vec<String>,
) {
    let Some(state) = bridge.ledger().call_state(binding, call_id) else {
        return;
    };
    let state = serde_json::to_value(state)
        .ok()
        .and_then(|value| value.as_str().map(str::to_string))
        .unwrap_or_else(|| format!("{state:?}").to_ascii_lowercase());
    if transitions.last() != Some(&state) {
        transitions.push(state);
    }
}

fn stream_state_name(state: StreamTerminalState) -> String {
    serde_json::to_value(state)
        .ok()
        .and_then(|value| value.as_str().map(str::to_string))
        .unwrap_or_else(|| "unknown".to_string())
}

pub fn replay_bundle(path: &Path) -> Result<ReplayReport, AppError> {
    let manifest_path = path.join("manifest.json");
    let manifest_bytes =
        fs::read(&manifest_path).map_err(|error| AppError::io(&manifest_path, error))?;
    let manifest: EvidenceManifest = serde_json::from_slice(&manifest_bytes)
        .map_err(|error| AppError::json(&manifest_path, error))?;
    if manifest.format_version != FORENSIC_FORMAT_VERSION {
        return Err(AppError::InvalidInput(format!(
            "unsupported bridge evidence format version: {}",
            manifest.format_version
        )));
    }

    let claude_request = load_json_artifact(path, &manifest, EvidenceArtifactKind::ClaudeRequest)?;
    let expected_codex_request =
        load_json_artifact(path, &manifest, EvidenceArtifactKind::CodexRequest)?;
    let prepared = ClaudeCodexBridge::with_ledger(ConversationLedger::default())
        .prepare_turn_with_session_identity(
            &AppType::Claude,
            claude_request,
            &replay_provider(),
            &manifest.session_id_hash,
            None,
        )
        .map_err(|error| AppError::Message(format!("request replay failed: {error}")))?;
    let actual_codex_request = prepared.request.clone();

    let mut differences = Vec::new();
    let evidence_matches = compare_stage2_evidence(path, &manifest, &prepared, &mut differences)?;
    compare_values(
        "$/codex_request",
        &canonicalize_value(expected_codex_request.clone()),
        &canonicalize_value(actual_codex_request.clone()),
        &mut differences,
    );
    let codex_request_matches =
        canonicalize_value(expected_codex_request) == canonicalize_value(actual_codex_request);

    let codex_response_artifact = artifact_for(
        &manifest,
        EvidenceArtifactKind::CodexResponse,
        &["json", "ndjson"],
    )?;
    if codex_response_artifact.file_name.ends_with(".ndjson") {
        replay_streaming(
            path,
            &manifest,
            codex_response_artifact,
            codex_request_matches,
            differences,
            prepared,
            evidence_matches,
        )
    } else {
        replay_non_streaming(
            path,
            &manifest,
            codex_response_artifact,
            codex_request_matches,
            differences,
            prepared,
            evidence_matches,
        )
    }
}

fn replay_non_streaming(
    root: &Path,
    manifest: &EvidenceManifest,
    response_artifact: &EvidenceArtifact,
    codex_request_matches: bool,
    mut differences: Vec<StructuralDifference>,
    prepared: PreparedCodexTurn,
    evidence_matches: Stage2EvidenceMatches,
) -> Result<ReplayReport, AppError> {
    let codex_response = parse_json_artifact(root, response_artifact)?;
    let expected_claude_response =
        load_json_artifact(root, manifest, EvidenceArtifactKind::ClaudeResponse)?;
    let actual_claude_response = prepared
        .consume_response(codex_response, None, None)
        .map_err(|error| AppError::Message(format!("response replay failed: {error}")))?;
    compare_values(
        "$/claude_response",
        &canonicalize_value(expected_claude_response.clone()),
        &canonicalize_value(actual_claude_response.clone()),
        &mut differences,
    );
    let claude_response_matches =
        canonicalize_value(expected_claude_response) == canonicalize_value(actual_claude_response);

    Ok(ReplayReport {
        mode: ReplayMode::NonStreaming,
        codex_request_matches,
        claude_response_matches,
        tool_registry_matches: evidence_matches.tool_registry,
        capability_report_matches: evidence_matches.capability_report,
        transform_decisions_match: evidence_matches.transform_decisions,
        structural_differences: differences,
        network_requests: 0,
    })
}

fn replay_streaming(
    root: &Path,
    manifest: &EvidenceManifest,
    response_artifact: &EvidenceArtifact,
    codex_request_matches: bool,
    mut differences: Vec<StructuralDifference>,
    prepared: PreparedCodexTurn,
    evidence_matches: Stage2EvidenceMatches,
) -> Result<ReplayReport, AppError> {
    let upstream_events = parse_ndjson_artifact(root, response_artifact)?;
    let expected_artifact =
        artifact_for(manifest, EvidenceArtifactKind::ClaudeResponse, &["ndjson"])?;
    let expected_events = parse_ndjson_artifact(root, expected_artifact)?;
    let mut upstream_sse = String::new();
    for event in upstream_events {
        let event_name = event
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("message");
        upstream_sse.push_str("event: ");
        upstream_sse.push_str(event_name);
        upstream_sse.push_str("\ndata: ");
        upstream_sse.push_str(
            &serde_json::to_string(&event).map_err(|source| AppError::JsonSerialize { source })?,
        );
        upstream_sse.push_str("\n\n");
    }
    let converted = futures::executor::block_on(async {
        create_anthropic_sse_stream_from_responses_with_prepared_turn(
            stream::iter(vec![Ok::<_, std::io::Error>(Bytes::from(upstream_sse))]),
            None,
            None,
            prepared,
        )
        .collect::<Vec<_>>()
        .await
    });
    let mut converted_bytes = Vec::new();
    for chunk in converted {
        converted_bytes.extend_from_slice(
            &chunk.map_err(|error| AppError::Message(format!("stream replay failed: {error}")))?,
        );
    }
    let actual_events = parse_sse_events(&converted_bytes)?;
    let expected = Value::Array(expected_events);
    let actual = Value::Array(actual_events);
    compare_values("$/claude_events", &expected, &actual, &mut differences);

    Ok(ReplayReport {
        mode: ReplayMode::Streaming,
        codex_request_matches,
        claude_response_matches: expected == actual,
        tool_registry_matches: evidence_matches.tool_registry,
        capability_report_matches: evidence_matches.capability_report,
        transform_decisions_match: evidence_matches.transform_decisions,
        structural_differences: differences,
        network_requests: 0,
    })
}

fn compare_stage2_evidence(
    root: &Path,
    manifest: &EvidenceManifest,
    prepared: &PreparedCodexTurn,
    differences: &mut Vec<StructuralDifference>,
) -> Result<Stage2EvidenceMatches, AppError> {
    let actual_registry = serde_json::json!({
        "bindings": prepared.tool_registry.bindings(),
        "identity_fingerprint": prepared.tool_registry.identity_fingerprint(),
        "schema_fingerprint": prepared.tool_registry.schema_fingerprint()
    });
    let tool_registry = compare_optional_json_artifact(
        root,
        manifest,
        EvidenceArtifactKind::ToolRegistry,
        "$/tool_registry",
        &actual_registry,
        differences,
    )?;

    let actual_report = serde_json::to_value(&prepared.negotiation_report)
        .map_err(|source| AppError::JsonSerialize { source })?;
    let capability_report = compare_optional_json_artifact(
        root,
        manifest,
        EvidenceArtifactKind::CapabilityReport,
        "$/capability_report",
        &actual_report,
        differences,
    )?;

    let actual_decisions = Value::Array(
        prepared
            .tool_registry
            .transform_decisions(&prepared.negotiation_report.schema_losses)
            .into_iter()
            .map(serde_json::to_value)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|source| AppError::JsonSerialize { source })?,
    );
    let transform_decisions = match optional_artifact_for(
        manifest,
        EvidenceArtifactKind::TransformDecisions,
        &["ndjson"],
    )? {
        Some(artifact) => {
            let expected = Value::Array(parse_ndjson_artifact(root, artifact)?);
            compare_values(
                "$/transform_decisions",
                &expected,
                &actual_decisions,
                differences,
            );
            expected == actual_decisions
        }
        None => true,
    };

    Ok(Stage2EvidenceMatches {
        tool_registry,
        capability_report,
        transform_decisions,
    })
}

fn compare_optional_json_artifact(
    root: &Path,
    manifest: &EvidenceManifest,
    kind: EvidenceArtifactKind,
    path: &str,
    actual: &Value,
    differences: &mut Vec<StructuralDifference>,
) -> Result<bool, AppError> {
    let Some(artifact) = optional_artifact_for(manifest, kind, &["json"])? else {
        return Ok(true);
    };
    let expected = parse_json_artifact(root, artifact)?;
    compare_values(path, &expected, actual, differences);
    Ok(expected == *actual)
}

fn replay_provider() -> Provider {
    Provider {
        id: "bridge-replay".to_string(),
        name: "Bridge Replay".to_string(),
        settings_config: serde_json::json!({}),
        website_url: None,
        category: Some("claude".to_string()),
        created_at: None,
        sort_index: None,
        notes: None,
        meta: Some(ProviderMeta {
            provider_type: Some("codex_oauth".to_string()),
            api_format: Some("openai_responses".to_string()),
            ..ProviderMeta::default()
        }),
        icon: None,
        icon_color: None,
        in_failover_queue: false,
    }
}

fn load_json_artifact(
    root: &Path,
    manifest: &EvidenceManifest,
    kind: EvidenceArtifactKind,
) -> Result<Value, AppError> {
    let artifact = artifact_for(manifest, kind, &["json"])?;
    parse_json_artifact(root, artifact)
}

fn parse_json_artifact(root: &Path, artifact: &EvidenceArtifact) -> Result<Value, AppError> {
    let bytes = load_artifact_bytes(root, artifact)?;
    let path = root.join(&artifact.file_name);
    serde_json::from_slice(&bytes).map_err(|error| AppError::json(path, error))
}

fn parse_ndjson_artifact(root: &Path, artifact: &EvidenceArtifact) -> Result<Vec<Value>, AppError> {
    let bytes = load_artifact_bytes(root, artifact)?;
    let path = root.join(&artifact.file_name);
    let text = std::str::from_utf8(&bytes).map_err(|error| {
        AppError::InvalidInput(format!("invalid UTF-8 evidence artifact: {error}"))
    })?;
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).map_err(|error| AppError::json(&path, error)))
        .collect()
}

fn load_artifact_bytes(root: &Path, artifact: &EvidenceArtifact) -> Result<Vec<u8>, AppError> {
    validate_artifact_name(&artifact.file_name)?;
    let path = root.join(&artifact.file_name);
    let bytes = fs::read(&path).map_err(|error| AppError::io(&path, error))?;
    let sha256 = format!("{:x}", Sha256::digest(&bytes));
    if bytes.len() as u64 != artifact.byte_len || sha256 != artifact.sha256 {
        return Err(AppError::InvalidInput(format!(
            "evidence artifact integrity check failed: {}",
            artifact.file_name
        )));
    }
    Ok(bytes)
}

fn artifact_for<'a>(
    manifest: &'a EvidenceManifest,
    kind: EvidenceArtifactKind,
    extensions: &[&str],
) -> Result<&'a EvidenceArtifact, AppError> {
    let matches: Vec<&EvidenceArtifact> = manifest
        .artifacts
        .iter()
        .filter(|artifact| {
            artifact.kind == kind
                && extensions
                    .iter()
                    .any(|extension| artifact.file_name.ends_with(&format!(".{extension}")))
        })
        .collect();
    match matches.as_slice() {
        [artifact] => Ok(*artifact),
        [] => Err(AppError::InvalidInput(format!(
            "missing replay artifact: {kind:?}"
        ))),
        _ => Err(AppError::InvalidInput(format!(
            "duplicate replay artifact: {kind:?}"
        ))),
    }
}

fn optional_artifact_for<'a>(
    manifest: &'a EvidenceManifest,
    kind: EvidenceArtifactKind,
    extensions: &[&str],
) -> Result<Option<&'a EvidenceArtifact>, AppError> {
    let matches: Vec<&EvidenceArtifact> = manifest
        .artifacts
        .iter()
        .filter(|artifact| artifact.kind == kind)
        .collect();
    match matches.as_slice() {
        [] => Ok(None),
        [artifact]
            if extensions
                .iter()
                .any(|extension| artifact.file_name.ends_with(&format!(".{extension}"))) =>
        {
            Ok(Some(*artifact))
        }
        [artifact] => Err(AppError::InvalidInput(format!(
            "invalid replay artifact extension for {kind:?}: {}",
            artifact.file_name
        ))),
        _ => Err(AppError::InvalidInput(format!(
            "duplicate replay artifact: {kind:?}"
        ))),
    }
}

fn validate_artifact_name(file_name: &str) -> Result<(), AppError> {
    let mut components = Path::new(file_name).components();
    if !matches!(components.next(), Some(Component::Normal(_))) || components.next().is_some() {
        return Err(AppError::InvalidInput(
            "invalid replay artifact file name".to_string(),
        ));
    }
    Ok(())
}

fn parse_sse_events(bytes: &[u8]) -> Result<Vec<Value>, AppError> {
    let mut buffer = std::str::from_utf8(bytes)
        .map_err(|error| AppError::InvalidInput(format!("invalid replay SSE UTF-8: {error}")))?
        .to_string();
    if !buffer.trim().is_empty() {
        buffer.push_str("\n\n");
    }
    let mut events = Vec::new();
    while let Some(block) = take_sse_block(&mut buffer) {
        let data = block
            .lines()
            .filter_map(|line| strip_sse_field(line, "data"))
            .collect::<Vec<_>>()
            .join("\n");
        if !data.is_empty() {
            events.push(
                serde_json::from_str(&data).map_err(|source| AppError::JsonSerialize { source })?,
            );
        }
    }
    Ok(events)
}

fn compare_values(
    path: &str,
    expected: &Value,
    actual: &Value,
    differences: &mut Vec<StructuralDifference>,
) {
    match (expected, actual) {
        (Value::Object(expected), Value::Object(actual)) => {
            let keys: BTreeSet<&String> = expected.keys().chain(actual.keys()).collect();
            for key in keys {
                let child_path = format!("{path}/{}", escape_json_pointer(key));
                match (expected.get(key), actual.get(key)) {
                    (Some(expected), Some(actual)) => {
                        compare_values(&child_path, expected, actual, differences)
                    }
                    (Some(expected), None) => differences.push(StructuralDifference {
                        path: child_path,
                        expected_type: value_type(expected).to_string(),
                        actual_type: "missing".to_string(),
                        reason: "missing_field".to_string(),
                    }),
                    (None, Some(actual)) => differences.push(StructuralDifference {
                        path: child_path,
                        expected_type: "missing".to_string(),
                        actual_type: value_type(actual).to_string(),
                        reason: "unexpected_field".to_string(),
                    }),
                    (None, None) => {}
                }
            }
        }
        (Value::Array(expected), Value::Array(actual)) => {
            let length = expected.len().max(actual.len());
            for index in 0..length {
                let child_path = format!("{path}/{index}");
                match (expected.get(index), actual.get(index)) {
                    (Some(expected), Some(actual)) => {
                        compare_values(&child_path, expected, actual, differences)
                    }
                    (Some(expected), None) => differences.push(StructuralDifference {
                        path: child_path,
                        expected_type: value_type(expected).to_string(),
                        actual_type: "missing".to_string(),
                        reason: "missing_item".to_string(),
                    }),
                    (None, Some(actual)) => differences.push(StructuralDifference {
                        path: child_path,
                        expected_type: "missing".to_string(),
                        actual_type: value_type(actual).to_string(),
                        reason: "unexpected_item".to_string(),
                    }),
                    (None, None) => {}
                }
            }
        }
        _ if expected != actual => differences.push(StructuralDifference {
            path: path.to_string(),
            expected_type: value_type(expected).to_string(),
            actual_type: value_type(actual).to_string(),
            reason: if value_type(expected) == value_type(actual) {
                "value_mismatch"
            } else {
                "type_mismatch"
            }
            .to_string(),
        }),
        _ => {}
    }
}

fn value_type(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn escape_json_pointer(value: &str) -> String {
    value.replace('~', "~0").replace('/', "~1")
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use serde_json::json;

    use super::*;

    #[test]
    fn shadow_replay_uses_production_comparison_without_network_or_plaintext() {
        let secret = "shadow-replay-secret Authorization cookie reasoning arguments";
        let report = replay_shadow_comparison(json!({
            "model":"gpt-test",
            "max_tokens":64,
            "messages":[{"role":"user","content":secret}],
            "tools":[
                {"name":"Read","input_schema":{"type":"object","properties":{}}},
                {"name":"Glob","input_schema":{"type":"object","properties":{}}},
                {"name":"Grep","input_schema":{"type":"object","properties":{}}},
                {"name":"Bash","input_schema":{"type":"object","properties":{}}},
                {"name":"Edit","input_schema":{"type":"object","properties":{}}},
                {"name":"Write","input_schema":{"type":"object","properties":{}}},
                {"name":"NotebookEdit","input_schema":{"type":"object","properties":{}}},
                {"name":"Task","input_schema":{"type":"object","properties":{}}},
                {"name":"mcp__fixture__lookup","input_schema":{"type":"object","properties":{}}}
            ]
        }))
        .unwrap();

        assert_eq!(report.network_requests, 0);
        assert_eq!(report.comparison.request.tool_count, 9);
        let encoded = serde_json::to_string(&report).unwrap();
        assert!(!encoded.contains(secret));
        assert!(!encoded.contains("Authorization"));
    }

    #[test]
    fn replays_non_stream_tool_call_without_network() {
        let fixture = fixture_path("non-stream-tool-call");

        let report = replay_bundle(&fixture).unwrap();

        assert_eq!(report.mode, ReplayMode::NonStreaming);
        assert!(report.codex_request_matches);
        assert!(report.claude_response_matches);
        assert!(report.structural_differences.is_empty());
        assert_eq!(report.network_requests, 0);
    }

    #[test]
    fn replays_conversation_tool_lifecycle_without_network() {
        use crate::proxy::claude_codex_bridge::ToolCallState;

        let report = replay_conversation_lifecycle(
            json!({
                "model": "gpt-test",
                "messages": [{"role": "user", "content": "read"}],
                "tools": [{
                    "name": "Read",
                    "input_schema": {
                        "type": "object",
                        "properties": {"file_path": {"type": "string"}},
                        "required": ["file_path"]
                    }
                }]
            }),
            json!({
                "id": "resp-1",
                "model": "gpt-test",
                "status": "completed",
                "output": [{
                    "type": "function_call",
                    "call_id": "call-1",
                    "name": "read_file",
                    "arguments": "{\"file_path\":\"src/main.rs\"}"
                }],
                "usage": {"input_tokens": 1, "output_tokens": 1}
            }),
            json!({
                "model": "gpt-test",
                "messages": [
                    {"role": "user", "content": "read"},
                    {"role": "assistant", "content": [{
                        "type": "tool_use",
                        "id": "call-1",
                        "name": "Read",
                        "input": {"file_path": "src/main.rs"}
                    }]},
                    {"role": "user", "content": [{
                        "type": "tool_result",
                        "tool_use_id": "call-1",
                        "content": "contents"
                    }]}
                ],
                "tools": [{
                    "name": "Read",
                    "input_schema": {
                        "type": "object",
                        "properties": {"file_path": {"type": "string"}},
                        "required": ["file_path"]
                    }
                }]
            }),
        )
        .unwrap();

        assert_eq!(report.final_state, ToolCallState::Completed);
        assert_eq!(report.network_requests, 0);
    }

    #[test]
    fn replays_stream_events_without_network() {
        use crate::proxy::bridge_forensics::{
            BridgeForensicStore, CaptureMetadata, EvidenceArtifactKind, EvidenceError,
            EvidenceErrorKind,
        };

        let temp = tempfile::tempdir().unwrap();
        let store = BridgeForensicStore::new(temp.path().to_path_buf());
        let claude_request = json!({
            "model": "gpt-test",
            "messages": [{"role": "user", "content": "hello"}],
            "tools": [{
                "name": "Read",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "file_path": {"type": "string"},
                        "api_key": {"type": "string"}
                    },
                    "required": ["file_path"],
                    "additionalProperties": false
                }
            }]
        });
        let prepared = ClaudeCodexBridge::builtin()
            .prepare_turn(
                &AppType::Claude,
                claude_request.clone(),
                &replay_provider(),
                None,
            )
            .unwrap();
        let mut capture = store
            .begin_turn(CaptureMetadata {
                provider_id: "fixture-provider".to_string(),
                model: "gpt-test".to_string(),
                session_id_hash: "fixture-session".to_string(),
            })
            .unwrap();
        capture
            .record_json(EvidenceArtifactKind::ClaudeRequest, &claude_request)
            .unwrap();
        capture
            .record_json(EvidenceArtifactKind::CodexRequest, &prepared.request)
            .unwrap();
        capture
            .record_json(
                EvidenceArtifactKind::ToolRegistry,
                &json!({
                    "bindings": prepared.tool_registry.bindings(),
                    "identity_fingerprint": prepared.tool_registry.identity_fingerprint(),
                    "schema_fingerprint": prepared.tool_registry.schema_fingerprint()
                }),
            )
            .unwrap();
        capture
            .record_json(
                EvidenceArtifactKind::CapabilityReport,
                &serde_json::to_value(&prepared.negotiation_report).unwrap(),
            )
            .unwrap();
        for decision in prepared
            .tool_registry
            .transform_decisions(&prepared.negotiation_report.schema_losses)
        {
            capture
                .append_ndjson(
                    EvidenceArtifactKind::TransformDecisions,
                    &serde_json::to_value(decision).unwrap(),
                )
                .unwrap();
        }
        capture
            .append_ndjson(
                EvidenceArtifactKind::CodexResponse,
                &json!({
                    "type": "response.created",
                    "response": {"id": "resp_1", "model": "gpt-test"}
                }),
            )
            .unwrap();
        capture
            .append_ndjson(
                EvidenceArtifactKind::CodexResponse,
                &json!({
                    "type": "response.completed",
                    "response": {
                        "status": "completed",
                        "usage": {"input_tokens": 1, "output_tokens": 0}
                    }
                }),
            )
            .unwrap();
        capture
            .append_ndjson(
                EvidenceArtifactKind::ClaudeResponse,
                &json!({
                    "type": "message_start",
                    "message": {
                        "id": "resp_1",
                        "type": "message",
                        "role": "assistant",
                        "model": "gpt-test",
                        "usage": {"input_tokens": 0, "output_tokens": 0}
                    }
                }),
            )
            .unwrap();
        capture
            .append_ndjson(
                EvidenceArtifactKind::ClaudeResponse,
                &json!({
                    "type": "message_delta",
                    "delta": {"stop_reason": "end_turn", "stop_sequence": null},
                    "usage": {"input_tokens": 1, "output_tokens": 0}
                }),
            )
            .unwrap();
        capture
            .append_ndjson(
                EvidenceArtifactKind::ClaudeResponse,
                &json!({"type": "message_stop"}),
            )
            .unwrap();
        let bundle = capture
            .commit_failure(EvidenceError {
                kind: EvidenceErrorKind::IncompleteStream,
                safe_summary: "fixture".to_string(),
                retryable: false,
                output_already_visible: false,
                streaming: None,
            })
            .unwrap();

        let report = replay_bundle(&bundle.path).unwrap();

        assert_eq!(report.mode, ReplayMode::Streaming);
        assert!(report.codex_request_matches);
        assert!(report.claude_response_matches);
        assert!(report.tool_registry_matches);
        assert!(report.capability_report_matches);
        assert!(report.transform_decisions_match);
        assert_eq!(report.network_requests, 0);
    }

    #[test]
    fn streaming_event_replay_covers_required_fixtures_without_network() {
        let success = [
            StrictStreamReplayFixture::TextOnly,
            StrictStreamReplayFixture::ReasoningAndText,
            StrictStreamReplayFixture::SingleTool,
            StrictStreamReplayFixture::ParallelTools,
            StrictStreamReplayFixture::ToolArgumentsChunks,
            StrictStreamReplayFixture::ToolLifecycle,
        ];
        for fixture in success {
            let report = replay_strict_stream_fixture(fixture).unwrap();
            assert_eq!(report.network_requests, 0, "{fixture:?}");
            assert!(report.error_kind.is_none(), "{fixture:?}: {report:?}");
            assert_eq!(report.terminal_state, "completed", "{fixture:?}");
            assert!(!report.typed_event_decisions.is_empty());
            assert!(report
                .claude_sse_shape
                .contains(&"message_start".to_string()));
            assert!(report
                .claude_sse_shape
                .contains(&"message_stop".to_string()));
            assert_eq!(report, replay_strict_stream_fixture(fixture).unwrap());
        }

        let lifecycle =
            replay_strict_stream_fixture(StrictStreamReplayFixture::ToolLifecycle).unwrap();
        assert!(lifecycle
            .ledger_transitions
            .iter()
            .any(|state| state == "returned_to_claude"));
        assert!(lifecycle
            .ledger_transitions
            .iter()
            .any(|state| state == "completed"));

        for fixture in [
            StrictStreamReplayFixture::Incomplete,
            StrictStreamReplayFixture::InvalidSequence,
            StrictStreamReplayFixture::UnknownTool,
            StrictStreamReplayFixture::ConflictingDuplicate,
        ] {
            let report = replay_strict_stream_fixture(fixture).unwrap();
            assert_eq!(report.network_requests, 0, "{fixture:?}");
            assert!(report.error_kind.is_some(), "{fixture:?}");
            assert_ne!(report.terminal_state, "completed", "{fixture:?}");
            assert_eq!(report, replay_strict_stream_fixture(fixture).unwrap());
        }
    }

    fn fixture_path(name: &str) -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/bridge-forensics")
            .join(name)
    }
}
