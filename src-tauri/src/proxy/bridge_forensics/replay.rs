use std::collections::BTreeSet;
use std::fs;
use std::path::{Component, Path};

use bytes::Bytes;
use futures::{stream, StreamExt};
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::{EvidenceArtifact, EvidenceArtifactKind, EvidenceManifest, FORENSIC_FORMAT_VERSION};
use crate::error::AppError;
use crate::proxy::json_canonical::canonicalize_value;
use crate::proxy::providers::streaming_responses::create_anthropic_sse_stream_from_responses;
use crate::proxy::providers::transform_responses::{
    anthropic_to_responses, responses_to_anthropic,
};
use crate::proxy::sse::{strip_sse_field, take_sse_block};

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
    let actual_codex_request = anthropic_to_responses(claude_request, None, true, false)
        .map_err(|error| AppError::Message(format!("request replay failed: {error}")))?;

    let mut differences = Vec::new();
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
        )
    } else {
        replay_non_streaming(
            path,
            &manifest,
            codex_response_artifact,
            codex_request_matches,
            differences,
        )
    }
}

fn replay_non_streaming(
    root: &Path,
    manifest: &EvidenceManifest,
    response_artifact: &EvidenceArtifact,
    codex_request_matches: bool,
    mut differences: Vec<StructuralDifference>,
) -> Result<ReplayReport, AppError> {
    let codex_response = parse_json_artifact(root, response_artifact)?;
    let expected_claude_response =
        load_json_artifact(root, manifest, EvidenceArtifactKind::ClaudeResponse)?;
    let actual_claude_response = responses_to_anthropic(codex_response)
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
        create_anthropic_sse_stream_from_responses(stream::iter(vec![Ok::<_, std::io::Error>(
            Bytes::from(upstream_sse),
        )]))
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
        structural_differences: differences,
        network_requests: 0,
    })
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

    use super::*;

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
    fn replays_stream_events_without_network() {
        use serde_json::json;

        use crate::proxy::bridge_forensics::{
            BridgeForensicStore, CaptureMetadata, EvidenceArtifactKind, EvidenceError,
            EvidenceErrorKind,
        };

        let temp = tempfile::tempdir().unwrap();
        let store = BridgeForensicStore::new(temp.path().to_path_buf());
        let mut capture = store
            .begin_turn(CaptureMetadata {
                provider_id: "fixture-provider".to_string(),
                model: "gpt-test".to_string(),
                session_id_hash: "fixture-session".to_string(),
            })
            .unwrap();
        capture
            .record_json(
                EvidenceArtifactKind::ClaudeRequest,
                &json!({
                    "model": "gpt-test",
                    "messages": [{"role": "user", "content": "hello"}]
                }),
            )
            .unwrap();
        capture
            .record_json(
                EvidenceArtifactKind::CodexRequest,
                &json!({
                    "model": "gpt-test",
                    "input": [{
                        "role": "user",
                        "content": [{"type": "input_text", "text": "hello"}]
                    }],
                    "store": false,
                    "include": ["reasoning.encrypted_content"],
                    "instructions": "",
                    "tools": [],
                    "parallel_tool_calls": false,
                    "stream": true
                }),
            )
            .unwrap();
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
            })
            .unwrap();

        let report = replay_bundle(&bundle.path).unwrap();

        assert_eq!(report.mode, ReplayMode::Streaming);
        assert!(report.codex_request_matches);
        assert!(report.claude_response_matches);
        assert_eq!(report.network_requests, 0);
    }

    fn fixture_path(name: &str) -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/bridge-forensics")
            .join(name)
    }
}
