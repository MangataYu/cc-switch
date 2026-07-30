//! OpenAI Responses API 流式转换模块
//!
//! 实现 Responses API SSE → Anthropic SSE 格式转换。
//!
//! Responses API 使用命名事件 (named events) 的生命周期模型：
//! response.created → output_item.added → content_part.added →
//! output_text.delta → content_part.done → output_item.done → response.completed
//!
//! 与 Chat Completions 的 delta chunk 模型完全不同，需要独立的状态机处理。

use super::read_trace::{ReadCallTrace, ReadTrace};
use super::reasoning_bridge::anthropic_block_from_openai_reasoning_item;
use super::tool_compat::{
    sanitize_anthropic_tool_use_input_json_with_protection, ReadOffsetProtection,
};
use super::transform_responses::{
    build_anthropic_usage_from_responses, map_responses_stop_reason,
    responses_to_anthropic_with_read_offset_protection_and_trace,
};
use crate::proxy::bridge_forensics::ForensicStreamObserver;
use crate::proxy::bridge_forensics::StreamingFailureContext;
use crate::proxy::claude_codex_bridge::{
    canonical_request_fingerprint,
    streaming::{
        claude_stream_event_kind, decode_codex_response_event, encode_claude_stream_event,
        event_identity_hashes, ClaudeStreamEvent, CodexResponseEventKind, PreparedCodexStream,
        StreamTerminalState,
    },
    BridgeError, PreparedCodexTurn, ToolRegistry,
};
use crate::proxy::sse::{strip_sse_field, take_sse_block};
use bytes::Bytes;
use futures::stream::{Stream, StreamExt};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

#[inline]
fn response_object_from_event(data: &Value) -> &Value {
    data.get("response").unwrap_or(data)
}

fn anthropic_sse(event_name: &str, payload: &Value) -> Bytes {
    Bytes::from(format!(
        "event: {event_name}\ndata: {}\n\n",
        serde_json::to_string(payload).unwrap_or_default()
    ))
}

fn responses_error_details(data: &Value, fallback: &str) -> (String, String) {
    let response = response_object_from_event(data);
    let error = response.get("error").unwrap_or(response);
    let message = error
        .get("message")
        .and_then(Value::as_str)
        .or_else(|| error.as_str())
        .filter(|message| !message.trim().is_empty())
        .unwrap_or(fallback)
        .to_string();
    let error_type = error
        .get("type")
        .and_then(Value::as_str)
        .or_else(|| error.get("code").and_then(Value::as_str))
        .unwrap_or("upstream_error")
        .to_string();
    (message, error_type)
}

fn anthropic_error_sse(message: &str, error_type: &str) -> Bytes {
    anthropic_sse(
        "error",
        &json!({
            "type": "error",
            "error": {"type": error_type, "message": message}
        }),
    )
}

/// Convert a compatible gateway's non-streaming Responses JSON into a complete
/// Anthropic SSE lifecycle. This is used when the client requested streaming but
/// the upstream ignored `stream:true` and returned `application/json`.
fn responses_json_to_anthropic_sse(
    mut body: Value,
    read_offset_protection: Option<&ReadOffsetProtection>,
    read_trace: Option<&ReadTrace>,
    tool_registry: Option<&ToolRegistry>,
) -> Vec<Bytes> {
    let upstream_body = body.clone();
    if let Some(registry) = tool_registry {
        body = match registry.restore_response(&body) {
            Ok(body) => body,
            Err(error) => {
                return vec![anthropic_error_sse(
                    &error.to_string(),
                    "tool_registry_violation",
                )]
            }
        };
    }
    let mut message = match responses_to_anthropic_with_read_offset_protection_and_trace(
        body,
        read_offset_protection,
        read_trace,
    ) {
        Ok(message) => message,
        Err(error) => {
            return vec![anthropic_error_sse(
                &error.to_string(),
                "response_transform_error",
            )]
        }
    };
    if let Some(registry) = tool_registry {
        message = match registry.restore_anthropic_message(&upstream_body, &message) {
            Ok(message) => message,
            Err(error) => {
                return vec![anthropic_error_sse(
                    &error.to_string(),
                    "tool_registry_violation",
                )]
            }
        };
    }

    let usage = message.get("usage").cloned().unwrap_or_else(|| json!({}));
    let mut start_usage = usage.clone();
    start_usage["output_tokens"] = json!(0);
    let mut events = vec![anthropic_sse(
        "message_start",
        &json!({
            "type": "message_start",
            "message": {
                "id": message.get("id").cloned().unwrap_or_else(|| json!("")),
                "type": "message",
                "role": "assistant",
                "model": message.get("model").cloned().unwrap_or_else(|| json!("")),
                "usage": start_usage
            }
        }),
    )];

    if let Some(content) = message.get("content").and_then(Value::as_array) {
        for (index, block) in content.iter().enumerate() {
            let index = index as u64;
            match block.get("type").and_then(Value::as_str) {
                Some("text") => {
                    events.push(anthropic_sse(
                        "content_block_start",
                        &json!({"type":"content_block_start","index":index,"content_block":{"type":"text","text":""}}),
                    ));
                    if let Some(text) = block.get("text").and_then(Value::as_str) {
                        if !text.is_empty() {
                            events.push(anthropic_sse(
                                "content_block_delta",
                                &json!({"type":"content_block_delta","index":index,"delta":{"type":"text_delta","text":text}}),
                            ));
                        }
                    }
                    events.push(anthropic_sse(
                        "content_block_stop",
                        &json!({"type":"content_block_stop","index":index}),
                    ));
                }
                Some("tool_use") => {
                    events.push(anthropic_sse(
                        "content_block_start",
                        &json!({
                            "type":"content_block_start",
                            "index":index,
                            "content_block":{
                                "type":"tool_use",
                                "id":block.get("id").cloned().unwrap_or_else(|| json!("")),
                                "name":block.get("name").cloned().unwrap_or_else(|| json!("")),
                                "input":{}
                            }
                        }),
                    ));
                    let input = block.get("input").cloned().unwrap_or_else(|| json!({}));
                    events.push(anthropic_sse(
                        "content_block_delta",
                        &json!({
                            "type":"content_block_delta",
                            "index":index,
                            "delta":{"type":"input_json_delta","partial_json":serde_json::to_string(&input).unwrap_or_else(|_| "{}".to_string())}
                        }),
                    ));
                    events.push(anthropic_sse(
                        "content_block_stop",
                        &json!({"type":"content_block_stop","index":index}),
                    ));
                }
                Some("thinking") => {
                    events.push(anthropic_sse(
                        "content_block_start",
                        &json!({"type":"content_block_start","index":index,"content_block":{"type":"thinking","thinking":""}}),
                    ));
                    if let Some(thinking) = block.get("thinking").and_then(Value::as_str) {
                        if !thinking.is_empty() {
                            events.push(anthropic_sse(
                                "content_block_delta",
                                &json!({"type":"content_block_delta","index":index,"delta":{"type":"thinking_delta","thinking":thinking}}),
                            ));
                        }
                    }
                    if let Some(signature) = block.get("signature").and_then(Value::as_str) {
                        if !signature.is_empty() {
                            events.push(anthropic_sse(
                                "content_block_delta",
                                &json!({"type":"content_block_delta","index":index,"delta":{"type":"signature_delta","signature":signature}}),
                            ));
                        }
                    }
                    events.push(anthropic_sse(
                        "content_block_stop",
                        &json!({"type":"content_block_stop","index":index}),
                    ));
                }
                Some("redacted_thinking") => {
                    events.push(anthropic_sse(
                        "content_block_start",
                        &json!({"type":"content_block_start","index":index,"content_block":block}),
                    ));
                    events.push(anthropic_sse(
                        "content_block_stop",
                        &json!({"type":"content_block_stop","index":index}),
                    ));
                }
                _ => {}
            }
        }
    }

    events.push(anthropic_sse(
        "message_delta",
        &json!({
            "type":"message_delta",
            "delta":{
                "stop_reason":message.get("stop_reason").cloned().unwrap_or(Value::Null),
                "stop_sequence":null
            },
            "usage":usage
        }),
    ));
    events.push(anthropic_sse(
        "message_stop",
        &json!({"type":"message_stop"}),
    ));
    events
}

#[inline]
fn content_part_key(data: &Value) -> Option<String> {
    if let (Some(item_id), Some(content_index)) = (
        data.get("item_id").and_then(|v| v.as_str()),
        data.get("content_index").and_then(|v| v.as_u64()),
    ) {
        return Some(format!("part:{item_id}:{content_index}"));
    }
    if let (Some(output_index), Some(content_index)) = (
        data.get("output_index").and_then(|v| v.as_u64()),
        data.get("content_index").and_then(|v| v.as_u64()),
    ) {
        return Some(format!("part:out:{output_index}:{content_index}"));
    }
    None
}

#[inline]
fn tool_item_key_from_added(data: &Value, item: &Value) -> Option<String> {
    if let Some(item_id) = item.get("id").and_then(|v| v.as_str()) {
        return Some(format!("tool:{item_id}"));
    }
    if let Some(item_id) = data.get("item_id").and_then(|v| v.as_str()) {
        return Some(format!("tool:{item_id}"));
    }
    if let Some(output_index) = data.get("output_index").and_then(|v| v.as_u64()) {
        return Some(format!("tool:out:{output_index}"));
    }
    None
}

#[inline]
fn tool_item_key_from_event(data: &Value) -> Option<String> {
    if let Some(item_id) = data.get("item_id").and_then(|v| v.as_str()) {
        return Some(format!("tool:{item_id}"));
    }
    if let Some(output_index) = data.get("output_index").and_then(|v| v.as_u64()) {
        return Some(format!("tool:out:{output_index}"));
    }
    None
}

#[inline]
fn reasoning_item_key(data: &Value, item: Option<&Value>) -> Option<String> {
    if let Some(item_id) = item
        .and_then(|value| value.get("id"))
        .and_then(Value::as_str)
        .or_else(|| data.get("item_id").and_then(Value::as_str))
    {
        return Some(format!("reasoning:{item_id}"));
    }
    data.get("output_index")
        .and_then(Value::as_u64)
        .map(|index| format!("reasoning:out:{index}"))
}

/// Resolve content index for a text/refusal content part event.
///
/// Uses `content_part_key` to look up or assign a stable index, falling back to
/// `fallback_open_index` when no key is available.
#[inline]
fn resolve_content_index(
    data: &Value,
    next_content_index: &mut u32,
    index_by_key: &mut HashMap<String, u32>,
    fallback_open_index: &mut Option<u32>,
) -> u32 {
    if let Some(k) = content_part_key(data) {
        if let Some(existing) = index_by_key.get(&k).copied() {
            existing
        } else {
            let assigned = *next_content_index;
            *next_content_index += 1;
            index_by_key.insert(k, assigned);
            assigned
        }
    } else if let Some(existing) = *fallback_open_index {
        existing
    } else {
        let assigned = *next_content_index;
        *next_content_index += 1;
        *fallback_open_index = Some(assigned);
        assigned
    }
}

/// 创建从 Responses API SSE 到 Anthropic SSE 的转换流
///
/// 状态机跟踪: message_id, current_model, has_sent_message_start, item/content index map
/// SSE 解析支持 named events (event: + data: 行)
#[cfg(test)]
pub fn create_anthropic_sse_stream_from_responses<E: std::error::Error + Send + 'static>(
    stream: impl Stream<Item = Result<Bytes, E>> + Send + 'static,
) -> impl Stream<Item = Result<Bytes, std::io::Error>> + Send {
    create_anthropic_sse_stream_from_responses_with_read_offset_protection_and_trace(
        stream, None, None,
    )
}

#[cfg(test)]
pub(crate) fn create_anthropic_sse_stream_from_responses_with_read_offset_protection<
    E: std::error::Error + Send + 'static,
>(
    stream: impl Stream<Item = Result<Bytes, E>> + Send + 'static,
    read_offset_protection: Option<ReadOffsetProtection>,
) -> impl Stream<Item = Result<Bytes, std::io::Error>> + Send {
    create_anthropic_sse_stream_from_responses_with_read_offset_protection_and_trace(
        stream,
        read_offset_protection,
        None,
    )
}

pub(crate) fn create_anthropic_sse_stream_from_responses_with_read_offset_protection_and_trace<
    E: std::error::Error + Send + 'static,
>(
    stream: impl Stream<Item = Result<Bytes, E>> + Send + 'static,
    read_offset_protection: Option<ReadOffsetProtection>,
    read_trace: Option<ReadTrace>,
) -> impl Stream<Item = Result<Bytes, std::io::Error>> + Send {
    create_anthropic_sse_stream_from_responses_core(
        stream,
        read_offset_protection,
        read_trace,
        None,
        None,
    )
}

#[cfg(test)]
pub(crate) fn create_anthropic_sse_stream_from_responses_with_registry<
    E: std::error::Error + Send + 'static,
>(
    stream: impl Stream<Item = Result<Bytes, E>> + Send + 'static,
    read_offset_protection: Option<ReadOffsetProtection>,
    read_trace: Option<ReadTrace>,
    tool_registry: Arc<ToolRegistry>,
) -> impl Stream<Item = Result<Bytes, std::io::Error>> + Send {
    create_anthropic_sse_stream_from_responses_core(
        stream,
        read_offset_protection,
        read_trace,
        Some(tool_registry),
        None,
    )
}

pub(crate) fn create_anthropic_sse_stream_from_responses_with_prepared_turn<
    E: std::error::Error + Send + 'static,
>(
    stream: impl Stream<Item = Result<Bytes, E>> + Send + 'static,
    read_offset_protection: Option<ReadOffsetProtection>,
    read_trace: Option<ReadTrace>,
    prepared_turn: PreparedCodexTurn,
) -> impl Stream<Item = Result<Bytes, std::io::Error>> + Send {
    create_strict_anthropic_sse_stream_from_responses(
        stream,
        read_offset_protection,
        read_trace,
        prepared_turn,
        Arc::new(Mutex::new(None)),
    )
}

pub(crate) fn create_anthropic_sse_stream_from_responses_with_evidence<
    E: std::error::Error + Send + 'static,
>(
    stream: impl Stream<Item = Result<Bytes, E>> + Send + 'static,
    read_offset_protection: Option<ReadOffsetProtection>,
    read_trace: Option<ReadTrace>,
    evidence: Option<ForensicStreamObserver>,
) -> impl Stream<Item = Result<Bytes, std::io::Error>> + Send {
    let observer = Arc::new(Mutex::new(evidence));
    let upstream = observe_upstream_stream(stream, observer.clone());
    let converted = create_anthropic_sse_stream_from_responses_core(
        upstream,
        read_offset_protection,
        read_trace,
        None,
        None,
    );
    observe_claude_stream(converted, observer)
}

pub(crate) fn create_anthropic_sse_stream_from_responses_with_shadow<
    E: std::error::Error + Send + 'static,
>(
    stream: impl Stream<Item = Result<Bytes, E>> + Send + 'static,
    read_offset_protection: Option<ReadOffsetProtection>,
    read_trace: Option<ReadTrace>,
    prepared_turn: PreparedCodexTurn,
    evidence: Option<ForensicStreamObserver>,
) -> impl Stream<Item = Result<Bytes, std::io::Error>> + Send {
    let comparison = Arc::new(Mutex::new(
        crate::proxy::claude_codex_bridge::shadow::ShadowComparisonSession::resume(prepared_turn),
    ));
    let evidence = Arc::new(Mutex::new(evidence));
    let stream = observe_upstream_stream(stream, evidence.clone());
    let upstream_comparison = comparison.clone();
    let upstream = stream.map(move |result| {
        if let Ok(bytes) = result.as_ref() {
            if let Ok(mut comparison) = upstream_comparison.lock() {
                comparison.observe_stream_chunk(bytes);
            }
        }
        result
    });
    let converted = create_anthropic_sse_stream_from_responses_core(
        upstream,
        read_offset_protection,
        read_trace,
        None,
        None,
    );
    let converted = observe_claude_stream(converted, evidence);
    async_stream::stream! {
        tokio::pin!(converted);
        while let Some(result) = converted.next().await {
            if let Ok(bytes) = result.as_ref() {
                if let Ok(mut comparison) = comparison.lock() {
                    comparison.observe_legacy_stream_chunk(bytes);
                }
            }
            yield result;
        }
        if let Ok(mut comparison) = comparison.lock() {
            comparison.finish_stream();
            let report = comparison.report();
            log::debug!(
                "[ClaudeCodexBridge] mode=shadow stream_differences={} unexplained={} comparison_failures={} bounded={}",
                report.differences.len(),
                report.readiness.unexplained_differences,
                report.readiness.comparison_failures,
                report.stream.as_ref().is_some_and(|stream| stream.bounded)
            );
        }
    }
}

pub(crate) fn create_anthropic_sse_stream_from_responses_with_prepared_turn_and_evidence<
    E: std::error::Error + Send + 'static,
>(
    stream: impl Stream<Item = Result<Bytes, E>> + Send + 'static,
    read_offset_protection: Option<ReadOffsetProtection>,
    read_trace: Option<ReadTrace>,
    prepared_turn: PreparedCodexTurn,
    evidence: Option<ForensicStreamObserver>,
) -> impl Stream<Item = Result<Bytes, std::io::Error>> + Send {
    let observer = Arc::new(Mutex::new(evidence));
    create_strict_anthropic_sse_stream_from_responses(
        stream,
        read_offset_protection,
        read_trace,
        prepared_turn,
        observer,
    )
}

fn create_strict_anthropic_sse_stream_from_responses<E: std::error::Error + Send + 'static>(
    stream: impl Stream<Item = Result<Bytes, E>> + Send + 'static,
    _read_offset_protection: Option<ReadOffsetProtection>,
    _read_trace: Option<ReadTrace>,
    prepared_turn: PreparedCodexTurn,
    observer: SharedForensicObserver,
) -> impl Stream<Item = Result<Bytes, std::io::Error>> + Send {
    async_stream::stream! {
        let turn_id = prepared_turn.turn_id.clone();
        let registry_fingerprint = format!(
            "{}:{}",
            prepared_turn.tool_registry.identity_fingerprint(),
            prepared_turn.tool_registry.schema_fingerprint()
        );
        let capability_fingerprint = canonical_request_fingerprint(
            &serde_json::to_value(prepared_turn.capability_snapshot.as_ref())
                .unwrap_or(Value::Null),
        );
        let mut machine = prepared_turn.start_stream();
        let mut buffer = String::new();
        let mut utf8_remainder = Vec::new();
        let mut failed = false;
        let stream = stream
            .map(|result| (result, false))
            .chain(futures::stream::once(async {
                (Ok::<Bytes, E>(Bytes::new()), true)
            }));
        tokio::pin!(stream);

        'upstream: while let Some((chunk, is_eof)) = stream.next().await {
            let bytes = match chunk {
                Ok(bytes) => bytes,
                Err(_) => {
                    let error = BridgeError::IncompleteStream {
                        summary: "upstream byte stream failed before terminal response".to_string(),
                    };
                    set_strict_failure_context(
                        &observer,
                        "byte_stream_error",
                        None,
                        None,
                        &machine,
                        &turn_id,
                        &registry_fingerprint,
                        &capability_fingerprint,
                    );
                    mark_observer_stream_error(&observer);
                    yield Ok(strict_error_sse(&error));
                    failed = true;
                    break;
                }
            };
            crate::proxy::sse::append_utf8_safe(&mut buffer, &mut utf8_remainder, &bytes);
            if is_eof && !utf8_remainder.is_empty() {
                let error = BridgeError::IncompleteStream {
                    summary: "stream ended inside a UTF-8 scalar".to_string(),
                };
                set_strict_failure_context(
                    &observer,
                    "utf8_truncation",
                    None,
                    None,
                    &machine,
                    &turn_id,
                    &registry_fingerprint,
                    &capability_fingerprint,
                );
                mark_observer_stream_error(&observer);
                yield Ok(strict_error_sse(&error));
                failed = true;
                break;
            }
            if is_eof && !buffer.trim().is_empty() {
                buffer.push_str("\n\n");
            }

            while let Some(block) = take_sse_block(&mut buffer) {
                if block.trim().is_empty() {
                    continue;
                }
                let (named_event, data) = match strict_sse_payload(&block) {
                    Ok(Some(value)) => value,
                    Ok(None) => continue,
                    Err(error) => {
                        set_strict_failure_context(
                            &observer,
                            "sse_decode_error",
                            None,
                            None,
                            &machine,
                            &turn_id,
                            &registry_fingerprint,
                            &capability_fingerprint,
                        );
                        mark_observer_stream_error(&observer);
                        yield Ok(strict_error_sse(&error));
                        failed = true;
                        break 'upstream;
                    }
                };
                let decoded = match decode_codex_response_event(named_event.as_deref(), data) {
                    Ok(decoded) => decoded,
                    Err(error) => {
                        set_strict_failure_context(
                            &observer,
                            "typed_decode_error",
                            None,
                            None,
                            &machine,
                            &turn_id,
                            &registry_fingerprint,
                            &capability_fingerprint,
                        );
                        mark_observer_stream_error(&observer);
                        yield Ok(strict_error_sse(&error));
                        failed = true;
                        break 'upstream;
                    }
                };
                for event in decoded {
                    let event_kind = event.kind();
                    let (item_hash, call_hash) = event_identity_hashes(&event);
                    observe_strict_upstream_event(&observer, event_kind);
                    let claude_events = match machine.apply(event) {
                        Ok(events) => {
                            observe_strict_decision(
                                &observer,
                                &machine,
                                &turn_id,
                                &registry_fingerprint,
                                &capability_fingerprint,
                            );
                            events
                        }
                        Err(error) => {
                            set_strict_failure_context(
                                &observer,
                                &format!("{event_kind:?}").to_ascii_lowercase(),
                                item_hash,
                                call_hash,
                                &machine,
                                &turn_id,
                                &registry_fingerprint,
                                &capability_fingerprint,
                            );
                            mark_observer_stream_error(&observer);
                            yield Ok(strict_error_sse(&error));
                            failed = true;
                            break 'upstream;
                        }
                    };
                    for claude_event in claude_events {
                        observe_strict_claude_event(&observer, &claude_event);
                        yield Ok(encode_claude_stream_event(&claude_event));
                        if let Err(error) = machine.acknowledge_emitted(&claude_event) {
                            mark_observer_stream_error(&observer);
                            yield Ok(strict_error_sse(&error));
                            failed = true;
                            break 'upstream;
                        }
                        update_strict_visibility(&observer, machine.visibility());
                    }
                }
            }
            if is_eof {
                break;
            }
        }

        if !failed {
            if let Err(error) = machine.finish() {
                set_strict_failure_context(
                    &observer,
                    "eof",
                    None,
                    None,
                    &machine,
                    &turn_id,
                    &registry_fingerprint,
                    &capability_fingerprint,
                );
                mark_observer_stream_error(&observer);
                yield Ok(strict_error_sse(&error));
                failed = true;
            }
        }
        finish_strict_observer(&observer, failed);
    }
}

fn observe_strict_decision(
    observer: &SharedForensicObserver,
    machine: &PreparedCodexStream,
    turn_id: &str,
    registry_fingerprint: &str,
    capability_fingerprint: &str,
) {
    let Some(decision) = machine.decisions().last() else {
        return;
    };
    if let Ok(mut guard) = observer.lock() {
        if let Some(evidence) = guard.as_mut() {
            if let Err(error) = evidence.typed_decision(
                turn_id,
                decision,
                machine.terminal_state(),
                registry_fingerprint,
                capability_fingerprint,
            ) {
                log::warn!("[BridgeEvidence] capture_failed stage=stream_decision error={error}");
                *guard = None;
            }
        }
    }
}

fn update_strict_visibility(
    observer: &SharedForensicObserver,
    visibility: crate::proxy::claude_codex_bridge::streaming::StreamVisibility,
) {
    if let Ok(mut guard) = observer.lock() {
        if let Some(evidence) = guard.as_mut() {
            evidence.update_stream_visibility(visibility);
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn set_strict_failure_context(
    observer: &SharedForensicObserver,
    event_kind: &str,
    item_identity_hash: Option<String>,
    call_identity_hash: Option<String>,
    machine: &PreparedCodexStream,
    turn_id: &str,
    registry_fingerprint: &str,
    capability_fingerprint: &str,
) {
    let visibility = machine.visibility();
    let state = serde_enum_name(machine.terminal_state());
    let context = StreamingFailureContext {
        event_kind: event_kind.to_string(),
        event_sequence: machine
            .decisions()
            .last()
            .map_or(1, |decision| decision.sequence.saturating_add(1)),
        turn_id: turn_id.to_string(),
        item_identity_hash,
        call_identity_hash,
        state_before: state.clone(),
        state_after: state,
        output_already_emitted: visibility.output_emitted,
        tool_visible: visibility.tool_visible,
        terminal_state: serde_enum_name(machine.terminal_state()),
        registry_fingerprint: registry_fingerprint.to_string(),
        capability_fingerprint: capability_fingerprint.to_string(),
    };
    if let Ok(mut guard) = observer.lock() {
        if let Some(evidence) = guard.as_mut() {
            evidence.set_stream_failure_context(context);
        }
    }
}

fn serde_enum_name(state: StreamTerminalState) -> String {
    serde_json::to_value(state)
        .ok()
        .and_then(|value| value.as_str().map(str::to_string))
        .unwrap_or_else(|| "unknown".to_string())
}

fn strict_sse_payload(block: &str) -> Result<Option<(Option<String>, Value)>, BridgeError> {
    let mut event_name = None;
    let mut data_parts = Vec::new();
    for line in block.lines() {
        if let Some(value) = strip_sse_field(line, "event") {
            event_name = Some(value.trim().to_string());
        } else if let Some(value) = strip_sse_field(line, "data") {
            data_parts.push(value.to_string());
        }
    }
    if data_parts.is_empty() {
        return Ok(None);
    }
    let data = data_parts.join("\n");
    if data.trim() == "[DONE]" {
        return Err(BridgeError::IncompleteStream {
            summary: "legacy DONE marker cannot replace a terminal Responses event".to_string(),
        });
    }
    let payload = serde_json::from_str(&data).map_err(|_| BridgeError::InvalidUpstreamEvent {
        event_kind: "malformed_json".to_string(),
        summary: "SSE data is not valid JSON".to_string(),
    })?;
    Ok(Some((event_name, payload)))
}

fn strict_error_sse(error: &BridgeError) -> Bytes {
    let error_type = match error {
        BridgeError::ToolRegistryViolation { .. } => "tool_registry_violation",
        BridgeError::ConversationStateConflict { .. } => "conversation_state_conflict",
        BridgeError::IncompleteStream { .. } => "incomplete_stream",
        _ => "invalid_upstream_event",
    };
    encode_claude_stream_event(&ClaudeStreamEvent::Error {
        error_type: error_type.to_string(),
        safe_message: error.to_string(),
    })
}

fn observe_strict_upstream_event(observer: &SharedForensicObserver, kind: CodexResponseEventKind) {
    let event_type = match kind {
        CodexResponseEventKind::ResponseCompleted => "response.completed",
        CodexResponseEventKind::ResponseFailed => "response.failed",
        _ => "typed_response_event",
    };
    offer_observer_event(
        observer,
        &json!({"type":event_type,"event_kind":kind}),
        true,
    );
}

fn observe_strict_claude_event(observer: &SharedForensicObserver, event: &ClaudeStreamEvent) {
    offer_observer_event(
        observer,
        &json!({"type":claude_stream_event_kind(event)}),
        false,
    );
}

fn finish_strict_observer(observer: &SharedForensicObserver, failed: bool) {
    let evidence = observer.lock().ok().and_then(|mut guard| guard.take());
    if let Some(evidence) = evidence {
        match evidence.finish(failed.then_some("typed stream validation failed")) {
            Ok(Some(bundle)) => log::error!(
                "[BridgeEvidence] bundle_id={} stage=stream_transform summary=typed_stream_failed",
                bundle.bundle_id.0
            ),
            Ok(None) => {}
            Err(error) => {
                log::warn!("[BridgeEvidence] capture_failed stage=stream_transform error={error}")
            }
        }
    }
}

fn create_anthropic_sse_stream_from_responses_core<E: std::error::Error + Send + 'static>(
    stream: impl Stream<Item = Result<Bytes, E>> + Send + 'static,
    read_offset_protection: Option<ReadOffsetProtection>,
    read_trace: Option<ReadTrace>,
    tool_registry: Option<Arc<ToolRegistry>>,
    prepared_turn: Option<PreparedCodexTurn>,
) -> impl Stream<Item = Result<Bytes, std::io::Error>> + Send {
    async_stream::stream! {
        let mut buffer = String::new();
        let mut utf8_remainder: Vec<u8> = Vec::new();
        let mut message_id: Option<String> = None;
        let mut current_model: Option<String> = None;
        let mut has_sent_message_start = false;
        let mut has_tool_use = false;
        let mut next_content_index: u32 = 0;
        let mut index_by_key: HashMap<String, u32> = HashMap::new();
        let mut open_indices: HashSet<u32> = HashSet::new();
        let mut fallback_open_index: Option<u32> = None;
        let mut current_text_index: Option<u32> = None;
        let mut tool_index_by_item_id: HashMap<String, u32> = HashMap::new();
        let mut tool_name_by_index: HashMap<u32, String> = HashMap::new();
        let mut tool_codex_name_by_index: HashMap<u32, String> = HashMap::new();
        let mut tool_args_by_index: HashMap<u32, String> = HashMap::new();
        let mut tool_had_delta: HashSet<u32> = HashSet::new();
        let mut tool_trace_by_index: HashMap<u32, ReadCallTrace> = HashMap::new();
        let mut tool_call_id_by_index: HashMap<u32, String> = HashMap::new();
        let mut pending_registry_tools: HashSet<u32> = HashSet::new();
        let mut completed_registry_tools: HashMap<u32, (String, String, String)> = HashMap::new();
        let mut duplicate_completed_registry_args: HashMap<u32, String> = HashMap::new();
        let mut last_tool_index: Option<u32> = None;
        let mut reasoning_index_by_item_id: HashMap<String, u32> = HashMap::new();
        let mut reasoning_item_by_index: HashMap<u32, Value> = HashMap::new();
        let mut reasoning_text_by_index: HashMap<u32, String> = HashMap::new();
        let mut legacy_reasoning_index: Option<u32> = None;
        let mut has_substantive_output = false;
        let mut terminated = false;

        // Append an EOF sentinel so the same parser handles a final SSE event that
        // omitted its trailing blank line. The boolean distinguishes the sentinel
        // from a legitimate empty upstream chunk.
        let stream = stream
            .map(|result| (result, false))
            .chain(futures::stream::once(async {
                (Ok::<Bytes, E>(Bytes::new()), true)
            }));
        tokio::pin!(stream);

        while let Some((chunk, is_eof)) = stream.next().await {
            match chunk {
                Ok(bytes) => {
                    crate::proxy::sse::append_utf8_safe(&mut buffer, &mut utf8_remainder, &bytes);

                    // A few compatible gateways ignore stream:true and return one
                    // JSON document. Hold it intact until EOF, including any pretty-
                    // printed blank lines that would otherwise look like SSE separators.
                    let looks_like_json = matches!(
                        buffer
                            .trim_start_matches(|ch: char| ch.is_whitespace() || ch == '\u{feff}')
                            .as_bytes()
                            .first(),
                        Some(b'{') | Some(b'[')
                    );
                    if looks_like_json && !is_eof {
                        continue;
                    }
                    if looks_like_json && is_eof {
                        match serde_json::from_str::<Value>(buffer.trim()) {
                            Ok(body) => {
                                if let Some(prepared) = prepared_turn.as_ref() {
                                    let ledger_result = body
                                        .get("output")
                                        .and_then(Value::as_array)
                                        .into_iter()
                                        .flatten()
                                        .filter(|item| {
                                            item.get("type").and_then(Value::as_str)
                                                == Some("function_call")
                                        })
                                        .try_for_each(|item| {
                                            prepared.observe_returned_tool_call(
                                                item.get("name")
                                                    .and_then(Value::as_str)
                                                    .unwrap_or(""),
                                                item.get("call_id")
                                                    .and_then(Value::as_str)
                                                    .unwrap_or(""),
                                                item.get("arguments")
                                                    .and_then(Value::as_str)
                                                    .unwrap_or(""),
                                            )
                                        });
                                    if let Err(error) = ledger_result {
                                        yield Ok(anthropic_error_sse(
                                            &error.to_string(),
                                            "conversation_state_conflict",
                                        ));
                                        terminated = true;
                                        buffer.clear();
                                        continue;
                                    }
                                }
                                for event in responses_json_to_anthropic_sse(
                                    body,
                                    read_offset_protection.as_ref(),
                                    read_trace.as_ref(),
                                    tool_registry.as_deref(),
                                ) {
                                    yield Ok(event);
                                }
                                terminated = true;
                            }
                            Err(error) => {
                                yield Ok(anthropic_error_sse(
                                    &format!("Invalid JSON response from Responses upstream: {error}"),
                                    "response_parse_error",
                                ));
                                terminated = true;
                            }
                        }
                        buffer.clear();
                        continue;
                    }

                    if is_eof && !buffer.trim().is_empty() {
                        buffer.push_str("\n\n");
                    }

                    // SSE 事件由 \n\n 分隔
                    while let Some(block) = take_sse_block(&mut buffer) {
                        if block.trim().is_empty() {
                            continue;
                        }

                        // 解析 SSE 块：提取 event: 和 data: 行
                        let mut event_type: Option<String> = None;
                        let mut data_parts: Vec<String> = Vec::new();

                        for line in block.lines() {
                            if let Some(evt) = strip_sse_field(line, "event") {
                                event_type = Some(evt.trim().to_string());
                            } else if let Some(d) = strip_sse_field(line, "data") {
                                data_parts.push(d.to_string());
                            }
                        }

                        if data_parts.is_empty() {
                            continue;
                        }

                        let data_str = data_parts.join("\n");

                        // 解析 JSON 数据
                        let data: Value = match serde_json::from_str(&data_str) {
                            Ok(v) => v,
                            Err(_) => continue,
                        };

                        // Official streams use both a named SSE event and `type` in
                        // the JSON payload. Compatible gateways sometimes omit the
                        // `event:` line, so fall back to the payload type.
                        let event_name = event_type
                            .as_deref()
                            .filter(|name| !name.is_empty())
                            .or_else(|| data.get("type").and_then(Value::as_str))
                            .unwrap_or("");

                        log::debug!("[Claude/Responses] <<< SSE event: {event_name}");

                        // Ignore every event after a terminal response. In particular,
                        // do not synthesize message_start if a broken gateway emits a
                        // late delta after response.failed/error.
                        if terminated {
                            continue;
                        }

                        let delta_requires_message_start = matches!(
                            event_name,
                            "response.output_text.delta"
                                | "response.refusal.delta"
                                | "response.function_call_arguments.delta"
                                | "response.reasoning_summary_text.delta"
                                | "response.reasoning_text.delta"
                                | "response.reasoning.delta"
                        );
                        if delta_requires_message_start {
                            has_substantive_output = true;
                        }
                        if delta_requires_message_start && !has_sent_message_start {
                            yield Ok(anthropic_sse(
                                "message_start",
                                &json!({
                                    "type":"message_start",
                                    "message":{
                                        "id":message_id.clone().unwrap_or_default(),
                                        "type":"message",
                                        "role":"assistant",
                                        "model":current_model.clone().unwrap_or_default(),
                                        "usage":{"input_tokens":0,"output_tokens":0}
                                    }
                                }),
                            ));
                            has_sent_message_start = true;
                        }

                        match event_name {
                            // ================================================
                            // response.created → message_start
                            // ================================================
                            "response.created" => {
                                let response_obj = response_object_from_event(&data);
                                if let Some(id) = response_obj.get("id").and_then(|i| i.as_str()) {
                                    message_id = Some(id.to_string());
                                }
                                if let Some(model) =
                                    response_obj.get("model").and_then(|m| m.as_str())
                                {
                                    current_model = Some(model.to_string());
                                }

                                has_sent_message_start = true;
                                // Build usage with defensive null handling
                                // Some() wrapper ensures build function always receives valid input
                                // Fallback to empty object {} if usage field missing, ensuring message_start
                                // event always has valid usage structure for VSCode Extension compatibility
                                let start_usage = build_anthropic_usage_from_responses(
                                    Some(response_obj.get("usage").unwrap_or(&json!({}))),
                                );

                                let event = json!({
                                    "type": "message_start",
                                    "message": {
                                        "id": message_id.clone().unwrap_or_default(),
                                        "type": "message",
                                        "role": "assistant",
                                        "model": current_model.clone().unwrap_or_default(),
                                        "usage": start_usage
                                    }
                                });
                                let sse = format!("event: message_start\ndata: {}\n\n",
                                    serde_json::to_string(&event).unwrap_or_default());
                                log::debug!("[Claude/Responses] >>> Anthropic SSE: message_start");
                                yield Ok(Bytes::from(sse));
                            }

                            // ================================================
                            // response.content_part.added → content_block_start (text)
                            // ================================================
                            "response.content_part.added" => {
                                // 确保 message_start 已发送
                                if !has_sent_message_start {
                                    let start_event = json!({
                                        "type": "message_start",
                                        "message": {
                                            "id": message_id.clone().unwrap_or_default(),
                                            "type": "message",
                                            "role": "assistant",
                                            "model": current_model.clone().unwrap_or_default(),
                                            "usage": { "input_tokens": 0, "output_tokens": 0 }
                                        }
                                    });
                                    let sse = format!("event: message_start\ndata: {}\n\n",
                                        serde_json::to_string(&start_event).unwrap_or_default());
                                    yield Ok(Bytes::from(sse));
                                    has_sent_message_start = true;
                                }

                                if let Some(part) = data.get("part") {
                                    let part_type = part.get("type").and_then(|t| t.as_str());
                                    if matches!(part_type, Some("output_text") | Some("refusal")) {
                                        let index = if let Some(index) = current_text_index {
                                            index
                                        } else {
                                            let index = resolve_content_index(
                                                &data,
                                                &mut next_content_index,
                                                &mut index_by_key,
                                                &mut fallback_open_index,
                                            );
                                            current_text_index = Some(index);
                                            index
                                        };

                                        if open_indices.contains(&index) {
                                            continue;
                                        }

                                        let event = json!({
                                            "type": "content_block_start",
                                            "index": index,
                                            "content_block": {
                                                "type": "text",
                                                "text": ""
                                            }
                                        });
                                        let sse = format!("event: content_block_start\ndata: {}\n\n",
                                            serde_json::to_string(&event).unwrap_or_default());
                                        yield Ok(Bytes::from(sse));
                                        open_indices.insert(index);
                                    }
                                }
                            }

                            // ================================================
                            // response.output_text.delta → content_block_delta (text_delta)
                            // ================================================
                            "response.output_text.delta" => {
                                if let Some(delta) = data.get("delta").and_then(|d| d.as_str()) {
                                    let index = if let Some(index) = current_text_index {
                                        index
                                    } else {
                                        let index = resolve_content_index(
                                            &data,
                                            &mut next_content_index,
                                            &mut index_by_key,
                                            &mut fallback_open_index,
                                        );
                                        current_text_index = Some(index);
                                        index
                                    };

                                    if !open_indices.contains(&index) {
                                        let start_event = json!({
                                            "type": "content_block_start",
                                            "index": index,
                                            "content_block": {
                                                "type": "text",
                                                "text": ""
                                            }
                                        });
                                        let start_sse = format!("event: content_block_start\ndata: {}\n\n",
                                            serde_json::to_string(&start_event).unwrap_or_default());
                                        yield Ok(Bytes::from(start_sse));
                                        open_indices.insert(index);
                                    }
                                    let event = json!({
                                        "type": "content_block_delta",
                                        "index": index,
                                        "delta": {
                                            "type": "text_delta",
                                            "text": delta
                                        }
                                    });
                                    let sse = format!("event: content_block_delta\ndata: {}\n\n",
                                        serde_json::to_string(&event).unwrap_or_default());
                                    yield Ok(Bytes::from(sse));
                                }
                            }

                            // ================================================
                            // response.refusal.delta → content_block_delta (text_delta)
                            // ================================================
                            "response.refusal.delta" => {
                                if let Some(delta) = data.get("delta").and_then(|d| d.as_str()) {
                                    let index = if let Some(index) = current_text_index {
                                        index
                                    } else {
                                        let index = resolve_content_index(
                                            &data,
                                            &mut next_content_index,
                                            &mut index_by_key,
                                            &mut fallback_open_index,
                                        );
                                        current_text_index = Some(index);
                                        index
                                    };

                                    if !open_indices.contains(&index) {
                                        let start_event = json!({
                                            "type": "content_block_start",
                                            "index": index,
                                            "content_block": {
                                                "type": "text",
                                                "text": ""
                                            }
                                        });
                                        let start_sse = format!("event: content_block_start\ndata: {}\n\n",
                                            serde_json::to_string(&start_event).unwrap_or_default());
                                        yield Ok(Bytes::from(start_sse));
                                        open_indices.insert(index);
                                    }

                                    let event = json!({
                                        "type": "content_block_delta",
                                        "index": index,
                                        "delta": {
                                            "type": "text_delta",
                                            "text": delta
                                        }
                                    });
                                    let sse = format!("event: content_block_delta\ndata: {}\n\n",
                                        serde_json::to_string(&event).unwrap_or_default());
                                    yield Ok(Bytes::from(sse));
                                }
                            }

                            // ================================================
                            // response.content_part.done → content_block_stop
                            // ================================================
                            "response.content_part.done" => {}

                            // ================================================
                            // response.output_item.added (function_call) → content_block_start (tool_use)
                            // ================================================
                            "response.output_item.added" => {
                                if let Some(item) = data.get("item") {
                                    let item_type = item.get("type").and_then(|t| t.as_str()).unwrap_or("");
                                    if item_type == "function_call" {
                                        has_tool_use = true;
                                        has_substantive_output = true;
                                        if let Some(index) = current_text_index.take() {
                                            if open_indices.remove(&index) {
                                                let stop_event = json!({
                                                    "type": "content_block_stop",
                                                    "index": index
                                                });
                                                let stop_sse = format!("event: content_block_stop\ndata: {}\n\n",
                                                    serde_json::to_string(&stop_event).unwrap_or_default());
                                                yield Ok(Bytes::from(stop_sse));
                                            }
                                            if fallback_open_index == Some(index) {
                                                fallback_open_index = None;
                                            }
                                        }
                                        // 确保 message_start 已发送
                                        if !has_sent_message_start {
                                            let start_event = json!({
                                                "type": "message_start",
                                                "message": {
                                                    "id": message_id.clone().unwrap_or_default(),
                                                    "type": "message",
                                                    "role": "assistant",
                                                    "model": current_model.clone().unwrap_or_default(),
                                                    "usage": { "input_tokens": 0, "output_tokens": 0 }
                                                }
                                            });
                                            let sse = format!("event: message_start\ndata: {}\n\n",
                                                serde_json::to_string(&start_event).unwrap_or_default());
                                            yield Ok(Bytes::from(sse));
                                            has_sent_message_start = true;
                                        }

                                        let call_id = item.get("call_id").and_then(|i| i.as_str()).unwrap_or("");
                                        let codex_name = item.get("name").and_then(|n| n.as_str()).unwrap_or("");
                                        if tool_registry.is_some() && call_id.is_empty() {
                                            yield Ok(anthropic_error_sse(
                                                "registered tool call requires a non-empty call_id",
                                                "tool_registry_violation",
                                            ));
                                            terminated = true;
                                            continue;
                                        }
                                        let name = match tool_registry.as_deref() {
                                            Some(registry) => match registry.claude_name_for_codex(codex_name) {
                                                Ok(name) => name.to_string(),
                                                Err(error) => {
                                                    yield Ok(anthropic_error_sse(
                                                        &error.to_string(),
                                                        "tool_registry_violation",
                                                    ));
                                                    terminated = true;
                                                    continue;
                                                }
                                            },
                                            None => codex_name.to_string(),
                                        };
                                        let index = if let Some(k) = tool_item_key_from_added(&data, item) {
                                            if let Some(existing) = index_by_key.get(&k).copied() {
                                                existing
                                            } else {
                                                let assigned = next_content_index;
                                                next_content_index += 1;
                                                index_by_key.insert(k, assigned);
                                                assigned
                                            }
                                        } else {
                                            let assigned = next_content_index;
                                            next_content_index += 1;
                                            assigned
                                        };
                                        if let Some(item_id) = item
                                            .get("id")
                                            .and_then(|v| v.as_str())
                                            .or_else(|| data.get("item_id").and_then(|v| v.as_str()))
                                        {
                                            tool_index_by_item_id.insert(item_id.to_string(), index);
                                        }
                                        if tool_registry.is_some() {
                                            if let Some((completed_name, completed_id, _)) =
                                                completed_registry_tools.get(&index)
                                            {
                                                if codex_name != completed_name
                                                    || call_id != completed_id
                                                {
                                                    yield Ok(anthropic_error_sse(
                                                        "duplicate registered tool start conflicts with the validated call",
                                                        "tool_registry_violation",
                                                    ));
                                                    terminated = true;
                                                } else {
                                                    duplicate_completed_registry_args
                                                        .entry(index)
                                                        .or_default();
                                                }
                                                continue;
                                            }
                                        }
                                        if tool_registry.is_some()
                                            && (tool_codex_name_by_index
                                                .get(&index)
                                                .is_some_and(|existing| existing != codex_name)
                                                || tool_call_id_by_index
                                                    .get(&index)
                                                    .is_some_and(|existing| existing != call_id))
                                        {
                                            yield Ok(anthropic_error_sse(
                                                "registered tool identity changed between stream events",
                                                "tool_registry_violation",
                                            ));
                                            terminated = true;
                                            continue;
                                        }
                                        tool_name_by_index.insert(index, name.clone());
                                        tool_codex_name_by_index.insert(index, codex_name.to_string());
                                        if !call_id.is_empty() {
                                            tool_call_id_by_index.insert(index, call_id.to_string());
                                        }
                                        last_tool_index = Some(index);

                                        if pending_registry_tools.contains(&index)
                                            || open_indices.contains(&index)
                                        {
                                            continue;
                                        }

                                        tool_args_by_index.insert(index, String::new());

                                        if tool_registry.is_some() {
                                            pending_registry_tools.insert(index);
                                            continue;
                                        }

                                        let event = json!({
                                            "type": "content_block_start",
                                            "index": index,
                                            "content_block": {
                                                "type": "tool_use",
                                                "id": call_id,
                                                "name": name
                                            }
                                        });
                                        let sse = format!("event: content_block_start\ndata: {}\n\n",
                                            serde_json::to_string(&event).unwrap_or_default());
                                        yield Ok(Bytes::from(sse));
                                        open_indices.insert(index);
                                    } else if item_type == "reasoning" {
                                        if !has_sent_message_start {
                                            let start_event = json!({
                                                "type": "message_start",
                                                "message": {
                                                    "id": message_id.clone().unwrap_or_default(),
                                                    "type": "message",
                                                    "role": "assistant",
                                                    "model": current_model.clone().unwrap_or_default(),
                                                    "usage": { "input_tokens": 0, "output_tokens": 0 }
                                                }
                                            });
                                            let sse = format!("event: message_start\ndata: {}\n\n",
                                                serde_json::to_string(&start_event).unwrap_or_default());
                                            yield Ok(Bytes::from(sse));
                                            has_sent_message_start = true;
                                        }

                                        let index = if let Some(key) = reasoning_item_key(&data, Some(item)) {
                                            if let Some(existing) = index_by_key.get(&key).copied() {
                                                existing
                                            } else {
                                                let assigned = next_content_index;
                                                next_content_index += 1;
                                                index_by_key.insert(key, assigned);
                                                assigned
                                            }
                                        } else {
                                            let assigned = next_content_index;
                                            next_content_index += 1;
                                            assigned
                                        };
                                        if let Some(item_id) = item
                                            .get("id")
                                            .and_then(Value::as_str)
                                            .or_else(|| data.get("item_id").and_then(Value::as_str))
                                        {
                                            reasoning_index_by_item_id.insert(item_id.to_string(), index);
                                        }
                                        reasoning_item_by_index.insert(index, item.clone());
                                        reasoning_text_by_index.entry(index).or_default();
                                    }
                                    // message type output_item.added is handled via content_part.added
                                }
                            }

                            // ================================================
                            // response.function_call_arguments.delta → content_block_delta (input_json_delta)
                            // ================================================
                            "response.function_call_arguments.delta" => {
                                if let Some(delta) = data.get("delta").and_then(|d| d.as_str()) {
                                    has_tool_use = true;
                                    let item_id = data.get("item_id").and_then(|v| v.as_str());
                                    let index = if let Some(id) = item_id {
                                        tool_index_by_item_id.get(id).copied()
                                    } else {
                                        None
                                    }
                                    .or_else(|| {
                                        tool_item_key_from_event(&data)
                                            .and_then(|k| index_by_key.get(&k).copied())
                                    })
                                    .or(last_tool_index)
                                    .unwrap_or_else(|| {
                                        let assigned = next_content_index;
                                        next_content_index += 1;
                                        assigned
                                    });

                                    if let Some(id) = item_id {
                                        tool_index_by_item_id.insert(id.to_string(), index);
                                    }
                                    if tool_registry.is_some() {
                                        if let Some((completed_name, completed_id, _)) =
                                            completed_registry_tools.get(&index)
                                        {
                                            let duplicate_name = data
                                                .get("name")
                                                .and_then(Value::as_str);
                                            let duplicate_id = data
                                                .get("call_id")
                                                .and_then(Value::as_str);
                                            if duplicate_name
                                                .is_some_and(|name| name != completed_name)
                                                || duplicate_id
                                                    .is_some_and(|id| id != completed_id)
                                            {
                                                yield Ok(anthropic_error_sse(
                                                    "duplicate registered tool arguments conflict with the validated call",
                                                    "tool_registry_violation",
                                                ));
                                                terminated = true;
                                            } else {
                                                duplicate_completed_registry_args
                                                    .entry(index)
                                                    .or_default()
                                                    .push_str(delta);
                                            }
                                            continue;
                                        }
                                    }
                                    if let Some(codex_name) = data.get("name").and_then(Value::as_str) {
                                        if tool_registry.is_some()
                                            && tool_codex_name_by_index
                                                .get(&index)
                                                .is_some_and(|existing| existing != codex_name)
                                        {
                                            yield Ok(anthropic_error_sse(
                                                "registered tool identity changed between stream events",
                                                "tool_registry_violation",
                                            ));
                                            terminated = true;
                                            continue;
                                        }
                                        let name = match tool_registry.as_deref() {
                                            Some(registry) => match registry.claude_name_for_codex(codex_name) {
                                                Ok(name) => name.to_string(),
                                                Err(error) => {
                                                    yield Ok(anthropic_error_sse(
                                                        &error.to_string(),
                                                        "tool_registry_violation",
                                                    ));
                                                    terminated = true;
                                                    continue;
                                                }
                                            },
                                            None => codex_name.to_string(),
                                        };
                                        tool_name_by_index.insert(index, name);
                                        tool_codex_name_by_index.insert(index, codex_name.to_string());
                                    } else {
                                        tool_name_by_index.entry(index).or_default();
                                    }
                                    if let Some(call_id) = data.get("call_id").and_then(Value::as_str) {
                                        if tool_registry.is_some()
                                            && tool_call_id_by_index
                                                .get(&index)
                                                .is_some_and(|existing| existing != call_id)
                                        {
                                            yield Ok(anthropic_error_sse(
                                                "registered tool identity changed between stream events",
                                                "tool_registry_violation",
                                            ));
                                            terminated = true;
                                            continue;
                                        }
                                        tool_call_id_by_index.insert(index, call_id.to_string());
                                    }
                                    last_tool_index = Some(index);

                                    tool_args_by_index
                                        .entry(index)
                                        .or_default()
                                        .push_str(delta);
                                    tool_had_delta.insert(index);

                                    if tool_registry.is_some() {
                                        let has_identity = tool_codex_name_by_index
                                            .get(&index)
                                            .is_some_and(|name| !name.is_empty());
                                        let has_call_id = tool_call_id_by_index
                                            .get(&index)
                                            .is_some_and(|id| !id.is_empty());
                                        if !has_identity || !has_call_id {
                                            yield Ok(anthropic_error_sse(
                                                "registered tool argument delta requires an explicit tool name and call_id",
                                                "tool_registry_violation",
                                            ));
                                            terminated = true;
                                            continue;
                                        }
                                        pending_registry_tools.insert(index);
                                        continue;
                                    }

                                    if !open_indices.contains(&index) {
                                        let name = tool_name_by_index
                                            .get(&index)
                                            .map(String::as_str)
                                            .unwrap_or("");
                                        if tool_registry.is_some() && name.is_empty() {
                                            yield Ok(anthropic_error_sse(
                                                "tool argument delta has no registered tool identity",
                                                "tool_registry_violation",
                                            ));
                                            terminated = true;
                                            continue;
                                        }
                                        let start_event = json!({
                                            "type": "content_block_start",
                                            "index": index,
                                            "content_block": {
                                                "type": "tool_use",
                                                "id": data
                                                    .get("call_id")
                                                    .and_then(|v| v.as_str())
                                                    .or(item_id)
                                                    .unwrap_or(""),
                                                "name": name
                                            }
                                        });
                                        let start_sse = format!("event: content_block_start\ndata: {}\n\n",
                                            serde_json::to_string(&start_event).unwrap_or_default());
                                        yield Ok(Bytes::from(start_sse));
                                        open_indices.insert(index);
                                    }

                                    if tool_registry.is_none()
                                        && tool_name_by_index.get(&index).map(String::as_str) == Some("Read")
                                    {
                                        if let Some(trace) = read_trace.as_ref() {
                                            let call = tool_trace_by_index
                                                .entry(index)
                                                .or_insert_with(|| trace.new_call());
                                            trace.upstream_fragment(
                                                call,
                                                event_name,
                                                None,
                                                data.get("output_index").and_then(Value::as_u64),
                                                tool_call_id_by_index
                                                    .get(&index)
                                                    .map(String::as_str)
                                                    .or_else(|| data.get("call_id").and_then(Value::as_str))
                                                    .or(item_id),
                                                "Read",
                                                delta,
                                            );
                                        }
                                    }

                                    if tool_registry.is_none()
                                        && tool_name_by_index.get(&index).map(String::as_str) == Some("Read")
                                    {
                                        continue;
                                    }

                                    let event = json!({
                                        "type": "content_block_delta",
                                        "index": index,
                                        "delta": {
                                            "type": "input_json_delta",
                                            "partial_json": delta
                                        }
                                    });
                                    let sse = format!("event: content_block_delta\ndata: {}\n\n",
                                        serde_json::to_string(&event).unwrap_or_default());
                                    yield Ok(Bytes::from(sse));
                                }
                            }

                            // ================================================
                            // response.function_call_arguments.done → content_block_stop
                            // ================================================
                            "response.function_call_arguments.done" => {
                                has_tool_use = true;
                                let item_id = data.get("item_id").and_then(|v| v.as_str());
                                let index = if let Some(id) = item_id {
                                    tool_index_by_item_id.get(id).copied()
                                } else {
                                    None
                                }
                                .or_else(|| {
                                    tool_item_key_from_event(&data)
                                        .and_then(|k| index_by_key.get(&k).copied())
                                })
                                .or(last_tool_index);
                                if let Some(index) = index {
                                    if let Some(registry) = tool_registry.as_deref() {
                                        if let Some((completed_name, completed_id, completed_raw)) =
                                            completed_registry_tools.get(&index)
                                        {
                                            let duplicate_raw = data
                                                .get("arguments")
                                                .or_else(|| data.pointer("/item/arguments"))
                                                .and_then(Value::as_str)
                                                .or_else(|| {
                                                    duplicate_completed_registry_args
                                                        .get(&index)
                                                        .map(String::as_str)
                                                })
                                                .unwrap_or("");
                                            let duplicate_name = data
                                                .get("name")
                                                .or_else(|| data.pointer("/item/name"))
                                                .and_then(Value::as_str);
                                            let duplicate_id = data
                                                .get("call_id")
                                                .or_else(|| data.pointer("/item/call_id"))
                                                .and_then(Value::as_str);
                                            if duplicate_raw != completed_raw
                                                || duplicate_name
                                                    .is_some_and(|name| name != completed_name)
                                                || duplicate_id
                                                    .is_some_and(|id| id != completed_id)
                                            {
                                                yield Ok(anthropic_error_sse(
                                                    "duplicate registered tool completion conflicts with the validated call",
                                                    "tool_registry_violation",
                                                ));
                                                terminated = true;
                                            }
                                            duplicate_completed_registry_args.remove(&index);
                                            continue;
                                        }
                                        if !pending_registry_tools.remove(&index) {
                                            yield Ok(anthropic_error_sse(
                                                "registered tool completion has no pending tool call",
                                                "tool_registry_violation",
                                            ));
                                            terminated = true;
                                            continue;
                                        }
                                        let raw = data
                                            .get("arguments")
                                            .or_else(|| data.pointer("/item/arguments"))
                                            .and_then(Value::as_str)
                                            .map(str::to_string)
                                            .unwrap_or_else(|| {
                                                tool_args_by_index
                                                    .get(&index)
                                                    .cloned()
                                                    .unwrap_or_default()
                                            });
                                        let codex_name = tool_codex_name_by_index
                                            .get(&index)
                                            .map(String::as_str)
                                            .unwrap_or("");
                                        let call_id = tool_call_id_by_index
                                            .get(&index)
                                            .map(String::as_str)
                                            .unwrap_or("");
                                        let completion_name = data
                                            .get("name")
                                            .or_else(|| data.pointer("/item/name"))
                                            .and_then(Value::as_str);
                                        let completion_call_id = data
                                            .get("call_id")
                                            .or_else(|| data.pointer("/item/call_id"))
                                            .and_then(Value::as_str);
                                        if completion_name.is_some_and(|name| name != codex_name)
                                            || completion_call_id.is_some_and(|id| id != call_id)
                                        {
                                            yield Ok(anthropic_error_sse(
                                                "registered tool identity changed between stream events",
                                                "tool_registry_violation",
                                            ));
                                            terminated = true;
                                            continue;
                                        }
                                        let call = match registry.restore_call(codex_name, call_id, &raw) {
                                            Ok(call) => call,
                                            Err(error) => {
                                                yield Ok(anthropic_error_sse(
                                                    &error.to_string(),
                                                    "tool_registry_violation",
                                                ));
                                                terminated = true;
                                                continue;
                                            }
                                        };
                                        if let Some(prepared) = prepared_turn.as_ref() {
                                            if let Err(error) = prepared.observe_returned_tool_call(
                                                codex_name,
                                                call_id,
                                                &raw,
                                            ) {
                                                yield Ok(anthropic_error_sse(
                                                    &error.to_string(),
                                                    "conversation_state_conflict",
                                                ));
                                                terminated = true;
                                                continue;
                                            }
                                        }
                                        yield Ok(anthropic_sse(
                                            "content_block_start",
                                            &json!({
                                                "type": "content_block_start",
                                                "index": index,
                                                "content_block": {
                                                    "type": "tool_use",
                                                    "id": call.tool_use_id,
                                                    "name": call.claude_name
                                                }
                                            }),
                                        ));
                                        yield Ok(anthropic_sse(
                                            "content_block_delta",
                                            &json!({
                                                "type": "content_block_delta",
                                                "index": index,
                                                "delta": {
                                                    "type": "input_json_delta",
                                                    "partial_json": raw
                                                }
                                            }),
                                        ));
                                        yield Ok(anthropic_sse(
                                            "content_block_stop",
                                            &json!({"type": "content_block_stop", "index": index}),
                                        ));
                                        completed_registry_tools.insert(
                                            index,
                                            (codex_name.to_string(), call_id.to_string(), raw.clone()),
                                        );
                                        if let Some(item_id) = item_id {
                                            tool_index_by_item_id.remove(item_id);
                                        }
                                        tool_name_by_index.remove(&index);
                                        tool_codex_name_by_index.remove(&index);
                                        tool_args_by_index.remove(&index);
                                        tool_had_delta.remove(&index);
                                        tool_call_id_by_index.remove(&index);
                                        continue;
                                    }
                                    if !open_indices.remove(&index) {
                                        continue;
                                    }
                                    if tool_registry.is_none()
                                        && tool_name_by_index.get(&index).map(String::as_str) == Some("Read")
                                    {
                                        let raw = data
                                            .get("arguments")
                                            .or_else(|| data.pointer("/item/arguments"))
                                            .and_then(|v| v.as_str())
                                            .map(str::to_string)
                                            .unwrap_or_else(|| {
                                                tool_args_by_index
                                                    .get(&index)
                                                    .cloned()
                                                    .unwrap_or_default()
                                            });
                                        let sanitized =
                                            sanitize_anthropic_tool_use_input_json_with_protection(
                                                "Read",
                                                &raw,
                                                read_offset_protection.as_ref(),
                                            );
                                        if let Some(trace) = read_trace.as_ref() {
                                            let call = tool_trace_by_index
                                                .entry(index)
                                                .or_insert_with(|| trace.new_call());
                                            trace.upstream_complete(
                                                call,
                                                event_name,
                                                None,
                                                data.get("output_index").and_then(Value::as_u64),
                                                tool_call_id_by_index
                                                    .get(&index)
                                                    .map(String::as_str)
                                                    .or(item_id),
                                                "Read",
                                                &raw,
                                                if data.get("arguments").is_some()
                                                    || data.pointer("/item/arguments").is_some()
                                                {
                                                    "done_arguments"
                                                } else {
                                                    "delta_buffer"
                                                },
                                            );
                                            let raw_offset = serde_json::from_str::<Value>(&raw)
                                                .ok()
                                                .and_then(|value| value.get("offset").cloned());
                                            let sanitized_value = serde_json::from_str::<Value>(&sanitized)
                                                .unwrap_or(Value::Null);
                                            trace.anthropic_emitted(
                                                call,
                                                tool_call_id_by_index
                                                    .get(&index)
                                                    .map(String::as_str)
                                                    .unwrap_or_default(),
                                                "Read",
                                                &sanitized_value,
                                                raw_offset != sanitized_value.get("offset").cloned(),
                                            );
                                        }
                                        if !sanitized.is_empty() {
                                            let event = json!({
                                                "type": "content_block_delta",
                                                "index": index,
                                                "delta": {
                                                    "type": "input_json_delta",
                                                    "partial_json": sanitized
                                                }
                                            });
                                            let sse = format!("event: content_block_delta\ndata: {}\n\n",
                                                serde_json::to_string(&event).unwrap_or_default());
                                            yield Ok(Bytes::from(sse));
                                        }
                                    } else if tool_registry.is_none()
                                        && !tool_had_delta.contains(&index)
                                    {
                                        // Some compatible gateways skip delta events and only
                                        // provide the complete arguments on the done event.
                                        if let Some(arguments) = data
                                            .get("arguments")
                                            .or_else(|| data.pointer("/item/arguments"))
                                            .and_then(Value::as_str)
                                            .filter(|value| !value.is_empty())
                                        {
                                            let event = json!({
                                                "type": "content_block_delta",
                                                "index": index,
                                                "delta": {
                                                    "type": "input_json_delta",
                                                    "partial_json": arguments
                                                }
                                            });
                                            let sse = format!("event: content_block_delta\ndata: {}\n\n",
                                                serde_json::to_string(&event).unwrap_or_default());
                                            yield Ok(Bytes::from(sse));
                                        }
                                    }
                                    let event = json!({
                                        "type": "content_block_stop",
                                        "index": index
                                    });
                                    let sse = format!("event: content_block_stop\ndata: {}\n\n",
                                        serde_json::to_string(&event).unwrap_or_default());
                                    yield Ok(Bytes::from(sse));
                                    if let Some(item_id) = item_id {
                                        tool_index_by_item_id.remove(item_id);
                                    }
                                    tool_name_by_index.remove(&index);
                                    tool_codex_name_by_index.remove(&index);
                                    tool_args_by_index.remove(&index);
                                    tool_had_delta.remove(&index);
                                    tool_trace_by_index.remove(&index);
                                    tool_call_id_by_index.remove(&index);
                                }
                            }

                            // ================================================
                            // response.refusal.done → content_block_stop
                            // ================================================
                            "response.refusal.done" => {
                                let index = current_text_index.take().or_else(|| {
                                    let key = content_part_key(&data);
                                    if let Some(k) = key {
                                        index_by_key.get(&k).copied()
                                    } else {
                                        fallback_open_index
                                    }
                                });
                                if let Some(index) = index {
                                    if !open_indices.remove(&index) {
                                        continue;
                                    }
                                    let event = json!({
                                        "type": "content_block_stop",
                                        "index": index
                                    });
                                    let sse = format!("event: content_block_stop\ndata: {}\n\n",
                                        serde_json::to_string(&event).unwrap_or_default());
                                    yield Ok(Bytes::from(sse));
                                    if fallback_open_index == Some(index) {
                                        fallback_open_index = None;
                                    }
                                }
                            }

                            // ================================================
                            // Official reasoning text events → thinking_delta.
                            // response.reasoning.delta is kept as a compatibility alias.
                            // ================================================
                            "response.reasoning_summary_text.delta"
                            | "response.reasoning_text.delta"
                            | "response.reasoning.delta" => {
                                if let Some(delta) = data
                                    .get("delta")
                                    .or_else(|| data.get("text"))
                                    .and_then(|d| d.as_str())
                                {
                                    if let Some(index) = current_text_index.take() {
                                        if open_indices.remove(&index) {
                                            let stop_event = json!({
                                                "type": "content_block_stop",
                                                "index": index
                                            });
                                            let stop_sse = format!("event: content_block_stop\ndata: {}\n\n",
                                                serde_json::to_string(&stop_event).unwrap_or_default());
                                            yield Ok(Bytes::from(stop_sse));
                                        }
                                        if fallback_open_index == Some(index) {
                                            fallback_open_index = None;
                                        }
                                    }
                                    let item_id = data.get("item_id").and_then(Value::as_str);
                                    let item_key = reasoning_item_key(&data, None);
                                    let is_keyless = item_id.is_none() && item_key.is_none();
                                    let index = item_id
                                        .and_then(|id| reasoning_index_by_item_id.get(id).copied())
                                        .or_else(|| {
                                            item_key
                                                .as_ref()
                                                .and_then(|key| index_by_key.get(key).copied())
                                        })
                                        .or_else(|| {
                                            is_keyless
                                                .then_some(legacy_reasoning_index)
                                                .flatten()
                                        })
                                        .unwrap_or_else(|| {
                                            let assigned = next_content_index;
                                            next_content_index += 1;
                                            if let Some(key) = item_key {
                                                index_by_key.insert(key, assigned);
                                            }
                                            if let Some(id) = item_id {
                                                reasoning_index_by_item_id
                                                    .insert(id.to_string(), assigned);
                                            } else if is_keyless {
                                                legacy_reasoning_index = Some(assigned);
                                            }
                                            assigned
                                        });

                                    if !open_indices.contains(&index) {
                                        let start_event = json!({
                                            "type": "content_block_start",
                                            "index": index,
                                            "content_block": {
                                                "type": "thinking",
                                                "thinking": ""
                                            }
                                        });
                                        let start_sse = format!("event: content_block_start\ndata: {}\n\n",
                                            serde_json::to_string(&start_event).unwrap_or_default());
                                        yield Ok(Bytes::from(start_sse));
                                        open_indices.insert(index);
                                    }

                                    reasoning_text_by_index
                                        .entry(index)
                                        .or_default()
                                        .push_str(delta);

                                    let event = json!({
                                        "type": "content_block_delta",
                                        "index": index,
                                        "delta": {
                                            "type": "thinking_delta",
                                            "thinking": delta
                                        }
                                    });
                                    let sse = format!("event: content_block_delta\ndata: {}\n\n",
                                        serde_json::to_string(&event).unwrap_or_default());
                                    yield Ok(Bytes::from(sse));
                                }
                            }

                            // ================================================
                            // Official done events carry the complete visible text. If a
                            // gateway omitted deltas, emit the text here. The block stays
                            // open until output_item.done supplies encrypted_content.
                            // ================================================
                            "response.reasoning_summary_text.done"
                            | "response.reasoning_text.done" => {
                                let item_id = data.get("item_id").and_then(Value::as_str);
                                let item_key = reasoning_item_key(&data, None);
                                let index = item_id
                                    .and_then(|id| reasoning_index_by_item_id.get(id).copied())
                                    .or_else(|| {
                                        item_key
                                            .as_ref()
                                            .and_then(|key| index_by_key.get(key).copied())
                                    })
                                    .or_else(|| {
                                        (item_id.is_none() && item_key.is_none())
                                            .then_some(legacy_reasoning_index)
                                            .flatten()
                                    });
                                if let Some(index) = index {
                                    let already_emitted = reasoning_text_by_index
                                        .get(&index)
                                        .is_some_and(|value| !value.is_empty());
                                    if !already_emitted {
                                        if let Some(text) = data
                                            .get("text")
                                            .and_then(Value::as_str)
                                            .filter(|value| !value.is_empty())
                                        {
                                            if !open_indices.contains(&index) {
                                                let start_event = json!({
                                                    "type": "content_block_start",
                                                    "index": index,
                                                    "content_block": {"type": "thinking", "thinking": ""}
                                                });
                                                let start_sse = format!("event: content_block_start\ndata: {}\n\n",
                                                    serde_json::to_string(&start_event).unwrap_or_default());
                                                yield Ok(Bytes::from(start_sse));
                                                open_indices.insert(index);
                                            }
                                            reasoning_text_by_index
                                                .entry(index)
                                                .or_default()
                                                .push_str(text);
                                            let event = json!({
                                                "type": "content_block_delta",
                                                "index": index,
                                                "delta": {"type": "thinking_delta", "thinking": text}
                                            });
                                            let sse = format!("event: content_block_delta\ndata: {}\n\n",
                                                serde_json::to_string(&event).unwrap_or_default());
                                            yield Ok(Bytes::from(sse));
                                        }
                                    }
                                }
                            }

                            // Legacy gateways do not emit output_item.done, so retain the
                            // old close behavior for their non-standard done event.
                            "response.reasoning.done" => {
                                let item_id = data.get("item_id").and_then(Value::as_str);
                                let item_key = reasoning_item_key(&data, None);
                                let index = item_id
                                    .and_then(|id| reasoning_index_by_item_id.get(id).copied())
                                    .or_else(|| {
                                        item_key
                                            .as_ref()
                                            .and_then(|key| index_by_key.get(key).copied())
                                    })
                                    .or_else(|| {
                                        (item_id.is_none() && item_key.is_none())
                                            .then_some(legacy_reasoning_index)
                                            .flatten()
                                    });
                                if let Some(index) = index {
                                    if open_indices.remove(&index) {
                                        let event = json!({"type": "content_block_stop", "index": index});
                                        let sse = format!("event: content_block_stop\ndata: {}\n\n",
                                            serde_json::to_string(&event).unwrap_or_default());
                                        yield Ok(Bytes::from(sse));
                                    }
                                    if legacy_reasoning_index == Some(index) {
                                        legacy_reasoning_index = None;
                                    }
                                }
                            }

                            // ================================================
                            // response.completed / response.incomplete → message_delta + message_stop
                            // ================================================
                            "response.completed" | "response.incomplete" => {
                                let response_obj = response_object_from_event(&data);
                                if matches!(
                                    response_obj.get("status").and_then(Value::as_str),
                                    Some("failed" | "cancelled")
                                ) || response_obj
                                    .get("error")
                                    .is_some_and(|error| !error.is_null())
                                {
                                    let (message, error_type) = responses_error_details(
                                        &data,
                                        "Responses upstream returned a failed terminal response",
                                    );
                                    yield Ok(anthropic_error_sse(&message, &error_type));
                                    terminated = true;
                                    continue;
                                }
                                if !pending_registry_tools.is_empty() {
                                    yield Ok(anthropic_error_sse(
                                        "registered tool call reached terminal response before validation",
                                        "tool_registry_violation",
                                    ));
                                    terminated = true;
                                    continue;
                                }
                                if !duplicate_completed_registry_args.is_empty() {
                                    let duplicate_conflict =
                                        duplicate_completed_registry_args.iter().any(
                                            |(index, arguments)| {
                                                completed_registry_tools
                                                    .get(index)
                                                    .is_none_or(|(_, _, completed)| {
                                                        completed != arguments
                                                    })
                                            },
                                        );
                                    duplicate_completed_registry_args.clear();
                                    if duplicate_conflict {
                                        yield Ok(anthropic_error_sse(
                                            "duplicate registered tool arguments did not match the validated call",
                                            "tool_registry_violation",
                                        ));
                                        terminated = true;
                                        continue;
                                    }
                                }
                                if !has_sent_message_start {
                                    if let Some(id) = response_obj.get("id").and_then(Value::as_str) {
                                        message_id = Some(id.to_string());
                                    }
                                    if let Some(model) =
                                        response_obj.get("model").and_then(Value::as_str)
                                    {
                                        current_model = Some(model.to_string());
                                    }
                                    yield Ok(anthropic_sse(
                                        "message_start",
                                        &json!({
                                            "type":"message_start",
                                            "message":{
                                                "id":message_id.clone().unwrap_or_default(),
                                                "type":"message",
                                                "role":"assistant",
                                                "model":current_model.clone().unwrap_or_default(),
                                                "usage":{"input_tokens":0,"output_tokens":0}
                                            }
                                        }),
                                    ));
                                    has_sent_message_start = true;
                                }
                                let terminal_status = response_obj
                                    .get("status")
                                    .and_then(Value::as_str)
                                    .or(match event_name {
                                        "response.incomplete" => Some("incomplete"),
                                        "response.completed" => Some("completed"),
                                        _ => None,
                                    });
                                let stop_reason = map_responses_stop_reason(
                                    terminal_status,
                                    has_tool_use,
                                    response_obj
                                        .pointer("/incomplete_details/reason")
                                        .and_then(|r| r.as_str()),
                                );

                                // Best effort: close any dangling blocks before message_delta/message_stop.
                                if !open_indices.is_empty() {
                                    let mut remaining: Vec<u32> = open_indices.iter().copied().collect();
                                    remaining.sort_unstable();
                                    for index in remaining {
                                        let stop_event = json!({
                                            "type": "content_block_stop",
                                            "index": index
                                        });
                                        let stop_sse = format!("event: content_block_stop\ndata: {}\n\n",
                                            serde_json::to_string(&stop_event).unwrap_or_default());
                                        yield Ok(Bytes::from(stop_sse));
                                        open_indices.remove(&index);
                                    }
                                }
                                fallback_open_index = None;

                                // Defensive: Always build usage_json, even if usage field missing
                                // Some() wrapper with fallback to {} ensures build_anthropic_usage_from_responses
                                // always receives valid input, preventing null pointer errors in VSCode Extension
                                let usage_json = build_anthropic_usage_from_responses(
                                    Some(response_obj.get("usage").unwrap_or(&json!({})))
                                );

                                // Emit message_delta (with usage + stop_reason)
                                let delta_event = json!({
                                    "type": "message_delta",
                                    "delta": {
                                        "stop_reason": stop_reason,
                                        "stop_sequence": null
                                    },
                                    "usage": usage_json
                                });
                                let sse = format!("event: message_delta\ndata: {}\n\n",
                                    serde_json::to_string(&delta_event).unwrap_or_default());
                                log::debug!("[Claude/Responses] >>> Anthropic SSE: message_delta");
                                yield Ok(Bytes::from(sse));

                                // Emit message_stop
                                let stop_event = json!({"type": "message_stop"});
                                let stop_sse = format!("event: message_stop\ndata: {}\n\n",
                                    serde_json::to_string(&stop_event).unwrap_or_default());
                                log::debug!("[Claude/Responses] >>> Anthropic SSE: message_stop");
                                yield Ok(Bytes::from(stop_sse));
                                terminated = true;
                            }

                            // ================================================
                            // Semantic failures can be carried inside an HTTP 2xx SSE.
                            // Preserve the upstream details instead of silently ending.
                            // ================================================
                            "response.failed" | "error" => {
                                let (message, error_type) = responses_error_details(
                                    &data,
                                    if event_name == "response.failed" {
                                        "Responses upstream reported response.failed"
                                    } else {
                                        "Responses upstream emitted an error event"
                                    },
                                );
                                yield Ok(anthropic_error_sse(&message, &error_type));
                                terminated = true;
                            }

                            // Lifecycle events that don't need Anthropic counterparts.
                            // Listed explicitly so new events trigger a match-completeness review.
                            "response.output_text.done" => {
                                if let Some(index) = current_text_index.take() {
                                    if open_indices.remove(&index) {
                                        let stop_event = json!({
                                            "type": "content_block_stop",
                                            "index": index
                                        });
                                        let stop_sse = format!("event: content_block_stop\ndata: {}\n\n",
                                            serde_json::to_string(&stop_event).unwrap_or_default());
                                        yield Ok(Bytes::from(stop_sse));
                                    }
                                    if fallback_open_index == Some(index) {
                                        fallback_open_index = None;
                                    }
                                }
                            }
                            "response.output_item.done" => {
                                let Some(item) = data.get("item") else {
                                    continue;
                                };
                                match item.get("type").and_then(Value::as_str) {
                                    Some("function_call") => {
                                        has_tool_use = true;
                                        let item_id = item
                                            .get("id")
                                            .and_then(Value::as_str)
                                            .or_else(|| data.get("item_id").and_then(Value::as_str));
                                        let index = item_id
                                            .and_then(|id| tool_index_by_item_id.get(id).copied())
                                            .or_else(|| {
                                                tool_item_key_from_event(&data)
                                                    .and_then(|key| index_by_key.get(&key).copied())
                                            })
                                            .or(last_tool_index);
                                        if let Some(index) = index {
                                            if let Some(registry) = tool_registry.as_deref() {
                                                if let Some((completed_name, completed_id, completed_raw)) =
                                                    completed_registry_tools.get(&index)
                                                {
                                                    let duplicate_raw = item
                                                        .get("arguments")
                                                        .and_then(Value::as_str)
                                                        .or_else(|| {
                                                            duplicate_completed_registry_args
                                                                .get(&index)
                                                                .map(String::as_str)
                                                        })
                                                        .unwrap_or("");
                                                    let duplicate_name = item
                                                        .get("name")
                                                        .and_then(Value::as_str);
                                                    let duplicate_id = item
                                                        .get("call_id")
                                                        .and_then(Value::as_str);
                                                    if duplicate_raw != completed_raw
                                                        || duplicate_name
                                                            .is_some_and(|name| name != completed_name)
                                                        || duplicate_id
                                                            .is_some_and(|id| id != completed_id)
                                                    {
                                                        yield Ok(anthropic_error_sse(
                                                            "duplicate registered tool completion conflicts with the validated call",
                                                            "tool_registry_violation",
                                                        ));
                                                        terminated = true;
                                                    }
                                                    duplicate_completed_registry_args
                                                        .remove(&index);
                                                    if let Some(id) = item_id {
                                                        tool_index_by_item_id.remove(id);
                                                    }
                                                    continue;
                                                }
                                                if !pending_registry_tools.remove(&index) {
                                                    yield Ok(anthropic_error_sse(
                                                        "registered output item has no pending tool call",
                                                        "tool_registry_violation",
                                                    ));
                                                    terminated = true;
                                                    continue;
                                                }
                                                let raw = item
                                                    .get("arguments")
                                                    .and_then(Value::as_str)
                                                    .filter(|value| !value.is_empty())
                                                    .map(str::to_string)
                                                    .unwrap_or_else(|| {
                                                        tool_args_by_index
                                                            .get(&index)
                                                            .cloned()
                                                            .unwrap_or_default()
                                                    });
                                                let codex_name = tool_codex_name_by_index
                                                    .get(&index)
                                                    .map(String::as_str)
                                                    .unwrap_or("");
                                                let call_id = tool_call_id_by_index
                                                    .get(&index)
                                                    .map(String::as_str)
                                                    .unwrap_or("");
                                                if item
                                                    .get("name")
                                                    .and_then(Value::as_str)
                                                    .is_some_and(|name| name != codex_name)
                                                    || item
                                                        .get("call_id")
                                                        .and_then(Value::as_str)
                                                        .is_some_and(|id| id != call_id)
                                                {
                                                    yield Ok(anthropic_error_sse(
                                                        "registered tool identity changed between stream events",
                                                        "tool_registry_violation",
                                                    ));
                                                    terminated = true;
                                                    continue;
                                                }
                                                let call = match registry.restore_call(codex_name, call_id, &raw) {
                                                    Ok(call) => call,
                                                    Err(error) => {
                                                        yield Ok(anthropic_error_sse(
                                                            &error.to_string(),
                                                            "tool_registry_violation",
                                                        ));
                                                        terminated = true;
                                                        continue;
                                                    }
                                                };
                                                if let Some(prepared) = prepared_turn.as_ref() {
                                                    if let Err(error) = prepared.observe_returned_tool_call(
                                                        codex_name,
                                                        call_id,
                                                        &raw,
                                                    ) {
                                                        yield Ok(anthropic_error_sse(
                                                            &error.to_string(),
                                                            "conversation_state_conflict",
                                                        ));
                                                        terminated = true;
                                                        continue;
                                                    }
                                                }
                                                yield Ok(anthropic_sse(
                                                    "content_block_start",
                                                    &json!({
                                                        "type": "content_block_start",
                                                        "index": index,
                                                        "content_block": {
                                                            "type": "tool_use",
                                                            "id": call.tool_use_id,
                                                            "name": call.claude_name
                                                        }
                                                    }),
                                                ));
                                                yield Ok(anthropic_sse(
                                                    "content_block_delta",
                                                    &json!({
                                                        "type": "content_block_delta",
                                                        "index": index,
                                                        "delta": {
                                                            "type": "input_json_delta",
                                                            "partial_json": raw
                                                        }
                                                    }),
                                                ));
                                                yield Ok(anthropic_sse(
                                                    "content_block_stop",
                                                    &json!({"type": "content_block_stop", "index": index}),
                                                ));
                                                completed_registry_tools.insert(
                                                    index,
                                                    (
                                                        codex_name.to_string(),
                                                        call_id.to_string(),
                                                        raw.clone(),
                                                    ),
                                                );
                                                if let Some(id) = item_id {
                                                    tool_index_by_item_id.remove(id);
                                                }
                                                tool_name_by_index.remove(&index);
                                                tool_codex_name_by_index.remove(&index);
                                                tool_args_by_index.remove(&index);
                                                tool_had_delta.remove(&index);
                                                tool_call_id_by_index.remove(&index);
                                                continue;
                                            }
                                            if !open_indices.contains(&index) {
                                                continue;
                                            }
                                            let name = tool_name_by_index
                                                .get(&index)
                                                .map(String::as_str)
                                                .unwrap_or("");
                                            if !tool_had_delta.contains(&index) || name == "Read" {
                                                let raw = item
                                                    .get("arguments")
                                                    .and_then(Value::as_str)
                                                    .filter(|value| !value.is_empty())
                                                    .map(str::to_string)
                                                    .unwrap_or_else(|| {
                                                        tool_args_by_index
                                                            .get(&index)
                                                            .cloned()
                                                            .unwrap_or_default()
                                                    });
                                                let arguments = if name == "Read" {
                                                    let sanitized = sanitize_anthropic_tool_use_input_json_with_protection(
                                                        name,
                                                        &raw,
                                                        read_offset_protection.as_ref(),
                                                    );
                                                    if let Some(trace) = read_trace.as_ref() {
                                                        let call = tool_trace_by_index
                                                            .entry(index)
                                                            .or_insert_with(|| trace.new_call());
                                                        trace.upstream_complete(
                                                            call,
                                                            event_name,
                                                            None,
                                                            data.get("output_index").and_then(Value::as_u64),
                                                            tool_call_id_by_index
                                                                .get(&index)
                                                                .map(String::as_str)
                                                                .or(item_id),
                                                            "Read",
                                                            &raw,
                                                            if item.get("arguments").is_some() {
                                                                "output_item_arguments"
                                                            } else {
                                                                "delta_buffer"
                                                            },
                                                        );
                                                        let raw_offset = serde_json::from_str::<Value>(&raw)
                                                            .ok()
                                                            .and_then(|value| value.get("offset").cloned());
                                                        let sanitized_value = serde_json::from_str::<Value>(&sanitized)
                                                            .unwrap_or(Value::Null);
                                                        trace.anthropic_emitted(
                                                            call,
                                                            tool_call_id_by_index
                                                                .get(&index)
                                                                .map(String::as_str)
                                                                .unwrap_or_default(),
                                                            "Read",
                                                            &sanitized_value,
                                                            raw_offset != sanitized_value.get("offset").cloned(),
                                                        );
                                                    }
                                                    sanitized
                                                } else {
                                                    raw
                                                };
                                                if !arguments.is_empty() {
                                                    let event = json!({
                                                        "type": "content_block_delta",
                                                        "index": index,
                                                        "delta": {
                                                            "type": "input_json_delta",
                                                            "partial_json": arguments
                                                        }
                                                    });
                                                    let sse = format!("event: content_block_delta\ndata: {}\n\n",
                                                        serde_json::to_string(&event).unwrap_or_default());
                                                    yield Ok(Bytes::from(sse));
                                                }
                                            }
                                            open_indices.remove(&index);
                                            let event = json!({"type": "content_block_stop", "index": index});
                                            let sse = format!("event: content_block_stop\ndata: {}\n\n",
                                                serde_json::to_string(&event).unwrap_or_default());
                                            yield Ok(Bytes::from(sse));
                                            if let Some(id) = item_id {
                                                tool_index_by_item_id.remove(id);
                                            }
                                            tool_name_by_index.remove(&index);
                                            tool_codex_name_by_index.remove(&index);
                                            tool_args_by_index.remove(&index);
                                            tool_had_delta.remove(&index);
                                        }
                                    }
                                    Some("reasoning") => {
                                        if let Some(prepared) = prepared_turn.as_ref() {
                                            if let Err(error) = prepared.observe_reasoning_item(item) {
                                                yield Ok(anthropic_error_sse(
                                                    &error.to_string(),
                                                    "conversation_state_conflict",
                                                ));
                                                terminated = true;
                                                continue;
                                            }
                                        }
                                        let item_id = item
                                            .get("id")
                                            .and_then(Value::as_str)
                                            .or_else(|| data.get("item_id").and_then(Value::as_str));
                                        let index = item_id
                                            .and_then(|id| reasoning_index_by_item_id.get(id).copied())
                                            .or_else(|| {
                                                reasoning_item_key(&data, Some(item))
                                                    .and_then(|key| index_by_key.get(&key).copied())
                                            })
                                            .unwrap_or_else(|| {
                                                let assigned = next_content_index;
                                                next_content_index += 1;
                                                assigned
                                            });
                                        reasoning_item_by_index.insert(index, item.clone());

                                        let final_item = reasoning_item_by_index
                                            .get(&index)
                                            .cloned()
                                            .unwrap_or_else(|| item.clone());
                                        let anthropic_block =
                                            anthropic_block_from_openai_reasoning_item(&final_item);
                                        let full_text = anthropic_block
                                            .as_ref()
                                            .and_then(|block| block.get("thinking"))
                                            .and_then(Value::as_str)
                                            .unwrap_or("");
                                        let emitted_text = reasoning_text_by_index
                                            .get(&index)
                                            .cloned()
                                            .unwrap_or_default();
                                        if emitted_text.is_empty() && !full_text.is_empty() {
                                            let start_event = json!({
                                                "type": "content_block_start",
                                                "index": index,
                                                "content_block": {"type": "thinking", "thinking": ""}
                                            });
                                            let start_sse = format!("event: content_block_start\ndata: {}\n\n",
                                                serde_json::to_string(&start_event).unwrap_or_default());
                                            yield Ok(Bytes::from(start_sse));
                                            open_indices.insert(index);
                                            let delta_event = json!({
                                                "type": "content_block_delta",
                                                "index": index,
                                                "delta": {"type": "thinking_delta", "thinking": full_text}
                                            });
                                            let delta_sse = format!("event: content_block_delta\ndata: {}\n\n",
                                                serde_json::to_string(&delta_event).unwrap_or_default());
                                            yield Ok(Bytes::from(delta_sse));
                                        }

                                        if let Some(signature) = anthropic_block
                                            .as_ref()
                                            .and_then(|block| block.get("signature"))
                                            .and_then(Value::as_str)
                                        {
                                            if !open_indices.contains(&index) {
                                                let start_event = json!({
                                                    "type": "content_block_start",
                                                    "index": index,
                                                    "content_block": {"type": "thinking", "thinking": ""}
                                                });
                                                let start_sse = format!("event: content_block_start\ndata: {}\n\n",
                                                    serde_json::to_string(&start_event).unwrap_or_default());
                                                yield Ok(Bytes::from(start_sse));
                                                open_indices.insert(index);
                                            }
                                            let signature_event = json!({
                                                "type": "content_block_delta",
                                                "index": index,
                                                "delta": {
                                                    "type": "signature_delta",
                                                    "signature": signature
                                                }
                                            });
                                            let signature_sse = format!("event: content_block_delta\ndata: {}\n\n",
                                                serde_json::to_string(&signature_event).unwrap_or_default());
                                            yield Ok(Bytes::from(signature_sse));
                                        }
                                        if open_indices.remove(&index) {
                                            let stop_event = json!({"type": "content_block_stop", "index": index});
                                            let stop_sse = format!("event: content_block_stop\ndata: {}\n\n",
                                                serde_json::to_string(&stop_event).unwrap_or_default());
                                            yield Ok(Bytes::from(stop_sse));
                                        }
                                        if let Some(id) = item_id {
                                            reasoning_index_by_item_id.remove(id);
                                        }
                                        reasoning_item_by_index.remove(&index);
                                        reasoning_text_by_index.remove(&index);
                                    }
                                    _ => {}
                                }
                            }
                            "response.reasoning_summary_part.added"
                            | "response.reasoning_summary_part.done"
                            | "response.in_progress" => {}

                            // Any other unknown/future events — silently skip.
                            _ => {}
                        }
                    }
                }
                Err(e) => {
                    log::error!("Responses stream error: {e}");
                    let error_event = json!({
                        "type": "error",
                        "error": {
                            "type": "stream_error",
                            "message": format!("Stream error: {e}")
                        }
                    });
                    let sse = format!("event: error\ndata: {}\n\n",
                        serde_json::to_string(&error_event).unwrap_or_default());
                    yield Ok(Bytes::from(sse));
                    terminated = true;
                    break;
                }
            }
        }

        if !terminated && !duplicate_completed_registry_args.is_empty() {
            let duplicate_conflict = duplicate_completed_registry_args.iter().any(
                |(index, arguments)| {
                    completed_registry_tools
                        .get(index)
                        .is_none_or(|(_, _, completed)| completed != arguments)
                },
            );
            duplicate_completed_registry_args.clear();
            if duplicate_conflict {
                yield Ok(anthropic_error_sse(
                    "duplicate registered tool arguments did not match the validated call",
                    "tool_registry_violation",
                ));
                terminated = true;
            }
        }

        if !terminated {
            let has_open_tool = !pending_registry_tools.is_empty()
                || open_indices.iter().any(|index| {
                    tool_name_by_index.contains_key(index) || tool_args_by_index.contains_key(index)
                });
            let has_open_reasoning = open_indices.iter().any(|index| {
                reasoning_item_by_index.contains_key(index)
                    || reasoning_text_by_index.contains_key(index)
                    || legacy_reasoning_index == Some(*index)
            });

            if has_substantive_output && !has_open_tool && !has_open_reasoning {
                // Text-only partial output is safe to expose as a max-token style
                // incomplete turn. Close blocks before the terminal events.
                let mut remaining: Vec<u32> = open_indices.iter().copied().collect();
                remaining.sort_unstable();
                for index in remaining {
                    yield Ok(anthropic_sse(
                        "content_block_stop",
                        &json!({"type":"content_block_stop","index":index}),
                    ));
                }
                if !has_sent_message_start {
                    yield Ok(anthropic_sse(
                        "message_start",
                        &json!({
                            "type":"message_start",
                            "message":{
                                "id":message_id.clone().unwrap_or_default(),
                                "type":"message",
                                "role":"assistant",
                                "model":current_model.clone().unwrap_or_default(),
                                "usage":{"input_tokens":0,"output_tokens":0}
                            }
                        }),
                    ));
                }
                yield Ok(anthropic_sse(
                    "message_delta",
                    &json!({
                        "type":"message_delta",
                        "delta":{"stop_reason":"max_tokens","stop_sequence":null},
                        "usage":{"input_tokens":0,"output_tokens":0}
                    }),
                ));
                yield Ok(anthropic_sse("message_stop", &json!({"type":"message_stop"})));
            } else {
                // A truncated tool/reasoning block cannot be safely finalized: tool
                // JSON may be partial and thinking may be missing its signature.
                yield Ok(anthropic_error_sse(
                    "Responses upstream stream ended before a terminal event",
                    "stream_truncated",
                ));
            }
        }
    }
}

type SharedForensicObserver = Arc<Mutex<Option<ForensicStreamObserver>>>;

fn observe_upstream_stream<E: std::error::Error + Send + 'static>(
    stream: impl Stream<Item = Result<Bytes, E>> + Send + 'static,
    observer: SharedForensicObserver,
) -> impl Stream<Item = Result<Bytes, std::io::Error>> + Send {
    async_stream::stream! {
        let mut stream = Box::pin(stream);
        let mut buffer = String::new();
        let mut utf8_remainder = Vec::new();
        while let Some(item) = stream.next().await {
            match item {
                Ok(bytes) => {
                    observe_protocol_bytes(
                        &observer,
                        &mut buffer,
                        &mut utf8_remainder,
                        &bytes,
                        false,
                        true,
                    );
                    yield Ok(bytes);
                }
                Err(error) => {
                    mark_observer_stream_error(&observer);
                    yield Err(std::io::Error::other(error.to_string()));
                    break;
                }
            }
        }
        observe_protocol_bytes(
            &observer,
            &mut buffer,
            &mut utf8_remainder,
            &[],
            true,
            true,
        );
    }
}

fn observe_claude_stream(
    stream: impl Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static,
    observer: SharedForensicObserver,
) -> impl Stream<Item = Result<Bytes, std::io::Error>> + Send {
    async_stream::stream! {
        let mut stream = Box::pin(stream);
        let mut buffer = String::new();
        let mut utf8_remainder = Vec::new();
        while let Some(item) = stream.next().await {
            match &item {
                Ok(bytes) => observe_protocol_bytes(
                    &observer,
                    &mut buffer,
                    &mut utf8_remainder,
                    bytes,
                    false,
                    false,
                ),
                Err(_) => mark_observer_stream_error(&observer),
            }
            yield item;
        }
        observe_protocol_bytes(
            &observer,
            &mut buffer,
            &mut utf8_remainder,
            &[],
            true,
            false,
        );

        let evidence = observer.lock().ok().and_then(|mut guard| guard.take());
        if let Some(evidence) = evidence {
            match evidence.finish(None) {
                Ok(Some(bundle)) => log::error!(
                    "[BridgeEvidence] bundle_id={} stage=stream_transform summary=codex_stream_failed",
                    bundle.bundle_id.0
                ),
                Ok(None) => {}
                Err(error) => log::warn!(
                    "[BridgeEvidence] capture_failed stage=stream_transform error={error}"
                ),
            }
        }
    }
}

fn observe_protocol_bytes(
    observer: &SharedForensicObserver,
    buffer: &mut String,
    utf8_remainder: &mut Vec<u8>,
    bytes: &[u8],
    eof: bool,
    upstream: bool,
) {
    crate::proxy::sse::append_utf8_safe(buffer, utf8_remainder, bytes);
    let trimmed = buffer.trim_start_matches(|ch: char| ch.is_whitespace() || ch == '\u{feff}');
    let looks_like_json = matches!(trimmed.as_bytes().first(), Some(b'{') | Some(b'['));
    if looks_like_json {
        if eof {
            if let Ok(value) = serde_json::from_str::<Value>(buffer.trim()) {
                offer_observer_event(observer, &value, upstream);
            } else {
                mark_observer_stream_error(observer);
            }
            buffer.clear();
        }
        return;
    }
    if eof && !buffer.trim().is_empty() {
        buffer.push_str("\n\n");
    }
    while let Some(block) = take_sse_block(buffer) {
        let mut data_parts = Vec::new();
        for line in block.lines() {
            if let Some(data) = strip_sse_field(line, "data") {
                data_parts.push(data.to_string());
            }
        }
        if data_parts.is_empty() {
            continue;
        }
        if let Ok(value) = serde_json::from_str::<Value>(&data_parts.join("\n")) {
            offer_observer_event(observer, &value, upstream);
        }
    }
}

fn offer_observer_event(observer: &SharedForensicObserver, event: &Value, upstream: bool) {
    let Ok(mut guard) = observer.lock() else {
        return;
    };
    let Some(evidence) = guard.as_mut() else {
        return;
    };
    let result = if upstream {
        evidence.upstream_event(event)
    } else {
        evidence.claude_event(event)
    };
    if let Err(error) = result {
        log::warn!("[BridgeEvidence] capture_failed stage=stream_event error={error}");
        *guard = None;
    }
}

fn mark_observer_stream_error(observer: &SharedForensicObserver) {
    if let Ok(mut guard) = observer.lock() {
        if let Some(evidence) = guard.as_mut() {
            evidence.mark_stream_error();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        app_config::AppType,
        provider::{Provider, ProviderMeta},
        proxy::claude_codex_bridge::{
            ClaudeCodexBridge, CodexOAuthCapabilities, ConversationLedger, ToolCallState,
            ToolRegistry, TurnBinding,
        },
    };
    use futures::stream;
    use futures::StreamExt;
    use std::collections::HashMap;

    async fn convert_stream_text(input: impl Into<Bytes>) -> String {
        let upstream = stream::iter(vec![Ok::<_, std::io::Error>(input.into())]);
        create_anthropic_sse_stream_from_responses(upstream)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|chunk| String::from_utf8_lossy(chunk.unwrap().as_ref()).to_string())
            .collect()
    }

    fn read_registry() -> Arc<ToolRegistry> {
        let (registry, _) = ToolRegistry::compile(
            &[json!({
                "name": "Read",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "file_path": {"type": "string"},
                        "pages": {"type": "string"},
                        "offset": {"type": "number"}
                    },
                    "required": ["file_path"],
                    "additionalProperties": false
                }
            })],
            CodexOAuthCapabilities::builtin().as_ref(),
        )
        .unwrap();
        Arc::new(registry)
    }

    async fn convert_stream_with_registry(input: &str) -> String {
        let upstream = stream::iter(vec![Ok::<_, std::io::Error>(Bytes::copy_from_slice(
            input.as_bytes(),
        ))]);
        create_anthropic_sse_stream_from_responses_with_registry(
            upstream,
            None,
            None,
            read_registry(),
        )
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|chunk| String::from_utf8_lossy(chunk.unwrap().as_ref()).to_string())
        .collect()
    }

    #[tokio::test]
    async fn prepared_stream_marks_validated_tool_call_returned_to_claude() {
        let provider = Provider {
            id: "stream-ledger".to_string(),
            name: "Stream Ledger".to_string(),
            settings_config: json!({}),
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
        };
        let bridge = ClaudeCodexBridge::with_ledger(ConversationLedger::default());
        let prepared = bridge
            .prepare_turn_with_session_identity(
                &AppType::Claude,
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
                &provider,
                "session-1",
                Some("session-1"),
            )
            .unwrap();
        let binding = prepared.ledger_binding().clone();
        let input = concat!(
            "event: response.created\n",
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp-1\",\"model\":\"gpt-test\"}}\n\n",
            "event: response.output_item.added\n",
            "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"id\":\"item-1\",\"type\":\"function_call\",\"call_id\":\"call-1\",\"name\":\"read_file\"}}\n\n",
            "event: response.function_call_arguments.done\n",
            "data: {\"type\":\"response.function_call_arguments.done\",\"item_id\":\"item-1\",\"output_index\":0,\"arguments\":\"{\\\"file_path\\\":\\\"src/main.rs\\\"}\"}\n\n",
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n"
        );
        let chunks = create_anthropic_sse_stream_from_responses_with_prepared_turn(
            stream::iter(vec![Ok::<_, std::io::Error>(Bytes::from(input))]),
            None,
            None,
            prepared,
        )
        .collect::<Vec<_>>()
        .await;

        assert!(chunks.iter().all(Result::is_ok));
        assert_eq!(
            bridge.ledger().call_state(&binding, "call-1"),
            Some(ToolCallState::ReturnedToClaude)
        );
    }

    fn strict_prepared_turn() -> PreparedCodexTurn {
        let provider = Provider {
            id: "strict-adapter".to_string(),
            name: "Strict Adapter".to_string(),
            settings_config: json!({}),
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
        };
        ClaudeCodexBridge::with_ledger(ConversationLedger::default())
            .prepare_turn(
                &AppType::Claude,
                json!({
                    "model":"gpt-5.6",
                    "max_tokens":64,
                    "messages":[{"role":"user","content":"strict fixture"}]
                }),
                &provider,
                Some("strict-adapter-session"),
            )
            .unwrap()
    }

    fn strict_prepared_tool_turn() -> (ClaudeCodexBridge, PreparedCodexTurn, TurnBinding, String) {
        let provider = Provider {
            id: "strict-tool-adapter".to_string(),
            name: "Strict Tool Adapter".to_string(),
            settings_config: json!({}),
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
        };
        let bridge = ClaudeCodexBridge::with_ledger(ConversationLedger::default());
        let prepared = bridge
            .prepare_turn(
                &AppType::Claude,
                json!({
                    "model":"gpt-5.6",
                    "max_tokens":64,
                    "messages":[{"role":"user","content":"read a file"}],
                    "tools":[{
                        "name":"Read",
                        "input_schema":{
                            "type":"object",
                            "properties":{"file_path":{"type":"string"}},
                            "required":["file_path"],
                            "additionalProperties":false
                        }
                    }]
                }),
                &provider,
                Some("strict-tool-adapter-session"),
            )
            .unwrap();
        let codex_name = prepared
            .tool_registry
            .codex_name_for_claude("Read")
            .unwrap()
            .to_string();
        let binding = prepared.ledger_binding().clone();
        (bridge, prepared, binding, codex_name)
    }

    #[tokio::test]
    async fn strict_streaming_responses_emits_only_validated_claude_shape() {
        let input = concat!(
            "event: response.created\n",
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\",\"model\":\"gpt-5.6\",\"usage\":{\"input_tokens\":2,\"output_tokens\":0}}}\n\n",
            "event: response.output_item.added\n",
            "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"id\":\"msg_1\",\"type\":\"message\"}}\n\n",
            "event: response.content_part.added\n",
            "data: {\"type\":\"response.content_part.added\",\"item_id\":\"msg_1\",\"part\":{\"type\":\"output_text\",\"text\":\"\"}}\n\n",
            "event: response.output_text.delta\n",
            "data: {\"type\":\"response.output_text.delta\",\"item_id\":\"msg_1\",\"sequence_number\":4,\"delta\":\"hello\"}\n\n",
            "event: response.output_text.done\n",
            "data: {\"type\":\"response.output_text.done\",\"item_id\":\"msg_1\",\"text\":\"hello\"}\n\n",
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":2,\"output_tokens\":1}}}\n\n"
        );
        let merged = create_anthropic_sse_stream_from_responses_with_prepared_turn(
            stream::iter(vec![Ok::<_, std::io::Error>(Bytes::from(input))]),
            None,
            None,
            strict_prepared_turn(),
        )
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|chunk| String::from_utf8_lossy(chunk.unwrap().as_ref()).to_string())
        .collect::<String>();

        let event_names = merged
            .split("\n\n")
            .filter_map(|block| block.lines().find_map(|line| line.strip_prefix("event: ")))
            .collect::<Vec<_>>();
        assert_eq!(
            event_names,
            vec![
                "message_start",
                "content_block_start",
                "content_block_delta",
                "content_block_stop",
                "message_delta",
                "message_stop"
            ]
        );
        assert!(merged.contains("\"text\":\"hello\""));
        assert!(merged.contains("\"stop_reason\":\"end_turn\""));
    }

    #[tokio::test]
    async fn strict_streaming_responses_fails_closed_for_unknown_or_incomplete_stream() {
        for input in [
            concat!(
                "event: response.created\n",
                "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\",\"model\":\"gpt-5.6\"}}\n\n",
                "event: response.future_semantic.delta\n",
                "data: {\"type\":\"response.future_semantic.delta\",\"delta\":\"secret-sentinel\"}\n\n",
                "event: response.output_item.added\n",
                "data: {\"type\":\"response.output_item.added\",\"item\":{\"id\":\"msg_1\",\"type\":\"message\"}}\n\n",
                "event: response.content_part.added\n",
                "data: {\"type\":\"response.content_part.added\",\"item_id\":\"msg_1\",\"part\":{\"type\":\"output_text\"}}\n\n",
                "event: response.output_text.delta\n",
                "data: {\"type\":\"response.output_text.delta\",\"item_id\":\"msg_1\",\"delta\":\"late\"}\n\n",
                "event: response.output_text.done\n",
                "data: {\"type\":\"response.output_text.done\",\"item_id\":\"msg_1\"}\n\n",
                "event: response.completed\n",
                "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n"
            ),
            concat!(
                "event: response.created\n",
                "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\",\"model\":\"gpt-5.6\"}}\n\n"
            ),
            concat!(
                "event: response.output_text.delta\n",
                "data: {\"type\":\"response.output_text.delta\",\"item_id\":\"msg_1\",\"delta\":\"no start\"}\n\n",
                "event: response.output_text.done\n",
                "data: {\"type\":\"response.output_text.done\",\"item_id\":\"msg_1\"}\n\n",
                "event: response.completed\n",
                "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n"
            ),
        ] {
            let merged = create_anthropic_sse_stream_from_responses_with_prepared_turn(
                stream::iter(vec![Ok::<_, std::io::Error>(Bytes::from(input))]),
                None,
                None,
                strict_prepared_turn(),
            )
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|chunk| String::from_utf8_lossy(chunk.unwrap().as_ref()).to_string())
            .collect::<String>();
            assert!(merged.contains("event: error"));
            assert!(!merged.contains("secret-sentinel"));
            assert!(!merged.contains("event: message_stop"));
        }
    }

    #[tokio::test]
    async fn strict_stream_forensics_redacts_visible_tool_failure_and_disables_retry() {
        use crate::proxy::bridge_forensics::{BridgeForensicStore, EvidenceManifest};

        let secret = "sentinel-tool-arguments-plaintext";
        let provider = Provider {
            id: "strict-forensics".to_string(),
            name: "Strict Forensics".to_string(),
            settings_config: json!({}),
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
        };
        let prepared = ClaudeCodexBridge::with_ledger(ConversationLedger::default())
            .prepare_turn(
                &AppType::Claude,
                json!({
                    "model":"gpt-5.6",
                    "max_tokens":64,
                    "messages":[{"role":"user","content":"fixture"}],
                    "tools":[{
                        "name":"lookup",
                        "input_schema":{
                            "type":"object",
                            "properties":{"q":{"type":"string"}},
                            "required":["q"],
                            "additionalProperties":false
                        }
                    }]
                }),
                &provider,
                Some("strict-forensics-session"),
            )
            .unwrap();
        let codex_name = prepared
            .tool_registry
            .codex_name_for_claude("lookup")
            .unwrap()
            .to_string();
        let input = format!(
            concat!(
                "event: response.created\n",
                "data: {{\"type\":\"response.created\",\"response\":{{\"id\":\"resp_1\",\"model\":\"gpt-5.6\"}}}}\n\n",
                "event: response.output_item.added\n",
                "data: {{\"type\":\"response.output_item.added\",\"item\":{{\"id\":\"fc_1\",\"type\":\"function_call\",\"call_id\":\"call_1\",\"name\":{codex_name:?}}}}}\n\n",
                "event: response.function_call_arguments.done\n",
                "data: {{\"type\":\"response.function_call_arguments.done\",\"item_id\":\"fc_1\",\"arguments\":\"{{\\\"q\\\":\\\"{secret}\\\"}}\"}}\n\n",
                "event: response.future_semantic.delta\n",
                "data: {{\"type\":\"response.future_semantic.delta\",\"delta\":\"{secret}\"}}\n\n"
            ),
            codex_name = codex_name,
            secret = secret,
        );
        let temp = tempfile::tempdir().unwrap();
        let store = BridgeForensicStore::new(temp.path().join("evidence"));
        let observer = evidence_capture(&store);
        let _ = create_anthropic_sse_stream_from_responses_with_prepared_turn_and_evidence(
            stream::iter(vec![Ok::<_, std::io::Error>(Bytes::from(input))]),
            None,
            None,
            prepared,
            Some(observer),
        )
        .collect::<Vec<_>>()
        .await;

        let bundles = store.list_bundles().unwrap();
        assert_eq!(bundles.len(), 1);
        let bundle_path = temp
            .path()
            .join("evidence")
            .join("bundles")
            .join(&bundles[0].bundle_id.0);
        let manifest: EvidenceManifest =
            serde_json::from_slice(&std::fs::read(bundle_path.join("manifest.json")).unwrap())
                .unwrap();
        assert!(!manifest.error.retryable);
        let context = manifest.error.streaming.as_ref().unwrap();
        assert!(context.output_already_emitted);
        assert!(context.tool_visible);

        for entry in std::fs::read_dir(bundle_path).unwrap() {
            let path = entry.unwrap().path();
            if path.is_file() {
                let bytes = std::fs::read(path).unwrap();
                assert!(!String::from_utf8_lossy(&bytes).contains(secret));
            }
        }
    }

    async fn convert_strict_chunks(chunks: Vec<Bytes>, prepared: PreparedCodexTurn) -> String {
        create_anthropic_sse_stream_from_responses_with_prepared_turn(
            stream::iter(chunks.into_iter().map(Ok::<_, std::io::Error>)),
            None,
            None,
            prepared,
        )
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|chunk| String::from_utf8_lossy(chunk.unwrap().as_ref()).to_string())
        .collect()
    }

    #[tokio::test]
    async fn shadow_stream_observer_preserves_legacy_bytes_and_uses_one_subscription() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let input = concat!(
            "event: response.created\n",
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_shadow\",\"model\":\"gpt-5.6\"}}\n\n",
            "event: response.content_part.added\n",
            "data: {\"type\":\"response.content_part.added\",\"item_id\":\"msg_1\",\"part\":{\"type\":\"output_text\"}}\n\n",
            "event: response.output_text.delta\n",
            "data: {\"type\":\"response.output_text.delta\",\"item_id\":\"msg_1\",\"sequence_number\":1,\"delta\":\"legacy-visible\"}\n\n",
            "event: response.output_text.done\n",
            "data: {\"type\":\"response.output_text.done\",\"item_id\":\"msg_1\"}\n\n",
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n"
        );
        let baseline =
            create_anthropic_sse_stream_from_responses(stream::iter(vec![
                Ok::<_, std::io::Error>(Bytes::from_static(input.as_bytes())),
            ]))
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(Result::unwrap)
            .collect::<Vec<_>>();
        let subscriptions = Arc::new(AtomicUsize::new(0));
        let subscription_counter = subscriptions.clone();
        let upstream = stream::iter(vec![
            Ok::<_, std::io::Error>(Bytes::copy_from_slice(&input.as_bytes()[..17])),
            Ok::<_, std::io::Error>(Bytes::copy_from_slice(&input.as_bytes()[17..])),
        ])
        .inspect(move |_| {
            subscription_counter.fetch_add(1, Ordering::SeqCst);
        });
        let observed = create_anthropic_sse_stream_from_responses_with_shadow(
            upstream,
            None,
            None,
            strict_prepared_turn(),
            None,
        )
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(Result::unwrap)
        .collect::<Vec<_>>();

        assert_eq!(observed, baseline);
        assert_eq!(subscriptions.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn strict_stream_fragmentation_is_invariant_at_every_utf8_sse_byte_boundary() {
        let input = concat!(
            "event: response.created\n",
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_utf8\",\"model\":\"gpt-5.6\"}}\n\n",
            "event: response.content_part.added\n",
            "data: {\"type\":\"response.content_part.added\",\"item_id\":\"msg_1\",\"part\":{\"type\":\"output_text\"}}\n\n",
            "event: response.output_text.delta\n",
            "data: {\"type\":\"response.output_text.delta\",\"item_id\":\"msg_1\",\"sequence_number\":1,\"delta\":\"你好🌏\"}\n\n",
            "event: response.output_text.done\n",
            "data: {\"type\":\"response.output_text.done\",\"item_id\":\"msg_1\"}\n\n",
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n"
        );
        let bytes = input.as_bytes();
        let baseline =
            convert_strict_chunks(vec![Bytes::copy_from_slice(bytes)], strict_prepared_turn())
                .await;
        for split in 0..=bytes.len() {
            let actual = convert_strict_chunks(
                vec![
                    Bytes::copy_from_slice(&bytes[..split]),
                    Bytes::copy_from_slice(&bytes[split..]),
                ],
                strict_prepared_turn(),
            )
            .await;
            assert_eq!(actual, baseline, "split={split}");
        }
    }

    #[tokio::test]
    async fn strict_stream_fragmentation_rejects_illegal_sequence_independent_of_chunks() {
        let input = concat!(
            "event: response.created\n",
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_bad\",\"model\":\"gpt-5.6\"}}\n\n",
            "event: response.future_semantic.delta\n",
            "data: {\"type\":\"response.future_semantic.delta\",\"delta\":\"sentinel-private\"}\n\n"
        );
        let bytes = input.as_bytes();
        let baseline =
            convert_strict_chunks(vec![Bytes::copy_from_slice(bytes)], strict_prepared_turn())
                .await;
        assert!(baseline.contains("event: error"));
        assert!(!baseline.contains("sentinel-private"));
        for split in 0..=bytes.len() {
            let actual = convert_strict_chunks(
                vec![
                    Bytes::copy_from_slice(&bytes[..split]),
                    Bytes::copy_from_slice(&bytes[split..]),
                ],
                strict_prepared_turn(),
            )
            .await;
            assert_eq!(actual, baseline, "split={split}");
        }
    }

    #[tokio::test]
    async fn strict_stream_tool_arguments_are_invariant_at_every_logical_delta_boundary() {
        fn event(name: &str, payload: Value) -> String {
            format!("event: {name}\ndata: {payload}\n\n")
        }

        let arguments = r#"{"file_path":"src/main.rs"}"#;
        let mut baseline = None;
        for split in 1..arguments.len() {
            let (bridge, prepared, binding, codex_name) = strict_prepared_tool_turn();
            let input = [
                event(
                    "response.created",
                    json!({"type":"response.created","response":{"id":"resp_tool","model":"gpt-5.6"}}),
                ),
                event(
                    "response.output_item.added",
                    json!({"type":"response.output_item.added","item":{"id":"fc_1","type":"function_call","call_id":"call_1","name":codex_name}}),
                ),
                event(
                    "response.function_call_arguments.delta",
                    json!({"type":"response.function_call_arguments.delta","item_id":"fc_1","sequence_number":1,"delta":&arguments[..split]}),
                ),
                event(
                    "response.function_call_arguments.delta",
                    json!({"type":"response.function_call_arguments.delta","item_id":"fc_1","sequence_number":2,"delta":&arguments[split..]}),
                ),
                event(
                    "response.function_call_arguments.done",
                    json!({"type":"response.function_call_arguments.done","item_id":"fc_1"}),
                ),
                event(
                    "response.completed",
                    json!({"type":"response.completed","response":{"status":"completed"}}),
                ),
            ]
            .concat();
            let actual =
                convert_strict_chunks(vec![Bytes::copy_from_slice(input.as_bytes())], prepared)
                    .await;
            assert!(actual.contains("content_block_start"), "split={split}");
            assert!(actual.contains("tool_use"), "split={split}");
            assert!(!actual.contains("event: error"), "split={split}");
            assert_eq!(
                bridge.ledger().call_state(&binding, "call_1"),
                Some(ToolCallState::ReturnedToClaude),
                "split={split}"
            );
            if let Some(expected) = &baseline {
                assert_eq!(&actual, expected, "split={split}");
            } else {
                baseline = Some(actual);
            }
        }
    }

    fn evidence_capture(
        store: &crate::proxy::bridge_forensics::BridgeForensicStore,
    ) -> crate::proxy::bridge_forensics::ForensicStreamObserver {
        use crate::proxy::bridge_forensics::{CaptureMetadata, ForensicStreamObserver};

        let capture = store
            .begin_turn(CaptureMetadata {
                provider_id: "provider-1".to_string(),
                model: "gpt-test".to_string(),
                session_id_hash: "session-hash".to_string(),
            })
            .unwrap();
        ForensicStreamObserver::new(capture)
    }

    #[tokio::test]
    async fn successful_stream_discards_staging_capture() {
        use crate::proxy::bridge_forensics::BridgeForensicStore;

        let input = concat!(
            "event: response.created\n",
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\",\"model\":\"gpt-test\"}}\n\n",
            "event: response.output_text.delta\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\n",
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n"
        );
        let baseline =
            create_anthropic_sse_stream_from_responses(stream::iter(vec![
                Ok::<_, std::io::Error>(Bytes::from(input)),
            ]))
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(Result::unwrap)
            .collect::<Vec<_>>();
        let temp = tempfile::tempdir().unwrap();
        let store = BridgeForensicStore::new(temp.path().to_path_buf());
        let observed = create_anthropic_sse_stream_from_responses_with_evidence(
            stream::iter(vec![Ok::<_, std::io::Error>(Bytes::from(input))]),
            None,
            None,
            Some(evidence_capture(&store)),
        )
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(Result::unwrap)
        .collect::<Vec<_>>();

        assert_eq!(observed, baseline);
        assert!(store.list_bundles().unwrap().is_empty());
        assert!(std::fs::read_dir(temp.path().join("staging"))
            .unwrap()
            .next()
            .is_none());
    }

    #[tokio::test]
    async fn truncated_stream_commits_raw_event_evidence() {
        use crate::proxy::bridge_forensics::{
            BridgeForensicStore, EvidenceErrorKind, EvidenceManifest, EvidenceStage,
        };

        let input = concat!(
            "event: response.created\n",
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\",\"model\":\"gpt-test\"}}\n\n",
            "event: response.function_call_arguments.delta\n",
            "data: {\"type\":\"response.function_call_arguments.delta\",\"item_id\":\"item_1\",\"name\":\"Read\",\"delta\":\"{\\\"file_path\\\":\"}\n\n"
        );
        let temp = tempfile::tempdir().unwrap();
        let store = BridgeForensicStore::new(temp.path().to_path_buf());

        let _output = create_anthropic_sse_stream_from_responses_with_evidence(
            stream::iter(vec![Ok::<_, std::io::Error>(Bytes::from(input))]),
            None,
            None,
            Some(evidence_capture(&store)),
        )
        .collect::<Vec<_>>()
        .await;

        let bundles = store.list_bundles().unwrap();
        assert_eq!(bundles.len(), 1);
        let bundle_path = temp.path().join("bundles").join(&bundles[0].bundle_id.0);
        let manifest: EvidenceManifest =
            serde_json::from_slice(&std::fs::read(bundle_path.join("manifest.json")).unwrap())
                .unwrap();
        assert_eq!(manifest.stage, EvidenceStage::StreamTransform);
        assert_eq!(manifest.error.kind, EvidenceErrorKind::IncompleteStream);
        assert!(bundle_path.join("codex-response.ndjson").is_file());
        assert!(bundle_path.join("claude-response.ndjson").is_file());
    }

    #[test]
    fn test_map_responses_stop_reason_tool_use() {
        assert_eq!(
            map_responses_stop_reason(Some("completed"), true, None),
            Some("tool_use")
        );
        assert_eq!(
            map_responses_stop_reason(Some("completed"), false, None),
            Some("end_turn")
        );
        assert_eq!(
            map_responses_stop_reason(Some("incomplete"), false, Some("max_output_tokens")),
            Some("max_tokens")
        );
        assert_eq!(
            map_responses_stop_reason(Some("incomplete"), false, Some("content_filter")),
            Some("end_turn")
        );
    }

    #[test]
    fn test_response_object_from_event_with_wrapper() {
        let data = json!({
            "type": "response.created",
            "response": {
                "id": "resp_1",
                "model": "gpt-4o"
            }
        });
        let obj = response_object_from_event(&data);
        assert_eq!(obj["id"], "resp_1");
        assert_eq!(obj["model"], "gpt-4o");
    }

    #[tokio::test]
    async fn test_response_failed_event_becomes_anthropic_error() {
        let input = concat!(
            "event: response.created\n",
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\",\"model\":\"gpt-5\"}}\n\n",
            "event: response.failed\n",
            "data: {\"type\":\"response.failed\",\"response\":{\"status\":\"failed\",\"error\":{\"type\":\"server_error\",\"message\":\"backend exploded\"}}}\n\n"
        );

        let merged = convert_stream_text(input).await;
        assert!(merged.contains("event: error"));
        assert!(merged.contains("backend exploded"));
        assert!(!merged.contains("event: message_stop"));
    }

    #[tokio::test]
    async fn test_late_delta_after_failure_does_not_emit_message_start() {
        let input = concat!(
            "event: response.failed\n",
            "data: {\"type\":\"response.failed\",\"response\":{\"status\":\"failed\",\"error\":{\"message\":\"boom\"}}}\n\n",
            "event: response.output_text.delta\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"too late\"}\n\n"
        );

        let merged = convert_stream_text(input).await;
        assert!(merged.contains("event: error"));
        assert!(!merged.contains("event: message_start"));
        assert!(!merged.contains("too late"));
    }

    #[tokio::test]
    async fn test_completed_event_with_failed_status_is_error() {
        let input = concat!(
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"failed\",\"error\":{\"type\":\"server_error\",\"message\":\"failed wrapper\"},\"output\":[]}}\n\n"
        );

        let merged = convert_stream_text(input).await;
        assert!(merged.contains("event: error"));
        assert!(merged.contains("failed wrapper"));
        assert!(!merged.contains("event: message_stop"));
    }

    #[tokio::test]
    async fn test_response_incomplete_event_terminates_with_max_tokens() {
        let input = concat!(
            "event: response.created\n",
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\",\"model\":\"gpt-5\"}}\n\n",
            "event: response.incomplete\n",
            "data: {\"type\":\"response.incomplete\",\"response\":{\"status\":\"incomplete\",\"incomplete_details\":{\"reason\":\"max_output_tokens\"},\"usage\":{\"input_tokens\":10,\"output_tokens\":3}}}\n\n"
        );

        let merged = convert_stream_text(input).await;
        assert!(merged.contains("\"stop_reason\":\"max_tokens\""));
        assert!(merged.contains("event: message_stop"));
        assert!(!merged.contains("event: error"));
    }

    #[tokio::test]
    async fn test_response_incomplete_event_without_status_uses_event_fallback() {
        let input = concat!(
            "event: response.incomplete\n",
            "data: {\"type\":\"response.incomplete\",\"response\":{\"usage\":{\"output_tokens\":3}}}\n\n"
        );

        let merged = convert_stream_text(input).await;
        assert!(merged.contains("\"stop_reason\":\"max_tokens\""));
        assert!(merged.contains("event: message_stop"));
    }

    #[tokio::test]
    async fn test_final_event_without_blank_line_is_processed() {
        let input = concat!(
            "event: response.created\n",
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\",\"model\":\"gpt-5\"}}\n\n",
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n"
        );

        let merged = convert_stream_text(input).await;
        assert!(merged.contains("\"stop_reason\":\"end_turn\""));
        assert_eq!(merged.matches("event: message_stop").count(), 1);
        assert!(!merged.contains("stream_truncated"));
    }

    #[tokio::test]
    async fn test_clean_eof_after_partial_text_is_explicitly_incomplete() {
        let input = concat!(
            "event: response.created\n",
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\",\"model\":\"gpt-5\"}}\n\n",
            "event: response.output_text.delta\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n"
        );

        let merged = convert_stream_text(input).await;
        assert!(merged.contains("\"stop_reason\":\"max_tokens\""));
        assert!(merged.contains("event: content_block_stop"));
        assert!(merged.contains("event: message_stop"));
    }

    #[tokio::test]
    async fn test_clean_eof_during_tool_arguments_is_error() {
        let input = concat!(
            "event: response.created\n",
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\",\"model\":\"gpt-5\"}}\n\n",
            "event: response.function_call_arguments.delta\n",
            "data: {\"type\":\"response.function_call_arguments.delta\",\"item_id\":\"fc_1\",\"call_id\":\"call_1\",\"name\":\"exec\",\"delta\":\"{\\\"cmd\\\":\"}\n\n"
        );

        let merged = convert_stream_text(input).await;
        assert!(merged.contains("event: error"));
        assert!(merged.contains("stream_truncated"));
        assert!(!merged.contains("event: message_stop"));
    }

    #[tokio::test]
    async fn test_stream_request_json_fallback_removes_known_past_eof_read_offset() {
        let request = json!({
            "messages": [
                {"role": "assistant", "content": [{"type": "tool_use", "id": "past-read", "name": "Read", "input": {"file_path": "file", "offset": 25000}}]},
                {"role": "user", "content": [{"type": "tool_result", "tool_use_id": "past-read", "content": "Warning: the file exists but is shorter than the provided offset (25000). The file has 2494 lines."}]}
            ]
        });
        let protection = ReadOffsetProtection::from_anthropic_request(&request);
        let input = Bytes::from_static(br#"{"id":"resp_json","status":"completed","model":"gpt-5","output":[{"type":"function_call","call_id":"new-read","name":"Read","arguments":"{\"file_path\":\"file\",\"offset\":2495,\"limit\":2000}"}]}"#);
        let upstream = stream::iter(vec![Ok::<_, std::io::Error>(input)]);
        let merged = create_anthropic_sse_stream_from_responses_with_read_offset_protection(
            upstream,
            Some(protection),
        )
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|chunk| String::from_utf8_lossy(chunk.unwrap().as_ref()).to_string())
        .collect::<String>();
        let event = merged
            .split("\n\n")
            .filter_map(|block| block.lines().find_map(|line| line.strip_prefix("data: ")))
            .filter_map(|data| serde_json::from_str::<Value>(data).ok())
            .find(|event| {
                event.pointer("/delta/type").and_then(Value::as_str) == Some("input_json_delta")
            })
            .expect("input JSON delta");
        let input: Value = serde_json::from_str(
            event
                .pointer("/delta/partial_json")
                .and_then(Value::as_str)
                .unwrap(),
        )
        .unwrap();
        assert!(input.get("offset").is_none());
        assert_eq!(input["limit"], 2000);
    }

    #[tokio::test]
    async fn test_stream_request_with_complete_json_response_is_converted() {
        let input = r#"{
            "id":"resp_json",
            "status":"completed",
            "model":"gpt-5",
            "output":[{"type":"message","content":[{"type":"output_text","text":"hello"}]}],
            "usage":{"input_tokens":4,"output_tokens":1}
        }"#;

        let merged = convert_stream_text(input).await;
        assert!(merged.contains("event: message_start"));
        assert!(merged.contains("\"text\":\"hello\""));
        assert!(merged.contains("event: message_stop"));
    }

    #[tokio::test]
    async fn test_stream_request_with_failed_json_response_is_error() {
        let input = r#"{
            "id":"resp_json",
            "status":"failed",
            "error":{"type":"server_error","message":"json backend failed"},
            "output":[]
        }"#;

        let merged = convert_stream_text(input).await;
        assert!(merged.contains("event: error"));
        assert!(merged.contains("json backend failed"));
        assert!(!merged.contains("event: message_stop"));
    }

    #[tokio::test]
    async fn terminal_responses_usage_keeps_all_context_counters_in_message_delta() {
        let input = concat!(
            "event: response.created\n",
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_usage\",\"model\":\"gpt-5\"}}\n\n",
            "event: response.output_text.delta\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"done\"}\n\n",
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":120,\"output_tokens\":9,\"input_tokens_details\":{\"cached_tokens\":80,\"cache_write_tokens\":20}}}}\n\n"
        );

        let events: Vec<Value> = convert_stream_text(input)
            .await
            .split("\n\n")
            .filter_map(|block| {
                block
                    .lines()
                    .find_map(|line| strip_sse_field(line, "data"))
                    .and_then(|data| serde_json::from_str(data).ok())
            })
            .collect();
        let usage = events
            .iter()
            .find(|event| event.get("type").and_then(Value::as_str) == Some("message_delta"))
            .and_then(|event| event.get("usage"))
            .expect("terminal message_delta usage");

        assert_eq!(usage["input_tokens"], json!(20));
        assert_eq!(usage["cache_read_input_tokens"], json!(80));
        assert_eq!(usage["cache_creation_input_tokens"], json!(20));
        assert_eq!(usage["output_tokens"], json!(9));
    }

    #[tokio::test]
    async fn test_streaming_conversion_with_wrapped_response_events() {
        let input = concat!(
            "event: response.created\n",
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\",\"model\":\"gpt-4o\",\"usage\":{\"input_tokens\":12,\"output_tokens\":0}}}\n\n",
            "event: response.output_item.added\n",
            "data: {\"type\":\"response.output_item.added\",\"item\":{\"type\":\"function_call\",\"call_id\":\"call_1\",\"name\":\"get_weather\"}}\n\n",
            "event: response.function_call_arguments.delta\n",
            "data: {\"type\":\"response.function_call_arguments.delta\",\"delta\":\"{\\\"city\\\":\\\"Tokyo\\\"}\"}\n\n",
            "event: response.function_call_arguments.done\n",
            "data: {\"type\":\"response.function_call_arguments.done\"}\n\n",
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":12,\"output_tokens\":3}}}\n\n"
        );

        let upstream = stream::iter(vec![Ok::<_, std::io::Error>(Bytes::from(
            input.as_bytes().to_vec(),
        ))]);
        let converted = create_anthropic_sse_stream_from_responses(upstream);
        let chunks: Vec<_> = converted.collect().await;

        let merged = chunks
            .into_iter()
            .map(|c| String::from_utf8_lossy(c.unwrap().as_ref()).to_string())
            .collect::<String>();

        assert!(merged.contains("\"type\":\"message_start\""));
        assert!(merged.contains("\"id\":\"resp_1\""));
        assert!(merged.contains("\"model\":\"gpt-4o\""));
        assert!(merged.contains("\"type\":\"tool_use\""));
        assert!(merged.contains("\"name\":\"get_weather\""));
        assert!(merged.contains("\"type\":\"input_json_delta\""));
        assert!(merged.contains("\"stop_reason\":\"tool_use\""));
        assert!(merged.contains("\"input_tokens\":12"));
        assert!(merged.contains("\"output_tokens\":3"));
        assert!(merged.contains("\"type\":\"message_stop\""));
    }

    #[tokio::test]
    async fn prepared_stream_restores_registered_alias_and_validates_arguments() {
        let input = concat!(
            "event: response.created\n",
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\",\"model\":\"gpt-test\"}}\n\n",
            "event: response.output_item.added\n",
            "data: {\"type\":\"response.output_item.added\",\"item\":{\"id\":\"item_1\",\"type\":\"function_call\",\"call_id\":\"call_1\",\"name\":\"read_file\"}}\n\n",
            "event: response.function_call_arguments.delta\n",
            "data: {\"type\":\"response.function_call_arguments.delta\",\"item_id\":\"item_1\",\"delta\":\"{\\\"file_path\\\":\\\"src/main.rs\\\"}\"}\n\n",
            "event: response.function_call_arguments.done\n",
            "data: {\"type\":\"response.function_call_arguments.done\",\"item_id\":\"item_1\"}\n\n",
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n"
        );

        let converted = convert_stream_with_registry(input).await;

        assert!(converted.contains("\"id\":\"call_1\""));
        assert!(converted.contains("\"name\":\"Read\""));
        assert!(converted.contains("src/main.rs"));
        assert!(!converted.contains("\"name\":\"read_file\""));
        assert!(!converted.contains("response_parse_error"));
    }

    #[tokio::test]
    async fn prepared_stream_rejects_unknown_tool_before_tool_use_visibility() {
        let input = concat!(
            "event: response.created\n",
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\",\"model\":\"gpt-test\"}}\n\n",
            "event: response.output_item.added\n",
            "data: {\"type\":\"response.output_item.added\",\"item\":{\"id\":\"item_1\",\"type\":\"function_call\",\"call_id\":\"call_1\",\"name\":\"unknown\"}}\n\n"
        );

        let converted = convert_stream_with_registry(input).await;

        assert!(converted.contains("tool_registry_violation"));
        assert!(!converted.contains("\"type\":\"tool_use\""));
        assert!(!converted.contains("\"name\":\"unknown\""));
    }

    #[tokio::test]
    async fn prepared_stream_rejects_invalid_arguments_without_closing_tool_block() {
        let input = concat!(
            "event: response.created\n",
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\",\"model\":\"gpt-test\"}}\n\n",
            "event: response.output_item.added\n",
            "data: {\"type\":\"response.output_item.added\",\"item\":{\"id\":\"item_1\",\"type\":\"function_call\",\"call_id\":\"call_1\",\"name\":\"read_file\"}}\n\n",
            "event: response.function_call_arguments.delta\n",
            "data: {\"type\":\"response.function_call_arguments.delta\",\"item_id\":\"item_1\",\"delta\":\"{}\"}\n\n",
            "event: response.function_call_arguments.done\n",
            "data: {\"type\":\"response.function_call_arguments.done\",\"item_id\":\"item_1\"}\n\n"
        );

        let converted = convert_stream_with_registry(input).await;

        assert!(converted.contains("tool_registry_violation"));
        assert!(!converted.contains("\"type\":\"tool_use\""));
        assert!(!converted.contains("\"type\":\"content_block_stop\""));
        assert!(!converted.contains("\"partial_json\":\"{}\""));
    }

    #[tokio::test]
    async fn prepared_stream_never_synthesizes_call_id_from_item_id() {
        let input = concat!(
            "event: response.created\n",
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\",\"model\":\"gpt-test\"}}\n\n",
            "event: response.function_call_arguments.delta\n",
            "data: {\"type\":\"response.function_call_arguments.delta\",\"item_id\":\"item_1\",\"name\":\"read_file\",\"delta\":\"{\\\"file_path\\\":\\\"src/main.rs\\\"}\"}\n\n",
            "event: response.function_call_arguments.done\n",
            "data: {\"type\":\"response.function_call_arguments.done\",\"item_id\":\"item_1\"}\n\n"
        );

        let converted = convert_stream_with_registry(input).await;

        assert!(converted.contains("tool_registry_violation"));
        assert!(!converted.contains("\"type\":\"tool_use\""));
        assert!(!converted.contains("\"id\":\"item_1\""));
    }

    #[tokio::test]
    async fn prepared_stream_output_item_done_emits_exact_read_arguments_once() {
        let input = concat!(
            "event: response.created\n",
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\",\"model\":\"gpt-test\"}}\n\n",
            "event: response.output_item.added\n",
            "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"id\":\"item_1\",\"type\":\"function_call\",\"call_id\":\"call_1\",\"name\":\"read_file\"}}\n\n",
            "event: response.output_item.done\n",
            "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"id\":\"item_1\",\"type\":\"function_call\",\"call_id\":\"call_1\",\"name\":\"read_file\",\"arguments\":\"{\\\"file_path\\\":\\\"src/main.rs\\\",\\\"pages\\\":\\\"\\\",\\\"offset\\\":2.300310976710655e22}\"}}\n\n",
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n"
        );

        let converted = convert_stream_with_registry(input).await;

        assert_eq!(converted.matches("\"type\":\"tool_use\"").count(), 1);
        assert_eq!(converted.matches("partial_json").count(), 1);
        assert!(converted.contains("\\\"pages\\\":\\\"\\\""));
        assert!(converted.contains("2.300310976710655e22"));
        assert!(!converted.contains("tool_registry_violation"));
    }

    #[tokio::test]
    async fn prepared_stream_rejects_conflicting_tool_identity_events() {
        let input = concat!(
            "event: response.created\n",
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\",\"model\":\"gpt-test\"}}\n\n",
            "event: response.output_item.added\n",
            "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"id\":\"item_1\",\"type\":\"function_call\",\"call_id\":\"call_1\",\"name\":\"read_file\"}}\n\n",
            "event: response.output_item.done\n",
            "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"id\":\"item_1\",\"type\":\"function_call\",\"call_id\":\"call_2\",\"name\":\"different_tool\",\"arguments\":\"{\\\"file_path\\\":\\\"src/main.rs\\\"}\"}}\n\n"
        );

        let converted = convert_stream_with_registry(input).await;

        assert!(converted.contains("tool_registry_violation"));
        assert!(!converted.contains("\"type\":\"tool_use\""));
    }

    #[tokio::test]
    async fn prepared_stream_accepts_consistent_arguments_done_then_output_item_done() {
        let input = concat!(
            "event: response.created\n",
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\",\"model\":\"gpt-test\"}}\n\n",
            "event: response.output_item.added\n",
            "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"id\":\"item_1\",\"type\":\"function_call\",\"call_id\":\"call_1\",\"name\":\"read_file\"}}\n\n",
            "event: response.function_call_arguments.delta\n",
            "data: {\"type\":\"response.function_call_arguments.delta\",\"item_id\":\"item_1\",\"output_index\":0,\"delta\":\"{\\\"file_path\\\":\\\"src/main.rs\\\"}\"}\n\n",
            "event: response.function_call_arguments.done\n",
            "data: {\"type\":\"response.function_call_arguments.done\",\"item_id\":\"item_1\",\"output_index\":0,\"arguments\":\"{\\\"file_path\\\":\\\"src/main.rs\\\"}\"}\n\n",
            "event: response.output_item.done\n",
            "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"id\":\"item_1\",\"type\":\"function_call\",\"call_id\":\"call_1\",\"name\":\"read_file\",\"arguments\":\"{\\\"file_path\\\":\\\"src/main.rs\\\"}\"}}\n\n",
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n"
        );

        let converted = convert_stream_with_registry(input).await;

        assert_eq!(converted.matches("\"type\":\"tool_use\"").count(), 1);
        assert_eq!(converted.matches("partial_json").count(), 1);
        assert!(!converted.contains("tool_registry_violation"));
    }

    #[tokio::test]
    async fn prepared_stream_does_not_reopen_a_completed_call_for_duplicate_events() {
        let input = concat!(
            "event: response.created\n",
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\",\"model\":\"gpt-test\"}}\n\n",
            "event: response.output_item.added\n",
            "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"id\":\"item_1\",\"type\":\"function_call\",\"call_id\":\"call_1\",\"name\":\"read_file\"}}\n\n",
            "event: response.function_call_arguments.delta\n",
            "data: {\"type\":\"response.function_call_arguments.delta\",\"item_id\":\"item_1\",\"output_index\":0,\"delta\":\"{\\\"file_path\\\":\\\"src/main.rs\\\"}\"}\n\n",
            "event: response.function_call_arguments.done\n",
            "data: {\"type\":\"response.function_call_arguments.done\",\"item_id\":\"item_1\",\"output_index\":0,\"arguments\":\"{\\\"file_path\\\":\\\"src/main.rs\\\"}\"}\n\n",
            "event: response.output_item.added\n",
            "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"id\":\"item_1\",\"type\":\"function_call\",\"call_id\":\"call_1\",\"name\":\"read_file\"}}\n\n",
            "event: response.function_call_arguments.delta\n",
            "data: {\"type\":\"response.function_call_arguments.delta\",\"item_id\":\"item_1\",\"output_index\":0,\"name\":\"read_file\",\"call_id\":\"call_1\",\"delta\":\"{\\\"file_path\\\":\\\"src/main.rs\\\"}\"}\n\n",
            "event: response.function_call_arguments.done\n",
            "data: {\"type\":\"response.function_call_arguments.done\",\"item_id\":\"item_1\",\"output_index\":0}\n\n",
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n"
        );

        let converted = convert_stream_with_registry(input).await;

        assert_eq!(converted.matches("\"type\":\"tool_use\"").count(), 1);
        assert_eq!(converted.matches("partial_json").count(), 1);
        assert!(!converted.contains("tool_registry_violation"));
    }

    #[tokio::test]
    async fn prepared_stream_rejects_unfinished_conflicting_duplicate_arguments_at_terminal() {
        let input = concat!(
            "event: response.created\n",
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\",\"model\":\"gpt-test\"}}\n\n",
            "event: response.output_item.added\n",
            "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"id\":\"item_1\",\"type\":\"function_call\",\"call_id\":\"call_1\",\"name\":\"read_file\"}}\n\n",
            "event: response.function_call_arguments.delta\n",
            "data: {\"type\":\"response.function_call_arguments.delta\",\"item_id\":\"item_1\",\"output_index\":0,\"delta\":\"{\\\"file_path\\\":\\\"src/main.rs\\\"}\"}\n\n",
            "event: response.function_call_arguments.done\n",
            "data: {\"type\":\"response.function_call_arguments.done\",\"item_id\":\"item_1\",\"output_index\":0}\n\n",
            "event: response.function_call_arguments.delta\n",
            "data: {\"type\":\"response.function_call_arguments.delta\",\"item_id\":\"item_1\",\"output_index\":0,\"name\":\"read_file\",\"call_id\":\"call_1\",\"delta\":\"{\\\"file_path\\\":\\\"different.rs\\\"}\"}\n\n",
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n"
        );

        let converted = convert_stream_with_registry(input).await;

        assert_eq!(converted.matches("\"type\":\"tool_use\"").count(), 1);
        assert!(converted.contains("tool_registry_violation"));
        assert!(!converted.contains("\"type\":\"message_stop\""));
    }

    #[tokio::test]
    async fn prepared_stream_json_fallback_restores_registry_identity() {
        let input = json!({
            "id": "resp_json",
            "model": "gpt-test",
            "status": "completed",
            "output": [{
                "type": "function_call",
                "call_id": "call_json",
                "name": "read_file",
                "arguments": "{\"file_path\":\"src/lib.rs\"}"
            }]
        })
        .to_string();

        let converted = convert_stream_with_registry(&input).await;

        assert!(converted.contains("\"id\":\"call_json\""));
        assert!(converted.contains("\"name\":\"Read\""));
        assert!(converted.contains("src/lib.rs"));
        assert!(!converted.contains("\"name\":\"read_file\""));
    }

    #[tokio::test]
    async fn test_streaming_read_tool_drops_empty_pages() {
        let input = concat!(
            "event: response.created\n",
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_read\",\"model\":\"gpt-5.5\"}}\n\n",
            "event: response.output_item.added\n",
            "data: {\"type\":\"response.output_item.added\",\"item\":{\"id\":\"fc_read\",\"type\":\"function_call\",\"call_id\":\"call_read\",\"name\":\"Read\"}}\n\n",
            "event: response.function_call_arguments.delta\n",
            "data: {\"type\":\"response.function_call_arguments.delta\",\"item_id\":\"fc_read\",\"delta\":\"{\\\"file_path\\\":\\\"/tmp/demo.py\\\",\\\"limit\\\":2000,\\\"offset\\\":0,\\\"pages\\\":\\\"\\\"}\"}\n\n",
            "event: response.function_call_arguments.done\n",
            "data: {\"type\":\"response.function_call_arguments.done\",\"item_id\":\"fc_read\",\"arguments\":\"{\\\"file_path\\\":\\\"/tmp/demo.py\\\",\\\"limit\\\":2000,\\\"offset\\\":0,\\\"pages\\\":\\\"\\\"}\"}\n\n",
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n"
        );

        let upstream = stream::iter(vec![Ok::<_, std::io::Error>(Bytes::from(
            input.as_bytes().to_vec(),
        ))]);
        let converted = create_anthropic_sse_stream_from_responses(upstream);
        let chunks: Vec<_> = converted.collect().await;

        let merged = chunks
            .into_iter()
            .map(|c| String::from_utf8_lossy(c.unwrap().as_ref()).to_string())
            .collect::<String>();

        assert!(merged.contains("\"name\":\"Read\""));
        assert!(merged.contains("\"partial_json\":\"{\\\"file_path\\\":\\\"/tmp/demo.py\\\",\\\"limit\\\":2000,\\\"offset\\\":0}"));
        assert!(!merged.contains("\\\"pages\\\":\\\"\\\""));
    }

    #[tokio::test]
    async fn test_streaming_read_tool_drops_scientific_notation_offset() {
        let input = concat!(
            "event: response.created\n",
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_read_bad_offset\",\"model\":\"gpt-5.5\"}}\n\n",
            "event: response.output_item.added\n",
            "data: {\"type\":\"response.output_item.added\",\"item\":{\"id\":\"fc_read\",\"type\":\"function_call\",\"call_id\":\"call_read\",\"name\":\"Read\"}}\n\n",
            "event: response.function_call_arguments.delta\n",
            "data: {\"type\":\"response.function_call_arguments.delta\",\"item_id\":\"fc_read\",\"delta\":\"{\\\"file_path\\\":\\\"file\\\",\\\"offset\\\":2.300\"}\n\n",
            "event: response.function_call_arguments.done\n",
            "data: {\"type\":\"response.function_call_arguments.done\",\"item_id\":\"fc_read\",\"arguments\":\"{\\\"file_path\\\":\\\"file\\\",\\\"offset\\\":2.300310976710655e+22,\\\"limit\\\":2000,\\\"pages\\\":\\\"\\\"}\"}\n\n",
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n"
        );

        let merged = convert_stream_text(input).await;

        assert!(!merged.contains("2.300310976710655e+22"));
        assert!(!merged.contains("\\\"offset\\\":"));
        assert!(!merged.contains("\\\"pages\\\":\\\"\\\""));
        assert!(merged.contains("\\\"file_path\\\":\\\"file\\\""));
        assert!(merged.contains("\\\"limit\\\":2000"));
    }

    #[tokio::test]
    async fn test_streaming_read_tool_duplicate_start_preserves_buffered_args() {
        let input = concat!(
            "event: response.created\n",
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_read\",\"model\":\"gpt-5.5\"}}\n\n",
            "event: response.output_item.added\n",
            "data: {\"type\":\"response.output_item.added\",\"item\":{\"id\":\"fc_read\",\"type\":\"function_call\",\"call_id\":\"call_read\",\"name\":\"Read\"}}\n\n",
            "event: response.function_call_arguments.delta\n",
            "data: {\"type\":\"response.function_call_arguments.delta\",\"item_id\":\"fc_read\",\"delta\":\"{\\\"file_path\\\":\\\"/tmp/demo.py\\\",\\\"limit\\\":2000,\\\"offset\\\":0,\\\"pages\\\":\\\"\\\"}\"}\n\n",
            "event: response.output_item.added\n",
            "data: {\"type\":\"response.output_item.added\",\"item\":{\"id\":\"fc_read\",\"type\":\"function_call\",\"call_id\":\"call_read\",\"name\":\"Read\"}}\n\n",
            "event: response.function_call_arguments.done\n",
            "data: {\"type\":\"response.function_call_arguments.done\",\"item_id\":\"fc_read\"}\n\n",
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n"
        );

        let upstream = stream::iter(vec![Ok::<_, std::io::Error>(Bytes::from(
            input.as_bytes().to_vec(),
        ))]);
        let converted = create_anthropic_sse_stream_from_responses(upstream);
        let chunks: Vec<_> = converted.collect().await;

        let merged = chunks
            .into_iter()
            .map(|c| String::from_utf8_lossy(c.unwrap().as_ref()).to_string())
            .collect::<String>();

        assert_eq!(merged.matches("event: content_block_start").count(), 1);
        assert_eq!(merged.matches("event: content_block_stop").count(), 1);
        assert!(merged.contains("\"partial_json\":\"{\\\"file_path\\\":\\\"/tmp/demo.py\\\",\\\"limit\\\":2000,\\\"offset\\\":0}"));
        assert!(!merged.contains("\\\"pages\\\":\\\"\\\""));
    }

    #[tokio::test]
    async fn test_streaming_conversion_interleaved_tool_deltas_by_item_id() {
        let input = concat!(
            "event: response.created\n",
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_2\",\"model\":\"gpt-4o\"}}\n\n",
            "event: response.output_item.added\n",
            "data: {\"type\":\"response.output_item.added\",\"item\":{\"id\":\"fc_1\",\"type\":\"function_call\",\"call_id\":\"call_1\",\"name\":\"first_tool\"}}\n\n",
            "event: response.output_item.added\n",
            "data: {\"type\":\"response.output_item.added\",\"item\":{\"id\":\"fc_2\",\"type\":\"function_call\",\"call_id\":\"call_2\",\"name\":\"second_tool\"}}\n\n",
            "event: response.function_call_arguments.delta\n",
            "data: {\"type\":\"response.function_call_arguments.delta\",\"item_id\":\"fc_2\",\"delta\":\"{\\\"b\\\":2}\"}\n\n",
            "event: response.function_call_arguments.delta\n",
            "data: {\"type\":\"response.function_call_arguments.delta\",\"item_id\":\"fc_1\",\"delta\":\"{\\\"a\\\":1}\"}\n\n",
            "event: response.function_call_arguments.done\n",
            "data: {\"type\":\"response.function_call_arguments.done\",\"item_id\":\"fc_1\"}\n\n",
            "event: response.function_call_arguments.done\n",
            "data: {\"type\":\"response.function_call_arguments.done\",\"item_id\":\"fc_2\"}\n\n",
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":8,\"output_tokens\":4}}}\n\n"
        );

        let upstream = stream::iter(vec![Ok::<_, std::io::Error>(Bytes::from(
            input.as_bytes().to_vec(),
        ))]);
        let converted = create_anthropic_sse_stream_from_responses(upstream);
        let chunks: Vec<_> = converted.collect().await;
        let merged = chunks
            .into_iter()
            .map(|c| String::from_utf8_lossy(c.unwrap().as_ref()).to_string())
            .collect::<String>();

        let events: Vec<Value> = merged
            .split("\n\n")
            .filter_map(|block| {
                let data = block
                    .lines()
                    .find_map(|line| strip_sse_field(line, "data"))?;
                serde_json::from_str::<Value>(data).ok()
            })
            .collect();

        let mut tool_index_by_call: HashMap<String, u64> = HashMap::new();
        for event in &events {
            if event.get("type").and_then(|v| v.as_str()) == Some("content_block_start") {
                let cb = event.get("content_block");
                if cb.and_then(|v| v.get("type")).and_then(|v| v.as_str()) == Some("tool_use") {
                    if let (Some(call_id), Some(index)) = (
                        cb.and_then(|v| v.get("id")).and_then(|v| v.as_str()),
                        event.get("index").and_then(|v| v.as_u64()),
                    ) {
                        tool_index_by_call.insert(call_id.to_string(), index);
                    }
                }
            }
        }

        let delta_indices: Vec<u64> = events
            .iter()
            .filter(|event| {
                event.get("type").and_then(|v| v.as_str()) == Some("content_block_delta")
                    && event.pointer("/delta/type").and_then(|v| v.as_str())
                        == Some("input_json_delta")
            })
            .filter_map(|event| event.get("index").and_then(|v| v.as_u64()))
            .collect();

        assert_eq!(delta_indices.len(), 2);
        assert_eq!(delta_indices[0], *tool_index_by_call.get("call_2").unwrap());
        assert_eq!(delta_indices[1], *tool_index_by_call.get("call_1").unwrap());
        assert_ne!(
            tool_index_by_call.get("call_1"),
            tool_index_by_call.get("call_2")
        );
    }

    #[tokio::test]
    async fn test_streaming_tool_done_arguments_fallback_without_deltas() {
        let input = concat!(
            "event: response.created\n",
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_done\",\"model\":\"gpt-5.6\"}}\n\n",
            "event: response.output_item.added\n",
            "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"id\":\"fc_done\",\"type\":\"function_call\",\"call_id\":\"call_done\",\"name\":\"lookup\",\"arguments\":\"\"}}\n\n",
            "event: response.function_call_arguments.done\n",
            "data: {\"type\":\"response.function_call_arguments.done\",\"item_id\":\"fc_done\",\"output_index\":0,\"item\":{\"id\":\"fc_done\",\"type\":\"function_call\",\"arguments\":\"{\\\"q\\\":\\\"rust\\\"}\"}}\n\n",
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n"
        );
        let upstream = stream::iter(vec![Ok::<_, std::io::Error>(Bytes::from(input))]);
        let merged = create_anthropic_sse_stream_from_responses(upstream)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|chunk| String::from_utf8_lossy(chunk.unwrap().as_ref()).to_string())
            .collect::<String>();

        assert!(merged.contains("\"partial_json\":\"{\\\"q\\\":\\\"rust\\\"}\""));
        assert_eq!(merged.matches("event: content_block_stop").count(), 1);
    }

    #[tokio::test]
    async fn test_official_reasoning_events_emit_signature_before_stop() {
        let input = concat!(
            "event: response.created\n",
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_reason\",\"model\":\"gpt-5.6\"}}\n\n",
            "event: response.output_item.added\n",
            "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"id\":\"rs_1\",\"type\":\"reasoning\",\"summary\":[]}}\n\n",
            "event: response.reasoning_summary_part.added\n",
            "data: {\"type\":\"response.reasoning_summary_part.added\",\"item_id\":\"rs_1\",\"output_index\":0,\"summary_index\":0,\"part\":{\"type\":\"summary_text\",\"text\":\"\"}}\n\n",
            "event: response.reasoning_summary_text.delta\n",
            "data: {\"type\":\"response.reasoning_summary_text.delta\",\"item_id\":\"rs_1\",\"output_index\":0,\"summary_index\":0,\"delta\":\"Need a tool.\"}\n\n",
            "event: response.reasoning_summary_text.done\n",
            "data: {\"type\":\"response.reasoning_summary_text.done\",\"item_id\":\"rs_1\",\"output_index\":0,\"summary_index\":0,\"text\":\"Need a tool.\"}\n\n",
            "event: response.output_item.done\n",
            "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"id\":\"rs_1\",\"type\":\"reasoning\",\"summary\":[{\"type\":\"summary_text\",\"text\":\"Need a tool.\"}],\"encrypted_content\":\"opaque\"}}\n\n",
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n"
        );
        let upstream = stream::iter(vec![Ok::<_, std::io::Error>(Bytes::from(input))]);
        let merged = create_anthropic_sse_stream_from_responses(upstream)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|chunk| String::from_utf8_lossy(chunk.unwrap().as_ref()).to_string())
            .collect::<String>();

        assert!(merged.contains("\"type\":\"thinking_delta\""));
        assert!(merged.contains("\"type\":\"signature_delta\""));
        let signature_position = merged.find("signature_delta").unwrap();
        let stop_position = merged.find("event: content_block_stop").unwrap();
        assert!(signature_position < stop_position);
        assert!(!merged[stop_position..].contains("content_block_delta"));
    }

    #[tokio::test]
    async fn test_encrypted_reasoning_without_summary_emits_empty_thinking_signature() {
        let input = concat!(
            "event: response.created\n",
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_reason_empty\",\"model\":\"gpt-5.6\"}}\n\n",
            "event: response.output_item.added\n",
            "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"id\":\"rs_empty\",\"type\":\"reasoning\",\"summary\":[]}}\n\n",
            "event: response.output_item.done\n",
            "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"id\":\"rs_empty\",\"type\":\"reasoning\",\"summary\":[],\"encrypted_content\":\"opaque\"}}\n\n",
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n"
        );
        let upstream = stream::iter(vec![Ok::<_, std::io::Error>(Bytes::from(input))]);
        let events: Vec<Value> = create_anthropic_sse_stream_from_responses(upstream)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .flat_map(|chunk| {
                String::from_utf8_lossy(chunk.unwrap().as_ref())
                    .split("\n\n")
                    .filter_map(|block| {
                        block
                            .lines()
                            .find_map(|line| line.strip_prefix("data: "))
                            .and_then(|data| serde_json::from_str(data).ok())
                    })
                    .collect::<Vec<Value>>()
            })
            .collect();

        let thinking_start = events
            .iter()
            .position(|event| {
                event.get("type").and_then(Value::as_str) == Some("content_block_start")
                    && event.pointer("/content_block/type").and_then(Value::as_str)
                        == Some("thinking")
            })
            .expect("encrypted reasoning must start a thinking block");
        let thinking_index = events[thinking_start]["index"].as_u64().unwrap();
        assert_eq!(events[thinking_start]["content_block"]["thinking"], "");
        assert!(!events.iter().any(|event| {
            event.pointer("/content_block/type").and_then(Value::as_str)
                == Some("redacted_thinking")
        }));
        assert!(!events.iter().any(|event| {
            event.pointer("/delta/type").and_then(Value::as_str) == Some("thinking_delta")
        }));

        let signature_position = events
            .iter()
            .position(|event| {
                event.get("index").and_then(Value::as_u64) == Some(thinking_index)
                    && event.pointer("/delta/type").and_then(Value::as_str)
                        == Some("signature_delta")
            })
            .expect("encrypted reasoning must emit a signature delta");
        assert!(events[signature_position]["delta"]["signature"]
            .as_str()
            .is_some_and(|value| value.starts_with("ccswitch-openai-reasoning-v1:")));
        let stop_position = events
            .iter()
            .position(|event| {
                event.get("type").and_then(Value::as_str) == Some("content_block_stop")
                    && event.get("index").and_then(Value::as_u64) == Some(thinking_index)
            })
            .expect("thinking block must stop");
        assert!(signature_position < stop_position);
        assert!(events
            .iter()
            .any(|event| event.get("type").and_then(Value::as_str) == Some("message_delta")));
        assert!(events
            .iter()
            .any(|event| event.get("type").and_then(Value::as_str) == Some("message_stop")));
    }

    #[tokio::test]
    async fn test_streaming_reasoning_delta_emits_thinking_blocks() {
        let input = concat!(
            "event: response.created\n",
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_r\",\"model\":\"o3\",\"usage\":{\"input_tokens\":5,\"output_tokens\":0}}}\n\n",
            "event: response.reasoning.delta\n",
            "data: {\"type\":\"response.reasoning.delta\",\"delta\":\"Let me \"}\n\n",
            "event: response.reasoning.delta\n",
            "data: {\"type\":\"response.reasoning.delta\",\"delta\":\"think...\"}\n\n",
            "event: response.reasoning.done\n",
            "data: {\"type\":\"response.reasoning.done\"}\n\n",
            "event: response.content_part.added\n",
            "data: {\"type\":\"response.content_part.added\",\"part\":{\"type\":\"output_text\",\"text\":\"\"},\"output_index\":0,\"content_index\":0}\n\n",
            "event: response.output_text.delta\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"42\",\"output_index\":0,\"content_index\":0}\n\n",
            "event: response.content_part.done\n",
            "data: {\"type\":\"response.content_part.done\",\"output_index\":0,\"content_index\":0}\n\n",
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":5,\"output_tokens\":10}}}\n\n"
        );

        let upstream = stream::iter(vec![Ok::<_, std::io::Error>(Bytes::from(
            input.as_bytes().to_vec(),
        ))]);
        let converted = create_anthropic_sse_stream_from_responses(upstream);
        let chunks: Vec<_> = converted.collect().await;
        let merged = chunks
            .into_iter()
            .map(|c| String::from_utf8_lossy(c.unwrap().as_ref()).to_string())
            .collect::<String>();

        // Should contain thinking block start, thinking delta, and text content
        assert!(
            merged.contains("\"type\":\"thinking\""),
            "should emit thinking content_block_start"
        );
        assert!(
            merged.contains("\"type\":\"thinking_delta\""),
            "should emit thinking_delta"
        );
        assert!(
            merged.contains("\"thinking\":\"Let me \"")
                && merged.contains("\"thinking\":\"think...\""),
            "should contain both thinking deltas"
        );
        assert!(
            merged.contains("\"type\":\"text_delta\""),
            "should also emit text content"
        );
        assert!(
            merged.contains("\"text\":\"42\""),
            "should contain text delta"
        );
        assert!(merged.contains("\"stop_reason\":\"end_turn\""));

        let events: Vec<Value> = merged
            .split("\n\n")
            .filter_map(|block| {
                block
                    .lines()
                    .find_map(|line| line.strip_prefix("data: "))
                    .and_then(|data| serde_json::from_str(data).ok())
            })
            .collect();
        let thinking_starts: Vec<&Value> = events
            .iter()
            .filter(|event| {
                event.get("type").and_then(Value::as_str) == Some("content_block_start")
                    && event.pointer("/content_block/type").and_then(Value::as_str)
                        == Some("thinking")
            })
            .collect();
        assert_eq!(
            thinking_starts.len(),
            1,
            "keyless deltas must share one block"
        );
        let thinking_index = thinking_starts[0]
            .get("index")
            .and_then(Value::as_u64)
            .unwrap();
        let thinking_delta_indices: Vec<u64> = events
            .iter()
            .filter(|event| {
                event.pointer("/delta/type").and_then(Value::as_str) == Some("thinking_delta")
            })
            .filter_map(|event| event.get("index").and_then(Value::as_u64))
            .collect();
        assert_eq!(thinking_delta_indices, vec![thinking_index, thinking_index]);

        let stop_position = events
            .iter()
            .position(|event| {
                event.get("type").and_then(Value::as_str) == Some("content_block_stop")
                    && event.get("index").and_then(Value::as_u64) == Some(thinking_index)
            })
            .expect("legacy reasoning done must close the thinking block");
        let text_start_position = events
            .iter()
            .position(|event| {
                event.get("type").and_then(Value::as_str) == Some("content_block_start")
                    && event.pointer("/content_block/type").and_then(Value::as_str) == Some("text")
            })
            .expect("text block must start");
        assert!(stop_position < text_start_position);
    }

    #[tokio::test]
    async fn test_streaming_text_parts_are_merged_into_one_text_block() {
        let input = concat!(
            "event: response.created\n",
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_merge\",\"model\":\"gpt-5.4\",\"usage\":{\"input_tokens\":5,\"output_tokens\":0}}}\n\n",
            "event: response.content_part.added\n",
            "data: {\"type\":\"response.content_part.added\",\"part\":{\"type\":\"output_text\",\"text\":\"\"},\"output_index\":0,\"content_index\":0}\n\n",
            "event: response.output_text.delta\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"你\",\"output_index\":0,\"content_index\":0}\n\n",
            "event: response.content_part.done\n",
            "data: {\"type\":\"response.content_part.done\",\"output_index\":0,\"content_index\":0}\n\n",
            "event: response.content_part.added\n",
            "data: {\"type\":\"response.content_part.added\",\"part\":{\"type\":\"output_text\",\"text\":\"\"},\"output_index\":0,\"content_index\":1}\n\n",
            "event: response.output_text.delta\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"好\",\"output_index\":0,\"content_index\":1}\n\n",
            "event: response.content_part.done\n",
            "data: {\"type\":\"response.content_part.done\",\"output_index\":0,\"content_index\":1}\n\n",
            "event: response.output_text.done\n",
            "data: {\"type\":\"response.output_text.done\",\"output_index\":0,\"content_index\":1}\n\n",
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":5,\"output_tokens\":2}}}\n\n"
        );

        let upstream = stream::iter(vec![Ok::<_, std::io::Error>(Bytes::from(
            input.as_bytes().to_vec(),
        ))]);
        let converted = create_anthropic_sse_stream_from_responses(upstream);
        let chunks: Vec<_> = converted.collect().await;
        let events: Vec<Value> = chunks
            .into_iter()
            .flat_map(|chunk| {
                let bytes = chunk.unwrap();
                let text = String::from_utf8_lossy(bytes.as_ref()).to_string();
                text.split("\n\n")
                    .filter_map(|block| {
                        block.lines().find_map(|line| {
                            strip_sse_field(line, "data")
                                .and_then(|payload| serde_json::from_str::<Value>(payload).ok())
                        })
                    })
                    .collect::<Vec<_>>()
            })
            .collect();

        let text_starts = events
            .iter()
            .filter(|event| {
                event.get("type").and_then(|v| v.as_str()) == Some("content_block_start")
                    && event
                        .pointer("/content_block/type")
                        .and_then(|v| v.as_str())
                        == Some("text")
            })
            .count();
        let text_stops = events
            .iter()
            .filter(|event| {
                event.get("type").and_then(|v| v.as_str()) == Some("content_block_stop")
            })
            .count();
        let text_deltas: Vec<String> = events
            .iter()
            .filter(|event| {
                event.get("type").and_then(|v| v.as_str()) == Some("content_block_delta")
                    && event.pointer("/delta/type").and_then(|v| v.as_str()) == Some("text_delta")
            })
            .filter_map(|event| {
                event
                    .pointer("/delta/text")
                    .and_then(|v| v.as_str())
                    .map(ToString::to_string)
            })
            .collect();

        assert_eq!(text_starts, 1);
        assert_eq!(text_stops, 1);
        assert_eq!(text_deltas, vec!["你".to_string(), "好".to_string()]);
    }

    #[tokio::test]
    async fn test_streaming_responses_chinese_split_across_chunks_no_replacement_chars() {
        // Chinese text delta split across two TCP chunks.
        let full = concat!(
            "event: response.created\n",
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_cn\",\"model\":\"gpt-4o\",\"usage\":{\"input_tokens\":5,\"output_tokens\":0}}}\n\n",
            "event: response.output_text.delta\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"你好世界\"}\n\n",
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":5,\"output_tokens\":4}}}\n\n"
        );
        let bytes = full.as_bytes();

        // Find "你" and split inside it
        let ni_start = bytes.windows(3).position(|w| w == "你".as_bytes()).unwrap();
        let split_point = ni_start + 2; // split after second byte of "你"

        let chunk1 = Bytes::from(bytes[..split_point].to_vec());
        let chunk2 = Bytes::from(bytes[split_point..].to_vec());

        let upstream = stream::iter(vec![
            Ok::<_, std::io::Error>(chunk1),
            Ok::<_, std::io::Error>(chunk2),
        ]);
        let converted = create_anthropic_sse_stream_from_responses(upstream);
        let chunks: Vec<_> = converted.collect().await;
        let merged = chunks
            .into_iter()
            .map(|c| String::from_utf8_lossy(c.unwrap().as_ref()).to_string())
            .collect::<String>();

        assert!(
            merged.contains("你好世界"),
            "expected '你好世界' in output, got replacement chars (U+FFFD)"
        );
        assert!(
            !merged.contains('\u{FFFD}'),
            "output must not contain U+FFFD replacement characters"
        );
    }
}
