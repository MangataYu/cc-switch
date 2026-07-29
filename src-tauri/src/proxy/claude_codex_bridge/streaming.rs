use super::BridgeError;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ItemId(pub String);

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CallId(pub String);

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodexUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub total_tokens: u64,
    pub cached_input_tokens: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CodexResponseEventKind {
    ResponseStarted,
    ReasoningStarted,
    ReasoningDelta,
    ReasoningDone,
    ToolCallStarted,
    ToolArgumentsDelta,
    ToolCallDone,
    TextStarted,
    TextDelta,
    TextDone,
    UsageUpdated,
    ResponseCompleted,
    ResponseFailed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CodexResponseEvent {
    ResponseStarted {
        response_id: String,
        model: String,
        usage: Option<CodexUsage>,
    },
    ReasoningStarted {
        item_id: ItemId,
    },
    ReasoningDelta {
        item_id: ItemId,
        text: String,
    },
    ReasoningDone {
        item_id: ItemId,
        encrypted_content: Option<String>,
    },
    ToolCallStarted {
        item_id: ItemId,
        call_id: CallId,
        codex_name: String,
    },
    ToolArgumentsDelta {
        item_id: ItemId,
        call_id: Option<CallId>,
        bytes: Vec<u8>,
    },
    ToolCallDone {
        item_id: ItemId,
        call_id: Option<CallId>,
        arguments: Option<Vec<u8>>,
    },
    TextStarted {
        item_id: ItemId,
    },
    TextDelta {
        item_id: ItemId,
        text: String,
    },
    TextDone {
        item_id: ItemId,
    },
    UsageUpdated {
        usage: CodexUsage,
    },
    ResponseCompleted {
        status: String,
        stop_reason: Option<String>,
    },
    ResponseFailed {
        error_type: String,
        safe_message: String,
    },
}

impl CodexResponseEvent {
    pub fn kind(&self) -> CodexResponseEventKind {
        match self {
            Self::ResponseStarted { .. } => CodexResponseEventKind::ResponseStarted,
            Self::ReasoningStarted { .. } => CodexResponseEventKind::ReasoningStarted,
            Self::ReasoningDelta { .. } => CodexResponseEventKind::ReasoningDelta,
            Self::ReasoningDone { .. } => CodexResponseEventKind::ReasoningDone,
            Self::ToolCallStarted { .. } => CodexResponseEventKind::ToolCallStarted,
            Self::ToolArgumentsDelta { .. } => CodexResponseEventKind::ToolArgumentsDelta,
            Self::ToolCallDone { .. } => CodexResponseEventKind::ToolCallDone,
            Self::TextStarted { .. } => CodexResponseEventKind::TextStarted,
            Self::TextDelta { .. } => CodexResponseEventKind::TextDelta,
            Self::TextDone { .. } => CodexResponseEventKind::TextDone,
            Self::UsageUpdated { .. } => CodexResponseEventKind::UsageUpdated,
            Self::ResponseCompleted { .. } => CodexResponseEventKind::ResponseCompleted,
            Self::ResponseFailed { .. } => CodexResponseEventKind::ResponseFailed,
        }
    }
}

pub fn decode_codex_response_event(
    named_event: Option<&str>,
    payload: Value,
) -> Result<Vec<CodexResponseEvent>, BridgeError> {
    let payload_event = payload.get("type").and_then(Value::as_str);
    if let (Some(named), Some(typed)) = (named_event.filter(|v| !v.is_empty()), payload_event) {
        if named != typed {
            return Err(invalid(
                "event_type",
                "named event conflicts with payload type",
            ));
        }
    }
    let event_name = named_event
        .filter(|value| !value.is_empty())
        .or(payload_event)
        .ok_or_else(|| invalid("missing", "event type is required"))?;
    let response = response_object(&payload);
    let mut events = Vec::new();

    match event_name {
        "response.created" => {
            let response_id = required_string(response, "id", "response_started")?;
            let model = required_string(response, "model", "response_started")?;
            let usage = usage_from(response.get("usage"));
            events.push(CodexResponseEvent::ResponseStarted {
                response_id,
                model,
                usage,
            });
        }
        "response.output_item.added" => {
            let item = payload
                .get("item")
                .ok_or_else(|| invalid("item_started", "output item is required"))?;
            let item_id = required_item_id(item, &payload, "item_started")?;
            match item.get("type").and_then(Value::as_str) {
                Some("reasoning") => events.push(CodexResponseEvent::ReasoningStarted { item_id }),
                Some("function_call") => {
                    let call_id = CallId(required_string(item, "call_id", "tool_call_started")?);
                    let codex_name = required_string(item, "name", "tool_call_started")?;
                    events.push(CodexResponseEvent::ToolCallStarted {
                        item_id,
                        call_id,
                        codex_name,
                    });
                }
                Some("message") => events.push(CodexResponseEvent::TextStarted { item_id }),
                _ => return Err(invalid("item_started", "unsupported output item type")),
            }
        }
        "response.reasoning_summary_text.delta" | "response.reasoning_text.delta" => {
            events.push(CodexResponseEvent::ReasoningDelta {
                item_id: required_item_id(&payload, &payload, "reasoning_delta")?,
                text: required_string(&payload, "delta", "reasoning_delta")?,
            });
        }
        "response.reasoning.delta" => {
            events.push(CodexResponseEvent::ReasoningDelta {
                item_id: required_item_id(&payload, &payload, "reasoning_delta")?,
                text: payload
                    .get("delta")
                    .or_else(|| payload.get("text"))
                    .and_then(Value::as_str)
                    .filter(|value| !value.is_empty())
                    .map(str::to_string)
                    .ok_or_else(|| invalid("reasoning_delta", "reasoning delta is required"))?,
            });
        }
        "response.output_item.done" => {
            let item = payload
                .get("item")
                .ok_or_else(|| invalid("item_done", "completed output item is required"))?;
            let item_id = required_item_id(item, &payload, "item_done")?;
            match item.get("type").and_then(Value::as_str) {
                Some("reasoning") => events.push(CodexResponseEvent::ReasoningDone {
                    item_id,
                    encrypted_content: item
                        .get("encrypted_content")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                }),
                Some("function_call") => events.push(CodexResponseEvent::ToolCallDone {
                    item_id,
                    call_id: optional_call_id(item)?,
                    arguments: item
                        .get("arguments")
                        .and_then(Value::as_str)
                        .map(|value| value.as_bytes().to_vec()),
                }),
                Some("message") => events.push(CodexResponseEvent::TextDone { item_id }),
                _ => return Err(invalid("item_done", "unsupported completed item type")),
            }
        }
        "response.function_call_arguments.delta" => {
            events.push(CodexResponseEvent::ToolArgumentsDelta {
                item_id: required_item_id(&payload, &payload, "tool_arguments_delta")?,
                call_id: optional_call_id(&payload)?,
                bytes: required_string(&payload, "delta", "tool_arguments_delta")?.into_bytes(),
            });
        }
        "response.function_call_arguments.done" => {
            events.push(CodexResponseEvent::ToolCallDone {
                item_id: required_item_id(&payload, &payload, "tool_call_done")?,
                call_id: optional_call_id(&payload)?,
                arguments: payload
                    .get("arguments")
                    .or_else(|| payload.pointer("/item/arguments"))
                    .and_then(Value::as_str)
                    .map(|value| value.as_bytes().to_vec()),
            });
        }
        "response.content_part.added" => {
            let part_type = payload.pointer("/part/type").and_then(Value::as_str);
            if !matches!(part_type, Some("output_text" | "refusal")) {
                return Err(invalid("text_started", "unsupported content part type"));
            }
            events.push(CodexResponseEvent::TextStarted {
                item_id: required_item_id(&payload, &payload, "text_started")?,
            });
        }
        "response.output_text.delta" | "response.refusal.delta" => {
            events.push(CodexResponseEvent::TextDelta {
                item_id: required_item_id(&payload, &payload, "text_delta")?,
                text: required_string(&payload, "delta", "text_delta")?,
            });
        }
        "response.output_text.done" | "response.content_part.done" => {
            events.push(CodexResponseEvent::TextDone {
                item_id: required_item_id(&payload, &payload, "text_done")?,
            });
        }
        "response.completed" | "response.incomplete" => {
            if matches!(
                response.get("status").and_then(Value::as_str),
                Some("failed" | "cancelled")
            ) || response.get("error").is_some_and(|value| !value.is_null())
            {
                events.push(failed_event(response));
            } else {
                if let Some(usage) = usage_from(response.get("usage")) {
                    events.push(CodexResponseEvent::UsageUpdated { usage });
                }
                let status = response
                    .get("status")
                    .and_then(Value::as_str)
                    .unwrap_or(if event_name == "response.incomplete" {
                        "incomplete"
                    } else {
                        "completed"
                    })
                    .to_string();
                let stop_reason = response
                    .pointer("/incomplete_details/reason")
                    .and_then(Value::as_str)
                    .map(str::to_string);
                events.push(CodexResponseEvent::ResponseCompleted {
                    status,
                    stop_reason,
                });
            }
        }
        "response.failed" | "error" => events.push(failed_event(response)),
        "response.in_progress"
        | "response.reasoning_summary_part.added"
        | "response.reasoning_summary_part.done"
        | "response.reasoning_summary_text.done"
        | "response.reasoning_text.done" => {}
        _ => return Err(invalid("unknown", "unknown semantic Responses event")),
    }
    Ok(events)
}

fn response_object(payload: &Value) -> &Value {
    payload.get("response").unwrap_or(payload)
}

fn invalid(event_kind: &str, summary: &str) -> BridgeError {
    BridgeError::InvalidUpstreamEvent {
        event_kind: event_kind.to_string(),
        summary: summary.to_string(),
    }
}

fn required_string(value: &Value, key: &str, event_kind: &str) -> Result<String, BridgeError> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .ok_or_else(|| invalid(event_kind, "required event identity or content is missing"))
}

fn required_item_id(
    primary: &Value,
    envelope: &Value,
    event_kind: &str,
) -> Result<ItemId, BridgeError> {
    primary
        .get("id")
        .or_else(|| primary.get("item_id"))
        .or_else(|| envelope.get("item_id"))
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(|value| ItemId(value.to_string()))
        .or_else(|| {
            envelope
                .get("output_index")
                .and_then(Value::as_u64)
                .map(|index| ItemId(format!("output:{index}")))
        })
        .ok_or_else(|| invalid(event_kind, "item identity is required"))
}

fn optional_call_id(value: &Value) -> Result<Option<CallId>, BridgeError> {
    match value
        .get("call_id")
        .or_else(|| value.pointer("/item/call_id"))
    {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if !value.is_empty() => Ok(Some(CallId(value.clone()))),
        _ => Err(invalid("tool_identity", "call identity is invalid")),
    }
}

fn usage_from(value: Option<&Value>) -> Option<CodexUsage> {
    let value = value?;
    let object = value.as_object()?;
    let input_tokens = object
        .get("input_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let output_tokens = object
        .get("output_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let total_tokens = object
        .get("total_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(input_tokens.saturating_add(output_tokens));
    let cached_input_tokens = value
        .pointer("/input_tokens_details/cached_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    Some(CodexUsage {
        input_tokens,
        output_tokens,
        total_tokens,
        cached_input_tokens,
    })
}

fn failed_event(response: &Value) -> CodexResponseEvent {
    let error_type = response
        .pointer("/error/type")
        .or_else(|| response.get("type"))
        .and_then(Value::as_str)
        .filter(|value| {
            !value.is_empty()
                && value.len() <= 64
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
        })
        .unwrap_or("upstream_error")
        .to_string();
    CodexResponseEvent::ResponseFailed {
        error_type,
        safe_message: "Responses upstream reported a terminal failure".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn typed_responses_event_decodes_supported_lifecycle() {
        let cases = [
            (
                "response.created",
                json!({"type":"response.created","response":{"id":"resp_1","model":"gpt-5.6","usage":{"input_tokens":3,"output_tokens":0}}}),
                CodexResponseEventKind::ResponseStarted,
            ),
            (
                "response.output_item.added",
                json!({"type":"response.output_item.added","output_index":0,"item":{"id":"rs_1","type":"reasoning"}}),
                CodexResponseEventKind::ReasoningStarted,
            ),
            (
                "response.reasoning_summary_text.delta",
                json!({"type":"response.reasoning_summary_text.delta","item_id":"rs_1","delta":"think"}),
                CodexResponseEventKind::ReasoningDelta,
            ),
            (
                "response.output_item.done",
                json!({"type":"response.output_item.done","output_index":0,"item":{"id":"rs_1","type":"reasoning","encrypted_content":"opaque"}}),
                CodexResponseEventKind::ReasoningDone,
            ),
            (
                "response.output_item.added",
                json!({"type":"response.output_item.added","output_index":1,"item":{"id":"fc_1","type":"function_call","call_id":"call_1","name":"codex_tool"}}),
                CodexResponseEventKind::ToolCallStarted,
            ),
            (
                "response.function_call_arguments.delta",
                json!({"type":"response.function_call_arguments.delta","item_id":"fc_1","call_id":"call_1","delta":"{\"q\":"}),
                CodexResponseEventKind::ToolArgumentsDelta,
            ),
            (
                "response.function_call_arguments.done",
                json!({"type":"response.function_call_arguments.done","item_id":"fc_1","call_id":"call_1","arguments":"{\"q\":1}"}),
                CodexResponseEventKind::ToolCallDone,
            ),
            (
                "response.content_part.added",
                json!({"type":"response.content_part.added","item_id":"msg_1","part":{"type":"output_text","text":""}}),
                CodexResponseEventKind::TextStarted,
            ),
            (
                "response.output_text.delta",
                json!({"type":"response.output_text.delta","item_id":"msg_1","delta":"hello"}),
                CodexResponseEventKind::TextDelta,
            ),
            (
                "response.output_text.done",
                json!({"type":"response.output_text.done","item_id":"msg_1","text":"hello"}),
                CodexResponseEventKind::TextDone,
            ),
            (
                "response.completed",
                json!({"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":3,"output_tokens":4}}}),
                CodexResponseEventKind::ResponseCompleted,
            ),
            (
                "response.failed",
                json!({"type":"response.failed","response":{"status":"failed","error":{"type":"server_error","message":"safe"}}}),
                CodexResponseEventKind::ResponseFailed,
            ),
        ];

        for (event_name, payload, expected) in cases {
            let decoded = decode_codex_response_event(Some(event_name), payload).unwrap();
            assert_eq!(decoded.last().map(CodexResponseEvent::kind), Some(expected));
        }
    }

    #[test]
    fn typed_responses_event_emits_usage_and_explicitly_ignores_metadata() {
        let decoded = decode_codex_response_event(
            Some("response.completed"),
            json!({"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":8,"output_tokens":5,"total_tokens":13}}}),
        )
        .unwrap();
        assert_eq!(
            decoded
                .iter()
                .map(CodexResponseEvent::kind)
                .collect::<Vec<_>>(),
            vec![
                CodexResponseEventKind::UsageUpdated,
                CodexResponseEventKind::ResponseCompleted
            ]
        );

        assert!(decode_codex_response_event(
            Some("response.reasoning_summary_part.added"),
            json!({"type":"response.reasoning_summary_part.added","item_id":"rs_1","part":{"type":"summary_text","text":""}}),
        )
        .unwrap()
        .is_empty());
        assert!(decode_codex_response_event(
            Some("response.in_progress"),
            json!({"type":"response.in_progress","response":{"id":"resp_1"}}),
        )
        .unwrap()
        .is_empty());
    }

    #[test]
    fn typed_responses_event_fails_closed_without_leaking_payload_content() {
        let secret = "sk-secret-tool-argument-plaintext";
        for (name, payload) in [
            (
                "response.future_semantic.delta",
                json!({"type":"response.future_semantic.delta","delta":secret}),
            ),
            (
                "response.output_text.delta",
                json!({"type":"response.output_text.delta","delta":secret}),
            ),
            (
                "response.function_call_arguments.delta",
                json!({"type":"response.function_call_arguments.delta","delta":secret}),
            ),
            (
                "response.output_item.added",
                json!({"type":"response.output_item.added","item":{"id":"fc_1","type":"function_call","call_id":"","name":"tool"},"secret":secret}),
            ),
        ] {
            let error = decode_codex_response_event(Some(name), payload).unwrap_err();
            assert!(matches!(error, BridgeError::InvalidUpstreamEvent { .. }));
            assert!(!error.to_string().contains(secret));
        }
    }
}
