use super::BridgeError;
use super::{PreparedCodexTurn, RestoredToolCall};
use crate::proxy::providers::{
    reasoning_bridge::anthropic_block_from_openai_reasoning_item,
    transform_responses::map_responses_stop_reason,
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::json;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::HashMap;

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
        sequence: Option<u64>,
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
        sequence: Option<u64>,
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
        sequence: Option<u64>,
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

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaudeUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_input_tokens: u64,
}

impl From<&CodexUsage> for ClaudeUsage {
    fn from(value: &CodexUsage) -> Self {
        Self {
            input_tokens: value.input_tokens.saturating_sub(value.cached_input_tokens),
            output_tokens: value.output_tokens,
            cache_read_input_tokens: value.cached_input_tokens,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClaudeContentBlock {
    Text,
    Thinking,
    ToolUse { id: String, name: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClaudeContentDelta {
    Text { text: String },
    Thinking { text: String },
    Signature { signature: String },
    InputJson { partial_json: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClaudeStreamEvent {
    MessageStart {
        id: String,
        model: String,
        usage: ClaudeUsage,
    },
    ContentBlockStart {
        index: u32,
        block: ClaudeContentBlock,
    },
    ContentBlockDelta {
        index: u32,
        delta: ClaudeContentDelta,
    },
    ContentBlockStop {
        index: u32,
    },
    MessageDelta {
        stop_reason: String,
        usage: ClaudeUsage,
    },
    MessageStop,
    Error {
        error_type: String,
        safe_message: String,
    },
}

pub fn encode_claude_stream_event(event: &ClaudeStreamEvent) -> Bytes {
    let (event_name, payload) = match event {
        ClaudeStreamEvent::MessageStart { id, model, usage } => (
            "message_start",
            json!({
                "type":"message_start",
                "message":{
                    "id":id,
                    "type":"message",
                    "role":"assistant",
                    "model":model,
                    "usage":claude_usage_json(usage)
                }
            }),
        ),
        ClaudeStreamEvent::ContentBlockStart { index, block } => {
            let content_block = match block {
                ClaudeContentBlock::Text => json!({"type":"text","text":""}),
                ClaudeContentBlock::Thinking => json!({"type":"thinking","thinking":""}),
                ClaudeContentBlock::ToolUse { id, name } => {
                    json!({"type":"tool_use","id":id,"name":name,"input":{}})
                }
            };
            (
                "content_block_start",
                json!({"type":"content_block_start","index":index,"content_block":content_block}),
            )
        }
        ClaudeStreamEvent::ContentBlockDelta { index, delta } => {
            let delta = match delta {
                ClaudeContentDelta::Text { text } => json!({"type":"text_delta","text":text}),
                ClaudeContentDelta::Thinking { text } => {
                    json!({"type":"thinking_delta","thinking":text})
                }
                ClaudeContentDelta::Signature { signature } => {
                    json!({"type":"signature_delta","signature":signature})
                }
                ClaudeContentDelta::InputJson { partial_json } => {
                    json!({"type":"input_json_delta","partial_json":partial_json})
                }
            };
            (
                "content_block_delta",
                json!({"type":"content_block_delta","index":index,"delta":delta}),
            )
        }
        ClaudeStreamEvent::ContentBlockStop { index } => (
            "content_block_stop",
            json!({"type":"content_block_stop","index":index}),
        ),
        ClaudeStreamEvent::MessageDelta { stop_reason, usage } => (
            "message_delta",
            json!({
                "type":"message_delta",
                "delta":{"stop_reason":stop_reason,"stop_sequence":null},
                "usage":claude_usage_json(usage)
            }),
        ),
        ClaudeStreamEvent::MessageStop => ("message_stop", json!({"type":"message_stop"})),
        ClaudeStreamEvent::Error {
            error_type,
            safe_message,
        } => (
            "error",
            json!({"type":"error","error":{"type":error_type,"message":safe_message}}),
        ),
    };
    Bytes::from(format!(
        "event: {event_name}\ndata: {}\n\n",
        serde_json::to_string(&payload).unwrap_or_else(|_| "{}".to_string())
    ))
}

pub fn claude_stream_event_kind(event: &ClaudeStreamEvent) -> &'static str {
    match event {
        ClaudeStreamEvent::MessageStart { .. } => "message_start",
        ClaudeStreamEvent::ContentBlockStart { .. } => "content_block_start",
        ClaudeStreamEvent::ContentBlockDelta { .. } => "content_block_delta",
        ClaudeStreamEvent::ContentBlockStop { .. } => "content_block_stop",
        ClaudeStreamEvent::MessageDelta { .. } => "message_delta",
        ClaudeStreamEvent::MessageStop => "message_stop",
        ClaudeStreamEvent::Error { .. } => "error",
    }
}

fn claude_usage_json(usage: &ClaudeUsage) -> Value {
    let mut value = json!({
        "input_tokens":usage.input_tokens,
        "output_tokens":usage.output_tokens
    });
    if usage.cache_read_input_tokens > 0 {
        value["cache_read_input_tokens"] = json!(usage.cache_read_input_tokens);
    }
    value
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamVisibility {
    pub output_emitted: bool,
    pub tool_visible: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamTerminalState {
    AwaitingResponse,
    Streaming,
    Completed,
    Failed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamDecision {
    pub sequence: u64,
    pub event_kind: CodexResponseEventKind,
    pub item_identity_hash: Option<String>,
    pub call_identity_hash: Option<String>,
    pub state_before: StreamTerminalState,
    pub state_after: StreamTerminalState,
    pub output_visible: bool,
    pub tool_visible: bool,
}

#[derive(Debug)]
enum StreamItem {
    Text {
        index: u32,
        done: bool,
    },
    Reasoning {
        index: u32,
        text: String,
        done: bool,
        completion_hash: Option<String>,
    },
    Tool {
        index: u32,
        call_id: CallId,
        codex_name: String,
        arguments: Vec<u8>,
        done: bool,
        completion_hash: Option<String>,
    },
}

impl StreamItem {
    fn is_done(&self) -> bool {
        match self {
            Self::Text { done, .. } | Self::Reasoning { done, .. } | Self::Tool { done, .. } => {
                *done
            }
        }
    }
}

#[derive(Debug)]
pub struct PreparedCodexStream {
    turn: PreparedCodexTurn,
    state: StreamTerminalState,
    items: HashMap<ItemId, StreamItem>,
    next_content_index: u32,
    usage: CodexUsage,
    visibility: StreamVisibility,
    decisions: Vec<StreamDecision>,
    event_sequence: u64,
    seen_delta_sequences: HashMap<u64, String>,
    terminal_hash: Option<String>,
    has_tool: bool,
}

impl PreparedCodexStream {
    pub(crate) fn new(turn: PreparedCodexTurn) -> Self {
        Self {
            turn,
            state: StreamTerminalState::AwaitingResponse,
            items: HashMap::new(),
            next_content_index: 0,
            usage: CodexUsage::default(),
            visibility: StreamVisibility::default(),
            decisions: Vec::new(),
            event_sequence: 0,
            seen_delta_sequences: HashMap::new(),
            terminal_hash: None,
            has_tool: false,
        }
    }

    pub fn apply(
        &mut self,
        event: CodexResponseEvent,
    ) -> Result<Vec<ClaudeStreamEvent>, BridgeError> {
        let state_before = self.state;
        let kind = event.kind();
        let event_hash = event_fingerprint(&event);
        let item_hash = event_item_id(&event).map(|value| safe_hash(value.0.as_bytes()));
        let call_hash = event_call_id(&event).map(|value| safe_hash(value.0.as_bytes()));
        if let Some(sequence) = event_delta_sequence(&event) {
            if let Some(existing) = self.seen_delta_sequences.get(&sequence) {
                if existing == &event_hash {
                    return Ok(Vec::new());
                }
                return Err(invalid(
                    "duplicate_delta",
                    "delta sequence content conflicts",
                ));
            }
            self.seen_delta_sequences
                .insert(sequence, event_hash.clone());
        }
        if matches!(
            self.state,
            StreamTerminalState::Completed | StreamTerminalState::Failed
        ) {
            if matches!(
                kind,
                CodexResponseEventKind::ResponseCompleted | CodexResponseEventKind::ResponseFailed
            ) && self.terminal_hash.as_deref() == Some(event_hash.as_str())
            {
                return Ok(Vec::new());
            }
            return Err(invalid(
                "terminal",
                "semantic event arrived after terminal response",
            ));
        }

        let output = self.apply_non_terminal(event)?;
        if matches!(
            self.state,
            StreamTerminalState::Completed | StreamTerminalState::Failed
        ) {
            self.terminal_hash = Some(event_hash);
        }
        self.event_sequence = self.event_sequence.saturating_add(1);
        self.decisions.push(StreamDecision {
            sequence: self.event_sequence,
            event_kind: kind,
            item_identity_hash: item_hash,
            call_identity_hash: call_hash,
            state_before,
            state_after: self.state,
            output_visible: self.visibility.output_emitted,
            tool_visible: self.visibility.tool_visible,
        });
        Ok(output)
    }

    fn apply_non_terminal(
        &mut self,
        event: CodexResponseEvent,
    ) -> Result<Vec<ClaudeStreamEvent>, BridgeError> {
        if self.state == StreamTerminalState::AwaitingResponse
            && !matches!(event, CodexResponseEvent::ResponseStarted { .. })
        {
            return Err(invalid(
                "response",
                "stream must begin with response.started",
            ));
        }
        match event {
            CodexResponseEvent::ResponseStarted {
                response_id,
                model,
                usage,
            } => {
                if self.state != StreamTerminalState::AwaitingResponse {
                    return Err(invalid(
                        "response_started",
                        "response started more than once",
                    ));
                }
                if let Some(usage) = usage {
                    self.usage = usage;
                }
                self.state = StreamTerminalState::Streaming;
                Ok(vec![ClaudeStreamEvent::MessageStart {
                    id: response_id,
                    model,
                    usage: ClaudeUsage::from(&self.usage),
                }])
            }
            CodexResponseEvent::TextStarted { item_id } => {
                if let Some(existing) = self.items.get(&item_id) {
                    return match existing {
                        StreamItem::Text { .. } => Ok(Vec::new()),
                        _ => Err(invalid("item_type", "item changed semantic type")),
                    };
                }
                let index = self.allocate_index();
                self.items
                    .insert(item_id, StreamItem::Text { index, done: false });
                Ok(vec![ClaudeStreamEvent::ContentBlockStart {
                    index,
                    block: ClaudeContentBlock::Text,
                }])
            }
            CodexResponseEvent::TextDelta { item_id, text, .. } => {
                let item = self
                    .items
                    .get(&item_id)
                    .ok_or_else(|| invalid("text_delta", "text item was not started"))?;
                let StreamItem::Text { index, done } = item else {
                    return Err(invalid("item_type", "item changed semantic type"));
                };
                if *done {
                    return Err(invalid(
                        "text_delta",
                        "text delta arrived after text completion",
                    ));
                }
                Ok(vec![ClaudeStreamEvent::ContentBlockDelta {
                    index: *index,
                    delta: ClaudeContentDelta::Text { text },
                }])
            }
            CodexResponseEvent::TextDone { item_id } => {
                let item = self
                    .items
                    .get_mut(&item_id)
                    .ok_or_else(|| invalid("text_done", "text item was not started"))?;
                let StreamItem::Text { index, done } = item else {
                    return Err(invalid("item_type", "item changed semantic type"));
                };
                if *done {
                    return Ok(Vec::new());
                }
                *done = true;
                Ok(vec![ClaudeStreamEvent::ContentBlockStop { index: *index }])
            }
            CodexResponseEvent::ReasoningStarted { item_id } => {
                if let Some(existing) = self.items.get(&item_id) {
                    return match existing {
                        StreamItem::Reasoning { .. } => Ok(Vec::new()),
                        _ => Err(invalid("item_type", "item changed semantic type")),
                    };
                }
                let index = self.allocate_index();
                self.items.insert(
                    item_id,
                    StreamItem::Reasoning {
                        index,
                        text: String::new(),
                        done: false,
                        completion_hash: None,
                    },
                );
                Ok(vec![ClaudeStreamEvent::ContentBlockStart {
                    index,
                    block: ClaudeContentBlock::Thinking,
                }])
            }
            CodexResponseEvent::ReasoningDelta { item_id, text, .. } => {
                let item = self
                    .items
                    .get_mut(&item_id)
                    .ok_or_else(|| invalid("reasoning_delta", "reasoning item was not started"))?;
                let StreamItem::Reasoning {
                    index,
                    text: accumulated,
                    done,
                    ..
                } = item
                else {
                    return Err(invalid("item_type", "item changed semantic type"));
                };
                if *done {
                    return Err(invalid(
                        "reasoning_delta",
                        "reasoning delta arrived after completion",
                    ));
                }
                accumulated.push_str(&text);
                Ok(vec![ClaudeStreamEvent::ContentBlockDelta {
                    index: *index,
                    delta: ClaudeContentDelta::Thinking { text },
                }])
            }
            CodexResponseEvent::ReasoningDone {
                item_id,
                encrypted_content,
            } => {
                let item = self
                    .items
                    .get_mut(&item_id)
                    .ok_or_else(|| invalid("reasoning_done", "reasoning item was not started"))?;
                let StreamItem::Reasoning {
                    index,
                    text,
                    done,
                    completion_hash,
                } = item
                else {
                    return Err(invalid("item_type", "item changed semantic type"));
                };
                let hash = safe_hash(encrypted_content.as_deref().unwrap_or("").as_bytes());
                if *done {
                    return if completion_hash.as_deref() == Some(hash.as_str()) {
                        Ok(Vec::new())
                    } else {
                        Err(invalid("reasoning_done", "reasoning completion conflicts"))
                    };
                }
                self.turn
                    .observe_reasoning_completion(&item_id.0, encrypted_content.as_deref())?;
                let reasoning_item = json!({
                    "id": item_id.0,
                    "type":"reasoning",
                    "summary":[{"type":"summary_text","text":text}],
                    "encrypted_content":encrypted_content
                });
                let signature = anthropic_block_from_openai_reasoning_item(&reasoning_item)
                    .and_then(|block| {
                        block
                            .get("signature")
                            .and_then(Value::as_str)
                            .map(str::to_string)
                    });
                text.clear();
                *done = true;
                *completion_hash = Some(hash);
                let mut output = Vec::new();
                if let Some(signature) = signature {
                    output.push(ClaudeStreamEvent::ContentBlockDelta {
                        index: *index,
                        delta: ClaudeContentDelta::Signature { signature },
                    });
                }
                output.push(ClaudeStreamEvent::ContentBlockStop { index: *index });
                Ok(output)
            }
            CodexResponseEvent::ToolCallStarted {
                item_id,
                call_id,
                codex_name,
            } => {
                self.turn.tool_registry.claude_name_for_codex(&codex_name)?;
                if let Some(existing) = self.items.get(&item_id) {
                    return match existing {
                        StreamItem::Tool {
                            call_id: existing_id,
                            codex_name: existing_name,
                            ..
                        } if existing_id == &call_id && existing_name == &codex_name => {
                            Ok(Vec::new())
                        }
                        StreamItem::Tool { .. } => {
                            Err(invalid("tool_identity", "tool item identity conflicts"))
                        }
                        _ => Err(invalid("item_type", "item changed semantic type")),
                    };
                }
                self.turn
                    .declare_streamed_tool_call(&codex_name, &call_id.0)?;
                let index = self.allocate_index();
                self.items.insert(
                    item_id,
                    StreamItem::Tool {
                        index,
                        call_id,
                        codex_name,
                        arguments: Vec::new(),
                        done: false,
                        completion_hash: None,
                    },
                );
                self.has_tool = true;
                Ok(Vec::new())
            }
            CodexResponseEvent::ToolArgumentsDelta {
                item_id,
                call_id,
                bytes,
                ..
            } => {
                let item = self
                    .items
                    .get_mut(&item_id)
                    .ok_or_else(|| invalid("tool_arguments_delta", "tool item was not started"))?;
                let StreamItem::Tool {
                    call_id: expected,
                    arguments,
                    done,
                    ..
                } = item
                else {
                    return Err(invalid("item_type", "item changed semantic type"));
                };
                if call_id.as_ref().is_some_and(|value| value != expected) {
                    return Err(invalid("tool_identity", "call identity conflicts"));
                }
                if *done {
                    return Err(invalid(
                        "tool_arguments_delta",
                        "tool arguments arrived after completion",
                    ));
                }
                self.turn
                    .observe_streamed_argument_fragment(&expected.0, &safe_hash(&bytes))?;
                arguments.extend(bytes);
                Ok(Vec::new())
            }
            CodexResponseEvent::ToolCallDone {
                item_id,
                call_id,
                arguments,
            } => {
                let item = self
                    .items
                    .get_mut(&item_id)
                    .ok_or_else(|| invalid("tool_call_done", "tool item was not started"))?;
                let StreamItem::Tool {
                    index,
                    call_id: expected,
                    codex_name,
                    arguments: buffered,
                    done,
                    completion_hash,
                } = item
                else {
                    return Err(invalid("item_type", "item changed semantic type"));
                };
                if call_id.as_ref().is_some_and(|value| value != expected) {
                    return Err(invalid("tool_identity", "call identity conflicts"));
                }
                let complete = arguments.clone().unwrap_or_else(|| buffered.clone());
                if arguments
                    .as_ref()
                    .is_some_and(|value| !buffered.is_empty() && value != buffered)
                {
                    return Err(invalid(
                        "tool_call_done",
                        "completed tool arguments conflict with streamed bytes",
                    ));
                }
                if *done {
                    if arguments.is_none() {
                        return Ok(Vec::new());
                    }
                    let hash = safe_hash(&complete);
                    return if completion_hash.as_deref() == Some(hash.as_str()) {
                        Ok(Vec::new())
                    } else {
                        Err(invalid("tool_call_done", "tool completion conflicts"))
                    };
                }
                let hash = safe_hash(&complete);
                let raw = std::str::from_utf8(&complete)
                    .map_err(|_| invalid("tool_call_done", "tool arguments are not valid UTF-8"))?
                    .to_string();
                let restored =
                    self.turn
                        .complete_streamed_tool_call(codex_name, &expected.0, &raw)?;
                buffered.clear();
                *done = true;
                *completion_hash = Some(hash);
                Ok(tool_events(*index, &restored, &raw))
            }
            CodexResponseEvent::UsageUpdated { usage } => {
                if usage.input_tokens < self.usage.input_tokens
                    || usage.output_tokens < self.usage.output_tokens
                {
                    return Err(invalid("usage", "usage counters regressed"));
                }
                self.usage = usage;
                Ok(Vec::new())
            }
            CodexResponseEvent::ResponseCompleted {
                status,
                stop_reason,
            } => {
                if self.items.values().any(|item| !item.is_done()) {
                    return Err(invalid(
                        "response_completed",
                        "terminal response has unfinished output items",
                    ));
                }
                let mapped =
                    map_responses_stop_reason(Some(&status), self.has_tool, stop_reason.as_deref())
                        .unwrap_or("end_turn")
                        .to_string();
                self.state = StreamTerminalState::Completed;
                Ok(vec![
                    ClaudeStreamEvent::MessageDelta {
                        stop_reason: mapped,
                        usage: ClaudeUsage::from(&self.usage),
                    },
                    ClaudeStreamEvent::MessageStop,
                ])
            }
            CodexResponseEvent::ResponseFailed {
                error_type,
                safe_message,
            } => {
                self.state = StreamTerminalState::Failed;
                Ok(vec![ClaudeStreamEvent::Error {
                    error_type,
                    safe_message,
                }])
            }
        }
    }

    pub fn acknowledge_emitted(&mut self, event: &ClaudeStreamEvent) -> Result<(), BridgeError> {
        if matches!(
            event,
            ClaudeStreamEvent::MessageStart { .. }
                | ClaudeStreamEvent::ContentBlockStart { .. }
                | ClaudeStreamEvent::ContentBlockDelta { .. }
                | ClaudeStreamEvent::ContentBlockStop { .. }
                | ClaudeStreamEvent::MessageDelta { .. }
                | ClaudeStreamEvent::MessageStop
                | ClaudeStreamEvent::Error { .. }
        ) {
            self.visibility.output_emitted = true;
        }
        if let ClaudeStreamEvent::ContentBlockStart {
            index,
            block: ClaudeContentBlock::ToolUse { .. },
        } = event
        {
            let call_id = self.items.values().find_map(|item| match item {
                StreamItem::Tool {
                    index: item_index,
                    call_id,
                    ..
                } if item_index == index => Some(call_id.0.clone()),
                _ => None,
            });
            let call_id = call_id.ok_or_else(|| {
                invalid("claude_encoder", "visible tool block has no validated call")
            })?;
            self.turn.mark_streamed_tool_visible(&call_id)?;
            self.visibility.tool_visible = true;
        }
        Ok(())
    }

    pub fn finish(&self) -> Result<StreamTerminalState, BridgeError> {
        if matches!(
            self.state,
            StreamTerminalState::Completed | StreamTerminalState::Failed
        ) {
            Ok(self.state)
        } else {
            Err(BridgeError::IncompleteStream {
                summary: "stream ended without a terminal response event".to_string(),
            })
        }
    }

    pub fn visibility(&self) -> StreamVisibility {
        self.visibility
    }

    pub fn decisions(&self) -> &[StreamDecision] {
        &self.decisions
    }

    pub fn terminal_state(&self) -> StreamTerminalState {
        self.state
    }

    #[cfg(test)]
    pub fn open_item_count(&self) -> usize {
        self.items.values().filter(|item| !item.is_done()).count()
    }

    fn allocate_index(&mut self) -> u32 {
        let index = self.next_content_index;
        self.next_content_index = self.next_content_index.saturating_add(1);
        index
    }
}

fn tool_events(index: u32, restored: &RestoredToolCall, raw: &str) -> Vec<ClaudeStreamEvent> {
    vec![
        ClaudeStreamEvent::ContentBlockStart {
            index,
            block: ClaudeContentBlock::ToolUse {
                id: restored.tool_use_id.clone(),
                name: restored.claude_name.clone(),
            },
        },
        ClaudeStreamEvent::ContentBlockDelta {
            index,
            delta: ClaudeContentDelta::InputJson {
                partial_json: raw.to_string(),
            },
        },
        ClaudeStreamEvent::ContentBlockStop { index },
    ]
}

fn event_delta_sequence(event: &CodexResponseEvent) -> Option<u64> {
    match event {
        CodexResponseEvent::ReasoningDelta { sequence, .. }
        | CodexResponseEvent::ToolArgumentsDelta { sequence, .. }
        | CodexResponseEvent::TextDelta { sequence, .. } => *sequence,
        _ => None,
    }
}

fn event_item_id(event: &CodexResponseEvent) -> Option<&ItemId> {
    match event {
        CodexResponseEvent::ReasoningStarted { item_id }
        | CodexResponseEvent::ReasoningDelta { item_id, .. }
        | CodexResponseEvent::ReasoningDone { item_id, .. }
        | CodexResponseEvent::ToolCallStarted { item_id, .. }
        | CodexResponseEvent::ToolArgumentsDelta { item_id, .. }
        | CodexResponseEvent::ToolCallDone { item_id, .. }
        | CodexResponseEvent::TextStarted { item_id }
        | CodexResponseEvent::TextDelta { item_id, .. }
        | CodexResponseEvent::TextDone { item_id } => Some(item_id),
        _ => None,
    }
}

fn event_call_id(event: &CodexResponseEvent) -> Option<&CallId> {
    match event {
        CodexResponseEvent::ToolCallStarted { call_id, .. } => Some(call_id),
        CodexResponseEvent::ToolArgumentsDelta { call_id, .. }
        | CodexResponseEvent::ToolCallDone { call_id, .. } => call_id.as_ref(),
        _ => None,
    }
}

pub fn event_identity_hashes(event: &CodexResponseEvent) -> (Option<String>, Option<String>) {
    (
        event_item_id(event).map(|value| safe_hash(value.0.as_bytes())),
        event_call_id(event).map(|value| safe_hash(value.0.as_bytes())),
    )
}

fn event_fingerprint(event: &CodexResponseEvent) -> String {
    safe_hash(format!("{event:?}").as_bytes())
}

fn safe_hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
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
                sequence: event_sequence(&payload),
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
                sequence: event_sequence(&payload),
            });
        }
        "response.reasoning.done" => {
            let item = payload.get("item").unwrap_or(&payload);
            events.push(CodexResponseEvent::ReasoningDone {
                item_id: required_item_id(item, &payload, "reasoning_done")?,
                encrypted_content: item
                    .get("encrypted_content")
                    .or_else(|| payload.get("encrypted_content"))
                    .and_then(Value::as_str)
                    .map(str::to_string),
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
                sequence: event_sequence(&payload),
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
                sequence: event_sequence(&payload),
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

fn event_sequence(value: &Value) -> Option<u64> {
    value.get("sequence_number").and_then(Value::as_u64)
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
    use crate::{
        app_config::AppType,
        provider::{Provider, ProviderMeta},
        proxy::claude_codex_bridge::{ClaudeCodexBridge, ConversationLedger, ToolCallState},
    };
    use serde_json::json;

    fn provider() -> Provider {
        Provider {
            id: "strict-stream-provider".to_string(),
            name: "Strict Stream Provider".to_string(),
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
        }
    }

    fn prepared_turn() -> crate::proxy::claude_codex_bridge::PreparedCodexTurn {
        ClaudeCodexBridge::with_ledger(ConversationLedger::default())
            .prepare_turn(
                &AppType::Claude,
                json!({
                    "model":"gpt-5.6",
                    "max_tokens":128,
                    "messages":[{"role":"user","content":"fixture"}],
                    "tools":[{
                        "name":"lookup",
                        "description":"Lookup a value",
                        "input_schema":{
                            "type":"object",
                            "properties":{"q":{"type":"string"}},
                            "required":["q"],
                            "additionalProperties":false
                        }
                    }]
                }),
                &provider(),
                Some("strict-stream-session"),
            )
            .unwrap()
    }

    fn start_event() -> CodexResponseEvent {
        CodexResponseEvent::ResponseStarted {
            response_id: "resp_1".to_string(),
            model: "gpt-5.6".to_string(),
            usage: Some(CodexUsage {
                input_tokens: 3,
                ..CodexUsage::default()
            }),
        }
    }

    fn acknowledge_all(stream: &mut PreparedCodexStream, events: &[ClaudeStreamEvent]) {
        for event in events {
            stream.acknowledge_emitted(event).unwrap();
        }
    }

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
    fn legacy_reasoning_done_requires_identity_and_preserves_encrypted_content() {
        let decoded = decode_codex_response_event(
            Some("response.reasoning.done"),
            json!({
                "type":"response.reasoning.done",
                "item_id":"reasoning_1",
                "encrypted_content":"opaque-signature"
            }),
        )
        .unwrap();
        assert_eq!(
            decoded,
            vec![CodexResponseEvent::ReasoningDone {
                item_id: ItemId("reasoning_1".to_string()),
                encrypted_content: Some("opaque-signature".to_string()),
            }]
        );

        assert!(matches!(
            decode_codex_response_event(
                Some("response.reasoning.done"),
                json!({"type":"response.reasoning.done"}),
            ),
            Err(BridgeError::InvalidUpstreamEvent { .. })
        ));
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

    #[test]
    fn strict_stream_state_validates_text_and_terminal_lifecycle() {
        let mut stream = prepared_turn().start_stream();
        let started = stream.apply(start_event()).unwrap();
        assert!(matches!(
            started.as_slice(),
            [ClaudeStreamEvent::MessageStart { .. }]
        ));
        acknowledge_all(&mut stream, &started);

        let item = ItemId("msg_1".to_string());
        let opened = stream
            .apply(CodexResponseEvent::TextStarted {
                item_id: item.clone(),
            })
            .unwrap();
        acknowledge_all(&mut stream, &opened);
        let delta = stream
            .apply(CodexResponseEvent::TextDelta {
                item_id: item.clone(),
                text: "hello".to_string(),
                sequence: Some(4),
            })
            .unwrap();
        acknowledge_all(&mut stream, &delta);
        let duplicate = stream
            .apply(CodexResponseEvent::TextDelta {
                item_id: item.clone(),
                text: "hello".to_string(),
                sequence: Some(4),
            })
            .unwrap();
        assert!(duplicate.is_empty());
        stream
            .apply(CodexResponseEvent::TextDone {
                item_id: item.clone(),
            })
            .unwrap();

        let error = stream
            .apply(CodexResponseEvent::TextDelta {
                item_id: item,
                text: "late".to_string(),
                sequence: Some(5),
            })
            .unwrap_err();
        assert!(matches!(error, BridgeError::InvalidUpstreamEvent { .. }));

        let completed = CodexResponseEvent::ResponseCompleted {
            status: "completed".to_string(),
            stop_reason: None,
        };
        let terminal = stream.apply(completed.clone()).unwrap();
        assert!(matches!(
            terminal.last(),
            Some(ClaudeStreamEvent::MessageStop)
        ));
        acknowledge_all(&mut stream, &terminal);
        assert!(stream.apply(completed).unwrap().is_empty());
        assert_eq!(stream.finish().unwrap(), StreamTerminalState::Completed);
        assert!(stream.visibility().output_emitted);
        assert!(!stream.visibility().tool_visible);
    }

    #[test]
    fn strict_stream_state_rejects_conflicting_duplicate_and_incomplete_eof() {
        let mut stream = prepared_turn().start_stream();
        stream.apply(start_event()).unwrap();
        let item = ItemId("msg_1".to_string());
        stream
            .apply(CodexResponseEvent::TextStarted {
                item_id: item.clone(),
            })
            .unwrap();
        stream
            .apply(CodexResponseEvent::TextDelta {
                item_id: item.clone(),
                text: "one".to_string(),
                sequence: Some(7),
            })
            .unwrap();
        let conflict = stream
            .apply(CodexResponseEvent::TextDelta {
                item_id: item,
                text: "two".to_string(),
                sequence: Some(7),
            })
            .unwrap_err();
        assert!(matches!(conflict, BridgeError::InvalidUpstreamEvent { .. }));
        assert!(matches!(
            stream.finish(),
            Err(BridgeError::IncompleteStream { .. })
        ));
    }

    #[test]
    fn strict_stream_state_validates_parallel_tools_before_visibility() {
        let prepared = prepared_turn();
        let binding = prepared.ledger_binding().clone();
        let ledger = prepared.ledger_for_test();
        let codex_name = prepared
            .tool_registry
            .codex_name_for_claude("lookup")
            .unwrap()
            .to_string();
        let mut stream = prepared.start_stream();
        stream.apply(start_event()).unwrap();

        for suffix in ["1", "2"] {
            stream
                .apply(CodexResponseEvent::ToolCallStarted {
                    item_id: ItemId(format!("fc_{suffix}")),
                    call_id: CallId(format!("call_{suffix}")),
                    codex_name: codex_name.clone(),
                })
                .unwrap();
        }
        for suffix in ["2", "1"] {
            stream
                .apply(CodexResponseEvent::ToolArgumentsDelta {
                    item_id: ItemId(format!("fc_{suffix}")),
                    call_id: Some(CallId(format!("call_{suffix}"))),
                    bytes: format!("{{\"q\":\"{suffix}\"}}").into_bytes(),
                    sequence: Some(if suffix == "1" { 11 } else { 10 }),
                })
                .unwrap();
        }
        for suffix in ["1", "2"] {
            let events = stream
                .apply(CodexResponseEvent::ToolCallDone {
                    item_id: ItemId(format!("fc_{suffix}")),
                    call_id: Some(CallId(format!("call_{suffix}"))),
                    arguments: None,
                })
                .unwrap();
            assert_eq!(
                ledger.call_state(&binding, &format!("call_{suffix}")),
                Some(ToolCallState::Ready)
            );
            acknowledge_all(&mut stream, &events);
            assert_eq!(
                ledger.call_state(&binding, &format!("call_{suffix}")),
                Some(ToolCallState::ReturnedToClaude)
            );
        }
        assert!(stream.visibility().tool_visible);
        assert_eq!(stream.open_item_count(), 0);
    }

    #[test]
    fn strict_stream_state_binds_reasoning_and_rejects_open_item_completion() {
        let mut stream = prepared_turn().start_stream();
        stream.apply(start_event()).unwrap();
        let item = ItemId("rs_1".to_string());
        stream
            .apply(CodexResponseEvent::ReasoningStarted {
                item_id: item.clone(),
            })
            .unwrap();
        stream
            .apply(CodexResponseEvent::ReasoningDelta {
                item_id: item.clone(),
                text: "think".to_string(),
                sequence: Some(2),
            })
            .unwrap();
        assert!(matches!(
            stream.apply(CodexResponseEvent::ResponseCompleted {
                status: "completed".to_string(),
                stop_reason: None,
            }),
            Err(BridgeError::InvalidUpstreamEvent { .. })
        ));
        let completed = CodexResponseEvent::ReasoningDone {
            item_id: item,
            encrypted_content: Some("opaque".to_string()),
        };
        let done = stream.apply(completed.clone()).unwrap();
        assert!(done.iter().any(|event| matches!(
            event,
            ClaudeStreamEvent::ContentBlockDelta {
                delta: ClaudeContentDelta::Signature { .. },
                ..
            }
        )));
        assert!(stream.apply(completed).unwrap().is_empty());
        assert!(matches!(
            stream.apply(CodexResponseEvent::ReasoningDone {
                item_id: ItemId("rs_1".to_string()),
                encrypted_content: Some("different".to_string()),
            }),
            Err(BridgeError::InvalidUpstreamEvent { .. })
        ));
    }
}
