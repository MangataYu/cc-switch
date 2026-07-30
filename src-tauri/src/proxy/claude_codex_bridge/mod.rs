mod capabilities;
mod conversation_ledger;
mod error;
mod schema;
pub mod streaming;
mod tools;

pub use capabilities::*;
pub use conversation_ledger::*;
pub use error::*;
pub use schema::*;
pub use tools::*;

use crate::{
    app_config::AppType,
    provider::Provider,
    proxy::providers::{
        read_trace::ReadTrace, tool_compat::ReadOffsetProtection,
        transform_claude_request_for_api_format, transform_responses,
    },
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    sync::{Arc, OnceLock},
};

use crate::proxy::json_canonical::canonical_json_string;

#[derive(Clone, Debug)]
pub struct ClaudeCodexBridge {
    capabilities: Arc<CodexOAuthCapabilities>,
    ledger: ConversationLedger,
}

#[derive(Clone, Debug)]
pub struct PreparedCodexTurn {
    pub request: Value,
    pub turn_id: String,
    pub tool_registry: Arc<ToolRegistry>,
    pub capability_snapshot: Arc<CodexOAuthCapabilities>,
    pub negotiation_report: NegotiationReport,
    pub reused_turn: bool,
    ledger: ConversationLedger,
    ledger_binding: TurnBinding,
    provider_hash: String,
    model_hash: String,
}

static BUILTIN_LEDGER: OnceLock<ConversationLedger> = OnceLock::new();

#[derive(Clone)]
struct HistoricalToolUse {
    claude_name: String,
    arguments_hash: String,
}

struct HistoricalToolResult {
    call_id: String,
    result_hash: String,
    matching_use: Option<HistoricalToolUse>,
}

pub fn canonical_request_fingerprint(request: &Value) -> String {
    format!(
        "{:x}",
        Sha256::digest(canonical_json_string(request).as_bytes())
    )
}

pub fn history_fingerprints(request: &Value) -> Vec<String> {
    request
        .get("messages")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(canonical_request_fingerprint)
        .collect()
}

fn stable_identity_hash(session_identity: &str) -> String {
    format!("{:x}", Sha256::digest(session_identity.as_bytes()))
}

pub fn bridge_scope_matches(app_type: &AppType, provider: &Provider) -> bool {
    matches!(app_type, AppType::Claude)
        && provider.is_codex_oauth()
        && provider
            .meta
            .as_ref()
            .and_then(|meta| meta.api_format.as_deref())
            == Some("openai_responses")
}

impl ClaudeCodexBridge {
    pub fn builtin() -> Self {
        Self {
            capabilities: CodexOAuthCapabilities::builtin(),
            ledger: BUILTIN_LEDGER
                .get_or_init(ConversationLedger::default)
                .clone(),
        }
    }

    pub fn with_ledger(ledger: ConversationLedger) -> Self {
        Self {
            capabilities: CodexOAuthCapabilities::builtin(),
            ledger,
        }
    }

    pub fn ledger(&self) -> &ConversationLedger {
        &self.ledger
    }

    #[cfg(test)]
    pub fn prepare_turn(
        &self,
        app_type: &AppType,
        request: Value,
        provider: &Provider,
        session_id: Option<&str>,
    ) -> Result<PreparedCodexTurn, BridgeError> {
        let fingerprint = canonical_request_fingerprint(&request);
        let fallback_identity = format!("anonymous:{fingerprint}");
        self.prepare_turn_with_session_identity(
            app_type,
            request,
            provider,
            session_id.unwrap_or(&fallback_identity),
            session_id,
        )
    }

    pub fn prepare_turn_with_session_identity(
        &self,
        app_type: &AppType,
        request: Value,
        provider: &Provider,
        session_identity: &str,
        client_session_id: Option<&str>,
    ) -> Result<PreparedCodexTurn, BridgeError> {
        if !bridge_scope_matches(app_type, provider) {
            return Err(BridgeError::OutOfScope);
        }

        let session_identity_hash = stable_identity_hash(session_identity);
        let historical_results = collect_historical_tool_results(&request)?;
        let request_fingerprint = canonical_request_fingerprint(&request);
        let history_fingerprints = history_fingerprints(&request);
        let provider_hash = stable_identity_hash(&provider.id);
        let model_hash = stable_identity_hash(
            request
                .get("model")
                .and_then(Value::as_str)
                .unwrap_or("unknown"),
        );

        let claude_tools = request
            .get("tools")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let forced_claude_tool = request
            .pointer("/tool_choice/name")
            .and_then(Value::as_str)
            .map(str::to_string);
        let existing = self
            .ledger
            .lookup_turn(&session_identity_hash, &request_fingerprint);
        let (registration, schema_losses) = if let Some(existing) = existing {
            let schema_losses = existing.schema_losses.clone();
            (existing, schema_losses)
        } else {
            let capability_snapshot = self.capabilities.clone();
            let (tool_registry, schema_losses) =
                ToolRegistry::compile(&claude_tools, capability_snapshot.as_ref())?;
            let registration = self.ledger.register_turn(
                &session_identity_hash,
                &request_fingerprint,
                Arc::new(tool_registry),
                capability_snapshot,
                schema_losses.clone(),
                &history_fingerprints,
            )?;
            (registration, schema_losses)
        };
        let tool_registry = registration.tool_registry.clone();
        let capability_snapshot = registration.capability_snapshot.clone();
        self.observe_tool_results(
            &session_identity_hash,
            &registration.binding,
            tool_registry.as_ref(),
            &historical_results,
        )?;
        let mut request = transform_claude_request_for_api_format(
            request,
            provider,
            "openai_responses",
            client_session_id,
            None,
        )?;
        request["tools"] = Value::Array(tool_registry.codex_tools().to_vec());
        if let Some(claude_name) = forced_claude_tool {
            request["tool_choice"] = serde_json::json!({
                "type": "function",
                "name": tool_registry.codex_name_for_claude(&claude_name)?
            });
        }
        let mut negotiation_report = capability_snapshot.negotiation_report();
        negotiation_report.schema_losses = schema_losses;

        Ok(PreparedCodexTurn {
            request,
            turn_id: registration.binding.turn_id.clone(),
            tool_registry,
            capability_snapshot,
            negotiation_report,
            reused_turn: registration.reused,
            ledger: self.ledger.clone(),
            ledger_binding: registration.binding,
            provider_hash,
            model_hash,
        })
    }

    fn observe_tool_results(
        &self,
        session_identity_hash: &str,
        turn_binding: &TurnBinding,
        tool_registry: &ToolRegistry,
        results: &[HistoricalToolResult],
    ) -> Result<(), BridgeError> {
        for result in results {
            match self.ledger.observe_result_for_session(
                session_identity_hash,
                &result.call_id,
                &result.result_hash,
            ) {
                Ok(()) => {}
                Err(BridgeError::ConversationStateConflict {
                    kind: ConversationConflictKind::OrphanToolResult,
                    ..
                }) => {
                    let historical_use = result.matching_use.as_ref().ok_or_else(|| {
                        BridgeError::ConversationStateConflict {
                            kind: ConversationConflictKind::OrphanToolResult,
                            summary: "tool_result has no matching tool_use identity".to_string(),
                        }
                    })?;
                    let codex_name = tool_registry
                        .codex_name_for_claude(&historical_use.claude_name)
                        .map_err(|_| BridgeError::ConversationStateConflict {
                            kind: ConversationConflictKind::UnknownToolIdentity,
                            summary: "historical tool_use is not registered for this turn"
                                .to_string(),
                        })?;
                    self.ledger
                        .declare_call(turn_binding, &result.call_id, codex_name)?;
                    self.ledger.arguments_streaming(
                        turn_binding,
                        &result.call_id,
                        &historical_use.arguments_hash,
                    )?;
                    self.ledger.mark_ready(
                        turn_binding,
                        &result.call_id,
                        &historical_use.arguments_hash,
                    )?;
                    self.ledger.mark_returned(turn_binding, &result.call_id)?;
                    self.ledger.observe_result(
                        turn_binding,
                        &result.call_id,
                        &result.result_hash,
                    )?;
                }
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }
}

fn collect_historical_tool_results(
    request: &Value,
) -> Result<Vec<HistoricalToolResult>, BridgeError> {
    let mut tool_uses: HashMap<String, HistoricalToolUse> = HashMap::new();
    let mut results = Vec::new();
    for message in request
        .get("messages")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        for block in message
            .get("content")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            match block.get("type").and_then(Value::as_str) {
                Some("tool_use") => {
                    let call_id = block.get("id").and_then(Value::as_str).unwrap_or("");
                    let claude_name = block.get("name").and_then(Value::as_str).unwrap_or("");
                    if call_id.is_empty() || claude_name.is_empty() {
                        return Err(BridgeError::ConversationStateConflict {
                            kind: ConversationConflictKind::CallIdConflict,
                            summary: "historical tool_use requires identity and name".to_string(),
                        });
                    }
                    let historical_use = HistoricalToolUse {
                        claude_name: claude_name.to_string(),
                        arguments_hash: canonical_request_fingerprint(
                            block.get("input").unwrap_or(&Value::Null),
                        ),
                    };
                    if let Some(existing) = tool_uses.get(call_id) {
                        if existing.claude_name != historical_use.claude_name
                            || existing.arguments_hash != historical_use.arguments_hash
                        {
                            return Err(BridgeError::ConversationStateConflict {
                                kind: ConversationConflictKind::CallIdConflict,
                                summary: "historical call_id has conflicting tool_use identity"
                                    .to_string(),
                            });
                        }
                    } else {
                        tool_uses.insert(call_id.to_string(), historical_use);
                    }
                }
                Some("tool_result") => {
                    let call_id = block
                        .get("tool_use_id")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    results.push(HistoricalToolResult {
                        call_id: call_id.to_string(),
                        result_hash: canonical_request_fingerprint(block),
                        matching_use: tool_uses.get(call_id).cloned(),
                    });
                }
                _ => {}
            }
        }
    }
    Ok(results)
}

impl PreparedCodexTurn {
    pub fn start_stream(self) -> streaming::PreparedCodexStream {
        streaming::PreparedCodexStream::new(self)
    }

    pub fn ledger_binding(&self) -> &TurnBinding {
        &self.ledger_binding
    }

    pub fn ledger_snapshot(
        &self,
        error_kind: Option<ConversationConflictKind>,
    ) -> Option<LedgerSnapshot> {
        self.ledger.snapshot(&self.ledger_binding, error_kind)
    }

    pub(crate) fn observe_returned_tool_call(
        &self,
        codex_name: &str,
        call_id: &str,
        arguments: &str,
    ) -> Result<(), BridgeError> {
        self.declare_streamed_tool_call(codex_name, call_id)?;
        let arguments_hash = canonical_request_fingerprint(
            &self
                .tool_registry
                .restore_call(codex_name, call_id, arguments)?
                .input,
        );
        self.ledger
            .arguments_streaming(&self.ledger_binding, call_id, &arguments_hash)?;
        self.complete_streamed_tool_call(codex_name, call_id, arguments)?;
        self.mark_streamed_tool_visible(call_id)
    }

    pub(crate) fn declare_streamed_tool_call(
        &self,
        codex_name: &str,
        call_id: &str,
    ) -> Result<(), BridgeError> {
        self.ledger
            .declare_call(&self.ledger_binding, call_id, codex_name)
    }

    pub(crate) fn observe_streamed_argument_fragment(
        &self,
        call_id: &str,
        fragment_hash: &str,
    ) -> Result<(), BridgeError> {
        self.ledger
            .arguments_streaming(&self.ledger_binding, call_id, fragment_hash)
    }

    pub(crate) fn complete_streamed_tool_call(
        &self,
        codex_name: &str,
        call_id: &str,
        arguments: &str,
    ) -> Result<RestoredToolCall, BridgeError> {
        let restored = self
            .tool_registry
            .restore_call(codex_name, call_id, arguments)?;
        let arguments_hash = canonical_request_fingerprint(&restored.input);
        self.ledger
            .mark_ready(&self.ledger_binding, call_id, &arguments_hash)?;
        Ok(restored)
    }

    pub(crate) fn mark_streamed_tool_visible(&self, call_id: &str) -> Result<(), BridgeError> {
        self.ledger.mark_returned(&self.ledger_binding, call_id)
    }

    pub(crate) fn observe_reasoning_item(&self, item: &Value) -> Result<(), BridgeError> {
        let item_id = item.get("id").and_then(Value::as_str).unwrap_or("");
        if item_id.is_empty() {
            return Err(BridgeError::ConversationStateConflict {
                kind: ConversationConflictKind::ReasoningBindingConflict,
                summary: "reasoning item requires a non-empty identity".to_string(),
            });
        }
        let encrypted = item.get("encrypted_content").and_then(Value::as_str);
        self.observe_reasoning_completion(item_id, encrypted)
    }

    pub(crate) fn observe_reasoning_completion(
        &self,
        item_id: &str,
        encrypted_content: Option<&str>,
    ) -> Result<(), BridgeError> {
        self.ledger.observe_reasoning(
            &self.ledger_binding,
            ReasoningBinding {
                item_id: item_id.to_string(),
                content_hash: canonical_request_fingerprint(
                    &encrypted_content.map(Value::from).unwrap_or(Value::Null),
                ),
                identity_hash: canonical_request_fingerprint(&serde_json::json!({
                    "id": item_id,
                    "type": "reasoning"
                })),
                provider_hash: self.provider_hash.clone(),
                model_hash: self.model_hash.clone(),
                capability_profile_version: self.capability_snapshot.profile_version.clone(),
            },
            ReasoningItemState::Completed,
        )
    }

    #[cfg(test)]
    pub(crate) fn ledger_for_test(&self) -> ConversationLedger {
        self.ledger.clone()
    }

    pub(crate) fn finalize_request(&mut self, request: Value) -> Result<(), BridgeError> {
        if request.get("tools") != self.request.get("tools") {
            return Err(BridgeError::ToolRegistryViolation {
                summary: "final request attempted to replace the frozen tool directory".to_string(),
            });
        }
        if request.get("tool_choice") != self.request.get("tool_choice") {
            return Err(BridgeError::ToolRegistryViolation {
                summary: "final request attempted to replace the frozen tool choice".to_string(),
            });
        }
        self.request = request;
        Ok(())
    }

    pub fn consume_response(
        &self,
        response: Value,
        read_offset_protection: Option<&ReadOffsetProtection>,
        read_trace: Option<&ReadTrace>,
    ) -> Result<Value, BridgeError> {
        for item in response
            .get("output")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter(|item| item.get("type").and_then(Value::as_str) == Some("reasoning"))
        {
            self.observe_reasoning_item(item)?;
        }
        let tool_calls = response
            .get("output")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter(|item| item.get("type").and_then(Value::as_str) == Some("function_call"))
            .map(|item| {
                let codex_name = item.get("name").and_then(Value::as_str).unwrap_or("");
                let call_id = item.get("call_id").and_then(Value::as_str).unwrap_or("");
                let arguments = item.get("arguments").and_then(Value::as_str).unwrap_or("");
                let restored = self
                    .tool_registry
                    .restore_call(codex_name, call_id, arguments)?;
                Ok((
                    call_id.to_string(),
                    codex_name.to_string(),
                    canonical_request_fingerprint(&restored.input),
                ))
            })
            .collect::<Result<Vec<_>, BridgeError>>()?;
        for (call_id, codex_name, arguments_hash) in &tool_calls {
            self.ledger
                .declare_call(&self.ledger_binding, call_id, codex_name)?;
            self.ledger
                .arguments_streaming(&self.ledger_binding, call_id, arguments_hash)?;
            self.ledger
                .mark_ready(&self.ledger_binding, call_id, arguments_hash)?;
        }
        let upstream_response = response.clone();
        let response = self.tool_registry.restore_response(&response)?;
        let anthropic =
            transform_responses::responses_to_anthropic_with_read_offset_protection_and_trace(
                response,
                read_offset_protection,
                read_trace,
            )
            .map_err(BridgeError::from)?;
        let anthropic = self
            .tool_registry
            .restore_anthropic_message(&upstream_response, &anthropic)?;
        for (call_id, _, _) in &tool_calls {
            self.ledger.mark_returned(&self.ledger_binding, call_id)?;
        }
        Ok(anthropic)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        app_config::AppType,
        provider::{Provider, ProviderMeta},
        proxy::providers::{transform_claude_request_for_api_format, transform_responses},
    };
    use serde_json::json;

    fn provider(provider_type: &str, api_format: &str) -> Provider {
        Provider {
            id: "bridge-test".to_string(),
            name: "Bridge Test".to_string(),
            settings_config: json!({}),
            website_url: None,
            category: Some("claude".to_string()),
            created_at: None,
            sort_index: None,
            notes: None,
            meta: Some(ProviderMeta {
                provider_type: Some(provider_type.to_string()),
                api_format: Some(api_format.to_string()),
                ..ProviderMeta::default()
            }),
            icon: None,
            icon_color: None,
            in_failover_queue: false,
        }
    }

    #[test]
    fn bridge_scope_matches_only_claude_code_codex_oauth_responses() {
        let codex_oauth = provider("codex_oauth", "openai_responses");

        assert!(bridge_scope_matches(&AppType::Claude, &codex_oauth));
        assert!(!bridge_scope_matches(&AppType::ClaudeDesktop, &codex_oauth));
        assert!(!bridge_scope_matches(&AppType::Codex, &codex_oauth));
        assert!(!bridge_scope_matches(
            &AppType::Claude,
            &provider("github_copilot", "openai_responses")
        ));
        assert!(!bridge_scope_matches(
            &AppType::Claude,
            &provider("codex_oauth", "openai_chat")
        ));
    }

    #[test]
    fn prepared_turn_freezes_profile_and_delegates_existing_codecs() {
        let provider = provider("codex_oauth", "openai_responses");
        let request = json!({
            "model": "gpt-test",
            "max_tokens": 128,
            "messages": [{"role": "user", "content": "hello"}]
        });
        let legacy_request = transform_claude_request_for_api_format(
            request.clone(),
            &provider,
            "openai_responses",
            Some("session-1"),
            None,
        )
        .unwrap();

        let prepared = ClaudeCodexBridge::builtin()
            .prepare_turn(&AppType::Claude, request, &provider, Some("session-1"))
            .unwrap();

        assert_eq!(prepared.request, legacy_request);
        assert!(!prepared.turn_id.is_empty());
        assert_eq!(
            prepared.capability_snapshot.profile_version,
            BUILTIN_CODEX_OAUTH_PROFILE_VERSION
        );
        assert_eq!(
            prepared.negotiation_report.profile_version,
            BUILTIN_CODEX_OAUTH_PROFILE_VERSION
        );

        let response = json!({
            "id": "resp_1",
            "model": "gpt-test",
            "status": "completed",
            "output": [{
                "type": "message",
                "id": "msg_1",
                "role": "assistant",
                "content": [{"type": "output_text", "text": "hello"}]
            }],
            "usage": {"input_tokens": 1, "output_tokens": 1, "total_tokens": 2}
        });
        let legacy_response =
            transform_responses::responses_to_anthropic(response.clone()).unwrap();

        assert_eq!(
            prepared.consume_response(response, None, None).unwrap(),
            legacy_response
        );
    }

    #[test]
    fn prepare_turn_rejects_requests_outside_bridge_scope() {
        let result = ClaudeCodexBridge::builtin().prepare_turn(
            &AppType::ClaudeDesktop,
            json!({"model": "gpt-test", "messages": []}),
            &provider("codex_oauth", "openai_responses"),
            None,
        );

        assert!(matches!(result, Err(BridgeError::OutOfScope)));
    }

    #[test]
    fn prepared_turn_freezes_registry_aliases_tool_choice_and_restores_response() {
        let provider = provider("codex_oauth", "openai_responses");
        let request = json!({
            "model": "gpt-test",
            "messages": [{"role": "user", "content": "read"}],
            "tools": [{
                "name": "Read",
                "description": "Read exact path",
                "input_schema": {
                    "type": "object",
                    "properties": {"file_path": {"type": "string"}},
                    "required": ["file_path"],
                    "additionalProperties": false
                }
            }],
            "tool_choice": {"type": "tool", "name": "Read"}
        });
        let mut original_after_prepare = request.clone();
        let mut prepared = ClaudeCodexBridge::builtin()
            .prepare_turn(&AppType::Claude, request, &provider, Some("session-1"))
            .unwrap();

        original_after_prepare["tools"][0]["name"] = json!("Write");
        assert_eq!(prepared.request["tools"][0]["name"], "read_file");
        assert_eq!(prepared.request["tool_choice"]["name"], "read_file");
        assert_eq!(prepared.tool_registry.bindings()[0].claude_name, "Read");
        assert_eq!(prepared.tool_registry.bindings()[0].codex_name, "read_file");
        assert!(!prepared.negotiation_report.schema_losses.is_empty());
        let registry = prepared.tool_registry.clone();
        let finalized = prepared.request.clone();
        prepared.finalize_request(finalized).unwrap();
        assert!(Arc::ptr_eq(&registry, &prepared.tool_registry));

        let response = json!({
            "id": "resp_1",
            "model": "gpt-test",
            "status": "completed",
            "output": [{
                "type": "function_call",
                "call_id": "call_1",
                "name": "read_file",
                "arguments": "{\"file_path\":\"src/main.rs\"}"
            }],
            "usage": {"input_tokens": 1, "output_tokens": 1}
        });
        let restored = prepared.consume_response(response, None, None).unwrap();
        assert_eq!(restored["content"][0]["name"], "Read");
        assert_eq!(restored["content"][0]["id"], "call_1");
        assert_eq!(restored["content"][0]["input"]["file_path"], "src/main.rs");
    }

    #[test]
    fn prepared_turn_returns_registry_validated_arguments_without_legacy_read_mutation() {
        let provider = provider("codex_oauth", "openai_responses");
        let prepared = ClaudeCodexBridge::builtin()
            .prepare_turn(
                &AppType::Claude,
                json!({
                    "model": "gpt-test",
                    "messages": [],
                    "tools": [{
                        "name": "Read",
                        "input_schema": {
                            "type": "object",
                            "properties": {
                                "file_path": {"type": "string"},
                                "pages": {"type": "string"},
                                "offset": {"type": "number"}
                            },
                            "required": ["file_path", "pages", "offset"],
                            "additionalProperties": false
                        }
                    }]
                }),
                &provider,
                None,
            )
            .unwrap();
        let exact_input = json!({
            "file_path": "src/main.rs",
            "pages": "",
            "offset": 2.300310976710655e22
        });
        let response = json!({
            "id": "resp_exact",
            "model": "gpt-test",
            "status": "completed",
            "output": [{
                "type": "function_call",
                "call_id": "call_exact",
                "name": "read_file",
                "arguments": serde_json::to_string(&exact_input).unwrap()
            }],
            "usage": {"input_tokens": 1, "output_tokens": 1}
        });

        let restored = prepared.consume_response(response, None, None).unwrap();

        assert_eq!(restored["content"][0]["name"], "Read");
        assert_eq!(restored["content"][0]["id"], "call_exact");
        assert_eq!(restored["content"][0]["input"], exact_input);
    }

    #[test]
    fn preparation_rejects_batch_tools_and_unknown_forced_tool_choice() {
        let provider = provider("codex_oauth", "openai_responses");
        for request in [
            json!({
                "model": "gpt-test",
                "messages": [],
                "tools": [{
                    "type": "BatchTool",
                    "name": "BatchTool",
                    "input_schema": {"type": "object"}
                }]
            }),
            json!({
                "model": "gpt-test",
                "messages": [],
                "tools": [{
                    "name": "Read",
                    "input_schema": {"type": "object"}
                }],
                "tool_choice": {"type": "tool", "name": "Write"}
            }),
        ] {
            assert!(matches!(
                ClaudeCodexBridge::builtin().prepare_turn(
                    &AppType::Claude,
                    request,
                    &provider,
                    None
                ),
                Err(BridgeError::ToolRegistryViolation { .. })
            ));
        }
    }

    #[test]
    fn finalized_request_cannot_replace_frozen_registry_tools() {
        let provider = provider("codex_oauth", "openai_responses");
        let mut prepared = ClaudeCodexBridge::builtin()
            .prepare_turn(
                &AppType::Claude,
                json!({
                    "model": "gpt-test",
                    "messages": [],
                    "tools": [{"name": "Read", "input_schema": {"type": "object"}}]
                }),
                &provider,
                None,
            )
            .unwrap();
        let frozen_request = prepared.request.clone();
        let mut injected = frozen_request.clone();
        injected["tools"] = json!([{
            "type": "function",
            "name": "unregistered",
            "parameters": {"type": "object"}
        }]);

        assert!(matches!(
            prepared.finalize_request(injected),
            Err(BridgeError::ToolRegistryViolation { .. })
        ));
        assert_eq!(prepared.request, frozen_request);
        assert_eq!(prepared.tool_registry.bindings()[0].claude_name, "Read");
    }

    #[test]
    fn canonical_request_fingerprint_is_stable_across_object_key_order() {
        let left = json!({
            "model": "gpt-test",
            "messages": [{"role": "user", "content": "hello"}],
            "metadata": {"b": 2, "a": 1}
        });
        let right = json!({
            "metadata": {"a": 1, "b": 2},
            "messages": [{"content": "hello", "role": "user"}],
            "model": "gpt-test"
        });

        assert_eq!(
            canonical_request_fingerprint(&left),
            canonical_request_fingerprint(&right)
        );
    }

    #[test]
    fn same_session_and_fingerprint_reuse_turn_registry_and_capabilities() {
        let bridge = ClaudeCodexBridge::with_ledger(ConversationLedger::default());
        let provider = provider("codex_oauth", "openai_responses");
        let request = json!({
            "model": "gpt-test",
            "messages": [{"role": "user", "content": "read"}],
            "tools": [{"name": "Read", "input_schema": {"properties": {}}}]
        });

        let first = bridge
            .prepare_turn_with_session_identity(
                &AppType::Claude,
                request.clone(),
                &provider,
                "session-1",
                Some("session-1"),
            )
            .unwrap();
        let retry = bridge
            .prepare_turn_with_session_identity(
                &AppType::Claude,
                request,
                &provider,
                "session-1",
                Some("session-1"),
            )
            .unwrap();

        assert_eq!(first.turn_id, retry.turn_id);
        assert!(Arc::ptr_eq(&first.tool_registry, &retry.tool_registry));
        assert!(Arc::ptr_eq(
            &first.capability_snapshot,
            &retry.capability_snapshot
        ));
        assert!(!first.negotiation_report.schema_losses.is_empty());
        assert_eq!(first.negotiation_report, retry.negotiation_report);
        assert!(retry.reused_turn);
    }

    #[test]
    fn matching_followup_tool_result_completes_returned_call() {
        let bridge = ClaudeCodexBridge::with_ledger(ConversationLedger::default());
        let provider = provider("codex_oauth", "openai_responses");
        let first_request = json!({
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
        });
        let first = bridge
            .prepare_turn_with_session_identity(
                &AppType::Claude,
                first_request,
                &provider,
                "session-1",
                Some("session-1"),
            )
            .unwrap();
        first
            .consume_response(
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
                None,
                None,
            )
            .unwrap();
        assert_eq!(
            bridge.ledger().call_state(first.ledger_binding(), "call-1"),
            Some(ToolCallState::ReturnedToClaude)
        );

        bridge
            .prepare_turn_with_session_identity(
                &AppType::Claude,
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
                            "content": "file contents"
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
                &provider,
                "session-1",
                Some("session-1"),
            )
            .unwrap();

        assert_eq!(
            bridge.ledger().call_state(first.ledger_binding(), "call-1"),
            Some(ToolCallState::Completed)
        );
    }

    #[test]
    fn orphan_tool_result_is_a_typed_conversation_conflict() {
        let bridge = ClaudeCodexBridge::with_ledger(ConversationLedger::default());
        let result = bridge.prepare_turn_with_session_identity(
            &AppType::Claude,
            json!({
                "model": "gpt-test",
                "messages": [{"role": "user", "content": [{
                    "type": "tool_result",
                    "tool_use_id": "missing",
                    "content": "must not become text"
                }]}]
            }),
            &provider("codex_oauth", "openai_responses"),
            "session-1",
            Some("session-1"),
        );

        assert_eq!(
            result.unwrap_err().conversation_conflict_kind().unwrap(),
            ConversationConflictKind::OrphanToolResult
        );
    }

    #[test]
    fn response_reasoning_is_recorded_as_bound_hashes_without_plaintext() {
        let bridge = ClaudeCodexBridge::with_ledger(ConversationLedger::default());
        let prepared = bridge
            .prepare_turn_with_session_identity(
                &AppType::Claude,
                json!({
                    "model": "gpt-test",
                    "messages": [{"role": "user", "content": "think"}]
                }),
                &provider("codex_oauth", "openai_responses"),
                "session-1",
                Some("session-1"),
            )
            .unwrap();

        prepared
            .consume_response(
                json!({
                    "id": "resp-1",
                    "model": "gpt-test",
                    "status": "completed",
                    "output": [{
                        "type": "reasoning",
                        "id": "rs-1",
                        "summary": [],
                        "encrypted_content": "opaque-ciphertext"
                    }, {
                        "type": "message",
                        "id": "msg-1",
                        "role": "assistant",
                        "content": [{"type": "output_text", "text": "done"}]
                    }],
                    "usage": {"input_tokens": 1, "output_tokens": 1}
                }),
                None,
                None,
            )
            .unwrap();

        let state = bridge
            .ledger()
            .reasoning_state(prepared.ledger_binding(), "rs-1")
            .unwrap();
        assert_eq!(state.state, ReasoningItemState::Completed);
        assert_ne!(state.content_hash, "opaque-ciphertext");
        assert_ne!(state.provider_hash, "bridge-test");
        assert_ne!(state.model_hash, "gpt-test");
    }

    #[test]
    fn paired_history_rebuilds_completed_call_after_process_local_state_loss() {
        let bridge = ClaudeCodexBridge::with_ledger(ConversationLedger::default());
        let prepared = bridge
            .prepare_turn_with_session_identity(
                &AppType::Claude,
                json!({
                    "model": "gpt-test",
                    "messages": [
                        {"role": "user", "content": "read"},
                        {"role": "assistant", "content": [{
                            "type": "tool_use",
                            "id": "historical-call",
                            "name": "Read",
                            "input": {"file_path": "src/main.rs"}
                        }]},
                        {"role": "user", "content": [{
                            "type": "tool_result",
                            "tool_use_id": "historical-call",
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
                &provider("codex_oauth", "openai_responses"),
                "restarted-session",
                Some("restarted-session"),
            )
            .unwrap();

        assert_eq!(
            bridge
                .ledger()
                .call_state(prepared.ledger_binding(), "historical-call"),
            Some(ToolCallState::Completed)
        );
    }
}
