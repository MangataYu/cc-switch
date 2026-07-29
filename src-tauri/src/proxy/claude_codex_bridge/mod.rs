mod capabilities;
mod error;
mod schema;
mod tools;

pub use capabilities::*;
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
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct ClaudeCodexBridge {
    capabilities: Arc<CodexOAuthCapabilities>,
}

#[derive(Clone, Debug)]
pub struct PreparedCodexTurn {
    pub request: Value,
    pub turn_id: String,
    pub tool_registry: Arc<ToolRegistry>,
    pub capability_snapshot: Arc<CodexOAuthCapabilities>,
    pub negotiation_report: NegotiationReport,
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
        }
    }

    pub fn prepare_turn(
        &self,
        app_type: &AppType,
        request: Value,
        provider: &Provider,
        session_id: Option<&str>,
    ) -> Result<PreparedCodexTurn, BridgeError> {
        if !bridge_scope_matches(app_type, provider) {
            return Err(BridgeError::OutOfScope);
        }

        let claude_tools = request
            .get("tools")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let forced_claude_tool = request
            .pointer("/tool_choice/name")
            .and_then(Value::as_str)
            .map(str::to_string);
        let capability_snapshot = self.capabilities.clone();
        let (tool_registry, schema_losses) =
            ToolRegistry::compile(&claude_tools, capability_snapshot.as_ref())?;
        let tool_registry = Arc::new(tool_registry);
        let mut request = transform_claude_request_for_api_format(
            request,
            provider,
            "openai_responses",
            session_id,
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
            turn_id: uuid::Uuid::new_v4().to_string(),
            tool_registry,
            capability_snapshot,
            negotiation_report,
        })
    }
}

impl PreparedCodexTurn {
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
        let upstream_response = response.clone();
        let response = self.tool_registry.restore_response(&response)?;
        let anthropic =
            transform_responses::responses_to_anthropic_with_read_offset_protection_and_trace(
                response,
                read_offset_protection,
                read_trace,
            )
            .map_err(BridgeError::from)?;
        self.tool_registry
            .restore_anthropic_message(&upstream_response, &anthropic)
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
}
