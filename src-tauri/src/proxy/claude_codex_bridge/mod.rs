mod capabilities;
mod error;

pub use capabilities::*;
pub use error::*;

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

        let request = transform_claude_request_for_api_format(
            request,
            provider,
            "openai_responses",
            session_id,
            None,
        )?;
        let capability_snapshot = self.capabilities.clone();
        let negotiation_report = capability_snapshot.negotiation_report();

        Ok(PreparedCodexTurn {
            request,
            turn_id: uuid::Uuid::new_v4().to_string(),
            capability_snapshot,
            negotiation_report,
        })
    }
}

impl PreparedCodexTurn {
    pub(crate) fn finalize_request(&mut self, request: Value) {
        self.request = request;
    }

    pub fn consume_response(
        &self,
        response: Value,
        read_offset_protection: Option<&ReadOffsetProtection>,
        read_trace: Option<&ReadTrace>,
    ) -> Result<Value, BridgeError> {
        transform_responses::responses_to_anthropic_with_read_offset_protection_and_trace(
            response,
            read_offset_protection,
            read_trace,
        )
        .map_err(BridgeError::from)
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
}
