use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub const BUILTIN_CODEX_OAUTH_PROFILE_VERSION: &str = "codex-oauth-2026-07-29.v1";

static BUILTIN_CODEX_OAUTH_PROFILE: Lazy<Arc<CodexOAuthCapabilities>> = Lazy::new(|| {
    Arc::new(CodexOAuthCapabilities {
        profile_version: BUILTIN_CODEX_OAUTH_PROFILE_VERSION.to_string(),
        function_tools: SupportLevel::Native,
        parallel_tool_calls: SupportLevel::Native,
        encrypted_reasoning: SupportLevel::Native,
        image_input: SupportLevel::Native,
        strict_json_schema: SupportLevel::Emulated,
        hosted_tools: SupportLevel::Unsupported,
    })
});

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CodexOAuthCapabilities {
    pub profile_version: String,
    pub function_tools: SupportLevel,
    pub parallel_tool_calls: SupportLevel,
    pub encrypted_reasoning: SupportLevel,
    pub image_input: SupportLevel,
    pub strict_json_schema: SupportLevel,
    pub hosted_tools: SupportLevel,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SupportLevel {
    Native,
    Emulated,
    Unsupported,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityDecisionKind {
    Native,
    Emulated,
    Rejected,
    Degraded,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CapabilityDecision {
    pub capability: String,
    pub support: SupportLevel,
    pub decision: CapabilityDecisionKind,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct NegotiationReport {
    pub profile_version: String,
    pub decisions: Vec<CapabilityDecision>,
    pub schema_losses: Vec<super::SchemaLoss>,
}

impl CodexOAuthCapabilities {
    pub fn builtin() -> Arc<Self> {
        BUILTIN_CODEX_OAUTH_PROFILE.clone()
    }

    pub fn negotiation_report(&self) -> NegotiationReport {
        let capabilities = [
            ("function_tools", self.function_tools),
            ("parallel_tool_calls", self.parallel_tool_calls),
            ("encrypted_reasoning", self.encrypted_reasoning),
            ("image_input", self.image_input),
            ("strict_json_schema", self.strict_json_schema),
            ("hosted_tools", self.hosted_tools),
        ];
        let decisions = capabilities
            .into_iter()
            .map(|(capability, support)| CapabilityDecision {
                capability: capability.to_string(),
                support,
                decision: match support {
                    SupportLevel::Native => CapabilityDecisionKind::Native,
                    SupportLevel::Emulated => CapabilityDecisionKind::Emulated,
                    SupportLevel::Unsupported => CapabilityDecisionKind::Rejected,
                },
            })
            .collect();

        NegotiationReport {
            profile_version: self.profile_version.clone(),
            decisions,
            schema_losses: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builtin_profile_is_versioned_and_explicit() {
        let profile = CodexOAuthCapabilities::builtin();

        assert_eq!(profile.profile_version, "codex-oauth-2026-07-29.v1");
        assert_eq!(profile.function_tools, SupportLevel::Native);
        assert_eq!(profile.parallel_tool_calls, SupportLevel::Native);
        assert_eq!(profile.encrypted_reasoning, SupportLevel::Native);
        assert_eq!(profile.image_input, SupportLevel::Native);
        assert_eq!(profile.strict_json_schema, SupportLevel::Emulated);
        assert_eq!(profile.hosted_tools, SupportLevel::Unsupported);
    }

    #[test]
    fn negotiation_report_covers_every_profile_capability() {
        let report = CodexOAuthCapabilities::builtin().negotiation_report();

        assert_eq!(report.profile_version, "codex-oauth-2026-07-29.v1");
        assert_eq!(report.decisions.len(), 6);
        assert!(report.schema_losses.is_empty());
        assert!(report.decisions.iter().any(|decision| {
            decision.capability == "hosted_tools"
                && decision.decision == CapabilityDecisionKind::Rejected
        }));
    }
}
