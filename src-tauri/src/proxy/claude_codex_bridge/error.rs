use crate::proxy::ProxyError;

use super::ConversationConflictKind;

#[derive(Debug, thiserror::Error)]
pub enum BridgeError {
    #[error("Claude Codex bridge is not available for this request")]
    OutOfScope,
    #[error("tool registry violation: {summary}")]
    ToolRegistryViolation { summary: String },
    #[error("tool schema adaptation rejected: {summary}")]
    SchemaAdaptationLoss { summary: String },
    #[error("conversation state conflict ({kind:?}): {summary}")]
    ConversationStateConflict {
        kind: ConversationConflictKind,
        summary: String,
    },
    #[error(transparent)]
    Codec(#[from] ProxyError),
}

impl BridgeError {
    pub fn into_proxy_error(self) -> ProxyError {
        match self {
            Self::OutOfScope => ProxyError::InvalidRequest(
                "Claude Codex bridge is not available for this request".to_string(),
            ),
            Self::ToolRegistryViolation { summary } => ProxyError::TransformError(format!(
                "Claude Codex tool registry violation: {summary}"
            )),
            Self::SchemaAdaptationLoss { summary } => ProxyError::InvalidRequest(format!(
                "Claude tool schema cannot be represented safely: {summary}"
            )),
            Self::ConversationStateConflict { kind, summary } => ProxyError::InvalidRequest(
                format!("Claude Codex conversation state conflict ({kind:?}): {summary}"),
            ),
            Self::Codec(error) => error,
        }
    }

    pub fn conversation_conflict_kind(&self) -> Option<ConversationConflictKind> {
        match self {
            Self::ConversationStateConflict { kind, .. } => Some(*kind),
            _ => None,
        }
    }
}
