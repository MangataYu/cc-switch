use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct EvidenceBundleId(pub String);

impl EvidenceBundleId {
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }
}

impl Default for EvidenceBundleId {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceStage {
    RequestTransform,
    UpstreamResponse,
    ResponseTransform,
    StreamTransform,
    Replay,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceErrorKind {
    CapabilityMismatch,
    SchemaAdaptationLoss,
    ToolRegistryViolation,
    ConversationStateConflict,
    InvalidUpstreamEvent,
    IncompleteStream,
    UpstreamRejected,
    LegacyTransformFailure,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EvidenceError {
    pub kind: EvidenceErrorKind,
    pub safe_summary: String,
    pub retryable: bool,
    pub output_already_visible: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EvidenceManifest {
    pub format_version: u32,
    pub bundle_id: EvidenceBundleId,
    pub created_at: DateTime<Utc>,
    pub provider_id: String,
    pub model: String,
    pub session_id_hash: String,
    pub stage: EvidenceStage,
    pub error: EvidenceError,
    pub full_capture: bool,
    pub suppression_reason: Option<String>,
    pub artifacts: Vec<EvidenceArtifact>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EvidenceArtifact {
    pub kind: EvidenceArtifactKind,
    pub file_name: String,
    pub byte_len: u64,
    pub sha256: String,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceArtifactKind {
    ClaudeRequest,
    CodexRequest,
    CodexResponse,
    ClaudeResponse,
    ToolRegistry,
    CapabilityReport,
    LedgerSnapshot,
    TransformDecisions,
}

#[cfg(test)]
impl EvidenceManifest {
    fn new_for_test(stage: EvidenceStage, kind: EvidenceErrorKind) -> Self {
        Self {
            format_version: 1,
            bundle_id: EvidenceBundleId("test-bundle".to_string()),
            created_at: Utc::now(),
            provider_id: "test-provider".to_string(),
            model: "gpt-test".to_string(),
            session_id_hash: "test-session".to_string(),
            stage,
            error: EvidenceError {
                kind,
                safe_summary: "test failure".to_string(),
                retryable: false,
                output_already_visible: false,
            },
            full_capture: true,
            suppression_reason: None,
            artifacts: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_serializes_stable_stage_and_error_kind() {
        let manifest = EvidenceManifest::new_for_test(
            EvidenceStage::ResponseTransform,
            EvidenceErrorKind::InvalidUpstreamEvent,
        );

        let value = serde_json::to_value(manifest).unwrap();

        assert_eq!(value["stage"], "response_transform");
        assert_eq!(value["error"]["kind"], "invalid_upstream_event");
        assert_eq!(value["format_version"], 1);
    }
}
