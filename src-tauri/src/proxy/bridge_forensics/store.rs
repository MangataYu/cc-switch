//! Local-only forensic bundle persistence.
//!
//! Unix permissions are restricted to the current user. On Windows, bundles
//! rely on the per-user application config directory ACL; this module never
//! shells out to mutate ACLs.

use std::fs;
use std::path::{Path, PathBuf};

use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

#[cfg(test)]
use super::EvidenceErrorKind;
use super::{
    redact_protocol_value, EvidenceArtifact, EvidenceArtifactKind, EvidenceBundleId, EvidenceError,
    EvidenceManifest, EvidenceStage,
};
use crate::config::atomic_write;
use crate::error::AppError;

pub const FORENSIC_FORMAT_VERSION: u32 = 1;
pub const RETENTION_DAYS: i64 = 7;
pub const RETENTION_MAX_BYTES: u64 = 200 * 1024 * 1024;

#[derive(Clone)]
pub struct BridgeForensicStore {
    root: PathBuf,
}

#[derive(Clone, Debug)]
pub struct CaptureMetadata {
    pub provider_id: String,
    pub model: String,
    pub session_id_hash: String,
}

pub struct ForensicTurnCapture {
    store: BridgeForensicStore,
    bundle_id: EvidenceBundleId,
    staging_dir: PathBuf,
    metadata: CaptureMetadata,
    stage: EvidenceStage,
    artifacts: Vec<PendingArtifact>,
    full_capture_allowed: bool,
    suppression_reasons: Vec<String>,
}

struct PendingArtifact {
    kind: EvidenceArtifactKind,
    file_name: String,
    path: PathBuf,
    byte_len: u64,
    sha256: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EvidenceBundleInfo {
    pub bundle_id: EvidenceBundleId,
    pub path: PathBuf,
    pub full_capture: bool,
}

impl BridgeForensicStore {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    pub fn begin_turn(&self, metadata: CaptureMetadata) -> Result<ForensicTurnCapture, AppError> {
        let staging_root = self.root.join("staging");
        let bundles_root = self.root.join("bundles");
        create_private_dir(&staging_root)?;
        create_private_dir(&bundles_root)?;

        let bundle_id = EvidenceBundleId::new();
        let staging_dir = staging_root.join(&bundle_id.0);
        create_private_dir(&staging_dir)?;

        Ok(ForensicTurnCapture {
            store: self.clone(),
            bundle_id,
            staging_dir,
            metadata,
            stage: EvidenceStage::RequestTransform,
            artifacts: Vec::new(),
            full_capture_allowed: true,
            suppression_reasons: Vec::new(),
        })
    }
}

impl ForensicTurnCapture {
    pub fn set_stage(&mut self, stage: EvidenceStage) {
        self.stage = stage;
    }

    pub fn record_json(
        &mut self,
        kind: EvidenceArtifactKind,
        value: &Value,
    ) -> Result<(), AppError> {
        let outcome = redact_protocol_value(value);
        self.note_redaction_uncertainty(outcome.safe_for_full_capture);

        let file_name = artifact_file_name(kind, "json");
        let bytes = serde_json::to_vec_pretty(&outcome.value)
            .map_err(|source| AppError::JsonSerialize { source })?;
        self.write_artifact(kind, file_name, &bytes)
    }

    pub fn append_ndjson(
        &mut self,
        kind: EvidenceArtifactKind,
        value: &Value,
    ) -> Result<(), AppError> {
        let outcome = redact_protocol_value(value);
        self.note_redaction_uncertainty(outcome.safe_for_full_capture);

        let file_name = artifact_file_name(kind, "ndjson");
        let path = self.staging_dir.join(&file_name);
        let mut bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => return Err(AppError::io(&path, error)),
        };
        serde_json::to_writer(&mut bytes, &outcome.value)
            .map_err(|source| AppError::JsonSerialize { source })?;
        bytes.push(b'\n');

        self.write_artifact(kind, file_name, &bytes)
    }

    pub fn commit_failure(mut self, error: EvidenceError) -> Result<EvidenceBundleInfo, AppError> {
        self.validate_staging_dir()?;
        if !self.full_capture_allowed {
            self.remove_staged_artifacts()?;
        }

        let artifacts = self
            .artifacts
            .iter()
            .map(|artifact| EvidenceArtifact {
                kind: artifact.kind,
                file_name: artifact.file_name.clone(),
                byte_len: artifact.byte_len,
                sha256: artifact.sha256.clone(),
            })
            .collect();
        let manifest = EvidenceManifest {
            format_version: FORENSIC_FORMAT_VERSION,
            bundle_id: self.bundle_id.clone(),
            created_at: Utc::now(),
            provider_id: self.metadata.provider_id.clone(),
            model: self.metadata.model.clone(),
            session_id_hash: self.metadata.session_id_hash.clone(),
            stage: self.stage,
            error,
            full_capture: self.full_capture_allowed,
            suppression_reason: (!self.suppression_reasons.is_empty())
                .then(|| self.suppression_reasons.join("; ")),
            artifacts,
        };
        let manifest_path = self.staging_dir.join("manifest.json");
        let manifest_bytes = serde_json::to_vec_pretty(&manifest)
            .map_err(|source| AppError::JsonSerialize { source })?;
        atomic_write(&manifest_path, &manifest_bytes)?;
        restrict_file_permissions(&manifest_path)?;

        let bundles_root = self.store.root.join("bundles");
        let bundle_path = bundles_root.join(&self.bundle_id.0);
        fs::rename(&self.staging_dir, &bundle_path)
            .map_err(|error| AppError::io(&self.staging_dir, error))?;

        Ok(EvidenceBundleInfo {
            bundle_id: self.bundle_id,
            path: bundle_path,
            full_capture: self.full_capture_allowed,
        })
    }

    pub fn discard_success(self) -> Result<(), AppError> {
        self.validate_staging_dir()?;
        fs::remove_dir_all(&self.staging_dir)
            .map_err(|error| AppError::io(&self.staging_dir, error))
    }

    fn write_artifact(
        &mut self,
        kind: EvidenceArtifactKind,
        file_name: String,
        bytes: &[u8],
    ) -> Result<(), AppError> {
        let path = self.staging_dir.join(&file_name);
        atomic_write(&path, bytes)?;
        restrict_file_permissions(&path)?;

        let pending = PendingArtifact {
            kind,
            file_name: file_name.clone(),
            path,
            byte_len: bytes.len() as u64,
            sha256: format!("{:x}", Sha256::digest(bytes)),
        };
        if let Some(existing) = self
            .artifacts
            .iter_mut()
            .find(|artifact| artifact.file_name == file_name)
        {
            *existing = pending;
        } else {
            self.artifacts.push(pending);
        }
        Ok(())
    }

    fn note_redaction_uncertainty(&mut self, safe_for_full_capture: bool) {
        if !safe_for_full_capture {
            self.full_capture_allowed = false;
            let reason = "uncertain credential-shaped field detected; full capture suppressed";
            if !self.suppression_reasons.iter().any(|item| item == reason) {
                self.suppression_reasons.push(reason.to_string());
            }
        }
    }

    fn remove_staged_artifacts(&mut self) -> Result<(), AppError> {
        for artifact in &self.artifacts {
            match fs::remove_file(&artifact.path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(AppError::io(&artifact.path, error)),
            }
        }
        self.artifacts.clear();
        Ok(())
    }

    fn validate_staging_dir(&self) -> Result<(), AppError> {
        let expected_parent = self.store.root.join("staging");
        if self.staging_dir.parent() != Some(expected_parent.as_path())
            || self.staging_dir.file_name().and_then(|name| name.to_str())
                != Some(self.bundle_id.0.as_str())
        {
            return Err(AppError::InvalidInput(
                "invalid bridge evidence staging directory".to_string(),
            ));
        }
        Ok(())
    }
}

fn artifact_file_name(kind: EvidenceArtifactKind, extension: &str) -> String {
    let stem = match kind {
        EvidenceArtifactKind::ClaudeRequest => "claude-request",
        EvidenceArtifactKind::CodexRequest => "codex-request",
        EvidenceArtifactKind::CodexResponse => "codex-response",
        EvidenceArtifactKind::ClaudeResponse => "claude-response",
        EvidenceArtifactKind::ToolRegistry => "tool-registry",
        EvidenceArtifactKind::CapabilityReport => "capability-report",
        EvidenceArtifactKind::LedgerSnapshot => "ledger-snapshot",
        EvidenceArtifactKind::TransformDecisions => "transform-decisions",
    };
    format!("{stem}.{extension}")
}

fn create_private_dir(path: &Path) -> Result<(), AppError> {
    fs::create_dir_all(path).map_err(|error| AppError::io(path, error))?;
    restrict_dir_permissions(path)
}

#[cfg(unix)]
fn restrict_dir_permissions(path: &Path) -> Result<(), AppError> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .map_err(|error| AppError::io(path, error))
}

#[cfg(windows)]
fn restrict_dir_permissions(_path: &Path) -> Result<(), AppError> {
    Ok(())
}

#[cfg(unix)]
fn restrict_file_permissions(path: &Path) -> Result<(), AppError> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        .map_err(|error| AppError::io(path, error))
}

#[cfg(windows)]
fn restrict_file_permissions(_path: &Path) -> Result<(), AppError> {
    Ok(())
}

#[cfg(test)]
impl CaptureMetadata {
    fn test_fixture() -> Self {
        Self {
            provider_id: "test-provider".to_string(),
            model: "gpt-test".to_string(),
            session_id_hash: "test-session".to_string(),
        }
    }
}

#[cfg(test)]
impl EvidenceError {
    fn test_fixture() -> Self {
        Self {
            kind: EvidenceErrorKind::LegacyTransformFailure,
            safe_summary: "test transform failure".to_string(),
            retryable: false,
            output_already_visible: false,
        }
    }
}

#[cfg(test)]
fn read_manifest(bundle_path: &Path) -> Result<EvidenceManifest, AppError> {
    let path = bundle_path.join("manifest.json");
    let bytes = fs::read(&path).map_err(|error| AppError::io(&path, error))?;
    serde_json::from_slice(&bytes).map_err(|error| AppError::json(&path, error))
}

#[cfg(test)]
mod tests {
    use std::fs;

    use serde_json::{json, Value};

    use super::*;
    use crate::proxy::bridge_forensics::{EvidenceArtifactKind, EvidenceError, EvidenceManifest};

    #[test]
    fn committed_bundle_is_atomic_and_contains_redacted_protocol_artifacts() {
        let temp = tempfile::tempdir().unwrap();
        let store = BridgeForensicStore::new(temp.path().to_path_buf());
        let mut capture = store.begin_turn(CaptureMetadata::test_fixture()).unwrap();
        capture
            .record_json(
                EvidenceArtifactKind::ClaudeRequest,
                &json!({
                    "headers": {"authorization": "Bearer hidden"},
                    "messages": [{"role": "user", "content": "read file"}]
                }),
            )
            .unwrap();

        let info = capture
            .commit_failure(EvidenceError::test_fixture())
            .unwrap();

        assert!(info.path.join("manifest.json").is_file());
        let request: Value =
            serde_json::from_slice(&fs::read(info.path.join("claude-request.json")).unwrap())
                .unwrap();
        assert_eq!(request["headers"]["authorization"], "[REDACTED]");
        assert_eq!(request["messages"][0]["content"], "read file");
        assert!(fs::read_dir(temp.path().join("staging"))
            .unwrap()
            .next()
            .is_none());
    }

    #[test]
    fn uncertain_secret_writes_structural_suppression_bundle_only() {
        let temp = tempfile::tempdir().unwrap();
        let store = BridgeForensicStore::new(temp.path().to_path_buf());
        let mut capture = store.begin_turn(CaptureMetadata::test_fixture()).unwrap();
        capture
            .record_json(
                EvidenceArtifactKind::CodexRequest,
                &json!({"vendor_credential": "cannot-persist"}),
            )
            .unwrap();

        let info = capture
            .commit_failure(EvidenceError::test_fixture())
            .unwrap();
        let manifest: EvidenceManifest = read_manifest(&info.path).unwrap();

        assert!(!manifest.full_capture);
        assert!(!info.path.join("codex-request.json").exists());
        assert!(manifest
            .suppression_reason
            .unwrap()
            .contains("uncertain credential"));
    }

    #[test]
    fn ndjson_redacts_each_line_and_terminates_it_with_newline() {
        let temp = tempfile::tempdir().unwrap();
        let store = BridgeForensicStore::new(temp.path().to_path_buf());
        let mut capture = store.begin_turn(CaptureMetadata::test_fixture()).unwrap();
        capture
            .append_ndjson(
                EvidenceArtifactKind::CodexResponse,
                &json!({"type": "response.created", "access_token": "hidden"}),
            )
            .unwrap();
        capture
            .append_ndjson(
                EvidenceArtifactKind::CodexResponse,
                &json!({"type": "response.completed"}),
            )
            .unwrap();

        let info = capture
            .commit_failure(EvidenceError::test_fixture())
            .unwrap();
        let bytes = fs::read(info.path.join("codex-response.ndjson")).unwrap();
        let text = String::from_utf8(bytes).unwrap();

        assert!(text.ends_with('\n'));
        let lines: Vec<Value> = text
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0]["access_token"], "[REDACTED]");
        assert_eq!(lines[1]["type"], "response.completed");
    }
}
