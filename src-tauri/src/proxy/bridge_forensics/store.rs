//! Local-only forensic bundle persistence.
//!
//! Unix permissions are restricted to the current user. On Windows, bundles
//! rely on the per-user application config directory ACL; this module never
//! shells out to mutate ACLs.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::{
    redact_protocol_value, EvidenceArtifact, EvidenceArtifactKind, EvidenceBundleId,
    EvidenceBundleSummary, EvidenceError, EvidenceErrorKind, EvidenceManifest, EvidenceStage,
    RetentionReport, StreamingFailureContext,
};
use crate::config::atomic_write;
use crate::error::AppError;
use crate::proxy::claude_codex_bridge::streaming::{
    StreamDecision, StreamTerminalState, StreamVisibility,
};
use zip::write::SimpleFileOptions;
use zip::{CompressionMethod, ZipWriter};

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
    active: bool,
}

struct PendingArtifact {
    kind: EvidenceArtifactKind,
    file_name: String,
    path: PathBuf,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EvidenceBundleInfo {
    pub bundle_id: EvidenceBundleId,
    pub path: PathBuf,
    pub full_capture: bool,
}

pub struct ForensicStreamObserver {
    capture: Option<ForensicTurnCapture>,
    saw_terminal_event: bool,
    output_visible: bool,
    tool_visible: bool,
    invalid_event: bool,
    streaming_context: Option<StreamingFailureContext>,
}

impl ForensicStreamObserver {
    pub fn new(capture: ForensicTurnCapture) -> Self {
        Self {
            capture: Some(capture),
            saw_terminal_event: false,
            output_visible: false,
            tool_visible: false,
            invalid_event: false,
            streaming_context: None,
        }
    }

    pub fn upstream_event(&mut self, event: &Value) -> Result<(), AppError> {
        if let Some(capture) = self.capture.as_mut() {
            capture.append_ndjson(EvidenceArtifactKind::CodexResponse, event)?;
        }
        match event.get("type").and_then(Value::as_str) {
            Some("response.completed" | "response.incomplete") => self.mark_terminal(),
            Some("response.failed" | "error") => {
                self.mark_terminal();
                self.invalid_event = true;
            }
            _ => {}
        }
        let status = event
            .get("status")
            .or_else(|| event.pointer("/response/status"))
            .and_then(Value::as_str);
        if matches!(status, Some("completed" | "incomplete")) {
            self.mark_terminal();
        } else if status == Some("failed") {
            self.mark_terminal();
            self.invalid_event = true;
        }
        Ok(())
    }

    pub fn claude_event(&mut self, event: &Value) -> Result<(), AppError> {
        if let Some(capture) = self.capture.as_mut() {
            capture.append_ndjson(EvidenceArtifactKind::ClaudeResponse, event)?;
        }
        match event.get("type").and_then(Value::as_str) {
            Some("content_block_start" | "content_block_delta" | "message_delta") => {
                self.mark_output_visible()
            }
            _ => {}
        }
        Ok(())
    }

    pub fn mark_output_visible(&mut self) {
        self.output_visible = true;
    }

    pub fn mark_terminal(&mut self) {
        self.saw_terminal_event = true;
    }

    pub fn mark_stream_error(&mut self) {
        self.invalid_event = true;
    }

    pub fn typed_decision(
        &mut self,
        turn_id: &str,
        decision: &StreamDecision,
        terminal_state: StreamTerminalState,
        registry_fingerprint: &str,
        capability_fingerprint: &str,
    ) -> Result<(), AppError> {
        if let Some(capture) = self.capture.as_mut() {
            let value = serde_json::to_value(decision)
                .map_err(|source| AppError::JsonSerialize { source })?;
            capture.append_ndjson(EvidenceArtifactKind::TransformDecisions, &value)?;
        }
        self.output_visible |= decision.output_visible;
        self.tool_visible |= decision.tool_visible;
        self.streaming_context = Some(StreamingFailureContext {
            event_kind: serde_name(&decision.event_kind),
            event_sequence: decision.sequence,
            turn_id: turn_id.to_string(),
            item_identity_hash: decision.item_identity_hash.clone(),
            call_identity_hash: decision.call_identity_hash.clone(),
            state_before: serde_name(&decision.state_before),
            state_after: serde_name(&decision.state_after),
            output_already_emitted: self.output_visible,
            tool_visible: self.tool_visible,
            terminal_state: serde_name(&terminal_state),
            registry_fingerprint: registry_fingerprint.to_string(),
            capability_fingerprint: capability_fingerprint.to_string(),
        });
        Ok(())
    }

    pub fn update_stream_visibility(&mut self, visibility: StreamVisibility) {
        self.output_visible |= visibility.output_emitted;
        self.tool_visible |= visibility.tool_visible;
        if let Some(context) = self.streaming_context.as_mut() {
            context.output_already_emitted = self.output_visible;
            context.tool_visible = self.tool_visible;
        }
    }

    pub fn set_stream_failure_context(&mut self, context: StreamingFailureContext) {
        self.output_visible |= context.output_already_emitted;
        self.tool_visible |= context.tool_visible;
        self.streaming_context = Some(context);
        self.invalid_event = true;
    }

    pub fn finish(
        mut self,
        stream_error: Option<&str>,
    ) -> Result<Option<EvidenceBundleInfo>, AppError> {
        let Some(mut capture) = self.capture.take() else {
            return Ok(None);
        };
        if stream_error.is_none() && !self.invalid_event && self.saw_terminal_event {
            capture.discard_success()?;
            return Ok(None);
        }

        capture.set_stage(EvidenceStage::StreamTransform);
        let kind = if stream_error.is_some() || self.invalid_event {
            EvidenceErrorKind::InvalidUpstreamEvent
        } else {
            EvidenceErrorKind::IncompleteStream
        };
        let safe_summary = match kind {
            EvidenceErrorKind::InvalidUpstreamEvent => {
                "Codex stream contained an invalid or failed event"
            }
            _ => "Codex stream ended before a terminal event",
        };
        let info = capture.commit_failure(EvidenceError {
            kind,
            safe_summary: safe_summary.to_string(),
            retryable: !self.output_visible && !self.tool_visible,
            output_already_visible: self.output_visible,
            streaming: self.streaming_context,
        })?;
        Ok(Some(info))
    }
}

fn serde_name<T: Serialize>(value: &T) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(str::to_string))
        .unwrap_or_else(|| "unknown".to_string())
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
            active: true,
        })
    }

    pub fn list_bundles(&self) -> Result<Vec<EvidenceBundleSummary>, AppError> {
        let mut summaries = self.load_bundle_records()?;
        summaries.sort_by(|left, right| {
            right
                .manifest
                .created_at
                .cmp(&left.manifest.created_at)
                .then_with(|| right.manifest.bundle_id.0.cmp(&left.manifest.bundle_id.0))
        });
        Ok(summaries
            .into_iter()
            .map(|record| record.summary())
            .collect())
    }

    pub fn delete_bundle(&self, id: &EvidenceBundleId) -> Result<(), AppError> {
        let path = self.bundle_path(id)?;
        fs::remove_dir_all(&path).map_err(|error| AppError::io(&path, error))
    }

    pub fn export_bundle(&self, id: &EvidenceBundleId, destination: &Path) -> Result<(), AppError> {
        let bundle_path = self.bundle_path(id)?;
        let manifest = load_manifest(&bundle_path)?;
        if manifest.bundle_id != *id {
            return Err(AppError::InvalidInput(
                "evidence manifest bundle id does not match directory".to_string(),
            ));
        }
        if destination.exists() {
            return Err(AppError::InvalidInput(
                "evidence export destination already exists".to_string(),
            ));
        }
        let destination_parent = destination.parent().ok_or_else(|| {
            AppError::InvalidInput("invalid evidence export destination".to_string())
        })?;
        fs::create_dir_all(destination_parent)
            .map_err(|error| AppError::io(destination_parent, error))?;
        let destination_name = destination
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| {
                AppError::InvalidInput("invalid evidence export destination".to_string())
            })?;
        let temporary_path = destination_parent.join(format!(
            ".{destination_name}.{}.tmp",
            EvidenceBundleId::new().0
        ));

        let result = self.write_bundle_zip(&bundle_path, &manifest, &temporary_path);
        if let Err(error) = result {
            let _ = fs::remove_file(&temporary_path);
            return Err(error);
        }
        if let Err(error) = fs::rename(&temporary_path, destination) {
            let _ = fs::remove_file(&temporary_path);
            return Err(AppError::io(&temporary_path, error));
        }
        Ok(())
    }

    pub fn enforce_retention(&self) -> Result<RetentionReport, AppError> {
        self.enforce_retention_at(Utc::now(), RETENTION_MAX_BYTES)
    }

    fn enforce_retention_at(
        &self,
        now: DateTime<Utc>,
        max_bytes: u64,
    ) -> Result<RetentionReport, AppError> {
        let mut records = self.load_bundle_records()?;
        records.sort_by(|left, right| {
            left.manifest
                .created_at
                .cmp(&right.manifest.created_at)
                .then_with(|| left.manifest.bundle_id.0.cmp(&right.manifest.bundle_id.0))
        });

        let expiry_cutoff = now - Duration::days(RETENTION_DAYS);
        let mut report = RetentionReport::default();
        let mut retained = Vec::new();
        for record in records {
            if record.manifest.created_at < expiry_cutoff {
                fs::remove_dir_all(&record.path)
                    .map_err(|error| AppError::io(&record.path, error))?;
                report.removed_expired += 1;
            } else {
                retained.push(record);
            }
        }

        let mut remaining_bytes: u64 = retained.iter().map(BundleRecord::byte_len).sum();
        let mut remove_count = 0usize;
        while remaining_bytes > max_bytes && remove_count < retained.len() {
            let record = &retained[remove_count];
            fs::remove_dir_all(&record.path).map_err(|error| AppError::io(&record.path, error))?;
            remaining_bytes = remaining_bytes.saturating_sub(record.byte_len());
            report.removed_over_limit += 1;
            remove_count += 1;
        }

        report.remaining_bundles = (retained.len() - remove_count) as u64;
        report.remaining_bytes = remaining_bytes;
        Ok(report)
    }

    fn load_bundle_records(&self) -> Result<Vec<BundleRecord>, AppError> {
        let bundles_root = self.root.join("bundles");
        match fs::read_dir(&bundles_root) {
            Ok(entries) => {
                let mut records = Vec::new();
                for entry in entries {
                    let entry = entry.map_err(|error| AppError::io(&bundles_root, error))?;
                    let file_type = entry
                        .file_type()
                        .map_err(|error| AppError::io(entry.path(), error))?;
                    if !file_type.is_dir() {
                        continue;
                    }
                    let Some(id) = entry.file_name().to_str().map(str::to_string) else {
                        continue;
                    };
                    if !is_valid_bundle_id(&id) {
                        continue;
                    }
                    let path = entry.path();
                    let manifest = load_manifest(&path)?;
                    if manifest.bundle_id.0 != id {
                        return Err(AppError::InvalidInput(
                            "evidence manifest bundle id does not match directory".to_string(),
                        ));
                    }
                    records.push(BundleRecord { path, manifest });
                }
                Ok(records)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
            Err(error) => Err(AppError::io(&bundles_root, error)),
        }
    }

    fn bundle_path(&self, id: &EvidenceBundleId) -> Result<PathBuf, AppError> {
        if !is_valid_bundle_id(&id.0) {
            return Err(AppError::InvalidInput(
                "invalid evidence bundle id".to_string(),
            ));
        }
        Ok(self.root.join("bundles").join(&id.0))
    }

    fn write_bundle_zip(
        &self,
        bundle_path: &Path,
        manifest: &EvidenceManifest,
        destination: &Path,
    ) -> Result<(), AppError> {
        let file = File::create(destination).map_err(|error| AppError::io(destination, error))?;
        let mut writer = ZipWriter::new(file);
        let options = SimpleFileOptions::default()
            .compression_method(CompressionMethod::Deflated)
            .unix_permissions(0o600);

        add_zip_file(&mut writer, bundle_path, "manifest.json", options)?;
        for artifact in &manifest.artifacts {
            validate_artifact_file_name(&artifact.file_name)?;
            add_zip_file(&mut writer, bundle_path, &artifact.file_name, options)?;
        }
        writer.finish().map_err(|error| {
            AppError::Message(format!("failed to finish evidence ZIP: {error}"))
        })?;
        Ok(())
    }
}

struct BundleRecord {
    path: PathBuf,
    manifest: EvidenceManifest,
}

impl BundleRecord {
    fn byte_len(&self) -> u64 {
        self.manifest
            .artifacts
            .iter()
            .map(|artifact| artifact.byte_len)
            .sum()
    }

    fn summary(self) -> EvidenceBundleSummary {
        let byte_len = self.byte_len();
        EvidenceBundleSummary {
            bundle_id: self.manifest.bundle_id,
            created_at: self.manifest.created_at,
            provider_id: self.manifest.provider_id,
            model: self.manifest.model,
            stage: self.manifest.stage,
            error_kind: self.manifest.error.kind,
            full_capture: self.manifest.full_capture,
            byte_len,
        }
    }
}

impl ForensicTurnCapture {
    pub fn set_stage(&mut self, stage: EvidenceStage) {
        self.stage = stage;
    }

    pub fn suppress_full_capture(&mut self, reason: impl Into<String>) {
        self.full_capture_allowed = false;
        let reason = reason.into();
        if !self.suppression_reasons.contains(&reason) {
            self.suppression_reasons.push(reason);
        }
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
        let mut line = Vec::new();
        serde_json::to_writer(&mut line, &outcome.value)
            .map_err(|source| AppError::JsonSerialize { source })?;
        line.push(b'\n');
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|error| AppError::io(&path, error))?;
        file.write_all(&line)
            .map_err(|error| AppError::io(&path, error))?;
        file.flush().map_err(|error| AppError::io(&path, error))?;
        restrict_file_permissions(&path)?;
        self.track_artifact(kind, file_name, path);
        Ok(())
    }

    pub fn commit_failure(mut self, error: EvidenceError) -> Result<EvidenceBundleInfo, AppError> {
        self.validate_staging_dir()?;
        if !self.full_capture_allowed {
            self.remove_staged_artifacts()?;
        }

        let mut artifacts = Vec::with_capacity(self.artifacts.len());
        for artifact in &self.artifacts {
            let bytes =
                fs::read(&artifact.path).map_err(|error| AppError::io(&artifact.path, error))?;
            artifacts.push(EvidenceArtifact {
                kind: artifact.kind,
                file_name: artifact.file_name.clone(),
                byte_len: bytes.len() as u64,
                sha256: format!("{:x}", Sha256::digest(&bytes)),
            });
        }
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

        self.active = false;

        Ok(EvidenceBundleInfo {
            bundle_id: self.bundle_id.clone(),
            path: bundle_path,
            full_capture: self.full_capture_allowed,
        })
    }

    pub fn discard_success(mut self) -> Result<(), AppError> {
        self.validate_staging_dir()?;
        fs::remove_dir_all(&self.staging_dir)
            .map_err(|error| AppError::io(&self.staging_dir, error))?;
        self.active = false;
        Ok(())
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

        self.track_artifact(kind, file_name, path);
        Ok(())
    }

    fn track_artifact(&mut self, kind: EvidenceArtifactKind, file_name: String, path: PathBuf) {
        let pending = PendingArtifact {
            kind,
            file_name: file_name.clone(),
            path,
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

impl Drop for ForensicTurnCapture {
    fn drop(&mut self) {
        if self.active && self.validate_staging_dir().is_ok() && self.staging_dir.is_dir() {
            let _ = fs::remove_dir_all(&self.staging_dir);
        }
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

fn is_valid_bundle_id(id: &str) -> bool {
    !id.is_empty()
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

fn validate_artifact_file_name(file_name: &str) -> Result<(), AppError> {
    let path = Path::new(file_name);
    let mut components = path.components();
    let is_single_normal_component =
        matches!(components.next(), Some(std::path::Component::Normal(_)))
            && components.next().is_none();
    if !is_single_normal_component || file_name == "manifest.json" {
        return Err(AppError::InvalidInput(
            "invalid evidence artifact file name".to_string(),
        ));
    }
    Ok(())
}

fn load_manifest(bundle_path: &Path) -> Result<EvidenceManifest, AppError> {
    let path = bundle_path.join("manifest.json");
    let bytes = fs::read(&path).map_err(|error| AppError::io(&path, error))?;
    serde_json::from_slice(&bytes).map_err(|error| AppError::json(&path, error))
}

fn add_zip_file(
    writer: &mut ZipWriter<File>,
    bundle_path: &Path,
    file_name: &str,
    options: SimpleFileOptions,
) -> Result<(), AppError> {
    let path = bundle_path.join(file_name);
    let metadata = fs::symlink_metadata(&path).map_err(|error| AppError::io(&path, error))?;
    if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
        return Err(AppError::InvalidInput(
            "evidence artifact is not a regular file".to_string(),
        ));
    }
    let mut source = File::open(&path).map_err(|error| AppError::io(&path, error))?;
    writer
        .start_file(file_name, options)
        .map_err(|error| AppError::Message(format!("failed to add evidence ZIP entry: {error}")))?;
    let mut buffer = [0u8; 16 * 1024];
    loop {
        let read = source
            .read(&mut buffer)
            .map_err(|error| AppError::io(&path, error))?;
        if read == 0 {
            break;
        }
        writer.write_all(&buffer[..read]).map_err(|error| {
            AppError::Message(format!("failed to write evidence ZIP entry: {error}"))
        })?;
    }
    Ok(())
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
impl BridgeForensicStore {
    fn write_test_bundle(
        &self,
        id: &str,
        created_at: DateTime<Utc>,
        byte_len: usize,
    ) -> Result<PathBuf, AppError> {
        let bundle_id = EvidenceBundleId(id.to_string());
        let bundle_path = self.bundle_path(&bundle_id)?;
        create_private_dir(&bundle_path)?;

        let mut payload = vec![b'x'; byte_len];
        if byte_len >= 2 {
            payload[0] = b'"';
            payload[byte_len - 1] = b'"';
        }
        let artifact_path = bundle_path.join("claude-request.json");
        atomic_write(&artifact_path, &payload)?;
        restrict_file_permissions(&artifact_path)?;
        let manifest = EvidenceManifest {
            format_version: FORENSIC_FORMAT_VERSION,
            bundle_id,
            created_at,
            provider_id: "test-provider".to_string(),
            model: "gpt-test".to_string(),
            session_id_hash: "test-session".to_string(),
            stage: EvidenceStage::ResponseTransform,
            error: EvidenceError::test_fixture(),
            full_capture: true,
            suppression_reason: None,
            artifacts: vec![EvidenceArtifact {
                kind: EvidenceArtifactKind::ClaudeRequest,
                file_name: "claude-request.json".to_string(),
                byte_len: payload.len() as u64,
                sha256: format!("{:x}", Sha256::digest(&payload)),
            }],
        };
        let manifest_path = bundle_path.join("manifest.json");
        let bytes = serde_json::to_vec_pretty(&manifest)
            .map_err(|source| AppError::JsonSerialize { source })?;
        atomic_write(&manifest_path, &bytes)?;
        restrict_file_permissions(&manifest_path)?;
        Ok(bundle_path)
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
            streaming: None,
        }
    }
}

#[cfg(test)]
fn read_manifest(bundle_path: &Path) -> Result<EvidenceManifest, AppError> {
    load_manifest(bundle_path)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::fs::File;

    use chrono::{DateTime, Duration, Utc};
    use serde_json::{json, Value};
    use zip::ZipArchive;

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

    #[test]
    fn dropped_capture_removes_staging_data() {
        let temp = tempfile::tempdir().unwrap();
        let store = BridgeForensicStore::new(temp.path().to_path_buf());
        let mut capture = store.begin_turn(CaptureMetadata::test_fixture()).unwrap();
        capture
            .record_json(
                EvidenceArtifactKind::ClaudeRequest,
                &json!({"messages": [{"role": "user", "content": "temporary"}]}),
            )
            .unwrap();

        drop(capture);

        assert!(fs::read_dir(temp.path().join("staging"))
            .unwrap()
            .next()
            .is_none());
    }

    #[test]
    fn retention_removes_expired_then_oldest_until_under_size_limit() {
        let fixture = RetentionFixture::new();
        fixture.bundle("expired", days_ago(8), 10);
        fixture.bundle("old", days_ago(2), 120);
        fixture.bundle("new", days_ago(1), 120);

        fixture.store.enforce_retention_at(now(), 200).unwrap();

        assert!(!fixture.bundle_path("expired").exists());
        assert!(!fixture.bundle_path("old").exists());
        assert!(fixture.bundle_path("new").exists());
    }

    #[test]
    fn delete_rejects_path_traversal_bundle_id() {
        let temp = tempfile::tempdir().unwrap();
        let store = BridgeForensicStore::new(temp.path().to_path_buf());

        let error = store
            .delete_bundle(&EvidenceBundleId("../config".into()))
            .unwrap_err();

        assert!(error.to_string().contains("invalid evidence bundle id"));
    }

    #[test]
    fn list_bundles_sorts_newest_first() {
        let fixture = RetentionFixture::new();
        fixture.bundle("older", days_ago(2), 10);
        fixture.bundle("newer", days_ago(1), 10);

        let summaries = fixture.store.list_bundles().unwrap();

        let ids: Vec<&str> = summaries
            .iter()
            .map(|summary| summary.bundle_id.0.as_str())
            .collect();
        assert_eq!(ids, vec!["newer", "older"]);
    }

    #[test]
    fn export_includes_only_manifest_enumerated_artifacts() {
        let fixture = RetentionFixture::new();
        let bundle_path = fixture.bundle("exportable", days_ago(1), 10);
        fs::write(bundle_path.join("unexpected.tmp"), b"must not export").unwrap();
        let destination = fixture.temp.path().join("evidence.zip");

        fixture
            .store
            .export_bundle(&EvidenceBundleId("exportable".into()), &destination)
            .unwrap();

        let mut archive = ZipArchive::new(File::open(destination).unwrap()).unwrap();
        let mut names: Vec<String> = (0..archive.len())
            .map(|index| archive.by_index(index).unwrap().name().to_string())
            .collect();
        names.sort();
        assert_eq!(names, vec!["claude-request.json", "manifest.json"]);
    }

    struct RetentionFixture {
        temp: tempfile::TempDir,
        store: BridgeForensicStore,
    }

    impl RetentionFixture {
        fn new() -> Self {
            let temp = tempfile::tempdir().unwrap();
            let store = BridgeForensicStore::new(temp.path().to_path_buf());
            Self { temp, store }
        }

        fn bundle(&self, id: &str, created_at: DateTime<Utc>, byte_len: usize) -> PathBuf {
            self.store
                .write_test_bundle(id, created_at, byte_len)
                .unwrap()
        }

        fn bundle_path(&self, id: &str) -> PathBuf {
            self.temp.path().join("bundles").join(id)
        }
    }

    fn now() -> DateTime<Utc> {
        Utc::now()
    }

    fn days_ago(days: i64) -> DateTime<Utc> {
        now() - Duration::days(days)
    }
}
