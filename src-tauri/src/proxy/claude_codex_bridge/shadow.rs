use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{
    canonical_request_fingerprint,
    streaming::{
        claude_stream_event_kind, decode_codex_response_event, ClaudeContentBlock,
        ClaudeContentDelta, ClaudeStreamEvent, PreparedCodexStream, StreamTerminalState,
    },
    PreparedCodexTurn, TransformAction,
};

const MAX_SHADOW_STREAM_BUFFER_BYTES: usize = 256 * 1024;
const MAX_SHADOW_STREAM_EVENTS: usize = 4096;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ShadowDifferenceDisposition {
    Equivalent,
    Expected,
    Accepted,
    Unexplained,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ShadowDifferenceKind {
    CapabilityDriven,
    SafeNormalization,
    LegacyOnlyRepair,
    BridgeStrictRejection,
    RegistrySchemaMismatch,
    RequestFieldMismatch,
    ResponseEventMismatch,
    ToolIdentityMismatch,
    UsageStopMismatch,
    TerminalMismatch,
    IncompleteObservation,
    InternalComparisonFailure,
    Unexplained,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ShadowReasonCode {
    EquivalentStructure,
    CapabilityProfileDecision,
    BridgeToolRegistryProjection,
    BridgeToolChoiceProjection,
    SafeRequestNormalization,
    LegacyResponseRepair,
    BridgeStrictRejection,
    RegistrySchemaMismatch,
    RequestFieldMismatch,
    ResponseEventMismatch,
    ToolIdentityMismatch,
    UsageStopMismatch,
    TerminalMismatch,
    IncompleteShadowObservation,
    InternalComparisonFailure,
    Unexplained,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShadowDifference {
    pub kind: ShadowDifferenceKind,
    pub disposition: ShadowDifferenceDisposition,
    pub reason_code: ShadowReasonCode,
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub legacy_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bridge_hash: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShadowRequestComparison {
    pub tool_count: usize,
    pub registry_identity_hash: String,
    pub schema_fingerprint: String,
    pub transform_decision_count: usize,
    pub preserved_fields: usize,
    pub normalized_fields: usize,
    pub dropped_fields: usize,
    pub rejected_fields: usize,
    pub capability_profile_version: String,
    pub capability_decision_count: usize,
    pub tool_choice_hash: String,
    pub model_hash: String,
    pub stream: bool,
    pub reasoning_hash: String,
    pub usage_hash: String,
    pub legacy_request_hash: String,
    pub bridge_request_hash: String,
    pub request_structure_matches: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShadowStateComparison {
    pub isolated_ledger: bool,
    pub output_visible: bool,
    pub tool_visible: bool,
    pub terminal_observed: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShadowVisibleResponseSummary {
    pub content_blocks: usize,
    pub text_blocks: usize,
    pub reasoning_blocks: usize,
    pub tool_calls: usize,
    pub tool_identity_hash: String,
    pub call_identity_hash: String,
    pub arguments_valid: bool,
    pub usage_hash: String,
    pub stop_reason: String,
    pub terminal: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShadowResponseComparison {
    pub legacy: ShadowVisibleResponseSummary,
    pub bridge: ShadowVisibleResponseSummary,
    pub claude_shape_matches: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShadowStreamShape {
    pub event_count: usize,
    pub text_events: usize,
    pub reasoning_events: usize,
    pub tool_events: usize,
    pub usage_events: usize,
    pub terminal_events: usize,
    pub structural_hash: String,
    pub complete: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShadowStreamComparison {
    pub legacy: ShadowStreamShape,
    pub bridge: ShadowStreamShape,
    pub shape_matches: bool,
    pub bounded: bool,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LiveSmokeStatus {
    #[default]
    NotRun,
    Pending,
    Passed,
    Failed,
    Blocked,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ShadowReadinessBlocker {
    NoSamples,
    FixtureCoverageIncomplete,
    UnexplainedDifferences,
    ComparisonFailures,
    ForensicSuppression,
    ForensicFailures,
    VisibleToolRetryUnsafe,
    RollbackUnavailable,
    LiveSmokeNotPassed,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShadowReadinessInput {
    pub sample_count: u64,
    pub supported_fixture_count: u64,
    pub required_fixture_count: u64,
    pub expected_differences: u64,
    pub accepted_differences: u64,
    pub unexplained_differences: u64,
    pub comparison_failures: u64,
    pub forensic_suppressions: u64,
    pub forensic_failures: u64,
    pub visible_tool_retry_safe: bool,
    pub rollback_available: bool,
    pub live_smoke_status: LiveSmokeStatus,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShadowReadinessSummary {
    pub sample_count: u64,
    pub supported_fixture_count: u64,
    pub required_fixture_count: u64,
    pub expected_differences: u64,
    pub accepted_differences: u64,
    pub unexplained_differences: u64,
    pub comparison_failures: u64,
    pub forensic_suppressions: u64,
    pub forensic_failures: u64,
    pub visible_tool_retry_safe: bool,
    pub rollback_available: bool,
    pub live_smoke_status: LiveSmokeStatus,
    pub blocking_reasons: Vec<ShadowReadinessBlocker>,
    pub ready: bool,
}

pub fn calculate_shadow_readiness(input: &ShadowReadinessInput) -> ShadowReadinessSummary {
    let mut blocking_reasons = Vec::new();
    if input.sample_count == 0 {
        blocking_reasons.push(ShadowReadinessBlocker::NoSamples);
    }
    if input.required_fixture_count == 0
        || input.supported_fixture_count < input.required_fixture_count
    {
        blocking_reasons.push(ShadowReadinessBlocker::FixtureCoverageIncomplete);
    }
    if input.unexplained_differences > 0 {
        blocking_reasons.push(ShadowReadinessBlocker::UnexplainedDifferences);
    }
    if input.comparison_failures > 0 {
        blocking_reasons.push(ShadowReadinessBlocker::ComparisonFailures);
    }
    if input.forensic_suppressions > 0 {
        blocking_reasons.push(ShadowReadinessBlocker::ForensicSuppression);
    }
    if input.forensic_failures > 0 {
        blocking_reasons.push(ShadowReadinessBlocker::ForensicFailures);
    }
    if !input.visible_tool_retry_safe {
        blocking_reasons.push(ShadowReadinessBlocker::VisibleToolRetryUnsafe);
    }
    if !input.rollback_available {
        blocking_reasons.push(ShadowReadinessBlocker::RollbackUnavailable);
    }
    if input.live_smoke_status != LiveSmokeStatus::Passed {
        blocking_reasons.push(ShadowReadinessBlocker::LiveSmokeNotPassed);
    }
    ShadowReadinessSummary {
        sample_count: input.sample_count,
        supported_fixture_count: input.supported_fixture_count,
        required_fixture_count: input.required_fixture_count,
        expected_differences: input.expected_differences,
        accepted_differences: input.accepted_differences,
        unexplained_differences: input.unexplained_differences,
        comparison_failures: input.comparison_failures,
        forensic_suppressions: input.forensic_suppressions,
        forensic_failures: input.forensic_failures,
        visible_tool_retry_safe: input.visible_tool_retry_safe,
        rollback_available: input.rollback_available,
        live_smoke_status: input.live_smoke_status,
        ready: blocking_reasons.is_empty(),
        blocking_reasons,
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShadowComparisonReport {
    pub request: ShadowRequestComparison,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response: Option<ShadowResponseComparison>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream: Option<ShadowStreamComparison>,
    pub state: ShadowStateComparison,
    pub differences: Vec<ShadowDifference>,
    pub readiness: ShadowReadinessSummary,
}

#[derive(Debug)]
pub struct ShadowComparisonSession {
    prepared: Option<PreparedCodexTurn>,
    report: ShadowComparisonReport,
    stream_observation: Option<ShadowStreamObservation>,
    stream_observation_failed: bool,
    legacy_stream: ShadowStreamAccumulator,
}

#[derive(Debug)]
struct ShadowStreamObservation {
    machine: PreparedCodexStream,
    upstream_tool_aliases: BTreeMap<String, String>,
    buffer: String,
    utf8_remainder: Vec<u8>,
    bridge: ShadowStreamAccumulator,
}

#[derive(Debug, Default)]
struct ShadowStreamAccumulator {
    buffer: String,
    utf8_remainder: Vec<u8>,
    event_kinds: Vec<String>,
    text_events: usize,
    reasoning_events: usize,
    tool_events: usize,
    usage_events: usize,
    terminal_events: usize,
    complete: bool,
}

impl ShadowComparisonSession {
    pub fn compare_request(mut prepared: PreparedCodexTurn, legacy: &Value) -> Self {
        let bridge = &prepared.request;
        let legacy_hash = canonical_request_fingerprint(legacy);
        let bridge_hash = canonical_request_fingerprint(bridge);
        let request_structure_matches = legacy_hash == bridge_hash;
        let decisions = prepared
            .tool_registry
            .transform_decisions(&prepared.negotiation_report.schema_losses);
        let mut differences = compare_request_fields(legacy, bridge);
        if request_structure_matches {
            differences.push(ShadowDifference {
                kind: ShadowDifferenceKind::SafeNormalization,
                disposition: ShadowDifferenceDisposition::Equivalent,
                reason_code: ShadowReasonCode::EquivalentStructure,
                path: "$".to_string(),
                legacy_hash: Some(legacy_hash.clone()),
                bridge_hash: Some(bridge_hash.clone()),
            });
        }
        let readiness = summarize_differences(&differences);
        let request = ShadowRequestComparison {
            tool_count: prepared.tool_registry.bindings().len(),
            registry_identity_hash: prepared.tool_registry.identity_fingerprint().to_string(),
            schema_fingerprint: prepared.tool_registry.schema_fingerprint().to_string(),
            transform_decision_count: decisions.len(),
            preserved_fields: decisions
                .iter()
                .filter(|decision| decision.action == TransformAction::Preserved)
                .count(),
            normalized_fields: decisions
                .iter()
                .filter(|decision| {
                    matches!(
                        decision.action,
                        TransformAction::Renamed | TransformAction::Normalized
                    )
                })
                .count(),
            dropped_fields: decisions
                .iter()
                .filter(|decision| decision.action == TransformAction::Dropped)
                .count(),
            rejected_fields: decisions
                .iter()
                .filter(|decision| decision.action == TransformAction::Rejected)
                .count(),
            capability_profile_version: prepared.capability_snapshot.profile_version.clone(),
            capability_decision_count: prepared.negotiation_report.decisions.len(),
            tool_choice_hash: value_hash(bridge.get("tool_choice")),
            model_hash: value_hash(bridge.get("model")),
            stream: bridge
                .get("stream")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            reasoning_hash: value_hash(bridge.get("reasoning")),
            usage_hash: value_hash(bridge.get("stream_options")),
            legacy_request_hash: legacy_hash,
            bridge_request_hash: bridge_hash,
            request_structure_matches,
        };
        let report = ShadowComparisonReport {
            request,
            response: None,
            stream: None,
            state: ShadowStateComparison {
                isolated_ledger: true,
                ..ShadowStateComparison::default()
            },
            differences,
            readiness,
        };
        prepared.shadow_upstream_tool_aliases =
            collect_shadow_upstream_tool_aliases(&prepared, legacy);
        prepared.shadow_comparison = Some(report.clone());
        Self {
            prepared: Some(prepared),
            report,
            stream_observation: None,
            stream_observation_failed: false,
            legacy_stream: ShadowStreamAccumulator::default(),
        }
    }

    pub fn resume(prepared: PreparedCodexTurn) -> Self {
        let report = prepared.shadow_comparison.clone().unwrap_or_else(|| {
            let request_hash = canonical_request_fingerprint(&prepared.request);
            ShadowComparisonReport {
                request: ShadowRequestComparison {
                    tool_count: prepared.tool_registry.bindings().len(),
                    registry_identity_hash: prepared
                        .tool_registry
                        .identity_fingerprint()
                        .to_string(),
                    schema_fingerprint: prepared.tool_registry.schema_fingerprint().to_string(),
                    transform_decision_count: 0,
                    preserved_fields: 0,
                    normalized_fields: 0,
                    dropped_fields: 0,
                    rejected_fields: 0,
                    capability_profile_version: prepared
                        .capability_snapshot
                        .profile_version
                        .clone(),
                    capability_decision_count: prepared.negotiation_report.decisions.len(),
                    tool_choice_hash: value_hash(prepared.request.get("tool_choice")),
                    model_hash: value_hash(prepared.request.get("model")),
                    stream: prepared
                        .request
                        .get("stream")
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
                    reasoning_hash: value_hash(prepared.request.get("reasoning")),
                    usage_hash: value_hash(prepared.request.get("stream_options")),
                    legacy_request_hash: request_hash.clone(),
                    bridge_request_hash: request_hash,
                    request_structure_matches: true,
                },
                response: None,
                stream: None,
                state: ShadowStateComparison {
                    isolated_ledger: true,
                    ..ShadowStateComparison::default()
                },
                differences: Vec::new(),
                readiness: ShadowReadinessSummary {
                    sample_count: 1,
                    ..ShadowReadinessSummary::default()
                },
            }
        });
        Self {
            prepared: Some(prepared),
            report,
            stream_observation: None,
            stream_observation_failed: false,
            legacy_stream: ShadowStreamAccumulator::default(),
        }
    }

    pub fn report(&self) -> ShadowComparisonReport {
        self.report.clone()
    }

    pub fn compare_non_streaming(&mut self, upstream: &Value, legacy: &Value) {
        let legacy_summary = summarize_claude_response(legacy, true);
        let Some(prepared) = self.prepared.as_ref() else {
            self.fail_open(ShadowReasonCode::InternalComparisonFailure);
            return;
        };
        match prepared.consume_response(upstream.clone(), None, None) {
            Ok(bridge) => {
                let bridge_summary = summarize_claude_response(&bridge, true);
                self.report.state.output_visible = legacy_summary.content_blocks > 0;
                self.report.state.tool_visible = legacy_summary.tool_calls > 0;
                self.report.state.terminal_observed = legacy_summary.terminal;
                let claude_shape_matches = legacy_summary == bridge_summary;
                compare_response_summaries(
                    &legacy_summary,
                    &bridge_summary,
                    &mut self.report.differences,
                );
                self.report.response = Some(ShadowResponseComparison {
                    legacy: legacy_summary,
                    bridge: bridge_summary,
                    claude_shape_matches,
                });
                self.report.readiness = summarize_differences(&self.report.differences);
            }
            Err(_) => {
                self.report.state.output_visible = legacy_summary.content_blocks > 0;
                self.report.state.tool_visible = legacy_summary.tool_calls > 0;
                self.report.state.terminal_observed = legacy_summary.terminal;
                self.report.response = Some(ShadowResponseComparison {
                    legacy: legacy_summary,
                    bridge: ShadowVisibleResponseSummary::default(),
                    claude_shape_matches: false,
                });
                self.report.differences.push(ShadowDifference {
                    kind: ShadowDifferenceKind::BridgeStrictRejection,
                    disposition: ShadowDifferenceDisposition::Expected,
                    reason_code: ShadowReasonCode::BridgeStrictRejection,
                    path: "$/response".to_string(),
                    legacy_hash: None,
                    bridge_hash: None,
                });
                self.report.readiness = summarize_differences(&self.report.differences);
                self.report.readiness.comparison_failures = 1;
            }
        }
    }

    pub fn fail_open(&mut self, reason_code: ShadowReasonCode) {
        self.report.differences.push(ShadowDifference {
            kind: ShadowDifferenceKind::InternalComparisonFailure,
            disposition: ShadowDifferenceDisposition::Accepted,
            reason_code,
            path: "$/comparison".to_string(),
            legacy_hash: None,
            bridge_hash: None,
        });
        self.report.readiness = summarize_differences(&self.report.differences);
        self.report.readiness.comparison_failures = 1;
    }

    pub fn observe_stream_chunk(&mut self, chunk: &[u8]) {
        if self.stream_observation_failed {
            return;
        }
        if self.stream_observation.is_none() {
            let Some(prepared) = self.prepared.take() else {
                self.fail_open(ShadowReasonCode::InternalComparisonFailure);
                return;
            };
            let upstream_tool_aliases = prepared.shadow_upstream_tool_aliases.clone();
            self.stream_observation = Some(ShadowStreamObservation {
                machine: prepared.start_stream(),
                upstream_tool_aliases,
                buffer: String::new(),
                utf8_remainder: Vec::new(),
                bridge: ShadowStreamAccumulator::default(),
            });
        }
        let Some(observation) = self.stream_observation.as_mut() else {
            return;
        };
        if observation.buffer.len().saturating_add(chunk.len()) > MAX_SHADOW_STREAM_BUFFER_BYTES
            || observation.bridge.event_kinds.len() >= MAX_SHADOW_STREAM_EVENTS
        {
            self.stream_observation = None;
            self.stream_observation_failed = true;
            self.fail_open(ShadowReasonCode::IncompleteShadowObservation);
            return;
        }
        crate::proxy::sse::append_utf8_safe(
            &mut observation.buffer,
            &mut observation.utf8_remainder,
            chunk,
        );
        let mut failed = false;
        while let Some(block) = crate::proxy::sse::take_sse_block(&mut observation.buffer) {
            let (event_name, mut payload) = match parse_sse_block(&block) {
                Some(parsed) => parsed,
                None => continue,
            };
            reproject_shadow_tool_identity(
                event_name.as_deref(),
                &mut payload,
                &observation.upstream_tool_aliases,
            );
            let events = match decode_codex_response_event(event_name.as_deref(), payload) {
                Ok(events) => events,
                Err(_) => {
                    failed = true;
                    break;
                }
            };
            for event in events {
                let outputs = match observation.machine.apply(event) {
                    Ok(outputs) => outputs,
                    Err(_) => {
                        failed = true;
                        break;
                    }
                };
                for output in outputs {
                    observation.bridge.record_bridge_event(&output);
                    if observation.machine.acknowledge_emitted(&output).is_err() {
                        failed = true;
                        break;
                    }
                }
                if failed {
                    break;
                }
            }
            if failed {
                break;
            }
        }
        if failed {
            self.stream_observation = None;
            self.stream_observation_failed = true;
            self.fail_open(ShadowReasonCode::IncompleteShadowObservation);
        }
    }

    pub fn observe_legacy_stream_chunk(&mut self, chunk: &[u8]) {
        if self.legacy_stream.buffer.len().saturating_add(chunk.len())
            > MAX_SHADOW_STREAM_BUFFER_BYTES
            || self.legacy_stream.event_kinds.len() >= MAX_SHADOW_STREAM_EVENTS
        {
            self.fail_open(ShadowReasonCode::IncompleteShadowObservation);
            return;
        }
        crate::proxy::sse::append_utf8_safe(
            &mut self.legacy_stream.buffer,
            &mut self.legacy_stream.utf8_remainder,
            chunk,
        );
        while let Some(block) = crate::proxy::sse::take_sse_block(&mut self.legacy_stream.buffer) {
            if let Some((event_name, payload)) = parse_sse_block(&block) {
                if let Some(event_name) = event_name {
                    self.legacy_stream
                        .record_legacy_event(&event_name, &payload);
                }
            }
        }
    }

    pub fn finish_stream(&mut self) {
        self.legacy_stream.complete = self
            .legacy_stream
            .event_kinds
            .iter()
            .any(|kind| kind == "message_stop");
        let Some(observation) = self.stream_observation.as_mut() else {
            if self.report.readiness.comparison_failures == 0 {
                self.fail_open(ShadowReasonCode::IncompleteShadowObservation);
            }
            return;
        };
        let bridge_complete = observation.machine.finish().is_ok()
            && observation.machine.terminal_state() == StreamTerminalState::Completed;
        observation.bridge.complete = bridge_complete;
        let legacy = self.legacy_stream.shape();
        let bridge = observation.bridge.shape();
        let shape_matches = legacy.event_count == bridge.event_count
            && legacy.text_events == bridge.text_events
            && legacy.reasoning_events == bridge.reasoning_events
            && legacy.tool_events == bridge.tool_events
            && legacy.terminal_events == bridge.terminal_events;
        if !shape_matches {
            self.report.differences.push(ShadowDifference {
                kind: ShadowDifferenceKind::ResponseEventMismatch,
                disposition: ShadowDifferenceDisposition::Unexplained,
                reason_code: ShadowReasonCode::ResponseEventMismatch,
                path: "$/stream/claude_shape".to_string(),
                legacy_hash: Some(legacy.structural_hash.clone()),
                bridge_hash: Some(bridge.structural_hash.clone()),
            });
        }
        if legacy.complete != bridge.complete {
            self.report.differences.push(ShadowDifference {
                kind: ShadowDifferenceKind::TerminalMismatch,
                disposition: ShadowDifferenceDisposition::Unexplained,
                reason_code: ShadowReasonCode::TerminalMismatch,
                path: "$/stream/terminal".to_string(),
                legacy_hash: Some(canonical_request_fingerprint(&Value::Bool(legacy.complete))),
                bridge_hash: Some(canonical_request_fingerprint(&Value::Bool(bridge.complete))),
            });
        }
        self.report.state.output_visible = legacy.event_count > 0;
        self.report.state.tool_visible = legacy.tool_events > 0;
        self.report.state.terminal_observed = legacy.complete;
        self.report.stream = Some(ShadowStreamComparison {
            legacy,
            bridge,
            shape_matches,
            bounded: true,
        });
        self.report.readiness = summarize_differences(&self.report.differences);
    }

    pub fn into_prepared_turn(mut self) -> Option<PreparedCodexTurn> {
        self.prepared.take()
    }
}

impl ShadowStreamAccumulator {
    fn record_bridge_event(&mut self, event: &ClaudeStreamEvent) {
        let kind = claude_stream_event_kind(event);
        let classification = match event {
            ClaudeStreamEvent::ContentBlockStart {
                block: ClaudeContentBlock::ToolUse { .. },
                ..
            }
            | ClaudeStreamEvent::ContentBlockDelta {
                delta: ClaudeContentDelta::InputJson { .. },
                ..
            } => ShadowEventClass::Tool,
            ClaudeStreamEvent::ContentBlockStart {
                block: ClaudeContentBlock::Thinking,
                ..
            }
            | ClaudeStreamEvent::ContentBlockDelta {
                delta: ClaudeContentDelta::Thinking { .. } | ClaudeContentDelta::Signature { .. },
                ..
            } => ShadowEventClass::Reasoning,
            ClaudeStreamEvent::ContentBlockDelta {
                delta: ClaudeContentDelta::Text { .. },
                ..
            } => ShadowEventClass::Text,
            _ => ShadowEventClass::Other,
        };
        self.record_classified(kind, classification);
    }

    fn record_legacy_event(&mut self, kind: &str, payload: &Value) {
        let content_type = payload
            .pointer("/content_block/type")
            .or_else(|| payload.pointer("/delta/type"))
            .and_then(Value::as_str);
        let classification = match content_type {
            Some("tool_use" | "input_json_delta") => ShadowEventClass::Tool,
            Some("thinking" | "thinking_delta" | "signature_delta") => ShadowEventClass::Reasoning,
            Some("text_delta") => ShadowEventClass::Text,
            _ => ShadowEventClass::Other,
        };
        self.record_classified(kind, classification);
    }

    fn record_classified(&mut self, kind: &str, classification: ShadowEventClass) {
        if self.event_kinds.len() >= MAX_SHADOW_STREAM_EVENTS {
            return;
        }
        self.event_kinds.push(kind.to_string());
        match classification {
            ShadowEventClass::Text => self.text_events += 1,
            ShadowEventClass::Reasoning => self.reasoning_events += 1,
            ShadowEventClass::Tool => self.tool_events += 1,
            ShadowEventClass::Other => {}
        }
        match kind {
            "content_block_start" | "content_block_stop" => {}
            "message_delta" => self.usage_events += 1,
            "message_stop" => self.terminal_events += 1,
            _ if classification == ShadowEventClass::Other
                && (kind.contains("reasoning") || kind.contains("thinking")) =>
            {
                self.reasoning_events += 1
            }
            _ if classification == ShadowEventClass::Other && kind.contains("tool") => {
                self.tool_events += 1
            }
            _ => {}
        }
    }

    fn shape(&self) -> ShadowStreamShape {
        ShadowStreamShape {
            event_count: self.event_kinds.len(),
            text_events: self.text_events,
            reasoning_events: self.reasoning_events,
            tool_events: self.tool_events,
            usage_events: self.usage_events,
            terminal_events: self.terminal_events,
            structural_hash: canonical_request_fingerprint(&Value::Array(
                self.event_kinds
                    .iter()
                    .cloned()
                    .map(Value::String)
                    .collect(),
            )),
            complete: self.complete,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ShadowEventClass {
    Text,
    Reasoning,
    Tool,
    Other,
}

fn collect_shadow_upstream_tool_aliases(
    prepared: &PreparedCodexTurn,
    legacy_request: &Value,
) -> BTreeMap<String, String> {
    let legacy_names = legacy_request
        .get("tools")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|tool| tool.get("name").and_then(Value::as_str))
        .collect::<BTreeSet<_>>();
    prepared
        .tool_registry
        .bindings()
        .iter()
        .filter(|binding| {
            binding.claude_name != binding.codex_name
                && legacy_names.contains(binding.claude_name.as_str())
        })
        .map(|binding| (binding.claude_name.clone(), binding.codex_name.clone()))
        .collect()
}

fn reproject_shadow_tool_identity(
    event_name: Option<&str>,
    payload: &mut Value,
    aliases: &BTreeMap<String, String>,
) {
    let event_name = event_name
        .filter(|name| !name.is_empty())
        .or_else(|| payload.get("type").and_then(Value::as_str));
    let name = match event_name {
        Some("response.output_item.added" | "response.output_item.done") => {
            payload.pointer_mut("/item/name")
        }
        Some(
            "response.function_call_arguments.delta" | "response.function_call_arguments.done",
        ) => payload.get_mut("name"),
        _ => None,
    };
    let Some(name) = name else {
        return;
    };
    let Some(alias) = name.as_str().and_then(|name| aliases.get(name)) else {
        return;
    };
    *name = Value::String(alias.clone());
}

fn parse_sse_block(block: &str) -> Option<(Option<String>, Value)> {
    let mut event_name = None;
    let mut data = Vec::new();
    for line in block.lines() {
        if let Some(value) = crate::proxy::sse::strip_sse_field(line, "event") {
            event_name = Some(value.trim().to_string());
        } else if let Some(value) = crate::proxy::sse::strip_sse_field(line, "data") {
            data.push(value);
        }
    }
    if data.is_empty() || data.first().is_some_and(|value| value.trim() == "[DONE]") {
        return None;
    }
    serde_json::from_str(&data.join("\n"))
        .ok()
        .map(|payload| (event_name, payload))
}

fn summarize_claude_response(value: &Value, arguments_valid: bool) -> ShadowVisibleResponseSummary {
    let blocks = value
        .get("content")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let tool_identities = blocks
        .iter()
        .filter(|block| block.get("type").and_then(Value::as_str) == Some("tool_use"))
        .map(|block| value_hash(block.get("name")))
        .collect::<Vec<_>>();
    let call_identities = blocks
        .iter()
        .filter(|block| block.get("type").and_then(Value::as_str) == Some("tool_use"))
        .map(|block| value_hash(block.get("id")))
        .collect::<Vec<_>>();
    ShadowVisibleResponseSummary {
        content_blocks: blocks.len(),
        text_blocks: count_blocks(&blocks, &["text"]),
        reasoning_blocks: count_blocks(&blocks, &["thinking", "redacted_thinking"]),
        tool_calls: count_blocks(&blocks, &["tool_use"]),
        tool_identity_hash: canonical_request_fingerprint(&Value::Array(
            tool_identities.into_iter().map(Value::String).collect(),
        )),
        call_identity_hash: canonical_request_fingerprint(&Value::Array(
            call_identities.into_iter().map(Value::String).collect(),
        )),
        arguments_valid,
        usage_hash: value_hash(value.get("usage")),
        stop_reason: value
            .get("stop_reason")
            .and_then(Value::as_str)
            .unwrap_or("missing")
            .to_string(),
        terminal: value.get("type").and_then(Value::as_str) == Some("message")
            || value.get("stop_reason").is_some(),
    }
}

fn count_blocks(blocks: &[Value], kinds: &[&str]) -> usize {
    blocks
        .iter()
        .filter(|block| {
            block
                .get("type")
                .and_then(Value::as_str)
                .is_some_and(|kind| kinds.contains(&kind))
        })
        .count()
}

fn compare_response_summaries(
    legacy: &ShadowVisibleResponseSummary,
    bridge: &ShadowVisibleResponseSummary,
    differences: &mut Vec<ShadowDifference>,
) {
    let mut push = |kind, reason_code, path: &str, legacy_hash, bridge_hash| {
        differences.push(ShadowDifference {
            kind,
            disposition: ShadowDifferenceDisposition::Unexplained,
            reason_code,
            path: path.to_string(),
            legacy_hash: Some(legacy_hash),
            bridge_hash: Some(bridge_hash),
        });
    };
    if legacy.content_blocks != bridge.content_blocks
        || legacy.text_blocks != bridge.text_blocks
        || legacy.reasoning_blocks != bridge.reasoning_blocks
    {
        push(
            ShadowDifferenceKind::ResponseEventMismatch,
            ShadowReasonCode::ResponseEventMismatch,
            "$/response/content_shape",
            canonical_request_fingerprint(&serde_json::json!([
                legacy.content_blocks,
                legacy.text_blocks,
                legacy.reasoning_blocks
            ])),
            canonical_request_fingerprint(&serde_json::json!([
                bridge.content_blocks,
                bridge.text_blocks,
                bridge.reasoning_blocks
            ])),
        );
    }
    if legacy.tool_calls != bridge.tool_calls
        || legacy.tool_identity_hash != bridge.tool_identity_hash
        || legacy.call_identity_hash != bridge.call_identity_hash
    {
        push(
            ShadowDifferenceKind::ToolIdentityMismatch,
            ShadowReasonCode::ToolIdentityMismatch,
            "$/response/tool_identity",
            legacy.tool_identity_hash.clone(),
            bridge.tool_identity_hash.clone(),
        );
    }
    if legacy.usage_hash != bridge.usage_hash || legacy.stop_reason != bridge.stop_reason {
        push(
            ShadowDifferenceKind::UsageStopMismatch,
            ShadowReasonCode::UsageStopMismatch,
            "$/response/usage_stop",
            legacy.usage_hash.clone(),
            bridge.usage_hash.clone(),
        );
    }
    if legacy.terminal != bridge.terminal {
        push(
            ShadowDifferenceKind::TerminalMismatch,
            ShadowReasonCode::TerminalMismatch,
            "$/response/terminal",
            canonical_request_fingerprint(&Value::Bool(legacy.terminal)),
            canonical_request_fingerprint(&Value::Bool(bridge.terminal)),
        );
    }
}

fn value_hash(value: Option<&Value>) -> String {
    canonical_request_fingerprint(value.unwrap_or(&Value::Null))
}

fn compare_request_fields(legacy: &Value, bridge: &Value) -> Vec<ShadowDifference> {
    let keys = legacy
        .as_object()
        .into_iter()
        .flat_map(|object| object.keys().cloned())
        .chain(
            bridge
                .as_object()
                .into_iter()
                .flat_map(|object| object.keys().cloned()),
        )
        .collect::<BTreeSet<_>>();
    keys.into_iter()
        .filter_map(|key| {
            let legacy_value = legacy.get(&key);
            let bridge_value = bridge.get(&key);
            if legacy_value == bridge_value {
                return None;
            }
            let (kind, disposition, reason_code) = match key.as_str() {
                "tools" => (
                    ShadowDifferenceKind::SafeNormalization,
                    ShadowDifferenceDisposition::Expected,
                    ShadowReasonCode::BridgeToolRegistryProjection,
                ),
                "tool_choice" => (
                    ShadowDifferenceKind::SafeNormalization,
                    ShadowDifferenceDisposition::Expected,
                    ShadowReasonCode::BridgeToolChoiceProjection,
                ),
                "include" | "store" | "parallel_tool_calls" | "reasoning" => (
                    ShadowDifferenceKind::CapabilityDriven,
                    ShadowDifferenceDisposition::Expected,
                    ShadowReasonCode::CapabilityProfileDecision,
                ),
                "instructions" | "input" | "max_output_tokens" | "stream_options" => (
                    ShadowDifferenceKind::SafeNormalization,
                    ShadowDifferenceDisposition::Accepted,
                    ShadowReasonCode::SafeRequestNormalization,
                ),
                _ => (
                    ShadowDifferenceKind::RequestFieldMismatch,
                    ShadowDifferenceDisposition::Unexplained,
                    ShadowReasonCode::RequestFieldMismatch,
                ),
            };
            Some(ShadowDifference {
                kind,
                disposition,
                reason_code,
                path: format!("$/{key}"),
                legacy_hash: Some(value_hash(legacy_value)),
                bridge_hash: Some(value_hash(bridge_value)),
            })
        })
        .collect()
}

fn summarize_differences(differences: &[ShadowDifference]) -> ShadowReadinessSummary {
    let expected_differences = differences
        .iter()
        .filter(|difference| difference.disposition == ShadowDifferenceDisposition::Expected)
        .count() as u64;
    let accepted_differences = differences
        .iter()
        .filter(|difference| difference.disposition == ShadowDifferenceDisposition::Accepted)
        .count() as u64;
    let unexplained_differences = differences
        .iter()
        .filter(|difference| difference.disposition == ShadowDifferenceDisposition::Unexplained)
        .count() as u64;
    calculate_shadow_readiness(&ShadowReadinessInput {
        sample_count: 1,
        supported_fixture_count: 0,
        required_fixture_count: 1,
        expected_differences,
        accepted_differences,
        unexplained_differences,
        comparison_failures: 0,
        forensic_suppressions: 0,
        forensic_failures: 0,
        visible_tool_retry_safe: true,
        rollback_available: true,
        live_smoke_status: LiveSmokeStatus::NotRun,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        app_config::AppType,
        provider::{Provider, ProviderMeta},
        proxy::claude_codex_bridge::{ClaudeCodexBridge, ConversationLedger},
    };
    use serde_json::json;

    fn provider() -> Provider {
        Provider {
            id: "shadow-test".to_string(),
            name: "Shadow Test".to_string(),
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

    fn request(secret: &str) -> serde_json::Value {
        json!({
            "model": "gpt-5.6",
            "stream": false,
            "max_tokens": 128,
            "messages": [{"role":"user","content":secret}],
            "tools": [{
                "name":"Read",
                "description":"Read a local file",
                "input_schema": {
                    "type":"object",
                    "properties":{"file_path":{"type":"string"}},
                    "required":["file_path"],
                    "additionalProperties":false
                }
            }],
            "tool_choice": {"type":"tool","name":"Read"}
        })
    }

    #[test]
    fn shadow_request_comparison_is_typed_deterministic_and_leak_free() {
        let secret = "sk-secret prompt Authorization cookie tool-arguments reasoning";
        let bridge = ClaudeCodexBridge::with_ledger(ConversationLedger::default());
        let prepared = bridge
            .prepare_turn(
                &AppType::Claude,
                request(secret),
                &provider(),
                Some("shadow-session"),
            )
            .unwrap();
        let mut legacy = prepared.request.clone();
        legacy["model"] = json!("legacy-model");

        let session = ShadowComparisonSession::compare_request(prepared, &legacy);
        let report = session.report();

        assert_eq!(report.request.tool_count, 1);
        assert!(!report.request.registry_identity_hash.is_empty());
        assert!(!report.request.schema_fingerprint.is_empty());
        assert_eq!(
            report.request.capability_profile_version,
            "codex-oauth-2026-07-29.v1"
        );
        assert!(report.differences.iter().any(|difference| {
            difference.kind == ShadowDifferenceKind::RequestFieldMismatch
                && difference.disposition == ShadowDifferenceDisposition::Unexplained
                && difference.reason_code == ShadowReasonCode::RequestFieldMismatch
                && difference.path == "$/model"
        }));

        let encoded = serde_json::to_string(&report).unwrap();
        assert!(!encoded.contains(secret));
        assert!(!encoded.contains("Authorization"));
        assert!(!encoded.contains("tool-arguments"));
    }

    #[test]
    fn shadow_request_comparison_classifies_known_transform_differences() {
        let bridge = ClaudeCodexBridge::with_ledger(ConversationLedger::default());
        let prepared = bridge
            .prepare_turn(
                &AppType::Claude,
                request("fixture"),
                &provider(),
                Some("shadow-known-difference"),
            )
            .unwrap();
        let legacy = prepared.request.clone();

        let report = ShadowComparisonSession::compare_request(prepared, &legacy).report();

        assert_eq!(report.readiness.sample_count, 1);
        assert_eq!(report.readiness.unexplained_differences, 0);
        assert!(report.differences.iter().all(|difference| {
            difference.disposition != ShadowDifferenceDisposition::Expected
                || difference.reason_code != ShadowReasonCode::Unexplained
        }));
        assert!(report.request.request_structure_matches);
    }

    #[test]
    fn shadow_non_streaming_compares_same_upstream_without_replacing_legacy() {
        let bridge = ClaudeCodexBridge::with_ledger(ConversationLedger::default());
        let prepared = bridge
            .prepare_turn(
                &AppType::Claude,
                request("fixture"),
                &provider(),
                Some("shadow-non-stream"),
            )
            .unwrap();
        let codex_name = prepared
            .tool_registry
            .codex_name_for_claude("Read")
            .unwrap()
            .to_string();
        let upstream = json!({
            "id":"resp-1",
            "model":"gpt-5.6",
            "status":"completed",
            "output":[{
                "id":"fc-1",
                "type":"function_call",
                "call_id":"call-1",
                "name":codex_name,
                "arguments":"{\"file_path\":\"secret.rs\"}"
            }],
            "usage":{"input_tokens":3,"output_tokens":4}
        });
        let legacy = crate::proxy::providers::transform_responses::responses_to_anthropic(
            prepared.tool_registry.restore_response(&upstream).unwrap(),
        )
        .unwrap();
        let served = legacy.clone();
        let mut session = ShadowComparisonSession::compare_request(prepared, &json!({}));

        session.compare_non_streaming(&upstream, &legacy);
        let report = session.report();

        assert_eq!(legacy, served);
        assert_eq!(report.response.as_ref().unwrap().legacy.content_blocks, 1);
        assert_eq!(report.response.as_ref().unwrap().bridge.tool_calls, 1);
        assert!(report.state.isolated_ledger);
        assert!(report.state.tool_visible);
        assert!(report.state.terminal_observed);
        let encoded = serde_json::to_string(&report).unwrap();
        assert!(!encoded.contains("secret.rs"));
    }

    #[test]
    fn shadow_non_streaming_failure_is_structured_and_fail_open() {
        let bridge = ClaudeCodexBridge::with_ledger(ConversationLedger::default());
        let prepared = bridge
            .prepare_turn(
                &AppType::Claude,
                request("fixture"),
                &provider(),
                Some("shadow-non-stream-failure"),
            )
            .unwrap();
        let upstream = json!({
            "id":"resp-bad",
            "status":"completed",
            "output":[{
                "type":"function_call",
                "call_id":"call-bad",
                "name":"unknown_tool",
                "arguments":"{}"
            }]
        });
        let legacy = json!({"id":"legacy","type":"message","content":[]});
        let served = legacy.clone();
        let mut session = ShadowComparisonSession::compare_request(prepared, &json!({}));

        session.compare_non_streaming(&upstream, &legacy);
        let report = session.report();

        assert_eq!(legacy, served);
        assert!(report.differences.iter().any(|difference| {
            difference.kind == ShadowDifferenceKind::BridgeStrictRejection
                && difference.disposition == ShadowDifferenceDisposition::Expected
                && difference.reason_code == ShadowReasonCode::BridgeStrictRejection
        }));
        assert_eq!(report.readiness.comparison_failures, 1);
    }

    #[test]
    fn shadow_stream_failure_is_recorded_once_after_observer_detaches() {
        let bridge = ClaudeCodexBridge::with_ledger(ConversationLedger::default());
        let prepared = bridge
            .prepare_turn(
                &AppType::Claude,
                request("fixture"),
                &provider(),
                Some("shadow-stream-detach"),
            )
            .unwrap();
        let mut session = ShadowComparisonSession::compare_request(prepared, &json!({}));
        let invalid = concat!(
            "event: response.output_item.added\n",
            "data: {\"type\":\"response.output_item.added\"}\n\n"
        );

        session.observe_stream_chunk(invalid.as_bytes());
        for _ in 0..3 {
            session.observe_stream_chunk(b"event: response.completed\ndata: {}\n\n");
        }
        session.finish_stream();

        let report = session.report();
        let failures = report
            .differences
            .iter()
            .filter(|difference| difference.kind == ShadowDifferenceKind::InternalComparisonFailure)
            .collect::<Vec<_>>();
        assert_eq!(failures.len(), 1);
        assert_eq!(
            failures[0].reason_code,
            ShadowReasonCode::IncompleteShadowObservation
        );
        assert_eq!(report.readiness.comparison_failures, 1);
    }

    #[tokio::test]
    async fn shadow_tool_stream_reprojects_legacy_tool_name_before_strict_observation() {
        use bytes::Bytes;
        use futures::{stream, StreamExt};

        let original = request("fixture");
        let bridge = ClaudeCodexBridge::with_ledger(ConversationLedger::default());
        let prepared = bridge
            .prepare_turn(
                &AppType::Claude,
                original.clone(),
                &provider(),
                Some("shadow-legacy-tool-name"),
            )
            .unwrap();
        let cache_key = prepared.request["prompt_cache_key"]
            .as_str()
            .expect("prepared request cache key")
            .to_string();
        let legacy_request = crate::proxy::providers::transform_responses::anthropic_to_responses(
            original,
            Some(&cache_key),
            true,
            false,
        )
        .unwrap();
        assert_eq!(legacy_request["tools"][0]["name"], "Read");
        assert_eq!(prepared.request["tools"][0]["name"], "read_file");

        let upstream = concat!(
            "event: response.created\n",
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_tool\",\"model\":\"gpt-5.6\"}}\n\n",
            "event: response.output_item.added\n",
            "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"id\":\"fc_1\",\"type\":\"function_call\",\"call_id\":\"call_1\",\"name\":\"Read\",\"status\":\"in_progress\"}}\n\n",
            "event: response.function_call_arguments.delta\n",
            "data: {\"type\":\"response.function_call_arguments.delta\",\"item_id\":\"fc_1\",\"output_index\":0,\"delta\":\"{\\\"file_path\\\":\\\"fixture.txt\\\"}\",\"sequence_number\":1}\n\n",
            "event: response.function_call_arguments.done\n",
            "data: {\"type\":\"response.function_call_arguments.done\",\"item_id\":\"fc_1\",\"output_index\":0,\"name\":\"Read\",\"arguments\":\"{\\\"file_path\\\":\\\"fixture.txt\\\"}\",\"sequence_number\":2}\n\n",
            "event: response.output_item.done\n",
            "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"id\":\"fc_1\",\"type\":\"function_call\",\"call_id\":\"call_1\",\"name\":\"Read\",\"arguments\":\"{\\\"file_path\\\":\\\"fixture.txt\\\"}\",\"status\":\"completed\"}}\n\n",
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":3,\"output_tokens\":4}}}\n\n"
        );
        let legacy_chunks =
            crate::proxy::providers::streaming_responses::create_anthropic_sse_stream_from_responses(
                stream::iter(vec![Ok::<_, std::io::Error>(Bytes::from_static(
                    upstream.as_bytes(),
                ))]),
            )
            .collect::<Vec<_>>()
            .await;
        let mut session = ShadowComparisonSession::compare_request(prepared, &legacy_request);

        session.observe_stream_chunk(upstream.as_bytes());
        for chunk in legacy_chunks {
            session.observe_legacy_stream_chunk(&chunk.unwrap());
        }
        session.finish_stream();

        let report = session.report();
        assert_eq!(report.readiness.comparison_failures, 0);
        assert_eq!(report.readiness.unexplained_differences, 0);
        assert!(report.stream.as_ref().is_some_and(|stream| {
            stream.bounded && stream.shape_matches && stream.legacy.tool_events == 2
        }));
        assert!(report.state.tool_visible);
        let encoded = serde_json::to_string(&report).unwrap();
        for secret in ["fixture.txt", "call_1", "Read", "read_file"] {
            assert!(!encoded.contains(secret));
        }
    }

    #[test]
    fn rollout_readiness_requires_every_local_and_live_gate() {
        let mut input = ShadowReadinessInput {
            sample_count: 12,
            supported_fixture_count: 12,
            required_fixture_count: 12,
            expected_differences: 3,
            accepted_differences: 1,
            unexplained_differences: 0,
            comparison_failures: 0,
            forensic_suppressions: 0,
            forensic_failures: 0,
            visible_tool_retry_safe: true,
            rollback_available: true,
            live_smoke_status: LiveSmokeStatus::NotRun,
        };

        let not_run = calculate_shadow_readiness(&input);
        assert!(!not_run.ready);
        assert_eq!(not_run.live_smoke_status, LiveSmokeStatus::NotRun);
        assert_eq!(
            not_run.blocking_reasons,
            vec![ShadowReadinessBlocker::LiveSmokeNotPassed]
        );

        input.live_smoke_status = LiveSmokeStatus::Passed;
        assert!(calculate_shadow_readiness(&input).ready);

        input.unexplained_differences = 1;
        let unexplained = calculate_shadow_readiness(&input);
        assert!(!unexplained.ready);
        assert!(unexplained
            .blocking_reasons
            .contains(&ShadowReadinessBlocker::UnexplainedDifferences));

        input.unexplained_differences = 0;
        input.rollback_available = false;
        assert!(calculate_shadow_readiness(&input)
            .blocking_reasons
            .contains(&ShadowReadinessBlocker::RollbackUnavailable));
    }

    #[test]
    fn rollout_readiness_reports_every_blocker_and_never_infers_live_success() {
        let blocked = calculate_shadow_readiness(&ShadowReadinessInput {
            sample_count: 0,
            supported_fixture_count: 1,
            required_fixture_count: 2,
            expected_differences: 0,
            accepted_differences: 0,
            unexplained_differences: 1,
            comparison_failures: 1,
            forensic_suppressions: 1,
            forensic_failures: 1,
            visible_tool_retry_safe: false,
            rollback_available: false,
            live_smoke_status: LiveSmokeStatus::Pending,
        });

        assert!(!blocked.ready);
        assert_eq!(blocked.live_smoke_status, LiveSmokeStatus::Pending);
        assert_eq!(
            blocked.blocking_reasons,
            vec![
                ShadowReadinessBlocker::NoSamples,
                ShadowReadinessBlocker::FixtureCoverageIncomplete,
                ShadowReadinessBlocker::UnexplainedDifferences,
                ShadowReadinessBlocker::ComparisonFailures,
                ShadowReadinessBlocker::ForensicSuppression,
                ShadowReadinessBlocker::ForensicFailures,
                ShadowReadinessBlocker::VisibleToolRetryUnsafe,
                ShadowReadinessBlocker::RollbackUnavailable,
                ShadowReadinessBlocker::LiveSmokeNotPassed,
            ]
        );

        for status in [
            LiveSmokeStatus::NotRun,
            LiveSmokeStatus::Pending,
            LiveSmokeStatus::Failed,
            LiveSmokeStatus::Blocked,
        ] {
            let mut otherwise_ready = ShadowReadinessInput {
                sample_count: 1,
                supported_fixture_count: 1,
                required_fixture_count: 1,
                expected_differences: 0,
                accepted_differences: 0,
                unexplained_differences: 0,
                comparison_failures: 0,
                forensic_suppressions: 0,
                forensic_failures: 0,
                visible_tool_retry_safe: true,
                rollback_available: true,
                live_smoke_status: LiveSmokeStatus::Passed,
            };
            otherwise_ready.live_smoke_status = status;
            assert!(!calculate_shadow_readiness(&otherwise_ready).ready);
        }
    }

    #[test]
    fn stream_shape_classifies_tool_and_reasoning_payloads_without_content() {
        use crate::proxy::claude_codex_bridge::streaming::{
            ClaudeContentBlock, ClaudeContentDelta, ClaudeStreamEvent,
        };

        let mut bridge = ShadowStreamAccumulator::default();
        bridge.record_bridge_event(&ClaudeStreamEvent::ContentBlockStart {
            index: 0,
            block: ClaudeContentBlock::ToolUse {
                id: "call-secret".to_string(),
                name: "tool-secret".to_string(),
            },
        });
        bridge.record_bridge_event(&ClaudeStreamEvent::ContentBlockDelta {
            index: 0,
            delta: ClaudeContentDelta::InputJson {
                partial_json: "{\"secret\":true}".to_string(),
            },
        });
        bridge.record_bridge_event(&ClaudeStreamEvent::ContentBlockDelta {
            index: 1,
            delta: ClaudeContentDelta::Thinking {
                text: "private reasoning".to_string(),
            },
        });

        let mut legacy = ShadowStreamAccumulator::default();
        legacy.record_legacy_event(
            "content_block_start",
            &json!({"content_block":{"type":"tool_use","name":"tool-secret"}}),
        );
        legacy.record_legacy_event(
            "content_block_delta",
            &json!({"delta":{"type":"input_json_delta","partial_json":"secret"}}),
        );
        legacy.record_legacy_event(
            "content_block_delta",
            &json!({"delta":{"type":"thinking_delta","thinking":"private reasoning"}}),
        );

        assert_eq!(bridge.tool_events, 2);
        assert_eq!(legacy.tool_events, 2);
        assert_eq!(bridge.reasoning_events, 1);
        assert_eq!(legacy.reasoning_events, 1);
        assert_eq!(bridge.text_events, 0);
        assert_eq!(legacy.text_events, 0);

        let encoded = serde_json::to_string(&bridge.shape()).unwrap();
        assert!(!encoded.contains("secret"));
        assert!(!encoded.contains("private reasoning"));
    }
}
