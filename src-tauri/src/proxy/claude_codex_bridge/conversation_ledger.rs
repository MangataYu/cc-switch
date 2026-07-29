use super::{BridgeError, CodexOAuthCapabilities, SchemaLoss, ToolRegistry};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

pub const DEFAULT_LEDGER_MAX_SESSIONS: usize = 128;
pub const DEFAULT_LEDGER_MAX_TURNS_PER_SESSION: usize = 32;
pub const DEFAULT_LEDGER_TTL: Duration = Duration::from_secs(30 * 60);

#[derive(Clone, Copy, Debug)]
pub struct LedgerLimits {
    pub max_sessions: usize,
    pub max_turns_per_session: usize,
    pub ttl: Duration,
}

impl Default for LedgerLimits {
    fn default() -> Self {
        Self {
            max_sessions: DEFAULT_LEDGER_MAX_SESSIONS,
            max_turns_per_session: DEFAULT_LEDGER_MAX_TURNS_PER_SESSION,
            ttl: DEFAULT_LEDGER_TTL,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum ConversationConflictKind {
    UnknownSession,
    UnknownTurn,
    UnknownToolIdentity,
    CallIdConflict,
    StateRegression,
    ArgumentConflict,
    ResultConflict,
    OrphanToolResult,
    ReasoningBindingConflict,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum ToolCallState {
    Declared,
    ArgumentsStreaming,
    Ready,
    ReturnedToClaude,
    ResultObserved,
    Completed,
    Aborted,
}

#[derive(Clone, Debug)]
pub struct ToolCallRecord {
    pub call_id: String,
    pub binding_identity: String,
    pub state: ToolCallState,
    argument_fragment_hashes: HashSet<String>,
    arguments_hash: Option<String>,
    result_hash: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct TurnSafeSummary {
    pub tool_call_count: usize,
    pub reasoning_item_count: usize,
}

#[derive(Clone, Debug)]
pub struct TurnState {
    pub turn_id: String,
    pub request_fingerprint: String,
    pub tool_registry: Arc<ToolRegistry>,
    pub capability_snapshot: Arc<CodexOAuthCapabilities>,
    pub schema_losses: Vec<SchemaLoss>,
    pub calls: HashMap<String, ToolCallRecord>,
    pub reasoning_items: HashMap<String, ReasoningIdentityState>,
    pub safe_summary: TurnSafeSummary,
    pub compaction_epoch: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReasoningIdentityState {
    pub item_id: String,
    pub content_hash: String,
    pub identity_hash: String,
    pub state: ReasoningItemState,
    pub provider_hash: String,
    pub model_hash: String,
    pub capability_profile_version: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReasoningBinding {
    pub item_id: String,
    pub content_hash: String,
    pub identity_hash: String,
    pub provider_hash: String,
    pub model_hash: String,
    pub capability_profile_version: String,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum ReasoningItemState {
    Declared,
    Completed,
    Aborted,
}

#[derive(Clone, Debug)]
pub struct SessionState {
    pub session_identity_hash: String,
    pub generation: u64,
    pub capability_profile_version: String,
    pub turns: VecDeque<TurnState>,
    pub compaction_epoch: u64,
    pub last_access: SystemTime,
    pub expires_at: SystemTime,
    history_fingerprints: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TurnBinding {
    pub session_identity_hash: String,
    pub generation: u64,
    pub compaction_epoch: u64,
    pub turn_id: String,
}

#[derive(Clone, Debug)]
pub struct TurnRegistration {
    pub binding: TurnBinding,
    pub reused: bool,
    pub tool_registry: Arc<ToolRegistry>,
    pub capability_snapshot: Arc<CodexOAuthCapabilities>,
    pub schema_losses: Vec<SchemaLoss>,
}

#[cfg(test)]
#[derive(Clone, Debug)]
pub struct SessionSnapshot {
    pub session_identity_hash: String,
    pub generation: u64,
    pub capability_profile_version: String,
    pub compaction_epoch: u64,
    pub turn_count: usize,
    pub last_access: SystemTime,
    pub expires_at: SystemTime,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LedgerSnapshot {
    pub session_hash: String,
    pub generation: u64,
    pub epoch: u64,
    pub turn_id: String,
    pub fingerprint: String,
    pub registry_fingerprint: String,
    pub calls: Vec<LedgerCallSnapshot>,
    pub error_kind: Option<ConversationConflictKind>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LedgerCallSnapshot {
    pub tool_call_id: String,
    pub binding_identity: String,
    pub state: ToolCallState,
}

#[derive(Debug, Default)]
struct LedgerInner {
    sessions: HashMap<String, SessionState>,
    session_order: VecDeque<String>,
}

#[derive(Clone, Debug)]
pub struct ConversationLedger {
    inner: Arc<Mutex<LedgerInner>>,
    limits: LedgerLimits,
}

impl Default for ConversationLedger {
    fn default() -> Self {
        Self::with_limits(LedgerLimits::default())
    }
}

impl ConversationLedger {
    pub fn with_limits(limits: LedgerLimits) -> Self {
        assert!(limits.max_sessions > 0);
        assert!(limits.max_turns_per_session > 0);
        Self {
            inner: Arc::new(Mutex::new(LedgerInner::default())),
            limits,
        }
    }

    pub fn register_turn(
        &self,
        session_identity_hash: &str,
        request_fingerprint: &str,
        tool_registry: Arc<ToolRegistry>,
        capability_snapshot: Arc<CodexOAuthCapabilities>,
        schema_losses: Vec<SchemaLoss>,
        history_fingerprints: &[String],
    ) -> Result<TurnRegistration, BridgeError> {
        self.cleanup_expired();
        let now = SystemTime::now();
        let mut inner = self.inner.lock().expect("conversation ledger poisoned");
        touch_order(&mut inner.session_order, session_identity_hash);
        if !inner.sessions.contains_key(session_identity_hash) {
            while inner.sessions.len() >= self.limits.max_sessions {
                let evictable = inner.session_order.iter().find_map(|key| {
                    inner
                        .sessions
                        .get(key)
                        .is_some_and(|session| session.turns.iter().all(turn_is_evictable))
                        .then_some(key.clone())
                });
                let Some(oldest) = evictable else {
                    break;
                };
                inner.sessions.remove(&oldest);
                inner.session_order.retain(|key| key != &oldest);
            }
            inner.sessions.insert(
                session_identity_hash.to_string(),
                SessionState {
                    session_identity_hash: session_identity_hash.to_string(),
                    generation: 1,
                    capability_profile_version: capability_snapshot.profile_version.clone(),
                    turns: VecDeque::new(),
                    compaction_epoch: 0,
                    last_access: now,
                    expires_at: now + self.limits.ttl,
                    history_fingerprints: history_fingerprints.to_vec(),
                },
            );
        }
        let session = inner
            .sessions
            .get_mut(session_identity_hash)
            .expect("inserted session missing");
        session.last_access = now;
        session.expires_at = now + self.limits.ttl;
        if session.capability_profile_version != capability_snapshot.profile_version {
            session.generation += 1;
            session.compaction_epoch += 1;
            session.capability_profile_version = capability_snapshot.profile_version.clone();
        }

        if !session.history_fingerprints.is_empty()
            && !history_fingerprints.starts_with(&session.history_fingerprints)
        {
            session.compaction_epoch += 1;
            for turn in &mut session.turns {
                if turn.compaction_epoch < session.compaction_epoch {
                    turn.calls.retain(|_, call| {
                        !matches!(
                            call.state,
                            ToolCallState::Completed | ToolCallState::Aborted
                        )
                    });
                    turn.reasoning_items.retain(|_, item| {
                        !matches!(
                            item.state,
                            ReasoningItemState::Completed | ReasoningItemState::Aborted
                        )
                    });
                    turn.safe_summary.tool_call_count = turn.calls.len();
                    turn.safe_summary.reasoning_item_count = turn.reasoning_items.len();
                }
            }
        }
        session.history_fingerprints = history_fingerprints.to_vec();

        if let Some(turn) = session
            .turns
            .iter()
            .find(|turn| turn.request_fingerprint == request_fingerprint)
        {
            return Ok(registration(session, turn, true));
        }

        while session.turns.len() >= self.limits.max_turns_per_session {
            let evictable = session.turns.iter().position(turn_is_evictable);
            let Some(index) = evictable else {
                break;
            };
            session.turns.remove(index);
        }
        session.turns.push_back(TurnState {
            turn_id: uuid::Uuid::new_v4().to_string(),
            request_fingerprint: request_fingerprint.to_string(),
            tool_registry,
            capability_snapshot,
            schema_losses,
            calls: HashMap::new(),
            reasoning_items: HashMap::new(),
            safe_summary: TurnSafeSummary::default(),
            compaction_epoch: session.compaction_epoch,
        });
        let turn = session.turns.back().expect("inserted turn missing");
        Ok(registration(session, turn, false))
    }

    pub fn lookup_turn(
        &self,
        session_identity_hash: &str,
        request_fingerprint: &str,
    ) -> Option<TurnRegistration> {
        self.cleanup_expired();
        let now = SystemTime::now();
        let mut inner = self.inner.lock().ok()?;
        touch_order(&mut inner.session_order, session_identity_hash);
        let session = inner.sessions.get_mut(session_identity_hash)?;
        session.last_access = now;
        session.expires_at = now + self.limits.ttl;
        let turn = session
            .turns
            .iter()
            .find(|turn| turn.request_fingerprint == request_fingerprint)?;
        Some(registration(session, turn, true))
    }

    pub fn observe_result_for_session(
        &self,
        session_identity_hash: &str,
        call_id: &str,
        result_hash: &str,
    ) -> Result<(), BridgeError> {
        let binding = {
            let inner = self.inner.lock().expect("conversation ledger poisoned");
            let session = inner.sessions.get(session_identity_hash).ok_or_else(|| {
                conflict(
                    ConversationConflictKind::OrphanToolResult,
                    "tool_result has no ledger-known session",
                )
            })?;
            let turn = session
                .turns
                .iter()
                .rev()
                .find(|turn| turn.calls.contains_key(call_id))
                .ok_or_else(|| {
                    conflict(
                        ConversationConflictKind::OrphanToolResult,
                        "tool_result has no ledger-known call_id",
                    )
                })?;
            TurnBinding {
                session_identity_hash: session.session_identity_hash.clone(),
                generation: session.generation,
                compaction_epoch: turn.compaction_epoch,
                turn_id: turn.turn_id.clone(),
            }
        };
        self.observe_result(&binding, call_id, result_hash)
    }

    pub fn declare_call(
        &self,
        binding: &TurnBinding,
        call_id: &str,
        codex_name: &str,
    ) -> Result<(), BridgeError> {
        self.with_turn_mut(binding, |turn| {
            if let Some(existing) = turn.calls.get(call_id) {
                return if existing.binding_identity == codex_name {
                    Ok(())
                } else {
                    Err(conflict(
                        ConversationConflictKind::CallIdConflict,
                        "call_id is already bound to a different tool identity",
                    ))
                };
            }
            turn.tool_registry
                .claude_name_for_codex(codex_name)
                .map_err(|_| {
                    conflict(
                        ConversationConflictKind::UnknownToolIdentity,
                        "tool identity is not registered for this turn",
                    )
                })?;
            turn.calls.insert(
                call_id.to_string(),
                ToolCallRecord {
                    call_id: call_id.to_string(),
                    binding_identity: codex_name.to_string(),
                    state: ToolCallState::Declared,
                    argument_fragment_hashes: HashSet::new(),
                    arguments_hash: None,
                    result_hash: None,
                },
            );
            turn.safe_summary.tool_call_count = turn.calls.len();
            Ok(())
        })
    }

    pub fn arguments_streaming(
        &self,
        binding: &TurnBinding,
        call_id: &str,
        fragment_hash: &str,
    ) -> Result<(), BridgeError> {
        self.with_call_mut(binding, call_id, |call| match call.state {
            ToolCallState::Declared | ToolCallState::ArgumentsStreaming => {
                call.argument_fragment_hashes
                    .insert(fragment_hash.to_string());
                call.state = ToolCallState::ArgumentsStreaming;
                Ok(())
            }
            _ => Err(conflict(
                ConversationConflictKind::StateRegression,
                "argument fragment arrived after arguments were finalized",
            )),
        })
    }

    pub fn mark_ready(
        &self,
        binding: &TurnBinding,
        call_id: &str,
        arguments_hash: &str,
    ) -> Result<(), BridgeError> {
        self.with_call_mut(binding, call_id, |call| {
            if let Some(existing) = &call.arguments_hash {
                return if existing == arguments_hash {
                    Ok(())
                } else {
                    Err(conflict(
                        ConversationConflictKind::ArgumentConflict,
                        "completed tool arguments changed for the same call_id",
                    ))
                };
            }
            match call.state {
                ToolCallState::Declared | ToolCallState::ArgumentsStreaming => {
                    call.arguments_hash = Some(arguments_hash.to_string());
                    call.state = ToolCallState::Ready;
                    Ok(())
                }
                _ => Err(conflict(
                    ConversationConflictKind::StateRegression,
                    "tool call cannot become ready from its current state",
                )),
            }
        })
    }

    pub fn mark_returned(&self, binding: &TurnBinding, call_id: &str) -> Result<(), BridgeError> {
        self.with_call_mut(binding, call_id, |call| match call.state {
            ToolCallState::Ready => {
                call.state = ToolCallState::ReturnedToClaude;
                Ok(())
            }
            ToolCallState::ReturnedToClaude
            | ToolCallState::ResultObserved
            | ToolCallState::Completed => Ok(()),
            _ => Err(conflict(
                ConversationConflictKind::StateRegression,
                "tool call was returned before its arguments were ready",
            )),
        })
    }

    pub fn observe_result(
        &self,
        binding: &TurnBinding,
        call_id: &str,
        result_hash: &str,
    ) -> Result<(), BridgeError> {
        self.with_turn_mut(binding, |turn| {
            let call = turn.calls.get_mut(call_id).ok_or_else(|| {
                conflict(
                    ConversationConflictKind::OrphanToolResult,
                    "tool_result has no ledger-known call_id",
                )
            })?;
            if let Some(existing) = &call.result_hash {
                return if existing == result_hash {
                    Ok(())
                } else {
                    Err(conflict(
                        ConversationConflictKind::ResultConflict,
                        "tool_result changed for the same call_id",
                    ))
                };
            }
            if call.state != ToolCallState::ReturnedToClaude {
                return Err(conflict(
                    ConversationConflictKind::StateRegression,
                    "tool_result was observed before the call was returned to Claude",
                ));
            }
            call.result_hash = Some(result_hash.to_string());
            call.state = ToolCallState::ResultObserved;
            call.state = ToolCallState::Completed;
            Ok(())
        })
    }

    pub fn observe_reasoning(
        &self,
        turn_binding: &TurnBinding,
        binding: ReasoningBinding,
        state: ReasoningItemState,
    ) -> Result<(), BridgeError> {
        {
            let inner = self.inner.lock().expect("conversation ledger poisoned");
            for session in inner.sessions.values() {
                for turn in &session.turns {
                    if let Some(existing) = turn.reasoning_items.get(&binding.item_id) {
                        let same_owner = session.session_identity_hash
                            == turn_binding.session_identity_hash
                            && turn.turn_id == turn_binding.turn_id;
                        let same_identity = existing.content_hash == binding.content_hash
                            && existing.identity_hash == binding.identity_hash
                            && existing.provider_hash == binding.provider_hash
                            && existing.model_hash == binding.model_hash
                            && existing.capability_profile_version
                                == binding.capability_profile_version;
                        if !same_owner || !same_identity {
                            return Err(conflict(
                                ConversationConflictKind::ReasoningBindingConflict,
                                "reasoning identity cannot cross its original binding",
                            ));
                        }
                    }
                }
            }
        }

        self.with_turn_mut(turn_binding, |turn| {
            if let Some(existing) = turn.reasoning_items.get_mut(&binding.item_id) {
                if state < existing.state {
                    return Err(conflict(
                        ConversationConflictKind::StateRegression,
                        "reasoning item state cannot move backward",
                    ));
                }
                if existing.state == ReasoningItemState::Aborted
                    && state != ReasoningItemState::Aborted
                {
                    return Err(conflict(
                        ConversationConflictKind::StateRegression,
                        "aborted reasoning item cannot resume",
                    ));
                }
                existing.state = state;
                return Ok(());
            }
            turn.reasoning_items.insert(
                binding.item_id.clone(),
                ReasoningIdentityState {
                    item_id: binding.item_id,
                    content_hash: binding.content_hash,
                    identity_hash: binding.identity_hash,
                    state,
                    provider_hash: binding.provider_hash,
                    model_hash: binding.model_hash,
                    capability_profile_version: binding.capability_profile_version,
                },
            );
            turn.safe_summary.reasoning_item_count = turn.reasoning_items.len();
            Ok(())
        })
    }

    pub fn call_state(&self, binding: &TurnBinding, call_id: &str) -> Option<ToolCallState> {
        let inner = self.inner.lock().ok()?;
        find_turn(&inner, binding)
            .and_then(|turn| turn.calls.get(call_id))
            .map(|call| call.state)
    }

    #[cfg(test)]
    pub fn session_snapshot(&self, session_identity_hash: &str) -> Option<SessionSnapshot> {
        let inner = self.inner.lock().ok()?;
        let session = inner.sessions.get(session_identity_hash)?;
        Some(SessionSnapshot {
            session_identity_hash: session.session_identity_hash.clone(),
            generation: session.generation,
            capability_profile_version: session.capability_profile_version.clone(),
            compaction_epoch: session.compaction_epoch,
            turn_count: session.turns.len(),
            last_access: session.last_access,
            expires_at: session.expires_at,
        })
    }

    pub fn snapshot(
        &self,
        binding: &TurnBinding,
        error_kind: Option<ConversationConflictKind>,
    ) -> Option<LedgerSnapshot> {
        let inner = self.inner.lock().ok()?;
        let turn = find_turn(&inner, binding)?;
        let mut calls = turn
            .calls
            .values()
            .map(|call| LedgerCallSnapshot {
                tool_call_id: call.call_id.clone(),
                binding_identity: call.binding_identity.clone(),
                state: call.state,
            })
            .collect::<Vec<_>>();
        calls.sort_by(|left, right| left.tool_call_id.cmp(&right.tool_call_id));
        Some(LedgerSnapshot {
            session_hash: binding.session_identity_hash.clone(),
            generation: binding.generation,
            epoch: turn.compaction_epoch,
            turn_id: turn.turn_id.clone(),
            fingerprint: turn.request_fingerprint.clone(),
            registry_fingerprint: turn.tool_registry.identity_fingerprint().to_string(),
            calls,
            error_kind,
        })
    }

    #[cfg(test)]
    pub fn reasoning_state(
        &self,
        binding: &TurnBinding,
        item_id: &str,
    ) -> Option<ReasoningIdentityState> {
        let inner = self.inner.lock().ok()?;
        find_turn(&inner, binding)?
            .reasoning_items
            .get(item_id)
            .cloned()
    }

    pub fn cleanup_expired(&self) -> usize {
        let now = SystemTime::now();
        let mut inner = self.inner.lock().expect("conversation ledger poisoned");
        let expired = inner
            .sessions
            .iter()
            .filter_map(|(key, session)| {
                let protected = session.turns.iter().any(|turn| !turn_is_evictable(turn));
                (session.expires_at <= now && !protected).then_some(key.clone())
            })
            .collect::<Vec<_>>();
        for key in &expired {
            inner.sessions.remove(key);
        }
        inner.session_order.retain(|key| !expired.contains(key));
        expired.len()
    }

    fn with_call_mut<T>(
        &self,
        binding: &TurnBinding,
        call_id: &str,
        operation: impl FnOnce(&mut ToolCallRecord) -> Result<T, BridgeError>,
    ) -> Result<T, BridgeError> {
        self.with_turn_mut(binding, |turn| {
            let call = turn.calls.get_mut(call_id).ok_or_else(|| {
                conflict(
                    ConversationConflictKind::CallIdConflict,
                    "call_id is not declared for this turn",
                )
            })?;
            operation(call)
        })
    }

    fn with_turn_mut<T>(
        &self,
        binding: &TurnBinding,
        operation: impl FnOnce(&mut TurnState) -> Result<T, BridgeError>,
    ) -> Result<T, BridgeError> {
        let mut inner = self.inner.lock().expect("conversation ledger poisoned");
        let session = inner
            .sessions
            .get_mut(&binding.session_identity_hash)
            .ok_or_else(|| conflict(ConversationConflictKind::UnknownSession, "session expired"))?;
        if session.generation != binding.generation {
            return Err(conflict(
                ConversationConflictKind::UnknownSession,
                "session generation no longer matches",
            ));
        }
        let turn = session
            .turns
            .iter_mut()
            .find(|turn| turn.turn_id == binding.turn_id)
            .ok_or_else(|| conflict(ConversationConflictKind::UnknownTurn, "turn expired"))?;
        operation(turn)
    }
}

fn registration(session: &SessionState, turn: &TurnState, reused: bool) -> TurnRegistration {
    TurnRegistration {
        binding: TurnBinding {
            session_identity_hash: session.session_identity_hash.clone(),
            generation: session.generation,
            compaction_epoch: turn.compaction_epoch,
            turn_id: turn.turn_id.clone(),
        },
        reused,
        tool_registry: turn.tool_registry.clone(),
        capability_snapshot: turn.capability_snapshot.clone(),
        schema_losses: turn.schema_losses.clone(),
    }
}

fn find_turn<'a>(inner: &'a LedgerInner, binding: &TurnBinding) -> Option<&'a TurnState> {
    let session = inner.sessions.get(&binding.session_identity_hash)?;
    if session.generation != binding.generation {
        return None;
    }
    session
        .turns
        .iter()
        .find(|turn| turn.turn_id == binding.turn_id)
}

fn touch_order(order: &mut VecDeque<String>, session_identity_hash: &str) {
    if let Some(index) = order
        .iter()
        .position(|existing| existing == session_identity_hash)
    {
        order.remove(index);
    }
    order.push_back(session_identity_hash.to_string());
}

fn turn_is_evictable(turn: &TurnState) -> bool {
    turn.calls.values().all(|call| {
        matches!(
            call.state,
            ToolCallState::Completed | ToolCallState::Aborted
        )
    }) && turn.reasoning_items.values().all(|item| {
        matches!(
            item.state,
            ReasoningItemState::Completed | ReasoningItemState::Aborted
        )
    })
}

fn conflict(kind: ConversationConflictKind, summary: &str) -> BridgeError {
    BridgeError::ConversationStateConflict {
        kind,
        summary: summary.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proxy::claude_codex_bridge::{
        CodexOAuthCapabilities, ConversationConflictKind, ToolRegistry,
    };
    use serde_json::json;
    use std::{sync::Arc, time::Duration};

    fn registry() -> Arc<ToolRegistry> {
        Arc::new(
            ToolRegistry::compile(
                &[json!({
                    "name": "Read",
                    "input_schema": {
                        "type": "object",
                        "properties": {"file_path": {"type": "string"}},
                        "required": ["file_path"],
                        "additionalProperties": false
                    }
                })],
                CodexOAuthCapabilities::builtin().as_ref(),
            )
            .unwrap()
            .0,
        )
    }

    fn registered_turn(ledger: &ConversationLedger) -> TurnRegistration {
        ledger
            .register_turn(
                "session-hash",
                "request-fingerprint",
                registry(),
                CodexOAuthCapabilities::builtin(),
                Vec::new(),
                &[],
            )
            .unwrap()
    }

    #[test]
    fn tracks_full_tool_lifecycle_and_parallel_calls_independently() {
        let ledger = ConversationLedger::with_limits(LedgerLimits {
            max_sessions: 4,
            max_turns_per_session: 4,
            ttl: Duration::from_secs(60),
        });
        let turn = registered_turn(&ledger);

        for call_id in ["call-1", "call-2"] {
            ledger
                .declare_call(&turn.binding, call_id, "read_file")
                .unwrap();
            ledger
                .arguments_streaming(&turn.binding, call_id, "chunk-hash")
                .unwrap();
            ledger
                .mark_ready(&turn.binding, call_id, "arguments-hash")
                .unwrap();
            ledger.mark_returned(&turn.binding, call_id).unwrap();
        }

        assert_eq!(
            ledger.call_state(&turn.binding, "call-1"),
            Some(ToolCallState::ReturnedToClaude)
        );
        assert_eq!(
            ledger.call_state(&turn.binding, "call-2"),
            Some(ToolCallState::ReturnedToClaude)
        );

        ledger
            .observe_result(&turn.binding, "call-2", "result-hash")
            .unwrap();
        assert_eq!(
            ledger.call_state(&turn.binding, "call-2"),
            Some(ToolCallState::Completed)
        );
        assert_eq!(
            ledger.call_state(&turn.binding, "call-1"),
            Some(ToolCallState::ReturnedToClaude)
        );
    }

    #[test]
    fn identical_events_are_idempotent_but_regressions_and_conflicts_fail() {
        let ledger = ConversationLedger::default();
        let turn = registered_turn(&ledger);
        ledger
            .declare_call(&turn.binding, "call-1", "read_file")
            .unwrap();
        ledger
            .declare_call(&turn.binding, "call-1", "read_file")
            .unwrap();
        ledger
            .arguments_streaming(&turn.binding, "call-1", "chunk-hash")
            .unwrap();
        ledger
            .arguments_streaming(&turn.binding, "call-1", "chunk-hash")
            .unwrap();
        ledger
            .mark_ready(&turn.binding, "call-1", "arguments-hash")
            .unwrap();
        ledger
            .mark_ready(&turn.binding, "call-1", "arguments-hash")
            .unwrap();
        ledger.mark_returned(&turn.binding, "call-1").unwrap();
        ledger
            .observe_result(&turn.binding, "call-1", "result-hash")
            .unwrap();
        ledger
            .observe_result(&turn.binding, "call-1", "result-hash")
            .unwrap();

        let identity_conflict = ledger
            .declare_call(&turn.binding, "call-1", "unknown_tool")
            .unwrap_err();
        assert_eq!(
            identity_conflict.conversation_conflict_kind().unwrap(),
            ConversationConflictKind::CallIdConflict
        );
        let argument_conflict = ledger
            .mark_ready(&turn.binding, "call-1", "different-arguments")
            .unwrap_err();
        assert_eq!(
            argument_conflict.conversation_conflict_kind().unwrap(),
            ConversationConflictKind::ArgumentConflict
        );
        let result_conflict = ledger
            .observe_result(&turn.binding, "call-1", "different-result")
            .unwrap_err();
        assert_eq!(
            result_conflict.conversation_conflict_kind().unwrap(),
            ConversationConflictKind::ResultConflict
        );
    }

    #[test]
    fn rejects_unknown_tool_identity_and_orphan_result() {
        let ledger = ConversationLedger::default();
        let turn = registered_turn(&ledger);

        let unknown = ledger
            .declare_call(&turn.binding, "call-1", "not_registered")
            .unwrap_err();
        assert_eq!(
            unknown.conversation_conflict_kind().unwrap(),
            ConversationConflictKind::UnknownToolIdentity
        );

        let orphan = ledger
            .observe_result(&turn.binding, "missing", "result-hash")
            .unwrap_err();
        assert_eq!(
            orphan.conversation_conflict_kind().unwrap(),
            ConversationConflictKind::OrphanToolResult
        );
    }

    #[test]
    fn session_metadata_is_bounded_and_contains_ttl_information() {
        let ledger = ConversationLedger::with_limits(LedgerLimits {
            max_sessions: 1,
            max_turns_per_session: 1,
            ttl: Duration::from_secs(30),
        });
        let first = registered_turn(&ledger);
        ledger
            .register_turn(
                "session-hash",
                "second-fingerprint",
                registry(),
                CodexOAuthCapabilities::builtin(),
                Vec::new(),
                &[],
            )
            .unwrap();
        let snapshot = ledger.session_snapshot("session-hash").unwrap();

        assert_eq!(snapshot.session_identity_hash, "session-hash");
        assert_eq!(snapshot.generation, 1);
        assert_eq!(
            snapshot.capability_profile_version,
            "codex-oauth-2026-07-29.v1"
        );
        assert_eq!(snapshot.turn_count, 1);
        assert!(snapshot.expires_at > snapshot.last_access);
        assert!(ledger.snapshot(&first.binding, None).is_none());
    }

    #[test]
    fn incremental_history_does_not_compact_but_discontinuity_advances_epoch() {
        let ledger = ConversationLedger::default();
        let registry = registry();
        let capabilities = CodexOAuthCapabilities::builtin();
        ledger
            .register_turn(
                "session-hash",
                "request-1",
                registry.clone(),
                capabilities.clone(),
                Vec::new(),
                &["m1".to_string()],
            )
            .unwrap();
        ledger
            .register_turn(
                "session-hash",
                "request-2",
                registry.clone(),
                capabilities.clone(),
                Vec::new(),
                &["m1".to_string(), "m2".to_string()],
            )
            .unwrap();
        assert_eq!(
            ledger
                .session_snapshot("session-hash")
                .unwrap()
                .compaction_epoch,
            0
        );

        let compacted = ledger
            .register_turn(
                "session-hash",
                "request-3",
                registry,
                capabilities,
                Vec::new(),
                &["summary".to_string(), "m3".to_string()],
            )
            .unwrap();
        assert_eq!(compacted.binding.compaction_epoch, 1);
        assert_eq!(
            ledger
                .session_snapshot("session-hash")
                .unwrap()
                .compaction_epoch,
            1
        );
    }

    #[test]
    fn compaction_cleans_closed_calls_but_retains_active_calls() {
        let ledger = ConversationLedger::default();
        let active = registered_turn(&ledger);
        for call_id in ["completed", "active"] {
            ledger
                .declare_call(&active.binding, call_id, "read_file")
                .unwrap();
            ledger
                .mark_ready(&active.binding, call_id, &format!("args-{call_id}"))
                .unwrap();
            ledger.mark_returned(&active.binding, call_id).unwrap();
        }
        ledger
            .observe_result(&active.binding, "completed", "result-hash")
            .unwrap();

        ledger
            .register_turn(
                "session-hash",
                "history-baseline",
                registry(),
                CodexOAuthCapabilities::builtin(),
                Vec::new(),
                &["original".to_string()],
            )
            .unwrap();

        ledger
            .register_turn(
                "session-hash",
                "compacted-request",
                registry(),
                CodexOAuthCapabilities::builtin(),
                Vec::new(),
                &["discontinuous".to_string()],
            )
            .unwrap();

        assert_eq!(ledger.call_state(&active.binding, "completed"), None);
        assert_eq!(
            ledger.call_state(&active.binding, "active"),
            Some(ToolCallState::ReturnedToClaude)
        );
    }

    #[test]
    fn reasoning_identity_is_bound_to_turn_provider_model_and_profile() {
        let ledger = ConversationLedger::default();
        let turn = registered_turn(&ledger);
        let binding = ReasoningBinding {
            item_id: "reasoning-1".to_string(),
            content_hash: "encrypted-hash".to_string(),
            identity_hash: "identity-hash".to_string(),
            provider_hash: "provider-hash".to_string(),
            model_hash: "model-hash".to_string(),
            capability_profile_version: "codex-oauth-2026-07-29.v1".to_string(),
        };

        ledger
            .observe_reasoning(&turn.binding, binding.clone(), ReasoningItemState::Declared)
            .unwrap();
        ledger
            .observe_reasoning(
                &turn.binding,
                binding.clone(),
                ReasoningItemState::Completed,
            )
            .unwrap();
        ledger
            .observe_reasoning(
                &turn.binding,
                binding.clone(),
                ReasoningItemState::Completed,
            )
            .unwrap();

        let mut wrong_model = binding;
        wrong_model.model_hash = "other-model".to_string();
        assert_eq!(
            ledger
                .observe_reasoning(&turn.binding, wrong_model, ReasoningItemState::Completed)
                .unwrap_err()
                .conversation_conflict_kind()
                .unwrap(),
            ConversationConflictKind::ReasoningBindingConflict
        );
    }

    #[test]
    fn child_agent_session_identity_is_independent() {
        let ledger = ConversationLedger::default();
        let parent = registered_turn(&ledger);
        let child = ledger
            .register_turn(
                "child-session-hash",
                "request-fingerprint",
                registry(),
                CodexOAuthCapabilities::builtin(),
                Vec::new(),
                &[],
            )
            .unwrap();

        assert_ne!(
            parent.binding.session_identity_hash,
            child.binding.session_identity_hash
        );
        assert_ne!(parent.binding.turn_id, child.binding.turn_id);
    }

    #[test]
    fn ledger_snapshot_contains_only_safe_structural_state() {
        let ledger = ConversationLedger::default();
        let turn = registered_turn(&ledger);
        ledger
            .declare_call(&turn.binding, "call-secret", "read_file")
            .unwrap();
        ledger
            .mark_ready(&turn.binding, "call-secret", "hashed-arguments")
            .unwrap();
        ledger.mark_returned(&turn.binding, "call-secret").unwrap();

        let snapshot = ledger.snapshot(&turn.binding, None).unwrap();
        let serialized = serde_json::to_string(&snapshot).unwrap();

        assert!(serialized.contains("call-secret"));
        assert!(serialized.contains("read_file"));
        for forbidden in [
            "super secret prompt",
            "fn main()",
            "raw tool arguments",
            "raw tool result",
            "access_token",
            "plaintext reasoning",
        ] {
            assert!(!serialized.contains(forbidden));
        }
    }

    #[test]
    fn ttl_cleanup_preserves_sessions_with_visible_or_active_calls() {
        let ledger = ConversationLedger::with_limits(LedgerLimits {
            max_sessions: 4,
            max_turns_per_session: 4,
            ttl: Duration::ZERO,
        });
        let protected = registered_turn(&ledger);
        ledger
            .declare_call(&protected.binding, "call-1", "read_file")
            .unwrap();
        ledger
            .mark_ready(&protected.binding, "call-1", "args-hash")
            .unwrap();
        ledger.mark_returned(&protected.binding, "call-1").unwrap();

        let removed = ledger.cleanup_expired();

        assert_eq!(removed, 0);
        assert!(ledger.session_snapshot("session-hash").is_some());
    }

    #[test]
    fn session_capacity_never_evicts_visible_or_active_calls() {
        let ledger = ConversationLedger::with_limits(LedgerLimits {
            max_sessions: 1,
            max_turns_per_session: 2,
            ttl: Duration::from_secs(60),
        });
        let protected = registered_turn(&ledger);
        ledger
            .declare_call(&protected.binding, "call-1", "read_file")
            .unwrap();
        ledger
            .mark_ready(&protected.binding, "call-1", "args-hash")
            .unwrap();
        ledger.mark_returned(&protected.binding, "call-1").unwrap();

        ledger
            .register_turn(
                "second-session",
                "second-request",
                registry(),
                CodexOAuthCapabilities::builtin(),
                Vec::new(),
                &[],
            )
            .unwrap();

        assert!(ledger.session_snapshot("session-hash").is_some());
        assert!(ledger.session_snapshot("second-session").is_some());
    }
}
