use super::{
    adapt_schema, validate_arguments, BridgeError, CodexOAuthCapabilities, SchemaLoss, SupportLevel,
};
use crate::proxy::json_canonical::canonical_json_string;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionOwner {
    ClaudeCode,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ToolSemantics {
    Builtin,
    ClaudeToolSearch,
    Dynamic,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ToolBinding {
    pub claude_name: String,
    pub codex_name: String,
    pub claude_schema: Value,
    pub codex_schema: Value,
    pub schema_hash: String,
    pub execution_owner: ExecutionOwner,
    pub semantics: ToolSemantics,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RestoredToolCall {
    pub claude_name: String,
    pub tool_use_id: String,
    pub input: Value,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TransformAction {
    Preserved,
    Renamed,
    Normalized,
    Dropped,
    Rejected,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransformDecision {
    pub source_path: String,
    pub source_value_type: String,
    pub target_path: Option<String>,
    pub action: TransformAction,
    pub reason_code: String,
    pub capability_reference: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ToolRegistry {
    bindings: Vec<ToolBinding>,
    codex_tools: Vec<Value>,
    codex_to_index: BTreeMap<String, usize>,
    identity_fingerprint: String,
    schema_fingerprint: String,
}

impl ToolRegistry {
    pub fn compile(
        tools: &[Value],
        capabilities: &CodexOAuthCapabilities,
    ) -> Result<(Self, Vec<SchemaLoss>), BridgeError> {
        if capabilities.function_tools == SupportLevel::Unsupported && !tools.is_empty() {
            return registry_error("function tools are unsupported by this capability profile");
        }
        let dynamic_names = plan_dynamic_names(tools)?;
        let mut bindings = Vec::with_capacity(tools.len());
        let mut codex_tools = Vec::with_capacity(tools.len());
        let mut claude_to_index = BTreeMap::new();
        let mut codex_to_index = BTreeMap::new();
        let mut losses = Vec::new();

        for tool in tools {
            let object = tool
                .as_object()
                .ok_or_else(|| BridgeError::ToolRegistryViolation {
                    summary: "tool definition must be an object".to_string(),
                })?;
            if object.get("type").and_then(Value::as_str) == Some("BatchTool") {
                return registry_error("BatchTool is unsupported by the Stage 2 bridge");
            }
            let claude_name = object
                .get("name")
                .and_then(Value::as_str)
                .filter(|name| !name.trim().is_empty())
                .ok_or_else(|| BridgeError::ToolRegistryViolation {
                    summary: "tool definition requires a non-empty name".to_string(),
                })?
                .to_string();
            if claude_to_index.contains_key(&claude_name) {
                return registry_error(&format!("duplicate Claude tool name: {claude_name}"));
            }
            let (codex_name, semantics) = builtin_alias(&claude_name)
                .map(|alias| (alias.to_string(), builtin_semantics(&claude_name)))
                .unwrap_or_else(|| {
                    (
                        dynamic_names
                            .get(&claude_name)
                            .expect("dynamic name was precomputed")
                            .clone(),
                        ToolSemantics::Dynamic,
                    )
                });
            if codex_to_index.contains_key(&codex_name) {
                return registry_error(&format!("conflicting Codex tool alias: {codex_name}"));
            }
            let claude_schema = object
                .get("input_schema")
                .cloned()
                .unwrap_or_else(|| json!({}));
            if !claude_schema.is_object() {
                return registry_error(&format!(
                    "input_schema for {claude_name} must be an object"
                ));
            }
            let adapted = adapt_schema(&claude_schema)?;
            losses.extend(adapted.decisions.iter().cloned());
            let description = model_description(
                &claude_name,
                semantics,
                object.get("description").and_then(Value::as_str),
            );
            let index = bindings.len();
            bindings.push(ToolBinding {
                claude_name: claude_name.clone(),
                codex_name: codex_name.clone(),
                claude_schema,
                codex_schema: adapted.schema.clone(),
                schema_hash: adapted.schema_hash,
                execution_owner: ExecutionOwner::ClaudeCode,
                semantics,
            });
            codex_tools.push(json!({
                "type": "function",
                "name": codex_name,
                "description": description,
                "parameters": adapted.schema
            }));
            claude_to_index.insert(claude_name, index);
            codex_to_index.insert(codex_name, index);
        }

        let identity_value = Value::Array(
            bindings
                .iter()
                .map(|binding| json!([binding.claude_name, binding.codex_name]))
                .collect(),
        );
        let schema_value = Value::Array(
            bindings
                .iter()
                .map(|binding| json!([binding.codex_name, binding.schema_hash]))
                .collect(),
        );
        Ok((
            Self {
                bindings,
                codex_tools,
                codex_to_index,
                identity_fingerprint: hash_value(&identity_value),
                schema_fingerprint: hash_value(&schema_value),
            },
            losses,
        ))
    }

    pub fn bindings(&self) -> &[ToolBinding] {
        &self.bindings
    }

    pub fn codex_tools(&self) -> &[Value] {
        &self.codex_tools
    }

    pub fn identity_fingerprint(&self) -> &str {
        &self.identity_fingerprint
    }

    pub fn schema_fingerprint(&self) -> &str {
        &self.schema_fingerprint
    }

    pub fn transform_decisions(&self, schema_losses: &[SchemaLoss]) -> Vec<TransformDecision> {
        let mut decisions: Vec<TransformDecision> = self
            .bindings
            .iter()
            .enumerate()
            .map(|(index, binding)| TransformDecision {
                source_path: format!("$/tools/{index}/name"),
                source_value_type: "string".to_string(),
                target_path: Some(format!("$/tools/{index}/name")),
                action: if binding.claude_name == binding.codex_name {
                    TransformAction::Preserved
                } else {
                    TransformAction::Renamed
                },
                reason_code: if binding.claude_name == binding.codex_name {
                    "tool_identity_preserved"
                } else {
                    "codex_semantic_alias"
                }
                .to_string(),
                capability_reference: Some("function_tools".to_string()),
            })
            .collect();
        decisions.extend(schema_losses.iter().map(|loss| TransformDecision {
            source_path: loss.source_path.clone(),
            source_value_type: "schema".to_string(),
            target_path:
                (loss.action != super::SchemaAction::Drop).then(|| loss.source_path.clone()),
            action: match loss.action {
                super::SchemaAction::Preserve => TransformAction::Preserved,
                super::SchemaAction::Normalize => TransformAction::Normalized,
                super::SchemaAction::Drop => TransformAction::Dropped,
                super::SchemaAction::Reject => TransformAction::Rejected,
            },
            reason_code: format!("{:?}", loss.reason).to_ascii_lowercase(),
            capability_reference: Some("strict_json_schema".to_string()),
        }));
        decisions
    }

    pub fn codex_name_for_claude(&self, claude_name: &str) -> Result<&str, BridgeError> {
        self.bindings
            .iter()
            .find(|binding| binding.claude_name == claude_name)
            .map(|binding| binding.codex_name.as_str())
            .ok_or_else(|| BridgeError::ToolRegistryViolation {
                summary: format!("Claude tool is not registered for this turn: {claude_name}"),
            })
    }

    pub fn claude_name_for_codex(&self, codex_name: &str) -> Result<&str, BridgeError> {
        let index = self.codex_to_index.get(codex_name).ok_or_else(|| {
            BridgeError::ToolRegistryViolation {
                summary: format!("upstream tool is not registered for this turn: {codex_name}"),
            }
        })?;
        Ok(self.bindings[*index].claude_name.as_str())
    }

    pub fn restore_call(
        &self,
        codex_name: &str,
        call_id: &str,
        arguments: &str,
    ) -> Result<RestoredToolCall, BridgeError> {
        if call_id.is_empty() {
            return registry_error("upstream tool call requires a non-empty call_id");
        }
        let index = self.codex_to_index.get(codex_name).ok_or_else(|| {
            BridgeError::ToolRegistryViolation {
                summary: format!("upstream tool is not registered for this turn: {codex_name}"),
            }
        })?;
        let binding = &self.bindings[*index];
        let input: Value =
            serde_json::from_str(arguments).map_err(|_| BridgeError::ToolRegistryViolation {
                summary: format!("arguments for {codex_name} are not valid JSON"),
            })?;
        if !input.is_object() {
            return registry_error(&format!("arguments for {codex_name} must be a JSON object"));
        }
        validate_arguments(&binding.claude_schema, &input)?;
        Ok(RestoredToolCall {
            claude_name: binding.claude_name.clone(),
            tool_use_id: call_id.to_string(),
            input,
        })
    }

    pub fn restore_response(&self, response: &Value) -> Result<Value, BridgeError> {
        let mut restored = response.clone();
        let output = restored
            .get_mut("output")
            .and_then(Value::as_array_mut)
            .ok_or_else(|| BridgeError::ToolRegistryViolation {
                summary: "Responses payload requires an output array".to_string(),
            })?;
        for item in output {
            if item.get("type").and_then(Value::as_str) != Some("function_call") {
                continue;
            }
            let codex_name = item
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let call_id = item
                .get("call_id")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let arguments = item
                .get("arguments")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let call = self.restore_call(&codex_name, &call_id, &arguments)?;
            item["name"] = Value::String(call.claude_name);
        }
        Ok(restored)
    }
}

fn builtin_alias(name: &str) -> Option<&'static str> {
    Some(match name {
        "Read" => "read_file",
        "Glob" => "find_files",
        "Grep" | "Search" => "search_text",
        "Edit" => "edit_file",
        "Write" => "write_file",
        "Bash" | "Shell" => "shell_command",
        "WebFetch" => "fetch_url",
        "WebSearch" => "search_web",
        "NotebookEdit" | "Notebook" => "edit_notebook",
        "Task" | "Agent" => "spawn_agent",
        "ToolSearch" | "tool_search" => "search_tools",
        _ => return None,
    })
}

fn builtin_semantics(name: &str) -> ToolSemantics {
    if matches!(name, "ToolSearch" | "tool_search") {
        ToolSemantics::ClaudeToolSearch
    } else {
        ToolSemantics::Builtin
    }
}

fn sanitize_dynamic_name(name: &str) -> String {
    if name
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || matches!(character, '_' | '-'))
    {
        return name.to_string();
    }
    let mut sanitized = String::with_capacity(name.len());
    for character in name.chars() {
        if character.is_ascii_alphanumeric() || matches!(character, '_' | '-') {
            sanitized.push(character);
        } else {
            sanitized.push('_');
        }
    }
    let sanitized = sanitized.trim_matches('_');
    let mut result = if sanitized.is_empty() {
        "tool".to_string()
    } else {
        sanitized.to_string()
    };
    if result.chars().next().is_some_and(|ch| ch.is_ascii_digit()) {
        result.insert_str(0, "tool_");
    }
    result
}

fn plan_dynamic_names(tools: &[Value]) -> Result<BTreeMap<String, String>, BridgeError> {
    let reserved: BTreeSet<&'static str> = [
        "read_file",
        "find_files",
        "search_text",
        "edit_file",
        "write_file",
        "shell_command",
        "fetch_url",
        "search_web",
        "edit_notebook",
        "spawn_agent",
        "search_tools",
    ]
    .into_iter()
    .collect();
    let mut seen = BTreeSet::new();
    let mut candidates = BTreeMap::new();
    let mut counts = BTreeMap::<String, usize>::new();
    for tool in tools {
        let object = tool
            .as_object()
            .ok_or_else(|| BridgeError::ToolRegistryViolation {
                summary: "tool definition must be an object".to_string(),
            })?;
        if object.get("type").and_then(Value::as_str) == Some("BatchTool") {
            return registry_error("BatchTool is unsupported by the Stage 2 bridge");
        }
        let name = object
            .get("name")
            .and_then(Value::as_str)
            .filter(|name| !name.trim().is_empty())
            .ok_or_else(|| BridgeError::ToolRegistryViolation {
                summary: "tool definition requires a non-empty name".to_string(),
            })?;
        if !seen.insert(name.to_string()) {
            return registry_error(&format!("duplicate Claude tool name: {name}"));
        }
        if builtin_alias(name).is_some() {
            continue;
        }
        let candidate = sanitize_dynamic_name(name);
        if reserved.contains(candidate.as_str()) {
            return registry_error(&format!(
                "dynamic tool conflicts with built-in alias namespace: {candidate}"
            ));
        }
        *counts.entry(candidate.clone()).or_default() += 1;
        candidates.insert(name.to_string(), candidate);
    }
    Ok(candidates
        .into_iter()
        .map(|(name, candidate)| {
            let planned = if counts.get(&candidate).copied().unwrap_or_default() > 1 {
                let hash = format!("{:x}", Sha256::digest(name.as_bytes()));
                format!("{candidate}__{}", &hash[..8])
            } else {
                candidate
            };
            (name, planned)
        })
        .collect())
}

fn model_description(name: &str, semantics: ToolSemantics, supplied: Option<&str>) -> String {
    if semantics == ToolSemantics::Dynamic {
        return supplied.unwrap_or("").trim().to_string();
    }
    let operation = supplied.unwrap_or(name).trim();
    format!(
        "{operation} This function is executed by Claude Code in the local Claude Code workspace. Exact local paths are not vector-store identifiers. Wait for the later tool result before claiming success."
    )
}

fn hash_value(value: &Value) -> String {
    format!("{:x}", Sha256::digest(canonical_json_string(value)))
}

fn registry_error<T>(summary: &str) -> Result<T, BridgeError> {
    Err(BridgeError::ToolRegistryViolation {
        summary: summary.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proxy::claude_codex_bridge::{
        BridgeError, CodexOAuthCapabilities, ExecutionOwner, ToolSemantics,
    };
    use serde_json::{json, Value};

    fn definition(name: &str) -> Value {
        json!({
            "name": name,
            "description": format!("Claude description for {name}"),
            "input_schema": {
                "type": "object",
                "properties": {"value": {"type": "string"}},
                "required": ["value"],
                "additionalProperties": false
            }
        })
    }

    #[test]
    fn compiles_complete_builtin_alias_matrix_as_claude_owned_functions() {
        let cases = [
            ("Read", "read_file"),
            ("Glob", "find_files"),
            ("Grep", "search_text"),
            ("Search", "search_text"),
            ("Edit", "edit_file"),
            ("Write", "write_file"),
            ("Bash", "shell_command"),
            ("Shell", "shell_command"),
            ("WebFetch", "fetch_url"),
            ("WebSearch", "search_web"),
            ("NotebookEdit", "edit_notebook"),
            ("Notebook", "edit_notebook"),
            ("Task", "spawn_agent"),
            ("Agent", "spawn_agent"),
        ];

        for (claude_name, codex_name) in cases {
            let (registry, losses) = ToolRegistry::compile(
                &[definition(claude_name)],
                CodexOAuthCapabilities::builtin().as_ref(),
            )
            .unwrap();
            let binding = &registry.bindings()[0];
            let tool = &registry.codex_tools()[0];

            assert_eq!(binding.claude_name, claude_name);
            assert_eq!(binding.codex_name, codex_name);
            assert_eq!(binding.execution_owner, ExecutionOwner::ClaudeCode);
            assert_eq!(binding.semantics, ToolSemantics::Builtin);
            assert_eq!(tool["type"], "function");
            assert_eq!(tool["name"], codex_name);
            let description = tool["description"].as_str().unwrap();
            assert!(description.contains("local Claude Code workspace"));
            assert!(description.contains("later tool result"));
            assert!(!binding.schema_hash.is_empty());
            assert!(!losses.is_empty());
        }
    }

    #[test]
    fn claude_tool_search_remains_a_claude_executed_function() {
        for name in ["ToolSearch", "tool_search"] {
            let (registry, _) = ToolRegistry::compile(
                &[definition(name)],
                CodexOAuthCapabilities::builtin().as_ref(),
            )
            .unwrap();

            assert_eq!(
                registry.bindings()[0].semantics,
                ToolSemantics::ClaudeToolSearch
            );
            assert_eq!(
                registry.bindings()[0].execution_owner,
                ExecutionOwner::ClaudeCode
            );
            assert_eq!(registry.codex_tools()[0]["type"], "function");
            assert_ne!(registry.codex_tools()[0]["type"], "tool_search");
        }
    }

    #[test]
    fn batch_tool_is_explicitly_unsupported() {
        let batch = json!({
            "type": "BatchTool",
            "name": "BatchTool",
            "input_schema": {"type": "object"}
        });

        assert!(matches!(
            ToolRegistry::compile(&[batch], CodexOAuthCapabilities::builtin().as_ref()),
            Err(BridgeError::ToolRegistryViolation { .. })
        ));
    }

    #[test]
    fn compilation_rejects_duplicate_names_and_builtin_alias_conflicts() {
        for tools in [
            vec![definition("Read"), definition("Read")],
            vec![definition("Grep"), definition("Search")],
            vec![definition("Bash"), definition("Shell")],
            vec![definition("Task"), definition("Agent")],
            vec![definition("Read"), definition("read_file")],
        ] {
            assert!(matches!(
                ToolRegistry::compile(&tools, CodexOAuthCapabilities::builtin().as_ref()),
                Err(BridgeError::ToolRegistryViolation { .. })
            ));
        }
    }

    #[test]
    fn compilation_rejects_invalid_definitions() {
        for tool in [
            json!({"name": "", "input_schema": {"type": "object"}}),
            json!({"input_schema": {"type": "object"}}),
            json!({"name": "Read", "input_schema": []}),
        ] {
            assert!(
                ToolRegistry::compile(&[tool], CodexOAuthCapabilities::builtin().as_ref()).is_err()
            );
        }
    }

    #[test]
    fn registry_identity_and_schema_fingerprints_are_stable() {
        let mut reordered = definition("Read");
        reordered["input_schema"] = json!({
            "additionalProperties": false,
            "required": ["value"],
            "properties": {"value": {"type": "string"}},
            "type": "object"
        });
        let (left, _) = ToolRegistry::compile(
            &[definition("Read")],
            CodexOAuthCapabilities::builtin().as_ref(),
        )
        .unwrap();
        let (right, _) =
            ToolRegistry::compile(&[reordered], CodexOAuthCapabilities::builtin().as_ref())
                .unwrap();

        assert_eq!(left.identity_fingerprint(), right.identity_fingerprint());
        assert_eq!(left.schema_fingerprint(), right.schema_fingerprint());
    }

    #[test]
    fn restores_every_builtin_alias_to_exact_claude_identity_and_contract() {
        let cases = [
            ("Read", "read_file"),
            ("Glob", "find_files"),
            ("Grep", "search_text"),
            ("Search", "search_text"),
            ("Edit", "edit_file"),
            ("Write", "write_file"),
            ("Bash", "shell_command"),
            ("Shell", "shell_command"),
            ("WebFetch", "fetch_url"),
            ("WebSearch", "search_web"),
            ("NotebookEdit", "edit_notebook"),
            ("Notebook", "edit_notebook"),
            ("Task", "spawn_agent"),
            ("Agent", "spawn_agent"),
        ];

        for (claude_name, codex_name) in cases {
            let (registry, _) = ToolRegistry::compile(
                &[definition(claude_name)],
                CodexOAuthCapabilities::builtin().as_ref(),
            )
            .unwrap();
            let restored = registry
                .restore_call(codex_name, "call_exact", r#"{"value":"literal"}"#)
                .unwrap();

            assert_eq!(restored.claude_name, claude_name);
            assert_eq!(restored.tool_use_id, "call_exact");
            assert_eq!(restored.input, json!({"value": "literal"}));
        }
    }

    #[test]
    fn restoration_rejects_unknown_identity_id_and_illegal_arguments() {
        let (registry, _) = ToolRegistry::compile(
            &[definition("Read")],
            CodexOAuthCapabilities::builtin().as_ref(),
        )
        .unwrap();

        for result in [
            registry.restore_call("unknown", "call_1", r#"{"value":"x"}"#),
            registry.restore_call("read_file", "", r#"{"value":"x"}"#),
            registry.restore_call("read_file", "call_1", "{"),
            registry.restore_call("read_file", "call_1", "[]"),
            registry.restore_call("read_file", "call_1", "{}"),
            registry.restore_call("read_file", "call_1", r#"{"value":3}"#),
            registry.restore_call("read_file", "call_1", r#"{"value":"x","extra":true}"#),
        ] {
            assert!(matches!(
                result,
                Err(BridgeError::ToolRegistryViolation { .. })
            ));
        }
    }

    #[test]
    fn response_restoration_changes_only_registered_function_call_identity() {
        let (registry, _) = ToolRegistry::compile(
            &[definition("Read")],
            CodexOAuthCapabilities::builtin().as_ref(),
        )
        .unwrap();
        let response = json!({
            "id": "resp_1",
            "output": [
                {"type": "message", "content": [{"type": "output_text", "text": "before"}]},
                {
                    "type": "function_call",
                    "call_id": "call_1",
                    "name": "read_file",
                    "arguments": "{\"value\":\"src/main.rs\"}"
                }
            ]
        });

        let restored = registry.restore_response(&response).unwrap();

        assert_eq!(restored["output"][0], response["output"][0]);
        assert_eq!(restored["output"][1]["name"], "Read");
        assert_eq!(restored["output"][1]["call_id"], "call_1");
        assert_eq!(
            restored["output"][1]["arguments"],
            response["output"][1]["arguments"]
        );
        assert_eq!(response["output"][1]["name"], "read_file");
    }

    #[test]
    fn dynamic_mcp_names_are_stable_and_restore_exactly() {
        let tools = [
            definition("mcp__filesystem__stat"),
            definition("插件 search/文件"),
        ];
        let (left, _) =
            ToolRegistry::compile(&tools, CodexOAuthCapabilities::builtin().as_ref()).unwrap();
        let (right, _) =
            ToolRegistry::compile(&tools, CodexOAuthCapabilities::builtin().as_ref()).unwrap();

        assert_eq!(left.bindings()[0].codex_name, "mcp__filesystem__stat");
        assert_eq!(left.bindings()[0].semantics, ToolSemantics::Dynamic);
        assert_eq!(
            left.bindings()[1].codex_name,
            right.bindings()[1].codex_name
        );
        assert!(left.bindings()[1]
            .codex_name
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-')));
        let restored = left
            .restore_call(
                &left.bindings()[1].codex_name,
                "call_plugin",
                r#"{"value":"x"}"#,
            )
            .unwrap();
        assert_eq!(restored.claude_name, "插件 search/文件");
    }

    #[test]
    fn sanitized_dynamic_collisions_receive_distinct_stable_hash_suffixes() {
        let tools = [definition("plugin/a"), definition("plugin a")];
        let (registry, _) =
            ToolRegistry::compile(&tools, CodexOAuthCapabilities::builtin().as_ref()).unwrap();

        let names: Vec<&str> = registry
            .bindings()
            .iter()
            .map(|binding| binding.codex_name.as_str())
            .collect();
        assert_ne!(names[0], names[1]);
        assert!(names.iter().all(|name| name.starts_with("plugin_a__")));
        assert!(names
            .iter()
            .all(|name| name.len() == "plugin_a__".len() + 8));
    }

    #[test]
    fn dynamic_tools_cannot_claim_exact_builtin_alias_namespace() {
        assert!(matches!(
            ToolRegistry::compile(
                &[definition("read_file")],
                CodexOAuthCapabilities::builtin().as_ref()
            ),
            Err(BridgeError::ToolRegistryViolation { .. })
        ));
    }
}
