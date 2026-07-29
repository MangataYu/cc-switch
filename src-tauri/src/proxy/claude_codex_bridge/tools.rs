use super::{adapt_schema, BridgeError, CodexOAuthCapabilities, SchemaLoss, SupportLevel};
use crate::proxy::json_canonical::canonical_json_string;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

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
                .unwrap_or_else(|| (sanitize_dynamic_name(&claude_name), ToolSemantics::Dynamic));
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
    let mut sanitized = String::with_capacity(name.len());
    for character in name.chars() {
        if character.is_ascii_alphanumeric() || matches!(character, '_' | '-') {
            sanitized.push(character);
        } else {
            sanitized.push('_');
        }
    }
    while sanitized.contains("__") {
        sanitized = sanitized.replace("__", "_");
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
}
