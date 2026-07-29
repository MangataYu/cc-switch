use super::BridgeError;
use crate::proxy::json_canonical::canonical_json_string;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SchemaAction {
    Preserve,
    Normalize,
    Drop,
    Reject,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SchemaLossReason {
    SupportedContract,
    MissingRootObjectType,
    AnnotationNotForwarded,
    InvalidSchema,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaLoss {
    pub source_path: String,
    pub action: SchemaAction,
    pub reason: SchemaLossReason,
    pub affects_correctness: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SchemaAdaptation {
    pub schema: Value,
    pub schema_hash: String,
    pub decisions: Vec<SchemaLoss>,
}

pub fn adapt_schema(source: &Value) -> Result<SchemaAdaptation, BridgeError> {
    let Value::Object(source_object) = source else {
        return schema_error("root schema must be an object");
    };
    let mut schema = Value::Object(source_object.clone());
    validate_schema_shape(&schema, "$")?;

    let object = schema.as_object_mut().expect("checked object");
    let mut decisions = vec![SchemaLoss {
        source_path: "$".to_string(),
        action: SchemaAction::Preserve,
        reason: SchemaLossReason::SupportedContract,
        affects_correctness: false,
    }];
    match object.get("type") {
        None => {
            object.insert("type".to_string(), Value::String("object".to_string()));
            decisions.push(SchemaLoss {
                source_path: "$".to_string(),
                action: SchemaAction::Normalize,
                reason: SchemaLossReason::MissingRootObjectType,
                affects_correctness: false,
            });
        }
        Some(Value::String(kind)) if kind == "object" => {}
        _ => return schema_error("root schema type must be object"),
    }
    if object.remove("$schema").is_some() {
        decisions.push(SchemaLoss {
            source_path: "$/\u{24}schema".to_string(),
            action: SchemaAction::Drop,
            reason: SchemaLossReason::AnnotationNotForwarded,
            affects_correctness: false,
        });
    }
    let schema_hash = format!("{:x}", Sha256::digest(canonical_json_string(&schema)));
    Ok(SchemaAdaptation {
        schema,
        schema_hash,
        decisions,
    })
}

fn validate_schema_shape(schema: &Value, path: &str) -> Result<(), BridgeError> {
    let Value::Object(object) = schema else {
        return schema_error(&format!("{path} must be a schema object"));
    };
    if let Some(required) = object.get("required") {
        let Some(required) = required.as_array() else {
            return schema_error(&format!("{path}/required must be an array"));
        };
        if required.iter().any(|name| !name.is_string()) {
            return schema_error(&format!("{path}/required must contain only strings"));
        }
    }
    if let Some(properties) = object.get("properties") {
        let Some(properties) = properties.as_object() else {
            return schema_error(&format!("{path}/properties must be an object"));
        };
        for (name, property) in properties {
            validate_schema_shape(property, &format!("{path}/properties/{name}"))?;
        }
    }
    if let Some(items) = object.get("items") {
        validate_schema_shape(items, &format!("{path}/items"))?;
    }
    for keyword in ["oneOf", "anyOf", "allOf"] {
        if let Some(branches) = object.get(keyword) {
            let Some(branches) = branches.as_array() else {
                return schema_error(&format!("{path}/{keyword} must be an array"));
            };
            if branches.is_empty() {
                return schema_error(&format!("{path}/{keyword} must not be empty"));
            }
            for (index, branch) in branches.iter().enumerate() {
                validate_schema_shape(branch, &format!("{path}/{keyword}/{index}"))?;
            }
        }
    }
    Ok(())
}

fn schema_error<T>(summary: &str) -> Result<T, BridgeError> {
    Err(BridgeError::SchemaAdaptationLoss {
        summary: summary.to_string(),
    })
}

pub fn validate_arguments(schema: &Value, arguments: &Value) -> Result<(), BridgeError> {
    validate_value(schema, arguments, "$")
        .map_err(|summary| BridgeError::ToolRegistryViolation { summary })
}

fn validate_value(schema: &Value, value: &Value, path: &str) -> Result<(), String> {
    let object = schema
        .as_object()
        .ok_or_else(|| format!("schema at {path} is not an object"))?;

    if let Some(expected) = object.get("const") {
        if value != expected {
            return Err(format!("argument at {path} does not match const"));
        }
    }
    if let Some(allowed) = object.get("enum").and_then(Value::as_array) {
        if !allowed.contains(value) {
            return Err(format!("argument at {path} is not in enum"));
        }
    }
    if let Some(branches) = object.get("oneOf").and_then(Value::as_array) {
        let matches = branches
            .iter()
            .filter(|branch| validate_value(branch, value, path).is_ok())
            .count();
        if matches != 1 {
            return Err(format!(
                "argument at {path} must match exactly one oneOf branch"
            ));
        }
    }
    if let Some(branches) = object.get("anyOf").and_then(Value::as_array) {
        if !branches
            .iter()
            .any(|branch| validate_value(branch, value, path).is_ok())
        {
            return Err(format!("argument at {path} must match an anyOf branch"));
        }
    }
    if let Some(branches) = object.get("allOf").and_then(Value::as_array) {
        for branch in branches {
            validate_value(branch, value, path)?;
        }
    }

    if let Some(kind) = object.get("type").and_then(Value::as_str) {
        let type_matches = match kind {
            "object" => value.is_object(),
            "array" => value.is_array(),
            "string" => value.is_string(),
            "integer" => value.as_i64().is_some() || value.as_u64().is_some(),
            "number" => value.is_number(),
            "boolean" => value.is_boolean(),
            "null" => value.is_null(),
            _ => return Err(format!("unsupported schema type {kind} at {path}")),
        };
        if !type_matches {
            return Err(format!("argument at {path} must be {kind}"));
        }
    }

    if let Some(value) = value.as_object() {
        validate_object(object, value, path)?;
    }
    if let Some(values) = value.as_array() {
        if let Some(items) = object.get("items") {
            for (index, value) in values.iter().enumerate() {
                validate_value(items, value, &format!("{path}/{index}"))?;
            }
        }
    }
    if let Some(number) = value.as_f64() {
        if object
            .get("minimum")
            .and_then(Value::as_f64)
            .is_some_and(|minimum| number < minimum)
        {
            return Err(format!("argument at {path} is below minimum"));
        }
        if object
            .get("maximum")
            .and_then(Value::as_f64)
            .is_some_and(|maximum| number > maximum)
        {
            return Err(format!("argument at {path} is above maximum"));
        }
    }
    Ok(())
}

fn validate_object(
    schema: &Map<String, Value>,
    value: &Map<String, Value>,
    path: &str,
) -> Result<(), String> {
    if let Some(required) = schema.get("required").and_then(Value::as_array) {
        for name in required.iter().filter_map(Value::as_str) {
            if !value.contains_key(name) {
                return Err(format!("missing required argument {path}/{name}"));
            }
        }
    }
    let properties = schema.get("properties").and_then(Value::as_object);
    for (name, child) in value {
        if let Some(property_schema) = properties.and_then(|properties| properties.get(name)) {
            validate_value(property_schema, child, &format!("{path}/{name}"))?;
        } else if schema.get("additionalProperties") == Some(&Value::Bool(false)) {
            return Err(format!("unexpected argument {path}/{name}"));
        } else if let Some(additional_schema) = schema
            .get("additionalProperties")
            .filter(|value| value.is_object())
        {
            validate_value(additional_schema, child, &format!("{path}/{name}"))?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn adaptation_preserves_constraints_and_hash_ignores_property_order() {
        let left = json!({
            "type": "object",
            "properties": {
                "path": {"type": "string"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 10}
            },
            "required": ["path"],
            "additionalProperties": false
        });
        let right = json!({
            "additionalProperties": false,
            "required": ["path"],
            "properties": {
                "limit": {"maximum": 10, "minimum": 1, "type": "integer"},
                "path": {"type": "string"}
            },
            "type": "object"
        });

        let adapted_left = adapt_schema(&left).unwrap();
        let adapted_right = adapt_schema(&right).unwrap();

        assert_eq!(adapted_left.schema, left);
        assert_eq!(adapted_left.schema_hash, adapted_right.schema_hash);
        assert!(adapted_left.decisions.iter().any(|decision| {
            decision.action == SchemaAction::Preserve && !decision.affects_correctness
        }));
    }

    #[test]
    fn adaptation_records_safe_normalize_and_drop_decisions() {
        let adapted = adapt_schema(&json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "properties": {"path": {"type": "string"}}
        }))
        .unwrap();

        assert_eq!(adapted.schema["type"], "object");
        assert!(adapted.schema.get("$schema").is_none());
        assert!(adapted.decisions.iter().any(|decision| {
            decision.source_path == "$"
                && decision.action == SchemaAction::Normalize
                && !decision.affects_correctness
        }));
        assert!(adapted.decisions.iter().any(|decision| {
            decision.source_path == "$/\u{24}schema"
                && decision.action == SchemaAction::Drop
                && !decision.affects_correctness
        }));
    }

    #[test]
    fn adaptation_rejects_malformed_or_non_object_contracts() {
        for schema in [
            json!({"type": "string"}),
            json!({"type": "object", "required": "path"}),
            json!({"type": "object", "properties": []}),
            json!({"type": "object", "required": [1]}),
        ] {
            assert!(matches!(
                adapt_schema(&schema),
                Err(BridgeError::SchemaAdaptationLoss { .. })
            ));
        }
    }

    #[test]
    fn argument_validation_enforces_original_contract_without_mutation() {
        let schema = json!({
            "type": "object",
            "properties": {
                "path": {"type": "string"},
                "mode": {"enum": ["read", "write"]},
                "limit": {"type": "integer", "minimum": 1, "maximum": 10}
            },
            "required": ["path"],
            "additionalProperties": false
        });
        let valid = json!({"path": "src/main.rs", "mode": "read", "limit": 2});

        validate_arguments(&schema, &valid).unwrap();
        assert_eq!(
            valid,
            json!({"path": "src/main.rs", "mode": "read", "limit": 2})
        );

        for invalid in [
            json!([]),
            json!({}),
            json!({"path": 3}),
            json!({"path": "x", "mode": "execute"}),
            json!({"path": "x", "limit": 0}),
            json!({"path": "x", "extra": true}),
        ] {
            assert!(matches!(
                validate_arguments(&schema, &invalid),
                Err(BridgeError::ToolRegistryViolation { .. })
            ));
        }
    }

    #[test]
    fn argument_validation_supports_unions_arrays_and_discriminators() {
        let schema = json!({
            "type": "object",
            "properties": {
                "target": {
                    "oneOf": [
                        {"type": "string"},
                        {"type": "array", "items": {"type": "integer"}}
                    ]
                },
                "kind": {"const": "local"}
            },
            "required": ["target", "kind"]
        });

        validate_arguments(&schema, &json!({"target": [1, 2], "kind": "local"})).unwrap();
        assert!(
            validate_arguments(&schema, &json!({"target": [1, "two"], "kind": "local"})).is_err()
        );
        assert!(validate_arguments(&schema, &json!({"target": "x", "kind": "remote"})).is_err());
    }
}
