use serde_json::Value;

const JSON_SAFE_INTEGER_MAX: u64 = 9_007_199_254_740_991;
const READ_OFFSET_MAX: u64 = 1_000_000_000;
const READ_OFFSET_GUIDANCE: &str = "Use 0 or omit this field for the first read. Only advance it using the line range returned by a previous Read call; do not guess it from file size.";

pub(crate) fn clean_anthropic_tool_schema(name: &str, mut schema: Value) -> Value {
    if name != "Read" {
        return schema;
    }

    let required_file_path = schema
        .get("required")
        .and_then(Value::as_array)
        .is_some_and(|required| {
            required
                .iter()
                .any(|value| value.as_str() == Some("file_path"))
        });
    let Some(properties) = schema.get_mut("properties").and_then(Value::as_object_mut) else {
        return schema;
    };

    let has_property_type = |key: &str, expected: &str| {
        properties
            .get(key)
            .and_then(|property| property.get("type"))
            .and_then(Value::as_str)
            == Some(expected)
    };
    let looks_like_claude_read = required_file_path
        && has_property_type("file_path", "string")
        && has_property_type("offset", "integer")
        && has_property_type("limit", "integer")
        && has_property_type("pages", "string");
    if !looks_like_claude_read {
        return schema;
    }

    for key in ["offset", "limit"] {
        let Some(property) = properties.get_mut(key).and_then(Value::as_object_mut) else {
            continue;
        };
        if property.get("maximum").and_then(Value::as_u64) == Some(JSON_SAFE_INTEGER_MAX) {
            property.remove("maximum");
        }
    }

    if let Some(offset) = properties.get_mut("offset").and_then(Value::as_object_mut) {
        match offset.get_mut("description") {
            Some(Value::String(description)) if !description.contains(READ_OFFSET_GUIDANCE) => {
                if !description.is_empty() && !description.ends_with(char::is_whitespace) {
                    description.push(' ');
                }
                description.push_str(READ_OFFSET_GUIDANCE);
            }
            None => {
                offset.insert(
                    "description".to_string(),
                    Value::String(READ_OFFSET_GUIDANCE.to_string()),
                );
            }
            _ => {}
        }
    }

    schema
}

pub(crate) fn sanitize_anthropic_tool_use_input(name: &str, input: Value) -> Value {
    if name != "Read" {
        return input;
    }

    let Value::Object(mut object) = input else {
        return input;
    };

    if matches!(object.get("pages"), Some(Value::String(value)) if value.is_empty()) {
        object.remove("pages");
    }

    if let Some(offset) = object.get("offset") {
        let offset_is_valid = offset
            .as_u64()
            .is_some_and(|offset| offset <= READ_OFFSET_MAX);
        if !offset_is_valid {
            let reason = match offset {
                Value::Number(number) if number.is_f64() => "non_integer_number",
                Value::Number(_) => "out_of_range_integer",
                Value::String(_) => "string",
                Value::Null => "null",
                Value::Bool(_) => "boolean",
                Value::Array(_) => "array",
                Value::Object(_) => "object",
            };
            log::warn!("[Tool compatibility] Removed invalid Read offset ({reason})");
            object.remove("offset");
        }
    }

    Value::Object(object)
}

pub(crate) fn sanitize_anthropic_tool_use_input_json(name: &str, raw: &str) -> String {
    if name != "Read" || raw.is_empty() {
        return raw.to_string();
    }

    let Ok(input) = serde_json::from_str::<Value>(raw) else {
        return raw.to_string();
    };

    serde_json::to_string(&sanitize_anthropic_tool_use_input(name, input))
        .unwrap_or_else(|_| raw.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn read_schema_drops_only_generic_safe_integer_maximum() {
        let schema = json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["file_path"],
            "properties": {
                "file_path": {"type": "string"},
                "offset": {
                    "type": "integer",
                    "minimum": 0,
                    "maximum": JSON_SAFE_INTEGER_MAX,
                    "description": "The line number to start reading from. Only provide if the file is too large to read at once"
                },
                "limit": {
                    "type": "integer",
                    "exclusiveMinimum": 0,
                    "maximum": JSON_SAFE_INTEGER_MAX,
                    "description": "The number of lines to read. Only provide if the file is too large to read at once."
                },
                "pages": {"type": "string"}
            }
        });

        let cleaned = clean_anthropic_tool_schema("Read", schema);

        assert_eq!(cleaned["properties"]["offset"]["type"], "integer");
        assert_eq!(cleaned["properties"]["offset"]["minimum"], 0);
        assert!(cleaned["properties"]["offset"].get("maximum").is_none());
        assert_eq!(cleaned["properties"]["limit"]["exclusiveMinimum"], 0);
        assert!(cleaned["properties"]["limit"].get("maximum").is_none());
        assert_eq!(cleaned["required"], json!(["file_path"]));
        assert_eq!(cleaned["additionalProperties"], false);
        assert_eq!(cleaned["properties"]["pages"]["type"], "string");
        assert!(cleaned["properties"]["offset"]["description"]
            .as_str()
            .unwrap()
            .contains(READ_OFFSET_GUIDANCE));
        assert_eq!(
            cleaned["properties"]["limit"]["description"],
            "The number of lines to read. Only provide if the file is too large to read at once."
        );
    }

    #[test]
    fn schema_cleanup_is_idempotent_and_preserves_business_maximums() {
        let schema = json!({
            "type": "object",
            "required": ["file_path"],
            "properties": {
                "file_path": {"type": "string"},
                "offset": {"type": "integer", "maximum": 10_000},
                "limit": {"type": "integer"},
                "pages": {"type": "string"}
            }
        });

        let cleaned = clean_anthropic_tool_schema("Read", schema.clone());
        let cleaned_twice = clean_anthropic_tool_schema("Read", cleaned.clone());

        assert_eq!(cleaned["properties"]["offset"]["maximum"], 10_000);
        assert_eq!(cleaned_twice, cleaned);
        assert_eq!(
            clean_anthropic_tool_schema("Search", schema.clone()),
            schema
        );
    }

    #[test]
    fn same_named_custom_read_schema_is_unchanged() {
        let schema = json!({
            "type": "object",
            "required": ["file_path"],
            "properties": {
                "file_path": {"type": "string"},
                "offset": {
                    "type": "integer",
                    "maximum": JSON_SAFE_INTEGER_MAX
                }
            }
        });

        assert_eq!(clean_anthropic_tool_schema("Read", schema.clone()), schema);
    }

    #[test]
    fn read_input_drops_invalid_offset_and_empty_pages() {
        let input = json!({
            "file_path": "C:\\main\\code\\anyfast-partner-hub\\model\\permission_feature_query.go",
            "offset": 2.300310976710655e+22,
            "limit": 2000,
            "pages": "",
            "unknown": true
        });

        let cleaned = sanitize_anthropic_tool_use_input("Read", input);

        assert!(cleaned.get("offset").is_none());
        assert!(cleaned.get("pages").is_none());
        assert_eq!(
            cleaned["file_path"],
            "C:\\main\\code\\anyfast-partner-hub\\model\\permission_feature_query.go"
        );
        assert_eq!(cleaned["limit"], 2000);
        assert_eq!(cleaned["unknown"], true);
    }

    #[test]
    fn read_input_preserves_valid_offsets() {
        for offset in [0, 42, READ_OFFSET_MAX] {
            let cleaned = sanitize_anthropic_tool_use_input(
                "Read",
                json!({"file_path": "file", "offset": offset, "pages": "1"}),
            );

            assert_eq!(cleaned["offset"], offset);
            assert_eq!(cleaned["pages"], "1");
        }
    }

    #[test]
    fn read_input_drops_all_other_invalid_offset_shapes() {
        for offset in [
            json!(-1),
            json!(1.5),
            json!("12"),
            Value::Null,
            json!(true),
            json!([]),
            json!({}),
            json!(READ_OFFSET_MAX + 1),
        ] {
            let cleaned = sanitize_anthropic_tool_use_input(
                "Read",
                json!({"file_path": "file", "offset": offset}),
            );

            assert!(cleaned.get("offset").is_none());
        }
    }

    #[test]
    fn raw_read_input_sanitization_is_fail_open_and_non_read_is_unchanged() {
        let raw = r#"{"file_path":"file","offset":2.300310976710655e+22,"pages":""}"#;
        let cleaned = sanitize_anthropic_tool_use_input_json("Read", raw);
        let cleaned_value: Value = serde_json::from_str(&cleaned).unwrap();

        assert!(cleaned_value.get("offset").is_none());
        assert!(cleaned_value.get("pages").is_none());
        assert_eq!(
            sanitize_anthropic_tool_use_input_json("Read", "{\"offset\":"),
            "{\"offset\":"
        );
        assert_eq!(
            sanitize_anthropic_tool_use_input("Search", json!({"offset": "not touched"})),
            json!({"offset": "not touched"})
        );
    }
}
