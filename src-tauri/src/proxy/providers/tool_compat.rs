use serde_json::Value;

const JSON_SAFE_INTEGER_MAX: u64 = 9_007_199_254_740_991;
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
}
