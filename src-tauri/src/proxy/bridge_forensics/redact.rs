use serde_json::Value;

const REDACTED: &str = "[REDACTED]";

const EXPLICIT_CREDENTIAL_KEYS: &[&str] = &[
    "authorization",
    "proxy_authorization",
    "x_api_key",
    "api_key",
    "access_token",
    "refresh_token",
    "id_token",
    "cookie",
    "set_cookie",
    "device_code",
    "client_secret",
    "password",
    "chatgpt_account_id",
    "account_id",
    "organization_id",
];

#[derive(Clone, Debug, PartialEq)]
pub struct RedactionOutcome {
    pub value: Value,
    pub safe_for_full_capture: bool,
    pub redacted_paths: Vec<String>,
    pub uncertain_paths: Vec<String>,
}

pub fn redact_protocol_value(value: &Value) -> RedactionOutcome {
    let mut value = value.clone();
    let mut redacted_paths = Vec::new();
    let mut uncertain_paths = Vec::new();

    redact_value(&mut value, "$", &mut redacted_paths, &mut uncertain_paths);

    RedactionOutcome {
        value,
        safe_for_full_capture: uncertain_paths.is_empty(),
        redacted_paths,
        uncertain_paths,
    }
}

fn redact_value(
    value: &mut Value,
    path: &str,
    redacted_paths: &mut Vec<String>,
    uncertain_paths: &mut Vec<String>,
) {
    match value {
        Value::Object(object) => {
            for (key, child) in object {
                let child_path = format!("{path}.{key}");
                let normalized_key = normalize_key(key);

                if EXPLICIT_CREDENTIAL_KEYS.contains(&normalized_key.as_str()) {
                    *child = Value::String(REDACTED.to_string());
                    redacted_paths.push(child_path);
                } else if looks_like_unknown_credential(&normalized_key) {
                    *child = Value::String(REDACTED.to_string());
                    redacted_paths.push(child_path.clone());
                    uncertain_paths.push(child_path);
                } else {
                    redact_value(child, &child_path, redacted_paths, uncertain_paths);
                }
            }
        }
        Value::Array(items) => {
            for (index, child) in items.iter_mut().enumerate() {
                redact_value(
                    child,
                    &format!("{path}[{index}]"),
                    redacted_paths,
                    uncertain_paths,
                );
            }
        }
        Value::String(text) if looks_like_embedded_credential(text) => {
            *value = Value::String(REDACTED.to_string());
            redacted_paths.push(path.to_string());
            uncertain_paths.push(path.to_string());
        }
        _ => {}
    }
}

fn normalize_key(key: &str) -> String {
    key.to_ascii_lowercase().replace('-', "_")
}

fn looks_like_unknown_credential(key: &str) -> bool {
    key.contains("secret") || key.contains("credential") || key.ends_with("_token")
}

fn looks_like_embedded_credential(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    if lower.contains("bearer ")
        || lower.contains("sk-")
        || lower.contains("ghp_")
        || lower.contains("github_pat_")
        || lower.contains("xoxb-")
        || lower.contains("xoxp-")
    {
        return true;
    }

    value.split_ascii_whitespace().any(|word| {
        let candidate = word.trim_matches(|character: char| {
            matches!(character, '"' | '\'' | ',' | ';' | '(' | ')' | '[' | ']')
        });
        candidate.starts_with("eyJ") && candidate.split('.').count() == 3 && candidate.len() >= 32
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_credentials_recursively_without_removing_prompt_content() {
        let input = serde_json::json!({
            "headers": {"Authorization": "Bearer secret", "x-api-key": "sk-test"},
            "auth": {"access_token": "a", "refresh_token": "r"},
            "input": [{"role": "user", "content": "inspect src/main.rs"}]
        });

        let outcome = redact_protocol_value(&input);

        assert!(outcome.safe_for_full_capture);
        assert_eq!(outcome.value["headers"]["Authorization"], "[REDACTED]");
        assert_eq!(outcome.value["headers"]["x-api-key"], "[REDACTED]");
        assert_eq!(outcome.value["auth"]["refresh_token"], "[REDACTED]");
        assert_eq!(outcome.value["input"][0]["content"], "inspect src/main.rs");
    }

    #[test]
    fn unknown_credential_shaped_key_suppresses_full_capture() {
        let input = serde_json::json!({"custom_super_secret_credential": "value"});

        let outcome = redact_protocol_value(&input);

        assert!(!outcome.safe_for_full_capture);
        assert!(outcome
            .uncertain_paths
            .contains(&"$.custom_super_secret_credential".to_string()));
    }

    #[test]
    fn embedded_credential_value_suppresses_full_capture() {
        let input = serde_json::json!({
            "input": [{"role": "user", "content": "debug Bearer oauth-secret"}]
        });

        let outcome = redact_protocol_value(&input);

        assert!(!outcome.safe_for_full_capture);
        assert_eq!(outcome.value["input"][0]["content"], "[REDACTED]");
        assert!(outcome
            .uncertain_paths
            .contains(&"$.input[0].content".to_string()));
    }
}
