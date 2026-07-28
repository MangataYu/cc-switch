use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use serde_json::Value;

use crate::proxy::json_canonical::short_sha256_hex;

/// Request-local, explicitly enabled diagnostics for OpenAI Read calls.
///
/// This deliberately records only a redacted projection of complete JSON. Fragments
/// that cannot be parsed as a complete object are represented by length and hash.
#[derive(Clone, Debug)]
pub(crate) struct ReadTrace {
    trace_id: Arc<str>,
    format: &'static str,
    next_call_instance: Arc<AtomicU64>,
}

#[derive(Clone, Debug)]
pub(crate) struct ReadCallTrace {
    instance: u64,
}

impl ReadTrace {
    pub(crate) fn from_env(trace_id: String, format: &'static str) -> Option<Self> {
        std::env::var("CC_SWITCH_READ_TRACE")
            .ok()
            .is_some_and(|value| read_trace_enabled_value(&value))
            .then(|| Self {
                trace_id: Arc::from(trace_id),
                format,
                next_call_instance: Arc::new(AtomicU64::new(1)),
            })
    }

    pub(crate) fn new_call(&self) -> ReadCallTrace {
        ReadCallTrace {
            instance: self.next_call_instance.fetch_add(1, Ordering::Relaxed),
        }
    }

    pub(crate) fn upstream_fragment(
        &self,
        call: &ReadCallTrace,
        event: &str,
        call_index: Option<usize>,
        output_index: Option<u64>,
        upstream_call_id: Option<&str>,
        name: &str,
        fragment: &str,
    ) {
        if name != "Read" {
            return;
        }
        self.log_raw(
            "upstream_fragment",
            call,
            event,
            call_index,
            output_index,
            upstream_call_id,
            name,
            fragment,
            None,
        );
    }

    pub(crate) fn upstream_complete(
        &self,
        call: &ReadCallTrace,
        event: &str,
        call_index: Option<usize>,
        output_index: Option<u64>,
        upstream_call_id: Option<&str>,
        name: &str,
        arguments: &str,
        source: &str,
    ) {
        if name != "Read" {
            return;
        }
        self.log_raw(
            "upstream_complete",
            call,
            event,
            call_index,
            output_index,
            upstream_call_id,
            name,
            arguments,
            Some(source),
        );
    }

    pub(crate) fn anthropic_emitted(
        &self,
        call: &ReadCallTrace,
        anthropic_tool_use_id: &str,
        name: &str,
        input: &Value,
        offset_sanitized: bool,
    ) {
        if name != "Read" {
            return;
        }
        let fields = describe_value(input);
        log::info!(
            "[read-trace] trace_id={} stage=anthropic_emitted format={} call_instance={} anthropic_tool_use_id={} name=Read {} offset_sanitized={}",
            self.trace_id,
            self.format,
            call.instance,
            anthropic_tool_use_id,
            fields,
            offset_sanitized,
        );
    }

    fn log_raw(
        &self,
        stage: &str,
        call: &ReadCallTrace,
        event: &str,
        call_index: Option<usize>,
        output_index: Option<u64>,
        upstream_call_id: Option<&str>,
        name: &str,
        raw: &str,
        source: Option<&str>,
    ) {
        let identity = format!(
            "call_index={} output_index={} upstream_call_id={} name={}",
            call_index.map_or_else(|| "absent".to_string(), |value| value.to_string()),
            output_index.map_or_else(|| "absent".to_string(), |value| value.to_string()),
            upstream_call_id.unwrap_or("absent"),
            name,
        );
        let payload = serde_json::from_str::<Value>(raw)
            .ok()
            .filter(Value::is_object)
            .map(|value| describe_value(&value))
            .unwrap_or_else(|| {
                format!(
                    "fragment_received=true fragment_length={} fragment_sha256={}",
                    raw.len(),
                    short_sha256_hex(raw.as_bytes())
                )
            });
        let source = source
            .map(|value| format!(" source={value}"))
            .unwrap_or_default();
        log::info!(
            "[read-trace] trace_id={} stage={} format={} event={} call_instance={} {} {}{}",
            self.trace_id,
            stage,
            self.format,
            event,
            call.instance,
            identity,
            payload,
            source,
        );
    }
}

fn read_trace_enabled_value(value: &str) -> bool {
    value == "1" || value.eq_ignore_ascii_case("true")
}

/// The only Read input projection allowed in diagnostics.
fn describe_value(value: &Value) -> String {
    let Some(object) = value.as_object() else {
        return format!(
            "fragment_received=true fragment_length={} fragment_sha256={}",
            serde_json::to_string(value).map_or(0, |value| value.len()),
            short_sha256_hex(serde_json::to_string(value).unwrap_or_default().as_bytes())
        );
    };

    let path = object.get("file_path").and_then(Value::as_str);
    let path_fields = match path {
        Some(path) => format!(
            "path_hash={} basename={}",
            short_sha256_hex(path.as_bytes()),
            basename(path)
        ),
        None => "path_hash=absent basename=absent".to_string(),
    };
    format!(
        "{} offset={} limit={} pages={}",
        path_fields,
        scalar_field(object.get("offset")),
        scalar_field(object.get("limit")),
        pages_field(object.get("pages")),
    )
}

fn basename(path: &str) -> &str {
    path.rsplit(['/', '\\']).next().unwrap_or(path)
}

fn scalar_field(value: Option<&Value>) -> String {
    match value {
        None => "absent".to_string(),
        Some(Value::Number(value)) => value.to_string(),
        Some(_) => "non_integer".to_string(),
    }
}

fn pages_field(value: Option<&Value>) -> String {
    match value {
        None => "absent".to_string(),
        Some(Value::String(value)) => format!("string:{}", value.len()),
        Some(Value::Number(value)) => value.to_string(),
        Some(_) => "present".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn trace_switch_requires_explicit_truthy_value() {
        assert!(read_trace_enabled_value("1"));
        assert!(read_trace_enabled_value("TRUE"));
        assert!(!read_trace_enabled_value(""));
        assert!(!read_trace_enabled_value("false"));
        assert!(!read_trace_enabled_value("yes"));
    }

    #[test]
    fn redacts_path_and_omits_unrelated_fields() {
        let secret_path = "C:\\secrets\\useUsersData.jsx";
        let fields = describe_value(&json!({
            "file_path": secret_path,
            "offset": 4951,
            "limit": 350,
            "pages": "1-2",
            "token": "must-not-appear"
        }));

        assert!(fields.contains("basename=useUsersData.jsx"));
        assert!(fields.contains("offset=4951"));
        assert!(!fields.contains(secret_path));
        assert!(!fields.contains("must-not-appear"));
    }

    #[test]
    fn non_object_is_hash_only() {
        let fields = describe_value(&json!("authorization=secret"));
        assert!(fields.contains("fragment_received=true"));
        assert!(!fields.contains("authorization"));
        assert!(!fields.contains("secret"));
    }

    #[test]
    fn call_instances_are_request_local_and_distinct() {
        let trace = ReadTrace {
            trace_id: Arc::from("request"),
            format: "openai_chat",
            next_call_instance: Arc::new(AtomicU64::new(1)),
        };
        assert_ne!(trace.new_call().instance, trace.new_call().instance);
    }
}
