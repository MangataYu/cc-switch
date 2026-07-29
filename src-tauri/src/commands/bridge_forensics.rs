use std::fs;
use std::path::{Component, Path, PathBuf};

use crate::config::get_app_config_dir;
use crate::proxy::bridge_forensics::{
    BridgeForensicStore, EvidenceBundleId, EvidenceBundleSummary, RetentionReport,
};

#[tauri::command]
pub fn list_bridge_evidence() -> Result<Vec<EvidenceBundleSummary>, String> {
    evidence_store()
        .list_bundles()
        .map_err(|error| error.to_string())
}

#[tauri::command]
pub fn delete_bridge_evidence(bundle_id: String) -> Result<(), String> {
    evidence_store()
        .delete_bundle(&EvidenceBundleId(bundle_id))
        .map_err(|error| error.to_string())
}

#[tauri::command]
pub fn export_bridge_evidence(bundle_id: String, destination: String) -> Result<(), String> {
    let root = evidence_root_from(&get_app_config_dir());
    let destination = PathBuf::from(destination);
    validate_export_destination(&root, &destination)?;
    BridgeForensicStore::new(root)
        .export_bundle(&EvidenceBundleId(bundle_id), &destination)
        .map_err(|error| error.to_string())
}

#[tauri::command]
pub fn cleanup_bridge_evidence() -> Result<RetentionReport, String> {
    evidence_store()
        .enforce_retention()
        .map_err(|error| error.to_string())
}

fn evidence_store() -> BridgeForensicStore {
    BridgeForensicStore::new(evidence_root_from(&get_app_config_dir()))
}

fn evidence_root_from(app_config_dir: &Path) -> PathBuf {
    app_config_dir.join("bridge-evidence")
}

fn validate_export_destination(root: &Path, destination: &Path) -> Result<(), String> {
    if destination.is_dir() {
        return Err("evidence export destination cannot be a directory".to_string());
    }
    if destination.file_name().is_none() {
        return Err("invalid evidence export destination".to_string());
    }

    let comparable_root = comparison_path(root)?;
    let comparable_destination = comparison_path(destination)?;
    if path_is_within(&comparable_destination, &comparable_root) {
        return Err("evidence export destination must be outside the evidence root".to_string());
    }
    Ok(())
}

fn comparison_path(path: &Path) -> Result<PathBuf, String> {
    if let Ok(canonical) = fs::canonicalize(path) {
        return Ok(canonical);
    }
    if let (Some(parent), Some(file_name)) = (path.parent(), path.file_name()) {
        if let Ok(canonical_parent) = fs::canonicalize(parent) {
            return Ok(canonical_parent.join(file_name));
        }
    }

    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|error| format!("failed to resolve export destination: {error}"))?
            .join(path)
    };
    Ok(normalize_lexically(&absolute))
}

fn normalize_lexically(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                if matches!(
                    normalized.components().next_back(),
                    Some(Component::Normal(_))
                ) {
                    normalized.pop();
                }
            }
            _ => normalized.push(component.as_os_str()),
        }
    }
    normalized
}

fn path_is_within(candidate: &Path, root: &Path) -> bool {
    #[cfg(windows)]
    {
        let candidate = candidate
            .to_string_lossy()
            .replace('\\', "/")
            .to_ascii_lowercase();
        let root = root
            .to_string_lossy()
            .replace('\\', "/")
            .trim_end_matches('/')
            .to_ascii_lowercase();
        candidate == root || candidate.starts_with(&format!("{root}/"))
    }

    #[cfg(not(windows))]
    {
        candidate.starts_with(root)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;

    use super::*;

    #[test]
    fn evidence_root_is_below_app_config_dir() {
        let root = evidence_root_from(Path::new("C:/tmp/cc-switch"));

        assert_eq!(root, Path::new("C:/tmp/cc-switch/bridge-evidence"));
    }

    #[test]
    fn export_rejects_destination_inside_bundle_root() {
        let root = Path::new("C:/tmp/cc-switch/bridge-evidence");
        let destination = root.join("bundles/out.zip");

        assert!(validate_export_destination(root, &destination).is_err());
    }

    #[test]
    fn export_rejects_existing_directory_destination() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("bridge-evidence");
        let destination = temp.path().join("existing-directory");
        fs::create_dir_all(&destination).unwrap();

        assert!(validate_export_destination(&root, &destination).is_err());
    }
}
