use std::path::Path;

fn main() {
    let path = std::env::args_os()
        .nth(1)
        .expect("usage: replay_bridge_bundle <bundle-dir>");
    if std::env::args_os().nth(2).is_some() {
        panic!("usage: replay_bridge_bundle <bundle-dir>");
    }
    let report = cc_switch_lib::replay_bundle(Path::new(&path)).expect("replay failed");
    println!(
        "{}",
        serde_json::to_string_pretty(&report).expect("failed to serialize replay report")
    );
}
