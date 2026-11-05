//! A PATH selector can prove that it resolves to the launching native image.
use std::process::Command;

#[test]
fn native_executable_report_has_no_room_or_config_side_effects() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    let room = root.path().join("room");
    std::fs::create_dir(&home).unwrap();
    let binary = std::path::Path::new(env!("CARGO_BIN_EXE_hcom"));
    let entries = vec![binary.to_path_buf()];
    #[cfg(unix)]
    let entries = {
        let mut entries = entries;
        use std::os::unix::fs::PermissionsExt;
        let wrapper = root.path().join("hcom-wrapper");
        let quoted = binary.to_string_lossy().replace('\'', "'\\''");
        std::fs::write(&wrapper, format!("#!/bin/sh\nexec '{quoted}' \"$@\"\n")).unwrap();
        std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o700)).unwrap();
        entries.push(wrapper);
        entries
    };
    for entry in entries {
        let output = Command::new(entry)
            .args(["--native-executable", "--name", "tuna"])
            .env_clear()
            .env("HOME", &home)
            .env("USERPROFILE", &home)
            .env("HCOM_DIR", &room)
            .env("HCOM_DEV_ROOT", root.path().join("missing-dev-root"))
            .current_dir(root.path())
            .output()
            .unwrap();
        assert!(output.status.success(), "{output:?}");
        assert!(output.stderr.is_empty());
        let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(report["hcom_native_image"], 1);
        assert_eq!(
            std::path::Path::new(report["executable"].as_str().unwrap()),
            binary.canonicalize().unwrap()
        );
        assert!(!room.exists());
        assert_eq!(std::fs::read_dir(&home).unwrap().count(), 0);
    }
}
