//! App-server clients own their raw streams; fixtures never call a model.
#![cfg(unix)]
mod support;

use serde_json::Value;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::fs::PermissionsExt;
use std::process::Stdio;
use support::Hcom;

fn fixture(h: &Hcom) -> std::path::PathBuf {
    let bin = h.root_path().join("pipe-tools");
    fs::create_dir(&bin).unwrap();
    let tool = bin.join("codex");
    fs::write(&tool, r#"#!/usr/bin/env python3
import json, os, re, shlex, signal, sqlite3, subprocess, sys
args = sys.argv[1:]
if args == ['--version']:
    print('codex-cli 0.134.0'); sys.exit(0)
commands = []
for arg in args:
    if arg.startswith('hooks.') and not arg.startswith('hooks.state='):
        commands += [json.loads('"' + x + '"') for x in re.findall(r'"?command"?\s*=\s*"((?:\\.|[^"\\])*)"', arg)]
if os.environ.get('HCOM_LAUNCHED') != '1':
    for line in sys.stdin:
        request = json.loads(line)
        if request.get('method') == 'initialize':
            print(json.dumps({'id': request['id'], 'result': {}}), flush=True)
        elif request.get('method') == 'hooks/list':
            if os.environ.get('FIXTURE_INSPECTIONS'):
                with open(os.environ['FIXTURE_INSPECTIONS'], 'a') as f: f.write('inspection\n')
            failure = os.environ.get('FIXTURE_INSPECTION_FAILURE')
            if failure and os.path.exists(failure):
                print(json.dumps({'id': request['id'], 'error': {'code': -32603, 'message': 'fixture hook list failure'}}), flush=True)
                continue
            hooks = [{'key': 'fixture-' + str(i), 'command': c, 'source': 'sessionFlags', 'currentHash': 'fixture-hash'} for i, c in enumerate(commands)]
            print(json.dumps({'id': request['id'], 'result': {'data': [{'hooks': hooks}]}}), flush=True)
    sys.exit(0)
assert not os.isatty(0) and not os.isatty(1), 'unexpected PTY'
if os.environ.get('FIXTURE_EARLY_EXIT'):
    sys.exit(int(os.environ['FIXTURE_EARLY_EXIT']))
if os.environ.get('FIXTURE_RETARGET_LINK'):
    os.unlink(os.environ['FIXTURE_RETARGET_LINK'])
    os.symlink(os.environ['FIXTURE_DECOY'], os.environ['FIXTURE_RETARGET_LINK'])
command = next(c for c in commands if c.endswith('codex-sessionstart'))
payload = {'session_id': '00000000-0000-0000-0000-000000000007', 'cwd': os.getcwd(), 'source': 'startup', 'transcript_path': os.path.join(os.getcwd(), 'fixture.jsonl')}
def hook(suffix, data):
    command = next(c for c in commands if c.endswith(suffix))
    result = subprocess.run(shlex.split(command) + ['--name', 'pezu'], input=json.dumps(data), text=True, capture_output=True)
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout) if result.stdout else {}
start_output = hook('codex-sessionstart', payload)
db = sqlite3.connect(os.path.join(os.environ['HCOM_DIR'], 'hcom.db'))
db.row_factory = sqlite3.Row
def row():
    return dict(db.execute('select * from instances where session_id=?', (payload['session_id'],)).fetchone())
states = {'start': row()}
hook('codex-userpromptsubmit', dict(payload, prompt='fixture'))
states['active'] = row()
hook('codex-sessionstart', {'session_id': '00000000-0000-0000-0000-000000000008', 'cwd': os.getcwd(), 'source': 'startup'})
states['secondary'] = row()
hook('codex-interrupt', payload)
states['interrupt'] = row()
hook('codex-stop', payload)
states['stop'] = row()
record = {'args': args, 'env': {k: os.environ.get(k) for k in ['HCOM_LAUNCHED','HCOM_TOOL','HCOM_STDIO_MODE','HCOM_PTY_MODE','HCOM_LAUNCHED_BY','HCOM_DIR','HCOM_INSTANCE_NAME','HCOM_PROCESS_ID','HCOM_LAUNCH_BATCH_ID','CODEX_EXEC_SERVER_URL','CODEX_HOME','CODEX_QUIET_MODE','OPENAI_BASE_URL','CLAUDE_CODE_MAX_OUTPUT_TOKENS']}, 'row': row(), 'states': states, 'start_output': start_output}
with open(os.environ['FIXTURE_RECORD'], 'w') as f: json.dump(record, f)
def cancelled(signum, frame):
    sys.stderr.write('cancelled\n'); sys.stderr.flush(); sys.exit(37)
if os.environ.get('FIXTURE_DEFAULT_SIGNAL') != '1':
    signal.signal(signal.SIGTERM, cancelled)
if os.environ.get('FIXTURE_WAIT') == '1':
    print('{"ready":true}', flush=True)
    sys.stdin.buffer.read()
    sys.exit(0)
if os.environ.get('FIXTURE_REPLACE_TOKEN') == '1':
    context = json.loads(row()['launch_context'])
    context['process_id'] = 'newer-launch'
    db.execute('update instances set launch_context=? where name=?', (json.dumps(context), row()['name']))
    db.commit()
sys.stdout.buffer.write(sys.stdin.buffer.read()); sys.stdout.buffer.flush()
sys.stderr.write('native diagnostic\n')
sys.exit(23)
"#).unwrap();
    fs::set_permissions(&tool, fs::Permissions::from_mode(0o755)).unwrap();
    let record = h.root_path().join("native.json");
    h.set_launch_env(
        "PATH",
        &format!(
            "{}:{}:{}",
            bin.display(),
            std::path::Path::new(env!("CARGO_BIN_EXE_hcom"))
                .parent()
                .unwrap()
                .display(),
            std::env::var("PATH").unwrap()
        ),
    );
    h.set_launch_env("FIXTURE_RECORD", record.to_str().unwrap());
    record
}

fn assert_clean(h: &Hcom) {
    let db = rusqlite::Connection::open(h.hcom_dir.join("hcom.db")).unwrap();
    let count: i64 = db
        .query_row("SELECT COUNT(*) FROM instances", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 0, "native exit left a registered instance");
    let actor: String = db.query_row("SELECT instance FROM events WHERE json_extract(data, '$.action') = 'batch_launched' ORDER BY id DESC LIMIT 1", [], |r| r.get(0)).unwrap();
    assert_eq!(actor, "pezu");
}

/// Each invocation has a fresh room while the tool installation, Codex home,
/// workspace, and validation cache remain the same. No live tool is launched.
struct CacheFixture {
    h: Hcom,
    cache: std::path::PathBuf,
    manifest: std::path::PathBuf,
    native: std::path::PathBuf,
    setup: std::path::PathBuf,
    inspections: std::path::PathBuf,
    failure: std::path::PathBuf,
}

impl CacheFixture {
    fn new() -> Self {
        let h = Hcom::new();
        fixture(&h);
        let cache = h.root_path().join("shared-validation-cache");
        let manifest = h.root_path().join("cache-inputs.json");
        // The manifest contract requires a native image, rather than allowing a
        // script to assert that its own digest covers its hidden dependencies.
        // This disposable image is only fingerprinted; the Python fixture owns
        // the protocol exchange and never executes it.
        let native = h.root_path().join("native-image");
        let native_target = h.root_path().join("native-image-v1");
        fs::copy("/bin/sh", &native_target).unwrap();
        std::os::unix::fs::symlink(&native_target, &native).unwrap();
        let setup = h.root_path().join("launcher-setup");
        fs::write(&setup, "setup-v1").unwrap();
        fs::write(
            &manifest,
            serde_json::json!({
                "version": 1,
                "native_binary": native,
                "files": [setup],
            })
            .to_string(),
        )
        .unwrap();
        let inspections = h.root_path().join("inspections");
        let failure = h.root_path().join("inspection-failure");
        h.set_launch_env("FIXTURE_INSPECTIONS", inspections.to_str().unwrap());
        h.set_launch_env("FIXTURE_INSPECTION_FAILURE", failure.to_str().unwrap());
        Self {
            h,
            cache,
            manifest,
            native,
            setup,
            inspections,
            failure,
        }
    }

    fn launch(&self, room: &str, manifest: bool) -> std::process::Output {
        self.launch_at(room, manifest, None, &[])
    }

    fn launch_at(
        &self,
        room: &str,
        manifest: bool,
        cwd: Option<&std::path::Path>,
        native_args: &[&str],
    ) -> std::process::Output {
        let room = self.h.root_path().join(room);
        fs::create_dir_all(room.join(".tmp/flags")).unwrap();
        fs::write(room.join(".tmp/flags/update_check"), "").unwrap();
        fs::copy(self.h.hcom_dir.join("env"), room.join("env")).unwrap();
        // Keep permission setup stable so this tests a change of room alone,
        // rather than the first launch materializing hcom.rules after preflight.
        fs::write(
            room.join("config.toml"),
            "[preferences]\nauto_approve = false\n",
        )
        .unwrap();
        let mut command = self.h.cmd();
        command
            .env("HCOM_DIR", &room)
            .env("HCOM_CODEX_CACHE_DIR", &self.cache)
            .env("CODEX_SESSION_ID", format!("session-{}", room.display()))
            .env("CODEX_THREAD_ID", format!("thread-{}", room.display()))
            .env("CODEX_TOOL_CALL_ID", format!("call-{}", room.display()))
            .env("CODEX_SQLITE_HOME", room.join("codex-sqlite"));
        if manifest {
            command.env("HCOM_CODEX_CACHE_INPUTS", &self.manifest);
        }
        if let Some(cwd) = cwd {
            command.current_dir(cwd);
        }
        command
            .args([
                "codex",
                "--stdio",
                "--run-here",
                "--go",
                "--name",
                "pezu",
                "--",
            ])
            .args(native_args)
            .arg("app-server")
            .stdin(Stdio::null())
            .output()
            .unwrap()
    }

    fn launch_ok(&self, room: &str, manifest: bool) -> Value {
        self.launch_ok_at(room, manifest, None, &[])
    }

    fn launch_ok_at(
        &self,
        room: &str,
        manifest: bool,
        cwd: Option<&std::path::Path>,
        native_args: &[&str],
    ) -> Value {
        let output = self.launch_at(room, manifest, cwd, native_args);
        assert_eq!(output.status.code(), Some(23), "{output:?}");
        assert_eq!(output.stderr, b"native diagnostic\n", "{output:?}");
        let record: Value =
            serde_json::from_slice(&fs::read(self.h.root_path().join("native.json")).unwrap())
                .unwrap();
        assert_eq!(
            record["env"]["HCOM_DIR"],
            self.h.root_path().join(room).to_str().unwrap()
        );
        record
    }

    fn count(&self) -> usize {
        fs::read_to_string(&self.inspections)
            .unwrap_or_default()
            .lines()
            .count()
    }

    fn entries(&self) -> Vec<std::path::PathBuf> {
        fn visit(dir: &std::path::Path, entries: &mut Vec<std::path::PathBuf>) {
            if !dir.exists() {
                return;
            }
            for entry in fs::read_dir(dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    visit(&path, entries);
                } else if path.extension().is_some_and(|ext| ext == "json") {
                    entries.push(path);
                }
            }
        }
        let mut entries = Vec::new();
        visit(&self.cache, &mut entries);
        entries
    }
}

#[test]
fn codex_validation_cache_reuses_across_rooms_and_invalidates_inputs() {
    let mut f = CacheFixture::new();
    let first = f.launch_ok("room-a", true);
    assert_eq!(f.count(), 1);
    let second = f.launch_ok("room-b", true);
    assert_eq!(f.count(), 1, "a new HCOM_DIR repeated hooks/list");
    let relocated_manifest = f.h.root_path().join("next-run-cache-inputs.json");
    fs::copy(&f.manifest, &relocated_manifest).unwrap();
    f.manifest = relocated_manifest;
    f.launch_ok("room-relocated-manifest", true);
    assert_eq!(
        f.count(),
        1,
        "relocating an unchanged launcher manifest repeated hooks/list"
    );
    assert_ne!(
        first["env"]["HCOM_PROCESS_ID"], second["env"]["HCOM_PROCESS_ID"],
        "sharing validation must not reuse launch identity"
    );
    assert!(f.h.root_path().join("room-a/hcom.db").exists());
    assert!(f.h.root_path().join("room-b/hcom.db").exists());
    assert!(!f.cache.join("hcom.db").exists());
    assert_eq!(f.entries().len(), 1);

    fs::write(f.h.codex_home.join("config.toml"), "model = 'fixture'\n").unwrap();
    f.launch_ok("room-config", true);
    assert_eq!(f.count(), 2, "Codex config change reused stale validation");

    fs::write(&f.setup, "setup-v2").unwrap();
    f.launch_ok("room-setup", true);
    assert_eq!(f.count(), 3, "wrapper setup change reused stale validation");

    fs::OpenOptions::new()
        .append(true)
        .open(&f.native)
        .unwrap()
        .write_all(b"changed native image")
        .unwrap();
    f.launch_ok("room-native", true);
    assert_eq!(
        f.count(),
        4,
        "same-version native change reused stale validation"
    );

    let new_target = f.h.root_path().join("native-image-v2");
    fs::copy(&f.native, &new_target).unwrap();
    fs::remove_file(&f.native).unwrap();
    std::os::unix::fs::symlink(&new_target, &f.native).unwrap();
    f.launch_ok("room-retarget", true);
    assert_eq!(
        f.count(),
        5,
        "same-content native pin retarget reused stale validation"
    );

    fs::OpenOptions::new()
        .append(true)
        .open(f.h.root_path().join("pipe-tools/codex"))
        .unwrap()
        .write_all(b"\n# wrapper implementation changed\n")
        .unwrap();
    f.launch_ok("room-wrapper", true);
    assert_eq!(
        f.count(),
        6,
        "same-version wrapper change reused stale validation"
    );
}

#[test]
fn codex_validation_cache_reuses_empty_bundles_and_tracks_project_config() {
    let f = CacheFixture::new();
    let native_args = [
        "-c",
        "model_reasoning_effort='high'",
        "-c",
        "service_tier='fast'",
    ];
    let project = f.h.root_path().join("panel-bundles");
    let bundles = [
        "draft/one",
        "ratify/retry/two",
        "draft/three",
        "ratify/four",
    ]
    .map(|name| project.join(name));
    for bundle in &bundles {
        fs::create_dir_all(bundle).unwrap();
    }

    // A panel changes both Popen's cwd and HCOM_DIR on every invocation.
    // Empty intermediate directories, including their depth, aren't setup.
    let first = f.launch_ok_at("bundle-room-a", true, Some(&bundles[0]), &native_args);
    let second = f.launch_ok_at("bundle-room-b", true, Some(&bundles[1]), &native_args);
    assert_eq!(
        f.count(),
        1,
        "equivalent empty bundle cwds repeated hooks/list"
    );
    assert_eq!(
        first["row"]["directory"],
        bundles[0].canonicalize().unwrap().to_str().unwrap()
    );
    assert_eq!(
        second["row"]["directory"],
        bundles[1].canonicalize().unwrap().to_str().unwrap()
    );
    assert_ne!(first["env"]["HCOM_DIR"], second["env"]["HCOM_DIR"]);

    fs::create_dir(project.join(".codex")).unwrap();
    let project_config = project.join(".codex/config.toml");
    fs::write(&project_config, "model = 'project-one'\n").unwrap();
    f.launch_ok_at(
        "bundle-room-project-created",
        true,
        Some(&bundles[2]),
        &native_args,
    );
    assert_eq!(
        f.count(),
        2,
        "new ancestor project config reused stale validation"
    );

    fs::write(&project_config, "model = 'project-two'\n").unwrap();
    f.launch_ok_at(
        "bundle-room-project-changed",
        true,
        Some(&bundles[3]),
        &native_args,
    );
    assert_eq!(
        f.count(),
        3,
        "changed ancestor project config reused stale validation"
    );

    fs::create_dir(bundles[3].join(".codex")).unwrap();
    fs::write(
        bundles[3].join(".codex/config.toml"),
        "model = 'bundle-local'\n",
    )
    .unwrap();
    f.launch_ok_at(
        "bundle-room-local-config",
        true,
        Some(&bundles[3]),
        &native_args,
    );
    assert_eq!(
        f.count(),
        4,
        "new bundle-local config reused stale validation"
    );
}

#[test]
fn codex_validation_cache_keeps_cwd_for_relative_config_overrides() {
    let f = CacheFixture::new();
    let bundles = ["relative-panel-a", "relative-panel-b"].map(|name| f.h.root_path().join(name));
    for (index, bundle) in bundles.iter().enumerate() {
        fs::create_dir(bundle).unwrap();
        fs::write(
            bundle.join("relative.md"),
            format!("instructions for panel {index}\n"),
        )
        .unwrap();
    }
    let native_args = ["-c", "model_instructions_file='relative.md'"];
    f.launch_ok_at("relative-room-a", true, Some(&bundles[0]), &native_args);
    f.launch_ok_at("relative-room-b", true, Some(&bundles[1]), &native_args);
    assert_eq!(
        f.count(),
        2,
        "a relative config path shared validation across different working directories"
    );
}

#[test]
fn codex_validation_cache_recovers_corruption_and_never_caches_failure() {
    let f = CacheFixture::new();
    f.launch_ok("room-warm", true);
    let entries = f.entries();
    assert_eq!(entries.len(), 1);
    // Syntactically valid JSON must still contain all validated hook entries.
    fs::write(&entries[0], "[]").unwrap();
    f.launch_ok("room-empty-cache", true);
    assert_eq!(f.count(), 2, "invalid cached entries bypassed hooks/list");

    fs::write(&entries[0], "{truncated").unwrap();
    fs::write(&f.failure, "fail inspection").unwrap();
    for (room, count) in [("room-failed-a", 3), ("room-failed-b", 4)] {
        let output = f.launch(room, true);
        assert!(!output.status.success(), "{output:?}");
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("fixture hook list failure"),
            "{output:?}"
        );
        assert_eq!(f.count(), count, "failed validation became a cache hit");
    }
    fs::remove_file(&f.failure).unwrap();
    f.launch_ok("room-recovered", true);
    assert_eq!(f.count(), 5);
    f.launch_ok("room-reused", true);
    assert_eq!(f.count(), 5, "recovered validation was not published");
}

#[test]
fn codex_validation_cache_requires_complete_wrapper_inputs() {
    let f = CacheFixture::new();
    for room in ["room-opaque-a", "room-opaque-b"] {
        f.launch_ok(room, false);
    }
    assert_eq!(f.count(), 2, "opaque wrapper reused unverifiable identity");
    assert!(f.entries().is_empty());

    fs::write(&f.manifest, "{broken").unwrap();
    for room in ["room-manifest-a", "room-manifest-b"] {
        f.launch_ok(room, true);
    }
    assert_eq!(f.count(), 4, "invalid manifest bypassed fresh validation");
    assert!(f.entries().is_empty());

    fs::write(
        &f.manifest,
        serde_json::json!({
            "version": 1,
            "native_binary": f.native,
            "files": [f.h.root_path().join("missing-setup-input")],
        })
        .to_string(),
    )
    .unwrap();
    for room in ["room-missing-input-a", "room-missing-input-b"] {
        f.launch_ok(room, true);
    }
    assert_eq!(
        f.count(),
        6,
        "missing setup input bypassed fresh validation"
    );
    assert!(f.entries().is_empty());
}

#[test]
fn codex_stdio_attributes_early_exit_and_cleans_up_before_session_start() {
    let h = Hcom::new();
    let record = fixture(&h);
    h.set_launch_env("FIXTURE_EARLY_EXIT", "9");
    let output = h
        .cmd()
        // The supervisor may inherit these from an unrelated parent launch.
        .env("HCOM_LAUNCHED_BY", "stale-launcher")
        .env("HCOM_LAUNCH_BATCH_ID", "stale-batch")
        .args([
            "codex",
            "--stdio",
            "--run-here",
            "--go",
            "--name",
            "pezu",
            "--tag",
            "acc",
            "--",
            "app-server",
        ])
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(9), "{output:?}");
    assert!(output.stdout.is_empty(), "{output:?}");
    assert!(output.stderr.is_empty(), "{output:?}");
    assert!(
        !record.exists(),
        "the fixture must exit before SessionStart"
    );

    let db = rusqlite::Connection::open(h.hcom_dir.join("hcom.db")).unwrap();
    let rows: Vec<(String, Value)> = db
        .prepare(
            "SELECT instance, data FROM events WHERE type = 'life'
               AND json_extract(data, '$.action') IN ('batch_launched', 'launch_failed')
             ORDER BY id",
        )
        .unwrap()
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                serde_json::from_str(&row.get::<_, String>(1)?).unwrap(),
            ))
        })
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(rows.len(), 2, "{rows:?}");
    let (launched_instance, launched) = &rows[0];
    let (failed_instance, failed) = &rows[1];
    assert_eq!(launched["action"], "batch_launched", "{rows:?}");
    assert_eq!(launched_instance, "pezu");
    assert_eq!(launched["by"], "pezu");
    assert_eq!(launched["tool"], "codex");
    assert_eq!(launched["tag"], "acc");
    assert_eq!(launched["stdio"], true);
    let batch = launched["batch_id"].as_str().unwrap();
    assert!(!batch.is_empty());
    assert_ne!(batch, "stale-batch");
    assert!(!failed_instance.is_empty());
    assert_eq!(launched["instances"], serde_json::json!([failed_instance]));
    assert_eq!(failed["action"], "launch_failed");
    assert_eq!(failed["by"], "pezu");
    assert_eq!(failed["batch_id"], batch);
    assert_eq!(failed["reason"], "exited_before_ready");
    let notice: String = db
        .query_row(
            "SELECT json_extract(data, '$.text') FROM events
             WHERE type = 'message' AND instance = 'sys_[hcom-launcher]'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        notice,
        format!(
            "@pezu Launch failed: {failed_instance}: codex stdio exited with status 9 before SessionStart (batch: {batch})"
        )
    );
    assert_clean(&h);
}

#[test]
fn codex_stdio_scrubs_instance_state_after_env_overlays() {
    let h = Hcom::new();
    let record = fixture(&h);
    // These extras are applied after the inherited environment is cleaned.
    // A parent-only check would miss instance state reintroduced by an overlay.
    h.set_launch_env("CODEX_EXEC_SERVER_URL", "overlay-parent-exec-server");
    h.set_launch_env("CODEX_QUIET_MODE", "1");
    h.set_launch_env("OPENAI_BASE_URL", "https://provider.example.invalid/v1");
    h.set_launch_env("CLAUDE_CODE_MAX_OUTPUT_TOKENS", "4096");
    let output = h
        .cmd()
        .env("CODEX_EXEC_SERVER_URL", "inherited-parent-exec-server")
        .args([
            "codex",
            "--stdio",
            "--run-here",
            "--go",
            "--name",
            "pezu",
            "--",
            "app-server",
        ])
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(23),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value: Value = serde_json::from_str(&fs::read_to_string(record).unwrap()).unwrap();
    let env = &value["env"];
    assert!(
        env["CODEX_EXEC_SERVER_URL"].is_null(),
        "stdio child inherited a parent's execution server"
    );
    assert_eq!(env["CODEX_HOME"], h.codex_home.to_str().unwrap());
    assert_eq!(env["CODEX_QUIET_MODE"], "1");
    assert_eq!(
        env["OPENAI_BASE_URL"],
        "https://provider.example.invalid/v1"
    );
    assert_eq!(env["CLAUDE_CODE_MAX_OUTPUT_TOKENS"], "4096");
    assert_eq!(env["HCOM_DIR"], h.hcom_dir.to_str().unwrap());
    assert_eq!(env["HCOM_INSTANCE_NAME"], value["row"]["name"]);
    assert_eq!(env["HCOM_LAUNCHED_BY"], "pezu");
    assert_eq!(env["HCOM_LAUNCHED"], "1");
    assert_eq!(env["HCOM_TOOL"], "codex");
    assert_eq!(env["HCOM_STDIO_MODE"], "1");
    assert_eq!(env["HCOM_PTY_MODE"], "0");
    let context: Value =
        serde_json::from_str(value["row"]["launch_context"].as_str().unwrap()).unwrap();
    assert_eq!(env["HCOM_PROCESS_ID"], context["process_id"]);
    for key in ["HCOM_PROCESS_ID", "HCOM_LAUNCH_BATCH_ID"] {
        assert!(!env[key].as_str().unwrap().is_empty(), "{key}");
    }
    assert_clean(&h);
}

#[test]
fn codex_stdio_preserves_protocol_hooks_identity_and_exit() {
    for native in [
        vec!["app-server"],
        vec![
            "app-server",
            "--cloud-skills-snapshot",
            "/tmp/cloud skills.json",
            "--code-mode-host",
            "http://localhost:9876",
            "--strict-config",
            "--stdio",
            "--startup-client-info",
            r#"{"name":"paradev"}"#,
        ],
        vec![
            "app-server",
            "--cloud-skills-snapshot=/tmp/cloud skills.json",
            "--code-mode-host=http://localhost:9876",
            "--listen=stdio://",
        ],
        vec![
            "-c",
            "model=\"fixture\"",
            "--enable",
            "hooks",
            "--dangerously-bypass-hook-trust",
            "app-server",
            "--listen",
            "stdio://",
        ],
    ] {
        let h = Hcom::new();
        let record = fixture(&h);
        // Interactive defaults are not valid app-server arguments.
        fs::write(
            h.hcom_dir.join("config.toml"),
            "codex_args = '-m interactive --full-auto'\n",
        )
        .unwrap();
        let mut child = h
            .cmd()
            .args([
                "codex",
                "--stdio",
                "--run-here",
                "--go",
                "--name",
                "pezu",
                "--",
            ])
            .args(&native)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let wire = b"{\"id\":7,\"method\":\"initialize\"}\n{\"json\":\"\\u2028\"}\r\n";
        child.stdin.take().unwrap().write_all(wire).unwrap();
        let output = child.wait_with_output().unwrap();
        assert_eq!(output.status.code(), Some(23), "{output:?}");
        assert_eq!(output.stdout, wire);
        assert_eq!(output.stderr, b"native diagnostic\n");
        let v: Value = serde_json::from_str(&fs::read_to_string(record).unwrap()).unwrap();
        let actual = v["args"].as_array().unwrap();
        assert_eq!(
            &actual[..native.len()],
            native
                .iter()
                .map(|a| Value::String((*a).into()))
                .collect::<Vec<_>>()
        );
        assert!(
            !actual
                .iter()
                .any(|a| a == "--full-auto" || a == "interactive")
        );
        let bootstrap = v["start_output"]["hookSpecificOutput"]["additionalContext"]
            .as_str()
            .unwrap();
        assert!(bootstrap.contains("pipe client"));
        assert!(bootstrap.contains(v["row"]["name"].as_str().unwrap()));
        assert_eq!(v["env"]["HCOM_LAUNCHED"], "1");
        assert_eq!(v["env"]["HCOM_TOOL"], "codex");
        assert_eq!(v["env"]["HCOM_STDIO_MODE"], "1");
        assert_eq!(v["env"]["HCOM_PTY_MODE"], "0");
        assert_eq!(v["env"]["HCOM_LAUNCHED_BY"], "pezu");
        assert_eq!(
            v["row"]["session_id"],
            "00000000-0000-0000-0000-000000000007"
        );
        assert_eq!(v["row"]["tool"], "codex");
        assert_eq!(v["states"]["active"]["status"], "active");
        assert_eq!(v["states"]["secondary"]["status"], "active");
        assert_eq!(
            v["states"]["secondary"]["session_id"],
            v["row"]["session_id"]
        );
        assert_eq!(v["states"]["interrupt"]["status"], "listening");
        assert_eq!(v["states"]["interrupt"]["status_context"], "interrupted");
        assert_eq!(v["states"]["stop"]["status"], "listening");
        assert!(v["row"]["pid"].as_i64().unwrap() > 0);
        assert!(!h.hcom_dir.join(".tmp/launch").exists());
        assert_clean(&h);
    }
}

#[test]
fn codex_stdio_hooks_keep_the_launch_image_after_path_and_symlink_change() {
    let h = Hcom::new();
    fixture(&h);
    let decoy = h.root_path().join("pipe-tools/hcom");
    fs::write(&decoy, "#!/bin/sh\necho wrong-hcom-image >&2\nexit 42\n").unwrap();
    fs::set_permissions(&decoy, fs::Permissions::from_mode(0o755)).unwrap();
    let link = h.root_path().join("hcom launch 'link");
    std::os::unix::fs::symlink(env!("CARGO_BIN_EXE_hcom"), &link).unwrap();
    h.set_launch_env("FIXTURE_RETARGET_LINK", link.to_str().unwrap());
    h.set_launch_env("FIXTURE_DECOY", decoy.to_str().unwrap());
    let template = h.cmd();
    let mut command = std::process::Command::new(&link);
    command.env_clear();
    for (key, value) in template.get_envs() {
        if let Some(value) = value {
            command.env(key, value);
        }
    }
    command.current_dir(template.get_current_dir().unwrap());
    let mut child = command
        .args([
            "codex",
            "--stdio",
            "--run-here",
            "--go",
            "--name",
            "pezu",
            "--",
            "app-server",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let wire = b"{\"id\":7}\n";
    child.stdin.take().unwrap().write_all(wire).unwrap();
    let output = child.wait_with_output().unwrap();
    assert_eq!(output.status.code(), Some(23), "{output:?}");
    assert_eq!(output.stdout, wire);
    assert_eq!(output.stderr, b"native diagnostic\n");
    assert_eq!(fs::read_link(&link).unwrap(), decoy);
    assert_clean(&h);
}

#[test]
fn codex_stdio_forwards_cancel_while_input_stays_open() {
    let h = Hcom::new();
    fixture(&h);
    h.set_launch_env("FIXTURE_WAIT", "1");
    let mut child = h
        .cmd()
        .args([
            "codex",
            "--stdio",
            "--go",
            "--name",
            "pezu",
            "--",
            "app-server",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut output = BufReader::new(child.stdout.take().unwrap());
    let mut line = String::new();
    output.read_line(&mut line).unwrap();
    assert_eq!(line, "{\"ready\":true}\n");
    let _input = child.stdin.take().unwrap();
    assert_eq!(unsafe { libc::kill(child.id() as i32, libc::SIGTERM) }, 0);
    let result = child.wait_with_output().unwrap();
    assert_eq!(result.status.code(), Some(37));
    assert_eq!(result.stderr, b"cancelled\n");
    assert_clean(&h);
}

#[test]
fn codex_stdio_rejects_non_pipe_modes_without_stdout() {
    for args in [
        vec!["codex", "--stdio"],
        vec!["codex", "--stdio", "--", "exec", "app-server"],
        vec!["codex", "--stdio", "--", "app-server", "generate-ts"],
        vec![
            "codex",
            "--stdio",
            "--",
            "app-server",
            "generate-json-schema",
        ],
        vec![
            "codex",
            "--stdio",
            "--",
            "app-server",
            "--listen",
            "ws://127.0.0.1:9000",
        ],
        vec![
            "codex",
            "--stdio",
            "--",
            "app-server",
            "--listen=ws://127.0.0.1:9000",
        ],
        vec!["codex", "--stdio", "--", "-c", "app-server"],
        vec!["2", "codex", "--stdio", "--", "app-server"],
        vec!["codex", "--stdio", "--headless", "--", "app-server"],
        vec!["codex", "--stdio", "--json", "--", "app-server"],
        vec!["codex", "--stdio", "--terminal", "here", "--", "app-server"],
        vec!["codex", "--stdio", "--device", "peer", "--", "app-server"],
        vec!["codex", "--stdio", "--no-run-here", "--", "app-server"],
        vec![
            "codex",
            "--stdio",
            "--hcom-prompt",
            "hello",
            "--",
            "app-server",
        ],
    ] {
        let h = Hcom::new();
        let (code, stdout, stderr) = h
            .cmd()
            .args(["--name", "pezu", "--go"])
            .args(&args)
            .output()
            .map(|o| {
                (
                    o.status.code().unwrap(),
                    String::from_utf8(o.stdout).unwrap(),
                    String::from_utf8(o.stderr).unwrap(),
                )
            })
            .unwrap();
        assert_ne!(code, 0, "{args:?}");
        assert!(stdout.is_empty(), "{args:?}: {stdout}");
        assert!(stderr.contains("--stdio"), "{args:?}: {stderr}");
    }
}

#[test]
fn codex_stdio_resume_keeps_canonical_identity_and_cleans_up() {
    let h = Hcom::new();
    let record = fixture(&h);
    let mut canonical = None;
    for _ in 0..2 {
        let output = h
            .cmd()
            .args([
                "codex",
                "--stdio",
                "--go",
                "--name",
                "pezu",
                "--",
                "app-server",
            ])
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(23), "{output:?}");
        let value: Value = serde_json::from_str(&fs::read_to_string(&record).unwrap()).unwrap();
        let name = value["row"]["name"].as_str().unwrap().to_string();
        if let Some(ref expected) = canonical {
            assert_eq!(&name, expected);
        }
        let bootstrap = value["start_output"]["hookSpecificOutput"]["additionalContext"]
            .as_str()
            .unwrap();
        assert!(
            bootstrap.contains(&format!("Your name: {name}")),
            "{bootstrap}"
        );
        canonical = Some(name);
        assert_clean(&h);
        let db = rusqlite::Connection::open(h.hcom_dir.join("hcom.db")).unwrap();
        let failures: i64 = db.query_row("SELECT COUNT(*) FROM events WHERE json_extract(data, '$.reason') = 'exited_before_ready'", [], |r| r.get(0)).unwrap();
        assert_eq!(failures, 0);
        let ready: i64 = db
            .query_row(
                "SELECT COUNT(*) FROM events WHERE json_extract(data, '$.action') = 'ready'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let batches: i64 = db.query_row("SELECT COUNT(DISTINCT json_extract(data, '$.batch_id')) FROM events WHERE json_extract(data, '$.action') = 'batch_launched'", [], |r| r.get(0)).unwrap();
        assert_eq!(
            ready, batches,
            "only the root thread emits ready, once per launch"
        );
    }
}

#[test]
fn codex_stdio_propagates_native_signal_exit() {
    let h = Hcom::new();
    fixture(&h);
    h.set_launch_env("FIXTURE_WAIT", "1");
    h.set_launch_env("FIXTURE_DEFAULT_SIGNAL", "1");
    let mut child = h
        .cmd()
        .args([
            "codex",
            "--stdio",
            "--go",
            "--name",
            "pezu",
            "--",
            "app-server",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut output = BufReader::new(child.stdout.take().unwrap());
    let mut line = String::new();
    output.read_line(&mut line).unwrap();
    assert_eq!(line, "{\"ready\":true}\n");
    let _input = child.stdin.take().unwrap();
    assert_eq!(unsafe { libc::kill(child.id() as i32, libc::SIGTERM) }, 0);
    let result = child.wait_with_output().unwrap();
    assert_eq!(result.status.code(), Some(128 + libc::SIGTERM));
    assert!(result.stderr.is_empty());
    assert_clean(&h);
}

#[test]
fn old_stdio_exit_does_not_finalize_a_newer_launch_token() {
    let h = Hcom::new();
    let record = fixture(&h);
    h.set_launch_env("FIXTURE_REPLACE_TOKEN", "1");
    let output = h
        .cmd()
        .args([
            "codex",
            "--stdio",
            "--go",
            "--name",
            "pezu",
            "--",
            "app-server",
        ])
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(23), "{output:?}");
    let value: Value = serde_json::from_str(&fs::read_to_string(record).unwrap()).unwrap();
    let db = rusqlite::Connection::open(h.hcom_dir.join("hcom.db")).unwrap();
    let token: String = db
        .query_row(
            "SELECT json_extract(launch_context, '$.process_id') FROM instances WHERE name=?",
            [value["row"]["name"].as_str().unwrap()],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(token, "newer-launch");
}

#[test]
fn codex_stdio_requires_go_inside_an_agent_without_printing_preview() {
    let h = Hcom::new();
    let output = h
        .cmd()
        .env("HCOM_LAUNCHED", "1")
        .args(["codex", "--stdio", "--name", "pezu", "--", "app-server"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("--stdio requires --go")
    );
}

#[test]
fn codex_stdio_preserves_explicit_system_prompt_as_toml() {
    let h = Hcom::new();
    let record = fixture(&h);
    let prompt = "Keep \"quoted\" text.\nSecond line.";
    let output = h
        .cmd()
        .args([
            "codex",
            "--stdio",
            "--go",
            "--name",
            "pezu",
            "--hcom-system-prompt",
            prompt,
            "--",
            "app-server",
        ])
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(23), "{output:?}");
    let value: Value = serde_json::from_str(&fs::read_to_string(record).unwrap()).unwrap();
    let arg = value["args"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(Value::as_str)
        .find(|arg| arg.starts_with("developer_instructions="))
        .unwrap();
    let parsed: toml::Value = toml::from_str(arg).unwrap();
    assert_eq!(parsed["developer_instructions"].as_str(), Some(prompt));
    assert_clean(&h);
}
