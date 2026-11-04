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
            hooks = [{'key': 'fixture-' + str(i), 'command': c, 'source': 'sessionFlags', 'currentHash': 'fixture-hash'} for i, c in enumerate(commands)]
            print(json.dumps({'id': request['id'], 'result': {'data': [{'hooks': hooks}]}}), flush=True)
    sys.exit(0)
assert not os.isatty(0) and not os.isatty(1), 'unexpected PTY'
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
record = {'args': args, 'env': {k: os.environ.get(k) for k in ['HCOM_LAUNCHED','HCOM_TOOL','HCOM_STDIO_MODE','HCOM_PTY_MODE','HCOM_LAUNCHED_BY']}, 'row': row(), 'states': states, 'start_output': start_output}
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

#[test]
fn codex_stdio_preserves_protocol_hooks_identity_and_exit() {
    for native in [
        vec!["app-server"],
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
