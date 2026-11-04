//! Existing-session takeover must move cleanup ownership through the real hook.
#![cfg(unix)]
mod support;

use serde_json::Value;
use std::fs;
use std::io::{BufRead, BufReader};
use std::os::unix::fs::PermissionsExt;
use std::process::{Child, Stdio};
use support::Hcom;

fn fixture(h: &Hcom) {
    let bin = h.root_path().join("takeover-tools");
    fs::create_dir(&bin).unwrap();
    let tool = bin.join("codex");
    fs::write(&tool, r#"#!/usr/bin/env python3
import json, os, re, shlex, sqlite3, subprocess, sys, time
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
db = sqlite3.connect(os.path.join(os.environ['HCOM_DIR'], 'hcom.db'))
db.row_factory = sqlite3.Row
token = os.environ['HCOM_PROCESS_ID']
# Synchronize with the supervisor's on_spawn write, isolating binding takeover
# from the independent startup callback race.
while True:
    row = db.execute('select i.pid from instances i join process_bindings b on b.instance_name=i.name where b.process_id=?', (token,)).fetchone()
    if row and row['pid'] == os.getpid():
        break
    time.sleep(0.01)
payload = {'session_id': '00000000-0000-0000-0000-000000000037', 'cwd': os.getcwd(), 'source': 'startup', 'transcript_path': os.path.join(os.getcwd(), 'takeover.jsonl')}
command = next(c for c in commands if c.endswith('codex-sessionstart'))
result = subprocess.run(shlex.split(command) + ['--name', 'pezu'], input=json.dumps(payload), text=True, capture_output=True)
assert result.returncode == 0, result.stderr
row = dict(db.execute('select * from instances where session_id=?', (payload['session_id'],)).fetchone())
print(json.dumps({'name': row['name'], 'pid': row['pid'], 'token': token, 'stored_token': json.loads(row['launch_context'])['process_id']}), flush=True)
sys.stdin.buffer.read()
"#).unwrap();
    fs::set_permissions(&tool, fs::Permissions::from_mode(0o755)).unwrap();
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
}

fn start(h: &Hcom) -> (Child, Value) {
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
    if line.is_empty() {
        drop(child.stdin.take());
        let result = child.wait_with_output().unwrap();
        panic!("native fixture did not bind: {result:?}");
    }
    (child, serde_json::from_str(&line).unwrap())
}

fn close(mut child: Child) {
    drop(child.stdin.take());
    let result = child.wait_with_output().unwrap();
    assert!(result.status.success(), "{result:?}");
    assert!(result.stderr.is_empty(), "{result:?}");
}

fn current_owner(h: &Hcom) -> Option<(String, i64, String)> {
    use rusqlite::OptionalExtension;
    let db = rusqlite::Connection::open(h.hcom_dir.join("hcom.db")).unwrap();
    db.query_row(
        "SELECT name,pid,json_extract(launch_context,'$.process_id') FROM instances WHERE session_id=?",
        ["00000000-0000-0000-0000-000000000037"],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    )
    .optional()
    .unwrap()
}

#[test]
fn new_stdio_owner_cleans_up_existing_canonical_row() {
    let h = Hcom::new();
    fixture(&h);
    let (old, first) = start(&h);
    let (new, second) = start(&h);
    close(new);
    let after_new_exit = current_owner(&h);
    close(old);

    assert_eq!(first["name"], second["name"]);
    assert_ne!(first["token"], second["token"]);
    assert_eq!(second["stored_token"], second["token"]);
    assert!(
        after_new_exit.is_none(),
        "replacement supervisor left its canonical row: {after_new_exit:?}"
    );
}

#[test]
fn old_stdio_owner_exit_preserves_rebound_canonical_row() {
    let h = Hcom::new();
    fixture(&h);
    let (old, first) = start(&h);
    let (new, second) = start(&h);
    close(old);
    let after_old_exit = current_owner(&h);
    close(new);
    let after_new_exit = current_owner(&h);

    assert_eq!(first["name"], second["name"]);
    assert_ne!(first["token"], second["token"]);
    assert_eq!(
        after_old_exit,
        Some((
            second["name"].as_str().unwrap().to_string(),
            second["pid"].as_i64().unwrap(),
            second["token"].as_str().unwrap().to_string(),
        )),
        "old supervisor finalized the replacement's row"
    );
    assert!(after_new_exit.is_none());
}
