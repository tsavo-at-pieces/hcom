//! Pipe clients must retain their transport through hcom's native registration.
#![cfg(unix)]

mod support;

use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::process::Stdio;
use std::time::{Duration, Instant};
use support::Hcom;

fn assert_no_registered_instances(h: &Hcom) {
    // Inspect before `hcom list`, whose stale-process cleanup could otherwise
    // conceal a missing supervisor finalizer.
    let db = rusqlite::Connection::open(h.hcom_dir.join("hcom.db")).unwrap();
    let count: i64 = db
        .query_row("SELECT COUNT(*) FROM instances", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 0, "exited native process left a registered instance");
}

/// Make `script` the only `claude` the stdio launch can find.
fn install_fake_claude(h: &Hcom, script: &str) {
    let bin = h.root_path().join("pipe-tools");
    fs::create_dir(&bin).unwrap();
    let tool = bin.join("claude");
    fs::write(&tool, script).unwrap();
    fs::set_permissions(&tool, fs::Permissions::from_mode(0o755)).unwrap();
    h.set_launch_env(
        "PATH",
        &format!("{}:{}", bin.display(), std::env::var("PATH").unwrap()),
    );
}

/// `(status, context, detail)` of the only registered instance.
fn only_status(h: &Hcom) -> Option<(String, String, String)> {
    use rusqlite::OptionalExtension;
    let db = rusqlite::Connection::open(h.hcom_dir.join("hcom.db")).unwrap();
    db.query_row(
        "SELECT status, status_context, status_detail FROM instances",
        [],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?.unwrap_or_default(),
                row.get::<_, Option<String>>(2)?.unwrap_or_default(),
            ))
        },
    )
    .optional()
    .unwrap()
}

fn status_contexts(h: &Hcom) -> Vec<String> {
    let db = rusqlite::Connection::open(h.hcom_dir.join("hcom.db")).unwrap();
    let mut statement = db
        .prepare(
            "SELECT json_extract(data, '$.context') FROM events WHERE type = 'status' ORDER BY id",
        )
        .unwrap();
    statement
        .query_map([], |row| row.get::<_, Option<String>>(0))
        .unwrap()
        .map(|context| context.unwrap().unwrap_or_default())
        .collect()
}

/// A stream-json Claude stand-in. Hooks run through the hcom binary in the
/// order native Claude runs them. Turn 2 is cancelled by an interrupt control
/// request: like native Claude, it then skips Stop and only emits its result.
const SDK_CLAUDE: &str = r##"#!/bin/sh
if [ "$1" = --version ]; then printf '2.1.309 (Claude Code)\n'; exit 0; fi
sid=4b8c2f0e-6d3a-4f1b-9e2c-7a5d8c1b3e60
emit() { printf '%s\n' "$1" >> "$FAKE_STDOUT_COPY"; printf '%s\n' "$1"; }
hook() { printf '%s' "$2" | "$FAKE_HCOM" "$1" > /dev/null; }
: > "$PWD/transcript.jsonl"
hook sessionstart "{\"session_id\":\"$sid\",\"transcript_path\":\"$PWD/transcript.jsonl\",\"cwd\":\"$PWD\",\"hook_event_name\":\"SessionStart\",\"source\":\"startup\"}"
emit "{\"type\":\"system\",\"subtype\":\"init\",\"session_id\":\"$sid\"}"
turn=0
while IFS= read -r line; do
  case "$line" in *'"type":"user"'*) ;; *) continue ;; esac
  turn=$((turn + 1))
  hook userpromptsubmit "{\"session_id\":\"$sid\",\"hook_event_name\":\"UserPromptSubmit\",\"prompt\":\"turn $turn\"}"
  emit "{\"type\":\"assistant\",\"session_id\":\"$sid\",\"message\":{\"content\":[{\"type\":\"text\",\"text\":\"turn $turn héllo ✓ 🎉\"}]}}"
  if [ "$turn" = 1 ]; then
    big=$(head -c 200000 /dev/zero | tr '\0' x)
    emit "{\"type\":\"user\",\"session_id\":\"$sid\",\"message\":{\"content\":[{\"type\":\"tool_result\",\"content\":\"$big\"}]}}"
  fi
  if [ "$turn" = 2 ]; then
    IFS= read -r line
    emit "{\"type\":\"control_response\",\"response\":{\"subtype\":\"success\",\"request_id\":\"stop-2\"}}"
    emit "{\"type\":\"result\",\"subtype\":\"error_during_execution\",\"is_error\":true,\"session_id\":\"$sid\",\"terminal_reason\":\"aborted_streaming\",\"queued_turn_count\":0}"
  else
    hook poll "{\"session_id\":\"$sid\",\"hook_event_name\":\"Stop\",\"stop_hook_active\":false}"
    emit "{\"type\":\"result\",\"subtype\":\"success\",\"is_error\":false,\"session_id\":\"$sid\",\"terminal_reason\":\"completed\",\"queued_turn_count\":0,\"result\":\"done $turn\"}"
  fi
done
printf 'crlf\r\nlast frame without newline' >> "$FAKE_STDOUT_COPY"
printf 'crlf\r\nlast frame without newline'
exit 7
"##;

#[test]
fn claude_stdio_cancelled_turn_becomes_listening_and_stdout_is_byte_exact() {
    use serde_json::{Value, json};
    use std::sync::mpsc;

    let h = Hcom::new();
    install_fake_claude(&h, SDK_CLAUDE);
    let copy = h.root_path().join("native-stdout");
    h.set_launch_envs(&[
        ("FAKE_STDOUT_COPY", copy.to_str().unwrap()),
        ("FAKE_HCOM", env!("CARGO_BIN_EXE_hcom")),
    ]);
    let mut child = h
        .cmd()
        .args([
            "claude",
            "--stdio",
            "--go",
            "--",
            "-p",
            "--input-format",
            "stream-json",
            "--output-format",
            "stream-json",
            "--verbose",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = child.stdout.take().unwrap();
    let (send_line, lines) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        let mut received = Vec::new();
        let mut chunk = [0u8; 8192];
        let mut line_start = 0;
        loop {
            let read = stdout.read(&mut chunk).unwrap();
            if read == 0 {
                return received;
            }
            received.extend_from_slice(&chunk[..read]);
            while let Some(end) = received[line_start..].iter().position(|&b| b == b'\n') {
                let line = String::from_utf8_lossy(&received[line_start..line_start + end]);
                let _ = send_line.send(line.into_owned());
                line_start += end + 1;
            }
        }
    });
    let next = |kind: &str| -> Value {
        loop {
            let line = lines
                .recv_timeout(Duration::from_secs(60))
                .unwrap_or_else(|_| panic!("no {kind} frame\n{}", h.diagnostics()));
            if let Ok(frame) = serde_json::from_str::<Value>(&line)
                && frame["type"] == kind
            {
                return frame;
            }
        }
    };
    let send = |stdin: &mut std::process::ChildStdin, value: Value| {
        writeln!(stdin, "{value}").unwrap();
        stdin.flush().unwrap();
    };
    let user = |text: &str| json!({"type": "user", "message": {"role": "user", "content": text}, "parent_tool_use_id": null});
    let idle = Some(("listening".to_string(), String::new(), String::new()));

    next("system");
    send(&mut stdin, user("one"));
    assert_eq!(next("result")["terminal_reason"], "completed");
    assert_eq!(only_status(&h), idle, "Stop records a completed turn");

    send(&mut stdin, user("two"));
    next("assistant");
    assert_eq!(only_status(&h).unwrap().1, "prompt");
    send(
        &mut stdin,
        json!({"type": "control_request", "request_id": "stop-2", "request": {"subtype": "interrupt"}}),
    );
    assert_eq!(next("result")["terminal_reason"], "aborted_streaming");
    let interrupted = Some((
        "listening".to_string(),
        "interrupted".to_string(),
        "aborted_streaming".to_string(),
    ));
    h.eventually(
        "the cancelled turn to become listening",
        Duration::from_secs(10),
        || Ok((only_status(&h) == interrupted).then_some(())),
    );
    assert!(
        child.try_wait().unwrap().is_none(),
        "the native process outlives a cancelled turn"
    );

    send(&mut stdin, user("three"));
    assert_eq!(next("result")["terminal_reason"], "completed");
    assert_eq!(only_status(&h), idle);

    drop(stdin);
    assert_eq!(child.wait().unwrap().code(), Some(7));
    let received = reader.join().unwrap();
    assert!(
        received == fs::read(&copy).unwrap(),
        "stdout differs from the native output"
    );
    assert_no_registered_instances(&h);
    let contexts = status_contexts(&h);
    let first_prompt = contexts
        .iter()
        .position(|context| context == "prompt")
        .unwrap();
    assert_eq!(
        contexts[first_prompt..],
        [
            "prompt",
            "",
            "prompt",
            "interrupted",
            "prompt",
            "",
            "exit:7"
        ],
        "{contexts:?}"
    );
}

#[test]
fn claude_stdio_closed_client_stdout_reaches_the_native_process() {
    let h = Hcom::new();
    install_fake_claude(
        &h,
        r#"#!/bin/sh
if [ "$1" = --version ]; then printf '2.1.309 (Claude Code)\n'; exit 0; fi
trap '' PIPE
printf '{"type":"system","subtype":"init"}\n'
i=0
while [ "$i" -lt 600 ]; do
  sleep 0.05
  printf '{"type":"keep_alive"}\n' | cat || exit 41
  i=$((i + 1))
done
exit 42
"#,
    );
    let mut child = h
        .cmd()
        .args([
            "claude",
            "--stdio",
            "--go",
            "--",
            "-p",
            "--output-format=stream-json",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());
    let mut line = String::new();
    stdout.read_line(&mut line).unwrap();
    assert_eq!(line, "{\"type\":\"system\",\"subtype\":\"init\"}\n");
    drop(stdout);
    // hcom survives its broken stdout; the child sees EPIPE and exits itself.
    let output = child.wait_with_output().unwrap();
    assert_eq!(output.status.code(), Some(41), "{output:?}");
    assert_no_registered_instances(&h);
}

#[test]
fn claude_stdio_delivers_last_frames_without_waiting_for_descendants() {
    let h = Hcom::new();
    install_fake_claude(
        &h,
        r#"#!/bin/sh
if [ "$1" = --version ]; then printf '2.1.309 (Claude Code)\n'; exit 0; fi
sleep 30 2>/dev/null &
printf '%s\n' "$!" > "$FAKE_DESCENDANT_PID"
printf '{"type":"result","subtype":"success","is_error":false}'
exit 3
"#,
    );
    let descendant = h.root_path().join("descendant.pid");
    h.set_launch_env("FAKE_DESCENDANT_PID", descendant.to_str().unwrap());
    let started = Instant::now();
    let output = h
        .cmd()
        .args([
            "claude",
            "--stdio",
            "--go",
            "--",
            "-p",
            "--output-format",
            "stream-json",
        ])
        .stdin(Stdio::null())
        .output()
        .unwrap();
    let elapsed = started.elapsed();
    let pid: i32 = fs::read_to_string(&descendant)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    unsafe { libc::kill(pid, libc::SIGKILL) };
    assert_eq!(output.status.code(), Some(3), "{output:?}");
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        "{\"type\":\"result\",\"subtype\":\"success\",\"is_error\":false}"
    );
    // The descendant holds the copied pipe for 30s; hcom stops waiting first.
    assert!(elapsed < Duration::from_secs(20), "{elapsed:?}");
    assert_no_registered_instances(&h);
}

#[test]
fn claude_stdio_listening_row_stays_live_while_the_child_runs() {
    let h = Hcom::new();
    install_fake_claude(
        &h,
        r##"#!/bin/sh
if [ "$1" = --version ]; then printf '2.1.309 (Claude Code)\n'; exit 0; fi
sid=7d1e5a2b-3c4f-4a6b-8d9e-0f1a2b3c4d5e
hook() { printf '%s' "$2" | "$FAKE_HCOM" "$1" > /dev/null; }
: > "$PWD/transcript.jsonl"
hook sessionstart "{\"session_id\":\"$sid\",\"transcript_path\":\"$PWD/transcript.jsonl\",\"cwd\":\"$PWD\",\"hook_event_name\":\"SessionStart\",\"source\":\"startup\"}"
hook userpromptsubmit "{\"session_id\":\"$sid\",\"hook_event_name\":\"UserPromptSubmit\",\"prompt\":\"one\"}"
hook poll "{\"session_id\":\"$sid\",\"hook_event_name\":\"Stop\",\"stop_hook_active\":false}"
printf 'idle\n'
IFS= read -r line
exit 0
"##,
    );
    h.set_launch_env("FAKE_HCOM", env!("CARGO_BIN_EXE_hcom"));
    // Text output has no stdout tap; the heartbeat covers every stdio launch.
    let mut child = h
        .cmd()
        .args(["claude", "--stdio", "--go", "--", "-p"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let stdin = child.stdin.take().unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());
    let mut line = String::new();
    stdout.read_line(&mut line).unwrap();
    assert_eq!(line, "idle\n");
    assert_eq!(only_status(&h).unwrap().0, "listening");
    // Without a heartbeat, `listening` without TCP reads as stale after 10s.
    std::thread::sleep(Duration::from_secs(12));
    let rows = h.list_json().unwrap();
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0]["status"], "listening", "{rows:?}");
    drop(stdin);
    assert!(child.wait().unwrap().success());
    assert_no_registered_instances(&h);
}

#[test]
fn claude_stdio_keeps_clean_channels_and_native_exit() {
    let h = Hcom::new();
    let bin = h.root_path().join("pipe-tools");
    fs::create_dir(&bin).unwrap();
    let tool = bin.join("claude");
    fs::write(&tool, r#"#!/bin/sh
if [ "$1" = --version ]; then printf '2.1.309 (Claude Code)\n'; exit 0; fi
if [ -t 0 ] || [ -t 1 ]; then printf 'unexpected PTY\n'; exit 31; fi
printf '{"launched":"%s","tool":"%s","stdio":"%s","pty":"%s"}\n' "$HCOM_LAUNCHED" "$HCOM_TOOL" "$HCOM_STDIO_MODE" "$HCOM_PTY_MODE"
IFS= read -r line
printf '%s\n' "$line"
printf 'native diagnostic\n' >&2
exit 23
"#).unwrap();
    fs::set_permissions(&tool, fs::Permissions::from_mode(0o755)).unwrap();
    let path = format!("{}:{}", bin.display(), std::env::var("PATH").unwrap());
    h.set_launch_env("PATH", &path);
    let mut child = h
        .cmd()
        .args([
            "claude",
            "--stdio",
            "--go",
            "--",
            "-p",
            "--input-format",
            "stream-json",
            "--output-format",
            "stream-json",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let _ = child
        .stdin
        .take()
        .unwrap()
        .write_all(b"{\"type\":\"user\"}\n");
    let output = child.wait_with_output().unwrap();
    assert_eq!(output.status.code(), Some(23), "{output:?}");
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        "{\"launched\":\"1\",\"tool\":\"claude\",\"stdio\":\"1\",\"pty\":\"0\"}\n{\"type\":\"user\"}\n"
    );
    assert_eq!(
        String::from_utf8(output.stderr).unwrap(),
        "native diagnostic\n"
    );
    assert!(
        !h.hcom_dir.join(".tmp/launch").exists(),
        "stdio must not serialize a shell runner"
    );
    assert_no_registered_instances(&h);
}

#[test]
fn claude_stdio_forwards_cancellation_and_waits_for_native_exit() {
    let h = Hcom::new();
    let bin = h.root_path().join("pipe-tools");
    fs::create_dir(&bin).unwrap();
    let tool = bin.join("claude");
    fs::write(
        &tool,
        r#"#!/bin/sh
if [ "$1" = --version ]; then printf '2.1.309 (Claude Code)\n'; exit 0; fi
trap 'printf "cancelled\n" >&2; exit 37' TERM
printf 'ready\n'
while IFS= read -r line; do :; done
exit 32
"#,
    )
    .unwrap();
    fs::set_permissions(&tool, fs::Permissions::from_mode(0o755)).unwrap();
    h.set_launch_env(
        "PATH",
        &format!("{}:{}", bin.display(), std::env::var("PATH").unwrap()),
    );
    let mut child = h
        .cmd()
        .args(["claude", "--stdio", "--go", "--", "-p"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());
    let mut line = String::new();
    stdout.read_line(&mut line).unwrap();
    assert_eq!(line, "ready\n");
    // Keep the SDK input channel open; cancellation must reach the native
    // process, not merely close its pipe as wait_with_output would.
    let _stdin = child.stdin.take().unwrap();
    assert_eq!(unsafe { libc::kill(child.id() as i32, libc::SIGTERM) }, 0);
    let output = child.wait_with_output().unwrap();
    assert_eq!(output.status.code(), Some(37));
    assert_eq!(String::from_utf8(output.stderr).unwrap(), "cancelled\n");
    assert_no_registered_instances(&h);
}

/// A stub Claude for the transport tests: answers `--version`, then runs `body`.
fn pipe_tool(h: &Hcom, body: &str) -> std::path::PathBuf {
    let bin = h.root_path().join("pipe-tools");
    fs::create_dir_all(&bin).unwrap();
    let tool = bin.join("claude");
    fs::write(
        &tool,
        format!(
            "#!/bin/sh\nif [ \"$1\" = --version ]; then printf '2.1.309 (Claude Code)\\n'; exit 0; fi\n{body}\n"
        ),
    )
    .unwrap();
    fs::set_permissions(&tool, fs::Permissions::from_mode(0o755)).unwrap();
    h.set_launch_env(
        "PATH",
        &format!("{}:{}", bin.display(), std::env::var("PATH").unwrap()),
    );
    tool
}

/// A launch from a Claude Bash tool inherits that parent's instance state. The
/// stdio child must not: CHILD_SESSION would turn off its own transcript (and
/// so resume, fork and lineage), and the messaging socket and token belong to
/// the parent. The runner scripts of the other modes `unset` these before exec.
#[test]
fn claude_stdio_child_never_inherits_a_parent_claude_instance_state() {
    let h = Hcom::new();
    pipe_tool(
        &h,
        r#"printf '%s|%s|%s\n' "${CLAUDE_CODE_CHILD_SESSION-unset}" "${CLAUDE_CODE_MESSAGING_SOCKET-unset}" "${CLAUDE_CODE_MESSAGING_TOKEN-unset}"
exit 0"#,
    );
    // One value arrives through the launch-environment overlay, two through the
    // supervising process's own environment: both reach the map stdio passes.
    h.set_launch_env("CLAUDE_CODE_CHILD_SESSION", "1");
    let output = h
        .cmd()
        .env("CLAUDE_CODE_MESSAGING_SOCKET", "/parent/messaging.sock")
        .env("CLAUDE_CODE_MESSAGING_TOKEN", "parent-token")
        .args(["claude", "--stdio", "--go", "--", "-p"])
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        "unset|unset|unset\n"
    );
    assert_no_registered_instances(&h);
}

/// The install gate accepts Claude from PATH or from its per-user install
/// directories. The spawn must run what the gate validated: a fallback-only
/// install launches, as it does through `hcom claude -p`.
#[test]
fn claude_stdio_runs_the_validated_executable_of_a_fallback_dir_install() {
    let h = Hcom::new();
    let local = h.home.join(".claude/local");
    fs::create_dir_all(&local).unwrap();
    let tool = local.join("claude");
    fs::write(
        &tool,
        "#!/bin/sh\nif [ \"$1\" = --version ]; then printf '2.1.309 (Claude Code)\\n'; exit 0; fi\nprintf 'fallback install\\n'\nexit 0\n",
    )
    .unwrap();
    fs::set_permissions(&tool, fs::Permissions::from_mode(0o755)).unwrap();
    // No claude on PATH: hcom's own directory and the system shells only. The
    // premise is stated, so a host with a system-wide claude fails here, not
    // by launching that CLI.
    for system in ["/usr/bin/claude", "/bin/claude"] {
        assert!(
            !std::path::Path::new(system).exists(),
            "{system} exists: this host cannot prove a fallback-only install"
        );
    }
    let hcom_dir = std::path::Path::new(env!("CARGO_BIN_EXE_hcom"))
        .parent()
        .unwrap();
    h.set_launch_env("PATH", &format!("{}:/usr/bin:/bin", hcom_dir.display()));
    let output = h
        .cmd()
        .args(["claude", "--stdio", "--go", "--", "-p"])
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        "fallback install\n"
    );
    assert_no_registered_instances(&h);
}

/// The install gate searches PATH relative to hcom's own directory; the child
/// changes to its working directory before exec. A relative PATH entry must
/// still run the validated file, not a same-named file under `--dir`.
#[test]
fn claude_stdio_runs_the_validated_file_when_path_is_relative_and_dir_differs() {
    let h = Hcom::new();
    let script = |text: &str| {
        format!(
            "#!/bin/sh\nif [ \"$1\" = --version ]; then printf '2.1.309 (Claude Code)\\n'; exit 0; fi\nprintf '{text}\\n'\nexit 0\n"
        )
    };
    let validated = h.workspace.join("toolbin").join("claude");
    let decoy_dir = h.workspace.join("other");
    let decoy = decoy_dir.join("toolbin").join("claude");
    for (path, text) in [(&validated, "validated"), (&decoy, "decoy under --dir")] {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, script(text)).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
    }
    let hcom_dir = std::path::Path::new(env!("CARGO_BIN_EXE_hcom"))
        .parent()
        .unwrap();
    // `toolbin` is relative: it names the validated file from hcom's directory
    // (the fixture workspace) and the decoy from the child's.
    h.set_launch_env(
        "PATH",
        &format!("toolbin:{}:/usr/bin:/bin", hcom_dir.display()),
    );
    let output = h
        .cmd()
        .args([
            "claude",
            "--stdio",
            "--go",
            "--dir",
            decoy_dir.to_str().unwrap(),
            "--",
            "-p",
        ])
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert_eq!(String::from_utf8(output.stdout).unwrap(), "validated\n");
    assert_no_registered_instances(&h);
}

/// The batch and startup-failure events of a stdio launch name the launch's
/// own launcher, tag and batch. The supervising hcom process may run inside
/// another launch (an agent's Bash tool); that launch's `HCOM_LAUNCHED_BY` and
/// batch must never be reported, nor notified, for the child it supervises.
#[test]
fn claude_stdio_attributes_its_batch_and_startup_failure_to_its_own_launcher() {
    for agent in [true, false] {
        let h = Hcom::new();
        pipe_tool(&h, "exit 9");
        let launcher = agent.then(|| h.start_with_process_id("proc-boss"));
        let mut command = h.cmd();
        if agent {
            command.env("HCOM_PROCESS_ID", "proc-boss");
        }
        // What the supervisor itself inherited from the launch it runs inside.
        command
            .env("HCOM_LAUNCHED_BY", "stale-launcher")
            .env("HCOM_LAUNCH_BATCH_ID", "stale-batch");
        let output = command
            .args(["claude", "--tag", "acc", "--stdio", "--go", "--", "-p"])
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(9), "agent={agent}: {output:?}");

        let db = rusqlite::Connection::open(h.hcom_dir.join("hcom.db")).unwrap();
        let rows: Vec<(String, serde_json::Value)> = db
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
        assert_eq!(rows.len(), 2, "agent={agent}: {rows:?}");
        let (launched_instance, launched) = &rows[0];
        let (failed_instance, failed) = &rows[1];
        assert_eq!(launched["action"], "batch_launched", "{rows:?}");
        assert_eq!(failed["action"], "launch_failed", "{rows:?}");
        let by = launched["by"].as_str().unwrap().to_string();
        match &launcher {
            Some(name) => assert_eq!(&by, name, "{rows:?}"),
            None => assert!(by != "unknown" && by != "stale-launcher", "{rows:?}"),
        }
        assert_eq!(
            launched_instance, &by,
            "the batch is filed under its launcher"
        );
        assert_eq!(launched["tag"], "acc", "{rows:?}");
        assert_eq!(launched["stdio"], true, "{rows:?}");
        let batch = launched["batch_id"].as_str().unwrap().to_string();
        assert_ne!(batch, "stale-batch");
        assert!(!failed_instance.is_empty(), "{rows:?}");
        assert_eq!(launched["instances"], serde_json::json!([failed_instance]));
        assert_eq!(failed["by"], by, "{rows:?}");
        assert_eq!(failed["batch_id"], batch, "{rows:?}");
        assert_eq!(failed["reason"], "exited_before_ready", "{rows:?}");
        // The failure notice reaches the launcher of this launch, about this batch.
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
                "@{by} Launch failed: {failed_instance}: Claude stdio exited with status 9 before SessionStart (batch: {batch})"
            )
        );
        let remaining: i64 = db
            .query_row(
                "SELECT COUNT(*) FROM instances WHERE name != ?1",
                [launcher.as_deref().unwrap_or("")],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(remaining, 0, "the failed child row was cleaned up");
    }
}

/// Per-run hooks name the hcom image itself. Invoked through a symlink, the
/// published command is the canonical image path, so retargeting that link
/// later cannot move a live session's hooks to another implementation.
/// Discriminating only where current_exe() reports the invoked link (macOS;
/// Linux pre-resolves /proc/self/exe): the unit test on
/// runtime_env::pinned_image guards the pin on every Unix.
#[test]
fn claude_stdio_hooks_name_the_image_behind_an_invoked_symlink() {
    let h = Hcom::new();
    pipe_tool(&h, "printf '%s\\n' \"$@\"\nexit 0");
    let image = fs::canonicalize(env!("CARGO_BIN_EXE_hcom")).unwrap();
    let link_dir = h.root_path().join("link-bin");
    fs::create_dir(&link_dir).unwrap();
    let link = link_dir.join("hcom");
    std::os::unix::fs::symlink(&image, &link).unwrap();
    let output = h
        .external_cmd(&link)
        .args(["claude", "--stdio", "--go", "--", "-p"])
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let argv: Vec<String> = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(str::to_string)
        .collect();
    let settings = argv
        .iter()
        .position(|arg| arg == "--settings")
        .map(|index| argv[index + 1].clone())
        .expect("per-run hooks are passed through --settings");
    let value: serde_json::Value = if settings.trim_start().starts_with('{') {
        serde_json::from_str(&settings).unwrap()
    } else {
        serde_json::from_str(&fs::read_to_string(&settings).unwrap()).unwrap()
    };
    let commands: Vec<String> = value["hooks"]
        .as_object()
        .unwrap()
        .values()
        .flat_map(|groups| groups.as_array().unwrap().iter())
        .flat_map(|group| group["hooks"].as_array().unwrap().iter())
        .map(|hook| hook["command"].as_str().unwrap().to_string())
        .collect();
    assert!(!commands.is_empty(), "{value}");
    for command in &commands {
        assert_eq!(std::path::Path::new(command), image.as_path(), "{value}");
    }
    assert!(
        commands
            .iter()
            .all(|command| command != link.to_str().unwrap())
    );
    assert_no_registered_instances(&h);
}

#[test]
fn stdio_rejects_incompatible_launches_without_stdout() {
    for args in [
        vec!["claude", "--stdio"],
        vec!["codex", "--stdio", "-p"],
        vec!["2", "claude", "--stdio", "-p"],
        vec!["claude", "--stdio", "--headless", "-p"],
        vec!["claude", "--stdio", "--json", "-p"],
        vec!["claude", "--stdio", "--device", "peer", "-p"],
        vec!["claude", "--stdio", "--terminal", "here", "-p"],
        vec!["claude", "--stdio", "--no-run-here", "-p"],
        vec!["r", "nova", "--stdio"],
    ] {
        let h = Hcom::new();
        let (code, stdout, stderr) = h.run(args.clone());
        assert_ne!(code, 0, "{args:?}");
        assert!(stdout.is_empty(), "{args:?}: {stdout}");
        assert!(stderr.contains("--stdio"), "{args:?}: {stderr}");
    }
}

/// The real native CLI speaks the same newline JSON control protocol used by
/// the Agent SDK. No hosted model is contacted: all turns use the local mock.
#[test]
#[ignore = "requires the real Claude CLI installed by scripts/install-mock-tools.sh"]
fn real_claude_stdio_two_turns_permission_channel_and_eof() {
    use serde_json::{Value, json};
    use std::sync::mpsc;
    use std::time::Duration;
    use support::claude_mock::{ClaudeCase, MODEL, claude_text, claude_tool_use, latest_user_turn};
    use support::mock_http::{MockHttp, Reply};
    use support::real_tool::ToolCase;

    let h = Hcom::new();
    let proof = h.workspace.join("approved.txt");
    // The deployed SDK build coexists with stock hcom. Its hooks must use the
    // exact build that understands stdio, irrespective of PATH resolution.
    let ordinary_bin = h.root_path().join("ordinary-bin");
    fs::create_dir(&ordinary_bin).unwrap();
    let ordinary_hcom = ordinary_bin.join("hcom");
    fs::write(
        &ordinary_hcom,
        "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$SDK_TEST_OLD_CALLS_FILE\"\nexit 86\n",
    )
    .unwrap();
    fs::set_permissions(&ordinary_hcom, fs::Permissions::from_mode(0o755)).unwrap();
    let old_calls = h.root_path().join("old-hcom-calls");
    h.set_launch_env("SDK_TEST_OLD_CALLS_FILE", old_calls.to_str().unwrap());
    h.set_launch_env(
        "PATH",
        &format!(
            "{}:{}:{}",
            ordinary_bin.display(),
            support::pins::mock_tools_bin().display(),
            std::env::var("PATH").unwrap()
        ),
    );
    let command = format!("printf approved > '{}'", proof.display());
    let mock = MockHttp::start(move |request| {
        if request.method.eq_ignore_ascii_case("HEAD") {
            return Reply::Empty(200);
        }
        if request.path.contains("count_tokens") {
            return Reply::Json(json!({"input_tokens": 1}).to_string());
        }
        if !request.path.contains("/v1/messages") {
            return Reply::Status(404);
        }
        let (tool, text) = latest_user_turn(&request.body).unwrap_or_default();
        if tool.as_deref() == Some("toolu_stdio") {
            Reply::Sse(claude_text("msg_one", "STDIO_ONE_DONE"))
        } else if text.contains("STDIO_ONE") {
            Reply::Sse(claude_tool_use(
                "msg_permission",
                "toolu_stdio",
                "Bash",
                &json!({"command": command, "description": "write permission proof"}),
            ))
        } else if text.contains("STDIO_TWO") {
            Reply::Sse(claude_text("msg_two", "STDIO_TWO_DONE"))
        } else {
            Reply::Status(500)
        }
    })
    .unwrap();
    ClaudeCase.prepare(&h, &ClaudeCase.provider_base_url(mock.port()));
    h.set_launch_env("CLAUDE_CODE_ENTRYPOINT", "sdk-ts");
    let mut child = h
        .cmd()
        .args([
            "claude",
            "--stdio",
            "--go",
            "--",
            "--print",
            "--input-format",
            "stream-json",
            "--output-format",
            "stream-json",
            "--verbose",
            "--permission-prompt-tool",
            "stdio",
            "--permission-mode",
            "manual",
            "--model",
            MODEL,
            "--setting-sources",
            "user",
            "--tools",
            "Bash",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    let stdout = child.stdout.take().unwrap();
    let stderr = child.stderr.take().unwrap();
    let errors = std::thread::spawn(move || {
        use std::io::Read;
        let mut text = String::new();
        let mut reader = stderr;
        reader.read_to_string(&mut text).unwrap();
        text
    });
    let (send, receive) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            if send.send(line.unwrap()).is_err() {
                break;
            }
        }
    });
    let send_json = |input: &mut std::process::ChildStdin, value: Value| {
        writeln!(input, "{value}").unwrap();
        input.flush().unwrap();
    };
    send_json(
        &mut stdin,
        json!({"type": "control_request", "request_id": "init", "request": {"subtype": "initialize"}}),
    );
    let mut session_id = None;
    let mut permission_requests = 0;
    for turn in ["STDIO_ONE", "STDIO_TWO"] {
        send_json(
            &mut stdin,
            json!({"type": "user", "message": {"role": "user", "content": turn}, "parent_tool_use_id": null}),
        );
        loop {
            let line = receive
                .recv_timeout(Duration::from_secs(90))
                .expect("native SDK frame (turn must not wait in hcom Stop)");
            let frame: Value =
                serde_json::from_str(&line).expect("stdout contains only native JSON frames");
            if frame["type"] == "control_request" && frame["request"]["subtype"] == "can_use_tool" {
                permission_requests += 1;
                assert_eq!(frame["request"]["tool_name"], "Bash");
                send_json(
                    &mut stdin,
                    json!({"type": "control_response", "response": {
                        "subtype": "success", "request_id": frame["request_id"],
                        "response": {"behavior": "allow", "updatedInput": frame["request"]["input"]}
                    }}),
                );
            }
            if frame["type"] == "result" {
                assert_eq!(frame["is_error"], false, "{frame}");
                assert!(
                    frame["result"]
                        .as_str()
                        .unwrap()
                        .contains(&format!("{turn}_DONE")),
                    "{frame}"
                );
                let id = frame["session_id"].as_str().unwrap().to_string();
                if let Some(ref expected) = session_id {
                    assert_eq!(expected, &id);
                }
                session_id = Some(id);
                break;
            }
        }
        let rows = h.list_json().unwrap();
        assert_eq!(rows.len(), 1, "{rows:?}");
        assert_eq!(rows[0]["session_id"].as_str(), session_id.as_deref());
        assert_eq!(rows[0]["status"], "listening", "{rows:?}");
    }
    assert_eq!(permission_requests, 1);
    assert_eq!(fs::read_to_string(proof).unwrap(), "approved");
    drop(stdin);
    assert!(child.wait().unwrap().success());
    reader.join().unwrap();
    let stderr = errors.join().unwrap();
    assert!(!stderr.contains("requires --print"), "{stderr}");
    assert_no_registered_instances(&h);
    let calls = fs::read_to_string(old_calls).unwrap_or_default();
    assert!(
        calls.lines().all(|line| line == "relay push"),
        "native hooks used ordinary hcom from PATH: {calls}"
    );
}
