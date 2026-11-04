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
/// order native Claude runs them, and session state frames follow them as
/// for a client that opted into them. Turn 2 is cancelled by an interrupt
/// control request: like native Claude, it then skips Stop and only emits its
/// result.
const SDK_CLAUDE: &str = r##"#!/bin/sh
if [ "$1" = --version ]; then printf '2.1.309 (Claude Code)\n'; exit 0; fi
sid=4b8c2f0e-6d3a-4f1b-9e2c-7a5d8c1b3e60
emit() { printf '%s\n' "$1" >> "$FAKE_STDOUT_COPY"; printf '%s\n' "$1"; }
hook() { printf '%s' "$2" | "$FAKE_HCOM" "$1" > /dev/null; }
state() { emit "{\"type\":\"system\",\"subtype\":\"session_state_changed\",\"state\":\"$1\",\"session_id\":\"$sid\"}"; }
: > "$PWD/transcript.jsonl"
hook sessionstart "{\"session_id\":\"$sid\",\"transcript_path\":\"$PWD/transcript.jsonl\",\"cwd\":\"$PWD\",\"hook_event_name\":\"SessionStart\",\"source\":\"startup\"}"
emit "{\"type\":\"system\",\"subtype\":\"init\",\"session_id\":\"$sid\"}"
turn=0
while IFS= read -r line; do
  case "$line" in *'"type":"user"'*) ;; *) continue ;; esac
  turn=$((turn + 1))
  hook userpromptsubmit "{\"session_id\":\"$sid\",\"hook_event_name\":\"UserPromptSubmit\",\"prompt\":\"turn $turn\"}"
  state running
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
  state idle
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

/// Kills a process the test started, however the test ends.
struct KillOnDrop(i32);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        unsafe { libc::kill(self.0, libc::SIGKILL) };
    }
}

#[test]
fn claude_stdio_delivers_last_frames_without_waiting_for_descendants() {
    let h = Hcom::new();
    install_fake_claude(
        &h,
        r#"#!/bin/sh
if [ "$1" = --version ]; then printf '2.1.309 (Claude Code)\n'; exit 0; fi
sleep 600 2>/dev/null &
printf '%s\n' "$!" > "$FAKE_DESCENDANT_PID"
printf '{"type":"result","subtype":"success","is_error":false}'
exit 3
"#,
    );
    let descendant = h.root_path().join("descendant.pid");
    h.set_launch_env("FAKE_DESCENDANT_PID", descendant.to_str().unwrap());
    let mut hcom = h
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
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdout = hcom.stdout.take().unwrap();
    let reader = std::thread::spawn(move || {
        let mut received = String::new();
        stdout.read_to_string(&mut received).unwrap();
        received
    });
    let descendant = KillOnDrop(shim_pid(&h, &descendant));
    // The descendant holds the copied pipe for 10 minutes. hcom stops
    // copying once it has stayed empty for 5 s after the child exits; how
    // long the launch took before that is up to the host.
    let status = wait_for_hcom(&h, &mut hcom, Duration::from_secs(120));
    assert_eq!(
        unsafe { libc::kill(descendant.0, 0) },
        0,
        "hcom waited for the descendant to close the pipe"
    );
    drop(descendant);
    assert_eq!(status.code(), Some(3));
    assert_eq!(
        reader.join().unwrap(),
        "{\"type\":\"result\",\"subtype\":\"success\",\"is_error\":false}"
    );
    assert_no_registered_instances(&h);
}

/// A Claude that completes one turn through its hooks, prints `idle` and runs
/// no further hook until stdin closes.
const IDLE_CLAUDE: &str = r##"#!/bin/sh
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
"##;

/// `last_stop`, the heartbeat column, of the only registered instance.
fn only_last_stop(h: &Hcom) -> i64 {
    let db = rusqlite::Connection::open(h.hcom_dir.join("hcom.db")).unwrap();
    db.query_row("SELECT last_stop FROM instances", [], |row| row.get(0))
        .unwrap()
}

#[test]
fn claude_stdio_listening_row_stays_live_while_the_child_runs() {
    let h = Hcom::new();
    install_fake_claude(&h, IDLE_CLAUDE);
    h.set_launch_env("FAKE_HCOM", env!("CARGO_BIN_EXE_hcom"));
    // The heartbeat belongs to the stream-json tap.
    let mut child = h
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

/// Only the stream-json tap refreshes the heartbeat: any other output format
/// keeps the inherited stdout and the row's liveness as Stop left it.
#[test]
fn claude_stdio_text_output_starts_no_heartbeat() {
    let h = Hcom::new();
    install_fake_claude(&h, IDLE_CLAUDE);
    h.set_launch_env("FAKE_HCOM", env!("CARGO_BIN_EXE_hcom"));
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
    let stopped = only_last_stop(&h);
    // Longer than one heartbeat interval.
    std::thread::sleep(Duration::from_secs(7));
    assert_eq!(
        only_last_stop(&h),
        stopped,
        "a text-output launch heartbeats"
    );
    drop(stdin);
    assert!(child.wait().unwrap().success());
    assert_no_registered_instances(&h);
}

/// A client that closes stdout but keeps stdin open ends the copy at hcom's
/// next write. Claude's last frame went into hcom's pipe without error, and
/// Claude then waits on stdin and writes nothing more, so it never meets the
/// broken pipe. The heartbeat stops with the copy, as it does at the reap:
/// nothing reaches the client any longer, so the row must not look live.
#[test]
fn claude_stdio_heartbeat_stops_with_the_copy_when_the_client_closes_stdout() {
    let h = Hcom::new();
    let wrote = h.root_path().join("wrote");
    h.set_launch_envs(&[
        ("FAKE_HCOM", env!("CARGO_BIN_EXE_hcom")),
        ("FAKE_WROTE", wrote.to_str().unwrap()),
    ]);
    install_fake_claude(
        &h,
        r##"#!/bin/sh
if [ "$1" = --version ]; then printf '2.1.309 (Claude Code)\n'; exit 0; fi
sid=5f2a8c1d-7e3b-4d9a-b6c4-1e0f9a8b7c6d
hook() { printf '%s' "$2" | "$FAKE_HCOM" "$1" > /dev/null; }
: > "$PWD/transcript.jsonl"
hook sessionstart "{\"session_id\":\"$sid\",\"transcript_path\":\"$PWD/transcript.jsonl\",\"cwd\":\"$PWD\",\"hook_event_name\":\"SessionStart\",\"source\":\"startup\"}"
hook userpromptsubmit "{\"session_id\":\"$sid\",\"hook_event_name\":\"UserPromptSubmit\",\"prompt\":\"one\"}"
hook poll "{\"session_id\":\"$sid\",\"hook_event_name\":\"Stop\",\"stop_hook_active\":false}"
printf '{"type":"system","subtype":"init","session_id":"%s"}\n' "$sid"
IFS= read -r line
# The client has closed stdout by now; this still goes into hcom's pipe.
printf '{"type":"result","subtype":"success","is_error":false,"session_id":"%s"}\n' "$sid"
: > "$FAKE_WROTE"
IFS= read -r line
exit 0
"##,
    );
    let mut hcom = h
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
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut stdin = hcom.stdin.take().unwrap();
    let mut stdout = BufReader::new(hcom.stdout.take().unwrap());
    let mut line = String::new();
    stdout.read_line(&mut line).unwrap();
    assert!(line.contains("\"subtype\":\"init\""), "{line}");
    let reset_heartbeat = || {
        rusqlite::Connection::open(h.hcom_dir.join("hcom.db"))
            .unwrap()
            .execute("UPDATE instances SET last_stop = 1", [])
            .unwrap();
    };
    // While the client reads, the heartbeat runs.
    reset_heartbeat();
    h.eventually("the heartbeat", Duration::from_secs(60), || {
        Ok((only_last_stop(&h) > 1).then_some(()))
    });
    drop(stdout);
    writeln!(stdin, "{{\"type\":\"user\"}}").unwrap();
    stdin.flush().unwrap();
    // Only a precondition: a loaded host may take long to run the shim.
    h.eventually("Claude's last frame", Duration::from_secs(300), || {
        Ok(wrote.exists().then_some(()))
    });
    let log = h.hcom_dir.join(".tmp/logs/hcom.log");
    h.eventually(
        "the copy to stop at the closed stdout",
        Duration::from_secs(60),
        || {
            Ok(fs::read_to_string(&log)
                .unwrap_or_default()
                .contains("\"event\":\"stdio.stdout_client_closed\"")
                .then_some(()))
        },
    );
    // A refresh already under way may still land; none starts after the end
    // of the copy, which hcom reports before it logs that end.
    std::thread::sleep(Duration::from_secs(1));
    reset_heartbeat();
    // More than two heartbeat intervals, with Claude still waiting on stdin.
    std::thread::sleep(Duration::from_secs(12));
    assert_eq!(only_last_stop(&h), 1, "the heartbeat outlived the copy");
    assert!(
        hcom.try_wait().unwrap().is_none(),
        "Claude stopped waiting on stdin"
    );
    drop(stdin);
    assert_eq!(
        wait_for_hcom(&h, &mut hcom, Duration::from_secs(60)).code(),
        Some(0)
    );
    assert_no_registered_instances(&h);
}

/// A client pipe filled to capacity, so hcom's first write to it waits for
/// the client. Returns the client's end, hcom's end and the filler's size.
fn full_client_pipe() -> (std::io::PipeReader, std::io::PipeWriter, usize) {
    use nix::fcntl::{FcntlArg, OFlag, fcntl};
    use std::os::fd::AsFd;

    let (client, sink) = std::io::pipe().unwrap();
    let flags = OFlag::from_bits_truncate(fcntl(sink.as_fd(), FcntlArg::F_GETFL).unwrap());
    fcntl(sink.as_fd(), FcntlArg::F_SETFL(flags | OFlag::O_NONBLOCK)).unwrap();
    let mut filled = 0;
    while let Ok(written) = nix::unistd::write(sink.as_fd(), &[b'.'; 4096]) {
        filled += written;
    }
    fcntl(sink.as_fd(), FcntlArg::F_SETFL(flags)).unwrap();
    (client, sink, filled)
}

/// The copy keeps every byte the child wrote before it exited, however long
/// the client takes to read them: hcom delivers them before it exits.
#[test]
fn claude_stdio_delivers_output_to_a_client_paused_past_the_drain_bounds() {
    let h = Hcom::new();
    let exited = h.root_path().join("exited");
    h.set_launch_env("FAKE_EXITED", exited.to_str().unwrap());
    install_fake_claude(
        &h,
        r#"#!/bin/sh
if [ "$1" = --version ]; then printf '2.1.309 (Claude Code)\n'; exit 0; fi
pad=$(head -c 30000 /dev/zero | tr '\0' z)
printf '{"type":"system","subtype":"init"}\n'
printf '{"type":"assistant","pad":"%s"}\n' "$pad"
printf '{"type":"assistant","pad":"%s"}\n' "$pad"
printf '{"type":"result","subtype":"success","is_error":false}\n'
: > "$FAKE_EXITED"
exit 0
"#,
    );
    let pad = "z".repeat(30_000);
    let expected = format!(
        "{{\"type\":\"system\",\"subtype\":\"init\"}}\n{assistant}{assistant}{{\"type\":\"result\",\"subtype\":\"success\",\"is_error\":false}}\n",
        assistant = format!("{{\"type\":\"assistant\",\"pad\":\"{pad}\"}}\n")
    );
    // The client's pipe starts full, so hcom's first write waits for the
    // client. The child's output still fits in the copy's buffer and its own
    // pipe, so the child exits while hcom holds its last bytes.
    let (mut client, sink, filler) = full_client_pipe();
    let mut child = {
        let mut command = h.cmd();
        command
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
            .stdout(sink)
            .stderr(Stdio::null());
        command.spawn().unwrap()
    };
    // Only a precondition: a loaded host may take long to exec the child.
    h.eventually("the child to exit", Duration::from_secs(300), || {
        Ok(exited.exists().then_some(()))
    });
    // The heartbeat stopped at the reap, before the drain: hcom holding the
    // output for longer than the 5 s interval refreshes nothing.
    std::thread::sleep(Duration::from_secs(1));
    let reaped = only_last_stop(&h);
    // Longer than the drain's idle bound and the old fixed deadline.
    std::thread::sleep(Duration::from_secs(6));
    assert!(
        child.try_wait().unwrap().is_none(),
        "hcom exited with the child's output undelivered"
    );
    assert_eq!(
        only_last_stop(&h),
        reaped,
        "the heartbeat ran on in the drain"
    );
    let mut received = Vec::new();
    client.read_to_end(&mut received).unwrap();
    assert_eq!(child.wait().unwrap().code(), Some(0));
    assert_eq!(received.len(), filler + expected.len());
    assert!(
        received[filler..] == *expected.as_bytes(),
        "stdout differs from the native output"
    );
}

/// Once the child has exited, HUP, INT or TERM stops hcom waiting for a
/// client that is not reading, as it would stop a child blocked writing to
/// that client. hcom still returns the child's status and cleans up.
#[test]
fn claude_stdio_signal_after_the_child_exits_ends_a_stalled_copy() {
    for (signal, name) in [
        (libc::SIGHUP, "HUP"),
        (libc::SIGINT, "INT"),
        (libc::SIGTERM, "TERM"),
    ] {
        let h = Hcom::new();
        let exited = h.root_path().join("exited");
        h.set_launch_env("FAKE_EXITED", exited.to_str().unwrap());
        install_fake_claude(
            &h,
            r#"#!/bin/sh
if [ "$1" = --version ]; then printf '2.1.309 (Claude Code)\n'; exit 0; fi
pad=$(head -c 30000 /dev/zero | tr '\0' z)
printf '{"type":"assistant","pad":"%s"}\n' "$pad"
printf '{"type":"result","subtype":"success","is_error":false}\n'
: > "$FAKE_EXITED"
exit 5
"#,
        );
        let (client, sink, _) = full_client_pipe();
        let mut child = h
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
            .stdout(sink)
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        // Only a precondition: a loaded host may take long to exec the child.
        h.eventually("the child to exit", Duration::from_secs(300), || {
            Ok(exited.exists().then_some(()))
        });
        // hcom has reaped the child by now and still holds its output.
        std::thread::sleep(Duration::from_secs(1));
        assert!(
            child.try_wait().unwrap().is_none(),
            "{name}: hcom exited before the client read"
        );
        assert_eq!(unsafe { libc::kill(child.id() as i32, signal) }, 0);
        let status = wait_for_hcom(&h, &mut child, Duration::from_secs(60));
        assert_eq!(status.code(), Some(5), "{name}");
        drop(client);
        assert_no_registered_instances(&h);
    }
}

/// Wait up to `limit` for hcom to exit. One still running then is killed, so
/// the test fails instead of hanging.
fn wait_for_hcom(
    h: &Hcom,
    hcom: &mut std::process::Child,
    limit: Duration,
) -> std::process::ExitStatus {
    let deadline = Instant::now() + limit;
    loop {
        if let Some(status) = hcom.try_wait().unwrap() {
            return status;
        }
        if Instant::now() >= deadline {
            let _ = hcom.kill();
            let _ = hcom.wait();
            panic!("hcom still ran after {limit:?}\n{}", h.diagnostics());
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// The PID a shim wrote to `file`, once it has.
fn shim_pid(h: &Hcom, file: &std::path::Path) -> i32 {
    // Only a precondition: a loaded host may take long to exec the shim.
    h.eventually("the child to start", Duration::from_secs(300), || {
        Ok(fs::read_to_string(file)
            .ok()
            .and_then(|pid| pid.trim().parse().ok()))
    })
}

/// A cancellation that hcom forwards while Claude runs still ends hcom with
/// Claude when the client has stopped reading: a write the client has not
/// taken for the drain's idle bound is not waited for, as hcom would not
/// wait for a Claude blocked writing to that client.
#[test]
fn claude_stdio_forwarded_cancellation_ends_a_copy_the_client_stopped_taking() {
    let h = Hcom::new();
    let started = h.root_path().join("started");
    h.set_launch_env("FAKE_STARTED", started.to_str().unwrap());
    install_fake_claude(
        &h,
        r#"#!/bin/sh
if [ "$1" = --version ]; then printf '2.1.309 (Claude Code)\n'; exit 0; fi
pad=$(head -c 30000 /dev/zero | tr '\0' z)
printf '{"type":"stream_event","pad":"%s"}\n' "$pad"
printf '%s\n' "$$" > "$FAKE_STARTED"
while :; do printf '{"type":"stream_event","pad":"%s"}\n' "$pad"; done
"#,
    );
    // The client's pipe starts full and is never read: the copy blocks on
    // its first write, and Claude then blocks writing as well.
    let (client, sink, _) = full_client_pipe();
    let mut hcom = h
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
        .stdout(sink)
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let pid = shim_pid(&h, &started);
    assert_eq!(unsafe { libc::kill(hcom.id() as i32, libc::SIGTERM) }, 0);
    let status = wait_for_hcom(&h, &mut hcom, Duration::from_secs(60));
    // hcom forwarded the TERM, and the child died of it.
    assert_eq!(status.code(), Some(128 + libc::SIGTERM));
    assert_ne!(unsafe { libc::kill(pid, 0) }, 0, "the child outlived hcom");
    drop(client);
    assert_no_registered_instances(&h);
}

/// Whether `pid` has exited and waits to be reaped.
fn is_zombie(pid: i32) -> bool {
    std::process::Command::new("ps")
        .args(["-o", "stat=", "-p", &pid.to_string()])
        .output()
        .is_ok_and(|output| {
            String::from_utf8_lossy(&output.stdout)
                .trim_start()
                .starts_with('Z')
        })
}

/// TERM that reaches hcom together with the child's exit, so that hcom finds
/// the child already exited, ends the wait for a client that is not reading
/// at once, as one arriving during the drain does. hcom is stopped while the
/// child exits, so both signals are pending when it resumes, and it takes
/// them in signal-number order: TERM before CHLD.
#[test]
fn claude_stdio_signal_arriving_with_the_exit_ends_a_stalled_copy() {
    let h = Hcom::new();
    let started = h.root_path().join("started");
    let go = h.root_path().join("go");
    h.set_launch_envs(&[
        ("FAKE_STARTED", started.to_str().unwrap()),
        ("FAKE_GO", go.to_str().unwrap()),
    ]);
    install_fake_claude(
        &h,
        r#"#!/bin/sh
if [ "$1" = --version ]; then printf '2.1.309 (Claude Code)\n'; exit 0; fi
printf '%s\n' "$$" > "$FAKE_STARTED"
while [ ! -e "$FAKE_GO" ]; do sleep 0.05; done
printf '{"type":"result","subtype":"success","is_error":false}\n'
exit 5
"#,
    );
    // Small enough for the child's pipe while hcom is stopped; the client's
    // full pipe then holds it in the copy.
    let (client, sink, _) = full_client_pipe();
    let mut hcom = h
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
        .stdout(sink)
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let pid = shim_pid(&h, &started);
    let supervisor = hcom.id() as i32;
    assert_eq!(unsafe { libc::kill(supervisor, libc::SIGSTOP) }, 0);
    fs::write(&go, "").unwrap();
    h.eventually("the child to exit", Duration::from_secs(300), || {
        Ok(is_zombie(pid).then_some(()))
    });
    assert_eq!(unsafe { libc::kill(supervisor, libc::SIGTERM) }, 0);
    assert_eq!(unsafe { libc::kill(supervisor, libc::SIGCONT) }, 0);
    let status = wait_for_hcom(&h, &mut hcom, Duration::from_secs(60));
    assert_eq!(status.code(), Some(5));
    drop(client);
    assert_no_registered_instances(&h);
}

/// HUP, INT and TERM reach a stream-json child's process group exactly as
/// they reach an untapped one, and what it writes on the way out arrives.
#[test]
fn claude_stdio_stream_json_forwards_hup_int_and_term() {
    for (signal, name, code) in [
        (libc::SIGHUP, "HUP", 41),
        (libc::SIGINT, "INT", 42),
        (libc::SIGTERM, "TERM", 43),
    ] {
        let h = Hcom::new();
        install_fake_claude(
            &h,
            r#"#!/bin/sh
if [ "$1" = --version ]; then printf '2.1.309 (Claude Code)\n'; exit 0; fi
trap 'printf "{\"signal\":\"HUP\"}\n"; exit 41' HUP
trap 'printf "{\"signal\":\"INT\"}\n"; exit 42' INT
trap 'printf "{\"signal\":\"TERM\"}\n"; exit 43' TERM
printf '{"type":"system","subtype":"init"}\n'
while IFS= read -r line; do :; done
exit 32
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
                "--output-format",
                "stream-json",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let mut stdout = BufReader::new(child.stdout.take().unwrap());
        let mut line = String::new();
        stdout.read_line(&mut line).unwrap();
        assert_eq!(line, "{\"type\":\"system\",\"subtype\":\"init\"}\n");
        let _stdin = child.stdin.take().unwrap();
        assert_eq!(unsafe { libc::kill(child.id() as i32, signal) }, 0);
        let mut rest = String::new();
        stdout.read_to_string(&mut rest).unwrap();
        assert_eq!(rest, format!("{{\"signal\":\"{name}\"}}\n"), "{name}");
        assert_eq!(child.wait().unwrap().code(), Some(code), "{name}");
        assert_no_registered_instances(&h);
    }
}

/// The copy never waits for the room database. While another process holds
/// its write lock, a cancelled turn's result and every later frame still
/// reach a ready client at once; the status lands once the lock is released.
#[test]
fn claude_stdio_cancelled_turn_waits_for_a_locked_room_but_its_output_does_not() {
    use serde_json::{Value, json};
    use std::sync::mpsc;

    let h = Hcom::new();
    let copy = h.root_path().join("native-stdout");
    h.set_launch_envs(&[
        ("FAKE_STDOUT_COPY", copy.to_str().unwrap()),
        ("FAKE_HCOM", env!("CARGO_BIN_EXE_hcom")),
    ]);
    install_fake_claude(
        &h,
        r##"#!/bin/sh
if [ "$1" = --version ]; then printf '2.1.309 (Claude Code)\n'; exit 0; fi
sid=0c3e9d2a-5b7f-4e18-a6d4-9f2b1c8e7a50
emit() { printf '%s\n' "$1" >> "$FAKE_STDOUT_COPY"; printf '%s\n' "$1"; }
hook() { printf '%s' "$2" | "$FAKE_HCOM" "$1" > /dev/null; }
: > "$PWD/transcript.jsonl"
hook sessionstart "{\"session_id\":\"$sid\",\"transcript_path\":\"$PWD/transcript.jsonl\",\"cwd\":\"$PWD\",\"hook_event_name\":\"SessionStart\",\"source\":\"startup\"}"
emit "{\"type\":\"system\",\"subtype\":\"init\",\"session_id\":\"$sid\"}"
IFS= read -r line
hook userpromptsubmit "{\"session_id\":\"$sid\",\"hook_event_name\":\"UserPromptSubmit\",\"prompt\":\"one\"}"
emit "{\"type\":\"system\",\"subtype\":\"session_state_changed\",\"state\":\"running\",\"session_id\":\"$sid\"}"
emit "{\"type\":\"assistant\",\"session_id\":\"$sid\"}"
IFS= read -r line
emit "{\"type\":\"control_response\",\"response\":{\"subtype\":\"success\",\"request_id\":\"stop-1\"}}"
emit "{\"type\":\"result\",\"subtype\":\"error_during_execution\",\"is_error\":true,\"session_id\":\"$sid\",\"terminal_reason\":\"aborted_streaming\",\"queued_turn_count\":0}"
emit "{\"type\":\"system\",\"subtype\":\"session_state_changed\",\"state\":\"idle\",\"session_id\":\"$sid\"}"
# More than both pipes hold: a stalled copy would stall the child too.
pad=$(head -c 30000 /dev/zero | tr '\0' y)
for i in 1 2 3 4 5 6 7 8; do emit "{\"type\":\"stream_event\",\"n\":$i,\"pad\":\"$pad\"}"; done
emit "{\"type\":\"system\",\"subtype\":\"marker\"}"
IFS= read -r line
exit 0
"##,
    );
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
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = child.stdout.take().unwrap();
    let (send_line, lines) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        let mut received = Vec::new();
        let mut chunk = [0u8; 65536];
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
    let next = |what: &str, matches: &dyn Fn(&Value) -> bool| -> Value {
        loop {
            let line = lines
                .recv_timeout(Duration::from_secs(60))
                .unwrap_or_else(|_| panic!("no {what} frame\n{}", h.diagnostics()));
            if let Ok(frame) = serde_json::from_str::<Value>(&line)
                && matches(&frame)
            {
                return frame;
            }
        }
    };
    let send = |stdin: &mut std::process::ChildStdin, value: Value| {
        writeln!(stdin, "{value}").unwrap();
        stdin.flush().unwrap();
    };
    next("init", &|frame| frame["subtype"] == "init");
    send(
        &mut stdin,
        json!({"type": "user", "message": {"role": "user", "content": "one"}}),
    );
    next("assistant", &|frame| frame["type"] == "assistant");
    assert_eq!(only_status(&h).unwrap().1, "prompt");

    // Another room writer holds the write lock across the cancellation.
    let room = rusqlite::Connection::open(h.hcom_dir.join("hcom.db")).unwrap();
    room.execute_batch("BEGIN IMMEDIATE").unwrap();
    let locked = Instant::now();
    send(
        &mut stdin,
        json!({"type": "control_request", "request_id": "stop-1", "request": {"subtype": "interrupt"}}),
    );
    next("marker", &|frame| frame["subtype"] == "marker");
    let delivered = locked.elapsed();
    assert!(
        delivered < Duration::from_secs(2),
        "frames after the result waited {delivered:?} for the database"
    );
    assert_eq!(
        only_status(&h).unwrap().1,
        "prompt",
        "recorded through the lock"
    );
    // Release well inside the recorder's busy timeout.
    std::thread::sleep(Duration::from_millis(2500).saturating_sub(locked.elapsed()));
    room.execute_batch("COMMIT").unwrap();
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

    drop(stdin);
    assert_eq!(child.wait().unwrap().code(), Some(0));
    let received = reader.join().unwrap();
    assert!(
        received == fs::read(&copy).unwrap(),
        "stdout differs from the native output"
    );
    assert_no_registered_instances(&h);
}

/// A client that takes nothing for a whole cancelled turn leaves the copy no
/// moment at which the pipe was seen empty after the turn's prompt. hcom
/// cannot tell that turn's end from the end of one before the prompt, so it
/// records neither the result nor the idle frame: the row stays as the hooks
/// left it, while every byte still reaches the client.
#[test]
fn claude_stdio_turn_read_whole_behind_a_stalled_client_keeps_the_hooks_status() {
    use std::os::fd::AsFd;

    let h = Hcom::new();
    let copy = h.root_path().join("native-stdout");
    let wrote = h.root_path().join("wrote");
    h.set_launch_envs(&[
        ("FAKE_STDOUT_COPY", copy.to_str().unwrap()),
        ("FAKE_WROTE", wrote.to_str().unwrap()),
        ("FAKE_HCOM", env!("CARGO_BIN_EXE_hcom")),
    ]);
    // Claude reports the `running` state before the prompt hook, and `init`
    // and the turn's output after it. The frame before them is larger than
    // the copy's buffer, so the copy is blocked on the client with a full
    // read before the hook runs, however it is scheduled.
    install_fake_claude(
        &h,
        r##"#!/bin/sh
if [ "$1" = --version ]; then printf '2.1.309 (Claude Code)\n'; exit 0; fi
sid=2e7c4a9b-1d3f-4b6a-8c5e-7f9a0b1c2d3e
emit() { printf '%s\n' "$1" >> "$FAKE_STDOUT_COPY"; printf '%s\n' "$1"; }
hook() { printf '%s' "$2" | "$FAKE_HCOM" "$1" > /dev/null; }
: > "$PWD/transcript.jsonl"
hook sessionstart "{\"session_id\":\"$sid\",\"transcript_path\":\"$PWD/transcript.jsonl\",\"cwd\":\"$PWD\",\"hook_event_name\":\"SessionStart\",\"source\":\"startup\"}"
emit "{\"type\":\"system\",\"subtype\":\"hook_response\",\"session_id\":\"$sid\"}"
IFS= read -r line
pad=$(head -c 70000 /dev/zero | tr '\0' p)
emit "{\"type\":\"stream_event\",\"session_id\":\"$sid\",\"pad\":\"$pad\"}"
emit "{\"type\":\"system\",\"subtype\":\"session_state_changed\",\"state\":\"running\",\"session_id\":\"$sid\"}"
hook userpromptsubmit "{\"session_id\":\"$sid\",\"hook_event_name\":\"UserPromptSubmit\",\"prompt\":\"one\"}"
emit "{\"type\":\"system\",\"subtype\":\"init\",\"session_id\":\"$sid\"}"
emit "{\"type\":\"assistant\",\"session_id\":\"$sid\"}"
IFS= read -r line
emit "{\"type\":\"control_response\",\"response\":{\"subtype\":\"success\",\"request_id\":\"stop-1\"}}"
emit "{\"type\":\"result\",\"subtype\":\"error_during_execution\",\"is_error\":true,\"session_id\":\"$sid\",\"terminal_reason\":\"aborted_streaming\",\"queued_turn_count\":0}"
emit "{\"type\":\"system\",\"subtype\":\"session_state_changed\",\"state\":\"idle\",\"session_id\":\"$sid\"}"
: > "$FAKE_WROTE"
IFS= read -r line
exit 0
"##,
    );
    let (mut client, sink) = std::io::pipe().unwrap();
    let filler = sink.try_clone().unwrap();
    let mut hcom = h
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
        .stdout(sink)
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut stdin = hcom.stdin.take().unwrap();
    let mut received = Vec::new();
    let mut chunk = [0u8; 65536];
    while !received
        .ends_with(b"\"hook_response\",\"session_id\":\"2e7c4a9b-1d3f-4b6a-8c5e-7f9a0b1c2d3e\"}\n")
    {
        let read = client.read(&mut chunk).unwrap();
        assert!(read > 0, "EOF before the session started");
        received.extend_from_slice(&chunk[..read]);
    }
    // From here the client takes nothing: its pipe is full, so the copy
    // blocks on its first read of the turn, before the prompt hook commits.
    let filled = {
        use nix::fcntl::{FcntlArg, OFlag, fcntl};
        let flags = OFlag::from_bits_truncate(fcntl(filler.as_fd(), FcntlArg::F_GETFL).unwrap());
        fcntl(filler.as_fd(), FcntlArg::F_SETFL(flags | OFlag::O_NONBLOCK)).unwrap();
        let mut filled = 0;
        while let Ok(written) = nix::unistd::write(filler.as_fd(), &[b'.'; 4096]) {
            filled += written;
        }
        fcntl(filler.as_fd(), FcntlArg::F_SETFL(flags)).unwrap();
        filled
    };
    drop(filler);
    writeln!(
        stdin,
        "{}",
        serde_json::json!({"type": "user", "message": {"role": "user", "content": "one"}})
    )
    .unwrap();
    // Only preconditions: a loaded host may take long to run the hooks.
    h.eventually("the prompt hook", Duration::from_secs(300), || {
        Ok(only_status(&h)
            .is_some_and(|row| row.1 == "prompt")
            .then_some(()))
    });
    writeln!(
        stdin,
        "{}",
        serde_json::json!({"type": "control_request", "request_id": "stop-1", "request": {"subtype": "interrupt"}})
    )
    .unwrap();
    h.eventually("the turn's last frame", Duration::from_secs(300), || {
        Ok(wrote.exists().then_some(()))
    });
    // The client reads on: the rest of the turn arrives in one read.
    let idle = b"\"state\":\"idle\",\"session_id\":\"2e7c4a9b-1d3f-4b6a-8c5e-7f9a0b1c2d3e\"}\n";
    while !received.ends_with(idle) {
        let read = client.read(&mut chunk).unwrap();
        assert!(read > 0, "EOF before the idle frame");
        received.extend_from_slice(&chunk[..read]);
    }
    // Long enough for the recorder to have applied either frame.
    std::thread::sleep(Duration::from_secs(2));
    assert_eq!(
        only_status(&h),
        Some(("active".to_string(), "prompt".to_string(), String::new()))
    );
    drop(stdin);
    client.read_to_end(&mut received).unwrap();
    assert_eq!(
        wait_for_hcom(&h, &mut hcom, Duration::from_secs(60)).code(),
        Some(0)
    );
    let native = fs::read(&copy).unwrap();
    let hook_response = native.iter().position(|&b| b == b'\n').unwrap() + 1;
    assert!(received[..hook_response] == native[..hook_response]);
    assert!(
        received[hook_response..hook_response + filled]
            .iter()
            .all(|&b| b == b'.')
    );
    assert!(
        received[hook_response + filled..] == native[hook_response..],
        "stdout differs from the native output"
    );
    assert_no_registered_instances(&h);
    let contexts = status_contexts(&h);
    assert!(
        !contexts.iter().any(|context| context == "interrupted"),
        "{contexts:?}"
    );
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

/// A real SDK interrupt, as X1 captured it: the native CLI aborts the streamed
/// reply, answers the control request, runs no Stop hook and writes a
/// `result` with `terminal_reason: aborted_streaming`, then the idle session
/// state. The mock provider holds the reply open; no hosted model is used.
#[test]
#[ignore = "requires the real Claude CLI installed by scripts/install-mock-tools.sh"]
fn real_claude_stdio_interrupt_marks_the_turn_listening_interrupted() {
    use serde_json::{Value, json};
    use std::sync::mpsc;
    use support::claude_mock::{
        ClaudeCase, MODEL, claude_partial_text, claude_text, latest_user_turn,
    };
    use support::mock_http::{MockHttp, Reply};
    use support::real_tool::ToolCase;

    let h = Hcom::new();
    h.set_launch_env(
        "PATH",
        &format!(
            "{}:{}",
            support::pins::mock_tools_bin().display(),
            std::env::var("PATH").unwrap()
        ),
    );
    let mock = MockHttp::start(|request| {
        if request.method.eq_ignore_ascii_case("HEAD") {
            return Reply::Empty(200);
        }
        if request.path.contains("count_tokens") {
            return Reply::Json(json!({"input_tokens": 1}).to_string());
        }
        if !request.path.contains("/v1/messages") {
            return Reply::Status(404);
        }
        // The newer turn first: Claude may fold the interrupted prompt into
        // the next request's last user message.
        let (_, text) = latest_user_turn(&request.body).unwrap_or_default();
        if text.contains("STDIO_AFTER") {
            Reply::Sse(claude_text("msg_after", "STDIO_AFTER_DONE"))
        } else if text.contains("STDIO_STALL") {
            Reply::Stall(claude_partial_text("msg_stall", "Counting: 1 2 3"))
        } else {
            Reply::Status(500)
        }
    })
    .unwrap();
    ClaudeCase.prepare(&h, &ClaudeCase.provider_base_url(mock.port()));
    // As the Agent SDK host launches it, session state frames included.
    h.set_launch_envs(&[
        ("CLAUDE_CODE_ENTRYPOINT", "sdk-ts"),
        ("CLAUDE_CODE_EMIT_SESSION_STATE_EVENTS", "1"),
    ]);
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
    let next = |kind: &str| -> Value {
        loop {
            let line = receive
                .recv_timeout(Duration::from_secs(90))
                .unwrap_or_else(|_| panic!("no {kind} frame\n{}", h.diagnostics()));
            let frame: Value =
                serde_json::from_str(&line).expect("stdout contains only native JSON frames");
            if frame["type"] == kind {
                return frame;
            }
        }
    };
    send_json(
        &mut stdin,
        json!({"type": "control_request", "request_id": "init", "request": {"subtype": "initialize"}}),
    );
    send_json(
        &mut stdin,
        json!({"type": "user", "message": {"role": "user", "content": "STDIO_STALL"}, "parent_tool_use_id": null}),
    );
    // The turn is under way once the provider holds its reply open.
    h.eventually(
        "the stalled turn to reach the provider",
        Duration::from_secs(90),
        || {
            let started = mock.requests().iter().any(|request| {
                request.path.contains("/v1/messages")
                    && !request.path.contains("count_tokens")
                    && request.body.contains("STDIO_STALL")
            });
            Ok((started && only_status(&h).is_some_and(|row| row.1 == "prompt")).then_some(()))
        },
    );
    send_json(
        &mut stdin,
        json!({"type": "control_request", "request_id": "stop-1", "request": {"subtype": "interrupt"}}),
    );
    let result = next("result");
    assert_eq!(result["terminal_reason"], "aborted_streaming", "{result}");
    assert_eq!(result["is_error"], true, "{result}");
    // The fallback trigger as the client sees it: the idle state, in the session.
    let idle = loop {
        let frame = next("system");
        if frame["subtype"] == "session_state_changed" {
            break frame;
        }
    };
    assert_eq!(idle["state"], "idle", "{idle}");
    assert_eq!(idle["session_id"], result["session_id"], "{idle}");
    let interrupted = Some((
        "listening".to_string(),
        "interrupted".to_string(),
        "aborted_streaming".to_string(),
    ));
    h.eventually(
        "the interrupted turn to become listening",
        Duration::from_secs(10),
        || Ok((only_status(&h) == interrupted).then_some(())),
    );
    assert!(
        child.try_wait().unwrap().is_none(),
        "the native process outlives a cancelled turn"
    );

    send_json(
        &mut stdin,
        json!({"type": "user", "message": {"role": "user", "content": "STDIO_AFTER"}, "parent_tool_use_id": null}),
    );
    let result = next("result");
    assert_eq!(result["is_error"], false, "{result}");
    assert!(
        result["result"]
            .as_str()
            .unwrap()
            .contains("STDIO_AFTER_DONE"),
        "{result}"
    );
    h.eventually("the next turn to end", Duration::from_secs(10), || {
        Ok(
            (only_status(&h).is_some_and(|row| row.0 == "listening" && row.1.is_empty()))
                .then_some(()),
        )
    });
    drop(stdin);
    assert!(child.wait().unwrap().success());
    reader.join().unwrap();
    let stderr = errors.join().unwrap();
    assert!(!stderr.contains("requires --print"), "{stderr}");
    assert_no_registered_instances(&h);
}
