//! Resolve the user's login+interactive shell environment for nested launches.
//!
//! This follows the editor-style "resolve shell env" pattern: run the user's
//! shell out-of-band, cache the captured env, and fail open to the caller on
//! any problem.

use std::collections::HashMap;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

const CACHE_TTL: Duration = Duration::from_secs(6 * 60 * 60);
// 3: seeds PREFIX into the resolver shell (see `clean_shell_seed_env`). Caches
// written by v2 can hold a TMPDIR resolved from an empty PREFIX, so they have
// to be discarded rather than aged out over CACHE_TTL.
const CACHE_VERSION: u32 = 3;
const SHELL_TIMEOUT: Duration = Duration::from_secs(10);
const MARKER_VAR: &str = "HCOM_SHELL_ENV_MARKER";
const RC_FILES: &[&str] = &[
    ".zshrc",
    ".zprofile",
    ".zshenv",
    ".bashrc",
    ".bash_profile",
    ".profile",
];

/// Resolve the user's login+interactive shell environment, out-of-band, cached.
/// Returns None on any failure so callers can fall back to parent inheritance.
pub fn resolved_shell_env() -> Option<HashMap<String, String>> {
    let cache_path = crate::paths::hcom_path(&["shell_env.json"]);
    let home = dirs::home_dir().or_else(|| std::env::var_os("HOME").map(PathBuf::from))?;
    let rc_mtime = rc_mtime_key_for_home(&home).ok()?;
    let now = epoch_secs(SystemTime::now())?;

    if let Some(entry) = read_cache(&cache_path)
        && cache_is_fresh(&entry, rc_mtime, now)
    {
        return Some(entry.env);
    }

    let env = resolve_shell_env_uncached()?;
    let entry = ShellEnvCache {
        version: CACHE_VERSION,
        rc_mtime,
        written_at: now,
        env: env.clone(),
    };
    let _ = write_cache(&cache_path, &entry);
    Some(env)
}

fn resolve_shell_env_uncached() -> Option<HashMap<String, String>> {
    // Windows has no login-shell PATH-stripping problem to work around (unlike
    // macOS GUI-launched apps), and `shell_path()`'s non-macOS fallback
    // (`/bin/bash`) never exists there — skip straight to the `None` fail-open
    // instead of spawning a command that's guaranteed to fail.
    if cfg!(windows) {
        return None;
    }
    let shell = shell_path()?;
    if unsupported_shell(&shell) {
        return None;
    }

    let marker = format!("hcom-shell-env-{}", uuid::Uuid::new_v4());
    let cmd = format!("printf %s \"${MARKER_VAR}\"; env -0; printf %s \"${MARKER_VAR}\"");
    let output = timed_shell_output(&shell, &cmd, &marker)?;
    parse_shell_env_output(&output.stdout, &marker, MARKER_VAR)
}

fn shell_path() -> Option<PathBuf> {
    if let Some(shell) = std::env::var_os("SHELL").filter(|s| !s.is_empty()) {
        return Some(PathBuf::from(shell));
    }
    #[cfg(target_os = "macos")]
    {
        Some(PathBuf::from("/bin/zsh"))
    }
    #[cfg(not(target_os = "macos"))]
    {
        Some(PathBuf::from("/bin/bash"))
    }
}

fn unsupported_shell(shell: &Path) -> bool {
    let Some(name) = shell.file_name().and_then(|s| s.to_str()) else {
        return true;
    };
    matches!(name, "fish" | "pwsh" | "powershell" | "nu")
}

struct ShellOutput {
    stdout: Vec<u8>,
}

/// Upper bound on what the resolver keeps from the shell. A login environment
/// is a few kilobytes; a shell that keeps streaming (an rc file that tails a
/// log, a prompt hook in a loop) is killed instead of being read without limit.
const MAX_SHELL_OUTPUT: usize = 8 * 1024 * 1024;

/// Size of one read from the shell's stdout.
const DRAIN_CHUNK: usize = 16 * 1024;

/// How many chunks the drain may queue ahead of the consumer. The byte budget
/// is enforced before a chunk is queued, so this only bounds the burst a
/// producer can run ahead by (1 MiB), not the total.
const DRAIN_QUEUE_CHUNKS: usize = 64;

/// What the drain thread hands over, piece by piece.
enum Drained {
    Chunk(Vec<u8>),
    Eof,
    /// The byte budget would be exceeded; the chunk that crossed it was dropped
    /// and nothing more is read.
    OverCapacity,
    Failed,
}

/// Read the shell's stdout on its own thread and hand each chunk over through
/// a bounded channel, so the shell never sits in a blocked write on a full pipe
/// while the parent waits for it to exit (#7).
///
/// The byte budget is enforced here, before a chunk is queued: the bytes held
/// anywhere (queue plus consumer) never exceed `budget`. `stop` ends the read
/// early. On unix the read waits in `poll` with a short timeout and checks the
/// flag between waits, so the thread can be joined on every return of the
/// resolver, including when a descendant of the shell still holds the pipe
/// open and EOF never comes.
fn drain_stdout(
    mut out: std::process::ChildStdout,
    tx: std::sync::mpsc::SyncSender<Drained>,
    budget: usize,
    stop: Arc<AtomicBool>,
) {
    let mut chunk = [0u8; DRAIN_CHUNK];
    let mut sent = 0usize;
    loop {
        if !wait_readable(&out, &stop) {
            return;
        }
        match out.read(&mut chunk) {
            Ok(0) => {
                let _ = tx.send(Drained::Eof);
                return;
            }
            Ok(n) => {
                if sent + n > budget {
                    let _ = tx.send(Drained::OverCapacity);
                    return;
                }
                sent += n;
                if tx.send(Drained::Chunk(chunk[..n].to_vec())).is_err() {
                    return;
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(_) => {
                let _ = tx.send(Drained::Failed);
                return;
            }
        }
    }
}

/// Wait until `out` has data, EOF or an error to report, checking `stop` every
/// 50 ms. Returns false once `stop` is set.
#[cfg(unix)]
fn wait_readable(out: &std::process::ChildStdout, stop: &AtomicBool) -> bool {
    use nix::poll::{PollFd, PollFlags, PollTimeout, poll};
    use std::os::fd::AsFd;
    loop {
        if stop.load(Ordering::Relaxed) {
            return false;
        }
        let mut fds = [PollFd::new(out.as_fd(), PollFlags::POLLIN)];
        match poll(&mut fds, PollTimeout::from(50u16)) {
            Ok(0) => continue,
            Err(nix::errno::Errno::EINTR) => continue,
            // Readable, hung up or in error: the read answers without blocking.
            Ok(_) | Err(_) => return true,
        }
    }
}

#[cfg(not(unix))]
fn wait_readable(_out: &std::process::ChildStdout, stop: &AtomicBool) -> bool {
    !stop.load(Ordering::Relaxed)
}

/// The drain thread and the parent's handles on it.
struct Drain {
    rx: std::sync::mpsc::Receiver<Drained>,
    stop: Arc<AtomicBool>,
    handle: std::thread::JoinHandle<()>,
}

impl Drain {
    fn start(out: std::process::ChildStdout, budget: usize) -> std::io::Result<Self> {
        let (tx, rx) = std::sync::mpsc::sync_channel(DRAIN_QUEUE_CHUNKS);
        let stop = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&stop);
        let handle = std::thread::Builder::new()
            .name("hcom-shell-env-drain".to_string())
            .spawn(move || drain_stdout(out, tx, budget, flag))?;
        Ok(Self { rx, stop, handle })
    }

    /// End the drain thread and wait for it. The receiver goes first, so a
    /// send blocked on a full queue fails; the flag ends the next wait; a read
    /// that already has data returns at once. Bounded by one poll interval.
    fn finish(self) {
        let Drain { rx, stop, handle } = self;
        drop(rx);
        stop.store(true, Ordering::Relaxed);
        let _ = handle.join();
    }
}

/// Whether `stdout` ends the way a complete capture does: with the closing
/// marker right after the NUL that terminates the last `env -0` entry. A
/// capture cut inside the marker variable's own value also ends with the
/// marker bytes, but after `=`, so it is not mistaken for the close.
fn output_is_complete(stdout: &[u8], marker: &str) -> bool {
    let m = marker.as_bytes();
    stdout.len() > m.len() && stdout.ends_with(m) && stdout[stdout.len() - m.len() - 1] == 0
}

fn timed_shell_output(shell: &Path, cmd: &str, marker: &str) -> Option<ShellOutput> {
    timed_shell_output_with_timeout(shell, cmd, marker, SHELL_TIMEOUT)
}

fn timed_shell_output_with_timeout(
    shell: &Path,
    cmd: &str,
    marker: &str,
    timeout: Duration,
) -> Option<ShellOutput> {
    timed_shell_output_bounded(shell, cmd, marker, timeout, MAX_SHELL_OUTPUT, None)
}

/// Run the shell and collect its stdout while waiting for it to exit.
///
/// The read and the wait run together: a drain thread reads the pipe and this
/// thread polls for the exit, so an environment larger than the pipe buffer
/// (16 KB on macOS) or a slow writer can no longer park the shell in a blocked
/// write until the timeout kills it. One deadline covers both halves; when it
/// runs out, or the output exceeds `capacity`, the shell's process group is
/// killed and the resolution fails.
///
/// Success needs the output to be complete: EOF, or, once the shell has exited
/// and everything the drain queued has been taken, the closing marker in its
/// place. The second form covers a background job started by an rc file that
/// inherited the pipe: after a short grace the output is returned, the job is
/// left alone, and the drain thread is stopped rather than left waiting on the
/// pipe. `home` replaces the seeded HOME (tests use a scratch directory).
fn timed_shell_output_bounded(
    shell: &Path,
    cmd: &str,
    marker: &str,
    timeout: Duration,
    capacity: usize,
    home: Option<&Path>,
) -> Option<ShellOutput> {
    const POLL: Duration = Duration::from_millis(25);
    const EXIT_GRACE: Duration = Duration::from_millis(200);

    enum Verdict {
        Complete,
        Fail,
    }

    fn kill_and_reap(child: &mut std::process::Child) {
        crate::sys::process::kill_child_group(child);
        let _ = child.wait();
    }

    let mut child = shell_command(shell, cmd, marker, home).spawn().ok()?;
    let Some(out) = child.stdout.take() else {
        kill_and_reap(&mut child);
        return None;
    };
    let Ok(drain) = Drain::start(out, capacity) else {
        kill_and_reap(&mut child);
        return None;
    };

    let start = std::time::Instant::now();
    let mut stdout = Vec::new();
    let mut eof = false;
    let mut failed = false;
    let mut exited: Option<(std::process::ExitStatus, std::time::Instant)> = None;
    // Apply one message from the drain; false once the capture cannot succeed.
    let take = |message: Drained, stdout: &mut Vec<u8>, eof: &mut bool| match message {
        Drained::Chunk(bytes) if stdout.len() + bytes.len() <= capacity => {
            stdout.extend_from_slice(&bytes);
            true
        }
        Drained::Chunk(_) | Drained::OverCapacity | Drained::Failed => false,
        Drained::Eof => {
            *eof = true;
            true
        }
    };
    let verdict = loop {
        if !eof && !failed {
            match drain.rx.recv_timeout(POLL) {
                Ok(message) => failed = !take(message, &mut stdout, &mut eof),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => failed = true,
            }
        } else if !failed {
            std::thread::sleep(POLL);
        }
        if failed {
            break Verdict::Fail;
        }
        if exited.is_none() {
            match child.try_wait() {
                Ok(Some(status)) => exited = Some((status, std::time::Instant::now())),
                Ok(None) => {}
                Err(_) => break Verdict::Fail,
            }
        }
        if let Some((status, at)) = exited {
            // The shell is gone: take everything the drain has queued before
            // judging the capture, so a late consumer cannot return a prefix
            // while chunks, EOF or the budget verdict still wait in the queue.
            while !eof && !failed {
                match drain.rx.try_recv() {
                    Ok(message) => failed = !take(message, &mut stdout, &mut eof),
                    Err(std::sync::mpsc::TryRecvError::Empty) => break,
                    Err(std::sync::mpsc::TryRecvError::Disconnected) => failed = true,
                }
            }
            if failed || !status.success() {
                break Verdict::Fail;
            }
            if eof || (at.elapsed() >= EXIT_GRACE && output_is_complete(&stdout, marker)) {
                break Verdict::Complete;
            }
        }
        if start.elapsed() >= timeout {
            break Verdict::Fail;
        }
    };

    match verdict {
        Verdict::Complete => {
            drain.finish();
            Some(ShellOutput { stdout })
        }
        Verdict::Fail => {
            // A shell that already exited is not killed: its group may be a
            // background job the user started, which is left alone as on the
            // success path.
            if exited.is_none() {
                kill_and_reap(&mut child);
            }
            drain.finish();
            None
        }
    }
}

/// The login shell invocation. `home` replaces the seeded HOME when given, so
/// a test can run a shell against an empty directory instead of the caller's
/// login files.
fn shell_command(shell: &Path, cmd: &str, marker: &str, home: Option<&Path>) -> Command {
    let mut seed = clean_shell_seed_env(shell);
    if let Some(home) = home {
        seed.insert("HOME".to_string(), home.to_string_lossy().to_string());
    }
    let mut command = Command::new(shell);
    command
        .arg("-lic")
        .arg(cmd)
        .env_clear()
        .envs(seed)
        .env(MARKER_VAR, marker)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());

    crate::sys::process::detach_session(&mut command);

    command
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ShellEnvCache {
    #[serde(default)]
    version: u32,
    rc_mtime: u64,
    written_at: u64,
    env: HashMap<String, String>,
}

fn read_cache(path: &Path) -> Option<ShellEnvCache> {
    let content = fs::read_to_string(path).ok()?;
    serde_json::from_str(&content).ok()
}

fn write_cache(path: &Path, entry: &ShellEnvCache) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let content = serde_json::to_vec(entry).map_err(std::io::Error::other)?;
    let tmp = tempfile::NamedTempFile::new_in(path.parent().unwrap_or_else(|| Path::new(".")))?;
    fs::write(tmp.path(), content)?;
    crate::sys::fs::set_private(tmp.path())?;
    tmp.persist(path).map_err(std::io::Error::other)?;
    crate::sys::fs::set_private(path)?;
    Ok(())
}

fn cache_is_fresh(entry: &ShellEnvCache, rc_mtime: u64, now: u64) -> bool {
    entry.version == CACHE_VERSION
        && entry.rc_mtime == rc_mtime
        && now
            .checked_sub(entry.written_at)
            .is_some_and(|age| age <= CACHE_TTL.as_secs())
}

fn clean_shell_seed_env(shell: &Path) -> HashMap<String, String> {
    let mut env = HashMap::new();
    // PREFIX: Termux-specific (unset elsewhere, so harmless on other platforms).
    // Termux rc files commonly derive TMPDIR from it (e.g. `export
    // TMPDIR="$PREFIX/tmp"`); without it seeded here, re-sourcing that rc file
    // in this stripped-down shell silently clobbers an already-correct TMPDIR
    // back to a bare "/tmp" — writable inside a proot container's private
    // /tmp, but not on real Android, where /tmp is owned by uid 2000 (shell).
    for key in ["HOME", "USER", "LOGNAME", "TMPDIR", "PREFIX"] {
        if let Some(value) = std::env::var_os(key).filter(|v| !v.is_empty()) {
            env.insert(key.to_string(), value.to_string_lossy().to_string());
        }
    }
    env.insert("SHELL".to_string(), shell.to_string_lossy().to_string());
    env.insert(
        "PATH".to_string(),
        "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin".to_string(),
    );
    env
}

fn rc_mtime_key_for_home(home: &Path) -> std::io::Result<u64> {
    let mut max = 0;
    for file in RC_FILES {
        let path = home.join(file);
        if let Ok(meta) = fs::metadata(path) {
            let modified = meta.modified()?;
            max = max.max(epoch_secs(modified).unwrap_or(0));
        }
    }
    Ok(max)
}

fn epoch_secs(time: SystemTime) -> Option<u64> {
    time.duration_since(UNIX_EPOCH).ok().map(|d| d.as_secs())
}

fn parse_shell_env_output(
    stdout: &[u8],
    marker: &str,
    marker_var: &str,
) -> Option<HashMap<String, String>> {
    let marker_bytes = marker.as_bytes();
    let first = find_bytes(stdout, marker_bytes)?;
    let after_first = first + marker_bytes.len();
    let second_rel = rfind_bytes(&stdout[after_first..], marker_bytes)?;
    let body = &stdout[after_first..after_first + second_rel];

    let mut env = HashMap::new();
    for entry in body.split(|b| *b == b'\0') {
        if entry.is_empty() {
            continue;
        }
        let Some(eq) = entry.iter().position(|b| *b == b'=') else {
            continue;
        };
        let key = String::from_utf8(entry[..eq].to_vec()).ok()?;
        if key.starts_with("HCOM_") || key == marker_var {
            continue;
        }
        let value = String::from_utf8(entry[eq + 1..].to_vec()).ok()?;
        env.insert(key, value);
    }
    Some(env)
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return None;
    }
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn rfind_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return None;
    }
    haystack
        .windows(needle.len())
        .rposition(|window| window == needle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[test]
    fn parse_shell_env_output_extracts_between_markers_and_strips_hcom() {
        let marker = "MARKER";
        let stdout = b"noiseMARKER\0a=1\0b=two\nlines\0HCOM_PROCESS_ID=pid\0HCOM_SHELL_ENV_MARKER=MARKER\0MARKERtail";

        let env = parse_shell_env_output(stdout, marker, MARKER_VAR).unwrap();

        assert_eq!(env.get("a").map(String::as_str), Some("1"));
        assert_eq!(env.get("b").map(String::as_str), Some("two\nlines"));
        assert!(!env.contains_key("HCOM_PROCESS_ID"));
        assert!(!env.contains_key(MARKER_VAR));
    }

    #[test]
    fn parse_shell_env_output_ignores_marker_value_inside_env_body() {
        let marker = "MARKER";
        let stdout = b"MARKERHCOM_SHELL_ENV_MARKER=MARKER\0a=1\0MARKER";

        let env = parse_shell_env_output(stdout, marker, MARKER_VAR).unwrap();

        assert_eq!(env.get("a").map(String::as_str), Some("1"));
        assert!(!env.contains_key(MARKER_VAR));
    }

    #[test]
    fn parse_shell_env_output_rejects_missing_marker() {
        assert!(parse_shell_env_output(b"a=1\0b=2", "MARKER", MARKER_VAR).is_none());
    }

    // Termux rc files write `export TMPDIR="$PREFIX/tmp"`. If PREFIX is absent
    // from the resolver shell's seed env that expands to a bare "/tmp", which
    // on real Android is uid 2000's and unwritable by the app -- launched
    // agents then die with EACCES on their scratch dir. Seed PREFIX so the rc
    // file re-derives the TMPDIR the user actually has.
    #[cfg(unix)]
    #[test]
    #[serial]
    fn resolver_shell_seeds_prefix_so_rc_derived_tmpdir_survives() {
        use crate::hooks::test_helpers::EnvVarGuard;
        let shell = test_shell_path();
        let seeded = {
            let _prefix = EnvVarGuard::set("PREFIX", "/seeded/prefix");
            clean_shell_seed_env(&shell)
        };
        let unset = {
            let _prefix = EnvVarGuard::unset("PREFIX");
            clean_shell_seed_env(&shell)
        };

        assert_eq!(
            seeded.get("PREFIX").map(String::as_str),
            Some("/seeded/prefix")
        );
        assert!(!unset.contains_key("PREFIX"));
    }

    #[test]
    fn cache_key_mtime_change_busts_cache() {
        let entry = ShellEnvCache {
            version: CACHE_VERSION,
            rc_mtime: 10,
            written_at: 100,
            env: HashMap::new(),
        };

        assert!(cache_is_fresh(&entry, 10, 101));
        assert!(!cache_is_fresh(&entry, 11, 101));
    }

    #[test]
    fn cache_version_change_busts_cache() {
        let entry = ShellEnvCache {
            version: CACHE_VERSION - 1,
            rc_mtime: 10,
            written_at: 100,
            env: HashMap::new(),
        };

        assert!(!cache_is_fresh(&entry, 10, 101));
    }

    #[test]
    fn cache_key_ttl_busts_cache() {
        let entry = ShellEnvCache {
            version: CACHE_VERSION,
            rc_mtime: 10,
            written_at: 100,
            env: HashMap::new(),
        };

        assert!(cache_is_fresh(&entry, 10, 100 + CACHE_TTL.as_secs()));
        assert!(!cache_is_fresh(&entry, 10, 101 + CACHE_TTL.as_secs()));
    }

    #[cfg(unix)]
    #[test]
    fn cache_write_uses_private_permissions() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("shell_env.json");
        let entry = ShellEnvCache {
            version: CACHE_VERSION,
            rc_mtime: 1,
            written_at: 2,
            env: HashMap::from([("OPENAI_API_KEY".to_string(), "sk-test".to_string())]),
        };

        write_cache(&path, &entry).unwrap();

        let mode = fs::metadata(path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
    }

    #[cfg(unix)]
    #[test]
    fn resolver_shell_starts_in_a_new_session() {
        use nix::sys::signal::{Signal, killpg};
        use nix::unistd::{Pid, getsid};

        let shell = test_shell_path();
        let mut command = shell_command(&shell, "sleep 30", "setsid-test", None);
        command.stdout(Stdio::null());
        let mut child = command.spawn().unwrap();
        let child_pid = Pid::from_raw(child.id() as i32);

        let current_session = getsid(None).unwrap();
        let child_session = getsid(Some(child_pid)).unwrap();

        let _ = killpg(child_pid, Signal::SIGKILL);
        let _ = child.wait();

        assert_eq!(child_session, child_pid);
        assert_ne!(child_session, current_session);
    }

    // Unix-only: drives a POSIX shell (`env -0`, sh loops) that isn't resolved
    // on Windows.
    #[cfg(unix)]
    #[test]
    fn resolver_discards_stderr_without_breaking_env_resolution() {
        let shell = test_shell_path();
        let marker = "hcom-shell-env-stderr-test";
        // ~256KB of stderr, several times the 64KB pipe buffer the resolver
        // would deadlock on if stderr were piped instead of discarded. The
        // chunk is doubled into a variable and emitted with the `echo`
        // builtin so the volume costs no forks: an external command per line
        // is ~23ms on syscall-heavy sandboxes (PRoot, Android), which turns a
        // per-line loop into minutes of wall time.
        let cmd = format!(
            "chunk=x; i=0; while [ \"$i\" -lt 12 ]; do chunk=\"$chunk$chunk\"; i=$((i + 1)); done; \
             i=0; while [ \"$i\" -lt 64 ]; do echo \"$chunk\" >&2; i=$((i + 1)); done; \
             printf %s \"${MARKER_VAR}\"; env -0; printf %s \"${MARKER_VAR}\""
        );

        let output =
            timed_shell_output_with_timeout(&shell, &cmd, marker, Duration::from_secs(30)).unwrap();
        let env = parse_shell_env_output(&output.stdout, marker, MARKER_VAR).unwrap();

        assert!(env.contains_key("PATH"));
        let expected_shell = shell.to_string_lossy();
        assert_eq!(
            env.get("SHELL").map(String::as_str),
            Some(expected_shell.as_ref())
        );
    }

    /// A stand-in for the login shell for the drain tests: accepts `-lic <cmd>`
    /// and runs the command with `/bin/sh -c`, reading no profile. Returned
    /// with the directory that keeps it alive.
    #[cfg(unix)]
    fn fake_shell() -> (PathBuf, tempfile::TempDir) {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fakesh");
        fs::write(
            &path,
            "#!/bin/sh\n[ \"$1\" = -lic ] || exit 97\nexec /bin/sh -c \"$2\"\n",
        )
        .unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
        (path, dir)
    }

    /// `timed_shell_output_bounded` through the fake shell with an empty HOME.
    #[cfg(unix)]
    fn fake_shell_output(cmd: &str, marker: &str, capacity: usize) -> Option<ShellOutput> {
        let (shell, _dir) = fake_shell();
        let home = tempfile::tempdir().unwrap();
        timed_shell_output_bounded(
            &shell,
            cmd,
            marker,
            Duration::from_secs(30),
            capacity,
            Some(home.path()),
        )
    }

    /// A plain `/bin/sh -c` child with stdout piped, for driving the drain
    /// thread directly.
    #[cfg(unix)]
    fn piped_child(cmd: &str) -> std::process::Child {
        Command::new("/bin/sh")
            .arg("-c")
            .arg(cmd)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap()
    }

    /// `chunk` holds 2^doublings bytes of `x` when this prefix has run.
    fn doubling(doublings: u32) -> String {
        format!(
            "chunk=x; i=0; while [ \"$i\" -lt {doublings} ]; do chunk=\"$chunk$chunk\"; i=$((i + 1)); done; "
        )
    }

    /// Issue #7's shape: an environment larger than the pipe buffer. Reading
    /// only after exit parked `env -0` in its final write until the timeout
    /// killed it; the concurrent drain reads it through. Eight 32 KiB values
    /// (Linux caps a single environment string at 128 KiB) total 256 KiB:
    /// 16x the macOS pipe buffer, 4x the Linux default.
    #[cfg(unix)]
    #[test]
    fn resolver_reads_an_environment_larger_than_the_pipe_buffer() {
        let marker = "hcom-shell-env-large-test";
        let mut cmd = doubling(15);
        for i in 1..=8 {
            cmd.push_str(&format!(
                "SHELL_ENV_TEST_BIG{i}=\"$chunk\"; export SHELL_ENV_TEST_BIG{i}; "
            ));
        }
        cmd.push_str(&format!(
            "printf %s \"${MARKER_VAR}\"; env -0; printf %s \"${MARKER_VAR}\""
        ));

        let output = fake_shell_output(&cmd, marker, MAX_SHELL_OUTPUT).unwrap();
        let env = parse_shell_env_output(&output.stdout, marker, MARKER_VAR).unwrap();

        for i in 1..=8 {
            assert_eq!(
                env.get(&format!("SHELL_ENV_TEST_BIG{i}")).map(String::len),
                Some(1 << 15),
                "variable {i}"
            );
        }
        assert!(env.contains_key("PATH"));
        assert!(output.stdout.len() > 256 * 1024);
    }

    /// Over the capacity the shell is killed and nothing is returned, and the
    /// rejection comes from the budget, not from the timeout: the shell would
    /// otherwise sleep far longer than the few seconds allowed here.
    #[cfg(unix)]
    #[test]
    fn resolver_rejects_output_over_capacity_and_kills_the_shell() {
        let dir = tempfile::tempdir().unwrap();
        let pid_file = dir.path().join("shell.pid");
        let cmd = format!(
            "echo $$ > {pid}; {doubling} printf %s \"$chunk\"; sleep 30",
            pid = pid_file.display(),
            doubling = doubling(20)
        );

        let started = std::time::Instant::now();
        let output = fake_shell_output(&cmd, "hcom-shell-env-capacity-test", 64 * 1024);
        let elapsed = started.elapsed();

        assert!(output.is_none());
        assert!(
            elapsed < Duration::from_secs(5),
            "rejected by the timeout, not the budget: {elapsed:?}"
        );
        let pid: i32 = fs::read_to_string(&pid_file)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        assert!(wait_for_process_exit(pid));
    }

    /// A quiet pipe is not the end of the output: the drain waits for the
    /// closing marker from a writer that pauses after the environment.
    #[cfg(unix)]
    #[test]
    fn resolver_waits_for_a_writer_that_pauses_mid_stream() {
        let marker = "hcom-shell-env-pause-test";
        let cmd =
            format!("printf %s \"${MARKER_VAR}\"; env -0; sleep 1; printf %s \"${MARKER_VAR}\"");

        let output = fake_shell_output(&cmd, marker, MAX_SHELL_OUTPUT).unwrap();
        let env = parse_shell_env_output(&output.stdout, marker, MARKER_VAR).unwrap();

        assert!(env.contains_key("PATH"));
    }

    /// A background job from an rc file that inherited stdout keeps the pipe
    /// open after the shell exits. The complete output is returned after the
    /// grace instead of waiting for that job, the job is left alone, and the
    /// drain thread is gone when the resolver returns.
    #[cfg(unix)]
    #[test]
    fn resolver_returns_after_exit_when_a_background_job_keeps_stdout_open() {
        let marker = "hcom-shell-env-background-test";
        let cmd = format!(
            "(sleep 5) & printf %s \"${MARKER_VAR}\"; env -0; printf %s \"${MARKER_VAR}\"; exit 0"
        );

        let started = std::time::Instant::now();
        let output = fake_shell_output(&cmd, marker, MAX_SHELL_OUTPUT).unwrap();
        let env = parse_shell_env_output(&output.stdout, marker, MARKER_VAR).unwrap();

        assert!(env.contains_key("PATH"));
        assert!(
            started.elapsed() < Duration::from_secs(4),
            "the resolver waited for the background job: {:?}",
            started.elapsed()
        );
    }

    /// Everything the drain queued while the parent was busy reaches the
    /// consumer, in order and followed by EOF: a late consumer never sees a
    /// prefix as the whole.
    #[cfg(unix)]
    #[test]
    fn drain_hands_queued_output_and_eof_to_a_late_consumer() {
        // 200 KiB fits the 1 MiB queue, so the producer finishes while the
        // consumer sleeps.
        let mut child = piped_child(&format!("{} printf %s \"$chunk\"; exit 0", doubling(17)));
        let drain = Drain::start(child.stdout.take().unwrap(), MAX_SHELL_OUTPUT).unwrap();
        std::thread::sleep(Duration::from_millis(500));

        let mut total = 0usize;
        let mut messages = Vec::new();
        loop {
            match drain.rx.recv_timeout(Duration::from_secs(5)).unwrap() {
                Drained::Chunk(bytes) => {
                    assert!(bytes.iter().all(|b| *b == b'x'));
                    total += bytes.len();
                }
                Drained::Eof => {
                    messages.push("eof");
                    break;
                }
                Drained::OverCapacity => panic!("budget hit under the cap"),
                Drained::Failed => panic!("read failed"),
            }
        }
        assert_eq!(total, 1 << 17);
        assert_eq!(messages, ["eof"]);
        drain.finish();
        let _ = child.wait();
    }

    /// The budget is enforced before a chunk is queued: with the consumer
    /// asleep, a producer streaming 1 MiB against a 100 KiB budget ends with
    /// OverCapacity and no more than the budget ever queued.
    #[cfg(unix)]
    #[test]
    fn drain_stops_at_the_byte_budget_before_enqueueing() {
        let mut child = piped_child(&format!("{} printf %s \"$chunk\"", doubling(20)));
        let budget = 100 * 1024;
        let drain = Drain::start(child.stdout.take().unwrap(), budget).unwrap();
        std::thread::sleep(Duration::from_millis(500));

        let mut total = 0usize;
        let last = loop {
            match drain.rx.recv_timeout(Duration::from_secs(5)).unwrap() {
                Drained::Chunk(bytes) => total += bytes.len(),
                other => break other,
            }
        };
        assert!(matches!(last, Drained::OverCapacity));
        assert!(
            total <= budget,
            "queued {total} bytes over a {budget} byte budget"
        );
        drain.finish();
        let _ = child.kill();
        let _ = child.wait();
    }

    /// With a descendant holding stdout open there is no EOF; finish() still
    /// ends the drain thread within its poll interval.
    #[cfg(unix)]
    #[test]
    fn drain_thread_ends_when_told_while_a_descendant_holds_stdout() {
        let mut child = piped_child("(sleep 3) & printf ready; exit 0");
        let drain = Drain::start(child.stdout.take().unwrap(), MAX_SHELL_OUTPUT).unwrap();
        let _ = child.wait();
        match drain.rx.recv_timeout(Duration::from_secs(5)).unwrap() {
            Drained::Chunk(bytes) => assert_eq!(bytes, b"ready"),
            _ => panic!("expected the chunk before the pipe went quiet"),
        }
        assert!(
            drain.rx.recv_timeout(Duration::from_millis(300)).is_err(),
            "no EOF while the job holds the pipe"
        );

        let started = std::time::Instant::now();
        drain.finish();
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "finish waited for the descendant: {:?}",
            started.elapsed()
        );
    }

    #[test]
    fn output_is_complete_requires_the_closing_marker_after_a_nul() {
        let marker = "hcom-shell-env-m";
        let complete = format!("{marker}A=1\0{MARKER_VAR}={marker}\0{marker}");
        assert!(output_is_complete(complete.as_bytes(), marker));
        // Cut inside the marker variable's value: ends with the marker bytes
        // after `=`, not after a NUL.
        let truncated = format!("{marker}A=1\0{MARKER_VAR}={marker}");
        assert!(!output_is_complete(truncated.as_bytes(), marker));
        assert!(!output_is_complete(marker.as_bytes(), marker));
        assert!(!output_is_complete(b"", marker));
    }

    #[cfg(unix)]
    #[test]
    fn timeout_kills_shell_process_group() {
        let shell = test_shell_path();
        let dir = tempfile::tempdir().unwrap();
        let pid_file = dir.path().join("resolver-pids");
        let pid_file_str = pid_file.to_string_lossy();
        let pid_file_arg = shell_words::quote(pid_file_str.as_ref());
        // The killed scope is the shell's process group. An interactive login
        // shell may give `&` jobs their own group (job control), so descendant
        // cleanup is best-effort and not asserted here.
        let cmd = format!("printf '%s' \"$$\" > {pid_file_arg}; sleep 30");

        let output = timed_shell_output_with_timeout(
            &shell,
            &cmd,
            "process-group-test",
            Duration::from_millis(500),
        );

        assert!(output.is_none());
        let pids = fs::read_to_string(pid_file).unwrap();
        let shell_pid = pids.trim().parse::<i32>().unwrap();

        assert!(wait_for_process_exit(shell_pid));
    }

    #[cfg(unix)]
    fn test_shell_path() -> PathBuf {
        ["/bin/sh", "/usr/bin/sh"]
            .into_iter()
            .map(PathBuf::from)
            .find(|path| path.exists())
            .or_else(shell_path)
            .expect("a POSIX-compatible shell is required for this test")
    }

    #[cfg(unix)]
    fn wait_for_process_exit(pid: i32) -> bool {
        for _ in 0..100 {
            if process_has_exited(pid) {
                return true;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        false
    }

    #[cfg(unix)]
    fn process_has_exited(pid: i32) -> bool {
        use nix::errno::Errno;
        use nix::sys::signal::kill;
        use nix::unistd::Pid;

        if matches!(kill(Pid::from_raw(pid), None), Err(Errno::ESRCH)) {
            return true;
        }

        #[cfg(any(target_os = "android", target_os = "linux"))]
        if let Ok(stat) = fs::read_to_string(format!("/proc/{pid}/stat")) {
            return stat
                .rsplit_once(") ")
                .and_then(|(_, fields)| fields.chars().next())
                == Some('Z');
        }

        false
    }

    #[test]
    #[serial]
    fn clean_shell_seed_env_excludes_parent_tool_contamination() {
        unsafe { std::env::set_var("CODEX_CI", "1") };
        unsafe { std::env::set_var("NO_COLOR", "1") };
        unsafe { std::env::set_var("CARGO_MANIFEST_DIR", "/tmp/hcom") };

        let env = clean_shell_seed_env(Path::new("/bin/zsh"));

        assert_eq!(env.get("SHELL").map(String::as_str), Some("/bin/zsh"));
        assert!(!env.contains_key("CODEX_CI"));
        assert!(!env.contains_key("NO_COLOR"));
        assert!(!env.contains_key("CARGO_MANIFEST_DIR"));

        unsafe { std::env::remove_var("CODEX_CI") };
        unsafe { std::env::remove_var("NO_COLOR") };
        unsafe { std::env::remove_var("CARGO_MANIFEST_DIR") };
    }
}
