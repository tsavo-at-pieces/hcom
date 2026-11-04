//! Process-control primitives: liveness, termination, and session/group setup.
//!
//! Unix uses `libc`/`nix` signals and `setsid`; Windows uses the Win32 process
//! and job-object APIs. See the module-level docs in [`crate::sys`].

use std::process::Command;

/// Observes a pipe client's stdout while [`run_stdio_child`] copies it. The
/// tap only reads: every byte reaches hcom's stdout unchanged and in order.
pub trait StdoutTap: Send {
    /// A complete line without its `\n`, before the read that completed it is
    /// written, so anything recorded here precedes the client's reaction.
    fn observe(&mut self, line: &[u8]);
    /// Everything read so far has been written.
    fn after_write(&mut self);
}

/// Run a foreground pipe client without a shell or PTY. The hcom process
/// remains the native child's parent and forwards cancellation to its group.
/// SIGCHLD wakes the same loop as cancellation: no polling, and no signalling
/// thread can race the reaping of a PID that the OS might reuse.
///
/// With a `stdout_tap`, the child writes into a pipe that a thread copies to
/// hcom's own stdout; otherwise it inherits that stdout directly.
#[cfg(unix)]
pub fn run_stdio_child(
    command: &mut Command,
    stdout_tap: Option<Box<dyn StdoutTap>>,
    on_spawn: impl FnOnce(u32) -> anyhow::Result<()>,
) -> anyhow::Result<i32> {
    use signal_hook::consts::{SIGCHLD, SIGHUP, SIGINT, SIGTERM};
    use signal_hook::iterator::Signals;
    use std::os::fd::AsFd;
    use std::os::unix::process::ExitStatusExt;
    use std::process::Stdio;

    let mut signals = Signals::new([SIGCHLD, SIGHUP, SIGINT, SIGTERM])?;
    let stdout_copy = match stdout_tap {
        Some(tap) => Some((std::io::stdout().as_fd().try_clone_to_owned()?, tap)),
        None => None,
    };
    command
        .stdin(Stdio::inherit())
        .stdout(if stdout_copy.is_some() {
            Stdio::piped()
        } else {
            Stdio::inherit()
        })
        .stderr(Stdio::inherit());
    detach_session(command);
    let mut child = command.spawn()?;
    let pid = child.id();
    let mut forwarder = None;
    let result = (|| {
        if let (Some(source), Some((sink, tap))) = (child.stdout.take(), stdout_copy) {
            forwarder = Some(tapped_stdout::Forwarder::spawn(source, sink, tap)?);
        }
        on_spawn(pid)?;
        loop {
            if let Some(status) = child.try_wait()? {
                return Ok(status
                    .code()
                    .unwrap_or_else(|| 128 + status.signal().unwrap_or(1)));
            }
            for signal in signals.wait() {
                // The child stays unreaped until this single-threaded loop
                // observes it. Never signal after a successful wait.
                if let Some(status) = child.try_wait()? {
                    return Ok(status
                        .code()
                        .unwrap_or_else(|| 128 + status.signal().unwrap_or(1)));
                }
                if signal != SIGCHLD {
                    // SAFETY: this is our unreaped child's own process group.
                    unsafe {
                        libc::kill(-(pid as i32), signal);
                    }
                }
            }
        }
    })();
    if result.is_err() {
        kill_group(pid);
        let _ = child.wait();
    }
    if let Some(forwarder) = forwarder {
        forwarder.finish(tapped_stdout::DRAIN_TIMEOUT);
    }
    result
}

#[cfg(not(unix))]
pub fn run_stdio_child(
    _command: &mut Command,
    _stdout_tap: Option<Box<dyn StdoutTap>>,
    _on_spawn: impl FnOnce(u32) -> anyhow::Result<()>,
) -> anyhow::Result<i32> {
    anyhow::bail!("--stdio is currently supported on Unix only")
}

/// The copy of a pipe client's stdout behind a [`StdoutTap`].
#[cfg(unix)]
mod tapped_stdout {
    use super::StdoutTap;
    use std::io::{ErrorKind, Read};
    use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, mpsc};
    use std::time::Duration;

    /// Longest line a tap is shown. Longer lines are still forwarded whole.
    const LINE_LIMIT: usize = 16 * 1024 * 1024;

    /// How long the copy may continue after the child exits. A descendant
    /// that inherited the pipe can keep it open indefinitely.
    pub(super) const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

    /// Copies the child's stdout to hcom's own on a dedicated thread.
    pub(super) struct Forwarder {
        /// Disconnects when the copy thread exits.
        done: mpsc::Receiver<()>,
        /// Cleared once hcom stops waiting, so a late line can't reach the tap.
        tap_open: Arc<AtomicBool>,
    }

    impl Forwarder {
        pub(super) fn spawn(
            source: std::process::ChildStdout,
            sink: OwnedFd,
            tap: Box<dyn StdoutTap>,
        ) -> std::io::Result<Self> {
            let (done_tx, done) = mpsc::channel();
            let tap_open = Arc::new(AtomicBool::new(true));
            let lines = LineTap::new(tap, tap_open.clone(), LINE_LIMIT);
            std::thread::Builder::new()
                .name("stdio-stdout".into())
                .spawn(move || {
                    let _done = done_tx;
                    forward(source, |bytes| write_all(sink.as_fd(), bytes), lines);
                })?;
            Ok(Self { done, tap_open })
        }

        /// Wait for the child's last bytes, which normally end in EOF as it exits.
        pub(super) fn finish(self, timeout: Duration) {
            if let Err(mpsc::RecvTimeoutError::Timeout) = self.done.recv_timeout(timeout) {
                self.tap_open.store(false, Ordering::Release);
                crate::log::log_warn(
                    "launcher",
                    "stdio.stdout_drain_timeout",
                    "a descendant still holds the native stdout pipe",
                );
            }
        }
    }

    /// Copy `source` to `write` until EOF or a failed write. Each complete
    /// line is shown to the tap before the chunk that completes it is written.
    fn forward(
        mut source: impl Read,
        mut write: impl FnMut(&[u8]) -> std::io::Result<()>,
        mut lines: LineTap,
    ) {
        let mut buf = vec![0; 64 * 1024];
        loop {
            let read = match source.read(&mut buf) {
                Ok(0) => return,
                Ok(read) => read,
                Err(error) if error.kind() == ErrorKind::Interrupted => continue,
                Err(error) => {
                    crate::log::log_warn(
                        "launcher",
                        "stdio.stdout_read_failed",
                        &error.to_string(),
                    );
                    return;
                }
            };
            lines.observe(&buf[..read]);
            if let Err(error) = write(&buf[..read]) {
                // Returning drops the read end, so the child gets the same
                // EPIPE it would have received writing to the client's pipe.
                if error.kind() != ErrorKind::BrokenPipe {
                    crate::log::log_warn(
                        "launcher",
                        "stdio.stdout_write_failed",
                        &error.to_string(),
                    );
                }
                return;
            }
            lines.after_write();
        }
    }

    /// Write all of `bytes` without changing the file status flags: the
    /// client may have made its end nonblocking.
    fn write_all(fd: BorrowedFd<'_>, mut bytes: &[u8]) -> std::io::Result<()> {
        use nix::errno::Errno;
        use nix::poll::{PollFd, PollFlags, PollTimeout, poll};

        while !bytes.is_empty() {
            match nix::unistd::write(fd, bytes) {
                Ok(0) => return Err(ErrorKind::WriteZero.into()),
                Ok(written) => bytes = &bytes[written..],
                Err(Errno::EINTR) => {}
                Err(Errno::EAGAIN) => {
                    let _ = poll(
                        &mut [PollFd::new(fd, PollFlags::POLLOUT)],
                        PollTimeout::NONE,
                    );
                }
                Err(errno) => return Err(errno.into()),
            }
        }
        Ok(())
    }

    /// Splits the copied bytes into lines for a [`StdoutTap`]. A tap that
    /// panics is dropped; the copy continues without it.
    struct LineTap {
        tap: Option<Box<dyn StdoutTap>>,
        open: Arc<AtomicBool>,
        limit: usize,
        /// Start of the current line, unless it outgrew `limit`.
        line: Vec<u8>,
        overflowed: bool,
    }

    impl LineTap {
        fn new(tap: Box<dyn StdoutTap>, open: Arc<AtomicBool>, limit: usize) -> Self {
            Self {
                tap: Some(tap),
                open,
                limit,
                line: Vec::new(),
                overflowed: false,
            }
        }

        fn observe(&mut self, mut chunk: &[u8]) {
            if self.tap.is_none() {
                return;
            }
            while let Some(end) = chunk.iter().position(|&byte| byte == b'\n') {
                let head = &chunk[..end];
                if !self.overflowed && self.line.len() + head.len() <= self.limit {
                    if self.line.is_empty() {
                        self.call(|tap| tap.observe(head));
                    } else {
                        let mut line = std::mem::take(&mut self.line);
                        line.extend_from_slice(head);
                        self.call(|tap| tap.observe(&line));
                    }
                }
                self.line.clear();
                self.overflowed = false;
                chunk = &chunk[end + 1..];
            }
            if self.overflowed || self.tap.is_none() {
                return;
            }
            if self.line.len() + chunk.len() <= self.limit {
                self.line.extend_from_slice(chunk);
            } else {
                self.line = Vec::new();
                self.overflowed = true;
            }
        }

        fn after_write(&mut self) {
            self.call(|tap| tap.after_write());
        }

        fn call(&mut self, f: impl FnOnce(&mut dyn StdoutTap)) {
            let Some(tap) = self.tap.as_deref_mut() else {
                return;
            };
            if !self.open.load(Ordering::Acquire)
                || catch_unwind(AssertUnwindSafe(|| f(tap))).is_err()
            {
                self.tap = None;
                self.line = Vec::new();
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::sync::Mutex;

        /// One tap call or sink write, in the order the forwarder made them.
        #[derive(Debug, Clone, PartialEq)]
        enum Copied {
            Observed(Vec<u8>),
            Wrote(Vec<u8>),
            AfterWrite,
        }

        struct RecordingTap {
            log: Arc<Mutex<Vec<Copied>>>,
            panic_on_observe: bool,
        }

        impl StdoutTap for RecordingTap {
            fn observe(&mut self, line: &[u8]) {
                self.log
                    .lock()
                    .unwrap()
                    .push(Copied::Observed(line.to_vec()));
                assert!(!self.panic_on_observe, "tap failure");
            }

            fn after_write(&mut self) {
                self.log.lock().unwrap().push(Copied::AfterWrite);
            }
        }

        fn recording_tap(
            log: &Arc<Mutex<Vec<Copied>>>,
            panic_on_observe: bool,
        ) -> Box<dyn StdoutTap> {
            Box::new(RecordingTap {
                log: log.clone(),
                panic_on_observe,
            })
        }

        /// Serves `data` in pseudo-random read sizes from 1 to `max`.
        struct ChunkedReader {
            data: Vec<u8>,
            offset: usize,
            state: u64,
            max: u64,
        }

        impl Read for ChunkedReader {
            fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                self.state ^= self.state << 13;
                self.state ^= self.state >> 7;
                self.state ^= self.state << 17;
                let size = (1 + (self.state % self.max) as usize)
                    .min(buf.len())
                    .min(self.data.len() - self.offset);
                buf[..size].copy_from_slice(&self.data[self.offset..self.offset + size]);
                self.offset += size;
                Ok(size)
            }
        }

        fn copy_with_tap(
            data: &[u8],
            seed: u64,
            max_read: u64,
            open: bool,
            panic_on_observe: bool,
        ) -> Vec<Copied> {
            let log = Arc::new(Mutex::new(Vec::new()));
            let source = ChunkedReader {
                data: data.to_vec(),
                offset: 0,
                state: seed,
                max: max_read,
            };
            let sink = log.clone();
            forward(
                source,
                |bytes| {
                    sink.lock().unwrap().push(Copied::Wrote(bytes.to_vec()));
                    Ok(())
                },
                LineTap::new(
                    recording_tap(&log, panic_on_observe),
                    Arc::new(AtomicBool::new(open)),
                    256,
                ),
            );
            log.lock().unwrap().clone()
        }

        fn written(copied: &[Copied]) -> Vec<u8> {
            copied
                .iter()
                .filter_map(|event| match event {
                    Copied::Wrote(bytes) => Some(bytes.as_slice()),
                    _ => None,
                })
                .flatten()
                .copied()
                .collect()
        }

        #[test]
        fn copy_is_byte_exact_and_shows_lines_before_writing_them() {
            let mut data = Vec::new();
            let mut lines = Vec::new();
            let mut line = |data: &mut Vec<u8>, bytes: &[u8], shown: bool| {
                data.extend_from_slice(bytes);
                if shown {
                    lines.push((bytes.to_vec(), data.len()));
                }
                data.push(b'\n');
            };
            line(
                &mut data,
                "{\"type\":\"assistant\",\"text\":\"h\u{e9}llo \u{2713} \u{1f389}\"}".as_bytes(),
                true,
            );
            line(&mut data, b"{\"type\":\"user\"}\r", true);
            line(&mut data, b"", true);
            // Over the 256-byte test limit: forwarded whole, never shown.
            line(&mut data, &[b'x'; 300], false);
            line(
                &mut data,
                "{\"type\":\"result\",\"result\":\"\u{2713}\"}".as_bytes(),
                true,
            );
            // A final line without newline, ending inside a UTF-8 sequence.
            data.extend_from_slice(b"partial \xe2\x9c");

            for seed in 1..=64 {
                for max_read in [1, 2, 3, 7, 64, 100_000] {
                    let copied = copy_with_tap(&data, seed, max_read, true, false);
                    assert_eq!(written(&copied), data, "seed={seed} max_read={max_read}");
                    let mut sent = 0;
                    let mut shown = lines.iter();
                    for (index, event) in copied.iter().enumerate() {
                        match event {
                            Copied::Observed(observed) => {
                                let (line, newline) = shown.next().expect("unexpected line");
                                assert_eq!(observed, line);
                                // Shown before its newline is written, and the
                                // next write contains that newline.
                                assert!(sent <= *newline);
                                let Some(Copied::Wrote(next)) = copied[index..]
                                    .iter()
                                    .find(|event| matches!(event, Copied::Wrote(_)))
                                else {
                                    panic!("line shown but never written");
                                };
                                assert!(sent + next.len() > *newline);
                            }
                            Copied::Wrote(bytes) => {
                                sent += bytes.len();
                                assert_eq!(copied.get(index + 1), Some(&Copied::AfterWrite));
                            }
                            Copied::AfterWrite => {}
                        }
                    }
                    assert!(shown.next().is_none(), "seed={seed} max_read={max_read}");
                }
            }
        }

        #[test]
        fn a_failed_or_closed_tap_never_interrupts_the_copy() {
            let data = b"one\ntwo\nthree\n";
            let copied = copy_with_tap(data, 7, 3, true, true);
            assert_eq!(written(&copied), data);
            // The first line panics the tap; nothing reaches it afterwards.
            let failed = copied
                .iter()
                .position(|event| matches!(event, Copied::Observed(_)))
                .unwrap();
            assert_eq!(copied[failed], Copied::Observed(b"one".to_vec()));
            assert!(
                copied[failed + 1..]
                    .iter()
                    .all(|event| matches!(event, Copied::Wrote(_)))
            );

            let copied = copy_with_tap(data, 7, 3, false, false);
            assert_eq!(written(&copied), data);
            assert!(copied.iter().all(|event| matches!(event, Copied::Wrote(_))));
        }

        #[test]
        fn a_closed_client_stdout_closes_the_child_pipe_too() {
            use std::io::Write;

            let (source, mut child_stdout) = std::io::pipe().unwrap();
            let (client, sink) = std::io::pipe().unwrap();
            drop(client);
            let log = Arc::new(Mutex::new(Vec::new()));
            let tap = recording_tap(&log, false);
            let copier = std::thread::spawn(move || {
                forward(
                    source,
                    |bytes| write_all(sink.as_fd(), bytes),
                    LineTap::new(tap, Arc::new(AtomicBool::new(true)), 256),
                );
            });
            child_stdout.write_all(b"{\"type\":\"result\"}\n").unwrap();
            copier.join().unwrap();
            assert_eq!(
                child_stdout.write_all(b"next\n").unwrap_err().kind(),
                ErrorKind::BrokenPipe
            );
            // Shown, but never confirmed as written to the client.
            assert_eq!(
                *log.lock().unwrap(),
                [Copied::Observed(b"{\"type\":\"result\"}".to_vec())]
            );
        }

        #[test]
        fn a_nonblocking_client_stdout_is_waited_for_not_reconfigured() {
            use nix::fcntl::{FcntlArg, OFlag, fcntl};

            let (mut client, sink) = std::io::pipe().unwrap();
            let flags = OFlag::from_bits_truncate(fcntl(sink.as_fd(), FcntlArg::F_GETFL).unwrap());
            fcntl(sink.as_fd(), FcntlArg::F_SETFL(flags | OFlag::O_NONBLOCK)).unwrap();
            let data: Vec<u8> = (0..1_000_000u32).map(|i| (i % 251) as u8).collect();
            let reader = std::thread::spawn(move || {
                // Let the pipe fill first, so the writer meets EAGAIN.
                std::thread::sleep(Duration::from_millis(100));
                let mut received = Vec::new();
                client.read_to_end(&mut received).unwrap();
                received
            });
            write_all(sink.as_fd(), &data).unwrap();
            let flags = OFlag::from_bits_truncate(fcntl(sink.as_fd(), FcntlArg::F_GETFL).unwrap());
            assert!(flags.contains(OFlag::O_NONBLOCK));
            drop(sink);
            assert!(reader.join().unwrap() == data);
        }
    }
}

/// Stable identity for one PID incarnation.
///
/// PIDs are reusable, so liveness checks for persisted ownership records must
/// compare the process creation time as well as the numeric PID.
pub fn identity(pid: u32) -> Option<String> {
    process_identity_platform(pid)
}

/// Whether this process runs inside an OS sandbox. Children inherit it for
/// life, so long-lived daemons must not be started from one.
///
/// macOS asks the kernel (covers any Seatbelt profile). Elsewhere only Codex's
/// marker is known; Linux sandboxes set nothing reliable.
pub fn is_sandboxed() -> bool {
    std::env::var_os("CODEX_SANDBOX").is_some() || is_sandboxed_platform()
}

#[cfg(target_os = "macos")]
fn is_sandboxed_platform() -> bool {
    unsafe extern "C" {
        fn sandbox_check(
            pid: libc::pid_t,
            operation: *const libc::c_char,
            kind: libc::c_int,
            ...
        ) -> libc::c_int;
    }
    // SAFETY: a null operation with SANDBOX_FILTER_NONE (0) only asks whether
    // the PID is sandboxed; no further arguments are read.
    unsafe { sandbox_check(libc::getpid(), std::ptr::null(), 0) != 0 }
}

#[cfg(not(target_os = "macos"))]
fn is_sandboxed_platform() -> bool {
    false
}

#[cfg(any(target_os = "android", target_os = "linux"))]
fn process_identity_platform(pid: u32) -> Option<String> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let boot_id = std::fs::read_to_string("/proc/sys/kernel/random/boot_id").ok()?;
    let boot_id = boot_id.trim();
    if boot_id.is_empty() {
        return None;
    }
    // Fields after the final `) ` begin at field 3 (`state`). Linux's process
    // start time is field 22, hence index 19 in this slice. Boot ID keeps the
    // boot-relative tick count unique across restarts.
    let start_ticks = stat.rsplit_once(") ")?.1.split_whitespace().nth(19)?;
    Some(format!("linux:{boot_id}:{start_ticks}"))
}

#[cfg(any(target_os = "ios", target_os = "macos"))]
fn process_identity_platform(pid: u32) -> Option<String> {
    // SAFETY: `info` is a correctly sized writable proc_bsdinfo buffer and
    // proc_pidinfo only fills it for the queried PID.
    let mut info: libc::proc_bsdinfo = unsafe { std::mem::zeroed() };
    let size = std::mem::size_of::<libc::proc_bsdinfo>() as libc::c_int;
    let read = unsafe {
        libc::proc_pidinfo(
            pid as libc::c_int,
            libc::PROC_PIDTBSDINFO,
            0,
            (&mut info as *mut libc::proc_bsdinfo).cast(),
            size,
        )
    };
    (read == size).then(|| format!("apple:{}:{}", info.pbi_start_tvsec, info.pbi_start_tvusec))
}

#[cfg(windows)]
fn process_identity_platform(pid: u32) -> Option<String> {
    creation_ticks_win(pid).map(|ticks| format!("windows:{ticks}"))
}

/// Process creation time in FILETIME ticks (100 ns units), or `None` when the
/// process cannot be opened for query — it has exited, or it is not ours.
///
/// Serves both the process identity above (a PID is only unique alongside the
/// creation time of the incarnation holding it) and the parent/child link check
/// in [`kill_tree_win_checked`].
#[cfg(windows)]
fn creation_ticks_win(pid: u32) -> Option<u64> {
    use windows_sys::Win32::Foundation::{CloseHandle, FILETIME};
    use windows_sys::Win32::System::Threading::{
        GetProcessTimes, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
    };

    // SAFETY: the handle is opened query-only, every FILETIME pointer is
    // valid, and the handle is closed before returning.
    unsafe {
        let handle = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid);
        if handle.is_null() {
            return None;
        }
        let mut created: FILETIME = std::mem::zeroed();
        let mut exited: FILETIME = std::mem::zeroed();
        let mut kernel: FILETIME = std::mem::zeroed();
        let mut user: FILETIME = std::mem::zeroed();
        let ok = GetProcessTimes(handle, &mut created, &mut exited, &mut kernel, &mut user) != 0;
        CloseHandle(handle);
        ok.then_some(((created.dwHighDateTime as u64) << 32) | created.dwLowDateTime as u64)
    }
}

#[cfg(all(
    unix,
    not(any(
        target_os = "android",
        target_os = "ios",
        target_os = "linux",
        target_os = "macos"
    ))
))]
fn process_identity_platform(pid: u32) -> Option<String> {
    let output = Command::new("ps")
        .args(["-p", &pid.to_string(), "-o", "lstart="])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let started = String::from_utf8(output.stdout).ok()?;
    let started = started.trim();
    (!started.is_empty()).then(|| format!("unix:{started}"))
}

/// Whether `pid` still names the same process incarnation as `identity`.
pub fn has_identity(pid: u32, expected: &str) -> bool {
    identity(pid).as_deref() == Some(expected)
}

/// Whether a process with the given PID is currently alive.
///
/// Unix: `kill(pid, 0)`, treating `EPERM` (the process exists but is owned by
/// another user) as alive. Windows: `OpenProcess` followed by
/// `GetExitCodeProcess` — a live handle alone isn't proof of life, since the
/// process object stays valid as long as any handle (ours, or a job object's)
/// is open even after the process has exited. A failure to open with
/// `ERROR_ACCESS_DENIED` also means the process exists.
pub fn is_alive(pid: u32) -> bool {
    #[cfg(unix)]
    {
        // SAFETY: kill(pid, 0) sends no signal; it only checks for existence.
        let ret = unsafe { libc::kill(pid as i32, 0) };
        if ret == 0 {
            return true;
        }
        std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
    }
    #[cfg(windows)]
    {
        use windows_sys::Win32::Foundation::{
            CloseHandle, ERROR_ACCESS_DENIED, GetLastError, STILL_ACTIVE,
        };
        use windows_sys::Win32::System::Threading::{
            GetExitCodeProcess, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
        };
        // SAFETY: query-only access mask; the handle is closed before returning.
        unsafe {
            let handle = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid);
            if !handle.is_null() {
                let mut exit_code = 0u32;
                let ok = GetExitCodeProcess(handle, &mut exit_code) != 0;
                CloseHandle(handle);
                return ok && exit_code == STILL_ACTIVE as u32;
            }
            GetLastError() == ERROR_ACCESS_DENIED
        }
    }
}

/// Forcefully terminate a process by PID. Best-effort; returns whether the
/// request was delivered.
///
/// Unix: `SIGKILL`. Windows: `TerminateProcess`.
pub fn kill(pid: u32) -> bool {
    #[cfg(unix)]
    {
        // SAFETY: standard kill(2) with a valid signal number.
        unsafe { libc::kill(pid as i32, libc::SIGKILL) == 0 }
    }
    #[cfg(windows)]
    {
        terminate_win(pid)
    }
}

/// Request graceful termination of a process by PID. Best-effort; returns
/// whether the request was delivered. Like Unix `SIGTERM`, this only *asks* the
/// process to exit — it does not force it. Callers that need a guarantee must
/// wait and then escalate to [`kill`].
///
/// Unix: `SIGTERM`. Windows: `CTRL_BREAK_EVENT` to the target's process group
/// (see [`request_shutdown_win`]), which mirrors the SIGTERM-sets-a-flag model.
pub fn terminate(pid: u32) -> bool {
    #[cfg(unix)]
    {
        // SAFETY: standard kill(2) with a valid signal number.
        unsafe { libc::kill(pid as i32, libc::SIGTERM) == 0 }
    }
    #[cfg(windows)]
    {
        request_shutdown_win(pid)
    }
}

/// Outcome of signalling a process group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupSignal {
    /// The signal was delivered.
    Sent,
    /// No such process/group exists (already gone).
    NotFound,
    /// The caller lacks permission to signal the group.
    #[cfg(unix)]
    PermissionDenied,
    /// Any other failure.
    #[cfg(unix)]
    Other,
}

/// Send a graceful termination request to a process group by PID.
///
/// Unix: `killpg(SIGTERM)`. Windows has no process-group signal and no graceful
/// per-tree signal for an unrelated process, so this maps to a forceful
/// process-tree termination ([`kill_tree_win`]) — the same as [`kill_group`].
pub fn terminate_group(pid: u32) -> GroupSignal {
    #[cfg(unix)]
    {
        signal_group_unix(pid, libc::SIGTERM)
    }
    #[cfg(windows)]
    {
        kill_tree_win(pid)
    }
}

/// Forcefully kill a process group by PID.
///
/// Unix: `killpg(SIGKILL)`. Windows has no process groups in the POSIX sense, so
/// this terminates the target PID **and all of its descendants** via a process
/// snapshot ([`kill_tree_win`]). This matters because hcom records the PID of
/// the launcher (e.g. the background `powershell` host), and the real agent runs
/// as its child; killing only the recorded PID would orphan the agent.
pub fn kill_group(pid: u32) -> GroupSignal {
    #[cfg(unix)]
    {
        signal_group_unix(pid, libc::SIGKILL)
    }
    #[cfg(windows)]
    {
        kill_tree_win(pid)
    }
}

#[cfg(unix)]
fn signal_group_unix(pid: u32, sig: libc::c_int) -> GroupSignal {
    // SAFETY: killpg with a valid signal number; return value is checked.
    let ret = unsafe { libc::killpg(pid as i32, sig) };
    if ret == 0 {
        return GroupSignal::Sent;
    }
    match std::io::Error::last_os_error().raw_os_error() {
        Some(libc::ESRCH) => GroupSignal::NotFound,
        Some(libc::EPERM) => GroupSignal::PermissionDenied,
        _ => GroupSignal::Other,
    }
}

/// Replace the current process with the given command.
///
/// Unix uses `exec()` and only returns (an error) on failure. Windows has no
/// `exec`, so it spawns the command, waits, and exits with the child's status
/// code — likewise not returning on success.
pub fn exec_replace(mut cmd: Command) -> std::io::Error {
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        cmd.exec()
    }
    #[cfg(windows)]
    {
        match cmd.status() {
            Ok(status) => std::process::exit(status.code().unwrap_or(1)),
            Err(e) => e,
        }
    }
}

/// Kill a child process together with its process group.
///
/// Unix: `killpg(SIGKILL)` on the child's group (set up via [`detach_session`]),
/// falling back to `Child::kill` if the group signal fails. Windows: terminates
/// the child's whole process tree ([`kill_tree_win`]).
pub fn kill_child_group(child: &mut std::process::Child) {
    #[cfg(unix)]
    {
        use nix::sys::signal::{Signal, killpg};
        use nix::unistd::Pid;

        if let Ok(raw_pid) = i32::try_from(child.id())
            && killpg(Pid::from_raw(raw_pid), Signal::SIGKILL).is_ok()
        {
            return;
        }
    }

    #[cfg(windows)]
    {
        // kill_tree_win_checked terminates `child` itself (with the hcom-kill
        // sentinel exit code 130, via terminate_win) and its whole descendant
        // tree. If root's own termination is confirmed, return rather than
        // falling through to child.kill() below: TerminateProcess only
        // requests termination asynchronously, so a second call racing the
        // first can overwrite the sentinel exit code with a different one,
        // corrupting the EXIT_WAS_KILLED check that reads it back.
        //
        // If root's termination was NOT confirmed, it may just be that the
        // fresh-by-PID OpenProcess inside terminate_win failed for a reason
        // other than "already gone" (a handle-table limit, or AV/EDR hooking
        // around freshly-opened handles, for instance) — fall through to
        // child.kill() as a retry via the handle Command already has open,
        // which a failing OpenProcess-by-PID wouldn't affect.
        let (_, root_terminated) = kill_tree_win_checked(child.id());
        if root_terminated {
            return;
        }
    }

    let _ = child.kill();
}

/// Put a not-yet-spawned [`Command`] into its own session / process group, so
/// the resulting child can be signalled as a group and is detached from the
/// parent's controlling terminal.
///
/// Unix: `setsid()` via a `pre_exec` hook. Windows: `CREATE_NEW_PROCESS_GROUP`
/// combined with `CREATE_NO_WINDOW`, so the child neither shares the parent's
/// console nor dies when that console closes.
pub fn detach_session(command: &mut Command) {
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        // SAFETY: setsid() runs in the child between fork and exec and is
        // async-signal-safe.
        unsafe {
            command.pre_exec(|| {
                if libc::setsid() == -1 {
                    Err(std::io::Error::last_os_error())
                } else {
                    Ok(())
                }
            });
        }
    }
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        command.creation_flags(CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW);
    }
}

/// Best-effort graceful shutdown request on Windows, the analogue of Unix
/// `SIGTERM`: it asks the target to exit but does not force it.
///
/// Sends `CTRL_BREAK_EVENT` to the target's process group. Processes spawned via
/// [`detach_session`] use `CREATE_NEW_PROCESS_GROUP`, so their process-group id
/// equals their PID, and a process that has registered a handler (see
/// `sys::signal::register_term`) sets its shutdown flag in response.
///
/// Delivery only reaches a process that shares the caller's console. A relay
/// worker spawned from a different console (the common case) does not receive
/// it, and this returns `false`; the caller's wait + [`kill`] fallback then
/// terminates it. This matches Unix, where `SIGTERM` likewise only *requests*
/// shutdown and callers escalate to `SIGKILL`.
#[cfg(windows)]
fn request_shutdown_win(pid: u32) -> bool {
    use windows_sys::Win32::System::Console::{CTRL_BREAK_EVENT, GenerateConsoleCtrlEvent};
    // SAFETY: plain FFI call, no pointers; the process-group id is the target
    // PID (valid because detach_session uses CREATE_NEW_PROCESS_GROUP).
    unsafe { GenerateConsoleCtrlEvent(CTRL_BREAK_EVENT, pid) != 0 }
}

#[cfg(windows)]
fn terminate_win(pid: u32) -> bool {
    use windows_sys::Win32::Foundation::CloseHandle;
    use windows_sys::Win32::System::Threading::{OpenProcess, PROCESS_TERMINATE, TerminateProcess};
    // SAFETY: opens a terminate-only handle, closes it before returning.
    unsafe {
        let handle = OpenProcess(PROCESS_TERMINATE, 0, pid);
        if handle.is_null() {
            return false;
        }
        // Exit code 130 (128 + SIGINT) is the hcom sentinel for "externally
        // killed via hcom kill". The pty proxy reads this back from child.wait()
        // to set EXIT_WAS_KILLED before the delivery thread records exit status.
        let ok = TerminateProcess(handle, 130) != 0;
        CloseHandle(handle);
        ok
    }
}

/// Snapshot every live process's pid -> parent_pid link via
/// `CreateToolhelp32Snapshot`, retrying once on transient failure.
///
/// Returns `None` if the snapshot could not be taken even after the retry.
#[cfg(windows)]
fn snapshot_parents() -> Option<std::collections::HashMap<u32, u32>> {
    use std::collections::HashMap;
    use windows_sys::Win32::Foundation::{CloseHandle, INVALID_HANDLE_VALUE};
    use windows_sys::Win32::System::Diagnostics::ToolHelp::{
        CreateToolhelp32Snapshot, PROCESSENTRY32W, Process32FirstW, Process32NextW,
        TH32CS_SNAPPROCESS,
    };

    let mut parents: HashMap<u32, u32> = HashMap::new();
    // SAFETY: snapshot handle is closed before returning; the PROCESSENTRY32W is
    // fully initialized (dwSize set) before the enumeration calls.
    unsafe {
        let mut snapshot = CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0);
        if snapshot == INVALID_HANDLE_VALUE {
            // Snapshot can fail transiently under high load; retry once.
            snapshot = CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0);
        }
        if snapshot == INVALID_HANDLE_VALUE {
            return None;
        }
        let mut entry: PROCESSENTRY32W = std::mem::zeroed();
        entry.dwSize = std::mem::size_of::<PROCESSENTRY32W>() as u32;
        if Process32FirstW(snapshot, &mut entry) != 0 {
            loop {
                parents.insert(entry.th32ProcessID, entry.th32ParentProcessID);
                if Process32NextW(snapshot, &mut entry) == 0 {
                    break;
                }
            }
        }
        CloseHandle(snapshot);
    }
    Some(parents)
}

/// Spawn a detached child without leaking the caller's captured stdio handles.
///
/// On Windows, `Command` must enable handle inheritance for explicitly supplied
/// child stdio (for example a background log file). Without an explicit handle
/// list, that can also inherit the hcom CLI's own stdout/stderr pipes. A caller
/// using `Command::output()` then waits forever for EOF after hcom exits because
/// the long-lived agent still owns duplicate pipe handles.
pub fn spawn_detached(command: &mut Command) -> std::io::Result<std::process::Child> {
    detach_session(command);

    #[cfg(not(windows))]
    {
        command.spawn()
    }

    #[cfg(windows)]
    {
        use std::sync::{Mutex, OnceLock};
        use windows_sys::Win32::Foundation::{
            GetHandleInformation, HANDLE_FLAG_INHERIT, SetHandleInformation,
        };
        use windows_sys::Win32::System::Console::{
            GetStdHandle, STD_ERROR_HANDLE, STD_INPUT_HANDLE, STD_OUTPUT_HANDLE,
        };

        static SPAWN_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        let _guard = SPAWN_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|error| error.into_inner());

        let mut restored = Vec::new();
        for kind in [STD_INPUT_HANDLE, STD_OUTPUT_HANDLE, STD_ERROR_HANDLE] {
            // SAFETY: GetStdHandle returns a borrowed process handle. We only
            // query/change its inheritance flag and restore it before returning.
            unsafe {
                let handle = GetStdHandle(kind);
                if handle.is_null() {
                    continue;
                }
                let mut flags = 0;
                if GetHandleInformation(handle, &mut flags) != 0
                    && flags & HANDLE_FLAG_INHERIT != 0
                    && SetHandleInformation(handle, HANDLE_FLAG_INHERIT, 0) != 0
                {
                    restored.push(handle);
                }
            }
        }

        let child = command.spawn();
        for handle in restored {
            // SAFETY: these are the same live borrowed standard handles whose
            // inheritance flag was cleared above.
            unsafe {
                SetHandleInformation(handle, HANDLE_FLAG_INHERIT, HANDLE_FLAG_INHERIT);
            }
        }
        child
    }
}

/// Whether `child` can really be a child of `parent`, judged by creation time.
///
/// A process cannot be older than the process that created it, so a candidate
/// whose creation time precedes its claimed parent's is a stale parent link:
/// the recorded parent PID belonged to a process that has since exited and had
/// its PID recycled by the (younger) process now holding it. Windows keeps
/// reporting the numeric PID of the original parent, so the orphan looks like a
/// child of whatever inherited that number.
///
/// `None` on either side means the process could not be opened for query, so
/// the link can neither be confirmed nor disproved — those are accepted, which
/// leaves the pre-existing behaviour untouched wherever this check has nothing
/// to say. A process we cannot query is one we generally cannot terminate
/// either.
#[cfg(windows)]
fn child_link_is_plausible(parent_ticks: Option<u64>, child_ticks: Option<u64>) -> bool {
    match (parent_ticks, child_ticks) {
        (Some(parent), Some(child)) => child >= parent,
        _ => true,
    }
}

/// Number of extra re-scan rounds run after the initial kill pass, to catch
/// descendants spawned after the first snapshot (see [`kill_tree_win_checked`]).
#[cfg(windows)]
const RESCAN_ROUNDS: u32 = 2;

/// Delay between re-scan rounds, giving the OS a moment to start tearing down
/// what was just killed before the next snapshot is taken.
#[cfg(windows)]
const RESCAN_DELAY: std::time::Duration = std::time::Duration::from_millis(75);

/// Terminate `root` and all of its descendants.
///
/// Windows has no process groups, so the only general way to "kill the agent and
/// its children" by PID from an unrelated process is to walk the parent/child
/// links in a process snapshot. The full descendant set is collected from a
/// single snapshot *before* any termination, so killing a parent can't strand a
/// child behind a now-stale parent PID (Windows does not reparent orphans).
///
/// Returns `Sent` if the root was present (and termination was attempted),
/// `NotFound` if no live process had the root PID. Like `killpg`, individual
/// termination failures are best-effort and don't change the result.
///
/// Caveat: PID reuse can make a parent link stale, the same theoretical race
/// `taskkill /T` carries. Links that claim a *pre-existing* process as a child
/// are rejected on creation time (see `child_link_is_plausible`), which covers
/// the direction that can reach a long-lived bystander; what remains is a
/// straggler spawned by whoever recycled a PID this walk already terminated.
#[cfg(windows)]
fn kill_tree_win(root: u32) -> GroupSignal {
    kill_tree_win_checked(root).0
}

/// Same as [`kill_tree_win`], but also reports whether `root`'s own
/// `terminate_win` call specifically succeeded (as opposed to just "root was
/// found in the snapshot"). `kill_tree_win`'s `Sent` doesn't distinguish
/// these — every descendant's individual termination result is discarded —
/// so [`kill_child_group`] uses this directly to decide whether a retry via
/// its own already-open `Child` handle is worthwhile.
///
/// Re-scan for stragglers: a single snapshot is inherently a point-in-time
/// view, so a process spawned by any tree member *after* the snapshot but
/// *before* that member is terminated is invisible to the first pass and would
/// otherwise survive. Unix's `killpg` doesn't have this problem for
/// already-existing group members — the kernel delivers the signal to the
/// whole group atomically — but Windows has no equivalent primitive, so after
/// the first kill pass this takes a couple of follow-up snapshots, each time
/// looking for new processes parented by *any* previously-known tree member
/// (not just `root`, since mid-tree descendants can keep spawning children of
/// their own even after `root` is gone) and killing those too. This narrows
/// the race window considerably but, being fundamentally snapshot-based, does
/// not eliminate it: a straggler spawned after the very last re-scan's
/// snapshot can still escape.
#[cfg(windows)]
fn kill_tree_win_checked(root: u32) -> (GroupSignal, bool) {
    let Some(parents) = snapshot_parents() else {
        // Still can't enumerate descendants; kill only the root. Any
        // surviving descendants will be reaped when the job object
        // closes. This still reports `Sent` (not a distinct "partial"
        // result) — `GroupSignal`/`KillResult` are a flat success/
        // not-found/permission-denied/other set consumed by ~15 call
        // sites across the CLI kill-reporting path and the relay JSON
        // protocol (see commands/kill.rs, relay/control.rs); threading a
        // new "partial" variant through all of them is a larger, separate
        // change. Logging at least makes this rare degraded path visible
        // instead of leaving zero trace anywhere.
        crate::log::log_error(
            "native",
            "win.kill_tree",
            &format!(
                "CreateToolhelp32Snapshot failed twice; killing root pid {root} only, \
                 descendants may be left running until the job object reaps them"
            ),
        );
        let ok = terminate_win(root);
        return (
            if ok {
                GroupSignal::Sent
            } else {
                GroupSignal::NotFound
            },
            ok,
        );
    };

    if !parents.contains_key(&root) {
        return (GroupSignal::NotFound, false);
    }

    // Collect root + all descendants (BFS over the parent links), rejecting any
    // candidate that is older than the tree member claiming it — that link is
    // stale, not a real parent/child edge (see `child_link_is_plausible`).
    //
    // Creation times are cached per PID, and the cache is what makes the check
    // hold up across the re-scan rounds below: it keeps each tree member's time
    // as read while that member was still alive, so a candidate is always
    // compared against the incarnation this walk actually decided to kill.
    let mut ticks: std::collections::HashMap<u32, Option<u64>> = std::collections::HashMap::new();
    let mut tree = vec![root];
    let mut i = 0;
    while i < tree.len() {
        let current = tree[i];
        let parent_ticks = *ticks
            .entry(current)
            .or_insert_with(|| creation_ticks_win(current));
        for (&pid, &ppid) in &parents {
            if ppid != current || tree.contains(&pid) {
                continue;
            }
            let child_ticks = *ticks.entry(pid).or_insert_with(|| creation_ticks_win(pid));
            if child_link_is_plausible(parent_ticks, child_ticks) {
                tree.push(pid);
            }
        }
        i += 1;
    }

    // Terminate children before parents (deepest first) so a parent can't spawn
    // a new child after we've passed it.
    let mut root_terminated = false;
    for &pid in tree.iter().rev() {
        let ok = terminate_win(pid);
        if pid == root {
            root_terminated = ok;
        }
    }

    // Re-scan for stragglers spawned between the snapshot and their parent's
    // termination (see doc comment above).
    //
    // This widens (but does not introduce in kind) the PID-reuse race already
    // called out on `kill_tree_win_checked`: each extra round is another
    // `terminate_win` -> sleep -> re-snapshot cycle, so there's more elapsed
    // time in which a PID this function just terminated could be recycled by
    // an unrelated process that happens to be parented under another PID
    // still in `tree`. The creation-time check applies here too, against the
    // times cached while each tree member was alive, so nothing older than the
    // member claiming it can be swept up; a process spawned *by* whoever
    // recycled such a PID is younger and still passes. `KillOnDropJob` (see
    // `pty::win::job`) remains the backstop that reaps the real tree regardless.
    //
    // Always run all RESCAN_ROUNDS —
    // round N finding nothing new doesn't mean a straggler can't still appear
    // before round N+1's snapshot, so stopping early would defeat the point of
    // having more than one round. Within a round, expand `tree` to a fixed
    // point (mirroring the initial BFS above) rather than a single pass, so a
    // straggler and *its own* child that both first appear in the same
    // snapshot are both caught in that round regardless of HashMap iteration
    // order.
    for _ in 0..RESCAN_ROUNDS {
        std::thread::sleep(RESCAN_DELAY);
        let Some(parents) = snapshot_parents() else {
            break;
        };
        let before = tree.len();
        loop {
            let start_len = tree.len();
            for (&pid, &ppid) in &parents {
                if !tree.contains(&ppid) || tree.contains(&pid) {
                    continue;
                }
                let parent_ticks = *ticks
                    .entry(ppid)
                    .or_insert_with(|| creation_ticks_win(ppid));
                let child_ticks = *ticks.entry(pid).or_insert_with(|| creation_ticks_win(pid));
                if child_link_is_plausible(parent_ticks, child_ticks) {
                    tree.push(pid);
                }
            }
            if tree.len() == start_len {
                break;
            }
        }
        // Kill only the newly discovered stragglers (deepest-first isn't
        // load-bearing here since these are freshly discovered leaves relative
        // to the known tree, but iterating in reverse discovery order costs
        // nothing and keeps the same "children before parents" spirit).
        for &pid in tree[before..].iter().rev() {
            terminate_win(pid);
        }
    }

    (GroupSignal::Sent, root_terminated)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_alive_current_process() {
        assert!(is_alive(std::process::id()));
    }

    #[test]
    fn test_is_alive_dead_process() {
        assert!(!is_alive(99_999_999));
    }

    #[test]
    fn test_process_identity_is_stable_for_current_process() {
        let pid = std::process::id();
        let first = identity(pid).expect("current process must have a start identity");
        assert_eq!(identity(pid).as_deref(), Some(first.as_str()));
        assert!(has_identity(pid, &first));
        assert!(!has_identity(pid, "a different process incarnation"));
    }

    #[test]
    fn test_process_identity_is_absent_for_dead_pid() {
        assert!(identity(u32::MAX).is_none());
    }

    #[cfg(any(target_os = "android", target_os = "linux"))]
    #[test]
    fn test_linux_process_identity_includes_boot_id() {
        let boot_id = std::fs::read_to_string("/proc/sys/kernel/random/boot_id").unwrap();
        let identity = identity(std::process::id()).unwrap();
        assert!(identity.contains(boot_id.trim()));
    }

    // Reproduces the bug fixed above: the process object stays valid (and
    // OpenProcess succeeds) as long as any handle is open, even after the
    // process has exited. Holding `child` past `wait()` keeps its handle
    // open while we probe the same PID with our own OpenProcess call.
    #[cfg(windows)]
    #[test]
    fn test_is_alive_false_for_exited_process_with_handle_still_open() {
        let mut child = std::process::Command::new("cmd")
            .args(["/C", "exit 0"])
            .spawn()
            .unwrap();
        let pid = child.id();
        child.wait().unwrap();
        assert!(
            !is_alive(pid),
            "an open handle to an already-exited process must not count as alive"
        );
        drop(child);
    }

    /// A child that stays alive until something kills it, so tests that kill it
    /// are testing the kill rather than racing the child's own exit.
    ///
    /// Not `timeout`: it refuses to run at all when stdin is redirected ("ERROR:
    /// Input redirection is not supported"), which is how every test harness
    /// runs it, and then exits within milliseconds. `ping` reads no input. All
    /// three stdio handles are detached so nothing the child prints lands in the
    /// test output and nothing it does depends on how the harness was invoked.
    #[cfg(windows)]
    fn spawn_long_lived_child() -> std::process::Child {
        std::process::Command::new("cmd")
            .args(["/C", "ping -n 31 127.0.0.1"])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap()
    }

    // Reproduces the bug fixed above: kill_tree_win's terminate_win writes the
    // hcom-kill sentinel exit code 130. A trailing child.kill() (a second,
    // competing TerminateProcess on the same PID) could overwrite it before
    // the OS settles on a final exit code.
    #[cfg(windows)]
    #[test]
    fn test_kill_child_group_preserves_sentinel_exit_code() {
        let mut child = spawn_long_lived_child();
        kill_child_group(&mut child);
        let status = child.wait().unwrap();
        assert_eq!(
            status.code(),
            Some(130),
            "kill_child_group must not let a second kill overwrite the hcom-kill sentinel"
        );
    }

    #[cfg(windows)]
    #[test]
    fn child_link_plausibility_rejects_candidates_older_than_their_claimed_parent() {
        assert!(
            !child_link_is_plausible(Some(500), Some(499)),
            "a process created before its claimed parent cannot be its child"
        );
        assert!(child_link_is_plausible(Some(500), Some(500)));
        assert!(child_link_is_plausible(Some(500), Some(501)));
        // Unqueryable on either side: nothing to disprove, so the link stands
        // and the walk behaves as it did before the check existed.
        assert!(child_link_is_plausible(None, Some(1)));
        assert!(child_link_is_plausible(Some(500), None));
    }

    // The case that matters in practice, against real `GetProcessTimes` data: a
    // stale parent link pointing at a freshly spawned PID would otherwise offer
    // up long-lived bystanders — here the test runner itself — as descendants
    // to terminate.
    #[cfg(windows)]
    #[test]
    fn a_fresh_child_never_adopts_a_process_that_predates_it() {
        let mut child = spawn_long_lived_child();
        let claimed_parent = creation_ticks_win(child.id());
        let bystander = creation_ticks_win(std::process::id());
        kill_child_group(&mut child);
        let _ = child.wait();

        assert!(claimed_parent.is_some(), "spawned child must be queryable");
        assert!(bystander.is_some(), "our own process must be queryable");
        assert!(
            !child_link_is_plausible(claimed_parent, bystander),
            "a process that predates the PID claiming it must not join the kill tree"
        );
    }
}
