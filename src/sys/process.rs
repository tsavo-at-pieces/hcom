//! Process-control primitives: liveness, termination, and session/group setup.
//!
//! Unix uses `libc`/`nix` signals and `setsid`; Windows uses the Win32 process
//! and job-object APIs. See the module-level docs in [`crate::sys`].

use std::process::Command;

/// Observes a pipe client's stdout while [`run_stdio_child`] copies it. The
/// tap is shown each complete line only after hcom's stdout took it, and can
/// change neither the bytes nor when they are written. Every call runs on
/// the only copy of the stream: none may block.
///
/// Off Unix, `run_stdio_child` refuses to start, so nothing calls a tap there.
#[cfg_attr(not(unix), allow(dead_code))]
pub trait StdoutTap: Send + Sync {
    /// A complete line without its `\n`. The child wrote all of it after
    /// `emitted_after`: the spawn, or the start of a read that showed a
    /// child writing whatever it has as soon as the pipe has room to have
    /// had nothing waiting, in a way [`Self::emptied`] describes, though
    /// that read may have ended inside a line. `read_at` is when the copy
    /// began the read that took the line's first byte.
    fn observe(
        &self,
        line: &[u8],
        emitted_after: std::time::SystemTime,
        read_at: std::time::SystemTime,
    );
    /// A complete line too long to be shown. It was copied whole.
    fn skipped(&self);
    /// The copy found the pipe empty with a read that ended at a line's
    /// end, and that showed a child writing whatever it has as soon as the
    /// pipe has room to have had nothing waiting when the read began: the
    /// read left room for any write the pipe might have refused, or the pipe
    /// then stayed empty for longer than such a child takes to write what
    /// waits. The tap has been shown, or told it skipped, every line read so
    /// far.
    fn emptied(&self);
    /// The child has been reaped; its remaining output drains next.
    fn child_exited(&self);
    /// The copy has ended: at EOF, at a write to hcom's stdout that failed
    /// (EPIPE once the client closed it), at a failed read, or where the
    /// drain stops. Nothing follows, and the child's pipe closes with it,
    /// while the child itself may still run, waiting on stdin.
    fn copy_ended(&self);
}

/// Run a foreground pipe client without a shell or PTY. The hcom process
/// remains the native child's parent and forwards cancellation to its group.
/// SIGCHLD wakes the same loop as cancellation: no polling, and no signalling
/// thread can race the reaping of a PID that the OS might reuse.
///
/// With a `stdout_tap`, the child writes into a pipe that a thread copies to
/// hcom's own stdout; otherwise it inherits that stdout directly. The copy
/// delivers everything the child wrote before it was reaped and is joined
/// before this returns, unless HUP, INT or TERM reaches hcom once the child
/// has exited, or hcom signalled the child and the client then takes nothing
/// for the drain's idle bound. Either ends the wait, as hcom would not wait
/// for a child blocked writing to that client.
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
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::process::Stdio;
    use std::sync::Arc;
    use tapped_stdout::DrainWait;

    let mut signals = Signals::new([SIGCHLD, SIGHUP, SIGINT, SIGTERM])?;
    let tap = stdout_tap.map(Arc::<dyn StdoutTap>::from);
    let sink = if tap.is_some() {
        Some(std::io::stdout().as_fd().try_clone_to_owned()?)
    } else {
        None
    };
    command
        .stdin(Stdio::inherit())
        .stdout(if sink.is_some() {
            Stdio::piped()
        } else {
            Stdio::inherit()
        })
        .stderr(Stdio::inherit());
    detach_session(command);
    // Taken before the child exists, so all of its output follows it.
    let spawned_at = std::time::SystemTime::now();
    let mut child = command.spawn()?;
    let pid = child.id();
    let mut forwarder = None;
    let mut drain_wait = DrainWait::Delivered;
    let result = (|| {
        if let (Some(source), Some(sink), Some(tap)) = (child.stdout.take(), sink, tap.clone()) {
            forwarder = Some(tapped_stdout::Forwarder::spawn(
                source.into(),
                sink,
                tap,
                spawned_at,
            )?);
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
                    if signal != SIGCHLD {
                        drain_wait = DrainWait::Signalled(signal);
                    }
                    return Ok(status
                        .code()
                        .unwrap_or_else(|| 128 + status.signal().unwrap_or(1)));
                }
                if signal != SIGCHLD {
                    // SAFETY: this is our unreaped child's own process group.
                    unsafe {
                        libc::kill(-(pid as i32), signal);
                    }
                    drain_wait = DrainWait::Cancelled;
                }
            }
        }
    })();
    if result.is_err() {
        kill_group(pid);
        let _ = child.wait();
        if let DrainWait::Delivered = drain_wait {
            drain_wait = DrainWait::Cancelled;
        }
    }
    // Reaped on every path: whatever needs a live child stops before the
    // drain, which has no deadline while the client takes the output.
    if let Some(tap) = &tap {
        let _ = catch_unwind(AssertUnwindSafe(|| tap.child_exited()));
    }
    if let Some(forwarder) = forwarder {
        forwarder.finish(&mut signals, drain_wait);
    }
    drop(tap);
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
    use nix::errno::Errno;
    use nix::poll::{PollFd, PollFlags, PollTimeout, poll};
    use signal_hook::consts::SIGCHLD;
    use signal_hook::iterator::{Handle, Signals};
    use std::io::ErrorKind;
    use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd};
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
    use std::time::{Duration, Instant, SystemTime};

    /// Longest line a tap is shown. Longer lines are still forwarded whole.
    const LINE_LIMIT: usize = 16 * 1024 * 1024;

    /// When the copy ends after the child exits. Bytes it has read, and those
    /// the pipe held at the exit, are always delivered; after them it waits
    /// for a descendant that inherited the pipe.
    #[derive(Clone, Copy)]
    struct DrainLimits {
        /// How long the pipe may stay empty before the copy ends.
        idle: Duration,
        /// After this, the copy ends the next time the pipe is empty.
        cap: Duration,
    }

    const DRAIN: DrainLimits = DrainLimits {
        idle: Duration::from_secs(5),
        cap: Duration::from_secs(30),
    };

    /// How long the pipe must stay empty after a read that ended at a
    /// line's end, but did not show that it left room for anything the
    /// child had waiting, for that read to count as having emptied it: far
    /// longer than a child writing what it has as soon as the pipe has room
    /// takes once a read has emptied it (Bun 1.4.4 took at most 6 ms).
    const SETTLE: Duration = Duration::from_secs(2);

    /// Copies the child's stdout to hcom's own on a dedicated thread.
    pub(super) struct Forwarder {
        thread: std::thread::JoinHandle<()>,
        /// Closed once the child is reaped, which starts the drain.
        exited: std::io::PipeWriter,
        ended: Arc<Mutex<Ended>>,
    }

    /// What ends the wait for an exited child's output besides its delivery
    /// and HUP, INT or TERM reaching hcom meanwhile.
    #[derive(Clone, Copy)]
    pub(super) enum DrainWait {
        /// Nothing else: the client gets it however slowly it reads.
        Delivered,
        /// One write to hcom's stdout under way for the idle bound. hcom
        /// signalled the child (forwarding HUP, INT or TERM, or killing it
        /// after a failed launch), so it does not wait for a client that
        /// takes nothing that long, as it would not have waited for a child
        /// blocked writing to that client.
        Cancelled,
        /// This signal, which reached hcom once the child had exited.
        Signalled(libc::c_int),
    }

    /// Whether the copy has ended, or has stalled writing to hcom's stdout.
    /// A supervisor waiting for signals leaves its handle here, so that
    /// either wakes it.
    #[derive(Default)]
    struct Ended {
        done: bool,
        stalled: bool,
        waiter: Option<Handle>,
        /// When the write to hcom's stdout now under way began.
        writing_since: Option<Instant>,
    }

    fn lock(ended: &Mutex<Ended>) -> MutexGuard<'_, Ended> {
        ended.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Held by the copy thread: records the end however the thread exits.
    struct EndGuard(Arc<Mutex<Ended>>);

    impl Drop for EndGuard {
        fn drop(&mut self) {
            let mut ended = lock(&self.0);
            ended.done = true;
            if let Some(waiter) = ended.waiter.take() {
                waiter.close();
            }
        }
    }

    /// Held by the copy thread: tells the tap the copy ended, however the
    /// thread exits, even after the tap panicked out of the line split.
    struct TapEnd(Arc<dyn StdoutTap>);

    impl Drop for TapEnd {
        fn drop(&mut self) {
            let _ = catch_unwind(AssertUnwindSafe(|| self.0.copy_ended()));
        }
    }

    impl Forwarder {
        pub(super) fn spawn(
            source: OwnedFd,
            sink: OwnedFd,
            tap: Arc<dyn StdoutTap>,
            spawned_at: SystemTime,
        ) -> std::io::Result<Self> {
            let (exit_notice, exited) = std::io::pipe()?;
            let lines = LineTap::new(tap.clone(), LINE_LIMIT);
            let ended = Arc::new(Mutex::new(Ended::default()));
            let guard = EndGuard(ended.clone());
            let writes = ended.clone();
            let thread = std::thread::Builder::new()
                .name("stdio-stdout".into())
                .spawn(move || {
                    let _guard = guard;
                    let tap_end = TapEnd(tap);
                    let end = forward(
                        source.as_fd(),
                        exit_notice.as_fd(),
                        |bytes| {
                            lock(&writes).writing_since = Some(Instant::now());
                            let written = write_all(sink.as_fd(), bytes);
                            lock(&writes).writing_since = None;
                            written
                        },
                        lines,
                        spawned_at,
                        DRAIN,
                        SETTLE,
                    );
                    // The tap first, so that what it stops has stopped by
                    // the time the end is logged.
                    drop(tap_end);
                    end.log();
                })?;
            Ok(Self {
                thread,
                exited,
                ended,
            })
        }

        /// Report the child's exit and wait until its output is delivered,
        /// or as `wait` allows. The copy, perhaps blocked writing to a client
        /// that is not reading, is then left to end with hcom.
        pub(super) fn finish(self, signals: &mut Signals, wait: DrainWait) {
            {
                let mut ended = lock(&self.ended);
                if !ended.done {
                    ended.waiter = Some(signals.handle());
                }
            }
            drop(self.exited);
            let mut stop = None;
            match wait {
                DrainWait::Delivered => {}
                DrainWait::Cancelled => watch_for_a_stall(self.ended.clone(), DRAIN.idle),
                DrainWait::Signalled(signal) => stop = Some(signal),
            }
            loop {
                let (done, stalled) = {
                    let ended = lock(&self.ended);
                    (ended.done, ended.stalled)
                };
                if done {
                    let _ = self.thread.join();
                    return;
                }
                if stalled {
                    copy_stopped(&format!(
                        "hcom signalled the child and one write waited {}s for the client",
                        DRAIN.idle.as_secs()
                    ));
                    return;
                }
                // Closing the handle at the end of the copy, or once it
                // stalls, wakes this wait.
                let signal = stop
                    .take()
                    .or_else(|| signals.wait().find(|&signal| signal != SIGCHLD));
                if let Some(signal) = signal {
                    copy_stopped(&format!("signal {signal} reached hcom"));
                    return;
                }
            }
        }
    }

    /// The supervisor stopped waiting for the exited child's stdout copy.
    fn copy_stopped(why: &str) {
        crate::log::log_warn(
            "launcher",
            "stdio.stdout_copy_stopped",
            &format!("stopped waiting for the copy of the exited child's stdout: {why}"),
        );
    }

    /// Mark the copy stalled and wake the supervisor once one write to
    /// hcom's stdout has been under way for `idle`.
    fn watch_for_a_stall(ended: Arc<Mutex<Ended>>, idle: Duration) {
        let watch = move || {
            loop {
                let wait = {
                    let mut ended = lock(&ended);
                    if ended.done {
                        return;
                    }
                    match ended.writing_since.map(|since| since.elapsed()) {
                        Some(waited) if waited >= idle => {
                            ended.stalled = true;
                            if let Some(waiter) = ended.waiter.take() {
                                waiter.close();
                            }
                            return;
                        }
                        Some(waited) => idle - waited,
                        None => idle,
                    }
                };
                std::thread::sleep(wait);
            }
        };
        if let Err(error) = std::thread::Builder::new()
            .name("stdio-stall".into())
            .spawn(watch)
        {
            // Without the watch, only the copy's end or a signal ends the wait.
            crate::log::log_warn("launcher", "stdio.stall_watch_failed", &error.to_string());
        }
    }

    /// The copy after the child's exit (see [`DrainLimits`]).
    struct Drain {
        /// Bytes still to read of those the pipe held when the exit was seen.
        owed: usize,
        started: Instant,
        last_read: Instant,
        limits: DrainLimits,
    }

    impl Drain {
        fn start(owed: usize, limits: DrainLimits) -> Self {
            let now = Instant::now();
            Self {
                owed,
                started: now,
                last_read: now,
                limits,
            }
        }

        /// How long to wait for more output before the copy ends, or none
        /// while it may not end.
        fn wait(&self) -> Option<Duration> {
            if self.owed > 0 {
                return None;
            }
            Some(if self.started.elapsed() >= self.limits.cap {
                Duration::ZERO
            } else {
                self.limits.idle.saturating_sub(self.last_read.elapsed())
            })
        }

        /// Whether the copy may end with the pipe empty now.
        fn expired(&self) -> bool {
            self.owed == 0
                && (self.started.elapsed() >= self.limits.cap
                    || self.last_read.elapsed() >= self.limits.idle)
        }

        /// Count one read; whether the copy may end now.
        fn read(&mut self, read: usize, drained: bool) -> bool {
            self.owed = if drained {
                0
            } else {
                self.owed.saturating_sub(read)
            };
            self.last_read = Instant::now();
            drained && self.started.elapsed() >= self.limits.cap
        }
    }

    /// Why [`forward`] stopped copying.
    #[derive(Debug)]
    enum End {
        /// Every writer closed the pipe.
        Eof,
        /// The drain ended before EOF, for this reason.
        Drained(&'static str),
        /// hcom's stdout refused a write: EPIPE once the client closed it.
        WriteFailed(std::io::Error),
        ReadFailed(Errno),
        PollFailed(Errno),
    }

    impl End {
        fn log(&self) {
            match self {
                Self::Eof => {}
                Self::Drained(why) => crate::log::log_info(
                    "launcher",
                    "stdio.stdout_drain_ended",
                    &format!("stopped copying the exited child's stdout: {why}"),
                ),
                Self::WriteFailed(error) if error.kind() == ErrorKind::BrokenPipe => {
                    crate::log::log_info(
                        "launcher",
                        "stdio.stdout_client_closed",
                        "the client closed stdout: the copy stopped, and the child's pipe closes with it",
                    )
                }
                Self::WriteFailed(error) => crate::log::log_warn(
                    "launcher",
                    "stdio.stdout_write_failed",
                    &error.to_string(),
                ),
                Self::ReadFailed(errno) => {
                    crate::log::log_warn("launcher", "stdio.stdout_read_failed", &errno.to_string())
                }
                Self::PollFailed(errno) => {
                    crate::log::log_warn("launcher", "stdio.stdout_poll_failed", &errno.to_string())
                }
            }
        }
    }

    /// Copy `source` to `write` chunk by chunk as it arrives, then show the
    /// tap each complete line. Ends at EOF or a failed write, or, once
    /// `exited` reports the child reaped, as [`DrainLimits`] allow.
    ///
    /// Each line carries the last time the pipe was seen empty, with nothing
    /// waiting to be written, before its first byte was read: a child that
    /// writes what it has whenever the pipe has room wrote the line later.
    /// The time is taken just before a read that poll has reported ready, so
    /// the read never waits. A read that left room for any write the pipe
    /// might have refused shows that at once ([`left_room_for_any_write`]).
    /// One that took little less than the pipe holds, or one of a pipe that
    /// does not show how much it holds, may have left the child more to
    /// write, as a pipe refuses a short write before it is full; if
    /// it ended at a line's end, it shows that too once the pipe has then
    /// stayed empty for `settle`, which such a child would not have left
    /// empty so long. Either is reported to the tap as well, once it has been
    /// shown the lines the read completed, if the read ended at a line's end.
    fn forward(
        source: BorrowedFd<'_>,
        exited: BorrowedFd<'_>,
        mut write: impl FnMut(&[u8]) -> std::io::Result<()>,
        mut lines: LineTap,
        spawned_at: SystemTime,
        limits: DrainLimits,
        settle: Duration,
    ) -> End {
        let mut buf = vec![0; 64 * 1024];
        let mut empty_at = spawned_at;
        let mut drain: Option<Drain> = None;
        // A read that ended at a line's end without showing the pipe empty:
        // when it began, and when it was done.
        let mut settling: Option<(SystemTime, Instant)> = None;
        loop {
            let mut fds = [
                PollFd::new(source, PollFlags::POLLIN),
                PollFd::new(exited, PollFlags::POLLIN),
            ];
            let watched = if drain.is_some() { 1 } else { 2 };
            let waits = [
                drain.as_ref().and_then(Drain::wait),
                settling.map(|(_, done)| settle.saturating_sub(done.elapsed())),
            ];
            match poll(
                &mut fds[..watched],
                poll_timeout(waits.into_iter().flatten().min()),
            ) {
                // Only a drain or a settle has a timeout: the pipe stayed
                // empty.
                Ok(0) => {
                    if let Some((began, done)) = settling
                        && done.elapsed() >= settle
                    {
                        settling = None;
                        empty_at = began;
                        lines.emptied();
                    }
                    if drain.as_ref().is_some_and(Drain::expired) {
                        return End::Drained("it stayed empty, held open by a descendant");
                    }
                    continue;
                }
                Ok(_) => {}
                Err(Errno::EINTR) => continue,
                Err(errno) => return End::PollFailed(errno),
            }
            if drain.is_none() && fds[1].revents().is_some_and(|events| !events.is_empty()) {
                drain = Some(Drain::start(pending(source), limits));
                continue;
            }
            if fds[0].revents().is_none_or(|events| events.is_empty()) {
                continue;
            }
            let capacity = pipe_capacity(source);
            let before = SystemTime::now();
            let read = match nix::unistd::read(source, &mut buf) {
                Ok(0) => return End::Eof,
                Ok(read) => read,
                Err(Errno::EINTR | Errno::EAGAIN) => continue,
                Err(errno) => return End::ReadFailed(errno),
            };
            if let Err(error) = write(&buf[..read]) {
                // Returning drops the read end, so the child gets the same
                // EPIPE it would have received writing to the client's pipe.
                return End::WriteFailed(error);
            }
            lines.push(&buf[..read], empty_at, before);
            settling = None;
            if left_room_for_any_write(read, capacity, buf.len()) {
                empty_at = before;
                lines.emptied();
            } else if lines.at_line_end() {
                settling = Some((before, Instant::now()));
            }
            // Short of what the pipe or the buffer holds, the read took all
            // the pipe held.
            let empty = read < capacity.min(buf.len());
            if let Some(drain) = &mut drain
                && drain.read(read, empty)
            {
                return End::Drained("it was empty once the cap had passed");
            }
        }
    }

    /// `wait` as a poll timeout, rounded up: poll counts whole milliseconds.
    /// No wait is no timeout.
    fn poll_timeout(wait: Option<Duration>) -> PollTimeout {
        wait.map_or(PollTimeout::NONE, |wait| {
            PollTimeout::try_from(wait.as_nanos().div_ceil(1_000_000)).unwrap_or(PollTimeout::MAX)
        })
    }

    /// Whether a read that took `read` bytes, of a pipe that could hold
    /// `capacity` (see [`pipe_capacity`]) when it began, into a buffer of
    /// `buffer`, left room for any write that the pipe might have refused
    /// before it began: then a child that writes what it has as soon as the
    /// pipe has room had nothing waiting. That holds when the read before it
    /// took all the pipe held, as each read of a pipe no larger than the
    /// buffer does, so that what this one took was all written since.
    ///
    /// Linux counts a pipe's room in pages: a write goes into the last page
    /// only if what it does not fill of a page fits there whole, and
    /// otherwise takes pages of its own, so any two adjacent pages hold more
    /// than one page, and a pipe of two pages or more that refuses a write,
    /// or takes part of one, holds more than half its capacity.
    #[cfg(any(target_os = "android", target_os = "linux"))]
    fn left_room_for_any_write(read: usize, capacity: usize, buffer: usize) -> bool {
        // SAFETY: sysconf only reads a system setting.
        let page = usize::try_from(unsafe { libc::sysconf(libc::_SC_PAGESIZE) });
        capacity <= buffer && page.is_ok_and(|page| capacity / 2 >= page) && read <= capacity / 2
    }

    /// [`left_room_for_any_write`] elsewhere: a write of at most PIPE_BUF
    /// bytes is taken whole or refused, so a pipe that refused one may be
    /// short of full by PIPE_BUF - 1 bytes, while a longer one is taken as
    /// far as there is room. A capacity of at most
    /// [`LEAST_REPORTED_CAPACITY`] does not show how much the pipe holds, so
    /// no read of such a pipe counts.
    #[cfg(not(any(target_os = "android", target_os = "linux")))]
    fn left_room_for_any_write(read: usize, capacity: usize, buffer: usize) -> bool {
        capacity > LEAST_REPORTED_CAPACITY && read + libc::PIPE_BUF <= capacity.min(buffer)
    }

    /// The least capacity macOS reports for a pipe, XNU's `PIPE_SIZE`: it
    /// reports a pipe's buffer as `MAX(PIPE_SIZE, size)`, so a report of
    /// this much shows only that the pipe holds no more.
    #[cfg(not(any(target_os = "android", target_os = "linux")))]
    const LEAST_REPORTED_CAPACITY: usize = 16 * 1024;

    /// How many bytes the pipe can hold now, or 0 when that is unknown, so
    /// that no read counts as emptying it. Linux pipes hold 64 KiB unless
    /// resized.
    #[cfg(any(target_os = "android", target_os = "linux"))]
    fn pipe_capacity(fd: BorrowedFd<'_>) -> usize {
        nix::fcntl::fcntl(fd, nix::fcntl::FcntlArg::F_GETPIPE_SZ)
            .map_or(0, |size| usize::try_from(size).unwrap_or(0))
    }

    /// How many bytes the pipe can hold now, or 0 when that is unknown, so
    /// that no read counts as emptying it. macOS reports a pipe's buffer as
    /// its block size, but never less than 16 KiB
    /// ([`LEAST_REPORTED_CAPACITY`]): a buffer starts at 512 bytes and grows,
    /// to at most 64 KiB, only when a write needs more room while the
    /// kernel's pipe memory is under its 16 MB cap. A report of 16 KiB thus
    /// shows only that the pipe holds no more, which is all the drain needs:
    /// a read of a pipe no larger than the copy's buffer takes all it holds.
    /// Elsewhere the block size may be smaller than the buffer, which only
    /// counts fewer reads as emptying it.
    #[cfg(not(any(target_os = "android", target_os = "linux")))]
    fn pipe_capacity(fd: BorrowedFd<'_>) -> usize {
        nix::sys::stat::fstat(fd).map_or(0, |stat| usize::try_from(stat.st_blksize).unwrap_or(0))
    }

    /// Bytes waiting in a pipe, or 0 when that is unknown.
    fn pending(fd: BorrowedFd<'_>) -> usize {
        let mut pending: libc::c_int = 0;
        // SAFETY: FIONREAD stores one c_int for this open descriptor.
        let status = unsafe { libc::ioctl(fd.as_raw_fd(), libc::FIONREAD, &mut pending) };
        if status == 0 {
            usize::try_from(pending).unwrap_or(0)
        } else {
            0
        }
    }

    /// Write all of `bytes` without changing the file status flags: the
    /// client may have made its end nonblocking.
    fn write_all(fd: BorrowedFd<'_>, mut bytes: &[u8]) -> std::io::Result<()> {
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
        tap: Option<Arc<dyn StdoutTap>>,
        limit: usize,
        /// Start of the current line, unless it outgrew `limit`.
        line: Vec<u8>,
        /// The `emitted_after` and start of the read holding the current
        /// line's first byte; none between lines.
        started: Option<(SystemTime, SystemTime)>,
        overflowed: bool,
    }

    impl LineTap {
        fn new(tap: Arc<dyn StdoutTap>, limit: usize) -> Self {
            Self {
                tap: Some(tap),
                limit,
                line: Vec::new(),
                started: None,
                overflowed: false,
            }
        }

        /// One chunk as read; `emitted_after` is when the pipe was last seen
        /// empty before that read, and `read_at` when the read began.
        fn push(&mut self, mut chunk: &[u8], emitted_after: SystemTime, read_at: SystemTime) {
            while self.tap.is_some()
                && let Some(end) = chunk.iter().position(|&byte| byte == b'\n')
            {
                let head = &chunk[..end];
                let (emitted, read) = self.started.take().unwrap_or((emitted_after, read_at));
                if !self.overflowed && self.line.len() + head.len() <= self.limit {
                    if self.line.is_empty() {
                        self.tell(|tap| tap.observe(head, emitted, read));
                    } else {
                        let mut line = std::mem::take(&mut self.line);
                        line.extend_from_slice(head);
                        self.tell(|tap| tap.observe(&line, emitted, read));
                    }
                } else {
                    self.tell(|tap| tap.skipped());
                }
                self.line.clear();
                self.overflowed = false;
                chunk = &chunk[end + 1..];
            }
            if self.tap.is_none() || chunk.is_empty() {
                return;
            }
            self.started.get_or_insert((emitted_after, read_at));
            if self.overflowed {
                return;
            }
            if self.line.len() + chunk.len() <= self.limit {
                self.line.extend_from_slice(chunk);
            } else {
                self.line = Vec::new();
                self.overflowed = true;
            }
        }

        /// Whether the last chunk pushed ended at a line's end.
        fn at_line_end(&self) -> bool {
            self.started.is_none()
        }

        /// The read last pushed found the pipe empty. The tap learns of it
        /// only when that read ended at a line's end, so that it has been
        /// shown everything read: part of a line may be a frame it would
        /// need to see first.
        fn emptied(&mut self) {
            if self.at_line_end() {
                self.tell(|tap| tap.emptied());
            }
        }

        fn tell(&mut self, call: impl FnOnce(&dyn StdoutTap)) {
            let Some(tap) = &self.tap else {
                return;
            };
            if catch_unwind(AssertUnwindSafe(|| call(tap.as_ref()))).is_err() {
                self.tap = None;
                self.line = Vec::new();
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::io::{Read, Write};
        use std::sync::Mutex;

        /// One tap call or sink write, in the order the forwarder made them.
        #[derive(Debug, Clone, PartialEq)]
        enum Copied {
            /// A line, its `emitted_after` and its `read_at`.
            Observed(Vec<u8>, SystemTime, SystemTime),
            Skipped,
            Emptied,
            Wrote(Vec<u8>),
            Ended,
        }

        struct RecordingTap {
            log: Arc<Mutex<Vec<Copied>>>,
            panic_on_observe: bool,
        }

        impl StdoutTap for RecordingTap {
            fn observe(&self, line: &[u8], emitted_after: SystemTime, read_at: SystemTime) {
                self.log.lock().unwrap().push(Copied::Observed(
                    line.to_vec(),
                    emitted_after,
                    read_at,
                ));
                assert!(!self.panic_on_observe, "tap failure");
            }

            fn skipped(&self) {
                self.log.lock().unwrap().push(Copied::Skipped);
            }

            fn emptied(&self) {
                self.log.lock().unwrap().push(Copied::Emptied);
            }

            fn child_exited(&self) {}

            fn copy_ended(&self) {
                self.log.lock().unwrap().push(Copied::Ended);
            }
        }

        fn recording_tap(log: &Arc<Mutex<Vec<Copied>>>, panic_on_observe: bool) -> LineTap {
            LineTap::new(
                Arc::new(RecordingTap {
                    log: log.clone(),
                    panic_on_observe,
                }),
                256,
            )
        }

        fn at(seconds: u64) -> SystemTime {
            SystemTime::UNIX_EPOCH + Duration::from_secs(seconds)
        }

        const FAST: DrainLimits = DrainLimits {
            idle: Duration::from_millis(200),
            cap: Duration::from_secs(5),
        };

        /// A settle longer than any test runs.
        const NO_SETTLE: Duration = Duration::from_secs(3600);

        /// Run [`forward`] on its own thread over a fresh pipe, as the
        /// launcher does; the test plays the child, holding the write end.
        struct Copier {
            log: Arc<Mutex<Vec<Copied>>>,
            child: Option<std::io::PipeWriter>,
            /// Dropping it reports the child's exit.
            exited: Option<std::io::PipeWriter>,
            copier: Option<std::thread::JoinHandle<End>>,
        }

        impl Copier {
            fn start(
                sink: impl FnMut(&[u8]) -> std::io::Result<()> + Send + 'static,
                log: &Arc<Mutex<Vec<Copied>>>,
                panic_on_observe: bool,
                limits: DrainLimits,
            ) -> Self {
                Self::start_on(
                    std::io::pipe().unwrap(),
                    sink,
                    log,
                    panic_on_observe,
                    limits,
                    NO_SETTLE,
                )
            }

            /// [`Self::start`] over a pipe the test may have written to,
            /// with a settle of its own.
            fn start_on(
                (source, child): (std::io::PipeReader, std::io::PipeWriter),
                sink: impl FnMut(&[u8]) -> std::io::Result<()> + Send + 'static,
                log: &Arc<Mutex<Vec<Copied>>>,
                panic_on_observe: bool,
                limits: DrainLimits,
                settle: Duration,
            ) -> Self {
                let (exit_notice, exited) = std::io::pipe().unwrap();
                let lines = recording_tap(log, panic_on_observe);
                let copier = std::thread::spawn(move || {
                    forward(
                        source.as_fd(),
                        exit_notice.as_fd(),
                        sink,
                        lines,
                        at(1),
                        limits,
                        settle,
                    )
                });
                Self {
                    log: log.clone(),
                    child: Some(child),
                    exited: Some(exited),
                    copier: Some(copier),
                }
            }

            fn recording(
                log: &Arc<Mutex<Vec<Copied>>>,
                panic_on_observe: bool,
                limits: DrainLimits,
            ) -> Self {
                Self::recording_on(std::io::pipe().unwrap(), log, panic_on_observe, limits)
            }

            fn recording_on(
                pipe: (std::io::PipeReader, std::io::PipeWriter),
                log: &Arc<Mutex<Vec<Copied>>>,
                panic_on_observe: bool,
                limits: DrainLimits,
            ) -> Self {
                Self::settling_on(pipe, log, panic_on_observe, limits, NO_SETTLE)
            }

            /// [`Self::recording_on`] with a settle of its own.
            fn settling_on(
                pipe: (std::io::PipeReader, std::io::PipeWriter),
                log: &Arc<Mutex<Vec<Copied>>>,
                panic_on_observe: bool,
                limits: DrainLimits,
                settle: Duration,
            ) -> Self {
                let sink = log.clone();
                Self::start_on(
                    pipe,
                    move |bytes| {
                        sink.lock().unwrap().push(Copied::Wrote(bytes.to_vec()));
                        Ok(())
                    },
                    log,
                    panic_on_observe,
                    limits,
                    settle,
                )
            }

            fn child(&mut self) -> &mut std::io::PipeWriter {
                self.child.as_mut().unwrap()
            }

            /// Wait for the copy to end. A copy that never ends fails the
            /// test instead of hanging it.
            fn wait(self) -> Vec<Copied> {
                self.wait_for_end().0
            }

            /// [`Self::wait`], with why the copy ended.
            fn wait_for_end(mut self) -> (Vec<Copied>, End) {
                let copier = self.copier.take().unwrap();
                let deadline = Instant::now() + Duration::from_secs(30);
                while !copier.is_finished() {
                    assert!(Instant::now() < deadline, "the copy never ended");
                    std::thread::sleep(Duration::from_millis(5));
                }
                let end = copier.join().unwrap();
                (self.log.lock().unwrap().clone(), end)
            }
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

        /// The lines shown, with their `emitted_after`.
        fn observed(copied: &[Copied]) -> Vec<(Vec<u8>, SystemTime)> {
            copied
                .iter()
                .filter_map(|event| match event {
                    Copied::Observed(line, at, _) => Some((line.clone(), *at)),
                    _ => None,
                })
                .collect()
        }

        /// Lines of `data` as the tap must see them: those within the 256-byte
        /// test limit, with the offset of their first byte and their newline.
        fn expected_lines(data: &[u8]) -> Vec<(Vec<u8>, usize, usize)> {
            let mut lines = Vec::new();
            let mut start = 0;
            while let Some(end) = data[start..].iter().position(|&byte| byte == b'\n') {
                let line = &data[start..start + end];
                if line.len() <= 256 {
                    lines.push((line.to_vec(), start, start + end));
                }
                start += end + 1;
            }
            lines
        }

        fn sample() -> Vec<u8> {
            let mut data = Vec::new();
            for line in [
                "{\"type\":\"assistant\",\"text\":\"h\u{e9}llo \u{2713} \u{1f389}\"}".as_bytes(),
                b"{\"type\":\"user\"}\r",
                b"",
                // Over the 256-byte test limit: forwarded whole, never shown.
                &[b'x'; 300],
                "{\"type\":\"result\",\"result\":\"\u{2713}\"}".as_bytes(),
            ] {
                data.extend_from_slice(line);
                data.push(b'\n');
            }
            // A final line without newline, ending inside a UTF-8 sequence.
            data.extend_from_slice(b"partial \xe2\x9c");
            data
        }

        #[test]
        fn lines_carry_the_empty_time_of_the_read_holding_their_first_byte() {
            let data = sample();
            let lines = expected_lines(&data);
            for seed in 1..=64u64 {
                for max_read in [1, 2, 3, 7, 64, 100_000] {
                    let log = Arc::new(Mutex::new(Vec::new()));
                    let mut tap = recording_tap(&log, false);
                    // Chunk `k` is stamped `k`, its read having begun at
                    // `1000 + k`.
                    let mut state = seed;
                    let mut chunks = Vec::new();
                    let mut offset = 0;
                    while offset < data.len() {
                        state ^= state << 13;
                        state ^= state >> 7;
                        state ^= state << 17;
                        let size = (1 + (state % max_read) as usize).min(data.len() - offset);
                        chunks.push(offset);
                        let stamp = chunks.len() as u64;
                        tap.push(&data[offset..offset + size], at(stamp), at(1000 + stamp));
                        offset += size;
                    }
                    let log = log.lock().unwrap();
                    let shown: Vec<_> = log
                        .iter()
                        .filter_map(|event| match event {
                            Copied::Observed(line, emitted, read) => Some((line, emitted, read)),
                            _ => None,
                        })
                        .collect();
                    assert_eq!(shown.len(), lines.len(), "seed={seed} max_read={max_read}");
                    for ((line, emitted, read), (expected, first, _)) in shown.iter().zip(&lines) {
                        assert_eq!(*line, expected, "seed={seed} max_read={max_read}");
                        let chunk = chunks.iter().rposition(|start| start <= first).unwrap() as u64;
                        assert_eq!(**emitted, at(chunk + 1), "seed={seed}");
                        assert_eq!(**read, at(1000 + chunk + 1), "seed={seed}");
                    }
                    // The over-limit line is reported in its place.
                    let order: Vec<_> = log
                        .iter()
                        .map(|event| matches!(event, Copied::Skipped))
                        .collect();
                    assert_eq!(order, [false, false, false, true, false], "seed={seed}");
                }
            }
        }

        #[test]
        fn the_copy_is_byte_exact_and_shows_lines_only_once_written() {
            let mut data = Vec::new();
            for round in 0..40 {
                data.extend_from_slice(&sample());
                data.extend_from_slice(format!("\n{{\"round\":{round}}}\n").as_bytes());
                // Larger than one read, so lines span reads.
                data.extend_from_slice(&vec![b'z'; 20_000 + round * 997]);
                data.push(b'\n');
            }
            let log = Arc::new(Mutex::new(Vec::new()));
            let mut copy = Copier::recording(&log, false, FAST);
            let mut state = 0x9e37_79b9_u64;
            let mut offset = 0;
            while offset < data.len() {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                let size = (1 + (state % 70_000) as usize).min(data.len() - offset);
                copy.child()
                    .write_all(&data[offset..offset + size])
                    .unwrap();
                offset += size;
            }
            copy.child.take();
            let copied = copy.wait();
            assert!(written(&copied) == data, "bytes differ");
            let lines = expected_lines(&data);
            assert_eq!(observed(&copied).len(), lines.len());
            let mut sent = 0;
            let mut shown = lines.iter();
            for event in &copied {
                match event {
                    Copied::Wrote(bytes) => sent += bytes.len(),
                    Copied::Observed(line, _, _) => {
                        let (expected, _, newline) = shown.next().unwrap();
                        assert_eq!(line, expected);
                        assert!(sent > *newline, "shown before its newline was written");
                    }
                    Copied::Skipped | Copied::Emptied => {}
                    Copied::Ended => panic!("forward itself never reports its end"),
                }
            }
        }

        /// What the tap was told, but the bytes written: a line shown,
        /// `skipped` or `emptied`.
        fn told(copied: &[Copied]) -> Vec<String> {
            copied
                .iter()
                .filter_map(|event| match event {
                    Copied::Observed(line, _, _) => Some(String::from_utf8_lossy(line).into()),
                    Copied::Skipped => Some("skipped".into()),
                    Copied::Emptied => Some("emptied".into()),
                    Copied::Wrote(_) => None,
                    Copied::Ended => Some("ended".into()),
                })
                .collect()
        }

        /// An empty pipe that has held more than 16 KiB, so that macOS has
        /// grown it to 64 KiB, what a Linux pipe holds from the start: macOS
        /// reports any smaller pipe as 16 KiB, so that no read of it shows
        /// it empty at once.
        fn grown_pipe() -> (std::io::PipeReader, std::io::PipeWriter) {
            use nix::fcntl::{FcntlArg, OFlag, fcntl};

            let (mut source, child) = std::io::pipe().unwrap();
            // Without blocking: a pipe that cannot grow takes only part.
            let flags = OFlag::from_bits_truncate(fcntl(child.as_fd(), FcntlArg::F_GETFL).unwrap());
            fcntl(child.as_fd(), FcntlArg::F_SETFL(flags | OFlag::O_NONBLOCK)).unwrap();
            let taken = nix::unistd::write(child.as_fd(), &[b'g'; 16 * 1024 + 1]).unwrap();
            fcntl(child.as_fd(), FcntlArg::F_SETFL(flags)).unwrap();
            source.read_exact(&mut vec![0; taken]).unwrap();
            assert_eq!(
                pipe_capacity(source.as_fd()),
                64 * 1024,
                "the pipe did not grow to 64 KiB"
            );
            (source, child)
        }

        /// The tap hears that a read found the pipe empty only when that
        /// read ended at a line's end, after the lines it completed: a read
        /// that ends inside a line leaves it to the read that ends it. A
        /// line spanning reads carries the start of the first.
        #[test]
        fn the_tap_hears_of_an_empty_pipe_only_when_a_read_ends_a_line() {
            let log = Arc::new(Mutex::new(Vec::new()));
            let mut copy = Copier::recording_on(grown_pipe(), &log, false, FAST);
            let copied_up_to = |bytes: &[u8]| {
                let deadline = Instant::now() + Duration::from_secs(10);
                while written(&log.lock().unwrap()) != bytes {
                    assert!(Instant::now() < deadline, "not copied: {bytes:?}");
                    std::thread::sleep(Duration::from_millis(5));
                }
            };
            copy.child().write_all(b"one\n").unwrap();
            copied_up_to(b"one\n");
            let before_part = SystemTime::now();
            copy.child().write_all(b"tw").unwrap();
            copied_up_to(b"one\ntw");
            let before_rest = SystemTime::now();
            copy.child().write_all(b"o\n").unwrap();
            copied_up_to(b"one\ntwo\n");
            // Over the 256-byte test limit.
            let mut long = vec![b'x'; 300];
            long.push(b'\n');
            copy.child().write_all(&long).unwrap();
            copy.child.take();
            let copied = copy.wait();
            assert_eq!(
                told(&copied),
                ["one", "emptied", "two", "emptied", "skipped", "emptied"]
            );
            let read_at = copied
                .iter()
                .find_map(|event| match event {
                    Copied::Observed(line, _, read_at) if line == b"two" => Some(*read_at),
                    _ => None,
                })
                .unwrap();
            assert!(before_part <= read_at && read_at <= before_rest);
        }

        #[test]
        fn a_line_follows_the_time_the_pipe_was_last_seen_empty() {
            let log = Arc::new(Mutex::new(Vec::new()));
            let mut copy = Copier::recording_on(grown_pipe(), &log, false, FAST);
            let delivered = |count: usize| {
                let deadline = Instant::now() + Duration::from_secs(10);
                while observed(&log.lock().unwrap()).len() < count {
                    assert!(Instant::now() < deadline, "line not copied");
                    std::thread::sleep(Duration::from_millis(5));
                }
            };
            let before_first = SystemTime::now();
            copy.child().write_all(b"first\n").unwrap();
            delivered(1);
            let before_second = SystemTime::now();
            copy.child().write_all(b"second\n").unwrap();
            delivered(2);
            copy.child.take();
            let shown = observed(&copy.wait());
            // Nothing was seen empty before the first line but the spawn.
            assert_eq!(shown[0], (b"first".to_vec(), at(1)));
            // The read that drained the first line began after it was
            // written, and before the second one was.
            let (line, emitted_after) = &shown[1];
            assert_eq!(line, b"second");
            assert!(before_first <= *emitted_after && *emitted_after <= before_second);
        }

        #[test]
        fn a_line_is_written_before_its_newline_arrives() {
            let log = Arc::new(Mutex::new(Vec::new()));
            let mut copy = Copier::recording(&log, false, FAST);
            copy.child().write_all(b"{\"type\":\"partial\"").unwrap();
            let deadline = Instant::now() + Duration::from_secs(10);
            while written(&log.lock().unwrap()) != b"{\"type\":\"partial\"" {
                assert!(
                    Instant::now() < deadline,
                    "the copy held back a partial line"
                );
                std::thread::sleep(Duration::from_millis(5));
            }
            assert!(observed(&log.lock().unwrap()).is_empty());
            copy.child().write_all(b"}\n").unwrap();
            copy.child.take();
            let copied = copy.wait();
            assert_eq!(written(&copied), b"{\"type\":\"partial\"}\n");
            assert_eq!(
                observed(&copied),
                [(b"{\"type\":\"partial\"}".to_vec(), at(1))]
            );
        }

        /// A read that takes all a full pipe held, however short of the
        /// copy's buffer, does not show the pipe empty: the child may have
        /// had more waiting for room, so a line read later keeps the time
        /// from before.
        #[test]
        fn a_read_that_takes_a_full_pipe_leaves_the_empty_time() {
            let (source, mut child) = std::io::pipe().unwrap();
            // macOS grows a pipe from 512 bytes as far as writes need, here
            // to the 16 KiB it reports from the start. Linux ones hold the
            // copy's 64 KiB buffer, so shrink this one to 16 KiB too.
            #[cfg(any(target_os = "android", target_os = "linux"))]
            nix::fcntl::fcntl(child.as_fd(), nix::fcntl::FcntlArg::F_SETPIPE_SZ(16 * 1024))
                .unwrap();
            let capacity = pipe_capacity(source.as_fd());
            assert!(capacity > 0 && capacity < 64 * 1024, "{capacity}");
            // Filled before the copy starts, so its first read takes all of
            // it, in writes small enough not to grow the pipe past that.
            let mut chunk = vec![b'x'; 4096 - 1];
            chunk.push(b'\n');
            for _ in 0..capacity / chunk.len() {
                child.write_all(&chunk).unwrap();
            }
            assert_eq!(pending(source.as_fd()), capacity, "the pipe is not full");
            assert_eq!(pipe_capacity(source.as_fd()), capacity, "the pipe grew");
            let log = Arc::new(Mutex::new(Vec::new()));
            let mut copy = Copier::recording_on((source, child), &log, false, FAST);
            let deadline = Instant::now() + Duration::from_secs(10);
            while written(&log.lock().unwrap()).len() < capacity {
                assert!(Instant::now() < deadline, "the full pipe was not copied");
                std::thread::sleep(Duration::from_millis(5));
            }
            copy.child().write_all(b"late\n").unwrap();
            copy.child.take();
            let copied = copy.wait();
            assert_eq!(written(&copied).len(), capacity + 5);
            // The full lines are over the test limit, so never shown. The
            // pipe was last seen empty at the spawn.
            assert_eq!(observed(&copied), [(b"late".to_vec(), at(1))]);
            // Nor does the tap hear of an empty pipe before `late` is read.
            // On Linux it hears of one then; macOS, which reports this pipe
            // as it would a smaller one, shows no read of it empty at once.
            let mut expected = vec!["skipped"; capacity / chunk.len()];
            expected.push("late");
            if cfg!(any(target_os = "android", target_os = "linux")) {
                expected.push("emptied");
            }
            assert_eq!(told(&copied), expected);
        }

        /// The same read, ending at a line's end, shows the pipe empty once
        /// the pipe has stayed empty for the settle after it, with no other
        /// write: the tap hears of the empty pipe then, not at once, and a
        /// line read later carries that read's start.
        #[test]
        fn a_full_read_ending_a_line_shows_the_pipe_empty_once_it_stays_empty() {
            let (source, mut child) = grown_pipe();
            let capacity = pipe_capacity(source.as_fd());
            let mut chunk = vec![b'x'; 4096 - 1];
            chunk.push(b'\n');
            for _ in 0..capacity / chunk.len() {
                child.write_all(&chunk).unwrap();
            }
            assert_eq!(pending(source.as_fd()), capacity, "the pipe is not full");
            let log = Arc::new(Mutex::new(Vec::new()));
            let started = SystemTime::now();
            let mut copy =
                Copier::settling_on((source, child), &log, false, FAST, Duration::from_secs(1));
            let emptied =
                |log: &Mutex<Vec<Copied>>| told(&log.lock().unwrap()).contains(&"emptied".into());
            let deadline = Instant::now() + Duration::from_secs(10);
            while written(&log.lock().unwrap()).len() < capacity {
                assert!(Instant::now() < deadline, "the full pipe was not copied");
                std::thread::sleep(Duration::from_millis(5));
            }
            let copied = SystemTime::now();
            assert!(!emptied(&log), "the copy reported the empty pipe at once");
            while !emptied(&log) {
                assert!(
                    Instant::now() < deadline,
                    "the copy never reported the empty pipe"
                );
                std::thread::sleep(Duration::from_millis(5));
            }
            copy.child().write_all(b"late\n").unwrap();
            copy.child.take();
            let copied_events = copy.wait();
            let mut expected = vec!["skipped"; capacity / chunk.len()];
            expected.extend(["emptied", "late", "emptied"]);
            assert_eq!(told(&copied_events), expected);
            let (line, emitted_after) = observed(&copied_events).pop().unwrap();
            assert_eq!(line, b"late");
            assert!(started <= emitted_after && emitted_after <= copied);
        }

        /// A read before the settle has passed cancels it: the pipe did not
        /// stay empty. Here the child, blocked on the full pipe, writes its
        /// next line as soon as the full read makes room, and the read that
        /// takes that line shows the pipe empty at once. Once the full
        /// read's settle would have passed, nothing reports the pipe empty
        /// again, and a line read later carries the start of the read that
        /// took the waiting line, not the full read's.
        #[test]
        fn a_read_before_the_settle_passes_cancels_it() {
            let (source, mut child) = grown_pipe();
            let capacity = pipe_capacity(source.as_fd());
            let mut chunk = vec![b'x'; 4096 - 1];
            chunk.push(b'\n');
            for _ in 0..capacity / chunk.len() {
                child.write_all(&chunk).unwrap();
            }
            assert_eq!(pending(source.as_fd()), capacity, "the pipe is not full");
            let mut waiting = child.try_clone().unwrap();
            let next = std::thread::spawn(move || waiting.write_all(b"next\n").unwrap());
            // Let the child block on the full pipe first.
            std::thread::sleep(Duration::from_millis(50));
            let settle = Duration::from_secs(1);
            let log = Arc::new(Mutex::new(Vec::new()));
            let mut copy = Copier::settling_on((source, child), &log, false, FAST, settle);
            next.join().unwrap();
            let deadline = Instant::now() + Duration::from_secs(10);
            while observed(&log.lock().unwrap()).is_empty() {
                assert!(Instant::now() < deadline, "the waiting line was not copied");
                std::thread::sleep(Duration::from_millis(5));
            }
            // Well past the end of the full read's settle.
            std::thread::sleep(settle * 2);
            copy.child().write_all(b"late\n").unwrap();
            copy.child.take();
            let copied = copy.wait();
            let mut expected = vec!["skipped"; capacity / chunk.len()];
            expected.extend(["next", "emptied", "late", "emptied"]);
            assert_eq!(told(&copied), expected);
            let read_next = copied
                .iter()
                .find_map(|event| match event {
                    Copied::Observed(line, _, read_at) if line == b"next" => Some(*read_at),
                    _ => None,
                })
                .unwrap();
            // The full read showed nothing; the read of `next` did.
            assert_eq!(
                observed(&copied),
                [(b"next".to_vec(), at(1)), (b"late".to_vec(), read_next)]
            );
        }

        /// A pipe refuses a short write that does not fit the room it has
        /// left, although it is not full: one of at most PIPE_BUF bytes is
        /// taken whole or not at all, and Linux, which counts room in
        /// pages, refuses it once the last page cannot take it and no page
        /// is free. A child whose next frame waits so does not show the
        /// pipe empty in the read that takes what the pipe held; the read
        /// after the waiting frame does.
        #[test]
        fn a_read_that_leaves_too_little_room_for_a_waiting_write_does_not_show_the_pipe_empty() {
            use nix::fcntl::{FcntlArg, OFlag, fcntl};

            let (source, child) = std::io::pipe().unwrap();
            let flags = OFlag::from_bits_truncate(fcntl(child.as_fd(), FcntlArg::F_GETFL).unwrap());
            fcntl(child.as_fd(), FcntlArg::F_SETFL(flags | OFlag::O_NONBLOCK)).unwrap();
            let mut line = vec![b'x'; 99];
            line.push(b'\n');
            let mut held = 0;
            while let Ok(written) = nix::unistd::write(child.as_fd(), &line) {
                assert_eq!(written, line.len());
                held += written;
            }
            // Claude's next `running` state frame.
            let frame = concat!(
                r#"{"type":"system","subtype":"session_state_changed","state":"running","#,
                r#""session_id":"2e7c4a9b-1d3f-4b6a-8c5e-7f9a0b1c2d3e"}"#,
                "\n"
            )
            .as_bytes();
            assert_eq!(
                nix::unistd::write(child.as_fd(), frame),
                Err(Errno::EAGAIN),
                "the frame fit"
            );
            assert!(held < pipe_capacity(source.as_fd()), "the pipe is full");
            let log = Arc::new(Mutex::new(Vec::new()));
            let mut copy = Copier::recording_on((source, child), &log, false, FAST);
            let deadline = Instant::now() + Duration::from_secs(10);
            while written(&log.lock().unwrap()).len() < held {
                assert!(Instant::now() < deadline, "the pipe was not copied");
                std::thread::sleep(Duration::from_millis(5));
            }
            // The child writes the frame once the pipe has room for it.
            fcntl(copy.child().as_fd(), FcntlArg::F_SETFL(flags)).unwrap();
            copy.child().write_all(frame).unwrap();
            copy.child.take();
            let told = told(&copy.wait());
            let lines = told.iter().take_while(|told| told.len() == 99).count();
            assert_eq!(lines, held / 100);
            assert_eq!(
                told[lines..],
                [
                    String::from_utf8_lossy(&frame[..frame.len() - 1]).into_owned(),
                    "emptied".to_string()
                ]
            );
        }

        /// macOS reports a pipe's buffer as no less than 16 KiB, though the
        /// buffer starts at 512 bytes and grows only as far as writes need:
        /// two lines of 4 KiB leave this one full at 8 KiB. The read that
        /// takes them left no room for a write the pipe might have refused,
        /// so it does not show the pipe empty, and nor does any read while
        /// the pipe reports 16 KiB. A pipe reported larger holds that much:
        /// once one has grown, the same reads show it empty at once.
        #[cfg(not(any(target_os = "android", target_os = "linux")))]
        #[test]
        fn only_a_pipe_reported_past_16_kib_shows_a_read_empty_at_once() {
            for grown in [false, true] {
                let (source, mut child) = if grown {
                    grown_pipe()
                } else {
                    std::io::pipe().unwrap()
                };
                let mut line = vec![b'x'; 4096 - 1];
                line.push(b'\n');
                child.write_all(&line).unwrap();
                child.write_all(&line).unwrap();
                assert_eq!(pending(source.as_fd()), 2 * line.len(), "grown={grown}");
                assert_eq!(
                    pipe_capacity(source.as_fd()),
                    if grown {
                        64 * 1024
                    } else {
                        LEAST_REPORTED_CAPACITY
                    },
                    "grown={grown}"
                );
                let log = Arc::new(Mutex::new(Vec::new()));
                let started = SystemTime::now();
                let mut copy = Copier::recording_on((source, child), &log, false, FAST);
                let deadline = Instant::now() + Duration::from_secs(10);
                while written(&log.lock().unwrap()).len() < 2 * line.len() {
                    assert!(
                        Instant::now() < deadline,
                        "grown={grown}: the pipe was not copied"
                    );
                    std::thread::sleep(Duration::from_millis(5));
                }
                let copied = SystemTime::now();
                copy.child().write_all(b"late\n").unwrap();
                copy.child.take();
                let copied_events = copy.wait();
                let (late, emitted_after) = observed(&copied_events).pop().unwrap();
                assert_eq!(late, b"late", "grown={grown}");
                if grown {
                    assert_eq!(
                        told(&copied_events),
                        ["skipped", "skipped", "emptied", "late", "emptied"]
                    );
                    assert!(started <= emitted_after && emitted_after <= copied);
                } else {
                    // The pipe was last seen empty at the spawn.
                    assert_eq!(told(&copied_events), ["skipped", "skipped", "late"]);
                    assert_eq!(emitted_after, at(1));
                }
            }
        }

        #[test]
        fn a_failed_tap_never_interrupts_the_copy() {
            let log = Arc::new(Mutex::new(Vec::new()));
            let mut copy = Copier::recording(&log, true, FAST);
            copy.child().write_all(b"one\ntwo\nthree\n").unwrap();
            copy.child.take();
            let copied = copy.wait();
            assert_eq!(written(&copied), b"one\ntwo\nthree\n");
            // The first line panics the tap; nothing reaches it afterwards.
            assert_eq!(observed(&copied), [(b"one".to_vec(), at(1))]);
        }

        #[test]
        fn a_closed_client_stdout_closes_the_child_pipe_too() {
            let (client, sink) = std::io::pipe().unwrap();
            drop(client);
            let log = Arc::new(Mutex::new(Vec::new()));
            let mut copy = Copier::start(
                move |bytes| write_all(sink.as_fd(), bytes),
                &log,
                false,
                FAST,
            );
            copy.child().write_all(b"{\"type\":\"result\"}\n").unwrap();
            let mut child = copy.child.take().unwrap();
            let (copied, end) = copy.wait_for_end();
            assert!(copied.is_empty(), "a line nobody received was shown");
            assert!(
                matches!(&end, End::WriteFailed(error) if error.kind() == ErrorKind::BrokenPipe),
                "{end:?}"
            );
            assert_eq!(
                child.write_all(b"next\n").unwrap_err().kind(),
                ErrorKind::BrokenPipe
            );
        }

        /// The copy thread tells the tap when the copy ends, however it
        /// ends, and does not wait for the child: a client that closed
        /// stdout ends it at hcom's first failed write, although the child,
        /// whose last line went into the pipe without error, runs on and
        /// writes nothing more.
        #[test]
        fn the_tap_learns_that_the_copy_ended_while_the_child_runs_on() {
            for client_closed in [true, false] {
                let (client, sink) = std::io::pipe().unwrap();
                let (source, child) = std::io::pipe().unwrap();
                let mut child = Some(child);
                let _client = (!client_closed).then_some(client);
                let log = Arc::new(Mutex::new(Vec::new()));
                let tap = Arc::new(RecordingTap {
                    log: log.clone(),
                    panic_on_observe: false,
                });
                let forwarder = Forwarder::spawn(source.into(), sink.into(), tap, at(1)).unwrap();
                if client_closed {
                    let child = child.as_mut().unwrap();
                    child.write_all(b"{\"type\":\"result\"}\n").unwrap();
                } else {
                    // EOF: every writer of the pipe closed it.
                    child = None;
                }
                let deadline = Instant::now() + Duration::from_secs(30);
                while !forwarder.thread.is_finished() {
                    assert!(
                        Instant::now() < deadline,
                        "client_closed={client_closed}: the copy never ended"
                    );
                    std::thread::sleep(Duration::from_millis(5));
                }
                forwarder.thread.join().unwrap();
                assert_eq!(
                    *log.lock().unwrap(),
                    [Copied::Ended],
                    "client_closed={client_closed}"
                );
                if let Some(mut child) = child {
                    assert_eq!(
                        child.write_all(b"next\n").unwrap_err().kind(),
                        ErrorKind::BrokenPipe
                    );
                }
            }
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

        /// A client pipe filled to capacity, so the next write blocks until
        /// the client reads. Returns the filler.
        fn fill(sink: &std::io::PipeWriter) -> usize {
            use nix::fcntl::{FcntlArg, OFlag, fcntl};

            let flags = OFlag::from_bits_truncate(fcntl(sink.as_fd(), FcntlArg::F_GETFL).unwrap());
            fcntl(sink.as_fd(), FcntlArg::F_SETFL(flags | OFlag::O_NONBLOCK)).unwrap();
            let mut filled = 0;
            while let Ok(written) = nix::unistd::write(sink.as_fd(), &[b'.'; 4096]) {
                filled += written;
            }
            fcntl(sink.as_fd(), FcntlArg::F_SETFL(flags)).unwrap();
            filled
        }

        #[test]
        fn the_drain_delivers_everything_to_a_client_paused_past_the_idle_bound() {
            for descendant in [false, true] {
                let (mut client, sink) = std::io::pipe().unwrap();
                let filler = fill(&sink);
                let log = Arc::new(Mutex::new(Vec::new()));
                let mut copy = Copier::start(
                    move |bytes| write_all(sink.as_fd(), bytes),
                    &log,
                    false,
                    FAST,
                );
                // The child's last output: more than the client pipe holds
                // with the filler still in it, less than the copy's buffer
                // and the child pipe together, so the child can exit first.
                let data: Vec<u8> = (0..60_000u32).map(|i| b'a' + (i % 26) as u8).collect();
                copy.child().write_all(&data).unwrap();
                let held = copy.child.take().filter(|_| descendant);
                copy.exited.take();
                // The client reads nothing for several idle bounds.
                std::thread::sleep(FAST.idle * 5);
                let mut received = Vec::new();
                let reader = std::thread::spawn(move || {
                    client.read_to_end(&mut received).unwrap();
                    received
                });
                let started = Instant::now();
                copy.wait();
                // With a descendant holding the pipe, the copy ends one idle
                // bound after the pipe went empty.
                assert!(
                    started.elapsed() < Duration::from_secs(5),
                    "descendant={descendant}"
                );
                drop(held);
                let received = reader.join().unwrap();
                assert_eq!(
                    received.len(),
                    filler + data.len(),
                    "descendant={descendant}"
                );
                assert!(received[filler..] == data[..], "descendant={descendant}");
            }
        }

        #[test]
        fn a_descendant_holding_an_empty_pipe_ends_the_drain_at_the_idle_bound() {
            let log = Arc::new(Mutex::new(Vec::new()));
            let mut copy = Copier::recording(&log, false, FAST);
            copy.child().write_all(b"last\n").unwrap();
            let _descendant = copy.child.take();
            std::thread::sleep(Duration::from_millis(50));
            let exited = Instant::now();
            copy.exited.take();
            let copied = copy.wait();
            let elapsed = exited.elapsed();
            assert_eq!(written(&copied), b"last\n");
            assert!(elapsed >= FAST.idle, "{elapsed:?}");
            assert!(elapsed < FAST.idle + Duration::from_secs(2), "{elapsed:?}");
        }

        #[test]
        fn past_the_cap_a_writing_descendant_ends_the_drain_once_the_pipe_is_empty() {
            let limits = DrainLimits {
                idle: Duration::from_secs(60),
                cap: Duration::from_millis(300),
            };
            let log = Arc::new(Mutex::new(Vec::new()));
            let mut copy = Copier::recording(&log, false, limits);
            let mut descendant = copy.child.take().unwrap();
            let writer = std::thread::spawn(move || {
                let mut sent = 0;
                // Until the copy is gone and the pipe breaks.
                while descendant.write_all(b"tick\n").is_ok() {
                    sent += 5;
                    std::thread::sleep(Duration::from_millis(20));
                }
                sent
            });
            std::thread::sleep(Duration::from_millis(100));
            let exited = Instant::now();
            copy.exited.take();
            let copied = copy.wait();
            let elapsed = exited.elapsed();
            assert!(elapsed >= limits.cap, "{elapsed:?}");
            assert!(elapsed < limits.cap + Duration::from_secs(2), "{elapsed:?}");
            assert!(written(&copied).chunks(5).all(|tick| tick == b"tick\n"));
            // The read end closed with the copy, so the descendant's next
            // write fails instead of blocking.
            writer.join().unwrap();
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
