//! Ordered PTY writes without blocking the proxy's control loop.

use anyhow::{Context, Result, bail};
use nix::errno::Errno;
use nix::poll::{PollFd, PollFlags, PollTimeout, poll};
use nix::unistd::{pipe, read, write};
use std::collections::VecDeque;
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};

pub(super) const QUEUE_LIMIT: usize = 1024 * 1024;
pub(super) const WRITE_BATCH: usize = 256 * 1024;

#[derive(Default)]
pub(super) struct PendingWrite {
    bytes: VecDeque<u8>,
    written: u64,
}

impl PendingWrite {
    pub fn len(&self) -> usize {
        self.bytes.len()
    }
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
    pub fn push(&mut self, bytes: &[u8]) {
        self.bytes.extend(bytes);
    }
    fn take_batch(&mut self) -> Vec<u8> {
        self.bytes.drain(..self.len().min(WRITE_BATCH)).collect()
    }
    pub fn flush<F: AsFd>(&mut self, fd: &F) -> Result<()> {
        let mut budget = WRITE_BATCH;
        while !self.is_empty() && budget > 0 {
            let (front, _) = self.bytes.as_slices();
            match write(fd, &front[..front.len().min(budget)]) {
                Ok(0) => bail!("write returned zero"),
                Ok(n) => {
                    self.bytes.drain(..n);
                    self.written += n as u64;
                    budget -= n;
                }
                Err(Errno::EINTR) => continue,
                Err(Errno::EAGAIN) => break,
                Err(e) => bail!("write failed: {e}"),
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum InputOrigin {
    User,
    Injected,
}

#[derive(Default)]
pub(super) struct ChildInput {
    pending: PendingWrite,
    completions: VecDeque<(u64, InputOrigin)>,
    receipts: VecDeque<(u64, crate::provenance::Receipt)>,
}

#[derive(Debug, PartialEq)]
pub(super) struct InputCompletion {
    pub origin: InputOrigin,
    pub receipt: Option<crate::provenance::Receipt>,
}

impl ChildInput {
    pub fn len(&self) -> usize {
        self.pending.len()
    }
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }
    pub fn push(&mut self, bytes: &[u8], origin: Option<InputOrigin>) {
        self.pending.push(bytes);
        if let Some(origin) = origin.filter(|_| !bytes.is_empty()) {
            self.completions
                .push_back((self.pending.written + self.len() as u64, origin));
        }
    }
    pub fn push_injected(&mut self, bytes: &[u8], receipt: Option<crate::provenance::Receipt>) {
        self.push(bytes, Some(InputOrigin::Injected));
        if !bytes.is_empty()
            && let Some(receipt) = receipt
        {
            if self.receipts.len() < 64 {
                self.receipts
                    .push_back((self.pending.written + self.len() as u64, receipt));
            } else {
                crate::log::log_warn(
                    "provenance",
                    "pty.pending",
                    "PTY receipt dropped: pending write metadata queue full",
                );
            }
        }
    }
    pub fn flush<F: AsFd>(&mut self, fd: &F) -> Result<Vec<InputCompletion>> {
        self.pending.flush(fd)?;
        let mut completed = Vec::new();
        while self
            .completions
            .front()
            .is_some_and(|(end, _)| *end <= self.pending.written)
        {
            let (end, origin) = self.completions.pop_front().unwrap();
            let receipt = if self
                .receipts
                .front()
                .is_some_and(|(receipt_end, _)| *receipt_end == end)
            {
                self.receipts.pop_front().map(|(_, receipt)| receipt)
            } else {
                None
            };
            completed.push(InputCompletion { origin, receipt });
        }
        Ok(completed)
    }
}

/// A single bounded batch is written by a worker. stdout's file status flags
/// are never changed: even SIGKILL cannot leave the launching shell nonblocking.
/// The worker may block on the terminal; the proxy keeps serving its sockets.
pub(super) struct TerminalOutput {
    sender: mpsc::SyncSender<Vec<u8>>,
    pending: Arc<AtomicUsize>,
    failed: Arc<Mutex<Option<String>>>,
    stopped: Arc<AtomicBool>,
    wake: OwnedFd,
}

impl TerminalOutput {
    pub fn new(stdout: BorrowedFd<'_>) -> Result<Self> {
        let output = stdout
            .try_clone_to_owned()
            .context("duplicate stdout failed")?;
        let (wake, wake_write) = pipe()?;
        super::set_nonblocking(&wake)?;
        super::set_nonblocking(&wake_write)?;
        let (sender, receiver) = mpsc::sync_channel::<Vec<u8>>(1);
        let pending = Arc::new(AtomicUsize::new(0));
        let failed = Arc::new(Mutex::new(None));
        let stopped = Arc::new(AtomicBool::new(false));
        let worker_pending = pending.clone();
        let worker_failed = failed.clone();
        let worker_stopped = stopped.clone();
        std::thread::Builder::new()
            .name("pty-stdout".into())
            .spawn(move || {
                while let Ok(bytes) = receiver.recv() {
                    let mut offset = 0;
                    while offset < bytes.len() {
                        if worker_stopped.load(Ordering::Acquire) {
                            return;
                        }
                        match write(&output, &bytes[offset..]) {
                            Ok(n) if n > 0 => offset += n,
                            Err(Errno::EINTR) => continue,
                            Err(Errno::EAGAIN) => {
                                let mut fds = [PollFd::new(output.as_fd(), PollFlags::POLLOUT)];
                                // Timeout only checks cancellation; it never drops bytes.
                                let _ = poll(&mut fds, PollTimeout::from(100u16));
                                continue;
                            }
                            result => {
                                *worker_failed.lock().unwrap() =
                                    Some(format!("stdout write failed: {result:?}"));
                                let _ = write(&wake_write, &[1]);
                                return;
                            }
                        }
                    }
                    worker_pending.store(0, Ordering::Release);
                    let _ = write(&wake_write, &[1]);
                }
            })?;
        Ok(Self {
            sender,
            pending,
            failed,
            stopped,
            wake,
        })
    }
    pub fn wake_fd(&self) -> BorrowedFd<'_> {
        self.wake.as_fd()
    }
    pub fn len(&self) -> usize {
        self.pending.load(Ordering::Acquire)
    }
    pub fn pump(&self, queue: &mut PendingWrite) -> Result<()> {
        let mut buf = [0u8; 64];
        while read(&self.wake, &mut buf).is_ok_and(|n| n > 0) {}
        if let Some(error) = self.failed.lock().unwrap().as_ref() {
            bail!("{error}");
        }
        if self.len() == 0 && !queue.is_empty() {
            let bytes = queue.take_batch();
            self.pending.store(bytes.len(), Ordering::Release);
            // Only one batch is outstanding, so this cannot wait for capacity.
            self.sender
                .try_send(bytes)
                .context("stdout worker stopped")?;
        }
        Ok(())
    }
}

impl Drop for TerminalOutput {
    fn drop(&mut self) {
        self.stopped.store(true, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nix::fcntl::{FcntlArg, fcntl};
    use std::io::Read;
    use std::os::unix::net::UnixStream;
    use std::time::{Duration, Instant};

    #[test]
    fn input_completion_waits_for_all_bytes_to_be_written() {
        let (writer, mut reader) = UnixStream::pair().unwrap();
        writer.set_nonblocking(true).unwrap();
        reader.set_nonblocking(true).unwrap();
        let mut input = ChildInput::default();
        input.push(&vec![b'x'; QUEUE_LIMIT], Some(InputOrigin::Injected));
        let before = input.len();
        assert!(input.flush(&writer).unwrap().is_empty());
        assert!(input.len() < before);
        input.push(b"y", Some(InputOrigin::User));
        let mut completed = Vec::new();
        let mut buf = [0u8; 65536];
        while !input.is_empty() {
            while reader.read(&mut buf).is_ok_and(|n| n > 0) {}
            completed.extend(input.flush(&writer).unwrap());
        }
        assert_eq!(
            completed.iter().map(|c| c.origin).collect::<Vec<_>>(),
            [InputOrigin::Injected, InputOrigin::User]
        );
        assert!(completed.iter().all(|c| c.receipt.is_none()));
    }

    #[test]
    fn injection_receipt_waits_for_full_write_and_never_attaches_to_user_input() {
        let (writer, mut reader) = UnixStream::pair().unwrap();
        writer.set_nonblocking(true).unwrap();
        reader.set_nonblocking(true).unwrap();
        let bytes = vec![b'x'; QUEUE_LIMIT];
        let receipt = crate::provenance::Receipt::new(
            "delivery",
            "pty_write",
            &bytes,
            crate::provenance::system_actor(),
            serde_json::json!({"name": "luna"}),
        );
        let mut input = ChildInput::default();
        input.push_injected(&bytes, Some(receipt.clone()));
        assert!(input.flush(&writer).unwrap().is_empty());
        input.push(b"human input", Some(InputOrigin::User));
        // An empty decoded injection must not steal the prior completion.
        input.push_injected(b"", Some(receipt.clone()));
        let mut completed = Vec::new();
        let mut buf = [0u8; 65536];
        while !input.is_empty() {
            while reader.read(&mut buf).is_ok_and(|n| n > 0) {}
            completed.extend(input.flush(&writer).unwrap());
        }
        assert_eq!(completed.len(), 2);
        assert_eq!(completed[0].receipt, Some(receipt));
        assert_eq!(completed[0].origin, InputOrigin::Injected);
        assert_eq!(completed[1].origin, InputOrigin::User);
        assert!(completed[1].receipt.is_none());
    }

    #[test]
    fn failed_pty_write_does_not_complete_receipt() {
        let (writer, reader) = UnixStream::pair().unwrap();
        drop(reader);
        let mut input = ChildInput::default();
        input.push_injected(
            b"text",
            Some(crate::provenance::Receipt::new(
                "delivery",
                "pty_write",
                b"text",
                crate::provenance::system_actor(),
                serde_json::json!({"name": "luna"}),
            )),
        );
        assert!(input.flush(&writer).is_err());
        assert_eq!(input.completions.len(), 1);
    }

    #[test]
    fn injection_receipt_metadata_is_bounded_without_dropping_input() {
        let mut input = ChildInput::default();
        let receipt = crate::provenance::Receipt::new(
            "delivery",
            "pty_write",
            b"x",
            crate::provenance::system_actor(),
            serde_json::json!({"name": "luna"}),
        );
        for _ in 0..65 {
            input.push_injected(b"x", Some(receipt.clone()));
        }
        assert_eq!(input.receipts.len(), 64);
        assert_eq!(input.completions.len(), 65);
        assert_eq!(input.len(), 65);
    }

    #[test]
    fn stdout_worker_preserves_flags_and_order_when_reader_pauses() {
        let (writer, mut reader) = UnixStream::pair().unwrap();
        reader.set_nonblocking(true).unwrap();
        let flags = fcntl(writer.as_fd(), FcntlArg::F_GETFL).unwrap() & libc::O_NONBLOCK;
        let output = TerminalOutput::new(writer.as_fd()).unwrap();
        assert_eq!(
            fcntl(writer.as_fd(), FcntlArg::F_GETFL).unwrap() & libc::O_NONBLOCK,
            flags
        );
        let mut queue = PendingWrite::default();
        // Split UTF-8, CSI, and OSC around separate queued writes. No worker
        // boundary may reorder the title or any continuation bytes.
        let parts = [
            [vec![b'x'; WRITE_BATCH - 1], vec![0xe2]].concat(),
            b"\x94\x80\x1b[31".to_vec(),
            b"m\x1b]8;;https://example".to_vec(),
            b".com\x07link\x1b]8;;\x07".to_vec(),
            b"\x1b]2;title\x07tail".to_vec(),
        ];
        let expected = parts.concat();
        for part in &parts {
            queue.push(part);
        }
        output.pump(&mut queue).unwrap();
        assert_eq!(output.len(), WRITE_BATCH);
        // A second pump must not wait on the blocked writer.
        output.pump(&mut queue).unwrap();
        let deadline = Instant::now() + Duration::from_secs(3);
        let mut actual = Vec::new();
        let mut buf = [0u8; 65536];
        while !queue.is_empty() || output.len() > 0 || actual.len() < expected.len() {
            while let Ok(n) = reader.read(&mut buf) {
                if n == 0 {
                    break;
                }
                actual.extend_from_slice(&buf[..n]);
            }
            output.pump(&mut queue).unwrap();
            assert!(Instant::now() < deadline, "stdout never drained");
            let mut wake = [PollFd::new(output.wake_fd(), PollFlags::POLLIN)];
            let _ = poll(&mut wake, PollTimeout::from(1u16));
        }
        assert_eq!(actual, expected);
        assert_eq!(
            fcntl(writer.as_fd(), FcntlArg::F_GETFL).unwrap() & libc::O_NONBLOCK,
            flags
        );
    }
}
