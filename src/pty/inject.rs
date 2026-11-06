//! TCP injection server — accepts text on a local port and writes to PTY master.

use anyhow::{Context, Result};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
#[cfg(unix)]
use std::os::fd::AsRawFd;

/// Magic prefix for query commands (not injection)
const QUERY_PREFIX: u8 = 0x00;
// Bound incomplete requests as well as the PTY queue. Larger requests are
// rejected rather than letting a sender hold unbounded memory in the proxy.
const MAX_REQUEST_BYTES: usize = 1024 * 1024;
const MAX_CLIENTS: usize = 32;

/// Result of reading from an inject client
pub enum InjectResult {
    /// Text to inject into PTY
    Inject(String),
    /// Query command — client removed from vec, caller must respond via stream
    Query(QueryClient),
    /// No data ready yet
    Pending,
}

/// A query client removed from the connection pool, ready for response
pub struct QueryClient {
    stream: TcpStream,
    pub command: QueryCommand,
}

#[derive(Debug)]
pub enum QueryCommand {
    Screen,
    Unknown,
}

impl QueryClient {
    /// Send response and close connection
    pub fn respond(mut self, response: &str) {
        let _ = self.stream.write_all(response.as_bytes());
        let _ = self.stream.flush();
        // stream dropped here, connection closed
    }
}

/// TCP server for text injection
pub struct InjectServer {
    listener: TcpListener,
    port: u16,
    clients: Vec<(TcpStream, Vec<u8>)>,
}

impl InjectServer {
    /// Create a new injection server on localhost
    pub fn new() -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").context("Failed to bind inject server")?;
        let port = listener.local_addr()?.port();

        // Set non-blocking
        listener.set_nonblocking(true)?;

        Ok(Self {
            listener,
            port,
            clients: Vec::new(),
        })
    }

    /// Get the port the server is listening on
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Get the listener raw file descriptor for polling (Unix poll loop only).
    #[cfg(unix)]
    pub fn listener_raw_fd(&self) -> i32 {
        self.listener.as_raw_fd()
    }

    /// Screen queries remain readable even when the child stops consuming input.
    #[cfg(unix)]
    pub fn pollable_clients(&self, input_budget: usize) -> Vec<(usize, i32)> {
        self.clients
            .iter()
            .enumerate()
            .filter_map(|(index, (stream, buffer))| {
                let mut first = [0u8; 1];
                let peek = stream.peek(&mut first).ok();
                let query = buffer
                    .first()
                    .copied()
                    .or_else(|| (peek == Some(1)).then_some(first[0]))
                    == Some(QUERY_PREFIX);
                let can_read = if peek == Some(0) && !buffer.is_empty() {
                    let decoded_len = Self::process_inject_data(buffer).len();
                    decoded_len <= input_budget || decoded_len > MAX_REQUEST_BYTES
                } else {
                    buffer.len() < input_budget || buffer.len() >= MAX_REQUEST_BYTES
                };
                // An unclassified idle connection must stay polled so a SCREEN
                // command arriving later wakes the proxy under input backpressure.
                let unknown = buffer.is_empty() && peek.is_none();
                (query || can_read || unknown).then_some((index, stream.as_raw_fd()))
            })
            .collect()
    }

    /// Number of connected inject clients (portable; the Unix loop uses
    /// `pollable_clients`, but tests and the Windows loop need this
    /// without raw fds).
    #[cfg(any(test, windows))]
    pub fn client_count(&self) -> usize {
        self.clients.len()
    }

    /// Accept a new connection.
    ///
    /// Returns `Ok(true)` if a connection was accepted, `Ok(false)` if the accept
    /// queue was empty (WouldBlock). The caller uses this to apply backoff on
    /// macOS, where a non-blocking listener can keep reporting POLLIN via poll()
    /// even after the accept queue is drained.
    pub fn accept(&mut self) -> Result<bool> {
        match self.listener.accept() {
            Ok((stream, _addr)) => {
                stream.set_nonblocking(true)?;
                if self.clients.len() >= MAX_CLIENTS {
                    let _ = (&stream).write_all(b"error: too many pending injection clients\n");
                    return Ok(true);
                }
                self.clients.push((stream, Vec::new()));
                Ok(true)
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// Read from a client by index. Returns InjectResult:
    /// - Inject(text): text to write to PTY
    /// - ScreenQuery(index): caller should dump screen and call respond_query()
    /// - Pending: no data ready yet
    #[cfg(any(windows, test))]
    pub fn read_client(&mut self, index: usize) -> Result<InjectResult> {
        self.read_client_with_budget(index, MAX_REQUEST_BYTES)
    }

    pub fn read_client_with_budget(
        &mut self,
        index: usize,
        input_budget: usize,
    ) -> Result<InjectResult> {
        if index >= self.clients.len() {
            return Ok(InjectResult::Pending);
        }

        let (stream, buffer) = &mut self.clients[index];
        let mut buf = [0u8; 8192];

        let mut read_bytes = 0;
        loop {
            let mut first = [0u8; 1];
            let query = buffer
                .first()
                .copied()
                .or_else(|| (stream.peek(&mut first).ok() == Some(1)).then_some(first[0]))
                == Some(QUERY_PREFIX);
            let limit = if query {
                MAX_REQUEST_BYTES
            } else {
                input_budget.min(MAX_REQUEST_BYTES)
            };
            let capacity = limit.saturating_sub(buffer.len());
            let count = buf.len().min(capacity);
            // At the budget boundary, only consume EOF. Leave further bytes
            // in TCP so the sender experiences backpressure until PTY input drains.
            if count == 0 {
                let mut peek = [0u8; 1];
                match stream.peek(&mut peek) {
                    Ok(0) => {}
                    Ok(_) if buffer.len() >= MAX_REQUEST_BYTES => {
                        let _ = stream.write_all(b"error: injection request exceeds 1 MiB\n");
                        crate::log::log_warn(
                            "native",
                            "inject.request_too_large",
                            "Rejected injection request exceeding 1 MiB",
                        );
                        self.clients.remove(index);
                        return Ok(InjectResult::Pending);
                    }
                    _ => return Ok(InjectResult::Pending),
                }
            }
            // A one-byte read here observes the EOF established by peek above.
            match stream.read(&mut buf[..count.max(1)]) {
                Ok(0) => {
                    // EOF - client closed, process the data
                    let data = buffer.as_slice();

                    // Check for command (starts with \x00)
                    if data.first() == Some(&QUERY_PREFIX) {
                        let cmd = std::str::from_utf8(&data[1..]).unwrap_or("").trim();
                        let command = match cmd {
                            "SCREEN" => QueryCommand::Screen,
                            _ => QueryCommand::Unknown,
                        };
                        let (stream, _) = self.clients.remove(index);
                        return Ok(InjectResult::Query(QueryClient { stream, command }));
                    }

                    let text = Self::process_inject_data(data);
                    if text.len() > MAX_REQUEST_BYTES {
                        let _ = stream.write_all(b"error: decoded injection exceeds 1 MiB\n");
                        self.clients.remove(index);
                        return Ok(InjectResult::Pending);
                    }
                    if text.len() > input_budget {
                        return Ok(InjectResult::Pending);
                    }
                    self.clients.remove(index);
                    return Ok(InjectResult::Inject(text));
                }
                Ok(n) => {
                    buffer.extend_from_slice(&buf[..n]);
                    read_bytes += n;
                    if read_bytes >= 64 * 1024 {
                        return Ok(InjectResult::Pending);
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    break;
                }
                Err(e) => {
                    self.clients.remove(index);
                    return Err(e.into());
                }
            }
        }

        Ok(InjectResult::Pending)
    }

    /// Process injection data: decode and strip trailing LF
    /// Fix #7: Use UTF-8 with Latin-1 fallback instead of lossy (which mangles bytes)
    fn process_inject_data(data: &[u8]) -> String {
        let mut text = match String::from_utf8(data.to_vec()) {
            Ok(s) => s,
            Err(_) => {
                // Fallback to Latin-1 (preserves all byte values as chars)
                data.iter().map(|&b| b as char).collect()
            }
        };

        // Strip single trailing LF (from echo/nc), preserve CR for submit
        if text.ends_with('\n') {
            text.pop();
        }

        text
    }
}

#[cfg(test)]
mod tests {
    use super::{InjectResult, InjectServer};
    use std::io::Write;
    use std::net::{Shutdown, TcpStream};
    use std::thread;
    use std::time::{Duration, Instant};

    #[test]
    fn injection_decoding_preserves_legacy_bytes_at_receipt_boundary() {
        assert_eq!(
            InjectServer::process_inject_data("café\r\n".as_bytes()),
            "café\r"
        );
        assert_eq!(InjectServer::process_inject_data(b"two\n\n"), "two\n");
        assert_eq!(InjectServer::process_inject_data(b"\xe9\r"), "é\r");
        assert_eq!(InjectServer::process_inject_data(b"\n"), "");
    }

    #[test]
    fn accept_returns_false_when_queue_is_empty() {
        let mut server = InjectServer::new().unwrap();

        assert!(!server.accept().unwrap());
        assert_eq!(server.client_count(), 0);
    }

    #[test]
    fn accept_returns_true_when_connection_is_pending() {
        let mut server = InjectServer::new().unwrap();
        let _client = TcpStream::connect(("127.0.0.1", server.port())).unwrap();

        let deadline = Instant::now() + Duration::from_secs(1);
        let accepted = loop {
            if server.accept().unwrap() {
                break true;
            }
            if Instant::now() >= deadline {
                break false;
            }
            thread::sleep(Duration::from_millis(10));
        };

        assert!(accepted);
        assert_eq!(server.client_count(), 1);
    }

    /// Poll `f` until it returns true or a second passes; under load the
    /// loopback connect and its bytes are not visible to the server instantly.
    #[cfg(unix)]
    fn eventually(mut f: impl FnMut() -> bool) -> bool {
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            if f() {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            thread::sleep(Duration::from_millis(1));
        }
    }

    #[cfg(unix)]
    #[test]
    fn screen_queries_bypass_input_backpressure() {
        let mut server = InjectServer::new().unwrap();
        let mut text = TcpStream::connect(("127.0.0.1", server.port())).unwrap();
        text.write_all(b"queued").unwrap();
        text.shutdown(Shutdown::Write).unwrap();
        assert!(eventually(|| server.accept().unwrap()));
        assert!(eventually(|| {
            assert!(matches!(
                server.read_client_with_budget(0, 3).unwrap(),
                InjectResult::Pending
            ));
            server.clients[0].1.len() == 3
        }));
        assert_eq!(server.clients[0].1, b"que");
        assert!(server.pollable_clients(0).is_empty());

        let mut query = TcpStream::connect(("127.0.0.1", server.port())).unwrap();
        assert!(eventually(|| server.accept().unwrap()));
        // Unclassified connections remain polled even before their first byte.
        assert_eq!(server.pollable_clients(0).len(), 1);
        query.write_all(b"\0SCREEN\n").unwrap();
        query.shutdown(Shutdown::Write).unwrap();
        assert!(eventually(|| matches!(
            server.read_client_with_budget(1, 0).unwrap(),
            InjectResult::Query(_)
        )));
        assert!(
            matches!(server.read_client_with_budget(0, 6).unwrap(), InjectResult::Inject(text) if text == "queued")
        );
    }

    #[test]
    fn completed_clients_can_be_drained_in_connection_order() {
        let mut server = InjectServer::new().unwrap();
        let mut first = TcpStream::connect(("127.0.0.1", server.port())).unwrap();
        first.write_all(b"text").unwrap();
        first.shutdown(Shutdown::Write).unwrap();
        let mut second = TcpStream::connect(("127.0.0.1", server.port())).unwrap();
        second.write_all(b"\r").unwrap();
        second.shutdown(Shutdown::Write).unwrap();

        let deadline = Instant::now() + Duration::from_secs(1);
        while server.client_count() < 2 && Instant::now() < deadline {
            let _ = server.accept();
            thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(server.client_count(), 2);

        let read_next = |server: &mut InjectServer| {
            let deadline = Instant::now() + Duration::from_secs(1);
            loop {
                if let InjectResult::Inject(text) = server.read_client(0).unwrap() {
                    break text;
                }
                assert!(Instant::now() < deadline, "client did not complete");
                thread::sleep(Duration::from_millis(5));
            }
        };
        assert_eq!(read_next(&mut server), "text");
        assert_eq!(read_next(&mut server), "\r");
    }
}
