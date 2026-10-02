use std::io;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use serde_json::Value;

use crate::ipc::{self, Conn};
use crate::protocol::*;

const DEFAULT_IO_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_MAX_ATTEMPTS: u32 = 40;
const RETRY_DELAY: Duration = Duration::from_millis(50);

/// What one connect+write+read inside the bounded wait hands back.
type Exchanged = Result<(Option<Vec<u8>>, Box<dyn Conn>), Error>;

#[derive(Debug)]
pub enum Error {
    /// Deterministic: same key, different request. Never retried.
    Conflict(String),
    /// Deterministic: the request itself is invalid or was rejected by SQLite.
    Invalid(String),
    /// Transient contention; exhausted retries. Safe to retry with the same key.
    Busy(String),
    /// Retries exhausted with an ambiguous or unavailable outcome.
    /// The caller may retry with the same key; the service deduplicates.
    Uncertain(String),
    Io(io::Error),
    Protocol(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Conflict(m) => write!(f, "conflict: {m}"),
            Error::Invalid(m) => write!(f, "invalid: {m}"),
            Error::Busy(m) => write!(f, "busy: {m}"),
            Error::Uncertain(m) => write!(f, "uncertain outcome after retries: {m}"),
            Error::Io(e) => write!(f, "ipc: {e}"),
            Error::Protocol(m) => write!(f, "protocol: {m}"),
        }
    }
}

impl std::error::Error for Error {}

pub struct Client {
    endpoint: String,
    io_timeout: Duration,
    max_attempts: u32,
    next_id: u64,
    stream: Option<Box<dyn Conn>>,
}

impl Client {
    pub fn new(endpoint: impl Into<String>) -> Client {
        Client {
            endpoint: endpoint.into(),
            io_timeout: DEFAULT_IO_TIMEOUT,
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            next_id: 0,
            stream: None,
        }
    }

    pub fn with_io_timeout(mut self, d: Duration) -> Client {
        self.io_timeout = d;
        self
    }

    pub fn with_max_attempts(mut self, n: u32) -> Client {
        self.max_attempts = n.max(1);
        self
    }

    pub fn ping(&mut self) -> Result<(), Error> {
        let id = self.alloc_id();
        match self.exchange(&Request::ping(id)) {
            Ok(resp) if resp.ok => Ok(()),
            Ok(resp) => Err(Error::Invalid(resp.error.unwrap_or_default())),
            Err(e) => Err(e),
        }
    }

    /// Readiness probe: writer heartbeat, queue, and WAL/checkpoint state.
    /// Performs no writes and never enters the write queue.
    pub fn ready(&mut self) -> Result<Value, Error> {
        let id = self.alloc_id();
        match self.exchange(&Request::ready(id)) {
            Ok(resp) if resp.ok => Ok(resp.result.unwrap_or(Value::Null)),
            Ok(resp) => Err(Error::Invalid(resp.error.unwrap_or_default())),
            Err(e) => Err(e),
        }
    }

    /// Execute statements under an idempotency key. Retries transient failures
    /// with the SAME key, so an ambiguous outcome (disconnect after commit)
    /// resolves to the original result instead of a duplicate operation.
    pub fn exec(&mut self, key: &str, stmts: Vec<Stmt>) -> Result<Value, Error> {
        let req = Request::exec(self.alloc_id(), key, stmts);
        #[allow(unused_assignments)]
        let mut last = String::from("no attempts made");
        let mut was_busy: bool;
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            match self.exchange(&req) {
                Ok(resp) if resp.ok => return Ok(resp.result.unwrap_or(Value::Null)),
                Ok(resp) => {
                    let code = resp.code.clone().unwrap_or_default();
                    let msg = resp.error.unwrap_or_default();
                    match code.as_str() {
                        "conflict" => return Err(Error::Conflict(msg)),
                        "invalid" => return Err(Error::Invalid(msg)),
                        "busy" => {
                            last = msg;
                            was_busy = true;
                        }
                        "internal" => {
                            last = msg;
                            was_busy = false;
                        }
                        other => {
                            last = format!("unexpected response code {other:?}: {msg}");
                            was_busy = false;
                        }
                    }
                }
                Err(Error::Io(e))
                    if matches!(
                        e.kind(),
                        io::ErrorKind::InvalidInput | io::ErrorKind::InvalidData
                    ) =>
                {
                    // A malformed/oversized frame or other protocol-level
                    // rejection can never succeed on retry — not "uncertain".
                    return Err(Error::Io(e));
                }
                Err(e @ Error::Protocol(_)) => return Err(e),
                Err(e) => {
                    last = e.to_string();
                    was_busy = false;
                }
            }
            if attempt >= self.max_attempts {
                if was_busy {
                    return Err(Error::Busy(last));
                }
                return Err(Error::Uncertain(format!(
                    "gave up after {attempt} attempts (last error: {last}); \
                     retrying with key {key:?} is safe and returns the original result"
                )));
            }
            thread::sleep(RETRY_DELAY);
        }
    }

    fn alloc_id(&mut self) -> u64 {
        self.next_id += 1;
        self.next_id
    }

    fn exchange(&mut self, req: &Request) -> Result<Response, Error> {
        let payload = serde_json::to_vec(req).map_err(|e| Error::Protocol(e.to_string()))?;

        // Connect, write, and read all inside one bounded wait: a blocked pipe
        // open, large frame, or half-open connection must not hang the caller
        // past io_timeout. The prior stream (if any) moves into the thread.
        let prior = self.stream.take();
        let endpoint = self.endpoint.clone();
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            let out = (|| -> Exchanged {
                let mut s = match prior {
                    Some(s) => s,
                    None => ipc::connect(&endpoint).map_err(Error::Io)?,
                };
                write_frame(&mut *s, &payload).map_err(Error::Io)?;
                let frame = read_frame(&mut *s).map_err(Error::Io)?;
                Ok((frame, s))
            })();
            let _ = tx.send(out);
        });

        let (frame, stream) = match rx.recv_timeout(self.io_timeout) {
            Ok(Ok(v)) => v,
            Ok(Err(e)) => return Err(e),
            Err(mpsc::RecvTimeoutError::Timeout) => {
                // Connection abandoned; when (if) the service replies, the pipe
                // errors and the reader thread exits on its own.
                return Err(Error::Io(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "timed out waiting for reply; result may be committed",
                )));
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Err(Error::Io(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "connection thread died",
                )));
            }
        };

        let bytes = match frame {
            Some(b) => b,
            None => {
                return Err(Error::Io(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "service closed the connection without a reply",
                )));
            }
        };
        // Only a complete response re-arms the connection; Io/timeout paths
        // leave stream None (reconnect next attempt), and a consumed
        // Protocol-error frame is safe to keep armed.
        self.stream = Some(stream);
        let resp: Response = serde_json::from_slice(&bytes)
            .map_err(|e| Error::Protocol(format!("bad response: {e}")))?;
        if resp.v != PROTO_VERSION {
            return Err(Error::Protocol(format!("response version {}", resp.v)));
        }
        if resp.id != req.id {
            return Err(Error::Protocol(format!(
                "response correlation id {} does not match request {}",
                resp.id, req.id
            )));
        }
        Ok(resp)
    }
}
