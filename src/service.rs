use std::env;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, Sender, SyncSender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
use rusqlite::{Connection, Error as SqlError};
use serde_json::{json, Value};

use crate::ipc::{self, Conn};
use crate::protocol::*;

const MAX_BATCH: usize = 64;
const DEFAULT_QUEUE_BYTES: usize = 8 * 1024 * 1024;
// Attempt a blocking WAL truncation only once the WAL clearly outgrew the
// auto-checkpoint target (1000 pages ≈ 4 MiB), and at most once per second.
// ponytail: fixed constants, not config; upgrade path = env knobs if operators
// need different thresholds.
const WAL_TRUNCATE_BYTES: u64 = 8 * 1024 * 1024;
const CKPT_RETRY_SECS: u64 = 1;
// Windows named pipes allow at most 255 instances; stay well below so the
// spare-instance slot always exists. ponytail: raise only with per-IP limits.
const MAX_CONNS: usize = 200;

pub struct Config {
    pub db_path: PathBuf,
    pub endpoint: String,
    pub queue_capacity: usize,
    pub queue_max_bytes: usize,
    pub busy_timeout_ms: u64,
    pub wal_truncate_bytes: u64,
}

impl Config {
    pub fn from_env() -> Result<Config, String> {
        let db_path = PathBuf::from(env::var("SQLW_DB").map_err(|_| "SQLW_DB is not set")?);
        // Local storage only: WAL over SMB/NFS is unsafe. The UNC form is the
        // reliably detectable network path on Windows; other network mounts
        // are documented (README) as the operator's responsibility.
        #[cfg(windows)]
        {
            let p = db_path.to_string_lossy();
            if p.starts_with(r"\\") || p.starts_with("//") {
                return Err(format!(
                    "SQLW_DB must be a plain local path (WAL is unsafe on \
                     network shares; the \\\\?\\ extended form is refused too): {p}"
                ));
            }
        }
        Ok(Config {
            db_path,
            endpoint: env::var("SQLW_ENDPOINT").map_err(|_| "SQLW_ENDPOINT is not set")?,
            queue_capacity: parse_env("SQLW_QUEUE_CAP", 1024)?,
            queue_max_bytes: parse_env("SQLW_QUEUE_MAX_BYTES", DEFAULT_QUEUE_BYTES)?,
            busy_timeout_ms: parse_env("SQLW_BUSY_TIMEOUT_MS", 5000)?,
            wal_truncate_bytes: parse_env("SQLW_WAL_TRUNCATE_BYTES", WAL_TRUNCATE_BYTES)?,
        })
    }
}

fn parse_env<T: std::str::FromStr>(key: &str, default: T) -> Result<T, String> {
    match env::var(key) {
        Ok(v) => v.parse().map_err(|_| format!("{key}: invalid value {v:?}")),
        Err(_) => Ok(default),
    }
}

struct Item {
    req: Request,
    reply: Sender<Response>,
    bytes: usize,
    enqueued_at: Instant,
}

/// Request queue with BOTH limits: item count (sync_channel) and total
/// reserved bytes (mutex + condvar gate). Senders block until both fit.
struct Queue {
    tx: SyncSender<Item>,
    gate: Mutex<usize>,
    not_full: Condvar,
    cap_bytes: usize,
}

#[derive(Debug)]
enum QueueSend {
    TooLarge,
    WriterGone,
}

impl Queue {
    fn new(capacity: usize, cap_bytes: usize) -> (Queue, Receiver<Item>) {
        let (tx, rx) = mpsc::sync_channel(capacity);
        (
            Queue {
                tx,
                gate: Mutex::new(0),
                not_full: Condvar::new(),
                cap_bytes,
            },
            rx,
        )
    }

    fn send(&self, item: Item) -> Result<(), QueueSend> {
        if item.bytes > self.cap_bytes {
            return Err(QueueSend::TooLarge);
        }
        let bytes = item.bytes;
        let mut held = lock(&self.gate);
        while *held + bytes > self.cap_bytes {
            held = wait(&self.not_full, held);
        }
        *held += bytes;
        drop(held);
        if self.tx.send(item).is_err() {
            self.release(bytes);
            return Err(QueueSend::WriterGone);
        }
        Ok(())
    }

    /// Return a reserved byte slot (item dequeued or send aborted).
    fn release(&self, bytes: usize) {
        {
            let mut held = lock(&self.gate);
            *held = held.saturating_sub(bytes);
        }
        self.not_full.notify_all();
    }

    fn bytes(&self) -> usize {
        *lock(&self.gate)
    }
}

// Ponytail: poison recovery instead of panics — one poisoned lock must not
// take down every connection thread; values are counters, corruption-safe.
fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

fn wait<'a, T>(c: &Condvar, g: std::sync::MutexGuard<'a, T>) -> std::sync::MutexGuard<'a, T> {
    c.wait(g).unwrap_or_else(|e| e.into_inner())
}

/// Service-wide readiness counters, shared with connection threads.
struct Shared {
    depth: AtomicUsize,
    last_batch_unix_ms: AtomicU64,
    last_queue_wait_us: AtomicU64,
    max_queue_wait_us: AtomicU64,
    last_commit_us: AtomicU64,
    max_commit_us: AtomicU64,
    // Commit health: readiness must distinguish "writer alive" from "writer
    // committing" — a persistently failing writer is not ready.
    last_commit_unix_ms: AtomicU64,
    commit_failures: AtomicU64,
    wal_bytes: AtomicU64,
    ckpt: Mutex<CkptStat>,
    // Effective settings from the writer connection (set once at startup).
    settings: Mutex<Value>,
}

#[derive(Clone, serde::Serialize)]
struct CkptStat {
    status: &'static str, // never | ok | busy | error
    at_unix_ms: u64,
    wal_before: u64,
    wal_after: u64,
    msg: String,
}

impl Default for Shared {
    fn default() -> Shared {
        Shared {
            depth: AtomicUsize::new(0),
            last_batch_unix_ms: AtomicU64::new(0),
            last_queue_wait_us: AtomicU64::new(0),
            max_queue_wait_us: AtomicU64::new(0),
            last_commit_us: AtomicU64::new(0),
            max_commit_us: AtomicU64::new(0),
            last_commit_unix_ms: AtomicU64::new(0),
            commit_failures: AtomicU64::new(0),
            wal_bytes: AtomicU64::new(0),
            ckpt: Mutex::new(CkptStat {
                status: "never",
                at_unix_ms: 0,
                wal_before: 0,
                wal_after: 0,
                msg: String::new(),
            }),
            settings: Mutex::new(Value::Null),
        }
    }
}

/// Test-only fault-injection hooks. Compiled out of release builds
/// (debug_assertions): production binaries cannot activate SQLW_TEST_* at all.
#[derive(Default)]
struct Hooks {
    before_commit: Option<Duration>,
    after_commit: Option<Duration>,
    #[cfg(debug_assertions)]
    panic_on_start: bool,
}

impl Hooks {
    #[cfg(debug_assertions)]
    fn from_env() -> Hooks {
        let hooks = Hooks {
            before_commit: env_ms("SQLW_TEST_HOLD_BEFORE_COMMIT_MS"),
            after_commit: env_ms("SQLW_TEST_HOLD_AFTER_COMMIT_MS"),
            panic_on_start: env::var("SQLW_TEST_WRITER_PANIC").is_ok(),
        };
        if hooks.before_commit.is_some() || hooks.after_commit.is_some() || hooks.panic_on_start {
            eprintln!("sqlw: WARNING SQLW_TEST_* fault-injection hooks are active (debug build)");
        }
        hooks
    }

    #[cfg(not(debug_assertions))]
    fn from_env() -> Hooks {
        Hooks::default()
    }
}

#[cfg(debug_assertions)]
fn env_ms(key: &str) -> Option<Duration> {
    env::var(key)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_millis)
}

/// Sleep helper so release builds contain no hook sleep path at all.
#[cfg(debug_assertions)]
fn hold(d: Option<Duration>) {
    if let Some(d) = d {
        thread::sleep(d);
    }
}

#[cfg(not(debug_assertions))]
fn hold(_: Option<Duration>) {}

#[cfg(debug_assertions)]
fn maybe_panic(hooks: &Hooks) {
    if hooks.panic_on_start {
        panic!("SQLW_TEST_WRITER_PANIC: writer thread failing on purpose");
    }
}

#[cfg(not(debug_assertions))]
fn maybe_panic(_: &Hooks) {}

pub fn run(cfg: Config) -> Result<(), String> {
    let hooks = Hooks::from_env();
    // Claim the endpoint before touching the database: a second instance must
    // lose the ownership race without mutating the file (p.md: "every other
    // instance fails immediately").
    let mut listener = ipc::bind(&cfg.endpoint).map_err(|e| e.to_string())?;
    // On any early return the listener is dropped: no accept loop ever starts.
    let conn = open_db(&cfg)?;
    // The authorizer is the SQL security boundary (not prefix parsing):
    // client statements run under a deny policy, service-owned SQL (schema,
    // transactions, receipts) runs with the policy disarmed via client_mode.
    let client_mode = Arc::new(AtomicBool::new(false));
    install_authorizer(&conn, client_mode.clone())?;

    let (queue, rx) = Queue::new(cfg.queue_capacity, cfg.queue_max_bytes);
    let queue = Arc::new(queue);
    let shared = Arc::new(Shared::default());
    // Read the effective settings back ONCE on the writer connection itself,
    // before it moves into the writer thread, so `ready` can prove them
    // (p.md: "required SQLite settings on the actual writer connection").
    *lock(&shared.settings) = writer_settings(&conn);
    let db_path = cfg.db_path.clone();
    let truncate_at = cfg.wal_truncate_bytes;
    thread::Builder::new()
        .name("sqlw-writer".into())
        .spawn({
            let queue = queue.clone();
            let shared = shared.clone();
            let client_mode = client_mode.clone();
            move || {
                writer_loop(
                    conn,
                    rx,
                    queue,
                    shared,
                    client_mode,
                    hooks,
                    &db_path,
                    truncate_at,
                )
            }
        })
        .map_err(|e| e.to_string())?;

    println!(
        "sqlw ready endpoint={} db={}",
        cfg.endpoint,
        cfg.db_path.display()
    );
    let active = Arc::new(AtomicUsize::new(0));
    loop {
        // An accept failure (pipe churn, EMFILE, ...) must not kill the
        // service: existing connections keep working while we recover.
        let stream = match listener.accept() {
            Ok(s) => s,
            Err(e) => {
                eprintln!("sqlw: accept failed (continuing): {e}");
                thread::sleep(Duration::from_millis(200));
                continue;
            }
        };
        if active.load(Ordering::Relaxed) >= MAX_CONNS {
            eprintln!("sqlw: connection limit {MAX_CONNS} reached; rejecting new client");
            drop(stream);
            continue;
        }
        active.fetch_add(1, Ordering::Relaxed);
        let queue = queue.clone();
        let shared = shared.clone();
        let guard_active = active.clone();
        if let Err(e) = thread::Builder::new()
            .name("sqlw-conn".into())
            .spawn(move || {
                let _guard = ConnGuard(guard_active);
                connection_loop(stream, queue, shared);
            })
        {
            active.fetch_sub(1, Ordering::Relaxed);
            eprintln!("sqlw: connection spawn failed: {e}");
        }
    }
}

fn install_authorizer(conn: &Connection, client_mode: Arc<AtomicBool>) -> Result<(), String> {
    conn.authorizer(Some(move |ctx: AuthContext<'_>| {
        if !client_mode.load(Ordering::Relaxed) {
            return Authorization::Allow;
        }
        client_policy(ctx)
    }))
    .map_err(|e| format!("install authorizer: {e}"))
}

/// Policy for statements that arrive from clients. Denies everything that
/// could control transactions, change pragmas, attach files, or touch the
/// service-owned receipt tables. Runs only while `client_mode` is set.
fn client_policy(ctx: AuthContext<'_>) -> Authorization {
    use AuthAction::*;
    let internal = |t: &str| t.starts_with("_writer_");
    match ctx.action {
        // Fail closed: an action code this SQLite version models but we do
        // not must never fall through to Allow.
        Unknown { .. } => Authorization::Deny,
        Pragma { .. } | Transaction { .. } | Attach { .. } | Detach { .. } => Authorization::Deny,
        // Temp-schema objects are how a client could shadow
        // `_writer_receipts` (unqualified service SQL resolves temp-first),
        // pre-forge receipt rows (`CREATE TEMP TABLE ... AS SELECT`), or plant
        // a trigger that fires while service SQL runs with the authorizer
        // disarmed. Clients have no legitimate use for them.
        CreateTempTable { .. }
        | CreateTempView { .. }
        | CreateTempTrigger { .. }
        | CreateTempIndex { .. }
        | DropTempTable { .. }
        | DropTempView { .. }
        | DropTempTrigger { .. }
        | DropTempIndex { .. } => Authorization::Deny,
        Insert { table_name } | Delete { table_name } if internal(table_name) => {
            Authorization::Deny
        }
        Update { table_name, .. } if internal(table_name) => Authorization::Deny,
        Read { table_name, .. } if internal(table_name) => Authorization::Deny,
        CreateTable { table_name } if internal(table_name) => Authorization::Deny,
        DropTable { table_name } if internal(table_name) => Authorization::Deny,
        AlterTable { table_name, .. } if internal(table_name) => Authorization::Deny,
        CreateIndex { table_name, .. } if internal(table_name) => Authorization::Deny,
        DropIndex { table_name, .. } if internal(table_name) => Authorization::Deny,
        CreateTrigger { table_name, .. } if internal(table_name) => Authorization::Deny,
        DropTrigger { table_name, .. } if internal(table_name) => Authorization::Deny,
        // Name-based variants: keep the `_writer_*` prefix off-limits too.
        CreateView { view_name } if internal(view_name) => Authorization::Deny,
        DropView { view_name } if internal(view_name) => Authorization::Deny,
        CreateVtable { table_name, .. } if internal(table_name) => Authorization::Deny,
        DropVtable { table_name, .. } if internal(table_name) => Authorization::Deny,
        Analyze { table_name } if internal(table_name) => Authorization::Deny,
        // Everything else (DDL on user tables, DML, SELECT, functions) is allowed;
        // result-shape rules are enforced by execute_stmt.
        _ => Authorization::Allow,
    }
}

impl Shared {
    fn ready_json(&self, queue: &Queue) -> Value {
        let now_ms = unix_now_ms();
        let last = self.last_batch_unix_ms.load(Ordering::Relaxed);
        let age = if last == 0 {
            Value::Null
        } else {
            json!(now_ms.saturating_sub(last))
        };
        json!({
            "queue_depth": self.depth.load(Ordering::Relaxed),
            "queue_bytes": queue.bytes(),
            "last_batch_age_ms": age,
            "last_queue_wait_us": self.last_queue_wait_us.load(Ordering::Relaxed),
            "max_queue_wait_us": self.max_queue_wait_us.load(Ordering::Relaxed),
            "last_commit_us": self.last_commit_us.load(Ordering::Relaxed),
            "max_commit_us": self.max_commit_us.load(Ordering::Relaxed),
            "last_commit_unix_ms": self.last_commit_unix_ms.load(Ordering::Relaxed),
            "commit_failures": self.commit_failures.load(Ordering::Relaxed),
            "wal_bytes": self.wal_bytes.load(Ordering::Relaxed),
            "checkpoint": lock(&self.ckpt).clone(),
            "settings": lock(&self.settings).clone(),
        })
    }
}

struct ConnGuard(Arc<AtomicUsize>);

impl Drop for ConnGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

fn connection_loop(mut stream: Box<dyn Conn>, queue: Arc<Queue>, shared: Arc<Shared>) {
    loop {
        let frame = match read_frame(&mut *stream) {
            Ok(Some(f)) => f,
            // Clean EOF, client disconnect, or protocol garbage: drop the connection.
            Ok(None) | Err(_) => return,
        };
        let resp = match serde_json::from_slice::<Request>(&frame) {
            Err(e) => Response::err(0, "invalid", format!("malformed request: {e}")),
            Ok(req) => {
                let id = req.id;
                if let Err(reason) = validate(&req) {
                    Response::err(id, "invalid", reason)
                } else if req.op == Op::Ping {
                    // Liveness: answered without entering the write queue.
                    Response::ok(id, Value::Null)
                } else if req.op == Op::Ready {
                    // Readiness: no writes; reads shared counters only.
                    Response::ok(id, shared.ready_json(&queue))
                } else {
                    let (reply_tx, reply_rx) = mpsc::channel();
                    let item = Item {
                        req,
                        reply: reply_tx,
                        bytes: frame.len(),
                        enqueued_at: Instant::now(),
                    };
                    // Count before send so the writer's dequeue-sub can never
                    // run first and expose a wrapped depth via `ready`.
                    shared.depth.fetch_add(1, Ordering::Relaxed);
                    match queue.send(item) {
                        Err(QueueSend::TooLarge) => {
                            shared.depth.fetch_sub(1, Ordering::Relaxed);
                            Response::err(id, "invalid", "request exceeds the queue byte capacity")
                        }
                        Err(QueueSend::WriterGone) => {
                            shared.depth.fetch_sub(1, Ordering::Relaxed);
                            Response::err(id, "internal", "writer loop is not running")
                        }
                        Ok(()) => {
                            // Caller waits until the transaction containing its request commits.
                            match reply_rx.recv() {
                                Ok(r) => r,
                                Err(_) => Response::err(id, "internal", "writer thread died"),
                            }
                        }
                    }
                }
            }
        };
        let payload = match serde_json::to_vec(&resp) {
            Ok(p) => p,
            Err(_) => return,
        };
        if write_frame(&mut *stream, &payload).is_err() {
            return;
        }
    }
}

/// If the writer thread ever stops, fail the service promptly instead of
/// leaving it apparently healthy without a writer (p.md supervision rule).
struct WriterGuard;

impl Drop for WriterGuard {
    fn drop(&mut self) {
        eprintln!("sqlw: writer thread terminated; exiting");
        std::process::exit(1);
    }
}

// The writer's inputs are flat rather than a ctx struct — private fn, 8 is fine.
#[allow(clippy::too_many_arguments)]
fn writer_loop(
    conn: Connection,
    rx: Receiver<Item>,
    queue: Arc<Queue>,
    shared: Arc<Shared>,
    client_mode: Arc<AtomicBool>,
    hooks: Hooks,
    db_path: &std::path::Path,
    truncate_at: u64,
) {
    let _guard = WriterGuard;
    maybe_panic(&hooks);
    let wal_path = wal_path(db_path);
    let mut last_ckpt_attempt_ms = 0u64;
    while let Ok(first) = rx.recv() {
        queue.release(first.bytes);
        shared.depth.fetch_sub(1, Ordering::Relaxed);
        let queue_wait_us = first.enqueued_at.elapsed().as_micros() as u64;
        // Micro-batch whatever already queued up; no artificial delay for the first caller.
        let mut batch = vec![first];
        while batch.len() < MAX_BATCH {
            match rx.try_recv() {
                Ok(item) => {
                    queue.release(item.bytes);
                    shared.depth.fetch_sub(1, Ordering::Relaxed);
                    batch.push(item);
                }
                Err(_) => break,
            }
        }
        process_batch(&conn, &batch, &hooks, &shared, client_mode.as_ref());
        shared
            .last_queue_wait_us
            .store(queue_wait_us, Ordering::Relaxed);
        shared
            .max_queue_wait_us
            .fetch_max(queue_wait_us, Ordering::Relaxed);
        shared
            .last_batch_unix_ms
            .store(unix_now_ms(), Ordering::Relaxed);
        // Monitor WAL size every batch (cheap stat); truncate adaptively when
        // it clearly outgrew the auto-checkpoint target — never on a fixed
        // blocking schedule (p.md). Busy means: defer and report.
        let wal = file_size(&wal_path);
        shared.wal_bytes.store(wal, Ordering::Relaxed);
        let now = unix_now_ms();
        if wal >= truncate_at && now.saturating_sub(last_ckpt_attempt_ms) >= CKPT_RETRY_SECS * 1000
        {
            last_ckpt_attempt_ms = now;
            attempt_truncate(&conn, &wal_path, &shared);
        }
    }
}

fn attempt_truncate(conn: &Connection, wal_path: &std::path::Path, shared: &Shared) {
    let before = file_size(wal_path);
    let stat = match conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| {
        Ok((
            r.get::<_, i64>(0)?,
            r.get::<_, i64>(1)?,
            r.get::<_, i64>(2)?,
        ))
    }) {
        // Columns: busy, log frames, checkpointed frames.
        Ok((busy, _log, _ckpt)) => {
            let after = file_size(wal_path);
            if busy == 0 && after < before {
                CkptStat {
                    status: "ok",
                    at_unix_ms: unix_now_ms(),
                    wal_before: before,
                    wal_after: after,
                    msg: String::new(),
                }
            } else if busy != 0 {
                CkptStat {
                    status: "busy",
                    at_unix_ms: unix_now_ms(),
                    wal_before: before,
                    wal_after: before,
                    msg: format!("{busy} reader(s) blocked checkpointing; deferred"),
                }
            } else {
                CkptStat {
                    status: "error",
                    at_unix_ms: unix_now_ms(),
                    wal_before: before,
                    wal_after: after,
                    msg: "checkpoint reported success but WAL did not shrink".into(),
                }
            }
        }
        Err(e) => CkptStat {
            status: "error",
            at_unix_ms: unix_now_ms(),
            wal_before: before,
            wal_after: file_size(wal_path),
            msg: e.to_string(),
        },
    };
    if stat.status != "ok" {
        eprintln!(
            "sqlw: WAL truncation not completed ({}): {}",
            stat.status, stat.msg
        );
    }
    *lock(&shared.ckpt) = stat;
}

fn wal_path(db: &std::path::Path) -> PathBuf {
    let mut s = db.as_os_str().to_owned();
    s.push("-wal");
    PathBuf::from(s)
}

fn file_size(p: &std::path::Path) -> u64 {
    std::fs::metadata(p).map(|m| m.len()).unwrap_or(0)
}

fn process_batch(
    conn: &Connection,
    batch: &[Item],
    hooks: &Hooks,
    shared: &Shared,
    client_mode: &AtomicBool,
) {
    if let Err(e) = conn.execute_batch("BEGIN IMMEDIATE") {
        // A leftover transaction (wedge from an earlier failure) would make
        // every later batch fail forever; clear it before replying.
        shared.commit_failures.fetch_add(1, Ordering::Relaxed);
        abort_txn(conn);
        reply_all(batch, map_fatal(&e));
        return;
    }

    let mut out: Vec<(Sender<Response>, Response)> = Vec::with_capacity(batch.len());
    let mut fatal: Option<(&'static str, String)> = None;
    for (i, item) in batch.iter().enumerate() {
        let key = &item.req.key;
        let fp = fingerprint(&item.req.stmts);
        match process_one(conn, i, key, &fp, &item.req.stmts, client_mode) {
            Ok(result) => out.push((item.reply.clone(), Response::ok(item.req.id, result))),
            Err(ItemErr::Fatal(e)) => {
                fatal = Some(map_fatal(&e));
                break;
            }
            Err(ItemErr::FatalMsg(msg)) => {
                fatal = Some(("internal", msg));
                break;
            }
            Err(ItemErr::Conflict(msg)) => {
                out.push((
                    item.reply.clone(),
                    Response::err(item.req.id, "conflict", msg),
                ));
            }
            Err(ItemErr::Busy(msg)) => {
                // Nothing from this item was applied; nothing else in the batch
                // depends on it, so the batch can still commit.
                out.push((item.reply.clone(), Response::err(item.req.id, "busy", msg)));
            }
            Err(ItemErr::Invalid(msg)) => {
                // Only this request's savepoint was rolled back; unrelated
                // items in the batch are unaffected (per-request isolation).
                out.push((
                    item.reply.clone(),
                    Response::err(item.req.id, "invalid", msg),
                ));
            }
        }
    }

    if let Some(resp) = fatal {
        shared.commit_failures.fetch_add(1, Ordering::Relaxed);
        abort_txn(conn);
        // No row of this batch committed; every caller must retry with its key.
        reply_all(batch, resp);
        return;
    }

    hold(hooks.before_commit);
    let commit_start = Instant::now();
    if let Err(e) = conn.execute_batch("COMMIT") {
        shared.commit_failures.fetch_add(1, Ordering::Relaxed);
        abort_txn(conn);
        reply_all(batch, map_fatal(&e));
        return;
    }
    let commit_us = commit_start.elapsed().as_micros() as u64;
    shared.last_commit_us.store(commit_us, Ordering::Relaxed);
    shared.max_commit_us.fetch_max(commit_us, Ordering::Relaxed);
    shared
        .last_commit_unix_ms
        .store(unix_now_ms(), Ordering::Relaxed);
    hold(hooks.after_commit);
    // Success is reported only after SQLite confirmed the commit.
    for (reply, resp) in out {
        let _ = reply.send(resp);
    }
}

/// Roll back the batch transaction. If rollback fails while still inside a
/// transaction, the writer connection is wedged in an unknown state: every
/// later batch would be corrupted, so exit and let a supervisor restart us.
fn abort_txn(conn: &Connection) {
    if conn.is_autocommit() {
        return;
    }
    if let Err(e) = conn.execute_batch("ROLLBACK") {
        if !conn.is_autocommit() {
            eprintln!("sqlw: rollback failed ({e}); writer wedged, exiting");
            std::process::exit(1);
        }
    }
}

enum ItemErr {
    Conflict(String),
    Busy(String),
    Invalid(String),
    Fatal(SqlError),
    FatalMsg(String),
}

fn process_one(
    conn: &Connection,
    i: usize,
    key: &str,
    fp: &str,
    stmts: &[Stmt],
    client_mode: &AtomicBool,
) -> Result<Value, ItemErr> {
    let sp = format!("sqlw_sp_{i}");
    exec_stmt(conn, &format!("SAVEPOINT {sp}")).map_err(ItemErr::Fatal)?;

    // Receipt lookup happens inside our own serialized write transaction, so it
    // cannot race; the PRIMARY KEY on `key` is still the enforcement backstop.
    if let Some((stored_fp, stored_result)) = lookup_receipt(conn, key).map_err(ItemErr::Fatal)? {
        release_savepoint(conn, &sp).map_err(ItemErr::Fatal)?;
        if stored_fp == fp {
            return Ok(serde_json::from_str(&stored_result).unwrap_or(Value::Null));
        }
        return Err(ItemErr::Conflict(
            "idempotency key reused with a different request".into(),
        ));
    }

    let mut changes = Vec::with_capacity(stmts.len());
    let mut rowids = Vec::with_capacity(stmts.len());
    for stmt in stmts {
        // Client SQL runs with the deny-policy authorizer armed; service SQL
        // (savepoints, receipts, transaction control) runs disarmed. No IPC,
        // network, or async work happens while the transaction is open.
        client_mode.store(true, Ordering::SeqCst);
        let step: Result<usize, ItemErr> = match check_single_statement(conn, &stmt.sql) {
            Ok(()) => match execute_stmt(conn, stmt) {
                Ok(n) => Ok(n),
                Err(e) => {
                    let class = classify(&e);
                    Err(match class {
                        Class::Busy => ItemErr::Busy(e.to_string()),
                        Class::Fatal => ItemErr::Fatal(e),
                        Class::Item => ItemErr::Invalid(e.to_string()),
                    })
                }
            },
            Err(msg) => Err(ItemErr::Invalid(msg)),
        };
        client_mode.store(false, Ordering::SeqCst);
        match step {
            Ok(n) => {
                // Defence in depth: if a statement ended our transaction
                // (ON CONFLICT ROLLBACK, ...), later work would commit outside
                // the batch and the savepoint is gone. Abort the whole batch.
                if conn.is_autocommit() {
                    return Err(ItemErr::FatalMsg(
                        "statement ended the write transaction".into(),
                    ));
                }
                changes.push(n);
                rowids.push(conn.last_insert_rowid());
            }
            Err(e) => {
                // Per-request isolation: undo just this request's savepoint
                // (service SQL, authorizer disarmed); the batch continues.
                if let Err(re) = conn.execute_batch(&format!("ROLLBACK TO {sp}; RELEASE {sp}")) {
                    return Err(ItemErr::FatalMsg(format!(
                        "savepoint rollback failed after statement error: {re}"
                    )));
                }
                return Err(e);
            }
        }
    }

    let result = json!({ "changes": changes, "rowids": rowids });
    let result_text = serde_json::to_string(&result).expect("serializable");
    let created_at = unix_now();
    if let Err(e) = insert_receipt(conn, key, fp, &result_text, created_at) {
        let class = classify(&e);
        if let Err(re) = conn.execute_batch(&format!("ROLLBACK TO {sp}; RELEASE {sp}")) {
            return Err(ItemErr::FatalMsg(format!(
                "savepoint rollback failed after receipt error: {re}"
            )));
        }
        if matches!(class, Class::Fatal | Class::Busy) {
            return Err(match class {
                Class::Busy => ItemErr::Busy(e.to_string()),
                _ => ItemErr::Fatal(e),
            });
        }
        // Defensive: the PK caught a key we did not see (impossible while the
        // writer is single-connection, but a constraint must never be ignored).
        if let Some((stored_fp, stored_result)) =
            lookup_receipt(conn, key).map_err(ItemErr::Fatal)?
        {
            // The savepoint was already released above; releasing again would
            // report "no such savepoint" and mask replay/conflict.
            return if stored_fp == fp {
                Ok(serde_json::from_str(&stored_result).unwrap_or(Value::Null))
            } else {
                Err(ItemErr::Conflict(
                    "idempotency key reused with a different request".into(),
                ))
            };
        }
        return Err(ItemErr::Invalid(e.to_string()));
    }
    release_savepoint(conn, &sp).map_err(ItemErr::Fatal)?;
    Ok(result)
}

fn lookup_receipt(conn: &Connection, key: &str) -> Result<Option<(String, String)>, SqlError> {
    match conn.query_row(
        "SELECT request_fingerprint, result FROM _writer_receipts WHERE key = ?1",
        [key],
        |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
    ) {
        Ok(v) => Ok(Some(v)),
        Err(SqlError::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(e),
    }
}

fn insert_receipt(
    conn: &Connection,
    key: &str,
    fp: &str,
    result: &str,
    created_at: u64,
) -> Result<(), SqlError> {
    conn.execute(
        "INSERT INTO _writer_receipts (key, request_fingerprint, result, created_at) VALUES (?1, ?2, ?3, ?4)",
        rusqlite::params![key, fp, result, created_at as i64],
    )?;
    Ok(())
}

fn exec_stmt(conn: &Connection, sql: &str) -> Result<(), SqlError> {
    conn.execute_batch(sql)?;
    Ok(())
}

fn release_savepoint(conn: &Connection, sp: &str) -> Result<(), SqlError> {
    conn.execute_batch(&format!("RELEASE {sp}"))?;
    Ok(())
}

fn execute_stmt(conn: &Connection, stmt: &Stmt) -> Result<usize, SqlError> {
    match &stmt.params {
        Value::Array(items) => {
            let vals: Vec<rusqlite::types::Value> = items.iter().map(to_sql).collect();
            conn.execute(&stmt.sql, rusqlite::params_from_iter(vals.iter()))
        }
        // Named rule (README): object keys may be given bare (`v`) or with the
        // SQLite binding prefix (`:v`, `@v`, `$v`); every statement parameter
        // must be supplied — partial binding would silently become NULL.
        Value::Object(map) => {
            let prep = conn.prepare(&stmt.sql)?;
            let count = prep.parameter_count();
            let mut resolved: Vec<(String, rusqlite::types::Value)> = Vec::with_capacity(map.len());
            for (k, v) in map {
                let candidates = [k.clone(), format!(":{k}"), format!("@{k}"), format!("${k}")];
                let hit = candidates
                    .iter()
                    .find(|c| matches!(prep.parameter_index(c), Ok(Some(_))));
                match hit {
                    Some(name) => {
                        if resolved.iter().any(|(n, _)| n == name) {
                            // {"id":1,":id":2} would otherwise pass the count
                            // check while leaving the real parameter unbound.
                            return Err(SqlError::InvalidParameterName(format!(
                                "parameter {name} bound twice"
                            )));
                        }
                        resolved.push((name.clone(), to_sql(v)))
                    }
                    None => {
                        return Err(SqlError::InvalidParameterName(format!(
                            "unknown parameter name {k}"
                        )))
                    }
                }
            }
            if resolved.len() != count {
                return Err(SqlError::InvalidParameterCount(resolved.len(), count));
            }
            drop(prep);
            let pairs: Vec<(&str, &rusqlite::types::Value)> =
                resolved.iter().map(|(n, v)| (n.as_str(), v)).collect();
            conn.execute(&stmt.sql, pairs.as_slice())
        }
        _ => unreachable!("validated before enqueue"),
    }
}

fn to_sql(v: &Value) -> rusqlite::types::Value {
    use rusqlite::types::Value as R;
    match v {
        Value::Null => R::Null,
        Value::Bool(b) => R::Integer(*b as i64),
        Value::Number(n) => n
            .as_i64()
            .map(R::Integer)
            .or_else(|| n.as_f64().map(R::Real))
            .unwrap_or_else(|| R::Text(n.to_string())),
        Value::String(s) => R::Text(s.clone()),
        _ => unreachable!("validated before enqueue"),
    }
}

enum Class {
    Busy,
    Fatal,
    Item,
}

fn classify(e: &SqlError) -> Class {
    match e {
        SqlError::SqliteFailure(ffi, _) => match ffi.extended_code & 0xff {
            5 | 6 => Class::Busy,
            // NOMEM, READONLY, INTERRUPT, IOERR, CORRUPT, FULL, CANTOPEN, NOTADB:
            // the outcome of the whole batch is uncertain or impossible.
            7 | 8 | 9 | 10 | 11 | 13 | 14 | 26 => Class::Fatal,
            _ => Class::Item,
        },
        // rusqlite-level problems (bad param binding, statement returned rows, ...):
        // statement-local, savepoint rollback is sufficient.
        _ => Class::Item,
    }
}

fn map_fatal(e: &SqlError) -> (&'static str, String) {
    if matches!(classify(e), Class::Busy) {
        ("busy", e.to_string())
    } else {
        ("internal", e.to_string())
    }
}

fn reply_all(batch: &[Item], (code, msg): (&'static str, String)) {
    for item in batch {
        let _ = item
            .reply
            .send(Response::err(item.req.id, code, msg.clone()));
    }
}

/// Reject trailing statements: `sqlite3_prepare_v2` only consumes the first
/// statement, so a client could otherwise smuggle `; COMMIT` etc. past the
/// authorizer (it would never run — but p.md requires explicit rejection).
/// Also surfaces authorizer denials (SQLITE_AUTH) with SQLite's own message.
fn check_single_statement(conn: &Connection, sql: &str) -> Result<(), String> {
    let c = CString::new(sql).map_err(|_| "statement contains a NUL byte".to_string())?;
    let mut stmt: *mut rusqlite::ffi::sqlite3_stmt = std::ptr::null_mut();
    let mut tail: *const c_char = std::ptr::null();
    let rc = unsafe {
        rusqlite::ffi::sqlite3_prepare_v2(conn.handle(), c.as_ptr(), -1, &mut stmt, &mut tail)
    };
    let errmsg = unsafe { CStr::from_ptr(rusqlite::ffi::sqlite3_errmsg(conn.handle())) }
        .to_string_lossy()
        .into_owned();
    if !stmt.is_null() {
        unsafe { rusqlite::ffi::sqlite3_finalize(stmt) };
    }
    if rc != rusqlite::ffi::SQLITE_OK {
        return Err(format!("statement rejected: {errmsg}"));
    }
    let rest = unsafe { CStr::from_ptr(tail) }
        .to_str()
        .map_err(|_| "statement is not valid UTF-8 after the first statement".to_string())?;
    if strip_leading_noise(rest).is_empty() {
        Ok(())
    } else {
        Err("exactly one statement per sql field is required (trailing SQL rejected)".into())
    }
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Effective durability settings as read back on the writer connection.
/// `verify_settings` already enforced them; this records what the writer
/// actually runs with so `ready` can prove it (p.md verification list).
fn writer_settings(conn: &Connection) -> Value {
    let pragma_str = |name: &str| -> String {
        conn.query_row(&format!("PRAGMA {name}"), [], |r| r.get::<_, String>(0))
            .unwrap_or_else(|_| "<error>".into())
    };
    let pragma_int = |name: &str| -> i64 {
        conn.query_row(&format!("PRAGMA {name}"), [], |r| r.get::<_, i64>(0))
            .unwrap_or(-1)
    };
    json!({
        "journal_mode": pragma_str("journal_mode"),
        "synchronous": pragma_int("synchronous"),
        "busy_timeout_ms": pragma_int("busy_timeout"),
        "wal_autocheckpoint": pragma_int("wal_autocheckpoint"),
    })
}

fn unix_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

pub fn open_db(cfg: &Config) -> Result<Connection, String> {
    let conn = Connection::open(&cfg.db_path)
        .map_err(|e| format!("open {}: {e}", cfg.db_path.display()))?;
    conn.busy_timeout(Duration::from_millis(cfg.busy_timeout_ms))
        .map_err(|e| format!("busy_timeout: {e}"))?;
    verify_settings(&conn)?;
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS _writer_receipts (
            key TEXT PRIMARY KEY,
            request_fingerprint TEXT NOT NULL,
            result TEXT NOT NULL,
            created_at INTEGER NOT NULL
        ) WITHOUT ROWID;",
    )
    .map_err(|e| format!("receipt schema: {e}"))?;
    Ok(conn)
}

/// Apply and verify the durability settings the design mandates. Fails with a
/// clear message when the file/platform cannot honour them (e.g. an in-memory
/// or network database that refuses WAL). Called by `run` at startup; the
/// caller owns any connection it is given, so this is safe to test directly.
pub fn verify_settings(conn: &Connection) -> Result<(), String> {
    let mode: String = conn
        .query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0))
        .map_err(|e| format!("journal_mode=WAL: {e}"))?;
    if !mode.eq_ignore_ascii_case("wal") {
        return Err(format!(
            "journal_mode must be WAL, database reports {mode:?} (network filesystem?)"
        ));
    }

    conn.pragma_update(None, "synchronous", "FULL")
        .map_err(|e| format!("synchronous=FULL: {e}"))?;
    let sync: i64 = conn
        .query_row("PRAGMA synchronous", [], |r| r.get(0))
        .map_err(|e| format!("read synchronous: {e}"))?;
    if sync != 2 {
        return Err(format!(
            "synchronous must be FULL (2), database reports {sync}"
        ));
    }

    // Explicit so the bound is visible and testable rather than implicit default.
    conn.pragma_update(None, "wal_autocheckpoint", 1000)
        .map_err(|e| format!("wal_autocheckpoint: {e}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(bytes: usize) -> Item {
        let (reply, _keep) = mpsc::channel();
        Item {
            req: Request::ping(1),
            reply,
            bytes,
            enqueued_at: Instant::now(),
        }
    }

    #[test]
    fn queue_rejects_item_larger_than_byte_cap() {
        let (q, _rx) = Queue::new(10, 100);
        assert!(matches!(q.send(item(101)), Err(QueueSend::TooLarge)));
        assert_eq!(q.bytes(), 0);
    }

    #[test]
    fn queue_byte_gate_blocks_until_release() {
        let (q, rx) = Queue::new(10, 100);
        q.send(item(60)).unwrap();
        assert_eq!(q.bytes(), 60);

        let qh = Arc::new(q);
        let sender = {
            let qh = qh.clone();
            thread::spawn(move || qh.send(item(50)))
        };
        thread::sleep(Duration::from_millis(100));
        assert!(
            !sender.is_finished(),
            "50-byte send must wait (60+50 > 100)"
        );
        assert_eq!(qh.bytes(), 60, "waiting sender must not reserve bytes yet");

        let taken = rx.recv().unwrap();
        qh.release(taken.bytes);
        sender.join().unwrap().expect("send succeeds after release");
        assert_eq!(qh.bytes(), 50);
    }

    #[test]
    fn queue_reports_writer_gone_when_receiver_dropped() {
        let (q, rx) = Queue::new(10, 100);
        drop(rx);
        assert!(matches!(q.send(item(10)), Err(QueueSend::WriterGone)));
        assert_eq!(q.bytes(), 0, "aborted send must release its reservation");
    }
}
