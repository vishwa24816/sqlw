use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use rusqlite::Connection;
use serde_json::json;
use sqlw::protocol::{read_frame, write_frame, Request, Response, MAX_FRAME};
use sqlw::{Client, Error, Stmt};

static COUNTER: AtomicU64 = AtomicU64::new(0);

// ---------- helpers ----------

struct Service {
    child: Child,
    endpoint: String,
}

impl Service {
    fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for Service {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn test_dir(tag: &str) -> PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let d = std::env::temp_dir().join(format!("sqlw-{}-{}-{}", tag, std::process::id(), n));
    let _ = fs::remove_dir_all(&d);
    fs::create_dir_all(&d).unwrap();
    d
}

fn endpoint_for(dir: &Path) -> String {
    #[cfg(windows)]
    {
        format!(
            "sqlw-t-{}-{}",
            std::process::id(),
            dir.file_name().unwrap().to_string_lossy()
        )
    }
    #[cfg(unix)]
    {
        dir.join("sock").to_string_lossy().into_owned()
    }
}

fn service_cmd(dir: &Path, extra: &[(&str, &str)]) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_sqlw"));
    cmd.env("SQLW_DB", dir.join("w.db"))
        .env("SQLW_ENDPOINT", endpoint_for(dir))
        .env_remove("SQLW_TEST_HOLD_BEFORE_COMMIT_MS")
        .env_remove("SQLW_TEST_HOLD_AFTER_COMMIT_MS")
        .env_remove("SQLW_TEST_WRITER_PANIC")
        .env_remove("SQLW_QUEUE_MAX_BYTES")
        .env_remove("SQLW_WAL_TRUNCATE_BYTES");
    for (k, v) in extra {
        cmd.env(k, v);
    }
    cmd
}

fn spawn_service(dir: &Path, extra: &[(&str, &str)]) -> Service {
    let endpoint = endpoint_for(dir);
    let mut child = service_cmd(dir, extra).spawn().expect("spawn sqlw");
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut client = Client::new(&endpoint);
    loop {
        if client.ping().is_ok() {
            return Service { child, endpoint };
        }
        if let Some(status) = child.try_wait().expect("try_wait") {
            panic!("service exited during startup: {status}");
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            panic!("service did not become ready in time");
        }
        thread::sleep(Duration::from_millis(50));
    }
}

/// Spawn a service expected to fail at startup; returns (stderr, exit status).
fn spawn_expect_failure(dir: &Path, extra: &[(&str, &str)]) -> (String, std::process::ExitStatus) {
    let mut child = service_cmd(dir, extra)
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn sqlw");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = child.try_wait().expect("try_wait") {
            let mut err = String::new();
            child
                .stderr
                .take()
                .unwrap()
                .read_to_string(&mut err)
                .unwrap();
            return (err, status);
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            panic!("failing service did not exit in time");
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn stmt(sql: &str, params: serde_json::Value) -> Stmt {
    Stmt::new(sql, params)
}

fn db(path: &Path) -> Connection {
    Connection::open(path).expect("open db")
}

fn db_path(dir: &Path) -> PathBuf {
    dir.join("w.db")
}

fn count_where(dir: &Path, table: &str, col: &str, val: &str) -> i64 {
    let conn = db(&db_path(dir));
    conn.query_row(
        &format!("SELECT count(*) FROM {table} WHERE {col} = ?1"),
        [val],
        |r| r.get(0),
    )
    .unwrap()
}

fn setup_table(svc: &Service, table_ddl: &str) {
    let mut c = Client::new(&svc.endpoint);
    c.exec("ddl-1", vec![stmt(table_ddl, json!([]))])
        .expect("ddl");
}

// ---------- tests ----------

/// Multiple OS processes submitting writes concurrently through the service.
#[test]
fn multiprocess_concurrent_writes() {
    let dir = test_dir("multi");
    let svc = spawn_service(&dir, &[]);
    setup_table(&svc, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)");

    let mut children = Vec::new();
    for i in 0..8 {
        let req = serde_json::to_string(&[stmt(
            "INSERT INTO t(v) VALUES(?1)",
            json!([format!("row{i}")]),
        )])
        .unwrap();
        children.push(
            Command::new(env!("CARGO_BIN_EXE_sqlwctl"))
                .env("SQLW_ENDPOINT", &svc.endpoint)
                .args(["exec", &format!("key-{i}"), &req])
                .spawn()
                .unwrap(),
        );
    }
    for mut ch in children {
        assert!(ch.wait().unwrap().success(), "sqlwctl failed");
    }
    assert_eq!(count_where(&dir, "t", "v", "row0"), 1);
    let conn = db(&db_path(&dir));
    let n: i64 = conn
        .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 8);
}

/// Many concurrent retries across processes using the SAME idempotency key:
/// exactly one business row, identical result for every caller.
#[test]
fn concurrent_same_key_processes() {
    let dir = test_dir("samekey");
    let svc = spawn_service(&dir, &[]);
    setup_table(&svc, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)");

    let req =
        serde_json::to_string(&[stmt("INSERT INTO t(v) VALUES(?1)", json!(["once"]))]).unwrap();
    let mut children = Vec::new();
    for _ in 0..8 {
        children.push(
            Command::new(env!("CARGO_BIN_EXE_sqlwctl"))
                .env("SQLW_ENDPOINT", &svc.endpoint)
                .args(["exec", "same-key", &req])
                .stdout(Stdio::piped())
                .spawn()
                .unwrap(),
        );
    }
    let mut outputs = Vec::new();
    for ch in children {
        let out = ch.wait_with_output().unwrap();
        assert!(
            out.status.success(),
            "sqlwctl failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        outputs.push(String::from_utf8(out.stdout).unwrap().trim().to_string());
    }
    assert!(
        outputs.iter().all(|o| o == &outputs[0]),
        "all callers must get the identical stored result"
    );
    assert_eq!(
        count_where(&dir, "t", "v", "once"),
        1,
        "operation must execute exactly once"
    );
}

/// Reusing a key with a different request is a clear conflict.
#[test]
fn key_reuse_different_request_conflicts() {
    let dir = test_dir("conflict");
    let svc = spawn_service(&dir, &[]);
    setup_table(&svc, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)");

    let mut c = Client::new(&svc.endpoint);
    c.exec("k", vec![stmt("INSERT INTO t(v) VALUES(?1)", json!(["a"]))])
        .unwrap();
    let err = c
        .exec("k", vec![stmt("INSERT INTO t(v) VALUES(?1)", json!(["b"]))])
        .unwrap_err();
    assert!(
        matches!(err, Error::Conflict(_)),
        "expected conflict, got {err:?}"
    );
    assert_eq!(count_where(&dir, "t", "v", "a"), 1);
    assert_eq!(count_where(&dir, "t", "v", "b"), 0);
}

/// Transaction control, pragmas, and attach/detach are service-internal:
/// rejected as invalid without ever being queued, and the writer stays healthy.
#[test]
fn statement_control_rejected() {
    let dir = test_dir("ctl");
    let svc = spawn_service(&dir, &[]);
    setup_table(&svc, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)");
    let mut c = Client::new(&svc.endpoint);
    for bad in [
        "COMMIT",
        "ROLLBACK",
        "BEGIN",
        "SAVEPOINT sp",
        "RELEASE sp",
        "PRAGMA query_only=1",
        "PRAGMA journal_mode=DELETE",
        "VACUUM",
        "ATTACH DATABASE ':memory:' AS x",
        "DETACH DATABASE x",
        "EXPLAIN PRAGMA query_only=1",
        "-- sneaky\nCOMMIT",
        "/* c */ ROLLBACK",
    ] {
        match c.exec("k-ctl", vec![stmt(bad, json!([]))]) {
            Err(Error::Invalid(_)) => {}
            other => panic!("{bad}: expected Invalid rejection, got {other:?}"),
        }
    }
    // The writer stayed healthy: nothing from those statements was applied.
    c.exec(
        "k-ctl-ok",
        vec![stmt("INSERT INTO t(v) VALUES(?1)", json!(["ok"]))],
    )
    .unwrap();
    assert_eq!(count_where(&dir, "t", "v", "ok"), 1);
}

/// If a database file cannot honour the mandated durability settings, startup
/// must fail with a clear message instead of silently running degraded.
#[test]
fn settings_reject_incompatible_database() {
    let mem = Connection::open(":memory:").unwrap();
    let err = sqlw::service::verify_settings(&mem).expect_err("in-memory db cannot do WAL");
    assert!(err.contains("WAL"), "error must name WAL, got: {err}");
}

/// Service crash (hard kill) while a transaction is open but before commit:
/// nothing commits; after restart the retry executes exactly once.
#[test]
fn crash_before_commit_rolls_back() {
    let dir = test_dir("crash-before");
    let mut svc = spawn_service(&dir, &[]);
    setup_table(&svc, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)");
    svc.kill();

    let mut svc = spawn_service(&dir, &[("SQLW_TEST_HOLD_BEFORE_COMMIT_MS", "2000")]);
    let endpoint = svc.endpoint.clone();
    let client_thread = thread::spawn(move || {
        let mut c = Client::new(endpoint)
            .with_io_timeout(Duration::from_millis(800))
            .with_max_attempts(1);
        c.exec(
            "k-crash",
            vec![stmt("INSERT INTO t(v) VALUES(?1)", json!(["pre"]))],
        )
    });
    thread::sleep(Duration::from_millis(400)); // request enqueued, writer inside txn, pre-commit
    assert!(
        !client_thread.is_finished(),
        "request must still be in flight at kill time (otherwise the test proves nothing)"
    );
    svc.kill();
    assert!(
        client_thread.join().unwrap().is_err(),
        "caller must not see success"
    );

    // The uncommitted write must be invisible after the crash.
    assert_eq!(
        count_where(&dir, "t", "v", "pre"),
        0,
        "nothing may commit before the kill point"
    );

    let svc2 = spawn_service(&dir, &[]);
    let mut c = Client::new(&svc2.endpoint);
    c.exec(
        "k-crash",
        vec![stmt("INSERT INTO t(v) VALUES(?1)", json!(["pre"]))],
    )
    .unwrap();
    assert_eq!(
        count_where(&dir, "t", "v", "pre"),
        1,
        "retry after restart executes exactly once"
    );
}

/// Service crash after COMMIT but before the reply: the caller sees an error,
/// retries with the same key, and gets the original result without re-executing.
#[test]
fn crash_after_commit_returns_original_result() {
    let dir = test_dir("crash-after");
    let mut svc = spawn_service(&dir, &[]);
    setup_table(&svc, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)");
    svc.kill();

    let mut svc = spawn_service(&dir, &[("SQLW_TEST_HOLD_AFTER_COMMIT_MS", "2000")]);
    let endpoint = svc.endpoint.clone();
    let client_thread = thread::spawn(move || {
        let mut c = Client::new(endpoint)
            .with_io_timeout(Duration::from_millis(800))
            .with_max_attempts(1);
        c.exec(
            "k-post",
            vec![stmt("INSERT INTO t(v) VALUES(?1)", json!(["post"]))],
        )
    });
    thread::sleep(Duration::from_millis(400)); // commit done, reply withheld
    assert!(
        !client_thread.is_finished(),
        "reply must still be withheld at kill time (otherwise the test proves nothing)"
    );
    svc.kill();
    assert!(
        client_thread.join().unwrap().is_err(),
        "reply was lost; caller sees failure"
    );

    // The commit is durable even though the reply never arrived.
    assert_eq!(
        count_where(&dir, "t", "v", "post"),
        1,
        "commit must survive the crash"
    );

    let svc2 = spawn_service(&dir, &[]);
    let mut c = Client::new(&svc2.endpoint);
    c.exec(
        "k-post",
        vec![stmt("INSERT INTO t(v) VALUES(?1)", json!(["post"]))],
    )
    .unwrap();
    // If the receipt were missing the insert would run again and count would be 2.
    assert_eq!(
        count_where(&dir, "t", "v", "post"),
        1,
        "retry must replay the stored result, not the operation"
    );
}

/// Client-side timeout on an uncertain result, then a clean retry with the
/// same key after the service finishes committing.
#[test]
fn client_timeout_then_retry() {
    let dir = test_dir("timeout");
    let mut svc = spawn_service(&dir, &[]);
    setup_table(&svc, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)");
    svc.kill();

    let svc = spawn_service(&dir, &[("SQLW_TEST_HOLD_AFTER_COMMIT_MS", "1500")]);
    let mut c = Client::new(&svc.endpoint)
        .with_io_timeout(Duration::from_millis(300))
        .with_max_attempts(1);
    let err = c
        .exec(
            "k-timeout",
            vec![stmt("INSERT INTO t(v) VALUES(?1)", json!(["t1"]))],
        )
        .unwrap_err();
    assert!(
        matches!(err, Error::Io(_) | Error::Uncertain(_)),
        "expected uncertain outcome, got {err:?}"
    );

    // Service still finishes its commit and answers (to a dead connection).
    thread::sleep(Duration::from_millis(1800));
    assert_eq!(count_where(&dir, "t", "v", "t1"), 1);

    let mut c2 = Client::new(&svc.endpoint);
    c2.exec(
        "k-timeout",
        vec![stmt("INSERT INTO t(v) VALUES(?1)", json!(["t1"]))],
    )
    .unwrap();
    assert_eq!(
        count_where(&dir, "t", "v", "t1"),
        1,
        "retry returns the stored result"
    );
}

/// Queue saturation: with a capacity-1 queue and a slow writer, callers block
/// (backpressure) instead of failing, and health checks stay responsive.
#[test]
fn queue_saturation_backpressure() {
    let dir = test_dir("saturate");
    let svc = spawn_service(
        &dir,
        &[
            ("SQLW_QUEUE_CAP", "1"),
            ("SQLW_TEST_HOLD_BEFORE_COMMIT_MS", "400"),
        ],
    );
    setup_table(&svc, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)");

    let spawn_write = |v: &'static str| {
        let ep = svc.endpoint.clone();
        thread::spawn(move || {
            let mut c = Client::new(ep);
            let t0 = Instant::now();
            let r = c.exec(
                &format!("k-{v}"),
                vec![stmt("INSERT INTO t(v) VALUES(?1)", json!([v]))],
            );
            (r, t0.elapsed())
        })
    };

    let a = spawn_write("a");
    thread::sleep(Duration::from_millis(50));
    let b = spawn_write("b");
    thread::sleep(Duration::from_millis(50));
    let c_write = spawn_write("c");

    // Writer is busy holding a transaction: a ping on its own connection
    // must still be answered without entering the queue.
    thread::sleep(Duration::from_millis(50));
    let ping_client_ep = svc.endpoint.clone();
    let pinger = thread::spawn(move || {
        let mut p = Client::new(ping_client_ep);
        let t0 = Instant::now();
        let r = p.ping();
        (r, t0.elapsed())
    });

    let (pr, ping_elapsed) = pinger.join().unwrap();
    pr.expect("ping must stay responsive while the queue is saturated");
    assert!(
        ping_elapsed < Duration::from_millis(300),
        "ping should not wait behind the queue, took {ping_elapsed:?}"
    );

    a.join().unwrap().0.expect("write a");
    b.join().unwrap().0.expect("write b");
    let (cr, c_elapsed) = c_write.join().unwrap();
    cr.expect("write c");
    // c had to wait for a queue slot plus its own transaction: observable waiting.
    assert!(
        c_elapsed >= Duration::from_millis(250),
        "c must wait under saturation, waited {c_elapsed:?}"
    );

    assert_eq!(count_where(&dir, "t", "v", "a"), 1);
    assert_eq!(count_where(&dir, "t", "v", "b"), 1);
    assert_eq!(count_where(&dir, "t", "v", "c"), 1);
}

/// Remaining SQLite contention: an external writer holds the write lock and
/// the service reports busy (never a false success), then recovers.
#[test]
fn busy_reports_error_then_recovers() {
    let dir = test_dir("busy");
    let svc = spawn_service(&dir, &[("SQLW_BUSY_TIMEOUT_MS", "200")]);
    setup_table(&svc, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)");

    let ext = db(&db_path(&dir));
    ext.execute_batch("BEGIN IMMEDIATE").unwrap(); // hold the write lock

    let mut c = Client::new(&svc.endpoint).with_max_attempts(1);
    let err = c
        .exec(
            "k-busy",
            vec![stmt("INSERT INTO t(v) VALUES(?1)", json!(["busy"]))],
        )
        .unwrap_err();
    assert!(
        matches!(err, Error::Busy(_) | Error::Uncertain(_)),
        "expected busy, got {err:?}"
    );
    assert_eq!(
        count_where(&dir, "t", "v", "busy"),
        0,
        "failed attempt must not write"
    );

    ext.execute_batch("ROLLBACK").unwrap();
    drop(ext);

    let mut c = Client::new(&svc.endpoint);
    c.exec(
        "k-busy",
        vec![stmt("INSERT INTO t(v) VALUES(?1)", json!(["busy"]))],
    )
    .unwrap();
    assert_eq!(count_where(&dir, "t", "v", "busy"), 1);
}

/// Configuration verification: startup fails clearly when the database cannot
/// be opened, and effective WAL/synchronous=FULL settings are confirmed.
#[test]
fn startup_fails_clearly_on_bad_database() {
    let dir = test_dir("bad-db");
    // Point SQLW_DB at a directory: opening it as a database must fail.
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_sqlw"));
    cmd.env("SQLW_DB", &dir)
        .env("SQLW_ENDPOINT", endpoint_for(&dir))
        .env_remove("SQLW_TEST_HOLD_BEFORE_COMMIT_MS")
        .env_remove("SQLW_TEST_HOLD_AFTER_COMMIT_MS");
    let mut child = cmd.stderr(Stdio::piped()).spawn().unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let status = loop {
        if let Some(st) = child.try_wait().unwrap() {
            break st;
        }
        assert!(Instant::now() < deadline, "service should have failed fast");
        thread::sleep(Duration::from_millis(50));
    };
    let mut err = String::new();
    child
        .stderr
        .take()
        .unwrap()
        .read_to_string(&mut err)
        .unwrap();
    assert!(
        !status.success(),
        "service must exit non-zero when the DB cannot be opened"
    );
    assert!(
        err.contains("sqlw:"),
        "must report a clear error, got: {err}"
    );
}

#[test]
fn wal_and_full_synchronous_effective() {
    let dir = test_dir("settings");
    let svc = spawn_service(&dir, &[]);
    let conn = db(&db_path(&dir));
    // journal_mode is file-persistent: proves the service set WAL on the file.
    let mode: String = conn
        .query_row("PRAGMA journal_mode", [], |r| r.get(0))
        .unwrap();
    assert_eq!(mode.to_lowercase(), "wal");
    // verify_settings applies and re-verifies every mandated setting on a
    // caller-owned connection (this is what open_db runs at startup).
    sqlw::service::verify_settings(&conn).expect("settings applicable on the live database");
    drop(conn);
    drop(svc);
}

/// WAL crash recovery plus checkpoint behavior; readers never write.
#[test]
fn wal_recovery_and_checkpoint() {
    let dir = test_dir("wal");
    let mut svc = spawn_service(&dir, &[]);
    setup_table(&svc, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)");
    let mut c = Client::new(&svc.endpoint);
    for i in 0..5 {
        c.exec(
            &format!("k-{i}"),
            vec![stmt(
                "INSERT INTO t(v) VALUES(?1)",
                json!([format!("v{i}")]),
            )],
        )
        .unwrap();
    }
    svc.kill(); // hard kill: WAL left on disk, recovery on next open

    let svc2 = spawn_service(&dir, &[]);
    // Recovery: uncheckpointed committed data must be readable after restart.
    assert_eq!(count_where(&dir, "t", "v", "v0"), 1);
    assert_eq!(count_where(&dir, "t", "v", "v4"), 1);

    // Readers open their own read-only connection while the service runs.
    let ro = Connection::open_with_flags(db_path(&dir), rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
        .unwrap();
    let n: i64 = ro
        .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 5);
    drop(ro);

    // Checkpoint behavior: write after restart so the WAL has pages, then
    // verify checkpoints are not blocked and actually move the data.
    let mut c = Client::new(&svc2.endpoint);
    c.exec(
        "k-after-restart",
        vec![stmt("INSERT INTO t(v) VALUES(?1)", json!(["fresh"]))],
    )
    .unwrap();

    let wal_path = PathBuf::from(format!("{}-wal", db_path(&dir).display()));
    let conn = db(&db_path(&dir));
    // PASSIVE reports real frame counts on this SQLite build.
    let (busy, log, done): (i64, i64, i64) = conn
        .query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
        })
        .unwrap();
    assert_eq!(busy, 0, "checkpoint must not be blocked by the service");
    assert!(
        log >= 1 && done >= 1,
        "frames must exist and be checkpointed, got log={log} done={done}"
    );

    // TRUNCATE shrinks the WAL file to zero. (Its reported counters can be
    // (0,0,0) even with frames present â€” stock SQLite behavior, observed
    // independently of this service.)
    conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| {
        r.get::<_, i64>(0)
    })
    .unwrap();
    drop(conn);
    if wal_path.exists() {
        let size = fs::metadata(&wal_path).unwrap().len();
        assert!(size < 4096, "TRUNCATE should shrink the WAL, size={size}");
    }
    drop(svc2);
}

/// Only one live writer service may own the endpoint at a time.
#[test]
fn second_service_instance_rejected() {
    let dir = test_dir("singleton");
    let svc = spawn_service(&dir, &[]);
    let (err, status) = spawn_expect_failure(&dir, &[]);
    assert!(!status.success(), "second instance must fail to start");
    assert!(
        err.contains("another sqlw service") || err.contains("cannot acquire"),
        "error must identify the ownership conflict, got: {err}"
    );
    // The original service keeps working.
    let mut c = Client::new(&svc.endpoint);
    c.ping().unwrap();
}

// ---------- p.md round-2 requirement coverage ----------

/// The security boundary is the SQLite authorizer plus an explicit
/// trailing-statement check â€” not prefix parsing. Comments, CTEs, NUL bytes,
/// multiple statements, and internal receipt tables must all be covered.
#[test]
fn sql_policy_bypass_attempts() {
    let dir = test_dir("policy");
    let svc = spawn_service(&dir, &[]);
    setup_table(&svc, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)");
    let mut c = Client::new(&svc.endpoint);

    for bad in [
        "INSERT INTO t(v) VALUES('a'); DROP TABLE t",
        "INSERT INTO t(v) VALUES('a'); COMMIT",
        "SELECT 1; SELECT 2",
        "INSERT INTO t(v) VALUES('a'); -- note\nDROP TABLE t",
        "SELECT * FROM _writer_receipts",
        "INSERT INTO _writer_receipts(key,request_fingerprint,result,created_at) VALUES('x','y','z',0)",
        "UPDATE _writer_receipts SET result='pwned'",
        "WITH x AS (SELECT * FROM _writer_receipts) INSERT INTO t(v) SELECT key FROM x",
        "SELECT 'a\u{0}b'",
        "DROP TABLE _writer_receipts",
        "ALTER TABLE _writer_receipts RENAME TO receipts2",
        // Temp-schema shadowing/forgery: temp resolves first for the
        // service's unqualified `_writer_receipts` SQL, so a client-created
        // temp object must be impossible.
        "CREATE TEMP TABLE _writer_receipts AS SELECT 'forged' AS k",
        "CREATE TEMP TABLE shadow(x)",
        "CREATE TEMP VIEW v AS SELECT 1",
        "CREATE TEMP TRIGGER tmp_tr AFTER INSERT ON t BEGIN SELECT 1; END",
        "CREATE TEMP INDEX tmp_idx ON t(v)",
        "DROP TEMP TABLE _writer_receipts",
        "DROP TEMP VIEW v_doesnotexist",
        "DROP TEMP TRIGGER tmp_tr_doesnotexist",
        // Renaming a user table onto an internal name collides with the real
        // receipts table; either the authorizer or SQLite rejects it.
        "ALTER TABLE t RENAME TO _writer_receipts",
    ] {
        match c.exec("k-policy", vec![stmt(bad, json!([]))]) {
            Err(Error::Invalid(_)) => {}
            other => panic!("{bad}: expected Invalid rejection, got {other:?}"),
        }
    }

    // Allowed forms: a comment after the statement, and a user-table CTE write.
    c.exec(
        "k-policy-tc",
        vec![stmt(
            "INSERT INTO t(v) VALUES('tc') -- trailing note",
            json!([]),
        )],
    )
    .unwrap();
    c.exec(
        "k-policy-cte",
        vec![stmt(
            "WITH src AS (SELECT 'cte-ok' AS v) INSERT INTO t(v) SELECT v FROM src",
            json!([]),
        )],
    )
    .unwrap();
    assert_eq!(count_where(&dir, "t", "v", "tc"), 1);
    assert_eq!(count_where(&dir, "t", "v", "cte-ok"), 1);

    // DDL on user tables (including triggers) is legitimate user schema, but a
    // trigger body that touches internal tables must be blocked â€” either at
    // creation time or when the trigger fires. Never silently allowed.
    let evil = "CREATE TRIGGER tr AFTER INSERT ON t BEGIN \
         INSERT INTO _writer_receipts(key,request_fingerprint,result,created_at) \
         VALUES('evil','evil','evil',0); END";
    match c.exec("k-evil-create", vec![stmt(evil, json!([]))]) {
        Err(Error::Invalid(_)) => {}
        Ok(_) => {
            // Body policed at fire time instead: the triggering write fails.
            let fired = c.exec(
                "k-evil-fire",
                vec![stmt("INSERT INTO t(v) VALUES('fired')", json!([]))],
            );
            assert!(
                matches!(fired, Err(Error::Invalid(_))),
                "trigger writing internal tables must be denied, got {fired:?}"
            );
        }
        other => panic!("unexpected result for evil trigger: {other:?}"),
    }
    assert_eq!(
        count_where(&dir, "t", "v", "fired"),
        0,
        "denied trigger must not commit the firing write"
    );

    // Only the two allowed requests plus the DDL wrote receipts â€” every
    // rejected statement left no trace in the internal table.
    let conn = db(&db_path(&dir));
    let receipts: i64 = conn
        .query_row("SELECT count(*) FROM _writer_receipts", [], |r| r.get(0))
        .unwrap();
    // ddl-1 + the two allowed writes, plus the trigger-creation receipt if
    // creation-time policy allowed it. Nothing rejected may ever persist.
    assert!(
        receipts == 3 || receipts == 4,
        "only allowed requests may persist receipts, got {receipts}"
    );
    let evildoers: i64 = conn
        .query_row(
            "SELECT count(*) FROM _writer_receipts \
             WHERE key = 'k-policy' OR (key LIKE 'k-evil%' AND key != 'k-evil-create')",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        evildoers, 0,
        "denied requests must not persist receipts (k-evil-create allowed only if creation succeeded)"
    );
    let rows: i64 = conn
        .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(rows, 2, "denied statements must not touch user data");
}

/// Named-parameter rule from the README example: object keys may be bare
/// (`v`) or prefixed (`:v`/`@v`/`$v`); unknown or partial bindings are
/// deterministic errors, never silent NULLs.
#[test]
fn named_parameters_follow_documented_rule() {
    let dir = test_dir("named");
    let svc = spawn_service(&dir, &[]);
    setup_table(&svc, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)");
    let mut c = Client::new(&svc.endpoint);

    for (i, sql) in [
        "INSERT INTO t(id,v) VALUES(:id,:v)",
        "INSERT INTO t(id,v) VALUES(@id,@v)",
        "INSERT INTO t(id,v) VALUES($id,$v)",
    ]
    .into_iter()
    .enumerate()
    {
        let id = i + 1;
        c.exec(
            &format!("k-named-{id}"),
            vec![stmt(sql, json!({"id": id, "v": format!("n{id}")}))],
        )
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    // Prefixed keys also work.
    c.exec(
        "k-named-prefixed",
        vec![stmt(
            "INSERT INTO t(id,v) VALUES(:id,:v)",
            json!({":id": 4, ":v": "n4"}),
        )],
    )
    .unwrap();

    // Unknown parameter name and partial binding are rejected, not NULL-filled.
    let err = c
        .exec(
            "k-named-bad",
            vec![stmt(
                "INSERT INTO t(id,v) VALUES(:id,:v)",
                json!({"nope": 1}),
            )],
        )
        .unwrap_err();
    assert!(matches!(err, Error::Invalid(_)), "got {err:?}");
    let err = c
        .exec(
            "k-named-partial",
            vec![stmt("INSERT INTO t(id,v) VALUES(:id,:v)", json!({"id": 9}))],
        )
        .unwrap_err();
    assert!(matches!(err, Error::Invalid(_)), "got {err:?}");
    // Same parameter bound twice under two names must not smuggle a NULL
    // past the count check.
    let err = c
        .exec(
            "k-named-dup",
            vec![stmt(
                "INSERT INTO t(id,v) VALUES(:id,:v)",
                json!({"id": 11, ":id": 12}),
            )],
        )
        .unwrap_err();
    assert!(matches!(err, Error::Invalid(_)), "got {err:?}");
    // Integers beyond i64 have no SQLite INTEGER form: rejected up front
    // instead of silently rounding through f64.
    let err = c
        .exec(
            "k-named-i64",
            vec![stmt(
                "INSERT INTO t(id,v) VALUES(?1,?2)",
                json!([18446744073709551615u64, "big"]),
            )],
        )
        .unwrap_err();
    assert!(matches!(err, Error::Invalid(_)), "got {err:?}");

    assert_eq!(count_where(&dir, "t", "v", "n1"), 1);
    assert_eq!(count_where(&dir, "t", "v", "n4"), 1);
    assert_eq!(
        count_where(&dir, "t", "id", "9"),
        0,
        "partial binding must not write"
    );
}

/// Receipts never expire: a replayed key returns the stored result no matter
/// how old the receipt is (created_at is forced to 0 to simulate any age),
/// and no code path prunes or re-executes it.
#[test]
fn receipt_replays_after_long_retention_window() {
    let dir = test_dir("lifetime");
    let svc = spawn_service(&dir, &[]);
    setup_table(&svc, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)");
    let mut c = Client::new(&svc.endpoint);

    let first = c
        .exec(
            "k-life",
            vec![stmt("INSERT INTO t(v) VALUES(?1)", json!(["orig"]))],
        )
        .unwrap();

    // Age the receipt beyond any conceivable retention window.
    let conn = db(&db_path(&dir));
    conn.execute("UPDATE _writer_receipts SET created_at = 0", [])
        .unwrap();
    drop(conn);

    // Unrelated traffic in between.
    c.exec(
        "k-life-other",
        vec![stmt("INSERT INTO t(v) VALUES(?1)", json!(["other"]))],
    )
    .unwrap();

    let replay = c
        .exec(
            "k-life",
            vec![stmt("INSERT INTO t(v) VALUES(?1)", json!(["orig"]))],
        )
        .unwrap();
    assert_eq!(replay, first, "replay must return the original result");
    assert_eq!(
        count_where(&dir, "t", "v", "orig"),
        1,
        "must not re-execute"
    );

    // The aged receipt is still stored untouched: no pruning anywhere.
    let conn = db(&db_path(&dir));
    let (age, n): (i64, i64) = conn
        .query_row(
            "SELECT created_at, count(*) FROM _writer_receipts WHERE key = 'k-life'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(age, 0, "receipt must not be pruned or rewritten");
    assert_eq!(n, 1);
}

/// A single request larger than the queue byte capacity is rejected
/// deterministically (`invalid`); anything within the cap still works.
#[test]
fn queue_byte_limit_rejects_oversized_request() {
    let dir = test_dir("bytes");
    let svc = spawn_service(&dir, &[("SQLW_QUEUE_MAX_BYTES", "16384")]);
    setup_table(&svc, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)");
    let mut c = Client::new(&svc.endpoint);

    let huge = format!("INSERT INTO t(v) VALUES('{}')", "x".repeat(40_000));
    let err = c.exec("k-big", vec![stmt(&huge, json!([]))]).unwrap_err();
    match err {
        Error::Invalid(msg) => assert!(
            msg.contains("byte capacity"),
            "error must name the byte limit, got: {msg}"
        ),
        other => panic!("expected Invalid byte-capacity error, got {other:?}"),
    }

    c.exec(
        "k-small",
        vec![stmt("INSERT INTO t(v) VALUES(?1)", json!(["small"]))],
    )
    .unwrap();
    assert_eq!(count_where(&dir, "t", "v", "small"), 1);
}

/// Oversized length prefixes, malformed JSON bodies, and truncated frames are
/// handled per-connection: the service survives and keeps serving.
#[test]
fn malformed_and_oversized_frames_dont_kill_service() {
    let dir = test_dir("frames");
    let svc = spawn_service(&dir, &[]);
    setup_table(&svc, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)");
    let mut c = Client::new(&svc.endpoint);

    // 1. Declared length over MAX_FRAME: server drops this connection.
    {
        let mut s = sqlw::ipc::connect(&svc.endpoint).unwrap();
        let _ = s.write_all(&((MAX_FRAME as u32 + 1).to_le_bytes()));
        let _ = s.flush();
    }
    c.ping().unwrap();

    // 2. Valid frame, malformed JSON body: answered with `invalid`.
    {
        let mut s = sqlw::ipc::connect(&svc.endpoint).unwrap();
        write_frame(&mut *s, b"{not json").unwrap();
        let raw = read_frame(&mut *s)
            .unwrap()
            .expect("server must answer malformed requests");
        let r: Response = serde_json::from_slice(&raw).unwrap();
        assert!(!r.ok, "malformed request must not succeed");
        assert_eq!(r.code.as_deref(), Some("invalid"));
    }
    c.ping().unwrap();

    // 3. Truncated frame body: server sees EOF mid-frame and drops the conn.
    {
        let mut s = sqlw::ipc::connect(&svc.endpoint).unwrap();
        let _ = s.write_all(&100u32.to_le_bytes());
        let _ = s.write_all(&[1, 2, 3, 4, 5]);
        let _ = s.flush();
    }
    thread::sleep(Duration::from_millis(150));
    c.ping().unwrap();

    // The service still executes writes normally.
    c.exec(
        "k-frame-ok",
        vec![stmt("INSERT INTO t(v) VALUES(?1)", json!(["frames"]))],
    )
    .unwrap();
    assert_eq!(count_where(&dir, "t", "v", "frames"), 1);
}

/// Durability boundary for producers: once the request frame has been read by
/// the service, it commits even if the producer dies before seeing the reply;
/// a later replay with the same key returns the stored result.
#[test]
fn client_disconnect_after_send_still_commits() {
    let dir = test_dir("producer");
    let svc = spawn_service(&dir, &[]);
    setup_table(&svc, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)");

    {
        let mut s = sqlw::ipc::connect(&svc.endpoint).unwrap();
        let req = Request::exec(
            7,
            "k-dc",
            vec![stmt("INSERT INTO t(v) VALUES('dc')", json!([]))],
        );
        write_frame(&mut *s, &serde_json::to_vec(&req).unwrap()).unwrap();
        // Producer dies here, without ever reading the reply.
    }

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if count_where(&dir, "t", "v", "dc") == 1 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "accepted request must commit even though the producer died"
        );
        thread::sleep(Duration::from_millis(100));
    }

    let mut c = Client::new(&svc.endpoint);
    let replay = c
        .exec(
            "k-dc",
            vec![stmt("INSERT INTO t(v) VALUES('dc')", json!([]))],
        )
        .unwrap();
    assert!(replay.is_object(), "replay returns the stored result");
    assert_eq!(count_where(&dir, "t", "v", "dc"), 1, "no re-execution");
}

/// Writer-thread failure must fail the service promptly (p.md supervision):
/// a panicking writer exits the process with a clear message instead of
/// leaving a healthy-looking service without a writer.
#[test]
fn writer_panic_fails_service_promptly() {
    let dir = test_dir("writer-panic");
    let (err, status) = spawn_expect_failure(&dir, &[("SQLW_TEST_WRITER_PANIC", "1")]);
    assert!(!status.success(), "service must exit non-zero");
    assert!(
        err.contains("writer thread terminated") || err.contains("panicked"),
        "must state the writer died, got: {err}"
    );
}

/// Liveness vs readiness: `ready` performs no writes, reports queue/commit/
/// checkpoint state, and reflects a writer that has committed.
#[test]
fn ready_reports_stats_without_writes() {
    let dir = test_dir("ready");
    let svc = spawn_service(&dir, &[]);
    let mut c = Client::new(&svc.endpoint);

    // Fresh service: ready must not create receipts or touch anything.
    let pre = c.ready().expect("ready must answer");
    let obj = pre.as_object().expect("ready returns an object");
    assert_eq!(obj["queue_depth"], 0);
    assert!(obj["last_batch_age_ms"].is_null(), "no batch has run yet");
    assert_eq!(obj["checkpoint"]["status"], "never");
    // Required settings as read back on the actual writer connection.
    let s = &obj["settings"];
    assert_eq!(s["journal_mode"], "wal", "writer journal_mode");
    assert_eq!(s["synchronous"], 2, "writer synchronous=FULL (2)");
    assert_eq!(s["busy_timeout_ms"], 5000, "writer busy_timeout");
    assert_eq!(s["wal_autocheckpoint"], 1000, "writer wal_autocheckpoint");
    assert_eq!(obj["commit_failures"], 0);
    assert_eq!(obj["last_commit_unix_ms"], 0, "no commit yet");
    let conn = db(&db_path(&dir));
    let receipts: i64 = conn
        .query_row("SELECT count(*) FROM _writer_receipts", [], |r| r.get(0))
        .unwrap();
    assert_eq!(receipts, 0, "ready must not write");
    drop(conn);

    setup_table(&svc, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)");
    c.exec(
        "k-ready",
        vec![stmt("INSERT INTO t(v) VALUES('w1')", json!([]))],
    )
    .unwrap();

    let post = c.ready().expect("ready after writes");
    let obj = post.as_object().unwrap();
    assert_eq!(obj["queue_depth"], 0);
    assert!(
        obj["last_commit_us"].as_u64().unwrap_or(0) > 0,
        "readiness must report commit timing after a commit"
    );
    assert!(
        obj["last_commit_unix_ms"].as_u64().unwrap_or(0) > 0,
        "readiness must prove the writer actually committed"
    );
    assert_eq!(obj["commit_failures"], 0);
    assert!(obj["last_batch_age_ms"].is_u64());
    assert!(obj["wal_bytes"].as_u64().is_some());
    assert_eq!(count_where(&dir, "t", "v", "w1"), 1, "ready must not write");
}

/// Deliberately long-lived reader: WAL grows past the truncate threshold,
/// the TRUNCATE checkpoint reports `busy` (deferred, never a false success),
/// and once the reader leaves the checkpoint completes and shrinks the WAL.
#[test]
fn long_lived_reader_defers_checkpoint_until_reader_leaves() {
    let dir = test_dir("reader-wal");
    let svc = spawn_service(
        &dir,
        &[
            ("SQLW_WAL_TRUNCATE_BYTES", "65536"),
            ("SQLW_BUSY_TIMEOUT_MS", "300"),
        ],
    );
    setup_table(&svc, "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)");
    let mut c = Client::new(&svc.endpoint);

    // Pin a read snapshot so checkpoints must defer while it is open.
    let ro = Connection::open_with_flags(db_path(&dir), rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
        .expect("open reader");
    ro.execute_batch("BEGIN").unwrap();
    let _pinned: i64 = ro
        .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
        .unwrap();

    // Grow the WAL well past the 64 KiB threshold.
    let mut n = 0u32;
    loop {
        let batch: Vec<Stmt> = (0..64)
            .map(|_| {
                n += 1;
                stmt(
                    "INSERT INTO t(v) VALUES(?1)",
                    json!([format!("row{n:05}-{}", "x".repeat(480))]),
                )
            })
            .collect();
        c.exec(&format!("k-wal-{n}"), batch).unwrap();
        if n >= 320 {
            break;
        }
    }

    // While the reader is pinned: attempts must report busy, never `ok`.
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let r = c.ready().expect("ready");
        let ckpt = &r["checkpoint"];
        let status = ckpt["status"].as_str().unwrap_or("");
        assert_ne!(
            status, "ok",
            "checkpoint must not claim success while a reader pins the WAL: {ckpt}"
        );
        if status == "busy" {
            assert!(
                r["wal_bytes"].as_u64().unwrap_or(0) >= 65536,
                "busy checkpoint must report the grown WAL, got {}",
                r["wal_bytes"]
            );
            break;
        }
        assert!(
            Instant::now() < deadline,
            "expected a busy checkpoint attempt, last: {ckpt}"
        );
        // Keep producing batches so the writer re-attempts the checkpoint.
        c.exec(
            &format!("k-wal-keep-{n}"),
            vec![stmt("INSERT INTO t(v) VALUES(?1)", json!(["keep"]))],
        )
        .unwrap();
        thread::sleep(Duration::from_millis(300));
    }

    // Reader leaves: the next paced attempt must truncate for real.
    ro.execute_batch("COMMIT").unwrap();
    drop(ro);
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        c.exec(
            &format!("k-wal-post-{n}"),
            vec![stmt("INSERT INTO t(v) VALUES(?1)", json!(["post"]))],
        )
        .unwrap();
        let r = c.ready().expect("ready");
        if r["checkpoint"]["status"] == "ok" && r["wal_bytes"].as_u64().unwrap_or(u64::MAX) < 65536
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "checkpoint must complete after the reader leaves, last: {}",
            r["checkpoint"]
        );
        thread::sleep(Duration::from_millis(300));
    }
}

/// Unix-only: a stale socket left by a killed service is replaced only after
/// it is confirmed dead, and the lock file is the ownership claim.
#[cfg(unix)]
#[test]
fn unix_stale_socket_cleanup_and_lock() {
    let dir = test_dir("unix-stale");
    let endpoint = endpoint_for(&dir);

    // Simulate a crashed service: socket file remains, nothing listens.
    {
        let _l = std::os::unix::net::UnixListener::bind(&endpoint).unwrap();
        // Listener dropped without unlinking: stale socket on disk.
    }
    assert!(
        Path::new(&endpoint).exists(),
        "stale socket must exist first"
    );

    let svc = spawn_service(&dir, &[]);
    let mut c = Client::new(&svc.endpoint);
    c.ping().unwrap();
    assert!(
        Path::new(&format!("{endpoint}.lock")).exists(),
        "lock file must exist while the service runs"
    );
    // Second instance still rejected while the first is alive.
    let (_err, status) = spawn_expect_failure(&dir, &[]);
    assert!(!status.success(), "second instance must fail");
}

/// Unix-only: two services started simultaneously must not unlink each
/// other's live endpoint â€” exactly one wins the flock and serves.
#[cfg(unix)]
#[test]
fn unix_simultaneous_startup_single_winner() {
    let dir = test_dir("unix-race");
    let endpoint = endpoint_for(&dir);
    let mut a = service_cmd(&dir, &[])
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut b = service_cmd(&dir, &[])
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    // Exactly one process must lose the race and exit non-zero.
    let deadline = Instant::now() + Duration::from_secs(10);
    let loser_exited = loop {
        if let Some(st) = a.try_wait().unwrap() {
            break (0, st);
        }
        if let Some(st) = b.try_wait().unwrap() {
            break (1, st);
        }
        assert!(
            Instant::now() < deadline,
            "startup race must resolve quickly"
        );
        thread::sleep(Duration::from_millis(50));
    };
    assert!(!loser_exited.1.success(), "the losing startup must fail");

    // The winner owns the endpoint and serves.
    let mut c = Client::new(&endpoint);
    let mut ok = false;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if c.ping().is_ok() {
            ok = true;
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    assert!(ok, "the winning service must answer pings");
    if a.try_wait().unwrap().is_none() {
        a.kill().ok();
        a.wait().ok();
    }
    if b.try_wait().unwrap().is_none() {
        b.kill().ok();
        b.wait().ok();
    }
}

/// Storage-location validation (p.md): a UNC/network database path is refused
/// at config time with exit code 2, before the endpoint or file is touched.
#[cfg(windows)]
#[test]
fn network_db_path_rejected_at_startup() {
    let dir = test_dir("unc");
    let (err, status) = spawn_expect_failure(&dir, &[("SQLW_DB", r"\\fileserver\share\w.db")]);
    assert_eq!(
        status.code(),
        Some(2),
        "config error must exit 2, stderr={err}"
    );
    assert!(err.contains("local path"), "stderr must explain: {err}");
}

/// Windows pipe access restrictions (p.md): the service pipe carries an
/// explicit DACL with exactly one ACCESS_ALLOWED ACE for the current user,
/// and a remote (SMB-style) open of the same pipe name is refused
/// (PIPE_REJECT_REMOTE_CLIENTS). Remote SMB refusal itself depends on host
/// configuration, but a remote-style path must never yield a client handle.
#[cfg(windows)]
#[test]
fn windows_pipe_dacl_grants_only_current_user_and_rejects_remote() {
    use std::ffi::c_void;

    type Handle = *mut c_void;
    #[repr(C)]
    struct AceHeader {
        ace_type: u8,
        _ace_flags: u8,
        _ace_size: u16,
    }
    #[repr(C)]
    struct Ace {
        _header: AceHeader,
        _mask: u32,
        sid_start: u32,
    }

    #[link(name = "kernel32")]
    extern "system" {
        fn GetCurrentProcess() -> Handle;
        fn CloseHandle(h: Handle) -> i32;
        fn CreateFileW(
            name: *const u16,
            access: u32,
            share: u32,
            sec: *mut c_void,
            disp: u32,
            flags: u32,
            tmpl: Handle,
        ) -> Handle;
        fn GetComputerNameW(buf: *mut u16, size: *mut u32) -> i32;
        fn LocalFree(mem: *mut c_void) -> Handle;
    }
    #[link(name = "advapi32")]
    extern "system" {
        fn GetSecurityInfo(
            h: Handle,
            obj_type: u32,
            info: u32,
            owner: *mut *mut c_void,
            group: *mut *mut c_void,
            dacl: *mut *mut c_void,
            sacl: *mut *mut c_void,
            sd: *mut *mut c_void,
        ) -> u32;
        fn OpenProcessToken(h: Handle, access: u32, token: *mut Handle) -> i32;
        fn GetTokenInformation(
            h: Handle,
            class: u32,
            info: *mut u8,
            len: u32,
            needed: *mut u32,
        ) -> i32;
        fn GetAce(acl: *mut c_void, idx: u32, ace: *mut *mut c_void) -> i32;
        fn EqualSid(a: *mut c_void, b: *mut c_void) -> i32;
    }

    const GENERIC_READ: u32 = 0x8000_0000;
    const OPEN_EXISTING: u32 = 3;
    const DACL_INFORMATION: u32 = 4;
    const TOKEN_QUERY: u32 = 0x0008;
    const TOKEN_USER_CLASS: u32 = 1;
    const ACCESS_ALLOWED_ACE_TYPE: u8 = 0;
    // The ACE is authored as GENERIC_ALL; applying the descriptor maps it to
    // FILE_ALL_ACCESS for the pipe object type.
    const FILE_ALL_ACCESS: u32 = 0x1F01FF;
    const INVALID: isize = -1;

    fn wide(s: &str) -> Vec<u16> {
        s.encode_utf16().chain(std::iter::once(0)).collect()
    }

    let dir = test_dir("dacl");
    let svc = spawn_service(&dir, &[]);

    // 1) The current user can open the pipe (services must stay usable).
    let wpipe = wide(&format!(r"\\.\pipe\{}", svc.endpoint));
    let h = unsafe {
        CreateFileW(
            wpipe.as_ptr(),
            GENERIC_READ,
            0,
            std::ptr::null_mut(),
            OPEN_EXISTING,
            0,
            std::ptr::null_mut(),
        )
    };
    assert!(h as isize != INVALID, "current user must open the pipe");

    // 2) Inspect the pipe's security descriptor: exactly one allow ACE, and
    //    it must be this process's user SID.
    // Named-pipe handles are queried as kernel objects: SE_FILE_OBJECT=1
    // returns ERROR_INVALID_PARAMETER (verified empirically), SE_KERNEL_OBJECT works.
    const SE_KERNEL_OBJECT: u32 = 6;
    let mut owner: *mut c_void = std::ptr::null_mut();
    let mut group: *mut c_void = std::ptr::null_mut();
    let mut dacl: *mut c_void = std::ptr::null_mut();
    let mut sacl: *mut c_void = std::ptr::null_mut();
    let mut sd: *mut c_void = std::ptr::null_mut();
    let err = unsafe {
        GetSecurityInfo(
            h,
            SE_KERNEL_OBJECT,
            DACL_INFORMATION,
            &mut owner,
            &mut group,
            &mut dacl,
            &mut sacl,
            &mut sd,
        )
    };
    assert_eq!(err, 0, "GetSecurityInfo failed with {err}");
    assert!(!dacl.is_null(), "DACL must exist");

    let mut token: Handle = std::ptr::null_mut();
    assert!(
        unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } != 0,
        "OpenProcessToken"
    );
    let mut tu = vec![0u8; 64];
    let mut tu_needed = 0u32;
    assert!(
        unsafe {
            GetTokenInformation(
                token,
                TOKEN_USER_CLASS,
                tu.as_mut_ptr(),
                tu.len() as u32,
                &mut tu_needed,
            )
        } != 0,
        "GetTokenInformation"
    );
    // TOKEN_USER { User: SID_AND_ATTRIBUTES { Sid at offset 0, .. } }.
    let user_sid = unsafe { *(tu.as_ptr() as *const *mut c_void) };
    assert!(!user_sid.is_null());

    let mut ace: *mut c_void = std::ptr::null_mut();
    assert!(
        unsafe { GetAce(dacl, 0, &mut ace) } != 0,
        "DACL must contain an ACE"
    );
    let first = unsafe { &*(ace as *const Ace) };
    assert_eq!(
        first._header.ace_type, ACCESS_ALLOWED_ACE_TYPE,
        "first ACE must be an allow ACE"
    );
    assert_eq!(
        first._mask, FILE_ALL_ACCESS,
        "the grant must be exactly GENERIC_ALL (mapped) for that one SID"
    );
    let ace_sid = std::ptr::addr_of!(first.sid_start) as *mut c_void;
    assert!(
        unsafe { EqualSid(ace_sid, user_sid) } != 0,
        "the only grant must be the current user"
    );
    let mut ace2: *mut c_void = std::ptr::null_mut();
    assert_eq!(
        unsafe { GetAce(dacl, 1, &mut ace2) },
        0,
        "no second ACE: nobody else may be granted access"
    );
    unsafe {
        LocalFree(sd);
        CloseHandle(token);
        CloseHandle(h);
    }

    // 3) A remote (SMB-style) path must never produce a client handle.
    let mut host = [0u16; 256];
    let mut host_len = 256u32;
    assert!(unsafe { GetComputerNameW(host.as_mut_ptr(), &mut host_len) } != 0);
    let host = String::from_utf16_lossy(&host[..host_len as usize]);
    let wremote = wide(&format!(r"\\{host}\pipe\{}", svc.endpoint));
    let hr = unsafe {
        CreateFileW(
            wremote.as_ptr(),
            GENERIC_READ,
            0,
            std::ptr::null_mut(),
            OPEN_EXISTING,
            0,
            std::ptr::null_mut(),
        )
    };
    assert!(
        hr as isize == INVALID,
        "remote-style open must be refused (PIPE_REJECT_REMOTE_CLIENTS)"
    );
}
