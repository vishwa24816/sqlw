# sqlw — single-writer SQLite service with local IPC

`sqlw` owns the one writable SQLite connection for a database. Application
processes on the same host send write requests over local IPC and wait until
the transaction containing their request has **committed** before they get a
success reply. Writes carry idempotency keys, so an ambiguous outcome (crash,
disconnect) can always be retried safely.

```
app proc ──┐                     ┌── writer thread (BEGIN IMMEDIATE … COMMIT)
app proc ──┤  IPC (pipe/socket)  │   count+byte bounded queue → micro-batch → receipts
app proc ──┘  → service process ─┘   WAL + synchronous=FULL
```

Everything else follows from that design: one writer, many readers; success
means *committed*; retries are free because every write is keyed.

## Contents

* [Building and distribution](#building-and-distribution)
* [Quick start](#quick-start)
* [Configuration](#configuration-environment-variables)
* [Binaries and CLI reference](#binaries-and-cli-reference)
* [Durability semantics](#durability-semantics)
* [Idempotency](#idempotency)
* [IPC protocol](#ipc-protocol)
* [Rust client library](#rust-client-library)
* [Recovery and checkpoints](#recovery-and-checkpoints)
* [Backup and restore](#backup-and-restore)
* [Deployment](#deployment)
* [Benchmark](#benchmark)
* [Operational limits and boundaries](#operational-limits-and-boundaries)
* [Verification](#verification)
* [Project layout](#project-layout)

## Building and distribution

**Prerequisites**

* Rust 1.95 or newer (edition 2021). Windows: an MSVC or GNU toolchain
  (`rustup default stable-x86_64-pc-windows-msvc` / `-gnu`).
* No C compiler needed: SQLite is bundled (`rusqlite` `bundled` feature,
  SQLite **3.53.2**) and compiled by `cc` alongside the crate.
* No other dependencies are downloaded beyond `rusqlite`, `serde`,
  `serde_json` — no tokio, no windows-sys, no clap.

**Build**

```sh
cargo build --release          # three binaries in target\release\
cargo test                     # 3 unit + 25 acceptance tests (multi-process)
cargo fmt --check              # formatting gate
cargo clippy --all-targets -- -D warnings   # lint gate
```

**Binaries produced**

| Binary | Role |
|---|---|
| `sqlw.exe` | The service. Holds the single writer connection. |
| `sqlwctl.exe` | Operator/client CLI: `ping`, `ready`, `exec`. |
| `sqlwbench.exe` | Load generator (latency percentiles + service stats). |

**Distributed copies:** `dist\sqlw.exe`, `dist\sqlwctl.exe`,
`dist\sqlwbench.exe` (plain copies of the release build; rebuild and re-copy
after any change). The library crate (`sqlw` as a dependency) is the same
code minus the two CLI `main` functions.

## Quick start

Windows (PowerShell) — JSON arguments need PowerShell's `--%` stop-parsing
token so the embedded quotes reach the CLI intact:

```powershell
$env:SQLW_DB = "C:\data\app.db"; $env:SQLW_ENDPOINT = "sqlw-app"
.\dist\sqlw.exe                     # service (leave running)
.\dist\sqlwctl.exe ping             # → pong
.\dist\sqlwctl.exe ready            # → readiness JSON
.\dist\sqlwctl.exe --% exec my-key "[{\"sql\":\"INSERT INTO t(v) VALUES(?1)\",\"params\":[\"hello\"]}]"
```

(cmd.exe: `sqlwctl.exe exec my-key "[{\"sql\":\"…\"}]"`. sh/zsh: quotes pass
through as written, no escaping needed.)

Unix:

```sh
SQLW_DB=/data/app.db SQLW_ENDPOINT=/run/sqlw.sock ./dist/sqlw &
SQLW_ENDPOINT=/run/sqlw.sock ./dist/sqlwctl ping
SQLW_ENDPOINT=/run/sqlw.sock ./dist/sqlwctl exec my-key \
  '[{"sql":"INSERT INTO t(v) VALUES(?1)","params":["hello"]}]'
```

The service creates the database if missing, applies `journal_mode=WAL` +
`synchronous=FULL`, creates its `_writer_receipts` table, and only then
claims readiness. A second service on the same endpoint exits with an error
before touching the database.

## Configuration (environment variables)

| Variable | Default | Meaning |
|---|---|---|
| `SQLW_DB` | *(required)* | Path to the SQLite database file (local storage only). On Windows, UNC/network paths (`\\server\...`, `//server/...`) and the `\\?\` extended-path form are rejected at startup with exit code 2 — use plain drive paths. |
| `SQLW_ENDPOINT` | *(required)* | Windows: named pipe name (e.g. `sqlw-app`). Unix: socket path (e.g. `/run/sqlw.sock`). |
| `SQLW_QUEUE_CAP` | `1024` | Bounded request queue (request count). Senders block when full (backpressure). |
| `SQLW_QUEUE_MAX_BYTES` | `8388608` (8 MiB) | Queue byte gate. Senders block while the queue holds ≥ this many bytes; a single request larger than the gate is rejected immediately as `invalid`. |
| `SQLW_BUSY_TIMEOUT_MS` | `5000` | SQLite busy timeout on the writer connection. |
| `SQLW_WAL_TRUNCATE_BYTES` | `8388608` (8 MiB) | WAL file size at which the writer attempts a `TRUNCATE` checkpoint (see [Recovery and checkpoints](#recovery-and-checkpoints)). |

Test-only (compiled out of release builds via `debug_assertions`, never set
in production):

| Variable | Meaning |
|---|---|
| `SQLW_TEST_HOLD_BEFORE_COMMIT_MS` | Sleep after the last statement, before `COMMIT`. |
| `SQLW_TEST_HOLD_AFTER_COMMIT_MS` | Sleep after `COMMIT`, before the reply. |
| `SQLW_TEST_WRITER_PANIC` | Kill the writer thread at the first batch (verifies writer-death ⇒ process exit). |

**Exit codes** (service and CLI):

| Code | Meaning |
|---|---|
| `0` | Success. |
| `1` | Runtime failure — endpoint already owned, settings could not be applied, DB unusable at runtime, **writer thread died** (see Operational limits); CLI: request failed or outcome uncertain. |
| `2` | Bad configuration/usage — missing or network `SQLW_DB`/`SQLW_ENDPOINT`, bad CLI arguments or JSON. |

## Binaries and CLI reference

### `sqlw` — the service

```sh
SQLW_DB=... SQLW_ENDPOINT=... sqlw
```

Startup sequence: parse config → claim the endpoint (pipe instance / flock)
→ open the database → set WAL + synchronous=FULL → **verify effective
settings by reading them back** (refuse to run if the filesystem silently
downgraded them) → create/verify `_writer_receipts` → capture the writer
connection's effective settings for `ready` → accept connections. On any
failure it prints `sqlw: <reason>` to stderr and exits non-zero.

The process dies (exit 1) if the writer thread ever terminates — see
[Operational limits](#operational-limits-and-boundaries).

### `sqlwctl` — operator CLI

Requires `SQLW_ENDPOINT` in the environment.

```sh
sqlwctl ping
# → pong

sqlwctl ready
# → {"queue_depth":0,"queue_bytes":0,"last_batch_age_ms":1,"…","settings":{…}}

sqlwctl exec <key> <stmts-json>
# → {"changes":[1],"rowids":[1]}     (first execution)
# → {"changes":[1],"rowids":[1]}     (same key replay — stored result returned,
#                                     the statements did NOT run again)
```

* `<stmts-json>` is a JSON array of statement objects:
  `[{"sql":"…","params":{…}|[…]}]`. `params` may be a named object
  (`{"v":"hello"}`, keys bare or `:prefixed`) or a positional array
  (`["hello"]`, `?1`-style). `result` carries one `changes`/`rowids` entry
  per statement in the request.
* `exec` prints the response `result` value on success; on failure it prints
  `sqlwctl: <code>: <error>` to stderr and exits 1.
* Exit 2 = usage error (missing endpoint/args, malformed JSON, unknown
  command).

### `sqlwbench` — load generator

```sh
SQLW_ENDPOINT=... sqlwbench [requests] [threads]     # defaults: 1000 4
```

Issues keyed `exec` inserts concurrently and prints client-side throughput
plus p50/p95/p99/max latency, then the service's own queue-wait/commit/WAL
stats from `ready` before and after the run. Keys are unique per run, so
re-running never replays old receipts.

## Durability semantics

* Startup **verifies** `journal_mode=WAL` and `synchronous=FULL` by reading the
  effective values back; the service refuses to run if either is not applied
  (e.g. WAL on a network filesystem reports a different mode). Durability
  settings are never lowered for performance — not even for the benchmark.
* Each batch runs inside one `BEGIN IMMEDIATE … COMMIT`. No IPC, network call,
  or async work happens while a transaction is open — the writer thread touches
  sockets only *after* `COMMIT` succeeds.
* A success reply is sent **only after SQLite reports a successful commit**.
  I/O errors, disk-full, failed syncs, and commit errors are reported as
  failures (`internal`/`busy`), never as success.
* In-memory queue: requests waiting in the queue do **not** survive a service
  crash. The guarantee boundary is: *after commit, the operation is durable;
  before commit, callers must retry*. If you need requests to survive a
  **producer** crash before the service ever receives them, the producer needs
  its own durable outbox/redelivery — this service cannot provide that.

## Idempotency

* Every `exec` request carries a `key`, unique per database (stored in
  `_writer_receipts` with `key TEXT PRIMARY KEY` — the constraint, not a
  pre-check, is the enforcement).
* Business statements and the receipt (key + request fingerprint + result) are
  committed in the **same transaction**.
* Retry with the same key **and** same statements → the stored result is
  returned, the statements do not run again.
* Same key with **different** statements → `conflict` error.
* Receipts **never expire**: a key replays its stored result forever. There is
  no retention window to out-run. The trade-off is one small row per unique key
  for the lifetime of the database (`created_at` is kept for diagnostics only).
  Upgrade path: an opt-in retention/prune policy if unbounded growth matters.
* If a committed operation must later trigger an external side effect (HTTP,
  email…), use a **transactional outbox**: write the intent into an outbox table
  in the same transaction and let a separate dispatcher deliver it. A SQLite
  transaction cannot make a network call atomic.

## IPC protocol

* **Transport:** Windows named pipes (`\\.\pipe\<endpoint>`), Unix domain
  sockets (socket mode `0600` via `umask 0177` at bind plus an explicit
  chmod, inside a private `0700` runtime directory). Local only — never
  exposed over a network.
* **Framing:** `u32` little-endian length prefix + JSON body. Max frame
  1 MiB; max 1024 statements per request; idempotency key ≤ 512 bytes; max
  4096 bound parameters per statement; NUL bytes in SQL are rejected.
* **Versioning:** every request/response carries `v: 1`; a mismatch is rejected
  with `invalid`.
* **Correlation:** every request has a client-chosen `id`, echoed in the
  response; clients must drop responses with a mismatched `id`.
* **Wire shapes:**

  ```jsonc
  // request
  {"v":1,"id":7,"op":"exec","key":"order-42",
   "stmts":[{"sql":"INSERT INTO t(v) VALUES(?1)","params":["hello"]}]}
  // success response (result holds one entry per statement; a replay
  // returns the originally stored result)
  {"v":1,"id":7,"ok":true,"result":{"changes":[1],"rowids":[1]}}
  // failure response
  {"v":1,"id":7,"ok":false,"code":"conflict",
   "error":"idempotency key reused with a different request"}
  ```

* **Ops:**
  * `{"v":1,"id":1,"op":"ping"}` — liveness. Answered without entering the
    write queue, performs no writes.
  * `{"v":1,"id":2,"op":"ready"}` — readiness + stats. Never enters the write
    queue. Returns `queue_depth`, `queue_bytes`, `last_batch_age_ms` (writer
    heartbeat), `last_queue_wait_us`/`max_queue_wait_us`,
    `last_commit_us`/`max_commit_us`, `last_commit_unix_ms` (0 until the
    first successful commit), `commit_failures`, `wal_bytes`, `checkpoint`
    (`status: never|ok|busy|error` + message), and `settings` — the effective
    `journal_mode`/`synchronous`/`busy_timeout`/`wal_autocheckpoint` read back
    **on the writer connection itself** at startup.
  * `op:"exec"` — runs `stmts` under `key`.
* **Response codes:** `ok`, `invalid` (deterministic, do not retry),
  `conflict` (deterministic, do not retry), `busy` (retry with same key),
  `internal` (retry with same key; outcome is a safe-to-retry failure).
* **Statement policy — two independent layers:**
  1. *Protocol layer:* transaction control (`BEGIN`/`END`/`COMMIT`/`ROLLBACK`/
     `SAVEPOINT`/`RELEASE`), `PRAGMA`, `VACUUM`, `ATTACH`/`DETACH` are rejected
     as `invalid` before enqueue — a client statement can never end the batch
     transaction, lower durability, or retarget writes. Leading comments and
     `EXPLAIN [QUERY PLAN]` prefixes are recognized before the check, and the
     statement must end after the first statement (no tail).
  2. *SQLite authorizer layer (enforced by SQLite itself):* while a client
     statement executes, the authorizer denies `PRAGMA`/transaction/
     attach/detach actions, **every read, write, or schema change touching
     `_writer_*` tables**, and all temp-schema objects
     (`CREATE/DROP TEMP TABLE/VIEW/TRIGGER/INDEX` — temp schemas resolve
     first for unqualified service SQL, so they could shadow
     `_writer_receipts` or pre-forge receipt rows). Unknown/future action
     codes are denied (fail closed). Service SQL (savepoints, receipts,
     transaction control) runs with the authorizer disarmed. DDL/DML on user
     tables — including `CREATE TRIGGER` on a user table — is allowed: a
     trigger body that touches internal tables is denied **each time it
     fires** (and a trigger declared `ON _writer_*` is denied at creation),
     with the firing write rolled back.
* **Writes only:** `exec` is the write API. A statement that returns rows
  (e.g. `SELECT`) is rejected as `invalid` — result sets are not transported.
  Read with your own read-only connection (`file:...?mode=ro&immutable=0`);
  the service never reads on your behalf.
* **Named parameters** are passed as a JSON object. Keys may be **bare** (`v`)
  or carry the SQLite binding prefix (`:v`, `@v`, `$v`) — whichever form
  matches a real parameter of the statement is used. Unknown names →
  `invalid`; a partial set (missing parameters would default to NULL) →
  `invalid`; the same parameter bound twice under different keys →
  `invalid`. Every parameter must be supplied exactly once. JSON integers
  that serde parses as integers but fall outside SQLite's `i64` range (e.g.
  `2^63`…`2^64-1`) are rejected rather than silently rounded through `f64`;
  literals beyond `u64` arrive as JSON floats and bind as REAL.
* **Rollback scope:** a failed request rolls back **only its own
  per-request savepoint** (created before its statements run); unrelated
  requests in the same micro-batch are unaffected and still commit. The
  whole batch is discarded only for fatal writer errors (including a failed
  `COMMIT`), and every caller in that batch retries with its key.
* **Access restriction:**
  * Windows: the pipe DACL grants the **current user only** (built from the
    process token: `OpenProcessToken` → `GetTokenInformation` → explicit ACL
    with one `ACCESS_ALLOWED` ACE — asserted by a test, including the mapped
    `FILE_ALL_ACCESS` mask), plus `PIPE_REJECT_REMOTE_CLIENTS` so remote
    (SMB) opens are refused outright. No inheritance: the security
    descriptor lives as long as the pipe.
  * Unix: the endpoint lives in a private runtime directory created (or
    tightened to `0700` — startup fails if that is impossible) for the
    service; a `{endpoint}.lock` file is held with `flock(LOCK_EX|LOCK_NB)`
    for the process lifetime (the second instance loses the lock without
    touching the socket; a lock file planted by another user before the
    directory was tightened is unlinked once and re-created — a foreign
    squat cannot DoS us). The socket is bound under `umask 0177`; a stale
    socket file is removed only while holding the lock and only after
    `connect()` proves nothing is listening. On Linux, `accept()` verifies
    the peer uid with `SO_PEERCRED` and drops other users' connections.
* **Single-writer ownership:** Windows `FILE_FLAG_FIRST_PIPE_INSTANCE` (atomic
  cross-process claim); Unix `flock` + `bind(2)` + stale-socket probe. The
  endpoint is claimed **before** the database file is opened, so a losing
  second instance fails without touching the data. A second service instance
  exits with an error — this is not a PID file.

### Client behavior (`sqlwctl` / `sqlw::Client`)

The client API is **blocking** (it waits for the commit before returning).
Do not call it directly on an async runtime worker thread — spawn it on a
blocking thread pool (`spawn_blocking`, `tokio::task::spawn_blocking`, etc.).

| Situation | Behavior |
|---|---|
| Disconnect / EOF before reply | Reconnect and retry with the **same key** (bounded attempts, 50 ms delay). |
| Timeout waiting for reply | Returns `Uncertain`/`Io`; retrying with the same key returns the original result if the commit landed. Connect + write + read all run inside the one bounded wait — there is no unbounded connect phase. |
| Client disconnects mid-request | Service finishes the transaction anyway. No cancellation: the write may commit after the caller gave up; retry returns the stored result. |
| Queue full (count or bytes) | Caller's send **blocks** until there is room — backpressure, no dropping, no false success. A single request above the byte gate is rejected `invalid` immediately. |
| Service shutdown/crash | Clients see connection errors and retry after restart; SQLite recovers the WAL on next open. |
| Deterministic rejections (`invalid`/`conflict`/malformed server reply) | Never retried — the retry loop only repeats outcomes that may have been transient (`busy`, `internal`, I/O). |
| Retries exhausted | Error text states the outcome may be uncertain and that retrying with the same key is safe. |

## Rust client library

```rust
use sqlw::{Client, Stmt};

let mut c = Client::new("sqlw-app");           // endpoint name or socket path
c.exec("order-42", vec![Stmt {
    sql: "INSERT INTO orders(id, total) VALUES(?1, ?2)".into(),
    params: serde_json::json!([42, 99]),
}])?;                                           // blocks until committed

let stats = c.ready()?;                         // serde_json::Value
c.ping()?;
```

Optional tuning: `Client::new(...).with_io_timeout(d).with_max_attempts(n)`.
Errors are `sqlw::Error`: `Invalid`/`Conflict` (deterministic — never
retried), `Busy`/`Uncertain` (retries exhausted — safe to retry with the
same key), `Io`, `Protocol`.

## Recovery and checkpoints

* Kill/crash at any point: uncommitted transactions roll back; committed ones
  survive in the WAL. Restarting the service lets SQLite recover normally;
  clients reconnect and retry with the same idempotency key.
* `wal_autocheckpoint=1000` is set explicitly at startup (PASSIVE: never
  blocks readers or writers, but starves under a constant reader stream). To
  bound WAL growth anyway, the writer runs an adaptive `TRUNCATE` checkpoint:
  it is attempted only when the WAL file has reached
  `SQLW_WAL_TRUNCATE_BYTES` (default 8 MiB) and at most once per second.
  `TRUNCATE` may briefly block readers (bounded by `SQLW_BUSY_TIMEOUT_MS`)
  while it drains; the attempt reports `ok`, `busy`, or `error` honestly in
  `ready` — `status:"ok"` is never claimed unless SQLite reported a successful
  truncate, and a `busy` result is retried on the next paced attempt.
* A long-lived reader can pin the WAL (checkpoint cannot truncate past an open
  read transaction): the checkpoint reports `busy` and is retried while the
  WAL stops growing only by the reader's own snapshot; once the reader leaves,
  the next attempt truncates. `ready` makes this observable
  (`wal_bytes`, `checkpoint`).
* The `TRUNCATE` attempt runs inline on the writer thread, so a forced
  checkpoint can stall incoming writes for up to `SQLW_BUSY_TIMEOUT_MS` while
  it drains (it only starts once the WAL is past the threshold and at most
  once per second). `ponytail:` inline checkpoint — move it to a spare
  connection if write latency during truncation ever matters.

## Backup and restore

* **Safest:** use SQLite's online backup — the `sqlite3` CLI's `.backup`
  command, or any client speaking the online-backup API — against the live
  database (or a read-only copy of it). Both are consistent without stopping
  the service. Note: `VACUUM INTO` cannot be used here — `VACUUM` is
  rejected by the protocol statement policy, and it could not run inside the
  batch transaction anyway.
* **File copy:** stop the service first, then copy the main file **plus its
  `-wal` (and `-shm`) siblings together**. Never copy only the main database
  file while WAL is active — committed data would still be only in the WAL.
* **Restore:** stop the service, replace the main file (and remove stale
  `-wal`/`-shm` only if the snapshot was taken with no WAL sidecars), start
  the service; it verifies settings and recovers normally. Callers retry
  with their idempotency keys.
* Readers open their own read-only connections (`mode=ro`), keep read
  transactions short, and never write.

## Deployment

* **Run one service per database/endpoint**, behind a supervisor that
  restarts it — writer death deliberately exits the process (exit 1), and
  idempotency keys make in-flight retries safe across the restart.
* Linux (systemd) sketch:

  ```ini
  [Service]
  Environment=SQLW_DB=/var/lib/app/app.db
  Environment=SQLW_ENDPOINT=/run/sqlw.sock
  ExecStart=/opt/sqlw/sqlw
  Restart=always
  RuntimeDirectory=sqlw          # private 0700 dir for the socket
  ```

* Windows: run `sqlw.exe` under a service wrapper or task scheduler with the
  same two environment variables; the pipe name is the endpoint. Grant the
  application accounts the ability to connect — the DACL only admits the
  service's own user, so run service and clients as the same account (or
  extend the ACL deliberately).
* Health checks: `ping` for liveness; `ready` for readiness — gate traffic
  on `last_commit_unix_ms > 0 && commit_failures == 0 && last_batch_age_ms`
  small after warm-up.
* Keep the database on local SSD storage; WAL on SMB/NFS is rejected by
  design.

## Benchmark

`sqlwbench` (release build) drives concurrent `exec` requests against a live
service configured exactly as documented above (`synchronous=FULL`, WAL,
default queue) and reports client-side latency percentiles plus the service's
own queue-wait/commit/WAL stats:

```sh
SQLW_DB=/tmp/bench.db SQLW_ENDPOINT=sqlw-bench ./target/release/sqlw &
SQLW_ENDPOINT=sqlw-bench ./target/release/sqlwbench 4000 4
```

Measured on this machine (Windows 10, release profile, bundled SQLite 3.53.2,
local SSD, single writer service, 96-byte insert payload):

| Run | Throughput | p50 | p95 | p99 | max |
|---|---|---|---|---|---|
| 4000 req, 4 threads | 367 req/s | 11.4 ms | 14.1 ms | 22.2 ms | 52.9 ms |
| 2000 req, 1 thread | 277 req/s | 4.7 ms | 8.0 ms | 11.1 ms | 53.7 ms |

Service stats from the same runs: per-commit time `last≈6-7 ms`,
`max≈34 ms`; queue wait collapses to microseconds when not contended
(`max≈35 ms` under 4-thread saturation); the WAL grew to ~4.2 MiB and the
adaptive checkpoint correctly stayed idle (`status: never`) because it was
below the 8 MiB threshold. Single-thread throughput is fsync-bound by
`synchronous=FULL` — that is the durability contract working as specified,
not a tuning target. Numbers are a snapshot of this hardware; re-run
`sqlwbench` for yours.

## Operational limits and boundaries

* **Single host, local storage only.** WAL must not live on a network
  filesystem (SMB/NFS) or be shared across hosts; startup verification fails
  if WAL cannot be applied. This design does **not** fit cross-host clients —
  put a network service in front of `sqlw` instead.
* One writer service per database/endpoint; many reader connections allowed.
* At most 200 concurrent client connections per service (one thread each);
  above that new connections are refused until a client disconnects. On
  Windows the named-pipe instance cap is 255 — the limit keeps headroom for
  the pending accept slot.
* The write queue is in-memory (bounded by count **and** bytes); see the
  durability boundary above. A request larger than `SQLW_QUEUE_MAX_BYTES` is
  rejected `invalid`.
* **Writer death is service death.** If the writer thread ever terminates
  (panic, unrecoverable SQLite error), it prints `sqlw: writer thread
  terminated; exiting` and the whole process exits with code 1 — a supervisor
  (systemd/`Restart=always`/orchestrator) restarts it, and idempotency keys
  make in-flight retries safe. There is no half-alive service that answers
  `ping` while silently failing writes: `ready` also reports
  `last_commit_unix_ms` and `commit_failures`, so a writer that is alive but
  unable to commit is visibly not-ready.
* Connections are not idle-reaped server-side: a client that opens a
  connection and never sends anything can hold one of the 200 slots until it
  disconnects. `ponytail:` client-side timeout only — add an idle read
  timeout if untrusted local processes can connect.
* Receipts are permanent (one row per unique key, forever).
* No server-side read/idle timeout and no protocol-level keepalive — liveness
  comes from `ping` and client-side timeouts.

## Verification

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```

Gates currently pass: 3 unit + 25 acceptance tests (multi-process, real
service binaries).

Covered by `tests/acceptance.rs`:

* concurrent writes from 8 separate processes; same-key retries across
  processes (exactly-once); key reuse with different request → conflict;
  service crash **before** commit (rollback) and **after** commit but before
  the reply (stored result replayed) — both tests assert the request is still
  in flight at kill time, so they cannot pass vacuously; client timeout +
  retry; queue saturation with backpressure and responsive health checks;
  SQLite busy handling; clear startup failure for an unusable database;
  transaction-control/`PRAGMA`/`ATTACH` statements rejected as `invalid` with
  the writer still healthy afterwards; a database that cannot honour the
  mandated settings (in-memory, no WAL) refused by `verify_settings`; effective
  `WAL` file mode + settings verification; WAL recovery after a hard kill,
  read-only readers during service uptime, checkpoint not blocked and WAL
  truncation; second service instance rejected.
* Statement-policy bypass attempts (multi-statement tails, dropped receipts
  table, CTE/comment forms, **temp-schema shadowing/forgery**
  (`CREATE/DROP TEMP TABLE/VIEW/TRIGGER/INDEX` on/against `_writer_*`),
  rename onto an internal name — and a trigger whose body writes
  `_writer_receipts` must be denied, with the firing write rolled back);
  named-parameter rule (bare/prefixed keys, unknown/partial/duplicate →
  `invalid`, integers beyond `i64` → `invalid`); receipt replay long after
  creation (no retention expiry); queue **byte** gate rejection of oversized
  requests; malformed/oversized frames kill only that connection, service
  survives; client disconnect after send still commits and the same key
  replays the stored result; injected writer panic kills the whole service
  promptly (exit code 1); `ready` reports stats (including the writer
  connection's effective settings, `last_commit_unix_ms`, `commit_failures`)
  without entering the write queue; a pinned reader defers WAL truncation
  (`busy`) until it leaves, then truncates; UNC/network `SQLW_DB` rejected at
  startup (exit 2); Windows pipe DACL asserted to be exactly one allow-ACE
  (`FILE_ALL_ACCESS` for the current user only) and a remote-style open
  refused; Unix: stale-socket cleanup under the lock and two simultaneous
  startups where exactly one wins.
* Unit tests: queue byte gate (blocks until released, oversize → `TooLarge`,
  writer death → `WriterGone`).

**Not testable in this environment:**

* Induced mid-`COMMIT` I/O failure (disk-full / EIO injection) — commit
  failures are handled as batch-wide `internal` errors (never success) by
  code path, but not exercised by an automated test here.
* The Unix transport (`cfg(unix)` in `src/ipc.rs`) cannot be compiled or run
  on this Windows-only machine (no linux C toolchain for the bundled SQLite
  build); it is reviewed code, not verified code. The two `cfg(unix)`
  acceptance tests run only on a Unix host.
* macOS peer-uid checks on `accept()` (SO_PEERCRED is Linux-only) — not
  implemented; see the `ponytail:` note in `src/ipc.rs`.

`SQLW_TEST_*` are test-only fault-injection hooks, compiled out of release
builds (`debug_assertions`) and skipped by the test harness for non-fault
tests. The service prints a startup warning when the sleep hooks are set.
Never set them in production.

Two independent review rounds checked this tree against the spec (including
a line-by-line sweep); findings were fixed and re-verified — the final round
confirmed the fixes and reported no remaining gaps.

## Project layout

```
src/
  lib.rs           crate root: Client/Error/Request/Response/Stmt re-exports
  protocol.rs      framing, request/response types, validation, statement
                   policy (deny-list + tail check), parameter checks
  ipc.rs           transports: Windows named pipes (kernel32/advapi32 FFI,
                   DACL construction) + Unix sockets (flock, umask, chmod,
                   stale-socket probe, SO_PEERCRED)
  service.rs       config, queue (count+bytes), writer loop, micro-batching,
                   authorizer policy, savepoints + receipts, adaptive WAL
                   checkpoint, ready/ping stats, writer-settings capture
  client.rs        blocking retrying client (bounded connect+write+read,
                   same-key retries, deterministic errors never retried)
  bin/sqlw.rs      service entry point
  bin/sqlwctl.rs   operator CLI
  bin/sqlwbench.rs load generator
tests/
  acceptance.rs    multi-process end-to-end suite (25 tests)
p.md               the specification this implementation was built against
dist/              release binary copies (sqlw, sqlwctl, sqlwbench)
```

* Dependencies: `rusqlite` 0.40 (bundled SQLite **3.53.2**, `hooks` feature
  for the authorizer), `serde`, `serde_json`. Nothing else — no tokio, no
  windows-sys, no clap.
* Windows-only machine: the unix transport is reviewed-not-compiled
  (disclosed above); rust-analyzer diagnostics were unreliable during
  development — trust `cargo check` / `clippy` / `test`.
