use std::io::{self, Read, Write};

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const PROTO_VERSION: u32 = 1;
pub const MAX_FRAME: usize = 1024 * 1024;
pub const MAX_STMTS: usize = 1024;
pub const MAX_KEY_LEN: usize = 512;
pub const MAX_BINDS: usize = 4096;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Stmt {
    pub sql: String,
    #[serde(default = "default_params")]
    pub params: Value,
}

impl Stmt {
    pub fn new(sql: impl Into<String>, params: Value) -> Self {
        Stmt {
            sql: sql.into(),
            params,
        }
    }
}

fn default_params() -> Value {
    Value::Array(Vec::new())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Op {
    Ping,
    Ready,
    Exec,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Request {
    pub v: u32,
    pub id: u64,
    pub op: Op,
    #[serde(default)]
    pub key: String,
    #[serde(default)]
    pub stmts: Vec<Stmt>,
}

impl Request {
    pub fn ping(id: u64) -> Self {
        Request {
            v: PROTO_VERSION,
            id,
            op: Op::Ping,
            key: String::new(),
            stmts: Vec::new(),
        }
    }
    pub fn ready(id: u64) -> Self {
        Request {
            v: PROTO_VERSION,
            id,
            op: Op::Ready,
            key: String::new(),
            stmts: Vec::new(),
        }
    }
    pub fn exec(id: u64, key: impl Into<String>, stmts: Vec<Stmt>) -> Self {
        Request {
            v: PROTO_VERSION,
            id,
            op: Op::Exec,
            key: key.into(),
            stmts,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Response {
    pub v: u32,
    pub id: u64,
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl Response {
    pub fn ok(id: u64, result: Value) -> Self {
        Response {
            v: PROTO_VERSION,
            id,
            ok: true,
            result: Some(result),
            code: None,
            error: None,
        }
    }
    pub fn err(id: u64, code: &str, error: impl Into<String>) -> Self {
        Response {
            v: PROTO_VERSION,
            id,
            ok: false,
            result: None,
            code: Some(code.into()),
            error: Some(error.into()),
        }
    }
}

pub fn validate(req: &Request) -> Result<(), String> {
    if req.v != PROTO_VERSION {
        return Err(format!(
            "unsupported protocol version {} (expected {})",
            req.v, PROTO_VERSION
        ));
    }
    match req.op {
        Op::Ping | Op::Ready => Ok(()),
        Op::Exec => {
            if req.key.is_empty() {
                return Err("exec requires a non-empty idempotency key".into());
            }
            if req.key.len() > MAX_KEY_LEN {
                return Err(format!("idempotency key longer than {MAX_KEY_LEN} bytes"));
            }
            if req.stmts.is_empty() {
                return Err("exec requires at least one statement".into());
            }
            if req.stmts.len() > MAX_STMTS {
                return Err(format!("more than {MAX_STMTS} statements"));
            }
            for s in &req.stmts {
                if s.sql.trim().is_empty() {
                    return Err("statement sql must be non-empty".into());
                }
                if s.sql.contains('\0') {
                    return Err("statement sql must not contain NUL bytes".into());
                }
                let kw = first_keyword(&s.sql);
                // Transaction control, connection pragmas, and file-attachment
                // are service-internal: a client statement must never be able
                // to end the batch transaction, lower durability, disable the
                // busy handler/checkpoints, or retarget writes.
                const DENIED: &[&str] = &[
                    "BEGIN",
                    "END",
                    "COMMIT",
                    "ROLLBACK",
                    "SAVEPOINT",
                    "RELEASE",
                    "PRAGMA",
                    "VACUUM",
                    "ATTACH",
                    "DETACH",
                ];
                if DENIED.contains(&kw.as_str()) {
                    return Err(format!("statement control keyword {kw} is not allowed"));
                }
                match &s.params {
                    Value::Array(items) => {
                        if items.len() > MAX_BINDS {
                            return Err(format!("more than {MAX_BINDS} bound parameters"));
                        }
                        for v in items {
                            scalar(v)?;
                        }
                    }
                    Value::Object(map) => {
                        if map.len() > MAX_BINDS {
                            return Err(format!("more than {MAX_BINDS} bound parameters"));
                        }
                        for v in map.values() {
                            scalar(v)?;
                        }
                    }
                    _ => {
                        return Err(
                            "params must be a JSON array (positional) or object (named)".into()
                        )
                    }
                }
            }
            Ok(())
        }
    }
}

fn scalar(v: &Value) -> Result<(), String> {
    match v {
        Value::Null | Value::Bool(_) | Value::String(_) => Ok(()),
        Value::Number(n) => {
            // A JSON integer above i64::MAX but within u64 has no SQLite
            // INTEGER representation and would silently round through f64 —
            // reject deterministically. (Literals beyond u64 demote to f64
            // before we ever see them and bind as REAL; floats stay REAL.)
            if n.as_i64().is_none() && n.as_u64().is_some() {
                return Err("integer parameter out of SQLite i64 range".into());
            }
            Ok(())
        }
        _ => Err("params values must be scalars (null/bool/number/string)".into()),
    }
}

/// First SQL keyword, skipping whitespace/comments and an `EXPLAIN [QUERY PLAN]` prefix.
fn first_keyword(sql: &str) -> String {
    let mut s = strip_leading_noise(sql);
    loop {
        let (word, rest) = take_word(s);
        let up = word.to_uppercase();
        if up == "EXPLAIN" {
            s = strip_leading_noise(rest);
            continue;
        }
        if up == "QUERY" {
            let (word2, rest2) = take_word(rest);
            if word2.eq_ignore_ascii_case("PLAN") {
                s = strip_leading_noise(rest2);
                continue;
            }
        }
        return up;
    }
}

/// Strip leading whitespace and `--`/`/*…*/` comments.
pub(crate) fn strip_leading_noise(s: &str) -> &str {
    let mut s = s.trim_start();
    loop {
        if let Some(rest) = s.strip_prefix("--") {
            s = rest.split('\n').nth(1).unwrap_or("").trim_start();
            continue;
        }
        if let Some(rest) = s.strip_prefix("/*") {
            s = match rest.find("*/") {
                Some(i) => &rest[i + 2..],
                None => "",
            }
            .trim_start();
            continue;
        }
        return s;
    }
}

/// One identifier/keyword token (letter/digit/underscore), then the remainder trimmed.
fn take_word(s: &str) -> (&str, &str) {
    let end = s
        .char_indices()
        .take_while(|(_, c)| c.is_alphanumeric() || *c == '_')
        .map(|(i, c)| i + c.len_utf8())
        .last()
        .unwrap_or(0);
    (&s[..end], s[end..].trim_start())
}

/// Deterministic request identity stored in the receipt row.
/// Exact canonical JSON instead of a hash: no collisions, no extra dependency.
pub fn fingerprint(stmts: &[Stmt]) -> String {
    serde_json::to_string(stmts).expect("Stmt always serializes")
}

pub fn write_frame(w: &mut dyn Write, bytes: &[u8]) -> io::Result<()> {
    if bytes.len() > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "frame too large",
        ));
    }
    let len = (bytes.len() as u32).to_le_bytes();
    w.write_all(&len)?;
    w.write_all(bytes)?;
    w.flush()
}

/// Returns `Ok(None)` only on a clean EOF at a frame boundary.
/// A truncated frame is an error.
pub fn read_frame(r: &mut dyn Read) -> io::Result<Option<Vec<u8>>> {
    let mut len_buf = [0u8; 4];
    match read_exact_opt(r, &mut len_buf)? {
        None => return Ok(None),
        Some(n) if n < 4 => {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "truncated frame header",
            ))
        }
        _ => {}
    }
    let len = u32::from_le_bytes(len_buf) as usize;
    if len > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("frame of {len} bytes exceeds limit {MAX_FRAME}"),
        ));
    }
    let mut body = vec![0u8; len];
    match read_exact_opt(r, &mut body)? {
        Some(n) if n == len => Ok(Some(body)),
        _ => Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "truncated frame body",
        )),
    }
}

/// Like `read_exact`, but reports how many bytes were read on a clean EOF.
fn read_exact_opt(r: &mut dyn Read, buf: &mut [u8]) -> io::Result<Option<usize>> {
    let mut n = 0;
    while n < buf.len() {
        match r.read(&mut buf[n..]) {
            Ok(0) => return Ok(if n == 0 { None } else { Some(n) }),
            Ok(k) => n += k,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(Some(n))
}
