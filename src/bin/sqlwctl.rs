use std::env;
use std::process::exit;

use sqlw::{Client, Stmt};

fn main() {
    let mut args = env::args().skip(1).collect::<Vec<_>>();
    let endpoint = match env::var("SQLW_ENDPOINT") {
        Ok(e) => e,
        Err(_) => {
            eprintln!("usage: SQLW_ENDPOINT=... sqlwctl <ping | ready | exec <key> <stmts-json>>");
            exit(2);
        }
    };
    if args.is_empty() {
        eprintln!("usage: sqlwctl <ping | ready | exec <key> <stmts-json>>");
        exit(2);
    }
    let cmd = args.remove(0);
    let mut client = Client::new(endpoint);

    let result = match cmd.as_str() {
        "ping" => client.ping().map(|_| String::from("pong")),
        "ready" => client.ready().and_then(|v| {
            serde_json::to_string(&v).map_err(|e| sqlw::Error::Protocol(e.to_string()))
        }),
        "exec" => {
            if args.len() != 2 {
                eprintln!("usage: sqlwctl exec <key> <stmts-json>");
                exit(2);
            }
            let stmts: Vec<Stmt> = match serde_json::from_str(&args[1]) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("sqlwctl: bad stmts json: {e}");
                    exit(2);
                }
            };
            client.exec(&args[0], stmts).and_then(|v| {
                serde_json::to_string(&v).map_err(|e| sqlw::Error::Protocol(e.to_string()))
            })
        }
        other => {
            eprintln!("sqlwctl: unknown command {other:?}");
            exit(2);
        }
    };

    match result {
        Ok(out) => println!("{out}"),
        Err(e) => {
            eprintln!("sqlwctl: {e}");
            exit(1);
        }
    }
}
