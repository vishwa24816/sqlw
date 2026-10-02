//! Minimal load generator: measures throughput, client-observed latency
//! percentiles, and reports the service's own queue-wait/commit/WAL stats.
//!
//! Usage: SQLW_ENDPOINT=... sqlwbench [requests] [threads]
//!
//! Runs against a live service with its normal configuration
//! (`synchronous=FULL`, WAL) — durability is never lowered for the numbers.

use std::env;
use std::time::Instant;

use serde_json::json;
use sqlw::{Client, Stmt};

fn pct(sorted: &[u128], p: f64) -> u128 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx]
}

fn main() {
    let requests: usize = env::args()
        .nth(1)
        .and_then(|a| a.parse().ok())
        .unwrap_or(2000);
    let threads: usize = env::args().nth(2).and_then(|a| a.parse().ok()).unwrap_or(4);
    let endpoint = env::var("SQLW_ENDPOINT").expect("SQLW_ENDPOINT must be set");

    let mut setup = Client::new(&endpoint);
    let before = setup.ready().expect("ready");
    setup
        .exec(
            "sqlwbench-ddl",
            vec![Stmt::new(
                "CREATE TABLE IF NOT EXISTS bench(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)",
                json!([]),
            )],
        )
        .expect("ddl");

    let payload = "x".repeat(96);
    let per_thread = requests / threads + usize::from(!requests.is_multiple_of(threads));
    let wall = Instant::now();
    let mut handles = Vec::with_capacity(threads);
    for t in 0..threads {
        let ep = endpoint.clone();
        let payload = payload.clone();
        let count = if t == threads - 1 {
            requests - per_thread * (threads - 1)
        } else {
            per_thread
        };
        handles.push(std::thread::spawn(move || {
            let mut c = Client::new(&ep);
            let mut lat = Vec::with_capacity(count);
            for i in 0..count {
                let t0 = Instant::now();
                c.exec(
                    &format!("bench-{t}-{i}"),
                    vec![Stmt::new(
                        "INSERT INTO bench(v) VALUES(?1)",
                        json!([payload]),
                    )],
                )
                .expect("exec");
                lat.push(t0.elapsed().as_micros());
            }
            lat
        }));
    }
    let mut all: Vec<u128> = Vec::with_capacity(requests);
    for h in handles {
        all.extend(h.join().expect("thread"));
    }
    let wall = wall.elapsed();
    all.sort_unstable();

    let mut probe = Client::new(&endpoint);
    let after = probe.ready().expect("ready");

    println!(
        "requests={} threads={} wall_ms={:.0} throughput_rps={:.0}",
        requests,
        threads,
        wall.as_millis(),
        requests as f64 / wall.as_secs_f64()
    );
    println!(
        "client_latency_us p50={} p95={} p99={} max={}",
        pct(&all, 0.50),
        pct(&all, 0.95),
        pct(&all, 0.99),
        all.last().copied().unwrap_or(0)
    );
    println!(
        "service queue_wait_us last={} max={} | commit_us last={} max={}",
        after["last_queue_wait_us"],
        after["max_queue_wait_us"],
        after["last_commit_us"],
        after["max_commit_us"]
    );
    println!(
        "wal_bytes before={} after={} checkpoint_status={:?} checkpoint_msg={:?}",
        before["wal_bytes"],
        after["wal_bytes"],
        after["checkpoint"]["status"].as_str(),
        after["checkpoint"]["msg"].as_str()
    );
}
