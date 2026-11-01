//! Many connections running transactions at once, retrying on conflicts as
//! clients do. The Tid order isn't visible from here, so these check
//! invariants that hold in every serial order, and that nothing hangs.
//!
//! STRESS_THREADS, STRESS_TXNS (per thread) and STRESS_SEED scale and
//! replay a run. Thread timing still differs between runs of one seed.

mod util;

use std::net::SocketAddr;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use util::{Running, SERIALIZATION_FAILURE, exec, ints, kv};
use wire::{Client, QueryResult};

/// How long a workload may take before it counts as hung.
const DEADLINE: Duration = Duration::from_secs(120);

fn env(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

struct Config {
    threads: usize,
    txns: usize,
    seed: u64,
}

fn config() -> Config {
    let seed = env(
        "STRESS_SEED",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64,
    );
    eprintln!("STRESS_SEED={seed}");
    Config {
        threads: env("STRESS_THREADS", 8) as usize,
        txns: env("STRESS_TXNS", 50) as usize,
        seed,
    }
}

/// Starts a server with `kv` holding keys `0..keys`, each set to `value`.
fn start(keys: i64, value: i64) -> Running {
    let server = Running::start(None);
    let mut c = server.connect();
    exec(
        &mut c,
        "CREATE TABLE kv (k INT PRIMARY KEY, v INT NOT NULL)",
    );
    let rows: Vec<String> = (0..keys).map(|k| format!("({k}, {value})")).collect();
    exec(
        &mut c,
        &format!("INSERT INTO kv VALUES {}", rows.join(", ")),
    );
    server
}

/// Runs `work` on `threads` connections at once and returns what each
/// thread returned. Panics if they don't all finish before [`DEADLINE`].
fn run_workers<T: Send + 'static>(
    addr: SocketAddr,
    config: &Config,
    work: impl Fn(usize, &mut Client, &mut StdRng) -> T + Send + Sync + 'static,
) -> Vec<T> {
    let work = Arc::new(work);
    let handles: Vec<_> = (0..config.threads)
        .map(|id| {
            let work = Arc::clone(&work);
            let seed = config.seed.wrapping_add(id as u64);
            thread::spawn(move || {
                let mut client = Client::connect(addr, "stress").unwrap();
                let mut rng = StdRng::seed_from_u64(seed);
                work(id, &mut client, &mut rng)
            })
        })
        .collect();
    let deadline = Instant::now() + DEADLINE;
    while !handles.iter().all(|h| h.is_finished()) {
        assert!(Instant::now() < deadline, "workers hung");
        thread::sleep(Duration::from_millis(20));
    }
    handles.into_iter().map(|h| h.join().unwrap()).collect()
}

struct Conflict;

/// Runs `sql`, panicking on any error but a conflict.
fn query(c: &mut Client, sql: &str) -> Result<Vec<QueryResult>, Conflict> {
    let response = c.simple_query(sql).unwrap();
    match response.error {
        None => Ok(response.results),
        Some(err) if err.code == SERIALIZATION_FAILURE => Err(Conflict),
        Some(err) => panic!("{sql}: {err}"),
    }
}

fn read(c: &mut Client, k: i64) -> Result<i64, Conflict> {
    let results = query(c, &format!("SELECT v FROM kv WHERE k = {k}"))?;
    Ok(ints(&results)[0][0])
}

fn set(c: &mut Client, k: i64, v: i64) -> Result<(), Conflict> {
    let results = query(c, &format!("UPDATE kv SET v = {v} WHERE k = {k}"))?;
    assert_eq!(results, [QueryResult::Command("UPDATE 1".into())]);
    Ok(())
}

fn expect_tag(c: &mut Client, sql: &str) {
    assert_eq!(exec(c, sql), [QueryResult::Command(sql.into())]);
}

/// Runs `body` in a block until it gets through without a conflict, then
/// commits or rolls back. Returns its result and how many tries
/// conflicted.
fn transact<T>(
    c: &mut Client,
    commit: bool,
    mut body: impl FnMut(&mut Client) -> Result<T, Conflict>,
) -> (T, u32) {
    let mut conflicts = 0;
    loop {
        expect_tag(c, "BEGIN");
        match body(c) {
            Ok(value) => {
                expect_tag(c, if commit { "COMMIT" } else { "ROLLBACK" });
                return (value, conflicts);
            }
            Err(Conflict) => {
                expect_tag(c, "ROLLBACK");
                conflicts += 1;
            }
        }
    }
}

/// Retries a statement outside a block until it gets through. The server
/// already retries it for a while; this covers the conflict timeout.
fn autocommit(c: &mut Client, sql: &str) {
    while query(c, sql).is_err() {}
}

fn check_after_restart(server: Running, expected: &[(i64, i64)]) {
    let server = server.restart();
    assert_eq!(kv(&mut server.connect()), expected, "after a restart");
    server.stop();
}

#[test]
fn increments_of_one_hot_row_are_never_lost() {
    let config = config();
    let server = start(1, 0);
    let txns = config.txns;
    run_workers(server.addr(), &config, move |_, c, rng| {
        for _ in 0..txns {
            if rng.random_bool(0.5) {
                // Read, then write back what was read plus one.
                transact(c, true, |c| {
                    let v = read(c, 0)?;
                    set(c, 0, v + 1)
                });
            } else {
                autocommit(c, "UPDATE kv SET v = v + 1 WHERE k = 0");
            }
        }
    });
    let expected = [(0, (config.threads * config.txns) as i64)];
    assert_eq!(kv(&mut server.connect()), expected);
    check_after_restart(server, &expected);
}

#[test]
fn transfers_keep_balances_exact_and_audits_see_one_total() {
    const ACCOUNTS: i64 = 8;
    const START: i64 = 1000;
    let config = config();
    let server = start(ACCOUNTS, START);
    let txns = config.txns;
    let addr = server.addr();
    // Each thread's committed transfers, as (from, to, amount).
    let committed = run_workers(addr, &config, move |_, c, rng| {
        let mut done = vec![];
        for _ in 0..txns {
            let roll = rng.random_range(0..100);
            if roll < 70 {
                let from = rng.random_range(0..ACCOUNTS);
                let to = (from + rng.random_range(1..ACCOUNTS)) % ACCOUNTS;
                let amount = rng.random_range(1..50);
                let commit = rng.random_bool(0.9);
                transact(c, commit, |c| {
                    let (a, b) = (read(c, from)?, read(c, to)?);
                    set(c, from, a - amount)?;
                    set(c, to, b + amount)
                });
                if commit {
                    done.push((from, to, amount));
                }
            } else if roll < 90 {
                let ((total, sum), _) = transact(c, true, |c| {
                    let mut total = 0;
                    for k in 0..ACCOUNTS {
                        total += read(c, k)?;
                    }
                    let sum = ints(&query(c, "SELECT sum(v) FROM kv")?)[0][0];
                    Ok((total, sum))
                });
                assert_eq!((total, sum), (ACCOUNTS * START, ACCOUNTS * START));
            } else {
                // Hang up partway through a block; the server rolls it back.
                let mut gone = Client::connect(addr, "stress").unwrap();
                expect_tag(&mut gone, "BEGIN");
                let k = rng.random_range(0..ACCOUNTS);
                let _ = query(
                    &mut gone,
                    &format!("UPDATE kv SET v = v + 1000 WHERE k = {k}"),
                );
                drop(gone);
            }
        }
        done
    });
    let mut expected: Vec<(i64, i64)> = (0..ACCOUNTS).map(|k| (k, START)).collect();
    for (from, to, amount) in committed.into_iter().flatten() {
        expected[from as usize].1 -= amount;
        expected[to as usize].1 += amount;
    }
    assert_eq!(kv(&mut server.connect()), expected);
    check_after_restart(server, &expected);
}

#[test]
fn transactions_on_separate_rows_never_conflict() {
    let config = config();
    let server = start(config.threads as i64, 0);
    let txns = config.txns;
    let conflicts = run_workers(server.addr(), &config, move |id, c, rng| {
        let k = id as i64;
        let mut conflicts = 0;
        for _ in 0..txns {
            if rng.random_bool(0.5) {
                conflicts += transact(c, true, |c| {
                    let v = read(c, k)?;
                    set(c, k, v + 1)
                })
                .1;
            } else {
                let results = query(c, &format!("UPDATE kv SET v = v + 1 WHERE k = {k}"));
                conflicts += u32::from(results.is_err());
            }
        }
        conflicts
    });
    assert_eq!(conflicts, vec![0; config.threads], "conflicts per thread");
    let expected: Vec<(i64, i64)> = (0..config.threads as i64)
        .map(|k| (k, config.txns as i64))
        .collect();
    assert_eq!(kv(&mut server.connect()), expected);
    check_after_restart(server, &expected);
}
