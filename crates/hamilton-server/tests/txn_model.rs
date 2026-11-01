//! Generated schedules of transactions over several connections, checked
//! against MVTO's promise: the result equals running the committed
//! transactions one after another in Tid order.
//!
//! One thread drives every connection, a statement at a time, so the order
//! the BEGINs (or single statements) reach the server is their Tid order.
//! The conflict timeout is off: a statement outside a block that waited for
//! another connection's block would wait on this thread forever.
//!
//! Only point reads, sums and updates of existing rows: inserts and deletes
//! would hit phantoms, a known gap (docs/internals/mvto.md).

mod util;

use std::collections::BTreeSet;
use std::fmt::Write;
use std::time::Duration;

use proptest::prelude::*;
use util::{Running, SERIALIZATION_FAILURE, exec, ints, kv};
use wire::{Client, QueryResult};

#[derive(Debug, Clone, Copy)]
enum Op {
    Read(i64),
    Sum,
    Set(i64, i64),
    Add(i64, i64),
}

impl Op {
    fn sql(self) -> String {
        match self {
            Op::Read(k) => format!("SELECT v FROM kv WHERE k = {k}"),
            Op::Sum => "SELECT sum(v) FROM kv".into(),
            Op::Set(k, c) => format!("UPDATE kv SET v = {c} WHERE k = {k}"),
            Op::Add(k, d) => format!("UPDATE kv SET v = v + {d} WHERE k = {k}"),
        }
    }

    /// What it reads and what it writes.
    fn footprint(self, keys: i64) -> (Vec<i64>, Vec<i64>) {
        match self {
            Op::Read(k) => (vec![k], vec![]),
            Op::Sum => ((0..keys).collect(), vec![]),
            Op::Set(k, _) | Op::Add(k, _) => (vec![], vec![k]),
        }
    }
}

#[derive(Debug, Clone)]
struct Txn {
    ops: Vec<Op>,
    /// A block of its own, rather than a single statement outside one.
    block: bool,
    commit: bool,
}

#[derive(Debug, Clone)]
struct Case {
    keys: i64,
    clients: Vec<Vec<Txn>>,
    /// Which client sends the next statement, modulo the number of clients.
    /// The rest are drained in turn after it runs out.
    schedule: Vec<usize>,
}

fn op(keys: i64) -> impl Strategy<Value = Op> {
    let k = 0..keys;
    prop_oneof![
        3 => k.clone().prop_map(Op::Read),
        1 => Just(Op::Sum),
        2 => (k.clone(), -100i64..100).prop_map(|(k, c)| Op::Set(k, c)),
        3 => (k, -10i64..10).prop_map(|(k, d)| Op::Add(k, d)),
    ]
}

fn txn(keys: i64) -> impl Strategy<Value = Txn> {
    prop_oneof![
        1 => op(keys).prop_map(|op| Txn { ops: vec![op], block: false, commit: true }),
        4 => (prop::collection::vec(op(keys), 1..=4), prop::bool::weighted(0.8))
            .prop_map(|(ops, commit)| Txn { ops, block: true, commit }),
    ]
}

fn case() -> impl Strategy<Value = Case> {
    (1i64..=4).prop_flat_map(|keys| {
        (
            prop::collection::vec(prop::collection::vec(txn(keys), 1..=4), 2..=4),
            prop::collection::vec(0usize..4, 0..80),
        )
            .prop_map(move |(clients, schedule)| Case {
                keys,
                clients,
                schedule,
            })
    })
}

/// What one transaction did, in the order the transactions began.
#[derive(Debug)]
struct Record {
    client: usize,
    ops: Vec<Op>,
    /// The ops that succeeded, with the value each read returned.
    done: Vec<(Op, Option<i64>)>,
    /// The op that failed with a conflict, if one did, and its statement
    /// number.
    conflicted: Option<(Op, usize)>,
    committed: bool,
    /// Statement numbers across all connections.
    began: usize,
    ended: Option<usize>,
}

enum Phase {
    Begin,
    Op(usize),
    End,
}

struct Cursor {
    txn: usize,
    phase: Phase,
    record: usize,
}

struct Driver<'a> {
    case: &'a Case,
    conns: Vec<Client>,
    cursors: Vec<Cursor>,
    records: Vec<Record>,
    step: usize,
    log: String,
}

impl Driver<'_> {
    fn done(&self, c: usize) -> bool {
        self.cursors[c].txn == self.case.clients[c].len()
    }

    fn send(&mut self, c: usize, sql: &str) -> Result<Result<Vec<QueryResult>, String>, String> {
        self.step += 1;
        let response = self.conns[c]
            .simple_query(sql)
            .map_err(|e| format!("client {c}: {sql}: {e}\n{}", self.log))?;
        let shown = match &response.error {
            Some(err) => format!("error {}: {}", err.code, err.message),
            None => match response.results.as_slice() {
                [QueryResult::Command(tag)] => tag.clone(),
                [QueryResult::Rows { rows, .. }] => format!("{rows:?}"),
                results => format!("{results:?}"),
            },
        };
        let _ = writeln!(self.log, "{:>3} [{c}] {sql:<45} => {shown}", self.step);
        Ok(match response.error {
            None => Ok(response.results),
            Some(err) => Err(err.code),
        })
    }

    /// Sends client `c`'s next statement.
    fn advance(&mut self, c: usize) -> Result<(), String> {
        let case = self.case;
        let txn = &case.clients[c][self.cursors[c].txn];
        match self.cursors[c].phase {
            Phase::Begin => {
                self.cursors[c].record = self.records.len();
                self.records.push(Record {
                    client: c,
                    ops: txn.ops.clone(),
                    done: vec![],
                    conflicted: None,
                    committed: false,
                    began: self.step + 1,
                    ended: None,
                });
                if !txn.block {
                    let op = txn.ops[0];
                    let committed = self.run_op(c, op)?;
                    let record = self.records.last_mut().unwrap();
                    record.committed = committed;
                    record.ended = Some(self.step);
                    self.next_txn(c);
                    return Ok(());
                }
                let results = self.send(c, "BEGIN")?;
                self.expect(c, results, "BEGIN")?;
                self.cursors[c].phase = Phase::Op(0);
            }
            Phase::Op(i) => {
                let op = txn.ops[i];
                let ok = self.run_op(c, op)?;
                self.cursors[c].phase = if ok && i + 1 < txn.ops.len() {
                    Phase::Op(i + 1)
                } else {
                    Phase::End
                };
            }
            Phase::End => {
                let record = self.cursors[c].record;
                let failed = self.records[record].conflicted.is_some();
                let sql = if txn.commit { "COMMIT" } else { "ROLLBACK" };
                // COMMIT of a failed block rolls it back.
                let tag = if txn.commit && !failed {
                    "COMMIT"
                } else {
                    "ROLLBACK"
                };
                let results = self.send(c, sql)?;
                self.expect(c, results, tag)?;
                self.records[record].committed = tag == "COMMIT";
                self.records[record].ended = Some(self.step);
                self.next_txn(c);
            }
        }
        Ok(())
    }

    fn next_txn(&mut self, c: usize) {
        self.cursors[c].txn += 1;
        self.cursors[c].phase = Phase::Begin;
    }

    /// Runs `op` for client `c`'s current transaction. Returns whether it
    /// succeeded; the other outcome is a conflict.
    fn run_op(&mut self, c: usize, op: Op) -> Result<bool, String> {
        let record = self.cursors[c].record;
        match self.send(c, &op.sql())? {
            Ok(results) => {
                let read = match op {
                    Op::Read(_) | Op::Sum => match ints(&results).as_slice() {
                        [row] if row.len() == 1 => Some(row[0]),
                        rows => return Err(self.fail(format!("{op:?} returned {rows:?}"))),
                    },
                    Op::Set(..) | Op::Add(..) => {
                        if results != [QueryResult::Command("UPDATE 1".into())] {
                            return Err(self.fail(format!("{op:?} returned {results:?}")));
                        }
                        None
                    }
                };
                self.records[record].done.push((op, read));
                Ok(true)
            }
            Err(code) if code == SERIALIZATION_FAILURE => {
                self.records[record].conflicted = Some((op, self.step));
                Ok(false)
            }
            Err(code) => Err(self.fail(format!("{op:?} failed with {code}"))),
        }
    }

    fn expect(
        &self,
        c: usize,
        results: Result<Vec<QueryResult>, String>,
        tag: &str,
    ) -> Result<(), String> {
        match results {
            Ok(results) if results == [QueryResult::Command(tag.into())] => Ok(()),
            other => Err(self.fail(format!("client {c}: expected {tag}, got {other:?}"))),
        }
    }

    fn fail(&self, message: String) -> String {
        format!("{message}\n\nstatements:\n{}", self.log)
    }
}

/// Runs `case` on a fresh server and checks it.
fn run(case: &Case) -> Result<(), String> {
    let server = Running::start(Some(Duration::ZERO));
    let mut setup = server.connect();
    exec(
        &mut setup,
        "CREATE TABLE kv (k INT PRIMARY KEY, v INT NOT NULL)",
    );
    let rows: Vec<String> = (0..case.keys).map(|k| format!("({k}, 0)")).collect();
    exec(
        &mut setup,
        &format!("INSERT INTO kv VALUES {}", rows.join(", ")),
    );

    let mut driver = Driver {
        case,
        conns: (0..case.clients.len()).map(|_| server.connect()).collect(),
        cursors: (0..case.clients.len())
            .map(|_| Cursor {
                txn: 0,
                phase: Phase::Begin,
                record: 0,
            })
            .collect(),
        records: vec![],
        step: 0,
        log: String::new(),
    };
    let n = case.clients.len();
    let picks = case.schedule.iter().map(|&c| c % n);
    let drain = (0..n).cycle().take(n * 4 * 6);
    for c in picks.chain(drain) {
        if !driver.done(c) {
            driver.advance(c)?;
        }
    }
    assert!((0..n).all(|c| driver.done(c)), "drain was too short");

    let committed = check(case.keys, &driver.records).map_err(|e| driver.fail(e))?;
    if kv(&mut setup) != committed {
        return Err(driver.fail(format!(
            "table is {:?}, serial order gives {committed:?}",
            kv(&mut setup)
        )));
    }
    drop(driver);
    drop(setup);
    let server = server.restart();
    let after = kv(&mut server.connect());
    server.stop();
    if after != committed {
        return Err(format!(
            "after a restart the table is {after:?}, expected {committed:?}"
        ));
    }
    Ok(())
}

/// Checks every read against the committed transactions run serially in
/// Tid order, and that every conflict had a cause. Returns the final table.
fn check(keys: i64, records: &[Record]) -> Result<Vec<(i64, i64)>, String> {
    let mut state = vec![0i64; keys as usize];
    for (tid, record) in records.iter().enumerate() {
        // Even a transaction that later aborts read only committed versions
        // older than it, and older ones can't commit a write it would have
        // had to see.
        let mut local = state.clone();
        for &(op, read) in &record.done {
            let expected = match op {
                Op::Read(k) => Some(local[k as usize]),
                Op::Sum => Some(local.iter().sum()),
                Op::Set(k, c) => {
                    local[k as usize] = c;
                    None
                }
                Op::Add(k, d) => {
                    local[k as usize] += d;
                    None
                }
            };
            if read != expected {
                return Err(format!(
                    "transaction {tid} (client {}): {op:?} read {read:?}, serial order gives {expected:?}",
                    record.client
                ));
            }
        }
        if record.committed {
            if record.done.len() != record.ops.len() {
                return Err(format!("transaction {tid} committed without all its ops"));
            }
            state = local;
        }
        if let Some((op, at)) = record.conflicted
            && !has_cause(keys, records, tid, op, at)
        {
            return Err(format!(
                "transaction {tid} (client {}): {op:?} conflicted with no concurrent transaction touching its rows",
                record.client
            ));
        }
    }
    Ok((0..keys).zip(state).collect())
}

/// Whether a transaction that ran alongside `records[tid]` touched a row
/// that its ops up to and including `failed`, at statement `failed_at`,
/// touched, one of the two writing it.
fn has_cause(keys: i64, records: &[Record], tid: usize, failed: Op, failed_at: usize) -> bool {
    let me = &records[tid];
    let (my_reads, my_writes) = footprint(keys, me.done.iter().map(|d| d.0).chain([failed]));
    records.iter().enumerate().any(|(other, them)| {
        let overlaps = them.began < failed_at && them.ended.is_none_or(|end| end > me.began);
        if other == tid || !overlaps {
            return false;
        }
        let ops = them
            .done
            .iter()
            .map(|d| d.0)
            .chain(them.conflicted.map(|c| c.0));
        let (reads, writes) = footprint(keys, ops);
        !my_writes.is_disjoint(&reads)
            || !my_writes.is_disjoint(&writes)
            || !my_reads.is_disjoint(&writes)
    })
}

fn footprint(keys: i64, ops: impl Iterator<Item = Op>) -> (BTreeSet<i64>, BTreeSet<i64>) {
    let (mut reads, mut writes) = (BTreeSet::new(), BTreeSet::new());
    for op in ops {
        let (r, w) = op.footprint(keys);
        reads.extend(r);
        writes.extend(w);
    }
    (reads, writes)
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: std::env::var("PROPTEST_CASES").ok().and_then(|n| n.parse().ok()).unwrap_or(64),
        failure_persistence: None,
        ..ProptestConfig::default()
    })]

    #[test]
    fn schedules_equal_running_committed_transactions_in_tid_order(case in case()) {
        if let Err(err) = run(&case) {
            prop_assert!(false, "{err}");
        }
    }
}
