//! Read timestamps: a write fails if a newer transaction already read the
//! tuple, reads are recorded whichever version they see, and entries are
//! freed at the watermark.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::thread;

use common::{Column, DataType, Rid, Schema, Tuple, TxnId, Value};
use storage::StorageError;
use storage::buffer::buffer_pool_manager::BufferPoolManager;
use storage::concurrency::transaction::Transaction;
use storage::concurrency::transaction_manager::TransactionManager;
use storage::disk::disk_manager::DiskManager;
use storage::table::table_heap::TableHeap;
use tempfile::TempDir;

struct Db {
    _dir: TempDir,
    heap: Arc<TableHeap>,
    tm: TransactionManager,
}

fn setup(num_frames: usize) -> Db {
    let dir = TempDir::new().unwrap();
    let dm = Arc::new(DiskManager::new(dir.path().join("test.db")).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(num_frames, dm));
    let heap = Arc::new(TableHeap::create(bpm).unwrap());
    Db {
        _dir: dir,
        heap,
        tm: TransactionManager::new(1),
    }
}

/// `(id INT NOT NULL, balance BIGINT NOT NULL, pad VARCHAR)`.
fn schema() -> Schema {
    Schema::new(vec![
        Column::new("id", DataType::Int, false),
        Column::new("balance", DataType::BigInt, false),
        Column::new("pad", DataType::Varchar(4000), true),
    ])
}

fn row(id: i32, balance: i64) -> Vec<Value> {
    vec![Value::Int(id), Value::BigInt(balance), Value::Null]
}

fn encode(values: &[Value]) -> Tuple {
    Tuple::encode(&schema(), values).unwrap()
}

fn balance(values: &[Value]) -> i64 {
    let Value::BigInt(balance) = values[1] else {
        panic!("balance is not set: {values:?}")
    };
    balance
}

impl Db {
    /// Inserts `rows` in one transaction and commits it.
    fn load(&self, rows: &[Vec<Value>]) -> Vec<Rid> {
        let txn = self.tm.begin();
        let rids = rows
            .iter()
            .map(|values| txn.insert(&self.heap, &schema(), &encode(values)).unwrap())
            .collect();
        self.tm.commit(&txn).unwrap();
        rids
    }

    fn read(&self, txn: &Transaction, rid: Rid) -> storage::Result<Option<Vec<Value>>> {
        Ok(txn
            .get(&self.heap, &schema(), rid)?
            .map(|(_, tuple)| tuple.decode(&schema()).unwrap()))
    }

    fn write(&self, txn: &Transaction, rid: Rid, values: &[Value]) -> storage::Result<Rid> {
        txn.update(&self.heap, &schema(), rid, &encode(values))
    }

    fn read_ts(&self, rid: Rid) -> Option<TxnId> {
        self.tm.read_timestamps().get(rid)
    }

    fn end_all(&self, txns: &[&Arc<Transaction>]) {
        for txn in txns {
            self.tm.commit(txn).unwrap();
        }
        self.tm.wait_for_gc();
        assert!(self.tm.read_timestamps().is_empty());
        assert!(self.tm.undo_log().is_empty());
    }
}

fn is_conflict(result: storage::Result<impl Sized>, rid: Rid, other: TxnId) -> bool {
    matches!(result, Err(StorageError::Conflict { rid: r, other: o }) if r == rid && o == other)
}

/// Begins a transaction that stays the oldest, so that the readers begun
/// after it record their reads. See `the_oldest_transaction_records_nothing`.
fn hold_watermark(db: &Db) -> Arc<Transaction> {
    db.tm.begin()
}

#[test]
fn the_oldest_transaction_records_nothing() {
    let db = setup(4);
    let [x, y] = db.load(&[row(1, 100), row(2, 100)])[..] else {
        unreachable!()
    };
    let txn = db.tm.begin();
    db.read(&txn, x).unwrap();
    assert_eq!(audit(&db, &txn).unwrap(), 200);
    assert!(db.tm.read_timestamps().is_empty());
    db.tm.commit(&txn).unwrap();

    // A reader records until the older transaction ends.
    let older = db.tm.begin();
    let reader = db.tm.begin();
    db.read(&reader, x).unwrap();
    assert_eq!(db.read_ts(x), Some(reader.tid()));
    db.tm.commit(&older).unwrap();
    db.read(&reader, y).unwrap();
    assert_eq!(audit(&db, &reader).unwrap(), 200);
    assert_eq!(db.read_ts(y), None);
    assert_eq!(db.tm.read_timestamps().len(), 1);
    db.end_all(&[&reader]);
}

#[test]
fn a_write_fails_once_a_newer_transaction_read_the_tuple() {
    let db = setup(4);
    let [x] = db.load(&[row(1, 100)])[..] else {
        unreachable!()
    };
    let older = db.tm.begin();
    let newer = db.tm.begin();
    assert_eq!(db.read(&newer, x).unwrap(), Some(row(1, 100)));
    assert_eq!(db.read_ts(x), Some(newer.tid()));

    // In Tid order `newer` should have seen this write; it did not.
    assert!(is_conflict(
        db.write(&older, x, &row(1, 50)),
        x,
        newer.tid()
    ));
    db.tm.abort(&older).unwrap();

    let later = db.tm.begin();
    assert_eq!(db.write(&later, x, &row(1, 50)).unwrap(), x);
    db.tm.commit(&later).unwrap();
    // `newer` still sees the version it read.
    assert_eq!(db.read(&newer, x).unwrap(), Some(row(1, 100)));
    db.end_all(&[&newer]);
}

/// A reader that walks past a newer writer's lock must still block older
/// writers, in case the lock holder aborts.
#[test]
fn a_read_past_a_lock_is_recorded() {
    let db = setup(4);
    let [x] = db.load(&[row(1, 100)])[..] else {
        unreachable!()
    };
    let older = db.tm.begin();
    let reader = db.tm.begin();
    let writer = db.tm.begin();
    db.write(&writer, x, &row(1, 70)).unwrap();
    assert_eq!(db.read(&reader, x).unwrap(), Some(row(1, 100)));
    db.tm.abort(&writer).unwrap();

    assert!(is_conflict(
        db.write(&older, x, &row(1, 40)),
        x,
        reader.tid()
    ));
    db.tm.abort(&older).unwrap();
    db.end_all(&[&reader]);
}

/// A read that fails is not recorded, so the lock holder can keep writing.
#[test]
fn a_read_that_conflicts_is_not_recorded() {
    let db = setup(4);
    let [x] = db.load(&[row(1, 100)])[..] else {
        unreachable!()
    };
    let _hold = hold_watermark(&db);
    let writer = db.tm.begin();
    let newer = db.tm.begin();
    db.write(&writer, x, &row(1, 60)).unwrap();
    assert!(is_conflict(db.read(&newer, x), x, writer.tid()));
    assert_eq!(db.read_ts(x), None);
    db.write(&writer, x, &row(1, 70)).unwrap();
    db.tm.commit(&writer).unwrap();
    db.tm.abort(&newer).unwrap();
    db.end_all(&[&_hold]);
}

#[test]
fn a_transaction_can_write_what_it_read() {
    let db = setup(4);
    let [x] = db.load(&[row(1, 100)])[..] else {
        unreachable!()
    };
    let _hold = hold_watermark(&db);
    let txn = db.tm.begin();
    assert_eq!(db.read(&txn, x).unwrap(), Some(row(1, 100)));
    assert_eq!(db.read_ts(x), Some(txn.tid()));
    db.write(&txn, x, &row(1, 90)).unwrap();
    assert_eq!(db.read(&txn, x).unwrap(), Some(row(1, 90)));
    txn.delete(&db.heap, &schema(), x).unwrap();
    db.tm.commit(&txn).unwrap();
    db.end_all(&[&_hold]);
}

#[test]
fn a_newer_writer_is_not_blocked() {
    let db = setup(4);
    let [x] = db.load(&[row(1, 100)])[..] else {
        unreachable!()
    };
    let _hold = hold_watermark(&db);
    let reader = db.tm.begin();
    let writer = db.tm.begin();
    assert_eq!(db.read(&reader, x).unwrap(), Some(row(1, 100)));
    db.write(&writer, x, &row(1, 60)).unwrap();
    db.tm.commit(&writer).unwrap();
    assert_eq!(db.read(&reader, x).unwrap(), Some(row(1, 100)));
    db.end_all(&[&reader, &_hold]);
}

/// A scan records every row of each page it has read, and only those.
#[test]
fn a_scan_records_each_page_it_reads() {
    let db = setup(8);
    let pad = "p".repeat(500);
    let rows: Vec<Vec<Value>> = (0..30)
        .map(|id| {
            vec![
                Value::Int(id),
                Value::BigInt(100),
                Value::from(pad.as_str()),
            ]
        })
        .collect();
    let rids = db.load(&rows);
    let first_page = rids[0].page_id;
    let (on_first, rest): (Vec<Rid>, Vec<Rid>) =
        rids.iter().partition(|rid| rid.page_id == first_page);
    assert!(!rest.is_empty());

    let _hold = hold_watermark(&db);
    let blocked = db.tm.begin();
    let unblocked = db.tm.begin();
    let reader = db.tm.begin();
    let mut scan = reader.scan(&db.heap, &schema()).unwrap();
    let batch = scan.next_batch().unwrap().unwrap();
    assert_eq!(batch.len(), on_first.len());

    let new = |rid: &Rid| {
        let idx = rids.iter().position(|r| r == rid).unwrap();
        let mut values = rows[idx].clone();
        values[1] = Value::BigInt(1);
        values
    };
    for rid in &on_first {
        assert_eq!(db.read_ts(*rid), Some(reader.tid()));
        assert!(is_conflict(
            db.write(&blocked, *rid, &new(rid)),
            *rid,
            reader.tid()
        ));
    }
    db.tm.abort(&blocked).unwrap();
    // A page the scan has not reached yet is not guarded.
    let later = rest[0];
    assert_eq!(db.read_ts(later), None);
    db.write(&unblocked, later, &new(&later)).unwrap();
    db.tm.commit(&unblocked).unwrap();

    // The rest of the scan sees that write, since it is older.
    let mut seen = batch.len();
    while let Some(batch) = scan.next_batch().unwrap() {
        for (_, tuple) in batch {
            let values = tuple.decode(&schema()).unwrap();
            let expected = if tuple.rid() == Some(later) { 1 } else { 100 };
            assert_eq!(balance(&values), expected);
            seen += 1;
        }
    }
    assert_eq!(seen, rids.len());
    for rid in &rids {
        assert_eq!(db.read_ts(*rid), Some(reader.tid()));
    }
    db.end_all(&[&reader, &_hold]);
}

/// A get that finds no version is recorded too: an older writer that would
/// otherwise get `TupleDeleted` conflicts. The watermark is held so that the
/// deleted tuple is not reclaimed.
#[test]
fn a_get_that_finds_nothing_is_recorded() {
    let db = setup(4);
    let [x] = db.load(&[row(1, 100)])[..] else {
        unreachable!()
    };
    let _hold = hold_watermark(&db);
    let deleter = db.tm.begin();
    let writer = db.tm.begin();
    let reader = db.tm.begin();
    deleter.delete(&db.heap, &schema(), x).unwrap();
    db.tm.commit(&deleter).unwrap();

    assert_eq!(db.read(&reader, x).unwrap(), None);
    assert_eq!(db.read_ts(x), Some(reader.tid()));
    assert!(is_conflict(
        db.write(&writer, x, &row(1, 50)),
        x,
        reader.tid()
    ));
    db.tm.abort(&writer).unwrap();
    db.end_all(&[&reader, &_hold]);
}

#[test]
fn entries_below_the_watermark_are_freed() {
    let db = setup(4);
    let [x, y] = db.load(&[row(1, 100), row(2, 100)])[..] else {
        unreachable!()
    };
    let old = db.tm.begin();
    let a = db.tm.begin();
    let b = db.tm.begin();
    db.read(&a, x).unwrap();
    db.read(&b, y).unwrap();
    db.tm.commit(&a).unwrap();
    db.tm.commit(&b).unwrap();

    // `old` holds the watermark, so both entries stay and still block it.
    assert_eq!(db.tm.read_timestamps().len(), 2);
    assert!(is_conflict(db.write(&old, x, &row(1, 50)), x, a.tid()));
    assert!(is_conflict(db.write(&old, y, &row(2, 50)), y, b.tid()));
    db.tm.abort(&old).unwrap();
    db.tm.wait_for_gc();
    assert!(db.tm.read_timestamps().is_empty());

    // With a newer transaction open, an older reader's entry is freed
    // when it ends, and a newer one's kept.
    let c = db.tm.begin();
    let d = db.tm.begin();
    db.read(&c, x).unwrap();
    db.read(&d, y).unwrap();
    db.read(&d, x).unwrap();
    db.tm.commit(&c).unwrap();
    assert_eq!(db.tm.read_timestamps().len(), 2);
    assert_eq!(db.read_ts(x), Some(d.tid()));
    db.end_all(&[&d]);
}

/// Bank transfers between a fixed set of accounts: each transaction reads
/// two balances and moves money between them, retrying on `Conflict`, and
/// some abort on purpose. Without read timestamps an older transfer could
/// overwrite a balance a newer one read, and money would appear or vanish.
/// There are no inserts or deletes, so phantoms cannot happen.
#[test]
fn concurrent_transfers_are_serializable() {
    const THREADS: u64 = 8;
    const TRANSFERS: u64 = 300;
    const ACCOUNTS: i32 = 10;
    const TOTAL: i64 = ACCOUNTS as i64 * 1000;
    let db = setup(16);
    let rids = db.load(&(0..ACCOUNTS).map(|id| row(id, 1000)).collect::<Vec<_>>());
    let scans = AtomicI64::new(0);

    thread::scope(|s| {
        for t in 0..THREADS {
            let (db, rids, scans) = (&db, &rids, &scans);
            s.spawn(move || {
                let mut rng = Rng(t * 7919 + 1);
                for _ in 0..TRANSFERS {
                    loop {
                        let txn = db.tm.begin();
                        let result = if rng.below(10) == 0 {
                            audit(db, &txn).map(|sum| {
                                assert_eq!(sum, TOTAL, "transaction {} saw", txn.tid());
                                scans.fetch_add(1, Ordering::Relaxed);
                            })
                        } else {
                            transfer(db, &txn, rids, &mut rng)
                        };
                        match result {
                            Ok(()) if rng.below(8) == 0 => db.tm.abort(&txn).unwrap(),
                            Ok(()) => {
                                db.tm.commit(&txn).unwrap();
                                break;
                            }
                            Err(StorageError::Conflict { .. }) => db.tm.abort(&txn).unwrap(),
                            Err(e) => panic!("transaction {} failed: {e}", txn.tid()),
                        }
                    }
                }
            });
        }
    });

    assert!(scans.load(Ordering::Relaxed) > 0);
    let last = db.tm.begin();
    assert_eq!(audit(&db, &last).unwrap(), TOTAL);
    db.end_all(&[&last]);
}

/// Moves a random amount from one random account to another.
fn transfer(db: &Db, txn: &Transaction, rids: &[Rid], rng: &mut Rng) -> storage::Result<()> {
    let from = rids[rng.below(rids.len() as u64) as usize];
    let to = loop {
        let to = rids[rng.below(rids.len() as u64) as usize];
        if to != from {
            break to;
        }
    };
    let amount = rng.below(100) as i64;
    let mut from_row = db.read(txn, from)?.expect("accounts are never deleted");
    let mut to_row = db.read(txn, to)?.expect("accounts are never deleted");
    from_row[1] = Value::BigInt(balance(&from_row) - amount);
    to_row[1] = Value::BigInt(balance(&to_row) + amount);
    db.write(txn, from, &from_row)?;
    db.write(txn, to, &to_row)?;
    Ok(())
}

/// The sum of every balance `txn` sees.
fn audit(db: &Db, txn: &Arc<Transaction>) -> storage::Result<i64> {
    let mut scan = txn.scan(&db.heap, &schema())?;
    let mut sum = 0;
    while let Some(batch) = scan.next_batch()? {
        for (_, tuple) in batch {
            sum += balance(&tuple.decode(&schema()).unwrap());
        }
    }
    Ok(sum)
}

/// xorshift64, so the stress test needs no `rand`.
struct Rng(u64);

impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n
    }
}
