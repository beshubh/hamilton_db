//! Transactions: begin, commit and abort, the write lock, rollback, the MVTO
//! checks that need only the lock and `ts` order, and garbage collection at
//! transaction end.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::thread;

use common::{Column, DataType, PageId, Rid, Schema, TXN_UNCOMMITTED_BIT, Tuple, TxnId, Value};
use parking_lot::Mutex;
use storage::StorageError;
use storage::buffer::arc_replacer::AccessType;
use storage::buffer::buffer_pool_manager::BufferPoolManager;
use storage::concurrency::transaction::{Transaction, TransactionState};
use storage::concurrency::transaction_manager::TransactionManager;
use storage::disk::disk_manager::DiskManager;
use storage::table::table_heap::TableHeap;
use storage::table::table_page::{TablePage, TupleMeta};
use tempfile::TempDir;

struct Db {
    _dir: TempDir,
    bpm: Arc<BufferPoolManager>,
    heap: Arc<TableHeap>,
    tm: TransactionManager,
}

fn setup(num_frames: usize) -> Db {
    let dir = TempDir::new().unwrap();
    let dm = Arc::new(DiskManager::new(dir.path().join("test.db")).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(num_frames, dm));
    let heap = Arc::new(TableHeap::create(Arc::clone(&bpm)).unwrap());
    Db {
        _dir: dir,
        bpm,
        heap,
        tm: TransactionManager::new(1),
    }
}

/// `(id INT NOT NULL, v BIGINT, name VARCHAR, flag BOOL)`.
fn schema() -> Schema {
    Schema::new(vec![
        Column::new("id", DataType::Int, false),
        Column::new("v", DataType::BigInt, true),
        Column::new("name", DataType::Varchar(4000), true),
        Column::new("flag", DataType::Bool, true),
    ])
}

/// Row `id` as written by transaction `tid`, with a name of `len` bytes
/// that also says who wrote it.
fn row(id: i32, tid: TxnId, len: usize) -> Vec<Value> {
    let mut name = format!("{tid}:");
    name.extend(std::iter::repeat_n('x', len.saturating_sub(name.len())));
    vec![
        Value::Int(id),
        Value::BigInt(tid as i64),
        Value::from(name.as_str()),
        Value::Null,
    ]
}

fn encode(values: &[Value]) -> Tuple {
    Tuple::encode(&schema(), values).unwrap()
}

impl Db {
    fn insert(&self, txn: &Transaction, values: &[Value]) -> Rid {
        txn.insert(&self.heap, &schema(), &encode(values)).unwrap()
    }

    fn update(&self, txn: &Transaction, rid: Rid, values: &[Value]) -> Rid {
        txn.update(&self.heap, &schema(), rid, &encode(values))
            .unwrap()
    }

    /// What `txn` reads at `rid`, checking the row is exactly what
    /// encoding its values gives, with no padding.
    fn read(&self, txn: &Transaction, rid: Rid) -> Option<Vec<Value>> {
        let (meta, tuple) = txn.get(&self.heap, &schema(), rid).unwrap()?;
        assert!(!meta.is_deleted);
        assert_eq!(tuple.rid(), Some(rid));
        let values = tuple.decode(&schema()).unwrap();
        assert_eq!(tuple.data(), encode(&values).data());
        Some(values)
    }

    /// Every row `txn` sees, in scan order.
    fn scan(&self, txn: &Arc<Transaction>) -> storage::Result<Vec<(Rid, Vec<Value>)>> {
        let mut scan = txn.scan(&self.heap, &schema())?;
        let mut rows = Vec::new();
        while let Some(batch) = scan.next_batch()? {
            for (_, tuple) in batch {
                rows.push((tuple.rid().unwrap(), tuple.decode(&schema()).unwrap()));
            }
        }
        Ok(rows)
    }

    /// Inserts `rows` in one transaction and commits it.
    fn load(&self, rows: &[Vec<Value>]) -> Vec<Rid> {
        let txn = self.tm.begin();
        let rids = rows
            .iter()
            .map(|values| self.insert(&txn, values))
            .collect();
        self.tm.commit(&txn).unwrap();
        rids
    }

    fn head(&self, rid: Rid) -> (TupleMeta, Tuple) {
        self.heap.get(rid).unwrap()
    }

    fn free_space(&self, page_id: PageId) -> usize {
        let guard = self
            .bpm
            .checked_read_page(page_id, AccessType::Unknown)
            .unwrap();
        TablePage::new(guard.get_data()).unwrap().free_space()
    }
}

fn is_conflict(result: storage::Result<impl Sized>, rid: Rid, other: TxnId) -> bool {
    matches!(result, Err(StorageError::Conflict { rid: r, other: o }) if r == rid && o == other)
}

#[test]
fn begin_hands_out_increasing_tids_and_the_watermark_follows() {
    let db = setup(4);
    assert_eq!(db.tm.watermark(), 1);
    let t1 = db.tm.begin();
    let t2 = db.tm.begin();
    let t3 = db.tm.begin();
    assert_eq!((t1.tid(), t2.tid(), t3.tid()), (1, 2, 3));
    assert_eq!(db.tm.watermark(), 1);

    db.tm.commit(&t2).unwrap();
    assert_eq!(db.tm.watermark(), 1);
    db.tm.abort(&t1).unwrap();
    assert_eq!(db.tm.watermark(), 3);
    db.tm.commit(&t3).unwrap();
    assert_eq!(db.tm.watermark(), 4);

    assert_eq!(t1.state(), TransactionState::Aborted);
    assert_eq!(t2.state(), TransactionState::Committed);
    assert!(matches!(
        db.tm.commit(&t2),
        Err(StorageError::TransactionNotRunning(2))
    ));
    assert!(matches!(
        db.tm.abort(&t1),
        Err(StorageError::TransactionNotRunning(1))
    ));
    assert_eq!(db.tm.watermark(), 4);
}

#[test]
fn an_ended_transaction_cannot_read_or_write() {
    let db = setup(4);
    let txn = db.tm.begin();
    let rid = db.insert(&txn, &row(1, txn.tid(), 10));
    db.tm.commit(&txn).unwrap();

    let not_running =
        |result: storage::Result<_>| matches!(result, Err(StorageError::TransactionNotRunning(1)));
    let tuple = encode(&row(1, 1, 10));
    assert!(not_running(
        txn.insert(&db.heap, &schema(), &tuple).map(|_| ())
    ));
    assert!(not_running(
        txn.update(&db.heap, &schema(), rid, &tuple).map(|_| ())
    ));
    assert!(not_running(txn.delete(&db.heap, &schema(), rid)));
    assert!(not_running(txn.get(&db.heap, &schema(), rid).map(|_| ())));
    assert!(not_running(txn.scan(&db.heap, &schema()).map(|_| ())));
}

#[test]
#[cfg(debug_assertions)]
#[should_panic(expected = "transaction 1 was dropped while running")]
fn dropping_a_running_transaction_panics_in_debug_builds() {
    let db = setup(4);
    drop(db.tm.begin());
}

#[test]
fn a_transaction_reads_its_own_writes() {
    let db = setup(4);
    let [a, b] = db.load(&[row(1, 1, 10), row(2, 1, 10)])[..] else {
        unreachable!()
    };
    let txn = db.tm.begin();
    let tid = txn.tid();

    let c = db.insert(&txn, &row(3, tid, 10));
    assert_eq!(db.read(&txn, c), Some(row(3, tid, 10)));
    assert_eq!(db.update(&txn, a, &row(1, tid, 50)), a);
    assert_eq!(db.read(&txn, a), Some(row(1, tid, 50)));
    // A shrinking update reads back without its padding.
    assert_eq!(db.update(&txn, a, &row(1, tid, 4)), a);
    assert_eq!(db.read(&txn, a), Some(row(1, tid, 4)));
    txn.delete(&db.heap, &schema(), b).unwrap();
    assert_eq!(db.read(&txn, b), None);

    let rows = db.scan(&txn).unwrap();
    assert_eq!(rows, [(a, row(1, tid, 4)), (c, row(3, tid, 10))]);

    // The heap holds the locked versions.
    assert_eq!(db.head(a).0.ts, tid | TXN_UNCOMMITTED_BIT);
    assert_eq!(db.head(c).0.ts, tid | TXN_UNCOMMITTED_BIT);
    db.tm.commit(&txn).unwrap();
}

#[test]
fn uncommitted_writes_are_hidden_until_commit() {
    let db = setup(4);
    let [rid] = db.load(&[row(1, 1, 30)])[..] else {
        unreachable!()
    };
    let older = db.tm.begin();
    let writer = db.tm.begin();
    let newer = db.tm.begin();
    let w = writer.tid();
    db.update(&writer, rid, &row(1, w, 10));
    let inserted = db.insert(&writer, &row(2, w, 10));

    // An older transaction reads the versions from before the writer.
    assert_eq!(db.read(&older, rid), Some(row(1, 1, 30)));
    assert_eq!(db.read(&older, inserted), None);
    assert_eq!(db.scan(&older).unwrap(), [(rid, row(1, 1, 30))]);
    // A newer one would have to read past an uncommitted version.
    assert!(is_conflict(newer.get(&db.heap, &schema(), rid), rid, w));
    assert!(is_conflict(
        newer.get(&db.heap, &schema(), inserted),
        inserted,
        w
    ));
    assert!(is_conflict(db.scan(&newer), rid, w));

    db.tm.commit(&writer).unwrap();
    for (rid, values) in [(rid, row(1, w, 10)), (inserted, row(2, w, 10))] {
        let (meta, tuple) = db.head(rid);
        assert_eq!(meta.ts, w);
        // Commit released the shrinking update's padding.
        assert_eq!(tuple.data(), encode(&values).data());
        assert_eq!(db.read(&newer, rid), Some(values));
    }
    assert_eq!(db.read(&older, rid), Some(row(1, 1, 30)));
    assert_eq!(db.read(&older, inserted), None);

    let later = db.tm.begin();
    assert_eq!(db.scan(&later).unwrap().len(), 2);
    for txn in [older, newer, later] {
        db.tm.commit(&txn).unwrap();
    }
    assert!(db.tm.undo_log().is_empty());
}

#[test]
fn several_writes_to_one_tuple_commit_as_the_last() {
    let db = setup(4);
    let [rid] = db.load(&[row(1, 1, 30)])[..] else {
        unreachable!()
    };
    let older = db.tm.begin();
    let writer = db.tm.begin();
    let w = writer.tid();
    for len in [100, 5, 60, 20] {
        db.update(&writer, rid, &row(1, w, len));
    }
    assert_eq!(db.read(&writer, rid), Some(row(1, w, 20)));
    db.tm.commit(&writer).unwrap();

    // The older reader walks past the writer's intermediate versions.
    assert_eq!(db.read(&older, rid), Some(row(1, 1, 30)));
    let newer = db.tm.begin();
    assert_eq!(db.read(&newer, rid), Some(row(1, w, 20)));
    assert_eq!(db.head(rid).1.data(), encode(&row(1, w, 20)).data());
    db.tm.commit(&older).unwrap();
    db.tm.commit(&newer).unwrap();
    assert!(db.tm.undo_log().is_empty());
}

#[test]
fn abort_restores_every_tuple_it_wrote() {
    let db = setup(4);
    let ids = 0..6;
    let rows: Vec<Vec<Value>> = ids.clone().map(|id| row(id, 1, 1500)).collect();
    let rids = db.load(&rows);
    let heads: Vec<(TupleMeta, Tuple)> = rids.iter().map(|&rid| db.head(rid)).collect();

    let older = db.tm.begin();
    let txn = db.tm.begin();
    let tid = txn.tid();
    // Updated in place.
    db.update(&txn, rids[0], &row(0, tid, 1200));
    // Deleted.
    txn.delete(&db.heap, &schema(), rids[1]).unwrap();
    // Written several times, shrinking, growing, then deleted.
    db.update(&txn, rids[2], &row(2, tid, 10));
    db.update(&txn, rids[2], &row(2, tid, 1550));
    db.update(&txn, rids[2], &row(2, tid, 3));
    txn.delete(&db.heap, &schema(), rids[2]).unwrap();
    // Too big for its page, so moved to a new Rid.
    let moved = db.update(&txn, rids[3], &row(3, tid, 3500));
    assert_ne!(moved, rids[3]);
    // Inserted, then updated.
    let inserted = db.insert(&txn, &row(9, tid, 40));
    db.update(&txn, inserted, &row(9, tid, 80));
    assert_eq!(db.scan(&txn).unwrap().len(), 5);

    db.tm.abort(&txn).unwrap();
    assert_eq!(txn.state(), TransactionState::Aborted);
    for (rid, head) in rids.iter().zip(&heads) {
        assert_eq!(&db.head(*rid), head);
    }
    for rid in [moved, inserted] {
        let (meta, _) = db.head(rid);
        assert!(meta.is_deleted);
        assert_eq!(meta.ts, 0);
    }

    let newer = db.tm.begin();
    for txn in [&older, &newer] {
        let expected: Vec<(Rid, Vec<Value>)> = rids.iter().copied().zip(rows.clone()).collect();
        assert_eq!(db.scan(txn).unwrap(), expected);
        assert_eq!(db.read(txn, moved), None);
        assert_eq!(db.read(txn, inserted), None);
    }
    db.tm.commit(&older).unwrap();
    db.tm.commit(&newer).unwrap();
    assert!(db.tm.undo_log().is_empty());

    // The restored tuples can be written again.
    let txn = db.tm.begin();
    db.update(&txn, rids[1], &row(1, txn.tid(), 5));
    db.tm.commit(&txn).unwrap();
}

#[test]
fn a_write_to_a_locked_tuple_conflicts_until_the_holder_ends() {
    let db = setup(4);
    let [rid] = db.load(&[row(1, 1, 30)])[..] else {
        unreachable!()
    };
    let t1 = db.tm.begin();
    let t2 = db.tm.begin();
    db.update(&t1, rid, &row(1, t1.tid(), 40));
    let before = db.head(rid);
    let undo_len = db.tm.undo_log().len();

    let new = encode(&row(1, t2.tid(), 50));
    assert!(is_conflict(
        t2.update(&db.heap, &schema(), rid, &new),
        rid,
        t1.tid()
    ));
    assert!(is_conflict(
        t2.delete(&db.heap, &schema(), rid),
        rid,
        t1.tid()
    ));
    // The newer writer's reads conflict too; the older one is not blocked
    // by a newer lock.
    assert!(is_conflict(t2.get(&db.heap, &schema(), rid), rid, t1.tid()));
    // Nothing changed.
    assert_eq!(db.head(rid), before);
    assert_eq!(db.tm.undo_log().len(), undo_len);

    db.tm.abort(&t1).unwrap();
    assert_eq!(db.update(&t2, rid, &row(1, t2.tid(), 50)), rid);
    db.tm.commit(&t2).unwrap();
    let t3 = db.tm.begin();
    assert_eq!(db.read(&t3, rid), Some(row(1, t2.tid(), 50)));
    db.tm.commit(&t3).unwrap();
}

#[test]
fn an_older_transaction_cannot_write_over_a_newer_commit() {
    let db = setup(4);
    let [a, b] = db.load(&[row(1, 1, 30), row(2, 1, 30)])[..] else {
        unreachable!()
    };
    let t1 = db.tm.begin();
    let t2 = db.tm.begin();
    db.update(&t2, a, &row(1, t2.tid(), 40));
    t2.delete(&db.heap, &schema(), b).unwrap();
    db.tm.commit(&t2).unwrap();

    let new = encode(&row(1, t1.tid(), 50));
    assert!(is_conflict(
        t1.update(&db.heap, &schema(), a, &new),
        a,
        t2.tid()
    ));
    assert!(is_conflict(t1.delete(&db.heap, &schema(), a), a, t2.tid()));
    // A delete by a newer transaction conflicts too, rather than reporting
    // the tuple deleted.
    assert!(is_conflict(
        t1.update(&db.heap, &schema(), b, &new),
        b,
        t2.tid()
    ));
    // T1 still reads the versions from before T2.
    assert_eq!(db.read(&t1, a), Some(row(1, 1, 30)));
    assert_eq!(db.read(&t1, b), Some(row(2, 1, 30)));
    db.tm.abort(&t1).unwrap();
}

/// A shrinking update keeps its bytes while it is uncommitted, so other
/// inserts cannot take them and its rollback always fits.
#[test]
fn a_shrinking_update_rolls_back_on_a_page_filled_since() {
    let db = setup(8);
    let [rid] = db.load(&[row(1, 1, 2500)])[..] else {
        unreachable!()
    };
    let page = rid.page_id;
    let free = db.free_space(page);

    let shrinker = db.tm.begin();
    db.update(&shrinker, rid, &row(1, shrinker.tid(), 10));
    assert_eq!(db.free_space(page), free);

    // Another transaction fills the page.
    let filler = db.tm.begin();
    let mut on_page = 0;
    loop {
        let new = db.insert(&filler, &row(100 + on_page, filler.tid(), 50));
        if new.page_id != page {
            break;
        }
        on_page += 1;
    }
    assert!(on_page > 0);
    db.tm.commit(&filler).unwrap();

    db.tm.abort(&shrinker).unwrap();
    let reader = db.tm.begin();
    assert_eq!(db.read(&reader, rid), Some(row(1, 1, 2500)));
    db.tm.commit(&reader).unwrap();

    // Committed, a shrink gives its bytes back.
    let shrinker = db.tm.begin();
    let free = db.free_space(page);
    db.update(&shrinker, rid, &row(1, shrinker.tid(), 10));
    assert_eq!(db.free_space(page), free);
    db.tm.commit(&shrinker).unwrap();
    assert_eq!(db.free_space(page), free + 2490);
}

#[test]
fn undo_records_live_as_long_as_a_reader_needs_them() {
    let db = setup(4);
    let rids = db.load(&[row(1, 1, 30), row(2, 1, 30)]);
    assert!(db.tm.undo_log().is_empty());

    let old = db.tm.begin();
    let mut last = 0;
    for _ in 0..5 {
        let txn = db.tm.begin();
        for (id, &rid) in rids.iter().enumerate() {
            db.update(&txn, rid, &row(id as i32 + 1, txn.tid(), 20));
        }
        db.tm.commit(&txn).unwrap();
        last = txn.tid();
    }
    // Each write's record is kept for the old reader.
    assert_eq!(db.tm.undo_log().len(), 10);
    assert_eq!(db.tm.watermark(), old.tid());
    assert_eq!(
        db.scan(&old).unwrap(),
        [(rids[0], row(1, 1, 30)), (rids[1], row(2, 1, 30))]
    );

    // Aborted writes' records are kept until the watermark passes them too.
    let aborted = db.tm.begin();
    db.update(&aborted, rids[0], &row(1, aborted.tid(), 5));
    db.tm.abort(&aborted).unwrap();
    assert_eq!(db.tm.undo_log().len(), 11);

    db.tm.commit(&old).unwrap();
    assert!(db.tm.undo_log().is_empty());
    let new = db.tm.begin();
    assert_eq!(db.read(&new, rids[0]), Some(row(1, last, 20)));
    db.tm.commit(&new).unwrap();
}

/// Threads run short transactions over shared rows: each scans the table,
/// then updates, deletes or inserts a row, and commits, or aborts on
/// purpose. A `Conflict` aborts the transaction and retries it.
///
/// Every transaction reads every row, so a write conflicts with any newer
/// transaction that scanned in between (see `read_ts`), and most attempts
/// abort. Aborted inserts leave slots that every later scan reads, since
/// nothing vacuums them, so the load is kept small.
#[test]
fn concurrent_transactions_see_only_committed_writes() {
    const THREADS: u64 = 4;
    const TXNS: u64 = 50;
    let db = setup(64);
    let initial: Vec<Vec<Value>> = (0..40).map(|id| row(id, 1, 100)).collect();
    db.load(&initial);

    let committed = Mutex::new(HashSet::from([1]));
    let aborted = Mutex::new(HashSet::new());
    // Who read whose write: (reader, writer).
    let reads = Mutex::new(Vec::new());

    thread::scope(|s| {
        for t in 0..THREADS {
            let (db, committed, aborted, reads) = (&db, &committed, &aborted, &reads);
            s.spawn(move || {
                let mut rng = Rng(t * 7919 + 1);
                let mut conflicts = 0;
                for _ in 0..TXNS {
                    loop {
                        let txn = db.tm.begin();
                        match random_transaction(db, &txn, &mut rng) {
                            Ok(seen) => {
                                reads
                                    .lock()
                                    .extend(seen.into_iter().map(|w| (txn.tid(), w)));
                                if rng.below(5) == 0 {
                                    db.tm.abort(&txn).unwrap();
                                    aborted.lock().insert(txn.tid());
                                } else {
                                    db.tm.commit(&txn).unwrap();
                                    committed.lock().insert(txn.tid());
                                }
                                break;
                            }
                            Err(StorageError::Conflict { .. } | StorageError::TupleDeleted(_)) => {
                                db.tm.abort(&txn).unwrap();
                                aborted.lock().insert(txn.tid());
                                conflicts += 1;
                            }
                            Err(e) => panic!("transaction {} failed: {e}", txn.tid()),
                        }
                    }
                }
                conflicts
            });
        }
    });

    let committed = committed.into_inner();
    let aborted = aborted.into_inner();
    for (reader, writer) in reads.into_inner() {
        assert!(writer < reader, "{reader} read the write of newer {writer}");
        assert!(
            committed.contains(&writer),
            "{reader} read the write of {writer}, which did not commit"
        );
    }

    let last = db.tm.begin();
    let rows = db.scan(&last).unwrap();
    assert!(!rows.is_empty());
    for (_, values) in &rows {
        let writer = writer_of(values);
        assert!(committed.contains(&writer) && !aborted.contains(&writer));
    }
    db.tm.commit(&last).unwrap();
    assert!(db.tm.undo_log().is_empty());
    assert!(!aborted.is_empty());
}

/// One transaction of the stress test. Returns the writers of the rows it
/// read, other than itself.
fn random_transaction(
    db: &Db,
    txn: &Arc<Transaction>,
    rng: &mut Rng,
) -> storage::Result<Vec<TxnId>> {
    let tid = txn.tid();
    let rows = db.scan(txn)?;
    let seen = rows.iter().map(|(_, values)| writer_of(values)).collect();
    let mut written = HashMap::new();
    for _ in 0..2 {
        let len = rng.below(1200) as usize;
        if rows.is_empty() || rng.below(10) == 0 {
            let id = 1000 + rng.below(1000) as i32;
            written.insert(db.insert(txn, &row(id, tid, len)), Some(row(id, tid, len)));
            continue;
        }
        let (rid, values) = &rows[rng.below(rows.len() as u64) as usize];
        let rid = *rid;
        if written.contains_key(&rid) {
            continue;
        }
        let Value::Int(id) = values[0] else {
            unreachable!()
        };
        if rng.below(10) == 0 {
            txn.delete(&db.heap, &schema(), rid)?;
            written.insert(rid, None);
        } else {
            let new = row(id, tid, len);
            let new_rid = txn.update(&db.heap, &schema(), rid, &encode(&new))?;
            if new_rid != rid {
                written.insert(rid, None);
            }
            written.insert(new_rid, Some(new));
        }
    }
    for (rid, values) in written {
        assert_eq!(db.read(txn, rid), values, "{tid} reads its own write");
    }
    Ok(seen)
}

fn writer_of(values: &[Value]) -> TxnId {
    let Value::BigInt(writer) = values[1] else {
        panic!("v is not set: {values:?}")
    };
    let Value::Varchar(name) = &values[2] else {
        panic!("name is not set: {values:?}")
    };
    assert!(
        name.starts_with(&format!("{writer}:")),
        "v and name come from different writes: {values:?}"
    );
    writer as TxnId
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
