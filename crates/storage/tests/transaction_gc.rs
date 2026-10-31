//! Transaction-level garbage collection: the tuples a transaction leaves
//! deleted are reclaimed once the watermark passes it, and their slots stay
//! dead.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::thread;

use common::{Column, DataType, PageId, Rid, Schema, SlotId, Tuple, TxnId, Value};
use parking_lot::Mutex;
use storage::StorageError;
use storage::buffer::arc_replacer::AccessType;
use storage::buffer::buffer_pool_manager::BufferPoolManager;
use storage::concurrency::transaction::Transaction;
use storage::concurrency::transaction_manager::TransactionManager;
use storage::disk::disk_manager::DiskManager;
use storage::page::PAGE_SIZE;
use storage::table::table_heap::TableHeap;
use storage::table::table_page::TablePage;
use tempfile::TempDir;

/// Bytes in front of each tuple's payload: `ts`, `undo_ptr` and flags.
const TUPLE_HEADER_SIZE: usize = 17;

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

/// `(id INT NOT NULL, v BIGINT, name VARCHAR)`.
fn schema() -> Schema {
    Schema::new(vec![
        Column::new("id", DataType::Int, false),
        Column::new("v", DataType::BigInt, true),
        Column::new("name", DataType::Varchar(4000), true),
    ])
}

/// Row `id` as written by transaction `tid`, with a name of `len` bytes.
fn row(id: i32, tid: TxnId, len: usize) -> Vec<Value> {
    vec![
        Value::Int(id),
        Value::BigInt(tid as i64),
        Value::from("x".repeat(len).as_str()),
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

    fn delete(&self, txn: &Transaction, rid: Rid) {
        txn.delete(&self.heap, &schema(), rid).unwrap()
    }

    fn read(&self, txn: &Transaction, rid: Rid) -> Option<Vec<Value>> {
        let (_, tuple) = txn.get(&self.heap, &schema(), rid).unwrap()?;
        Some(tuple.decode(&schema()).unwrap())
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

    /// Deletes `rid` in a transaction of its own and commits it.
    fn delete_committed(&self, rid: Rid) {
        let txn = self.tm.begin();
        self.delete(&txn, rid);
        self.tm.commit(&txn).unwrap();
    }

    fn with_page<T>(
        &self,
        page_id: PageId,
        f: impl FnOnce(&TablePage<&[u8; PAGE_SIZE]>) -> T,
    ) -> T {
        let guard = self
            .bpm
            .checked_read_page(page_id, AccessType::Unknown)
            .unwrap();
        f(&TablePage::new(guard.get_data()).unwrap())
    }

    fn is_dead(&self, rid: Rid) -> bool {
        self.with_page(rid.page_id, |page| page.is_dead(rid.slot))
    }

    fn free_space(&self, page_id: PageId) -> usize {
        self.with_page(page_id, |page| page.free_space())
    }

    fn slot_count(&self, page_id: PageId) -> SlotId {
        self.with_page(page_id, |page| page.slot_count())
    }
}

#[test]
fn a_committed_delete_is_reclaimed_once_older_readers_end() {
    let db = setup(8);
    let [a, b] = db.load(&[row(1, 1, 100), row(2, 1, 100)])[..] else {
        unreachable!()
    };
    let free = db.free_space(a.page_id);

    let older = db.tm.begin();
    let deleter = db.tm.begin();
    db.delete(&deleter, a);
    db.tm.commit(&deleter).unwrap();
    db.tm.wait_for_gc();

    // `older` still reads the row from before the delete, so it stays.
    assert!(!db.is_dead(a));
    assert_eq!(db.tm.garbage_len(), 1);
    assert_eq!(db.read(&older, a), Some(row(1, 1, 100)));
    assert_eq!(db.free_space(a.page_id), free);

    db.tm.commit(&older).unwrap();
    db.tm.wait_for_gc();
    assert!(db.is_dead(a));
    assert_eq!(db.tm.garbage_len(), 0);
    assert_eq!(
        db.free_space(a.page_id),
        free + TUPLE_HEADER_SIZE + encode(&row(1, 1, 100)).len()
    );
    assert!(matches!(
        db.heap.get(a),
        Err(StorageError::InvalidSlot(slot)) if slot == a.slot
    ));

    let reader = db.tm.begin();
    assert_eq!(db.read(&reader, a), None);
    assert_eq!(db.scan(&reader).unwrap(), [(b, row(2, 1, 100))]);
    db.tm.commit(&reader).unwrap();
}

#[test]
fn an_aborted_insert_is_reclaimed_once_the_watermark_passes_it() {
    let db = setup(8);
    let older = db.tm.begin();
    let txn = db.tm.begin();
    let rid = db.insert(&txn, &row(1, txn.tid(), 100));
    db.tm.abort(&txn).unwrap();
    db.tm.wait_for_gc();

    // Rollback leaves it deleted at ts 0 until `older` ends.
    let (meta, _) = db.heap.get(rid).unwrap();
    assert!(meta.is_deleted && meta.ts == 0);
    assert!(!db.is_dead(rid));
    assert_eq!(db.read(&older, rid), None);
    db.tm.commit(&older).unwrap();
    db.tm.wait_for_gc();
    assert!(db.is_dead(rid));

    // With no older transaction, the pass the abort asks for reclaims it.
    let txn = db.tm.begin();
    let rid = db.insert(&txn, &row(2, txn.tid(), 100));
    db.update(&txn, rid, &row(2, txn.tid(), 300));
    db.tm.abort(&txn).unwrap();
    db.tm.wait_for_gc();
    assert!(db.is_dead(rid));
    assert_eq!(db.tm.garbage_len(), 0);
}

#[test]
fn a_row_inserted_and_deleted_by_one_transaction_is_reclaimed() {
    let db = setup(8);
    let txn = db.tm.begin();
    let rid = db.insert(&txn, &row(1, txn.tid(), 100));
    db.delete(&txn, rid);
    db.tm.commit(&txn).unwrap();
    db.tm.wait_for_gc();
    assert!(db.is_dead(rid));
}

#[test]
fn an_update_that_moved_reclaims_the_old_rid() {
    let db = setup(8);
    let rids = db.load(&[row(1, 1, 1000), row(2, 1, 1000), row(3, 1, 1000)]);
    let old = rids[0];
    let txn = db.tm.begin();
    let values = row(1, txn.tid(), 2500);
    let new = db.update(&txn, old, &values);
    assert_ne!(new.page_id, old.page_id, "the update did not fit in place");
    db.tm.commit(&txn).unwrap();
    db.tm.wait_for_gc();

    assert!(db.is_dead(old));
    assert!(!db.is_dead(new));
    let reader = db.tm.begin();
    assert_eq!(db.read(&reader, old), None);
    assert_eq!(db.read(&reader, new), Some(values.clone()));
    assert_eq!(
        db.scan(&reader).unwrap(),
        [
            (rids[1], row(2, 1, 1000)),
            (rids[2], row(3, 1, 1000)),
            (new, values)
        ]
    );
    db.tm.commit(&reader).unwrap();
}

#[test]
fn updates_and_aborted_deletes_leave_nothing_to_reclaim() {
    let db = setup(8);
    let [a, b] = db.load(&[row(1, 1, 100), row(2, 1, 100)])[..] else {
        unreachable!()
    };
    let updater = db.tm.begin();
    let updated = row(1, updater.tid(), 50);
    assert_eq!(db.update(&updater, a, &updated), a);
    db.tm.commit(&updater).unwrap();

    let deleter = db.tm.begin();
    db.delete(&deleter, b);
    db.tm.abort(&deleter).unwrap();
    db.tm.wait_for_gc();

    assert_eq!(db.tm.garbage_len(), 0);
    assert!(!db.is_dead(a) && !db.is_dead(b));
    let reader = db.tm.begin();
    assert_eq!(db.read(&reader, a), Some(updated));
    assert_eq!(db.read(&reader, b), Some(row(2, 1, 100)));
    db.tm.commit(&reader).unwrap();
    db.tm.wait_for_gc();
    assert!(db.tm.undo_log().is_empty());
}

#[test]
fn a_dead_slot_is_not_reused_and_reads_as_nothing() {
    let db = setup(8);
    let [a, b] = db.load(&[row(1, 1, 100), row(2, 1, 100)])[..] else {
        unreachable!()
    };
    db.delete_committed(a);
    db.tm.wait_for_gc();
    assert!(db.is_dead(a));

    let slots = db.slot_count(a.page_id);
    let txn = db.tm.begin();
    let c = db.insert(&txn, &row(3, txn.tid(), 100));
    assert_eq!(c, Rid::new(a.page_id, slots));
    // A write to the dead `Rid` finds the tuple deleted.
    let tuple = encode(&row(1, txn.tid(), 100));
    assert!(matches!(
        txn.update(&db.heap, &schema(), a, &tuple),
        Err(StorageError::TupleDeleted(rid)) if rid == a
    ));
    assert!(matches!(
        txn.delete(&db.heap, &schema(), a),
        Err(StorageError::TupleDeleted(rid)) if rid == a
    ));
    db.tm.commit(&txn).unwrap();

    // `hold` keeps `reader` from being the oldest, so its reads are recorded:
    // all but the read of the dead slot, which no write can succeed on.
    let hold = db.tm.begin();
    let reader = db.tm.begin();
    assert_eq!(db.read(&reader, a), None);
    assert_eq!(db.read(&reader, b), Some(row(2, 1, 100)));
    assert_eq!(db.tm.read_timestamps().get(a), None);
    assert_eq!(db.tm.read_timestamps().get(b), Some(reader.tid()));
    db.tm.commit(&reader).unwrap();
    db.tm.commit(&hold).unwrap();
    db.tm.wait_for_gc();
    assert!(db.is_dead(a));
}

#[test]
fn garbage_collection_is_idempotent() {
    let db = setup(8);
    let [a, b] = db.load(&[row(1, 1, 100), row(2, 1, 100)])[..] else {
        unreachable!()
    };
    db.delete_committed(a);
    db.tm.wait_for_gc();
    assert!(db.is_dead(a));
    let free = db.free_space(a.page_id);

    db.tm.garbage_collect().unwrap();
    db.tm.garbage_collect().unwrap();
    let watermark = db.tm.watermark();
    assert!(!db.heap.reclaim(a, watermark).unwrap());
    // A live tuple is not reclaimed either.
    assert!(!db.heap.reclaim(b, watermark).unwrap());

    assert!(db.is_dead(a) && !db.is_dead(b));
    assert_eq!(db.free_space(a.page_id), free);
    assert_eq!(db.tm.garbage_len(), 0);
}

#[test]
fn garbage_waits_until_the_watermark_passes_its_transaction() {
    let db = setup(8);
    let [a, b] = db.load(&[row(1, 1, 100), row(2, 1, 100)])[..] else {
        unreachable!()
    };
    let first = db.tm.begin();
    db.delete_committed(a);
    db.tm.wait_for_gc();
    assert_eq!(db.tm.garbage_len(), 1);
    // An explicit pass does not reclaim it while `first` holds the watermark.
    db.tm.garbage_collect().unwrap();
    assert_eq!(db.tm.garbage_len(), 1);

    let second = db.tm.begin();
    db.delete_committed(b);
    db.tm.wait_for_gc();
    assert_eq!(db.tm.garbage_len(), 2);
    assert!(!db.is_dead(a) && !db.is_dead(b));

    // The watermark moves to `second`: past `a`'s deleter, not `b`'s.
    db.tm.commit(&first).unwrap();
    db.tm.wait_for_gc();
    assert_eq!(db.tm.garbage_len(), 1);
    assert!(db.is_dead(a) && !db.is_dead(b));

    db.tm.commit(&second).unwrap();
    db.tm.wait_for_gc();
    assert_eq!(db.tm.garbage_len(), 0);
    assert!(db.is_dead(b));
}

#[test]
fn reclaiming_a_tuple_drops_its_read_timestamp() {
    let db = setup(8);
    let [a] = db.load(&[row(1, 1, 100)])[..] else {
        unreachable!()
    };
    let hold = db.tm.begin();
    let deleter = db.tm.begin();
    let reader = db.tm.begin();
    db.delete(&deleter, a);
    db.tm.commit(&deleter).unwrap();
    assert_eq!(db.read(&reader, a), None);
    assert_eq!(db.tm.read_timestamps().get(a), Some(reader.tid()));

    // The watermark passes the deleter but not `reader`, so only the
    // reclaim frees the entry.
    db.tm.commit(&hold).unwrap();
    db.tm.wait_for_gc();
    assert!(db.is_dead(a));
    assert_eq!(db.tm.read_timestamps().get(a), None);
    db.tm.commit(&reader).unwrap();
}

/// Dropping the manager stops its collector thread, after the pass still
/// owed for the transactions that ended last.
#[test]
fn dropping_the_manager_finishes_collection() {
    let db = setup(8);
    let rids = db.load(&(0..20).map(|id| row(id, 1, 100)).collect::<Vec<_>>());
    for &rid in &rids {
        db.delete_committed(rid);
    }
    let Db {
        _dir,
        bpm,
        heap: _heap,
        tm,
    } = db;
    drop(tm);
    for rid in rids {
        let guard = bpm
            .checked_read_page(rid.page_id, AccessType::Unknown)
            .unwrap();
        assert!(TablePage::new(guard.get_data()).unwrap().is_dead(rid.slot));
    }
}

/// A write a transaction made: the values it left at `rid`, or `None` for a
/// delete.
struct Write {
    rid: Rid,
    values: Option<Vec<Value>>,
    inserted: bool,
}

/// Threads run short transactions that insert, update and delete shared
/// rows, and commit, or abort on purpose or on a conflict. Once all have
/// ended, nothing is left to collect, every row deleted or inserted by an
/// abort is dead, and a scan sees exactly the rows the committed writes
/// left, in Tid order.
#[test]
fn concurrent_transactions_leave_no_garbage() {
    const THREADS: u64 = 4;
    const TXNS: u64 = 40;
    let db = setup(64);
    let initial: Vec<Vec<Value>> = (0..30).map(|id| row(id, 1, 100)).collect();
    let rids = db.load(&initial);
    let loaded = rids
        .into_iter()
        .zip(initial)
        .map(|(rid, values)| Write {
            rid,
            values: Some(values),
            inserted: true,
        })
        .collect();

    let committed = Mutex::new(vec![(1, loaded)]);
    let aborted_inserts = Mutex::new(Vec::new());
    thread::scope(|s| {
        for t in 0..THREADS {
            let (db, committed, aborted_inserts) = (&db, &committed, &aborted_inserts);
            s.spawn(move || {
                let mut rng = Rng(t * 7919 + 1);
                for _ in 0..TXNS {
                    loop {
                        let txn = db.tm.begin();
                        let mut writes = Vec::new();
                        let done = match random_transaction(db, &txn, &mut rng, &mut writes) {
                            Ok(()) => true,
                            Err(StorageError::Conflict { .. } | StorageError::TupleDeleted(_)) => {
                                false
                            }
                            Err(e) => panic!("transaction {} failed: {e}", txn.tid()),
                        };
                        if done && rng.below(5) != 0 {
                            db.tm.commit(&txn).unwrap();
                            committed.lock().push((txn.tid(), writes));
                        } else {
                            db.tm.abort(&txn).unwrap();
                            let inserted = writes.iter().filter(|w| w.inserted);
                            aborted_inserts.lock().extend(inserted.map(|w| w.rid));
                        }
                        if done {
                            break;
                        }
                    }
                }
            });
        }
    });

    db.tm.wait_for_gc();
    assert_eq!(db.tm.garbage_len(), 0);
    assert!(db.tm.undo_log().is_empty());
    assert!(db.tm.read_timestamps().is_empty());

    // A later committed write to a tuple always has a larger Tid.
    let mut committed = committed.into_inner();
    committed.sort_by_key(|&(tid, _)| tid);
    let mut expected = HashMap::new();
    for write in committed.into_iter().flat_map(|(_, writes)| writes) {
        expected.insert(write.rid, write.values);
    }
    let aborted_inserts = aborted_inserts.into_inner();
    assert!(!aborted_inserts.is_empty());
    for rid in &aborted_inserts {
        assert!(!expected.contains_key(rid));
        assert!(db.is_dead(*rid), "aborted insert {rid} is not reclaimed");
    }
    let mut deleted = 0;
    for (&rid, values) in &expected {
        assert_eq!(db.is_dead(rid), values.is_none(), "{rid}");
        deleted += values.is_none() as usize;
    }
    assert!(deleted > 0);

    let live: HashMap<Rid, Vec<Value>> = expected
        .into_iter()
        .filter_map(|(rid, values)| Some((rid, values?)))
        .collect();
    let reader = db.tm.begin();
    let rows = db.scan(&reader).unwrap();
    let rids: HashSet<Rid> = rows.iter().map(|(rid, _)| *rid).collect();
    assert_eq!(rids.len(), rows.len(), "a row was scanned twice");
    assert_eq!(rows.into_iter().collect::<HashMap<_, _>>(), live);
    db.tm.commit(&reader).unwrap();
}

/// One transaction of the stress test: scans the table, then makes two
/// writes, recording each in `writes` as it succeeds.
fn random_transaction(
    db: &Db,
    txn: &Arc<Transaction>,
    rng: &mut Rng,
    writes: &mut Vec<Write>,
) -> storage::Result<()> {
    let tid = txn.tid();
    let rows = db.scan(txn)?;
    let mut touched = HashSet::new();
    for _ in 0..2 {
        let len = rng.below(1200) as usize;
        if rows.is_empty() || rng.below(5) == 0 {
            let values = row(1000 + rng.below(1000) as i32, tid, len);
            let rid = txn.insert(&db.heap, &schema(), &encode(&values))?;
            writes.push(Write {
                rid,
                values: Some(values),
                inserted: true,
            });
            continue;
        }
        let (rid, values) = &rows[rng.below(rows.len() as u64) as usize];
        let rid = *rid;
        if !touched.insert(rid) {
            continue;
        }
        if rng.below(4) == 0 {
            txn.delete(&db.heap, &schema(), rid)?;
            writes.push(Write {
                rid,
                values: None,
                inserted: false,
            });
            continue;
        }
        let Value::Int(id) = values[0] else {
            unreachable!()
        };
        let new = row(id, tid, len);
        let new_rid = txn.update(&db.heap, &schema(), rid, &encode(&new))?;
        let moved = new_rid != rid;
        if moved {
            writes.push(Write {
                rid,
                values: None,
                inserted: false,
            });
        }
        writes.push(Write {
            rid: new_rid,
            values: Some(new),
            inserted: moved,
        });
    }
    Ok(())
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
