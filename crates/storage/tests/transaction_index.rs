//! Index entries written through transactions, as `index_versions`
//! describes: writes add entries and retire them, older readers still reach
//! the versions they see, garbage collection removes retired entries no one
//! needs, and a primary key is checked against rows rather than entries.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

use common::{Column, DataType, Rid, Schema, Tuple, Value};
use storage::StorageError;
use storage::buffer::buffer_pool_manager::BufferPoolManager;
use storage::concurrency::index_versions::IndexedTable;
use storage::concurrency::transaction::Transaction;
use storage::concurrency::transaction_manager::TransactionManager;
use storage::disk::disk_manager::DiskManager;
use storage::index::btree_index::BtreeIndex;
use storage::index::{Index, IndexMetadata, IndexTrait, LocatorKind, RowLocator};
use storage::table::table_heap::TableHeap;
use tempfile::TempDir;

/// A table `(id INT NOT NULL, name VARCHAR(32))` with primary key `id`, and
/// two indexes on `name`: `by_name` stores primary keys, `by_name_rid` rids.
struct Db {
    _dir: TempDir,
    tm: TransactionManager,
    table: Arc<IndexedTable>,
    pkey: Arc<Index>,
    by_name: Arc<Index>,
    by_name_rid: Arc<Index>,
}

fn schema() -> Schema {
    Schema::new(vec![
        Column::new("id", DataType::Int, false),
        Column::new("name", DataType::Varchar(32), true),
    ])
}

fn id_schema() -> Schema {
    Schema::new(vec![Column::new("id", DataType::Int, false)])
}

fn name_schema() -> Schema {
    Schema::new(vec![Column::new("name", DataType::Varchar(32), true)])
}

fn setup() -> Db {
    let dir = TempDir::new().unwrap();
    let dm = Arc::new(DiskManager::new(dir.path().join("test.db")).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(16, dm));
    let index = |name: &str, column, key_schema, is_primary_key, locator| {
        let metadata = IndexMetadata::new(
            name,
            "t",
            vec![column],
            Arc::new(key_schema),
            is_primary_key,
            locator,
        );
        let index = BtreeIndex::new(metadata, bpm.new_page(), Arc::clone(&bpm)).unwrap();
        Arc::new(Index::from(index))
    };
    let pkey = index("t_pkey", 0, id_schema(), true, LocatorKind::Rid);
    let by_name = index("t_name", 1, name_schema(), false, LocatorKind::PrimaryKey);
    let by_name_rid = index("t_name_rid", 1, name_schema(), false, LocatorKind::Rid);
    let heap = Arc::new(TableHeap::create(Arc::clone(&bpm)).unwrap());
    let table = Arc::new(IndexedTable::new(heap, schema(), Some(Arc::clone(&pkey))));
    Db {
        _dir: dir,
        tm: TransactionManager::new(1),
        table,
        pkey,
        by_name,
        by_name_rid,
    }
}

fn row(id: i32, name: &str) -> Vec<Value> {
    vec![Value::Int(id), Value::from(name)]
}

fn id_key(id: i32) -> Tuple {
    Tuple::encode(&id_schema(), &[Value::Int(id)]).unwrap()
}

fn name_key(name: &str) -> Tuple {
    Tuple::encode(&name_schema(), &[Value::from(name)]).unwrap()
}

/// An index entry: the index, the key and the locator.
type Entry<'a> = (&'a Arc<Index>, Tuple, RowLocator);

impl Db {
    /// The entries of row `values` at `rid`, primary key first.
    fn entries(&self, values: &[Value], rid: Rid) -> Vec<Entry<'_>> {
        let (id, name) = match values {
            [Value::Int(id), Value::Varchar(name)] => (*id, name.as_str()),
            values => panic!("{values:?}"),
        };
        vec![
            (&self.pkey, id_key(id), RowLocator::Rid(rid)),
            (
                &self.by_name,
                name_key(name),
                RowLocator::PrimaryKey(id_key(id)),
            ),
            (&self.by_name_rid, name_key(name), RowLocator::Rid(rid)),
        ]
    }

    /// Inserts `values` as an INSERT does. `None` if the primary key is
    /// taken.
    fn insert(&self, txn: &Transaction, values: &[Value]) -> storage::Result<Option<Rid>> {
        let tuple = Tuple::encode(&schema(), values).unwrap();
        let rid = txn.insert(&self.table.heap, &schema(), &tuple)?;
        for (index, key, locator) in self.entries(values, rid) {
            if !txn.insert_entry(&self.table, index, &key, &locator)? {
                return Ok(None);
            }
        }
        Ok(Some(rid))
    }

    /// Replaces the row at `rid` with `values` as an UPDATE does, returning
    /// its new rid. `None` if the primary key is taken.
    fn update(
        &self,
        txn: &Transaction,
        rid: Rid,
        values: &[Value],
    ) -> storage::Result<Option<Rid>> {
        let old = self.get(txn, rid);
        let tuple = Tuple::encode(&schema(), values).unwrap();
        let new_rid = txn.update(&self.table.heap, &schema(), rid, &tuple)?;
        let entries = self
            .entries(&old, rid)
            .into_iter()
            .zip(self.entries(values, new_rid));
        for ((index, old_key, old_locator), (_, key, locator)) in entries {
            if old_key == key && old_locator == locator {
                continue;
            }
            txn.retire_entry(&self.table, index, &old_key, &old_locator)?;
            if !txn.insert_entry(&self.table, index, &key, &locator)? {
                return Ok(None);
            }
        }
        Ok(Some(new_rid))
    }

    /// Deletes the row at `rid` as a DELETE does.
    fn delete(&self, txn: &Transaction, rid: Rid) -> storage::Result<()> {
        let old = self.get(txn, rid);
        txn.delete(&self.table.heap, &schema(), rid)?;
        for (index, key, locator) in self.entries(&old, rid) {
            txn.retire_entry(&self.table, index, &key, &locator)?;
        }
        Ok(())
    }

    /// The row at `rid` as `txn` sees it, which must exist.
    fn get(&self, txn: &Transaction, rid: Rid) -> Vec<Value> {
        let (_, tuple) = txn.get(&self.table.heap, &schema(), rid).unwrap().unwrap();
        tuple.decode(&schema()).unwrap()
    }

    /// The rows `txn` finds through `index` with the key `value`, as an
    /// index scan does: following each entry, and keeping the rows whose
    /// visible version has the key.
    fn find(&self, txn: &Transaction, index: &Index, value: Value) -> Vec<Vec<Value>> {
        let metadata = index.metadata();
        let key = Tuple::encode(metadata.key_schema(), std::slice::from_ref(&value)).unwrap();
        let mut locators = Vec::new();
        index.scan_key(&key, &mut locators).unwrap();
        let column = metadata.key_attrs()[0];
        locators
            .iter()
            .filter_map(|locator| txn.row_at(&self.table, locator).unwrap())
            .map(|(_, tuple)| tuple.decode(&schema()).unwrap())
            .filter(|row| row[column] == value)
            .collect()
    }

    /// The rows `txn` finds through every index with the key of `values`:
    /// by id, and by name through both name indexes. Panics if the indexes
    /// disagree.
    fn find_row(&self, txn: &Transaction, values: &[Value]) -> Vec<Vec<Value>> {
        let by_id = self.find(txn, &self.pkey, values[0].clone());
        let by_id: Vec<_> = by_id.into_iter().filter(|row| row == values).collect();
        for index in [&self.by_name, &self.by_name_rid] {
            let by_name = self.find(txn, index, values[1].clone());
            let by_name: Vec<_> = by_name.into_iter().filter(|row| row == values).collect();
            assert_eq!(by_name, by_id, "{} disagrees", index.metadata().name());
        }
        by_id
    }

    /// The locators `index` holds for `key`, live or not.
    fn locators(&self, index: &Index, key: &Tuple) -> Vec<RowLocator> {
        let mut found = Vec::new();
        index.scan_key(key, &mut found).unwrap();
        found
    }

    /// The number of entries in all three indexes.
    fn entry_count(&self) -> usize {
        [&self.pkey, &self.by_name, &self.by_name_rid]
            .into_iter()
            .map(|index| {
                let Index::BtreeIndex(btree) = &**index;
                btree.tree().begin().unwrap().count()
            })
            .sum()
    }

    /// Runs `f` in a transaction of its own and commits it.
    fn run<T>(&self, f: impl FnOnce(&Transaction) -> T) -> T {
        let txn = self.tm.begin();
        let result = f(&txn);
        self.tm.commit(&txn).unwrap();
        result
    }
}

#[test]
fn older_readers_find_deleted_rows() {
    let db = setup();
    let rid = db.run(|txn| db.insert(txn, &row(1, "a")).unwrap().unwrap());
    let older = db.tm.begin();
    db.run(|txn| db.delete(txn, rid).unwrap());

    assert_eq!(db.find_row(&older, &row(1, "a")), [row(1, "a")]);
    db.run(|txn| assert!(db.find_row(txn, &row(1, "a")).is_empty()));

    // The entries stay until the older reader ends.
    db.tm.wait_for_gc();
    assert_eq!(db.entry_count(), 3);
    db.tm.commit(&older).unwrap();
    db.tm.wait_for_gc();
    assert_eq!(db.entry_count(), 0);
}

#[test]
fn older_readers_find_rows_under_their_old_keys() {
    let db = setup();
    let rid = db.run(|txn| db.insert(txn, &row(1, "a")).unwrap().unwrap());
    let older = db.tm.begin();
    db.run(|txn| db.update(txn, rid, &row(2, "b")).unwrap().unwrap());

    assert_eq!(db.find_row(&older, &row(1, "a")), [row(1, "a")]);
    assert!(db.find_row(&older, &row(2, "b")).is_empty());
    db.run(|txn| {
        assert!(db.find_row(txn, &row(1, "a")).is_empty());
        assert_eq!(db.find_row(txn, &row(2, "b")), [row(2, "b")]);
    });

    db.tm.commit(&older).unwrap();
    db.tm.wait_for_gc();
    assert_eq!(db.entry_count(), 3);
    assert!(db.locators(&db.pkey, &id_key(1)).is_empty());
    assert!(db.locators(&db.by_name, &name_key("a")).is_empty());
    assert!(db.locators(&db.by_name_rid, &name_key("a")).is_empty());
}

/// A row too long for its page moves to a new rid. Older readers follow the
/// entries for the old rid, newer ones those for the new.
#[test]
fn older_readers_find_rows_that_moved() {
    let db = setup();
    let wide = "w".repeat(32);
    let rids: Vec<Rid> = db.run(|txn| {
        (1..=150)
            .map(|id| db.insert(txn, &row(id, "")).unwrap().unwrap())
            .collect()
    });
    let older = db.tm.begin();
    // Widens rows until the first page is full and one has to move.
    let (id, moved) = db.run(|txn| {
        for (id, &rid) in (1..).zip(&rids) {
            let new_rid = db.update(txn, rid, &row(id, &wide)).unwrap().unwrap();
            if new_rid != rid {
                return (id, new_rid);
            }
        }
        panic!("no row moved");
    });

    assert_eq!(db.find_row(&older, &row(id, "")), [row(id, "")]);
    db.run(|txn| assert_eq!(db.find_row(txn, &row(id, &wide)), [row(id, &wide)]));
    assert_eq!(db.locators(&db.pkey, &id_key(id)).len(), 2);

    db.tm.commit(&older).unwrap();
    db.tm.wait_for_gc();
    assert_eq!(db.locators(&db.pkey, &id_key(id)), [RowLocator::Rid(moved)]);
    db.run(|txn| assert_eq!(db.find_row(txn, &row(id, &wide)), [row(id, &wide)]));
}

/// A retired entry stays while a newer version has its key again.
#[test]
fn an_entry_whose_key_comes_back_stays() {
    let db = setup();
    let rid = db.run(|txn| db.insert(txn, &row(1, "a")).unwrap().unwrap());
    db.run(|txn| db.update(txn, rid, &row(1, "b")).unwrap().unwrap());
    db.run(|txn| db.update(txn, rid, &row(1, "a")).unwrap().unwrap());
    db.tm.wait_for_gc();

    assert_eq!(db.entry_count(), 3);
    assert!(db.locators(&db.by_name, &name_key("b")).is_empty());
    db.run(|txn| assert_eq!(db.find_row(txn, &row(1, "a")), [row(1, "a")]));
}

#[test]
fn abort_removes_the_entries_it_added() {
    let db = setup();
    let rid = db.run(|txn| db.insert(txn, &row(1, "a")).unwrap().unwrap());

    let txn = db.tm.begin();
    db.update(&txn, rid, &row(2, "b")).unwrap().unwrap();
    db.insert(&txn, &row(3, "c")).unwrap().unwrap();
    assert_eq!(db.entry_count(), 9);
    db.tm.abort(&txn).unwrap();

    assert_eq!(db.entry_count(), 3);
    db.tm.wait_for_gc();
    assert_eq!(db.entry_count(), 3);
    db.run(|txn| assert_eq!(db.find_row(txn, &row(1, "a")), [row(1, "a")]));
}

#[test]
fn a_deleted_key_can_be_inserted_again() {
    let db = setup();
    let old_rid = db.run(|txn| db.insert(txn, &row(1, "a")).unwrap().unwrap());
    let older = db.tm.begin();
    db.run(|txn| db.delete(txn, old_rid).unwrap());
    let new_rid = db.run(|txn| db.insert(txn, &row(1, "b")).unwrap().unwrap());

    assert_eq!(db.find_row(&older, &row(1, "a")), [row(1, "a")]);
    assert!(db.find_row(&older, &row(1, "b")).is_empty());
    db.run(|txn| {
        assert!(db.find_row(txn, &row(1, "a")).is_empty());
        assert_eq!(db.find_row(txn, &row(1, "b")), [row(1, "b")]);
    });

    db.tm.commit(&older).unwrap();
    db.tm.wait_for_gc();
    assert_eq!(
        db.locators(&db.pkey, &id_key(1)),
        [RowLocator::Rid(new_rid)]
    );
    assert_eq!(db.entry_count(), 3);
}

#[test]
fn a_key_the_inserter_sees_is_taken() {
    let db = setup();
    db.run(|txn| db.insert(txn, &row(1, "a")).unwrap().unwrap());

    let txn = db.tm.begin();
    assert_eq!(db.insert(&txn, &row(1, "b")).unwrap(), None);
    // The entry it added for the taken key is gone again.
    assert_eq!(db.locators(&db.pkey, &id_key(1)).len(), 1);
    db.tm.abort(&txn).unwrap();
}

/// The inserter's Tid comes before the delete, so in Tid order the key is
/// still taken when it inserts.
#[test]
fn a_key_deleted_after_the_inserter_began_is_taken() {
    let db = setup();
    let rid = db.run(|txn| db.insert(txn, &row(1, "a")).unwrap().unwrap());
    let older = db.tm.begin();
    db.run(|txn| db.delete(txn, rid).unwrap());

    assert_eq!(db.insert(&older, &row(1, "b")).unwrap(), None);
    db.tm.abort(&older).unwrap();
}

/// In Tid order the older transaction inserts first, so the newer insert,
/// already committed, would have been the duplicate.
#[test]
fn a_key_inserted_after_the_inserter_began_conflicts() {
    let db = setup();
    let older = db.tm.begin();
    db.run(|txn| db.insert(txn, &row(1, "a")).unwrap().unwrap());

    let err = db.insert(&older, &row(1, "b")).unwrap_err();
    assert!(matches!(err, StorageError::Conflict { .. }), "{err:?}");
    db.tm.abort(&older).unwrap();
}

#[test]
fn a_key_an_uncommitted_insert_holds_conflicts() {
    let db = setup();
    let first = db.tm.begin();
    let second = db.tm.begin();
    db.insert(&first, &row(1, "a")).unwrap().unwrap();
    for txn in [&second, &db.tm.begin()] {
        let err = db.insert(txn, &row(1, "b")).unwrap_err();
        assert!(matches!(err, StorageError::Conflict { .. }), "{err:?}");
        db.tm.abort(txn).unwrap();
    }

    // Once it aborts the key is free.
    db.tm.abort(&first).unwrap();
    db.run(|txn| db.insert(txn, &row(1, "c")).unwrap().unwrap());
    db.run(|txn| assert_eq!(db.find_row(txn, &row(1, "c")), [row(1, "c")]));
}

/// Deleting a key and aborting, while another transaction tries to insert
/// it, leaves exactly the one row. Removing entries on delete used to let
/// the insert in, and the abort then had no entry to put back.
#[test]
fn a_key_an_uncommitted_delete_holds_conflicts() {
    let db = setup();
    let rid = db.run(|txn| db.insert(txn, &row(1, "a")).unwrap().unwrap());
    let deleter = db.tm.begin();
    let inserter = db.tm.begin();
    db.delete(&deleter, rid).unwrap();

    let err = db.insert(&inserter, &row(1, "b")).unwrap_err();
    assert!(matches!(err, StorageError::Conflict { .. }), "{err:?}");
    db.tm.abort(&inserter).unwrap();
    db.tm.abort(&deleter).unwrap();

    db.tm.wait_for_gc();
    db.run(|txn| {
        assert_eq!(db.find_row(txn, &row(1, "a")), [row(1, "a")]);
        assert_eq!(db.find(txn, &db.pkey, Value::Int(1)), [row(1, "a")]);
    });
}

/// Rows `UPDATE SET id = id + 1` would write: every row first, then the
/// entries, so each new key is checked against the other rows' new versions.
#[test]
fn keys_can_shift_between_rows() {
    let db = setup();
    let rids: Vec<Rid> = db.run(|txn| {
        (1..=3)
            .map(|id| db.insert(txn, &row(id, "x")).unwrap().unwrap())
            .collect()
    });
    db.run(|txn| {
        let old: Vec<_> = rids.iter().map(|&rid| db.get(txn, rid)).collect();
        for (&rid, id) in rids.iter().zip(2..) {
            let tuple = Tuple::encode(&schema(), &row(id, "x")).unwrap();
            txn.update(&db.table.heap, &schema(), rid, &tuple).unwrap();
        }
        for ((&rid, old), id) in rids.iter().zip(&old).zip(2..) {
            let entries = db
                .entries(old, rid)
                .into_iter()
                .zip(db.entries(&row(id, "x"), rid));
            for ((index, old_key, old_locator), (_, key, locator)) in entries {
                if old_key != key || old_locator != locator {
                    txn.retire_entry(&db.table, index, &old_key, &old_locator)
                        .unwrap();
                    assert!(txn.insert_entry(&db.table, index, &key, &locator).unwrap());
                }
            }
        }
    });
    db.run(|txn| {
        for id in 2..=4 {
            assert_eq!(db.find_row(txn, &row(id, "x")), [row(id, "x")]);
        }
    });
}

/// Of transactions inserting one key at once, at most one commits.
#[test]
fn concurrent_inserts_of_one_key() {
    let db = Arc::new(setup());
    for round in 0..20 {
        let committed = thread::scope(|s| {
            let inserters: Vec<_> = (0..4)
                .map(|_| {
                    let db = Arc::clone(&db);
                    s.spawn(move || {
                        let txn = db.tm.begin();
                        match db.insert(&txn, &row(round, "x")) {
                            Ok(Some(_)) => {
                                db.tm.commit(&txn).unwrap();
                                1
                            }
                            Ok(None) | Err(StorageError::Conflict { .. }) => {
                                db.tm.abort(&txn).unwrap();
                                0
                            }
                            Err(e) => panic!("{e}"),
                        }
                    })
                })
                .collect();
            inserters
                .into_iter()
                .map(|t| t.join().unwrap())
                .sum::<usize>()
        });
        assert!(
            committed <= 1,
            "round {round}: {committed} inserts committed"
        );
        let found = db.run(|txn| db.find(txn, &db.pkey, Value::Int(round)).len());
        assert_eq!(found, committed, "round {round}");
    }
}

/// Writers set keys back while garbage collection checks the entries they
/// retired before: an entry must survive, whichever comes first. The race
/// is narrow, so this runs many rounds.
#[test]
fn garbage_collection_keeps_entries_writers_reuse() {
    const WRITERS: i32 = 4;
    const ROUNDS: i32 = 1000;
    let db = setup();
    let rids: Vec<Rid> = db.run(|txn| {
        (0..WRITERS)
            .map(|id| {
                db.insert(txn, &row(id, &format!("a{id}")))
                    .unwrap()
                    .unwrap()
            })
            .collect()
    });
    let writing = AtomicUsize::new(rids.len());
    thread::scope(|s| {
        s.spawn(|| {
            while writing.load(Ordering::Relaxed) > 0 {
                db.tm.garbage_collect().unwrap();
            }
        });
        for (id, &rid) in (0..).zip(&rids) {
            let (db, writing) = (&db, &writing);
            s.spawn(move || {
                // Stops the collector even if an assertion fails.
                let _done = Finished(writing);
                // Each writer has names of its own, so it never reads another's
                // locked row.
                for i in 0..ROUNDS {
                    let name = format!("{}{id}", if i % 2 == 0 { "b" } else { "a" });
                    let new = row(id, &name);
                    db.run(|txn| db.update(txn, rid, &new).unwrap().unwrap());
                    db.run(|txn| assert_eq!(db.find_row(txn, &new), std::slice::from_ref(&new)));
                }
            });
        }
    });
    db.tm.wait_for_gc();
    assert_eq!(db.entry_count(), 3 * rids.len());
}

/// Counts a writer thread out when dropped.
struct Finished<'a>(&'a AtomicUsize);

impl Drop for Finished<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

#[test]
fn an_ended_transaction_writes_no_entries() {
    let db = setup();
    let txn = db.tm.begin();
    db.tm.commit(&txn).unwrap();
    let locator = RowLocator::Rid(Rid::new(1, 0));
    assert!(
        txn.insert_entry(&db.table, &db.pkey, &id_key(1), &locator)
            .is_err()
    );
    assert!(
        txn.retire_entry(&db.table, &db.pkey, &id_key(1), &locator)
            .is_err()
    );
    assert_eq!(db.entry_count(), 0);
}
