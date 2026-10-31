//! Index entries written through a transaction: kept on commit, undone on
//! abort.

use std::sync::Arc;

use common::{Column, DataType, Rid, Schema, Tuple, Value};
use storage::buffer::buffer_pool_manager::BufferPoolManager;
use storage::concurrency::transaction_manager::TransactionManager;
use storage::disk::disk_manager::DiskManager;
use storage::index::btree_index::BtreeIndex;
use storage::index::{Index, IndexMetadata, IndexTrait, LocatorKind, RowLocator};
use tempfile::TempDir;

struct Db {
    _dir: TempDir,
    index: Arc<Index>,
    tm: TransactionManager,
}

/// An index on one INT column, storing rids.
fn setup() -> Db {
    let dir = TempDir::new().unwrap();
    let dm = Arc::new(DiskManager::new(dir.path().join("test.db")).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(8, dm));
    let key_schema = Arc::new(Schema::new(vec![Column::new("id", DataType::Int, false)]));
    let metadata = IndexMetadata::new("t_id", "t", vec![0], key_schema, false, LocatorKind::Rid);
    let index = BtreeIndex::new(metadata, bpm.new_page(), bpm).unwrap();
    Db {
        _dir: dir,
        index: Arc::new(index.into()),
        tm: TransactionManager::new(1),
    }
}

fn key(id: i32) -> Tuple {
    let schema = Schema::new(vec![Column::new("id", DataType::Int, false)]);
    Tuple::encode(&schema, &[Value::Int(id)]).unwrap()
}

fn rid(slot: u16) -> RowLocator {
    RowLocator::Rid(Rid::new(1, slot))
}

impl Db {
    /// The rows the index holds for `id`.
    fn lookup(&self, id: i32) -> Vec<RowLocator> {
        let mut found = Vec::new();
        self.index.scan_key(&key(id), &mut found).unwrap();
        found
    }
}

#[test]
fn abort_undoes_index_writes_newest_first() {
    let db = setup();
    db.index.insert_entry(&key(1), &rid(1)).unwrap();

    let txn = db.tm.begin();
    // Moves row 1 from key 1 to key 2, then to key 3, and adds row 2.
    assert!(txn.delete_entry(&db.index, &key(1), &rid(1)).unwrap());
    assert!(txn.insert_entry(&db.index, &key(2), &rid(1)).unwrap());
    assert!(txn.delete_entry(&db.index, &key(2), &rid(1)).unwrap());
    assert!(txn.insert_entry(&db.index, &key(3), &rid(1)).unwrap());
    assert!(txn.insert_entry(&db.index, &key(3), &rid(2)).unwrap());
    // Writes that change nothing are not recorded.
    assert!(!txn.insert_entry(&db.index, &key(3), &rid(2)).unwrap());
    assert!(!txn.delete_entry(&db.index, &key(9), &rid(9)).unwrap());
    assert_eq!(db.lookup(3), [rid(1), rid(2)]);
    db.tm.abort(&txn).unwrap();

    assert_eq!(db.lookup(1), [rid(1)]);
    assert!(db.lookup(2).is_empty());
    assert!(db.lookup(3).is_empty());
    assert!(db.lookup(9).is_empty());
}

#[test]
fn commit_keeps_index_writes() {
    let db = setup();
    db.index.insert_entry(&key(1), &rid(1)).unwrap();
    let txn = db.tm.begin();
    txn.delete_entry(&db.index, &key(1), &rid(1)).unwrap();
    txn.insert_entry(&db.index, &key(2), &rid(1)).unwrap();
    db.tm.commit(&txn).unwrap();
    assert!(db.lookup(1).is_empty());
    assert_eq!(db.lookup(2), [rid(1)]);
}

#[test]
fn an_ended_transaction_writes_no_entries() {
    let db = setup();
    let txn = db.tm.begin();
    db.tm.commit(&txn).unwrap();
    assert!(txn.insert_entry(&db.index, &key(1), &rid(1)).is_err());
    assert!(db.lookup(1).is_empty());
}
