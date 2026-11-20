#[path = "../../storage/tests/util/crash_fs.rs"]
mod crash_fs;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use catalog::{Catalog, IndexInfo, LockMode, TableInfo};
use common::{Column, DataType, PageId, Rid, Schema, Tuple, TxnId, Value};
use crash_fs::{CrashFs, Lost};
use storage::Storage;
use storage::concurrency::index_versions::IndexedTable;
use storage::concurrency::transaction::Transaction;
use storage::concurrency::transaction_manager::TransactionManager;
use storage::index::{Index, IndexTrait, RowLocator};
use storage::wal::log::{FIRST_LSN, Log};
use storage::wal::record::Kind;

const DB: &str = "/db/test.db";
const LOG: &str = "/db/test.log";
const SEGMENT: u64 = 32 << 10;

fn schema() -> Schema {
    Schema::new(vec![
        Column::new("id", DataType::BigInt, false),
        Column::new("v", DataType::Int, true),
        Column::new("pad", DataType::Varchar(100), true),
    ])
}

fn key(index: &IndexInfo, value: Value) -> Tuple {
    Tuple::encode(&index.key_schema, &[value]).unwrap()
}

struct Db {
    storage: Storage,
    catalog: Arc<Catalog>,
    tm: TransactionManager,
}

impl Db {
    fn create(fs: &CrashFs) -> Self {
        let storage = storage::open_with(Arc::new(fs.clone()), DB, 8, None, SEGMENT).unwrap();
        let catalog = Arc::new(Catalog::open(Arc::clone(&storage.bpm)).unwrap());
        let tm = TransactionManager::new(Arc::clone(&storage.bpm));
        let db = Self {
            storage,
            catalog,
            tm,
        };
        let txn = db.tm.begin();
        db.catalog
            .create_table_with_constraints(&txn, "t", schema(), &["id"], &["id"], &[])
            .unwrap();
        db.tm.commit(&txn).unwrap();
        let txn = db.tm.begin();
        db.catalog.create_index(&txn, "t_v", "t", &["v"]).unwrap();
        db.tm.commit(&txn).unwrap();
        let txn = db.tm.begin();
        for v in 0..400 {
            db.insert(&txn, v);
        }
        db.tm.commit(&txn).unwrap();
        db.storage.bpm.checkpoint().unwrap();
        db
    }

    fn open(fs: &CrashFs) -> Self {
        let storage = storage::open_with(Arc::new(fs.clone()), DB, 8, None, SEGMENT)
            .unwrap_or_else(|e| panic!("recovery failed: {e}"));
        let catalog = Catalog::open(Arc::clone(&storage.bpm))
            .unwrap_or_else(|e| panic!("the catalog does not open: {e}"));
        let tm = TransactionManager::new(Arc::clone(&storage.bpm));
        Self {
            storage,
            catalog: Arc::new(catalog),
            tm,
        }
    }

    fn table(&self) -> Arc<TableInfo> {
        self.catalog.get_table("t").unwrap()
    }

    fn primary_key(&self) -> Arc<IndexInfo> {
        self.catalog.get_index("t_pkey").unwrap()
    }

    fn insert(&self, txn: &Transaction, v: i32) {
        let table = self.table();
        self.catalog
            .lock_table(txn, table.oid, LockMode::RowExclusive)
            .unwrap();
        let pk = self.primary_key();
        let by_v = self.catalog.get_index("t_v").unwrap();
        let indexed = Arc::new(IndexedTable::new(
            Arc::clone(&table.heap),
            table.schema.clone(),
            Some(Arc::clone(&pk.index)),
        ));
        let id = table.identity(0).unwrap().next_value().unwrap();
        let values = [
            Value::BigInt(id),
            Value::Int(v),
            Value::from("p".repeat(60)),
        ];
        let tuple = Tuple::encode(&table.schema, &values).unwrap();
        let rid = txn.insert(&table.heap, &table.schema, &tuple).unwrap();
        let pk_key = key(&pk, Value::BigInt(id));
        let by_rid = RowLocator::Rid(rid);
        assert!(
            txn.insert_entry(&indexed, &pk.index, &pk_key, &by_rid, rid)
                .unwrap()
        );
        let by_pk = RowLocator::PrimaryKey(pk_key);
        let v_key = key(&by_v, Value::Int(v));
        assert!(
            txn.insert_entry(&indexed, &by_v.index, &v_key, &by_pk, rid)
                .unwrap()
        );
    }

    fn drop_index(&self, txn: &Transaction) {
        let index = self.catalog.get_index("t_v").unwrap();
        self.catalog
            .lock_table(txn, index.table_oid, LockMode::AccessExclusive)
            .unwrap();
        self.catalog.drop_index(txn, index.oid).unwrap();
    }

    fn header(&self) -> PageId {
        self.catalog.get_index("t_v").unwrap().header_page_id
    }

    fn allocated(&self) -> BTreeSet<PageId> {
        self.storage.bpm.checkpoint().unwrap();
        let disk = self.storage.bpm.disk_manager();
        (0..disk.next_page_id())
            .filter(|&page_id| disk.has_page(page_id))
            .collect()
    }

    fn reachable(&self) -> BTreeSet<PageId> {
        let mut pages: BTreeSet<_> = self.table().heap.page_ids().unwrap().into_iter().collect();
        for index in self.catalog.table_indexes("t") {
            let Index::BtreeIndex(btree) = &*index.index;
            pages.extend(btree.tree().page_ids().unwrap());
        }
        let sequence = self.catalog.get_table("t_id_seq").unwrap();
        pages.extend(sequence.heap.page_ids().unwrap());
        pages
    }

    fn rows(&self) -> BTreeMap<i64, (i32, Rid)> {
        let table = self.table();
        let txn = self.tm.begin();
        let mut scan = txn.scan(&table.heap, &table.schema).unwrap();
        let mut rows = BTreeMap::new();
        while let Some(batch) = scan.next_batch().unwrap() {
            for (_, tuple) in batch {
                let values = tuple.decode(&table.schema).unwrap();
                let [Value::BigInt(id), Value::Int(v), _] = &values[..] else {
                    panic!("bad row {values:?}");
                };
                rows.insert(*id, (*v, tuple.rid().unwrap()));
            }
        }
        self.tm.commit(&txn).unwrap();
        rows
    }
}

fn committed_in(fs: &CrashFs, tid: TxnId) -> bool {
    let log = Log::open(Arc::new(fs.with_removed()), LOG, SEGMENT).unwrap();
    let mut scan = log.scan(FIRST_LSN);
    let mut committed = false;
    while let Some(record) = scan.next_record().unwrap() {
        committed |= record.tid == tid && record.kind == Kind::Commit;
    }
    committed
}

struct Expect {
    rows: usize,
    added: Vec<i32>,
    system: BTreeSet<PageId>,
}

fn recover_and_check(image: &CrashFs, tid: TxnId, expect: &Expect, at: &str) -> bool {
    let dropped = committed_in(image, tid);
    let db = Db::open(image);
    let rows = db.rows();
    let added = if dropped { expect.added.len() } else { 0 };
    assert_eq!(rows.len(), expect.rows + added, "{at}: rows");
    let pk = db.primary_key();
    for (&id, &(_, rid)) in &rows {
        let mut found = Vec::new();
        pk.index
            .scan_key(&key(&pk, Value::BigInt(id)), &mut found)
            .unwrap();
        assert!(found.contains(&RowLocator::Rid(rid)), "{at}: pk of {id}");
    }
    let by_v = db.catalog.get_index("t_v");
    assert_eq!(by_v.is_none(), dropped, "{at}: t_v");
    if let Some(by_v) = by_v {
        for (&id, &(v, _)) in &rows {
            let mut found = Vec::new();
            by_v.index
                .scan_key(&key(&by_v, Value::Int(v)), &mut found)
                .unwrap();
            let pk_key = key(&pk, Value::BigInt(id));
            assert!(
                found.contains(&RowLocator::PrimaryKey(pk_key)),
                "{at}: t_v of {id}"
            );
        }
        let Index::BtreeIndex(btree) = &*by_v.index;
        let entries = btree.tree().begin().unwrap().count();
        assert_eq!(entries, rows.len(), "{at}: t_v entries");
    }
    let mut expected = expect.system.clone();
    expected.extend(db.reachable());
    assert_eq!(db.allocated(), expected, "{at}: allocated pages");
    dropped
}

fn crash_points(fs: &CrashFs, from: usize, to: usize) -> impl Iterator<Item = (CrashFs, String)> {
    (from..=to).flat_map(move |n| {
        [Lost::Nothing, Lost::Everything, Lost::Some(n as u64)]
            .into_iter()
            .map(move |lost| {
                (
                    fs.crash_after(n, lost),
                    format!("crash after event {n}, losing {lost:?}"),
                )
            })
    })
}

#[test]
fn a_crash_anywhere_in_a_drop_keeps_the_index_unless_the_drop_committed() {
    let fs = CrashFs::new();
    let db = Db::create(&fs);
    let system: BTreeSet<_> = db
        .allocated()
        .difference(&db.reachable())
        .copied()
        .collect();
    let rows = db.rows().len();
    let start = fs.num_events();
    let header = db.header();

    let reader = db.tm.begin();
    let txn = db.tm.begin();
    let added: Vec<i32> = (1000..1040).collect();
    for &v in &added {
        db.insert(&txn, v);
    }
    db.drop_index(&txn);
    db.tm.commit(&txn).unwrap();
    let disk = Arc::clone(db.storage.bpm.disk_manager());
    db.storage.bpm.flush_all_pages().unwrap();
    assert!(disk.has_page(header), "the tree went while a reader ran");
    db.tm.abort(&reader).unwrap();
    db.tm.wait_for_gc();
    assert!(!disk.has_page(header), "the tree is still there");
    db.storage.bpm.log().flush_all().unwrap();
    let end = fs.num_events();

    let expect = Expect {
        rows,
        added,
        system,
    };
    let (mut kept, mut dropped) = (0, 0);
    for (image, at) in crash_points(&fs, start, end) {
        match recover_and_check(&image, txn.tid(), &expect, &at) {
            true => dropped += 1,
            false => kept += 1,
        }
    }
    assert!(
        kept > 10 && dropped > 10,
        "{kept} crashes kept t_v, {dropped} dropped it"
    );
}

#[test]
fn a_crash_during_a_drops_rollback_keeps_the_index() {
    let fs = CrashFs::new();
    let db = Db::create(&fs);
    let system: BTreeSet<_> = db
        .allocated()
        .difference(&db.reachable())
        .copied()
        .collect();
    let rows = db.rows().len();
    let txn = db.tm.begin();
    for v in 1000..1040 {
        db.insert(&txn, v);
    }
    db.drop_index(&txn);
    db.storage.bpm.log().flush_all().unwrap();
    let start = fs.num_events();
    db.tm.abort(&txn).unwrap();
    db.storage.bpm.log().flush_all().unwrap();
    let end = fs.num_events();
    assert!(
        end - start > 2,
        "the rollback wrote only {} events",
        end - start
    );

    let expect = Expect {
        rows,
        added: Vec::new(),
        system,
    };
    for (image, at) in crash_points(&fs, start, end) {
        assert!(!recover_and_check(&image, txn.tid(), &expect, &at));
    }
}
