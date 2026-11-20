//! The sweep that opening a database runs after recovery and the catalog
//! (`Catalog::sweep`). Garbage collection queues its work in memory, and
//! some garbage never joins the queue, so a crash or a restart leaves
//! garbage behind. After the sweep none is left: no deleted tuple holds
//! space, each index holds exactly one entry per live row, and new rows take
//! the space. A crash during the sweep leaves nothing the next one does not
//! finish.

#[path = "../../storage/tests/util/crash_fs.rs"]
mod crash_fs;

use std::collections::BTreeMap;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;

use catalog::header_page::{CATALOG_PAGE_ID, SystemTableRoots};
use catalog::{Catalog, IndexInfo, SWEEP_BATCH, Swept, TableInfo};
use common::{Column, DataType, Rid, Schema, Tuple, UniqueKey, Value};
use crash_fs::{CrashFs, Lost};
use planner::{ExecutionEngine, Output};
use storage::Storage;
use storage::buffer::arc_replacer::AccessType;
use storage::index::{Index, IndexTrait, LocatorKind, RowLocator};
use storage::table::table_heap::TableHeap;
use storage::table::table_page::TablePage;
use storage::wal::SEGMENT_SIZE;
use storage::wal::log::Log;

const DB: &str = "/db/test.db";
const LOG: &str = "/db/test.log";

/// Few, so the sweep's changes reach the disk while it runs.
const FRAMES: usize = 16;

/// How long the rows of `t` are: each takes a page of its own, so the row
/// a test inserts last fills the heap's last page.
const PAD: usize = 1900;

struct Db {
    storage: Storage,
    engine: ExecutionEngine,
}

impl Db {
    /// Opens the database in `fs`, recovering it, without sweeping it.
    fn open(fs: &CrashFs) -> Self {
        Self::with_frames(fs, FRAMES)
    }

    fn with_frames(fs: &CrashFs, frames: usize) -> Self {
        let storage = storage::open_with(Arc::new(fs.clone()), DB, frames, None, SEGMENT_SIZE)
            .unwrap_or_else(|e| panic!("recovery failed: {e}"));
        let catalog = Arc::new(Catalog::open(Arc::clone(&storage.bpm)).unwrap());
        Self {
            storage,
            engine: ExecutionEngine::new(catalog),
        }
    }

    /// Sweeps the database, as opening it for the server does.
    fn sweep(&self) -> Swept {
        self.sweep_in_batches_of(SWEEP_BATCH)
    }

    fn sweep_in_batches_of(&self, batch: usize) -> Swept {
        let catalog = self.engine.catalog();
        catalog
            .sweep(self.engine.transaction_manager(), batch)
            .unwrap()
    }

    /// Shuts down as the server does, so the next open recovers nothing.
    fn close(self) {
        let catalog = Arc::clone(self.engine.catalog());
        drop(self.engine);
        catalog.save_sequences().unwrap();
        self.storage.close().unwrap();
    }

    fn ok(&mut self, sql: &str) -> Output {
        run(&mut self.engine, sql)
    }

    fn rows(&mut self, sql: &str) -> Vec<Vec<Value>> {
        match self.ok(sql) {
            Output::Rows { rows, .. } => rows,
            output => panic!("{sql}: {output:?}"),
        }
    }

    fn table(&self, name: &str) -> Arc<TableInfo> {
        self.engine.catalog().get_table(name).unwrap()
    }
}

fn run(session: &mut ExecutionEngine, sql: &str) -> Output {
    let mut outputs = session
        .execute_sql(sql)
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    outputs.pop().unwrap()
}

/// A new database with `t (id INT PRIMARY KEY, k INT, pad VARCHAR(2000))`,
/// indexed on `k` too, holding a row for each of `ids`.
fn with_t(fs: &CrashFs, ids: Range<i32>) -> Db {
    let mut db = Db::open(fs);
    db.ok("CREATE TABLE t (id INT PRIMARY KEY, k INT, pad VARCHAR(2000))");
    db.ok("CREATE INDEX t_k ON t (k)");
    insert_t(&mut db.engine, ids);
    db
}

/// Inserts a row of `t` for each of `ids`, in one statement.
fn insert_t(session: &mut ExecutionEngine, ids: Range<i32>) {
    let pad = "x".repeat(PAD);
    let rows: Vec<String> = ids
        .map(|id| format!("({id}, {}, '{pad}')", id % 10))
        .collect();
    run(
        session,
        &format!("INSERT INTO t VALUES {}", rows.join(", ")),
    );
}

/// Inserts a row into `t` and returns whether the heap took it without
/// growing: into the space the sweep reclaimed on its last page.
fn takes_a_row_in_place(db: &mut Db, id: i32) -> bool {
    let heap = Arc::clone(&db.table("t").heap);
    let pages = heap.page_ids().unwrap().len();
    insert_t(&mut db.engine, id..id + 1);
    heap.page_ids().unwrap().len() == pages
}

/// Every heap of the database, by name: the system tables and the tables.
fn heaps(db: &Db) -> Vec<(String, Arc<TableHeap>)> {
    let bpm = &db.storage.bpm;
    let roots = {
        let guard = bpm.checked_read_page(CATALOG_PAGE_ID, AccessType::Unknown);
        SystemTableRoots::read_from(guard.unwrap().get_data()).unwrap()
    };
    let system = [
        ("tables", roots.tables),
        ("columns", roots.columns),
        ("indexes", roots.indexes),
        ("index_columns", roots.index_columns),
    ];
    let mut heaps: Vec<_> = system
        .into_iter()
        .map(|(name, first)| {
            let heap = TableHeap::open(Arc::clone(bpm), first).unwrap();
            (name.to_owned(), Arc::new(heap))
        })
        .collect();
    let tables = db.engine.catalog().tables().into_iter();
    heaps.extend(tables.map(|table| (table.name.clone(), Arc::clone(&table.heap))));
    heaps
}

/// The tuples on the pages of `heap`, deleted ones too, as their meta and
/// payload length.
fn tuples(db: &Db, heap: &TableHeap) -> Vec<(bool, usize)> {
    let mut tuples = Vec::new();
    for page_id in heap.page_ids().unwrap() {
        let guard = db
            .storage
            .bpm
            .checked_read_page(page_id, AccessType::Unknown);
        let guard = guard.unwrap();
        let page = TablePage::new(guard.get_data()).unwrap();
        tuples.extend(
            page.iter()
                .map(|(_, meta, payload)| (meta.is_deleted, payload.len())),
        );
    }
    tuples
}

/// How many deleted tuples still hold space in each heap that has any.
fn garbage(db: &Db) -> BTreeMap<String, usize> {
    let counts = heaps(db).into_iter().map(|(name, heap)| {
        let deleted = tuples(db, &heap)
            .iter()
            .filter(|(deleted, _)| *deleted)
            .count();
        (name, deleted)
    });
    counts.filter(|&(_, deleted)| deleted > 0).collect()
}

/// The payload bytes the tuples of the system tables hold.
fn catalog_bytes(db: &Db) -> usize {
    let system = heaps(db).into_iter().take(4);
    let bytes = system
        .flat_map(|(_, heap)| tuples(db, &heap))
        .map(|(_, len)| len);
    bytes.sum()
}

/// The rows of `table` a new transaction sees, with their rids.
fn visible(db: &Db, table: &TableInfo) -> Vec<(Rid, Tuple)> {
    let tm = db.engine.transaction_manager();
    let txn = tm.begin();
    let mut scan = txn.scan(&table.heap, &table.schema).unwrap();
    let mut rows = Vec::new();
    while let Some(batch) = scan.next_batch().unwrap() {
        rows.extend(
            batch
                .into_iter()
                .map(|(_, tuple)| (tuple.rid().unwrap(), tuple)),
        );
    }
    tm.commit(&txn).unwrap();
    rows
}

fn key_of(table: &TableInfo, index: &IndexInfo, tuple: &Tuple) -> Tuple {
    tuple
        .to_key(&table.schema, &index.key_schema, &index.key_columns)
        .unwrap()
}

/// What an entry of `index`, one of `table`'s, for the row `tuple` at
/// `rid` leads to.
fn locator_of(
    db: &Db,
    table: &TableInfo,
    index: &IndexInfo,
    rid: Rid,
    tuple: &Tuple,
) -> RowLocator {
    match index.locator {
        LocatorKind::Rid => RowLocator::Rid(rid),
        LocatorKind::PrimaryKey => {
            let pk = db.engine.catalog().primary_key_index(table.oid).unwrap();
            RowLocator::PrimaryKey(key_of(table, &pk, tuple))
        }
    }
}

/// The entries index `name` holds.
fn entries(db: &Db, name: &str) -> usize {
    let index = db.engine.catalog().get_index(name).unwrap();
    let Index::BtreeIndex(btree) = &*index.index;
    btree.tree().begin().unwrap().count()
}

/// The pages of index `name`'s tree.
fn index_pages(db: &Db, name: &str) -> usize {
    let index = db.engine.catalog().get_index(name).unwrap();
    let Index::BtreeIndex(btree) = &*index.index;
    btree.tree().page_ids().unwrap().len()
}

/// Checks that every index holds an entry for each row a new transaction
/// sees, and no other entry.
fn assert_indexes_match_rows(db: &Db, at: &str) {
    let catalog = db.engine.catalog();
    for table in catalog.tables() {
        let rows = visible(db, &table);
        for index in catalog.table_indexes(&table.name) {
            assert_eq!(
                entries(db, &index.name),
                rows.len(),
                "{at}: entries of {} for {} rows",
                index.name,
                rows.len()
            );
            for (rid, tuple) in &rows {
                let mut found = Vec::new();
                index
                    .index
                    .scan_key(&key_of(&table, &index, tuple), &mut found)
                    .unwrap();
                assert!(
                    found.contains(&locator_of(db, &table, &index, *rid, tuple)),
                    "{at}: {} has no entry for the row at {rid}",
                    index.name
                );
            }
        }
    }
}

fn int(value: &Value) -> i64 {
    match *value {
        Value::Int(n) => n.into(),
        ref other => panic!("{other:?} is not an INT"),
    }
}

/// The only garbage is `deleted` tuples in `t`.
fn only_in_t(deleted: usize) -> BTreeMap<String, usize> {
    BTreeMap::from([("t".to_owned(), deleted)])
}

/// A rolled-back INSERT leaves its rows deleted for garbage collection,
/// which waits for the older block. The crash loses that queue.
#[test]
fn an_aborted_insert_is_reclaimed_after_a_crash() {
    let fs = CrashFs::new();
    let mut db = with_t(&fs, 1..4);
    let mut older = db.engine.session();
    run(&mut older, "BEGIN");
    db.ok("BEGIN");
    insert_t(&mut db.engine, 10..15);
    db.ok("ROLLBACK");
    // An abort syncs nothing; the next commit would.
    db.storage.bpm.log().flush_all().unwrap();
    let image = fs.crash_now();
    drop((older, db));

    let mut db = Db::open(&image);
    assert_eq!(garbage(&db), only_in_t(5), "before the sweep");
    assert_eq!(db.sweep().tuples, 5);
    assert!(garbage(&db).is_empty(), "{:?}", garbage(&db));
    assert_indexes_match_rows(&db, "after the sweep");
    assert!(takes_a_row_in_place(&mut db, 20));
}

/// Recovery undoes a block the crash cut off, leaving its rows deleted,
/// and nothing queues them.
#[test]
fn a_loser_transactions_rows_are_reclaimed_after_a_crash() {
    let fs = CrashFs::new();
    let db = with_t(&fs, 1..4);
    let mut loser = db.engine.session();
    run(&mut loser, "BEGIN");
    insert_t(&mut loser, 10..15);
    db.storage.bpm.log().flush_all().unwrap();
    let image = fs.crash_now();
    drop((loser, db));

    let mut db = Db::open(&image);
    assert_eq!(garbage(&db), only_in_t(5), "before the sweep");
    db.sweep();
    assert!(garbage(&db).is_empty(), "{:?}", garbage(&db));
    assert_indexes_match_rows(&db, "after the sweep");
    assert_eq!(db.rows("SELECT count(*) FROM t"), [[Value::BigInt(3)]]);
    assert!(takes_a_row_in_place(&mut db, 20));
}

/// A create writes its catalog rows as its transaction's changes, outside
/// the write set, so an abort leaves them deleted and nothing ever queues
/// them, not even for a clean shutdown. The live rows beside them stay.
#[test]
fn an_aborted_creates_catalog_rows_are_reclaimed_after_a_restart() {
    let fs = CrashFs::new();
    let mut db = Db::open(&fs);
    db.ok("CREATE TABLE kept (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, v INT)");
    let before = catalog_bytes(&db);
    let (catalog, tm) = (db.engine.catalog(), db.engine.transaction_manager());
    let schema = Schema::new(vec![
        Column::new("id", DataType::Int, false),
        Column::new("name", DataType::Varchar(16), true),
    ]);
    let unique = UniqueKey {
        name: None,
        columns: vec!["name".to_owned()],
        nulls_distinct: true,
    };
    let txn = tm.begin();
    catalog
        .create_table_with_constraints(&txn, "gone", schema, &["id"], &["id"], &[unique])
        .unwrap();
    tm.abort(&txn).unwrap();
    drop(txn);
    db.close();

    let mut db = Db::open(&fs);
    let left: Vec<String> = garbage(&db).into_keys().collect();
    assert_eq!(left, ["columns", "index_columns", "indexes", "tables"]);
    db.sweep();
    assert!(garbage(&db).is_empty(), "{:?}", garbage(&db));
    assert_eq!(
        catalog_bytes(&db),
        before,
        "bytes the catalog's tuples hold"
    );
    db.ok("CREATE TABLE gone (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, v INT)");
    assert_eq!(
        db.rows("INSERT INTO gone (v) VALUES (1) RETURNING id"),
        [[Value::Int(1)]]
    );
    db.close();

    let db = Db::open(&fs);
    let tables = db.engine.catalog().tables();
    let names: Vec<&str> = tables.iter().map(|table| table.name.as_str()).collect();
    assert_eq!(names, ["gone", "kept"]);
}

/// A committed DELETE leaves its rows deleted and its entries retired until
/// the older block ends, and the crash comes first.
#[test]
fn deleted_rows_whose_collection_never_ran_are_reclaimed_after_a_crash() {
    let fs = CrashFs::new();
    let mut db = with_t(&fs, 1..9);
    let mut older = db.engine.session();
    run(&mut older, "BEGIN");
    db.ok("DELETE FROM t WHERE id > 4");
    let image = fs.crash_now();
    drop((older, db));

    let mut db = Db::open(&image);
    assert_eq!(garbage(&db), only_in_t(4), "before the sweep");
    assert_eq!(entries(&db, "t_k"), 8, "before the sweep");
    let swept = db.sweep();
    assert_eq!(
        swept,
        Swept {
            tuples: 4,
            entries: 8
        }
    );
    assert!(garbage(&db).is_empty(), "{:?}", garbage(&db));
    assert_indexes_match_rows(&db, "after the sweep");
    assert!(takes_a_row_in_place(&mut db, 20));
}

/// Entries that updates and deletes retired, through an index storing the
/// primary key and one storing rids, outlive their rows' keys after the
/// crash, and only the live rows' entries are left after the sweep.
#[test]
fn index_entries_no_live_row_needs_are_removed_after_a_crash() {
    let fs = CrashFs::new();
    let mut db = with_t(&fs, 1..9);
    db.ok("CREATE TABLE log (n INT, note VARCHAR(16))");
    db.ok("CREATE INDEX log_n ON log (n)");
    let notes: Vec<String> = (0..600).map(|n| format!("({n}, 'note {n}')")).collect();
    db.ok(&format!("INSERT INTO log VALUES {}", notes.join(", ")));
    let mut older = db.engine.session();
    run(&mut older, "BEGIN");
    db.ok("UPDATE t SET k = k + 100 WHERE id % 2 = 0");
    db.ok("UPDATE t SET id = id + 100 WHERE id = 1");
    db.ok("DELETE FROM log WHERE n >= 100");
    let image = fs.crash_now();
    drop((older, db));

    let mut db = Db::open(&image);
    assert_eq!(entries(&db, "t_pkey"), 9, "before the sweep");
    assert_eq!(entries(&db, "t_k"), 13, "before the sweep");
    assert_eq!(entries(&db, "log_n"), 600, "before the sweep");
    let pages = index_pages(&db, "log_n");
    db.sweep();
    assert_indexes_match_rows(&db, "after the sweep");
    assert!(garbage(&db).is_empty(), "{:?}", garbage(&db));
    assert!(
        index_pages(&db, "log_n") < pages,
        "log_n still has {pages} pages"
    );
    let seen = db.rows("SELECT k, id FROM t");
    for k in [1, 102, 104, 3] {
        let mut want: Vec<Value> = (seen.iter())
            .filter(|row| row[0] == Value::Int(k))
            .map(|row| row[1].clone())
            .collect();
        want.sort_by_key(|id| format!("{id:?}"));
        let found = db.rows(&format!("SELECT id FROM t WHERE k = {k} ORDER BY id"));
        assert_eq!(found.concat(), want, "rows with k = {k}");
    }
}

/// A sequence's row is rewritten in place and never deleted, so the sweep
/// leaves it: ids go on exactly after a clean restart, and past every id a
/// row holds after a crash.
#[test]
fn identity_values_go_on_after_the_sweep() {
    let fs = CrashFs::new();
    let mut db = Db::open(&fs);
    db.ok("CREATE TABLE s (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, v INT)");
    db.ok("INSERT INTO s (v) VALUES (1), (2), (3)");
    db.close();

    let mut db = Db::open(&fs);
    db.sweep();
    let insert = "INSERT INTO s (v) VALUES (0) RETURNING id";
    assert_eq!(db.rows(insert), [[Value::Int(4)]]);
    let image = fs.crash_now();
    drop(db);

    let mut db = Db::open(&image);
    db.sweep();
    let id = int(&db.rows(insert)[0][0]);
    assert!(id > 4, "id {id} again");
    assert_eq!(db.rows("SELECT count(*) FROM s"), [[Value::BigInt(5)]]);
    db.rows("SELECT last_value FROM s_id_seq");
}

/// Garbage of every kind, left by a crash: a DELETE and UPDATEs that
/// committed while an older block kept garbage collection off them, an
/// INSERT and a create rolled back, and a block cut off by the crash, in
/// tables whose indexes store primary keys and rids, next to a sequence.
fn crash_with_garbage(fs: &CrashFs) -> CrashFs {
    let mut db = with_t(fs, 1..31);
    db.ok("CREATE TABLE log (n INT, note VARCHAR(16))");
    db.ok("CREATE INDEX log_n ON log (n)");
    let notes: Vec<String> = (0..300).map(|n| format!("({n}, 'note {n}')")).collect();
    db.ok(&format!("INSERT INTO log VALUES {}", notes.join(", ")));
    db.ok("CREATE TABLE s (id INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, v INT)");
    db.ok("INSERT INTO s (v) VALUES (1), (2), (3)");

    let mut older = db.engine.session();
    run(&mut older, "BEGIN");
    db.ok("DELETE FROM t WHERE id % 3 = 0");
    db.ok("UPDATE t SET k = k + 100 WHERE id % 4 = 0");
    db.ok("DELETE FROM log WHERE n % 2 = 0");
    db.ok("UPDATE log SET n = n + 1000 WHERE n % 5 = 1");
    db.ok("DELETE FROM s WHERE v = 2");
    db.ok("BEGIN");
    insert_t(&mut db.engine, 100..104);
    db.ok("ROLLBACK");
    let (catalog, tm) = (db.engine.catalog(), db.engine.transaction_manager());
    let txn = tm.begin();
    let schema = Schema::new(vec![Column::new("id", DataType::BigInt, false)]);
    catalog
        .create_table_with_constraints(&txn, "gone", schema, &["id"], &["id"], &[])
        .unwrap();
    tm.abort(&txn).unwrap();
    let mut loser = db.engine.session();
    run(&mut loser, "BEGIN");
    insert_t(&mut loser, 200..204);
    run(&mut loser, "INSERT INTO s (v) VALUES (9)");
    db.storage.bpm.log().flush_all().unwrap();
    let image = fs.crash_now();
    drop((loser, older, db));
    image
}

/// The rows of every table, in order.
fn all_rows(db: &mut Db) -> Vec<Vec<Value>> {
    [
        "SELECT id, k FROM t ORDER BY id",
        "SELECT n, note FROM log ORDER BY n, note",
        "SELECT id, v FROM s ORDER BY id",
    ]
    .iter()
    .flat_map(|sql| db.rows(sql))
    .collect()
}

/// Checks the database after its sweep: the rows of `want`, no garbage,
/// indexes that match the rows, and a sequence that hands out new ids.
fn check_swept(db: &mut Db, want: &[Vec<Value>], at: &str) {
    assert_eq!(all_rows(db), want, "{at}: rows");
    let garbage = garbage(db);
    assert!(garbage.is_empty(), "{at}: garbage left {garbage:?}");
    assert_indexes_match_rows(db, at);
    let ids = db.rows("SELECT id FROM s");
    let max = ids.iter().map(|row| int(&row[0])).max().unwrap();
    let new = int(&db.rows("INSERT INTO s (v) VALUES (0) RETURNING id")[0][0]);
    assert!(new > max, "{at}: id {new} again, after {max}");
}

/// A sweep that holds a few entries at a time resumes each index's scan
/// after the last entry it held, so it collects what one holding a whole
/// index at once does.
#[test]
fn a_sweep_in_small_batches_collects_what_one_whole_batch_does() {
    let image = crash_with_garbage(&CrashFs::new());
    let whole = image.crash_now();
    let mut db = Db::open(&whole);
    let want = all_rows(&mut db);
    assert!(entries(&db, "log_n") < SWEEP_BATCH);
    let swept = db.sweep();
    drop(db);

    let db = Db::open(&image);
    assert!(entries(&db, "t_k") > 10 && entries(&db, "log_n") > 100);
    assert_eq!(db.sweep_in_batches_of(3), swept);
    assert_eq!(
        db.sweep_in_batches_of(3),
        Swept::default(),
        "a second sweep"
    );
    let mut db = db;
    check_swept(&mut db, &want, "after a sweep in batches of 3");
}

#[test]
fn a_crash_during_the_sweep_leaves_nothing_the_next_one_does_not_finish() {
    let image = crash_with_garbage(&CrashFs::new());
    let mut db = Db::open(&image);
    let want = all_rows(&mut db);
    let start = image.num_events();
    let swept = db.sweep();
    let end = image.num_events();
    drop(db);
    assert!(swept.tuples >= 25 && swept.entries >= 150, "{swept:?}");
    assert!(end - start >= 30, "the sweep wrote {} times", end - start);

    let mut crashes = 0;
    image.for_each_moment(|n, moment| {
        if !(start..=end).contains(&n) {
            return;
        }
        for lost in [Lost::Nothing, Lost::Everything, Lost::Some(n as u64)] {
            let at = format!("crash after event {n}, losing {lost:?}");
            let mut db = Db::open(&moment.crash(lost));
            db.sweep();
            check_swept(&mut db, &want, &at);
            crashes += 1;
        }
    });
    assert!(crashes >= 90, "only {crashes} crash points");
}

/// With a frame for every page, the sweep writes nothing but log, so
/// cutting the log after any of its records stops the sweep there: a
/// table's tuples part reclaimed, its entries not removed yet, a tree part
/// rebalanced.
#[test]
fn every_prefix_of_the_sweeps_log_is_finished_by_the_next_sweep() {
    let image = crash_with_garbage(&CrashFs::new());
    let mut db = Db::with_frames(&image, 1024);
    let want = all_rows(&mut db);
    let from = db.storage.bpm.log().end();
    db.sweep();
    db.storage.bpm.log().flush_all().unwrap();
    drop(db);
    let segment = PathBuf::from(format!("{LOG}.{:08}", 1));
    assert_eq!(
        image.files_in(segment.parent().unwrap()).len(),
        3,
        "one segment"
    );
    let bytes = image.contents(&segment);
    let log = Log::open(Arc::new(image.crash_now()), LOG, SEGMENT_SIZE).unwrap();
    let mut cuts = vec![log.end()];
    let mut scan = log.scan(from);
    while let Some(record) = scan.next_record().unwrap() {
        cuts.push(record.lsn);
    }
    assert!(cuts.len() > 300, "{} records", cuts.len());
    for cut in cuts {
        let at = format!("log cut at {cut}");
        let mut db = Db::with_frames(&image.with_contents(&segment, &bytes[..cut as usize]), 1024);
        db.sweep();
        check_swept(&mut db, &want, &at);
    }
}
