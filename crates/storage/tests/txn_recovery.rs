//! Crash recovery of transactions. Whatever moment a crash comes at, and
//! whatever unsynced writes it loses or tears, the database that opens
//! again holds exactly the transactions whose `Commit` reached the log,
//! every acknowledged commit among them; no tuple stays locked; and each
//! index has an entry for every row, and none for an insert undone.

#[path = "util/crash_fs.rs"]
mod crash_fs;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use common::{
    Column, DataType, INVALID_TXN_ID, IndexKind, PageId, Rid, Schema, TXN_UNCOMMITTED_BIT, Tuple,
    TxnId, Value,
};
use crash_fs::{CrashFs, Lost, next};
use storage::Storage;
use storage::buffer::arc_replacer::AccessType;
use storage::concurrency::index_versions::IndexedTable;
use storage::concurrency::transaction::Transaction;
use storage::concurrency::transaction_manager::TransactionManager;
use storage::index::btree_index::BtreeIndex;
use storage::index::{Index, IndexMetadata, IndexTrait, LocatorKind, RowLocator};
use storage::table::table_heap::TableHeap;
use storage::table::table_page::TablePage;
use storage::wal::checkpoint;
use storage::wal::log::{FIRST_LSN, Log};
use storage::wal::record::Kind;

const DB: &str = "/db/test.db";
const LOG: &str = "/db/test.log";

/// Small, so the workloads start and remove segments.
const SEGMENT: u64 = 32 << 10;

/// Rows have ids below this, so ids are deleted and inserted again.
const IDS: i32 = 40;

/// `(id INT NOT NULL, v INT, pad VARCHAR(400))`. Pads of many lengths make
/// updates grow and shrink, move rows off full pages, and pad shrinking
/// ones while they are locked.
fn schema() -> Schema {
    Schema::new(vec![
        Column::new("id", DataType::Int, false),
        Column::new("v", DataType::Int, true),
        Column::new("pad", DataType::Varchar(400), true),
    ])
}

fn key_schema(column: usize) -> Schema {
    Schema::new(vec![schema().columns()[column].clone()])
}

fn id_key(id: i32) -> Tuple {
    Tuple::encode(&key_schema(0), &[Value::Int(id)]).unwrap()
}

fn v_key(v: i32) -> Tuple {
    Tuple::encode(&key_schema(1), &[Value::Int(v)]).unwrap()
}

fn encode(id: i32, (v, pad): (i32, usize)) -> Tuple {
    let pad = "p".repeat(pad);
    Tuple::encode(
        &schema(),
        &[Value::Int(id), Value::Int(v), Value::from(pad)],
    )
    .unwrap()
}

/// A row's value and pad length.
type Row = (i32, usize);

/// What a committed transaction left of each row it wrote: the row, or
/// nothing for a delete.
type Effects = BTreeMap<i32, Option<Row>>;

/// Where a database's heap and indexes start.
#[derive(Clone, Copy)]
struct Layout {
    heap: PageId,
    pk: PageId,
    by_v: PageId,
}

/// A table with a primary key index on `id`, storing rids, and an index on
/// `v`, storing primary keys.
struct Db {
    storage: Storage,
    tm: TransactionManager,
    table: Arc<IndexedTable>,
    pk: Arc<Index>,
    by_v: Arc<Index>,
}

fn index(db: &Storage, name: &str, column: usize, kind: IndexKind, header: PageId) -> Arc<Index> {
    let locator = match kind {
        IndexKind::PrimaryKey => LocatorKind::Rid,
        _ => LocatorKind::PrimaryKey,
    };
    let key = Arc::new(key_schema(column));
    let metadata = IndexMetadata::new(name, "t", vec![column], key, kind, locator);
    let index = BtreeIndex::new(metadata, header, Arc::clone(&db.bpm)).unwrap();
    Arc::new(Index::from(index))
}

impl Db {
    fn create(fs: &CrashFs, frames: usize, segment: u64) -> (Self, Layout) {
        let storage = storage::open_with(Arc::new(fs.clone()), DB, frames, None, segment).unwrap();
        let (pk, by_v) = (storage.bpm.new_page(), storage.bpm.new_page());
        let heap = TableHeap::create(Arc::clone(&storage.bpm)).unwrap();
        let layout = Layout {
            heap: heap.first_page_id(),
            pk,
            by_v,
        };
        drop(heap);
        (Self::open_on(storage, layout, 1), layout)
    }

    /// Opens the database in `fs`, recovering it, with Tids from `next_tid`.
    fn open(fs: &CrashFs, frames: usize, layout: Layout, next_tid: TxnId) -> Self {
        let storage = storage::open_with(Arc::new(fs.clone()), DB, frames, None, SEGMENT)
            .unwrap_or_else(|e| panic!("recovery failed: {e}"));
        Self::open_on(storage, layout, next_tid)
    }

    fn open_on(storage: Storage, layout: Layout, next_tid: TxnId) -> Self {
        let pk = index(&storage, "t_pkey", 0, IndexKind::PrimaryKey, layout.pk);
        let by_v = index(&storage, "t_v", 1, IndexKind::Plain, layout.by_v);
        let heap = TableHeap::open(Arc::clone(&storage.bpm), layout.heap).unwrap();
        let table = IndexedTable::new(Arc::new(heap), schema(), Some(Arc::clone(&pk)));
        let tm = TransactionManager::new(Arc::clone(&storage.bpm), next_tid);
        Self {
            storage,
            tm,
            table: Arc::new(table),
            pk,
            by_v,
        }
    }
}

/// A transaction the workload has open, with the rows it wrote as it sees
/// them: each row's value, pad length and rid, or `None` once deleted.
struct Open {
    txn: Arc<Transaction>,
    rows: BTreeMap<i32, Option<(Row, Rid)>>,
}

/// Transactions that insert, update and delete rows of `Db`, interleaved,
/// on one thread. They commit, abort, or are still open at the end, and a
/// pool too small for the table writes pages before their changes commit.
struct Workload {
    fs: CrashFs,
    db: Db,
    layout: Layout,
    /// The fs events and the log's end once the table was durable: a crash
    /// before then has no table to check.
    setup: (usize, u64),
    rng: u64,
    committed: BTreeMap<i32, (Row, Rid)>,
    open: Vec<Open>,
    expect: Expect,
}

/// What the workload did, which decides what a crash must leave.
#[derive(Clone, Default)]
struct Expect {
    /// What each committed transaction left, by Tid.
    effects: HashMap<TxnId, Effects>,
    /// Each commit, with the number of fs events when it returned.
    acked: Vec<(TxnId, usize)>,
}

impl Workload {
    fn new(fs: &CrashFs, frames: usize, segment: u64, seed: u64) -> Self {
        let (db, layout) = Db::create(fs, frames, segment);
        db.storage.bpm.checkpoint().unwrap();
        let setup = (fs.num_events(), db.storage.bpm.log().end());
        Self {
            fs: fs.clone(),
            db,
            layout,
            setup,
            rng: seed,
            committed: BTreeMap::new(),
            open: Vec::new(),
            expect: Expect::default(),
        }
    }

    fn below(&mut self, n: usize) -> usize {
        next(&mut self.rng) as usize % n.max(1)
    }

    fn run(&mut self, steps: usize, maintenance: bool) {
        for _ in 0..steps {
            match self.below(20) {
                _ if self.open.is_empty() => self.begin(),
                0..=2 if self.open.len() < 3 => self.begin(),
                0..=11 => self.write(),
                12..=14 => {
                    let at = self.below(self.open.len());
                    self.commit(at);
                }
                15 => {
                    let at = self.below(self.open.len());
                    self.abort(at);
                }
                16 if maintenance => {
                    let pages = self.db.table.heap.page_ids().unwrap();
                    let page_id = pages[self.below(pages.len())];
                    self.db.storage.bpm.flush_page(page_id).unwrap();
                }
                17 if maintenance => checkpoint::fuzzy(&self.db.storage.bpm).unwrap(),
                18 if maintenance => self.db.storage.bpm.checkpoint().unwrap(),
                _ => self.write(),
            }
        }
    }

    fn begin(&mut self) {
        let txn = self.db.tm.begin();
        self.open.push(Open {
            txn,
            rows: BTreeMap::new(),
        });
    }

    /// The row `id` as open transaction `at` sees it, with its rid.
    fn view(&self, at: usize, id: i32) -> Option<(Row, Rid)> {
        match self.open[at].rows.get(&id) {
            Some(row) => *row,
            None => self.committed.get(&id).copied(),
        }
    }

    /// Whether another open transaction wrote row `id`.
    fn held_by_other(&self, at: usize, id: i32) -> bool {
        (0..self.open.len()).any(|other| other != at && self.open[other].rows.contains_key(&id))
    }

    /// Inserts, updates or deletes a row in a random open transaction. One
    /// that conflicts, or would duplicate a key, is aborted.
    fn write(&mut self) {
        if self.open.is_empty() {
            return self.begin();
        }
        let at = self.below(self.open.len());
        let id = self.below(IDS as usize) as i32;
        if self.held_by_other(at, id) {
            return;
        }
        let row = (self.below(1000) as i32, self.below(320));
        let wrote = match (self.view(at, id), self.below(3)) {
            (None, _) => self.insert(at, id, row),
            (Some(old), 0) => self.delete(at, id, old),
            (Some(old), _) => self.update(at, id, old, row),
        };
        match wrote {
            Ok(true) => {}
            Ok(false) | Err(storage::StorageError::Conflict { .. }) => self.abort(at),
            Err(e) => panic!("writing row {id}: {e}"),
        }
    }

    fn insert(&mut self, at: usize, id: i32, row: Row) -> storage::Result<bool> {
        let Db {
            table, pk, by_v, ..
        } = &self.db;
        let txn = Arc::clone(&self.open[at].txn);
        let rid = txn.insert(&table.heap, &table.schema, &encode(id, row))?;
        self.open[at].rows.insert(id, Some((row, rid)));
        let by_rid = RowLocator::Rid(rid);
        let by_pk = RowLocator::PrimaryKey(id_key(id));
        Ok(txn.insert_entry(table, pk, &id_key(id), &by_rid, rid)?
            && txn.insert_entry(table, by_v, &v_key(row.0), &by_pk, rid)?)
    }

    fn update(&mut self, at: usize, id: i32, old: (Row, Rid), row: Row) -> storage::Result<bool> {
        let Db {
            table, pk, by_v, ..
        } = &self.db;
        let txn = Arc::clone(&self.open[at].txn);
        let (old_row, old_rid) = old;
        let rid = txn.update(&table.heap, &table.schema, old_rid, &encode(id, row))?;
        self.open[at].rows.insert(id, Some((row, rid)));
        let by_pk = RowLocator::PrimaryKey(id_key(id));
        if rid != old_rid {
            if !txn.insert_entry(table, pk, &id_key(id), &RowLocator::Rid(rid), rid)? {
                return Ok(false);
            }
            txn.retire_entry(table, pk, &id_key(id), &RowLocator::Rid(old_rid))?;
        }
        if row.0 != old_row.0 {
            if !txn.insert_entry(table, by_v, &v_key(row.0), &by_pk, rid)? {
                return Ok(false);
            }
            txn.retire_entry(table, by_v, &v_key(old_row.0), &by_pk)?;
        }
        Ok(true)
    }

    fn delete(&mut self, at: usize, id: i32, old: (Row, Rid)) -> storage::Result<bool> {
        let Db {
            table, pk, by_v, ..
        } = &self.db;
        let txn = Arc::clone(&self.open[at].txn);
        let ((v, _), rid) = old;
        txn.delete(&table.heap, rid)?;
        self.open[at].rows.insert(id, None);
        txn.retire_entry(table, pk, &id_key(id), &RowLocator::Rid(rid))?;
        txn.retire_entry(table, by_v, &v_key(v), &RowLocator::PrimaryKey(id_key(id)))?;
        Ok(true)
    }

    fn commit(&mut self, at: usize) {
        let Open { txn, rows } = self.open.remove(at);
        self.db.tm.commit(&txn).unwrap();
        // A transaction that wrote nothing logs no `Commit`.
        if !rows.is_empty() {
            self.expect.acked.push((txn.tid(), self.fs.num_events()));
        }
        let effects = rows
            .iter()
            .map(|(&id, row)| (id, row.map(|(row, _)| row)))
            .collect();
        self.expect.effects.insert(txn.tid(), effects);
        for (id, row) in rows {
            match row {
                Some(row) => self.committed.insert(id, row),
                None => self.committed.remove(&id),
            };
        }
    }

    fn abort(&mut self, at: usize) {
        let Open { txn, .. } = self.open.remove(at);
        self.db.tm.abort(&txn).unwrap();
    }

    /// Ends the workload as a crash would, leaving its open transactions
    /// running.
    fn crash(self) -> (Expect, Layout) {
        for open in self.open {
            std::mem::forget(open.txn);
        }
        (self.expect, self.layout)
    }

    /// Writes a row in open transaction `at`, for each id `pick` keeps that
    /// no other open transaction holds, giving each a long pad, until one
    /// write fails.
    fn write_rows(&mut self, at: usize, pick: impl Fn(i32) -> bool) {
        for id in (0..IDS).filter(|&id| pick(id)) {
            if self.held_by_other(at, id) {
                continue;
            }
            let row = (id + 1000, 310);
            let wrote = match self.view(at, id) {
                None => self.insert(at, id, row),
                Some(old) => self.update(at, id, old, row),
            };
            if !matches!(wrote, Ok(true)) {
                return;
            }
        }
    }
}

/// The Tids whose `Commit` is in `fs`'s log, every segment it ever had, in
/// log order, and the highest Tid of any record.
fn commits_in(fs: &CrashFs) -> (Vec<TxnId>, TxnId) {
    let log = Log::open(Arc::new(fs.with_removed()), LOG, SEGMENT).unwrap();
    let (mut commits, mut max_tid) = (Vec::new(), INVALID_TXN_ID);
    let mut scan = log.scan(FIRST_LSN);
    while let Some(record) = scan.next_record().unwrap() {
        max_tid = max_tid.max(record.tid);
        if record.kind == Kind::Commit {
            commits.push(record.tid);
        }
    }
    (commits, max_tid)
}

/// The rows the transactions committed in `commits` leave, in order.
fn rows_after(expect: &Expect, commits: &[TxnId]) -> BTreeMap<i32, Row> {
    let mut rows = BTreeMap::new();
    for tid in commits {
        for (&id, &row) in &expect.effects[tid] {
            match row {
                Some(row) => rows.insert(id, row),
                None => rows.remove(&id),
            };
        }
    }
    rows
}

/// Recovers `image`, a crash after `events` fs events, and checks it
/// against what the workload did. With `write_all`, a transaction then
/// updates every row, which fails if one is still locked.
fn recover_and_check(
    image: &CrashFs,
    events: usize,
    expect: &Expect,
    layout: Layout,
    write_all: bool,
    at: &str,
) {
    let (commits, max_tid) = commits_in(image);
    let survived: HashSet<_> = commits.iter().copied().collect();
    for &(tid, acked_at) in &expect.acked {
        assert!(
            acked_at > events || survived.contains(&tid),
            "{at}: transaction {tid}, acknowledged after event {acked_at}, is lost"
        );
    }
    let db = Db::open(image, 4, layout, max_tid + 1);
    check(&db, &rows_after(expect, &commits), at);
    if write_all {
        let txn = db.tm.begin();
        let rows = visible(&db, &txn);
        for (rid, (id, row)) in rows {
            let table = &db.table;
            txn.update(&table.heap, &table.schema, rid, &encode(id, row))
                .unwrap_or_else(|e| panic!("{at}: row {id} at {rid} is not writable: {e}"));
        }
        db.tm.commit(&txn).unwrap();
    }
}

/// The rows `txn` sees, with their rids.
fn visible(db: &Db, txn: &Arc<Transaction>) -> Vec<(Rid, (i32, Row))> {
    let mut scan = txn.scan(&db.table.heap, &db.table.schema).unwrap();
    let mut rows = Vec::new();
    while let Some(batch) = scan.next_batch().unwrap() {
        for (_, tuple) in batch {
            let values = tuple.decode(&db.table.schema).unwrap();
            let [Value::Int(id), Value::Int(v), Value::Varchar(pad)] = &values[..] else {
                panic!("bad row {values:?}");
            };
            rows.push((tuple.rid().unwrap(), (*id, (*v, pad.len()))));
        }
    }
    rows
}

/// Checks that `db` holds exactly `expected`, that no tuple is locked, that
/// each row has its index entries, and that no primary key entry is left
/// from an insert undone.
fn check(db: &Db, expected: &BTreeMap<i32, Row>, at: &str) {
    let bpm = &db.storage.bpm;
    for page_id in db.table.heap.page_ids().unwrap() {
        let guard = bpm.read_page(page_id, AccessType::Unknown);
        for (slot, meta, _) in TablePage::new(guard.get_data()).unwrap().iter() {
            assert_eq!(
                meta.ts & TXN_UNCOMMITTED_BIT,
                0,
                "{at}: tuple ({page_id}, {slot}) is still locked by {}",
                meta.ts & !TXN_UNCOMMITTED_BIT
            );
        }
    }

    let txn = db.tm.begin();
    let rows = visible(db, &txn);
    let seen: BTreeMap<i32, Row> = rows.iter().map(|&(_, row)| row).collect();
    assert_eq!(&seen, expected, "{at}: rows");
    for &(rid, (id, (v, _))) in &rows {
        let mut found = Vec::new();
        db.pk.scan_key(&id_key(id), &mut found).unwrap();
        assert!(
            found.contains(&RowLocator::Rid(rid)),
            "{at}: no key entry for row {id}"
        );
        found.clear();
        db.by_v.scan_key(&v_key(v), &mut found).unwrap();
        let by_pk = RowLocator::PrimaryKey(id_key(id));
        assert!(found.contains(&by_pk), "{at}: no v entry for row {id}");
    }
    db.tm.commit(&txn).unwrap();

    let Index::BtreeIndex(pk) = &*db.pk;
    for entry in pk.tree().begin().unwrap() {
        let (_, rid) = entry.unwrap();
        let guard = bpm.read_page(rid.page_id, AccessType::Unknown);
        let page = TablePage::new(guard.get_data()).unwrap();
        if page.is_dead(rid.slot) {
            continue;
        }
        let (meta, _) = page.get(rid.slot).unwrap();
        assert!(
            !(meta.is_deleted && meta.ts == INVALID_TXN_ID),
            "{at}: an entry of an undone insert is left at {rid}"
        );
    }
}

#[test]
fn every_crash_point_keeps_exactly_the_transactions_whose_commit_reached_the_log() {
    let fs = CrashFs::new();
    let mut workload = Workload::new(&fs, 6, SEGMENT, 0x7a1e);
    workload.run(300, true);
    let start = workload.setup.0;
    let (expect, layout) = workload.crash();

    assert!(
        expect.acked.len() >= 20,
        "only {} commits",
        expect.acked.len()
    );
    let (commits, _) = commits_in(&fs.crash_now());
    let mut crashes = 0;
    fs.for_each_moment(|n, moment| {
        if n < start {
            return;
        }
        for lost in [Lost::Nothing, Lost::Everything, Lost::Some(n as u64)] {
            let at = format!("crash after event {n}, losing {lost:?}");
            recover_and_check(&moment.crash(lost), n, &expect, layout, n % 16 == 0, &at);
            crashes += 1;
        }
    });
    assert!(crashes > 600, "only {crashes} crash points");
    assert!(commits.len() == expect.acked.len());
}

/// Recovery's own records, its stamps, compensations and ends, must bring
/// a second crash, anywhere in the first recovery, back to the same rows.
#[test]
fn a_crash_during_recovery_recovers_the_same_transactions() {
    let fs = CrashFs::new();
    let mut workload = Workload::new(&fs, 6, SEGMENT, 0x0dd5);
    workload.run(160, true);
    // Losers that changed more pages than recovery's pool holds, and a
    // commit last, whose stamps and `End` are not synced: losing them
    // leaves recovery to finish it.
    for k in 0..3 {
        workload.begin();
        let at = workload.open.len() - 1;
        workload.write_rows(at, |id| id % 3 == k);
    }
    let last = workload.open.len() - 1;
    assert!(
        !workload.open[last].rows.is_empty(),
        "the last wrote nothing"
    );
    workload.commit(last);
    let (expect, layout) = workload.crash();
    let unfinished = expect.acked.last().unwrap().0;
    let ends = |image: &CrashFs| {
        let log = Log::open(Arc::new(image.with_removed()), LOG, SEGMENT).unwrap();
        let mut scan = log.scan(FIRST_LSN);
        let mut ends = 0;
        while let Some(record) = scan.next_record().unwrap() {
            ends += usize::from(record.tid == unfinished && record.kind == Kind::End);
        }
        ends
    };
    let synced = fs.crash_after(fs.num_events(), Lost::Everything);
    assert_eq!(ends(&synced), 0, "the last commit's end is durable");

    for lost in [Lost::Everything, Lost::Some(7), Lost::Nothing] {
        let image = fs.crash_after(fs.num_events(), lost);
        let (commits, _) = commits_in(&image);
        let rows = rows_after(&expect, &commits);
        drop(Db::open(&image, 4, layout, u64::MAX >> 2));
        let recovering = image.num_events();
        assert!(recovering > 5, "recovery wrote only {recovering} events");
        image.for_each_moment(|n, moment| {
            for again in [Lost::Nothing, Lost::Everything, Lost::Some(n as u64)] {
                let crashed = moment.crash(again);
                let (_, max_tid) = commits_in(&crashed);
                let db = Db::open(&crashed, 4, layout, max_tid + 1);
                let at = format!("{lost:?}, then a crash after event {n} of {recovering} in recovery, losing {again:?}");
                check(&db, &rows, &at);
            }
        });
    }
}

/// A rollback writes pages before its compensations are durable, and a
/// crash in the middle of it must finish the rollback.
#[test]
fn a_crash_during_a_rollback_finishes_it() {
    let fs = CrashFs::new();
    let mut workload = Workload::new(&fs, 4, SEGMENT, 0xab07);
    workload.run(80, false);
    while !workload.open.is_empty() {
        workload.commit(0);
    }
    workload.begin();
    workload.write_rows(0, |_| true);
    assert_eq!(workload.open[0].rows.len(), IDS as usize, "a write failed");
    workload.db.storage.bpm.log().flush_all().unwrap();
    let before = fs.num_events();
    workload.abort(0);
    let after = fs.num_events();
    let (expect, layout) = workload.crash();

    assert!(
        after - before > 4,
        "the rollback wrote only {} events",
        after - before
    );
    for n in before..=after {
        for lost in [Lost::Nothing, Lost::Everything, Lost::Some(n as u64)] {
            let at =
                format!("crash after event {n} of a rollback {before}..{after}, losing {lost:?}");
            recover_and_check(&fs.crash_after(n, lost), n, &expect, layout, true, &at);
        }
    }
}

/// Without page writes the log is all there is, so the commits below any
/// cut are exactly the ones a recovery from it keeps.
#[test]
fn every_prefix_of_the_log_keeps_the_commits_below_the_cut() {
    let fs = CrashFs::new();
    let mut workload = Workload::new(&fs, 256, u64::MAX, 0x9f1c);
    workload.run(70, false);
    workload.db.storage.bpm.log().flush_all().unwrap();
    let durable = workload.setup.1;
    let (expect, layout) = workload.crash();

    let segment = std::path::PathBuf::from(format!("{LOG}.{:08}", 1));
    let bytes = fs.contents(&segment);
    let log = Log::open(Arc::new(fs.crash_now()), LOG, u64::MAX).unwrap();
    let mut cuts = vec![log.end()];
    let mut scan = log.scan(durable);
    while let Some(record) = scan.next_record().unwrap() {
        cuts.push(record.lsn);
    }
    assert!(cuts.len() > 100, "{} records", cuts.len());
    for cut in cuts {
        let image = fs.with_contents(&segment, &bytes[..cut as usize]);
        let (commits, max_tid) = commits_in(&image);
        let db = Db::open(&image, 64, layout, max_tid + 1);
        check(
            &db,
            &rows_after(&expect, &commits),
            &format!("log cut at {cut}"),
        );
    }
}

/// A reader that sees a commit must never lose it to a crash: the stamps
/// that make it visible come only once `Commit` is durable.
#[test]
fn a_commit_a_reader_sees_survives_a_crash() {
    let fs = CrashFs::new();
    let (db, layout) = Db::create(&fs, 16, SEGMENT);
    let txn = db.tm.begin();
    let rid = txn
        .insert(&db.table.heap, &db.table.schema, &encode(1, (0, 8)))
        .unwrap();
    db.tm.commit(&txn).unwrap();
    fs.slow_syncs(Duration::from_millis(3));

    let db = Arc::new(db);
    let done = Arc::new(AtomicBool::new(false));
    let writer = {
        let (db, done) = (Arc::clone(&db), Arc::clone(&done));
        thread::spawn(move || {
            for v in 1..=60 {
                let txn = db.tm.begin();
                let table = &db.table;
                txn.update(&table.heap, &table.schema, rid, &encode(1, (v, 8)))
                    .unwrap();
                db.tm.commit(&txn).unwrap();
            }
            done.store(true, Ordering::Release);
        })
    };
    let mut checked = 0;
    let mut last = 0;
    while !done.load(Ordering::Acquire) && !writer.is_finished() {
        // Read as of the newest commit, recording no read the writer would
        // then conflict with.
        let (table, undo) = (&db.table, db.tm.undo_log());
        let seen = table
            .heap
            .get_at(rid, TxnId::MAX >> 1, &table.schema, undo, None);
        let Ok(Some((_, tuple))) = seen else {
            continue;
        };
        let v = match tuple.decode(&db.table.schema).unwrap()[1] {
            Value::Int(v) => v,
            ref other => panic!("{other:?}"),
        };
        if v == last {
            continue;
        }
        last = v;
        let image = fs.crash_after(fs.num_events(), Lost::Everything);
        let (commits, max_tid) = commits_in(&image);
        let recovered = Db::open(&image, 16, layout, max_tid + 1);
        let txn = recovered.tm.begin();
        let rows = visible(&recovered, &txn);
        recovered.tm.commit(&txn).unwrap();
        assert!(
            rows.iter().any(|&(_, (_, (kept, _)))| kept >= v),
            "a reader saw v = {v}, but a crash then keeps {rows:?} ({} commits)",
            commits.len()
        );
        checked += 1;
    }
    writer.join().unwrap();
    assert!(checked >= 3, "the reader saw only {checked} commits");
}

/// A rollback compensates each change once, and each compensation names
/// the change before the one it undoes, so undo that a crash cuts off goes
/// on from where it stopped.
#[test]
fn a_rollback_logs_one_compensation_per_change() {
    let fs = CrashFs::new();
    let mut workload = Workload::new(&fs, 16, SEGMENT, 0xc1a5);
    workload.begin();
    for id in 0..12 {
        assert!(matches!(workload.insert(0, id, (id, 40)), Ok(true)));
    }
    let tid = workload.open[0].txn.tid();
    workload.abort(0);
    workload.db.storage.bpm.log().flush_all().unwrap();
    drop(workload.crash());

    let log = Log::open(Arc::new(fs.crash_now()), LOG, SEGMENT).unwrap();
    let mut scan = log.scan(FIRST_LSN);
    let (mut updates, mut compensated) = (Vec::new(), Vec::new());
    let mut kinds = Vec::new();
    while let Some(record) = scan.next_record().unwrap() {
        if record.tid != tid {
            continue;
        }
        kinds.push(record.kind);
        match record.kind {
            Kind::Update => updates.push((record.lsn, record.prev_lsn)),
            Kind::Clr => compensated.push(record.undo_next().unwrap()),
            _ => {}
        }
    }
    assert_eq!(kinds.first(), Some(&Kind::Update));
    assert_eq!(kinds.last(), Some(&Kind::End));
    assert_eq!(kinds.iter().filter(|&&k| k == Kind::Abort).count(), 1);
    // Newest change first, each compensation naming the change before it.
    let expected: Vec<_> = updates.iter().rev().map(|&(_, prev)| prev).collect();
    assert_eq!(compensated, expected, "updates {updates:?}");
}

/// Undo cut off by a crash goes on after the last change it compensated,
/// and never undoes a change twice: a row whose change it took back, which
/// another transaction then wrote and committed, keeps that commit.
#[test]
fn a_crash_during_a_rollback_keeps_a_commit_to_a_row_it_gave_back() {
    let fs = CrashFs::new();
    let mut workload = Workload::new(&fs, 4, SEGMENT, 0x5107);
    workload.begin();
    workload.write_rows(0, |_| true);
    workload.commit(0);
    workload.begin();
    workload.write_rows(0, |_| true);
    assert_eq!(workload.open[0].rows.len(), IDS as usize, "a write failed");
    let (aborting, layout) = (Arc::clone(&workload.open[0].txn), workload.layout);
    // Its rollback gives this row back first.
    let (last, rid) = (IDS - 1, workload.committed[&(IDS - 1)].1);
    let Workload { db, open, .. } = workload;
    std::mem::forget(open);
    let db = Arc::new(db);
    fs.slow_syncs(Duration::from_millis(2));

    let rollback = {
        let (db, aborting) = (Arc::clone(&db), Arc::clone(&aborting));
        thread::spawn(move || db.tm.abort(&aborting).unwrap())
    };
    let table = &db.table;
    let image = loop {
        let txn = db.tm.begin();
        match txn.get(&table.heap, &table.schema, rid) {
            Err(storage::StorageError::Conflict { .. }) => db.tm.abort(&txn).unwrap(),
            Ok(Some(_)) => {
                txn.update(&table.heap, &table.schema, rid, &encode(last, (7777, 5)))
                    .unwrap();
                db.tm.commit(&txn).unwrap();
                break fs.crash_after(fs.num_events(), Lost::Everything);
            }
            other => panic!("{other:?}"),
        }
    };
    rollback.join().unwrap();

    let log = Log::open(Arc::new(image.with_removed()), LOG, SEGMENT).unwrap();
    let mut scan = log.scan(FIRST_LSN);
    while let Some(record) = scan.next_record().unwrap() {
        assert!(
            record.tid != aborting.tid() || record.kind != Kind::End,
            "the rollback ended before the crash, so the crash tests nothing"
        );
    }
    let (_, max_tid) = commits_in(&image);
    let recovered = Db::open(&image, 4, layout, max_tid + 1);
    let txn = recovered.tm.begin();
    let rows = visible(&recovered, &txn);
    recovered.tm.commit(&txn).unwrap();
    let row = rows
        .iter()
        .find(|&&(_, (id, _))| id == last)
        .map(|&(_, (_, row))| row);
    assert_eq!(row, Some((7777, 5)), "the commit after the undo was lost");
}
