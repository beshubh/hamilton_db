//! Mini-transactions: how page changes reach the log, and the log reaches
//! disk before the pages do.

use std::sync::Arc;
use std::thread;

use common::{INVALID_LSN, Lsn, PageId, Tuple};
use storage::StorageError;
use storage::buffer::arc_replacer::AccessType;
use storage::buffer::buffer_pool_manager::BufferPoolManager;
use storage::disk::disk_manager::DiskManager;
use storage::page::page_header::{PageHeader, page_lsn};
use storage::page::{PAGE_SIZE, TEMP_PAGE_BASE};
use storage::table::table_heap::TableHeap;
use storage::table::table_page::{MAX_PAYLOAD_SIZE, TablePage, TupleMeta};
use storage::wal::record::{Kind, PageChanges, PageRedo};
use storage::wal::{Logged, PageOp};
use tempfile::TempDir;

/// Where tests put their bytes: past the page header, whose LSN an mtr sets.
const AT: usize = PageHeader::SIZE;

/// The part of a page an image must match: all but the LSN, which redo
/// takes from the record.
const PAST_LSN: usize = size_of::<Lsn>();

struct Db {
    _dir: TempDir,
    dm: Arc<DiskManager>,
    bpm: Arc<BufferPoolManager>,
}

fn setup(frames: usize) -> Db {
    let dir = TempDir::new().unwrap();
    let bpm = storage::open(dir.path().join("test.db"), frames)
        .unwrap()
        .bpm;
    let dm = Arc::clone(bpm.disk_manager());
    Db { _dir: dir, dm, bpm }
}

/// Writes `bytes` to the page through an mtr of its own and returns the
/// record's LSN.
fn write(bpm: &BufferPoolManager, page_id: PageId, bytes: &[u8]) -> Lsn {
    let mtr = bpm.mtr(Logged::System);
    mtr.write_page(page_id).unwrap().raw_mut()[AT..][..bytes.len()].copy_from_slice(bytes);
    mtr.commit().unwrap()
}

/// How a record logs one page: its image, or the page its ops leave when
/// made on `base`.
#[derive(Debug, PartialEq)]
enum Form {
    Image(Box<[u8; PAGE_SIZE]>),
    Ops {
        ops: Vec<String>,
        replace_page: bool,
        redone: Box<[u8; PAGE_SIZE]>,
    },
}

/// How the record at `lsn` logs each page, with ops made on `base`, and
/// the pages it frees.
fn logged_on(
    bpm: &BufferPoolManager,
    lsn: Lsn,
    base: &[u8; PAGE_SIZE],
) -> (Vec<(PageId, Form)>, Vec<PageId>) {
    let mut buf = Vec::new();
    let record = bpm.log().read(lsn, &mut buf).unwrap();
    assert_eq!(
        (record.kind, record.tid, record.prev_lsn),
        (Kind::System, 0, INVALID_LSN)
    );
    let changes = PageChanges::decode(record.body).unwrap();
    let pages = changes
        .pages()
        .map(|(page_id, redo)| {
            let form = match redo {
                PageRedo::Image(image) => Form::Image(Box::new(*image)),
                PageRedo::Ops(ops) => {
                    let mut redone = Box::new(*base);
                    ops.apply(&mut redone).unwrap();
                    Form::Ops {
                        ops: ops.iter().map(|op| format!("{:?}", op.unwrap())).collect(),
                        replace_page: ops.replace_page(),
                        redone,
                    }
                }
            };
            (page_id, form)
        })
        .collect();
    (pages, changes.frees().collect())
}

/// The pages the record at `lsn` holds images of, and the pages it frees.
fn logged(bpm: &BufferPoolManager, lsn: Lsn) -> (Vec<(PageId, [u8; PAGE_SIZE])>, Vec<PageId>) {
    let (pages, frees) = logged_on(bpm, lsn, &[0; PAGE_SIZE]);
    let images = pages.into_iter().map(|(page_id, form)| match form {
        Form::Image(image) => (page_id, *image),
        ops => panic!("page {page_id} is logged as {ops:?}"),
    });
    (images.collect(), frees)
}

fn in_memory(bpm: &BufferPoolManager, page_id: PageId) -> [u8; PAGE_SIZE] {
    *bpm.read_page(page_id, AccessType::Unknown).get_data()
}

fn on_disk(dm: &DiskManager, page_id: PageId) -> [u8; PAGE_SIZE] {
    let mut data = [0; PAGE_SIZE];
    dm.read_page(page_id, &mut data).unwrap();
    data
}

fn page_ids(pages: &[(PageId, [u8; PAGE_SIZE])]) -> Vec<PageId> {
    pages.iter().map(|&(page_id, _)| page_id).collect()
}

#[test]
fn a_changed_page_carries_its_records_lsn_and_the_record_its_image() {
    let db = setup(4);
    let page_id = db.bpm.new_page();
    let lsn = write(&db.bpm, page_id, b"after");

    let page = in_memory(&db.bpm, page_id);
    assert_eq!(page_lsn(&page), lsn);
    assert_eq!(&page[AT..][..5], b"after");
    let (pages, frees) = logged(&db.bpm, lsn);
    assert_eq!(page_ids(&pages), [page_id]);
    assert_eq!(pages[0].1[PAST_LSN..], page[PAST_LSN..]);
    assert!(frees.is_empty());
}

#[test]
fn every_page_an_mtr_changes_is_in_its_one_record() {
    let db = setup(4);
    let (a, b) = (db.bpm.new_page(), db.bpm.new_page());
    let end = db.bpm.log().end();
    let mtr = db.bpm.mtr(Logged::System);
    let (mut page_a, mut page_b) = (mtr.write_page(a).unwrap(), mtr.write_page(b).unwrap());
    page_a.raw_mut()[AT] = 1;
    page_b.raw_mut()[AT] = 2;
    drop((page_a, page_b));
    let lsn = mtr.commit().unwrap();

    assert_eq!(lsn, end, "one record");
    let (pages, _) = logged(&db.bpm, lsn);
    assert_eq!(page_ids(&pages), [a, b]);
    assert_eq!(page_lsn(&in_memory(&db.bpm, a)), lsn);
    assert_eq!(page_lsn(&in_memory(&db.bpm, b)), lsn);
}

#[test]
fn a_page_latched_but_not_changed_is_released_at_once_and_not_logged() {
    let db = setup(4);
    let page_id = db.bpm.new_page();
    let end = db.bpm.log().end();
    let mtr = db.bpm.mtr(Logged::System);
    drop(mtr.write_page(page_id).unwrap());

    assert_eq!(db.bpm.get_pin_count(page_id), Some(0), "released early");
    assert_eq!(mtr.commit(), None);
    assert_eq!(db.bpm.log().end(), end);
    assert!(!db.bpm.read_page(page_id, AccessType::Unknown).is_dirty());
}

#[test]
fn a_changed_page_stays_latched_until_its_record_is_appended() {
    let db = setup(4);
    let page_id = db.bpm.new_page();
    let mtr = db.bpm.mtr(Logged::System);
    mtr.write_page(page_id).unwrap().raw_mut()[AT] = 7;
    assert_eq!(db.bpm.get_pin_count(page_id), Some(1), "the mtr holds it");

    let reader = {
        let bpm = Arc::clone(&db.bpm);
        thread::spawn(move || {
            let guard = bpm.read_page(page_id, AccessType::Unknown);
            (guard.get_data()[AT], page_lsn(guard.get_data()))
        })
    };
    while db.bpm.get_pin_count(page_id) != Some(2) {
        thread::yield_now();
    }
    let lsn = mtr.commit().unwrap();

    assert_eq!(reader.join().unwrap(), (7, lsn), "seen only with its LSN");
    assert_eq!(db.bpm.get_pin_count(page_id), Some(0));
}

#[test]
fn dropping_an_mtr_commits_it() {
    let db = setup(4);
    let page_id = db.bpm.new_page();
    let end = db.bpm.log().end();
    {
        let mtr = db.bpm.mtr(Logged::System);
        mtr.write_page(page_id).unwrap().raw_mut()[AT] = 9;
    }
    assert_eq!(page_lsn(&in_memory(&db.bpm, page_id)), end);
    assert_eq!(page_ids(&logged(&db.bpm, end).0), [page_id]);
}

#[test]
fn a_page_changed_twice_in_one_mtr_comes_back_latched_and_is_logged_once() {
    let db = setup(4);
    let page_id = db.bpm.new_page();
    let mtr = db.bpm.mtr(Logged::System);
    mtr.write_page(page_id).unwrap().raw_mut()[AT] = 1;
    mtr.write_page(page_id).unwrap().raw_mut()[AT + 1] = 2;
    let lsn = mtr.commit().unwrap();

    let (pages, _) = logged(&db.bpm, lsn);
    assert_eq!(page_ids(&pages), [page_id]);
    assert_eq!(pages[0].1[AT..AT + 2], [1, 2]);
}

#[test]
fn a_flushed_page_reaches_disk_after_its_record() {
    let db = setup(4);
    let page_id = db.bpm.new_page();
    let lsn = write(&db.bpm, page_id, b"durable");
    assert!(db.bpm.log().durable_end() <= lsn, "nothing forced it yet");

    assert!(db.bpm.flush_page(page_id).unwrap());
    assert!(db.bpm.log().durable_end() > lsn);
    let mut scan = db.bpm.log().scan(lsn);
    assert_eq!(scan.next_record().unwrap().unwrap().lsn, lsn, "in the file");
    let disk = on_disk(&db.dm, page_id);
    assert_eq!(page_lsn(&disk), lsn);
    assert_eq!(&disk[AT..][..7], b"durable");
}

#[test]
fn an_evicted_page_reaches_disk_after_its_record() {
    let db = setup(1);
    let (page_id, other) = (db.bpm.new_page(), db.bpm.new_page());
    let lsn = write(&db.bpm, page_id, b"evicted");
    let syncs = db.bpm.log().num_syncs();

    drop(db.bpm.read_page(other, AccessType::Unknown));
    assert!(db.bpm.log().durable_end() > lsn);
    assert_eq!(db.bpm.log().num_syncs(), syncs + 1);
    assert_eq!(page_lsn(&on_disk(&db.dm, page_id)), lsn);
}

#[test]
fn temporary_pages_are_never_logged() {
    let db = setup(1);
    let temp = db.bpm.new_temp_page();
    assert!(temp >= TEMP_PAGE_BASE);
    let (end, syncs) = (db.bpm.log().end(), db.bpm.log().num_syncs());
    db.bpm.write_temp_page(temp).unwrap().get_data_mut()[AT..][..5].copy_from_slice(b"spill");
    let other = db.bpm.new_page();
    drop(db.bpm.read_page(other, AccessType::Unknown));

    let disk = on_disk(&db.dm, temp);
    assert_eq!(
        (&disk[AT..][..5], page_lsn(&disk)),
        (&b"spill"[..], INVALID_LSN)
    );
    assert_eq!(&in_memory(&db.bpm, temp)[AT..][..5], b"spill");
    assert_eq!((db.bpm.log().end(), db.bpm.log().num_syncs()), (end, syncs));

    let mtr = db.bpm.mtr(Logged::System);
    assert!(matches!(
        mtr.write_page(temp),
        Err(StorageError::InvalidPageId(id)) if id == temp
    ));
    assert!(matches!(
        db.bpm.write_temp_page(other),
        Err(StorageError::InvalidPageId(id)) if id == other
    ));
}

#[test]
fn rec_lsn_is_the_first_record_since_the_page_was_written() {
    let db = setup(4);
    let page_id = db.bpm.new_page();
    let rec_lsn = || db.bpm.read_page(page_id, AccessType::Unknown).rec_lsn();
    assert_eq!(rec_lsn(), INVALID_LSN);

    let first = write(&db.bpm, page_id, b"one");
    assert_eq!(rec_lsn(), first);
    let second = write(&db.bpm, page_id, b"two");
    assert!(second > first);
    assert_eq!(rec_lsn(), first, "already dirty");

    db.bpm.flush_page(page_id).unwrap();
    let guard = db.bpm.read_page(page_id, AccessType::Unknown);
    assert_eq!((guard.rec_lsn(), guard.is_dirty()), (INVALID_LSN, false));
    drop(guard);
    let third = write(&db.bpm, page_id, b"three");
    assert_eq!(rec_lsn(), third);
}

#[test]
fn a_freed_page_is_deleted_once_its_record_is_appended() {
    let db = setup(4);
    let (kept, freed) = (db.bpm.new_page(), db.bpm.new_page());
    write(&db.bpm, freed, b"gone");
    db.bpm.flush_page(freed).unwrap();
    assert!(db.dm.has_page(freed));

    let mtr = db.bpm.mtr(Logged::System);
    mtr.write_page(kept).unwrap().raw_mut()[AT] = 1;
    mtr.write_page(freed).unwrap().raw_mut()[AT] = 2;
    mtr.free_page(freed);
    assert!(
        db.bpm.get_pin_count(freed).is_some(),
        "kept until the commit"
    );
    let lsn = mtr.commit().unwrap();

    let (pages, frees) = logged(&db.bpm, lsn);
    assert_eq!(page_ids(&pages), [kept], "no image of a freed page");
    assert_eq!(frees, [freed]);
    assert_eq!(db.bpm.get_pin_count(freed), None);
    assert!(!db.dm.has_page(freed));
}

fn row(bytes: &[u8]) -> Tuple {
    Tuple::from_bytes(bytes.to_vec())
}

/// Asserts that the record at `lsn` logs only `page_id`, as ops that give
/// `after` when made on `before`, and returns the ops.
fn assert_logged_as_ops(
    bpm: &BufferPoolManager,
    lsn: Lsn,
    page_id: PageId,
    before: &[u8; PAGE_SIZE],
    after: &[u8; PAGE_SIZE],
) -> Vec<String> {
    let (pages, _) = logged_on(bpm, lsn, before);
    let [(logged_id, Form::Ops { ops, redone, .. })] = <[_; 1]>::try_from(pages).unwrap() else {
        panic!("page {page_id} is not logged as ops");
    };
    assert_eq!(logged_id, page_id);
    assert_eq!(redone[PAST_LSN..], after[PAST_LSN..], "redo of {ops:?}");
    ops
}

#[test]
fn a_heap_insert_into_a_dirty_page_logs_only_the_row() {
    let db = setup(4);
    let heap = TableHeap::create(Arc::clone(&db.bpm)).unwrap();
    heap.insert(&TupleMeta::new(1), &row(b"first")).unwrap();
    let before = in_memory(&db.bpm, heap.first_page_id());
    let end = db.bpm.log().end();
    let rid = heap.insert(&TupleMeta::new(2), &row(b"second")).unwrap();

    let after = in_memory(&db.bpm, rid.page_id);
    assert_eq!(page_lsn(&after), end);
    let ops = assert_logged_as_ops(&db.bpm, end, rid.page_id, &before, &after);
    let expected = PageOp::HeapInsert {
        slot: rid.slot,
        meta: TupleMeta::new(2),
        payload: b"second",
    };
    assert_eq!(ops, [format!("{expected:?}")]);
}

#[test]
fn a_new_heap_page_is_logged_as_ops_that_format_it() {
    let db = setup(4);
    let end = db.bpm.log().end();
    let heap = TableHeap::create(Arc::clone(&db.bpm)).unwrap();

    let junk = [0xee; PAGE_SIZE];
    let (pages, _) = logged_on(&db.bpm, end, &junk);
    let page = in_memory(&db.bpm, heap.first_page_id());
    match &pages[..] {
        [
            (
                page_id,
                Form::Ops {
                    replace_page: true,
                    redone,
                    ..
                },
            ),
        ] => {
            assert_eq!(*page_id, heap.first_page_id());
            assert_eq!(
                redone[PAST_LSN..],
                page[PAST_LSN..],
                "formatting needs no old page"
            );
        }
        other => panic!("logged as {other:?}"),
    }
}

#[test]
fn the_first_change_after_a_write_logs_the_whole_page() {
    let db = setup(4);
    let heap = TableHeap::create(Arc::clone(&db.bpm)).unwrap();
    heap.insert(&TupleMeta::new(1), &row(b"first")).unwrap();
    db.bpm.flush_page(heap.first_page_id()).unwrap();
    let end = db.bpm.log().end();
    let rid = heap.insert(&TupleMeta::new(2), &row(b"second")).unwrap();

    let (pages, _) = logged(&db.bpm, end);
    assert_eq!(page_ids(&pages), [rid.page_id]);
    assert_eq!(
        pages[0].1[PAST_LSN..],
        in_memory(&db.bpm, rid.page_id)[PAST_LSN..]
    );

    let before = in_memory(&db.bpm, rid.page_id);
    let next = db.bpm.log().end();
    heap.insert(&TupleMeta::new(3), &row(b"third")).unwrap();
    let after = in_memory(&db.bpm, rid.page_id);
    assert_logged_as_ops(&db.bpm, next, rid.page_id, &before, &after);
}

#[test]
fn a_page_changed_raw_after_its_ops_is_logged_as_an_image() {
    let db = setup(4);
    let heap = TableHeap::create(Arc::clone(&db.bpm)).unwrap();
    let page_id = heap.first_page_id();
    let mtr = db.bpm.mtr(Logged::System);
    let mut page = mtr.write_page(page_id).unwrap();
    let insert = PageOp::HeapInsert {
        slot: 0,
        meta: TupleMeta::new(1),
        payload: b"op",
    };
    page.apply(insert).unwrap();
    TablePage::new(page.raw_mut())
        .unwrap()
        .insert(&TupleMeta::new(2), b"raw")
        .unwrap();
    drop(page);
    let lsn = mtr.commit().unwrap();

    let (pages, _) = logged(&db.bpm, lsn);
    assert_eq!(
        pages[0].1[PAST_LSN..],
        in_memory(&db.bpm, page_id)[PAST_LSN..]
    );
}

#[test]
fn ops_larger_than_the_page_are_logged_as_an_image() {
    let db = setup(4);
    let heap = TableHeap::create(Arc::clone(&db.bpm)).unwrap();
    let rid = heap.insert(&TupleMeta::new(1), &row(b"row")).unwrap();
    let mtr = db.bpm.mtr(Logged::System);
    let mut page = mtr.write_page(rid.page_id).unwrap();
    for len in [MAX_PAYLOAD_SIZE, 1, MAX_PAYLOAD_SIZE] {
        let set = PageOp::HeapSetTuple {
            slot: rid.slot,
            meta: TupleMeta::new(2),
            payload: &vec![7; len],
        };
        page.apply(set).unwrap();
    }
    drop(page);
    let lsn = mtr.commit().unwrap();

    let (pages, _) = logged(&db.bpm, lsn);
    assert_eq!(page_ids(&pages), [rid.page_id]);
}

#[test]
fn an_op_that_fails_changes_and_logs_nothing() {
    let db = setup(4);
    let heap = TableHeap::create(Arc::clone(&db.bpm)).unwrap();
    db.bpm.flush_page(heap.first_page_id()).unwrap();
    let end = db.bpm.log().end();
    let mtr = db.bpm.mtr(Logged::System);
    let mut page = mtr.write_page(heap.first_page_id()).unwrap();
    let missing = PageOp::HeapSetMeta {
        slot: 3,
        meta: TupleMeta::new(1),
    };
    assert!(matches!(
        page.apply(missing),
        Err(StorageError::InvalidSlot(3))
    ));
    assert!(!page.is_dirty());
    drop(page);

    assert_eq!(mtr.commit(), None);
    assert_eq!(db.bpm.log().end(), end);
}
