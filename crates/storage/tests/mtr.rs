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
use storage::table::table_page::TupleMeta;
use storage::wal::Logged;
use storage::wal::record::{Kind, PageChanges, PageRedo};
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

/// The pages the record at `lsn` holds images of, and the pages it frees.
fn logged(bpm: &BufferPoolManager, lsn: Lsn) -> (Vec<(PageId, [u8; PAGE_SIZE])>, Vec<PageId>) {
    let mut buf = Vec::new();
    let record = bpm.log().read(lsn, &mut buf).unwrap();
    assert_eq!(
        (record.kind, record.tid, record.prev_lsn),
        (Kind::System, 0, INVALID_LSN)
    );
    let changes = PageChanges::decode(record.body).unwrap();
    let pages = changes
        .pages()
        .map(|(page_id, PageRedo::Image(image))| (page_id, *image))
        .collect();
    (pages, changes.frees().collect())
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

#[test]
fn a_heap_insert_logs_the_page_it_changed() {
    let db = setup(4);
    let heap = TableHeap::create(Arc::clone(&db.bpm)).unwrap();
    let end = db.bpm.log().end();
    let tuple = Tuple::from_bytes(b"row".to_vec());
    let rid = heap.insert(&TupleMeta::new(1), &tuple).unwrap();

    let page = in_memory(&db.bpm, rid.page_id);
    assert_eq!(page_lsn(&page), end);
    let (pages, _) = logged(&db.bpm, end);
    assert_eq!(page_ids(&pages), [rid.page_id]);
    assert_eq!(pages[0].1[PAST_LSN..], page[PAST_LSN..]);
}
