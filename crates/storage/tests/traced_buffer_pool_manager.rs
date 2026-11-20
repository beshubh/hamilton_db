use storage::StorageError;
use storage::buffer::arc_replacer::AccessType;
use storage::buffer::traced_buffer_pool_manager::{PageAccess, TracedBufferPoolManager};
use storage::page::page_header::PageHeader;
use storage::wal::Logged;
use tempfile::TempDir;

fn setup() -> (TempDir, TracedBufferPoolManager) {
    let dir = TempDir::new().unwrap();
    let bpm = storage::open(dir.path().join("test.db"), 4).unwrap().bpm;
    (dir, TracedBufferPoolManager::new(bpm))
}

/// Writes `byte` to the page through a mini-transaction of its own.
fn write(bpm: &TracedBufferPoolManager, page_id: i32, byte: u8) {
    let mtr = bpm.bpm().mtr(Logged::System);
    bpm.write_page(&mtr, page_id).unwrap().raw_mut()[PageHeader::SIZE] = byte;
}

#[test]
fn records_nothing_until_started() {
    let (_dir, bpm) = setup();
    assert!(!bpm.is_tracing());
    let page_id = bpm.new_page();
    write(&bpm, page_id, 1);
    assert!(bpm.take_trace().is_empty());
}

#[test]
fn records_operations_in_order() {
    let (_dir, bpm) = setup();
    bpm.start_tracing();
    let page_id = bpm.new_page();
    write(&bpm, page_id, 7);
    assert_eq!(
        bpm.checked_read_page(page_id, AccessType::Unknown)
            .unwrap()
            .get_data()[PageHeader::SIZE],
        7
    );
    let mtr = bpm.bpm().mtr(Logged::System);
    bpm.free_page(&mtr, page_id);
    mtr.commit();

    assert_eq!(
        bpm.take_trace(),
        [
            PageAccess::New(page_id),
            PageAccess::Write(page_id),
            PageAccess::Read(page_id),
            PageAccess::Delete(page_id),
        ]
    );
    assert!(bpm.take_trace().is_empty());
    assert_eq!(bpm.get_pin_count(page_id), None);
}

#[test]
fn failed_operations_are_not_recorded() {
    let (_dir, bpm) = setup();
    bpm.start_tracing();
    assert!(matches!(
        bpm.checked_read_page(99, AccessType::Unknown),
        Err(StorageError::InvalidPageId(99))
    ));

    let page_id = bpm.new_page();
    let _guard = bpm.checked_read_page(page_id, AccessType::Unknown).unwrap();
    let mtr = bpm.bpm().mtr(Logged::System);
    assert!(matches!(
        bpm.write_page(&mtr, 99),
        Err(StorageError::InvalidPageId(99))
    ));
    assert_eq!(
        bpm.take_trace(),
        [PageAccess::New(page_id), PageAccess::Read(page_id)]
    );
}

#[test]
fn stop_tracing_keeps_what_was_recorded() {
    let (_dir, bpm) = setup();
    bpm.start_tracing();
    let first = bpm.new_page();
    bpm.stop_tracing();
    bpm.new_page();
    assert_eq!(bpm.take_trace(), [PageAccess::New(first)]);
}

#[test]
fn calls_on_the_inner_pool_are_not_traced() {
    let (_dir, bpm) = setup();
    bpm.start_tracing();
    bpm.bpm().new_page();
    assert!(bpm.take_trace().is_empty());
}
