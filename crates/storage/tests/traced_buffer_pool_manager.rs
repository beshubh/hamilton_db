use std::sync::Arc;

use storage::StorageError;
use storage::buffer::arc_replacer::AccessType;
use storage::buffer::buffer_pool_manager::BufferPoolManager;
use storage::buffer::traced_buffer_pool_manager::{PageAccess, TracedBufferPoolManager};
use storage::disk::disk_manager::DiskManager;
use tempfile::TempDir;

fn setup() -> (TempDir, TracedBufferPoolManager) {
    let dir = TempDir::new().unwrap();
    let dm = Arc::new(DiskManager::new(dir.path().join("test.db")).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(4, dm));
    (dir, TracedBufferPoolManager::new(bpm))
}

#[test]
fn records_nothing_until_started() {
    let (_dir, bpm) = setup();
    assert!(!bpm.is_tracing());
    let page_id = bpm.new_page();
    bpm.checked_write_page(page_id, AccessType::Unknown)
        .unwrap()
        .get_data_mut()[0] = 1;
    assert!(bpm.take_trace().is_empty());
}

#[test]
fn records_operations_in_order() {
    let (_dir, bpm) = setup();
    bpm.start_tracing();
    let page_id = bpm.new_page();
    bpm.checked_write_page(page_id, AccessType::Unknown)
        .unwrap()
        .get_data_mut()[0] = 7;
    assert_eq!(
        bpm.checked_read_page(page_id, AccessType::Unknown)
            .unwrap()
            .get_data()[0],
        7
    );
    bpm.delete_page(page_id).unwrap();

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
    assert!(matches!(
        bpm.delete_page(page_id),
        Err(StorageError::PagePinned(_))
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
