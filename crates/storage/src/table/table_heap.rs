use std::sync::Arc;

use common::{INVALID_PAGE_ID, PageId, Rid, SlotId};
use parking_lot::Mutex;

use crate::buffer::arc_replacer::AccessType;
use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::error::{Result, StorageError};
use crate::page::page_guard::{ReadPageGuard, WritePageGuard};
use crate::table::table_iterator::TableIterator;
use crate::table::table_page::{TablePage, TupleMeta};

/// A table stored as a linked list of table pages, reached through the
/// buffer pool.
///
/// Each operation latches only the pages it touches, through the buffer
/// pool's page guards. Inserts also take `last_page_id`'s mutex, so that two
/// inserts never both append a new page to the end of the list.
pub struct TableHeap {
    bpm: Arc<BufferPoolManager>,
    first_page_id: PageId,
    last_page_id: Mutex<PageId>,
}

impl TableHeap {
    /// Creates an empty table with one empty page.
    pub fn create(bpm: Arc<BufferPoolManager>) -> Result<Self> {
        let page_id = bpm.new_page();
        let mut guard = bpm.checked_write_page(page_id, AccessType::Unknown)?;
        TablePage::init(guard.get_data_mut());
        drop(guard);
        Ok(Self {
            bpm,
            first_page_id: page_id,
            last_page_id: Mutex::new(page_id),
        })
    }

    /// Opens an existing table, walking its page list to find the last page.
    pub fn open(bpm: Arc<BufferPoolManager>, first_page_id: PageId) -> Result<Self> {
        let mut last = first_page_id;
        loop {
            let guard = bpm.checked_read_page(last, AccessType::Scan)?;
            let next = TablePage::new(guard.get_data())?.next_page_id();
            if next == INVALID_PAGE_ID {
                break;
            }
            last = next;
        }
        Ok(Self {
            bpm,
            first_page_id,
            last_page_id: Mutex::new(last),
        })
    }

    pub fn first_page_id(&self) -> PageId {
        self.first_page_id
    }

    /// Appends a tuple to the last page, starting a new page when that one is
    /// full, and returns where it went.
    pub fn insert(&self, meta: &TupleMeta, payload: &[u8]) -> Result<Rid> {
        let mut last_page_id = self.last_page_id.lock();
        let mut guard = self
            .bpm
            .checked_write_page(*last_page_id, AccessType::Unknown)?;
        let mut page = TablePage::new(guard.get_data_mut())?;
        match page.insert(meta, payload) {
            Ok(slot) => return Ok(Rid::new(*last_page_id, slot)),
            Err(StorageError::PageFull) => {}
            Err(e) => return Err(e),
        }

        // The new page is unreachable until it is linked in, so latching it
        // while still holding the old last page cannot deadlock.
        let new_page_id = self.bpm.new_page();
        let mut new_guard = self
            .bpm
            .checked_write_page(new_page_id, AccessType::Unknown)?;
        let mut new_page = TablePage::init(new_guard.get_data_mut());
        let slot = new_page.insert(meta, payload)?;
        page.set_next_page_id(new_page_id);
        *last_page_id = new_page_id;
        Ok(Rid::new(new_page_id, slot))
    }

    /// The tuple at `rid`, copied out of the page. Deleted tuples are still
    /// returned; their meta says so.
    pub fn get(&self, rid: Rid) -> Result<(TupleMeta, Vec<u8>)> {
        let guard = self
            .bpm
            .checked_read_page(rid.page_id, AccessType::Lookup)?;
        let page = TablePage::new(guard.get_data())?;
        let (meta, payload) = page.get(rid.slot)?;
        Ok((meta, payload.to_vec()))
    }

    /// Replaces the meta of the tuple at `rid`, such as stamping the commit
    /// timestamp once its transaction commits.
    pub fn update_meta(&self, rid: Rid, meta: &TupleMeta) -> Result<()> {
        let mut guard = self.write_page(rid)?;
        TablePage::new(guard.get_data_mut())?.update_meta(rid.slot, meta)
    }

    /// Marks the tuple at `rid` deleted as of `ts`, with `undo_ptr` pointing
    /// at the undo entry that restores it. The tuple stays on the page until
    /// vacuum frees its slot.
    pub fn mark_delete(&self, rid: Rid, ts: u64, undo_ptr: u64) -> Result<()> {
        let mut guard = self.write_page(rid)?;
        let mut page = TablePage::new(guard.get_data_mut())?;
        let (old, _) = page.get(rid.slot)?;
        let meta = TupleMeta {
            ts,
            undo_ptr,
            is_deleted: true,
            ..old
        };
        page.update_meta(rid.slot, &meta)
    }

    /// Replaces the tuple at `rid`, keeping its `Rid`. Fails with `PageFull`
    /// if the new version does not fit on the tuple's page; the caller then
    /// deletes it and inserts the new version elsewhere.
    pub fn update_in_place(&self, rid: Rid, meta: &TupleMeta, payload: &[u8]) -> Result<()> {
        let mut guard = self.write_page(rid)?;
        TablePage::new(guard.get_data_mut())?.update(rid.slot, meta, payload)
    }

    /// Iterates over the live rows of the table. See `TableIterator`.
    pub fn iter(&self) -> Result<TableIterator<'_>> {
        TableIterator::new(self)
    }

    /// The last page and its slot count, read together so no insert can
    /// land between the two.
    pub(crate) fn end(&self) -> Result<(PageId, SlotId)> {
        let last_page_id = self.last_page_id.lock();
        let guard = self.read_page(*last_page_id, AccessType::Scan)?;
        let slot_count = TablePage::new(guard.get_data())?.slot_count();
        Ok((*last_page_id, slot_count))
    }

    pub(crate) fn read_page(
        &self,
        page_id: PageId,
        access_type: AccessType,
    ) -> Result<ReadPageGuard> {
        self.bpm.checked_read_page(page_id, access_type)
    }

    fn write_page(&self, rid: Rid) -> Result<WritePageGuard> {
        self.bpm.checked_write_page(rid.page_id, AccessType::Lookup)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disk::disk_manager::DiskManager;
    use crate::table::table_page::MAX_PAYLOAD_SIZE;
    use std::collections::HashSet;
    use std::thread;
    use tempfile::TempDir;

    fn setup(num_frames: usize) -> (TempDir, Arc<BufferPoolManager>) {
        let dir = TempDir::new().unwrap();
        let dm = Arc::new(DiskManager::new(dir.path().join("test.db")).unwrap());
        (dir, Arc::new(BufferPoolManager::new(num_frames, dm)))
    }

    #[test]
    fn insert_and_get() {
        let (_dir, bpm) = setup(4);
        let heap = TableHeap::create(bpm).unwrap();
        let a = heap.insert(&TupleMeta::new(1), b"alpha").unwrap();
        let b = heap.insert(&TupleMeta::new(2), b"beta").unwrap();
        assert_eq!(a, Rid::new(heap.first_page_id(), 0));
        assert_eq!(b, Rid::new(heap.first_page_id(), 1));
        assert_eq!(heap.get(a).unwrap(), (TupleMeta::new(1), b"alpha".to_vec()));
        assert_eq!(heap.get(b).unwrap(), (TupleMeta::new(2), b"beta".to_vec()));
    }

    #[test]
    fn insert_spills_onto_new_pages() {
        let (_dir, bpm) = setup(3);
        let heap = TableHeap::create(Arc::clone(&bpm)).unwrap();
        let rids: Vec<Rid> = (0..200u32)
            .map(|i| {
                heap.insert(&TupleMeta::new(i as u64), &[i as u8; 100])
                    .unwrap()
            })
            .collect();

        let pages: HashSet<PageId> = rids.iter().map(|r| r.page_id).collect();
        assert!(
            pages.len() > 3,
            "more pages than frames, so some were evicted"
        );
        for (i, &rid) in rids.iter().enumerate() {
            assert_eq!(
                heap.get(rid).unwrap(),
                (TupleMeta::new(i as u64), vec![i as u8; 100])
            );
        }

        // Reopening walks the chain to the same last page.
        let reopened = TableHeap::open(bpm, heap.first_page_id()).unwrap();
        assert_eq!(*reopened.last_page_id.lock(), *heap.last_page_id.lock());
        let next = reopened.insert(&TupleMeta::new(0), b"tail").unwrap();
        assert_eq!(next.page_id, rids.last().unwrap().page_id);
    }

    #[test]
    fn oversized_tuple_does_not_allocate() {
        let (_dir, bpm) = setup(4);
        let heap = TableHeap::create(bpm).unwrap();
        let first = heap.first_page_id();
        assert!(matches!(
            heap.insert(&TupleMeta::new(1), &vec![0u8; MAX_PAYLOAD_SIZE + 1]),
            Err(StorageError::TupleTooLarge { .. })
        ));
        assert_eq!(*heap.last_page_id.lock(), first);
    }

    #[test]
    fn mark_delete() {
        let (_dir, bpm) = setup(4);
        let heap = TableHeap::create(bpm).unwrap();
        let meta = TupleMeta {
            has_nulls: true,
            ..TupleMeta::new(1)
        };
        let rid = heap.insert(&meta, b"row").unwrap();
        heap.mark_delete(rid, 7, 42).unwrap();

        let expected = TupleMeta {
            ts: 7,
            undo_ptr: 42,
            is_deleted: true,
            has_nulls: true,
        };
        assert_eq!(heap.get(rid).unwrap(), (expected, b"row".to_vec()));
    }

    #[test]
    fn update_meta() {
        let (_dir, bpm) = setup(4);
        let heap = TableHeap::create(bpm).unwrap();
        let rid = heap.insert(&TupleMeta::new(1 << 63 | 5), b"row").unwrap();
        heap.update_meta(rid, &TupleMeta::new(10)).unwrap();
        assert_eq!(heap.get(rid).unwrap().0, TupleMeta::new(10));
    }

    #[test]
    fn update_in_place() {
        let (_dir, bpm) = setup(4);
        let heap = TableHeap::create(bpm).unwrap();
        let rid = heap.insert(&TupleMeta::new(1), b"short").unwrap();
        heap.update_in_place(rid, &TupleMeta::new(2), b"a much longer row")
            .unwrap();
        assert_eq!(
            heap.get(rid).unwrap(),
            (TupleMeta::new(2), b"a much longer row".to_vec())
        );
    }

    #[test]
    fn update_in_place_on_full_page() {
        let (_dir, bpm) = setup(4);
        let heap = TableHeap::create(bpm).unwrap();
        let rid = heap.insert(&TupleMeta::new(1), &[1u8; 1500]).unwrap();
        heap.insert(&TupleMeta::new(1), &[2u8; 1500]).unwrap();
        assert!(matches!(
            heap.update_in_place(rid, &TupleMeta::new(2), &[3u8; 3000]),
            Err(StorageError::PageFull)
        ));
        assert_eq!(heap.get(rid).unwrap().1, vec![1u8; 1500]);
    }

    #[test]
    fn invalid_rid() {
        let (_dir, bpm) = setup(4);
        let heap = TableHeap::create(bpm).unwrap();
        let rid = Rid::new(heap.first_page_id(), 3);
        assert!(matches!(heap.get(rid), Err(StorageError::InvalidSlot(3))));
        assert!(matches!(
            heap.mark_delete(rid, 1, 0),
            Err(StorageError::InvalidSlot(3))
        ));
    }

    #[test]
    fn concurrent_inserts() {
        let (_dir, bpm) = setup(8);
        let heap = Arc::new(TableHeap::create(bpm).unwrap());
        let handles: Vec<_> = (0..4u8)
            .map(|t| {
                let heap = Arc::clone(&heap);
                thread::spawn(move || {
                    (0..100u8)
                        .map(|i| {
                            (
                                heap.insert(&TupleMeta::new(t as u64), &[t, i]).unwrap(),
                                t,
                                i,
                            )
                        })
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        let inserted: Vec<_> = handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();

        let rids: HashSet<Rid> = inserted.iter().map(|&(rid, _, _)| rid).collect();
        assert_eq!(rids.len(), 400);
        for (rid, t, i) in inserted {
            assert_eq!(
                heap.get(rid).unwrap(),
                (TupleMeta::new(t as u64), vec![t, i])
            );
        }
    }
}
