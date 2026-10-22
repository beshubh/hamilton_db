use std::collections::VecDeque;

use std::sync::Arc;

use common::{INVALID_PAGE_ID, PageId, Rid, SlotId, Tuple};

use crate::buffer::arc_replacer::AccessType;
use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::error::Result;
use crate::table::table_heap::TableHeap;
use crate::table::table_page::{TablePage, TupleMeta};

/// A row copied out of a table page. The tuple carries its `Rid`.
pub type TableRow = (TupleMeta, Tuple);

/// Iterates over the live rows of a table heap, page by page. Deleted rows
/// are skipped.
///
/// Each page's rows are copied out under one `ReadPageGuard`, which is
/// released before the rows are handed out, so a scan takes one latch per
/// page rather than per row. `next_batch` returns those page-sized batches
/// directly.
///
/// The scan ends at the last row that existed when the iterator was created.
/// Rows inserted later are not seen, so a statement that inserts into the
/// table it is scanning cannot keep feeding itself. Rows changed on a page
/// the scan has not reached yet are seen as they are when it gets there.
///
/// The iterator holds the buffer pool rather than borrowing the heap, so it
/// can be kept next to the `Arc` that owns the heap, as a scan executor does.
pub struct TableIterator {
    bpm: Arc<BufferPoolManager>,
    /// The next page to read, or `INVALID_PAGE_ID` once the scan is done.
    next_page_id: PageId,
    last_page_id: PageId,
    /// Slots at or past this on the last page were filled after the scan
    /// started.
    last_slot_count: SlotId,
    rows: VecDeque<TableRow>,
}

impl TableIterator {
    pub(crate) fn new(heap: &TableHeap) -> Result<Self> {
        let (last_page_id, last_slot_count) = heap.end()?;
        Ok(Self {
            bpm: Arc::clone(heap.bpm()),
            next_page_id: heap.first_page_id(),
            last_page_id,
            last_slot_count,
            rows: VecDeque::new(),
        })
    }

    /// The live rows of the next page that has any, or `None` once the scan
    /// is done. Does not return rows already buffered for `next`.
    pub fn next_batch(&mut self) -> Result<Option<Vec<TableRow>>> {
        while self.next_page_id != INVALID_PAGE_ID {
            let page_id = self.next_page_id;
            let guard = self.bpm.checked_read_page(page_id, AccessType::Scan)?;
            let page = TablePage::new(guard.get_data())?;
            let is_last = page_id == self.last_page_id;
            let end = if is_last {
                self.last_slot_count
            } else {
                SlotId::MAX
            };
            let rows: Vec<TableRow> = page
                .iter()
                .take_while(|&(slot, _, _)| slot < end)
                .filter(|(_, meta, _)| !meta.is_deleted)
                .map(|(slot, meta, payload)| {
                    let tuple = Tuple::from_bytes(payload.to_vec());
                    (meta, tuple.with_rid(Rid::new(page_id, slot)))
                })
                .collect();
            self.next_page_id = if is_last {
                INVALID_PAGE_ID
            } else {
                page.next_page_id()
            };
            drop(guard);

            if !rows.is_empty() {
                return Ok(Some(rows));
            }
        }
        Ok(None)
    }
}

impl Iterator for TableIterator {
    type Item = Result<TableRow>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.rows.is_empty() {
            match self.next_batch() {
                Ok(Some(rows)) => self.rows = rows.into(),
                Ok(None) => return None,
                Err(e) => {
                    self.next_page_id = INVALID_PAGE_ID;
                    return Some(Err(e));
                }
            }
        }
        self.rows.pop_front().map(Ok)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disk::disk_manager::DiskManager;
    use tempfile::TempDir;

    fn setup(num_frames: usize) -> (TempDir, Arc<BufferPoolManager>) {
        let dir = TempDir::new().unwrap();
        let dm = Arc::new(DiskManager::new(dir.path().join("test.db")).unwrap());
        (dir, Arc::new(BufferPoolManager::new(num_frames, dm)))
    }

    fn tuple(bytes: impl AsRef<[u8]>) -> Tuple {
        Tuple::from_bytes(bytes.as_ref().to_vec())
    }

    fn payloads(heap: &TableHeap) -> Vec<Vec<u8>> {
        heap.iter()
            .unwrap()
            .map(|row| row.unwrap().1.into_data())
            .collect()
    }

    #[test]
    fn empty_table() {
        let (_dir, bpm) = setup(4);
        let heap = TableHeap::create(bpm).unwrap();
        assert_eq!(heap.iter().unwrap().count(), 0);
        assert!(heap.iter().unwrap().next_batch().unwrap().is_none());
    }

    #[test]
    fn yields_rows_in_order_across_pages() {
        let (_dir, bpm) = setup(3);
        let heap = TableHeap::create(bpm).unwrap();
        let rids: Vec<Rid> = (0..200u32)
            .map(|i| {
                heap.insert(&TupleMeta::new(i as u64), &tuple([i as u8; 100]))
                    .unwrap()
            })
            .collect();

        let rows: Vec<TableRow> = heap.iter().unwrap().map(Result::unwrap).collect();
        assert_eq!(rows.len(), 200);
        for (i, (meta, tuple)) in rows.into_iter().enumerate() {
            assert_eq!(tuple.rid(), Some(rids[i]));
            assert_eq!(meta, TupleMeta::new(i as u64));
            assert_eq!(tuple.into_data(), vec![i as u8; 100]);
        }
    }

    #[test]
    fn batches_are_one_page_each() {
        let (_dir, bpm) = setup(3);
        let heap = TableHeap::create(bpm).unwrap();
        for i in 0..100u8 {
            heap.insert(&TupleMeta::new(1), &tuple([i; 100])).unwrap();
        }
        let mut iter = heap.iter().unwrap();
        let mut total = 0;
        while let Some(batch) = iter.next_batch().unwrap() {
            let page_id = batch[0].1.rid().unwrap().page_id;
            assert!(
                batch
                    .iter()
                    .all(|(_, tuple)| tuple.rid().unwrap().page_id == page_id)
            );
            total += batch.len();
        }
        assert_eq!(total, 100);
    }

    #[test]
    fn skips_deleted_rows_and_empty_pages() {
        let (_dir, bpm) = setup(4);
        let heap = TableHeap::create(bpm).unwrap();
        let rids: Vec<Rid> = (0..90u8)
            .map(|i| heap.insert(&TupleMeta::new(1), &tuple([i; 100])).unwrap())
            .collect();
        // Delete every row on the first page, and every other row after.
        let first = heap.first_page_id();
        for (i, &rid) in rids.iter().enumerate() {
            if rid.page_id == first || i % 2 == 0 {
                heap.mark_delete(rid, 2, 0).unwrap();
            }
        }
        let expected: Vec<Vec<u8>> = rids
            .iter()
            .enumerate()
            .filter(|&(i, rid)| rid.page_id != first && i % 2 == 1)
            .map(|(i, _)| vec![i as u8; 100])
            .collect();
        assert!(!expected.is_empty());
        assert_eq!(payloads(&heap), expected);
    }

    #[test]
    fn rows_inserted_after_creation_are_not_seen() {
        let (_dir, bpm) = setup(4);
        let heap = TableHeap::create(bpm).unwrap();
        for i in 0..40u8 {
            heap.insert(&TupleMeta::new(1), &tuple([i; 100])).unwrap();
        }
        let mut seen = 0;
        // Insert a copy of every row while scanning, as `INSERT INTO t SELECT
        // * FROM t` would. Spills onto new pages and fills the old last page.
        for row in heap.iter().unwrap() {
            let (meta, tuple) = row.unwrap();
            heap.insert(&meta, &tuple).unwrap();
            seen += 1;
        }
        assert_eq!(seen, 40);
        assert_eq!(heap.iter().unwrap().count(), 80);
    }
}
