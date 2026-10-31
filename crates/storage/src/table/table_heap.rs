use std::sync::Arc;

use common::{
    INVALID_PAGE_ID, INVALID_TXN_ID, PageId, Rid, Schema, SlotId, TXN_UNCOMMITTED_BIT, Tuple,
    TxnId, Value,
};
use parking_lot::Mutex;

use crate::buffer::arc_replacer::AccessType;
use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::error::{Result, StorageError};
use crate::page::page_guard::WritePageGuard;
use crate::table::table_iterator::{TableIterator, TableRow};
use crate::table::table_page::{INVALID_UNDO_PTR, TablePage, TupleMeta};
use crate::table::undo_log::{self, UndoLog, UndoRecord};

/// A table stored as a linked list of table pages, reached through the
/// buffer pool.
///
/// The heap holds the newest version of each tuple. The `_versioned` writes
/// and `get_at` keep older versions in an `UndoLog`, as `undo_log`
/// describes; the other methods read and write the newest version alone.
///
/// A versioned write whose `ts` has `TXN_UNCOMMITTED_BIT` set write-locks
/// the tuple for its transaction until `stamp_commit` or `rollback`. A
/// shrinking locked update keeps the tuple's old length, padding the new
/// payload with zeros, so that its rollback always fits back in place;
/// `stamp_commit` releases the padding. Reads at a timestamp cut it off;
/// `get` and `iter` return it.
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
    pub fn insert(&self, meta: &TupleMeta, tuple: &Tuple) -> Result<Rid> {
        let mut last_page_id = self.last_page_id.lock();
        let mut guard = self
            .bpm
            .checked_write_page(*last_page_id, AccessType::Unknown)?;
        let mut page = TablePage::new(guard.get_data_mut())?;
        match page.insert(meta, tuple.data()) {
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
        let slot = new_page.insert(meta, tuple.data())?;
        page.set_next_page_id(new_page_id);
        *last_page_id = new_page_id;
        Ok(Rid::new(new_page_id, slot))
    }

    /// The tuple at `rid`, copied out of the page and carrying `rid`. Deleted
    /// tuples are still returned; their meta says so.
    pub fn get(&self, rid: Rid) -> Result<(TupleMeta, Tuple)> {
        let guard = self
            .bpm
            .checked_read_page(rid.page_id, AccessType::Lookup)?;
        let page = TablePage::new(guard.get_data())?;
        let (meta, payload) = page.get(rid.slot)?;
        Ok((meta, Tuple::from_bytes(payload.to_vec()).with_rid(rid)))
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
    pub fn update_in_place(&self, rid: Rid, meta: &TupleMeta, tuple: &Tuple) -> Result<()> {
        let mut guard = self.write_page(rid)?;
        TablePage::new(guard.get_data_mut())?.update(rid.slot, meta, tuple.data())
    }

    /// Replaces the tuple at `rid` with `new`, written at `ts`, keeping its
    /// `Rid`. The version it replaces stays readable through a record pushed
    /// to `undo`, which holds the old values of the columns that changed.
    ///
    /// Fails with `PageFull` if `new` does not fit on the tuple's page,
    /// leaving the heap and `undo` unchanged. The caller then deletes the
    /// tuple with `delete_versioned` and inserts `new` at a new `Rid`:
    /// version chains never cross `Rid`s. Fails with `Conflict` if the tuple
    /// is locked by another transaction or was committed after `ts` (see
    /// `check_write`), and with `TupleDeleted` if it is deleted.
    pub fn update_versioned(
        &self,
        rid: Rid,
        new: &Tuple,
        ts: u64,
        schema: &Schema,
        undo: &UndoLog,
    ) -> Result<()> {
        let mut guard = self.write_page(rid)?;
        let mut page = TablePage::new(guard.get_data_mut())?;
        let (head, payload) = page.get(rid.slot)?;
        check_write(rid, &head, ts)?;
        if head.is_deleted {
            return Err(StorageError::TupleDeleted(rid));
        }
        let old = Tuple::from_bytes(payload.to_vec());
        let mut padded = Vec::new();
        let data = if ts & TXN_UNCOMMITTED_BIT != 0 && new.len() < old.len() {
            padded.extend_from_slice(new.data());
            padded.resize(old.len(), 0);
            &padded
        } else {
            new.data()
        };
        page.check_update(rid.slot, data.len())?;

        let old_values = old.decode(schema)?;
        let new_values = new.decode(schema)?;
        let record = UndoRecord {
            rid,
            writer_ts: ts & !TXN_UNCOMMITTED_BIT,
            meta: head,
            delta: undo_log::diff(old_values, &new_values),
        };
        // The record is in the log before the heap points at it. `update`
        // cannot fail after `check_update`; if it did, the record would be
        // orphaned until garbage collection passes `ts`.
        let undo_ptr = undo.push(record);
        let meta = TupleMeta {
            ts,
            undo_ptr,
            is_deleted: false,
            has_nulls: new_values.iter().any(|value| matches!(value, Value::Null)),
        };
        page.update(rid.slot, &meta, data)
    }

    /// Marks the tuple at `rid` deleted as of `ts`. The version before the
    /// delete stays readable through a record pushed to `undo`, with no
    /// changed columns: the tuple's bytes stay on the page. Fails with
    /// `Conflict` as `update_versioned` does, and with `TupleDeleted` if the
    /// tuple is already deleted.
    pub fn delete_versioned(&self, rid: Rid, ts: u64, undo: &UndoLog) -> Result<()> {
        let mut guard = self.write_page(rid)?;
        let mut page = TablePage::new(guard.get_data_mut())?;
        let (head, _) = page.get(rid.slot)?;
        check_write(rid, &head, ts)?;
        if head.is_deleted {
            return Err(StorageError::TupleDeleted(rid));
        }
        let undo_ptr = undo.push(UndoRecord {
            rid,
            writer_ts: ts & !TXN_UNCOMMITTED_BIT,
            meta: head,
            delta: Vec::new(),
        });
        let meta = TupleMeta {
            ts,
            undo_ptr,
            is_deleted: true,
            ..head
        };
        page.update_meta(rid.slot, &meta)
    }

    /// The version of the tuple at `rid` visible at `ts`, with its meta, or
    /// `None` if the tuple does not exist at `ts`. See `undo_log` for the
    /// rule.
    ///
    /// The head is copied out under the page's read guard, and older
    /// versions are rebuilt after the guard is released. A head visible at
    /// `ts` is returned without decoding it or reading `undo`. Fails with
    /// `MissingUndoRecord` if a record on the way was freed, which happens
    /// only if garbage collection ran past a reader still at `ts`.
    pub fn get_at(
        &self,
        rid: Rid,
        ts: u64,
        schema: &Schema,
        undo: &UndoLog,
    ) -> Result<Option<TableRow>> {
        let head = self.get(rid)?;
        undo_log::version_at(rid, head, ts, schema, undo)
    }

    /// Commits the head of the tuple at `rid`, which the running transaction
    /// `tid` wrote: clears `TXN_UNCOMMITTED_BIT` from its `ts`, and releases
    /// any padding a shrinking update kept, which needs `schema` to find the
    /// payload's real length. The undo records are left alone: their
    /// `writer_ts` is already `tid`.
    pub fn stamp_commit(&self, rid: Rid, tid: TxnId, schema: &Schema) -> Result<()> {
        let mut guard = self.write_page(rid)?;
        let mut page = TablePage::new(guard.get_data_mut())?;
        let (head, payload) = page.get(rid.slot)?;
        debug_assert_eq!(
            head.ts,
            tid | TXN_UNCOMMITTED_BIT,
            "tuple {rid} is not locked by transaction {tid}"
        );
        let meta = TupleMeta { ts: tid, ..head };
        let tuple = Tuple::from_bytes(payload.to_vec());
        let len = tuple.encoded_len(schema)?;
        if len < tuple.len() {
            page.update(rid.slot, &meta, &tuple.data()[..len])
        } else {
            page.update_meta(rid.slot, &meta)
        }
    }

    /// Undoes every write the running transaction `tid` made to the tuple at
    /// `rid`, newest first, and puts back the version from before the first
    /// of them. A tuple that `tid` inserted is left deleted at `ts` 0, which
    /// no reader sees, until vacuum frees it.
    ///
    /// The restored version always fits in place, because a shrinking locked
    /// update keeps the tuple's old length. Fails with `MissingUndoRecord` if
    /// one of `tid`'s records is gone, which garbage collection does not do
    /// while `tid` is running.
    pub fn rollback(&self, rid: Rid, tid: TxnId, schema: &Schema, undo: &UndoLog) -> Result<()> {
        let locked = tid | TXN_UNCOMMITTED_BIT;
        let mut guard = self.write_page(rid)?;
        let mut page = TablePage::new(guard.get_data_mut())?;
        let (head, payload) = page.get(rid.slot)?;
        debug_assert_eq!(
            head.ts, locked,
            "tuple {rid} is not locked by transaction {tid}"
        );
        let tuple = Tuple::from_bytes(payload.to_vec());

        let mut meta = head;
        let mut values: Option<Vec<Value>> = None;
        while meta.ts == locked {
            if meta.undo_ptr == INVALID_UNDO_PTR {
                // `tid` inserted the tuple.
                let meta = TupleMeta {
                    ts: INVALID_TXN_ID,
                    is_deleted: true,
                    ..meta
                };
                return page.update_meta(rid.slot, &meta);
            }
            let record = undo_log::older_record(rid, &meta, undo)?;
            if !record.delta.is_empty() {
                let values = match &mut values {
                    Some(values) => values,
                    None => values.insert(tuple.decode(schema)?),
                };
                record.apply(values)?;
            }
            meta = record.meta;
        }

        let data = match values {
            Some(values) => Tuple::encode(schema, &values)?.into_data(),
            // Only deletes, or writes that changed nothing, were undone: the
            // bytes are the old version's, perhaps padded.
            None => {
                let len = tuple.encoded_len(schema)?;
                let mut data = tuple.into_data();
                data.truncate(len);
                data
            }
        };
        page.update(rid.slot, &meta, &data)
    }

    /// Iterates over the live rows of the table. See `TableIterator`.
    pub fn iter(&self) -> Result<TableIterator> {
        TableIterator::new(self)
    }

    /// The last page and its slot count, read together so no insert can
    /// land between the two.
    pub(crate) fn end(&self) -> Result<(PageId, SlotId)> {
        let last_page_id = self.last_page_id.lock();
        let guard = self
            .bpm
            .checked_read_page(*last_page_id, AccessType::Scan)?;
        let slot_count = TablePage::new(guard.get_data())?.slot_count();
        Ok((*last_page_id, slot_count))
    }

    pub(crate) fn bpm(&self) -> &Arc<BufferPoolManager> {
        &self.bpm
    }

    fn write_page(&self, rid: Rid) -> Result<WritePageGuard> {
        self.bpm.checked_write_page(rid.page_id, AccessType::Lookup)
    }
}

/// Checks that a versioned write at `ts` may replace `head`, the newest
/// version of the tuple at `rid`, made under the page's write guard. A head
/// locked by another transaction, or committed after the writer's Tid, fails
/// with `Conflict`. A transaction may write its own locked head again.
fn check_write(rid: Rid, head: &TupleMeta, ts: u64) -> Result<()> {
    if head.ts & TXN_UNCOMMITTED_BIT != 0 {
        if head.ts != ts {
            return Err(StorageError::Conflict {
                rid,
                other: head.ts & !TXN_UNCOMMITTED_BIT,
            });
        }
    } else if head.ts > ts & !TXN_UNCOMMITTED_BIT {
        return Err(StorageError::Conflict {
            rid,
            other: head.ts,
        });
    }
    Ok(())
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

    fn tuple(bytes: impl AsRef<[u8]>) -> Tuple {
        Tuple::from_bytes(bytes.as_ref().to_vec())
    }

    /// The meta and bytes at `rid`, checking the tuple says where it is.
    fn get(heap: &TableHeap, rid: Rid) -> (TupleMeta, Vec<u8>) {
        let (meta, tuple) = heap.get(rid).unwrap();
        assert_eq!(tuple.rid(), Some(rid));
        (meta, tuple.into_data())
    }

    #[test]
    fn insert_and_get() {
        let (_dir, bpm) = setup(4);
        let heap = TableHeap::create(bpm).unwrap();
        let a = heap.insert(&TupleMeta::new(1), &tuple(b"alpha")).unwrap();
        let b = heap.insert(&TupleMeta::new(2), &tuple(b"beta")).unwrap();
        assert_eq!(a, Rid::new(heap.first_page_id(), 0));
        assert_eq!(b, Rid::new(heap.first_page_id(), 1));
        assert_eq!(get(&heap, a), (TupleMeta::new(1), b"alpha".to_vec()));
        assert_eq!(get(&heap, b), (TupleMeta::new(2), b"beta".to_vec()));
    }

    #[test]
    fn insert_spills_onto_new_pages() {
        let (_dir, bpm) = setup(3);
        let heap = TableHeap::create(Arc::clone(&bpm)).unwrap();
        let rids: Vec<Rid> = (0..200u32)
            .map(|i| {
                heap.insert(&TupleMeta::new(i as u64), &tuple([i as u8; 100]))
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
                get(&heap, rid),
                (TupleMeta::new(i as u64), vec![i as u8; 100])
            );
        }

        // Reopening walks the chain to the same last page.
        let reopened = TableHeap::open(bpm, heap.first_page_id()).unwrap();
        assert_eq!(*reopened.last_page_id.lock(), *heap.last_page_id.lock());
        let next = reopened
            .insert(&TupleMeta::new(0), &tuple(b"tail"))
            .unwrap();
        assert_eq!(next.page_id, rids.last().unwrap().page_id);
    }

    #[test]
    fn oversized_tuple_does_not_allocate() {
        let (_dir, bpm) = setup(4);
        let heap = TableHeap::create(bpm).unwrap();
        let first = heap.first_page_id();
        assert!(matches!(
            heap.insert(&TupleMeta::new(1), &tuple(vec![0u8; MAX_PAYLOAD_SIZE + 1])),
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
        let rid = heap.insert(&meta, &tuple(b"row")).unwrap();
        heap.mark_delete(rid, 7, 42).unwrap();

        let expected = TupleMeta {
            ts: 7,
            undo_ptr: 42,
            is_deleted: true,
            has_nulls: true,
        };
        assert_eq!(get(&heap, rid), (expected, b"row".to_vec()));
    }

    #[test]
    fn update_meta() {
        let (_dir, bpm) = setup(4);
        let heap = TableHeap::create(bpm).unwrap();
        let rid = heap
            .insert(&TupleMeta::new(1 << 63 | 5), &tuple(b"row"))
            .unwrap();
        heap.update_meta(rid, &TupleMeta::new(10)).unwrap();
        assert_eq!(get(&heap, rid).0, TupleMeta::new(10));
    }

    #[test]
    fn update_in_place() {
        let (_dir, bpm) = setup(4);
        let heap = TableHeap::create(bpm).unwrap();
        let rid = heap.insert(&TupleMeta::new(1), &tuple(b"short")).unwrap();
        heap.update_in_place(rid, &TupleMeta::new(2), &tuple(b"a much longer row"))
            .unwrap();
        assert_eq!(
            get(&heap, rid),
            (TupleMeta::new(2), b"a much longer row".to_vec())
        );
    }

    #[test]
    fn update_in_place_on_full_page() {
        let (_dir, bpm) = setup(4);
        let heap = TableHeap::create(bpm).unwrap();
        let rid = heap
            .insert(&TupleMeta::new(1), &tuple([1u8; 1500]))
            .unwrap();
        heap.insert(&TupleMeta::new(1), &tuple([2u8; 1500]))
            .unwrap();
        assert!(matches!(
            heap.update_in_place(rid, &TupleMeta::new(2), &tuple([3u8; 3000])),
            Err(StorageError::PageFull)
        ));
        assert_eq!(get(&heap, rid).1, vec![1u8; 1500]);
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
                                heap.insert(&TupleMeta::new(t as u64), &tuple([t, i]))
                                    .unwrap(),
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
            assert_eq!(get(&heap, rid), (TupleMeta::new(t as u64), vec![t, i]));
        }
    }
}
