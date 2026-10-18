//! Slotted page holding table tuples.
//!
//! ```text
//! +----------------------------------------------------------------+
//! | PageHeader (lsn, checksum, page_type)                     16 B |
//! | TableHeader (next_page_id, slot_count, free_lower,             |
//! |              free_upper, hole_bytes, flags)               14 B |
//! +----------------------------------------------------------------+
//! | slot array, grows toward the end →  [off | len | flags] ...   |
//! +----------------------------------------------------------------+
//! |                          free space                            |
//! +----------------------------------------------------------------+
//! |          ← tuples, grow toward the start  [TupleHeader | data] |
//! +----------------------------------------------------------------+
//! ```
//!
//! `free_lower` is the end of the slot array and `free_upper` the start of
//! the tuple area. Bytes in the tuple area that no live tuple uses (left by
//! shrinking updates, moved tuples and freed slots) are counted in
//! `hole_bytes` and recovered by compacting the page.
//!
//! Slot flags record the physical state of a slot (live or free). Whether a
//! tuple is deleted is a logical, MVCC-level fact and lives in the tuple
//! header: a deleted tuple keeps its slot until vacuum frees it.
//!
//! All multi-byte fields are little-endian and unaligned, so a page can be
//! viewed from any byte buffer.

use std::cmp::Reverse;
use std::ops::{Deref, DerefMut};

use common::{INVALID_PAGE_ID, PageId, SlotId};
use zerocopy::byteorder::little_endian::{I32, U16, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::error::{Result, StorageError};
use crate::page::PAGE_SIZE;
use crate::page::page_header::{PageHeader, PageType};

const _: () = assert!(PAGE_SIZE <= u16::MAX as usize);

/// Inserts stop once the page would be this full, so that most growing
/// updates still fit in place.
const INSERT_LIMIT: usize = PAGE_SIZE * 9 / 10;

const TABLE_HEADER_OFFSET: usize = PageHeader::SIZE;
const SLOTS_OFFSET: usize = TABLE_HEADER_OFFSET + size_of::<TableHeader>();
const SLOT_SIZE: usize = size_of::<Slot>();
const TUPLE_HEADER_SIZE: usize = size_of::<TupleHeader>();

/// The largest tuple payload a page accepts: what fits in an empty page under
/// the insert limit. Updates are held to the same bound, so any tuple can
/// always be moved to a fresh page.
pub const MAX_PAYLOAD_SIZE: usize = INSERT_LIMIT - SLOTS_OFFSET - SLOT_SIZE - TUPLE_HEADER_SIZE;

/// `undo_ptr` of a tuple with no older versions.
pub const INVALID_UNDO_PTR: u64 = 0;

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct TableHeader {
    next_page_id: I32,
    slot_count: U16,
    free_lower: U16,
    free_upper: U16,
    hole_bytes: U16,
    /// Reserved.
    flags: U16,
}

const _: () = assert!(size_of::<TableHeader>() == 14);

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct Slot {
    offset: U16,
    /// Length of the whole tuple, header included.
    len: U16,
    flags: u8,
}

const _: () = assert!(size_of::<Slot>() == 5);

const SLOT_LIVE: u8 = 1;

impl Slot {
    fn is_live(&self) -> bool {
        self.flags & SLOT_LIVE != 0
    }
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct TupleHeader {
    ts: U64,
    undo_ptr: U64,
    flags: u8,
}

const _: () = assert!(size_of::<TupleHeader>() == 17);

const TUPLE_DELETED: u8 = 1;
const TUPLE_HAS_NULLS: u8 = 2;

/// The version metadata stored in front of every tuple.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TupleMeta {
    /// Commit timestamp of this version, or the writing transaction's id with
    /// the top bit set while it is uncommitted.
    pub ts: u64,
    /// The newest delta in the undo log, or `INVALID_UNDO_PTR`.
    pub undo_ptr: u64,
    pub is_deleted: bool,
    pub has_nulls: bool,
}

impl TupleMeta {
    pub fn new(ts: u64) -> Self {
        Self {
            ts,
            ..Self::default()
        }
    }
}

impl From<&TupleHeader> for TupleMeta {
    fn from(h: &TupleHeader) -> Self {
        Self {
            ts: h.ts.get(),
            undo_ptr: h.undo_ptr.get(),
            is_deleted: h.flags & TUPLE_DELETED != 0,
            has_nulls: h.flags & TUPLE_HAS_NULLS != 0,
        }
    }
}

impl From<&TupleMeta> for TupleHeader {
    fn from(m: &TupleMeta) -> Self {
        let mut flags = 0;
        if m.is_deleted {
            flags |= TUPLE_DELETED;
        }
        if m.has_nulls {
            flags |= TUPLE_HAS_NULLS;
        }
        Self {
            ts: m.ts.into(),
            undo_ptr: m.undo_ptr.into(),
            flags,
        }
    }
}

/// A view of a table page over borrowed page bytes, such as
/// `guard.get_data()` (read-only) or `guard.get_data_mut()` (read-write).
/// The guard keeps the page pinned and latched for as long as the view lives.
pub struct TablePage<B> {
    data: B,
}

impl<B: Deref<Target = [u8; PAGE_SIZE]>> TablePage<B> {
    /// Views `data` as a table page, checking its page type.
    pub fn new(data: B) -> Result<Self> {
        let (header, _) = PageHeader::read_from_prefix(&data[..]).unwrap();
        match header.page_type() {
            Ok(PageType::Table) => Ok(Self { data }),
            Ok(other) => Err(StorageError::UnexpectedPageType {
                expected: PageType::Table,
                found: other as u32,
            }),
            Err(found) => Err(StorageError::UnexpectedPageType {
                expected: PageType::Table,
                found,
            }),
        }
    }

    pub fn next_page_id(&self) -> PageId {
        self.header().next_page_id.get()
    }

    /// Number of slots in the slot array, free ones included.
    pub fn slot_count(&self) -> SlotId {
        self.header().slot_count.get()
    }

    /// Bytes not used by headers, slots or live tuples, including holes that
    /// compaction would recover.
    pub fn free_space(&self) -> usize {
        PAGE_SIZE - self.used_bytes()
    }

    /// The tuple in `slot`. Deleted tuples are still returned; their meta says
    /// so.
    pub fn get(&self, slot: SlotId) -> Result<(TupleMeta, &[u8])> {
        let s = self.live_slot(slot)?;
        let tuple = &self.data[s.offset.get() as usize..][..s.len.get() as usize];
        let (header, payload) = TupleHeader::ref_from_prefix(tuple).unwrap();
        Ok((TupleMeta::from(header), payload))
    }

    /// Every tuple in slot order, deleted ones included. Free slots are
    /// skipped.
    pub fn iter(&self) -> impl Iterator<Item = (SlotId, TupleMeta, &[u8])> {
        (0..self.slot_count()).filter_map(|slot| {
            let (meta, payload) = self.get(slot).ok()?;
            Some((slot, meta, payload))
        })
    }

    fn header(&self) -> &TableHeader {
        TableHeader::ref_from_bytes(&self.data[TABLE_HEADER_OFFSET..SLOTS_OFFSET]).unwrap()
    }

    fn slot(&self, slot: SlotId) -> &Slot {
        let at = SLOTS_OFFSET + slot as usize * SLOT_SIZE;
        Slot::ref_from_bytes(&self.data[at..at + SLOT_SIZE]).unwrap()
    }

    fn live_slot(&self, slot: SlotId) -> Result<&Slot> {
        if slot >= self.slot_count() {
            return Err(StorageError::InvalidSlot(slot));
        }
        let s = self.slot(slot);
        if !s.is_live() {
            return Err(StorageError::InvalidSlot(slot));
        }
        Ok(s)
    }

    fn contiguous_free(&self) -> usize {
        let h = self.header();
        (h.free_upper.get() - h.free_lower.get()) as usize
    }

    fn used_bytes(&self) -> usize {
        let h = self.header();
        h.free_lower.get() as usize + (PAGE_SIZE - h.free_upper.get() as usize)
            - h.hole_bytes.get() as usize
    }
}

impl<B: DerefMut<Target = [u8; PAGE_SIZE]>> TablePage<B> {
    /// Formats `data` as an empty table page and views it.
    pub fn init(mut data: B) -> Self {
        PageHeader::init(&mut data, PageType::Table);
        let header = TableHeader {
            next_page_id: INVALID_PAGE_ID.into(),
            slot_count: 0.into(),
            free_lower: (SLOTS_OFFSET as u16).into(),
            free_upper: (PAGE_SIZE as u16).into(),
            hole_bytes: 0.into(),
            flags: 0.into(),
        };
        header
            .write_to(&mut data[TABLE_HEADER_OFFSET..SLOTS_OFFSET])
            .unwrap();
        Self { data }
    }

    pub fn set_next_page_id(&mut self, page_id: PageId) {
        self.header_mut().next_page_id = page_id.into();
    }

    /// Stores a tuple and returns its slot, reusing a free slot if there is
    /// one. Fails with `PageFull` if the page would end up past the insert
    /// limit, or `TupleTooLarge` if no page could hold the tuple.
    pub fn insert(&mut self, meta: &TupleMeta, payload: &[u8]) -> Result<SlotId> {
        check_payload_size(payload)?;
        let tuple_len = TUPLE_HEADER_SIZE + payload.len();
        let reuse = (0..self.slot_count()).find(|&i| !self.slot(i).is_live());
        let slot_bytes = if reuse.is_some() { 0 } else { SLOT_SIZE };
        if self.used_bytes() + slot_bytes + tuple_len > INSERT_LIMIT {
            return Err(StorageError::PageFull);
        }
        if self.contiguous_free() < slot_bytes + tuple_len {
            self.compact();
        }

        let slot = match reuse {
            Some(slot) => slot,
            None => {
                let h = self.header_mut();
                let slot = h.slot_count.get();
                h.slot_count += 1;
                h.free_lower += SLOT_SIZE as u16;
                slot
            }
        };
        let offset = self.write_tuple(meta, payload);
        *self.slot_mut(slot) = Slot {
            offset: offset.into(),
            len: (tuple_len as u16).into(),
            flags: SLOT_LIVE,
        };
        Ok(slot)
    }

    /// Replaces the meta of the tuple in `slot`, leaving its data alone. A
    /// logical delete is an update with `is_deleted` set.
    pub fn update_meta(&mut self, slot: SlotId, meta: &TupleMeta) -> Result<()> {
        let offset = self.live_slot(slot)?.offset.get() as usize;
        TupleHeader::from(meta)
            .write_to(&mut self.data[offset..offset + TUPLE_HEADER_SIZE])
            .unwrap();
        Ok(())
    }

    /// Replaces the tuple in `slot`; its slot, and so its `Rid`, stays the
    /// same. A tuple that no longer fits in place moves within the page,
    /// compacting it if needed. Updates may use the room the insert limit
    /// leaves free. Fails with `PageFull` if the page cannot hold the new
    /// version at all.
    pub fn update(&mut self, slot: SlotId, meta: &TupleMeta, payload: &[u8]) -> Result<()> {
        check_payload_size(payload)?;
        let s = self.live_slot(slot)?;
        let (offset, old_len) = (s.offset.get(), s.len.get() as usize);
        let new_len = TUPLE_HEADER_SIZE + payload.len();

        if new_len <= old_len {
            let tuple = &mut self.data[offset as usize..][..new_len];
            TupleHeader::from(meta).write_to_prefix(tuple).unwrap();
            tuple[TUPLE_HEADER_SIZE..].copy_from_slice(payload);
            self.slot_mut(slot).len = (new_len as u16).into();
            self.header_mut().hole_bytes += (old_len - new_len) as u16;
            return Ok(());
        }

        if self.free_space() + old_len < new_len {
            return Err(StorageError::PageFull);
        }
        // Give up the old bytes first, so compaction can reclaim them.
        self.slot_mut(slot).len = 0.into();
        self.header_mut().hole_bytes += old_len as u16;
        if self.contiguous_free() < new_len {
            self.compact();
        }
        let offset = self.write_tuple(meta, payload);
        let s = self.slot_mut(slot);
        s.offset = offset.into();
        s.len = (new_len as u16).into();
        Ok(())
    }

    /// Physically removes the tuple in `slot` and makes the slot available to
    /// later inserts. Only for vacuum, once no index entry or reader can still
    /// reach the tuple: a reused slot gives a new tuple the same `Rid`.
    pub fn free_slot(&mut self, slot: SlotId) -> Result<()> {
        let len = self.live_slot(slot)?.len.get();
        *self.slot_mut(slot) = Slot {
            offset: 0.into(),
            len: 0.into(),
            flags: 0,
        };
        self.header_mut().hole_bytes += len;
        Ok(())
    }

    /// Writes a tuple just below `free_upper`, which must have room for it,
    /// and returns its offset.
    fn write_tuple(&mut self, meta: &TupleMeta, payload: &[u8]) -> u16 {
        let len = TUPLE_HEADER_SIZE + payload.len();
        let h = self.header_mut();
        h.free_upper -= len as u16;
        let offset = h.free_upper.get();
        let tuple = &mut self.data[offset as usize..][..len];
        TupleHeader::from(meta).write_to_prefix(tuple).unwrap();
        tuple[TUPLE_HEADER_SIZE..].copy_from_slice(payload);
        offset
    }

    /// Slides every live tuple to the end of the page, closing the holes
    /// between them. Slots keep their ids; only their offsets change.
    fn compact(&mut self) {
        let mut live: Vec<(SlotId, usize, usize)> = (0..self.slot_count())
            .filter_map(|i| {
                let s = self.slot(i);
                let len = s.len.get() as usize;
                (s.is_live() && len > 0).then(|| (i, s.offset.get() as usize, len))
            })
            .collect();
        // Highest offset first: each tuple then moves toward the end of the
        // page and never over a tuple that has yet to move.
        live.sort_unstable_by_key(|&(_, offset, _)| Reverse(offset));

        let mut upper = PAGE_SIZE;
        for (slot, offset, len) in live {
            upper -= len;
            self.data.copy_within(offset..offset + len, upper);
            self.slot_mut(slot).offset = (upper as u16).into();
        }
        let h = self.header_mut();
        h.free_upper = (upper as u16).into();
        h.hole_bytes = 0.into();
    }

    fn header_mut(&mut self) -> &mut TableHeader {
        TableHeader::mut_from_bytes(&mut self.data[TABLE_HEADER_OFFSET..SLOTS_OFFSET]).unwrap()
    }

    fn slot_mut(&mut self, slot: SlotId) -> &mut Slot {
        let at = SLOTS_OFFSET + slot as usize * SLOT_SIZE;
        Slot::mut_from_bytes(&mut self.data[at..at + SLOT_SIZE]).unwrap()
    }
}

fn check_payload_size(payload: &[u8]) -> Result<()> {
    if payload.len() > MAX_PAYLOAD_SIZE {
        return Err(StorageError::TupleTooLarge {
            size: payload.len(),
            max: MAX_PAYLOAD_SIZE,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::PageBuf;

    fn empty() -> PageBuf {
        let mut buf = PageBuf::zeroed();
        TablePage::init(&mut *buf);
        buf
    }

    #[test]
    fn layout() {
        assert_eq!(SLOTS_OFFSET, 30);
        assert_eq!(MAX_PAYLOAD_SIZE, 3686 - 30 - 5 - 17);
    }

    #[test]
    fn init_and_reopen() {
        let mut buf = PageBuf::zeroed();
        let mut page = TablePage::init(&mut *buf);
        assert_eq!(page.next_page_id(), INVALID_PAGE_ID);
        assert_eq!(page.slot_count(), 0);
        assert_eq!(page.free_space(), PAGE_SIZE - SLOTS_OFFSET);
        page.set_next_page_id(9);

        let page = TablePage::new(&*buf).unwrap();
        assert_eq!(page.next_page_id(), 9);
    }

    #[test]
    fn new_rejects_other_page_types() {
        let buf = PageBuf::zeroed();
        assert!(matches!(
            TablePage::new(&*buf),
            Err(StorageError::UnexpectedPageType {
                expected: PageType::Table,
                found: 0
            })
        ));
    }

    #[test]
    fn insert_and_get() {
        let mut buf = empty();
        let mut page = TablePage::new(&mut *buf).unwrap();
        let meta = TupleMeta {
            ts: 5,
            undo_ptr: 77,
            is_deleted: false,
            has_nulls: true,
        };
        assert_eq!(page.insert(&meta, b"hello").unwrap(), 0);
        assert_eq!(page.insert(&TupleMeta::new(6), b"world!").unwrap(), 1);
        assert_eq!(page.slot_count(), 2);

        assert_eq!(page.get(0).unwrap(), (meta, &b"hello"[..]));
        assert_eq!(page.get(1).unwrap(), (TupleMeta::new(6), &b"world!"[..]));
        assert_eq!(
            page.free_space(),
            PAGE_SIZE - SLOTS_OFFSET - 2 * SLOT_SIZE - 2 * TUPLE_HEADER_SIZE - 11
        );
    }

    #[test]
    fn empty_payload() {
        let mut buf = empty();
        let mut page = TablePage::new(&mut *buf).unwrap();
        let slot = page.insert(&TupleMeta::new(1), b"").unwrap();
        assert_eq!(page.get(slot).unwrap().1, b"");
    }

    #[test]
    fn get_invalid_slot() {
        let buf = empty();
        let page = TablePage::new(&*buf).unwrap();
        assert!(matches!(page.get(0), Err(StorageError::InvalidSlot(0))));
    }

    #[test]
    fn logical_delete_keeps_tuple() {
        let mut buf = empty();
        let mut page = TablePage::new(&mut *buf).unwrap();
        let slot = page.insert(&TupleMeta::new(1), b"row").unwrap();
        let deleted = TupleMeta {
            ts: 2,
            undo_ptr: 40,
            is_deleted: true,
            has_nulls: false,
        };
        page.update_meta(slot, &deleted).unwrap();
        assert_eq!(page.get(slot).unwrap(), (deleted, &b"row"[..]));
    }

    #[test]
    fn insert_stops_at_fill_factor() {
        let mut buf = empty();
        let mut page = TablePage::new(&mut *buf).unwrap();
        let payload = [7u8; 100];
        let mut n = 0;
        while page.insert(&TupleMeta::new(1), &payload).is_ok() {
            n += 1;
        }
        let per_tuple = SLOT_SIZE + TUPLE_HEADER_SIZE + payload.len();
        assert_eq!(n, (INSERT_LIMIT - SLOTS_OFFSET) / per_tuple);
        assert!(matches!(
            page.insert(&TupleMeta::new(1), &payload),
            Err(StorageError::PageFull)
        ));
        assert!(PAGE_SIZE - page.free_space() <= INSERT_LIMIT);
    }

    #[test]
    fn oversized_tuple() {
        let mut buf = empty();
        let mut page = TablePage::new(&mut *buf).unwrap();
        let max = vec![1u8; MAX_PAYLOAD_SIZE];
        let slot = page.insert(&TupleMeta::new(1), &max).unwrap();
        assert_eq!(page.get(slot).unwrap().1, &max[..]);

        let too_big = vec![1u8; MAX_PAYLOAD_SIZE + 1];
        assert!(matches!(
            page.update(slot, &TupleMeta::new(2), &too_big),
            Err(StorageError::TupleTooLarge { .. })
        ));
    }

    #[test]
    fn shrinking_update_stays_in_place() {
        let mut buf = empty();
        let mut page = TablePage::new(&mut *buf).unwrap();
        let slot = page.insert(&TupleMeta::new(1), b"long value").unwrap();
        let other = page.insert(&TupleMeta::new(1), b"neighbour").unwrap();
        let free = page.free_space();

        page.update(slot, &TupleMeta::new(2), b"short").unwrap();
        assert_eq!(page.get(slot).unwrap(), (TupleMeta::new(2), &b"short"[..]));
        assert_eq!(page.get(other).unwrap().1, b"neighbour");
        assert_eq!(page.free_space(), free + 5);
    }

    #[test]
    fn growing_update_moves_tuple() {
        let mut buf = empty();
        let mut page = TablePage::new(&mut *buf).unwrap();
        let a = page.insert(&TupleMeta::new(1), b"aaaa").unwrap();
        let b = page.insert(&TupleMeta::new(1), b"bbbb").unwrap();
        page.update(a, &TupleMeta::new(2), b"aaaaaaaaaaaa").unwrap();
        assert_eq!(
            page.get(a).unwrap(),
            (TupleMeta::new(2), &b"aaaaaaaaaaaa"[..])
        );
        assert_eq!(page.get(b).unwrap().1, b"bbbb");
    }

    #[test]
    fn growing_update_uses_fill_factor_headroom() {
        let mut buf = empty();
        let mut page = TablePage::new(&mut *buf).unwrap();
        let payload = [3u8; 100];
        let mut slots = Vec::new();
        while let Ok(slot) = page.insert(&TupleMeta::new(1), &payload) {
            slots.push(slot);
        }
        // Past the insert limit, but updates can still grow into the headroom.
        let bigger = [4u8; 300];
        page.update(slots[0], &TupleMeta::new(2), &bigger).unwrap();
        assert_eq!(page.get(slots[0]).unwrap().1, &bigger[..]);
        assert!(PAGE_SIZE - page.free_space() > INSERT_LIMIT);
    }

    #[test]
    fn growing_update_compacts_when_fragmented() {
        let mut buf = empty();
        let mut page = TablePage::new(&mut *buf).unwrap();
        let payload = [0u8; 200];
        let mut slots = Vec::new();
        for i in 0u8.. {
            match page.insert(&TupleMeta::new(i as u64), &[i; 200]) {
                Ok(slot) => slots.push(slot),
                Err(_) => break,
            }
        }
        // Shrink every other tuple: lots of free bytes, none contiguous enough.
        for &slot in slots.iter().step_by(2) {
            page.update(slot, &TupleMeta::new(99), &payload[..10])
                .unwrap();
        }
        let target = slots[1];
        let grown = [0xabu8; 1200];
        assert!(page.contiguous_free() < TUPLE_HEADER_SIZE + grown.len());
        page.update(target, &TupleMeta::new(100), &grown).unwrap();

        assert_eq!(page.get(target).unwrap(), (TupleMeta::new(100), &grown[..]));
        for (i, &slot) in slots.iter().enumerate() {
            if slot == target {
                continue;
            }
            let (meta, data) = page.get(slot).unwrap();
            if i % 2 == 0 {
                assert_eq!((meta.ts, data), (99, &payload[..10]));
            } else {
                assert_eq!((meta.ts, data), (i as u64, &[i as u8; 200][..]));
            }
        }
    }

    #[test]
    fn update_that_cannot_fit_fails_and_leaves_tuple() {
        let mut buf = empty();
        let mut page = TablePage::new(&mut *buf).unwrap();
        let payload = [5u8; 1000];
        let a = page.insert(&TupleMeta::new(1), &payload).unwrap();
        let _b = page.insert(&TupleMeta::new(1), &payload).unwrap();
        let _c = page.insert(&TupleMeta::new(1), &payload).unwrap();
        let grown = [6u8; 2100];
        assert!(matches!(
            page.update(a, &TupleMeta::new(2), &grown),
            Err(StorageError::PageFull)
        ));
        assert_eq!(page.get(a).unwrap(), (TupleMeta::new(1), &payload[..]));
    }

    #[test]
    fn free_slot_is_reused() {
        let mut buf = empty();
        let mut page = TablePage::new(&mut *buf).unwrap();
        let a = page.insert(&TupleMeta::new(1), b"first").unwrap();
        let b = page.insert(&TupleMeta::new(1), b"second").unwrap();
        let free = page.free_space();

        page.free_slot(a).unwrap();
        assert!(matches!(page.get(a), Err(StorageError::InvalidSlot(0))));
        assert!(matches!(
            page.free_slot(a),
            Err(StorageError::InvalidSlot(0))
        ));
        assert_eq!(page.free_space(), free + TUPLE_HEADER_SIZE + 5);

        assert_eq!(page.insert(&TupleMeta::new(2), b"third").unwrap(), a);
        assert_eq!(page.slot_count(), 2);
        assert_eq!(page.get(a).unwrap().1, b"third");
        assert_eq!(page.get(b).unwrap().1, b"second");
    }

    #[test]
    fn insert_compacts_freed_space() {
        let mut buf = empty();
        let mut page = TablePage::new(&mut *buf).unwrap();
        let payload = [1u8; 1000];
        let a = page.insert(&TupleMeta::new(1), &payload).unwrap();
        let b = page.insert(&TupleMeta::new(1), &payload).unwrap();
        page.insert(&TupleMeta::new(1), &payload).unwrap();
        assert!(matches!(
            page.insert(&TupleMeta::new(1), &payload),
            Err(StorageError::PageFull)
        ));

        page.free_slot(a).unwrap();
        page.free_slot(b).unwrap();
        let big = [2u8; 1500];
        let slot = page.insert(&TupleMeta::new(2), &big).unwrap();
        assert_eq!(page.get(slot).unwrap().1, &big[..]);
    }

    #[test]
    fn through_buffer_pool() {
        use crate::buffer::arc_replacer::AccessType;
        use crate::buffer::buffer_pool_manager::BufferPoolManager;
        use crate::disk::disk_manager::DiskManager;
        use std::sync::Arc;

        let dir = tempfile::TempDir::new().unwrap();
        let dm = Arc::new(DiskManager::new(dir.path().join("test.db")).unwrap());
        let bpm = BufferPoolManager::new(2, dm);
        let pid = bpm.new_page();
        {
            let mut guard = bpm.write_page(pid, AccessType::Unknown);
            let mut page = TablePage::init(guard.get_data_mut());
            page.insert(&TupleMeta::new(1), b"persisted").unwrap();
        }
        // Push the page out of the pool so it is read back from disk.
        for _ in 0..2 {
            let other = bpm.new_page();
            drop(bpm.write_page(other, AccessType::Unknown));
        }
        let guard = bpm.read_page(pid, AccessType::Unknown);
        let page = TablePage::new(guard.get_data()).unwrap();
        assert_eq!(page.get(0).unwrap().1, b"persisted");
    }
}
