//! What B+tree leaf and internal pages share: the comparator that orders
//! their keys and the slotted layout that stores them.
//!
//! ```text
//! +----------------------------------------------------------------+
//! | PageHeader (lsn, checksum, page_type)                     16 B |
//! | SlottedHeader (size, key_start, hole_bytes)                6 B |
//! | the page's own header fields and arrays                        |
//! +----------------------------------------------------------------+
//! | SLOT(0) | SLOT(1) | ... | SLOT(size - 1)    grows toward the end → |
//! +----------------------------------------------------------------+
//! |                          free space                            |
//! +----------------------------------------------------------------+
//! |              ← key bytes, grow toward the start                |
//! +----------------------------------------------------------------+
//! ```
//!
//! A slot is the offset and length of its key (u16 each), followed by the
//! page's value for that key: a child page id or a record id. Slots are kept
//! in key order, so a search is a binary search over the slot array. Key
//! bytes sit in the key area, from `key_start` to the end of the page, in
//! whatever order they were written.
//!
//! Removing a key leaves its bytes behind as a hole, counted in
//! `hole_bytes`. Holes count as free space: the key area is compacted when a
//! write needs them.
//!
//! A page is full when the next entry, its slot and key together, does not
//! fit in the free space, and it splits so both halves hold about the same
//! number of bytes. No key may be longer than a quarter of a page's capacity,
//! less a slot (`max_key_size`): then a page that has no room for one more
//! entry holds at least four, and each half of its split has room for the
//! entry that did not fit.
//!
//! All multi-byte fields are little-endian and nothing is aligned, so a page
//! can be viewed from any byte buffer.

use std::cmp::{Ordering, Reverse};
use std::ops::Range;

use zerocopy::byteorder::little_endian::U16;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::error::{Result, StorageError};
use crate::page::PAGE_SIZE;
use crate::page::page_header::PageHeader;

const _: () = assert!(PAGE_SIZE <= u16::MAX as usize);

/// Orders the keys of a B+tree.
///
/// It must be a total order, and a tree must always be used with the same
/// one: its pages are sorted by it. Keys that compare equal are the same key,
/// even if their bytes differ.
pub trait KeyComparator {
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering;
}

/// Orders keys by their bytes, as `memcmp` does, so a shorter key sorts
/// before any longer key it is a prefix of.
#[derive(Debug, Clone, Copy, Default)]
pub struct BytewiseComparator;

impl KeyComparator for BytewiseComparator {
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        a.cmp(b)
    }
}

/// The header fields every B+tree page starts with, right after the
/// `PageHeader`.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct SlottedHeader {
    /// Number of slots.
    size: U16,
    /// Start of the key area, which runs to the end of the page.
    key_start: U16,
    /// Bytes in the key area that no slot points to.
    hole_bytes: U16,
}

const _: () = assert!(size_of::<SlottedHeader>() == 6);

const SLOTTED_HEADER_OFFSET: usize = PageHeader::SIZE;
/// Where a page's own header fields start.
pub(crate) const SLOTTED_HEADER_END: usize = SLOTTED_HEADER_OFFSET + size_of::<SlottedHeader>();

/// Where a slot's key is.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct KeyRef {
    offset: U16,
    len: U16,
}

const KEY_REF_SIZE: usize = size_of::<KeyRef>();

/// The slotted layout of one kind of B+tree page: where its slot array
/// starts and how large its values are. Pages call these with their bytes;
/// checking that an entry fits, and returning an error if it does not, is
/// up to them.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SlotLayout {
    slots_offset: usize,
    slot_size: usize,
}

impl SlotLayout {
    pub(crate) const fn new(slots_offset: usize, value_size: usize) -> Self {
        assert!(slots_offset >= SLOTTED_HEADER_END);
        Self {
            slots_offset,
            slot_size: KEY_REF_SIZE + value_size,
        }
    }

    /// Bytes for slots and keys in an empty page.
    pub(crate) const fn capacity(self) -> usize {
        PAGE_SIZE - self.slots_offset
    }

    /// Bytes an entry with a key of `key_len` bytes takes: its slot and key.
    pub(crate) const fn entry_size(self, key_len: usize) -> usize {
        self.slot_size + key_len
    }

    /// The longest key a page accepts. See the module docs.
    pub(crate) const fn max_key_size(self) -> usize {
        self.capacity() / 4 - self.slot_size
    }

    /// Formats the slotted header of an empty page.
    pub(crate) fn init(self, data: &mut [u8; PAGE_SIZE]) {
        let header = SlottedHeader {
            size: 0.into(),
            key_start: (PAGE_SIZE as u16).into(),
            hole_bytes: 0.into(),
        };
        header
            .write_to(&mut data[SLOTTED_HEADER_OFFSET..SLOTTED_HEADER_END])
            .unwrap();
    }

    pub(crate) fn size(self, data: &[u8; PAGE_SIZE]) -> usize {
        header(data).size.get() as usize
    }

    /// Bytes a new entry can use, holes included.
    pub(crate) fn free_space(self, data: &[u8; PAGE_SIZE]) -> usize {
        self.contiguous_free(data) + header(data).hole_bytes.get() as usize
    }

    /// Bytes the entries take: their slots and keys.
    pub(crate) fn used_space(self, data: &[u8; PAGE_SIZE]) -> usize {
        self.capacity() - self.free_space(data)
    }

    pub(crate) fn key(self, data: &[u8; PAGE_SIZE], index: usize) -> &[u8] {
        let key = self.key_ref(data, index);
        let offset = key.offset.get() as usize;
        &data[offset..offset + key.len.get() as usize]
    }

    pub(crate) fn value(self, data: &[u8; PAGE_SIZE], index: usize) -> &[u8] {
        &data[self.value_range(index)]
    }

    pub(crate) fn value_mut(self, data: &mut [u8; PAGE_SIZE], index: usize) -> &mut [u8] {
        &mut data[self.value_range(index)]
    }

    /// Inserts an entry at slot `index`, moving the slots from `index` on
    /// one to the right.
    ///
    /// # Panics
    /// If `index` is past the end or the entry does not fit.
    pub(crate) fn insert(self, data: &mut [u8; PAGE_SIZE], index: usize, key: &[u8], value: &[u8]) {
        let size = self.size(data);
        assert!(index <= size, "slot {index} out of range 0..={size}");
        assert_eq!(value.len(), self.slot_size - KEY_REF_SIZE, "value size");
        let entry = self.entry_size(key.len());
        assert!(
            entry <= self.free_space(data),
            "no room for an entry of {entry} bytes"
        );
        if self.contiguous_free(data) < entry {
            self.compact(data);
        }

        let offset = self.append_key(data, key);
        let slots = self.slot_range(index).start..self.slot_range(size).start;
        data.copy_within(slots.clone(), slots.start + self.slot_size);
        let key_ref = KeyRef {
            offset: (offset as u16).into(),
            len: (key.len() as u16).into(),
        };
        let slot = self.slot_range(index);
        key_ref
            .write_to(&mut data[slot.start..slot.start + KEY_REF_SIZE])
            .unwrap();
        data[slot.start + KEY_REF_SIZE..slot.end].copy_from_slice(value);
        header_mut(data).size = (size as u16 + 1).into();
    }

    /// Removes slot `index`, moving the slots after it one to the left. Its
    /// key becomes a hole.
    pub(crate) fn remove(self, data: &mut [u8; PAGE_SIZE], index: usize) {
        let size = self.size(data);
        assert!(index < size, "slot {index} out of range 0..{size}");
        let len = self.key_ref(data, index).len.get();
        let slots = self.slot_range(index + 1).start..self.slot_range(size).start;
        data.copy_within(slots, self.slot_range(index).start);
        let header = header_mut(data);
        header.size = (size as u16 - 1).into();
        header.hole_bytes += len;
    }

    /// Replaces the key of slot `index`. The old key becomes a hole first,
    /// so its bytes count toward the room for the new one.
    ///
    /// # Panics
    /// If the new key does not fit.
    pub(crate) fn set_key(self, data: &mut [u8; PAGE_SIZE], index: usize, key: &[u8]) {
        let old_len = self.key_ref(data, index).len.get();
        self.key_ref_mut(data, index).len = 0.into();
        header_mut(data).hole_bytes += old_len;
        assert!(
            key.len() <= self.free_space(data),
            "no room for a key of {} bytes",
            key.len()
        );
        if self.contiguous_free(data) < key.len() {
            self.compact(data);
        }
        let offset = self.append_key(data, key);
        let key_ref = self.key_ref_mut(data, index);
        key_ref.offset = (offset as u16).into();
        key_ref.len = (key.len() as u16).into();
    }

    /// Drops the slots from `len` on. Their keys become holes.
    pub(crate) fn truncate(self, data: &mut [u8; PAGE_SIZE], len: usize) {
        let size = self.size(data);
        assert!(len <= size, "truncate to {len} of {size} slots");
        let freed: u16 = (len..size)
            .map(|index| self.key_ref(data, index).len.get())
            .sum();
        let header = header_mut(data);
        header.size = (len as u16).into();
        header.hole_bytes += freed;
    }

    /// Where to split the page so both halves hold about the same number of
    /// bytes: the slot boundary closest to half the page's entry bytes, kept
    /// in `1..size` so neither half is empty. Each half then holds at most
    /// half the bytes plus half an entry.
    ///
    /// # Panics
    /// If the page has fewer than 2 slots.
    pub(crate) fn split_index(self, data: &[u8; PAGE_SIZE]) -> usize {
        let size = self.size(data);
        assert!(size >= 2, "cannot split a page with {size} slots");
        let half = self.used_space(data) / 2;
        // The first boundary at or past half, and the bytes before it and
        // before the boundary one slot earlier.
        let (mut index, mut before, mut previous) = (0, 0, 0);
        while before < half {
            previous = before;
            before += self.entry_size(self.key_ref(data, index).len.get() as usize);
            index += 1;
        }
        if index > 0 && half - previous < before - half {
            index -= 1;
        }
        index.clamp(1, size - 1)
    }

    /// Bytes between the slot array and the key area.
    fn contiguous_free(self, data: &[u8; PAGE_SIZE]) -> usize {
        let slots_end = self.slot_range(self.size(data)).start;
        header(data).key_start.get() as usize - slots_end
    }

    /// Writes `key` right before the key area, which must have room for it,
    /// and returns its offset.
    fn append_key(self, data: &mut [u8; PAGE_SIZE], key: &[u8]) -> usize {
        let offset = header(data).key_start.get() as usize - key.len();
        data[offset..offset + key.len()].copy_from_slice(key);
        header_mut(data).key_start = (offset as u16).into();
        offset
    }

    /// Packs the keys against the end of the page, removing every hole.
    fn compact(self, data: &mut [u8; PAGE_SIZE]) {
        let mut slots: Vec<usize> = (0..self.size(data)).collect();
        // Highest key first: each key moves toward the end of the page, so it
        // only lands on bytes already moved or freed.
        slots.sort_unstable_by_key(|&index| Reverse(self.key_ref(data, index).offset.get()));
        let mut end = PAGE_SIZE;
        for index in slots {
            let key_ref = self.key_ref(data, index);
            let (offset, len) = (key_ref.offset.get() as usize, key_ref.len.get() as usize);
            let start = end - len;
            data.copy_within(offset..offset + len, start);
            self.key_ref_mut(data, index).offset = (start as u16).into();
            end = start;
        }
        let header = header_mut(data);
        header.key_start = (end as u16).into();
        header.hole_bytes = 0.into();
    }

    fn slot_range(self, index: usize) -> Range<usize> {
        let start = self.slots_offset + index * self.slot_size;
        start..start + self.slot_size
    }

    fn value_range(self, index: usize) -> Range<usize> {
        let slot = self.slot_range(index);
        slot.start + KEY_REF_SIZE..slot.end
    }

    fn key_ref(self, data: &[u8; PAGE_SIZE], index: usize) -> &KeyRef {
        let start = self.slot_range(index).start;
        KeyRef::ref_from_bytes(&data[start..start + KEY_REF_SIZE]).unwrap()
    }

    fn key_ref_mut(self, data: &mut [u8; PAGE_SIZE], index: usize) -> &mut KeyRef {
        let start = self.slot_range(index).start;
        KeyRef::mut_from_bytes(&mut data[start..start + KEY_REF_SIZE]).unwrap()
    }
}

/// Fails with `KeyTooLarge` if `key` is longer than `max`.
pub(crate) fn check_key_size(key: &[u8], max: usize) -> Result<()> {
    if key.len() > max {
        return Err(StorageError::KeyTooLarge {
            size: key.len(),
            max,
        });
    }
    Ok(())
}

fn header(data: &[u8; PAGE_SIZE]) -> &SlottedHeader {
    SlottedHeader::ref_from_bytes(&data[SLOTTED_HEADER_OFFSET..SLOTTED_HEADER_END]).unwrap()
}

fn header_mut(data: &mut [u8; PAGE_SIZE]) -> &mut SlottedHeader {
    SlottedHeader::mut_from_bytes(&mut data[SLOTTED_HEADER_OFFSET..SLOTTED_HEADER_END]).unwrap()
}
