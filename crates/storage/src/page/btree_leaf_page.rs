//! B+tree leaf page, mapping variable-length keys to record ids.
//!
//! ```text
//! +----------------------------------------------------------------+
//! | PageHeader (lsn, checksum, page_type)                     16 B |
//! | SlottedHeader (size, key_start, hole_bytes)                6 B |
//! | LeafHeader (next_page_id, num_tombstones)                  6 B |
//! +----------------------------------------------------------------+
//! | TOMB(0) | TOMB(1) | ... | TOMB(NUM_TOMBS - 1)                  |
//! +----------------------------------------------------------------+
//! | SLOT(0) | SLOT(1) | ... | SLOT(size - 1)    grows toward the end → |
//! +----------------------------------------------------------------+
//! |                          free space                            |
//! +----------------------------------------------------------------+
//! |              ← key bytes, grow toward the start                |
//! +----------------------------------------------------------------+
//! ```
//!
//! `SLOT(i)` holds where `KEY(i)` is and its record id (10 B). The slotted
//! layout, shared with internal pages, is described in `btree_page`. Keys
//! are in increasing order, as the tree's `KeyComparator` orders them.
//! `next_page_id` links the leaves left to right for range scans.
//!
//! Deleting a key does not remove it right away: its slot index is appended
//! to the tombstone buffer, which holds up to `NUM_TOMBS` indexes, oldest
//! first. A tombstoned key keeps its slot and bytes and still counts in
//! `size`, but lookups skip it. Deleting when the buffer is full removes the
//! oldest tombstoned key for good to make room, and inserting a tombstoned
//! key revives it in place. With `NUM_TOMBS = 0` deletes take effect at once.

use std::ops::{Deref, DerefMut, Range};

use common::{INVALID_PAGE_ID, PageId, Rid};
use zerocopy::byteorder::little_endian::{I32, U16};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::error::{Result, StorageError};
use crate::page::PAGE_SIZE;
use crate::page::btree_page::{KeyComparator, SLOTTED_HEADER_END, SlotLayout, check_key_size};
use crate::page::page_header::{PageHeader, PageType};

const LEAF_HEADER_OFFSET: usize = SLOTTED_HEADER_END;
const TOMBS_OFFSET: usize = LEAF_HEADER_OFFSET + size_of::<LeafHeader>();
const TOMB_SIZE: usize = size_of::<U16>();
const RID_SIZE: usize = size_of::<RidBytes>();

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct LeafHeader {
    next_page_id: I32,
    num_tombstones: U16,
}

const _: () = assert!(size_of::<LeafHeader>() == 6);

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct RidBytes {
    page_id: I32,
    slot: U16,
}

const _: () = assert!(size_of::<RidBytes>() == 6);

/// A view of a B+tree leaf page with room for `NUM_TOMBS` tombstones over
/// borrowed page bytes `BufT`, such as `guard.get_data()` (read-only) or
/// `guard.get_data_mut()` (read-write).
///
/// A page must always be viewed with the `NUM_TOMBS` it was initialized
/// with: it decides where the slot array starts.
pub struct BtreeLeafPage<BufT, const NUM_TOMBS: usize> {
    data: BufT,
}

impl<BufT, const NUM_TOMBS: usize> BtreeLeafPage<BufT, NUM_TOMBS> {
    const LAYOUT: SlotLayout = SlotLayout::new(TOMBS_OFFSET + NUM_TOMBS * TOMB_SIZE, RID_SIZE);

    /// Bytes for entries in an empty page.
    pub const CAPACITY: usize = Self::LAYOUT.capacity();

    /// The longest key the page accepts.
    pub const MAX_KEY_SIZE: usize = Self::LAYOUT.max_key_size();

    /// Bytes an entry with a key of `key_len` bytes takes: its slot and key.
    pub const fn entry_size(key_len: usize) -> usize {
        Self::LAYOUT.entry_size(key_len)
    }

    fn tomb_range(pos: usize) -> Range<usize> {
        let start = TOMBS_OFFSET + pos * TOMB_SIZE;
        start..start + TOMB_SIZE
    }
}

impl<BufT: Deref<Target = [u8; PAGE_SIZE]>, const NUM_TOMBS: usize> BtreeLeafPage<BufT, NUM_TOMBS> {
    /// Views `data` as a leaf page, checking its page type.
    pub fn new(data: BufT) -> Result<Self> {
        let (header, _) = PageHeader::read_from_prefix(&data[..]).unwrap();
        match header.page_type() {
            Ok(PageType::BTreeLeaf) => Ok(Self { data }),
            Ok(other) => Err(StorageError::UnexpectedPageType {
                expected: PageType::BTreeLeaf,
                found: other as u32,
            }),
            Err(found) => Err(StorageError::UnexpectedPageType {
                expected: PageType::BTreeLeaf,
                found,
            }),
        }
    }

    /// Number of keys, tombstoned ones included.
    pub fn size(&self) -> usize {
        Self::LAYOUT.size(&self.data)
    }

    /// Number of keys that are not tombstoned.
    pub fn live_size(&self) -> usize {
        self.size() - self.num_tombstones()
    }

    /// Bytes a new entry can use.
    pub fn free_space(&self) -> usize {
        Self::LAYOUT.free_space(&self.data)
    }

    /// Bytes the entries take, tombstoned ones included.
    pub fn used_space(&self) -> usize {
        Self::LAYOUT.used_space(&self.data)
    }

    /// Whether a key of `key_len` bytes can be inserted without a split.
    pub fn has_room_for(&self, key_len: usize) -> bool {
        Self::entry_size(key_len) <= self.free_space()
    }

    /// Whether a non-root page is less than half full and has to borrow from
    /// or merge with a sibling.
    pub fn is_underfull(&self) -> bool {
        self.used_space() < Self::CAPACITY / 2
    }

    /// The next leaf to the right, or `INVALID_PAGE_ID` for the last one.
    pub fn next_page_id(&self) -> PageId {
        self.header().next_page_id.get()
    }

    pub fn num_tombstones(&self) -> usize {
        self.header().num_tombstones.get() as usize
    }

    /// Slot indexes of the tombstoned keys, oldest first.
    pub fn tombstones(&self) -> impl Iterator<Item = usize> + '_ {
        (0..self.num_tombstones()).map(|pos| self.read_tomb(pos))
    }

    pub fn is_tombstone(&self, index: usize) -> bool {
        self.tomb_position(index).is_some()
    }

    /// The key at `index`, which must be in `0..size`. It may be tombstoned.
    pub fn key_at(&self, index: usize) -> &[u8] {
        self.check_index(index);
        Self::LAYOUT.key(&self.data, index)
    }

    /// The record id at `index`, which must be in `0..size`. It may be
    /// tombstoned.
    pub fn value_at(&self, index: usize) -> Rid {
        self.check_index(index);
        self.read_value(index)
    }

    /// The record id of `key`, unless it is missing or tombstoned.
    pub fn lookup(&self, key: &[u8], cmp: &impl KeyComparator) -> Option<Rid> {
        let index = self.key_index(key, cmp);
        (self.holds_at(index, key, cmp) && !self.is_tombstone(index))
            .then(|| self.read_value(index))
    }

    /// Bytes `remove(key)` would free: those of the entry it removes for
    /// good, which is `key`'s with `NUM_TOMBS = 0` and the oldest tombstoned
    /// key's when the tombstone buffer is full. 0 if it only tombstones
    /// `key`, or if `key` is missing or already tombstoned.
    pub fn bytes_freed_by_remove(&self, key: &[u8], cmp: &impl KeyComparator) -> usize {
        let index = self.key_index(key, cmp);
        if !self.holds_at(index, key, cmp) || self.is_tombstone(index) {
            return 0;
        }
        let removed = if NUM_TOMBS == 0 {
            index
        } else if self.num_tombstones() == NUM_TOMBS {
            self.read_tomb(0)
        } else {
            return 0;
        };
        Self::entry_size(Self::LAYOUT.key(&self.data, removed).len())
    }

    /// Index of the first key `>= key`, tombstoned or not, or `size` if there
    /// is none. A range scan starts here.
    pub fn key_index(&self, key: &[u8], cmp: &impl KeyComparator) -> usize {
        let (mut lo, mut hi) = (0, self.size());
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if cmp.compare(Self::LAYOUT.key(&self.data, mid), key).is_lt() {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    /// Whether slot `index`, as found by `key_index`, holds `key`, tombstoned
    /// or not.
    fn holds_at(&self, index: usize, key: &[u8], cmp: &impl KeyComparator) -> bool {
        index < self.size()
            && cmp
                .compare(Self::LAYOUT.key(&self.data, index), key)
                .is_eq()
    }

    /// Position in the tombstone buffer of the tombstone for slot `index`.
    fn tomb_position(&self, index: usize) -> Option<usize> {
        (0..self.num_tombstones()).find(|&pos| self.read_tomb(pos) == index)
    }

    fn read_value(&self, index: usize) -> Rid {
        let rid = RidBytes::ref_from_bytes(Self::LAYOUT.value(&self.data, index)).unwrap();
        Rid::new(rid.page_id.get(), rid.slot.get())
    }

    fn read_tomb(&self, pos: usize) -> usize {
        U16::ref_from_bytes(&self.data[Self::tomb_range(pos)])
            .unwrap()
            .get() as usize
    }

    fn check_index(&self, index: usize) {
        assert!(
            index < self.size(),
            "key index {index} out of range 0..{}",
            self.size()
        );
    }

    fn header(&self) -> &LeafHeader {
        LeafHeader::ref_from_bytes(&self.data[LEAF_HEADER_OFFSET..TOMBS_OFFSET]).unwrap()
    }
}

impl<BufT: DerefMut<Target = [u8; PAGE_SIZE]>, const NUM_TOMBS: usize>
    BtreeLeafPage<BufT, NUM_TOMBS>
{
    /// Formats `data` as an empty leaf page.
    pub fn init(mut data: BufT) -> Self {
        PageHeader::init(&mut data, PageType::BTreeLeaf);
        Self::LAYOUT.init(&mut data);
        let header = LeafHeader {
            next_page_id: INVALID_PAGE_ID.into(),
            num_tombstones: 0.into(),
        };
        header
            .write_to(&mut data[LEAF_HEADER_OFFSET..TOMBS_OFFSET])
            .unwrap();
        Self { data }
    }

    pub fn set_next_page_id(&mut self, page_id: PageId) {
        self.header_mut().next_page_id = page_id.into();
    }

    /// Inserts `key` with `rid` and returns its index. A tombstoned `key` is
    /// revived in place with the new `rid`.
    ///
    /// Fails with `KeyTooLarge` if `key` is longer than `MAX_KEY_SIZE`, with
    /// `DuplicateKey` if `key` is already live, and with `PageFull` if the
    /// entry does not fit; the caller splits the page first, or makes room
    /// with `purge_tombstones`.
    pub fn insert(&mut self, key: &[u8], rid: Rid, cmp: &impl KeyComparator) -> Result<usize> {
        check_key_size(key, Self::MAX_KEY_SIZE)?;
        let index = self.key_index(key, cmp);
        if self.holds_at(index, key, cmp) {
            let Some(pos) = self.tomb_position(index) else {
                return Err(StorageError::DuplicateKey);
            };
            self.remove_tomb(pos);
            self.write_value(index, rid);
            return Ok(index);
        }
        if !self.has_room_for(key.len()) {
            return Err(StorageError::PageFull);
        }
        for pos in 0..self.num_tombstones() {
            let t = self.read_tomb(pos);
            if t >= index {
                self.write_tomb(pos, t + 1);
            }
        }
        Self::LAYOUT.insert(&mut self.data, index, key, rid_bytes(rid).as_bytes());
        Ok(index)
    }

    /// Deletes `key` by tombstoning it, and returns its record id, or `None`
    /// if it is missing or already tombstoned. If the tombstone buffer is
    /// full, the oldest tombstoned key is removed for good first.
    pub fn remove(&mut self, key: &[u8], cmp: &impl KeyComparator) -> Option<Rid> {
        let mut index = self.key_index(key, cmp);
        if !self.holds_at(index, key, cmp) || self.is_tombstone(index) {
            return None;
        }
        let rid = self.read_value(index);
        if NUM_TOMBS == 0 {
            self.remove_slot(index);
            return Some(rid);
        }
        if self.num_tombstones() == NUM_TOMBS {
            let oldest = self.read_tomb(0);
            self.remove_slot(oldest);
            if index > oldest {
                index -= 1;
            }
        }
        let count = self.num_tombstones();
        self.write_tomb(count, index);
        self.header_mut().num_tombstones = (count as u16 + 1).into();
        Some(rid)
    }

    /// Removes the key at `index` for good, tombstoned or not, and returns it
    /// with its record id. Used to move keys between siblings.
    pub fn remove_at(&mut self, index: usize) -> (Vec<u8>, Rid) {
        self.check_index(index);
        let removed = (self.key_at(index).to_vec(), self.read_value(index));
        self.remove_slot(index);
        removed
    }

    /// Removes every tombstoned key for good.
    pub fn purge_tombstones(&mut self) {
        let mut tombs: Vec<usize> = self.tombstones().collect();
        // Highest first, so removing one does not move the others.
        tombs.sort_unstable_by(|a, b| b.cmp(a));
        for index in tombs {
            self.remove_slot(index);
        }
    }

    /// Moves the upper half of the keys, by bytes, to `right`, an empty leaf
    /// stored in page `right_page_id`, and links `right` in after this page.
    /// Tombstones move with their keys. The first key of `right` separates
    /// the two pages in their parent.
    ///
    /// # Panics
    /// If `right` is not empty or this page holds fewer than 2 keys.
    pub fn split_to<RightBufT: DerefMut<Target = [u8; PAGE_SIZE]>>(
        &mut self,
        right: &mut BtreeLeafPage<RightBufT, NUM_TOMBS>,
        right_page_id: PageId,
    ) {
        assert_eq!(right.size(), 0, "split into a leaf that is not empty");
        let at = Self::LAYOUT.split_index(&self.data);
        for index in at..self.size() {
            Self::LAYOUT.insert(
                &mut right.data,
                index - at,
                Self::LAYOUT.key(&self.data, index),
                Self::LAYOUT.value(&self.data, index),
            );
        }
        Self::LAYOUT.truncate(&mut self.data, at);

        let (kept, moved): (Vec<usize>, Vec<usize>) = self.tombstones().partition(|&t| t < at);
        self.set_tombstones(&kept);
        right.set_tombstones(&moved.iter().map(|t| t - at).collect::<Vec<_>>());

        right.set_next_page_id(self.next_page_id());
        self.set_next_page_id(right_page_id);
    }

    /// Moves every key to the end of `left`, the leaf before this one, and
    /// links `left` to this page's next leaf, leaving this page empty.
    ///
    /// # Panics
    /// If either page holds tombstones (purge them first) or the keys do not
    /// fit in `left`.
    pub fn move_all_to<LeftBufT: DerefMut<Target = [u8; PAGE_SIZE]>>(
        &mut self,
        left: &mut BtreeLeafPage<LeftBufT, NUM_TOMBS>,
    ) {
        self.assert_no_tombstones();
        left.assert_no_tombstones();
        for index in 0..self.size() {
            let end = left.size();
            Self::LAYOUT.insert(
                &mut left.data,
                end,
                Self::LAYOUT.key(&self.data, index),
                Self::LAYOUT.value(&self.data, index),
            );
        }
        Self::LAYOUT.truncate(&mut self.data, 0);
        left.set_next_page_id(self.next_page_id());
    }

    /// Moves the first key to the end of `left`, the leaf before this one.
    ///
    /// # Panics
    /// If this page is empty, either page holds tombstones, or the key does
    /// not fit in `left`.
    pub fn move_first_to_end_of<LeftBufT: DerefMut<Target = [u8; PAGE_SIZE]>>(
        &mut self,
        left: &mut BtreeLeafPage<LeftBufT, NUM_TOMBS>,
    ) {
        self.assert_no_tombstones();
        left.assert_no_tombstones();
        self.check_index(0);
        let end = left.size();
        Self::LAYOUT.insert(
            &mut left.data,
            end,
            Self::LAYOUT.key(&self.data, 0),
            Self::LAYOUT.value(&self.data, 0),
        );
        Self::LAYOUT.remove(&mut self.data, 0);
    }

    /// Moves the last key to the front of `right`, the leaf after this one.
    ///
    /// # Panics
    /// If this page is empty, either page holds tombstones, or the key does
    /// not fit in `right`.
    pub fn move_last_to_front_of<RightBufT: DerefMut<Target = [u8; PAGE_SIZE]>>(
        &mut self,
        right: &mut BtreeLeafPage<RightBufT, NUM_TOMBS>,
    ) {
        self.assert_no_tombstones();
        right.assert_no_tombstones();
        let last = self.size().checked_sub(1).expect("move from an empty leaf");
        Self::LAYOUT.insert(
            &mut right.data,
            0,
            Self::LAYOUT.key(&self.data, last),
            Self::LAYOUT.value(&self.data, last),
        );
        Self::LAYOUT.truncate(&mut self.data, last);
    }

    fn assert_no_tombstones(&self) {
        assert_eq!(
            self.num_tombstones(),
            0,
            "move keys of a leaf with tombstones"
        );
    }

    /// Removes slot `index` and its tombstone, if any. Tombstones follow
    /// their keys.
    fn remove_slot(&mut self, index: usize) {
        if let Some(pos) = self.tomb_position(index) {
            self.remove_tomb(pos);
        }
        Self::LAYOUT.remove(&mut self.data, index);
        for pos in 0..self.num_tombstones() {
            let t = self.read_tomb(pos);
            if t > index {
                self.write_tomb(pos, t - 1);
            }
        }
    }

    /// Drops the tombstone at `pos` in the buffer, keeping the rest in order.
    fn remove_tomb(&mut self, pos: usize) {
        let count = self.num_tombstones();
        let tombs = Self::tomb_range(pos + 1).start..Self::tomb_range(count).start;
        self.data.copy_within(tombs, Self::tomb_range(pos).start);
        self.header_mut().num_tombstones = (count as u16 - 1).into();
    }

    /// Replaces the whole tombstone buffer, oldest first.
    fn set_tombstones(&mut self, tombs: &[usize]) {
        for (pos, &index) in tombs.iter().enumerate() {
            self.write_tomb(pos, index);
        }
        self.header_mut().num_tombstones = (tombs.len() as u16).into();
    }

    fn write_value(&mut self, index: usize, rid: Rid) {
        Self::LAYOUT
            .value_mut(&mut self.data, index)
            .copy_from_slice(rid_bytes(rid).as_bytes());
    }

    fn write_tomb(&mut self, pos: usize, index: usize) {
        self.data[Self::tomb_range(pos)].copy_from_slice(U16::new(index as u16).as_bytes());
    }

    fn header_mut(&mut self) -> &mut LeafHeader {
        LeafHeader::mut_from_bytes(&mut self.data[LEAF_HEADER_OFFSET..TOMBS_OFFSET]).unwrap()
    }
}

fn rid_bytes(rid: Rid) -> RidBytes {
    RidBytes {
        page_id: rid.page_id.into(),
        slot: rid.slot.into(),
    }
}
