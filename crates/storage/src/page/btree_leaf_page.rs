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

use common::{INVALID_PAGE_ID, PageId, Rid, SlotId};
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

/// Where `insert_at` puts a key, as `insert_position` found it on a page
/// that has not changed since.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InsertPosition {
    pub(crate) index: usize,
    /// The tombstone of the key at `index`, which the insert revives.
    pub(crate) tomb: Option<usize>,
}

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
    const LAYOUT: SlotLayout = layout(NUM_TOMBS);

    /// Bytes for entries in an empty page.
    pub const CAPACITY: usize = Self::LAYOUT.capacity();

    /// The longest key the page accepts.
    pub const MAX_KEY_SIZE: usize = Self::LAYOUT.max_key_size();

    /// Bytes an entry with a key of `key_len` bytes takes: its slot and key.
    pub const fn entry_size(key_len: usize) -> usize {
        Self::LAYOUT.entry_size(key_len)
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
        num_tombstones(&self.data)
    }

    /// Slot indexes of the tombstoned keys, oldest first.
    pub fn tombstones(&self) -> impl Iterator<Item = usize> + '_ {
        (0..self.num_tombstones()).map(|pos| read_tomb(&self.data, pos))
    }

    pub fn is_tombstone(&self, index: usize) -> bool {
        tomb_position(&self.data, index).is_some()
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
        self.live_index(key, cmp)
            .map(|index| self.read_value(index))
    }

    /// The index of `key`, unless it is missing or tombstoned: what
    /// `remove` deletes.
    pub fn live_index(&self, key: &[u8], cmp: &impl KeyComparator) -> Option<usize> {
        let index = self.key_index(key, cmp);
        (self.holds_at(index, key, cmp) && !self.is_tombstone(index)).then_some(index)
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
            read_tomb(&self.data, 0)
        } else {
            return 0;
        };
        Self::entry_size(Self::LAYOUT.key(&self.data, removed).len())
    }

    /// Where `insert` would put `key`. Fails as `insert` does, and like it
    /// changes nothing then.
    pub fn insert_position(&self, key: &[u8], cmp: &impl KeyComparator) -> Result<InsertPosition> {
        check_key_size(key, Self::MAX_KEY_SIZE)?;
        let index = self.key_index(key, cmp);
        if self.holds_at(index, key, cmp) {
            let tomb = tomb_position(&self.data, index).ok_or(StorageError::DuplicateKey)?;
            return Ok(InsertPosition {
                index,
                tomb: Some(tomb),
            });
        }
        if !self.has_room_for(key.len()) {
            return Err(StorageError::PageFull);
        }
        Ok(InsertPosition { index, tomb: None })
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

    fn read_value(&self, index: usize) -> Rid {
        let rid = RidBytes::ref_from_bytes(Self::LAYOUT.value(&self.data, index)).unwrap();
        Rid::new(rid.page_id.get(), rid.slot.get())
    }

    fn check_index(&self, index: usize) {
        assert!(
            index < self.size(),
            "key index {index} out of range 0..{}",
            self.size()
        );
    }

    fn header(&self) -> &LeafHeader {
        header(&self.data)
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
        header_mut(&mut self.data).next_page_id = page_id.into();
    }

    /// Inserts `key` with `rid` and returns its index. A tombstoned `key` is
    /// revived in place with the new `rid`.
    ///
    /// Fails with `KeyTooLarge` if `key` is longer than `MAX_KEY_SIZE`, with
    /// `DuplicateKey` if `key` is already live, and with `PageFull` if the
    /// entry does not fit; the caller splits the page first, or makes room
    /// with `purge_tombstones`.
    pub fn insert(&mut self, key: &[u8], rid: Rid, cmp: &impl KeyComparator) -> Result<usize> {
        let position = self.insert_position(key, cmp)?;
        Ok(self.insert_at(position, key, rid))
    }

    /// Inserts `key` with `rid` at `position`, which `insert_position` gave
    /// for `key`, and returns its index.
    pub fn insert_at(&mut self, position: InsertPosition, key: &[u8], rid: Rid) -> usize {
        insert_at(&mut self.data, NUM_TOMBS, position, key, rid);
        position.index
    }

    /// Deletes `key` by tombstoning it, and returns its record id, or `None`
    /// if it is missing or already tombstoned. If the tombstone buffer is
    /// full, the oldest tombstoned key is removed for good first.
    pub fn remove(&mut self, key: &[u8], cmp: &impl KeyComparator) -> Option<Rid> {
        let index = self.live_index(key, cmp)?;
        let rid = self.read_value(index);
        remove_live(&mut self.data, NUM_TOMBS, index);
        Some(rid)
    }

    /// Removes the key at `index` for good, tombstoned or not, and returns it
    /// with its record id. Used to move keys between siblings.
    pub fn remove_at(&mut self, index: usize) -> (Vec<u8>, Rid) {
        self.check_index(index);
        let removed = (self.key_at(index).to_vec(), self.read_value(index));
        remove_slot(&mut self.data, Self::LAYOUT, index);
        removed
    }

    /// Removes every tombstoned key for good.
    pub fn purge_tombstones(&mut self) {
        let mut tombs: Vec<usize> = self.tombstones().collect();
        // Highest first, so removing one does not move the others.
        tombs.sort_unstable_by(|a, b| b.cmp(a));
        for index in tombs {
            remove_slot(&mut self.data, Self::LAYOUT, index);
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

    /// Replaces the whole tombstone buffer, oldest first.
    fn set_tombstones(&mut self, tombs: &[usize]) {
        for (pos, &index) in tombs.iter().enumerate() {
            write_tomb(&mut self.data, pos, index);
        }
        header_mut(&mut self.data).num_tombstones = (tombs.len() as u16).into();
    }
}

// The changes a `PageOp` logs, written once over a tombstone capacity known
// only at run time. Redo learns it from the op, and the typed page passes
// `NUM_TOMBS`. Every other change to a leaf is logged as an image.

/// Inserts as `insert_at` does, once it has checked that `insert_position`
/// could give `position` for `key` on the leaf in `data`. Fails with
/// `InvalidSlot` or `PageFull`, changing nothing, if it could not.
pub(crate) fn apply_insert(
    data: &mut [u8; PAGE_SIZE],
    num_tombs: usize,
    position: InsertPosition,
    key: &[u8],
    rid: Rid,
) -> Result<()> {
    check_leaf(data)?;
    let layout = layout(num_tombs);
    let InsertPosition { index, tomb } = position;
    let bad = || StorageError::InvalidSlot(index as SlotId);
    if index > layout.size(data) || num_tombstones(data) > num_tombs {
        return Err(bad());
    }
    match tomb {
        Some(pos) if pos >= num_tombstones(data) || read_tomb(data, pos) != index => {
            return Err(bad());
        }
        Some(_) => {}
        None => {
            check_key_size(key, layout.max_key_size())?;
            if layout.entry_size(key.len()) > layout.free_space(data) {
                return Err(StorageError::PageFull);
            }
        }
    }
    insert_at(data, num_tombs, position, key, rid);
    Ok(())
}

/// Removes the live key at `index`, as `remove` does. Fails with
/// `InvalidSlot`, changing nothing, if there is none.
pub(crate) fn apply_remove(
    data: &mut [u8; PAGE_SIZE],
    num_tombs: usize,
    index: usize,
) -> Result<()> {
    check_leaf(data)?;
    if index >= layout(num_tombs).size(data)
        || tomb_position(data, index).is_some()
        || num_tombstones(data) > num_tombs
    {
        return Err(StorageError::InvalidSlot(index as SlotId));
    }
    remove_live(data, num_tombs, index);
    Ok(())
}

fn insert_at(
    data: &mut [u8; PAGE_SIZE],
    num_tombs: usize,
    position: InsertPosition,
    key: &[u8],
    rid: Rid,
) {
    let InsertPosition { index, tomb } = position;
    let layout = layout(num_tombs);
    if let Some(pos) = tomb {
        remove_tomb(data, pos);
        layout
            .value_mut(data, index)
            .copy_from_slice(rid_bytes(rid).as_bytes());
        return;
    }
    for pos in 0..num_tombstones(data) {
        let t = read_tomb(data, pos);
        if t >= index {
            write_tomb(data, pos, t + 1);
        }
    }
    layout.insert(data, index, key, rid_bytes(rid).as_bytes());
}

/// Tombstones the live key at `index`, or removes it for good with no
/// room for tombstones. A full tombstone buffer gives up its oldest key
/// for good first.
fn remove_live(data: &mut [u8; PAGE_SIZE], num_tombs: usize, mut index: usize) {
    let layout = layout(num_tombs);
    if num_tombs == 0 {
        remove_slot(data, layout, index);
        return;
    }
    if num_tombstones(data) == num_tombs {
        let oldest = read_tomb(data, 0);
        remove_slot(data, layout, oldest);
        if index > oldest {
            index -= 1;
        }
    }
    let count = num_tombstones(data);
    write_tomb(data, count, index);
    header_mut(data).num_tombstones = (count as u16 + 1).into();
}

/// Removes slot `index` and its tombstone, if any. Tombstones follow their
/// keys.
fn remove_slot(data: &mut [u8; PAGE_SIZE], layout: SlotLayout, index: usize) {
    if let Some(pos) = tomb_position(data, index) {
        remove_tomb(data, pos);
    }
    layout.remove(data, index);
    for pos in 0..num_tombstones(data) {
        let t = read_tomb(data, pos);
        if t > index {
            write_tomb(data, pos, t - 1);
        }
    }
}

/// The slot layout of a leaf with room for `num_tombs` tombstones.
const fn layout(num_tombs: usize) -> SlotLayout {
    SlotLayout::new(TOMBS_OFFSET + num_tombs * TOMB_SIZE, RID_SIZE)
}

fn check_leaf(data: &[u8; PAGE_SIZE]) -> Result<()> {
    BtreeLeafPage::<_, 0>::new(data).map(drop)
}

fn header(data: &[u8; PAGE_SIZE]) -> &LeafHeader {
    LeafHeader::ref_from_bytes(&data[LEAF_HEADER_OFFSET..TOMBS_OFFSET]).unwrap()
}

fn header_mut(data: &mut [u8; PAGE_SIZE]) -> &mut LeafHeader {
    LeafHeader::mut_from_bytes(&mut data[LEAF_HEADER_OFFSET..TOMBS_OFFSET]).unwrap()
}

fn num_tombstones(data: &[u8; PAGE_SIZE]) -> usize {
    header(data).num_tombstones.get() as usize
}

fn tomb_range(pos: usize) -> Range<usize> {
    let start = TOMBS_OFFSET + pos * TOMB_SIZE;
    start..start + TOMB_SIZE
}

fn read_tomb(data: &[u8; PAGE_SIZE], pos: usize) -> usize {
    U16::ref_from_bytes(&data[tomb_range(pos)]).unwrap().get() as usize
}

fn write_tomb(data: &mut [u8; PAGE_SIZE], pos: usize, index: usize) {
    data[tomb_range(pos)].copy_from_slice(U16::new(index as u16).as_bytes());
}

/// Position in the tombstone buffer of the tombstone for slot `index`.
fn tomb_position(data: &[u8; PAGE_SIZE], index: usize) -> Option<usize> {
    (0..num_tombstones(data)).find(|&pos| read_tomb(data, pos) == index)
}

/// Drops the tombstone at `pos` in the buffer, keeping the rest in order.
fn remove_tomb(data: &mut [u8; PAGE_SIZE], pos: usize) {
    let count = num_tombstones(data);
    let tombs = tomb_range(pos + 1).start..tomb_range(count).start;
    data.copy_within(tombs, tomb_range(pos).start);
    header_mut(data).num_tombstones = (count as u16 - 1).into();
}

fn rid_bytes(rid: Rid) -> RidBytes {
    RidBytes {
        page_id: rid.page_id.into(),
        slot: rid.slot.into(),
    }
}
