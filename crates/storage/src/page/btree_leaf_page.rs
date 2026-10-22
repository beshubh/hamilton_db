//! B+tree leaf page, mapping keys to record ids.
//!
//! ```text
//! +----------------------------------------------------------------+
//! | PageHeader (lsn, checksum, page_type)                     16 B |
//! | LeafHeader (size, max_size, next_page_id, num_tombstones) 10 B |
//! +----------------------------------------------------------------+
//! | TOMB(0) | TOMB(1) | ... | TOMB(NUM_TOMBS - 1)                  |
//! +----------------------------------------------------------------+
//! | KEY(0) | KEY(1) | ... | KEY(SLOT_CNT - 1)                      |
//! +----------------------------------------------------------------+
//! | RID(0) | RID(1) | ... | RID(SLOT_CNT - 1)                      |
//! +----------------------------------------------------------------+
//! ```
//!
//! Keys are stored in increasing order, as the tree's `KeyComparator` orders
//! them, `RID(i)` being the record of `KEY(i)`. `next_page_id` links the
//! leaves left to right for range scans.
//!
//! Deleting a key does not remove it right away: its slot index is appended
//! to the tombstone buffer, which holds up to `NUM_TOMBS` indexes, oldest
//! first. A tombstoned key keeps its slot and still counts in `size`, but
//! lookups skip it. Deleting when the buffer is full removes the oldest
//! tombstoned key for good to make room, and inserting a tombstoned key
//! revives it in place. With `NUM_TOMBS = 0` deletes take effect at once.
//!
//! All three arrays are fixed-size, so `SLOT_CNT` depends on the key size and
//! `NUM_TOMBS`. Multi-byte fields are little-endian and nothing is aligned,
//! so a page can be viewed from any byte buffer.

use std::marker::PhantomData;
use std::ops::{Deref, DerefMut, Range};

use common::{INVALID_PAGE_ID, PageId, Rid};
use zerocopy::byteorder::little_endian::{I32, U16};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::error::{Result, StorageError};
use crate::page::PAGE_SIZE;
use crate::page::btree_internal_page::{BtreeKey, KeyComparator};
use crate::page::page_header::{PageHeader, PageType};

const _: () = assert!(PAGE_SIZE <= u16::MAX as usize);

const LEAF_HEADER_OFFSET: usize = PageHeader::SIZE;
const TOMBS_OFFSET: usize = LEAF_HEADER_OFFSET + size_of::<LeafHeader>();
const TOMB_SIZE: usize = size_of::<U16>();
const RID_SIZE: usize = size_of::<RidBytes>();

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct LeafHeader {
    /// Number of keys, tombstoned ones included.
    size: U16,
    /// Most keys the page may hold before it has to split.
    max_size: U16,
    next_page_id: I32,
    num_tombstones: U16,
}

const _: () = assert!(size_of::<LeafHeader>() == 10);

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct RidBytes {
    page_id: I32,
    slot: U16,
}

const _: () = assert!(size_of::<RidBytes>() == 6);

/// A view of a B+tree leaf page with keys of type `KeyT` and room for
/// `NUM_TOMBS` tombstones over borrowed page bytes `BufT`, such as
/// `guard.get_data()` (read-only) or `guard.get_data_mut()` (read-write).
///
/// A page must always be viewed with the `KeyT` and `NUM_TOMBS` it was
/// initialized with: both decide where the arrays sit.
pub struct BtreeLeafPage<BufT, KeyT, const NUM_TOMBS: usize> {
    data: BufT,
    _key: PhantomData<KeyT>,
}

impl<BufT, KeyT, const NUM_TOMBS: usize> BtreeLeafPage<BufT, KeyT, NUM_TOMBS> {
    const KEYS_OFFSET: usize = TOMBS_OFFSET + NUM_TOMBS * TOMB_SIZE;

    /// The most keys a page can hold.
    pub const SLOT_CNT: usize = (PAGE_SIZE - Self::KEYS_OFFSET) / (size_of::<KeyT>() + RID_SIZE);

    const RIDS_OFFSET: usize = Self::KEYS_OFFSET + Self::SLOT_CNT * size_of::<KeyT>();

    fn tomb_range(pos: usize) -> Range<usize> {
        let start = TOMBS_OFFSET + pos * TOMB_SIZE;
        start..start + TOMB_SIZE
    }

    fn key_range(index: usize) -> Range<usize> {
        let start = Self::KEYS_OFFSET + index * size_of::<KeyT>();
        start..start + size_of::<KeyT>()
    }

    fn value_range(index: usize) -> Range<usize> {
        let start = Self::RIDS_OFFSET + index * RID_SIZE;
        start..start + RID_SIZE
    }
}

impl<BufT: Deref<Target = [u8; PAGE_SIZE]>, KeyT: BtreeKey, const NUM_TOMBS: usize>
    BtreeLeafPage<BufT, KeyT, NUM_TOMBS>
{
    /// Views `data` as a leaf page, checking its page type.
    pub fn new(data: BufT) -> Result<Self> {
        let (header, _) = PageHeader::read_from_prefix(&data[..]).unwrap();
        match header.page_type() {
            Ok(PageType::BTreeLeaf) => Ok(Self {
                data,
                _key: PhantomData,
            }),
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
        self.header().size.get() as usize
    }

    /// Number of keys that are not tombstoned.
    pub fn live_size(&self) -> usize {
        self.size() - self.num_tombstones()
    }

    pub fn max_size(&self) -> usize {
        self.header().max_size.get() as usize
    }

    /// Fewest keys a non-root page may hold before it has to borrow from or
    /// merge with a sibling.
    pub fn min_size(&self) -> usize {
        self.max_size().div_ceil(2)
    }

    pub fn is_full(&self) -> bool {
        self.size() >= self.max_size()
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
    pub fn key_at(&self, index: usize) -> KeyT {
        self.check_index(index);
        self.read_key(index)
    }

    /// The record id at `index`, which must be in `0..size`. It may be
    /// tombstoned.
    pub fn value_at(&self, index: usize) -> Rid {
        self.check_index(index);
        self.read_value(index)
    }

    /// The record id of `key`, unless it is missing or tombstoned.
    pub fn lookup(&self, key: &KeyT, cmp: &impl KeyComparator<KeyT>) -> Option<Rid> {
        let index = self.key_index(key, cmp);
        (self.holds_at(index, key, cmp) && !self.is_tombstone(index))
            .then(|| self.read_value(index))
    }

    /// Index of the first key `>= key`, tombstoned or not, or `size` if there
    /// is none. A range scan starts here.
    pub fn key_index(&self, key: &KeyT, cmp: &impl KeyComparator<KeyT>) -> usize {
        let (mut lo, mut hi) = (0, self.size());
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if cmp.compare(&self.read_key(mid), key).is_lt() {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    /// Whether slot `index`, as found by `key_index`, holds `key`, tombstoned
    /// or not.
    fn holds_at(&self, index: usize, key: &KeyT, cmp: &impl KeyComparator<KeyT>) -> bool {
        index < self.size() && cmp.compare(&self.read_key(index), key).is_eq()
    }

    /// Position in the tombstone buffer of the tombstone for slot `index`.
    fn tomb_position(&self, index: usize) -> Option<usize> {
        (0..self.num_tombstones()).find(|&pos| self.read_tomb(pos) == index)
    }

    fn read_key(&self, index: usize) -> KeyT {
        KeyT::read_from_bytes(&self.data[Self::key_range(index)]).unwrap()
    }

    fn read_value(&self, index: usize) -> Rid {
        let rid = RidBytes::ref_from_bytes(&self.data[Self::value_range(index)]).unwrap();
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

impl<BufT: DerefMut<Target = [u8; PAGE_SIZE]>, KeyT: BtreeKey, const NUM_TOMBS: usize>
    BtreeLeafPage<BufT, KeyT, NUM_TOMBS>
{
    /// Formats `data` as an empty leaf page that splits once it holds more
    /// than `max_size` keys.
    ///
    /// # Panics
    /// If `max_size` is less than 2 or more than `SLOT_CNT`.
    pub fn init(mut data: BufT, max_size: usize) -> Self {
        Self::check_max_size(max_size);
        PageHeader::init(&mut data, PageType::BTreeLeaf);
        let header = LeafHeader {
            size: 0.into(),
            max_size: (max_size as u16).into(),
            next_page_id: INVALID_PAGE_ID.into(),
            num_tombstones: 0.into(),
        };
        header
            .write_to(&mut data[LEAF_HEADER_OFFSET..TOMBS_OFFSET])
            .unwrap();
        Self {
            data,
            _key: PhantomData,
        }
    }

    pub fn set_next_page_id(&mut self, page_id: PageId) {
        self.header_mut().next_page_id = page_id.into();
    }

    /// # Panics
    /// If `max_size` is less than 2, more than `SLOT_CNT` or less than the
    /// current size.
    pub fn set_max_size(&mut self, max_size: usize) {
        Self::check_max_size(max_size);
        assert!(
            max_size >= self.size(),
            "max size {max_size} is less than size {}",
            self.size()
        );
        self.header_mut().max_size = (max_size as u16).into();
    }

    /// Inserts `key` with `rid` and returns its index. A tombstoned `key` is
    /// revived in place with the new `rid`.
    ///
    /// Fails with `DuplicateKey` if `key` is already live, and with
    /// `PageFull` if the page already holds `max_size` keys; the caller
    /// splits it first, or makes room with `purge_tombstones`.
    pub fn insert(&mut self, key: KeyT, rid: Rid, cmp: &impl KeyComparator<KeyT>) -> Result<usize> {
        let index = self.key_index(&key, cmp);
        if self.holds_at(index, &key, cmp) {
            let Some(pos) = self.tomb_position(index) else {
                return Err(StorageError::DuplicateKey);
            };
            self.remove_tomb(pos);
            self.write_value(index, rid);
            return Ok(index);
        }
        if self.is_full() {
            return Err(StorageError::PageFull);
        }
        self.shift_right(index);
        self.write_key(index, key);
        self.write_value(index, rid);
        Ok(index)
    }

    /// Deletes `key` by tombstoning it, and returns its record id, or `None`
    /// if it is missing or already tombstoned. If the tombstone buffer is
    /// full, the oldest tombstoned key is removed for good first.
    pub fn remove(&mut self, key: &KeyT, cmp: &impl KeyComparator<KeyT>) -> Option<Rid> {
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
    pub fn remove_at(&mut self, index: usize) -> (KeyT, Rid) {
        self.check_index(index);
        let removed = (self.read_key(index), self.read_value(index));
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

    /// Moves the keys and record ids at `index..size` one slot right and
    /// grows the page by one, leaving slot `index` to be written. Tombstones
    /// follow their keys.
    fn shift_right(&mut self, index: usize) {
        let size = self.size();
        let keys = Self::key_range(index).start..Self::key_range(size).start;
        self.data
            .copy_within(keys.clone(), keys.start + size_of::<KeyT>());
        let values = Self::value_range(index).start..Self::value_range(size).start;
        self.data
            .copy_within(values.clone(), values.start + RID_SIZE);
        for pos in 0..self.num_tombstones() {
            let t = self.read_tomb(pos);
            if t >= index {
                self.write_tomb(pos, t + 1);
            }
        }
        self.header_mut().size = (size as u16 + 1).into();
    }

    /// Removes slot `index` and its tombstone, if any, moving the keys and
    /// record ids after it one slot left. Tombstones follow their keys.
    fn remove_slot(&mut self, index: usize) {
        if let Some(pos) = self.tomb_position(index) {
            self.remove_tomb(pos);
        }
        let size = self.size();
        let keys = Self::key_range(index + 1).start..Self::key_range(size).start;
        self.data.copy_within(keys, Self::key_range(index).start);
        let values = Self::value_range(index + 1).start..Self::value_range(size).start;
        self.data
            .copy_within(values, Self::value_range(index).start);
        for pos in 0..self.num_tombstones() {
            let t = self.read_tomb(pos);
            if t > index {
                self.write_tomb(pos, t - 1);
            }
        }
        self.header_mut().size = (size as u16 - 1).into();
    }

    /// Drops the tombstone at `pos` in the buffer, keeping the rest in order.
    fn remove_tomb(&mut self, pos: usize) {
        let count = self.num_tombstones();
        let tombs = Self::tomb_range(pos + 1).start..Self::tomb_range(count).start;
        self.data.copy_within(tombs, Self::tomb_range(pos).start);
        self.header_mut().num_tombstones = (count as u16 - 1).into();
    }

    fn write_key(&mut self, index: usize, key: KeyT) {
        self.data[Self::key_range(index)].copy_from_slice(key.as_bytes());
    }

    fn write_value(&mut self, index: usize, rid: Rid) {
        let rid = RidBytes {
            page_id: rid.page_id.into(),
            slot: rid.slot.into(),
        };
        self.data[Self::value_range(index)].copy_from_slice(rid.as_bytes());
    }

    fn write_tomb(&mut self, pos: usize, index: usize) {
        self.data[Self::tomb_range(pos)].copy_from_slice(U16::new(index as u16).as_bytes());
    }

    fn check_max_size(max_size: usize) {
        assert!(
            (2..=Self::SLOT_CNT).contains(&max_size),
            "max size {max_size} out of range 2..={}",
            Self::SLOT_CNT
        );
    }

    fn header_mut(&mut self) -> &mut LeafHeader {
        LeafHeader::mut_from_bytes(&mut self.data[LEAF_HEADER_OFFSET..TOMBS_OFFSET]).unwrap()
    }
}
