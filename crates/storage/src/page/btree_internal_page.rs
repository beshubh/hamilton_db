//! B+tree internal page.
//!
//! ```text
//! +----------------------------------------------------------------+
//! | PageHeader (lsn, checksum, page_type)                     16 B |
//! | InternalHeader (size, max_size)                            4 B |
//! +----------------------------------------------------------------+
//! | KEY(0) (invalid) | KEY(1) | KEY(2) | ... | KEY(SLOT_CNT - 1)   |
//! +----------------------------------------------------------------+
//! | PAGE_ID(0) | PAGE_ID(1) | PAGE_ID(2) | ... | PAGE_ID(SLOT_CNT - 1) |
//! +----------------------------------------------------------------+
//! ```
//!
//! `size` counts children, so a page with `n + 1` children holds `n` keys,
//! in increasing order. `PAGE_ID(i)` points to the subtree whose keys `K`
//! satisfy `KEY(i) <= K < KEY(i + 1)`. The leftmost child has no lower bound,
//! so `KEY(0)` is never read: every search and lookup ignores it.
//!
//! Both arrays are sized for the most children a page can hold (`SLOT_CNT`),
//! which depends on the key size. Keys are any fixed-size type that can be
//! copied to and from bytes (`BtreeKey`); page ids are little-endian. Nothing
//! is aligned, so a page can be viewed from any byte buffer.

use std::marker::PhantomData;
use std::ops::{Deref, DerefMut, Range};

use common::PageId;
use zerocopy::byteorder::little_endian::{I32, U16};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::error::{Result, StorageError};
use crate::page::PAGE_SIZE;
use crate::page::page_header::{PageHeader, PageType};

const _: () = assert!(PAGE_SIZE <= u16::MAX as usize);

const INTERNAL_HEADER_OFFSET: usize = PageHeader::SIZE;
const KEYS_OFFSET: usize = INTERNAL_HEADER_OFFSET + size_of::<InternalHeader>();
const PAGE_ID_SIZE: usize = size_of::<I32>();

/// A key stored in a B+tree page: fixed-size, copied in and out as raw bytes,
/// and ordered by `Ord`.
pub trait BtreeKey: FromBytes + IntoBytes + Immutable + Copy + Ord {}

impl<T: FromBytes + IntoBytes + Immutable + Copy + Ord> BtreeKey for T {}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct InternalHeader {
    /// Number of children.
    size: U16,
    /// Most children the page may hold before it has to split.
    max_size: U16,
}

const _: () = assert!(size_of::<InternalHeader>() == 4);

/// A view of a B+tree internal page with keys of type `KeyT` over borrowed
/// page bytes `BufT`, such as `guard.get_data()` (read-only) or
/// `guard.get_data_mut()` (read-write).
pub struct BtreeInternalPage<BufT, KeyT> {
    data: BufT,
    _key: PhantomData<KeyT>,
}

impl<BufT, KeyT> BtreeInternalPage<BufT, KeyT> {
    /// The most children a page can hold.
    pub const SLOT_CNT: usize = (PAGE_SIZE - KEYS_OFFSET) / (size_of::<KeyT>() + PAGE_ID_SIZE);

    const PAGE_IDS_OFFSET: usize = KEYS_OFFSET + Self::SLOT_CNT * size_of::<KeyT>();

    fn key_range(index: usize) -> Range<usize> {
        let start = KEYS_OFFSET + index * size_of::<KeyT>();
        start..start + size_of::<KeyT>()
    }

    fn value_range(index: usize) -> Range<usize> {
        let start = Self::PAGE_IDS_OFFSET + index * PAGE_ID_SIZE;
        start..start + PAGE_ID_SIZE
    }
}

impl<BufT: Deref<Target = [u8; PAGE_SIZE]>, KeyT: BtreeKey> BtreeInternalPage<BufT, KeyT> {
    /// Views `data` as an internal page, checking its page type.
    pub fn new(data: BufT) -> Result<Self> {
        let (header, _) = PageHeader::read_from_prefix(&data[..]).unwrap();
        match header.page_type() {
            Ok(PageType::BTreeInternal) => Ok(Self {
                data,
                _key: PhantomData,
            }),
            Ok(other) => Err(StorageError::UnexpectedPageType {
                expected: PageType::BTreeInternal,
                found: other as u32,
            }),
            Err(found) => Err(StorageError::UnexpectedPageType {
                expected: PageType::BTreeInternal,
                found,
            }),
        }
    }

    /// Number of children. The page holds one key fewer.
    pub fn size(&self) -> usize {
        self.header().size.get() as usize
    }

    pub fn max_size(&self) -> usize {
        self.header().max_size.get() as usize
    }

    /// Fewest children a non-root page may hold before it has to borrow from
    /// or merge with a sibling.
    pub fn min_size(&self) -> usize {
        self.max_size().div_ceil(2)
    }

    pub fn is_full(&self) -> bool {
        self.size() >= self.max_size()
    }

    /// The key at `index`, which must be in `1..size`: key 0 is invalid.
    pub fn key_at(&self, index: usize) -> KeyT {
        self.check_key_index(index);
        self.read_key(index)
    }

    /// The child at `index`, which must be in `0..size`.
    pub fn value_at(&self, index: usize) -> PageId {
        self.check_value_index(index);
        self.read_value(index)
    }

    /// Index of the child `page_id`, if it is one of this page's children.
    pub fn value_index(&self, page_id: PageId) -> Option<usize> {
        (0..self.size()).find(|&i| self.read_value(i) == page_id)
    }

    /// The child whose subtree may hold `key`: the one after the last key
    /// `<= key`, or the leftmost child if every key is greater.
    ///
    /// # Panics
    /// If the page has no children.
    pub fn lookup(&self, key: &KeyT) -> PageId {
        assert!(self.size() > 0, "lookup in an empty internal page");
        self.read_value(self.upper_bound(key) - 1)
    }

    /// Index of the first key `> key` in `1..size`, or `size` if there is none.
    fn upper_bound(&self, key: &KeyT) -> usize {
        let (mut lo, mut hi) = (1, self.size().max(1));
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.read_key(mid) <= *key {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    fn read_key(&self, index: usize) -> KeyT {
        KeyT::read_from_bytes(&self.data[Self::key_range(index)]).unwrap()
    }

    fn read_value(&self, index: usize) -> PageId {
        I32::read_from_bytes(&self.data[Self::value_range(index)])
            .unwrap()
            .get()
    }

    fn check_key_index(&self, index: usize) {
        assert!(
            (1..self.size()).contains(&index),
            "key index {index} out of range 1..{}",
            self.size()
        );
    }

    fn check_value_index(&self, index: usize) {
        assert!(
            index < self.size(),
            "child index {index} out of range 0..{}",
            self.size()
        );
    }

    fn header(&self) -> &InternalHeader {
        InternalHeader::ref_from_bytes(&self.data[INTERNAL_HEADER_OFFSET..KEYS_OFFSET]).unwrap()
    }
}

impl<BufT: DerefMut<Target = [u8; PAGE_SIZE]>, KeyT: BtreeKey> BtreeInternalPage<BufT, KeyT> {
    /// Formats `data` as an empty internal page that splits once it holds
    /// more than `max_size` children.
    ///
    /// # Panics
    /// If `max_size` is less than 2 or more than `SLOT_CNT`.
    pub fn init(mut data: BufT, max_size: usize) -> Self {
        Self::check_max_size(max_size);
        PageHeader::init(&mut data, PageType::BTreeInternal);
        let header = InternalHeader {
            size: 0.into(),
            max_size: (max_size as u16).into(),
        };
        header
            .write_to(&mut data[INTERNAL_HEADER_OFFSET..KEYS_OFFSET])
            .unwrap();
        Self {
            data,
            _key: PhantomData,
        }
    }

    /// Makes this page a root with two children split by `key`.
    pub fn populate_new_root(&mut self, left: PageId, key: KeyT, right: PageId) {
        self.set_size(2);
        self.write_value(0, left);
        self.write_key(1, key);
        self.write_value(1, right);
    }

    /// # Panics
    /// If `size` is more than `max_size`.
    pub fn set_size(&mut self, size: usize) {
        assert!(
            size <= self.max_size(),
            "size {size} exceeds max size {}",
            self.max_size()
        );
        self.header_mut().size = (size as u16).into();
    }

    /// # Panics
    /// If the new size is negative or more than `max_size`.
    pub fn change_size_by(&mut self, amount: isize) {
        let size = self
            .size()
            .checked_add_signed(amount)
            .expect("size would become negative");
        self.set_size(size);
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

    /// Replaces the key at `index`, which must be in `1..size`. The caller
    /// keeps the keys in order.
    pub fn set_key_at(&mut self, index: usize, key: KeyT) {
        self.check_key_index(index);
        self.write_key(index, key);
    }

    /// Replaces the child at `index`, which must be in `0..size`.
    pub fn set_value_at(&mut self, index: usize, page_id: PageId) {
        self.check_value_index(index);
        self.write_value(index, page_id);
    }

    /// Inserts `key` with `child` as its right child, after any equal keys,
    /// and returns the index they were stored at.
    ///
    /// Fails with `PageFull` if the page already holds `max_size` children;
    /// the caller splits it first.
    ///
    /// # Panics
    /// If the page has no children: the leftmost child has no key, so it has
    /// to be set with `populate_new_root` or `push_front`.
    pub fn insert(&mut self, key: KeyT, child: PageId) -> Result<usize> {
        assert!(self.size() > 0, "insert into an empty internal page");
        if self.is_full() {
            return Err(StorageError::PageFull);
        }
        let index = self.upper_bound(&key);
        self.shift_right(index);
        self.write_key(index, key);
        self.write_value(index, child);
        Ok(index)
    }

    /// Removes the key and child at `index`, which must be in `0..size`.
    /// Removing child 0 also drops key 1, since the next child becomes the
    /// leftmost one and its key is no longer read.
    pub fn remove_at(&mut self, index: usize) {
        self.check_value_index(index);
        self.shift_left(index);
    }

    /// Removes the leftmost child and the key after it, and returns them as
    /// `(key, child)`. Used to move them to the left sibling, with the key
    /// going up to the parent.
    ///
    /// # Panics
    /// If the page has fewer than 2 children.
    pub fn remove_first(&mut self) -> (KeyT, PageId) {
        assert!(self.size() >= 2, "remove_first needs at least 2 children");
        let removed = (self.read_key(1), self.read_value(0));
        self.shift_left(0);
        removed
    }

    /// Makes `child` the new leftmost child, with `key` between it and the
    /// old leftmost child. Used to take a child from the left sibling, with
    /// the separator coming down from the parent as `key`.
    ///
    /// Fails with `PageFull` if the page already holds `max_size` children.
    pub fn push_front(&mut self, key: KeyT, child: PageId) -> Result<()> {
        if self.is_full() {
            return Err(StorageError::PageFull);
        }
        self.shift_right(0);
        self.write_value(0, child);
        if self.size() > 1 {
            self.write_key(1, key);
        }
        Ok(())
    }

    /// Moves the keys and children at `index..size` one slot right and grows
    /// the page by one, leaving slot `index` to be written.
    fn shift_right(&mut self, index: usize) {
        let size = self.size();
        let key_size = size_of::<KeyT>();
        let keys = Self::key_range(index).start..Self::key_range(size).start;
        self.data.copy_within(keys.clone(), keys.start + key_size);
        let values = Self::value_range(index).start..Self::value_range(size).start;
        self.data
            .copy_within(values.clone(), values.start + PAGE_ID_SIZE);
        self.set_size(size + 1);
    }

    /// Moves the keys and children at `index + 1..size` one slot left over
    /// slot `index` and shrinks the page by one.
    fn shift_left(&mut self, index: usize) {
        let size = self.size();
        let keys = Self::key_range(index + 1).start..Self::key_range(size).start;
        self.data.copy_within(keys, Self::key_range(index).start);
        let values = Self::value_range(index + 1).start..Self::value_range(size).start;
        self.data
            .copy_within(values, Self::value_range(index).start);
        self.set_size(size - 1);
    }

    fn write_key(&mut self, index: usize, key: KeyT) {
        self.data[Self::key_range(index)].copy_from_slice(key.as_bytes());
    }

    fn write_value(&mut self, index: usize, page_id: PageId) {
        self.data[Self::value_range(index)].copy_from_slice(I32::new(page_id).as_bytes());
    }

    fn check_max_size(max_size: usize) {
        assert!(
            (2..=Self::SLOT_CNT).contains(&max_size),
            "max size {max_size} out of range 2..={}",
            Self::SLOT_CNT
        );
    }

    fn header_mut(&mut self) -> &mut InternalHeader {
        InternalHeader::mut_from_bytes(&mut self.data[INTERNAL_HEADER_OFFSET..KEYS_OFFSET]).unwrap()
    }
}
