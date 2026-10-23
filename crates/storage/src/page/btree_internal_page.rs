//! B+tree internal page, routing variable-length keys to child pages.
//!
//! ```text
//! +----------------------------------------------------------------+
//! | PageHeader (lsn, checksum, page_type)                     16 B |
//! | SlottedHeader (size, key_start, hole_bytes)                6 B |
//! +----------------------------------------------------------------+
//! | SLOT(0) | SLOT(1) | ... | SLOT(size - 1)    grows toward the end → |
//! +----------------------------------------------------------------+
//! |                          free space                            |
//! +----------------------------------------------------------------+
//! |              ← key bytes, grow toward the start                |
//! +----------------------------------------------------------------+
//! ```
//!
//! `SLOT(i)` holds where `KEY(i)` is and the child page id `PAGE_ID(i)`
//! (8 B). The slotted layout, shared with leaf pages, is described in
//! `btree_page`.
//!
//! `size` counts children, so a page with `n + 1` children holds `n` keys,
//! in increasing order as the tree's `KeyComparator` orders them.
//! `PAGE_ID(i)` points to the subtree whose keys `K` satisfy
//! `KEY(i) <= K < KEY(i + 1)`. The leftmost child has no lower bound, so
//! `KEY(0)` is empty and never read.

use std::ops::{Deref, DerefMut};

use common::PageId;
use zerocopy::byteorder::little_endian::I32;
use zerocopy::{FromBytes, IntoBytes};

use crate::error::{Result, StorageError};
use crate::page::PAGE_SIZE;
use crate::page::btree_page::{KeyComparator, SLOTTED_HEADER_END, SlotLayout, check_key_size};
use crate::page::page_header::{PageHeader, PageType};

const LAYOUT: SlotLayout = SlotLayout::new(SLOTTED_HEADER_END, size_of::<I32>());

/// A view of a B+tree internal page over borrowed page bytes `BufT`, such as
/// `guard.get_data()` (read-only) or `guard.get_data_mut()` (read-write).
pub struct BtreeInternalPage<BufT> {
    data: BufT,
}

impl<BufT> BtreeInternalPage<BufT> {
    /// Bytes for entries in an empty page.
    pub const CAPACITY: usize = LAYOUT.capacity();

    /// The longest key the page accepts.
    pub const MAX_KEY_SIZE: usize = LAYOUT.max_key_size();

    /// Bytes an entry with a key of `key_len` bytes takes: its slot and key.
    pub const fn entry_size(key_len: usize) -> usize {
        LAYOUT.entry_size(key_len)
    }
}

impl<BufT: Deref<Target = [u8; PAGE_SIZE]>> BtreeInternalPage<BufT> {
    /// Views `data` as an internal page, checking its page type.
    pub fn new(data: BufT) -> Result<Self> {
        let (header, _) = PageHeader::read_from_prefix(&data[..]).unwrap();
        match header.page_type() {
            Ok(PageType::BTreeInternal) => Ok(Self { data }),
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
        LAYOUT.size(&self.data)
    }

    /// Bytes a new entry can use.
    pub fn free_space(&self) -> usize {
        LAYOUT.free_space(&self.data)
    }

    /// Bytes the entries take.
    pub fn used_space(&self) -> usize {
        LAYOUT.used_space(&self.data)
    }

    /// Whether a key of `key_len` bytes and its child can be inserted
    /// without a split.
    pub fn has_room_for(&self, key_len: usize) -> bool {
        Self::entry_size(key_len) <= self.free_space()
    }

    /// Whether a non-root page is less than half full and has to borrow from
    /// or merge with a sibling.
    pub fn is_underfull(&self) -> bool {
        self.used_space() < Self::CAPACITY / 2
    }

    /// The key at `index`, which must be in `1..size`: key 0 is invalid.
    pub fn key_at(&self, index: usize) -> &[u8] {
        self.check_key_index(index);
        LAYOUT.key(&self.data, index)
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
    pub fn lookup(&self, key: &[u8], cmp: &impl KeyComparator) -> PageId {
        assert!(self.size() > 0, "lookup in an empty internal page");
        self.read_value(self.upper_bound(key, cmp) - 1)
    }

    /// Index of the first key `> key` in `1..size`, or `size` if there is none.
    fn upper_bound(&self, key: &[u8], cmp: &impl KeyComparator) -> usize {
        let (mut lo, mut hi) = (1, self.size().max(1));
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if cmp.compare(LAYOUT.key(&self.data, mid), key).is_le() {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    fn read_value(&self, index: usize) -> PageId {
        I32::read_from_bytes(LAYOUT.value(&self.data, index))
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
}

impl<BufT: DerefMut<Target = [u8; PAGE_SIZE]>> BtreeInternalPage<BufT> {
    /// Formats `data` as an empty internal page.
    pub fn init(mut data: BufT) -> Self {
        PageHeader::init(&mut data, PageType::BTreeInternal);
        LAYOUT.init(&mut data);
        Self { data }
    }

    /// Makes this empty page a root with two children split by `key`.
    ///
    /// Fails with `KeyTooLarge` if `key` is longer than `MAX_KEY_SIZE`.
    ///
    /// # Panics
    /// If the page is not empty.
    pub fn populate_new_root(&mut self, left: PageId, key: &[u8], right: PageId) -> Result<()> {
        assert_eq!(self.size(), 0, "populate a root that is not empty");
        check_key_size(key, Self::MAX_KEY_SIZE)?;
        LAYOUT.insert(&mut self.data, 0, &[], I32::new(left).as_bytes());
        LAYOUT.insert(&mut self.data, 1, key, I32::new(right).as_bytes());
        Ok(())
    }

    /// Replaces the key at `index`, which must be in `1..size`. The caller
    /// keeps the keys in order.
    ///
    /// Fails with `KeyTooLarge` if `key` is longer than `MAX_KEY_SIZE`, and
    /// with `PageFull` if it is longer than the old key by more than the free
    /// space.
    pub fn set_key_at(&mut self, index: usize, key: &[u8]) -> Result<()> {
        self.check_key_index(index);
        check_key_size(key, Self::MAX_KEY_SIZE)?;
        if key.len() > self.free_space() + LAYOUT.key(&self.data, index).len() {
            return Err(StorageError::PageFull);
        }
        LAYOUT.set_key(&mut self.data, index, key);
        Ok(())
    }

    /// Replaces the child at `index`, which must be in `0..size`.
    pub fn set_value_at(&mut self, index: usize, page_id: PageId) {
        self.check_value_index(index);
        self.write_value(index, page_id);
    }

    /// Inserts `key` with `child` as its right child, after any equal keys,
    /// and returns the index they were stored at.
    ///
    /// Fails with `KeyTooLarge` if `key` is longer than `MAX_KEY_SIZE`, and
    /// with `PageFull` if the entry does not fit; the caller splits the page
    /// first.
    ///
    /// # Panics
    /// If the page has no children: the leftmost child has no key, so it has
    /// to be set with `populate_new_root` or `push_front`.
    pub fn insert(&mut self, key: &[u8], child: PageId, cmp: &impl KeyComparator) -> Result<usize> {
        assert!(self.size() > 0, "insert into an empty internal page");
        check_key_size(key, Self::MAX_KEY_SIZE)?;
        if !self.has_room_for(key.len()) {
            return Err(StorageError::PageFull);
        }
        let index = self.upper_bound(key, cmp);
        LAYOUT.insert(&mut self.data, index, key, I32::new(child).as_bytes());
        Ok(index)
    }

    /// Removes the key and child at `index`, which must be in `0..size`.
    /// Removing child 0 also drops key 1, since the next child becomes the
    /// leftmost one and its key is no longer read.
    pub fn remove_at(&mut self, index: usize) {
        self.check_value_index(index);
        LAYOUT.remove(&mut self.data, index);
        if index == 0 && self.size() > 0 {
            LAYOUT.set_key(&mut self.data, 0, &[]);
        }
    }

    /// Removes the leftmost child and the key after it, and returns them as
    /// `(key, child)`. Used to move them to the left sibling, with the key
    /// going up to the parent.
    ///
    /// # Panics
    /// If the page has fewer than 2 children.
    pub fn remove_first(&mut self) -> (Vec<u8>, PageId) {
        assert!(self.size() >= 2, "remove_first needs at least 2 children");
        let removed = (self.key_at(1).to_vec(), self.read_value(0));
        self.remove_at(0);
        removed
    }

    /// Makes `child` the new leftmost child, with `key` between it and the
    /// old leftmost child. Used to take a child from the left sibling, with
    /// the separator coming down from the parent as `key`. On an empty page
    /// `key` is not stored.
    ///
    /// Fails with `KeyTooLarge` if `key` is longer than `MAX_KEY_SIZE`, and
    /// with `PageFull` if the entry does not fit.
    pub fn push_front(&mut self, key: &[u8], child: PageId) -> Result<()> {
        if self.size() == 0 {
            LAYOUT.insert(&mut self.data, 0, &[], I32::new(child).as_bytes());
            return Ok(());
        }
        check_key_size(key, Self::MAX_KEY_SIZE)?;
        if !self.has_room_for(key.len()) {
            return Err(StorageError::PageFull);
        }
        LAYOUT.insert(&mut self.data, 0, &[], I32::new(child).as_bytes());
        LAYOUT.set_key(&mut self.data, 1, key);
        Ok(())
    }

    /// Moves the upper half of the children, by bytes, to `right`, an empty
    /// internal page, and returns the key between the two halves. That key
    /// goes up to the parent: it is left in neither page, and the child that
    /// was to its right becomes `right`'s leftmost child.
    ///
    /// # Panics
    /// If `right` is not empty or this page has fewer than 2 children.
    pub fn split_to<RightBufT: DerefMut<Target = [u8; PAGE_SIZE]>>(
        &mut self,
        right: &mut BtreeInternalPage<RightBufT>,
    ) -> Vec<u8> {
        assert_eq!(right.size(), 0, "split into a page that is not empty");
        let at = LAYOUT.split_index(&self.data);
        let separator = LAYOUT.key(&self.data, at).to_vec();
        LAYOUT.insert(&mut right.data, 0, &[], LAYOUT.value(&self.data, at));
        for index in at + 1..self.size() {
            LAYOUT.insert(
                &mut right.data,
                index - at,
                LAYOUT.key(&self.data, index),
                LAYOUT.value(&self.data, index),
            );
        }
        LAYOUT.truncate(&mut self.data, at);
        separator
    }

    /// Moves every child to the end of `left`, the page before this one,
    /// with `middle_key`, the parent's key between the two pages, as the key
    /// of this page's first child. Leaves this page empty.
    ///
    /// # Panics
    /// If the children do not fit in `left`.
    pub fn move_all_to<LeftBufT: DerefMut<Target = [u8; PAGE_SIZE]>>(
        &mut self,
        left: &mut BtreeInternalPage<LeftBufT>,
        middle_key: &[u8],
    ) {
        for index in 0..self.size() {
            let key = if index == 0 {
                middle_key
            } else {
                LAYOUT.key(&self.data, index)
            };
            let end = left.size();
            LAYOUT.insert(&mut left.data, end, key, LAYOUT.value(&self.data, index));
        }
        LAYOUT.truncate(&mut self.data, 0);
    }

    /// Moves the first child to the end of `left`, the page before this one,
    /// with `middle_key`, the parent's key between the two pages, as its key.
    /// This page's key 1 is dropped: it is the new key between the pages,
    /// which the caller puts in the parent.
    ///
    /// # Panics
    /// If this page has fewer than 2 children or the child does not fit in
    /// `left`.
    pub fn move_first_to_end_of<LeftBufT: DerefMut<Target = [u8; PAGE_SIZE]>>(
        &mut self,
        left: &mut BtreeInternalPage<LeftBufT>,
        middle_key: &[u8],
    ) {
        let (_, child) = self.remove_first();
        let end = left.size();
        LAYOUT.insert(&mut left.data, end, middle_key, I32::new(child).as_bytes());
    }

    /// Moves the last child to the front of `right`, the page after this
    /// one, with `middle_key`, the parent's key between the two pages, as the
    /// key between it and `right`'s old first child. The moved child's key is
    /// dropped: it is the new key between the pages, which the caller puts
    /// in the parent.
    ///
    /// # Panics
    /// If this page has fewer than 2 children, `right` is empty, or the
    /// child does not fit in `right`.
    pub fn move_last_to_front_of<RightBufT: DerefMut<Target = [u8; PAGE_SIZE]>>(
        &mut self,
        right: &mut BtreeInternalPage<RightBufT>,
        middle_key: &[u8],
    ) {
        let last = self.size() - 1;
        self.check_key_index(last);
        assert!(right.size() > 0, "move into an empty internal page");
        LAYOUT.insert(&mut right.data, 0, &[], LAYOUT.value(&self.data, last));
        LAYOUT.set_key(&mut right.data, 1, middle_key);
        LAYOUT.truncate(&mut self.data, last);
    }

    fn write_value(&mut self, index: usize, page_id: PageId) {
        LAYOUT
            .value_mut(&mut self.data, index)
            .copy_from_slice(I32::new(page_id).as_bytes());
    }
}
