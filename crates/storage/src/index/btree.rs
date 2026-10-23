//! A B+tree mapping variable-length keys to record ids.
//!
//! The tree is entered through its header page (`BtreeHeaderPage`), which
//! holds the root page id. Internal pages route a key to the child whose
//! range holds it; leaves hold the keys with their record ids and are linked
//! left to right for scans. Keys are unique, as the tree's comparator
//! decides: two keys it orders as equal are the same key.
//!
//! Pages split by bytes, not by key count: a page is full when the next key
//! does not fit, and splits into halves holding about the same number of
//! bytes. A key may be at most `MAX_KEY_SIZE` bytes, which guarantees each
//! half has room for it.
//!
//! Readers descend holding at most two read latches: a child's is taken
//! before its parent's is released (latch crabbing), so a writer can never
//! slip in between.
//!
//! An insert first tries the optimistic way: it descends like a reader and
//! write-latches only the leaf. Most inserts fit in their leaf and are done
//! there. One that does not, or one into an empty tree, starts over the
//! pessimistic way: it write-latches from the header page down, releasing
//! everything above a page that is safe (one a split below cannot reach),
//! splits the leaf and carries the split up through the pages still held.
//!
//! A remove works the same way. The optimistic one removes from the leaf if
//! that leaves it at least half full (a root leaf only has to keep a key).
//! The pessimistic one rebalances an underfull page with a sibling, the
//! right one if it has one: the two merge if they fit in one page, and
//! otherwise keys move from the sibling until the two are about even. A
//! merge takes a key out of the parent, which may leave it underfull in
//! turn, and a root left with one child gives way to that child.

use std::collections::VecDeque;
use std::sync::Arc;

use common::{INVALID_PAGE_ID, PageId, Rid};
use zerocopy::FromBytes;

use crate::buffer::arc_replacer::AccessType;
use crate::buffer::traced_buffer_pool_manager::TracedBufferPoolManager;
use crate::error::{Result, StorageError};
use crate::index::index_iter::IndexIterator;
use crate::page::PAGE_SIZE;
use crate::page::btree_header_page::BtreeHeaderPage;
use crate::page::btree_internal_page::BtreeInternalPage;
use crate::page::btree_leaf_page::BtreeLeafPage;
use crate::page::btree_page::{KeyComparator, check_key_size};
use crate::page::page_guard::{ReadPageGuard, WritePageGuard};
use crate::page::page_header::{PageHeader, PageType};

type InternalPage<'a> = BtreeInternalPage<&'a [u8; PAGE_SIZE]>;
type LeafPage<'a, const NUM_TOMBS: usize> = BtreeLeafPage<&'a [u8; PAGE_SIZE], NUM_TOMBS>;
type LeafPageMut<'a, const NUM_TOMBS: usize> = BtreeLeafPage<&'a mut [u8; PAGE_SIZE], NUM_TOMBS>;
type InternalPageMut<'a> = BtreeInternalPage<&'a mut [u8; PAGE_SIZE]>;

/// The pages an insert or remove holds on its way down the tree.
///
/// A writer keeps every page it may have to change latched until it reaches
/// a child that is safe, one that will not split or underflow, and then
/// releases everything above it. What is left in `write_set` is the path it
/// still has to fix up on the way back.
struct Context {
    /// The header page, held while the root may still change.
    header_page: Option<WritePageGuard>,
    /// The root when the operation started.
    root_page_id: PageId,
    /// Latched pages from the top down, the last one being the deepest.
    write_set: VecDeque<WritePageGuard>,
    /// Pages read on an optimistic descent, before retaking them for writing.
    #[allow(dead_code)] // Not used by any operation yet.
    read_set: VecDeque<ReadPageGuard>,
}

impl Context {
    fn new() -> Self {
        Self {
            header_page: None,
            root_page_id: INVALID_PAGE_ID,
            write_set: VecDeque::new(),
            read_set: VecDeque::new(),
        }
    }

    fn is_root_page(&self, page_id: PageId) -> bool {
        page_id == self.root_page_id
    }
}

/// A B+tree whose keys are ordered by `CmpT` and whose leaves keep up to
/// `NUM_TOMBS` tombstones.
///
/// A tree must always be opened with the `NUM_TOMBS` it was built with,
/// since it decides where a leaf's slots sit, and with a comparator that
/// orders keys the same way, since its pages are sorted by it.
pub struct Btree<CmpT, const NUM_TOMBS: usize = 0> {
    index_name: String,
    bpm: Arc<TracedBufferPoolManager>,
    comparator: CmpT,
    header_page_id: PageId,
}

impl<CmpT: KeyComparator, const NUM_TOMBS: usize> Btree<CmpT, NUM_TOMBS> {
    /// The longest key the tree accepts. Keys are copied into internal pages
    /// as separators, so it is the smaller of the two pages' limits.
    pub const MAX_KEY_SIZE: usize = {
        let leaf = LeafPage::<NUM_TOMBS>::MAX_KEY_SIZE;
        let internal = InternalPage::MAX_KEY_SIZE;
        if leaf < internal { leaf } else { internal }
    };

    /// Opens the tree whose header page is `header_page_id`. A header page
    /// that was allocated but never written, as the catalog leaves it, is
    /// formatted as an empty tree first.
    ///
    /// Fails with `UnexpectedPageType` if the page is some other kind of page.
    pub fn new(
        index_name: impl Into<String>,
        header_page_id: PageId,
        bpm: Arc<TracedBufferPoolManager>,
        comparator: CmpT,
    ) -> Result<Self> {
        let mut guard = bpm.checked_write_page(header_page_id, AccessType::Index)?;
        if page_type(guard.get_data()) == Ok(PageType::Invalid) {
            BtreeHeaderPage::init(guard.get_data_mut());
        } else {
            BtreeHeaderPage::new(guard.get_data())?;
        }
        drop(guard);

        Ok(Self {
            index_name: index_name.into(),
            bpm,
            comparator,
            header_page_id,
        })
    }

    pub fn index_name(&self) -> &str {
        &self.index_name
    }

    pub fn header_page_id(&self) -> PageId {
        self.header_page_id
    }

    pub fn comparator(&self) -> &CmpT {
        &self.comparator
    }

    /// The buffer pool the tree reads and writes its pages through.
    pub fn bpm(&self) -> &Arc<TracedBufferPoolManager> {
        &self.bpm
    }

    /// The root page, or `INVALID_PAGE_ID` if the tree is empty.
    pub fn get_root_page_id(&self) -> Result<PageId> {
        let guard = self
            .bpm
            .checked_read_page(self.header_page_id, AccessType::Index)?;
        Ok(BtreeHeaderPage::new(guard.get_data())?.root_page_id())
    }

    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.get_root_page_id()? == INVALID_PAGE_ID)
    }

    /// Appends the record id of `key` to `result` and returns `true`, or
    /// returns `false` if the tree does not hold `key`.
    pub fn get_value(&self, key: &[u8], result: &mut Vec<Rid>) -> Result<bool> {
        let Some(guard) = self.find_leaf(Some(key))? else {
            return Ok(false);
        };
        let leaf = LeafPage::<NUM_TOMBS>::new(guard.get_data())?;
        let Some(rid) = leaf.lookup(key, &self.comparator) else {
            return Ok(false);
        };
        result.push(rid);
        Ok(true)
    }

    /// Inserts `key` with `rid`. Returns `false`, changing nothing, if the
    /// tree already holds `key`.
    ///
    /// Fails with `KeyTooLarge` if `key` is longer than `MAX_KEY_SIZE`.
    pub fn insert(&self, key: &[u8], rid: Rid) -> Result<bool> {
        check_key_size(key, Self::MAX_KEY_SIZE)?;
        match self.insert_optimistic(key, rid)? {
            Some(inserted) => Ok(inserted),
            None => self.insert_pessimistic(key, rid),
        }
    }

    /// Inserts `key` into its leaf, holding only read latches above it.
    /// Returns `None`, changing nothing, if the tree is empty or the leaf
    /// has no room for `key`: either needs the pessimistic insert.
    fn insert_optimistic(&self, key: &[u8], rid: Rid) -> Result<Option<bool>> {
        let Some((mut guard, _)) = self.find_leaf_mut(key)? else {
            return Ok(None);
        };
        let mut leaf = LeafPageMut::<NUM_TOMBS>::new(guard.get_data_mut())?;
        match leaf.insert(key, rid, &self.comparator) {
            Ok(_) => Ok(Some(true)),
            Err(StorageError::DuplicateKey) => Ok(Some(false)),
            Err(StorageError::PageFull) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Inserts `key` holding write latches from the header page down, so it
    /// can split pages all the way up to the root.
    ///
    /// On the way down, reaching a safe page releases every page above it,
    /// the header page included: a split stops there, so they do not change.
    fn insert_pessimistic(&self, key: &[u8], rid: Rid) -> Result<bool> {
        let mut ctx = Context::new();
        let mut header = self
            .bpm
            .checked_write_page(self.header_page_id, AccessType::Index)?;
        ctx.root_page_id = BtreeHeaderPage::new(header.get_data())?.root_page_id();
        // Another insert may have started the tree since the optimistic
        // attempt saw it empty.
        if ctx.root_page_id == INVALID_PAGE_ID {
            self.start_new_tree(&mut header, key, rid)?;
            return Ok(true);
        }
        ctx.header_page = Some(header);

        let mut guard = self
            .bpm
            .checked_write_page(ctx.root_page_id, AccessType::Index)?;
        if self.is_insert_safe(guard.get_data(), key)? {
            ctx.header_page = None;
        }
        while page_type(guard.get_data()) != Ok(PageType::BTreeLeaf) {
            let child_page_id = InternalPage::new(guard.get_data())?.lookup(key, &self.comparator);
            let child = self
                .bpm
                .checked_write_page(child_page_id, AccessType::Index)?;
            if self.is_insert_safe(child.get_data(), key)? {
                ctx.header_page = None;
                ctx.write_set.clear();
                // Assigning drops the current page's guard after the
                // child's is taken.
                guard = child;
            } else {
                ctx.write_set
                    .push_back(std::mem::replace(&mut guard, child));
            }
        }

        let leaf_page_id = guard.page_id();
        let mut leaf = LeafPageMut::<NUM_TOMBS>::new(guard.get_data_mut())?;
        // The leaf may not need splitting after all: `key` may be there
        // already, or tombstoned, or another writer made room.
        match leaf.insert(key, rid, &self.comparator) {
            Ok(_) => return Ok(true),
            Err(StorageError::DuplicateKey) => return Ok(false),
            Err(StorageError::PageFull) => {}
            Err(e) => return Err(e),
        }

        let right_page_id = self.bpm.new_page();
        let mut right_guard = self
            .bpm
            .checked_write_page(right_page_id, AccessType::Index)?;
        let mut right = LeafPageMut::<NUM_TOMBS>::init(right_guard.get_data_mut());
        leaf.split_to(&mut right, right_page_id);
        let separator = right.key_at(0).to_vec();
        if self.comparator.compare(key, &separator).is_lt() {
            leaf.insert(key, rid, &self.comparator)
        } else {
            right.insert(key, rid, &self.comparator)
        }
        .expect("each half of a split has room for one more key");
        drop((guard, right_guard));

        self.insert_into_parent(&mut ctx, leaf_page_id, separator, right_page_id)?;
        Ok(true)
    }

    /// Puts `separator` and `right_page_id`, the page split off the right of
    /// `left_page_id`, into their parent, the deepest page in the write set.
    /// A parent with no room splits in turn and sends its own separator up,
    /// and a root that splits gets a new root above it.
    fn insert_into_parent(
        &self,
        ctx: &mut Context,
        mut left_page_id: PageId,
        mut separator: Vec<u8>,
        mut right_page_id: PageId,
    ) -> Result<()> {
        while let Some(mut guard) = ctx.write_set.pop_back() {
            let page_id = guard.page_id();
            let mut page = InternalPageMut::new(guard.get_data_mut())?;
            match page.insert(&separator, right_page_id, &self.comparator) {
                Ok(_) => return Ok(()),
                Err(StorageError::PageFull) => {}
                Err(e) => return Err(e),
            }

            let new_page_id = self.bpm.new_page();
            let mut new_guard = self
                .bpm
                .checked_write_page(new_page_id, AccessType::Index)?;
            let mut new_page = InternalPageMut::init(new_guard.get_data_mut());
            let promoted = page.split_to(&mut new_page);
            if self.comparator.compare(&separator, &promoted).is_lt() {
                page.insert(&separator, right_page_id, &self.comparator)
            } else {
                new_page.insert(&separator, right_page_id, &self.comparator)
            }
            .expect("each half of a split has room for one more key");

            (left_page_id, separator, right_page_id) = (page_id, promoted, new_page_id);
        }

        assert!(
            ctx.is_root_page(left_page_id),
            "split page {left_page_id} has no parent but is not the root"
        );
        let header = ctx
            .header_page
            .as_mut()
            .expect("the header page is held while the root may split");
        let root_page_id = self.bpm.new_page();
        let mut root = self
            .bpm
            .checked_write_page(root_page_id, AccessType::Index)?;
        InternalPageMut::init(root.get_data_mut()).populate_new_root(
            left_page_id,
            &separator,
            right_page_id,
        )?;
        BtreeHeaderPage::new(header.get_data_mut())?.set_root_page_id(root_page_id);
        Ok(())
    }

    /// Whether inserting `key` under the page in `data` cannot split it: a
    /// leaf with room for `key`, or an internal page with room for any
    /// separator a split below may send up, which is at most `MAX_KEY_SIZE`.
    fn is_insert_safe(&self, data: &[u8; PAGE_SIZE], key: &[u8]) -> Result<bool> {
        if page_type(data) == Ok(PageType::BTreeLeaf) {
            Ok(LeafPage::<NUM_TOMBS>::new(data)?.has_room_for(key.len()))
        } else {
            Ok(InternalPage::new(data)?.has_room_for(Self::MAX_KEY_SIZE))
        }
    }

    /// Makes a leaf holding only `key` the root of the empty tree whose
    /// header page is write-latched in `header`.
    fn start_new_tree(&self, header: &mut WritePageGuard, key: &[u8], rid: Rid) -> Result<()> {
        let root_page_id = self.bpm.new_page();
        let mut guard = self
            .bpm
            .checked_write_page(root_page_id, AccessType::Index)?;
        LeafPageMut::<NUM_TOMBS>::init(guard.get_data_mut()).insert(key, rid, &self.comparator)?;
        BtreeHeaderPage::new(header.get_data_mut())?.set_root_page_id(root_page_id);
        Ok(())
    }

    /// Removes `key`, if the tree holds it.
    pub fn remove(&self, key: &[u8]) -> Result<()> {
        if self.remove_optimistic(key)? {
            return Ok(());
        }
        self.remove_pessimistic(key)
    }

    /// Removes `key` from its leaf, holding only read latches above it.
    /// Returns `false`, changing nothing, if that would leave the leaf
    /// underfull, or a root leaf empty: either needs the pessimistic remove.
    fn remove_optimistic(&self, key: &[u8]) -> Result<bool> {
        let Some((mut guard, is_root)) = self.find_leaf_mut(key)? else {
            return Ok(true);
        };
        let mut leaf = LeafPageMut::<NUM_TOMBS>::new(guard.get_data_mut())?;
        if !self.is_leaf_remove_safe(&leaf, key, is_root) {
            return Ok(false);
        }
        leaf.remove(key, &self.comparator);
        Ok(true)
    }

    /// Removes `key` holding write latches from the header page down, so it
    /// can merge pages all the way up to the root.
    ///
    /// On the way down, reaching a safe page releases every page above it,
    /// the header page included: rebalancing stops there, so they do not
    /// change.
    fn remove_pessimistic(&self, key: &[u8]) -> Result<()> {
        let mut ctx = Context::new();
        let header = self
            .bpm
            .checked_write_page(self.header_page_id, AccessType::Index)?;
        ctx.root_page_id = BtreeHeaderPage::new(header.get_data())?.root_page_id();
        if ctx.root_page_id == INVALID_PAGE_ID {
            return Ok(());
        }
        ctx.header_page = Some(header);

        let mut guard = self
            .bpm
            .checked_write_page(ctx.root_page_id, AccessType::Index)?;
        if self.is_remove_safe(guard.get_data(), key, true)? {
            ctx.header_page = None;
        }
        while page_type(guard.get_data()) != Ok(PageType::BTreeLeaf) {
            let child_page_id = InternalPage::new(guard.get_data())?.lookup(key, &self.comparator);
            let child = self
                .bpm
                .checked_write_page(child_page_id, AccessType::Index)?;
            if self.is_remove_safe(child.get_data(), key, false)? {
                ctx.header_page = None;
                ctx.write_set.clear();
                // Assigning drops the current page's guard after the
                // child's is taken.
                guard = child;
            } else {
                ctx.write_set
                    .push_back(std::mem::replace(&mut guard, child));
            }
        }

        let removed =
            LeafPageMut::<NUM_TOMBS>::new(guard.get_data_mut())?.remove(key, &self.comparator);
        if removed.is_none() {
            return Ok(());
        }
        self.rebalance(&mut ctx, guard)
    }

    /// Fixes up the tree after a remove, from the leaf in `guard` up through
    /// the write set: an underfull page is rebalanced with a sibling, which
    /// may leave its parent underfull in turn, and an empty root leaf or a
    /// root with one child is dropped.
    fn rebalance(&self, ctx: &mut Context, mut guard: WritePageGuard) -> Result<()> {
        loop {
            if ctx.is_root_page(guard.page_id()) {
                return self.shrink_root(ctx, guard);
            }
            if !Self::is_underfull(guard.get_data())? {
                return Ok(());
            }
            // An underfull page whose parent was released was already
            // underfull and lost nothing: it is left as it is.
            let Some(mut parent) = ctx.write_set.pop_back() else {
                return Ok(());
            };
            self.rebalance_child(&mut parent, guard)?;
            guard = parent;
        }
    }

    /// Drops the root in `guard` if it is a leaf with no keys, leaving the
    /// tree empty, or an internal page with one child, which becomes the
    /// root.
    fn shrink_root(&self, ctx: &mut Context, guard: WritePageGuard) -> Result<()> {
        let new_root_page_id = if page_type(guard.get_data()) == Ok(PageType::BTreeLeaf) {
            if LeafPage::<NUM_TOMBS>::new(guard.get_data())?.size() > 0 {
                return Ok(());
            }
            INVALID_PAGE_ID
        } else {
            let root = InternalPage::new(guard.get_data())?;
            if root.size() > 1 {
                return Ok(());
            }
            root.value_at(0)
        };
        let header = ctx
            .header_page
            .as_mut()
            .expect("the header page is held while the root may go");
        BtreeHeaderPage::new(header.get_data_mut())?.set_root_page_id(new_root_page_id);
        let page_id = guard.page_id();
        drop(guard);
        self.delete_unlinked_page(page_id)
    }

    /// Deletes a page the tree no longer links to, whose write latch this
    /// thread held after unlinking it.
    ///
    /// Another thread may still pin it for a moment: a page guard releases
    /// its latch before its pin, so a scan that just moved past the page can
    /// be between the two. Nothing can reach the page any more, so that pin
    /// is about to go, and the delete is retried until it does.
    fn delete_unlinked_page(&self, page_id: PageId) -> Result<()> {
        loop {
            match self.bpm.delete_page(page_id) {
                Err(StorageError::PagePinned(_)) => std::thread::yield_now(),
                result => return result,
            }
        }
    }

    /// Rebalances `child`, an underfull child of `parent`, with its right
    /// sibling, or with its left one if it is the last child. A page that is
    /// its parent's only child is left as it is.
    fn rebalance_child(&self, parent: &mut WritePageGuard, child: WritePageGuard) -> Result<()> {
        let mut parent = InternalPageMut::new(parent.get_data_mut())?;
        let index = parent
            .value_index(child.page_id())
            .expect("an underfull page is a child of the page above it in the write set");
        if index + 1 < parent.size() {
            let right = self
                .bpm
                .checked_write_page(parent.value_at(index + 1), AccessType::Index)?;
            self.merge_or_redistribute(&mut parent, index + 1, child, right, true)
        } else if index > 0 {
            // Scans latch leaves left to right, so latching the left sibling
            // while holding the child could deadlock with one. The parent's
            // write latch keeps both pages as they are meanwhile.
            let child_page_id = child.page_id();
            drop(child);
            let left = self
                .bpm
                .checked_write_page(parent.value_at(index - 1), AccessType::Index)?;
            let child = self
                .bpm
                .checked_write_page(child_page_id, AccessType::Index)?;
            self.merge_or_redistribute(&mut parent, index, left, child, false)
        } else {
            Ok(())
        }
    }

    /// Rebalances two adjacent children of `parent`, `KEY(sep_index)` being
    /// the key between them, of which the left one is underfull if
    /// `left_is_underfull` and the right one otherwise. If they fit in one
    /// page, the right one merges into the left and is deleted. If not, keys
    /// move to the underfull one until the two are about even, as long as
    /// the parent has room for the new key between them.
    fn merge_or_redistribute(
        &self,
        parent: &mut InternalPageMut,
        sep_index: usize,
        mut left_guard: WritePageGuard,
        mut right_guard: WritePageGuard,
        left_is_underfull: bool,
    ) -> Result<()> {
        let right_page_id = right_guard.page_id();
        let merged = if page_type(left_guard.get_data()) == Ok(PageType::BTreeLeaf) {
            let mut left = LeafPageMut::<NUM_TOMBS>::new(left_guard.get_data_mut())?;
            let mut right = LeafPageMut::<NUM_TOMBS>::new(right_guard.get_data_mut())?;
            // Moving a key would lose its tombstone.
            left.purge_tombstones();
            right.purge_tombstones();
            if left.used_space() + right.used_space() <= LeafPage::<NUM_TOMBS>::CAPACITY {
                right.move_all_to(&mut left);
                true
            } else {
                redistribute_leaves(parent, sep_index, &mut left, &mut right, left_is_underfull)?;
                false
            }
        } else {
            let mut left = InternalPageMut::new(left_guard.get_data_mut())?;
            let mut right = InternalPageMut::new(right_guard.get_data_mut())?;
            let middle_key = parent.key_at(sep_index).to_vec();
            if left.used_space() + right.used_space() + middle_key.len() <= InternalPage::CAPACITY {
                right.move_all_to(&mut left, &middle_key);
                true
            } else {
                redistribute_internals(
                    parent,
                    sep_index,
                    &mut left,
                    &mut right,
                    left_is_underfull,
                )?;
                false
            }
        };
        if merged {
            parent.remove_at(sep_index);
            drop(right_guard);
            self.delete_unlinked_page(right_page_id)?;
        }
        Ok(())
    }

    /// Whether a page other than the root is less than half full.
    fn is_underfull(data: &[u8; PAGE_SIZE]) -> Result<bool> {
        if page_type(data) == Ok(PageType::BTreeLeaf) {
            Ok(LeafPage::<NUM_TOMBS>::new(data)?.is_underfull())
        } else {
            Ok(InternalPage::new(data)?.is_underfull())
        }
    }

    /// Whether removing `key` under the page in `data` cannot leave it
    /// underfull, or leave a root without keys or with one child.
    fn is_remove_safe(&self, data: &[u8; PAGE_SIZE], key: &[u8], is_root: bool) -> Result<bool> {
        if page_type(data) == Ok(PageType::BTreeLeaf) {
            let leaf = LeafPage::<NUM_TOMBS>::new(data)?;
            return Ok(self.is_leaf_remove_safe(&leaf, key, is_root));
        }
        let page = InternalPage::new(data)?;
        // Rebalancing a child takes at most one key of at most
        // `MAX_KEY_SIZE` out of the page, or shortens one by as much.
        Ok(if is_root {
            page.size() > 2
        } else {
            page.used_space()
                >= InternalPage::CAPACITY / 2 + InternalPage::entry_size(Self::MAX_KEY_SIZE)
        })
    }

    /// Whether removing `key` from `leaf` cannot leave it underfull, or empty
    /// for a root leaf.
    fn is_leaf_remove_safe<BufT: std::ops::Deref<Target = [u8; PAGE_SIZE]>>(
        &self,
        leaf: &BtreeLeafPage<BufT, NUM_TOMBS>,
        key: &[u8],
        is_root: bool,
    ) -> bool {
        let freed = leaf.bytes_freed_by_remove(key, &self.comparator);
        if freed == 0 {
            true
        } else if is_root {
            leaf.size() > 1
        } else {
            leaf.used_space() - freed >= LeafPage::<NUM_TOMBS>::CAPACITY / 2
        }
    }

    /// An iterator over every key in the tree, in increasing order.
    pub fn begin(&self) -> Result<IndexIterator<NUM_TOMBS>> {
        let leaf = self.find_leaf(None)?;
        Ok(IndexIterator::new(Arc::clone(&self.bpm), leaf, 0))
    }

    /// An iterator that is already past the last key.
    pub fn end(&self) -> IndexIterator<NUM_TOMBS> {
        IndexIterator::new(Arc::clone(&self.bpm), None, 0)
    }

    /// Descends from the root to the leaf whose range holds `key`, or to the
    /// leftmost leaf for `None`, and returns it read-latched. Returns `None`
    /// if the tree is empty.
    fn find_leaf(&self, key: Option<&[u8]>) -> Result<Option<ReadPageGuard>> {
        let header = self
            .bpm
            .checked_read_page(self.header_page_id, AccessType::Index)?;
        let root_page_id = BtreeHeaderPage::new(header.get_data())?.root_page_id();
        if root_page_id == INVALID_PAGE_ID {
            return Ok(None);
        }
        let mut guard = self
            .bpm
            .checked_read_page(root_page_id, AccessType::Index)?;
        drop(header);

        while page_type(guard.get_data()) != Ok(PageType::BTreeLeaf) {
            let child = {
                let page = InternalPage::new(guard.get_data())?;
                match key {
                    Some(key) => page.lookup(key, &self.comparator),
                    None => page.value_at(0),
                }
            };
            // Assigning drops the parent's guard only after the child's is
            // taken.
            guard = self.bpm.checked_read_page(child, AccessType::Index)?;
        }
        Ok(Some(guard))
    }

    /// Descends to the leaf whose range holds `key` like `find_leaf`, but
    /// returns it write-latched, with whether it is the root. Returns `None`
    /// if the tree is empty.
    ///
    /// A page's type is only known once it is latched, so the leaf is first
    /// read-latched like the pages above it, then released and write-latched.
    /// Its parent (or the header page, for a root leaf) stays read-latched
    /// meanwhile, and a leaf cannot be split or merged away without a write
    /// latch on that page, so it is still the leaf for `key`. Nor can it stop
    /// or start being the root while it is latched.
    fn find_leaf_mut(&self, key: &[u8]) -> Result<Option<(WritePageGuard, bool)>> {
        let header = self
            .bpm
            .checked_read_page(self.header_page_id, AccessType::Index)?;
        let mut page_id = BtreeHeaderPage::new(header.get_data())?.root_page_id();
        if page_id == INVALID_PAGE_ID {
            return Ok(None);
        }
        let root_page_id = page_id;
        let mut parent = header;
        loop {
            let guard = self.bpm.checked_read_page(page_id, AccessType::Index)?;
            if page_type(guard.get_data()) == Ok(PageType::BTreeLeaf) {
                drop(guard);
                let leaf = self.bpm.checked_write_page(page_id, AccessType::Index)?;
                drop(parent);
                return Ok(Some((leaf, page_id == root_page_id)));
            }
            page_id = InternalPage::new(guard.get_data())?.lookup(key, &self.comparator);
            // Assigning drops the parent's guard only after the child's is
            // taken.
            parent = guard;
        }
    }
}

/// Moves keys to the underfull one of two adjacent leaves, `KEY(sep_index)`
/// of `parent` being the key between them, from the front or the back of the
/// other one, until one more would leave it the fuller of the two, or the
/// parent has no room for the new key between them.
fn redistribute_leaves<const NUM_TOMBS: usize>(
    parent: &mut InternalPageMut,
    sep_index: usize,
    left: &mut LeafPageMut<NUM_TOMBS>,
    right: &mut LeafPageMut<NUM_TOMBS>,
    left_is_underfull: bool,
) -> Result<()> {
    let entry_size = LeafPage::<NUM_TOMBS>::entry_size;
    if left_is_underfull {
        while right.size() >= 2 {
            let moved = entry_size(right.key_at(0).len());
            if left.used_space() + moved > right.used_space() - moved
                || !try_set_key(parent, sep_index, right.key_at(1))?
            {
                break;
            }
            right.move_first_to_end_of(left);
        }
    } else {
        while left.size() >= 2 {
            let last = left.size() - 1;
            let moved = entry_size(left.key_at(last).len());
            if right.used_space() + moved > left.used_space() - moved
                || !try_set_key(parent, sep_index, left.key_at(last))?
            {
                break;
            }
            left.move_last_to_front_of(right);
        }
    }
    Ok(())
}

/// `redistribute_leaves` for internal pages, whose children move through
/// the parent: the key between the pages comes down as the moved child's
/// key, and the key it had goes up in its place.
fn redistribute_internals(
    parent: &mut InternalPageMut,
    sep_index: usize,
    left: &mut InternalPageMut,
    right: &mut InternalPageMut,
    left_is_underfull: bool,
) -> Result<()> {
    let entry_size = InternalPage::entry_size;
    if left_is_underfull {
        while right.size() >= 2 {
            let middle_key = parent.key_at(sep_index).to_vec();
            if left.used_space() + entry_size(middle_key.len())
                > right.used_space() - entry_size(right.key_at(1).len())
                || !try_set_key(parent, sep_index, right.key_at(1))?
            {
                break;
            }
            right.move_first_to_end_of(left, &middle_key);
        }
    } else {
        while left.size() >= 2 {
            let middle_key = parent.key_at(sep_index).to_vec();
            let last = left.size() - 1;
            if right.used_space() + entry_size(middle_key.len())
                > left.used_space() - entry_size(left.key_at(last).len())
                || !try_set_key(parent, sep_index, left.key_at(last))?
            {
                break;
            }
            left.move_last_to_front_of(right, &middle_key);
        }
    }
    Ok(())
}

/// Sets `KEY(index)` of `page` to `key`, or returns `false`, changing
/// nothing, if the page has no room for it.
fn try_set_key(page: &mut InternalPageMut, index: usize, key: &[u8]) -> Result<bool> {
    match page.set_key_at(index, key) {
        Ok(()) => Ok(true),
        Err(StorageError::PageFull) => Ok(false),
        Err(e) => Err(e),
    }
}

/// The page type in a page's header, or the raw value if it is unknown.
fn page_type(data: &[u8; PAGE_SIZE]) -> std::result::Result<PageType, u32> {
    let (header, _) = PageHeader::read_from_prefix(&data[..]).unwrap();
    header.page_type()
}
