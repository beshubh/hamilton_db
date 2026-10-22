//! A B+tree mapping fixed-size keys to record ids.
//!
//! The tree is entered through its header page (`BtreeHeaderPage`), which
//! holds the root page id. Internal pages route a key to the child whose
//! range holds it; leaves hold the keys with their record ids and are linked
//! left to right for scans. Keys are unique, as the tree's comparator
//! decides: two keys it orders as equal are the same key.
//!
//! Readers descend holding at most two read latches: a child's is taken
//! before its parent's is released (latch crabbing), so a writer can never
//! slip in between.

use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::Arc;

use common::{INVALID_PAGE_ID, PageId, Rid};
use zerocopy::FromBytes;

use crate::buffer::arc_replacer::AccessType;
use crate::buffer::traced_buffer_pool_manager::TracedBufferPoolManager;
use crate::error::Result;
use crate::index::index_iter::IndexIterator;
use crate::page::PAGE_SIZE;
use crate::page::btree_header_page::BtreeHeaderPage;
use crate::page::btree_internal_page::{BtreeInternalPage, BtreeKey, KeyComparator};
use crate::page::btree_leaf_page::BtreeLeafPage;
use crate::page::page_guard::{ReadPageGuard, WritePageGuard};
use crate::page::page_header::{PageHeader, PageType};

type InternalPage<'a, KeyT> = BtreeInternalPage<&'a [u8; PAGE_SIZE], KeyT>;
type LeafPage<'a, KeyT, const NUM_TOMBS: usize> =
    BtreeLeafPage<&'a [u8; PAGE_SIZE], KeyT, NUM_TOMBS>;

/// The pages an insert or remove holds on its way down the tree.
///
/// A writer keeps every page it may have to change latched until it reaches
/// a child that is safe, one that will not split or underflow, and then
/// releases everything above it. What is left in `write_set` is the path it
/// still has to fix up on the way back.
#[allow(dead_code)] // Used by `insert` and `remove` once they are written.
struct Context {
    /// The header page, held while the root may still change.
    header_page: Option<WritePageGuard>,
    /// The root when the operation started.
    root_page_id: PageId,
    /// Latched pages from the top down, the last one being the deepest.
    write_set: VecDeque<WritePageGuard>,
    /// Pages read on an optimistic descent, before retaking them for writing.
    read_set: VecDeque<ReadPageGuard>,
}

#[allow(dead_code)]
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

/// A B+tree with keys of type `KeyT`, ordered by `CmpT`, whose leaves keep
/// up to `NUM_TOMBS` tombstones.
///
/// A tree must always be opened with the `KeyT` and `NUM_TOMBS` it was built
/// with, since both decide where the arrays sit in its pages, and with a
/// comparator that orders keys the same way, since its pages are sorted by it.
pub struct Btree<KeyT, CmpT, const NUM_TOMBS: usize = 0> {
    index_name: String,
    bpm: Arc<TracedBufferPoolManager>,
    comparator: CmpT,
    leaf_max_size: usize,
    internal_max_size: usize,
    header_page_id: PageId,
    _key: PhantomData<KeyT>,
}

impl<KeyT: BtreeKey, CmpT: KeyComparator<KeyT>, const NUM_TOMBS: usize>
    Btree<KeyT, CmpT, NUM_TOMBS>
{
    /// Opens the tree whose header page is `header_page_id`. A header page
    /// that was allocated but never written, as the catalog leaves it, is
    /// formatted as an empty tree first.
    ///
    /// New leaves split once they hold more than `leaf_max_size` keys and
    /// new internal pages once they hold more than `internal_max_size`
    /// children. Pages already in the tree keep the sizes they were made with.
    ///
    /// Fails with `UnexpectedPageType` if the page is some other kind of page.
    ///
    /// # Panics
    /// If either max size is less than 2 or more than fits on a page.
    pub fn new(
        index_name: impl Into<String>,
        header_page_id: PageId,
        bpm: Arc<TracedBufferPoolManager>,
        comparator: CmpT,
        leaf_max_size: usize,
        internal_max_size: usize,
    ) -> Result<Self> {
        let leaf_slots = BtreeLeafPage::<&[u8; PAGE_SIZE], KeyT, NUM_TOMBS>::SLOT_CNT;
        assert!(
            (2..=leaf_slots).contains(&leaf_max_size),
            "leaf max size {leaf_max_size} out of range 2..={leaf_slots}"
        );
        let internal_slots = BtreeInternalPage::<&[u8; PAGE_SIZE], KeyT>::SLOT_CNT;
        assert!(
            (2..=internal_slots).contains(&internal_max_size),
            "internal max size {internal_max_size} out of range 2..={internal_slots}"
        );

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
            leaf_max_size,
            internal_max_size,
            header_page_id,
            _key: PhantomData,
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

    pub fn leaf_max_size(&self) -> usize {
        self.leaf_max_size
    }

    pub fn internal_max_size(&self) -> usize {
        self.internal_max_size
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
    pub fn get_value(&self, key: &KeyT, result: &mut Vec<Rid>) -> Result<bool> {
        let Some(guard) = self.find_leaf(Some(key))? else {
            return Ok(false);
        };
        let leaf = LeafPage::<KeyT, NUM_TOMBS>::new(guard.get_data())?;
        let Some(rid) = leaf.lookup(key, &self.comparator) else {
            return Ok(false);
        };
        result.push(rid);
        Ok(true)
    }

    /// Inserts `key` with `rid`. Returns `false`, changing nothing, if the
    /// tree already holds `key`.
    pub fn insert(&self, key: KeyT, rid: Rid) -> Result<bool> {
        let _ = (key, rid);
        todo!("B+tree insert")
    }

    /// Removes `key`, if the tree holds it.
    pub fn remove(&self, key: &KeyT) -> Result<()> {
        let _ = key;
        todo!("B+tree remove")
    }

    /// An iterator over every key in the tree, in increasing order.
    pub fn begin(&self) -> Result<IndexIterator<KeyT, NUM_TOMBS>> {
        let leaf = self.find_leaf(None)?;
        Ok(IndexIterator::new(Arc::clone(&self.bpm), leaf, 0))
    }

    /// An iterator that is already past the last key.
    pub fn end(&self) -> IndexIterator<KeyT, NUM_TOMBS> {
        IndexIterator::new(Arc::clone(&self.bpm), None, 0)
    }

    /// Descends from the root to the leaf whose range holds `key`, or to the
    /// leftmost leaf for `None`, and returns it read-latched. Returns `None`
    /// if the tree is empty.
    fn find_leaf(&self, key: Option<&KeyT>) -> Result<Option<ReadPageGuard>> {
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
                let page = InternalPage::<KeyT>::new(guard.get_data())?;
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
}

/// The page type in a page's header, or the raw value if it is unknown.
fn page_type(data: &[u8; PAGE_SIZE]) -> std::result::Result<PageType, u32> {
    let (header, _) = PageHeader::read_from_prefix(&data[..]).unwrap();
    header.page_type()
}
