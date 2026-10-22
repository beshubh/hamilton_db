use std::marker::PhantomData;
use std::sync::Arc;

use common::{INVALID_PAGE_ID, Rid};

use crate::buffer::arc_replacer::AccessType;
use crate::buffer::traced_buffer_pool_manager::TracedBufferPoolManager;
use crate::error::Result;
use crate::page::btree_internal_page::BtreeKey;
use crate::page::btree_leaf_page::BtreeLeafPage;
use crate::page::page_guard::ReadPageGuard;

/// Iterates over the live keys of a B+tree in increasing order, with their
/// record ids, following the leaves' `next_page_id` links. Tombstoned keys
/// are skipped.
///
/// The current leaf stays read-latched until the iterator moves past it or
/// is dropped. The next leaf is latched before the current one is released,
/// so leaves are always latched left to right.
///
/// After an error the iterator is done.
pub struct IndexIterator<KeyT, const NUM_TOMBS: usize = 0> {
    bpm: Arc<TracedBufferPoolManager>,
    /// The leaf being read, or `None` once the iterator is done.
    guard: Option<ReadPageGuard>,
    /// Next slot to read on that leaf.
    idx: usize,
    _key: PhantomData<KeyT>,
}

impl<KeyT: BtreeKey, const NUM_TOMBS: usize> IndexIterator<KeyT, NUM_TOMBS> {
    /// Starts at slot `idx` of `leaf`, or is already done for `None`.
    pub(crate) fn new(
        bpm: Arc<TracedBufferPoolManager>,
        leaf: Option<ReadPageGuard>,
        idx: usize,
    ) -> Self {
        Self {
            bpm,
            guard: leaf,
            idx,
            _key: PhantomData,
        }
    }

    /// Whether the iterator is done. It may also turn out to be done on the
    /// next call to `next`, if the rest of the tree holds no live key.
    pub fn is_end(&self) -> bool {
        self.guard.is_none()
    }

    /// Moves to the next live key, latching later leaves as needed.
    fn advance(&mut self) -> Result<Option<(KeyT, Rid)>> {
        while let Some(guard) = &self.guard {
            let leaf = BtreeLeafPage::<_, KeyT, NUM_TOMBS>::new(guard.get_data())?;
            while self.idx < leaf.size() {
                let idx = self.idx;
                self.idx += 1;
                if !leaf.is_tombstone(idx) {
                    return Ok(Some((leaf.key_at(idx), leaf.value_at(idx))));
                }
            }
            let next_page_id = leaf.next_page_id();
            self.guard = if next_page_id == INVALID_PAGE_ID {
                None
            } else {
                Some(self.bpm.checked_read_page(next_page_id, AccessType::Scan)?)
            };
            self.idx = 0;
        }
        Ok(None)
    }
}

impl<KeyT: BtreeKey, const NUM_TOMBS: usize> Iterator for IndexIterator<KeyT, NUM_TOMBS> {
    type Item = Result<(KeyT, Rid)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.advance() {
            Ok(entry) => entry.map(Ok),
            Err(e) => {
                self.guard = None;
                Some(Err(e))
            }
        }
    }
}
