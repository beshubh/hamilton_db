use std::cmp::Ordering;
use std::sync::Arc;

use common::{PageId, Rid, Schema, Tuple};

use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::buffer::traced_buffer_pool_manager::TracedBufferPoolManager;
use crate::error::Result;
use crate::index::btree::Btree;
use crate::index::{IndexMetadata, IndexTrait};
use crate::page::PAGE_SIZE;
use crate::page::btree_internal_page::{BtreeInternalPage, KeyComparator};
use crate::page::btree_leaf_page::BtreeLeafPage;

/// Bytes in a key stored in a `BtreeIndex`.
pub const INDEX_KEY_SIZE: usize = 16;

/// A key as a `BtreeIndex` stores it: the key tuple's encoding
/// (`Tuple::to_key`), zero-padded. `KeySchemaComparator` orders it.
// TODO: build these from key tuples, rejecting ones longer than INDEX_KEY_SIZE.
pub type IndexKey = [u8; INDEX_KEY_SIZE];

type Tree = Btree<IndexKey, KeySchemaComparator>;

/// Orders `IndexKey`s by decoding them with an index's key schema and
/// comparing column by column, as `Value::compare` orders values. NULL sorts
/// before every other value, and NULLs are equal to each other.
///
/// # Panics
/// If a key does not decode with the key schema, or a column holds a type
/// its schema does not.
pub struct KeySchemaComparator {
    key_schema: Arc<Schema>,
}

impl KeySchemaComparator {
    pub fn new(key_schema: Arc<Schema>) -> Self {
        Self { key_schema }
    }
}

impl KeyComparator<IndexKey> for KeySchemaComparator {
    fn compare(&self, a: &IndexKey, b: &IndexKey) -> Ordering {
        // Columns are found through the schema's offsets, so the padding is
        // never read.
        let (a, b) = (Tuple::from_bytes(a.to_vec()), Tuple::from_bytes(b.to_vec()));
        for idx in 0..self.key_schema.len() {
            let decode = |key: &Tuple| {
                key.value(&self.key_schema, idx)
                    .unwrap_or_else(|e| panic!("corrupt index key: {e}"))
            };
            let (a, b) = (decode(&a), decode(&b));
            let order = match (a.is_null(), b.is_null()) {
                (true, true) => Ordering::Equal,
                (true, false) => Ordering::Less,
                (false, true) => Ordering::Greater,
                (false, false) => a
                    .compare(&b)
                    .unwrap_or_else(|e| panic!("corrupt index key: {e}"))
                    .expect("neither value is NULL"),
            };
            if order.is_ne() {
                return order;
            }
        }
        Ordering::Equal
    }
}

/// An index stored in a B+tree.
pub struct BtreeIndex {
    metadata: IndexMetadata,
    tree: Tree,
}

impl BtreeIndex {
    /// Opens the index whose B+tree header page is `header_page_id`,
    /// formatting it as an empty tree if it was never written. Pages are
    /// filled as far as they can be before they split.
    pub fn new(
        metadata: IndexMetadata,
        header_page_id: PageId,
        bpm: Arc<BufferPoolManager>,
    ) -> Result<Self> {
        let tree = Tree::new(
            metadata.name(),
            header_page_id,
            Arc::new(TracedBufferPoolManager::new(bpm)),
            KeySchemaComparator::new(Arc::clone(metadata.key_schema())),
            BtreeLeafPage::<&[u8; PAGE_SIZE], IndexKey, 0>::SLOT_CNT,
            BtreeInternalPage::<&[u8; PAGE_SIZE], IndexKey>::SLOT_CNT,
        )?;
        Ok(Self { metadata, tree })
    }

    pub fn tree(&self) -> &Btree<IndexKey, KeySchemaComparator> {
        &self.tree
    }
}

impl IndexTrait for BtreeIndex {
    fn metadata(&self) -> &IndexMetadata {
        &self.metadata
    }

    fn insert_entry(&self, key: &Tuple, rid: Rid) -> Result<bool> {
        let _ = (key, rid);
        todo!("encode the key and call Btree::insert")
    }

    fn delete_entry(&self, key: &Tuple, rid: Rid) -> Result<()> {
        let _ = (key, rid);
        todo!("encode the key and call Btree::remove")
    }

    fn scan_key(&self, key: &Tuple, result: &mut Vec<Rid>) -> Result<()> {
        let _ = (key, result);
        todo!("encode the key and call Btree::get_value")
    }
}
