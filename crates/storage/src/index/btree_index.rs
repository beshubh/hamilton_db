use std::cmp::Ordering;
use std::sync::Arc;

use common::{PageId, Rid, Schema, Tuple};

use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::buffer::traced_buffer_pool_manager::TracedBufferPoolManager;
use crate::error::Result;
use crate::index::btree::Btree;
use crate::index::{IndexMetadata, IndexTrait};
use crate::page::btree_page::KeyComparator;

type Tree = Btree<KeySchemaComparator>;

/// Orders index keys, which are key tuples as `Tuple::to_key` encodes them,
/// by decoding them with the index's key schema and comparing column by
/// column, as `Value::compare` orders values. NULL sorts before every other
/// value, and NULLs are equal to each other.
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

impl KeyComparator for KeySchemaComparator {
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
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

/// An index stored in a B+tree. Its keys are key tuples as
/// `Tuple::to_key` encodes them.
pub struct BtreeIndex {
    metadata: IndexMetadata,
    tree: Tree,
}

impl BtreeIndex {
    /// The longest encoded key tuple the index accepts.
    pub const MAX_KEY_SIZE: usize = Tree::MAX_KEY_SIZE;

    /// Opens the index whose B+tree header page is `header_page_id`,
    /// formatting it as an empty tree if it was never written.
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
        )?;
        Ok(Self { metadata, tree })
    }

    pub fn tree(&self) -> &Btree<KeySchemaComparator> {
        &self.tree
    }
}

impl IndexTrait for BtreeIndex {
    fn metadata(&self) -> &IndexMetadata {
        &self.metadata
    }

    /// Fails with `KeyTooLarge` if the encoded key is longer than
    /// `MAX_KEY_SIZE`.
    fn insert_entry(&self, key: &Tuple, rid: Rid) -> Result<bool> {
        self.tree.insert(key.data(), rid)
    }

    /// Keys are unique, so `key` is removed whatever record id it has.
    fn delete_entry(&self, key: &Tuple, _rid: Rid) -> Result<()> {
        self.tree.remove(key.data())
    }

    fn scan_key(&self, key: &Tuple, result: &mut Vec<Rid>) -> Result<()> {
        self.tree.get_value(key.data(), result)?;
        Ok(())
    }
}
