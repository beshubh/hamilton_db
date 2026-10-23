use std::cmp::Ordering;
use std::ops::Bound;
use std::sync::Arc;

use common::{PageId, Rid, Schema, SlotId, Tuple, Value, ValueError};

use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::buffer::traced_buffer_pool_manager::TracedBufferPoolManager;
use crate::error::Result;
use crate::index::btree::Btree;
use crate::index::{IndexMetadata, IndexTrait};
use crate::page::btree_page::{KeyComparator, check_key_size};

type Tree = Btree<EntryComparator>;

/// Bytes a record id takes at the end of an entry.
const RID_SIZE: usize = size_of::<PageId>() + size_of::<SlotId>();

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

impl KeySchemaComparator {
    /// Compares the first `prefix_schema.len()` columns of `key` with
    /// `prefix`, a tuple of just those columns as `prefix_schema` encodes
    /// them.
    fn compare_prefix(&self, key: &[u8], prefix: &[u8], prefix_schema: &Schema) -> Ordering {
        let (key, prefix) = (
            Tuple::from_bytes(key.to_vec()),
            Tuple::from_bytes(prefix.to_vec()),
        );
        for idx in 0..prefix_schema.len() {
            let decode = |tuple: &Tuple, schema| {
                tuple
                    .value(schema, idx)
                    .unwrap_or_else(|e| panic!("corrupt index key: {e}"))
            };
            let order = compare_values(
                &decode(&key, &self.key_schema),
                &decode(&prefix, prefix_schema),
            );
            if order.is_ne() {
                return order;
            }
        }
        Ordering::Equal
    }
}

impl KeyComparator for KeySchemaComparator {
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        self.compare_prefix(a, b, &self.key_schema)
    }
}

/// Orders two values of a key column, with NULL first.
fn compare_values(a: &Value, b: &Value) -> Ordering {
    match (a.is_null(), b.is_null()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Less,
        (false, true) => Ordering::Greater,
        (false, false) => a
            .compare(b)
            .unwrap_or_else(|e| panic!("corrupt index key: {e}"))
            .expect("neither value is NULL"),
    }
}

/// Orders a `BtreeIndex`'s entries: by key with `KeySchemaComparator`, and
/// then by record id for an index whose entries end in one.
///
/// # Panics
/// If an entry is shorter than a record id, or its key does not decode.
pub struct EntryComparator {
    keys: KeySchemaComparator,
    /// Whether entries are `key || rid` rather than just the key.
    with_rid: bool,
}

impl EntryComparator {
    /// The entry for `key` in the row at `rid`.
    fn entry(&self, key: &[u8], rid: Rid) -> Vec<u8> {
        let mut entry = Vec::with_capacity(key.len() + RID_SIZE);
        entry.extend_from_slice(key);
        if self.with_rid {
            entry.extend_from_slice(&rid.page_id.to_le_bytes());
            entry.extend_from_slice(&rid.slot.to_le_bytes());
        }
        entry
    }

    /// The key of `entry`, without its record id if it has one.
    pub fn key_of<'a>(&self, entry: &'a [u8]) -> &'a [u8] {
        if self.with_rid {
            split_rid(entry).0
        } else {
            entry
        }
    }
}

impl KeyComparator for EntryComparator {
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        if !self.with_rid {
            return self.keys.compare(a, b);
        }
        let ((a_key, a_rid), (b_key, b_rid)) = (split_rid(a), split_rid(b));
        self.keys.compare(a_key, b_key).then(a_rid.cmp(&b_rid))
    }
}

/// Splits `key || rid` into the key's bytes and the record id.
///
/// # Panics
/// If `entry` is shorter than a record id.
fn split_rid(entry: &[u8]) -> (&[u8], Rid) {
    assert!(
        entry.len() >= RID_SIZE,
        "index entry shorter than a record id"
    );
    let (key, rid) = entry.split_at(entry.len() - RID_SIZE);
    let (page_id, slot) = rid.split_at(size_of::<PageId>());
    let rid = Rid::new(
        PageId::from_le_bytes(page_id.try_into().unwrap()),
        SlotId::from_le_bytes(slot.try_into().unwrap()),
    );
    (key, rid)
}

/// Values for the first columns of an index's key, any number of them, as a
/// tuple of just those columns.
#[derive(Clone)]
struct KeyPrefix {
    key: Tuple,
    /// The first columns of the key schema, as many as the prefix has.
    schema: Arc<Schema>,
}

/// Places a key prefix among an index's entries, for
/// `Btree::begin_at_by`: before every entry whose key starts with it if
/// `tie` is `Greater`, after them if it is `Less`.
struct PrefixProbe<'a> {
    entries: &'a EntryComparator,
    schema: &'a Schema,
    /// How an entry whose key starts with the prefix compares with it.
    tie: Ordering,
}

impl KeyComparator for PrefixProbe<'_> {
    fn compare(&self, entry: &[u8], prefix: &[u8]) -> Ordering {
        let key = self.entries.key_of(entry);
        self.entries
            .keys
            .compare_prefix(key, prefix, self.schema)
            .then(self.tie)
    }
}

/// An index stored in a B+tree, with keys that are key tuples as
/// `Tuple::to_key` encodes them.
///
/// A primary key index is unique: each entry is a key, mapped to its row's
/// record id, and an insert of a key it already holds fails. Rows whose keys
/// hold a NULL collide too, as NULLs are equal to each other.
///
/// Any other index holds one entry per row, its key followed by its record
/// id, mapped to the same record id. Rows with the same key are distinct
/// entries, kept in record id order.
pub struct BtreeIndex {
    metadata: IndexMetadata,
    tree: Tree,
}

impl BtreeIndex {
    /// The longest encoded key tuple the index accepts: what is left of the
    /// tree's limit after a record id, whether or not entries have one.
    pub const MAX_KEY_SIZE: usize = Tree::MAX_KEY_SIZE - RID_SIZE;

    /// Opens the index whose B+tree header page is `header_page_id`,
    /// formatting it as an empty tree if it was never written.
    pub fn new(
        metadata: IndexMetadata,
        header_page_id: PageId,
        bpm: Arc<BufferPoolManager>,
    ) -> Result<Self> {
        let comparator = EntryComparator {
            keys: KeySchemaComparator::new(Arc::clone(metadata.key_schema())),
            with_rid: !metadata.is_primary_key(),
        };
        let tree = Tree::new(
            metadata.name(),
            header_page_id,
            Arc::new(TracedBufferPoolManager::new(bpm)),
            comparator,
        )?;
        Ok(Self { metadata, tree })
    }

    /// The tree, whose keys are entries as its comparator's `key_of` reads
    /// them.
    pub fn tree(&self) -> &Btree<EntryComparator> {
        &self.tree
    }

    /// The record ids of the rows whose keys are between `lower` and
    /// `upper`, in key order and then record id order.
    ///
    /// A bound holds values for the first columns of the key, any number of
    /// them, and keys are compared with it on those columns only. On an
    /// index on `(name, age)`, a lower bound of `Excluded(&[name])` starts
    /// after every row with that name, and one of `Included(&[name, age])`
    /// at the first row with both. `(Unbounded, Unbounded)` scans the whole
    /// index. NULL sorts first, as in the index.
    ///
    /// Fails with `ColumnCountMismatch` if a bound has more values than the
    /// key has columns, or `TypeMismatch` if a value does not have its
    /// column's type.
    ///
    /// The leaf being read stays read-latched until the iterator moves past
    /// it or is dropped, as with `Btree::begin`.
    pub fn scan_range(
        &self,
        lower: Bound<&[Value]>,
        upper: Bound<&[Value]>,
    ) -> Result<impl Iterator<Item = Result<Rid>>> {
        self.scan(self.prefix_bound(lower)?, self.prefix_bound(upper)?)
    }

    fn prefix_bound(&self, bound: Bound<&[Value]>) -> Result<Bound<KeyPrefix>> {
        Ok(match bound {
            Bound::Included(values) => Bound::Included(self.prefix(values)?),
            Bound::Excluded(values) => Bound::Excluded(self.prefix(values)?),
            Bound::Unbounded => Bound::Unbounded,
        })
    }

    fn prefix(&self, values: &[Value]) -> Result<KeyPrefix> {
        let columns = self.metadata.key_schema().columns();
        if values.len() > columns.len() {
            return Err(ValueError::ColumnCountMismatch {
                expected: columns.len(),
                found: values.len(),
            }
            .into());
        }
        let schema = Schema::new(columns[..values.len()].to_vec());
        Ok(KeyPrefix {
            key: Tuple::encode(&schema, values)?,
            schema: Arc::new(schema),
        })
    }

    fn scan(
        &self,
        lower: Bound<KeyPrefix>,
        upper: Bound<KeyPrefix>,
    ) -> Result<impl Iterator<Item = Result<Rid>>> {
        let cmp = self.tree.comparator();
        let seek = |prefix: &KeyPrefix, tie| {
            let probe = PrefixProbe {
                entries: cmp,
                schema: &prefix.schema,
                tie,
            };
            self.tree.begin_at_by(prefix.key.data(), &probe)
        };
        let iter = match &lower {
            Bound::Included(prefix) => seek(prefix, Ordering::Greater)?,
            Bound::Excluded(prefix) => seek(prefix, Ordering::Less)?,
            Bound::Unbounded => self.tree.begin()?,
        };
        let below_upper = move |entry: &[u8]| {
            let key = cmp.key_of(entry);
            match &upper {
                Bound::Included(p) => cmp
                    .keys
                    .compare_prefix(key, p.key.data(), &p.schema)
                    .is_le(),
                Bound::Excluded(p) => cmp
                    .keys
                    .compare_prefix(key, p.key.data(), &p.schema)
                    .is_lt(),
                Bound::Unbounded => true,
            }
        };
        Ok(iter
            // An error is passed on, and the iterator ends after it.
            .take_while(move |e| match e {
                Ok((entry, _)) => below_upper(entry),
                Err(_) => true,
            })
            .map(|e| e.map(|(_, rid)| rid)))
    }
}

impl IndexTrait for BtreeIndex {
    fn metadata(&self) -> &IndexMetadata {
        &self.metadata
    }

    /// Fails with `KeyTooLarge` if the encoded key is longer than
    /// `MAX_KEY_SIZE`.
    fn insert_entry(&self, key: &Tuple, rid: Rid) -> Result<bool> {
        check_key_size(key.data(), Self::MAX_KEY_SIZE)?;
        let entry = self.tree.comparator().entry(key.data(), rid);
        self.tree.insert(&entry, rid)
    }

    /// In a primary key index, `key` is removed whatever record id it has.
    fn delete_entry(&self, key: &Tuple, rid: Rid) -> Result<()> {
        self.tree
            .remove(&self.tree.comparator().entry(key.data(), rid))
    }

    fn scan_key(&self, key: &Tuple, result: &mut Vec<Rid>) -> Result<()> {
        let key = KeyPrefix {
            key: key.clone(),
            schema: Arc::clone(self.metadata.key_schema()),
        };
        for rid in self.scan(Bound::Included(key.clone()), Bound::Included(key))? {
            result.push(rid?);
        }
        Ok(())
    }
}
