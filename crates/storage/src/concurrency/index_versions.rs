//! Index entries under MVTO.
//!
//! Index entries are not versioned, but every version a reader can see must
//! stay reachable through them. So a transaction never removes an entry.
//! It adds the entries its new versions need (`Transaction::insert_entry`),
//! and *retires* the ones its writes left behind (`retire_entry`): every
//! entry of a row it deletes, and the old entry of a row whose key or
//! locator it changes. Readers skip entries that lead them to a row whose
//! visible version has another key.
//!
//! **Garbage collection.** A committed transaction's retired entries wait
//! with its dead tuples until the watermark passes its Tid. Then each one is
//! removed unless some version of a row it leads to, visible at or above the
//! watermark, still has its key (see [`TableEntry::is_needed`]): a later
//! write may have set the key back. An entry that stores a rid leads to
//! that row; one that stores a primary key, to every row the primary key
//! index holds that key for. Abort removes the entries the transaction
//! added, and retires those it found already there.
//!
//! **The entry lock.** A writer that finds its entry already in the index
//! counts on it staying. Writers add entries under the read side of the
//! manager's entry lock, and garbage collection checks and removes each one
//! under the write side, so an add lands before the check sees the
//! writer's version or after the removal. For the same reason a row's
//! primary key entry goes in before its other entries: the check finds the
//! rows behind a primary key through it.
//!
//! **Uniqueness.** A primary key index may hold a key for many rids: old
//! versions of a row keep their entries after it moves, or after it is
//! deleted and the key inserted again. `Transaction::insert_entry` adds the
//! entry first and then looks at every other row the key leads to. A
//! version there with the key, written by another transaction that is
//! running or committed after this one, is a `Conflict`; failing that, the
//! key is taken if the version this transaction sees has it. Adding before
//! checking means that of two transactions inserting a key at once, at
//! least one sees the other.

use std::sync::Arc;

use common::{Rid, Schema, Tuple};

use crate::error::Result;
use crate::index::btree_index::KeySchemaComparator;
use crate::index::{Index, IndexTrait, RowLocator};
use crate::page::btree_page::KeyComparator;
use crate::table::table_heap::TableHeap;
use crate::table::undo_log::UndoLog;

/// A table as its index entries need it: the heap and schema that rows are
/// read from, and the primary key index that entries storing primary keys
/// are followed through.
pub struct IndexedTable {
    pub heap: Arc<TableHeap>,
    pub schema: Schema,
    pub primary_key: Option<Arc<Index>>,
}

impl IndexedTable {
    pub fn new(heap: Arc<TableHeap>, schema: Schema, primary_key: Option<Arc<Index>>) -> Self {
        Self {
            heap,
            schema,
            primary_key,
        }
    }

    /// The rids that `locator` may lead to: itself, or each one the primary
    /// key index holds the primary key for.
    pub fn rids(&self, locator: &RowLocator) -> Result<Vec<Rid>> {
        let pk = match locator {
            RowLocator::Rid(rid) => return Ok(vec![*rid]),
            RowLocator::PrimaryKey(pk) => pk,
        };
        let index = self
            .primary_key
            .as_ref()
            .expect("only a table with a primary key has indexes that store it");
        let mut found = Vec::new();
        index.scan_key(pk, &mut found)?;
        Ok(found
            .iter()
            .map(|locator| locator.as_rid().expect("the primary key index stores rids"))
            .collect())
    }
}

/// An entry of one of a table's indexes, as a transaction records it.
pub(crate) struct TableEntry {
    pub table: Arc<IndexedTable>,
    pub index: Arc<Index>,
    pub key: Tuple,
    pub locator: RowLocator,
}

impl TableEntry {
    /// Whether a reader at or above `watermark` may still need the entry:
    /// some version it might see, of a row the entry leads to, has the
    /// entry's key, and the primary key the entry stores if it stores one.
    pub fn is_needed(&self, watermark: u64, undo: &UndoLog) -> Result<bool> {
        let table = &self.table;
        for rid in table.rids(&self.locator)? {
            let versions = table
                .heap
                .versions_since(rid, watermark, &table.schema, undo)?;
            for (meta, version) in versions {
                if meta.is_deleted || !has_key(&self.index, &table.schema, &version, &self.key)? {
                    continue;
                }
                let pk_matches = match &self.locator {
                    RowLocator::Rid(_) => true,
                    RowLocator::PrimaryKey(pk) => {
                        let primary_key = table.primary_key.as_ref().expect("checked by `rids`");
                        has_key(primary_key, &table.schema, &version, pk)?
                    }
                };
                if pk_matches {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }
}

/// Whether `tuple`, a row laid out as `schema`, has `key` in `index`, as the
/// index compares keys.
pub(crate) fn has_key(index: &Index, schema: &Schema, tuple: &Tuple, key: &Tuple) -> Result<bool> {
    let metadata = index.metadata();
    let own = tuple.to_key(schema, metadata.key_schema(), metadata.key_attrs())?;
    let keys = KeySchemaComparator::new(Arc::clone(metadata.key_schema()));
    Ok(keys.compare(own.data(), key.data()).is_eq())
}
