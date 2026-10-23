use std::fmt;
use std::sync::Arc;

use btree_index::BtreeIndex;
use common::{Rid, Schema, Tuple};
use enum_dispatch::enum_dispatch;

use crate::error::Result;

pub mod btree;
pub mod btree_index;
pub mod index_iter;

/// What an index is on: its table, which of the table's columns make up the
/// key, and the key columns' own schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexMetadata {
    name: String,
    table_name: String,
    /// Positions in the table's schema of the key columns, in key order.
    key_attrs: Vec<usize>,
    /// The key columns as a schema of their own, for encoding keys.
    key_schema: Arc<Schema>,
    is_primary_key: bool,
}

impl IndexMetadata {
    pub fn new(
        name: impl Into<String>,
        table_name: impl Into<String>,
        key_attrs: Vec<usize>,
        key_schema: Arc<Schema>,
        is_primary_key: bool,
    ) -> Self {
        debug_assert_eq!(
            key_attrs.len(),
            key_schema.len(),
            "one key attribute per key column"
        );
        Self {
            name: name.into(),
            table_name: table_name.into(),
            key_attrs,
            key_schema,
            is_primary_key,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Positions in the table's schema of the key columns, in key order, as
    /// `Tuple::to_key` takes them.
    pub fn key_attrs(&self) -> &[usize] {
        &self.key_attrs
    }

    pub fn key_schema(&self) -> &Arc<Schema> {
        &self.key_schema
    }

    pub fn is_primary_key(&self) -> bool {
        self.is_primary_key
    }
}

impl fmt::Display for IndexMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ON {} (", self.name, self.table_name)?;
        for (i, column) in self.key_schema.columns().iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{} {}", column.name, column.data_type)?;
        }
        write!(f, ")")?;
        if self.is_primary_key {
            write!(f, " PRIMARY KEY")?;
        }
        Ok(())
    }
}

/// An index from keys to the record ids of the rows that have them. Keys are
/// tuples in the index's key schema, as `Tuple::to_key` builds them. A
/// primary key index is unique, holding a key for at most one row. In any
/// other index, any number of rows may share a key.
#[enum_dispatch]
pub trait IndexTrait {
    fn metadata(&self) -> &IndexMetadata;

    /// Adds `key` pointing to `rid`. Returns `false`, changing nothing, if
    /// the index already holds `key` for `rid`, or for any row in a unique
    /// index.
    // TODO: take the transaction once there are transactions.
    fn insert_entry(&self, key: &Tuple, rid: Rid) -> Result<bool>;

    /// Removes `key` for `rid`, if the index holds it. Other rows with
    /// `key` keep their entries.
    fn delete_entry(&self, key: &Tuple, rid: Rid) -> Result<()>;

    /// Appends the record ids of the rows with `key` to `result`.
    fn scan_key(&self, key: &Tuple, result: &mut Vec<Rid>) -> Result<()>;
}

#[enum_dispatch(IndexTrait)]
pub enum Index {
    BtreeIndex,
}
