use std::fmt;
use std::sync::Arc;

use btree_index::BtreeIndex;
use common::{Rid, Schema, Tuple};
use enum_dispatch::enum_dispatch;

use crate::error::Result;

pub mod btree;
pub mod btree_index;
pub mod index_iter;

/// What an index entry stores to find its row.
///
/// The primary key index stores the rid. A secondary index stores the row's
/// primary key if the table has one, and the rid otherwise.
///
/// Storing the primary key means a row can move to a new rid without any
/// secondary index changing. The cost is a second lookup, in the primary key
/// index, to turn the primary key into a rid.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowLocator {
    Rid(Rid),
    /// The row's primary key, as `Tuple::to_key` encodes it.
    PrimaryKey(Tuple),
}

impl RowLocator {
    pub fn kind(&self) -> LocatorKind {
        match self {
            RowLocator::Rid(_) => LocatorKind::Rid,
            RowLocator::PrimaryKey(_) => LocatorKind::PrimaryKey,
        }
    }

    /// The record id, if this locator is one.
    pub fn as_rid(&self) -> Option<Rid> {
        match self {
            RowLocator::Rid(rid) => Some(*rid),
            RowLocator::PrimaryKey(_) => None,
        }
    }
}

impl From<Rid> for RowLocator {
    fn from(rid: Rid) -> Self {
        RowLocator::Rid(rid)
    }
}

/// Which kind of `RowLocator` an index stores.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocatorKind {
    Rid,
    PrimaryKey,
}

/// What an index is on: its table, which of the table's columns make up the
/// key, the key columns' own schema, and which `RowLocator` it stores.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexMetadata {
    name: String,
    table_name: String,
    /// Positions in the table's schema of the key columns, in key order.
    key_attrs: Vec<usize>,
    /// The key columns as a schema of their own, for encoding keys.
    key_schema: Arc<Schema>,
    is_primary_key: bool,
    locator: LocatorKind,
}

impl IndexMetadata {
    /// `locator` must be `LocatorKind::Rid` for a primary key index.
    pub fn new(
        name: impl Into<String>,
        table_name: impl Into<String>,
        key_attrs: Vec<usize>,
        key_schema: Arc<Schema>,
        is_primary_key: bool,
        locator: LocatorKind,
    ) -> Self {
        debug_assert_eq!(
            key_attrs.len(),
            key_schema.len(),
            "one key attribute per key column"
        );
        debug_assert!(
            !is_primary_key || locator == LocatorKind::Rid,
            "a primary key index points with record ids"
        );
        Self {
            name: name.into(),
            table_name: table_name.into(),
            key_attrs,
            key_schema,
            is_primary_key,
            locator,
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

    /// Which `RowLocator` the index's entries store.
    pub fn locator(&self) -> LocatorKind {
        self.locator
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

/// An index from keys to rows. Keys are tuples in the index's key schema, as
/// `Tuple::to_key` builds them. A primary key index holds each key at most
/// once; other indexes may hold a key for many rows.
///
/// Rows are given as `RowLocator`s of the kind `IndexMetadata::locator`
/// says. Passing the wrong kind is a bug, checked in debug builds.
#[enum_dispatch]
pub trait IndexTrait {
    fn metadata(&self) -> &IndexMetadata;

    /// Adds `key` for the row at `locator`. Returns `false`, changing
    /// nothing, if that entry exists, or if this is a primary key index and
    /// `key` is taken.
    // TODO: take the transaction once there are transactions.
    fn insert_entry(&self, key: &Tuple, locator: &RowLocator) -> Result<bool>;

    /// Removes `key` for the row at `locator`, if present. Other rows with
    /// `key` keep their entries.
    fn delete_entry(&self, key: &Tuple, locator: &RowLocator) -> Result<()>;

    /// Appends the locators of the rows with `key` to `result`.
    fn scan_key(&self, key: &Tuple, result: &mut Vec<RowLocator>) -> Result<()>;
}

#[enum_dispatch(IndexTrait)]
pub enum Index {
    BtreeIndex,
}
