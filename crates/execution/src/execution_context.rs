use std::sync::Arc;

use catalog::{Catalog, IndexInfo, IndexOid, TableInfo, TableOid};
use common::Rid;
use storage::concurrency::transaction::Transaction;
use storage::index::{IndexTrait, RowLocator};

use crate::error::{ExecutionError, Result};

/// How many buffer frames a join gets by default; see
/// [`ExecutionContext::join_frames`].
pub const DEFAULT_JOIN_FRAMES: usize = 64;

/// How many buffer frames an aggregation gets by default; see
/// [`ExecutionContext::aggregation_frames`].
pub const DEFAULT_AGGREGATION_FRAMES: usize = 64;

/// How much memory operators get, set once for a session and passed to the
/// context of each statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionSettings {
    join_frames: usize,
    aggregation_frames: usize,
}

impl Default for ExecutionSettings {
    fn default() -> Self {
        Self {
            join_frames: DEFAULT_JOIN_FRAMES,
            aggregation_frames: DEFAULT_AGGREGATION_FRAMES,
        }
    }
}

impl ExecutionSettings {
    /// Gives each join `frames` buffers instead of [`DEFAULT_JOIN_FRAMES`].
    /// Panics if `frames` is below 3: one for each side and one for output.
    pub fn with_join_frames(mut self, frames: usize) -> Self {
        assert!(frames >= 3, "a join needs at least 3 frames, got {frames}");
        self.join_frames = frames;
        self
    }

    /// Gives each aggregation `frames` buffers instead of
    /// [`DEFAULT_AGGREGATION_FRAMES`]. Panics if `frames` is below 3: one for
    /// input and at least two partitions to split groups into.
    pub fn with_aggregation_frames(mut self, frames: usize) -> Self {
        assert!(
            frames >= 3,
            "an aggregation needs at least 3 frames, got {frames}"
        );
        self.aggregation_frames = frames;
        self
    }

    /// See [`ExecutionContext::join_frames`].
    pub fn join_frames(&self) -> usize {
        self.join_frames
    }

    /// See [`ExecutionContext::aggregation_frames`].
    pub fn aggregation_frames(&self) -> usize {
        self.aggregation_frames
    }
}

/// What the executors of one statement share: the catalog, the transaction
/// the statement runs in, and the session's settings.
///
/// Executors read and write tables and indexes through the transaction, so
/// that its commit or abort covers everything the statement did.
pub struct ExecutionContext {
    catalog: Arc<Catalog>,
    txn: Arc<Transaction>,
    settings: ExecutionSettings,
}

impl ExecutionContext {
    pub fn new(catalog: Arc<Catalog>, txn: Arc<Transaction>, settings: ExecutionSettings) -> Self {
        Self {
            catalog,
            txn,
            settings,
        }
    }

    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    pub fn txn(&self) -> &Arc<Transaction> {
        &self.txn
    }

    /// The table a plan names by `oid`.
    pub fn table(&self, oid: TableOid) -> Result<Arc<TableInfo>> {
        self.catalog
            .get_table_by_oid(oid)
            .ok_or(ExecutionError::TableNotFound(oid))
    }

    /// The index a plan names by `oid`.
    pub fn index(&self, oid: IndexOid) -> Result<Arc<IndexInfo>> {
        self.catalog
            .get_index_by_oid(oid)
            .ok_or(ExecutionError::IndexNotFound(oid))
    }

    /// The rid of the row of `table` that `locator` points to. A primary key
    /// is looked up in the table's primary key index; `None` if it isn't
    /// there, meaning the row is gone.
    ///
    /// # Panics
    /// If `locator` is a primary key and `table` has no primary key index.
    pub fn locate(&self, table: &TableInfo, locator: &RowLocator) -> Result<Option<Rid>> {
        locate(&self.catalog, table, locator)
    }

    /// The `B` page-sized buffers a join may hold its rows in, on top of the
    /// buffer pool. A nested loop join keeps `B - 2` pages of left rows, one
    /// of right rows and one of output.
    pub fn join_frames(&self) -> usize {
        self.settings.join_frames
    }

    /// The `B` page-sized buffers a hash aggregation may hold its groups in,
    /// on top of the buffer pool: `B - 1` for its hash table, or for the
    /// partitions it spills to, and one to read input.
    pub fn aggregation_frames(&self) -> usize {
        self.settings.aggregation_frames
    }
}

/// See [`ExecutionContext::locate`].
pub fn locate(catalog: &Catalog, table: &TableInfo, locator: &RowLocator) -> Result<Option<Rid>> {
    let pk = match locator {
        RowLocator::Rid(rid) => return Ok(Some(*rid)),
        RowLocator::PrimaryKey(pk) => pk,
    };
    let index = catalog
        .primary_key_index(table.oid)
        .unwrap_or_else(|| panic!("table {} has no primary key index", table.name));
    let mut found = Vec::with_capacity(1);
    index.index.scan_key(pk, &mut found)?;
    debug_assert!(found.len() <= 1, "a primary key index is unique");
    Ok(found
        .pop()
        .map(|locator| locator.as_rid().expect("the primary key index stores rids")))
}
