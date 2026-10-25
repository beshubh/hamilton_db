use std::sync::Arc;

use catalog::{Catalog, IndexInfo, IndexOid, TableInfo, TableOid};

use crate::error::{ExecutionError, Result};

/// How many buffer frames a join gets by default; see
/// [`ExecutionContext::join_frames`].
pub const DEFAULT_JOIN_FRAMES: usize = 64;

/// How many buffer frames an aggregation gets by default; see
/// [`ExecutionContext::aggregation_frames`].
pub const DEFAULT_AGGREGATION_FRAMES: usize = 64;

/// What the executors of one statement share.
pub struct ExecutionContext {
    catalog: Arc<Catalog>,
    join_frames: usize,
    aggregation_frames: usize,
    // TBD
    // txn manager
    // lock manager maybe
}

impl ExecutionContext {
    pub fn new(catalog: Arc<Catalog>) -> Self {
        Self {
            catalog,
            join_frames: DEFAULT_JOIN_FRAMES,
            aggregation_frames: DEFAULT_AGGREGATION_FRAMES,
        }
    }

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

    pub fn catalog(&self) -> &Catalog {
        &self.catalog
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

    /// The `B` page-sized buffers a join may hold its rows in, on top of the
    /// buffer pool. A nested loop join keeps `B - 2` pages of left rows, one
    /// of right rows and one of output.
    pub fn join_frames(&self) -> usize {
        self.join_frames
    }

    /// The `B` page-sized buffers a hash aggregation may hold its groups in,
    /// on top of the buffer pool: `B - 1` for its hash table, or for the
    /// partitions it spills to, and one to read input.
    pub fn aggregation_frames(&self) -> usize {
        self.aggregation_frames
    }
}
