use std::sync::Arc;

use catalog::Catalog;

/// How many buffer frames a join gets by default; see
/// [`ExecutionContext::join_frames`].
pub const DEFAULT_JOIN_FRAMES: usize = 64;

/// What the executors of one statement share.
pub struct ExecutionContext {
    catalog: Arc<Catalog>,
    join_frames: usize,
    // TBD
    // txn manager
    // lock manager maybe
}

impl ExecutionContext {
    pub fn new(catalog: Arc<Catalog>) -> Self {
        Self {
            catalog,
            join_frames: DEFAULT_JOIN_FRAMES,
        }
    }

    /// Gives each join `frames` buffers instead of [`DEFAULT_JOIN_FRAMES`].
    /// Panics if `frames` is below 3: one for each side and one for output.
    pub fn with_join_frames(mut self, frames: usize) -> Self {
        assert!(frames >= 3, "a join needs at least 3 frames, got {frames}");
        self.join_frames = frames;
        self
    }

    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    /// The `B` page-sized buffers a join may hold its rows in, on top of the
    /// buffer pool. A nested loop join keeps `B - 2` pages of left rows, one
    /// of right rows and one of output.
    pub fn join_frames(&self) -> usize {
        self.join_frames
    }
}
