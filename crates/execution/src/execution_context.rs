use std::sync::Arc;

use catalog::Catalog;

/// What the executors of one statement share.
pub struct ExecutionContext {
    catalog: Arc<Catalog>,
    // TBD
    // txn manager
    // lock manager maybe
}

impl ExecutionContext {
    pub fn new(catalog: Arc<Catalog>) -> Self {
        Self { catalog }
    }

    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }
}
