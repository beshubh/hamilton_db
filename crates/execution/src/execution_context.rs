use catalog::Catalog;
use storage::buffer::buffer_pool_manager::BufferPoolManager;

pub struct ExecutionContext {
    cataglog: Catalog,
    bpm: BufferPoolManager,
    is_delete: bool,
    // TBD
    // txn manager
    // lock manager maybe
    // nlj check executors associated with this execution ctx
}
