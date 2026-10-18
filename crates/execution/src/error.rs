use storage::StorageError;
use thiserror::Error;

pub type Result<T, E = ExecutionError> = std::result::Result<T, E>;

/// Everything that can go wrong while running a query plan.
#[derive(Debug, Error)]
pub enum ExecutionError {
    #[error(transparent)]
    Storage(#[from] StorageError),
}
