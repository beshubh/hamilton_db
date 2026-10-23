use common::ValueError;
use storage::StorageError;
use thiserror::Error;

use crate::catalog::MAX_NAME_LEN;

pub type Result<T, E = CatalogError> = std::result::Result<T, E>;

/// Everything that can go wrong while reading or changing the catalog.
#[derive(Debug, Error)]
pub enum CatalogError {
    #[error(transparent)]
    Storage(#[from] StorageError),

    #[error(transparent)]
    Value(#[from] ValueError),

    #[error("table {0} already exists")]
    TableExists(String),

    #[error("table {0} does not exist")]
    TableNotFound(String),

    #[error("index {0} already exists")]
    IndexExists(String),

    #[error("column {column} does not exist in table {table}")]
    ColumnNotFound { table: String, column: String },

    /// A table's columns, or an index's key columns, name a column twice.
    #[error("column {0} is specified more than once")]
    DuplicateColumn(String),

    #[error("an index needs at least one key column")]
    EmptyIndexKey,

    #[error("primary key column {0} must be NOT NULL")]
    NullablePrimaryKey(String),

    #[error("name {0:?} is empty or longer than {MAX_NAME_LEN} bytes")]
    InvalidName(String),

    /// Page 0 or the system tables do not hold a valid catalog.
    #[error("corrupt catalog: {0}")]
    Corrupt(String),
}
