use common::{TxnId, ValueError};
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

    #[error("identity column {0} must be a NOT NULL INT or BIGINT")]
    InvalidIdentity(String),

    /// CREATE INDEX on a sequence.
    #[error("{0} is not a table")]
    NotATable(String),

    /// Another transaction holds a lock that conflicts.
    #[error("could not obtain lock on relation \"{table}\"")]
    LockConflict { table: String, other: TxnId },

    /// A newer transaction truncated the table, removing rows this one
    /// would see, or used it, so this one can't truncate it.
    #[error("could not serialize access to relation \"{table}\" due to a newer transaction")]
    ChangedSince { table: String, other: TxnId },

    #[error("nextval: reached maximum value of sequence {name} ({max})")]
    SequenceExhausted { name: String, max: i64 },

    #[error("name {0:?} is empty or longer than {MAX_NAME_LEN} bytes")]
    InvalidName(String),

    /// Page 0 or the system tables do not hold a valid catalog.
    #[error("corrupt catalog: {0}")]
    Corrupt(String),
}
