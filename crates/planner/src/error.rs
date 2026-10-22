use common::{DataType, ValueError};
use thiserror::Error;

pub type Result<T, E = PlannerError> = std::result::Result<T, E>;

/// Everything that can go wrong while binding or planning a statement.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum PlannerError {
    /// Operand types an operator does not accept, a value of the wrong type
    /// for a column, a NULL for a NOT NULL column, an INSERT row of the
    /// wrong width, or an integer literal that does not fit its column.
    #[error(transparent)]
    Value(#[from] ValueError),

    #[error("table {0} does not exist")]
    TableNotFound(String),

    #[error("table {0} already exists")]
    TableExists(String),

    /// A qualifier (`t.a`, `t.*`) names no table in FROM.
    #[error("missing FROM entry for table {0}")]
    UnknownTable(String),

    /// The same name (or alias) appears twice in FROM.
    #[error("table name {0} is specified more than once")]
    DuplicateTable(String),

    #[error("column {0} does not exist")]
    ColumnNotFound(String),

    /// An unqualified column that more than one table in FROM has.
    #[error("column reference {0} is ambiguous")]
    AmbiguousColumn(String),

    /// An INSERT column list or an UPDATE names a column twice.
    #[error("column {0} is specified more than once")]
    DuplicateColumn(String),

    #[error("function {0} does not exist")]
    UnknownFunction(String),

    #[error("{function} takes {expected} argument(s), found {found}")]
    WrongArgumentCount {
        function: String,
        expected: usize,
        found: usize,
    },

    /// A WHERE or ON condition that is not BOOL.
    #[error("argument of {clause} must be BOOL, not {found}")]
    NotBoolean {
        clause: &'static str,
        found: DataType,
    },

    #[error("SELECT * with no tables specified")]
    WildcardWithoutFrom,

    /// `ORDER BY n` where the select list has no n-th item.
    #[error("ORDER BY position {position} is not in the select list")]
    OrderByPositionOutOfRange { position: i64 },

    /// A statement that runs without a plan, such as CREATE TABLE, which
    /// goes to the catalog directly.
    #[error("{0} has no query plan")]
    NoPlan(&'static str),
}
