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

    /// An aggregate call in WHERE, ON, GROUP BY, VALUES or UPDATE.
    #[error("aggregate functions are not allowed in {clause}")]
    AggregateNotAllowed { clause: &'static str },

    #[error("aggregate function calls cannot be nested")]
    NestedAggregate,

    /// A window call anywhere but the select list and ORDER BY of a SELECT.
    #[error("window functions are not allowed in {clause}")]
    WindowNotAllowed { clause: &'static str },

    #[error("window function calls cannot be nested")]
    NestedWindow,

    /// `rank()` or `row_number()` without OVER.
    #[error("window function {0} requires an OVER clause")]
    WindowWithoutOver(String),

    /// OVER after a function that is neither a window function nor an
    /// aggregate.
    #[error("OVER specified, but {0} is not a window function nor an aggregate function")]
    NotAWindowFunction(String),

    /// In an aggregating SELECT, a column outside an aggregate that is not
    /// a GROUP BY key.
    #[error("column {0} must appear in the GROUP BY clause or be used in an aggregate function")]
    NotGrouped(String),

    /// A scalar or IN subquery with other than one output column.
    #[error("subquery must return only one column, found {found}")]
    SubqueryColumns { found: usize },

    /// Something the parser accepts but the binder or planner does not
    /// handle yet, such as a correlated subquery.
    #[error("{0} is not supported")]
    Unsupported(String),
}
