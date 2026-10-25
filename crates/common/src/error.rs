use thiserror::Error;

use crate::tuple::MAX_TUPLE_SIZE;
use crate::value::DataType;

pub type Result<T, E = ValueError> = std::result::Result<T, E>;

/// Everything that can go wrong while operating on values and encoding them
/// as tuples.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ValueError {
    /// The two values have types that have no ordering between them.
    #[error("cannot compare {left} with {right}")]
    Incomparable {
        left: &'static str,
        right: &'static str,
    },

    /// A binary operator does not accept operands of these types.
    #[error("cannot apply {op} to {left} and {right}")]
    InvalidOperands {
        op: &'static str,
        left: &'static str,
        right: &'static str,
    },

    /// A unary operator does not accept an operand of this type.
    #[error("cannot apply {op} to {operand}")]
    InvalidOperand {
        op: &'static str,
        operand: &'static str,
    },

    /// The result does not fit in the operands' integer type.
    #[error("integer out of range")]
    Overflow,

    /// The result of DOUBLE arithmetic is too large to be finite.
    #[error("value out of range: overflow")]
    DoubleOverflow,

    #[error("division by zero")]
    DivisionByZero,

    /// A row has a different number of values than its schema has columns.
    #[error("expected {expected} values, found {found}")]
    ColumnCountMismatch { expected: usize, found: usize },

    #[error("column {column} is {expected}, found {found}")]
    TypeMismatch {
        column: String,
        expected: DataType,
        found: &'static str,
    },

    #[error("column {0} is not nullable")]
    NullNotAllowed(String),

    /// A string is longer than its VARCHAR column allows.
    #[error("value of {len} bytes is too long for column {column} (max {max})")]
    ValueTooLong {
        column: String,
        len: usize,
        max: u32,
    },

    /// The column's type exists only in expressions, never in a tuple.
    #[error("column {column} is {data_type}, which cannot be stored")]
    NotStorable { column: String, data_type: DataType },

    #[error("encoded tuple is larger than {} bytes", MAX_TUPLE_SIZE)]
    TupleTooLarge,

    #[error("column {idx} is out of range for a schema of {count} columns")]
    ColumnOutOfRange { idx: usize, count: usize },

    /// The bytes are not a tuple encoded with this schema.
    #[error("corrupt tuple: {0}")]
    CorruptTuple(&'static str),
}
