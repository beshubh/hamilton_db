use thiserror::Error;

pub type Result<T, E = ValueError> = std::result::Result<T, E>;

/// Everything that can go wrong while operating on values.
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

    #[error("division by zero")]
    DivisionByZero,
}
