use common::ValueError;
use storage::StorageError;
use thiserror::Error;

pub type Result<T, E = ExecutionError> = std::result::Result<T, E>;

/// Everything that can go wrong while running a query plan.
#[derive(Debug, Error)]
pub enum ExecutionError {
    #[error(transparent)]
    Storage(#[from] StorageError),

    /// An expression failed on a value: bad operand types, overflow,
    /// division by zero, or a tuple that does not decode.
    #[error(transparent)]
    Value(#[from] ValueError),

    /// A column expression reads an input the evaluation does not have,
    /// such as the right side of a join outside a join. A planner bug.
    #[error("column reads input {tuple_idx}, but only {inputs} input(s) were given")]
    TupleIndexOutOfRange { tuple_idx: usize, inputs: usize },
}
