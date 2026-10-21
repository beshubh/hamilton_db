//! Expressions evaluated by query plans against one tuple at a time.
//!
//! Every expression is a variant of [`AbstractExpr`], and `enum_dispatch`
//! forwards [`Expr`] calls to the variant, so a tree of expressions is plain
//! owned enums with no `Box<dyn Expr>`. Each expression lives in its own
//! module under `expressions/`.

mod arithmetic;
mod array;
mod column_value;
mod constant;
mod logic;
mod string;
#[cfg(test)]
mod test_util;

pub use arithmetic::{ArithmeticExpr, ArithmeticOp};
pub use array::ArrayExpr;
pub use column_value::ColumnValueExpr;
pub use constant::ConstantExpr;
pub use logic::{LogicExpr, LogicOp};
pub use string::{StringExpr, StringOp};

use common::{Column, DataType, Schema, Tuple, Value};
use enum_dispatch::enum_dispatch;

use crate::Result;

/// Name given to computed columns, which have no name of their own.
pub(crate) const UNNAMED_COLUMN: &str = "?column?";

#[enum_dispatch]
pub trait Expr {
    /// Evaluates the expression on `tuple`, whose layout is `schema`.
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value>;

    /// Evaluates the expression on a pair of tuples from the two inputs of
    /// a join, e.g. a join predicate. Columns choose a side by their
    /// `tuple_idx`.
    fn evaluate_join(
        &self,
        left: &Tuple,
        left_schema: &Schema,
        right: &Tuple,
        right_schema: &Schema,
    ) -> Result<Value>;

    fn children(&self) -> &[AbstractExpr];

    /// The column this expression produces: its type and whether it can be
    /// NULL.
    fn return_type(&self) -> &Column;
}

#[enum_dispatch(Expr)]
#[derive(Clone, Debug)]
pub enum AbstractExpr {
    ColumnValueExpr,
    ConstantExpr,
    ArithmeticExpr,
    LogicExpr,
    StringExpr,
    ArrayExpr,
}

/// The SQL name of a type, for error messages; matches [`Value::type_name`].
pub(crate) fn type_name(data_type: DataType) -> &'static str {
    match data_type {
        DataType::Int => "INT",
        DataType::BigInt => "BIGINT",
        DataType::Bool => "BOOL",
        DataType::Varchar(_) => "VARCHAR",
        DataType::Array => "ARRAY",
    }
}
