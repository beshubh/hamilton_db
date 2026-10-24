//! Turning bound expressions into execution expressions.
//!
//! The binder has already type-checked every expression with the same rules
//! the execution expressions check when built, so building one fails only
//! if the two disagree.

use execution::ExecutionError;
use execution::expressions::{
    AbstractExpr, ArithmeticExpr, ArithmeticOp, ArrayExpr, CastExpr, ColumnValueExpr,
    ComparisonExpr, ComparisonOp, ConstantExpr, IsNullExpr, LogicExpr, LogicOp, StringExpr,
    StringOp, UnaryExpr, UnaryOp,
};
use parser::BinaryOp;

use crate::bound::{BoundExpr, BoundExprKind, Function};
use crate::error::{PlannerError, Result};

/// The inputs of the plan an expression is evaluated by, which decide how a
/// bound column index is read.
#[derive(Clone, Copy, Debug)]
pub(super) enum Inputs {
    /// A plan with one input (or none): the index is a column of it.
    One,
    /// A join: the index is into the left row followed by the right row.
    Join { left_len: usize },
    /// The right input of a join on its own, such as for a hash join's right
    /// keys: the index is into the left row followed by the right row, but
    /// only columns of the right row are read.
    RightOfJoin { left_len: usize },
}

pub(super) fn plan_expr(expr: &BoundExpr, inputs: Inputs) -> Result<AbstractExpr> {
    let planned = match &expr.kind {
        BoundExprKind::Column { idx, .. } => {
            let (tuple_idx, col_idx) = match inputs {
                Inputs::Join { left_len } if *idx >= left_len => (1, idx - left_len),
                Inputs::RightOfJoin { left_len } => {
                    let idx = idx.checked_sub(left_len);
                    (
                        0,
                        idx.expect("an expression on the right input reads a left column"),
                    )
                }
                _ => (0, *idx),
            };
            ColumnValueExpr::new(tuple_idx, col_idx, expr.return_type.clone()).into()
        }
        BoundExprKind::Constant(value) => {
            ConstantExpr::new(value.clone(), expr.return_type.clone()).into()
        }
        BoundExprKind::Binary { op, left, right } => {
            plan_binary(*op, plan_expr(left, inputs)?, plan_expr(right, inputs)?)?
        }
        BoundExprKind::Unary { op, expr: arg } => {
            let op = match op {
                parser::UnaryOp::Not => UnaryOp::Not,
                parser::UnaryOp::Minus => UnaryOp::Neg,
            };
            built(UnaryExpr::new(op, plan_expr(arg, inputs)?))?.into()
        }
        BoundExprKind::IsNull { expr: arg, negated } => {
            IsNullExpr::new(plan_expr(arg, inputs)?, *negated).into()
        }
        BoundExprKind::Function { function, args } => {
            let args = args
                .iter()
                .map(|arg| plan_expr(arg, inputs))
                .collect::<Result<_>>()?;
            plan_function(*function, args)?
        }
        BoundExprKind::Array(elements) => {
            let elements = elements
                .iter()
                .map(|element| plan_expr(element, inputs))
                .collect::<Result<_>>()?;
            ArrayExpr::new(elements).into()
        }
        BoundExprKind::Cast(arg) => built(CastExpr::new(
            plan_expr(arg, inputs)?,
            expr.return_type.data_type,
        ))?
        .into(),
        BoundExprKind::Subquery(_)
        | BoundExprKind::Exists { .. }
        | BoundExprKind::InSubquery { .. } => {
            return Err(PlannerError::Unsupported(
                "planning a subquery in an expression".to_owned(),
            ));
        }
        BoundExprKind::Aggregate(_) | BoundExprKind::Window(_) => {
            unreachable!("the binder replaces aggregate and window calls with columns")
        }
    };
    Ok(planned)
}

pub(super) fn plan_binary(
    op: BinaryOp,
    left: AbstractExpr,
    right: AbstractExpr,
) -> Result<AbstractExpr> {
    built(match op {
        BinaryOp::Add => arithmetic(ArithmeticOp::Add, left, right),
        BinaryOp::Sub => arithmetic(ArithmeticOp::Sub, left, right),
        BinaryOp::Mul => arithmetic(ArithmeticOp::Mul, left, right),
        BinaryOp::Div => arithmetic(ArithmeticOp::Div, left, right),
        BinaryOp::Rem => arithmetic(ArithmeticOp::Rem, left, right),
        BinaryOp::Eq => comparison(ComparisonOp::Eq, left, right),
        BinaryOp::NotEq => comparison(ComparisonOp::NotEq, left, right),
        BinaryOp::Lt => comparison(ComparisonOp::Lt, left, right),
        BinaryOp::LtEq => comparison(ComparisonOp::LtEq, left, right),
        BinaryOp::Gt => comparison(ComparisonOp::Gt, left, right),
        BinaryOp::GtEq => comparison(ComparisonOp::GtEq, left, right),
        BinaryOp::And => logic(LogicOp::And, left, right),
        BinaryOp::Or => logic(LogicOp::Or, left, right),
    })
}

fn arithmetic(
    op: ArithmeticOp,
    left: AbstractExpr,
    right: AbstractExpr,
) -> execution::Result<AbstractExpr> {
    ArithmeticExpr::new(op, left, right).map(Into::into)
}

fn comparison(
    op: ComparisonOp,
    left: AbstractExpr,
    right: AbstractExpr,
) -> execution::Result<AbstractExpr> {
    ComparisonExpr::new(op, left, right).map(Into::into)
}

fn logic(op: LogicOp, left: AbstractExpr, right: AbstractExpr) -> execution::Result<AbstractExpr> {
    LogicExpr::new(op, left, right).map(Into::into)
}

fn plan_function(function: Function, args: Vec<AbstractExpr>) -> Result<AbstractExpr> {
    match function {
        Function::Upper | Function::Lower => {
            let op = if function == Function::Upper {
                StringOp::Upper
            } else {
                StringOp::Lower
            };
            let found = args.len();
            let [arg] = <[AbstractExpr; 1]>::try_from(args).map_err(|_| {
                PlannerError::WrongArgumentCount {
                    function: function.name().to_owned(),
                    expected: 1,
                    found,
                }
            })?;
            Ok(built(StringExpr::new(op, arg))?.into())
        }
    }
}

/// Building an expression fails only on its operand types (a
/// [`ValueError`](common::ValueError)), which the binder has already checked.
fn built<T>(result: execution::Result<T>) -> Result<T> {
    result.map_err(|err| match err {
        ExecutionError::Value(err) => err.into(),
        err => unreachable!("building an expression failed: {err}"),
    })
}
