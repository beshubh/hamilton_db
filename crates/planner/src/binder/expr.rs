//! Binding and type-checking expressions.
//!
//! The rules match the execution crate's expressions and [`Value`]'s
//! operators, so a bound expression does not fail on types at run time:
//!
//! - `+ - * / %` and unary `-` take integers. INT with INT is INT;
//!   any BIGINT makes the result BIGINT.
//! - Comparisons take two integers, two BOOLs or two VARCHARs.
//! - AND, OR and NOT take BOOLs.
//! - The result can be NULL if any operand can, except for IS [NOT] NULL.
//!
//! A NULL literal has no type of its own. It takes the type its context
//! asks for (the other operand, BOOL under AND, the column it is stored
//! in) and is INT where nothing does, as in `SELECT NULL`.

use common::{Column, DataType, Value, ValueError};
use parser::{BinaryOp, Expr, UnaryOp};

use super::scope::Scope;
use crate::bound::{BoundExpr, BoundExprKind, Function};
use crate::error::{PlannerError, Result};

/// Name of a result column that is not simply a table column.
pub(crate) const UNNAMED_COLUMN: &str = "?column?";

pub(super) fn bind_expr(expr: &Expr, scope: &Scope) -> Result<BoundExpr> {
    match expr {
        Expr::Column { table, name } => {
            let resolved = scope.resolve(table.as_deref(), name)?;
            Ok(BoundExpr {
                kind: BoundExprKind::Column {
                    table: resolved.table,
                    idx: resolved.idx,
                },
                return_type: resolved.column,
            })
        }
        Expr::Literal(value) => Ok(constant(value.clone())),
        Expr::Binary { op, left, right } => {
            bind_binary(*op, bind_expr(left, scope)?, bind_expr(right, scope)?)
        }
        Expr::Unary { op, expr } => bind_unary(*op, bind_expr(expr, scope)?),
        Expr::IsNull { expr, negated } => Ok(BoundExpr {
            kind: BoundExprKind::IsNull {
                expr: Box::new(bind_expr(expr, scope)?),
                negated: *negated,
            },
            return_type: unnamed(DataType::Bool, false),
        }),
        Expr::Function { name, args } => {
            let args = args
                .iter()
                .map(|arg| bind_expr(arg, scope))
                .collect::<Result<_>>()?;
            bind_function(name, args)
        }
        Expr::Array(elements) => Ok(BoundExpr {
            kind: BoundExprKind::Array(
                elements
                    .iter()
                    .map(|e| bind_expr(e, scope))
                    .collect::<Result<_>>()?,
            ),
            return_type: unnamed(DataType::Array, false),
        }),
    }
}

/// Binds a WHERE or ON condition, which must be BOOL.
pub(super) fn bind_condition(
    expr: &Expr,
    scope: &Scope,
    clause: &'static str,
) -> Result<BoundExpr> {
    let mut bound = bind_expr(expr, scope)?;
    type_null(&mut bound, DataType::Bool);
    match bound.return_type.data_type {
        DataType::Bool => Ok(bound),
        found => Err(PlannerError::NotBoolean { clause, found }),
    }
}

/// Converts `expr` for storing in `column`: INT and BIGINT convert into
/// each other, a VARCHAR fits any VARCHAR column (its length is checked
/// when the row is stored), and a NULL literal fits any nullable column.
pub(super) fn assign(mut expr: BoundExpr, column: &Column) -> Result<BoundExpr> {
    if is_null_literal(&expr) {
        if !column.nullable {
            return Err(ValueError::NullNotAllowed(column.name.clone()).into());
        }
        type_null(&mut expr, column.data_type);
        return Ok(expr);
    }

    let from = expr.return_type.data_type;
    match (from, column.data_type) {
        (DataType::Int, DataType::Int)
        | (DataType::BigInt, DataType::BigInt)
        | (DataType::Bool, DataType::Bool)
        | (DataType::Varchar(_), DataType::Varchar(_)) => Ok(expr),
        (DataType::Int | DataType::BigInt, to @ (DataType::Int | DataType::BigInt)) => {
            cast_integer(expr, to)
        }
        _ => Err(ValueError::TypeMismatch {
            column: column.name.clone(),
            expected: column.data_type,
            found: from.type_name(),
        }
        .into()),
    }
}

/// A NULL of `column`'s type, for a column an INSERT leaves out.
pub(super) fn null_for(column: &Column) -> Result<BoundExpr> {
    assign(constant(Value::Null), column)
}

/// Casts an integer expression to the other integer type. A literal is
/// converted now, so one out of range is an error here rather than at run
/// time.
fn cast_integer(expr: BoundExpr, to: DataType) -> Result<BoundExpr> {
    if let BoundExprKind::Constant(value) = &expr.kind {
        let value = match (value, to) {
            (Value::Int(v), DataType::BigInt) => Value::BigInt(i64::from(*v)),
            (Value::BigInt(v), DataType::Int) => {
                Value::Int(i32::try_from(*v).map_err(|_| ValueError::Overflow)?)
            }
            _ => unreachable!("cast_integer is only called between INT and BIGINT"),
        };
        return Ok(constant(value));
    }
    let nullable = expr.return_type.nullable;
    Ok(BoundExpr {
        kind: BoundExprKind::Cast(Box::new(expr)),
        return_type: unnamed(to, nullable),
    })
}

fn bind_binary(op: BinaryOp, mut left: BoundExpr, mut right: BoundExpr) -> Result<BoundExpr> {
    let operand_type = match op {
        BinaryOp::And | BinaryOp::Or => Some(DataType::Bool),
        // Arithmetic and comparisons: whatever the other side is.
        _ => None,
    };
    match operand_type {
        Some(ty) => {
            type_null(&mut left, ty);
            type_null(&mut right, ty);
        }
        None => {
            let (l, r) = (left.return_type.data_type, right.return_type.data_type);
            type_null(&mut left, r);
            type_null(&mut right, l);
        }
    }

    let (l, r) = (left.return_type.data_type, right.return_type.data_type);
    let data_type = match op {
        BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Rem => {
            match (l, r) {
                (DataType::Int, DataType::Int) => Some(DataType::Int),
                (DataType::Int | DataType::BigInt, DataType::Int | DataType::BigInt) => {
                    Some(DataType::BigInt)
                }
                _ => None,
            }
        }
        BinaryOp::Eq
        | BinaryOp::NotEq
        | BinaryOp::Lt
        | BinaryOp::LtEq
        | BinaryOp::Gt
        | BinaryOp::GtEq => match (l, r) {
            (DataType::Int | DataType::BigInt, DataType::Int | DataType::BigInt)
            | (DataType::Bool, DataType::Bool)
            | (DataType::Varchar(_), DataType::Varchar(_)) => Some(DataType::Bool),
            _ => None,
        },
        BinaryOp::And | BinaryOp::Or => {
            (l == DataType::Bool && r == DataType::Bool).then_some(DataType::Bool)
        }
    };
    let Some(data_type) = data_type else {
        return Err(ValueError::InvalidOperands {
            op: op.symbol(),
            left: l.type_name(),
            right: r.type_name(),
        }
        .into());
    };

    let nullable = left.return_type.nullable || right.return_type.nullable;
    Ok(BoundExpr {
        kind: BoundExprKind::Binary {
            op,
            left: Box::new(left),
            right: Box::new(right),
        },
        return_type: unnamed(data_type, nullable),
    })
}

fn bind_unary(op: UnaryOp, mut expr: BoundExpr) -> Result<BoundExpr> {
    let accepted = match op {
        UnaryOp::Not => {
            type_null(&mut expr, DataType::Bool);
            expr.return_type.data_type == DataType::Bool
        }
        UnaryOp::Minus => matches!(expr.return_type.data_type, DataType::Int | DataType::BigInt),
    };
    if !accepted {
        return Err(ValueError::InvalidOperand {
            op: op.symbol(),
            operand: expr.return_type.data_type.type_name(),
        }
        .into());
    }
    let return_type = unnamed(expr.return_type.data_type, expr.return_type.nullable);
    Ok(BoundExpr {
        kind: BoundExprKind::Unary {
            op,
            expr: Box::new(expr),
        },
        return_type,
    })
}

fn bind_function(name: &str, mut args: Vec<BoundExpr>) -> Result<BoundExpr> {
    let function =
        Function::lookup(name).ok_or_else(|| PlannerError::UnknownFunction(name.to_owned()))?;
    match function {
        Function::Upper | Function::Lower => {
            if args.len() != 1 {
                return Err(PlannerError::WrongArgumentCount {
                    function: function.name().to_owned(),
                    expected: 1,
                    found: args.len(),
                });
            }
            let arg = &mut args[0];
            // Its length doesn't matter: the value is NULL.
            type_null(arg, DataType::Varchar(0));
            let ty = arg.return_type.data_type;
            if !matches!(ty, DataType::Varchar(_)) {
                return Err(ValueError::InvalidOperand {
                    op: if function == Function::Upper {
                        "UPPER"
                    } else {
                        "LOWER"
                    },
                    operand: ty.type_name(),
                }
                .into());
            }
            let nullable = arg.return_type.nullable;
            Ok(BoundExpr {
                kind: BoundExprKind::Function { function, args },
                return_type: unnamed(ty, nullable),
            })
        }
    }
}

fn constant(value: Value) -> BoundExpr {
    let data_type = match &value {
        // Until its context gives it one; see `type_null`.
        Value::Null => DataType::Int,
        Value::Int(_) => DataType::Int,
        Value::BigInt(_) => DataType::BigInt,
        Value::Bool(_) => DataType::Bool,
        Value::Varchar(s) => DataType::Varchar(u32::try_from(s.len()).unwrap_or(u32::MAX)),
        Value::Array(_) => DataType::Array,
    };
    let nullable = value.is_null();
    BoundExpr {
        kind: BoundExprKind::Constant(value),
        return_type: unnamed(data_type, nullable),
    }
}

fn is_null_literal(expr: &BoundExpr) -> bool {
    matches!(expr.kind, BoundExprKind::Constant(Value::Null))
}

/// Gives a NULL literal the type its context expects; other expressions
/// are left alone.
fn type_null(expr: &mut BoundExpr, data_type: DataType) {
    if is_null_literal(expr) {
        expr.return_type.data_type = data_type;
    }
}

fn unnamed(data_type: DataType, nullable: bool) -> Column {
    Column::new(UNNAMED_COLUMN, data_type, nullable)
}
