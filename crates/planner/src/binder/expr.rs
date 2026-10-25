//! Binding and type-checking expressions.
//!
//! The rules match the execution crate's expressions and [`Value`]'s
//! operators, so a bound expression does not fail on types at run time:
//!
//! - `+ - * /` and unary `-` take numbers (INT, BIGINT, DOUBLE), and `%`
//!   integers. INT with INT is INT; any DOUBLE makes the result DOUBLE, and
//!   otherwise any BIGINT makes it BIGINT.
//! - Comparisons take two numbers, two BOOLs or two VARCHARs.
//! - AND, OR and NOT take BOOLs.
//! - The result can be NULL if any operand can, except for IS [NOT] NULL.
//! - `count` takes anything, `sum` and `avg` numbers, and `min` and `max`
//!   anything comparable. `sum` is BIGINT for integers and DOUBLE for
//!   doubles; `avg` is always DOUBLE. `x IN (SELECT y ...)` needs `x = y` to
//!   be valid.
//!
//! A NULL literal has no type of its own. It takes the type its context
//! asks for (the other operand, BOOL under AND, the column it is stored
//! in) and is INT where nothing does, as in `SELECT NULL`.

use common::{Column, DataType, Value, ValueError};
use parser::{BinaryOp, Expr, UnaryOp, WindowSpec};

use super::Binder;
use super::scope::Scope;
use crate::bound::{
    AggregateFunction, BoundAggregate, BoundExpr, BoundExprKind, BoundOrderBy, BoundSelect,
    BoundWindow, BoundWindowFunction, Function,
};
use crate::error::{PlannerError, Result};

/// Name of a result column that is not simply a table column.
pub(crate) const UNNAMED_COLUMN: &str = "?column?";

impl Binder<'_> {
    pub(super) fn bind_expr(&self, expr: &Expr, scope: &Scope) -> Result<BoundExpr> {
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
            Expr::Binary { op, left, right } => bind_binary(
                *op,
                self.bind_expr(left, scope)?,
                self.bind_expr(right, scope)?,
            ),
            Expr::Unary { op, expr } => bind_unary(*op, self.bind_expr(expr, scope)?),
            Expr::IsNull { expr, negated } => Ok(BoundExpr {
                kind: BoundExprKind::IsNull {
                    expr: Box::new(self.bind_expr(expr, scope)?),
                    negated: *negated,
                },
                return_type: unnamed(DataType::Bool, false),
            }),
            Expr::Function {
                name,
                args,
                star,
                over,
            } => {
                let args = args
                    .iter()
                    .map(|arg| self.bind_expr(arg, scope))
                    .collect::<Result<_>>()?;
                match over {
                    Some(over) => self.bind_window(name, args, *star, over, scope),
                    None => bind_function(name, args, *star),
                }
            }
            Expr::Array(elements) => Ok(BoundExpr {
                kind: BoundExprKind::Array(
                    elements
                        .iter()
                        .map(|e| self.bind_expr(e, scope))
                        .collect::<Result<_>>()?,
                ),
                return_type: unnamed(DataType::Array, false),
            }),
            Expr::Subquery(query) => {
                let subquery = self.bind_select(query, Some(scope))?;
                let column = single_column(&subquery)?;
                Ok(BoundExpr {
                    kind: BoundExprKind::Subquery(Box::new(subquery)),
                    // NULL when the subquery produces no rows.
                    return_type: unnamed(column.data_type, true),
                })
            }
            Expr::Exists { subquery, negated } => Ok(BoundExpr {
                kind: BoundExprKind::Exists {
                    subquery: Box::new(self.bind_select(subquery, Some(scope))?),
                    negated: *negated,
                },
                return_type: unnamed(DataType::Bool, false),
            }),
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                let mut left = self.bind_expr(expr, scope)?;
                let subquery = self.bind_select(subquery, Some(scope))?;
                let right = single_column(&subquery)?;
                type_null(&mut left, right.data_type);
                let l = left.return_type.data_type;
                if !comparable(l, right.data_type) {
                    return Err(ValueError::InvalidOperands {
                        op: "IN",
                        left: l.type_name(),
                        right: right.data_type.type_name(),
                    }
                    .into());
                }
                Ok(BoundExpr {
                    kind: BoundExprKind::InSubquery {
                        expr: Box::new(left),
                        subquery: Box::new(subquery),
                        negated: *negated,
                    },
                    return_type: unnamed(DataType::Bool, true),
                })
            }
        }
    }

    /// Binds a WHERE, ON or HAVING condition, which must be BOOL.
    pub(super) fn bind_condition(
        &self,
        expr: &Expr,
        scope: &Scope,
        clause: &'static str,
    ) -> Result<BoundExpr> {
        let mut bound = self.bind_expr(expr, scope)?;
        type_null(&mut bound, DataType::Bool);
        match bound.return_type.data_type {
            DataType::Bool => Ok(bound),
            found => Err(PlannerError::NotBoolean { clause, found }),
        }
    }

    /// Binds a WHERE or ON condition, where aggregate and window calls are
    /// not allowed.
    pub(super) fn bind_filter(
        &self,
        expr: &Expr,
        scope: &Scope,
        clause: &'static str,
    ) -> Result<BoundExpr> {
        let bound = self.bind_condition(expr, scope, clause)?;
        forbid_calls(&bound, clause)?;
        Ok(bound)
    }

    /// `name(args) OVER (...)`, for an aggregate or a window-only function.
    fn bind_window(
        &self,
        name: &str,
        args: Vec<BoundExpr>,
        star: bool,
        over: &WindowSpec,
        scope: &Scope,
    ) -> Result<BoundExpr> {
        if args.iter().any(|arg| arg.contains(&is_window)) {
            return Err(PlannerError::NestedWindow);
        }
        let function = if let Some(function) = AggregateFunction::lookup(name) {
            BoundWindowFunction::Aggregate(bind_aggregate(function, args, star)?)
        } else if let Some(function) = BoundWindowFunction::lookup(name) {
            if star || !args.is_empty() {
                return Err(PlannerError::WrongArgumentCount {
                    function: name.to_owned(),
                    expected: 0,
                    found: if star { 1 } else { args.len() },
                });
            }
            function
        } else if Function::lookup(name).is_some() {
            return Err(PlannerError::NotAWindowFunction(name.to_owned()));
        } else {
            return Err(PlannerError::UnknownFunction(name.to_owned()));
        };

        let partition_by = over
            .partition_by
            .iter()
            .map(|expr| self.bind_expr(expr, scope))
            .collect::<Result<_>>()?;
        let order_by = over
            .order_by
            .iter()
            .map(|item| {
                Ok(BoundOrderBy {
                    expr: self.bind_expr(&item.expr, scope)?,
                    desc: item.desc,
                })
            })
            .collect::<Result<_>>()?;
        let return_type = match &function {
            BoundWindowFunction::Aggregate(aggregate) => aggregate.return_type.clone(),
            BoundWindowFunction::Rank | BoundWindowFunction::RowNumber => {
                unnamed(DataType::BigInt, false)
            }
        };
        let window = BoundWindow {
            function,
            partition_by,
            order_by,
            return_type: return_type.clone(),
        };
        if window
            .children()
            .iter()
            .any(|child| child.contains(&is_window))
        {
            return Err(PlannerError::NestedWindow);
        }
        Ok(BoundExpr {
            kind: BoundExprKind::Window(Box::new(window)),
            return_type,
        })
    }
}

/// Rejects aggregate and window calls in a clause that allows neither.
pub(super) fn forbid_calls(expr: &BoundExpr, clause: &'static str) -> Result<()> {
    forbid_windows(expr, clause)?;
    if expr.contains(&is_aggregate) {
        return Err(PlannerError::AggregateNotAllowed { clause });
    }
    Ok(())
}

/// Rejects window calls in a clause other than the select list and ORDER BY.
pub(super) fn forbid_windows(expr: &BoundExpr, clause: &'static str) -> Result<()> {
    if expr.contains(&is_window) {
        return Err(PlannerError::WindowNotAllowed { clause });
    }
    Ok(())
}

pub(super) fn is_aggregate(kind: &BoundExprKind) -> bool {
    matches!(kind, BoundExprKind::Aggregate(_))
}

fn is_window(kind: &BoundExprKind) -> bool {
    matches!(kind, BoundExprKind::Window(_))
}

/// The one output column of a scalar or IN subquery.
fn single_column(subquery: &BoundSelect) -> Result<Column> {
    match subquery.projection.as_slice() {
        [item] => Ok(item.expr.return_type.clone()),
        items => Err(PlannerError::SubqueryColumns { found: items.len() }),
    }
}

/// Converts `expr` for storing in `column`: numbers convert into each other
/// (a DOUBLE is rounded into an integer column), a VARCHAR fits any VARCHAR
/// column (its length is checked when the row is stored), and a NULL literal
/// fits any nullable column.
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
        | (DataType::Double, DataType::Double)
        | (DataType::Bool, DataType::Bool)
        | (DataType::Varchar(_), DataType::Varchar(_)) => Ok(expr),
        (from, to) if is_number(from) && is_number(to) => cast_number(expr, to),
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

/// Casts a numeric expression to another numeric type. A literal is
/// converted now, so one out of range is an error here rather than at run
/// time.
fn cast_number(expr: BoundExpr, to: DataType) -> Result<BoundExpr> {
    if let BoundExprKind::Constant(value) = &expr.kind {
        return Ok(constant(value.cast_number(to)?));
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
                _ if is_number(l) && is_number(r) && op != BinaryOp::Rem => Some(DataType::Double),
                _ => None,
            }
        }
        BinaryOp::Eq
        | BinaryOp::NotEq
        | BinaryOp::Lt
        | BinaryOp::LtEq
        | BinaryOp::Gt
        | BinaryOp::GtEq => comparable(l, r).then_some(DataType::Bool),
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
        UnaryOp::Minus => is_number(expr.return_type.data_type),
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

/// Whether `=`, `<` and the like accept these operand types.
fn comparable(l: DataType, r: DataType) -> bool {
    (is_number(l) && is_number(r))
        || matches!(
            (l, r),
            (DataType::Bool, DataType::Bool) | (DataType::Varchar(_), DataType::Varchar(_))
        )
}

fn is_number(data_type: DataType) -> bool {
    matches!(
        data_type,
        DataType::Int | DataType::BigInt | DataType::Double
    )
}

/// A call without OVER: an aggregate or a scalar function.
fn bind_function(name: &str, mut args: Vec<BoundExpr>, star: bool) -> Result<BoundExpr> {
    if let Some(function) = AggregateFunction::lookup(name) {
        // A window aggregate may take an aggregate, as it runs after
        // grouping; a plain one runs on the FROM row.
        for arg in &args {
            if arg.contains(&is_aggregate) {
                return Err(PlannerError::NestedAggregate);
            }
            forbid_windows(arg, "aggregate function calls")?;
        }
        let aggregate = bind_aggregate(function, args, star)?;
        return Ok(BoundExpr {
            return_type: aggregate.return_type.clone(),
            kind: BoundExprKind::Aggregate(Box::new(aggregate)),
        });
    }
    if BoundWindowFunction::lookup(name).is_some() {
        return Err(PlannerError::WindowWithoutOver(name.to_owned()));
    }
    if star {
        return Err(PlannerError::UnknownFunction(format!("{name}(*)")));
    }
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

/// `function(arg)`, or `count(*)` when `star`.
fn bind_aggregate(
    function: AggregateFunction,
    mut args: Vec<BoundExpr>,
    star: bool,
) -> Result<BoundAggregate> {
    if star {
        if function != AggregateFunction::Count {
            return Err(PlannerError::UnknownFunction(format!(
                "{}(*)",
                function.name()
            )));
        }
        return Ok(BoundAggregate {
            function,
            arg: None,
            return_type: unnamed(DataType::BigInt, false),
        });
    }
    if args.len() != 1 {
        return Err(PlannerError::WrongArgumentCount {
            function: function.name().to_owned(),
            expected: 1,
            found: args.len(),
        });
    }
    let mut arg = args.remove(0);
    // A NULL argument is NULL whatever its type; INT is as good as any.
    type_null(&mut arg, DataType::Int);
    let ty = arg.return_type.data_type;
    let accepted = match function {
        AggregateFunction::Count => true,
        AggregateFunction::Sum | AggregateFunction::Avg => is_number(ty),
        AggregateFunction::Min | AggregateFunction::Max => comparable(ty, ty),
    };
    if !accepted {
        return Err(ValueError::InvalidOperand {
            op: function.name(),
            operand: ty.type_name(),
        }
        .into());
    }
    let return_type = match function {
        AggregateFunction::Count => unnamed(DataType::BigInt, false),
        AggregateFunction::Sum if ty == DataType::Double => unnamed(DataType::Double, true),
        AggregateFunction::Sum => unnamed(DataType::BigInt, true),
        AggregateFunction::Avg => unnamed(DataType::Double, true),
        AggregateFunction::Min | AggregateFunction::Max => unnamed(ty, true),
    };
    Ok(BoundAggregate {
        function,
        arg: Some(arg),
        return_type,
    })
}

fn constant(value: Value) -> BoundExpr {
    let data_type = match &value {
        // Until its context gives it one; see `type_null`.
        Value::Null => DataType::Int,
        Value::Int(_) => DataType::Int,
        Value::BigInt(_) => DataType::BigInt,
        Value::Double(_) => DataType::Double,
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
