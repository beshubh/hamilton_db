//! Moving the expressions of an aggregating or windowed SELECT onto the rows
//! they are evaluated on.
//!
//! The select list, HAVING and ORDER BY are first bound on the FROM row, with
//! aggregate and window calls left in place as [`BoundExprKind::Aggregate`]
//! and [`BoundExprKind::Window`]. [`Rewriter`] then collects those calls and
//! replaces them, and in an aggregating SELECT the GROUP BY keys too, with
//! columns of the row computed for them (see [`crate::bound`]).

use common::Column;

use crate::bound::{
    BoundAggregate, BoundExpr, BoundExprKind, BoundOrderBy, BoundWindow, BoundWindowFunction,
};
use crate::error::{PlannerError, Result};

pub(super) struct Rewriter<'a> {
    group_by: &'a [BoundExpr],
    aggregating: bool,
    aggregates: Vec<BoundAggregate>,
    /// Width of the row the window results are appended to.
    window_offset: usize,
    windows: Vec<BoundWindow>,
}

impl<'a> Rewriter<'a> {
    /// `exprs` are every expression evaluated after grouping (select list,
    /// HAVING, ORDER BY), still on the FROM row of width `from_width`. The
    /// SELECT aggregates if it has GROUP BY keys, a HAVING or an aggregate
    /// call.
    pub fn new<'e>(
        group_by: &'a [BoundExpr],
        has_having: bool,
        exprs: impl IntoIterator<Item = &'e BoundExpr>,
        from_width: usize,
    ) -> Self {
        let mut aggregates = Vec::new();
        for expr in exprs {
            collect_aggregates(expr, &mut aggregates);
        }
        let aggregating = !group_by.is_empty() || has_having || !aggregates.is_empty();
        let window_offset = if aggregating {
            group_by.len() + aggregates.len()
        } else {
            from_width
        };
        Self {
            group_by,
            aggregating,
            aggregates,
            window_offset,
            windows: Vec::new(),
        }
    }

    /// The aggregate calls and window calls found, in column order.
    pub fn finish(self) -> (Vec<BoundAggregate>, Vec<BoundWindow>) {
        (self.aggregates, self.windows)
    }

    /// `expr` with its aggregate and window calls and GROUP BY keys replaced
    /// by columns. Fails on a column that an aggregating SELECT does not
    /// group by.
    pub fn rewrite(&mut self, expr: BoundExpr) -> Result<BoundExpr> {
        if self.aggregating
            && let Some(idx) = self.group_by.iter().position(|key| *key == expr)
        {
            return Ok(column(idx, expr.return_type));
        }
        match expr.kind {
            BoundExprKind::Aggregate(aggregate) => {
                let idx = self
                    .aggregates
                    .iter()
                    .position(|a| *a == *aggregate)
                    .expect("Rewriter::new collected every aggregate");
                Ok(column(self.group_by.len() + idx, expr.return_type))
            }
            BoundExprKind::Window(window) => {
                let window = self.rewrite_window(*window)?;
                let idx = match self.windows.iter().position(|w| *w == window) {
                    Some(idx) => idx,
                    None => {
                        self.windows.push(window);
                        self.windows.len() - 1
                    }
                };
                Ok(column(self.window_offset + idx, expr.return_type))
            }
            BoundExprKind::Column { table, .. } if self.aggregating => Err(
                PlannerError::NotGrouped(format!("{table}.{}", expr.return_type.name)),
            ),
            kind => self.rewrite_children(BoundExpr {
                kind,
                return_type: expr.return_type,
            }),
        }
    }

    /// A window's argument, partition and sort keys are on the row before
    /// the windows, so they are rewritten the same way.
    fn rewrite_window(&mut self, window: BoundWindow) -> Result<BoundWindow> {
        let function = match window.function {
            BoundWindowFunction::Aggregate(aggregate) => {
                BoundWindowFunction::Aggregate(BoundAggregate {
                    arg: aggregate.arg.map(|arg| self.rewrite(arg)).transpose()?,
                    ..aggregate
                })
            }
            function => function,
        };
        Ok(BoundWindow {
            function,
            partition_by: self.rewrite_all(window.partition_by)?,
            order_by: window
                .order_by
                .into_iter()
                .map(|item| {
                    Ok(BoundOrderBy {
                        expr: self.rewrite(item.expr)?,
                        desc: item.desc,
                    })
                })
                .collect::<Result<_>>()?,
            return_type: window.return_type,
        })
    }

    fn rewrite_all(&mut self, exprs: Vec<BoundExpr>) -> Result<Vec<BoundExpr>> {
        exprs.into_iter().map(|expr| self.rewrite(expr)).collect()
    }

    fn rewrite_boxed(&mut self, expr: BoundExpr) -> Result<Box<BoundExpr>> {
        Ok(Box::new(self.rewrite(expr)?))
    }

    fn rewrite_children(&mut self, expr: BoundExpr) -> Result<BoundExpr> {
        let kind = match expr.kind {
            BoundExprKind::Binary { op, left, right } => BoundExprKind::Binary {
                op,
                left: self.rewrite_boxed(*left)?,
                right: self.rewrite_boxed(*right)?,
            },
            BoundExprKind::Unary { op, expr } => BoundExprKind::Unary {
                op,
                expr: self.rewrite_boxed(*expr)?,
            },
            BoundExprKind::IsNull { expr, negated } => BoundExprKind::IsNull {
                expr: self.rewrite_boxed(*expr)?,
                negated,
            },
            BoundExprKind::Function { function, args } => BoundExprKind::Function {
                function,
                args: self.rewrite_all(args)?,
            },
            BoundExprKind::Array(elements) => BoundExprKind::Array(self.rewrite_all(elements)?),
            BoundExprKind::Cast(expr) => BoundExprKind::Cast(self.rewrite_boxed(*expr)?),
            BoundExprKind::InSubquery {
                expr,
                subquery,
                negated,
            } => BoundExprKind::InSubquery {
                expr: self.rewrite_boxed(*expr)?,
                subquery,
                negated,
            },
            // Columns of a SELECT that doesn't aggregate stay on the FROM
            // row; subqueries are bound on their own rows.
            kind @ (BoundExprKind::Column { .. }
            | BoundExprKind::Constant(_)
            | BoundExprKind::Subquery(_)
            | BoundExprKind::Exists { .. }) => kind,
            BoundExprKind::Aggregate(_) | BoundExprKind::Window(_) => {
                unreachable!("rewrite handles calls itself")
            }
        };
        Ok(BoundExpr {
            kind,
            return_type: expr.return_type,
        })
    }
}

/// Adds the aggregate calls in `expr` that `out` doesn't have yet. Calls
/// are not nested (the binder rejects that), but may sit inside a window's
/// argument, partition or sort keys.
fn collect_aggregates(expr: &BoundExpr, out: &mut Vec<BoundAggregate>) {
    if let BoundExprKind::Aggregate(aggregate) = &expr.kind {
        if !out.contains(aggregate) {
            out.push((**aggregate).clone());
        }
        return;
    }
    for child in expr.children() {
        collect_aggregates(child, out);
    }
}

/// A column of an aggregation or window row.
fn column(idx: usize, return_type: Column) -> BoundExpr {
    BoundExpr {
        kind: BoundExprKind::Column {
            table: String::new(),
            idx,
        },
        return_type,
    }
}
