//! Describing a prepared statement: the types of its parameters and the
//! columns it produces, before any parameter has a value.

use std::cell::RefCell;

use common::{Column, DataType};
use parser::Statement;

use super::{Binder, Parameters};
use crate::bound::{BoundExpr, BoundExprKind, BoundFrom, BoundSelect, BoundStatement};
use crate::error::Result;

/// What a client learns of a prepared statement before running it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Described {
    /// The type of each `$n`, `None` where nothing in the statement or the
    /// client gave it one.
    pub parameters: Vec<Option<DataType>>,
    /// The columns of the rows it produces, if it produces rows.
    pub columns: Option<Vec<Column>>,
}

impl Binder<'_> {
    /// Describes `stmt`, whose parameters have the types in `given` where
    /// those are `Some`, and otherwise the type their context asks for.
    pub fn describe(mut self, stmt: &Statement, given: &[Option<DataType>]) -> Result<Described> {
        // Other statements can't hold a parameter or produce rows, and
        // binding them now could fail on a catalog that changes by the time
        // they run.
        if !matches!(
            stmt,
            Statement::Select(_)
                | Statement::Insert(_)
                | Statement::Update(_)
                | Statement::Delete(_)
                | Statement::Explain(_)
        ) {
            return Ok(Described {
                parameters: given.to_vec(),
                columns: None,
            });
        }
        self.parameters = Parameters::Types(RefCell::new(given.to_vec()));
        let bound = self.bind(stmt)?;
        let Parameters::Types(types) = self.parameters else {
            unreachable!("set above")
        };
        let mut parameters = types.into_inner();
        collect_statement(&bound, &mut parameters);
        let columns = match &bound {
            BoundStatement::Select(select) => Some(select.output_schema().columns().to_vec()),
            BoundStatement::Insert(insert) if !insert.returning.is_empty() => {
                Some(insert.returning_schema().columns().to_vec())
            }
            BoundStatement::Explain(_) => Some(vec![Column::new(
                "QUERY PLAN",
                DataType::Varchar(u32::MAX),
                false,
            )]),
            _ => None,
        };
        Ok(Described {
            parameters,
            columns,
        })
    }
}

/// Fills in the types the statement's context gave its untyped parameters.
fn collect_statement(stmt: &BoundStatement, types: &mut [Option<DataType>]) {
    match stmt {
        BoundStatement::Insert(insert) => {
            for expr in insert.rows.iter().flatten() {
                collect_expr(expr, types);
            }
            for item in &insert.returning {
                collect_expr(&item.expr, types);
            }
        }
        BoundStatement::Select(select) => collect_select(select, types),
        BoundStatement::Update(update) => {
            for (_, expr) in &update.assignments {
                collect_expr(expr, types);
            }
            if let Some(selection) = &update.selection {
                collect_expr(selection, types);
            }
        }
        BoundStatement::Delete(delete) => {
            if let Some(selection) = &delete.selection {
                collect_expr(selection, types);
            }
        }
        BoundStatement::Explain(stmt) => collect_statement(stmt, types),
        _ => {}
    }
}

fn collect_select(select: &BoundSelect, types: &mut [Option<DataType>]) {
    if let Some(from) = &select.from {
        collect_from(from, types);
    }
    let windows = select.windows.iter().flat_map(|window| window.children());
    let aggregates = select.aggregates.iter().filter_map(|a| a.arg.as_ref());
    for expr in select
        .selection
        .iter()
        .chain(&select.group_by)
        .chain(aggregates)
        .chain(&select.having)
        .chain(windows)
        .chain(select.projection.iter().map(|item| &item.expr))
        .chain(select.order_by.iter().map(|item| &item.expr))
    {
        collect_expr(expr, types);
    }
}

fn collect_from(from: &BoundFrom, types: &mut [Option<DataType>]) {
    match from {
        BoundFrom::Table(_) => {}
        BoundFrom::Subquery { select, .. } => collect_select(select, types),
        BoundFrom::Join {
            left, right, on, ..
        } => {
            collect_from(left, types);
            collect_from(right, types);
            if let Some(on) = on {
                collect_expr(on, types);
            }
        }
    }
}

fn collect_expr(expr: &BoundExpr, types: &mut [Option<DataType>]) {
    match &expr.kind {
        BoundExprKind::Parameter { index, typed: true } => {
            types[index - 1].get_or_insert(expr.return_type.data_type);
        }
        BoundExprKind::Subquery(select)
        | BoundExprKind::Exists {
            subquery: select, ..
        }
        | BoundExprKind::InSubquery {
            subquery: select, ..
        } => collect_select(select, types),
        _ => {}
    }
    for child in expr.children() {
        collect_expr(child, types);
    }
}
