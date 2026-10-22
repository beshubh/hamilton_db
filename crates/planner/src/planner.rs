//! Turns bound statements into plan trees for the execution engine.
//!
//! The planner builds the plain plan for each statement: scans, nested loop
//! joins, a filter for WHERE, aggregation, a filter for HAVING, windows,
//! then sort, limit and projection. Choosing better operators (hash joins,
//! index scans, top-N) is left to rewrites of that tree.
//!
//! Subqueries in FROM are planned as part of the tree. Subqueries in
//! expressions are bound but not planned yet.

mod expr;
#[cfg(test)]
mod tests;

use std::sync::Arc;

use catalog::Catalog;
use common::Schema;
use execution::expressions::{AbstractExpr, ColumnValueExpr};
use execution::plans::{
    AbstractPlanNode, Aggregate, AggregationPlan, DeletePlan, FilterPlan, InsertPlan, JoinType,
    LimitPlan, NestedLoopJoinPlan, OrderBy, PlanNode, ProjectionPlan, SeqScanPlan, SortPlan,
    UpdatePlan, ValuesPlan, WindowColumn, WindowFunction, WindowFunctionKind, WindowPlan,
};
use parser::JoinKind;

use self::expr::{Inputs, plan_expr};
use crate::bound::{
    AggregateFunction, BoundAggregate, BoundDelete, BoundExpr, BoundFrom, BoundInsert,
    BoundOrderBy, BoundSelect, BoundStatement, BoundTable, BoundUpdate, BoundWindow,
    BoundWindowFunction,
};
use crate::error::{PlannerError, Result};

pub struct Planner<'a> {
    // For choosing index scans, once storage has indexes to scan.
    #[allow(dead_code)]
    catalog: &'a Catalog,
}

impl<'a> Planner<'a> {
    pub fn new(catalog: &'a Catalog) -> Self {
        Self { catalog }
    }

    /// The root of the plan for `stmt`. CREATE TABLE has none: it goes to
    /// the catalog directly.
    pub fn plan(&self, stmt: &BoundStatement) -> Result<Arc<AbstractPlanNode>> {
        let plan = match stmt {
            BoundStatement::CreateTable(_) => return Err(PlannerError::NoPlan("CREATE TABLE")),
            BoundStatement::Insert(insert) => self.plan_insert(insert)?,
            BoundStatement::Select(select) => self.plan_select(select)?,
            BoundStatement::Update(update) => self.plan_update(update)?,
            BoundStatement::Delete(delete) => self.plan_delete(delete)?,
        };
        Ok(Arc::new(plan))
    }

    /// `Projection(Limit(Sort(Window(Filter(Aggregation(Filter(from)))))))`,
    /// leaving out what the query doesn't have: the first filter is WHERE
    /// and the second HAVING. Sorting comes before the projection because
    /// ORDER BY can use columns the select list leaves out; limiting does
    /// too, so the select list is computed only for the rows returned.
    fn plan_select(&self, select: &BoundSelect) -> Result<AbstractPlanNode> {
        let mut plan = match &select.from {
            Some(from) => self.plan_from(from)?,
            // One empty row for the select list to be computed on.
            None => Arc::new(ValuesPlan::new(Schema::default(), vec![vec![]]).into()),
        };
        if let Some(selection) = &select.selection {
            plan = filter(plan, selection)?;
        }
        if select.is_aggregate() {
            let group_bys = plan_exprs(&select.group_by)?;
            let aggregates = select
                .aggregates
                .iter()
                .map(plan_aggregate)
                .collect::<Result<_>>()?;
            plan = Arc::new(AggregationPlan::new(group_bys, aggregates, plan).into());
        }
        if let Some(having) = &select.having {
            plan = filter(plan, having)?;
        }
        if !select.windows.is_empty() {
            plan = window(plan, &select.windows)?;
        }
        if !select.order_by.is_empty() {
            let order_bys = plan_order_bys(&select.order_by)?;
            plan = Arc::new(SortPlan::new(order_bys, plan).into());
        }
        if let Some(limit) = select.limit {
            plan = Arc::new(LimitPlan::new(limit, plan).into());
        }
        let exprs = select
            .projection
            .iter()
            .map(|item| plan_expr(&item.expr, Inputs::One))
            .collect::<Result<_>>()?;
        Ok(ProjectionPlan::new(select.output_schema(), exprs, plan).into())
    }

    /// Scans for tables, the subquery's plan for a subquery, and nested loop
    /// joins for joins; their output is the FROM row of
    /// [`BoundFrom::schema`].
    fn plan_from(&self, from: &BoundFrom) -> Result<Arc<AbstractPlanNode>> {
        let plan = match from {
            BoundFrom::Table(table) => return Ok(scan(table)),
            BoundFrom::Subquery { select, .. } => return Ok(Arc::new(self.plan_select(select)?)),
            BoundFrom::Join {
                kind,
                left,
                right,
                on,
            } => {
                let left = self.plan_from(left)?;
                let right = self.plan_from(right)?;
                let inputs = Inputs::Join {
                    left_len: left.get_output_schema().len(),
                };
                let on = on.as_ref().map(|on| plan_expr(on, inputs)).transpose()?;
                let join_type = match kind {
                    JoinKind::Cross | JoinKind::Inner => JoinType::Inner,
                    JoinKind::Left => JoinType::Left,
                };
                NestedLoopJoinPlan::new(join_type, left, right, on)
            }
        };
        Ok(Arc::new(plan.into()))
    }

    /// `Insert(Values)`.
    fn plan_insert(&self, insert: &BoundInsert) -> Result<AbstractPlanNode> {
        let rows = insert
            .rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|value| plan_expr(value, Inputs::One))
                    .collect()
            })
            .collect::<Result<_>>()?;
        let values = ValuesPlan::new(insert.table.schema.clone(), rows);
        Ok(InsertPlan::new(insert.table.oid, Arc::new(values.into())).into())
    }

    /// `Update(Filter(SeqScan))`, with a target expression for every column:
    /// the assigned value, or else the old value.
    fn plan_update(&self, update: &BoundUpdate) -> Result<AbstractPlanNode> {
        let table = &update.table;
        let mut target_exprs: Vec<AbstractExpr> = table
            .schema
            .columns()
            .iter()
            .enumerate()
            .map(|(idx, column)| ColumnValueExpr::new(0, idx, column.clone()).into())
            .collect();
        for (idx, value) in &update.assignments {
            target_exprs[*idx] = plan_expr(value, Inputs::One)?;
        }
        let child = filtered_scan(table, update.selection.as_ref())?;
        Ok(UpdatePlan::new(table.oid, target_exprs, child).into())
    }

    /// `Delete(Filter(SeqScan))`.
    fn plan_delete(&self, delete: &BoundDelete) -> Result<AbstractPlanNode> {
        let child = filtered_scan(&delete.table, delete.selection.as_ref())?;
        Ok(DeletePlan::new(delete.table.oid, child).into())
    }
}

fn scan(table: &BoundTable) -> Arc<AbstractPlanNode> {
    let scan = SeqScanPlan::new(table.oid, &table.name, table.schema.clone(), None);
    Arc::new(scan.into())
}

/// The rows of `table` that pass `selection`, for UPDATE and DELETE.
fn filtered_scan(
    table: &BoundTable,
    selection: Option<&BoundExpr>,
) -> Result<Arc<AbstractPlanNode>> {
    match selection {
        Some(selection) => filter(scan(table), selection),
        None => Ok(scan(table)),
    }
}

fn filter(child: Arc<AbstractPlanNode>, predicate: &BoundExpr) -> Result<Arc<AbstractPlanNode>> {
    let predicate = plan_expr(predicate, Inputs::One)?;
    Ok(Arc::new(FilterPlan::new(predicate, child).into()))
}

/// The child's columns followed by one per window, the row the binder put
/// window results on.
fn window(child: Arc<AbstractPlanNode>, windows: &[BoundWindow]) -> Result<Arc<AbstractPlanNode>> {
    let input = child.get_output_schema().columns().to_vec();
    let mut columns: Vec<WindowColumn> = input
        .iter()
        .enumerate()
        .map(|(idx, column)| {
            WindowColumn::Expr(ColumnValueExpr::new(0, idx, column.clone()).into())
        })
        .collect();
    let mut schema = input;
    for window in windows {
        let function = match &window.function {
            BoundWindowFunction::Aggregate(aggregate) => {
                WindowFunctionKind::Aggregate(plan_aggregate(aggregate)?)
            }
            BoundWindowFunction::Rank => WindowFunctionKind::Rank,
            BoundWindowFunction::RowNumber => WindowFunctionKind::RowNumber,
        };
        columns.push(WindowColumn::Window(WindowFunction {
            function,
            partition_by: plan_exprs(&window.partition_by)?,
            order_by: plan_order_bys(&window.order_by)?,
        }));
        let mut column = window.return_type.clone();
        column.name = window.function.name().to_owned();
        schema.push(column);
    }
    Ok(Arc::new(
        WindowPlan::new(Schema::new(schema), columns, child).into(),
    ))
}

fn plan_aggregate(aggregate: &BoundAggregate) -> Result<Aggregate> {
    let arg = aggregate
        .arg
        .as_ref()
        .map(|arg| plan_expr(arg, Inputs::One))
        .transpose()?;
    Ok(match (aggregate.function, arg) {
        (AggregateFunction::Count, None) => Aggregate::CountStar,
        (AggregateFunction::Count, Some(arg)) => Aggregate::Count(arg),
        (AggregateFunction::Sum, Some(arg)) => Aggregate::Sum(arg),
        (AggregateFunction::Min, Some(arg)) => Aggregate::Min(arg),
        (AggregateFunction::Max, Some(arg)) => Aggregate::Max(arg),
        (function, None) => unreachable!("the binder allows * only for count, not {function:?}"),
    })
}

/// Expressions on the row of a single-input plan.
fn plan_exprs(exprs: &[BoundExpr]) -> Result<Vec<AbstractExpr>> {
    exprs
        .iter()
        .map(|expr| plan_expr(expr, Inputs::One))
        .collect()
}

fn plan_order_bys(order_by: &[BoundOrderBy]) -> Result<Vec<OrderBy>> {
    order_by
        .iter()
        .map(|item| {
            Ok(OrderBy {
                expr: plan_expr(&item.expr, Inputs::One)?,
                desc: item.desc,
            })
        })
        .collect()
}
