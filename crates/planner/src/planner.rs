//! Turns bound statements into plan trees for the execution engine.
//!
//! The planner builds the plain plan for each statement: scans, nested loop
//! joins, a filter for WHERE, then sort, limit and projection. Choosing
//! better operators (hash joins, index scans, top-N) is left to rewrites of
//! that tree.

mod expr;
#[cfg(test)]
mod tests;

use std::sync::Arc;

use catalog::Catalog;
use common::Schema;
use execution::expressions::{AbstractExpr, ColumnValueExpr};
use execution::plans::{
    AbstractPlanNode, DeletePlan, FilterPlan, InsertPlan, JoinType, LimitPlan, NestedLoopJoinPlan,
    OrderBy, PlanNode, ProjectionPlan, SeqScanPlan, SortPlan, UpdatePlan, ValuesPlan,
};
use parser::JoinKind;

use self::expr::{Inputs, plan_expr};
use crate::bound::{
    BoundDelete, BoundExpr, BoundFrom, BoundInsert, BoundSelect, BoundStatement, BoundTable,
    BoundUpdate,
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

    /// `Projection(Limit(Sort(Filter(from))))`, leaving out what the query
    /// doesn't have. Sorting comes before the projection because ORDER BY
    /// can use columns the select list leaves out; limiting does too, so the
    /// select list is computed only for the rows returned.
    fn plan_select(&self, select: &BoundSelect) -> Result<AbstractPlanNode> {
        let mut plan = match &select.from {
            Some(from) => self.plan_from(from)?,
            // One empty row for the select list to be computed on.
            None => Arc::new(ValuesPlan::new(Schema::default(), vec![vec![]]).into()),
        };
        if let Some(selection) = &select.selection {
            plan = filter(plan, selection)?;
        }
        if !select.order_by.is_empty() {
            let order_bys = select
                .order_by
                .iter()
                .map(|item| {
                    Ok(OrderBy {
                        expr: plan_expr(&item.expr, Inputs::One)?,
                        desc: item.desc,
                    })
                })
                .collect::<Result<_>>()?;
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

    /// Scans for tables and nested loop joins for joins; their output is the
    /// FROM row of [`BoundFrom::schema`].
    fn plan_from(&self, from: &BoundFrom) -> Result<Arc<AbstractPlanNode>> {
        let plan = match from {
            BoundFrom::Table(table) => return Ok(scan(table)),
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
