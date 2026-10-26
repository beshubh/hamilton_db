//! Turns bound statements into plan trees for the execution engine.
//!
//! The planner builds the plain plan for each statement: scans, hash joins,
//! a filter for WHERE, aggregation, a filter for HAVING, windows, then sort,
//! limit and projection. Choosing better operators (index scans, index
//! nested loop joins, top-N) is left to the [`Optimizer`]'s rewrites of that
//! tree, which [`Planner::plan`] runs before returning it.
//!
//! Every join is a hash join. The `left = right` equalities ANDed together
//! in its ON condition are its keys, and the rest of the condition is
//! checked on the pairs whose keys match. A join without such equalities,
//! such as a cross join, has no keys: every row lands in the same partition,
//! and the join compares every pair, like a block nested loop join. WHERE
//! conditions are not moved into joins, so `FROM a, b WHERE a.x = b.y` is a
//! cross join under a filter.
//!
//! Subqueries in FROM are planned as part of the tree. Subqueries in
//! expressions are bound but not planned yet.

mod expr;

use std::sync::Arc;

use catalog::Catalog;
use common::Schema;
use execution::expressions::{AbstractExpr, ColumnValueExpr};
use execution::optimizer::Optimizer;
use execution::plans::{
    AbstractPlanNode, Aggregate, AggregationPlan, CreateIndexPlan, CreateTablePlan, DeletePlan,
    ExplainPlan, FilterPlan, HashJoinPlan, InsertPlan, JoinType, LimitPlan, OrderBy, PlanNode,
    ProjectionPlan, SeqScanPlan, SortPlan, UpdatePlan, ValuesPlan, WindowColumn, WindowFunction,
    WindowFunctionKind, WindowPlan,
};
use parser::{BinaryOp, JoinKind};

use self::expr::{Inputs, plan_binary, plan_expr};
use crate::bound::{
    AggregateFunction, BoundAggregate, BoundDelete, BoundExpr, BoundExprKind, BoundFrom,
    BoundInsert, BoundOrderBy, BoundSelect, BoundStatement, BoundTable, BoundUpdate, BoundWindow,
    BoundWindowFunction,
};
use crate::error::Result;

pub struct Planner<'a> {
    /// For the optimizer to choose indexes from.
    catalog: &'a Catalog,
}

impl<'a> Planner<'a> {
    pub fn new(catalog: &'a Catalog) -> Self {
        Self { catalog }
    }

    /// The root of the optimized plan for `stmt`. For EXPLAIN, the plan
    /// shown is the optimized one, as that is what would run.
    pub fn plan(&self, stmt: &BoundStatement) -> Result<Arc<AbstractPlanNode>> {
        let plan = self.plan_statement(stmt)?;
        Ok(Arc::new(Optimizer::new(self.catalog).optimize(&plan)))
    }

    /// The plain plan for `stmt`, before optimizing.
    fn plan_statement(&self, stmt: &BoundStatement) -> Result<AbstractPlanNode> {
        let plan = match stmt {
            BoundStatement::CreateTable(create) => CreateTablePlan::new(
                &create.name,
                create.schema.clone(),
                create.primary_key.clone(),
            )
            .into(),
            BoundStatement::CreateIndex(create) => {
                CreateIndexPlan::new(&create.name, &create.table, create.columns.clone()).into()
            }
            BoundStatement::Insert(insert) => self.plan_insert(insert)?,
            BoundStatement::Select(select) => self.plan_select(select)?,
            BoundStatement::Update(update) => self.plan_update(update)?,
            BoundStatement::Delete(delete) => self.plan_delete(delete)?,
            BoundStatement::Explain(stmt) => {
                ExplainPlan::new(Arc::new(self.plan_statement(stmt)?)).into()
            }
        };
        Ok(plan)
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

    /// Scans for tables, the subquery's plan for a subquery, and hash joins
    /// for joins; their output is the FROM row of [`BoundFrom::schema`].
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
                let left_len = left.get_output_schema().len();
                let join_type = match kind {
                    JoinKind::Cross | JoinKind::Inner => JoinType::Inner,
                    JoinKind::Left => JoinType::Left,
                };
                let (mut left_keys, mut right_keys, mut rest) = (vec![], vec![], vec![]);
                for conjunct in on.iter().flat_map(conjuncts) {
                    match join_keys(conjunct, left_len) {
                        Some((left_key, right_key)) => {
                            left_keys.push(plan_expr(left_key, Inputs::One)?);
                            right_keys
                                .push(plan_expr(right_key, Inputs::RightOfJoin { left_len })?);
                        }
                        None => rest.push(plan_expr(conjunct, Inputs::Join { left_len })?),
                    }
                }
                let predicate = rest
                    .into_iter()
                    .map(Ok)
                    .reduce(|l, r| plan_binary(BinaryOp::And, l?, r?))
                    .transpose()?;
                HashJoinPlan::new(join_type, left, right, left_keys, right_keys, predicate)
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
        Ok(InsertPlan::new(
            insert.table.oid,
            &insert.table.name,
            Arc::new(values.into()),
        )
        .into())
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
        Ok(UpdatePlan::new(table.oid, &table.name, target_exprs, child).into())
    }

    /// `Delete(Filter(SeqScan))`.
    fn plan_delete(&self, delete: &BoundDelete) -> Result<AbstractPlanNode> {
        let child = filtered_scan(&delete.table, delete.selection.as_ref())?;
        Ok(DeletePlan::new(delete.table.oid, &delete.table.name, child).into())
    }
}

/// The expressions ANDed together in `expr`, or `expr` itself.
fn conjuncts(expr: &BoundExpr) -> Vec<&BoundExpr> {
    match &expr.kind {
        BoundExprKind::Binary {
            op: BinaryOp::And,
            left,
            right,
        } => {
            let mut all = conjuncts(left);
            all.extend(conjuncts(right));
            all
        }
        _ => vec![expr],
    }
}

/// If `expr` is `a = b` with one side reading only left columns of a join
/// and the other only right columns, the two sides as a hash join's
/// `(left key, right key)`. Columns of the join row below `left_len` are
/// the left row's.
fn join_keys(expr: &BoundExpr, left_len: usize) -> Option<(&BoundExpr, &BoundExpr)> {
    let BoundExprKind::Binary {
        op: BinaryOp::Eq,
        left,
        right,
    } = &expr.kind
    else {
        return None;
    };
    match (join_side(left, left_len)?, join_side(right, left_len)?) {
        (JoinSide::Left, JoinSide::Right) => Some((left, right)),
        (JoinSide::Right, JoinSide::Left) => Some((right, left)),
        _ => None,
    }
}

#[derive(PartialEq, Eq)]
enum JoinSide {
    Left,
    Right,
}

/// The side of a join whose columns `expr` reads, or `None` if it reads
/// both, neither, or has a subquery.
fn join_side(expr: &BoundExpr, left_len: usize) -> Option<JoinSide> {
    let has_subquery = expr.contains(&|kind| {
        matches!(
            kind,
            BoundExprKind::Subquery(_)
                | BoundExprKind::Exists { .. }
                | BoundExprKind::InSubquery { .. }
        )
    });
    let reads_left =
        expr.contains(&|kind| matches!(kind, BoundExprKind::Column { idx, .. } if *idx < left_len));
    let reads_right = expr
        .contains(&|kind| matches!(kind, BoundExprKind::Column { idx, .. } if *idx >= left_len));
    match (has_subquery, reads_left, reads_right) {
        (false, true, false) => Some(JoinSide::Left),
        (false, false, true) => Some(JoinSide::Right),
        _ => None,
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
        (AggregateFunction::Avg, Some(arg)) => Aggregate::Avg(arg),
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
