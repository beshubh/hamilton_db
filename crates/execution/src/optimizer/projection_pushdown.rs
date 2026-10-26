//! Narrows rows to the columns read above, so sorts, joins and aggregations
//! keep and spill less.
//!
//! - Projections drop unread expressions.
//! - Sorts, joins and aggregations get a projection below if the child has extra columns.
//! - Filters and limits stream, so they pass the narrowing down without one.
//!
//! Aggregations keep all outputs. Other plans keep all child columns
//! (DML and windows need them; index NLJ and top-N aren't handled yet).

use std::collections::BTreeSet;
use std::sync::Arc;

use common::Schema;

use super::expr::{for_each_column, map_columns};
use crate::expressions::{AbstractExpr, ColumnValueExpr, Expr};
use crate::optimizer::Optimizer;
use crate::plans::{
    AbstractPlanNode, Aggregate, AggregationPlan, FilterPlan, HashJoinPlan, NestedLoopJoinPlan,
    OrderBy, PlanNode, ProjectionPlan, SortPlan,
};

/// Output columns of a plan, by index.
type Columns = BTreeSet<usize>;

impl<'a> Optimizer<'a> {
    pub(crate) fn optimize_projection_pushdown(&self, plan: &AbstractPlanNode) -> AbstractPlanNode {
        prune(plan, &all_columns(plan)).plan
    }
}

/// A plan rewritten to output only some of its columns.
struct Pruned {
    plan: AbstractPlanNode,
    /// Original column index of each output column, ascending.
    columns: Vec<usize>,
}

impl Pruned {
    fn new(plan: AbstractPlanNode, columns: &Columns) -> Self {
        Self {
            plan,
            columns: columns.iter().copied().collect(),
        }
    }

    fn all(plan: AbstractPlanNode) -> Self {
        let columns = all_columns(&plan);
        Self::new(plan, &columns)
    }

    /// Index in `plan`'s output of original column `col_idx`.
    fn position(&self, col_idx: usize) -> usize {
        self.columns
            .binary_search(&col_idx)
            .expect("the columns read are kept")
    }

    /// `expr` moved from the original output onto `plan`'s.
    fn rewrite(&self, expr: &AbstractExpr) -> AbstractExpr {
        map_columns(expr, &|column| self.moved(column))
    }

    fn moved(&self, column: &ColumnValueExpr) -> AbstractExpr {
        let col_idx = self.position(column.col_idx());
        ColumnValueExpr::new(column.tuple_idx(), col_idx, column.return_type().clone()).into()
    }
}

/// `plan` outputting at least the `read` columns.
fn prune(plan: &AbstractPlanNode, read: &Columns) -> Pruned {
    match plan {
        AbstractPlanNode::ProjectionPlan(projection) => {
            let exprs: Vec<_> = read.iter().map(|&idx| &projection.exprs()[idx]).collect();
            let child = prune(projection.child(), &columns_read(exprs.iter().copied()));
            let exprs = exprs.into_iter().map(|expr| child.rewrite(expr)).collect();
            let schema = schema_of(projection.get_output_schema(), read.iter().copied());
            let plan = ProjectionPlan::new(schema, exprs, Arc::new(child.plan));
            Pruned::new(plan.into(), read)
        }
        AbstractPlanNode::FilterPlan(filter) => {
            let child = prune(
                filter.child(),
                &with_columns_read(read, [filter.predicate()]),
            );
            let predicate = child.rewrite(filter.predicate());
            Pruned {
                plan: FilterPlan::new(predicate, Arc::new(child.plan)).into(),
                columns: child.columns,
            }
        }
        AbstractPlanNode::LimitPlan(limit) => {
            let child = prune(limit.child(), read);
            Pruned {
                plan: plan.clone_with_children(vec![Arc::new(child.plan)]),
                columns: child.columns,
            }
        }
        AbstractPlanNode::SortPlan(sort) => {
            let keys = sort.order_bys().iter().map(|order_by| &order_by.expr);
            let child = narrowed(sort.child(), &with_columns_read(read, keys));
            let order_bys = sort
                .order_bys()
                .iter()
                .map(|order_by| OrderBy {
                    expr: child.rewrite(&order_by.expr),
                    desc: order_by.desc,
                })
                .collect();
            Pruned {
                plan: SortPlan::new(order_bys, Arc::new(child.plan)).into(),
                columns: child.columns,
            }
        }
        AbstractPlanNode::AggregationPlan(aggregation) => {
            let args = aggregation.aggregates().iter().filter_map(Aggregate::arg);
            let group_bys = aggregation.group_bys();
            let child = narrowed(
                aggregation.child(),
                &columns_read(group_bys.iter().chain(args)),
            );
            let group_bys = group_bys.iter().map(|expr| child.rewrite(expr)).collect();
            let aggregates = aggregation
                .aggregates()
                .iter()
                .map(|aggregate| rewrite_aggregate(aggregate, &child))
                .collect();
            Pruned::all(AggregationPlan::new(group_bys, aggregates, Arc::new(child.plan)).into())
        }
        AbstractPlanNode::HashJoinPlan(join) => {
            let left_len = join.left().get_output_schema().len();
            let (left, right) = narrowed_sides(
                join.left(),
                join.right(),
                read,
                (join.left_keys(), join.right_keys()),
                join.predicate(),
            );
            let left_keys = join
                .left_keys()
                .iter()
                .map(|key| left.rewrite(key))
                .collect();
            let right_keys = join
                .right_keys()
                .iter()
                .map(|key| right.rewrite(key))
                .collect();
            let predicate = join
                .predicate()
                .map(|predicate| rewrite_pair(predicate, &left, &right));
            let columns = join_columns(&left, &right, left_len);
            let plan = HashJoinPlan::new(
                join.join_type(),
                Arc::new(left.plan),
                Arc::new(right.plan),
                left_keys,
                right_keys,
                predicate,
            );
            Pruned {
                plan: plan.into(),
                columns,
            }
        }
        AbstractPlanNode::NestedLoopJoinPlan(join) => {
            let left_len = join.left().get_output_schema().len();
            let (left, right) = narrowed_sides(
                join.left(),
                join.right(),
                read,
                (&[], &[]),
                join.predicate(),
            );
            let predicate = join
                .predicate()
                .map(|predicate| rewrite_pair(predicate, &left, &right));
            let columns = join_columns(&left, &right, left_len);
            let plan = NestedLoopJoinPlan::new(
                join.join_type(),
                Arc::new(left.plan),
                Arc::new(right.plan),
                predicate,
            );
            Pruned {
                plan: plan.into(),
                columns,
            }
        }
        _ => {
            let children = plan
                .get_children()
                .iter()
                .map(|child| Arc::new(prune(child, &all_columns(child)).plan))
                .collect();
            Pruned::all(plan.clone_with_children(children))
        }
    }
}

/// `plan` outputting exactly `read`, under a projection if needed.
fn narrowed(plan: &AbstractPlanNode, read: &Columns) -> Pruned {
    let pruned = prune(plan, read);
    if pruned.columns.len() == read.len() {
        return pruned;
    }
    let child_schema = pruned.plan.get_output_schema();
    let positions: Vec<_> = read.iter().map(|&idx| pruned.position(idx)).collect();
    let exprs = positions
        .iter()
        .map(|&idx| {
            let column = child_schema.column(idx).expect("a column of the child");
            ColumnValueExpr::new(0, idx, column.clone()).into()
        })
        .collect();
    let schema = schema_of(child_schema, positions.iter().copied());
    let projection = ProjectionPlan::new(schema, exprs, Arc::new(pruned.plan));
    Pruned::new(projection.into(), read)
}

/// Join sides narrowed to `read` (on the joined row) plus what `keys` and `on_pair` read.
fn narrowed_sides(
    left: &AbstractPlanNode,
    right: &AbstractPlanNode,
    read: &Columns,
    keys: (&[AbstractExpr], &[AbstractExpr]),
    on_pair: Option<&AbstractExpr>,
) -> (Pruned, Pruned) {
    let left_len = left.get_output_schema().len();
    let (read_left, read_right): (Columns, Columns) = read
        .iter()
        .copied()
        .partition(|&col_idx| col_idx < left_len);
    let mut read_left = with_columns_read(&read_left, keys.0);
    let read_right = read_right
        .iter()
        .map(|col_idx| col_idx - left_len)
        .collect();
    let mut read_right = with_columns_read(&read_right, keys.1);
    if let Some(on_pair) = on_pair {
        for_each_column(on_pair, &mut |column| {
            let side = match column.tuple_idx() {
                0 => &mut read_left,
                _ => &mut read_right,
            };
            side.insert(column.col_idx());
        });
    }
    (narrowed(left, &read_left), narrowed(right, &read_right))
}

/// `on_pair` moved onto the narrowed `left` and `right`.
fn rewrite_pair(on_pair: &AbstractExpr, left: &Pruned, right: &Pruned) -> AbstractExpr {
    map_columns(on_pair, &|column| match column.tuple_idx() {
        0 => left.moved(column),
        _ => right.moved(column),
    })
}

/// Original join column of each output column of the narrowed join.
fn join_columns(left: &Pruned, right: &Pruned, left_len: usize) -> Vec<usize> {
    let right_columns = right.columns.iter().map(|col_idx| col_idx + left_len);
    left.columns.iter().copied().chain(right_columns).collect()
}

fn rewrite_aggregate(aggregate: &Aggregate, child: &Pruned) -> Aggregate {
    match aggregate {
        Aggregate::CountStar => Aggregate::CountStar,
        Aggregate::Count(arg) => Aggregate::Count(child.rewrite(arg)),
        Aggregate::Sum(arg) => Aggregate::Sum(child.rewrite(arg)),
        Aggregate::Min(arg) => Aggregate::Min(child.rewrite(arg)),
        Aggregate::Max(arg) => Aggregate::Max(child.rewrite(arg)),
        Aggregate::Avg(arg) => Aggregate::Avg(child.rewrite(arg)),
    }
}

fn all_columns(plan: &AbstractPlanNode) -> Columns {
    (0..plan.get_output_schema().len()).collect()
}

fn columns_read<'e>(exprs: impl IntoIterator<Item = &'e AbstractExpr>) -> Columns {
    with_columns_read(&Columns::new(), exprs)
}

/// `read` plus the columns `exprs` read.
fn with_columns_read<'e>(
    read: &Columns,
    exprs: impl IntoIterator<Item = &'e AbstractExpr>,
) -> Columns {
    let mut read = read.clone();
    for expr in exprs {
        for_each_column(expr, &mut |column| {
            read.insert(column.col_idx());
        });
    }
    read
}

fn schema_of(schema: &Schema, indexes: impl Iterator<Item = usize>) -> Schema {
    let columns = indexes
        .map(|idx| schema.column(idx).expect("a column of the schema").clone())
        .collect();
    Schema::new(columns)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executors::test_util::{Db, column, compare, int};
    use crate::expressions::ComparisonOp;
    use crate::optimizer::test_util::{
        assert_same_rows, db_with_indexes, filter, hash_join, joined, project, sort, text,
    };
    use crate::plans::{DeletePlan, JoinType};

    fn db() -> Db {
        db_with_indexes(&[])
    }

    fn optimize(db: &Db, plan: &AbstractPlanNode) -> AbstractPlanNode {
        Optimizer::new(db.catalog()).optimize_projection_pushdown(plan)
    }

    /// The expressions of `plan`, which must be a projection.
    fn projected(plan: &AbstractPlanNode) -> &[AbstractExpr] {
        match plan {
            AbstractPlanNode::ProjectionPlan(projection) => projection.exprs(),
            _ => panic!("expected a projection, got {plan:?}"),
        }
    }

    fn is_scan(plan: &AbstractPlanNode) -> bool {
        matches!(plan, AbstractPlanNode::SeqScanPlan(_))
    }

    #[test]
    fn narrows_the_sides_of_a_join() {
        // SELECT a.name FROM users a JOIN users b ON a.id = b.id: b's name
        // is read by nothing.
        let db = db();
        let join = hash_join(&db, JoinType::Inner, true, None);
        let plan = project(vec![joined(1)], Arc::new(join.into()));
        let optimized = optimize(&db, &plan);
        let join = optimized.get_child_at(0);
        assert!(is_scan(&join.get_child_at(0)));
        let right = join.get_child_at(1);
        assert_eq!(projected(&right).len(), 1);
        assert_eq!(join.get_output_schema().len(), 3);
        assert_same_rows(&db, &plan, &optimized);
    }

    #[test]
    fn narrows_the_rows_a_sort_keeps() {
        // SELECT id FROM users WHERE name <> 'b' ORDER BY id
        let db = db();
        let not_b = filter(&db, compare(ComparisonOp::NotEq, column(1), text("b")));
        let plan = project(vec![column(0)], Arc::new(sort(not_b, &[(0, false)])));
        let optimized = optimize(&db, &plan);
        let sort = optimized.get_child_at(0);
        let below_sort = sort.get_child_at(0);
        assert_eq!(projected(&below_sort).len(), 1);
        assert!(matches!(
            *below_sort.get_child_at(0),
            AbstractPlanNode::FilterPlan(_)
        ));
        assert_eq!(db.run(optimized).unwrap(), db.run(plan).unwrap());
    }

    #[test]
    fn adds_no_projection_where_rows_stream_to_one() {
        // SELECT id FROM users WHERE name <> 'b'
        let db = db();
        let not_b = filter(&db, compare(ComparisonOp::NotEq, column(1), text("b")));
        let plan = project(vec![column(0)], not_b);
        let optimized = optimize(&db, &plan);
        let filter = optimized.get_child_at(0);
        assert!(is_scan(&filter.get_child_at(0)));
        assert_same_rows(&db, &plan, &optimized);
    }

    #[test]
    fn drops_the_expressions_of_a_projection_not_read_above() {
        // SELECT id FROM (SELECT id, name, id > 1 FROM users) ORDER BY id
        let db = db();
        let above_one = compare(ComparisonOp::Gt, column(0), int(1));
        let inner = project(vec![column(0), column(1), above_one], db.scan(None));
        let plan = project(
            vec![column(0)],
            Arc::new(sort(Arc::new(inner), &[(0, false)])),
        );
        let optimized = optimize(&db, &plan);
        let inner = optimized.get_child_at(0).get_child_at(0);
        assert_eq!(projected(&inner).len(), 1);
        assert_eq!(db.run(optimized).unwrap(), db.run(plan).unwrap());
    }

    #[test]
    fn rows_can_be_narrowed_to_no_columns() {
        // SELECT COUNT(*) FROM users
        let db = db();
        let count = AggregationPlan::new(vec![], vec![Aggregate::CountStar], db.scan(None));
        let plan: AbstractPlanNode = count.into();
        let optimized = optimize(&db, &plan);
        assert!(projected(&optimized.get_child_at(0)).is_empty());
        assert_eq!(db.run(optimized).unwrap(), db.run(plan).unwrap());

        // SELECT 1 FROM users a, users b
        let join = hash_join(&db, JoinType::Inner, false, None);
        let plan = project(vec![int(1)], Arc::new(join.into()));
        let optimized = optimize(&db, &plan);
        let join = optimized.get_child_at(0);
        assert!(join.get_output_schema().is_empty());
        assert_eq!(db.run(optimized).unwrap().len(), 36);
    }

    #[test]
    fn keeps_the_whole_rows_of_a_delete() {
        let db = db();
        let not_b = filter(&db, compare(ComparisonOp::NotEq, column(1), text("b")));
        let plan: AbstractPlanNode = DeletePlan::new(db.users.oid, "users", not_b).into();
        let optimized = optimize(&db, &plan);
        let filter = optimized.get_child_at(0);
        assert!(is_scan(&filter.get_child_at(0)));
        assert_eq!(db.run(optimized).unwrap(), [[common::Value::BigInt(2)]]);
    }
}
