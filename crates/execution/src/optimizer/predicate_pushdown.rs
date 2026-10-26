//! Splits filters into conjuncts and pushes each as far down as it goes:
//! into scans, through sorts and projections, through GROUP BY if it reads
//! only group columns, and into joins (to one side, or as a key/predicate).
//!
//! LEFT join: only WHERE on left columns and ON on right columns move down.
//! Conjuncts reading no columns count as left. Other plans keep a filter above.

use std::sync::Arc;

use super::expr::{
    Sides, conjunction, conjuncts, for_each_column, hash_keys, map_columns, sides_read,
    to_right_row,
};
use crate::expressions::{AbstractExpr, ColumnValueExpr, Expr};
use crate::optimizer::Optimizer;
use crate::plans::{
    AbstractPlanNode, FilterPlan, HashJoinPlan, IndexScanPlan, JoinType, NestedLoopJoinPlan,
    PlanNode, SeqScanPlan,
};

impl<'a> Optimizer<'a> {
    pub(crate) fn optimize_predicate_pushdown(&self, plan: &AbstractPlanNode) -> AbstractPlanNode {
        push_down(plan, Vec::new())
    }
}

/// `plan` filtered by `conjuncts_above` (on its output row), all pushed down.
fn push_down(plan: &AbstractPlanNode, mut conjuncts_above: Vec<AbstractExpr>) -> AbstractPlanNode {
    match plan {
        AbstractPlanNode::FilterPlan(filter) => {
            conjuncts_above.extend(conjuncts(filter.predicate()).into_iter().cloned());
            push_down(filter.child(), conjuncts_above)
        }
        AbstractPlanNode::SeqScanPlan(scan) => {
            let filter = with_conjuncts(scan.filter(), conjuncts_above);
            let schema = scan.get_output_schema().clone();
            SeqScanPlan::new(scan.table_oid(), scan.table_name(), schema, filter).into()
        }
        AbstractPlanNode::IndexScanPlan(scan) => IndexScanPlan::new(
            scan.table_oid(),
            scan.index_oid(),
            scan.index_name(),
            scan.get_output_schema().clone(),
            scan.range().clone(),
            with_conjuncts(scan.filter(), conjuncts_above),
        )
        .into(),
        AbstractPlanNode::SortPlan(sort) => {
            with_child(plan, push_down(sort.child(), conjuncts_above))
        }
        AbstractPlanNode::ProjectionPlan(projection) => {
            let exprs = projection.exprs();
            let below = conjuncts_above
                .iter()
                .map(|conjunct| map_columns(conjunct, &|column| exprs[column.col_idx()].clone()))
                .collect();
            with_child(plan, push_down(projection.child(), below))
        }
        AbstractPlanNode::AggregationPlan(aggregation) if !aggregation.group_bys().is_empty() => {
            let group_bys = aggregation.group_bys();
            let (on_groups, rest): (Vec<_>, Vec<_>) = conjuncts_above
                .into_iter()
                .partition(|conjunct| reads_only_columns_below(conjunct, group_bys.len()));
            let below = on_groups
                .iter()
                .map(|conjunct| {
                    map_columns(conjunct, &|column| group_bys[column.col_idx()].clone())
                })
                .collect();
            let aggregation = with_child(plan, push_down(aggregation.child(), below));
            filtered(aggregation, rest)
        }
        AbstractPlanNode::HashJoinPlan(join) => push_into_hash_join(join, conjuncts_above),
        AbstractPlanNode::NestedLoopJoinPlan(join) => {
            push_into_nested_loop_join(join, conjuncts_above)
        }
        _ => {
            let children = plan
                .get_children()
                .iter()
                .map(|child| Arc::new(push_down(child, Vec::new())))
                .collect();
            filtered(plan.clone_with_children(children), conjuncts_above)
        }
    }
}

fn push_into_hash_join(
    join: &HashJoinPlan,
    conjuncts_above: Vec<AbstractExpr>,
) -> AbstractPlanNode {
    let conditions = JoinConditions::split(
        join.join_type(),
        join.left().get_output_schema().len(),
        conjuncts_above,
        join.predicate(),
    );
    let (mut left_keys, mut right_keys) = (join.left_keys().to_vec(), join.right_keys().to_vec());
    let mut predicate = Vec::new();
    for condition in conditions.on_pairs {
        match hash_keys(&condition) {
            Some((left_key, right_key)) => {
                left_keys.push(left_key);
                right_keys.push(right_key);
            }
            None => predicate.push(condition),
        }
    }
    let join = HashJoinPlan::new(
        join.join_type(),
        Arc::new(push_down(join.left(), conditions.on_left)),
        Arc::new(push_down(join.right(), conditions.on_right)),
        left_keys,
        right_keys,
        conjunction(predicate),
    );
    filtered(join.into(), conditions.above)
}

fn push_into_nested_loop_join(
    join: &NestedLoopJoinPlan,
    conjuncts_above: Vec<AbstractExpr>,
) -> AbstractPlanNode {
    let conditions = JoinConditions::split(
        join.join_type(),
        join.left().get_output_schema().len(),
        conjuncts_above,
        join.predicate(),
    );
    let join = NestedLoopJoinPlan::new(
        join.join_type(),
        Arc::new(push_down(join.left(), conditions.on_left)),
        Arc::new(push_down(join.right(), conditions.on_right)),
        conjunction(conditions.on_pairs),
    );
    filtered(join.into(), conditions.above)
}

/// Where the conditions on a join go.
struct JoinConditions {
    on_left: Vec<AbstractExpr>,
    on_right: Vec<AbstractExpr>,
    /// For the join itself.
    on_pairs: Vec<AbstractExpr>,
    /// For a filter above the join.
    above: Vec<AbstractExpr>,
}

impl JoinConditions {
    /// `conjuncts_above` are on the joined row, `predicate` on the pair.
    fn split(
        join_type: JoinType,
        left_len: usize,
        conjuncts_above: Vec<AbstractExpr>,
        predicate: Option<&AbstractExpr>,
    ) -> Self {
        let mut conditions = Self {
            on_left: Vec::new(),
            on_right: Vec::new(),
            on_pairs: Vec::new(),
            above: Vec::new(),
        };
        for conjunct in conjuncts_above {
            let on_pair = to_pair(&conjunct, left_len);
            match (sides_read(&on_pair), join_type) {
                (Sides::Left | Sides::Neither, _) => conditions.on_left.push(on_pair),
                (Sides::Right, JoinType::Inner) => conditions.on_right.push(to_right_row(&on_pair)),
                (Sides::Both, JoinType::Inner) => conditions.on_pairs.push(on_pair),
                (Sides::Right | Sides::Both, JoinType::Left) => conditions.above.push(conjunct),
            }
        }
        for conjunct in predicate.into_iter().flat_map(conjuncts) {
            match (sides_read(conjunct), join_type) {
                (Sides::Left | Sides::Neither, JoinType::Inner) => {
                    conditions.on_left.push(conjunct.clone())
                }
                (Sides::Right, _) => conditions.on_right.push(to_right_row(conjunct)),
                _ => conditions.on_pairs.push(conjunct.clone()),
            }
        }
        conditions
    }
}

/// `conjunct` on the joined row, rewritten onto the (left, right) pair.
fn to_pair(conjunct: &AbstractExpr, left_len: usize) -> AbstractExpr {
    map_columns(conjunct, &|column| {
        if column.col_idx() < left_len {
            return column.clone().into();
        }
        let col_idx = column.col_idx() - left_len;
        ColumnValueExpr::new(1, col_idx, column.return_type().clone()).into()
    })
}

/// Whether every column `conjunct` reads is one of the first `len`.
fn reads_only_columns_below(conjunct: &AbstractExpr, len: usize) -> bool {
    let mut below = true;
    for_each_column(conjunct, &mut |column| below &= column.col_idx() < len);
    below
}

/// `filter` AND `more`.
fn with_conjuncts(filter: Option<&AbstractExpr>, more: Vec<AbstractExpr>) -> Option<AbstractExpr> {
    let existing = filter.into_iter().flat_map(conjuncts).cloned();
    conjunction(existing.chain(more))
}

/// `plan` under a filter on `conjuncts`, if any.
fn filtered(plan: AbstractPlanNode, conjuncts: Vec<AbstractExpr>) -> AbstractPlanNode {
    match conjunction(conjuncts) {
        Some(predicate) => FilterPlan::new(predicate, Arc::new(plan)).into(),
        None => plan,
    }
}

/// Replaces the only child of `plan`.
fn with_child(plan: &AbstractPlanNode, child: AbstractPlanNode) -> AbstractPlanNode {
    plan.clone_with_children(vec![Arc::new(child)])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executors::test_util::{Db, column, compare, constant, int};
    use crate::expressions::{ArithmeticExpr, ArithmeticOp, ComparisonOp, IsNullExpr};
    use crate::optimizer::test_util::{
        and, assert_same_rows, db_with_indexes, filter, filter_on, hash_join, joined, project,
        right, sort, text,
    };
    use crate::plans::{Aggregate, AggregationPlan, LimitPlan};
    use common::{DataType, Value};

    fn db() -> Db {
        db_with_indexes(&[])
    }

    fn optimize(db: &Db, plan: &AbstractPlanNode) -> AbstractPlanNode {
        Optimizer::new(db.catalog()).optimize_predicate_pushdown(plan)
    }

    fn scan_filter(plan: &AbstractPlanNode) -> Option<&AbstractExpr> {
        match plan {
            AbstractPlanNode::SeqScanPlan(scan) => scan.filter(),
            _ => panic!("expected a table scan, got {plan:?}"),
        }
    }

    fn the_join(plan: &AbstractPlanNode) -> &HashJoinPlan {
        match plan {
            AbstractPlanNode::HashJoinPlan(join) => join,
            _ => panic!("expected a hash join, got {plan:?}"),
        }
    }

    /// Column `idx` of `plan`'s output.
    fn output(plan: &impl PlanNode, idx: usize) -> AbstractExpr {
        let column = plan.get_output_schema().column(idx).unwrap().clone();
        ColumnValueExpr::new(0, idx, column).into()
    }

    #[test]
    fn moves_a_filter_into_the_scan() {
        let db = db();
        let plan =
            Arc::unwrap_or_clone(filter(&db, compare(ComparisonOp::Eq, column(1), text("b"))));
        let optimized = optimize(&db, &plan);
        assert!(scan_filter(&optimized).is_some());
        assert_same_rows(&db, &plan, &optimized);
    }

    #[test]
    fn goes_through_sorts_and_projections() {
        // SELECT * FROM (SELECT id + 1 AS next, name FROM users ORDER BY id)
        // WHERE next > 3
        let db = db();
        let next = ArithmeticExpr::new(ArithmeticOp::Add, column(0), int(1)).unwrap();
        let sorted = Arc::new(sort(db.scan(None), &[(0, false)]));
        let projection = project(vec![next.into(), column(1)], sorted);
        let next_above_3 = compare(ComparisonOp::Gt, output(&projection, 0), int(3));
        let plan = filter_on(next_above_3, projection);
        let optimized = optimize(&db, &plan);
        let AbstractPlanNode::ProjectionPlan(projection) = &optimized else {
            panic!("expected a projection, got {optimized:?}");
        };
        assert!(scan_filter(&projection.child().get_child_at(0)).is_some());
        assert_eq!(db.run(optimized).unwrap(), db.run(plan).unwrap());
    }

    #[test]
    fn a_where_on_both_sides_of_a_cross_join_becomes_a_hash_key() {
        // FROM users a, users b WHERE a.id = b.id AND a.name = 'b' AND b.id > 3
        let db = db();
        let predicate = and(
            and(
                compare(ComparisonOp::Eq, joined(0), joined(2)),
                compare(ComparisonOp::Eq, joined(1), text("b")),
            ),
            compare(ComparisonOp::Gt, joined(2), int(3)),
        );
        let plan = filter_on(predicate, hash_join(&db, JoinType::Inner, false, None));
        let optimized = optimize(&db, &plan);
        let join = the_join(&optimized);
        assert_eq!(join.left_keys().len(), 1);
        assert!(join.predicate().is_none());
        assert!(scan_filter(join.left()).is_some());
        assert!(scan_filter(join.right()).is_some());
        assert_eq!(db.run(optimized.clone()).unwrap().len(), 1);
        assert_same_rows(&db, &plan, &optimized);
    }

    #[test]
    fn other_conditions_on_both_sides_go_in_the_join_predicate() {
        // FROM users a, users b WHERE a.id < b.id
        let db = db();
        let predicate = compare(ComparisonOp::Lt, joined(0), joined(2));
        let plan = filter_on(predicate, hash_join(&db, JoinType::Inner, false, None));
        let optimized = optimize(&db, &plan);
        let join = the_join(&optimized);
        assert!(join.left_keys().is_empty());
        assert!(join.predicate().is_some());
        assert_same_rows(&db, &plan, &optimized);
    }

    #[test]
    fn an_inner_joins_condition_on_one_side_moves_to_that_side() {
        // FROM users a JOIN users b ON a.id = b.id AND a.id > 2 AND b.name <> 'c'
        let db = db();
        let on = and(
            compare(ComparisonOp::Gt, column(0), int(2)),
            compare(ComparisonOp::NotEq, right(1), text("c")),
        );
        let plan: AbstractPlanNode = hash_join(&db, JoinType::Inner, true, Some(on)).into();
        let optimized = optimize(&db, &plan);
        let join = the_join(&optimized);
        assert!(join.predicate().is_none());
        assert!(scan_filter(join.left()).is_some());
        assert!(scan_filter(join.right()).is_some());
        assert_same_rows(&db, &plan, &optimized);
    }

    #[test]
    fn a_left_join_keeps_its_unmatched_rows() {
        // FROM users a LEFT JOIN users b ON a.id = b.id AND a.name = 'b'
        //     AND b.id > 3
        // WHERE a.id < 6 AND b.name IS NULL
        let db = db();
        let on = and(
            compare(ComparisonOp::Eq, column(1), text("b")),
            compare(ComparisonOp::Gt, right(0), int(3)),
        );
        let condition = and(
            compare(ComparisonOp::Lt, joined(0), int(6)),
            IsNullExpr::new(joined(3), false).into(),
        );
        let plan = filter_on(condition, hash_join(&db, JoinType::Left, true, Some(on)));
        let optimized = optimize(&db, &plan);
        // b.name IS NULL stays above, and a.name = 'b' in the join.
        let AbstractPlanNode::FilterPlan(above) = &optimized else {
            panic!("expected a filter, got {optimized:?}");
        };
        let join = the_join(above.child());
        assert!(join.predicate().is_some());
        assert!(scan_filter(join.left()).is_some());
        assert!(scan_filter(join.right()).is_some());
        // Users 1, 2, 3 and 5, unmatched.
        assert_eq!(db.run(optimized.clone()).unwrap().len(), 4);
        assert_same_rows(&db, &plan, &optimized);
    }

    #[test]
    fn a_condition_on_the_groups_goes_below_the_aggregation() {
        // SELECT name, COUNT(*) FROM users GROUP BY name
        // HAVING name <> 'a' AND COUNT(*) > 1
        let db = db();
        let aggregation =
            AggregationPlan::new(vec![column(1)], vec![Aggregate::CountStar], db.scan(None));
        let one = constant(Value::BigInt(1), DataType::BigInt);
        let having = and(
            compare(ComparisonOp::NotEq, output(&aggregation, 0), text("a")),
            compare(ComparisonOp::Gt, output(&aggregation, 1), one),
        );
        let plan = filter_on(having, aggregation);
        let optimized = optimize(&db, &plan);
        let AbstractPlanNode::FilterPlan(above) = &optimized else {
            panic!("expected a filter, got {optimized:?}");
        };
        let AbstractPlanNode::AggregationPlan(aggregation) = above.child().as_ref() else {
            panic!("expected an aggregation, got {:?}", above.child());
        };
        assert!(scan_filter(aggregation.child()).is_some());
        assert_same_rows(&db, &plan, &optimized);
    }

    #[test]
    fn stays_above_a_limit() {
        let db = db();
        // Limit(Filter(SeqScan)) under a filter.
        let not_b = Arc::unwrap_or_clone(filter(
            &db,
            compare(ComparisonOp::NotEq, column(1), text("b")),
        ));
        let limit = LimitPlan::new(2, Arc::new(not_b));
        let plan = filter_on(compare(ComparisonOp::Gt, column(0), int(1)), limit);
        let optimized = optimize(&db, &plan);
        let AbstractPlanNode::FilterPlan(above) = &optimized else {
            panic!("expected a filter, got {optimized:?}");
        };
        let AbstractPlanNode::LimitPlan(limit) = above.child().as_ref() else {
            panic!("expected a limit, got {:?}", above.child());
        };
        assert!(scan_filter(limit.child()).is_some());
        assert_same_rows(&db, &plan, &optimized);
    }
}
