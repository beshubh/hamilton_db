//! Nested loop join -> hash join when a conjunct is `left = right`.
//! Those conjuncts become keys, the rest the predicate. Taken whenever a key exists.

use std::sync::Arc;

use super::expr::{conjunction, conjuncts, hash_keys};
use crate::optimizer::Optimizer;
use crate::plans::{AbstractPlanNode, HashJoinPlan, NestedLoopJoinPlan, PlanNode};

impl<'a> Optimizer<'a> {
    pub(crate) fn optimize_nlj_as_hash_join(&self, plan: &AbstractPlanNode) -> AbstractPlanNode {
        let children = plan
            .get_children()
            .iter()
            .map(|child| Arc::new(self.optimize_nlj_as_hash_join(child)))
            .collect();
        let plan = plan.clone_with_children(children);
        match &plan {
            AbstractPlanNode::NestedLoopJoinPlan(join) => match as_hash_join(join) {
                Some(hash_join) => hash_join.into(),
                None => plan,
            },
            _ => plan,
        }
    }
}

fn as_hash_join(join: &NestedLoopJoinPlan) -> Option<HashJoinPlan> {
    let (mut left_keys, mut right_keys, mut rest) = (Vec::new(), Vec::new(), Vec::new());
    for conjunct in join.predicate().into_iter().flat_map(conjuncts) {
        match hash_keys(conjunct) {
            Some((left_key, right_key)) => {
                left_keys.push(left_key);
                right_keys.push(right_key);
            }
            None => rest.push(conjunct.clone()),
        }
    }
    if left_keys.is_empty() {
        return None;
    }
    Some(HashJoinPlan::new(
        join.join_type(),
        Arc::clone(join.left()),
        Arc::clone(join.right()),
        left_keys,
        right_keys,
        conjunction(rest),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executors::test_util::{Db, column, compare, int};
    use crate::expressions::{AbstractExpr, ArithmeticExpr, ArithmeticOp, ComparisonOp};
    use crate::optimizer::test_util::{
        and, assert_same_rows, db_with_indexes, filter_on, nested_loop_join, right, text,
    };
    use crate::plans::JoinType;

    fn db() -> Db {
        db_with_indexes(&[])
    }

    fn optimize(db: &Db, plan: &AbstractPlanNode) -> AbstractPlanNode {
        Optimizer::new(db.catalog()).optimize_nlj_as_hash_join(plan)
    }

    fn the_hash_join(plan: &AbstractPlanNode) -> &HashJoinPlan {
        match plan {
            AbstractPlanNode::HashJoinPlan(join) => join,
            _ => panic!("expected a hash join, got {plan:?}"),
        }
    }

    #[test]
    fn an_equality_between_the_sides_becomes_a_key() {
        // a.id = b.id, and b.id = a.id
        let db = db();
        for predicate in [
            compare(ComparisonOp::Eq, column(0), right(0)),
            compare(ComparisonOp::Eq, right(0), column(0)),
        ] {
            let plan = nested_loop_join(&db, JoinType::Inner, Some(predicate));
            let optimized = optimize(&db, &plan);
            let join = the_hash_join(&optimized);
            assert_eq!(join.left_keys().len(), 1);
            assert!(join.predicate().is_none());
            assert_eq!(db.run(optimized.clone()).unwrap().len(), 6);
            assert_same_rows(&db, &plan, &optimized);
        }
    }

    #[test]
    fn the_rest_of_the_condition_stays_the_predicate() {
        // a.name = b.name AND a.id < b.id AND a.id + 1 = b.id
        let db = db();
        let next = ArithmeticExpr::new(ArithmeticOp::Add, column(0), int(1)).unwrap();
        let predicate = and(
            and(
                compare(ComparisonOp::Eq, column(1), right(1)),
                compare(ComparisonOp::Lt, column(0), right(0)),
            ),
            compare(ComparisonOp::Eq, next.into(), right(0)),
        );
        let plan = nested_loop_join(&db, JoinType::Inner, Some(predicate));
        let optimized = optimize(&db, &plan);
        let join = the_hash_join(&optimized);
        assert_eq!(join.left_keys().len(), 2);
        assert!(join.predicate().is_some());
        assert_same_rows(&db, &plan, &optimized);
    }

    #[test]
    fn a_left_join_keeps_its_unmatched_rows() {
        // a LEFT JOIN b ON a.id = b.id AND b.name <> 'b'
        let db = db();
        let predicate = and(
            compare(ComparisonOp::Eq, column(0), right(0)),
            compare(ComparisonOp::NotEq, right(1), text("b")),
        );
        let plan = nested_loop_join(&db, JoinType::Left, Some(predicate));
        let optimized = optimize(&db, &plan);
        let join = the_hash_join(&optimized);
        assert_eq!(join.join_type(), JoinType::Left);
        assert!(join.predicate().is_some());
        assert_eq!(db.run(optimized.clone()).unwrap().len(), 6);
        assert_same_rows(&db, &plan, &optimized);
    }

    #[test]
    fn a_null_key_matches_nothing() {
        // a.name = b.name: the two users without a name match no one.
        let db = db();
        let predicate = compare(ComparisonOp::Eq, column(1), right(1));
        let plan = nested_loop_join(&db, JoinType::Inner, Some(predicate));
        let optimized = optimize(&db, &plan);
        the_hash_join(&optimized);
        assert_same_rows(&db, &plan, &optimized);
    }

    #[test]
    fn keeps_joins_without_an_equality_between_the_sides() {
        let db = db();
        let both_sides = ArithmeticExpr::new(ArithmeticOp::Add, column(0), right(0)).unwrap();
        let predicates: [Option<AbstractExpr>; 4] = [
            None,
            Some(compare(ComparisonOp::Lt, column(0), right(0))),
            // Each reads one side only.
            Some(and(
                compare(ComparisonOp::Eq, column(0), int(1)),
                compare(ComparisonOp::Eq, right(0), int(2)),
            )),
            Some(compare(ComparisonOp::Eq, both_sides.into(), int(4))),
        ];
        for predicate in predicates {
            let plan = nested_loop_join(&db, JoinType::Inner, predicate);
            assert!(matches!(
                optimize(&db, &plan),
                AbstractPlanNode::NestedLoopJoinPlan(_)
            ));
        }
    }

    #[test]
    fn replaces_joins_below_other_plans() {
        let db = db();
        let predicate = compare(ComparisonOp::Eq, column(0), right(0));
        let join = nested_loop_join(&db, JoinType::Inner, Some(predicate));
        let plan = filter_on(compare(ComparisonOp::Gt, column(0), int(2)), join);
        let optimized = optimize(&db, &plan);
        let AbstractPlanNode::FilterPlan(filter) = &optimized else {
            panic!("expected a filter, got {optimized:?}");
        };
        the_hash_join(filter.child());
        assert_same_rows(&db, &plan, &optimized);
    }
}
