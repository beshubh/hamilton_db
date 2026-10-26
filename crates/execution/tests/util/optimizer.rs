//! A users table with indexes, and plans for the optimizer rule tests.

use std::sync::Arc;

use common::{DataType, Schema, Value};

use crate::executors::test_util::{Db, column, constant, users_schema};
use crate::expressions::{AbstractExpr, ColumnValueExpr, Expr, LogicExpr, LogicOp};
use crate::plans::{
    AbstractPlanNode, FilterPlan, HashJoinPlan, IndexScanPlan, JoinType, NestedLoopJoinPlan,
    OrderBy, ProjectionPlan, SortPlan,
};

/// Users 1 to 6, two of them without a name, with index `name` on the
/// columns `key`.
pub fn db_with_index(name: &str, key: &[&str]) -> Db {
    db_with_indexes(&[(name, key)])
}

pub fn db_with_indexes(indexes: &[(&str, &[&str])]) -> Db {
    let db = Db::new();
    // Created before the rows are inserted, so that the rows are in them.
    for (name, key) in indexes {
        db.catalog().create_index(name, "users", key).unwrap();
    }
    db.insert(&[
        (4, Some("b")),
        (1, Some("a")),
        (6, None),
        (3, Some("b")),
        (2, Some("c")),
        (5, None),
    ]);
    db
}

pub fn text(s: &str) -> AbstractExpr {
    constant(Value::from(s), DataType::Varchar(8))
}

pub fn and(left: AbstractExpr, right: AbstractExpr) -> AbstractExpr {
    LogicExpr::new(LogicOp::And, left, right).unwrap().into()
}

/// `Filter(SeqScan)` of the users table.
pub fn filter(db: &Db, predicate: AbstractExpr) -> Arc<AbstractPlanNode> {
    Arc::new(FilterPlan::new(predicate, db.scan(None)).into())
}

/// A sort on users columns, each `(column, desc)`.
pub fn sort(child: Arc<AbstractPlanNode>, keys: &[(usize, bool)]) -> AbstractPlanNode {
    let order_bys = keys
        .iter()
        .map(|&(idx, desc)| OrderBy {
            expr: column(idx),
            desc,
        })
        .collect();
    SortPlan::new(order_bys, child).into()
}

/// Column `idx` of a join of users with users: `id, name, id, name`.
pub fn joined(idx: usize) -> AbstractExpr {
    let column = users_schema().column(idx % 2).unwrap().clone();
    ColumnValueExpr::new(0, idx, column).into()
}

/// Column `idx` of the right users row of a pair.
pub fn right(idx: usize) -> AbstractExpr {
    let column = users_schema().column(idx).unwrap().clone();
    ColumnValueExpr::new(1, idx, column).into()
}

/// A hash join of users with users on `left.id = right.id`, or with no keys
/// if `on_id` is false.
pub fn hash_join(
    db: &Db,
    join_type: JoinType,
    on_id: bool,
    predicate: Option<AbstractExpr>,
) -> HashJoinPlan {
    let keys = || if on_id { vec![column(0)] } else { vec![] };
    HashJoinPlan::new(
        join_type,
        db.scan(None),
        db.scan(None),
        keys(),
        keys(),
        predicate,
    )
}

/// A nested loop join of users with users on `predicate`.
pub fn nested_loop_join(
    db: &Db,
    join_type: JoinType,
    predicate: Option<AbstractExpr>,
) -> AbstractPlanNode {
    NestedLoopJoinPlan::new(join_type, db.scan(None), db.scan(None), predicate).into()
}

/// A projection computing `exprs`, named after their return types.
pub fn project(exprs: Vec<AbstractExpr>, child: Arc<AbstractPlanNode>) -> AbstractPlanNode {
    let columns = exprs
        .iter()
        .map(|expr| expr.return_type().clone())
        .collect();
    ProjectionPlan::new(Schema::new(columns), exprs, child).into()
}

pub fn filter_on(predicate: AbstractExpr, child: impl Into<AbstractPlanNode>) -> AbstractPlanNode {
    FilterPlan::new(predicate, Arc::new(child.into())).into()
}

pub fn index_scan(plan: &AbstractPlanNode) -> &IndexScanPlan {
    match plan {
        AbstractPlanNode::IndexScanPlan(scan) => scan,
        _ => panic!("expected an index scan, got {plan:?}"),
    }
}

/// Checks that `optimized` returns the rows `plan` does: in the same order
/// if `plan` sorts, else in any order.
pub fn assert_same_rows(db: &Db, plan: &AbstractPlanNode, optimized: &AbstractPlanNode) {
    let (mut expected, mut actual) = (
        db.run(plan.clone()).unwrap(),
        db.run(optimized.clone()).unwrap(),
    );
    if !matches!(plan, AbstractPlanNode::SortPlan(_)) {
        let key = |row: &Vec<Value>| format!("{row:?}");
        expected.sort_by_key(key);
        actual.sort_by_key(key);
    }
    assert_eq!(actual, expected);
}
