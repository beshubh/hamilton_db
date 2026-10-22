use std::sync::Arc;

use catalog::Catalog;
use common::{Column, DataType, Schema};
use execution::expressions::{AbstractExpr, Expr};
use execution::plans::{AbstractPlanNode, JoinType, PlanNode};
use storage::buffer::buffer_pool_manager::BufferPoolManager;
use storage::disk::disk_manager::DiskManager;
use tempfile::TempDir;

use super::Planner;
use crate::Binder;
use crate::bound::BoundStatement;
use crate::error::{PlannerError, Result};

/// The tables of the binder tests:
///
/// - `users (id INT NOT NULL, name VARCHAR(32), score BIGINT NOT NULL, active BOOL)`
/// - `orders (id INT NOT NULL, user_id INT, total BIGINT)`
struct Db {
    _dir: TempDir,
    catalog: Catalog,
}

impl Db {
    fn new() -> Self {
        let dir = TempDir::new().unwrap();
        let dm = Arc::new(DiskManager::new(dir.path().join("test.db")).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(16, dm));
        let catalog = Catalog::open(bpm).unwrap();
        catalog
            .create_table(
                "users",
                Schema::new(vec![
                    Column::new("id", DataType::Int, false),
                    Column::new("name", DataType::Varchar(32), true),
                    Column::new("score", DataType::BigInt, false),
                    Column::new("active", DataType::Bool, true),
                ]),
            )
            .unwrap();
        catalog
            .create_table(
                "orders",
                Schema::new(vec![
                    Column::new("id", DataType::Int, false),
                    Column::new("user_id", DataType::Int, true),
                    Column::new("total", DataType::BigInt, true),
                ]),
            )
            .unwrap();
        Self { _dir: dir, catalog }
    }

    fn bind(&self, sql: &str) -> BoundStatement {
        let mut stmts = parser::parse(sql).unwrap();
        assert_eq!(stmts.len(), 1, "{sql}");
        Binder::new(&self.catalog).bind(&stmts.remove(0)).unwrap()
    }

    fn try_plan(&self, sql: &str) -> Result<Arc<AbstractPlanNode>> {
        Planner::new(&self.catalog).plan(&self.bind(sql))
    }

    fn plan(&self, sql: &str) -> Arc<AbstractPlanNode> {
        self.try_plan(sql).unwrap()
    }
}

/// The plan tree as `Name(child, ...)`.
fn shape(plan: &AbstractPlanNode) -> String {
    let name = match plan {
        AbstractPlanNode::SeqScanPlan(scan) => return format!("SeqScan {}", scan.table_name()),
        AbstractPlanNode::IndexScanPlan(_) => "IndexScan",
        AbstractPlanNode::ValuesPlan(_) => "Values",
        AbstractPlanNode::InsertPlan(_) => "Insert",
        AbstractPlanNode::UpdatePlan(_) => "Update",
        AbstractPlanNode::DeletePlan(_) => "Delete",
        AbstractPlanNode::FilterPlan(_) => "Filter",
        AbstractPlanNode::ProjectionPlan(_) => "Projection",
        AbstractPlanNode::NestedLoopJoinPlan(_) => "NestedLoopJoin",
        AbstractPlanNode::HashJoinPlan(_) => "HashJoin",
        AbstractPlanNode::NestedIndexJoinPlan(_) => "NestedIndexJoin",
        AbstractPlanNode::AggregationPlan(_) => "Aggregation",
        AbstractPlanNode::WindowPlan(_) => "Window",
        AbstractPlanNode::SortPlan(_) => "Sort",
        AbstractPlanNode::LimitPlan(_) => "Limit",
        AbstractPlanNode::TopNPlan(_) => "TopN",
        AbstractPlanNode::TopNPerGroupPlan(_) => "TopNPerGroup",
    };
    let children: Vec<_> = plan.get_children().iter().map(|c| shape(c)).collect();
    format!("{name}({})", children.join(", "))
}

/// `(tuple_idx, col_idx)` of a column expression.
fn column_ref(expr: &AbstractExpr) -> (usize, usize) {
    match expr {
        AbstractExpr::ColumnValueExpr(column) => (column.tuple_idx(), column.col_idx()),
        other => panic!("expected a column, got {other:?}"),
    }
}

fn names(schema: &Schema) -> Vec<&str> {
    schema.columns().iter().map(|c| c.name.as_str()).collect()
}

#[test]
fn select_stacks_filter_sort_limit_and_projection() {
    let db = Db::new();
    let plan = db.plan("SELECT name AS n FROM users WHERE id = 1 ORDER BY score DESC LIMIT 2");
    assert_eq!(
        shape(&plan),
        "Projection(Limit(Sort(Filter(SeqScan users))))"
    );
    assert_eq!(names(plan.get_output_schema()), ["n"]);

    // ORDER BY reads `score`, which the select list leaves out.
    let AbstractPlanNode::SortPlan(sort) = &*plan.get_child_at(0).get_child_at(0) else {
        panic!("expected a sort");
    };
    assert_eq!(column_ref(&sort.order_bys()[0].expr), (0, 2));
    assert!(sort.order_bys()[0].desc);
}

#[test]
fn select_without_from_projects_one_empty_row() {
    let db = Db::new();
    let plan = db.plan("SELECT 1 + 2 AS three");
    assert_eq!(shape(&plan), "Projection(Values())");
    let AbstractPlanNode::ValuesPlan(values) = &*plan.get_child_at(0) else {
        panic!("expected values");
    };
    assert_eq!(values.rows().len(), 1);
    assert!(values.rows()[0].is_empty());
}

#[test]
fn join_condition_reads_each_side() {
    let db = Db::new();
    let plan = db.plan("SELECT o.total FROM users u LEFT JOIN orders o ON u.id = o.user_id");
    assert_eq!(
        shape(&plan),
        "Projection(NestedLoopJoin(SeqScan users, SeqScan orders))"
    );
    let AbstractPlanNode::NestedLoopJoinPlan(join) = &*plan.get_child_at(0) else {
        panic!("expected a join");
    };
    assert_eq!(join.join_type(), JoinType::Left);
    let on = join.predicate().unwrap().children();
    assert_eq!(column_ref(&on[0]), (0, 0));
    assert_eq!(column_ref(&on[1]), (1, 1));

    // Above the join, columns index the whole FROM row; a LEFT JOIN's right
    // side is nullable.
    let AbstractPlanNode::ProjectionPlan(projection) = &*plan else {
        panic!("expected a projection");
    };
    assert_eq!(column_ref(&projection.exprs()[0]), (0, 6));
    assert!(plan.get_output_schema().column(0).unwrap().nullable);
}

#[test]
fn nested_join_splits_on_its_own_left_side() {
    let db = Db::new();
    // The second ON sees `users u JOIN orders o` (7 columns) on its left.
    let plan = db.plan(
        "SELECT * FROM users u JOIN orders o ON u.id = o.user_id \
         JOIN users v ON o.id = v.id",
    );
    let AbstractPlanNode::NestedLoopJoinPlan(outer) = &*plan.get_child_at(0) else {
        panic!("expected a join");
    };
    let on = outer.predicate().unwrap().children();
    assert_eq!(column_ref(&on[0]), (0, 4));
    assert_eq!(column_ref(&on[1]), (1, 0));
    assert_eq!(plan.get_output_schema().len(), 11);
}

#[test]
fn cross_join_is_an_inner_join_without_a_condition() {
    let db = Db::new();
    let plan = db.plan("SELECT * FROM users, orders WHERE users.id = orders.user_id");
    assert_eq!(
        shape(&plan),
        "Projection(Filter(NestedLoopJoin(SeqScan users, SeqScan orders)))"
    );
    let AbstractPlanNode::NestedLoopJoinPlan(join) = &*plan.get_child_at(0).get_child_at(0) else {
        panic!("expected a join");
    };
    assert_eq!(join.join_type(), JoinType::Inner);
    assert!(join.predicate().is_none());
}

#[test]
fn insert_values_in_table_order() {
    let db = Db::new();
    let plan = db.plan("INSERT INTO orders (total, id) VALUES (10, 1), (20, 2)");
    assert_eq!(shape(&plan), "Insert(Values())");
    let AbstractPlanNode::ValuesPlan(values) = &*plan.get_child_at(0) else {
        panic!("expected values");
    };
    assert_eq!(values.rows().len(), 2);
    let row: Vec<_> = values.rows()[0]
        .iter()
        .map(|e| e.return_type().data_type)
        .collect();
    // id, then the omitted user_id as a typed NULL, then total.
    assert_eq!(row, [DataType::Int, DataType::Int, DataType::BigInt]);
    assert_eq!(names(plan.get_output_schema()), ["count"]);
}

#[test]
fn update_keeps_unassigned_columns() {
    let db = Db::new();
    let plan = db.plan("UPDATE users SET score = id, name = NULL WHERE active");
    assert_eq!(shape(&plan), "Update(Filter(SeqScan users))");
    let AbstractPlanNode::UpdatePlan(update) = &*plan else {
        panic!("expected an update");
    };
    let targets = update.target_exprs();
    assert_eq!(targets.len(), 4);
    assert_eq!(column_ref(&targets[0]), (0, 0));
    assert!(matches!(targets[1], AbstractExpr::ConstantExpr(_)));
    // INT stored into BIGINT.
    assert!(matches!(targets[2], AbstractExpr::CastExpr(_)));
    assert_eq!(targets[2].return_type().data_type, DataType::BigInt);
    assert_eq!(column_ref(&targets[3]), (0, 3));
}

#[test]
fn delete_without_where_scans_everything() {
    let db = Db::new();
    assert_eq!(
        shape(&db.plan("DELETE FROM users")),
        "Delete(SeqScan users)"
    );
    assert_eq!(
        shape(&db.plan("DELETE FROM users WHERE id > 3")),
        "Delete(Filter(SeqScan users))"
    );
}

#[test]
fn expressions_keep_the_bound_types() {
    let db = Db::new();
    let sql = "SELECT -id, NOT active, name IS NOT NULL, upper(name), ARRAY[1, NULL], \
               id + 1 > score OR active, score % 2, name <> 'x' FROM users";
    let BoundStatement::Select(select) = db.bind(sql) else {
        panic!("expected a select");
    };
    let plan = Planner::new(&db.catalog)
        .plan(&BoundStatement::Select(select.clone()))
        .unwrap();
    let AbstractPlanNode::ProjectionPlan(projection) = &*plan else {
        panic!("expected a projection");
    };
    for (item, expr) in select.projection.iter().zip(projection.exprs()) {
        let (bound, planned) = (&item.expr.return_type, expr.return_type());
        assert_eq!(bound.data_type, planned.data_type, "{}", item.name);
        assert_eq!(bound.nullable, planned.nullable, "{}", item.name);
    }
}

#[test]
fn create_table_has_no_plan() {
    let db = Db::new();
    let err = db.try_plan("CREATE TABLE t (a INT)").unwrap_err();
    assert_eq!(err, PlannerError::NoPlan("CREATE TABLE"));
}
