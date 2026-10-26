use std::sync::Arc;

use catalog::Catalog;
use common::{Column, DataType, Schema};
use execution::expressions::{AbstractExpr, Expr};
use execution::plans::{
    AbstractPlanNode, Aggregate, HashJoinPlan, JoinType, PlanNode, WindowColumn, WindowFunctionKind,
};
use storage::buffer::buffer_pool_manager::BufferPoolManager;
use storage::disk::disk_manager::DiskManager;
use tempfile::TempDir;

use planner::bound::BoundStatement;
use planner::error::{PlannerError, Result};
use planner::{Binder, Planner};

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
                &[],
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
                &[],
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
        AbstractPlanNode::CreateTablePlan(_) => "CreateTable",
        AbstractPlanNode::CreateIndexPlan(_) => "CreateIndex",
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
        AbstractPlanNode::ExplainPlan(_) => "Explain",
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

/// The hash join `plan` is, or the one under `plan`'s filter.
fn hash_join(plan: Arc<AbstractPlanNode>) -> HashJoinPlan {
    match &*plan {
        AbstractPlanNode::HashJoinPlan(join) => join.clone(),
        AbstractPlanNode::FilterPlan(_) => hash_join(plan.get_child_at(0)),
        other => panic!("expected a hash join, got {other:?}"),
    }
}

/// Each key reads its own side's row on its own, so the right key's column
/// is counted from the start of the right row.
#[test]
fn join_condition_becomes_hash_keys() {
    let db = Db::new();
    let plan = db.plan("SELECT o.total FROM users u LEFT JOIN orders o ON u.id = o.user_id");
    assert_eq!(
        shape(&plan),
        "Projection(HashJoin(SeqScan users, SeqScan orders))"
    );
    let join = hash_join(plan.get_child_at(0));
    assert_eq!(join.join_type(), JoinType::Left);
    assert_eq!(join.left_keys().len(), 1);
    assert_eq!(column_ref(&join.left_keys()[0]), (0, 0));
    assert_eq!(column_ref(&join.right_keys()[0]), (0, 1));
    assert!(join.predicate().is_none());

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
    let outer = hash_join(plan.get_child_at(0));
    assert_eq!(column_ref(&outer.left_keys()[0]), (0, 4));
    assert_eq!(column_ref(&outer.right_keys()[0]), (0, 0));
    assert_eq!(plan.get_output_schema().len(), 11);
}

/// Equalities ANDed together are keys whichever way round they are
/// written; the rest of the condition is the predicate, reading both rows.
#[test]
fn join_keys_and_the_rest_of_the_condition() {
    let db = Db::new();
    let plan = db.plan(
        "SELECT * FROM users u JOIN orders o \
         ON o.user_id = u.id AND o.total > 10 AND u.score = o.total + 1",
    );
    let join = hash_join(plan.get_child_at(0));
    let left_keys: Vec<_> = join.left_keys().iter().map(column_ref).collect();
    assert_eq!(left_keys, [(0, 0), (0, 2)]);
    assert_eq!(column_ref(&join.right_keys()[0]), (0, 1));
    let AbstractExpr::ArithmeticExpr(total_plus_one) = &join.right_keys()[1] else {
        panic!("expected o.total + 1, got {:?}", join.right_keys()[1]);
    };
    assert_eq!(column_ref(&total_plus_one.children()[0]), (0, 2));

    let total_over_10 = join.predicate().unwrap().children();
    assert_eq!(column_ref(&total_over_10[0]), (1, 2));
}

/// Without an equality between the two sides there are no keys: `OR`,
/// comparisons other than `=`, and `=` on one side only.
#[test]
fn join_without_keys() {
    let db = Db::new();
    for on in [
        "u.id = o.user_id OR u.id = o.id",
        "u.id < o.user_id",
        "u.id = 1 AND o.id = o.user_id",
    ] {
        let plan = db.plan(&format!("SELECT * FROM users u JOIN orders o ON {on}"));
        let join = hash_join(plan.get_child_at(0));
        assert!(join.left_keys().is_empty(), "{on}");
        assert!(join.predicate().is_some(), "{on}");
    }
}

#[test]
fn cross_join_is_an_inner_join_without_a_condition() {
    let db = Db::new();
    let plan = db.plan("SELECT * FROM users, orders WHERE users.id = orders.user_id");
    assert_eq!(
        shape(&plan),
        "Projection(Filter(HashJoin(SeqScan users, SeqScan orders)))"
    );
    let join = hash_join(plan.get_child_at(0));
    assert_eq!(join.join_type(), JoinType::Inner);
    assert!(join.left_keys().is_empty());
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
fn explain_sits_above_the_plan() {
    let db = Db::new();
    let plan = db.plan(
        "EXPLAIN SELECT u.name, sum(o.total) FROM users u JOIN orders o ON u.id = o.user_id \
         WHERE o.total > 10 GROUP BY u.name ORDER BY 2 DESC LIMIT 3",
    );
    assert_eq!(
        shape(&plan),
        "Explain(Projection(Limit(Sort(Aggregation(Filter(HashJoin(SeqScan users, SeqScan orders)))))))"
    );
    assert_eq!(names(plan.get_output_schema()), ["QUERY PLAN"]);
    assert_eq!(
        plan.get_child_at(0).to_string(),
        "Projection
  Output: name, sum
  ->  Limit 3
        ->  Sort
              Key: sum DESC
              ->  Aggregation
                    Group By: name
                    Aggregates: sum(total)
                    ->  Filter
                          Condition: total > 10
                          ->  HashJoin (inner)
                                Keys: left.id = right.user_id
                                ->  SeqScan on users
                                ->  SeqScan on orders"
    );
}

#[test]
fn explain_dml() {
    let db = Db::new();
    assert_eq!(
        db.plan("INSERT INTO orders VALUES (1, 2, 3)").to_string(),
        "Insert on orders\n  ->  Values (1 row)"
    );
    assert_eq!(
        db.plan("UPDATE users SET name = 'x', score = score + 1 WHERE id = 1")
            .to_string(),
        "Update on users
  Set: name = 'x', score = score + 1
  ->  Filter
        Condition: id = 1
        ->  SeqScan on users"
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
fn create_table() {
    let db = Db::new();
    let plan = db.plan("CREATE TABLE t (a INT NOT NULL, b VARCHAR(8))");
    let AbstractPlanNode::CreateTablePlan(create) = &*plan else {
        panic!("{plan:?}");
    };
    assert_eq!(create.table_name(), "t");
    assert_eq!(names(create.table_schema()), ["a", "b"]);
    assert!(create.primary_key().is_empty());
    assert!(create.get_output_schema().is_empty());

    let plan = db.plan("CREATE TABLE t (a INT, b INT, PRIMARY KEY (b, a))");
    let AbstractPlanNode::CreateTablePlan(create) = &*plan else {
        panic!("{plan:?}");
    };
    assert_eq!(create.primary_key(), ["b", "a"]);
}

#[test]
fn create_index() {
    let db = Db::new();
    let plan = db.plan("CREATE INDEX users_name ON users (name, id)");
    let AbstractPlanNode::CreateIndexPlan(create) = &*plan else {
        panic!("{plan:?}");
    };
    assert_eq!(create.index_name(), "users_name");
    assert_eq!(create.table_name(), "users");
    assert_eq!(create.key_columns(), ["name", "id"]);
    assert!(create.get_output_schema().is_empty());
}

#[test]
fn aggregation_sits_between_where_and_having() {
    let db = Db::new();
    let plan = db.plan(
        "SELECT user_id, count(*) AS n FROM orders WHERE total > 0 \
         GROUP BY user_id HAVING sum(total) > 10 ORDER BY n DESC LIMIT 5",
    );
    assert_eq!(
        shape(&plan),
        "Projection(Limit(Sort(Filter(Aggregation(Filter(SeqScan orders))))))"
    );
    assert_eq!(names(plan.get_output_schema()), ["user_id", "n"]);

    let aggregation = plan
        .get_child_at(0)
        .get_child_at(0)
        .get_child_at(0)
        .get_child_at(0);
    let AbstractPlanNode::AggregationPlan(aggregation) = &*aggregation else {
        panic!("expected an aggregation");
    };
    assert_eq!(column_ref(&aggregation.group_bys()[0]), (0, 1));
    assert!(matches!(aggregation.aggregates()[0], Aggregate::CountStar));
    assert!(matches!(aggregation.aggregates()[1], Aggregate::Sum(_)));
    // Group key, count(*), sum(total).
    assert_eq!(aggregation.get_output_schema().len(), 3);

    // Above it, columns index the aggregation row.
    let AbstractPlanNode::ProjectionPlan(projection) = &*plan else {
        panic!("expected a projection");
    };
    assert_eq!(column_ref(&projection.exprs()[1]), (0, 1));
}

#[test]
fn aggregate_without_group_by_still_aggregates() {
    let db = Db::new();
    assert_eq!(
        shape(&db.plan("SELECT count(*) FROM users")),
        "Projection(Aggregation(SeqScan users))"
    );
}

#[test]
fn window_appends_its_results_to_the_row() {
    let db = Db::new();
    let plan = db
        .plan("SELECT name, rank() OVER (PARTITION BY active ORDER BY score DESC) AS r FROM users");
    assert_eq!(shape(&plan), "Projection(Window(SeqScan users))");
    let AbstractPlanNode::WindowPlan(window) = &*plan.get_child_at(0) else {
        panic!("expected a window");
    };
    assert_eq!(
        names(window.get_output_schema()),
        ["id", "name", "score", "active", "rank"]
    );
    let WindowColumn::Window(rank) = &window.columns()[4] else {
        panic!("expected a window function");
    };
    assert!(matches!(rank.function, WindowFunctionKind::Rank));
    assert_eq!(column_ref(&rank.partition_by[0]), (0, 3));
    assert!(rank.order_by[0].desc);

    let AbstractPlanNode::ProjectionPlan(projection) = &*plan else {
        panic!("expected a projection");
    };
    assert_eq!(column_ref(&projection.exprs()[1]), (0, 4));
}

#[test]
fn window_over_aggregation() {
    let db = Db::new();
    let plan = db
        .plan("SELECT user_id, sum(count(*)) OVER (ORDER BY user_id) FROM orders GROUP BY user_id");
    assert_eq!(
        shape(&plan),
        "Projection(Window(Aggregation(SeqScan orders)))"
    );
    let AbstractPlanNode::WindowPlan(window) = &*plan.get_child_at(0) else {
        panic!("expected a window");
    };
    let WindowColumn::Window(sum) = &window.columns()[2] else {
        panic!("expected a window function");
    };
    // sum over the aggregation row's count(*).
    let WindowFunctionKind::Aggregate(Aggregate::Sum(arg)) = &sum.function else {
        panic!("expected sum");
    };
    assert_eq!(column_ref(arg), (0, 1));
}

#[test]
fn subquery_in_from_is_planned_inline() {
    let db = Db::new();
    let plan = db.plan(
        "SELECT t.n FROM (SELECT user_id AS n FROM orders WHERE total > 1) AS t, users \
         WHERE t.n = users.id",
    );
    assert_eq!(
        shape(&plan),
        "Projection(Filter(HashJoin(Projection(Filter(SeqScan orders)), SeqScan users)))"
    );
    let join = hash_join(plan.get_child_at(0));
    assert_eq!(names(join.get_output_schema())[..2], ["n", "id"]);
}

#[test]
fn subqueries_in_expressions_are_not_planned_yet() {
    let db = Db::new();
    for sql in [
        "SELECT (SELECT 1)",
        "SELECT * FROM users WHERE EXISTS (SELECT * FROM orders)",
        "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)",
    ] {
        assert!(
            matches!(db.try_plan(sql), Err(PlannerError::Unsupported(_))),
            "{sql}"
        );
    }
}
