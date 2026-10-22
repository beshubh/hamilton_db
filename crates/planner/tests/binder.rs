use std::sync::Arc;

use catalog::Catalog;
use common::{Column, DataType, Schema, Value, ValueError};
use parser::{BinaryOp, JoinKind, Statement, UnaryOp};
use storage::buffer::buffer_pool_manager::BufferPoolManager;
use storage::disk::disk_manager::DiskManager;
use tempfile::TempDir;

use planner::Binder;
use planner::bound::*;
use planner::error::{PlannerError, Result};

/// A catalog with two tables:
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

    fn bind(&self, sql: &str) -> Result<BoundStatement> {
        let mut stmts = parser::parse(sql).unwrap();
        assert_eq!(stmts.len(), 1, "{sql}");
        Binder::new(&self.catalog).bind(&stmts.remove(0))
    }

    fn select(&self, sql: &str) -> BoundSelect {
        match self.bind(sql).unwrap() {
            BoundStatement::Select(select) => select,
            other => panic!("expected SELECT, got {other:?}"),
        }
    }

    /// The first select item of `SELECT <expr> FROM users`.
    fn expr(&self, expr: &str) -> Result<BoundExpr> {
        match self.bind(&format!("SELECT {expr} FROM users"))? {
            BoundStatement::Select(mut select) => Ok(select.projection.remove(0).expr),
            other => panic!("expected SELECT, got {other:?}"),
        }
    }

    fn err(&self, sql: &str) -> PlannerError {
        match self.bind(sql) {
            Err(err) => err,
            Ok(bound) => panic!("{sql}: expected an error, got {bound:?}"),
        }
    }
}

/// `(alias, idx)` of a column reference.
fn column_ref(expr: &BoundExpr) -> (&str, usize) {
    match &expr.kind {
        BoundExprKind::Column { table, idx } => (table, *idx),
        other => panic!("expected a column, got {other:?}"),
    }
}

fn constant_of(expr: &BoundExpr) -> &Value {
    match &expr.kind {
        BoundExprKind::Constant(value) => value,
        other => panic!("expected a constant, got {other:?}"),
    }
}

fn names(select: &BoundSelect) -> Vec<&str> {
    select.projection.iter().map(|p| p.name.as_str()).collect()
}

fn invalid_operands(op: &'static str, left: &'static str, right: &'static str) -> PlannerError {
    PlannerError::Value(ValueError::InvalidOperands { op, left, right })
}

#[test]
fn select_star_expands_every_column() {
    let db = Db::new();
    let select = db.select("SELECT * FROM users");
    assert_eq!(names(&select), ["id", "name", "score", "active"]);
    for (i, item) in select.projection.iter().enumerate() {
        assert_eq!(column_ref(&item.expr), ("users", i));
    }

    let users = db.catalog.get_table("users").unwrap();
    assert_eq!(select.output_schema(), users.schema);
    let Some(BoundFrom::Table(table)) = &select.from else {
        panic!("expected one table");
    };
    assert_eq!(table.oid, users.oid);
    assert_eq!(table.alias, "users");
}

#[test]
fn columns_resolve_across_tables() {
    let db = Db::new();
    let select = db.select("SELECT name, orders.id, total, users.* FROM users, orders");
    assert_eq!(column_ref(&select.projection[0].expr), ("users", 1));
    assert_eq!(column_ref(&select.projection[1].expr), ("orders", 4));
    assert_eq!(column_ref(&select.projection[2].expr), ("orders", 6));
    assert_eq!(names(&select)[3..], ["id", "name", "score", "active"]);
    assert_eq!(select.output_schema().len(), 7);
    assert!(matches!(
        select.from,
        Some(BoundFrom::Join {
            kind: JoinKind::Cross,
            on: None,
            ..
        })
    ));
}

#[test]
fn name_resolution_errors() {
    let db = Db::new();
    assert_eq!(
        db.err("SELECT * FROM missing"),
        PlannerError::TableNotFound("missing".into())
    );
    assert_eq!(
        db.err("SELECT nope FROM users"),
        PlannerError::ColumnNotFound("nope".into())
    );
    assert_eq!(
        db.err("SELECT users.total FROM users, orders"),
        PlannerError::ColumnNotFound("users.total".into())
    );
    assert_eq!(
        db.err("SELECT id FROM users, orders"),
        PlannerError::AmbiguousColumn("id".into())
    );
    assert_eq!(
        db.err("SELECT x.id FROM users"),
        PlannerError::UnknownTable("x".into())
    );
    assert_eq!(
        db.err("SELECT x.* FROM users"),
        PlannerError::UnknownTable("x".into())
    );
    assert_eq!(
        db.err("SELECT * FROM users, users"),
        PlannerError::DuplicateTable("users".into())
    );
}

#[test]
fn aliases_replace_table_names() {
    let db = Db::new();
    let select = db.select("SELECT a.id, b.id FROM users a, users AS b");
    assert_eq!(column_ref(&select.projection[0].expr), ("a", 0));
    assert_eq!(column_ref(&select.projection[1].expr), ("b", 4));

    // Once aliased, the table's own name is out of scope.
    assert_eq!(
        db.err("SELECT users.id FROM users u"),
        PlannerError::UnknownTable("users".into())
    );
}

#[test]
fn on_sees_only_its_join() {
    let db = Db::new();
    let select = db.select("SELECT * FROM orders, users u JOIN orders o ON u.id = o.user_id");
    let Some(BoundFrom::Join {
        kind: JoinKind::Cross,
        right,
        ..
    }) = &select.from
    else {
        panic!("expected a cross join");
    };
    let BoundFrom::Join {
        kind: JoinKind::Inner,
        on: Some(on),
        ..
    } = right.as_ref()
    else {
        panic!("expected an inner join");
    };
    // Positions are in the join's own row: u's 4 columns, then o's.
    let BoundExprKind::Binary { op, left, right } = &on.kind else {
        panic!("expected a comparison");
    };
    assert_eq!(*op, BinaryOp::Eq);
    assert_eq!(column_ref(left), ("u", 0));
    assert_eq!(column_ref(right), ("o", 5));

    // The first FROM item is not visible to the second's ON.
    assert_eq!(
        db.err("SELECT * FROM orders, users u JOIN orders o ON orders.id = o.id"),
        PlannerError::UnknownTable("orders".into())
    );
    assert_eq!(
        db.err("SELECT * FROM users JOIN orders ON users.id"),
        PlannerError::NotBoolean {
            clause: "ON",
            found: DataType::Int
        }
    );
}

#[test]
fn left_join_makes_the_right_side_nullable() {
    let db = Db::new();
    let select =
        db.select("SELECT o.id FROM users u LEFT JOIN orders o ON u.id = o.id WHERE o.id IS NULL");
    // In ON the right side's rows are real, so o.id is still NOT NULL there.
    let Some(BoundFrom::Join { on: Some(on), .. }) = &select.from else {
        panic!("expected a join");
    };
    let BoundExprKind::Binary { right, .. } = &on.kind else {
        panic!("expected a comparison");
    };
    assert!(!right.return_type.nullable);

    // Above the join it can be NULL.
    assert!(select.projection[0].expr.return_type.nullable);
    let schema = select.from.as_ref().unwrap().schema();
    assert!(!schema.columns()[0].nullable);
    assert!(schema.columns()[4].nullable);
    assert!(select.output_schema().columns()[0].nullable);
}

#[test]
fn where_must_be_bool() {
    let db = Db::new();
    assert_eq!(
        db.err("SELECT * FROM users WHERE score"),
        PlannerError::NotBoolean {
            clause: "WHERE",
            found: DataType::BigInt
        }
    );
    let select = db.select("SELECT * FROM users WHERE NULL");
    assert_eq!(
        select.selection.unwrap().return_type.data_type,
        DataType::Bool
    );
    let select = db.select("SELECT * FROM users WHERE active AND id > 3");
    assert!(select.selection.is_some());
}

#[test]
fn arithmetic_types() {
    let db = Db::new();
    let int = db.expr("id + 1").unwrap();
    assert_eq!(int.return_type.data_type, DataType::Int);
    assert!(!int.return_type.nullable);

    let big = db.expr("id * score").unwrap();
    assert_eq!(big.return_type.data_type, DataType::BigInt);

    let neg = db.expr("-score").unwrap();
    assert_eq!(neg.return_type.data_type, DataType::BigInt);
    assert!(matches!(
        neg.kind,
        BoundExprKind::Unary {
            op: UnaryOp::Minus,
            ..
        }
    ));

    assert_eq!(
        db.expr("name + 1").unwrap_err(),
        invalid_operands("+", "VARCHAR", "INT")
    );
    assert_eq!(
        db.expr("-name").unwrap_err(),
        PlannerError::Value(ValueError::InvalidOperand {
            op: "-",
            operand: "VARCHAR"
        })
    );
}

#[test]
fn comparison_and_logic_types() {
    let db = Db::new();
    let cmp = db.expr("id < score").unwrap();
    assert_eq!(cmp.return_type.data_type, DataType::Bool);
    assert!(!cmp.return_type.nullable);
    assert!(db.expr("name = 'bob'").unwrap().return_type.nullable);
    assert!(db.expr("active = TRUE").is_ok());

    assert_eq!(
        db.expr("id = name").unwrap_err(),
        invalid_operands("=", "INT", "VARCHAR")
    );
    assert_eq!(
        db.expr("active AND 1").unwrap_err(),
        invalid_operands("AND", "BOOL", "INT")
    );
    assert_eq!(
        db.expr("NOT id").unwrap_err(),
        PlannerError::Value(ValueError::InvalidOperand {
            op: "NOT",
            operand: "INT"
        })
    );

    let is_null = db.expr("name IS NOT NULL").unwrap();
    assert_eq!(is_null.return_type.data_type, DataType::Bool);
    assert!(!is_null.return_type.nullable);
}

#[test]
fn null_literals_take_their_context_type() {
    let db = Db::new();
    let cases = [
        ("NULL", DataType::Int),
        ("NULL + score", DataType::BigInt),
        ("NULL AND TRUE", DataType::Bool),
        ("NOT NULL", DataType::Bool),
        ("name = NULL", DataType::Bool),
        ("upper(NULL)", DataType::Varchar(0)),
    ];
    for (expr, want) in cases {
        let bound = db.expr(expr).unwrap();
        assert_eq!(bound.return_type.data_type, want, "{expr}");
        assert!(bound.return_type.nullable, "{expr}");
    }

    // The NULL itself was typed too.
    let bound = db.expr("NULL + score").unwrap();
    let BoundExprKind::Binary { left, .. } = &bound.kind else {
        panic!("expected arithmetic");
    };
    assert_eq!(left.return_type.data_type, DataType::BigInt);
}

#[test]
fn functions() {
    let db = Db::new();
    let select = db.select("SELECT upper(name), LOWER(name) AS l FROM users");
    assert_eq!(names(&select), ["upper", "l"]);
    let upper = &select.projection[0].expr;
    assert_eq!(upper.return_type.data_type, DataType::Varchar(32));
    assert!(matches!(
        upper.kind,
        BoundExprKind::Function {
            function: Function::Upper,
            ..
        }
    ));

    assert_eq!(
        db.expr("concat(name)").unwrap_err(),
        PlannerError::UnknownFunction("concat".into())
    );
    assert_eq!(
        db.expr("upper(name, name)").unwrap_err(),
        PlannerError::WrongArgumentCount {
            function: "upper".into(),
            expected: 1,
            found: 2
        }
    );
    assert_eq!(
        db.expr("lower(id)").unwrap_err(),
        PlannerError::Value(ValueError::InvalidOperand {
            op: "LOWER",
            operand: "INT"
        })
    );
}

#[test]
fn literals_and_arrays() {
    let db = Db::new();
    let select = db.select("SELECT 1, 'abc', ARRAY[1, 'a', NULL], 5000000000 AS big");
    let types: Vec<DataType> = select
        .output_schema()
        .columns()
        .iter()
        .map(|c| c.data_type)
        .collect();
    assert_eq!(
        types,
        [
            DataType::Int,
            DataType::Varchar(3),
            DataType::Array,
            DataType::BigInt
        ]
    );
    assert!(select.from.is_none());
    // ARRAY's elements are bound like any expression.
    assert!(db.expr("ARRAY[id, name]").is_ok());
    assert_eq!(
        db.err("SELECT ARRAY[id]"),
        PlannerError::ColumnNotFound("id".into())
    );
    assert_eq!(names(&select), ["?column?", "?column?", "?column?", "big"]);
}

#[test]
fn select_without_from() {
    let db = Db::new();
    let select = db.select("SELECT 1 + 2 WHERE TRUE");
    assert!(select.from.is_none());
    assert!(select.selection.is_some());
    assert_eq!(db.err("SELECT *"), PlannerError::WildcardWithoutFrom);
}

#[test]
fn order_by_positions_names_and_expressions() {
    let db = Db::new();
    let select =
        db.select("SELECT name, score AS s FROM users ORDER BY 2 DESC, s, active, id + 1 LIMIT 5");
    let keys: Vec<_> = select.order_by.iter().map(|o| (&o.expr, o.desc)).collect();
    assert_eq!(column_ref(keys[0].0), ("users", 2));
    assert!(keys[0].1);
    assert_eq!(column_ref(keys[1].0), ("users", 2));
    // Not in the select list: still a column of the FROM row.
    assert_eq!(column_ref(keys[2].0), ("users", 3));
    assert!(matches!(keys[3].0.kind, BoundExprKind::Binary { .. }));
    assert_eq!(select.limit, Some(5));

    for position in [0, 3] {
        assert_eq!(
            db.err(&format!(
                "SELECT name, score FROM users ORDER BY {position}"
            )),
            PlannerError::OrderByPositionOutOfRange { position }
        );
    }
    assert_eq!(
        db.err("SELECT id AS x, name AS x FROM users ORDER BY x"),
        PlannerError::AmbiguousColumn("x".into())
    );
    // The same expression twice is not ambiguous.
    db.select("SELECT id, id FROM users ORDER BY id");
}

#[test]
fn insert_full_rows() {
    let db = Db::new();
    let BoundStatement::Insert(insert) = db
        .bind("INSERT INTO users VALUES (1, 'a', 5, TRUE), (2, NULL, 3000000000, NULL)")
        .unwrap()
    else {
        panic!("expected INSERT");
    };
    assert_eq!(insert.table.name, "users");
    let values: Vec<Vec<&Value>> = insert
        .rows
        .iter()
        .map(|row| row.iter().map(constant_of).collect())
        .collect();
    assert_eq!(
        values,
        [
            // The INT literal 5 was converted for the BIGINT column.
            vec![
                &Value::Int(1),
                &Value::from("a"),
                &Value::BigInt(5),
                &Value::Bool(true)
            ],
            vec![
                &Value::Int(2),
                &Value::Null,
                &Value::BigInt(3_000_000_000),
                &Value::Null
            ],
        ]
    );
    // Each NULL got its column's type.
    assert_eq!(insert.rows[1][3].return_type.data_type, DataType::Bool);
}

#[test]
fn insert_with_a_column_list() {
    let db = Db::new();
    let BoundStatement::Insert(insert) = db
        .bind("INSERT INTO users (score, id) VALUES (2 + 3, 7)")
        .unwrap()
    else {
        panic!("expected INSERT");
    };
    let row = &insert.rows[0];
    assert_eq!(row.len(), 4);
    assert_eq!(constant_of(&row[0]), &Value::Int(7));
    assert_eq!(constant_of(&row[1]), &Value::Null);
    assert_eq!(row[1].return_type.data_type, DataType::Varchar(32));
    // An INT expression stored in BIGINT is cast at run time.
    assert!(matches!(row[2].kind, BoundExprKind::Cast(_)));
    assert_eq!(row[2].return_type.data_type, DataType::BigInt);
    assert_eq!(constant_of(&row[3]), &Value::Null);
}

#[test]
fn insert_errors() {
    let db = Db::new();
    let cases = [
        (
            "INSERT INTO missing VALUES (1)",
            PlannerError::TableNotFound("missing".into()),
        ),
        (
            "INSERT INTO users VALUES (1, 'a', 2)",
            PlannerError::Value(ValueError::ColumnCountMismatch {
                expected: 4,
                found: 3,
            }),
        ),
        (
            "INSERT INTO users (id, nope) VALUES (1, 2)",
            PlannerError::ColumnNotFound("nope".into()),
        ),
        (
            "INSERT INTO users (id, id) VALUES (1, 2)",
            PlannerError::DuplicateColumn("id".into()),
        ),
        (
            // score is NOT NULL and has no value.
            "INSERT INTO users (id) VALUES (1)",
            PlannerError::Value(ValueError::NullNotAllowed("score".into())),
        ),
        (
            "INSERT INTO users VALUES (NULL, 'a', 1, TRUE)",
            PlannerError::Value(ValueError::NullNotAllowed("id".into())),
        ),
        (
            "INSERT INTO users VALUES ('x', 'a', 1, TRUE)",
            PlannerError::Value(ValueError::TypeMismatch {
                column: "id".into(),
                expected: DataType::Int,
                found: "VARCHAR",
            }),
        ),
        (
            "INSERT INTO users VALUES (3000000000, 'a', 1, TRUE)",
            PlannerError::Value(ValueError::Overflow),
        ),
        (
            "INSERT INTO users VALUES (id, 'a', 1, TRUE)",
            PlannerError::ColumnNotFound("id".into()),
        ),
    ];
    for (sql, want) in cases {
        assert_eq!(db.err(sql), want, "{sql}");
    }
}

#[test]
fn update() {
    let db = Db::new();
    let BoundStatement::Update(update) = db
        .bind("UPDATE users AS u SET score = id, name = upper(u.name) WHERE u.id = 1")
        .unwrap()
    else {
        panic!("expected UPDATE");
    };
    assert_eq!(update.table.alias, "u");
    assert_eq!(update.assignments[0].0, 2);
    assert!(matches!(
        update.assignments[0].1.kind,
        BoundExprKind::Cast(_)
    ));
    assert_eq!(update.assignments[1].0, 1);
    assert!(update.selection.is_some());

    assert_eq!(
        db.err("UPDATE users SET nope = 1"),
        PlannerError::ColumnNotFound("nope".into())
    );
    assert_eq!(
        db.err("UPDATE users SET id = 1, id = 2"),
        PlannerError::DuplicateColumn("id".into())
    );
    assert_eq!(
        db.err("UPDATE users SET active = 1"),
        PlannerError::Value(ValueError::TypeMismatch {
            column: "active".into(),
            expected: DataType::Bool,
            found: "INT",
        })
    );
    assert_eq!(
        db.err("UPDATE users SET score = NULL"),
        PlannerError::Value(ValueError::NullNotAllowed("score".into()))
    );
}

#[test]
fn delete() {
    let db = Db::new();
    let BoundStatement::Delete(delete) = db.bind("DELETE FROM orders WHERE total > 100").unwrap()
    else {
        panic!("expected DELETE");
    };
    assert_eq!(delete.table.name, "orders");
    let BoundExprKind::Binary { left, .. } = &delete.selection.unwrap().kind else {
        panic!("expected a comparison");
    };
    assert_eq!(column_ref(left), ("orders", 2));

    assert_eq!(
        db.err("DELETE FROM missing"),
        PlannerError::TableNotFound("missing".into())
    );
    assert_eq!(
        db.err("DELETE FROM orders WHERE total"),
        PlannerError::NotBoolean {
            clause: "WHERE",
            found: DataType::BigInt
        }
    );
}

#[test]
fn create_table() {
    let db = Db::new();
    let BoundStatement::CreateTable(create) = db
        .bind("CREATE TABLE t (a INT NOT NULL, b VARCHAR(4))")
        .unwrap()
    else {
        panic!("expected CREATE TABLE");
    };
    assert_eq!(create.name, "t");
    assert_eq!(
        create.schema,
        Schema::new(vec![
            Column::new("a", DataType::Int, false),
            Column::new("b", DataType::Varchar(4), true),
        ])
    );
    assert_eq!(
        db.err("CREATE TABLE users (a INT)"),
        PlannerError::TableExists("users".into())
    );
}

#[test]
fn every_statement_kind_binds() {
    let db = Db::new();
    let stmts = parser::parse(
        "CREATE TABLE t (a INT); INSERT INTO orders VALUES (1, 2, 3); \
         SELECT * FROM orders; UPDATE orders SET total = 0; DELETE FROM orders",
    )
    .unwrap();
    let binder = Binder::new(&db.catalog);
    for stmt in &stmts {
        let bound = binder.bind(stmt).unwrap();
        let same_kind = matches!(
            (stmt, &bound),
            (Statement::CreateTable(_), BoundStatement::CreateTable(_))
                | (Statement::Insert(_), BoundStatement::Insert(_))
                | (Statement::Select(_), BoundStatement::Select(_))
                | (Statement::Update(_), BoundStatement::Update(_))
                | (Statement::Delete(_), BoundStatement::Delete(_))
        );
        assert!(same_kind, "{stmt:?} bound to {bound:?}");
    }
}

#[test]
fn grouped_select_reads_the_aggregation_row() {
    let db = Db::new();
    let select =
        db.select("SELECT user_id, count(*), sum(total) AS s FROM orders GROUP BY user_id");
    assert!(select.is_aggregate());
    assert_eq!(column_ref(&select.group_by[0]), ("orders", 1));

    let functions: Vec<_> = select.aggregates.iter().map(|a| a.function).collect();
    assert_eq!(
        functions,
        [AggregateFunction::Count, AggregateFunction::Sum]
    );
    assert_eq!(select.aggregates[0].arg, None);
    assert_eq!(
        column_ref(select.aggregates[1].arg.as_ref().unwrap()),
        ("orders", 2)
    );

    // Group key, then the aggregates.
    let refs: Vec<_> = select
        .projection
        .iter()
        .map(|p| column_ref(&p.expr))
        .collect();
    assert_eq!(refs, [("", 0), ("", 1), ("", 2)]);
    assert_eq!(names(&select), ["user_id", "count", "s"]);
    assert_eq!(
        select.output_schema(),
        Schema::new(vec![
            Column::new("user_id", DataType::Int, true),
            Column::new("count", DataType::BigInt, false),
            Column::new("s", DataType::BigInt, true),
        ])
    );
}

#[test]
fn aggregates_are_shared_by_having_select_list_and_order_by() {
    let db = Db::new();
    let select = db.select("SELECT count(*) FROM orders HAVING count(*) > 1 ORDER BY max(total)");
    assert!(select.group_by.is_empty());
    assert_eq!(select.aggregates.len(), 2);
    let Some(BoundExpr {
        kind: BoundExprKind::Binary { left, .. },
        ..
    }) = &select.having
    else {
        panic!("expected a comparison");
    };
    assert_eq!(column_ref(left), ("", 0));
    assert_eq!(column_ref(&select.projection[0].expr), ("", 0));
    assert_eq!(column_ref(&select.order_by[0].expr), ("", 1));
    assert_eq!(
        select.order_by[0].expr.return_type.data_type,
        DataType::BigInt
    );
}

#[test]
fn having_alone_makes_one_group() {
    let db = Db::new();
    let select = db.select("SELECT 1 FROM users HAVING TRUE");
    assert!(select.is_aggregate());
    assert!(select.group_by.is_empty() && select.aggregates.is_empty());
}

#[test]
fn group_by_keys_match_whole_expressions() {
    let db = Db::new();
    let select = db.select("SELECT id + 1, count(*) FROM users GROUP BY id + 1 ORDER BY id + 1");
    assert_eq!(column_ref(&select.projection[0].expr), ("", 0));
    assert_eq!(column_ref(&select.order_by[0].expr), ("", 0));

    // `id` alone is not a key, though `id + 1` is.
    assert_eq!(
        db.err("SELECT id FROM users GROUP BY id + 1"),
        PlannerError::NotGrouped("users.id".into())
    );
}

#[test]
fn ungrouped_columns_are_errors() {
    let db = Db::new();
    let not_grouped = |column: &str| PlannerError::NotGrouped(column.into());
    assert_eq!(
        db.err("SELECT name, count(*) FROM users"),
        not_grouped("users.name")
    );
    assert_eq!(
        db.err("SELECT * FROM users GROUP BY id"),
        not_grouped("users.name")
    );
    assert_eq!(
        db.err("SELECT id FROM users GROUP BY id HAVING score > 1"),
        not_grouped("users.score")
    );
    assert_eq!(
        db.err("SELECT id FROM users GROUP BY id ORDER BY name"),
        not_grouped("users.name")
    );
}

#[test]
fn aggregates_where_they_are_not_allowed() {
    let db = Db::new();
    let not_allowed = |clause| PlannerError::AggregateNotAllowed { clause };
    assert_eq!(
        db.err("SELECT * FROM users WHERE count(*) > 1"),
        not_allowed("WHERE")
    );
    assert_eq!(
        db.err("SELECT 1 FROM users GROUP BY count(*)"),
        not_allowed("GROUP BY")
    );
    assert_eq!(
        db.err("SELECT * FROM users JOIN orders ON count(*) > 1"),
        not_allowed("ON")
    );
    assert_eq!(
        db.err("UPDATE users SET score = count(*)"),
        not_allowed("UPDATE")
    );
    assert_eq!(
        db.err("INSERT INTO orders VALUES (count(*), 1, 1)"),
        not_allowed("VALUES")
    );
    assert_eq!(
        db.err("SELECT sum(count(*)) FROM users"),
        PlannerError::NestedAggregate
    );
}

#[test]
fn aggregate_argument_types() {
    let db = Db::new();
    let min = db.expr("min(name)").unwrap();
    assert_eq!(min.return_type.data_type, DataType::Varchar(32));
    assert!(min.return_type.nullable);
    let count = db.expr("count(name)").unwrap();
    assert_eq!(count.return_type.data_type, DataType::BigInt);
    assert!(!count.return_type.nullable);
    let BoundStatement::Select(select) = db.bind("SELECT sum(NULL) FROM users").unwrap() else {
        panic!("expected a select");
    };
    let arg = select.aggregates[0].arg.as_ref().unwrap();
    assert_eq!(arg.return_type.data_type, DataType::Int);

    assert_eq!(
        db.expr("sum(name)").unwrap_err(),
        PlannerError::Value(ValueError::InvalidOperand {
            op: "sum",
            operand: "VARCHAR"
        })
    );
    assert_eq!(
        db.expr("max(ARRAY[1])").unwrap_err(),
        PlannerError::Value(ValueError::InvalidOperand {
            op: "max",
            operand: "ARRAY"
        })
    );
    assert_eq!(
        db.expr("sum(*)").unwrap_err(),
        PlannerError::UnknownFunction("sum(*)".into())
    );
    assert_eq!(
        db.expr("upper(*)").unwrap_err(),
        PlannerError::UnknownFunction("upper(*)".into())
    );
    assert_eq!(
        db.expr("count(id, name)").unwrap_err(),
        PlannerError::WrongArgumentCount {
            function: "count".into(),
            expected: 1,
            found: 2
        }
    );
}

#[test]
fn window_results_follow_the_from_row() {
    let db = Db::new();
    let select = db.select(
        "SELECT name, rank() OVER (PARTITION BY active ORDER BY score DESC) AS r, \
         sum(score) OVER () FROM users ORDER BY row_number() OVER ()",
    );
    assert!(!select.is_aggregate());
    assert_eq!(select.windows.len(), 3);
    let rank = &select.windows[0];
    assert_eq!(rank.function, BoundWindowFunction::Rank);
    assert_eq!(column_ref(&rank.partition_by[0]), ("users", 3));
    assert_eq!(column_ref(&rank.order_by[0].expr), ("users", 2));
    assert!(rank.order_by[0].desc);
    assert!(matches!(
        select.windows[1].function,
        BoundWindowFunction::Aggregate(BoundAggregate {
            function: AggregateFunction::Sum,
            ..
        })
    ));

    // `users` has 4 columns; the windows come after them.
    assert_eq!(column_ref(&select.projection[0].expr), ("users", 1));
    assert_eq!(column_ref(&select.projection[1].expr), ("", 4));
    assert_eq!(column_ref(&select.projection[2].expr), ("", 5));
    assert_eq!(column_ref(&select.order_by[0].expr), ("", 6));
    assert_eq!(names(&select), ["name", "r", "sum"]);
    assert!(!select.projection[1].expr.return_type.nullable);
}

#[test]
fn windows_over_aggregates() {
    let db = Db::new();
    let select = db.select(
        "SELECT user_id, rank() OVER (ORDER BY count(*) DESC) FROM orders GROUP BY user_id",
    );
    assert_eq!(select.aggregates.len(), 1);
    // The window sorts by the aggregate's column of the aggregation row,
    // and its own result comes after that row.
    assert_eq!(column_ref(&select.windows[0].order_by[0].expr), ("", 1));
    assert_eq!(column_ref(&select.projection[1].expr), ("", 2));

    assert_eq!(
        db.err("SELECT rank() OVER (ORDER BY total) FROM orders GROUP BY user_id"),
        PlannerError::NotGrouped("orders.total".into())
    );
}

#[test]
fn identical_windows_are_computed_once() {
    let db = Db::new();
    let select = db.select("SELECT row_number() OVER (), row_number() OVER () + 1 FROM users");
    assert_eq!(select.windows.len(), 1);
}

#[test]
fn window_errors() {
    let db = Db::new();
    assert_eq!(
        db.expr("rank()").unwrap_err(),
        PlannerError::WindowWithoutOver("rank".into())
    );
    assert_eq!(
        db.expr("upper(name) OVER ()").unwrap_err(),
        PlannerError::NotAWindowFunction("upper".into())
    );
    assert_eq!(
        db.expr("nope() OVER ()").unwrap_err(),
        PlannerError::UnknownFunction("nope".into())
    );
    assert_eq!(
        db.expr("rank(id) OVER ()").unwrap_err(),
        PlannerError::WrongArgumentCount {
            function: "rank".into(),
            expected: 0,
            found: 1
        }
    );
    assert_eq!(
        db.expr("sum(rank() OVER ()) OVER ()").unwrap_err(),
        PlannerError::NestedWindow
    );
    assert_eq!(
        db.expr("sum(rank() OVER ())").unwrap_err(),
        PlannerError::WindowNotAllowed {
            clause: "aggregate function calls"
        }
    );
    assert_eq!(
        db.err("SELECT * FROM users WHERE rank() OVER () = 1"),
        PlannerError::WindowNotAllowed { clause: "WHERE" }
    );
    assert_eq!(
        db.err("SELECT 1 FROM users HAVING rank() OVER () = 1"),
        PlannerError::WindowNotAllowed { clause: "HAVING" }
    );
}

#[test]
fn subquery_in_from() {
    let db = Db::new();
    let select = db.select(
        "SELECT t.n, o.total FROM \
         (SELECT user_id AS n, count(*) AS c FROM orders GROUP BY user_id) t \
         JOIN orders o ON t.n = o.user_id WHERE t.c > 1",
    );
    let Some(BoundFrom::Join {
        left, on: Some(on), ..
    }) = &select.from
    else {
        panic!("expected a join");
    };
    let BoundFrom::Subquery {
        select: inner,
        alias,
    } = &**left
    else {
        panic!("expected a subquery");
    };
    assert_eq!(alias, "t");
    assert!(inner.is_aggregate());

    // `t` contributes its two output columns to the FROM row.
    assert_eq!(column_ref(&select.projection[0].expr), ("t", 0));
    assert_eq!(column_ref(&select.projection[1].expr), ("o", 4));
    let BoundExprKind::Binary { left, right, .. } = &on.kind else {
        panic!("expected a comparison");
    };
    assert_eq!(column_ref(left), ("t", 0));
    assert_eq!(column_ref(right), ("o", 3));
    assert_eq!(
        select.output_schema(),
        Schema::new(vec![
            Column::new("n", DataType::Int, true),
            Column::new("total", DataType::BigInt, true),
        ])
    );

    assert_eq!(
        db.err("SELECT * FROM users, (SELECT 1) AS users"),
        PlannerError::DuplicateTable("users".into())
    );
    // A FROM subquery can't see the other FROM items.
    assert_eq!(
        db.err("SELECT * FROM users, (SELECT users.id) AS x"),
        PlannerError::UnknownTable("users".into())
    );
}

#[test]
fn subqueries_in_expressions() {
    let db = Db::new();
    let select = db.select(
        "SELECT (SELECT max(total) FROM orders) AS m, EXISTS (SELECT * FROM orders), \
         id NOT IN (SELECT user_id FROM orders GROUP BY user_id HAVING count(*) > 1), \
         (SELECT user_id FROM orders) FROM users",
    );
    // The subqueries' aggregates are their own.
    assert!(!select.is_aggregate());
    assert_eq!(names(&select), ["m", "exists", "?column?", "user_id"]);

    let kinds: Vec<_> = select.projection.iter().map(|p| &p.expr.kind).collect();
    assert!(matches!(kinds[0], BoundExprKind::Subquery(_)));
    assert!(matches!(
        kinds[1],
        BoundExprKind::Exists { negated: false, .. }
    ));
    assert!(matches!(
        kinds[2],
        BoundExprKind::InSubquery { negated: true, .. }
    ));

    let types: Vec<_> = select
        .projection
        .iter()
        .map(|p| (p.expr.return_type.data_type, p.expr.return_type.nullable))
        .collect();
    assert_eq!(
        types,
        [
            (DataType::BigInt, true),
            (DataType::Bool, false),
            (DataType::Bool, true),
            (DataType::Int, true),
        ]
    );
}

#[test]
fn subquery_errors() {
    let db = Db::new();
    assert_eq!(
        db.expr("(SELECT id, name FROM users)").unwrap_err(),
        PlannerError::SubqueryColumns { found: 2 }
    );
    assert_eq!(
        db.expr("id IN (SELECT * FROM orders)").unwrap_err(),
        PlannerError::SubqueryColumns { found: 3 }
    );
    assert_eq!(
        db.expr("id IN (SELECT name FROM users)").unwrap_err(),
        invalid_operands("IN", "INT", "VARCHAR")
    );
    assert_eq!(
        db.err(
            "SELECT * FROM users u WHERE EXISTS \
             (SELECT * FROM orders o WHERE o.user_id = u.id)"
        ),
        PlannerError::Unsupported(
            "correlated subquery (column u.id belongs to an outer query)".into()
        )
    );
    assert_eq!(
        db.err("SELECT (SELECT score) FROM users"),
        PlannerError::Unsupported(
            "correlated subquery (column score belongs to an outer query)".into()
        )
    );
    // A name no query has is still just missing.
    assert_eq!(
        db.err("SELECT (SELECT nope FROM orders) FROM users"),
        PlannerError::ColumnNotFound("nope".into())
    );
}
