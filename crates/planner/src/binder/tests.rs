use std::sync::Arc;

use catalog::Catalog;
use common::{Column, DataType, Schema, Value, ValueError};
use parser::{BinaryOp, JoinKind, Statement, UnaryOp};
use storage::buffer::buffer_pool_manager::BufferPoolManager;
use storage::disk::disk_manager::DiskManager;
use tempfile::TempDir;

use super::Binder;
use crate::bound::*;
use crate::error::{PlannerError, Result};

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
