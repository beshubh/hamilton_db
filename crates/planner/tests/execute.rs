//! SQL statements taken through the parser, binder and planner, and run by
//! the executors.

use std::sync::Arc;

use catalog::Catalog;
use common::{Tuple, Value};
use execution::ExecutionError;
use execution::execution_context::ExecutionContext;
use execution::executors::{Executor, create_executor};
use planner::{Binder, Planner, PlannerError};
use storage::buffer::buffer_pool_manager::BufferPoolManager;
use storage::disk::disk_manager::DiskManager;
use storage::index::IndexTrait;
use tempfile::TempDir;

struct Db {
    _dir: TempDir,
    ctx: Arc<ExecutionContext>,
}

impl Db {
    fn new() -> Self {
        let dir = TempDir::new().unwrap();
        let dm = Arc::new(DiskManager::new(dir.path().join("test.db")).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(8, dm));
        let catalog = Arc::new(Catalog::open(bpm).unwrap());
        Self {
            _dir: dir,
            ctx: Arc::new(ExecutionContext::new(catalog)),
        }
    }

    /// Runs one statement, returning its rows.
    fn run(&self, sql: &str) -> Vec<Vec<Value>> {
        self.try_run(sql).unwrap()
    }

    /// Runs one statement, which must bind and plan, returning its rows or
    /// the error that stopped its executors.
    fn try_run(&self, sql: &str) -> Result<Vec<Vec<Value>>, ExecutionError> {
        let mut stmts = parser::parse(sql).unwrap();
        assert_eq!(stmts.len(), 1, "{sql}");
        let catalog = self.ctx.catalog();
        let bound = Binder::new(catalog).bind(&stmts.remove(0)).unwrap();
        let plan = Planner::new(catalog).plan(&bound).unwrap();
        let mut executor = create_executor(&self.ctx, &plan)?;
        executor.init()?;
        let (mut tuples, mut rids) = (Vec::new(), Vec::new());
        let mut rows = Vec::new();
        while executor.next(&mut tuples, &mut rids, 3)? {
            for tuple in &tuples {
                rows.push(tuple.decode(executor.get_output_schema())?);
            }
        }
        Ok(rows)
    }

    fn try_plan(&self, sql: &str) -> Result<(), ExecutionError> {
        let stmts = parser::parse(sql).unwrap();
        let catalog = self.ctx.catalog();
        let bound = Binder::new(catalog).bind(&stmts[0]).unwrap();
        let plan = Planner::new(catalog).plan(&bound).unwrap();
        create_executor(&self.ctx, &plan).map(drop)
    }
}

fn count(n: i64) -> Vec<Vec<Value>> {
    vec![vec![Value::BigInt(n)]]
}

#[test]
fn create_insert_select_delete() {
    let db = Db::new();
    assert!(
        db.run("CREATE TABLE users (id INT NOT NULL, name VARCHAR(16), score BIGINT)")
            .is_empty()
    );
    assert_eq!(
        db.run(
            "INSERT INTO users VALUES (1, 'ann', 10), (2, 'bo', 20), (3, NULL, 30), (4, 'cy', NULL)"
        ),
        count(4)
    );
    // Omitted columns are NULL; INT literals are cast to BIGINT.
    assert_eq!(db.run("INSERT INTO users (id) VALUES (5)"), count(1));

    assert_eq!(
        db.run("SELECT name, score * 2 FROM users WHERE score > 10 AND id < 4"),
        [
            vec![Value::from("bo"), Value::BigInt(40)],
            vec![Value::Null, Value::BigInt(60)],
        ]
    );
    assert_eq!(
        db.run("SELECT upper(u.name) FROM users u WHERE u.name IS NOT NULL AND u.id <> 2"),
        [vec![Value::from("ANN")], vec![Value::from("CY")]]
    );
    assert_eq!(
        db.run("SELECT 1 + 2, 'x'"),
        [vec![Value::Int(3), Value::from("x")]]
    );

    assert_eq!(db.run("DELETE FROM users WHERE score IS NULL"), count(2));
    assert_eq!(
        db.run("SELECT * FROM users"),
        [
            vec![Value::Int(1), Value::from("ann"), Value::BigInt(10)],
            vec![Value::Int(2), Value::from("bo"), Value::BigInt(20)],
            vec![Value::Int(3), Value::Null, Value::BigInt(30)],
        ]
    );
    assert_eq!(db.run("DELETE FROM users"), count(3));
    assert!(db.run("SELECT id FROM users").is_empty());
}

#[test]
fn create_table_with_primary_key_and_index() {
    let db = Db::new();
    db.run("CREATE TABLE users (id INT, name VARCHAR(16), PRIMARY KEY (id))");
    assert!(db.run("CREATE INDEX users_name ON users (name)").is_empty());

    let catalog = db.ctx.catalog();
    let table = catalog.get_table("users").unwrap();
    assert!(!table.schema.column(0).unwrap().nullable);
    let indexes = catalog.table_indexes("users");
    let summary: Vec<_> = indexes
        .iter()
        .map(|index| {
            (
                index.name.as_str(),
                index.key_columns.clone(),
                index.is_primary_key,
            )
        })
        .collect();
    assert_eq!(
        summary,
        [
            ("users_pkey", vec![0], true),
            ("users_name", vec![1], false)
        ]
    );
}

fn assert_duplicate_key(result: Result<Vec<Vec<Value>>, ExecutionError>, index: &str, key: &str) {
    match result {
        Err(ExecutionError::DuplicateKey { index: i, key: k }) if i == index && k == key => {}
        other => panic!("expected a duplicate {key} in {index}, got {other:?}"),
    }
}

#[test]
fn primary_key_rejects_duplicates() {
    let db = Db::new();
    db.run("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(16))");
    assert_eq!(db.run("INSERT INTO users VALUES (1, 'ann')"), count(1));

    assert_duplicate_key(
        db.try_run("INSERT INTO users VALUES (1, 'bo')"),
        "users_pkey",
        "(id)=(1)",
    );
    // A duplicate within one statement, or anywhere in it, inserts nothing.
    assert_duplicate_key(
        db.try_run("INSERT INTO users VALUES (2, 'bo'), (2, 'cy')"),
        "users_pkey",
        "(id)=(2)",
    );
    assert_duplicate_key(
        db.try_run("INSERT INTO users VALUES (3, 'cy'), (1, 'dee')"),
        "users_pkey",
        "(id)=(1)",
    );
    assert_eq!(
        db.run("SELECT * FROM users"),
        [vec![Value::Int(1), Value::from("ann")]]
    );

    // A deleted row's key can be used again.
    assert_eq!(db.run("DELETE FROM users WHERE id = 1"), count(1));
    assert_eq!(db.run("INSERT INTO users VALUES (1, 'bo')"), count(1));
    assert_eq!(
        db.run("SELECT * FROM users"),
        [vec![Value::Int(1), Value::from("bo")]]
    );
}

#[test]
fn composite_primary_key_rejects_duplicates_of_the_whole_key() {
    let db = Db::new();
    db.run("CREATE TABLE t (a INT, b VARCHAR(8), PRIMARY KEY (a, b))");
    assert_eq!(
        db.run("INSERT INTO t VALUES (1, 'x'), (1, 'y'), (2, 'x')"),
        count(3)
    );
    assert_duplicate_key(
        db.try_run("INSERT INTO t VALUES (1, 'y')"),
        "t_pkey",
        "(a, b)=(1, y)",
    );
}

#[test]
fn plans_without_executors_are_rejected() {
    let db = Db::new();
    db.run("CREATE TABLE t (a INT)");
    assert!(matches!(
        db.try_plan("SELECT a FROM t ORDER BY a"),
        Err(ExecutionError::NoExecutor("Sort"))
    ));
}

#[test]
fn update_sets_columns_from_the_old_row() {
    let db = Db::new();
    db.run("CREATE TABLE users (id INT NOT NULL, name VARCHAR(16), score BIGINT)");
    db.run("INSERT INTO users VALUES (1, 'ann', 10), (2, 'bo', 20), (3, NULL, 30)");

    // INT literals are cast to BIGINT; a column not set keeps its value.
    assert_eq!(
        db.run("UPDATE users SET score = score + id * 100, name = upper(name) WHERE id >= 2"),
        count(2)
    );
    // Updated rows are moved to the end of the table.
    assert_eq!(
        db.run("SELECT * FROM users"),
        [
            vec![Value::Int(1), Value::from("ann"), Value::BigInt(10)],
            vec![Value::Int(2), Value::from("BO"), Value::BigInt(220)],
            vec![Value::Int(3), Value::Null, Value::BigInt(330)],
        ]
    );
    assert_eq!(db.run("UPDATE users SET name = 'x' WHERE id > 9"), count(0));
    assert_eq!(db.run("UPDATE users SET score = NULL"), count(3));
    assert!(matches!(
        db.try_run("UPDATE users SET id = id - 1, name = 'y' WHERE id = 1 OR 10 / (id - 2) > 0"),
        Err(ExecutionError::Value(_))
    ));
    assert_eq!(
        db.run("SELECT id, name FROM users WHERE score IS NULL"),
        [
            vec![Value::Int(1), Value::from("ann")],
            vec![Value::Int(2), Value::from("BO")],
            vec![Value::Int(3), Value::Null],
        ]
    );
}

#[test]
fn update_keeps_primary_keys_unique() {
    let db = Db::new();
    db.run("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(16))");
    db.run("CREATE INDEX users_name ON users (name)");
    db.run("INSERT INTO users VALUES (1, 'ann'), (2, 'bo'), (3, 'cy')");

    // Every key shifts onto the next row's: unique once the statement ends.
    assert_eq!(db.run("UPDATE users SET id = id + 1"), count(3));
    assert_duplicate_key(
        db.try_run("UPDATE users SET id = 4 WHERE name = 'ann'"),
        "users_pkey",
        "(id)=(4)",
    );
    assert_duplicate_key(
        db.try_run("UPDATE users SET id = 9 WHERE id < 4"),
        "users_pkey",
        "(id)=(9)",
    );
    assert_eq!(
        db.run("UPDATE users SET name = 'dee' WHERE id = 2"),
        count(1)
    );
    assert_eq!(
        db.run("SELECT * FROM users"),
        [
            vec![Value::Int(3), Value::from("bo")],
            vec![Value::Int(4), Value::from("cy")],
            vec![Value::Int(2), Value::from("dee")],
        ]
    );

    // Both indexes point at the rows' new versions.
    let catalog = db.ctx.catalog();
    let table = catalog.get_table("users").unwrap();
    let lookup = |index: &str, key: Value| {
        let index = catalog.get_index(index).unwrap();
        let key = Tuple::encode(&index.key_schema, &[key]).unwrap();
        let mut rids = Vec::new();
        index.index.scan_key(&key, &mut rids).unwrap();
        rids.into_iter()
            .map(|rid| {
                table
                    .heap
                    .get(rid)
                    .unwrap()
                    .1
                    .decode(&table.schema)
                    .unwrap()
            })
            .collect::<Vec<_>>()
    };
    let dee = vec![vec![Value::Int(2), Value::from("dee")]];
    assert_eq!(lookup("users_pkey", Value::Int(2)), dee);
    assert_eq!(lookup("users_name", Value::from("dee")), dee);
    assert!(lookup("users_pkey", Value::Int(1)).is_empty());
    assert!(lookup("users_name", Value::from("ann")).is_empty());
}

#[test]
fn binding_sees_tables_created_by_earlier_statements() {
    let db = Db::new();
    let stmts = parser::parse("SELECT a FROM t").unwrap();
    let err = Binder::new(db.ctx.catalog()).bind(&stmts[0]).unwrap_err();
    assert!(matches!(err, PlannerError::TableNotFound(_)), "{err:?}");
    db.run("CREATE TABLE t (a INT)");
    db.run("INSERT INTO t VALUES (7)");
    assert_eq!(db.run("SELECT a FROM t"), [vec![Value::Int(7)]]);
}
