//! SQL statements taken through the parser, binder and planner, and run by
//! the executors.

use std::sync::Arc;

use catalog::Catalog;
use common::Value;
use execution::ExecutionError;
use execution::execution_context::ExecutionContext;
use execution::executors::{Executor, create_executor};
use planner::{Binder, Planner, PlannerError};
use storage::buffer::buffer_pool_manager::BufferPoolManager;
use storage::disk::disk_manager::DiskManager;
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
        let mut stmts = parser::parse(sql).unwrap();
        assert_eq!(stmts.len(), 1, "{sql}");
        let catalog = self.ctx.catalog();
        let bound = Binder::new(catalog).bind(&stmts.remove(0)).unwrap();
        let plan = Planner::new(catalog).plan(&bound).unwrap();
        let mut executor = create_executor(&self.ctx, &plan).unwrap();
        executor.init().unwrap();
        let (mut tuples, mut rids) = (Vec::new(), Vec::new());
        let mut rows = Vec::new();
        while executor.next(&mut tuples, &mut rids, 3).unwrap() {
            for tuple in &tuples {
                rows.push(tuple.decode(executor.get_output_schema()).unwrap());
            }
        }
        rows
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
fn plans_without_executors_are_rejected() {
    let db = Db::new();
    db.run("CREATE TABLE t (a INT)");
    assert!(matches!(
        db.try_plan("SELECT a FROM t ORDER BY a"),
        Err(ExecutionError::NoExecutor("Sort"))
    ));
    assert!(matches!(
        db.try_plan("UPDATE t SET a = 1"),
        Err(ExecutionError::NoExecutor("Update"))
    ));
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
