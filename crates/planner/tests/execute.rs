//! SQL statements taken through the parser, binder and planner, and run by
//! the executors.

use std::cell::RefCell;
use std::sync::Arc;

use catalog::Catalog;
use common::{Tuple, Value};
use execution::ExecutionError;
use execution::execution_context::{self, ExecutionSettings};
use planner::{Binder, EngineError, ExecutionEngine, Output, PlannerError};
use storage::buffer::buffer_pool_manager::BufferPoolManager;
use storage::disk::disk_manager::DiskManager;
use storage::index::IndexTrait;
use tempfile::TempDir;

struct Db {
    _dir: TempDir,
    engine: RefCell<ExecutionEngine>,
}

impl Db {
    fn new() -> Self {
        Self::with_settings(ExecutionSettings::default())
    }

    /// A database whose joins get `frames` buffers.
    fn with_join_frames(frames: usize) -> Self {
        Self::with_settings(ExecutionSettings::default().with_join_frames(frames))
    }

    /// A database whose aggregations get `frames` buffers.
    fn with_aggregation_frames(frames: usize) -> Self {
        Self::with_settings(ExecutionSettings::default().with_aggregation_frames(frames))
    }

    fn with_settings(settings: ExecutionSettings) -> Self {
        let dir = TempDir::new().unwrap();
        let dm = Arc::new(DiskManager::new(dir.path().join("test.db")).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(8, dm));
        let catalog = Arc::new(Catalog::open(bpm).unwrap());
        let engine = ExecutionEngine::new(catalog)
            .unwrap()
            .with_settings(settings)
            .with_batch_size(3);
        Self {
            _dir: dir,
            engine: RefCell::new(engine),
        }
    }

    fn catalog(&self) -> Arc<Catalog> {
        Arc::clone(self.engine.borrow().catalog())
    }

    /// Runs one statement, returning its rows.
    fn run(&self, sql: &str) -> Vec<Vec<Value>> {
        self.try_run(sql).unwrap()
    }

    /// Runs one statement, which must bind and plan, returning its rows or
    /// the error that stopped its executors. INSERT, UPDATE and DELETE
    /// return their count as a row, and other statements no rows.
    fn try_run(&self, sql: &str) -> Result<Vec<Vec<Value>>, ExecutionError> {
        let mut engine = self.engine.borrow_mut();
        let mut stmts = engine.parse(sql).unwrap();
        assert_eq!(stmts.len(), 1, "{sql}");
        match engine.execute(&stmts.remove(0)) {
            Ok(Output::Rows { rows, .. }) => Ok(rows),
            Ok(Output::Count { count, .. }) => Ok(vec![vec![Value::BigInt(count)]]),
            Ok(Output::Done(_)) => Ok(Vec::new()),
            Err(EngineError::Execution(e)) => Err(e),
            Err(e) => panic!("{sql}: {e}"),
        }
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

    let catalog = db.catalog();
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
fn doubles_are_stored_compared_and_computed() {
    let db = Db::new();
    db.run("CREATE TABLE m (id INT NOT NULL, x DOUBLE PRECISION)");
    db.run("INSERT INTO m VALUES (1, 1.5), (2, -0.25), (3, NULL), (4, 2)");
    assert_eq!(
        db.run("SELECT id, x * 2, x + id FROM m WHERE x > 1 ORDER BY x"),
        [
            vec![Value::Int(1), Value::Double(3.0), Value::Double(2.5)],
            vec![Value::Int(4), Value::Double(4.0), Value::Double(6.0)],
        ]
    );
    assert_eq!(
        db.run("SELECT id FROM m WHERE x = 2"),
        [vec![Value::Int(4)]]
    );
    assert!(matches!(
        db.try_run("SELECT x / 0 FROM m WHERE id = 1"),
        Err(ExecutionError::Value(common::ValueError::DivisionByZero))
    ));
}

#[test]
fn aggregates_with_group_by_and_having() {
    let db = Db::new();
    db.run("CREATE TABLE orders (user_id INT, total INT, price DOUBLE)");
    db.run(
        "INSERT INTO orders VALUES (1, 10, 1.5), (1, 20, 2.5), (2, 5, NULL), \
         (NULL, 7, 0.5), (NULL, NULL, 1.0), (3, 1, 9.0), (3, 2, 9.0)",
    );
    let null = Value::Null;
    assert_eq!(
        db.run(
            "SELECT user_id, count(*), count(total), sum(total), avg(total), \
             min(price), max(price), sum(price) FROM orders \
             GROUP BY user_id HAVING count(*) > 1 ORDER BY user_id"
        ),
        [
            vec![
                Value::Int(1),
                Value::BigInt(2),
                Value::BigInt(2),
                Value::BigInt(30),
                Value::Double(15.0),
                Value::Double(1.5),
                Value::Double(2.5),
                Value::Double(4.0),
            ],
            vec![
                Value::Int(3),
                Value::BigInt(2),
                Value::BigInt(2),
                Value::BigInt(3),
                Value::Double(1.5),
                Value::Double(9.0),
                Value::Double(9.0),
                Value::Double(18.0),
            ],
            // NULLs sort last, and all NULL user_ids are one group.
            vec![
                null.clone(),
                Value::BigInt(2),
                Value::BigInt(1),
                Value::BigInt(7),
                Value::Double(7.0),
                Value::Double(0.5),
                Value::Double(1.0),
                Value::Double(1.5),
            ],
        ]
    );
    assert_eq!(
        db.run("SELECT count(*), sum(total), avg(price) FROM orders WHERE total > 100"),
        [vec![Value::BigInt(0), null.clone(), null]]
    );
    assert!(
        db.run("SELECT user_id, count(*) FROM orders WHERE total > 100 GROUP BY user_id")
            .is_empty()
    );
}

#[test]
fn aggregation_spills_groups_that_do_not_fit() {
    let groups = 1500;
    let rows: Vec<String> = (0..groups)
        .flat_map(|k| [format!("({k}, {k})"), format!("({k}, 1)")])
        .collect();
    let expected: Vec<Vec<Value>> = (0..groups)
        .map(|k| {
            vec![
                Value::Int(k),
                Value::BigInt(2),
                Value::BigInt(i64::from(k) + 1),
            ]
        })
        .collect();
    for db in [Db::new(), Db::with_aggregation_frames(3)] {
        db.run("CREATE TABLE t (k INT, v INT)");
        db.run(&format!("INSERT INTO t VALUES {}", rows.join(", ")));
        assert_eq!(
            db.run("SELECT k, count(*), sum(v) FROM t GROUP BY k ORDER BY k"),
            expected
        );
    }
}

#[test]
fn plans_without_executors_are_rejected() {
    let db = Db::new();
    db.run("CREATE TABLE t (a INT)");
    assert!(matches!(
        db.try_run("SELECT a, row_number() OVER () FROM t"),
        Err(ExecutionError::NoExecutor("Window"))
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
    // Updated in place, the rows keep their order in storage.
    assert_eq!(
        db.run("SELECT * FROM users"),
        [
            vec![Value::Int(2), Value::from("dee")],
            vec![Value::Int(3), Value::from("bo")],
            vec![Value::Int(4), Value::from("cy")],
        ]
    );

    // Both indexes find the rows' new versions, as a new transaction sees
    // them.
    let catalog = db.catalog();
    let table = catalog.get_table("users").unwrap();
    let indexed = execution_context::indexed_table(&catalog, &table);
    let lookup = |index: &str, key: Value| {
        let index = catalog.get_index(index).unwrap();
        let key = Tuple::encode(&index.key_schema, &[key]).unwrap();
        let mut locators = Vec::new();
        index.index.scan_key(&key, &mut locators).unwrap();
        let engine = db.engine.borrow();
        let tm = engine.transaction_manager();
        let txn = tm.begin();
        let rows = locators
            .iter()
            .filter_map(|locator| txn.row_at(&indexed, locator).unwrap())
            .map(|(_, tuple)| tuple.decode(&table.schema).unwrap())
            .filter(|row| {
                let own = Tuple::encode(&index.key_schema, &[row[index.key_columns[0]].clone()]);
                own.unwrap() == key
            })
            .collect::<Vec<_>>();
        tm.commit(&txn).unwrap();
        rows
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
    let err = Binder::new(&db.catalog()).bind(&stmts[0]).unwrap_err();
    assert!(matches!(err, PlannerError::TableNotFound(_)), "{err:?}");
    db.run("CREATE TABLE t (a INT)");
    db.run("INSERT INTO t VALUES (7)");
    assert_eq!(db.run("SELECT a FROM t"), [vec![Value::Int(7)]]);
}

/// Rows in a fixed order, for queries whose order SQL leaves open. There is
/// no ORDER BY to run yet.
fn sorted(mut rows: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
    rows.sort_by_key(|row| format!("{row:?}"));
    rows
}

fn int(v: i32) -> Value {
    Value::Int(v)
}

fn text(s: &str) -> Value {
    Value::from(s)
}

/// `users (id, name)` with ann, bo and cy, and `orders (id, user_id, item)`
/// with two orders for ann, one for cy, and one with no user.
fn shop() -> Db {
    let db = Db::new();
    db.run("CREATE TABLE users (id INT NOT NULL, name VARCHAR(16))");
    db.run("CREATE TABLE orders (id INT NOT NULL, user_id INT, item VARCHAR(16))");
    db.run("INSERT INTO users VALUES (1, 'ann'), (2, 'bo'), (3, 'cy')");
    db.run(
        "INSERT INTO orders VALUES (10, 1, 'pen'), (11, 3, 'ink'), (12, NULL, 'box'), (13, 1, 'cap')",
    );
    db
}

#[test]
fn inner_join() {
    let db = shop();
    let expected = sorted(vec![
        vec![text("ann"), text("pen")],
        vec![text("ann"), text("cap")],
        vec![text("cy"), text("ink")],
    ]);
    for sql in [
        "SELECT u.name, o.item FROM users u JOIN orders o ON u.id = o.user_id",
        "SELECT name, item FROM users INNER JOIN orders ON users.id = orders.user_id",
        // The same join written with a comma and WHERE.
        "SELECT u.name, o.item FROM users u, orders o WHERE u.id = o.user_id",
    ] {
        assert_eq!(sorted(db.run(sql)), expected, "{sql}");
    }

    // WHERE filters the joined rows; expressions read both sides.
    assert_eq!(
        sorted(db.run(
            "SELECT u.id * 100 + o.id, upper(o.item) FROM users u JOIN orders o \
             ON u.id = o.user_id WHERE o.item <> 'cap' AND u.name IS NOT NULL"
        )),
        sorted(vec![
            vec![int(110), text("PEN")],
            vec![int(311), text("INK")],
        ])
    );
}

/// Bo has no orders, and the order without a user matches no one: NULL = 1
/// is not TRUE.
#[test]
fn left_join_pads_with_nulls() {
    let db = shop();
    assert_eq!(
        sorted(db.run("SELECT u.name, o.item FROM users u LEFT JOIN orders o ON u.id = o.user_id")),
        sorted(vec![
            vec![text("ann"), text("pen")],
            vec![text("ann"), text("cap")],
            vec![text("bo"), Value::Null],
            vec![text("cy"), text("ink")],
        ])
    );
    // WHERE runs after the join, so it sees the padding: users without
    // orders.
    assert_eq!(
        db.run(
            "SELECT u.name FROM users u LEFT OUTER JOIN orders o ON u.id = o.user_id \
             WHERE o.id IS NULL"
        ),
        [vec![text("bo")]]
    );
    // A condition in ON only decides what matches; every user stays.
    assert_eq!(
        sorted(db.run(
            "SELECT u.name, o.item FROM users u LEFT JOIN orders o \
             ON u.id = o.user_id AND o.item = 'ink'"
        )),
        sorted(vec![
            vec![text("ann"), Value::Null],
            vec![text("bo"), Value::Null],
            vec![text("cy"), text("ink")],
        ])
    );
}

#[test]
fn cross_join_pairs_every_row() {
    let db = shop();
    for sql in [
        "SELECT * FROM users CROSS JOIN orders",
        "SELECT * FROM users, orders",
        "SELECT * FROM users JOIN orders ON TRUE",
    ] {
        assert_eq!(db.run(sql).len(), 3 * 4, "{sql}");
    }
    assert!(
        db.run("SELECT * FROM users JOIN orders ON FALSE")
            .is_empty()
    );
}

#[test]
fn star_expands_both_sides() {
    let db = shop();
    assert_eq!(
        sorted(
            db.run("SELECT * FROM users u JOIN orders o ON u.id = o.user_id WHERE o.item = 'ink'")
        ),
        [vec![int(3), text("cy"), int(11), int(3), text("ink")]]
    );
    assert_eq!(
        sorted(db.run("SELECT o.*, u.name FROM users u JOIN orders o ON u.id = o.user_id")),
        sorted(vec![
            vec![int(10), int(1), text("pen"), text("ann")],
            vec![int(11), int(3), text("ink"), text("cy")],
            vec![int(13), int(1), text("cap"), text("ann")],
        ])
    );
}

/// The second ON reads a column of the first join's left side, and the
/// WHERE one of each table.
#[test]
fn three_way_join() {
    let db = shop();
    db.run("CREATE TABLE ratings (user_id INT, stars INT)");
    db.run("INSERT INTO ratings VALUES (1, 5), (3, 2), (3, 4)");
    assert_eq!(
        sorted(db.run(
            "SELECT u.name, o.item, r.stars FROM users u \
             JOIN orders o ON u.id = o.user_id \
             JOIN ratings r ON r.user_id = u.id \
             WHERE r.stars > 1 AND o.id < 13"
        )),
        sorted(vec![
            vec![text("ann"), text("pen"), int(5)],
            vec![text("cy"), text("ink"), int(2)],
            vec![text("cy"), text("ink"), int(4)],
        ])
    );
    // LEFT JOINs one after the other: bo has neither orders nor ratings.
    assert_eq!(
        sorted(db.run(
            "SELECT u.name, o.item, r.stars FROM users u \
             LEFT JOIN orders o ON u.id = o.user_id AND o.item = 'pen' \
             LEFT JOIN ratings r ON r.user_id = u.id AND r.stars = 5"
        )),
        sorted(vec![
            vec![text("ann"), text("pen"), int(5)],
            vec![text("bo"), Value::Null, Value::Null],
            vec![text("cy"), Value::Null, Value::Null],
        ])
    );
}

/// `a, b JOIN c ON ...` joins b and c first, so the ON's columns are
/// counted from b, not from a.
#[test]
fn comma_binds_looser_than_join() {
    let db = shop();
    db.run("CREATE TABLE colors (name VARCHAR(8))");
    db.run("INSERT INTO colors VALUES ('red'), ('blue')");
    assert_eq!(
        sorted(db.run(
            "SELECT c.name, u.name, o.item FROM colors c, users u \
             JOIN orders o ON u.id = o.user_id WHERE o.item = 'ink'"
        )),
        sorted(vec![
            vec![text("blue"), text("cy"), text("ink")],
            vec![text("red"), text("cy"), text("ink")],
        ])
    );
}

#[test]
fn self_join() {
    let db = shop();
    assert_eq!(
        sorted(db.run("SELECT a.name, b.name FROM users a JOIN users b ON a.id + 1 = b.id")),
        sorted(vec![
            vec![text("ann"), text("bo")],
            vec![text("bo"), text("cy")],
        ])
    );
}

#[test]
fn join_with_a_subquery() {
    let db = shop();
    assert_eq!(
        sorted(db.run(
            "SELECT s.label, o.item FROM \
             (SELECT id, upper(name) AS label FROM users WHERE id > 1) AS s \
             JOIN orders o ON s.id = o.user_id"
        )),
        [vec![text("CY"), text("ink")]]
    );
}

/// Few join frames and many rows: the outer side takes several blocks,
/// and the inner side is scanned again for each.
#[test]
fn join_across_many_blocks() {
    let db = Db::with_join_frames(3);
    db.run("CREATE TABLE a (id INT NOT NULL, label VARCHAR(32))");
    db.run("CREATE TABLE b (id INT NOT NULL)");
    let rows: Vec<_> = (1..=500)
        .map(|id| format!("({id}, 'row number {id:0>10}')"))
        .collect();
    db.run(&format!("INSERT INTO a VALUES {}", rows.join(", ")));
    let rows: Vec<_> = (1..=500).step_by(7).map(|id| format!("({id})")).collect();
    db.run(&format!("INSERT INTO b VALUES {}", rows.join(", ")));

    let joined = db.run("SELECT a.id, b.id FROM a JOIN b ON a.id = b.id");
    assert_eq!(joined.len(), 72);
    assert!(joined.iter().all(|row| row[0] == row[1]));
    let padded = db.run("SELECT a.id FROM a LEFT JOIN b ON a.id = b.id WHERE b.id IS NULL");
    assert_eq!(padded.len(), 500 - 72);
}

/// EXPLAIN shows the plan, one row per line, and runs none of it: the
/// DELETE leaves the rows, and the window plan needs no executor.
#[test]
fn explain_does_not_run_the_statement() {
    let db = shop();
    let lines = |sql| db.run(sql).concat();
    assert_eq!(
        lines("EXPLAIN DELETE FROM orders WHERE user_id = 1"),
        [
            "Delete on orders",
            "  ->  Filter",
            "        Condition: user_id = 1",
            "        ->  SeqScan on orders",
        ]
        .map(text)
    );
    assert_eq!(db.run("SELECT id FROM orders").len(), 4);
    assert_eq!(
        lines("EXPLAIN SELECT name, rank() OVER (ORDER BY id DESC) FROM users"),
        [
            "Projection",
            "  Output: name, rank",
            "  ->  Window",
            "        Output: id, name, rank() OVER (ORDER BY id DESC)",
            "        ->  SeqScan on users",
        ]
        .map(text)
    );
}
