use common::{Column, DataType, Value};
use hamilton_server::{Database, Session};
use planner::{Output, TransactionStatus};
use tempfile::TempDir;

fn run(session: &mut Session, sql: &str) -> Result<Vec<Output>, String> {
    let mut outputs = Vec::new();
    session
        .execute(sql, |output| outputs.push(output))
        .map_err(|e| e.to_string())?;
    Ok(outputs)
}

fn one(session: &mut Session, sql: &str) -> Output {
    let mut outputs = run(session, sql).unwrap();
    assert_eq!(outputs.len(), 1, "{sql}");
    outputs.remove(0)
}

fn rows(session: &mut Session, sql: &str) -> Vec<Vec<Value>> {
    match one(session, sql) {
        Output::Rows { rows, .. } => rows,
        output => panic!("{output:?}"),
    }
}

fn open(dir: &TempDir) -> Database {
    Database::open(dir.path().join("test.db")).unwrap()
}

#[test]
fn runs_statements() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);
    let s = &mut db.session();
    assert_eq!(
        one(s, "CREATE TABLE t (id INT NOT NULL, name VARCHAR(8))"),
        Output::Done("CREATE TABLE")
    );
    assert_eq!(
        one(s, "INSERT INTO t VALUES (1, 'a'), (2, NULL), (3, 'c')"),
        Output::Count {
            command: "INSERT",
            count: 3
        }
    );
    let output = one(s, "SELECT name, id * 10 AS ten FROM t WHERE id > 1");
    assert_eq!(
        output,
        Output::Rows {
            columns: vec![
                Column::new("name", DataType::Varchar(8), true),
                Column::new("ten", DataType::Int, false),
            ],
            rows: vec![
                vec![Value::Null, Value::Int(20)],
                vec![Value::from("c"), Value::Int(30)],
            ],
        }
    );
    assert_eq!(
        one(s, "DELETE FROM t WHERE id >= 2"),
        Output::Count {
            command: "DELETE",
            count: 2
        }
    );
}

#[test]
fn stops_at_the_first_failing_statement() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);
    let s = &mut db.session();
    let mut outputs = Vec::new();
    let err = s
        .execute(
            "CREATE TABLE t (id INT NOT NULL); SELECT 1 / 0; INSERT INTO t VALUES (1)",
            |output| outputs.push(output),
        )
        .unwrap_err();
    assert_eq!(outputs, [Output::Done("CREATE TABLE")]);
    assert!(err.to_string().contains("division by zero"), "{err}");
    assert!(rows(s, "SELECT * FROM t").is_empty());
}

#[test]
fn errors_leave_the_session_usable() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);
    let s = &mut db.session();
    run(s, "CREATE TABLE t (id INT NOT NULL)").unwrap();
    for bad in [
        "SELEC 1",
        "SELECT * FROM missing",
        "INSERT INTO t VALUES (1), (NULL)",
        "SELECT 1 / 0",
    ] {
        assert!(run(s, bad).is_err(), "{bad}");
    }
    assert!(rows(s, "SELECT * FROM t").is_empty());
    run(s, "INSERT INTO t VALUES (1)").unwrap();
    assert_eq!(rows(s, "SELECT * FROM t"), [vec![Value::Int(1)]]);
}

#[test]
fn a_failed_block_reports_rollback() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);
    let s = &mut db.session();
    run(s, "CREATE TABLE t (id INT NOT NULL)").unwrap();
    run(s, "BEGIN; INSERT INTO t VALUES (1)").unwrap();
    assert_eq!(s.status(), TransactionStatus::InBlock);
    assert!(run(s, "SELECT 1 / 0").is_err());
    assert_eq!(s.status(), TransactionStatus::Failed);
    let err = run(s, "SELECT * FROM t").unwrap_err();
    assert!(err.contains("current transaction is aborted"), "{err}");
    assert_eq!(one(s, "COMMIT"), Output::Done("ROLLBACK"));
    assert_eq!(s.status(), TransactionStatus::Idle);
    assert!(rows(s, "SELECT * FROM t").is_empty());
}

#[test]
fn sessions_run_their_own_transactions() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);
    let (a, b) = (&mut db.session(), &mut db.session());
    run(a, "CREATE TABLE t (id INT NOT NULL)").unwrap();
    // b begins first: a reader newer than a's insert would conflict.
    run(b, "BEGIN").unwrap();
    run(a, "BEGIN; INSERT INTO t VALUES (1)").unwrap();
    assert!(rows(b, "SELECT * FROM t").is_empty());
    assert_eq!(one(a, "COMMIT"), Output::Done("COMMIT"));
    assert!(rows(b, "SELECT * FROM t").is_empty());
    run(b, "COMMIT").unwrap();
    assert_eq!(rows(b, "SELECT * FROM t"), [vec![Value::Int(1)]]);
}

#[test]
fn dropping_a_session_rolls_back_its_block() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);
    let s = &mut db.session();
    run(
        s,
        "CREATE TABLE t (id INT NOT NULL); INSERT INTO t VALUES (1)",
    )
    .unwrap();
    let mut other = db.session();
    run(&mut other, "BEGIN; INSERT INTO t VALUES (2)").unwrap();
    drop(other);
    assert_eq!(rows(s, "SELECT * FROM t"), [vec![Value::Int(1)]]);
}

#[test]
fn data_and_indexes_survive_reopening() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);
    let mut session = db.session();
    let s = &mut session;
    run(s, "CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(8))").unwrap();
    run(s, "CREATE INDEX t_name ON t (name)").unwrap();
    run(
        s,
        "INSERT INTO t VALUES (1, 'a'), (2, 'b'); DELETE FROM t WHERE id = 1",
    )
    .unwrap();
    drop(session);
    db.close().unwrap();

    let db = open(&dir);
    let indexes = db.catalog().table_indexes("t");
    let names: Vec<_> = indexes.iter().map(|i| i.name.as_str()).collect();
    assert_eq!(names, ["t_pkey", "t_name"]);
    assert_eq!(
        rows(&mut db.session(), "SELECT * FROM t"),
        [vec![Value::Int(2), Value::from("b")]]
    );
}

/// Committed rows carry their transaction's id, so a reopened database
/// must hand out ids above them to see them.
#[test]
fn transaction_ids_continue_after_reopening() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);
    let mut session = db.session();
    let s = &mut session;
    run(s, "CREATE TABLE t (id INT NOT NULL)").unwrap();
    for id in 1..=5 {
        run(s, &format!("INSERT INTO t VALUES ({id})")).unwrap();
    }
    let next_tid = db.catalog().next_tid().unwrap();
    assert!(next_tid > 5);
    drop(session);
    db.close().unwrap();

    let db = open(&dir);
    assert_eq!(db.catalog().next_tid().unwrap(), next_tid);
    let s = &mut db.session();
    assert_eq!(rows(s, "SELECT * FROM t").len(), 5);
    run(s, "UPDATE t SET id = id * 10 WHERE id = 5").unwrap();
    assert_eq!(rows(s, "SELECT * FROM t WHERE id = 50").len(), 1);
}

#[test]
fn a_second_open_is_refused() {
    let dir = TempDir::new().unwrap();
    let db = open(&dir);
    let err = Database::open(dir.path().join("test.db")).err().unwrap();
    assert!(err.to_string().contains("already open"), "{err}");
    drop(db);
    open(&dir);
}
