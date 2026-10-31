//! The database the CLI runs statements against: its files, buffer pool
//! and catalog, and the session that runs statements on them.

use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use catalog::Catalog;
use common::{Column, Value};
use planner::{ExecutionEngine, TransactionStatus};
use storage::buffer::buffer_pool_manager::{BufferPoolManager, DEFAULT_POOL_SIZE};
use storage::disk::disk_manager::DiskManager;

/// What a statement produced, for printing.
#[derive(Debug, PartialEq)]
pub enum Output {
    /// The rows of a SELECT.
    Rows {
        columns: Vec<Column>,
        rows: Vec<Vec<Value>>,
    },
    /// How many rows an INSERT, UPDATE or DELETE changed.
    Count { command: &'static str, count: i64 },
    /// A statement that reports only that it ran.
    Done(&'static str),
}

impl From<planner::Output> for Output {
    fn from(output: planner::Output) -> Self {
        match output {
            planner::Output::Rows { columns, rows } => Output::Rows { columns, rows },
            planner::Output::Count { command, count } => Output::Count { command, count },
            planner::Output::Done(command) => Output::Done(command),
        }
    }
}

pub struct Database {
    disk: Arc<DiskManager>,
    bpm: Arc<BufferPoolManager>,
    catalog: Arc<Catalog>,
    /// The CLI's one session.
    engine: Mutex<ExecutionEngine>,
}

impl Database {
    /// Opens the database stored at `path`, creating it if there is none.
    /// Its `.meta` and `.log` files sit next to it.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let disk = Arc::new(DiskManager::new(path)?);
        let bpm = Arc::new(BufferPoolManager::new(DEFAULT_POOL_SIZE, Arc::clone(&disk)));
        let catalog = Arc::new(Catalog::open(Arc::clone(&bpm))?);
        let engine = ExecutionEngine::new(Arc::clone(&catalog))?;
        Ok(Self {
            disk,
            bpm,
            catalog,
            engine: Mutex::new(engine),
        })
    }

    pub fn catalog(&self) -> &Arc<Catalog> {
        &self.catalog
    }

    /// Whether the session is in a transaction block, and if it failed.
    pub fn transaction_status(&self) -> TransactionStatus {
        self.engine().status()
    }

    /// Runs the statements in `sql` in order, stopping at the first that
    /// fails.
    ///
    /// Everything is written to disk after each statement that leaves the
    /// session outside a transaction block: one that ran in a transaction of
    /// its own, COMMIT and ROLLBACK. Statements in a block are not flushed
    /// until it ends. There is no write-ahead log to recover from.
    pub fn execute(&self, sql: &str) -> Result<Vec<Output>> {
        let mut engine = self.engine();
        let stmts = engine.parse(sql)?;
        let mut outputs = Vec::with_capacity(stmts.len());
        for stmt in &stmts {
            let result = engine.execute(stmt);
            if engine.status() == TransactionStatus::Idle {
                self.flush()?;
            }
            outputs.push(result?.into());
        }
        Ok(outputs)
    }

    /// Rolls back an open transaction block, writes everything to disk and
    /// closes the files.
    pub fn close(self) -> Result<()> {
        let mut engine = self.engine.into_inner().unwrap_or_else(|e| e.into_inner());
        engine.close()?;
        // Dropping the engine stops its garbage collector, which writes pages.
        drop(engine);
        self.bpm.flush_all_pages()?;
        self.disk.shut_down()?;
        Ok(())
    }

    fn engine(&self) -> std::sync::MutexGuard<'_, ExecutionEngine> {
        self.engine.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn flush(&self) -> Result<()> {
        self.bpm.flush_all_pages()?;
        self.disk.checkpoint()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use common::DataType;
    use tempfile::TempDir;

    use super::*;

    fn rows(output: &Output) -> &[Vec<Value>] {
        match output {
            Output::Rows { rows, .. } => rows,
            output => panic!("{output:?}"),
        }
    }

    fn one(db: &Database, sql: &str) -> Output {
        let mut outputs = db.execute(sql).unwrap();
        assert_eq!(outputs.len(), 1, "{sql}");
        outputs.remove(0)
    }

    #[test]
    fn runs_statements() {
        let dir = TempDir::new().unwrap();
        let db = Database::open(dir.path().join("test.db")).unwrap();
        assert_eq!(
            one(&db, "CREATE TABLE t (id INT NOT NULL, name VARCHAR(8))"),
            Output::Done("CREATE TABLE")
        );
        assert_eq!(
            one(&db, "INSERT INTO t VALUES (1, 'a'), (2, NULL), (3, 'c')"),
            Output::Count {
                command: "INSERT",
                count: 3
            }
        );
        let output = one(&db, "SELECT name, id * 10 AS ten FROM t WHERE id > 1");
        let Output::Rows { columns, rows } = output else {
            panic!("{output:?}");
        };
        assert_eq!(
            columns,
            [
                Column::new("name", DataType::Varchar(8), true),
                Column::new("ten", DataType::Int, false),
            ]
        );
        assert_eq!(
            rows,
            [
                vec![Value::Null, Value::Int(20)],
                vec![Value::from("c"), Value::Int(30)],
            ]
        );
        assert_eq!(
            one(&db, "UPDATE t SET name = 'z' WHERE id >= 2"),
            Output::Count {
                command: "UPDATE",
                count: 2
            }
        );
        assert_eq!(
            one(&db, "DELETE FROM t WHERE name = 'z'"),
            Output::Count {
                command: "DELETE",
                count: 2
            }
        );
    }

    #[test]
    fn explain_shows_the_plan() {
        let dir = TempDir::new().unwrap();
        let db = Database::open(dir.path().join("test.db")).unwrap();
        db.execute("CREATE TABLE t (id INT NOT NULL)").unwrap();
        let output = one(&db, "EXPLAIN INSERT INTO t VALUES (1), (2)");
        let Output::Rows {
            columns,
            rows: lines,
        } = output
        else {
            panic!("{output:?}");
        };
        assert_eq!(columns[0].name, "QUERY PLAN");
        assert_eq!(
            lines,
            [
                vec![Value::from("Insert on t")],
                vec![Value::from("  ->  Values (2 rows)")],
            ]
        );
        assert!(rows(&one(&db, "SELECT * FROM t")).is_empty());
    }

    #[test]
    fn creates_indexes() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let db = Database::open(&path).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(8))")
            .unwrap();
        assert_eq!(
            one(&db, "CREATE INDEX t_name ON t (name)"),
            Output::Done("CREATE INDEX")
        );
        db.close().unwrap();

        let db = Database::open(&path).unwrap();
        let indexes = db.catalog().table_indexes("t");
        let names: Vec<_> = indexes.iter().map(|i| i.name.as_str()).collect();
        assert_eq!(names, ["t_pkey", "t_name"]);
        assert!(indexes[0].is_primary_key);
    }

    #[test]
    fn errors_leave_the_database_usable() {
        let dir = TempDir::new().unwrap();
        let db = Database::open(dir.path().join("test.db")).unwrap();
        db.execute("CREATE TABLE t (id INT NOT NULL)").unwrap();
        for bad in [
            "SELEC 1",
            "SELECT * FROM missing",
            "INSERT INTO t VALUES (1), (NULL)",
            "SELECT 1 / 0",
        ] {
            assert!(db.execute(bad).is_err(), "{bad}");
        }
        // The failed INSERT inserted nothing.
        assert!(rows(&one(&db, "SELECT * FROM t")).is_empty());
        db.execute("INSERT INTO t VALUES (1)").unwrap();
        assert_eq!(rows(&one(&db, "SELECT * FROM t")), [vec![Value::Int(1)]]);
    }

    #[test]
    fn data_survives_reopening() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let db = Database::open(&path).unwrap();
        db.execute("CREATE TABLE t (id INT NOT NULL, name VARCHAR(8))")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')")
            .unwrap();
        db.execute("DELETE FROM t WHERE id = 1").unwrap();
        db.close().unwrap();

        let db = Database::open(&path).unwrap();
        assert_eq!(
            rows(&one(&db, "SELECT * FROM t")),
            [vec![Value::Int(2), Value::from("b")]]
        );
    }

    #[test]
    fn transaction_blocks_commit_and_roll_back() {
        let dir = TempDir::new().unwrap();
        let db = Database::open(dir.path().join("test.db")).unwrap();
        db.execute("CREATE TABLE t (id INT NOT NULL)").unwrap();
        let outputs = db
            .execute("BEGIN; INSERT INTO t VALUES (1); ROLLBACK")
            .unwrap();
        assert_eq!(outputs[0], Output::Done("BEGIN"));
        assert_eq!(outputs[2], Output::Done("ROLLBACK"));
        assert!(rows(&one(&db, "SELECT * FROM t")).is_empty());

        assert_eq!(one(&db, "BEGIN"), Output::Done("BEGIN"));
        assert_eq!(db.transaction_status(), TransactionStatus::InBlock);
        db.execute("INSERT INTO t VALUES (2)").unwrap();
        assert_eq!(rows(&one(&db, "SELECT * FROM t")), [vec![Value::Int(2)]]);
        assert_eq!(one(&db, "COMMIT"), Output::Done("COMMIT"));
        assert_eq!(db.transaction_status(), TransactionStatus::Idle);
        assert_eq!(rows(&one(&db, "SELECT * FROM t")), [vec![Value::Int(2)]]);
    }

    #[test]
    fn a_failed_block_reports_rollback() {
        let dir = TempDir::new().unwrap();
        let db = Database::open(dir.path().join("test.db")).unwrap();
        db.execute("CREATE TABLE t (id INT NOT NULL)").unwrap();
        db.execute("BEGIN; INSERT INTO t VALUES (1)").unwrap();
        assert!(db.execute("SELECT 1 / 0").is_err());
        assert_eq!(db.transaction_status(), TransactionStatus::Failed);
        let err = db.execute("SELECT * FROM t").unwrap_err();
        assert!(
            err.to_string().contains("current transaction is aborted"),
            "{err}"
        );
        assert_eq!(one(&db, "COMMIT"), Output::Done("ROLLBACK"));
        assert!(rows(&one(&db, "SELECT * FROM t")).is_empty());
    }

    #[test]
    fn closing_rolls_back_an_open_block() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let db = Database::open(&path).unwrap();
        db.execute("CREATE TABLE t (id INT NOT NULL)").unwrap();
        db.execute("INSERT INTO t VALUES (1)").unwrap();
        db.execute("BEGIN; INSERT INTO t VALUES (2)").unwrap();
        db.close().unwrap();

        let db = Database::open(&path).unwrap();
        assert_eq!(rows(&one(&db, "SELECT * FROM t")), [vec![Value::Int(1)]]);
    }

    /// Committed rows carry their transaction's id, so a reopened database
    /// must hand out ids above them to see them.
    #[test]
    fn transaction_ids_continue_after_reopening() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let db = Database::open(&path).unwrap();
        db.execute("CREATE TABLE t (id INT NOT NULL)").unwrap();
        for id in 1..=5 {
            db.execute(&format!("INSERT INTO t VALUES ({id})")).unwrap();
        }
        let next_tid = db.engine().transaction_manager().next_tid();
        assert!(next_tid > 5);
        db.close().unwrap();

        let db = Database::open(&path).unwrap();
        assert_eq!(db.engine().transaction_manager().next_tid(), next_tid);
        assert_eq!(rows(&one(&db, "SELECT * FROM t")).len(), 5);
        db.execute("UPDATE t SET id = id * 10 WHERE id = 5")
            .unwrap();
        assert_eq!(rows(&one(&db, "SELECT * FROM t WHERE id = 50")).len(), 1);
    }

    /// Each statement is flushed as it runs, so a session that ends without
    /// `close` (killed, or Ctrl-C) loses nothing.
    #[test]
    fn data_survives_without_close() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let db = Database::open(&path).unwrap();
        db.execute("CREATE TABLE t (id INT NOT NULL)").unwrap();
        db.execute("INSERT INTO t VALUES (7)").unwrap();
        drop(db);

        let db = Database::open(&path).unwrap();
        assert_eq!(rows(&one(&db, "SELECT * FROM t")), [vec![Value::Int(7)]]);
    }
}
