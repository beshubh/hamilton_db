//! The database the CLI runs statements against: its files, buffer pool
//! and catalog, and the sessions that run statements on them.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

use anyhow::Result;
use catalog::Catalog;
use common::{Column, Value};
use planner::{ExecutionEngine, TransactionStatus};
use storage::buffer::buffer_pool_manager::{BufferPoolManager, DEFAULT_POOL_SIZE};
use storage::disk::disk_manager::DiskManager;

/// The session statements run in until `\session` picks another.
pub const DEFAULT_SESSION: &str = "main";

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
    sessions: Mutex<Sessions>,
}

/// Named sessions, each with its own transaction state, so that one CLI can
/// interleave transactions. Statements run in the current one.
struct Sessions {
    current: String,
    engines: BTreeMap<String, ExecutionEngine>,
}

impl Sessions {
    fn current(&mut self) -> &mut ExecutionEngine {
        self.engines
            .get_mut(&self.current)
            .expect("the current session exists")
    }
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
            sessions: Mutex::new(Sessions {
                current: DEFAULT_SESSION.to_owned(),
                engines: BTreeMap::from([(DEFAULT_SESSION.to_owned(), engine)]),
            }),
        })
    }

    pub fn catalog(&self) -> &Arc<Catalog> {
        &self.catalog
    }

    /// The name of the session statements run in.
    pub fn session(&self) -> String {
        self.sessions().current.clone()
    }

    /// Makes `name` the session statements run in, opening it if it is new.
    pub fn switch_session(&self, name: &str) {
        let mut sessions = self.sessions();
        if !sessions.engines.contains_key(name) {
            let engine = sessions.current().session();
            sessions.engines.insert(name.to_owned(), engine);
        }
        sessions.current = name.to_owned();
    }

    /// Whether the current session is in a transaction block, and if it
    /// failed.
    pub fn transaction_status(&self) -> TransactionStatus {
        self.sessions().current().status()
    }

    /// Runs the statements in `sql` in order in the current session,
    /// stopping at the first that fails.
    ///
    /// Everything is written to disk after each statement that leaves the
    /// session outside a transaction block: one that ran in a transaction of
    /// its own, COMMIT and ROLLBACK. That includes what other sessions have
    /// written in blocks still open. There is no write-ahead log to recover
    /// from.
    pub fn execute(&self, sql: &str) -> Result<Vec<Output>> {
        let mut sessions = self.sessions();
        let engine = sessions.current();
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

    /// Rolls back every open transaction block, writes everything to disk
    /// and closes the files.
    pub fn close(self) -> Result<()> {
        let sessions = self
            .sessions
            .into_inner()
            .unwrap_or_else(|e| e.into_inner());
        for mut engine in sessions.engines.into_values() {
            engine.close()?;
            // Dropping the last engine stops the garbage collector, which
            // writes pages.
            drop(engine);
        }
        self.bpm.flush_all_pages()?;
        self.disk.shut_down()?;
        Ok(())
    }

    fn sessions(&self) -> MutexGuard<'_, Sessions> {
        self.sessions.lock().unwrap_or_else(|e| e.into_inner())
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

    #[test]
    fn sessions_run_their_own_transactions() {
        let dir = TempDir::new().unwrap();
        let db = Database::open(dir.path().join("test.db")).unwrap();
        db.execute("CREATE TABLE t (id INT NOT NULL)").unwrap();
        // b begins first: a reader newer than a's insert would conflict.
        db.switch_session("b");
        db.execute("BEGIN").unwrap();
        db.switch_session("a");
        db.execute("BEGIN; INSERT INTO t VALUES (1)").unwrap();
        db.switch_session("b");
        assert_eq!(db.transaction_status(), TransactionStatus::InBlock);
        assert!(rows(&one(&db, "SELECT * FROM t")).is_empty());
        db.switch_session("a");
        assert_eq!(one(&db, "COMMIT"), Output::Done("COMMIT"));
        db.switch_session(DEFAULT_SESSION);
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
        let next_tid = db.sessions().current().transaction_manager().next_tid();
        assert!(next_tid > 5);
        db.close().unwrap();

        let db = Database::open(&path).unwrap();
        assert_eq!(
            db.sessions().current().transaction_manager().next_tid(),
            next_tid
        );
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
