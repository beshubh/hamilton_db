//! The database the CLI runs statements against: its files, buffer pool
//! and catalog, and the pipeline from SQL to rows.

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use catalog::Catalog;
use common::{Column, Value};
use execution::execution_context::ExecutionContext;
use execution::executors::{Executor, create_executor};
use parser::Statement;
use planner::{Binder, BoundStatement, Planner};
use storage::buffer::buffer_pool_manager::BufferPoolManager;
use storage::disk::disk_manager::DiskManager;

/// Pages kept in memory.
const NUM_FRAMES: usize = 256;

/// Rows pulled from the root executor at a time.
const BATCH_SIZE: usize = 128;

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

pub struct Database {
    disk: Arc<DiskManager>,
    bpm: Arc<BufferPoolManager>,
    catalog: Arc<Catalog>,
    ctx: Arc<ExecutionContext>,
}

impl Database {
    /// Opens the database stored at `path`, creating it if there is none.
    /// Its `.meta` and `.log` files sit next to it.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let disk = Arc::new(DiskManager::new(path)?);
        let bpm = Arc::new(BufferPoolManager::new(NUM_FRAMES, Arc::clone(&disk)));
        let catalog = Arc::new(Catalog::open(Arc::clone(&bpm))?);
        Ok(Self {
            disk,
            bpm,
            ctx: Arc::new(ExecutionContext::new(Arc::clone(&catalog))),
            catalog,
        })
    }

    pub fn catalog(&self) -> &Arc<Catalog> {
        &self.catalog
    }

    /// Runs the statements in `sql` in order, stopping at the first that
    /// fails. Everything written is on disk when this returns, whether or
    /// not a statement failed: there is no write-ahead log to recover from.
    pub fn execute(&self, sql: &str) -> Result<Vec<Output>> {
        let result = parser::parse(sql)
            .map_err(Into::into)
            .and_then(|stmts| stmts.iter().map(|stmt| self.run(stmt)).collect());
        self.flush()?;
        result
    }

    /// Writes everything to disk and closes the files.
    pub fn close(self) -> Result<()> {
        self.bpm.flush_all_pages()?;
        self.disk.shut_down()?;
        Ok(())
    }

    fn run(&self, stmt: &Statement) -> Result<Output> {
        let catalog = self.ctx.catalog();
        let bound = Binder::new(catalog).bind(stmt)?;
        let plan = Planner::new(catalog).plan(&bound)?;
        let mut executor = create_executor(&self.ctx, &plan)?;
        executor.init()?;
        let (mut tuples, mut rids) = (Vec::new(), Vec::new());
        let mut rows = Vec::new();
        while executor.next(&mut tuples, &mut rids, BATCH_SIZE)? {
            for tuple in &tuples {
                rows.push(tuple.decode(executor.get_output_schema())?);
            }
        }
        let command = match bound {
            BoundStatement::CreateTable(_) => return Ok(Output::Done("CREATE TABLE")),
            BoundStatement::CreateIndex(_) => return Ok(Output::Done("CREATE INDEX")),
            BoundStatement::Select(_) => {
                let columns = executor.get_output_schema().columns().to_vec();
                return Ok(Output::Rows { columns, rows });
            }
            BoundStatement::Insert(_) => "INSERT",
            BoundStatement::Update(_) => "UPDATE",
            BoundStatement::Delete(_) => "DELETE",
        };
        match rows.as_slice() {
            [row] => match row.as_slice() {
                [Value::BigInt(count)] => Ok(Output::Count {
                    command,
                    count: *count,
                }),
                _ => unreachable!("{command} returns one BIGINT, not {row:?}"),
            },
            _ => unreachable!("{command} returns one row, not {}", rows.len()),
        }
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
        let indexes = db.ctx.catalog().table_indexes("t");
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
