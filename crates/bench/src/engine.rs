//! The two databases the queries run on, behind one interface. Both return
//! rows as text, the way Postgres sends them, so results can be compared.

use std::sync::Arc;

use anyhow::{Context, Result};
use catalog::Catalog;
use common::Value;
use execution::execution_context::ExecutionContext;
use execution::executors::{Executor, create_executor};
use planner::{Binder, Planner};
use postgres::{Client, NoTls, SimpleQueryMessage};
use storage::buffer::buffer_pool_manager::BufferPoolManager;
use storage::disk::disk_manager::DiskManager;
use tempfile::TempDir;

pub type Row = Vec<Option<String>>;

pub trait Engine {
    fn name(&self) -> &'static str;

    /// Runs one statement and returns the rows it produced.
    fn execute(&mut self, sql: &str) -> Result<Vec<Row>>;

    /// Called once the tables are loaded, before any query runs.
    fn finish_load(&mut self) -> Result<()>;
}

/// Rows pulled from the root executor at a time, as in the CLI.
const BATCH_SIZE: usize = 128;

/// A fresh hamilton database in a temporary directory, run through the
/// same pipeline as the CLI: parse, bind, plan (which optimizes), execute.
pub struct Hamilton {
    disk: Arc<DiskManager>,
    bpm: Arc<BufferPoolManager>,
    ctx: Arc<ExecutionContext>,
    _dir: TempDir,
}

impl Hamilton {
    pub fn new(frames: usize) -> Result<Self> {
        let dir = TempDir::new()?;
        let disk = Arc::new(DiskManager::new(dir.path().join("bench.db"))?);
        let bpm = Arc::new(BufferPoolManager::new(frames, Arc::clone(&disk)));
        let catalog = Arc::new(Catalog::open(Arc::clone(&bpm))?);
        Ok(Self {
            disk,
            bpm,
            ctx: Arc::new(ExecutionContext::new(catalog)),
            _dir: dir,
        })
    }
}

impl Engine for Hamilton {
    fn name(&self) -> &'static str {
        "hamilton"
    }

    fn execute(&mut self, sql: &str) -> Result<Vec<Row>> {
        let statements = parser::parse(sql)?;
        let [stmt] = statements.as_slice() else {
            anyhow::bail!("expected one statement, got {}", statements.len());
        };
        let catalog = self.ctx.catalog();
        let bound = Binder::new(catalog).bind(stmt)?;
        let plan = Planner::new(catalog).plan(&bound)?;
        let mut executor = create_executor(&self.ctx, &plan)?;
        executor.init()?;
        let (mut tuples, mut rids) = (Vec::new(), Vec::new());
        let mut rows = Vec::new();
        while executor.next(&mut tuples, &mut rids, BATCH_SIZE)? {
            for tuple in &tuples {
                let values = tuple.decode(executor.get_output_schema())?;
                rows.push(values.iter().map(text).collect());
            }
        }
        Ok(rows)
    }

    fn finish_load(&mut self) -> Result<()> {
        self.bpm.flush_all_pages()?;
        self.disk.checkpoint()?;
        Ok(())
    }
}

fn text(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        value => Some(value.to_string()),
    }
}

/// A Postgres database, emptied of the benchmark's tables on connect.
pub struct Postgres {
    client: Client,
}

impl Postgres {
    /// Connects to the server at `url`, creating database `db` there if it
    /// doesn't exist. Unless `parallel`, queries run on one core with no
    /// JIT, like hamilton.
    pub fn connect(url: &str, db: &str, parallel: bool) -> Result<Self> {
        let mut config: postgres::Config = url.parse().context("bad Postgres URL")?;
        let mut admin = config
            .connect(NoTls)
            .with_context(|| format!("connecting to {url}"))?;
        let exists = admin.query_opt("SELECT 1 FROM pg_database WHERE datname = $1", &[&db])?;
        if exists.is_none() {
            admin.batch_execute(&format!("CREATE DATABASE \"{db}\""))?;
        }
        config.dbname(db);
        let mut client = config.connect(NoTls)?;
        client.batch_execute("DROP TABLE IF EXISTS orders, products")?;
        if !parallel {
            client.batch_execute("SET max_parallel_workers_per_gather = 0; SET jit = off")?;
        }
        Ok(Self { client })
    }
}

impl Engine for Postgres {
    fn name(&self) -> &'static str {
        "postgres"
    }

    fn execute(&mut self, sql: &str) -> Result<Vec<Row>> {
        let messages = self.client.simple_query(sql)?;
        Ok(messages
            .iter()
            .filter_map(|message| match message {
                SimpleQueryMessage::Row(row) => Some(
                    (0..row.len())
                        .map(|i| row.get(i).map(str::to_owned))
                        .collect(),
                ),
                _ => None,
            })
            .collect())
    }

    fn finish_load(&mut self) -> Result<()> {
        // Sets the visibility map and planner statistics, as a table that
        // has been around a while would have.
        self.client
            .batch_execute("VACUUM ANALYZE orders, products")?;
        Ok(())
    }
}
