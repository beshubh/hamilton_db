//! The database the server runs: its files, buffer pool and catalog, and
//! the sessions that run statements on them, one per connection.

use std::fmt;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use catalog::{Catalog, SWEEP_BATCH};
use common::{DataType, Value};
use parser::Statement;
use planner::{Described, EngineError, ExecutionEngine, Output, TransactionStatus};
use storage::Storage;
use storage::buffer::buffer_pool_manager::DEFAULT_POOL_SIZE;
use storage::disk::fs::OsFs;
use storage::wal::{CHECKPOINT_INTERVAL, SEGMENT_SIZE};

pub struct Database {
    storage: Storage,
    catalog: Arc<Catalog>,
    /// Never runs a statement. Sessions are made from it so that they share
    /// its transaction manager.
    root: ExecutionEngine,
}

impl Database {
    /// Opens the database stored at `path`, creating it if there is none,
    /// recovers what a crash left behind, and collects the garbage that
    /// garbage collection lost with the restart (`Catalog::sweep`). Its
    /// `.meta` file and log segments sit next to it. Fails if another
    /// process has it open.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with(path, DEFAULT_POOL_SIZE, SEGMENT_SIZE)
    }

    /// `open` with `pool_size` frames in the buffer pool, starting a log
    /// segment, and taking a checkpoint, each time the log grows by
    /// `log_segment_size` bytes.
    pub fn open_with(
        path: impl AsRef<Path>,
        pool_size: usize,
        log_segment_size: u64,
    ) -> Result<Self> {
        let started = Instant::now();
        let storage = storage::open_with(
            Arc::new(OsFs),
            path,
            pool_size,
            Some(CHECKPOINT_INTERVAL),
            log_segment_size,
        )?;
        log::info!("recovered in {} ms", started.elapsed().as_millis());
        let catalog = Arc::new(Catalog::open(Arc::clone(&storage.bpm))?);
        let root = ExecutionEngine::new(Arc::clone(&catalog));
        let started = Instant::now();
        let swept = catalog.sweep(root.transaction_manager(), SWEEP_BATCH)?;
        log::info!(
            "swept {} dead tuples and {} index entries in {} ms",
            swept.tuples,
            swept.entries,
            started.elapsed().as_millis()
        );
        Ok(Self {
            storage,
            catalog,
            root,
        })
    }

    /// See [`ExecutionEngine::with_conflict_timeout`]. Zero turns retrying
    /// off.
    pub fn with_conflict_timeout(self, timeout: Duration) -> Self {
        Self {
            root: self.root.with_conflict_timeout(timeout),
            ..self
        }
    }

    pub fn catalog(&self) -> &Arc<Catalog> {
        &self.catalog
    }

    /// A new session, outside a transaction block.
    pub fn session(&self) -> Session<'_> {
        Session {
            db: PhantomData,
            engine: self.root.session(),
        }
    }

    /// Saves the sequences, takes a checkpoint and closes the files, so the
    /// next open has nothing to recover and skips no identity value. Every
    /// session has been dropped by now, rolling back the blocks they had
    /// open.
    pub fn close(self) -> Result<()> {
        // Dropping the last engine stops the garbage collector, which
        // writes pages.
        drop(self.root);
        self.catalog.save_sequences()?;
        self.storage.close()?;
        Ok(())
    }
}

/// One connection's statements and transaction state. Dropping it rolls
/// back the block it has open.
pub struct Session<'db> {
    db: PhantomData<&'db Database>,
    engine: ExecutionEngine,
}

impl Session<'_> {
    pub fn status(&self) -> TransactionStatus {
        self.engine.status()
    }

    /// Runs the statements in `sql` in order, handing each one's output to
    /// `each`, and stops at the first that fails.
    pub fn execute(&mut self, sql: &str, mut each: impl FnMut(Output)) -> Result<(), Error> {
        let stmts = self.engine.parse(sql)?;
        for stmt in &stmts {
            each(self.engine.execute(stmt)?);
        }
        Ok(())
    }

    /// See [`ExecutionEngine::prepare`].
    pub fn prepare(
        &mut self,
        sql: &str,
        given: &[Option<DataType>],
    ) -> Result<Option<(Statement, Described)>, Error> {
        Ok(self.engine.prepare(sql, given)?)
    }

    /// Runs a prepared statement with `$n` bound to `parameters[n - 1]`.
    pub fn execute_prepared(
        &mut self,
        stmt: &Statement,
        parameters: &[Value],
    ) -> Result<Output, Error> {
        Ok(self.engine.execute_with(stmt, parameters)?)
    }
}

/// Also keeps the database borrowed until the session is gone, so `close`
/// can't run while its engine still holds the transaction manager.
impl Drop for Session<'_> {
    fn drop(&mut self) {
        // The block's own error is the one that mattered; this is cleanup.
        let _ = self.engine.close();
    }
}

/// Why a statement failed.
#[derive(Debug)]
pub enum Error {
    Engine(EngineError),
}

impl From<EngineError> for Error {
    fn from(err: EngineError) -> Self {
        Error::Engine(err)
    }
}

impl fmt::Display for Error {
    /// The error and every error behind it, as `a: b: c`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let err: &dyn std::error::Error = match self {
            Error::Engine(err) => err,
        };
        write!(f, "{err}")?;
        let mut source = err.source();
        while let Some(err) = source {
            write!(f, ": {err}")?;
            source = err.source();
        }
        Ok(())
    }
}
