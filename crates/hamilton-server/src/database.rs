//! The database the server runs: its files, buffer pool and catalog, and
//! the sessions that run statements on them, one per connection.

use std::fmt;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use catalog::Catalog;
use planner::{EngineError, ExecutionEngine, Output, TransactionStatus};
use storage::StorageError;
use storage::buffer::buffer_pool_manager::{BufferPoolManager, DEFAULT_POOL_SIZE};
use storage::disk::disk_manager::DiskManager;

pub struct Database {
    disk: Arc<DiskManager>,
    bpm: Arc<BufferPoolManager>,
    catalog: Arc<Catalog>,
    /// Never runs a statement. Sessions are made from it so that they share
    /// its transaction manager.
    root: ExecutionEngine,
}

impl Database {
    /// Opens the database stored at `path`, creating it if there is none.
    /// Its `.meta` and `.log` files sit next to it. Fails if another process
    /// has it open.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let disk = Arc::new(DiskManager::new(path)?);
        let bpm = Arc::new(BufferPoolManager::new(DEFAULT_POOL_SIZE, Arc::clone(&disk)));
        let catalog = Arc::new(Catalog::open(Arc::clone(&bpm))?);
        let root = ExecutionEngine::new(Arc::clone(&catalog))?;
        Ok(Self {
            disk,
            bpm,
            catalog,
            root,
        })
    }

    pub fn catalog(&self) -> &Arc<Catalog> {
        &self.catalog
    }

    /// A new session, outside a transaction block.
    pub fn session(&self) -> Session<'_> {
        Session {
            db: self,
            engine: self.root.session(),
        }
    }

    /// Writes everything to disk and closes the files. Every session has
    /// been dropped by now, rolling back the blocks they had open.
    pub fn close(self) -> Result<()> {
        // Dropping the last engine stops the garbage collector, which
        // writes pages.
        drop(self.root);
        self.bpm.flush_all_pages()?;
        self.disk.shut_down()?;
        Ok(())
    }

    fn flush(&self) -> Result<(), StorageError> {
        self.bpm.flush_all_pages()?;
        self.disk.checkpoint()
    }
}

/// One connection's statements and transaction state. Dropping it rolls
/// back the block it has open.
pub struct Session<'db> {
    db: &'db Database,
    engine: ExecutionEngine,
}

impl Session<'_> {
    pub fn status(&self) -> TransactionStatus {
        self.engine.status()
    }

    /// Runs the statements in `sql` in order, handing each one's output to
    /// `each`, and stops at the first that fails.
    ///
    /// Everything is written to disk after each statement that leaves the
    /// session outside a transaction block: one that ran in a transaction of
    /// its own, COMMIT and ROLLBACK. That includes what other sessions have
    /// written in blocks still open. There is no write-ahead log to recover
    /// from.
    pub fn execute(&mut self, sql: &str, mut each: impl FnMut(Output)) -> Result<(), Error> {
        let stmts = self.engine.parse(sql)?;
        for stmt in &stmts {
            let result = self.engine.execute(stmt);
            if self.engine.status() == TransactionStatus::Idle {
                self.db.flush().map_err(Error::Flush)?;
            }
            each(result?);
        }
        Ok(())
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
    /// The statement ran, but writing to disk afterwards failed.
    Flush(StorageError),
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
            Error::Flush(err) => err,
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
