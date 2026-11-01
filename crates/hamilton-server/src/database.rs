//! The database the server runs: its files, buffer pool and catalog, and
//! the sessions that run statements on them, one per connection.

use std::fmt;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use anyhow::Result;
use catalog::Catalog;
use planner::{EngineError, ExecutionEngine, Output, TransactionStatus};
use storage::buffer::buffer_pool_manager::{BufferPoolManager, DEFAULT_POOL_SIZE};
use storage::disk::disk_manager::DiskManager;

/// How often dirty pages are written out while the server runs.
const CHECKPOINT_INTERVAL: Duration = Duration::from_secs(5);

pub struct Database {
    checkpointer: Checkpointer,
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
            checkpointer: Checkpointer::start(Arc::clone(&bpm), Arc::clone(&disk)),
            disk,
            bpm,
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

    /// Writes everything to disk and closes the files. Every session has
    /// been dropped by now, rolling back the blocks they had open.
    pub fn close(self) -> Result<()> {
        drop(self.checkpointer);
        // Dropping the last engine stops the garbage collector, which
        // writes pages.
        drop(self.root);
        self.bpm.flush_all_pages()?;
        self.disk.shut_down()?;
        Ok(())
    }
}

/// A thread that writes every dirty page to disk each
/// `CHECKPOINT_INTERVAL`, until dropped.
struct Checkpointer {
    shared: Arc<(Mutex<bool>, Condvar)>,
    thread: Option<JoinHandle<()>>,
}

impl Checkpointer {
    fn start(bpm: Arc<BufferPoolManager>, disk: Arc<DiskManager>) -> Self {
        let shared = Arc::new((Mutex::new(false), Condvar::new()));
        let thread = {
            let shared = Arc::clone(&shared);
            thread::Builder::new()
                .name("checkpoint".into())
                .spawn(move || {
                    let (stop, wake) = &*shared;
                    loop {
                        let stop = stop.lock().unwrap();
                        let (stop, _) = wake
                            .wait_timeout_while(stop, CHECKPOINT_INTERVAL, |stop| !*stop)
                            .unwrap();
                        if *stop {
                            return;
                        }
                        drop(stop);
                        if let Err(err) = bpm.flush_all_pages().and_then(|()| disk.checkpoint()) {
                            eprintln!("hamilton-server: checkpoint failed: {err}");
                        }
                    }
                })
                .expect("failed to start the checkpoint thread")
        };
        Self {
            shared,
            thread: Some(thread),
        }
    }
}

impl Drop for Checkpointer {
    fn drop(&mut self) {
        let (stop, wake) = &*self.shared;
        *stop.lock().unwrap() = true;
        wake.notify_one();
        if let Some(thread) = self.thread.take() {
            // A checkpoint thread that panicked has already reported it.
            let _ = thread.join();
        }
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
