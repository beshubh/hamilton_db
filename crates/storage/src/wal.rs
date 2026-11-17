//! The write-ahead log, checkpoints and restart recovery.
//!
//! See docs/internals/recovery.md.

pub mod checkpoint;
pub mod log;
pub mod mtr;
pub mod op;
pub mod record;
mod recovery;
pub mod txn_log;
pub(crate) mod undo;

use std::fmt;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

pub use mtr::{Logged, Mtr, MtrPage};
pub use op::PageOp;
pub use txn_log::TxnLog;

use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::disk::disk_manager::DiskManager;
use crate::disk::fs::{Fs, OsFs};
use crate::error::Result;
use checkpoint::Checkpointer;
use log::Log;

/// How often `open` has a fuzzy checkpoint taken.
pub const CHECKPOINT_INTERVAL: Duration = Duration::from_secs(30);

/// The size `open` lets a log segment grow to before starting the next.
/// Starting a segment costs two syncs on the flush that needs it, and
/// removing one costs a directory sync, so a segment is as large as the log
/// growth that triggers a checkpoint, which then removes about one.
pub const SEGMENT_SIZE: u64 = 64 << 20;

/// An open database: its buffer pool, and the thread that takes its
/// checkpoints. Dropping it stops the thread and writes nothing, as a crash
/// would; `close` takes a sharp checkpoint first.
pub struct Storage {
    pub bpm: Arc<BufferPoolManager>,
    checkpointer: Option<Checkpointer>,
}

impl Storage {
    /// Stops the checkpoints and takes a sharp one, so the next open has
    /// nothing to redo. Nothing else may still be changing pages.
    pub fn close(mut self) -> Result<()> {
        drop(self.checkpointer.take());
        self.bpm.checkpoint()
    }
}

/// Opens the database stored at `path`, creating it if there is none, with
/// `pool_size` frames. Its `.meta` file and its log's segments, from
/// `.log.00000001` on, sit next to it. Recovers whatever a crash left behind
/// first: every change the log holds is back in the pages when this
/// returns. Fails with `DatabaseLocked` if another process has it open.
pub fn open(path: impl AsRef<Path>, pool_size: usize) -> Result<Storage> {
    open_with(
        Arc::new(OsFs),
        path,
        pool_size,
        Some(CHECKPOINT_INTERVAL),
        SEGMENT_SIZE,
    )
}

/// `open` on the files of `fs`, taking a fuzzy checkpoint every
/// `checkpoint_interval`, or never, and starting a log segment once the
/// last holds `segment_size` bytes.
pub fn open_with(
    fs: Arc<dyn Fs>,
    path: impl AsRef<Path>,
    pool_size: usize,
    checkpoint_interval: Option<Duration>,
    segment_size: u64,
) -> Result<Storage> {
    let path = path.as_ref();
    // Opening the log writes to it, so it waits for the disk manager to
    // lock the database.
    let disk = Arc::new(DiskManager::open(Arc::clone(&fs), path)?);
    let log = Log::open(fs, path.with_extension("log"), segment_size)?;
    let bpm = Arc::new(BufferPoolManager::new(pool_size, disk, log));
    recovery::recover(&bpm)?;
    let checkpointer =
        checkpoint_interval.map(|interval| Checkpointer::start(Arc::clone(&bpm), interval));
    Ok(Storage { bpm, checkpointer })
}

/// Stops the process on a failure that leaves a change in memory the log
/// cannot account for. Unwinding would let the change reach disk, or let a
/// caller take back a commit that a crash keeps; recovery finishes the work
/// from the log instead.
pub(crate) fn fatal(why: fmt::Arguments<'_>) -> ! {
    eprintln!("hamilton: {why}");
    std::process::abort()
}

/// Runs `f` until it gets a frame. Work that must finish once begun, such
/// as stamping a durable commit or undoing a write, would otherwise stop
/// the process when other transactions briefly pin every frame.
pub(crate) fn when_a_frame_is_free<T>(mut f: impl FnMut() -> Result<T>) -> Result<T> {
    loop {
        match f() {
            Err(crate::error::StorageError::NoFreeFrame) => std::thread::yield_now(),
            result => return result,
        }
    }
}

/// Stops the process if dropped while unwinding, for work that must not
/// stop halfway once it began, such as stamping a commit that is durable.
pub(crate) struct NoUnwind(pub(crate) &'static str);

impl Drop for NoUnwind {
    fn drop(&mut self) {
        if std::thread::panicking() {
            fatal(format_args!("panicked while {}", self.0));
        }
    }
}
