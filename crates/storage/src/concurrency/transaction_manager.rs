use std::collections::BTreeMap;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use common::TxnId;
use parking_lot::{Condvar, Mutex, MutexGuard};

use crate::concurrency::read_ts::ReadTimestamps;
use crate::concurrency::transaction::{DeadTuples, Transaction, TransactionState};
use crate::concurrency::watermark::Watermark;
use crate::error::Result;
use crate::table::undo_log::UndoLog;

/// Begins and ends transactions. It owns the watermark, the one undo log
/// every table's versions go into, and the read timestamps of every table's
/// tuples, and a background thread that collects garbage from them.
///
/// Garbage is collected per transaction. A transaction that ends leaves
/// some tuples deleted (see `Transaction::finish`). Once the watermark
/// passes its Tid, no running or future reader can see them, and a pass of
/// `garbage_collect` reclaims them all at once, with the undo records and
/// read timestamps below the watermark.
///
/// Commit and abort only wake the collector thread and return, so the work
/// stays off their latency. Wakes that arrive during a pass are served by one
/// more pass. Collecting late is safe: the watermark only grows, so whatever
/// was garbage stays garbage. Dropping the manager runs the pass still owed,
/// then stops the thread.
///
/// **Known gaps.** Tuples deleted outside a transaction are not collected,
/// and reclaimed tuples keep their slots (see `table_page`).
pub struct TransactionManager {
    shared: Arc<Shared>,
    collector: Option<JoinHandle<()>>,
}

/// What the manager shares with its collector thread.
struct Shared {
    watermark: Arc<Watermark>,
    undo: Arc<UndoLog>,
    read_ts: Arc<ReadTimestamps>,
    /// The tuples each ended transaction left deleted, by Tid, until the
    /// watermark passes it.
    garbage: Mutex<BTreeMap<TxnId, DeadTuples>>,
    passes: Mutex<Passes>,
    /// Wakes the collector when a pass is requested or it should stop.
    wake: Condvar,
    /// Wakes `wait_for_gc` callers when a pass finishes.
    finished: Condvar,
}

/// Passes are counted so that a waiter knows when one that began after it
/// asked has finished.
#[derive(Default)]
struct Passes {
    requested: u64,
    /// The `requested` count that the last finished pass began at.
    done: u64,
    stop: bool,
}

impl TransactionManager {
    /// A manager whose first transaction gets Tid `next_tid`, with its
    /// collector thread started. See `Watermark::new`.
    pub fn new(next_tid: TxnId) -> Self {
        let shared = Arc::new(Shared {
            watermark: Arc::new(Watermark::new(next_tid)),
            undo: Arc::new(UndoLog::new()),
            read_ts: Arc::new(ReadTimestamps::new()),
            garbage: Mutex::new(BTreeMap::new()),
            passes: Mutex::new(Passes::default()),
            wake: Condvar::new(),
            finished: Condvar::new(),
        });
        let collector = {
            let shared = Arc::clone(&shared);
            thread::Builder::new()
                .name("gc".into())
                .spawn(move || shared.run_collector())
                .expect("failed to start the garbage collector thread")
        };
        Self {
            shared,
            collector: Some(collector),
        }
    }

    pub fn begin(&self) -> Arc<Transaction> {
        let tid = self.shared.watermark.begin();
        Arc::new(Transaction::new(
            tid,
            Arc::clone(&self.shared.undo),
            Arc::clone(&self.shared.read_ts),
            Arc::clone(&self.shared.watermark),
        ))
    }

    /// Commits `txn`: unlocks the versions it wrote, which makes them
    /// visible to newer transactions. Fails with `TransactionNotRunning` if
    /// it has already ended. If unlocking a tuple fails, the error is
    /// returned and `txn` stays running, partly committed; there is no log
    /// to finish or undo it.
    pub fn commit(&self, txn: &Transaction) -> Result<()> {
        self.end(txn, TransactionState::Committed)
    }

    /// Aborts `txn`: puts back every tuple it wrote as it was before. Fails
    /// as `commit` does.
    pub fn abort(&self, txn: &Transaction) -> Result<()> {
        self.end(txn, TransactionState::Aborted)
    }

    /// Runs a pass of garbage collection on the calling thread, alongside
    /// the collector's. A pass frees everything no running or future reader
    /// can reach: the undo records and read timestamps below the watermark,
    /// and the tuples left deleted by transactions below it.
    ///
    /// Passes may run at once; two never reclaim the same tuple. If
    /// reclaiming a tuple fails, the others are still reclaimed and the first
    /// error is returned; that tuple stays on its page, wasting its space,
    /// and is not retried.
    pub fn garbage_collect(&self) -> Result<()> {
        self.shared.garbage_collect()
    }

    /// Wakes the collector and blocks until it finishes a pass that began
    /// after this call, so everything ended before the call has been
    /// collected as far as the watermark allows.
    ///
    /// Panics if the collector thread has died.
    pub fn wait_for_gc(&self) {
        let mut passes = self.shared.request_pass();
        let target = passes.requested;
        while passes.done < target {
            self.shared
                .finished
                .wait_for(&mut passes, Duration::from_millis(100));
            let dead = self.collector.as_ref().is_none_or(|c| c.is_finished());
            assert!(
                passes.done >= target || !dead,
                "the garbage collector thread died"
            );
        }
    }

    pub fn undo_log(&self) -> &Arc<UndoLog> {
        &self.shared.undo
    }

    pub fn read_timestamps(&self) -> &Arc<ReadTimestamps> {
        &self.shared.read_ts
    }

    /// Number of deleted tuples waiting for the watermark to pass the
    /// transaction that left them.
    pub fn garbage_len(&self) -> usize {
        self.shared.garbage.lock().values().map(Vec::len).sum()
    }

    /// The oldest Tid any running transaction reads at. See `Watermark`.
    pub fn watermark(&self) -> TxnId {
        self.shared.watermark.get()
    }

    fn end(&self, txn: &Transaction, outcome: TransactionState) -> Result<()> {
        debug_assert!(
            Arc::ptr_eq(txn.undo_log(), &self.shared.undo),
            "transaction {} was begun by another manager",
            txn.tid()
        );
        let deleted = txn.finish(outcome)?;
        // Queued before the Tid ends, so the watermark cannot pass it first.
        if !deleted.is_empty() {
            self.shared.garbage.lock().insert(txn.tid(), deleted);
        }
        self.shared.watermark.end(txn.tid());
        drop(self.shared.request_pass());
        Ok(())
    }
}

impl Drop for TransactionManager {
    fn drop(&mut self) {
        self.shared.passes.lock().stop = true;
        self.shared.wake.notify_one();
        if let Some(collector) = self.collector.take() {
            // A collector that panicked has already reported it.
            let _ = collector.join();
        }
    }
}

impl Shared {
    /// Asks the collector for a pass, returning the lock on the counts.
    fn request_pass(&self) -> MutexGuard<'_, Passes> {
        let mut passes = self.passes.lock();
        passes.requested += 1;
        self.wake.notify_one();
        passes
    }

    /// The collector thread: one pass for every batch of requests, until
    /// asked to stop with none left owed.
    fn run_collector(&self) {
        let mut passes = self.passes.lock();
        loop {
            while passes.done == passes.requested && !passes.stop {
                self.wake.wait(&mut passes);
            }
            if passes.done == passes.requested {
                return;
            }
            let began_at = passes.requested;
            // The transaction that asked has ended; a tuple that fails to be
            // reclaimed only wastes space.
            MutexGuard::unlocked(&mut passes, || {
                let _ = self.garbage_collect();
            });
            passes.done = began_at;
            self.finished.notify_all();
        }
    }

    /// One pass. See `TransactionManager::garbage_collect`.
    fn garbage_collect(&self) -> Result<()> {
        let watermark = self.watermark.get();
        self.undo.collect(watermark);
        self.read_ts.collect(watermark);
        let dead = {
            let mut garbage = self.garbage.lock();
            let kept = garbage.split_off(&watermark);
            std::mem::replace(&mut *garbage, kept)
        };
        let mut first_error = None;
        for (heap, rid) in dead.into_values().flatten() {
            match heap.reclaim(rid, watermark) {
                Ok(true) => self.read_ts.remove(rid),
                Ok(false) => {}
                Err(e) => {
                    first_error.get_or_insert(e);
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }
}
