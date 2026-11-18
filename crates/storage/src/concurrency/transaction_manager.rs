use std::collections::BTreeMap;
use std::mem;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use common::{Rid, Tuple, TxnId};
use parking_lot::{Condvar, Mutex, MutexGuard, RwLock};

use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::concurrency::index_versions::{IndexedTable, TableEntry};
use crate::concurrency::read_ts::ReadTimestamps;
use crate::concurrency::transaction::{Garbage, Outcome, Transaction};
use crate::concurrency::watermark::Watermark;
use crate::error::Result;
use crate::index::{Index, IndexTrait, RowLocator};
use crate::table::table_heap::TableHeap;
use crate::table::undo_log::UndoLog;

/// Begins and ends transactions. It owns the watermark, the one undo log
/// every table's versions go into, and the read timestamps of every table's
/// tuples, and a background thread that collects garbage from them.
///
/// Garbage is collected per transaction. A transaction that ends leaves
/// some tuples deleted and some index entries retired (see
/// `Transaction::finish`). Once the watermark passes its Tid, no running or
/// future reader can see the tuples, and a pass of `garbage_collect`
/// reclaims them all at once, with the undo records and read timestamps
/// below the watermark. It removes each entry no reader needs any more, as
/// `index_versions` describes. The entries an index build added wait
/// longer, until every transaction running when the build ended has ended.
///
/// Commit and abort only wake the collector thread and return, so the work
/// stays off their latency. Wakes that arrive during a pass are served by one
/// more pass. Collecting late is safe: the watermark only grows, so whatever
/// was garbage stays garbage. Dropping the manager runs the pass still owed,
/// then stops the thread.
///
/// The queue is in memory, so a crash loses it, and some garbage never
/// joins it, such as the catalog rows an undo leaves deleted. `sweep_heap`
/// and `sweep_index` find garbage by scanning instead, and collect it as a
/// pass does.
///
/// **Known gap.** Reclaimed tuples keep their slots (see `table_page`).
pub struct TransactionManager {
    shared: Arc<Shared>,
    collector: Option<JoinHandle<()>>,
}

/// What the manager shares with its collector thread.
struct Shared {
    /// The database whose log the transactions write.
    bpm: Arc<BufferPoolManager>,
    watermark: Arc<Watermark>,
    undo: Arc<UndoLog>,
    read_ts: Arc<ReadTimestamps>,
    /// See `index_versions`.
    entry_lock: Arc<RwLock<()>>,
    /// What each ended transaction left, by Tid, until the watermark passes
    /// it.
    garbage: Mutex<BTreeMap<TxnId, Garbage>>,
    /// Passes collect garbage holding this shared, and run deferred work
    /// holding it exclusively, so that work such as freeing pages waits for
    /// another pass still collecting garbage on them.
    deferring: RwLock<()>,
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
    /// A manager of transactions on the database behind `bpm`, with its
    /// collector thread started. Its first Tid is above every Tid in the
    /// log (`Log::next_tid`), so it never takes a crashed transaction's rows
    /// for its own.
    pub fn new(bpm: Arc<BufferPoolManager>) -> Self {
        let next_tid = bpm.log().next_tid();
        let shared = Arc::new(Shared {
            bpm,
            watermark: Arc::new(Watermark::new(next_tid)),
            undo: Arc::new(UndoLog::new()),
            read_ts: Arc::new(ReadTimestamps::new()),
            entry_lock: Arc::new(RwLock::new(())),
            garbage: Mutex::new(BTreeMap::new()),
            deferring: RwLock::new(()),
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
            Arc::clone(&self.shared.bpm),
            Arc::clone(&self.shared.undo),
            Arc::clone(&self.shared.read_ts),
            Arc::clone(&self.shared.watermark),
            Arc::clone(&self.shared.entry_lock),
        ))
    }

    /// Commits `txn`: logs its `Commit` and returns once that is durable,
    /// having unlocked the versions it wrote, which makes them visible to
    /// newer transactions. Fails only with `TransactionNotRunning`, if it
    /// has already ended. Past the `Commit`, a failure stops the process,
    /// and recovery finishes the commit.
    pub fn commit(&self, txn: &Transaction) -> Result<()> {
        self.end(txn, Outcome::Committed)
    }

    /// Aborts `txn`: puts back every tuple and index entry it wrote, as its
    /// records in the log say. Fails as `commit` does.
    pub fn abort(&self, txn: &Transaction) -> Result<()> {
        self.end(txn, Outcome::Aborted)
    }

    /// Runs a pass of garbage collection on the calling thread, alongside
    /// the collector's. A pass frees everything no running or future reader
    /// can reach: the undo records and read timestamps below the watermark,
    /// and the tuples left deleted and index entries retired by transactions
    /// below it.
    ///
    /// Passes may run at once; two never reclaim the same tuple. If
    /// reclaiming a tuple or removing an entry fails, the others are still
    /// done and the first error is returned; that tuple stays on its page,
    /// wasting its space, or that entry in its index, and is not retried.
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

    /// Reclaims every tuple of `heap` that a pass would, found by scanning
    /// the heap rather than in the queue. Returns how many it reclaimed.
    pub fn sweep_heap(&self, heap: &TableHeap) -> Result<usize> {
        let watermark = self.shared.watermark.get();
        let mut reclaimed = 0;
        for rids in heap.reclaimable(watermark) {
            for rid in rids? {
                reclaimed += usize::from(self.shared.reclaim(heap, rid, watermark)?);
            }
        }
        Ok(reclaimed)
    }

    /// Removes every entry of `index`, one of `table`'s, that a pass would
    /// remove if it were retired: each one no version a reader at or above
    /// the watermark might see needs. Holds at most `batch` entries at a
    /// time. Returns how many it removed.
    pub fn sweep_index(
        &self,
        table: &Arc<IndexedTable>,
        index: &Arc<Index>,
        batch: usize,
    ) -> Result<usize> {
        let watermark = self.shared.watermark.get();
        let Index::BtreeIndex(btree) = &**index;
        let mut removed = 0;
        let mut entries = Vec::with_capacity(batch);
        let mut after: Option<(Tuple, RowLocator)> = None;
        loop {
            // A removal may need the leaf the scan holds, so a batch's
            // removals wait for its scan's end.
            let scan = btree.entries_after(after.as_ref().map(|(key, locator)| (key, locator)))?;
            for entry in scan.take(batch) {
                let (key, locator) = entry?;
                entries.push(TableEntry {
                    table: Arc::clone(table),
                    index: Arc::clone(index),
                    key,
                    locator,
                });
            }
            let Some(last) = entries.last() else {
                return Ok(removed);
            };
            after = Some((last.key.clone(), last.locator.clone()));
            let full = entries.len() == batch;
            for entry in entries.drain(..) {
                removed += usize::from(self.shared.remove_entry(&entry, watermark)?);
            }
            if !full {
                return Ok(removed);
            }
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
        let garbage = self.shared.garbage.lock();
        garbage.values().map(|garbage| garbage.tuples.len()).sum()
    }

    /// The Tid the next transaction will get.
    pub fn next_tid(&self) -> TxnId {
        self.shared.watermark.next()
    }

    /// Blocks until transaction `tid` has committed or aborted, or until
    /// `deadline`. Returns whether it has ended.
    pub fn wait_for_end(&self, tid: TxnId, deadline: Instant) -> bool {
        self.shared.watermark.wait_for_end(tid, deadline)
    }

    /// The oldest Tid any running transaction reads at. See `Watermark`.
    pub fn watermark(&self) -> TxnId {
        self.shared.watermark.get()
    }

    fn end(&self, txn: &Transaction, outcome: Outcome) -> Result<()> {
        debug_assert!(
            Arc::ptr_eq(txn.undo_log(), &self.shared.undo),
            "transaction {} was begun by another manager",
            txn.tid()
        );
        let mut garbage = txn.finish(outcome)?;
        // Queued before the Tid ends, so the watermark cannot pass it first.
        // An index build's entries, and deferred work, wait for the newest
        // Tid begun so far, so every transaction running now ends first (see
        // `index_versions`).
        // Another transaction may queue at that Tid too, so queues merge.
        let built = Garbage {
            built: mem::take(&mut garbage.built),
            deferred: mem::take(&mut garbage.deferred),
            ..Garbage::default()
        };
        let newest = self.shared.watermark.next() - 1;
        let mut queue = self.shared.garbage.lock();
        for (tid, garbage) in [(txn.tid(), garbage), (newest, built)] {
            if !garbage.is_empty() {
                queue.entry(tid).or_default().append(garbage);
            }
        }
        drop(queue);
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
        // Taken before `dead`, so a pass that took older garbage is still
        // counted when another takes newer deferred work.
        let collecting = self.deferring.read();
        let watermark = self.watermark.get();
        self.undo.collect(watermark);
        self.read_ts.collect(watermark);
        let dead = {
            let mut garbage = self.garbage.lock();
            let kept = garbage.split_off(&watermark);
            std::mem::replace(&mut *garbage, kept)
        };
        let mut first_error = None;
        let mut deferred = Vec::new();
        for mut garbage in dead.into_values() {
            deferred.append(&mut garbage.deferred);
            let tuples = garbage.tuples.iter();
            let tuples = tuples.map(|(heap, rid)| self.reclaim(heap, *rid, watermark));
            let entries = garbage.entries.iter().chain(&garbage.built);
            let entries = entries.map(|entry| self.remove_entry(entry, watermark));
            for result in tuples.chain(entries) {
                if let Err(e) = result {
                    first_error.get_or_insert(e);
                }
            }
        }
        drop(collecting);
        if !deferred.is_empty() {
            let _deferring = self.deferring.write();
            deferred.into_iter().for_each(|work| work());
        }
        first_error.map_or(Ok(()), Err)
    }

    /// Reclaims the tuple at `rid` of `heap` if no reader at or above
    /// `watermark` can see it. Returns whether it did.
    fn reclaim(&self, heap: &TableHeap, rid: Rid, watermark: TxnId) -> Result<bool> {
        let reclaimed = heap.reclaim(rid, watermark)?;
        if reclaimed {
            self.read_ts.remove(rid);
        }
        Ok(reclaimed)
    }

    /// Removes `entry` from its index unless a reader at or above
    /// `watermark` may need it. Returns whether it did.
    fn remove_entry(&self, entry: &TableEntry, watermark: TxnId) -> Result<bool> {
        let _entries = self.entry_lock.write();
        if entry.is_needed(watermark, &self.undo)? {
            return Ok(false);
        }
        entry.index.delete_entry(&entry.key, &entry.locator)
    }
}
