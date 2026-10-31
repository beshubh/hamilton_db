use std::sync::Arc;

use common::TxnId;

use crate::concurrency::transaction::{Transaction, TransactionState};
use crate::concurrency::watermark::Watermark;
use crate::error::Result;
use crate::table::undo_log::UndoLog;

/// Begins and ends transactions. It owns the watermark and the one undo log
/// every table's versions go into, and collects that log's garbage each time
/// a transaction ends.
pub struct TransactionManager {
    watermark: Watermark,
    undo: Arc<UndoLog>,
}

impl TransactionManager {
    /// A manager whose first transaction gets Tid `next_tid`. See
    /// `Watermark::new`.
    pub fn new(next_tid: TxnId) -> Self {
        Self {
            watermark: Watermark::new(next_tid),
            undo: Arc::new(UndoLog::new()),
        }
    }

    pub fn begin(&self) -> Arc<Transaction> {
        let tid = self.watermark.begin();
        Arc::new(Transaction::new(tid, Arc::clone(&self.undo)))
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

    pub fn undo_log(&self) -> &Arc<UndoLog> {
        &self.undo
    }

    /// The oldest Tid any running transaction reads at. See `Watermark`.
    pub fn watermark(&self) -> TxnId {
        self.watermark.get()
    }

    fn end(&self, txn: &Transaction, outcome: TransactionState) -> Result<()> {
        debug_assert!(
            Arc::ptr_eq(txn.undo_log(), &self.undo),
            "transaction {} was begun by another manager",
            txn.tid()
        );
        txn.finish(outcome)?;
        self.watermark.end(txn.tid());
        self.undo.collect(self.watermark.get());
        Ok(())
    }
}
