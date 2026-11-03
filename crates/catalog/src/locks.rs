//! Table locks, held until the transaction ends. A lock that conflicts with
//! one another transaction holds fails at once rather than waiting, so locks
//! can't deadlock; the engine retries a statement outside a block once the
//! holder ends. See `docs/internals/mvto.md`.

use std::sync::Arc;

use common::TxnId;
use parking_lot::Mutex;
use storage::concurrency::transaction::Transaction;

use crate::error::{CatalogError, Result};

/// How a statement uses a table, as Postgres's lock modes of the same names.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum LockMode {
    /// Reading.
    AccessShare,
    /// Writing rows, or adding an index.
    RowExclusive,
    /// Replacing the table's storage, as TRUNCATE does.
    AccessExclusive,
}

impl LockMode {
    /// Readers and writers share a table, as MVTO orders their rows.
    fn conflicts_with(self, other: Self) -> bool {
        self == Self::AccessExclusive || other == Self::AccessExclusive
    }
}

/// One table's locks. A truncate's new `TableInfo` shares them.
#[derive(Clone, Default)]
pub(crate) struct TableLocks(Arc<Mutex<State>>);

#[derive(Default)]
struct State {
    holders: Vec<(TxnId, LockMode)>,
    /// The newest transaction that locked the table. A truncate older than
    /// it would remove rows it wrote or read after them in Tid order.
    used_by: TxnId,
    /// The newest transaction whose truncate of the table committed. An
    /// older one would see the rows it removed, which are gone.
    truncated_by: TxnId,
}

impl TableLocks {
    /// Locks the table, named `name`, in `mode` until `txn` ends.
    pub fn lock(&self, txn: &Transaction, name: &str, mode: LockMode) -> Result<()> {
        let me = txn.tid();
        let mut state = self.0.lock();
        let holder = state
            .holders
            .iter()
            .find(|&&(tid, held)| tid != me && held.conflicts_with(mode));
        if let Some(&(other, _)) = holder {
            return Err(CatalogError::LockConflict {
                table: name.to_owned(),
                other,
            });
        }
        let newer = if mode == LockMode::AccessExclusive {
            state.used_by.max(state.truncated_by)
        } else {
            state.truncated_by
        };
        if me < newer {
            return Err(CatalogError::ChangedSince {
                table: name.to_owned(),
                other: newer,
            });
        }
        match state.holders.iter_mut().find(|(tid, _)| *tid == me) {
            Some((_, held)) => *held = (*held).max(mode),
            None => {
                let locks = self.clone();
                txn.on_end(move |_| locks.release(me))?;
                state.holders.push((me, mode));
            }
        }
        state.used_by = state.used_by.max(me);
        Ok(())
    }

    /// Records that `tid`'s truncate of the table committed.
    pub fn truncated(&self, tid: TxnId) {
        let mut state = self.0.lock();
        state.truncated_by = state.truncated_by.max(tid);
    }

    fn release(&self, tid: TxnId) {
        self.0.lock().holders.retain(|&(holder, _)| holder != tid);
    }
}
