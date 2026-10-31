//! The transaction watermark: the oldest point in time any running
//! transaction can still read.
//!
//! Under MVTO a transaction's commit timestamp is its Tid, so a version
//! written by transaction X has `ts = X`, and only readers with `tid < X` ever
//! look past it into the undo record holding the version X replaced. The
//! watermark is the smallest active Tid, or the Tid the next transaction will
//! get when none is active. No active or future reader is older than it, so
//! every undo record written by a committed transaction below the watermark
//! is dead and can be freed.
//!
//! The watermark owns the Tid counter, and [`Watermark::begin`] hands out a
//! Tid and registers it as active under one lock. If they were separate steps
//! a transaction could hold a Tid that the watermark doesn't count yet, and
//! GC could free records that transaction still needs.
//!
//! The watermark never decreases: every new Tid is larger than all active
//! ones, so beginning a transaction cannot lower the minimum, and ending one
//! only removes a candidate. GC relies on this, since a record judged dead
//! stays dead.
//!
//! Tids start at 1, because 0 is `INVALID_TXN_ID` and every tuple written
//! before transactions existed carries `ts = 0`, which must read as committed
//! before every transaction. They stay below `TXN_UNCOMMITTED_BIT`, because
//! a tuple marks an uncommitted version by storing its writer's Tid with
//! that bit set.
//!
//! The counter is not persisted yet. Once real timestamps reach disk, a
//! restarted counter must not start below any committed `ts` there;
//! [`Watermark::new`] takes the first Tid so recovery can supply it.

use std::collections::BTreeSet;

use common::{INVALID_TXN_ID, TXN_UNCOMMITTED_BIT, TxnId};
use parking_lot::Mutex;

/// Allocates Tids and tracks which are active, to give the watermark.
pub struct Watermark {
    inner: Mutex<Inner>,
}

struct Inner {
    next: TxnId,
    active: BTreeSet<TxnId>,
}

impl Watermark {
    /// Creates a watermark that hands out Tids starting from `next`.
    ///
    /// Panics if `next` is `INVALID_TXN_ID` or has the top bit set.
    pub fn new(next: TxnId) -> Self {
        assert!(next != INVALID_TXN_ID, "the first Tid must be at least 1");
        assert!(
            next < TXN_UNCOMMITTED_BIT,
            "the first Tid {next} has the top bit set"
        );
        Self {
            inner: Mutex::new(Inner {
                next,
                active: BTreeSet::new(),
            }),
        }
    }

    /// Allocates the next Tid and registers it as active.
    ///
    /// Panics if the counter would reach the top bit.
    pub fn begin(&self) -> TxnId {
        let mut inner = self.inner.lock();
        let tid = inner.next;
        assert!(
            tid + 1 < TXN_UNCOMMITTED_BIT,
            "transaction ids are exhausted"
        );
        inner.next = tid + 1;
        inner.active.insert(tid);
        tid
    }

    /// Removes `tid` from the active transactions, when it commits or aborts.
    ///
    /// Panics if `tid` is not active.
    pub fn end(&self, tid: TxnId) {
        let removed = self.inner.lock().active.remove(&tid);
        assert!(removed, "transaction {tid} is not active");
    }

    /// The smallest active Tid, or the next Tid to hand out if none is active.
    pub fn get(&self) -> TxnId {
        let inner = self.inner.lock();
        inner.active.first().copied().unwrap_or(inner.next)
    }
}
