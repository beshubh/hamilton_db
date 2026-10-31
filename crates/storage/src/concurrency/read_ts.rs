//! Read timestamps: the last MVTO rule. A write by T fails with `Conflict`
//! if a transaction newer than T already read the tuple, since in Tid order
//! that reader should have seen T's write.
//!
//! Each tuple's read-ts is the largest Tid that read it, kept in memory by
//! `Rid` rather than in the tuple header, so reads never dirty pages. Only a
//! tuple's head can be written, so one entry per tuple is enough.
//!
//! **Record every read**, whichever version the reader R sees, or none:
//! - R saw a version older than the head, committed after R: any writer is
//!   newer than the head, so newer than R, and never blocked.
//! - R read past a newer writer W's locked head: if W aborts, an older
//!   writer must not replace the version R read. If W commits, later
//!   writers are newer than W, so newer than R.
//! - R read its own locked write: after R ends, writers are newer than R.
//!
//! A read is recorded while the reader still holds the page latch it copied
//! the head under; a writer checks under the page's write latch. Otherwise a
//! writer could slip in between the copy and the record. Lock order: page
//! latch, then this map's mutex. A read that fails with `Conflict` is not
//! recorded: it aborts, and its entry would block the lock holder's own
//! later writes.
//!
//! A writer at T passes when `read_ts <= T`, so a transaction can read a
//! tuple and then write it. Entries below the watermark can never fail a
//! check, since no running or future writer is older; `collect` frees them.
//! For the same reason the oldest running transaction records nothing.
//!
//! **Known gap: phantoms.** A read-ts guards tuples that exist. A row
//! inserted by an older transaction after a newer one scanned the table is
//! not caught.

use std::collections::{BTreeMap, HashMap};

use common::{Rid, TxnId};
use parking_lot::Mutex;

use crate::error::{Result, StorageError};

/// The read-ts of every tuple read since the watermark. One lock covers it.
#[derive(Default)]
pub struct ReadTimestamps {
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    read_ts: HashMap<Rid, TxnId>,
    /// The tuples whose read-ts each reader raised, so that `collect` visits
    /// only the entries it frees. A `Rid` stays under an older reader after a
    /// newer one raises it again, or after `remove`; `collect` skips those.
    by_reader: BTreeMap<TxnId, Vec<Rid>>,
}

impl Inner {
    fn record(&mut self, rid: Rid, reader: TxnId) {
        let read_ts = self.read_ts.entry(rid).or_default();
        if *read_ts < reader {
            *read_ts = reader;
            self.by_reader.entry(reader).or_default().push(rid);
        }
    }
}

impl ReadTimestamps {
    pub fn new() -> Self {
        Self::default()
    }

    /// Records that `reader` read the tuple at `rid`.
    pub fn record(&self, rid: Rid, reader: TxnId) {
        self.inner.lock().record(rid, reader);
    }

    /// Records that `reader` read every tuple in `rids`, under one lock.
    pub fn record_page(&self, rids: impl IntoIterator<Item = Rid>, reader: TxnId) {
        let mut inner = self.inner.lock();
        for rid in rids {
            inner.record(rid, reader);
        }
    }

    /// Fails with `Conflict` if a transaction newer than `writer` read the
    /// tuple at `rid`.
    pub fn check(&self, rid: Rid, writer: TxnId) -> Result<()> {
        match self.inner.lock().read_ts.get(&rid) {
            Some(&other) if other > writer => Err(StorageError::Conflict { rid, other }),
            _ => Ok(()),
        }
    }

    /// Drops the read-ts of the tuple at `rid`, once garbage collection has
    /// reclaimed it.
    pub fn remove(&self, rid: Rid) {
        self.inner.lock().read_ts.remove(&rid);
    }

    /// The read-ts of the tuple at `rid`, if it has one.
    pub fn get(&self, rid: Rid) -> Option<TxnId> {
        self.inner.lock().read_ts.get(&rid).copied()
    }

    /// Frees every entry below `watermark`, and returns how many it freed.
    pub fn collect(&self, watermark: TxnId) -> usize {
        let mut inner = self.inner.lock();
        let kept = inner.by_reader.split_off(&watermark);
        let dead = std::mem::replace(&mut inner.by_reader, kept);
        let mut freed = 0;
        for rid in dead.into_values().flatten() {
            if inner.read_ts.get(&rid).is_some_and(|&ts| ts < watermark) {
                inner.read_ts.remove(&rid);
                freed += 1;
            }
        }
        freed
    }

    /// Number of tuples with a read-ts.
    pub fn len(&self) -> usize {
        self.inner.lock().read_ts.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
