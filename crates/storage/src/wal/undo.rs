//! Undo, the one way a transaction's changes are taken back: by a runtime
//! abort, and by recovery for the transactions a crash cut off. Both walk
//! the transaction's records from its newest, through `prev_lsn`, and log a
//! `Clr` for each change they undo, naming the record to undo next, so
//! undo that a crash interrupts goes on where its last `Clr` left off.

use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use common::hash::IntKeys;
use common::{INVALID_LSN, INVALID_TXN_ID, Lsn, PageId, Rid};

use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::error::Result;
use crate::index::btree::Btree;
use crate::index::btree_index::{BtreeIndex, EntryComparator};
use crate::table::table_page::{TablePage, TupleMeta};
use crate::wal::mtr::Logged;
use crate::wal::op::PageOp;
use crate::wal::record::{Before, Kind, Undo};
use crate::wal::txn_log::{self, TxnLog};
use crate::wal::when_a_frame_is_free;

/// Rolls back a running transaction: logs its `Abort`, undoes its changes,
/// newest first, and logs its `End`. Returns the tuples it inserted, now
/// deleted at `ts` 0 for garbage collection to reclaim. A transaction with
/// no records logs nothing.
pub(crate) fn rollback(txn: &TxnLog) -> Result<Vec<Rid>> {
    let mut walk = Walk::default();
    if !txn.has_records() {
        return Ok(walk.inserted);
    }
    let mut lsn = txn.abort();
    while lsn != INVALID_LSN {
        lsn = walk.undo_record(txn, lsn)?;
    }
    txn.end();
    Ok(walk.inserted)
}

/// Rolls back every transaction in `losers`, each given with its last
/// record, taking the newest record of any of them next, and logs each
/// one's `End`. The tuples their inserts leave deleted stay until a
/// cleanup reclaims them.
pub(crate) fn undo_losers(losers: &[(TxnLog, Lsn)]) -> Result<()> {
    let mut to_undo: BinaryHeap<(Lsn, usize)> = losers
        .iter()
        .enumerate()
        .map(|(at, &(_, last_lsn))| (last_lsn, at))
        .collect();
    let mut walk = Walk::default();
    while let Some((lsn, at)) = to_undo.pop() {
        let txn = &losers[at].0;
        match walk.undo_record(txn, lsn)? {
            INVALID_LSN => txn.end(),
            next => to_undo.push((next, at)),
        }
    }
    Ok(())
}

/// What one undo keeps from record to record: a buffer to read them into,
/// the B+ trees it has opened, so each opens once, and the tuples it left
/// deleted.
#[derive(Default)]
struct Walk {
    buf: Vec<u8>,
    trees: HashMap<PageId, Btree<EntryComparator>, IntKeys>,
    inserted: Vec<Rid>,
}

impl Walk {
    /// Undoes `txn`'s record at `lsn`, if it is an `Update`, and returns the
    /// next of its records to undo. Each undone record gets one `Clr`, even
    /// one whose undo found nothing to change. A `Clr` is never undone: the
    /// walk skips to the record it names.
    fn undo_record(&mut self, txn: &TxnLog, lsn: Lsn) -> Result<Lsn> {
        let record = txn.bpm().log().read(lsn, &mut self.buf)?;
        debug_assert_eq!(record.tid, txn.tid(), "record {lsn} is another's");
        match record.kind {
            Kind::Update => {
                let by = Logged::Clr {
                    txn,
                    undo_next: record.prev_lsn,
                };
                match record.undo()? {
                    Undo::Heap { rid, before, .. } => {
                        undo_heap(txn, by, rid, before)?;
                        if before == Before::Inserted {
                            self.inserted.push(rid);
                        }
                    }
                    Undo::IndexEntry { header, entry } => {
                        let trees = &mut self.trees;
                        // A failed remove changed nothing, so it can be retried.
                        let removed = when_a_frame_is_free(|| {
                            tree(trees, txn.bpm(), header)?.remove_by(by, entry)
                        })?;
                        if !removed {
                            txn.bpm().mtr(by).commit_always();
                        }
                    }
                    Undo::Truncate { new, .. } => {
                        let new: Vec<PageId> = new.iter().collect();
                        for header in &new[1..] {
                            self.trees.remove(header);
                        }
                        txn_log::free_storage(txn.bpm(), by, &new)?;
                    }
                }
                Ok(record.prev_lsn)
            }
            Kind::Clr => record.undo_next(),
            Kind::Abort | Kind::Completion | Kind::Commit | Kind::End | Kind::System => {
                Ok(record.prev_lsn)
            }
        }
    }
}

/// The tree whose header page is `header`, opened from its header alone.
fn tree<'t>(
    trees: &'t mut HashMap<PageId, Btree<EntryComparator>, IntKeys>,
    bpm: &Arc<BufferPoolManager>,
    header: PageId,
) -> Result<&'t Btree<EntryComparator>> {
    Ok(match trees.entry(header) {
        Entry::Occupied(open) => open.into_mut(),
        Entry::Vacant(slot) => slot.insert(BtreeIndex::open_tree(Arc::clone(bpm), header)?),
    })
}

/// Puts the tuple at `rid` back as it was before the change: an inserted
/// one is left deleted at `ts` 0, which no reader sees. The tuple is still
/// locked by the transaction, and never longer than it was before (see
/// `TableHeap`'s padding rule), so it changes in place.
fn undo_heap(txn: &TxnLog, by: Logged<'_>, rid: Rid, before: Before<'_>) -> Result<()> {
    let mtr = txn.bpm().mtr(by);
    let mut page = when_a_frame_is_free(|| mtr.write_page(rid.page_id))?;
    let op = match before {
        Before::Inserted => {
            let (meta, _) = TablePage::new(page.data())?.get(rid.slot)?;
            let meta = TupleMeta {
                ts: INVALID_TXN_ID,
                is_deleted: true,
                ..meta
            };
            PageOp::HeapSetMeta {
                slot: rid.slot,
                meta,
            }
        }
        Before::Meta(meta) => PageOp::HeapSetMeta {
            slot: rid.slot,
            meta,
        },
        Before::Tuple { meta, payload } => PageOp::HeapSetTuple {
            slot: rid.slot,
            meta,
            payload,
            padded_to: payload.len(),
        },
    };
    page.apply(op)
}
