//! Undo, the one way a transaction's changes are taken back: by a runtime
//! abort, and by recovery for the transactions a crash cut off. Both walk
//! the transaction's records from its newest, through `prev_lsn`, and log a
//! `Clr` for each change they undo, naming the record to undo next, so
//! undo that a crash interrupts goes on where its last `Clr` left off.

use std::collections::BinaryHeap;
use std::sync::Arc;

use common::{INVALID_LSN, INVALID_TXN_ID, Lsn, Rid};

use crate::error::Result;
use crate::index::btree_index::BtreeIndex;
use crate::table::table_page::{TablePage, TupleMeta};
use crate::wal::mtr::Logged;
use crate::wal::op::PageOp;
use crate::wal::record::{Before, Kind, Undo};
use crate::wal::txn_log::TxnLog;
use crate::wal::when_a_frame_is_free;

/// Rolls back a running transaction: logs its `Abort`, undoes its changes,
/// newest first, and logs its `End`. Returns the tuples it inserted, now
/// deleted at `ts` 0 for garbage collection to reclaim. A transaction with
/// no records logs nothing.
pub(crate) fn rollback(txn: &TxnLog) -> Result<Vec<Rid>> {
    let mut inserted = Vec::new();
    if !txn.has_records() {
        return Ok(inserted);
    }
    let mut lsn = txn.abort();
    let mut buf = Vec::new();
    while lsn != INVALID_LSN {
        lsn = undo_record(txn, lsn, &mut buf, &mut inserted)?;
    }
    txn.end();
    Ok(inserted)
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
    let (mut buf, mut inserted) = (Vec::new(), Vec::new());
    while let Some((lsn, at)) = to_undo.pop() {
        let txn = &losers[at].0;
        match undo_record(txn, lsn, &mut buf, &mut inserted)? {
            INVALID_LSN => txn.end(),
            next => to_undo.push((next, at)),
        }
    }
    Ok(())
}

/// Undoes `txn`'s record at `lsn`, if it is an `Update`, and returns the
/// next of its records to undo. Each undone record gets one `Clr`, even one
/// whose undo found nothing to change. A `Clr` is never undone: the walk
/// skips to the record it names.
fn undo_record(txn: &TxnLog, lsn: Lsn, buf: &mut Vec<u8>, inserted: &mut Vec<Rid>) -> Result<Lsn> {
    let record = txn.bpm().log().read(lsn, buf)?;
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
                        inserted.push(rid);
                    }
                }
                Undo::IndexEntry { header, entry } => {
                    // A failed remove changed nothing, so it can be retried.
                    let removed = when_a_frame_is_free(|| {
                        BtreeIndex::open_tree(Arc::clone(txn.bpm()), header)?.remove_by(by, entry)
                    })?;
                    if !removed {
                        txn.bpm().mtr(by).commit_always();
                    }
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
