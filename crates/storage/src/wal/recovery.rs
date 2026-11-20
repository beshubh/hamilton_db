//! Restart recovery: analysis, redo, then the transactions a crash cut off
//! are finished: committed ones stamped, the rest undone. A sharp
//! checkpoint ends it.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use common::{Lsn, PageId, TxnId};

use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::error::{Result, StorageError};
use crate::wal::checkpoint::{self, Checkpoint};
use crate::wal::txn_log::{self, Att, Resumed};
use crate::wal::undo;

/// Brings every page up to date with the log, from the checkpoint in the
/// `.meta` file on, finishes or undoes every transaction without an `End`,
/// then takes a sharp checkpoint, which needs no log before the end, so
/// every segment but the last goes. Redo writes no records, and what
/// completion and undo log brings a crash in the middle of them back to
/// where they stopped.
pub(crate) fn recover(bpm: &Arc<BufferPoolManager>) -> Result<()> {
    let log = bpm.log();
    let checkpoint = Checkpoint::decode(bpm.disk_manager().opened_checkpoint(), log.durable_end())?;
    let in_log = log.first_lsn()..=log.durable_end();
    if !in_log.contains(&checkpoint.log_needed_from())
        || !in_log.contains(&checkpoint.start_lsn)
        || checkpoint.redo_lsn > checkpoint.start_lsn
    {
        return Err(StorageError::CorruptLog(format!(
            "the checkpoint reads from {} and {}, but the log holds {}..{}",
            checkpoint.log_needed_from(),
            checkpoint.start_lsn,
            in_log.start(),
            in_log.end(),
        )));
    }
    let (dirty, att, next_tid) = analyze(bpm, &checkpoint)?;
    redo(bpm, checkpoint.redo_lsn, dirty)?;
    finish_transactions(bpm, att, next_tid)?;
    let saved = checkpoint::sharp(bpm)?;
    if !saved.dirty.is_empty() || saved.redo_lsn != saved.start_lsn || !saved.att.is_empty() {
        return Err(StorageError::CorruptLog(format!(
            "recovery left pages that need the log from {}, or transactions without an end",
            saved.log_needed_from()
        )));
    }
    Ok(())
}

/// Finds the pages redo must bring up to date, each with the first record
/// it must apply: those dirty at the checkpoint, and those the log changes
/// after it began, less the ones it frees. Finds the transactions without
/// an `End`, starting from those the checkpoint saved, and checks that each
/// record is one its transaction may log then (`TxnStatus::after`) and
/// that its `prev_lsn` is its transaction's record before it. Raises the
/// next page id past every page the log names, and returns a Tid above
/// every one the checkpoint and the log name.
fn analyze(
    bpm: &BufferPoolManager,
    checkpoint: &Checkpoint,
) -> Result<(HashMap<PageId, Lsn>, Att, TxnId)> {
    let mut dirty: HashMap<PageId, Lsn> = checkpoint.dirty.iter().copied().collect();
    let mut att = checkpoint.att.clone();
    let mut next_tid = checkpoint.next_tid;
    let mut next_page_id = checkpoint
        .next_page_id
        .max(bpm.disk_manager().next_page_id());
    let mut scan = bpm.log().scan(checkpoint.start_lsn);
    while let Some(record) = scan.next_record()? {
        next_tid = next_tid.max(record.tid + 1);
        let prev_lsn = att
            .link(record.tid, record.kind, record.lsn)
            .map_err(|e| StorageError::CorruptLog(format!("the record at {}: {e}", record.lsn)))?;
        if prev_lsn != record.prev_lsn {
            return Err(StorageError::CorruptLog(format!(
                "the record at {} follows {}, but its transaction's last record was {prev_lsn}",
                record.lsn, record.prev_lsn
            )));
        }
        let Some(changes) = record.page_changes()? else {
            continue;
        };
        for (page_id, _) in changes.pages() {
            dirty.entry(page_id).or_insert(record.lsn);
            next_page_id = next_page_id.max(page_id + 1);
        }
        for page_id in changes.frees() {
            dirty.remove(&page_id);
            next_page_id = next_page_id.max(page_id + 1);
        }
    }
    bpm.raise_next_page_id(next_page_id);
    Ok((dirty, att, next_tid))
}

/// Applies, from `redo_lsn` on, every record to the pages in `dirty` it is
/// not older than, and replays every free. The first record redo applies to
/// a page replaces all of it, so redo never reads a page's copy on disk:
/// see "Redo never reads a page from disk" in docs/internals/recovery.md.
fn redo(bpm: &BufferPoolManager, redo_lsn: Lsn, mut dirty: HashMap<PageId, Lsn>) -> Result<()> {
    let mut started = HashSet::new();
    let mut scan = bpm.log().scan(redo_lsn);
    while let Some(record) = scan.next_record()? {
        let Some(changes) = record.page_changes()? else {
            continue;
        };
        for (page_id, redo) in changes.pages() {
            if dirty.get(&page_id).is_none_or(|&first| first > record.lsn) {
                continue;
            }
            if started.insert(page_id) && !redo.replaces_page() {
                return Err(StorageError::CorruptLog(format!(
                    "redo of page {page_id} starts at {}, which does not replace the page",
                    record.lsn
                )));
            }
            bpm.redo(page_id, redo, record.lsn)?;
        }
        for page_id in changes.frees() {
            dirty.remove(&page_id);
            bpm.discard_page(page_id)?;
        }
    }
    Ok(())
}

/// Finishes each transaction in `att`: stamps a committed one's tuples,
/// and undoes the others, first logging the `Abort` of one still running,
/// as a runtime abort does. Each then gets its `End`.
fn finish_transactions(bpm: &Arc<BufferPoolManager>, att: Att, next_tid: TxnId) -> Result<()> {
    let entries = att.entries();
    // Their records chain onto the ones analysis found.
    bpm.log().restore(att, next_tid);
    let mut losers = Vec::new();
    // In Tid order: a truncate is newer than every transaction that wrote
    // the storage it frees, so their stamps come first. Runtime frees are
    // logged in commit order for the same reason.
    for (tid, entry) in entries {
        match txn_log::resume(tid, &entry, Arc::clone(bpm)) {
            Resumed::Committed(txn, last_lsn) => txn_log::complete(txn, last_lsn)?,
            Resumed::Aborting(txn, last_lsn) => losers.push((txn, last_lsn)),
        }
    }
    undo::undo_losers(losers)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common::{TXN_UNCOMMITTED_BIT, Tuple};
    use tempfile::TempDir;

    use crate::disk::fs::OsFs;
    use crate::error::{Result, StorageError};
    use crate::table::table_heap::TableHeap;
    use crate::table::table_page::TupleMeta;
    use crate::wal::record::Kind;
    use crate::wal::txn_log::{Att, TxnLog};
    use crate::wal::{Logged, SEGMENT_SIZE};

    fn open(dir: &TempDir) -> Result<crate::Storage> {
        let path = dir.path().join("test.db");
        crate::open_with(Arc::new(OsFs), path, 4, None, SEGMENT_SIZE)
    }

    /// An `Update` after its transaction's `Commit`, chained to it, is
    /// refused: recovering it as a loser would undo the commit too.
    #[test]
    fn analysis_refuses_an_update_after_its_transactions_commit() {
        let dir = TempDir::new().unwrap();
        let storage = open(&dir).unwrap();
        let bpm = &storage.bpm;
        let heap = TableHeap::create(Arc::clone(bpm)).unwrap();
        let (tid, row) = (7, Tuple::from_bytes(b"row".to_vec()));
        let meta = TupleMeta::new(tid | TXN_UNCOMMITTED_BIT);
        let txn = TxnLog::new(tid, Arc::clone(bpm));
        heap.insert(Logged::Txn(&txn), &meta, &row).unwrap();
        let committed = txn.commit().unwrap();
        let commit = bpm.log().last_lsn(tid).unwrap();
        // A writer that missed the `Commit`: its table has the transaction
        // still running, with the `Commit` as its last record.
        let mut stale = Att::default();
        stale.link(tid, Kind::Update, commit).unwrap();
        bpm.log().restore(stale, tid + 1);
        let again = TxnLog::new(tid, Arc::clone(bpm));
        heap.insert(Logged::Txn(&again), &meta, &row).unwrap();
        let update = bpm.log().last_lsn(tid).unwrap();
        bpm.log().flush_all().unwrap();
        drop((committed, again, heap, storage));

        match open(&dir) {
            Err(StorageError::CorruptLog(msg)) => {
                for named in [&update.to_string(), "Update", "Committed"] {
                    assert!(msg.contains(named), "{msg:?} does not name {named}");
                }
            }
            Err(e) => panic!("{e}"),
            Ok(_) => panic!("recovered an Update after its transaction's Commit"),
        }
    }
}
