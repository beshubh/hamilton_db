//! Restart recovery: analysis, redo, then the transactions a crash cut off
//! are finished: committed ones stamped, the rest undone. A sharp
//! checkpoint ends it.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use common::{Lsn, PageId};

use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::error::{Result, StorageError};
use crate::wal::checkpoint::{self, Checkpoint};
use crate::wal::txn_log::{self, Att, TxnLog, TxnStatus};
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
    let (dirty, att) = analyze(bpm, &checkpoint)?;
    redo(bpm, checkpoint.redo_lsn, dirty)?;
    finish_transactions(bpm, att)?;
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
/// record's `prev_lsn` is its transaction's record before it. Raises the
/// next page id past every page the log names.
fn analyze(
    bpm: &BufferPoolManager,
    checkpoint: &Checkpoint,
) -> Result<(HashMap<PageId, Lsn>, Att)> {
    let mut dirty: HashMap<PageId, Lsn> = checkpoint.dirty.iter().copied().collect();
    let mut att = checkpoint.att.clone();
    let mut next_page_id = checkpoint
        .next_page_id
        .max(bpm.disk_manager().next_page_id());
    let mut scan = bpm.log().scan(checkpoint.start_lsn);
    while let Some(record) = scan.next_record()? {
        let prev_lsn = att.link(record.tid, record.kind, record.lsn);
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
    Ok((dirty, att))
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
/// and undoes the others. Each then gets its `End`.
fn finish_transactions(bpm: &Arc<BufferPoolManager>, att: Att) -> Result<()> {
    let entries = att.entries();
    // Their records chain onto the ones analysis found.
    bpm.log().restore_att(att);
    let mut losers = Vec::new();
    // In Tid order: a truncate is newer than every transaction that wrote
    // the storage it frees, so their stamps come first. Runtime frees are
    // logged in commit order for the same reason.
    for (tid, entry) in entries {
        let txn = TxnLog::new(tid, Arc::clone(bpm));
        match entry.status {
            TxnStatus::Committed => txn_log::complete(&txn, entry.last_lsn)?,
            TxnStatus::Running | TxnStatus::Aborting => losers.push((txn, entry.last_lsn)),
        }
    }
    undo::undo_losers(&losers)
}
