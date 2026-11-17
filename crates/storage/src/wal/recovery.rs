//! Restart recovery: analysis, then redo, then a sharp checkpoint.

use std::collections::{HashMap, HashSet};

use common::{Lsn, PageId};

use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::error::{Result, StorageError};
use crate::wal::checkpoint::{self, Checkpoint};
use crate::wal::record::{Kind, PageChanges, Record};

/// Brings every page up to date with the log, from the checkpoint in the
/// `.meta` file on, then takes a sharp checkpoint, which needs no log
/// before the end, so every segment but the last goes. Writes no records,
/// so a crash in the middle of it leaves files that recover the same pages.
pub(crate) fn recover(bpm: &BufferPoolManager) -> Result<()> {
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
    let dirty = analyze(bpm, &checkpoint)?;
    redo(bpm, checkpoint.redo_lsn, dirty)?;
    let saved = checkpoint::sharp(bpm)?;
    if !saved.dirty.is_empty() || saved.redo_lsn != saved.start_lsn {
        return Err(StorageError::CorruptLog(format!(
            "recovery left pages that need the log from {}",
            saved.redo_lsn
        )));
    }
    Ok(())
}

/// Finds the pages redo must bring up to date, each with the first record
/// it must apply: those dirty at the checkpoint, and those the log changes
/// after it began, less the ones it frees. Raises the next page id past
/// every page the log names.
fn analyze(bpm: &BufferPoolManager, checkpoint: &Checkpoint) -> Result<HashMap<PageId, Lsn>> {
    let mut dirty: HashMap<PageId, Lsn> = checkpoint.dirty.iter().copied().collect();
    let mut next_page_id = checkpoint
        .next_page_id
        .max(bpm.disk_manager().next_page_id());
    let mut scan = bpm.log().scan(checkpoint.start_lsn);
    while let Some(record) = scan.next_record()? {
        let Some(changes) = page_changes(&record)? else {
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
    Ok(dirty)
}

/// Applies, from `redo_lsn` on, every record to the pages in `dirty` it is
/// not older than, and replays every free. A page whose ops find its copy
/// on disk unreadable waits for a record that replaces it; see "Redo reads
/// a page from disk only for ops" in docs/internals/recovery.md.
fn redo(bpm: &BufferPoolManager, redo_lsn: Lsn, mut dirty: HashMap<PageId, Lsn>) -> Result<()> {
    let mut needs_image = HashSet::new();
    let mut scan = bpm.log().scan(redo_lsn);
    while let Some(record) = scan.next_record()? {
        let Some(changes) = page_changes(&record)? else {
            continue;
        };
        for (page_id, redo) in changes.pages() {
            if dirty.get(&page_id).is_none_or(|&first| first > record.lsn) {
                continue;
            }
            if redo.replaces_page() {
                needs_image.remove(&page_id);
            } else if needs_image.contains(&page_id) {
                continue;
            }
            if !bpm.redo(page_id, redo, record.lsn)? {
                needs_image.insert(page_id);
            }
        }
        for page_id in changes.frees() {
            dirty.remove(&page_id);
            needs_image.remove(&page_id);
            bpm.discard_page(page_id)?;
        }
    }
    match needs_image.into_iter().min() {
        Some(page_id) => Err(StorageError::CorruptLog(format!(
            "page {page_id} cannot be read, and no record after it replaces the whole page"
        ))),
        None => Ok(()),
    }
}

/// The page changes in `record`, if it is of a kind that has them.
fn page_changes<'a>(record: &Record<'a>) -> Result<Option<PageChanges<'a>>> {
    match record.kind {
        Kind::Update | Kind::Clr | Kind::Completion | Kind::System => {
            PageChanges::decode(record.body).map(Some)
        }
        Kind::Commit | Kind::Abort | Kind::End => Ok(None),
    }
}
