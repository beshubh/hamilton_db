//! Checkpoints: the record recovery starts from, saved in the `.meta` file
//! together with the page directory, so one rename publishes both. Once it
//! is saved, the log segments it no longer needs are removed.

use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use common::{FIRST_TXN_ID, Lsn, PageId, TxnId};
use parking_lot::{Condvar, Mutex};

use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::error::{Result, StorageError};
use crate::wal::txn_log::Att;

/// What recovery needs to know about the log as of a checkpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Checkpoint {
    /// The end of the log when the checkpoint began: analysis reads from
    /// here.
    pub start_lsn: Lsn,
    /// Redo reads from here: the oldest change to a page in `dirty`, or the
    /// oldest free the saved directory may not show yet, or `start_lsn`.
    pub redo_lsn: Lsn,
    /// Above every page id the saved directory and the log before
    /// `start_lsn` name.
    pub next_page_id: PageId,
    /// Above the Tid of every record before `start_lsn`.
    pub next_tid: TxnId,
    /// Pages whose changes may not be on disk, each with the first record
    /// that changed it since it was last written.
    pub dirty: Vec<(PageId, Lsn)>,
    /// The transactions with records before `start_lsn` and no `End`.
    pub att: Att,
}

impl Checkpoint {
    /// The oldest record recovery from this checkpoint reads: redo's start,
    /// or the first record of a transaction it may have to finish or undo.
    /// It never moves back from one checkpoint to the next, so the log
    /// segments wholly before it are never needed again.
    pub(crate) fn log_needed_from(&self) -> Lsn {
        let oldest_txn = self.att.oldest_first_lsn().unwrap_or(self.start_lsn);
        self.redo_lsn.min(self.start_lsn).min(oldest_txn)
    }

    /// The checkpoint in `bytes`, as `encode` wrote it. A database has none
    /// until its first open ends with a checkpoint, before it logs anything,
    /// so for empty `bytes` any records before the log's `end` belong to
    /// another database's files, and recovery skips them.
    pub(crate) fn decode(bytes: &[u8], end: Lsn) -> Result<Self> {
        if bytes.is_empty() {
            return Ok(Self {
                start_lsn: end,
                redo_lsn: end,
                next_page_id: 0,
                next_tid: FIRST_TXN_ID,
                dirty: Vec::new(),
                att: Att::default(),
            });
        }
        let corrupt = || StorageError::CorruptMetadata("bad checkpoint".into());
        let (head, rest) = bytes.split_at_checked(HEAD).ok_or_else(corrupt)?;
        let u64_at = |at: usize| u64::from_le_bytes(head[at..at + 8].try_into().unwrap());
        let num_dirty = u32::from_le_bytes(head[28..32].try_into().unwrap()) as usize;
        let (entries, rest) = rest
            .split_at_checked(num_dirty * DIRTY_ENTRY)
            .ok_or_else(corrupt)?;
        let dirty = entries
            .chunks_exact(DIRTY_ENTRY)
            .map(|entry| {
                let page_id = PageId::from_le_bytes(entry[..4].try_into().unwrap());
                (page_id, u64::from_le_bytes(entry[4..].try_into().unwrap()))
            })
            .collect();
        let (att, rest) = Att::decode(rest)?;
        if !rest.is_empty() {
            return Err(corrupt());
        }
        Ok(Self {
            start_lsn: u64_at(0),
            redo_lsn: u64_at(8),
            next_tid: u64_at(16),
            next_page_id: PageId::from_le_bytes(head[24..28].try_into().unwrap()),
            dirty,
            att,
        })
    }

    /// Lays the checkpoint out as `start_lsn: u64, redo_lsn: u64,
    /// next_tid: u64, next_page_id: i32, num_dirty: u32, (page_id: i32,
    /// rec_lsn: u64)*, att`, all little-endian, the transaction table as
    /// `Att::encode` lays it out.
    fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(HEAD + self.dirty.len() * DIRTY_ENTRY);
        bytes.extend_from_slice(&self.start_lsn.to_le_bytes());
        bytes.extend_from_slice(&self.redo_lsn.to_le_bytes());
        bytes.extend_from_slice(&self.next_tid.to_le_bytes());
        bytes.extend_from_slice(&self.next_page_id.to_le_bytes());
        bytes.extend_from_slice(&(self.dirty.len() as u32).to_le_bytes());
        for &(page_id, rec_lsn) in &self.dirty {
            bytes.extend_from_slice(&page_id.to_le_bytes());
            bytes.extend_from_slice(&rec_lsn.to_le_bytes());
        }
        self.att.encode(&mut bytes);
        bytes
    }
}

/// Bytes before the dirty pages.
const HEAD: usize = 32;
const DIRTY_ENTRY: usize = 12;

/// What checkpoints keep between them. The buffer pool holds it.
#[derive(Default)]
pub(crate) struct Checkpoints {
    /// Held while a checkpoint runs. The `start_lsn` of the last one.
    last_start: Mutex<Lsn>,
    /// Records whose frees are logged but not yet applied to the page
    /// directory. An mtr applies them after it appends, so a directory
    /// copied in between still lists the pages.
    pending_frees: Mutex<Vec<Lsn>>,
}

impl Checkpoints {
    pub(crate) fn free_logged(&self, lsn: Lsn) {
        self.pending_frees.lock().push(lsn);
    }

    pub(crate) fn free_applied(&self, lsn: Lsn) {
        let mut pending = self.pending_frees.lock();
        let at = pending.iter().position(|&l| l == lsn);
        pending.swap_remove(at.expect("a free applied that was never logged"));
    }

    fn oldest_pending_free(&self) -> Option<Lsn> {
        self.pending_frees.lock().iter().copied().min()
    }
}

/// Takes a fuzzy checkpoint: transactions keep running, and only pages
/// changed before the last checkpoint began are written, so redo never
/// reads much more than two checkpoint intervals of log.
pub fn fuzzy(bpm: &BufferPoolManager) -> Result<()> {
    let mut last_start = bpm.checkpoints().last_start.lock();
    bpm.flush_older_than(*last_start)?;
    take(bpm, &mut last_start).map(drop)
}

/// Takes a sharp checkpoint: writes every dirty page first, so that,
/// unless pages change meanwhile, recovery has nothing to redo.
pub(crate) fn sharp(bpm: &BufferPoolManager) -> Result<Checkpoint> {
    let mut last_start = bpm.checkpoints().last_start.lock();
    bpm.flush_all_pages()?;
    take(bpm, &mut last_start)
}

fn take(bpm: &BufferPoolManager, last_start: &mut Lsn) -> Result<Checkpoint> {
    let log = bpm.log();
    let disk = bpm.disk_manager();
    // Under the log's lock, so a page left out of `dirty` takes its next
    // change after `start_lsn`, logged whole: redo never reads a page from
    // disk. A free logged before `start_lsn` and applied after the
    // directory copy is still pending, so redo reads from it.
    let (start_lsn, att, next_tid, dirty, pending_free) = log.at_end(|end, att, next_tid| {
        let pending_free = bpm.checkpoints().oldest_pending_free();
        (end, att.clone(), next_tid, bpm.dirty_pages(), pending_free)
    });
    // Frames are scanned before the directory is copied. A page the scan
    // finds clean finished its write, and a first write's slot is in the
    // directory before the frame is marked clean, so the copy has it. A page
    // still dirty is redone from the record at its rec_lsn, which replaces
    // all of it.
    let directory = disk.directory();
    let next_page_id = bpm.next_page_id();
    // The log is synced first. The frees the copy shows, and the writes of
    // pages left out of `dirty`, each came after their records.
    log.flush_all()?;
    disk.sync()?;
    let redo_lsn = dirty
        .iter()
        .map(|&(_, rec_lsn)| rec_lsn)
        .chain(pending_free)
        .fold(start_lsn, Lsn::min);
    let checkpoint = Checkpoint {
        start_lsn,
        redo_lsn,
        next_page_id,
        next_tid,
        dirty,
        att,
    };
    // Saving it would replace a checkpoint recovery can start from with one
    // it cannot.
    if checkpoint.log_needed_from() < log.first_lsn() {
        return Err(StorageError::CorruptLog(format!(
            "the checkpoint needs the log from {}, which was removed",
            checkpoint.log_needed_from()
        )));
    }
    disk.save_meta(&directory, &checkpoint.encode())?;
    *last_start = start_lsn;
    // The checkpoint is saved; a segment left behind is removed by the next.
    if let Err(e) = log.remove_segments_before(checkpoint.log_needed_from()) {
        eprintln!("hamilton: cannot remove old log segments: {e}");
    }
    Ok(checkpoint)
}

/// How often `Checkpointer` looks at the log.
const POLL: Duration = Duration::from_secs(1);

/// The log growth that triggers a checkpoint before its interval is up.
const LOG_GROWTH: u64 = crate::wal::SEGMENT_SIZE;

/// A thread that takes a fuzzy checkpoint every interval, or sooner once
/// the log has grown by `LOG_GROWTH`, until dropped.
pub(crate) struct Checkpointer {
    stop: Arc<(Mutex<bool>, Condvar)>,
    thread: Option<JoinHandle<()>>,
}

impl Checkpointer {
    pub(crate) fn start(bpm: Arc<BufferPoolManager>, interval: Duration) -> Self {
        let stop = Arc::new((Mutex::new(false), Condvar::new()));
        let thread = {
            let stop = Arc::clone(&stop);
            thread::Builder::new()
                .name("checkpoint".into())
                .spawn(move || run(&bpm, interval, &stop))
                .expect("failed to start the checkpoint thread")
        };
        Self {
            stop,
            thread: Some(thread),
        }
    }
}

fn run(bpm: &BufferPoolManager, interval: Duration, stop: &(Mutex<bool>, Condvar)) {
    let (mut last_time, mut last_end) = (Instant::now(), bpm.log().end());
    loop {
        {
            let mut stopped = stop.0.lock();
            if !*stopped {
                stop.1.wait_for(&mut stopped, POLL);
            }
            if *stopped {
                return;
            }
        }
        let end = bpm.log().end();
        let due = last_time.elapsed() >= interval || end - last_end >= LOG_GROWTH;
        let idle = end == last_end && bpm.dirty_pages().is_empty();
        if !due || idle {
            continue;
        }
        if let Err(e) = fuzzy(bpm) {
            eprintln!("hamilton: checkpoint failed: {e}");
        }
        (last_time, last_end) = (Instant::now(), end);
    }
}

impl Drop for Checkpointer {
    fn drop(&mut self) {
        *self.stop.0.lock() = true;
        self.stop.1.notify_one();
        if let Some(thread) = self.thread.take() {
            // A checkpoint thread that panicked has already reported it.
            let _ = thread.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::arc_replacer::AccessType;
    use crate::disk::fs::OsFs;
    use crate::page::PAGE_SIZE;
    use crate::page::page_header::PageHeader;
    use crate::table::table_heap::TableHeap;
    use crate::table::table_page::{TablePage, TupleMeta};
    use crate::wal::mtr::{BEFORE_FREES, Logged};
    use crate::wal::txn_log::TxnLog;
    use common::{PageId, Tuple};
    use tempfile::TempDir;

    fn open(dir: &TempDir) -> crate::Storage {
        let path = dir.path().join("test.db");
        crate::open_with(Arc::new(OsFs), path, 4, None, crate::wal::SEGMENT_SIZE).unwrap()
    }

    #[test]
    fn a_free_the_directory_copy_missed_is_redone() {
        let dir = TempDir::new().unwrap();
        let storage = open(&dir);
        let (freed, kept) = (storage.bpm.new_page(), storage.bpm.new_page());
        for page_id in [freed, kept] {
            let mtr = storage.bpm.mtr(Logged::System);
            mtr.write_page(page_id).unwrap().raw_mut()[PageHeader::SIZE] = 1;
        }
        storage.bpm.checkpoint().unwrap();

        let bpm = Arc::clone(&storage.bpm);
        BEFORE_FREES.set(Some(Box::new(move || fuzzy(&bpm).unwrap())));
        storage.bpm.mtr(Logged::System).free_page(freed);
        BEFORE_FREES.set(None);
        assert!(!storage.bpm.disk_manager().has_page(freed));
        drop(storage);

        let storage = open(&dir);
        assert!(
            !storage.bpm.disk_manager().has_page(freed),
            "freed page leaked"
        );
        let guard = storage.bpm.read_page(kept, AccessType::Unknown);
        assert_eq!(guard.get_data()[PageHeader::SIZE], 1);
    }

    fn row(bytes: &[u8]) -> Tuple {
        Tuple::from_bytes(bytes.to_vec())
    }

    /// Flips a byte of the page's copy on disk, as a torn write would.
    fn tear(storage: &crate::Storage, page_id: PageId) {
        let disk = storage.bpm.disk_manager();
        let mut data = [0; PAGE_SIZE];
        disk.read_page(page_id, &mut data).unwrap();
        data[PAGE_SIZE / 2] ^= 0xff;
        disk.write_page(page_id, &data).unwrap();
    }

    /// A page clean when a fuzzy checkpoint began logs all of itself with
    /// its next change, so a torn write of it after the checkpoint is
    /// repaired without reading its copy on disk.
    #[test]
    fn a_page_torn_after_a_checkpoint_is_redone_from_its_next_change() {
        let dir = TempDir::new().unwrap();
        let storage = open(&dir);
        let heap = TableHeap::create(Arc::clone(&storage.bpm)).unwrap();
        let rid = heap
            .insert(Logged::System, &TupleMeta::new(1), &row(b"first"))
            .unwrap();
        storage.bpm.flush_page(rid.page_id).unwrap();
        fuzzy(&storage.bpm).unwrap();
        heap.update_meta(rid, &TupleMeta::new(2)).unwrap();
        storage.bpm.flush_page(rid.page_id).unwrap();
        tear(&storage, rid.page_id);
        let first = heap.first_page_id();
        drop((heap, storage));

        let storage = open(&dir);
        let heap = TableHeap::open(Arc::clone(&storage.bpm), first).unwrap();
        assert_eq!(heap.get(rid).unwrap().0, TupleMeta::new(2));
        let guard = storage.bpm.read_page(rid.page_id, AccessType::Unknown);
        assert!(TablePage::new(guard.get_data()).is_ok());
    }

    #[test]
    fn a_checkpoint_saves_the_transactions_without_an_end() {
        let dir = TempDir::new().unwrap();
        let storage = open(&dir);
        let log = storage.bpm.log();
        let (running, done) = (TxnLog::new(7, Arc::clone(&storage.bpm)), 8);
        let heap = TableHeap::create(Arc::clone(&storage.bpm)).unwrap();
        heap.insert(Logged::Txn(&running), &TupleMeta::new(7), &row(b"r"))
            .unwrap();
        let ended = TxnLog::new(done, Arc::clone(&storage.bpm));
        heap.insert(Logged::Txn(&ended), &TupleMeta::new(8), &row(b"e"))
            .unwrap();
        ended.end();
        let checkpoint = sharp(&storage.bpm).unwrap();
        let entries = checkpoint.att.entries();
        assert_eq!(entries.len(), 1, "{entries:?}");
        assert_eq!(entries[0].0, 7);
        assert_eq!(Some(entries[0].1.last_lsn), log.last_lsn(7));
        assert!(checkpoint.log_needed_from() <= entries[0].1.first_lsn);
        let decoded = Checkpoint::decode(&checkpoint.encode(), 0).unwrap();
        assert_eq!(decoded, checkpoint);
    }
}
