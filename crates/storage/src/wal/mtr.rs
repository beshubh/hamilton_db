//! Mini-transactions: the only way a durable page changes.

use std::cell::RefCell;

use common::{INVALID_LSN, INVALID_TXN_ID, Lsn, PageId};
use smallvec::SmallVec;

use crate::buffer::arc_replacer::AccessType;
use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::error::{Result, StorageError};
use crate::page::page_guard::WritePageGuard;
use crate::page::page_header::set_page_lsn;
use crate::page::{PAGE_SIZE, TEMP_PAGE_BASE};
use crate::wal::record::{Kind, RecordBuf, encode_page_changes};

/// Who a change belongs to, which decides the kind of record that logs it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Logged {
    /// No transaction: redo-only.
    System,
}

impl Logged {
    fn kind(self) -> Kind {
        match self {
            Self::System => Kind::System,
        }
    }
}

thread_local! {
    static RECORD: RefCell<RecordBuf> = RefCell::new(RecordBuf::new());
}

#[cfg(test)]
thread_local! {
    /// Runs in `finish` after the append and before the frees, so a test
    /// can take a checkpoint in between.
    pub(crate) static BEFORE_FREES: RefCell<Option<Box<dyn FnMut()>>> =
        const { RefCell::new(None) };
}

/// Page changes logged as one record, which redo applies whole or not at
/// all. A changed page stays latched until the record is appended; an
/// unchanged one is released when its `MtrPage` drops, so B+tree writers can
/// still let go of safe ancestors. Dropping an mtr commits it.
pub struct Mtr<'a> {
    bpm: &'a BufferPoolManager,
    by: Logged,
    state: RefCell<State>,
}

#[derive(Default)]
struct State {
    /// Changed pages whose `MtrPage` was dropped, still latched.
    changed: SmallVec<[WritePageGuard; 4]>,
    frees: SmallVec<[PageId; 2]>,
}

impl<'a> Mtr<'a> {
    pub(crate) fn new(bpm: &'a BufferPoolManager, by: Logged) -> Self {
        Self {
            bpm,
            by,
            state: RefCell::default(),
        }
    }

    /// Write-latches `page_id` for this mtr. A page this mtr already changed
    /// comes back still latched. Fails with `InvalidPageId` for a temporary
    /// page, which is never logged.
    pub fn write_page(&self, page_id: PageId) -> Result<MtrPage<'_>> {
        if page_id >= TEMP_PAGE_BASE {
            return Err(StorageError::InvalidPageId(page_id));
        }
        let held = {
            let mut state = self.state.borrow_mut();
            let at = state.changed.iter().position(|g| g.page_id() == page_id);
            at.map(|at| state.changed.remove(at))
        };
        let (guard, changed) = match held {
            Some(guard) => (guard, true),
            None => (
                self.bpm.checked_write_page(page_id, AccessType::Unknown)?,
                false,
            ),
        };
        Ok(MtrPage {
            mtr: self,
            guard: Some(guard),
            changed,
        })
    }

    /// Allocates a page and write-latches it. It reads as zeros.
    pub fn new_page(&self) -> Result<MtrPage<'_>> {
        self.write_page(self.bpm.new_page())
    }

    /// Deletes `page_id` once this mtr's record is appended. The caller has
    /// unlinked the page; a thread still pinning it for a moment, such as a
    /// scan that just moved past it, is waited out.
    pub fn free_page(&self, page_id: PageId) {
        self.state.borrow_mut().frees.push(page_id);
    }

    /// Logs and releases the changed pages, then deletes the freed ones.
    /// Returns the record's LSN, or `None` if the mtr changed nothing.
    ///
    /// # Panics
    /// If the log fails: the change is in memory but can never be logged.
    pub fn commit(mut self) -> Option<Lsn> {
        self.finish()
    }

    fn finish(&mut self) -> Option<Lsn> {
        let State { mut changed, frees } = std::mem::take(self.state.get_mut());
        if changed.is_empty() && frees.is_empty() {
            return None;
        }
        let lsn = self.append(&changed, &frees);
        for guard in &mut changed {
            set_page_lsn(guard.data_mut_for_mtr(), lsn);
        }
        drop(changed);
        #[cfg(test)]
        BEFORE_FREES.with_borrow_mut(|hook| hook.as_mut().map(|hook| hook()));
        for &page_id in &frees {
            loop {
                match self.bpm.discard_page(page_id) {
                    Err(StorageError::PagePinned(_)) => std::thread::yield_now(),
                    Err(e) => {
                        // The free is logged; a checkpoint must not save a
                        // directory that still lists the page.
                        eprintln!("hamilton: cannot delete page {page_id}, freed at {lsn}: {e}");
                        std::process::abort()
                    }
                    Ok(()) => break,
                }
            }
        }
        if !frees.is_empty() {
            self.bpm.checkpoints().free_applied(lsn);
        }
        Some(lsn)
    }

    /// Logs an image of every changed page that is not freed, and the frees.
    fn append(&self, changed: &[WritePageGuard], frees: &[PageId]) -> Lsn {
        let mut record = RECORD.take();
        record.begin(self.by.kind(), INVALID_TXN_ID);
        let images = changed
            .iter()
            .filter(|guard| !frees.contains(&guard.page_id()))
            .map(|guard| (guard.page_id(), guard.get_data()));
        encode_page_changes(record.body(), images, frees);
        record.finish();
        // Under the log's lock, so whoever reads the log's end past this
        // record also sees the rec_lsn of every page it changed, and its
        // frees as pending. Takes no buffer pool, frame or disk lock: an
        // eviction holds a frame latch while it waits on the log.
        let appended = self.bpm.log().append(&record, |lsn| {
            for guard in changed {
                guard.frame().set_rec_lsn_if_clean(lsn);
            }
            if !frees.is_empty() {
                self.bpm.checkpoints().free_logged(lsn);
            }
            INVALID_LSN
        });
        RECORD.set(record);
        appended.unwrap_or_else(|e| {
            // Unwinding would release latches on pages the log never got, and
            // the server catches panics, so they could reach disk.
            eprintln!("hamilton: cannot log a page change: {e}");
            std::process::abort()
        })
    }
}

impl Drop for Mtr<'_> {
    fn drop(&mut self) {
        let state = self.state.get_mut();
        if std::thread::panicking() && !(state.changed.is_empty() && state.frees.is_empty()) {
            // The panic may have stopped a change halfway; logging it would
            // make that durable. Recovery redoes whole mtrs only.
            eprintln!("hamilton: panicked in the middle of a page change");
            std::process::abort();
        }
        self.finish();
    }
}

/// A page write-latched through an `Mtr`.
pub struct MtrPage<'m> {
    mtr: &'m Mtr<'m>,
    /// `Some` until `drop`.
    guard: Option<WritePageGuard>,
    changed: bool,
}

impl MtrPage<'_> {
    pub fn page_id(&self) -> PageId {
        self.guard().page_id()
    }

    pub fn data(&self) -> &[u8; PAGE_SIZE] {
        self.guard().get_data()
    }

    /// Mutable access to the page. The whole page is logged when the mtr
    /// commits, whether it changed or not, so check with `data` first when
    /// the change may turn out not to happen.
    pub fn raw_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        self.changed = true;
        self.guard
            .as_mut()
            .expect("guard is held until drop")
            .data_mut_for_mtr()
    }

    /// Whether the page has changes not written to disk yet.
    pub fn is_dirty(&self) -> bool {
        self.guard().is_dirty()
    }

    fn guard(&self) -> &WritePageGuard {
        self.guard.as_ref().expect("guard is held until drop")
    }
}

impl Drop for MtrPage<'_> {
    fn drop(&mut self) {
        let guard = self.guard.take().expect("guard is held until drop");
        if self.changed {
            self.mtr.state.borrow_mut().changed.push(guard);
        }
    }
}
