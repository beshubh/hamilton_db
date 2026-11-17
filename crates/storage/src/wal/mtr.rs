//! Mini-transactions: the only way a durable page changes.

use std::cell::RefCell;
use std::ops::Range;

use common::{INVALID_LSN, INVALID_TXN_ID, Lsn, PageId};
use smallvec::SmallVec;

use crate::buffer::arc_replacer::AccessType;
use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::error::{Result, StorageError};
use crate::page::page_guard::WritePageGuard;
use crate::page::page_header::set_page_lsn;
use crate::page::{PAGE_SIZE, TEMP_PAGE_BASE};
use crate::wal::op::{Ops, PageOp};
use crate::wal::record::{Kind, PageChangesWriter, RecordBuf};

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
    /// The ops buffer of the last mtr, kept so the next allocates nothing.
    static OPS: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
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
///
/// A page changed only through `MtrPage::apply` is logged as its ops,
/// unless it was clean. The first record to change a page after a write
/// holds all of it, as an image or as ops that format it first, so redo of
/// a dirty page never needs the page's copy on disk.
pub struct Mtr<'a> {
    bpm: &'a BufferPoolManager,
    by: Logged,
    state: RefCell<State>,
}

#[derive(Default)]
struct State {
    /// Changed pages whose `MtrPage` was dropped, still latched.
    changed: SmallVec<[Changed; 4]>,
    frees: SmallVec<[PageId; 2]>,
    /// The encoded ops of every page, which `Logging::Ops` points into.
    ops: Vec<u8>,
}

struct Changed {
    guard: WritePageGuard,
    logging: Logging,
}

/// How an mtr logs a page it changed.
enum Logging {
    /// As the ops it made, unless it decides on an image at commit.
    Ops {
        /// Where the page's ops are in `State::ops`, in the order made.
        runs: SmallVec<[Range<usize>; 1]>,
        /// The page before the mtr changed it, to check that redo of the
        /// ops gives what they did.
        #[cfg(debug_assertions)]
        before: Box<crate::page::PageBuf>,
    },
    /// As an image: it was changed through `raw_mut`.
    Image,
}

impl<'a> Mtr<'a> {
    pub(crate) fn new(bpm: &'a BufferPoolManager, by: Logged) -> Self {
        Self {
            bpm,
            by,
            state: RefCell::new(State {
                ops: OPS.take(),
                ..State::default()
            }),
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
            let at = state
                .changed
                .iter()
                .position(|page| page.guard.page_id() == page_id);
            at.map(|at| state.changed.remove(at))
        };
        let (guard, logging) = match held {
            Some(Changed { guard, logging }) => (guard, Some(logging)),
            None => (
                self.bpm.checked_write_page(page_id, AccessType::Unknown)?,
                None,
            ),
        };
        Ok(MtrPage {
            mtr: self,
            guard: Some(guard),
            logging,
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
        let State {
            mut changed,
            frees,
            mut ops,
        } = std::mem::take(self.state.get_mut());
        let lsn =
            (!changed.is_empty() || !frees.is_empty()).then(|| self.append(&changed, &frees, &ops));
        // `commit` runs this twice, the second time with no buffer to keep.
        if ops.capacity() > 0 {
            ops.clear();
            OPS.set(ops);
        }
        let lsn = lsn?;
        for page in &mut changed {
            set_page_lsn(page.guard.data_mut_for_mtr(), lsn);
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

    /// Logs every changed page that is not freed, as its ops or an image,
    /// and the frees.
    fn append(&self, changed: &[Changed], frees: &[PageId], ops: &[u8]) -> Lsn {
        #[cfg(debug_assertions)]
        check_redo(changed, ops);
        let mut record = RECORD.take();
        record.begin(self.by.kind(), INVALID_TXN_ID);
        let mut pages = PageChangesWriter::new(record.body());
        for page in changed {
            let page_id = page.guard.page_id();
            if frees.contains(&page_id) {
                continue;
            }
            match page.ops_to_log(ops) {
                Some(runs) => pages.ops(page_id, runs.iter().map(|run| &ops[run.clone()])),
                None => pages.image(page_id, page.guard.get_data()),
            }
        }
        pages.finish(frees);
        record.finish();
        // Under the log's lock, so whoever reads the log's end past this
        // record also sees the rec_lsn of every page it changed, and its
        // frees as pending. Takes no buffer pool, frame or disk lock: an
        // eviction holds a frame latch while it waits on the log.
        let appended = self.bpm.log().append(&record, |lsn| {
            for page in changed {
                page.guard.frame().set_rec_lsn_if_clean(lsn);
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

impl Changed {
    /// The runs of ops to log for the page, or `None` to log its image. The
    /// page's write latch keeps its `rec_lsn` from changing until the
    /// append.
    fn ops_to_log(&self, ops: &[u8]) -> Option<&[Range<usize>]> {
        let Logging::Ops { runs, .. } = &self.logging else {
            return None;
        };
        let len: usize = runs.iter().map(ExactSizeIterator::len).sum();
        let first_change = self.guard.frame().rec_lsn() == INVALID_LSN;
        let replaces_page = Ops::new(&ops[runs[0].clone()]).replace_page();
        (len < PAGE_SIZE && (replaces_page || !first_change)).then_some(runs)
    }
}

/// Checks that redo of each page's ops, made on the page as it was before
/// the mtr, gives the page they left, so `apply` depends on nothing but
/// the page and the op. Ops that format the page are made on junk instead,
/// to check that they need nothing from before.
#[cfg(debug_assertions)]
fn check_redo(changed: &[Changed], ops: &[u8]) {
    for page in changed {
        let Logging::Ops { runs, before } = &page.logging else {
            continue;
        };
        let logged: Vec<u8> = runs
            .iter()
            .flat_map(|run| &ops[run.clone()])
            .copied()
            .collect();
        let logged = Ops::new(&logged);
        let mut redone = before.clone();
        if logged.replace_page() {
            redone.fill(0xa5);
        }
        let result = logged.apply(&mut redone);
        if result.is_err() || redone.0 != *page.guard.get_data() {
            eprintln!(
                "hamilton: redo of the ops on page {} would not give the page they made: {result:?}, {:?}",
                page.guard.page_id(),
                logged.iter().collect::<Vec<_>>(),
            );
            std::process::abort();
        }
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
    /// `None` until the page changes.
    logging: Option<Logging>,
}

impl MtrPage<'_> {
    pub fn page_id(&self) -> PageId {
        self.guard().page_id()
    }

    pub fn data(&self) -> &[u8; PAGE_SIZE] {
        self.guard().get_data()
    }

    /// Makes `op` on the page and logs it as itself, a few bytes instead of
    /// the page. Fails as `op.apply` does, changing nothing.
    pub fn apply(&mut self, op: PageOp<'_>) -> Result<()> {
        #[cfg(debug_assertions)]
        let before = self
            .logging
            .is_none()
            .then(|| Box::new(crate::page::PageBuf(*self.data())));
        self.guard
            .as_mut()
            .expect("guard is held until drop")
            .change_for_mtr(|data| op.apply(data))?;
        let logging = self.logging.get_or_insert_with(|| Logging::Ops {
            runs: SmallVec::new(),
            #[cfg(debug_assertions)]
            before: before.expect("taken while the page had no logging"),
        });
        if let Logging::Ops { runs, .. } = logging {
            let ops = &mut self.mtr.state.borrow_mut().ops;
            let start = ops.len();
            op.encode(ops);
            match runs.last_mut() {
                Some(run) if run.end == start => run.end = ops.len(),
                _ => runs.push(start..ops.len()),
            }
        }
        Ok(())
    }

    /// Mutable access to the page. The whole page is logged when the mtr
    /// commits, whether it changed or not, so check with `data` first when
    /// the change may turn out not to happen.
    pub fn raw_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        self.logging = Some(Logging::Image);
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
        if let Some(logging) = self.logging.take() {
            let changed = Changed { guard, logging };
            self.mtr.state.borrow_mut().changed.push(changed);
        }
    }
}
