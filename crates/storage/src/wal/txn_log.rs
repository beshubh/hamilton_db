//! Transactions in the log: the order of a transaction's records, the
//! active transaction table, each transaction's handle on the log, and the
//! work after a `Commit`, stamping and freeing the storage truncates
//! replaced and drops gave up, which runtime commits and recovery share.

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use common::hash::IntKeys;
use common::{INVALID_LSN, Lsn, PageId, Rid, TXN_UNCOMMITTED_BIT, TxnId};

use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::error::{Result, StorageError};
use crate::index::btree_index::BtreeIndex;
use crate::table::table_heap;
use crate::table::table_page::TablePage;
use crate::wal::mtr::Logged;
use crate::wal::op::PageOp;
use crate::wal::record::{Kind, RecordBuf, Undo};
use crate::wal::{fatal, when_a_frame_is_free};

/// How far a transaction in the active transaction table got.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnStatus {
    Running,
    /// Its `Commit` is in the log. Some of its stamps may not be.
    Committed,
    /// Its `Abort` is in the log. Some of its undo may not be.
    Aborting,
}

impl TxnStatus {
    /// The status of a transaction in `status` once it logs a record of
    /// `kind`, or `None` if it may not log one there. A status of `None` is
    /// no entry in the table: the transaction has logged nothing yet, or has
    /// ended. The one table of the order a transaction's records come in:
    /// analysis checks the log against it, and `TxnLog` takes only its
    /// steps.
    pub(crate) fn after(status: Option<Self>, kind: Kind) -> Option<Option<Self>> {
        use TxnStatus::{Aborting, Committed, Running};
        let (from, to): (&[Option<Self>], _) = match kind {
            Kind::Update => (&[None, Some(Running)], Some(Running)),
            Kind::Commit => (&[Some(Running)], Some(Committed)),
            Kind::Abort => (&[Some(Running)], Some(Aborting)),
            Kind::Completion => (&[Some(Committed)], Some(Committed)),
            Kind::Clr => (&[Some(Aborting)], Some(Aborting)),
            Kind::End => (&[Some(Committed), Some(Aborting)], None),
            // No transaction's: `Att::link` passes it by.
            Kind::System => (&[], None),
        };
        from.contains(&status).then_some(to)
    }
}

/// A record its transaction may not log in the status it is in.
#[derive(Debug)]
pub(crate) struct OutOfOrder {
    tid: TxnId,
    kind: Kind,
    status: Option<TxnStatus>,
}

impl fmt::Display for OutOfOrder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self { tid, kind, status } = self;
        match status {
            Some(status) => write!(f, "{kind:?} of transaction {tid}, which is {status:?}"),
            None => write!(
                f,
                "{kind:?} of transaction {tid}, which has logged nothing or has ended"
            ),
        }
    }
}

/// A transaction's first and last records in the log, and its status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TxnEntry {
    pub first_lsn: Lsn,
    pub last_lsn: Lsn,
    pub status: TxnStatus,
}

/// The active transaction table: every transaction with records in the log
/// and no `End` yet. The log keeps one, changed only as it appends (see
/// `Log::append`), so it always agrees with the log; analysis rebuilds one
/// with the same `link`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Att(HashMap<TxnId, TxnEntry, IntKeys>);

/// Bytes an entry takes in a checkpoint.
const ENTRY_SIZE: usize = 25;

impl Att {
    /// Records that `tid`'s record of `kind` is at `lsn`, and returns the
    /// transaction's record before it, the new record's `prev_lsn`. A
    /// `System` record belongs to no transaction. Fails, changing nothing,
    /// if the transaction may not log `kind` now (`TxnStatus::after`).
    pub(crate) fn link(&mut self, tid: TxnId, kind: Kind, lsn: Lsn) -> Result<Lsn, OutOfOrder> {
        if kind == Kind::System {
            return Ok(INVALID_LSN);
        }
        let slot = self.0.entry(tid);
        let entry = match &slot {
            Entry::Occupied(slot) => Some(*slot.get()),
            Entry::Vacant(_) => None,
        };
        let status = entry.map(|entry| entry.status);
        match TxnStatus::after(status, kind) {
            None => return Err(OutOfOrder { tid, kind, status }),
            Some(Some(status)) => {
                let first_lsn = entry.map_or(lsn, |entry| entry.first_lsn);
                slot.insert_entry(TxnEntry {
                    first_lsn,
                    last_lsn: lsn,
                    status,
                });
            }
            Some(None) => {
                if let Entry::Occupied(slot) = slot {
                    slot.remove();
                }
            }
        }
        Ok(entry.map_or(INVALID_LSN, |entry| entry.last_lsn))
    }

    pub fn get(&self, tid: TxnId) -> Option<&TxnEntry> {
        self.0.get(&tid)
    }

    /// The transactions, in Tid order.
    pub fn entries(&self) -> Vec<(TxnId, TxnEntry)> {
        let mut entries: Vec<_> = self.0.iter().map(|(&tid, &entry)| (tid, entry)).collect();
        entries.sort_unstable_by_key(|&(tid, _)| tid);
        entries
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The first record of the transaction that began logging first.
    pub(crate) fn oldest_first_lsn(&self) -> Option<Lsn> {
        self.0.values().map(|entry| entry.first_lsn).min()
    }

    /// Appends `num u32 | (tid u64 | first_lsn u64 | last_lsn u64 |
    /// status u8)*`, little-endian, in Tid order.
    pub(crate) fn encode(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&(self.0.len() as u32).to_le_bytes());
        for (tid, entry) in self.entries() {
            buf.extend_from_slice(&tid.to_le_bytes());
            buf.extend_from_slice(&entry.first_lsn.to_le_bytes());
            buf.extend_from_slice(&entry.last_lsn.to_le_bytes());
            buf.push(match entry.status {
                TxnStatus::Running => 1,
                TxnStatus::Committed => 2,
                TxnStatus::Aborting => 3,
            });
        }
    }

    /// The table at the start of `bytes`, as `encode` wrote it, and the
    /// bytes after it.
    pub(crate) fn decode(bytes: &[u8]) -> Result<(Self, &[u8])> {
        let corrupt = || StorageError::CorruptMetadata("bad transaction table".into());
        let (num, rest) = bytes.split_first_chunk::<4>().ok_or_else(corrupt)?;
        let len = u32::from_le_bytes(*num) as usize * ENTRY_SIZE;
        let (entries, rest) = rest.split_at_checked(len).ok_or_else(corrupt)?;
        let mut att = Self::default();
        for entry in entries.chunks_exact(ENTRY_SIZE) {
            let u64_at = |at: usize| u64::from_le_bytes(entry[at..at + 8].try_into().unwrap());
            let status = match entry[24] {
                1 => TxnStatus::Running,
                2 => TxnStatus::Committed,
                3 => TxnStatus::Aborting,
                _ => return Err(corrupt()),
            };
            let entry = TxnEntry {
                first_lsn: u64_at(8),
                last_lsn: u64_at(16),
                status,
            };
            att.0.insert(u64_at(0), entry);
        }
        Ok((att, rest))
    }
}

/// A transaction's handle on the log, through which its records chain.
/// `S` is how far the transaction got. Each step consumes the handle and
/// returns the next, so a record out of order does not compile.
///
/// No `End` before the `Commit` or `Abort`:
///
/// ```compile_fail,E0599
/// # use storage::wal::txn_log::{Running, TxnLog};
/// fn end(txn: TxnLog<Running>) {
///     txn.end();
/// }
/// ```
///
/// No `Completion` before the `Commit`:
///
/// ```compile_fail,E0308
/// # use storage::wal::{Logged, txn_log::{Running, TxnLog}};
/// fn complete(txn: &TxnLog<Running>) -> Logged<'_> {
///     Logged::Completion(txn)
/// }
/// ```
///
/// No `Update` after it:
///
/// ```compile_fail,E0308
/// # use storage::wal::{Logged, txn_log::{Committed, TxnLog}};
/// fn update(txn: &TxnLog<Committed>) -> Logged<'_> {
///     Logged::Txn(txn)
/// }
/// ```
///
/// No `Commit` after the `Abort`:
///
/// ```compile_fail,E0599
/// # use storage::wal::txn_log::{Aborting, TxnLog};
/// fn commit(txn: TxnLog<Aborting>) {
///     txn.commit();
/// }
/// ```
///
/// No undo before the `Abort`:
///
/// ```compile_fail,E0308
/// # use storage::wal::{Logged, txn_log::{Running, TxnLog}};
/// fn undo(txn: &TxnLog<Running>) -> Logged<'_> {
///     Logged::Clr { txn, undo_next: 0 }
/// }
/// ```
///
/// No undo after the `Commit`:
///
/// ```compile_fail,E0308
/// # use storage::wal::{Logged, txn_log::{Committed, TxnLog}};
/// fn undo(txn: &TxnLog<Committed>) -> Logged<'_> {
///     Logged::Clr { txn, undo_next: 0 }
/// }
/// ```
///
/// No second `Commit`:
///
/// ```compile_fail,E0382
/// # use storage::wal::txn_log::{Running, TxnLog};
/// fn commit_twice(txn: TxnLog<Running>) {
///     let _ = txn.commit();
///     let _ = txn.commit();
/// }
/// ```
///
/// Nothing after the `End`:
///
/// ```compile_fail,E0382
/// # use storage::wal::{Logged, txn_log::{Committed, TxnLog}};
/// fn complete_after_end(txn: TxnLog<Committed>) {
///     txn.end();
///     let _ = Logged::Completion(&txn);
/// }
/// ```
///
/// Only this crate makes a handle: for a transaction that begins, or one
/// recovery resumes from the active transaction table (`resume`). The
/// steps are public anyway, so that the examples fail only for their order.
#[must_use = "a transaction's log ends with `end()`, or recovery finishes it"]
pub struct TxnLog<S> {
    tid: TxnId,
    bpm: Arc<BufferPoolManager>,
    state: S,
}

/// A transaction that may still change pages. It may have logged nothing,
/// and then logs no `Commit`, `Abort` or `End` either.
pub struct Running {
    /// Set once a record of the transaction is appended.
    logged: AtomicBool,
}

/// A transaction whose `Commit` is durable, with its stamps and frees
/// (`Completion`s) and its `End` left to log.
pub struct Committed;

/// A transaction whose `Abort` is logged, with its undo (`Clr`s) and its
/// `End` left to log.
pub struct Aborting;

thread_local! {
    static MARKER: std::cell::RefCell<RecordBuf> = std::cell::RefCell::new(RecordBuf::new());
}

impl<S> TxnLog<S> {
    pub fn tid(&self) -> TxnId {
        self.tid
    }

    pub(crate) fn bpm(&self) -> &Arc<BufferPoolManager> {
        &self.bpm
    }

    fn step<T>(self, state: T) -> TxnLog<T> {
        TxnLog {
            tid: self.tid,
            bpm: self.bpm,
            state,
        }
    }

    fn append(&self, kind: Kind) -> Lsn {
        let appended = MARKER.with_borrow_mut(|record| {
            record.begin(kind, self.tid);
            record.finish();
            self.bpm.log().append(record, |_| {})
        });
        appended.unwrap_or_else(|e| {
            fatal(format_args!(
                "cannot log the {kind:?} of transaction {}: {e}",
                self.tid
            ))
        })
    }
}

impl TxnLog<Running> {
    /// For a transaction that begins. Recovery resumes one with `resume`.
    pub(crate) fn new(tid: TxnId, bpm: Arc<BufferPoolManager>) -> Self {
        let logged = AtomicBool::new(false);
        Self {
            tid,
            bpm,
            state: Running { logged },
        }
    }

    pub(crate) fn note_logged(&self) {
        self.state.logged.store(true, Ordering::Relaxed);
    }

    fn has_records(&self) -> bool {
        self.state.logged.load(Ordering::Relaxed)
    }

    /// Appends `Commit` and returns once it is durable: from then on the
    /// transaction has committed, and a crash leaves recovery to finish it.
    /// Returns `None` at once for a transaction with no records. Aborts the
    /// process if the log fails, since whether the commit survives is then
    /// unknown.
    #[must_use = "a committed transaction's log ends with `end()`"]
    pub fn commit(self) -> Option<TxnLog<Committed>> {
        if !self.has_records() {
            return None;
        }
        let lsn = self.append(Kind::Commit);
        if let Err(e) = self.bpm.log().flush_to(lsn) {
            fatal(format_args!(
                "cannot make the commit of transaction {} durable: {e}",
                self.tid
            ));
        }
        Some(self.step(Committed))
    }

    /// Appends `Abort`, and returns the handle that undoes the transaction
    /// with the `Abort`'s LSN. Returns `None` for a transaction with no
    /// records.
    #[must_use = "an aborting transaction's log ends with `end()`"]
    pub fn abort(self) -> Option<(TxnLog<Aborting>, Lsn)> {
        if !self.has_records() {
            return None;
        }
        let lsn = self.append(Kind::Abort);
        Some((self.step(Aborting), lsn))
    }
}

impl TxnLog<Committed> {
    /// Appends `End` once the stamps and frees are logged. Not synced:
    /// recovery that misses it does them again, finding nothing left to
    /// change.
    pub fn end(self) {
        self.append(Kind::End);
    }
}

impl TxnLog<Aborting> {
    /// Appends `End` once every change is undone. Not synced, as for a
    /// commit.
    pub fn end(self) {
        self.append(Kind::End);
    }
}

/// A transaction recovery resumes, each with its last record in the log.
pub(crate) enum Resumed {
    Committed(TxnLog<Committed>, Lsn),
    Aborting(TxnLog<Aborting>, Lsn),
}

/// For recovery: transaction `tid`, in the state its `entry` in the active
/// transaction table holds. One still running is aborted, as a runtime
/// abort would be, so that every loser's undo follows its `Abort`.
pub(crate) fn resume(tid: TxnId, entry: &TxnEntry, bpm: Arc<BufferPoolManager>) -> Resumed {
    match entry.status {
        TxnStatus::Committed => {
            let txn = TxnLog {
                tid,
                bpm,
                state: Committed,
            };
            Resumed::Committed(txn, entry.last_lsn)
        }
        TxnStatus::Aborting => {
            let txn = TxnLog {
                tid,
                bpm,
                state: Aborting,
            };
            Resumed::Aborting(txn, entry.last_lsn)
        }
        TxnStatus::Running => {
            let running = TxnLog::new(tid, bpm);
            running.note_logged();
            let (txn, abort) = running.abort().expect("a resumed transaction has records");
            Resumed::Aborting(txn, abort)
        }
    }
}

/// Commits the tuples `txn` wrote at `writes`, sorted by rid, each with the
/// payload length its commit keeps (`None`: all of it), in one `Completion`
/// record per page. A tuple no longer locked by `txn` is left alone:
/// recovery finds those whose stamp reached the log before a crash. Calls
/// `deleted` with each tuple the commit leaves deleted.
pub(crate) fn stamp(
    txn: &TxnLog<Committed>,
    writes: &[(Rid, Option<usize>)],
    mut deleted: impl FnMut(Rid),
) -> Result<()> {
    let locked = txn.tid | TXN_UNCOMMITTED_BIT;
    for on_page in writes.chunk_by(|a, b| a.0.page_id == b.0.page_id) {
        let page_id = on_page[0].0.page_id;
        let mtr = txn.bpm.mtr(Logged::Completion(txn));
        let mut page = when_a_frame_is_free(|| mtr.write_page(page_id))?;
        for &(rid, keep_len) in on_page {
            let (meta, len) = {
                let heap = TablePage::new(page.data())?;
                let (meta, payload) = heap.get(rid.slot)?;
                (meta, payload.len())
            };
            if meta.ts != locked {
                continue;
            }
            page.apply(PageOp::HeapStamp {
                slot: rid.slot,
                keep_len: keep_len.unwrap_or(len),
            })?;
            if meta.is_deleted {
                deleted(rid);
            }
        }
    }
    Ok(())
}

/// Storage a commit frees: the heaps starting at `heaps` and the trees
/// whose header pages are `trees`. A truncate replaced it or a drop gave it
/// up.
pub(crate) struct Released {
    pub heaps: Vec<PageId>,
    pub trees: Vec<PageId>,
}

impl Released {
    /// Frees all of it in one record, logged as `txn`'s completion.
    pub(crate) fn free(&self, txn: &TxnLog<Committed>) -> Result<()> {
        free_storage(&txn.bpm, Logged::Completion(txn), &self.heaps, &self.trees)
    }

    /// The page whose free shows that all of it was freed.
    fn first(&self) -> PageId {
        let first = self.heaps.first().or(self.trees.first());
        *first.expect("a truncate or drop releases storage")
    }
}

/// Finishes a commit whose `End` the log lacks: stamps the tuples it still
/// holds, frees the storage its truncates replaced and its drops gave up,
/// and logs `End`. Its logged frees came after its stamps, and each storage
/// went in one record, so its first page shows whether that storage was
/// freed.
pub(crate) fn complete(txn: TxnLog<Committed>, last_lsn: Lsn) -> Result<()> {
    let log = txn.bpm.log();
    let mut writes: BTreeMap<Rid, Option<usize>> = BTreeMap::new();
    let mut released: Vec<Released> = Vec::new();
    let mut freed: HashSet<PageId, IntKeys> = HashSet::default();
    let mut buf = Vec::new();
    let mut lsn = last_lsn;
    while lsn != INVALID_LSN {
        let record = log.read(lsn, &mut buf)?;
        match record.kind {
            Kind::Update => match record.undo()? {
                Undo::Heap { rid, keep_len, .. } => {
                    let kept = writes.entry(rid).or_insert(keep_len);
                    *kept = kept.or(keep_len);
                }
                Undo::Truncate { old, .. } => {
                    let mut old = old.iter();
                    released.push(Released {
                        heaps: old.next().into_iter().collect(),
                        trees: old.collect(),
                    });
                }
                Undo::Drop { heaps, trees } => released.push(Released {
                    heaps: heaps.iter().collect(),
                    trees: trees.iter().collect(),
                }),
                Undo::IndexEntry { .. } | Undo::Create { .. } => {}
            },
            Kind::Completion => {
                let changes = record.page_changes()?.expect("a completion changes pages");
                freed.extend(changes.frees());
            }
            _ => {}
        }
        lsn = record.prev_lsn;
    }
    let writes: Vec<_> = writes
        .into_iter()
        .filter(|(rid, _)| !freed.contains(&rid.page_id))
        .collect();
    stamp(&txn, &writes, |_| {})?;
    for storage in released.iter().filter(|s| !freed.contains(&s.first())) {
        storage.free(&txn)?;
    }
    txn.end();
    Ok(())
}

/// Frees every page of the heaps starting at `heaps` and of the trees whose
/// header pages are `trees`. One record logged as `by`'s frees them all, so
/// a crash leaves all of them or none.
pub(crate) fn free_storage(
    bpm: &Arc<BufferPoolManager>,
    by: Logged<'_>,
    heaps: &[PageId],
    trees: &[PageId],
) -> Result<()> {
    let mut pages = Vec::new();
    for &heap in heaps {
        pages.extend(when_a_frame_is_free(|| table_heap::page_ids(bpm, heap))?);
    }
    for &header in trees {
        let tree = when_a_frame_is_free(|| BtreeIndex::open_tree(Arc::clone(bpm), header))?;
        pages.extend(when_a_frame_is_free(|| tree.page_ids())?);
    }
    let mtr = bpm.mtr(by);
    for page_id in pages {
        mtr.free_page(page_id);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::TxnStatus::{self, Committed};
    use crate::wal::record::Kind::{self, Abort, Clr, Commit, Completion, End, Update};

    /// The status `kinds` leave a new transaction in, or `None` if one of
    /// them is out of order.
    fn after(kinds: &[Kind]) -> Option<Option<TxnStatus>> {
        kinds
            .iter()
            .try_fold(None, |status, &kind| TxnStatus::after(status, kind))
    }

    #[test]
    fn a_transaction_commits_or_aborts_then_ends() {
        let commit = [Update, Update, Commit, Completion, Completion, End];
        assert_eq!(after(&commit), Some(None));
        assert_eq!(after(&[Update, Abort, Clr, Clr, End]), Some(None));
        assert_eq!(after(&[Update, Commit]), Some(Some(Committed)));
    }

    #[test]
    fn a_record_out_of_order_is_refused() {
        let out_of_order: [&[Kind]; 15] = [
            &[Commit],
            &[Abort],
            &[Completion],
            &[Clr],
            &[End],
            &[Update, Commit, Update],
            &[Update, Commit, Commit],
            &[Update, Abort, Abort],
            &[Update, Abort, Commit],
            &[Update, Completion],
            &[Update, Abort, Completion],
            &[Update, Clr],
            &[Update, Commit, Clr],
            &[Update, End],
            &[Update, Commit, End, Completion],
        ];
        for kinds in out_of_order {
            assert_eq!(after(kinds), None, "{kinds:?}");
        }
    }
}
