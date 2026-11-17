//! Transactions in the log: the active transaction table, the records
//! that end a transaction, and commit stamping, which runtime commits and
//! recovery share.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use common::hash::IntKeys;
use common::{INVALID_LSN, Lsn, Rid, TXN_UNCOMMITTED_BIT, TxnId};

use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::error::{Result, StorageError};
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
    /// `System` record belongs to no transaction.
    pub(crate) fn link(&mut self, tid: TxnId, kind: Kind, lsn: Lsn) -> Lsn {
        if kind == Kind::System {
            return INVALID_LSN;
        }
        let entry = self.0.entry(tid).or_insert(TxnEntry {
            first_lsn: lsn,
            last_lsn: INVALID_LSN,
            status: TxnStatus::Running,
        });
        let prev_lsn = std::mem::replace(&mut entry.last_lsn, lsn);
        match kind {
            Kind::Commit => entry.status = TxnStatus::Committed,
            Kind::Abort => entry.status = TxnStatus::Aborting,
            Kind::End => {
                self.0.remove(&tid);
            }
            Kind::Update | Kind::Clr | Kind::Completion | Kind::System => {}
        }
        prev_lsn
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
/// A transaction that changes nothing logs nothing, not even its end.
pub struct TxnLog {
    tid: TxnId,
    bpm: Arc<BufferPoolManager>,
    /// Set once a record of the transaction is appended.
    logged: AtomicBool,
}

thread_local! {
    static MARKER: std::cell::RefCell<RecordBuf> = std::cell::RefCell::new(RecordBuf::new());
}

impl TxnLog {
    pub(crate) fn new(tid: TxnId, bpm: Arc<BufferPoolManager>) -> Self {
        Self {
            tid,
            bpm,
            logged: AtomicBool::new(false),
        }
    }

    pub fn tid(&self) -> TxnId {
        self.tid
    }

    pub(crate) fn bpm(&self) -> &Arc<BufferPoolManager> {
        &self.bpm
    }

    pub(crate) fn note_logged(&self) {
        self.logged.store(true, Ordering::Relaxed);
    }

    pub(crate) fn has_records(&self) -> bool {
        self.logged.load(Ordering::Relaxed)
    }

    /// Appends `Commit` and returns once it is durable: from then on the
    /// transaction has committed, and a crash leaves recovery to finish it.
    /// Returns `false` at once for a transaction with no records. Aborts the
    /// process if the log fails, since whether the commit survives is then
    /// unknown.
    pub(crate) fn commit(&self) -> bool {
        if !self.has_records() {
            return false;
        }
        let lsn = self.append(Kind::Commit);
        if let Err(e) = self.bpm.log().flush_to(lsn) {
            fatal(format_args!(
                "cannot make the commit of transaction {} durable: {e}",
                self.tid
            ));
        }
        true
    }

    /// Appends `Abort` and returns its LSN.
    pub(crate) fn abort(&self) -> Lsn {
        self.append(Kind::Abort)
    }

    /// Appends `End`: nothing of the transaction is left to do. Not
    /// synced: recovery that misses it does the rest again, finding nothing
    /// left to change.
    pub(crate) fn end(&self) {
        self.append(Kind::End);
    }

    fn append(&self, kind: Kind) -> Lsn {
        let appended = MARKER.with_borrow_mut(|record| {
            record.begin(kind, self.tid);
            record.finish();
            self.bpm.log().append(record, |_| {})
        });
        self.note_logged();
        appended.unwrap_or_else(|e| {
            fatal(format_args!(
                "cannot log the {kind:?} of transaction {}: {e}",
                self.tid
            ))
        })
    }
}

/// Commits the tuples `txn` wrote at `writes`, sorted by rid, each with the
/// payload length its commit keeps (`None`: all of it), in one `Completion`
/// record per page. A tuple no longer locked by `txn` is left alone:
/// recovery finds those whose stamp reached the log before a crash. Calls
/// `deleted` with each tuple the commit leaves deleted.
pub(crate) fn stamp(
    txn: &TxnLog,
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

/// Finishes the commit of a transaction whose `Commit` is in the log but
/// whose `End` is not: stamps every tuple its `Update` records name that it
/// still holds, keeping the payload length of its newest record that set
/// one, then logs its `End`.
pub(crate) fn complete(txn: &TxnLog, last_lsn: Lsn) -> Result<()> {
    let log = txn.bpm.log();
    let mut writes: BTreeMap<Rid, Option<usize>> = BTreeMap::new();
    let mut buf = Vec::new();
    let mut lsn = last_lsn;
    while lsn != INVALID_LSN {
        let record = log.read(lsn, &mut buf)?;
        if record.kind == Kind::Update
            && let Undo::Heap { rid, keep_len, .. } = record.undo()?
        {
            let kept = writes.entry(rid).or_insert(keep_len);
            *kept = kept.or(keep_len);
        }
        lsn = record.prev_lsn;
    }
    let writes: Vec<_> = writes.into_iter().collect();
    stamp(txn, &writes, |_| {})?;
    txn.end();
    Ok(())
}
