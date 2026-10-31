//! Transactions under multi-version timestamp ordering (MVTO).
//!
//! A transaction's Tid is its timestamp: the result must equal running the
//! transactions one after another in Tid order. A transaction reads each
//! tuple as of its Tid, by the rule in `undo_log`, recording the read in
//! `read_ts`, and writes versions stamped `tid | TXN_UNCOMMITTED_BIT`, which
//! write-lock their tuples until it ends. Storage reports a read or write
//! that would break Tid order as `Conflict`; the caller then aborts the
//! transaction.
//!
//! Every write goes into the write set, by table, so that commit can clear
//! the bit on each written tuple (`TableHeap::stamp_commit`) and abort can
//! put back each tuple's version from before the transaction
//! (`TableHeap::rollback`). Either way, the tuples it leaves deleted go to
//! the `TransactionManager`, which ends the Tid in the watermark. Its
//! background collector reclaims them once the watermark passes the Tid,
//! with the undo records below it.
//!
//! Index entries are added at once and never removed by a write: the
//! entries a transaction's writes leave behind are retired, and garbage
//! collection removes them. See `index_versions`.
//!
//! **Known gaps.** Phantoms: read timestamps guard the tuples a transaction
//! read, not rows inserted later that it should have seen, so schedules
//! with inserts are not serializable. Nothing is logged, so a commit or
//! abort that fails partway leaves some tuples done and the rest locked,
//! and the transaction stays running; a crash does the same to every
//! running transaction. Reclaimed tuples keep their slots (see
//! `table_page`).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use common::{PageId, Rid, Schema, TXN_UNCOMMITTED_BIT, Tuple, TxnId};
use parking_lot::{Mutex, RwLock, RwLockReadGuard};

use crate::concurrency::index_versions::{self, IndexedTable, TableEntry};
use crate::concurrency::read_ts::ReadTimestamps;
use crate::concurrency::watermark::Watermark;
use crate::error::{Result, StorageError};
use crate::index::{Index, IndexTrait, RowLocator};
use crate::table::table_heap::TableHeap;
use crate::table::table_iterator::{TableIterator, TableRow};
use crate::table::table_page::TupleMeta;
use crate::table::undo_log::UndoLog;

/// What an ended transaction leaves for garbage collection: the tuples it
/// left deleted and the index entries it retired.
#[derive(Default)]
pub(crate) struct Garbage {
    pub tuples: Vec<(Arc<TableHeap>, Rid)>,
    pub entries: Vec<TableEntry>,
}

impl Garbage {
    pub fn is_empty(&self) -> bool {
        self.tuples.is_empty() && self.entries.is_empty()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    Running,
    Committed,
    Aborted,
}

/// A running or finished transaction, begun by a `TransactionManager`.
///
/// Its methods read and write tables at its Tid and record every write.
/// They take `&self`, so the executors of one statement can share it. Each
/// fails with `TransactionNotRunning` once the transaction has ended.
///
/// A transaction must be committed or aborted: one dropped while running
/// would keep its Tid in the watermark forever and stall garbage collection.
/// Debug builds panic if that happens.
pub struct Transaction {
    tid: TxnId,
    /// Reads and writes hold this shared for their whole call, and commit
    /// and abort exclusively, so a transaction never ends halfway through a
    /// write and misses it.
    state: RwLock<TransactionState>,
    undo: Arc<UndoLog>,
    read_ts: Arc<ReadTimestamps>,
    watermark: Arc<Watermark>,
    /// Set once this is the oldest running transaction, which it then stays.
    oldest: AtomicBool,
    write_set: Mutex<HashMap<PageId, TableWrites>>,
    /// The manager's entry lock; see `index_versions`.
    entry_lock: Arc<RwLock<()>>,
    /// The index entries this transaction added or found already there,
    /// oldest first.
    index_writes: Mutex<Vec<IndexWrite>>,
    /// The index entries this transaction's writes left behind.
    retired: Mutex<Vec<TableEntry>>,
}

/// The tuples a transaction wrote in one table, keyed by the table's first
/// page. The schema is kept because rollback decodes the deltas.
struct TableWrites {
    heap: Arc<TableHeap>,
    schema: Schema,
    rids: HashSet<Rid>,
}

/// An index entry a transaction needs: one it `added`, or found already in
/// the index.
struct IndexWrite {
    entry: TableEntry,
    added: bool,
}

impl Transaction {
    pub(crate) fn new(
        tid: TxnId,
        undo: Arc<UndoLog>,
        read_ts: Arc<ReadTimestamps>,
        watermark: Arc<Watermark>,
        entry_lock: Arc<RwLock<()>>,
    ) -> Self {
        Self {
            tid,
            state: RwLock::new(TransactionState::Running),
            undo,
            read_ts,
            watermark,
            oldest: AtomicBool::new(false),
            write_set: Mutex::new(HashMap::new()),
            entry_lock,
            index_writes: Mutex::new(Vec::new()),
            retired: Mutex::new(Vec::new()),
        }
    }

    pub fn tid(&self) -> TxnId {
        self.tid
    }

    pub fn state(&self) -> TransactionState {
        *self.state.read()
    }

    /// Inserts `tuple`, locked until the transaction ends, and returns its
    /// `Rid`.
    pub fn insert(&self, heap: &Arc<TableHeap>, schema: &Schema, tuple: &Tuple) -> Result<Rid> {
        let _running = self.running()?;
        self.insert_locked(heap, schema, tuple)
    }

    /// Replaces the tuple at `rid` with `new` and returns where `new` is. If
    /// `new` does not fit on the tuple's page, the tuple is deleted and `new`
    /// inserted at a new `Rid`. Fails with `Conflict` if another transaction
    /// holds the tuple's lock, or committed or read it after this one's Tid,
    /// and with `TupleDeleted` if it is deleted.
    pub fn update(
        &self,
        heap: &Arc<TableHeap>,
        schema: &Schema,
        rid: Rid,
        new: &Tuple,
    ) -> Result<Rid> {
        let _running = self.running()?;
        match heap.update_versioned(rid, new, self.write_ts(), schema, &self.undo, &self.read_ts) {
            Ok(()) => {
                self.record_write(heap, schema, rid);
                Ok(rid)
            }
            Err(StorageError::PageFull) => {
                heap.delete_versioned(rid, self.write_ts(), &self.undo, &self.read_ts)?;
                self.record_write(heap, schema, rid);
                self.insert_locked(heap, schema, new)
            }
            Err(e) => Err(e),
        }
    }

    /// Deletes the tuple at `rid`. Fails as `update` does.
    pub fn delete(&self, heap: &Arc<TableHeap>, schema: &Schema, rid: Rid) -> Result<()> {
        let _running = self.running()?;
        heap.delete_versioned(rid, self.write_ts(), &self.undo, &self.read_ts)?;
        self.record_write(heap, schema, rid);
        Ok(())
    }

    /// Makes sure `index`, one of `table`'s, holds `key` for the row at
    /// `locator`, which this transaction has just written. Abort removes the
    /// entry if this added it.
    ///
    /// In a primary key index the key must also be free, as
    /// `index_versions` describes: returns `false`, adding nothing, if the
    /// row this transaction sees at another rid has it, and fails with
    /// `Conflict` if another transaction's newer or uncommitted version
    /// does. Add a row's primary key entry before its other entries.
    pub fn insert_entry(
        &self,
        table: &Arc<IndexedTable>,
        index: &Arc<Index>,
        key: &Tuple,
        locator: &RowLocator,
    ) -> Result<bool> {
        let _running = self.running()?;
        let added = {
            let _entries = self.entry_lock.read();
            index.insert_entry(key, locator)?
        };
        let free = if index.metadata().is_primary_key() {
            let rid = locator.as_rid().expect("the primary key index stores rids");
            self.key_is_free(table, index, key, rid)
        } else {
            Ok(true)
        };
        if matches!(free, Ok(false)) {
            if added {
                index.delete_entry(key, locator)?;
            }
            return Ok(false);
        }
        // Recorded on `Conflict` too, for abort to remove.
        self.index_writes.lock().push(IndexWrite {
            entry: TableEntry {
                table: Arc::clone(table),
                index: Arc::clone(index),
                key: key.clone(),
                locator: locator.clone(),
            },
            added,
        });
        free
    }

    /// Retires the entry `key` for the row at `locator` in `index`, one of
    /// `table`'s, which this transaction's write to the row left behind. It
    /// stays in the index; once this transaction has committed and no
    /// reader needs it, garbage collection removes it.
    pub fn retire_entry(
        &self,
        table: &Arc<IndexedTable>,
        index: &Arc<Index>,
        key: &Tuple,
        locator: &RowLocator,
    ) -> Result<()> {
        let _running = self.running()?;
        self.retired.lock().push(TableEntry {
            table: Arc::clone(table),
            index: Arc::clone(index),
            key: key.clone(),
            locator: locator.clone(),
        });
        Ok(())
    }

    /// The version of the tuple at `rid` this transaction sees, or `None` if
    /// there is none at its Tid. Fails with `Conflict` if an older
    /// transaction holds the tuple's lock.
    pub fn get(&self, heap: &TableHeap, schema: &Schema, rid: Rid) -> Result<Option<TableRow>> {
        let _running = self.running()?;
        heap.get_at(rid, self.tid, schema, &self.undo, self.reads())
    }

    /// The row this transaction sees that `locator`, from an entry of one of
    /// `table`'s indexes, leads to, with its rid: the row at the rid, or the
    /// one with the primary key. `None` if there is none. The row's key in
    /// the index is not checked; it may have changed since the entry was
    /// added. Fails as `get` does.
    pub fn row_at(
        &self,
        table: &IndexedTable,
        locator: &RowLocator,
    ) -> Result<Option<(Rid, Tuple)>> {
        let _running = self.running()?;
        let schema = &table.schema;
        let mut found = None;
        for rid in table.rids(locator)? {
            let row = table
                .heap
                .get_at(rid, self.tid, schema, &self.undo, self.reads())?;
            let Some((_, tuple)) = row else {
                continue;
            };
            // The primary key index holds old versions' rids too.
            if let RowLocator::PrimaryKey(pk) = locator {
                let primary_key = table.primary_key.as_ref().expect("checked by `rids`");
                if !index_versions::has_key(primary_key, schema, &tuple, pk)? {
                    continue;
                }
            }
            debug_assert!(found.is_none(), "two rows {locator:?} are visible");
            found = Some((rid, tuple));
        }
        Ok(found)
    }

    /// Scans `heap` as this transaction sees it. See `TableIterator`.
    pub fn scan(self: &Arc<Self>, heap: &TableHeap, schema: &Schema) -> Result<TransactionScan> {
        let _running = self.running()?;
        Ok(TransactionScan {
            txn: Arc::clone(self),
            iter: heap.iter()?,
            schema: schema.clone(),
        })
    }

    /// Clears the lock on every tuple written, or rolls each back along
    /// with the index entries added, and sets the state to `outcome`.
    /// Returns the garbage: the tuples left deleted (those deleted by a
    /// commit, and those inserted by an abort), and the entries retired by a
    /// commit or found already there by an abort. The manager calls this
    /// before ending the Tid.
    pub(crate) fn finish(&self, outcome: TransactionState) -> Result<Garbage> {
        let mut state = self.state.write();
        if *state != TransactionState::Running {
            return Err(StorageError::TransactionNotRunning(self.tid));
        }
        let mut garbage = Garbage::default();
        let mut index_writes = self.index_writes.lock();
        let mut retired = self.retired.lock();
        if outcome == TransactionState::Aborted {
            while let Some(IndexWrite { entry, added }) = index_writes.pop() {
                if added {
                    entry.index.delete_entry(&entry.key, &entry.locator)?;
                } else {
                    garbage.entries.push(entry);
                }
            }
            retired.clear();
        } else {
            index_writes.clear();
            garbage.entries.append(&mut retired);
        }
        let mut write_set = self.write_set.lock();
        for table in write_set.values() {
            for &rid in &table.rids {
                let is_deleted = match outcome {
                    TransactionState::Committed => {
                        table.heap.stamp_commit(rid, self.tid, &table.schema)?
                    }
                    TransactionState::Aborted => {
                        table
                            .heap
                            .rollback(rid, self.tid, &table.schema, &self.undo)?
                    }
                    TransactionState::Running => unreachable!("a transaction ends running"),
                };
                if is_deleted {
                    garbage.tuples.push((Arc::clone(&table.heap), rid));
                }
            }
        }
        write_set.clear();
        *state = outcome;
        Ok(garbage)
    }

    pub(crate) fn undo_log(&self) -> &Arc<UndoLog> {
        &self.undo
    }

    /// Holds the state shared, failing if the transaction has ended.
    fn running(&self) -> Result<RwLockReadGuard<'_, TransactionState>> {
        let state = self.state.read();
        if *state != TransactionState::Running {
            return Err(StorageError::TransactionNotRunning(self.tid));
        }
        Ok(state)
    }

    /// Where this transaction's reads are recorded: nowhere once it is the
    /// oldest running transaction. Only an older writer could conflict with
    /// its reads, and Tids only grow, so none is running or will begin.
    fn reads(&self) -> Option<&ReadTimestamps> {
        if !self.oldest.load(Ordering::Relaxed) {
            if self.watermark.get() != self.tid {
                return Some(&self.read_ts);
            }
            self.oldest.store(true, Ordering::Relaxed);
        }
        None
    }

    /// The `ts` this transaction writes versions at: its Tid, locked.
    fn write_ts(&self) -> u64 {
        self.tid | TXN_UNCOMMITTED_BIT
    }

    /// Inserts `tuple` locked and records it. The caller holds `running`.
    fn insert_locked(&self, heap: &Arc<TableHeap>, schema: &Schema, tuple: &Tuple) -> Result<Rid> {
        let mut has_nulls = false;
        for idx in 0..schema.len() {
            has_nulls |= tuple.is_null(schema, idx)?;
        }
        let meta = TupleMeta {
            has_nulls,
            ..TupleMeta::new(self.write_ts())
        };
        let rid = heap.insert(&meta, tuple)?;
        self.record_write(heap, schema, rid);
        Ok(rid)
    }

    /// Whether `key`, which this transaction has just added to the primary
    /// key `index` for its row at `rid`, is free: no other row the key leads
    /// to has it. See `index_versions` for the rule. The caller holds
    /// `running`.
    fn key_is_free(
        &self,
        table: &IndexedTable,
        index: &Index,
        key: &Tuple,
        rid: Rid,
    ) -> Result<bool> {
        let schema = &table.schema;
        let mut holders = Vec::new();
        index.scan_key(key, &mut holders)?;
        let others = holders
            .iter()
            .map(|locator| locator.as_rid().expect("the primary key index stores rids"))
            .filter(|&other| other != rid);
        for other in others {
            let versions = table
                .heap
                .versions_since(other, self.tid, schema, &self.undo)?;
            for (meta, version) in versions {
                let by_another = meta.ts > self.tid && meta.ts != self.write_ts();
                if by_another
                    && !meta.is_deleted
                    && index_versions::has_key(index, schema, &version, key)?
                {
                    return Err(StorageError::Conflict {
                        rid: other,
                        other: meta.ts & !TXN_UNCOMMITTED_BIT,
                    });
                }
            }
            let visible = table
                .heap
                .get_at(other, self.tid, schema, &self.undo, self.reads())?;
            if let Some((_, version)) = visible
                && index_versions::has_key(index, schema, &version, key)?
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn record_write(&self, heap: &Arc<TableHeap>, schema: &Schema, rid: Rid) {
        self.write_set
            .lock()
            .entry(heap.first_page_id())
            .or_insert_with(|| TableWrites {
                heap: Arc::clone(heap),
                schema: schema.clone(),
                rids: HashSet::new(),
            })
            .rids
            .insert(rid);
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if cfg!(debug_assertions)
            && !thread::panicking()
            && *self.state.get_mut() == TransactionState::Running
        {
            panic!(
                "transaction {} was dropped while running; commit or abort it",
                self.tid
            );
        }
    }
}

/// A scan of a table as a transaction sees it, page by page.
pub struct TransactionScan {
    txn: Arc<Transaction>,
    iter: TableIterator,
    schema: Schema,
}

impl TransactionScan {
    /// The rows of the next page that has any visible to the transaction,
    /// or `None` once the scan is done. Fails with `Conflict` as
    /// `Transaction::get` does.
    pub fn next_batch(&mut self) -> Result<Option<Vec<TableRow>>> {
        let _running = self.txn.running()?;
        self.iter
            .next_batch_at(self.txn.tid, &self.schema, &self.txn.undo, self.txn.reads())
    }
}
