//! Sequences, which hand out 1, 2, 3 and so on to identity columns, as
//! Postgres's do.
//!
//! A sequence is a relation of its own, named like `orders_id_seq`, whose
//! heap holds a single row:
//!
//! ```text
//! last_value BIGINT   the value handed out last, or the first one to hand
//!                     out while is_called is false
//! log_cnt    INT      values left before nextval must log ahead again,
//!                     covering the LOG_VALS values after the one it hands
//!                     out
//! is_called  BOOL     whether last_value has been handed out
//! ```
//!
//! `nextval` changes the row in place, outside any transaction, so a
//! rolled-back INSERT leaves a gap. Values reach disk before they are handed
//! out (`log_ahead`), so a crash skips up to LOG_VALS values rather than
//! handing any out again.
//!
//! `TRUNCATE ... RESTART IDENTITY` (`restart`) belongs to a transaction. It
//! must not reach disk before the truncate does, or a crash would bring
//! back rows whose ids the sequence hands out again. So until the
//! transaction ends, the restarted sequence counts in memory and the row
//! keeps its old value. The truncate's commit puts the restarted value in
//! the row once the empty table is on disk; anything else only ever moves
//! the row forward.

use std::fmt;
use std::sync::{Arc, LazyLock};

use common::{Column, DataType, PageId, Schema, SlotId, Tuple};
use parking_lot::Mutex;
use storage::buffer::arc_replacer::AccessType;
use storage::buffer::buffer_pool_manager::BufferPoolManager;
use storage::page::page_guard::WritePageGuard;
use storage::table::table_heap::TableHeap;
use storage::table::table_page::{TablePage, TupleMeta};
use zerocopy::byteorder::little_endian::{I32, I64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::catalog::TableOid;
use crate::error::{CatalogError, Result};
use crate::system_tables::CATALOG_TS;

/// How far past the value it hands out logging ahead covers; Postgres's
/// `SEQ_LOG_VALS`.
const LOG_VALS: i32 = 32;

/// The row is the only tuple on the heap's only page.
const SLOT: SlotId = 0;

/// The columns of every sequence.
pub(crate) static SCHEMA: LazyLock<Schema> = LazyLock::new(|| {
    Schema::new(vec![
        Column::new("last_value", DataType::BigInt, false),
        Column::new("log_cnt", DataType::Int, false),
        Column::new("is_called", DataType::Bool, false),
    ])
});

/// A row of `SCHEMA` as `Tuple::encode` lays it out, so `nextval` can
/// change it without allocating.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct SequenceRow {
    null_bitmap: u8,
    last_value: I64,
    log_cnt: I32,
    is_called: u8,
}

/// Creates a sequence's heap, holding the row of a sequence that hands out
/// 1 first.
pub(crate) fn create_heap(bpm: &Arc<BufferPoolManager>) -> Result<TableHeap> {
    let heap = TableHeap::create(Arc::clone(bpm))?;
    let row = SequenceRow {
        null_bitmap: 0,
        last_value: 1.into(),
        log_cnt: 0.into(),
        is_called: 0,
    };
    let tuple = Tuple::from_bytes(row.as_bytes().to_vec());
    let rid = heap.insert(&TupleMeta::new(CATALOG_TS), &tuple)?;
    debug_assert_eq!((rid.page_id, rid.slot), (heap.first_page_id(), SLOT));
    Ok(heap)
}

/// The largest value a sequence for a column of `data_type` hands out, or
/// `None` if no identity column has that type.
pub(crate) fn max_value(data_type: DataType) -> Option<i64> {
    match data_type {
        DataType::Int => Some(i32::MAX.into()),
        DataType::BigInt => Some(i64::MAX),
        _ => None,
    }
}

/// The handle `nextval` goes through. The sequence's relation, which SELECT
/// reads, is a `TableInfo` on the same heap.
pub struct Sequence {
    pub oid: TableOid,
    pub name: String,
    bpm: Arc<BufferPoolManager>,
    page_id: PageId,
    max: i64,
    /// The value a restart whose transaction hasn't ended handed out last,
    /// or 0 for none, as values start at 1. `nextval` takes values from it
    /// rather than from the row.
    restart: Mutex<Option<i64>>,
}

impl Sequence {
    /// The sequence `oid`, whose row is on `page_id` and whose values stop
    /// at `max`.
    pub(crate) fn new(
        oid: TableOid,
        name: String,
        bpm: Arc<BufferPoolManager>,
        page_id: PageId,
        max: i64,
    ) -> Self {
        Self {
            oid,
            name,
            bpm,
            page_id,
            max,
            restart: Mutex::new(None),
        }
    }

    /// Hands out the next value, as Postgres's `nextval` does with the
    /// default options: INCREMENT 1, CACHE 1, NO CYCLE.
    pub fn next_value(&self) -> Result<i64> {
        // Held throughout, so a restart can't end between the check and the
        // row change.
        let mut restart = self.restart.lock();
        if let Some(last) = restart.as_mut() {
            *last = self.after(*last)?;
            return Ok(*last);
        }
        let mut guard = self
            .bpm
            .checked_write_page(self.page_id, AccessType::Lookup)?;
        let (meta, mut row) = self.read(&guard)?;

        let is_called = row.is_called == 1;
        let value = if is_called {
            self.after(row.last_value.get())?
        } else {
            row.last_value.get()
        };
        let log_cnt = match row.log_cnt.get() {
            // A clean page was just written as it is, by a checkpoint or an
            // eviction, so disk no longer covers the values after it.
            n if is_called && n > 0 && guard.is_dirty() => n - 1,
            _ => {
                let logged = value.saturating_add(LOG_VALS.into()).min(self.max);
                self.log_ahead(&mut guard, &meta, logged)?;
                // At most LOG_VALS.
                (logged - value) as i32
            }
        };

        row.last_value = value.into();
        row.log_cnt = log_cnt.into();
        row.is_called = 1;
        TablePage::new(guard.get_data_mut())?.update(SLOT, &meta, row.as_bytes())?;
        Ok(value)
    }

    /// Makes the next value 1 again, until `end_restart`. The caller holds
    /// the table locked, so only its transaction takes values meanwhile.
    pub(crate) fn restart(&self) {
        *self.restart.lock() = Some(0);
    }

    /// Puts the pending restart, if there is one, in the row: as it is if
    /// the truncate is `durable`, else only if that moves the row forward,
    /// so a rolled-back restart leaves a gap of the values it handed out.
    pub(crate) fn end_restart(&self, durable: bool) {
        let mut restart = self.restart.lock();
        let Some(last) = restart.take() else {
            return;
        };
        // A later `nextval` logs ahead from here.
        let row = SequenceRow {
            null_bitmap: 0,
            last_value: last.max(1).into(),
            log_cnt: 0.into(),
            is_called: u8::from(last > 0),
        };
        let next = |row: &SequenceRow| i128::from(row.last_value.get()) + i128::from(row.is_called);
        if let Err(e) = self.replace_if(&row, |now| durable || next(now) < next(&row)) {
            log::error!("ending the restart of {}: {e}", self.name);
        }
    }

    /// The value after `last`, if the sequence hasn't reached its maximum.
    fn after(&self, last: i64) -> Result<i64> {
        last.checked_add(1)
            .filter(|&value| value <= self.max)
            .ok_or_else(|| CatalogError::SequenceExhausted {
                name: self.name.clone(),
                max: self.max,
            })
    }

    /// Writes `row` over the current row if `replace(current)`.
    fn replace_if(
        &self,
        row: &SequenceRow,
        replace: impl FnOnce(&SequenceRow) -> bool,
    ) -> Result<()> {
        let mut guard = self
            .bpm
            .checked_write_page(self.page_id, AccessType::Lookup)?;
        let (meta, current) = self.read(&guard)?;
        if replace(&current) {
            TablePage::new(guard.get_data_mut())?.update(SLOT, &meta, row.as_bytes())?;
        }
        Ok(())
    }

    /// Reads the row without `get_data_mut`, which dirties the page even if
    /// the caller then fails.
    fn read(&self, guard: &WritePageGuard) -> Result<(TupleMeta, SequenceRow)> {
        let page = TablePage::new(guard.get_data())?;
        let (meta, payload) = page.get(SLOT)?;
        let row = SequenceRow::read_from_bytes(payload).map_err(|_| self.corrupt())?;
        if row.null_bitmap != 0 || row.is_called > 1 {
            return Err(self.corrupt());
        }
        Ok((meta, row))
    }

    /// Writes the row to disk as if every value through `last_value` had
    /// been handed out, leaving the page (latched by `guard`) holding that
    /// row for the caller to overwrite with the real one.
    fn log_ahead(
        &self,
        guard: &mut WritePageGuard,
        meta: &TupleMeta,
        last_value: i64,
    ) -> Result<()> {
        // TODO(wal): write the row as a WAL record instead and stamp its LSN
        // on the page. Writing the page survives a process crash but, with
        // no fsync, not a power loss.
        log::debug!(
            "sequence {}: WAL record would cover values through {last_value}",
            self.name
        );
        let ahead = SequenceRow {
            null_bitmap: 0,
            last_value: last_value.into(),
            log_cnt: 0.into(),
            is_called: 1,
        };
        TablePage::new(guard.get_data_mut())?.update(SLOT, meta, ahead.as_bytes())?;
        guard.flush()?;
        Ok(())
    }

    fn corrupt(&self) -> CatalogError {
        CatalogError::Corrupt(format!("sequence {} has a malformed row", self.name))
    }
}

/// The same sequence, not one in the same state: bound statements compare
/// equal only if they use the same sequence.
impl PartialEq for Sequence {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self, other)
    }
}

impl Eq for Sequence {}

impl fmt::Debug for Sequence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sequence")
            .field("oid", &self.oid)
            .field("name", &self.name)
            .field("page_id", &self.page_id)
            .field("max", &self.max)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::Value;
    use storage::disk::disk_manager::DiskManager;
    use tempfile::TempDir;

    fn sequence(dir: &TempDir, max: i64) -> (Sequence, TableHeap) {
        let (seq, heap, _) = sequence_on_disk(dir, max);
        (seq, heap)
    }

    fn sequence_on_disk(dir: &TempDir, max: i64) -> (Sequence, TableHeap, Arc<DiskManager>) {
        let dm = Arc::new(DiskManager::new(dir.path().join("test.db")).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(4, Arc::clone(&dm)));
        let heap = create_heap(&bpm).unwrap();
        let seq = Sequence::new(7, "t_id_seq".into(), bpm, heap.first_page_id(), max);
        (seq, heap, dm)
    }

    fn row(heap: &TableHeap) -> Vec<Value> {
        let (_, tuple) = heap.iter().unwrap().next().unwrap().unwrap();
        tuple.decode(&SCHEMA).unwrap()
    }

    fn values(last_value: i64, log_cnt: i32, is_called: bool) -> Vec<Value> {
        vec![
            Value::BigInt(last_value),
            Value::Int(log_cnt),
            Value::Bool(is_called),
        ]
    }

    #[test]
    fn the_row_is_laid_out_as_a_tuple_of_the_schema() {
        let row = SequenceRow {
            null_bitmap: 0,
            last_value: (-2).into(),
            log_cnt: 31.into(),
            is_called: 1,
        };
        let encoded = Tuple::encode(&SCHEMA, &values(-2, 31, true)).unwrap();
        assert_eq!(row.as_bytes(), encoded.data());
    }

    /// As Postgres: the first value is 1, and a WAL record is due when the
    /// values the last one covered run out.
    #[test]
    fn hands_out_values_and_counts_down_to_the_next_log() {
        let dir = TempDir::new().unwrap();
        let (seq, heap) = sequence(&dir, i64::MAX);
        assert_eq!(row(&heap), values(1, 0, false));
        assert_eq!(seq.next_value().unwrap(), 1);
        assert_eq!(row(&heap), values(1, 32, true));
        assert_eq!(seq.next_value().unwrap(), 2);
        assert_eq!(row(&heap), values(2, 31, true));
        for expected in 3..=33 {
            assert_eq!(seq.next_value().unwrap(), expected);
        }
        assert_eq!(row(&heap), values(33, 0, true));
        assert_eq!(seq.next_value().unwrap(), 34);
        assert_eq!(row(&heap), values(34, 32, true));
    }

    #[test]
    fn writes_its_page_once_per_log_vals_values() {
        let dir = TempDir::new().unwrap();
        let (seq, _heap, dm) = sequence_on_disk(&dir, i64::MAX);
        let writes = dm.num_writes();
        assert_eq!(seq.next_value().unwrap(), 1);
        assert_eq!(dm.num_writes(), writes + 1);
        for expected in 2..=33 {
            assert_eq!(seq.next_value().unwrap(), expected);
        }
        assert_eq!(dm.num_writes(), writes + 1);
        assert_eq!(seq.next_value().unwrap(), 34);
        assert_eq!(dm.num_writes(), writes + 2);

        // Once a flush has written the page as it is, the next value is
        // written ahead again.
        seq.bpm.flush_all_pages().unwrap();
        let writes = dm.num_writes();
        assert_eq!(seq.next_value().unwrap(), 35);
        assert_eq!(dm.num_writes(), writes + 1);
    }

    #[test]
    fn stops_at_its_maximum() {
        let dir = TempDir::new().unwrap();
        let (seq, heap) = sequence(&dir, 3);
        assert_eq!(seq.next_value().unwrap(), 1);
        // The log covers no values past the maximum.
        assert_eq!(row(&heap), values(1, 2, true));
        assert_eq!(seq.next_value().unwrap(), 2);
        assert_eq!(seq.next_value().unwrap(), 3);
        seq.bpm.flush_all_pages().unwrap();
        for _ in 0..2 {
            assert!(matches!(
                seq.next_value(),
                Err(CatalogError::SequenceExhausted { name, max: 3 }) if name == "t_id_seq"
            ));
        }
        assert_eq!(row(&heap), values(3, 0, true));
        // Failing left the page as it was, so it has nothing to write.
        let guard = seq
            .bpm
            .checked_read_page(seq.page_id, AccessType::Lookup)
            .unwrap();
        assert!(!guard.is_dirty());
    }

    #[test]
    fn concurrent_calls_hand_out_each_value_once() {
        let dir = TempDir::new().unwrap();
        let (seq, _heap) = sequence(&dir, i64::MAX);
        let mut got: Vec<i64> = std::thread::scope(|s| {
            let threads: Vec<_> = (0..4)
                .map(|_| {
                    s.spawn(|| {
                        (0..100)
                            .map(|_| seq.next_value().unwrap())
                            .collect::<Vec<_>>()
                    })
                })
                .collect();
            threads
                .into_iter()
                .flat_map(|t| t.join().unwrap())
                .collect()
        });
        got.sort_unstable();
        assert_eq!(got, (1..=400).collect::<Vec<_>>());
    }
}
