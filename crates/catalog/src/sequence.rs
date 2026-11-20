//! Sequences, which hand out 1, 2, 3 and so on to identity columns, as
//! Postgres's do.
//!
//! A sequence is a relation of its own, named like `orders_id_seq`, whose
//! heap holds a single row:
//!
//! ```text
//! last_value BIGINT   the last value the row covers: every value up to it
//!                     may have been handed out; or, while is_called is
//!                     false, the first value to hand out
//! log_cnt    INT      0, as in the row Postgres logs ahead
//! is_called  BOOL     whether last_value counts as handed out
//! ```
//!
//! `nextval` hands values out from a counter in memory, outside any
//! transaction, so a rolled-back INSERT leaves a gap. When the row no
//! longer covers the next value, it is first written LOG_VALS values past
//! it (`log_ahead`), a redo-only change that is not synced. A value reaches
//! a committed row only through a transaction that logs its `Commit` after
//! that record, and the commit's sync makes the record durable too. So a
//! crash skips up to LOG_VALS values, and never hands out
//! one a committed row holds. SELECT reads the row, so it shows the value
//! logged ahead.
//!
//! `TRUNCATE ... RESTART IDENTITY` (`restart`) is a change of the
//! truncate's transaction, so its abort, or a crash before its commit,
//! puts the row back as it was before the restart, as Postgres does. That
//! also takes back every `nextval` since, which is right only because the
//! truncate holds the table locked: its own inserts are the only ones that
//! took values meanwhile, and they are undone too. An abort then puts back
//! the counter from before the restart (`restore`).
//!
//! A clean shutdown writes the row as the last value handed out (`save`),
//! so only a crash skips values.

use std::fmt;
use std::sync::{Arc, LazyLock};

use common::{Column, DataType, PageId, Schema, SlotId, Tuple};
use parking_lot::Mutex;
use storage::buffer::arc_replacer::AccessType;
use storage::buffer::buffer_pool_manager::BufferPoolManager;
use storage::page::PAGE_SIZE;
use storage::table::table_heap::TableHeap;
use storage::table::table_page::{TablePage, TupleMeta};
use storage::wal::{Logged, MtrPage, PageOp};
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

impl SequenceRow {
    /// The row of a sequence that hands out 1 next.
    fn first() -> Self {
        Self {
            null_bitmap: 0,
            last_value: 1.into(),
            log_cnt: 0.into(),
            is_called: 0,
        }
    }
}

/// Puts the row of a sequence that hands out 1 first on `heap`, a new
/// sequence's empty heap. Redo-only: the heap is storage its create's
/// transaction made, which an abort frees whole.
pub(crate) fn init(heap: &TableHeap) -> Result<()> {
    let tuple = Tuple::from_bytes(SequenceRow::first().as_bytes().to_vec());
    let rid = heap.insert(Logged::System, &TupleMeta::new(CATALOG_TS), &tuple)?;
    debug_assert_eq!((rid.page_id, rid.slot), (heap.first_page_id(), SLOT));
    Ok(())
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
    /// `None` until read from the row. Held while the row changes, too.
    counter: Mutex<Option<Counter>>,
}

/// A sequence's counter from before a restart; see `Sequence::restore`.
pub(crate) struct Saved(Option<Counter>);

/// Where `nextval` is.
#[derive(Clone, Copy)]
struct Counter {
    /// The last value handed out, or the first to hand out until
    /// `is_called`.
    last_value: i64,
    is_called: bool,
    /// The row's `last_value`, once it covers values: up to it, values go
    /// out without writing the row.
    logged: Option<i64>,
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
            counter: Mutex::new(None),
        }
    }

    /// Hands out the next value, as Postgres's `nextval` does with the
    /// default options: INCREMENT 1, CACHE 1, NO CYCLE.
    pub fn next_value(&self) -> Result<i64> {
        let mut counter = self.counter.lock();
        if counter.is_none() {
            *counter = Some(self.read_counter()?);
        }
        let counter = counter.as_mut().expect("read above");
        let value = if counter.is_called {
            self.after(counter.last_value)?
        } else {
            counter.last_value
        };
        if counter.logged.is_none_or(|logged| value > logged) {
            let logged = value.saturating_add(LOG_VALS.into()).min(self.max);
            self.cover(logged)?;
            counter.logged = Some(logged);
        }
        counter.last_value = value;
        counter.is_called = true;
        Ok(value)
    }

    /// Makes the next value 1 again, as `by`'s change: a truncate's, whose
    /// undo puts the row back (see the module docs). Returns the counter
    /// from before, for `restore`.
    pub(crate) fn restart(&self, by: Logged<'_>) -> Result<Saved> {
        let mut counter = self.counter.lock();
        let mtr = self.bpm.mtr(by);
        let mut page = mtr.write_page(self.page_id)?;
        let (meta, _) = self.read(page.data())?;
        page.apply(PageOp::HeapSetTuple {
            slot: SLOT,
            meta,
            payload: SequenceRow::first().as_bytes(),
            padded_to: size_of::<SequenceRow>(),
        })?;
        Ok(Saved(counter.take()))
    }

    /// Puts back the counter a restart replaced, once undo has put back the
    /// row: it covers what that counter handed out.
    pub(crate) fn restore(&self, saved: Saved) {
        *self.counter.lock() = saved.0;
    }

    /// Writes the row as the last value handed out, so a handle opened on
    /// it next goes on right after that value rather than past the ones
    /// logged ahead. Later values are logged ahead again.
    pub(crate) fn save(&self) -> Result<()> {
        let mut counter = self.counter.lock();
        let Some(counter) = counter.as_mut().filter(|counter| counter.is_called) else {
            return Ok(());
        };
        if counter.logged != Some(counter.last_value) {
            self.cover(counter.last_value)?;
            counter.logged = Some(counter.last_value);
        }
        Ok(())
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

    fn read_counter(&self) -> Result<Counter> {
        let guard = self
            .bpm
            .checked_read_page(self.page_id, AccessType::Lookup)?;
        let (_, row) = self.read(guard.get_data())?;
        let is_called = row.is_called == 1;
        Ok(Counter {
            last_value: row.last_value.get(),
            is_called,
            logged: is_called.then(|| row.last_value.get()),
        })
    }

    fn read(&self, data: &[u8; PAGE_SIZE]) -> Result<(TupleMeta, SequenceRow)> {
        let page = TablePage::new(data)?;
        let (meta, payload) = page.get(SLOT)?;
        let row = SequenceRow::read_from_bytes(payload).map_err(|_| self.corrupt())?;
        if row.null_bitmap != 0 || row.is_called > 1 {
            return Err(self.corrupt());
        }
        Ok((meta, row))
    }

    /// Writes the row as if every value through `last_value` had been
    /// handed out. The caller holds `counter`.
    fn cover(&self, last_value: i64) -> Result<()> {
        log::debug!(
            "sequence {}: the row covers values through {last_value}",
            self.name
        );
        let mtr = self.bpm.mtr(Logged::System);
        let mut page = mtr.write_page(self.page_id)?;
        let (meta, _) = self.read(page.data())?;
        put(&mut page, &meta, last_value)
    }

    fn corrupt(&self) -> CatalogError {
        CatalogError::Corrupt(format!("sequence {} has a malformed row", self.name))
    }
}

/// Writes `last_value`, handed out, over the row on `page`.
fn put(page: &mut MtrPage<'_>, meta: &TupleMeta, last_value: i64) -> Result<()> {
    let row = SequenceRow {
        null_bitmap: 0,
        last_value: last_value.into(),
        log_cnt: 0.into(),
        is_called: 1,
    };
    page.apply(PageOp::HeapSetTuple {
        slot: SLOT,
        meta: *meta,
        payload: row.as_bytes(),
        padded_to: size_of::<SequenceRow>(),
    })?;
    Ok(())
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
    use storage::buffer::arc_replacer::AccessType;
    use tempfile::TempDir;

    fn sequence(dir: &TempDir, max: i64) -> (Sequence, TableHeap) {
        let bpm = storage::open(dir.path().join("test.db"), 4).unwrap().bpm;
        let heap = TableHeap::create(Arc::clone(&bpm)).unwrap();
        init(&heap).unwrap();
        let seq = Sequence::new(7, "t_id_seq".into(), bpm, heap.first_page_id(), max);
        (seq, heap)
    }

    /// Another handle on `seq`'s row, as a restart opens it.
    fn reopened(seq: &Sequence) -> Sequence {
        let bpm = Arc::clone(&seq.bpm);
        Sequence::new(seq.oid, seq.name.clone(), bpm, seq.page_id, seq.max)
    }

    fn row(heap: &TableHeap) -> Vec<Value> {
        let (_, tuple) = heap.iter().unwrap().next().unwrap().unwrap();
        tuple.decode(&SCHEMA).unwrap()
    }

    fn values(last_value: i64, is_called: bool) -> Vec<Value> {
        vec![
            Value::BigInt(last_value),
            Value::Int(0),
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
        let encoded = Tuple::encode(&SCHEMA, &[Value::BigInt(-2), Value::Int(31), true.into()]);
        assert_eq!(row.as_bytes(), encoded.unwrap().data());
    }

    /// As Postgres: the first value is 1, and the row is logged
    /// LOG_VALS values ahead whenever the values it covers run out.
    #[test]
    fn hands_out_values_and_logs_ahead_when_they_run_out() {
        let dir = TempDir::new().unwrap();
        let (seq, heap) = sequence(&dir, i64::MAX);
        assert_eq!(row(&heap), values(1, false));
        assert_eq!(seq.next_value().unwrap(), 1);
        assert_eq!(row(&heap), values(33, true));
        for expected in 2..=33 {
            assert_eq!(seq.next_value().unwrap(), expected);
        }
        assert_eq!(row(&heap), values(33, true));
        assert_eq!(seq.next_value().unwrap(), 34);
        assert_eq!(row(&heap), values(66, true));
    }

    #[test]
    fn logs_once_per_log_vals_values_and_never_syncs() {
        let dir = TempDir::new().unwrap();
        let (seq, _heap) = sequence(&dir, i64::MAX);
        let log = Arc::clone(seq.bpm.log());
        let (end, syncs) = (log.end(), log.num_syncs());
        assert_eq!(seq.next_value().unwrap(), 1);
        let logged = log.end();
        assert!(logged > end);
        for expected in 2..=33 {
            assert_eq!(seq.next_value().unwrap(), expected);
        }
        assert_eq!(log.end(), logged);
        assert_eq!(seq.next_value().unwrap(), 34);
        assert!(log.end() > logged);
        assert_eq!(log.num_syncs(), syncs);
    }

    /// The row covers every value handed out, so a handle opened on it
    /// later, as after a crash, skips the rest of them.
    #[test]
    fn a_new_handle_goes_on_past_the_values_logged_ahead() {
        let dir = TempDir::new().unwrap();
        let (seq, _heap) = sequence(&dir, i64::MAX);
        for expected in 1..=5 {
            assert_eq!(seq.next_value().unwrap(), expected);
        }
        assert_eq!(reopened(&seq).next_value().unwrap(), 34);
        for expected in 6..=33 {
            assert_eq!(seq.next_value().unwrap(), expected);
        }
        assert_eq!(reopened(&seq).next_value().unwrap(), 67);
    }

    #[test]
    fn stops_at_its_maximum() {
        let dir = TempDir::new().unwrap();
        let (seq, heap) = sequence(&dir, 3);
        assert_eq!(seq.next_value().unwrap(), 1);
        // The log covers no values past the maximum.
        assert_eq!(row(&heap), values(3, true));
        assert_eq!(seq.next_value().unwrap(), 2);
        assert_eq!(seq.next_value().unwrap(), 3);
        seq.bpm.flush_all_pages().unwrap();
        for _ in 0..2 {
            assert!(matches!(
                seq.next_value(),
                Err(CatalogError::SequenceExhausted { name, max: 3 }) if name == "t_id_seq"
            ));
        }
        assert!(matches!(
            reopened(&seq).next_value(),
            Err(CatalogError::SequenceExhausted { .. })
        ));
        let guard = seq
            .bpm
            .checked_read_page(seq.page_id, AccessType::Lookup)
            .unwrap();
        assert!(!guard.is_dirty(), "failing changed the row");
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

    /// A restart takes effect at once, and once its undo has put the row
    /// back, `restore` goes on from the value before it.
    #[test]
    fn restore_goes_on_from_before_the_restart() {
        let dir = TempDir::new().unwrap();
        let (seq, heap) = sequence(&dir, i64::MAX);
        for expected in 1..=5 {
            assert_eq!(seq.next_value().unwrap(), expected);
        }
        let saved = seq.restart(Logged::System).unwrap();
        assert_eq!(row(&heap), values(1, false));
        assert_eq!(seq.next_value().unwrap(), 1);
        assert_eq!(seq.next_value().unwrap(), 2);
        // As the undo of the restart would.
        {
            let mtr = seq.bpm.mtr(Logged::System);
            let mut page = mtr.write_page(seq.page_id).unwrap();
            let (meta, _) = seq.read(page.data()).unwrap();
            put(&mut page, &meta, 33).unwrap();
        }
        seq.restore(saved);
        assert_eq!(seq.next_value().unwrap(), 6);
    }

    /// `save` leaves the row covering only what went out, so a handle
    /// opened on it next, as after a clean restart, skips nothing.
    #[test]
    fn a_saved_sequence_goes_on_without_a_gap() {
        let dir = TempDir::new().unwrap();
        let (seq, heap) = sequence(&dir, i64::MAX);
        for expected in 1..=5 {
            assert_eq!(seq.next_value().unwrap(), expected);
        }
        seq.save().unwrap();
        assert_eq!(row(&heap), values(5, true));
        assert_eq!(reopened(&seq).next_value().unwrap(), 6);
        assert_eq!(seq.next_value().unwrap(), 6);
        assert_eq!(row(&heap), values(38, true));
    }
}
