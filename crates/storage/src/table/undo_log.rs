//! Older versions of table tuples, kept in memory as undo records.
//!
//! The heap holds each tuple's newest version in place. A write pushes an
//! [`UndoRecord`] holding the old values of the columns it changed (none for
//! a delete) and the old version's meta, and points the new version's
//! `undo_ptr` at it. Following `undo_ptr`s walks from newest to oldest.
//! Inserts push nothing, and chains never cross `Rid`s.
//!
//! ```text
//!  heap: ts 30 -> record: ts 20, name = 'b' -> record: ts 10, v = 1 -> INVALID_UNDO_PTR
//! ```
//!
//! **Visibility.** At time `ts` a tuple is its newest version with a `ts` at
//! or below `ts` (`version_at`); none if that version is deleted or the
//! chain ends first. A version ends where the next newer one starts.
//! While its writer runs, a version's `ts` is the writer's Tid with
//! `TXN_UNCOMMITTED_BIT` set: the writer reads it, older readers walk past
//! it, and newer readers fail with `Conflict` instead of reading past it.
//!
//! **Deltas** are `(column, old value)` pairs rather than encoded bytes:
//! records never reach disk, and a version is rebuilt by decoding the head
//! once, overwriting columns, and encoding once.
//!
//! **In memory, not durable.** After a crash no transaction survives and
//! new Tids exceed every `ts` on disk, so no reader walks a chain. Undoing
//! crashed writes is the WAL's job.
//!
//! **GC.** A record's `writer_ts` is its writer's plain Tid, so commit
//! restamps the heap alone. A record pushed at X is read only by readers
//! below X, so [`UndoLog::collect`] frees every writer's records below the
//! watermark; a running writer is never below it.
//!
//! **Known gaps.** No durability; `undo_ptr`s on disk dangle after a restart,
//! so they are only followed through a checked lookup (`older_record`).

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use common::{Rid, Schema, TXN_UNCOMMITTED_BIT, Tuple, Value, ValueError};
use parking_lot::RwLock;

use crate::error::{Result, StorageError};
use crate::table::table_iterator::TableRow;
use crate::table::table_page::{INVALID_UNDO_PTR, TupleMeta};

/// What a write needs to rebuild the version of a tuple that it replaced.
/// Records are never changed once they are pushed.
#[derive(Debug, Clone, PartialEq)]
pub struct UndoRecord {
    /// The tuple whose chain this record is on.
    pub rid: Rid,
    /// The `ts` of the write that pushed this record, without
    /// `TXN_UNCOMMITTED_BIT`. It is also the `ts` of the version that points
    /// at the record, once that version is committed. Garbage collection
    /// frees records by this `ts`.
    pub writer_ts: u64,
    /// The meta of the version this record rebuilds: its `ts`, its flags,
    /// and in `undo_ptr` the next older record.
    pub meta: TupleMeta,
    /// The old values of the columns the write changed, in column order.
    /// Empty for a delete.
    pub delta: Vec<(usize, Value)>,
}

impl UndoRecord {
    /// Turns `values`, the values of the version that points at this record,
    /// into the values of the version this record rebuilds.
    pub fn apply(&self, values: &mut [Value]) -> Result<()> {
        let count = values.len();
        for (idx, old) in &self.delta {
            let value = values
                .get_mut(*idx)
                .ok_or(ValueError::ColumnOutOfRange { idx: *idx, count })?;
            *value = old.clone();
        }
        Ok(())
    }
}

/// The delta that turns `new` back into `old`: each column whose value
/// differs, with its old value. Doubles compare by bits, so `0.0` to `-0.0`
/// counts as a change.
pub fn diff(old: Vec<Value>, new: &[Value]) -> Vec<(usize, Value)> {
    old.into_iter()
        .zip(new)
        .enumerate()
        .filter(|(_, (old, new))| old != *new)
        .map(|(idx, (old, _))| (idx, old))
        .collect()
}

/// Undo records of every tuple, reached by handle. One lock covers the log:
/// pushes and GC take it exclusively, lookups shared.
#[derive(Default)]
pub struct UndoLog {
    records: RwLock<Records>,
}

#[derive(Default)]
struct Records {
    /// The last handle issued; 0 until the first push.
    last_ptr: u64,
    by_ptr: HashMap<u64, Arc<UndoRecord>>,
    /// The handles of the records each write pushed, by the write's `ts`.
    by_writer: BTreeMap<u64, Vec<u64>>,
}

impl UndoLog {
    pub fn new() -> Self {
        Self::default()
    }

    /// Stores `record` and returns its handle, which is never
    /// `INVALID_UNDO_PTR`.
    pub fn push(&self, record: UndoRecord) -> u64 {
        let mut records = self.records.write();
        records.last_ptr += 1;
        let ptr = records.last_ptr;
        records
            .by_writer
            .entry(record.writer_ts)
            .or_default()
            .push(ptr);
        records.by_ptr.insert(ptr, Arc::new(record));
        ptr
    }

    /// The record named by `ptr`, or `None` if it was freed or never issued.
    pub fn get(&self, ptr: u64) -> Option<Arc<UndoRecord>> {
        self.records.read().by_ptr.get(&ptr).cloned()
    }

    /// Frees every record pushed by a write whose `ts` is below `watermark`,
    /// and returns how many it freed. No reader at or above `watermark`
    /// walks into them.
    pub fn collect(&self, watermark: u64) -> usize {
        let mut records = self.records.write();
        let kept = records.by_writer.split_off(&watermark);
        let dead = std::mem::replace(&mut records.by_writer, kept);
        let mut freed = 0;
        for ptr in dead.into_values().flatten() {
            records.by_ptr.remove(&ptr);
            freed += 1;
        }
        freed
    }

    /// Number of records held.
    pub fn len(&self) -> usize {
        self.records.read().by_ptr.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// The version of the tuple at `rid` visible at `ts`, given its head
/// version: the newest version whose `ts` is at or below `ts`. Returns
/// `None` if that version is deleted or the tuple was inserted after `ts`.
///
/// A head written by the transaction `ts` itself, still uncommitted, is
/// visible, with any padding of a shrinking update cut off. A head locked by
/// an older transaction fails with `Conflict`: in `ts` order it comes before
/// the reader, but it may still change or be rolled back.
///
/// A visible committed head is returned as it is, without decoding it or
/// reading `undo`. Otherwise the head's values are decoded when the first record
/// that changes a column is applied, and encoded again once at the end.
/// Fails with `MissingUndoRecord` if the chain leads to a record that is not
/// in `undo`, which means a reader older than the watermark is still
/// running.
pub(crate) fn version_at(
    rid: Rid,
    head: TableRow,
    ts: u64,
    schema: &Schema,
    undo: &UndoLog,
) -> Result<Option<TableRow>> {
    let (mut meta, tuple) = head;
    if meta.ts == ts | TXN_UNCOMMITTED_BIT {
        if meta.is_deleted {
            return Ok(None);
        }
        let len = tuple.encoded_len(schema)?;
        if len == tuple.len() {
            return Ok(Some((meta, tuple)));
        }
        let mut data = tuple.into_data();
        data.truncate(len);
        return Ok(Some((meta, Tuple::from_bytes(data).with_rid(rid))));
    }
    if meta.ts & TXN_UNCOMMITTED_BIT != 0 {
        let writer = meta.ts & !TXN_UNCOMMITTED_BIT;
        if writer < ts {
            return Err(StorageError::Conflict { rid, other: writer });
        }
    } else if meta.ts <= ts {
        return Ok((!meta.is_deleted).then_some((meta, tuple)));
    }

    // Uncommitted versions have the top bit set, so the walk passes them.
    let mut values: Option<Vec<Value>> = None;
    while meta.ts > ts {
        if meta.undo_ptr == INVALID_UNDO_PTR {
            // This version was inserted after `ts`.
            return Ok(None);
        }
        let record = older_record(rid, &meta, undo)?;
        if !record.delta.is_empty() {
            let values = match &mut values {
                Some(values) => values,
                None => values.insert(tuple.decode(schema)?),
            };
            record.apply(values)?;
        }
        meta = record.meta;
    }

    if meta.is_deleted {
        return Ok(None);
    }
    let tuple = match values {
        Some(values) => Tuple::encode(schema, &values)?.with_rid(rid),
        // Only deletes were undone, and they leave the tuple's bytes alone.
        None => tuple,
    };
    Ok(Some((meta, tuple)))
}

/// The record that `meta`, a version of the tuple at `rid`, points at. The
/// log issues handles from 1 again after a restart, so an old `undo_ptr` can
/// name another tuple's record. A record must match both `rid` and the `ts`
/// of the version pointing at it, without the uncommitted bit; pre-restart
/// `ts`s are older than every write since, so a stale pointer never matches
/// and is reported missing.
pub(crate) fn older_record(rid: Rid, meta: &TupleMeta, undo: &UndoLog) -> Result<Arc<UndoRecord>> {
    undo.get(meta.undo_ptr)
        .filter(|record| record.rid == rid && record.writer_ts == meta.ts & !TXN_UNCOMMITTED_BIT)
        .ok_or(StorageError::MissingUndoRecord {
            rid,
            undo_ptr: meta.undo_ptr,
        })
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::thread;

    use super::*;

    fn record(writer_ts: u64) -> UndoRecord {
        UndoRecord {
            rid: Rid::new(1, 0),
            writer_ts,
            meta: TupleMeta::new(writer_ts - 1),
            delta: vec![(0, Value::Int(1))],
        }
    }

    #[test]
    fn handles_start_at_one_and_are_checked() {
        let undo = UndoLog::new();
        assert!(undo.get(INVALID_UNDO_PTR).is_none());
        let a = undo.push(record(10));
        let b = undo.push(record(20));
        assert_eq!((a, b), (1, 2));
        assert_eq!(*undo.get(a).unwrap(), record(10));
        assert_eq!(*undo.get(b).unwrap(), record(20));
        assert!(undo.get(3).is_none());
        assert_eq!(undo.len(), 2);
    }

    #[test]
    fn collect_frees_every_record_of_each_old_writer() {
        let undo = UndoLog::new();
        let tens: Vec<u64> = (0..3).map(|_| undo.push(record(10))).collect();
        let twenty = undo.push(record(20));
        let thirty = undo.push(record(30));

        assert_eq!(undo.collect(10), 0);
        assert_eq!(undo.collect(21), 4);
        for ptr in tens.into_iter().chain([twenty]) {
            assert!(undo.get(ptr).is_none());
        }
        assert!(undo.get(thirty).is_some());
        assert_eq!(undo.collect(21), 0);
        assert_eq!(undo.collect(u64::MAX), 1);
        assert!(undo.is_empty());
        // Handles are not reused after a collection.
        assert_eq!(undo.push(record(40)), 6);
    }

    #[test]
    fn concurrent_pushes_get_distinct_handles() {
        let undo = UndoLog::new();
        let handles: Vec<u64> = thread::scope(|s| {
            let threads: Vec<_> = (0..4)
                .map(|t| {
                    let undo = &undo;
                    s.spawn(move || {
                        (0..250)
                            .map(|i| undo.push(record(t * 1000 + i + 1)))
                            .collect::<Vec<_>>()
                    })
                })
                .collect();
            threads
                .into_iter()
                .flat_map(|t| t.join().unwrap())
                .collect()
        });
        let distinct: HashSet<u64> = handles.iter().copied().collect();
        assert_eq!(distinct.len(), 1000);
        assert!(!distinct.contains(&INVALID_UNDO_PTR));
        assert_eq!(undo.len(), 1000);
    }

    #[test]
    fn diff_keeps_only_changed_columns() {
        let old = vec![
            Value::Int(1),
            Value::from("a"),
            Value::Double(0.0),
            Value::Null,
        ];
        let new = [
            Value::Int(1),
            Value::from("ab"),
            Value::Double(-0.0),
            Value::Null,
        ];
        let delta = diff(old.clone(), &new);
        assert_eq!(delta, [(1, Value::from("a")), (2, Value::Double(0.0))]);

        let undo = UndoRecord { delta, ..record(2) };
        let mut values = new.to_vec();
        undo.apply(&mut values).unwrap();
        assert_eq!(values, old);
        assert!(undo.apply(&mut values[..2]).is_err());
    }
}
