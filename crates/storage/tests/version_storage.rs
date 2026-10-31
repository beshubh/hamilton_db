//! Version storage: versioned writes to a table heap, reads at a timestamp
//! through the undo log, and garbage collection of undo records.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use common::{Column, DataType, PageId, Rid, Schema, Tuple, Value};
use storage::StorageError;
use storage::buffer::buffer_pool_manager::BufferPoolManager;
use storage::concurrency::read_ts::ReadTimestamps;
use storage::disk::disk_manager::DiskManager;
use storage::table::table_heap::TableHeap;
use storage::table::table_page::{INVALID_UNDO_PTR, TupleMeta};
use storage::table::undo_log::UndoLog;
use tempfile::TempDir;

fn setup(num_frames: usize) -> (TempDir, TableHeap) {
    let dir = TempDir::new().unwrap();
    let dm = Arc::new(DiskManager::new(dir.path().join("test.db")).unwrap());
    let bpm = Arc::new(BufferPoolManager::new(num_frames, dm));
    (dir, TableHeap::create(bpm).unwrap())
}

/// `(id INT NOT NULL, v BIGINT, name VARCHAR, flag BOOL)`.
fn schema() -> Schema {
    Schema::new(vec![
        Column::new("id", DataType::Int, false),
        Column::new("v", DataType::BigInt, true),
        Column::new("name", DataType::Varchar(4000), true),
        Column::new("flag", DataType::Bool, true),
    ])
}

fn row(id: i32, v: Option<i64>, name: Option<&str>, flag: Option<bool>) -> Vec<Value> {
    vec![
        Value::Int(id),
        v.map_or(Value::Null, Value::BigInt),
        name.map_or(Value::Null, Value::from),
        flag.map_or(Value::Null, Value::Bool),
    ]
}

/// Row `id` as written at `ts`: `v` and `name` say which write it was.
fn version(id: i32, ts: u64) -> Vec<Value> {
    row(id, Some(ts as i64), Some(&format!("v{ts}")), Some(true))
}

fn encode(values: &[Value]) -> Tuple {
    Tuple::encode(&schema(), values).unwrap()
}

fn insert(heap: &TableHeap, values: &[Value], ts: u64) -> Rid {
    heap.insert(&TupleMeta::new(ts), &encode(values)).unwrap()
}

fn update(heap: &TableHeap, undo: &UndoLog, rid: Rid, values: &[Value], ts: u64) {
    heap.update_versioned(
        rid,
        &encode(values),
        ts,
        &schema(),
        undo,
        &ReadTimestamps::new(),
    )
    .unwrap();
}

/// The values of the version of `rid` visible at `ts`, checking that the
/// tuple carries its rid and that its meta says it is live.
fn read(heap: &TableHeap, undo: &UndoLog, rid: Rid, ts: u64) -> Option<Vec<Value>> {
    let (meta, tuple) = heap.get_at(rid, ts, &schema(), undo, None).unwrap()?;
    assert!(!meta.is_deleted);
    assert!(meta.ts <= ts);
    assert_eq!(tuple.rid(), Some(rid));
    Some(tuple.decode(&schema()).unwrap())
}

/// The undo record the head of `rid` points at.
fn head_record(heap: &TableHeap, undo: &UndoLog, rid: Rid) -> storage::table::undo_log::UndoRecord {
    let (meta, _) = heap.get(rid).unwrap();
    (*undo.get(meta.undo_ptr).unwrap()).clone()
}

/// A row inserted at 10 and updated at 20 and 30.
fn three_versions(heap: &TableHeap, undo: &UndoLog) -> Rid {
    let rid = insert(heap, &version(1, 10), 10);
    update(heap, undo, rid, &version(1, 20), 20);
    update(heap, undo, rid, &version(1, 30), 30);
    rid
}

#[test]
fn each_ts_reads_the_version_written_at_or_before_it() {
    let (_dir, heap) = setup(4);
    let undo = UndoLog::new();
    let rid = three_versions(&heap, &undo);
    assert_eq!(undo.len(), 2);

    assert_eq!(read(&heap, &undo, rid, 5), None);
    for (ts, written) in [(10, 10), (15, 10), (20, 20), (25, 20), (30, 30), (35, 30)] {
        assert_eq!(
            read(&heap, &undo, rid, ts),
            Some(version(1, written)),
            "at {ts}"
        );
        let (meta, _) = heap
            .get_at(rid, ts, &schema(), &undo, None)
            .unwrap()
            .unwrap();
        assert_eq!(meta.ts, written);
    }
    // The head is still the newest version, in place.
    let (meta, tuple) = heap.get(rid).unwrap();
    assert_eq!(meta.ts, 30);
    assert_eq!(tuple.decode(&schema()).unwrap(), version(1, 30));
}

#[test]
fn records_hold_only_the_changed_columns() {
    let (_dir, heap) = setup(4);
    let undo = UndoLog::new();
    let v10 = row(1, Some(100), Some("short"), Some(false));
    let v20 = row(1, Some(100), Some("a much longer name"), Some(false));
    let v30 = row(1, None, Some("a much longer name"), Some(false));
    let v40 = row(1, Some(7), None, None);
    let rid = insert(&heap, &v10, 10);

    update(&heap, &undo, rid, &v20, 20);
    assert_eq!(
        head_record(&heap, &undo, rid).delta,
        [(2, Value::from("short"))]
    );
    update(&heap, &undo, rid, &v30, 30);
    assert_eq!(
        head_record(&heap, &undo, rid).delta,
        [(1, Value::BigInt(100))]
    );
    update(&heap, &undo, rid, &v40, 40);
    assert_eq!(
        head_record(&heap, &undo, rid).delta,
        [
            (1, Value::Null),
            (2, Value::from("a much longer name")),
            (3, Value::Bool(false))
        ]
    );

    // Every column is rebuilt, and each version keeps its own has_nulls.
    for (ts, values, has_nulls) in [
        (10, &v10, false),
        (20, &v20, false),
        (30, &v30, true),
        (40, &v40, true),
    ] {
        assert_eq!(
            read(&heap, &undo, rid, ts).as_ref(),
            Some(values),
            "at {ts}"
        );
        let (meta, tuple) = heap
            .get_at(rid, ts, &schema(), &undo, None)
            .unwrap()
            .unwrap();
        assert_eq!(meta.has_nulls, has_nulls, "at {ts}");
        // A rebuilt version encodes to the bytes it had when it was the head.
        assert_eq!(tuple.data(), encode(values).data());
    }
}

#[test]
fn an_update_that_changes_nothing_keeps_the_old_version() {
    let (_dir, heap) = setup(4);
    let undo = UndoLog::new();
    let rid = insert(&heap, &version(1, 10), 10);
    update(&heap, &undo, rid, &version(1, 10), 20);
    let record = head_record(&heap, &undo, rid);
    assert!(record.delta.is_empty());
    assert_eq!(record.meta.ts, 10);
    assert_eq!(read(&heap, &undo, rid, 15), Some(version(1, 10)));
    assert_eq!(
        heap.get_at(rid, 15, &schema(), &undo, None)
            .unwrap()
            .unwrap()
            .0
            .ts,
        10
    );
}

#[test]
fn a_delete_hides_the_row_from_its_ts_on() {
    let (_dir, heap) = setup(4);
    let undo = UndoLog::new();
    let rid = three_versions(&heap, &undo);
    heap.delete_versioned(rid, 40, &undo, &ReadTimestamps::new())
        .unwrap();

    let record = head_record(&heap, &undo, rid);
    assert!(record.delta.is_empty());
    assert_eq!(record.meta.ts, 30);
    assert!(!record.meta.is_deleted);

    assert_eq!(read(&heap, &undo, rid, 39), Some(version(1, 30)));
    assert_eq!(read(&heap, &undo, rid, 25), Some(version(1, 20)));
    for ts in [40, 41, 1000] {
        assert_eq!(read(&heap, &undo, rid, ts), None, "at {ts}");
    }
    // The plain scan skips the deleted head, as before.
    assert_eq!(heap.iter().unwrap().count(), 0);

    // A deleted tuple cannot be written again.
    assert!(matches!(
        heap.delete_versioned(rid, 50, &undo, &ReadTimestamps::new()),
        Err(StorageError::TupleDeleted(r)) if r == rid
    ));
    assert!(matches!(
        heap.update_versioned(rid, &encode(&version(1, 50)), 50, &schema(), &undo, &ReadTimestamps::new()),
        Err(StorageError::TupleDeleted(r)) if r == rid
    ));
    assert_eq!(undo.len(), 3);
}

#[test]
fn an_insert_is_not_seen_before_its_ts() {
    let (_dir, heap) = setup(4);
    let undo = UndoLog::new();
    let rid = insert(&heap, &version(1, 50), 50);
    assert_eq!(read(&heap, &undo, rid, 49), None);
    assert_eq!(read(&heap, &undo, rid, 50), Some(version(1, 50)));
    assert!(undo.is_empty());
}

/// The payload is not a tuple of the schema, so decoding it would fail,
/// and the head's `undo_ptr` names no record, so following it would fail.
#[test]
fn reading_a_visible_head_decodes_nothing_and_skips_the_undo_log() {
    let (_dir, heap) = setup(4);
    let undo = UndoLog::new();
    let garbage = Tuple::from_bytes(b"not a tuple of the schema".to_vec());
    let dangling = TupleMeta {
        undo_ptr: 12345,
        ..TupleMeta::new(10)
    };
    let rid = heap.insert(&dangling, &garbage).unwrap();
    let (meta, tuple) = heap
        .get_at(rid, 10, &schema(), &undo, None)
        .unwrap()
        .unwrap();
    assert_eq!((meta, tuple.data()), (dangling, garbage.data()));

    // Reading before the head follows the dangling pointer, which fails.
    assert!(matches!(
        heap.get_at(rid, 9, &schema(), &undo, None),
        Err(StorageError::MissingUndoRecord {
            undo_ptr: 12345,
            ..
        })
    ));

    // A tuple with no history, read before its insert, is just absent.
    let fresh = heap.insert(&TupleMeta::new(10), &garbage).unwrap();
    assert!(
        heap.get_at(fresh, 5, &schema(), &undo, None)
            .unwrap()
            .is_none()
    );
    assert!(
        heap.get_at(fresh, 20, &schema(), &undo, None)
            .unwrap()
            .is_some()
    );
}

/// After a restart the undo log issues handles from 1 again, while tuples
/// still hold `undo_ptr`s from before. Following one must fail rather than
/// read another tuple's record.
#[test]
fn an_undo_ptr_from_before_a_restart_is_not_followed() {
    let (_dir, heap) = setup(4);
    // From before the restart: head at 30 pointing at handle 1.
    let stale = TupleMeta {
        undo_ptr: 1,
        ..TupleMeta::new(30)
    };
    let old = heap.insert(&stale, &encode(&version(1, 30))).unwrap();

    // After the restart, another tuple's update is issued handle 1.
    let undo = UndoLog::new();
    let other = insert(&heap, &version(2, 40), 40);
    update(&heap, &undo, other, &version(2, 50), 50);
    assert_eq!(heap.get(other).unwrap().0.undo_ptr, 1);

    assert_eq!(read(&heap, &undo, old, 30), Some(version(1, 30)));
    assert!(matches!(
        heap.get_at(old, 25, &schema(), &undo, None),
        Err(StorageError::MissingUndoRecord { undo_ptr: 1, .. })
    ));

    // Its first write after the restart chains onto the stale pointer, and
    // a read before that write stops at the stale head.
    update(&heap, &undo, old, &version(1, 60), 60);
    assert_eq!(read(&heap, &undo, old, 55), Some(version(1, 30)));
    assert!(heap.get_at(old, 25, &schema(), &undo, None).is_err());
}

#[test]
fn garbage_collection_frees_each_writers_records() {
    let (_dir, heap) = setup(4);
    let undo = UndoLog::new();
    let a = three_versions(&heap, &undo);
    let b = insert(&heap, &version(2, 10), 10);
    update(&heap, &undo, b, &version(2, 20), 20);
    // Handles of the records written at 20, which hold the versions at 10.
    let from_20: Vec<u64> = [a, b]
        .iter()
        .map(|&rid| {
            let (head, _) = heap.get(rid).unwrap();
            let newest = undo.get(head.undo_ptr).unwrap();
            if newest.writer_ts == 20 {
                head.undo_ptr
            } else {
                newest.meta.undo_ptr
            }
        })
        .collect();
    assert_eq!(undo.len(), 3);

    assert_eq!(undo.collect(20), 0);
    assert_eq!(undo.collect(25), 2);
    assert_eq!(undo.collect(25), 0);
    for ptr in from_20 {
        assert!(undo.get(ptr).is_none());
    }
    assert!(undo.get(0).is_none());
    assert!(undo.get(999).is_none());

    for (ts, written) in [(25, 20), (30, 30), (35, 30)] {
        assert_eq!(
            read(&heap, &undo, a, ts),
            Some(version(1, written)),
            "at {ts}"
        );
    }
    assert_eq!(read(&heap, &undo, b, 25), Some(version(2, 20)));
    // A reader below the watermark finds its version gone: an error, not
    // the wrong version.
    assert!(matches!(
        heap.get_at(a, 15, &schema(), &undo, None),
        Err(StorageError::MissingUndoRecord { .. })
    ));

    assert_eq!(undo.collect(31), 1);
    assert!(undo.is_empty());
    assert_eq!(read(&heap, &undo, a, 30), Some(version(1, 30)));
    assert_eq!(read(&heap, &undo, b, 30), Some(version(2, 20)));
}

#[test]
fn a_growing_update_that_does_not_fit_leaves_the_heap_alone() {
    let (_dir, heap) = setup(4);
    let undo = UndoLog::new();
    let big = |ts: u64, len: usize| row(1, Some(ts as i64), Some(&"x".repeat(len)), None);
    let rid = insert(&heap, &big(10, 1500), 10);
    insert(&heap, &row(2, None, Some(&"y".repeat(1500)), None), 10);
    let before = heap.get(rid).unwrap();

    assert!(matches!(
        heap.update_versioned(
            rid,
            &encode(&big(20, 3000)),
            20,
            &schema(),
            &undo,
            &ReadTimestamps::new()
        ),
        Err(StorageError::PageFull)
    ));
    assert_eq!(heap.get(rid).unwrap(), before);
    assert!(undo.is_empty());

    // What the caller does instead: delete the tuple and insert the new
    // version at a new Rid, which starts its own chain.
    heap.delete_versioned(rid, 20, &undo, &ReadTimestamps::new())
        .unwrap();
    let moved = insert(&heap, &big(20, 3000), 20);
    assert_ne!(moved.page_id, rid.page_id);
    assert_eq!(read(&heap, &undo, rid, 15), Some(big(10, 1500)));
    assert_eq!(read(&heap, &undo, rid, 20), None);
    assert_eq!(read(&heap, &undo, moved, 15), None);
    assert_eq!(read(&heap, &undo, moved, 20), Some(big(20, 3000)));
}

/// What happens to a row in the scan test, by its index.
#[derive(Clone, Copy)]
enum Fate {
    Kept,
    Updated,
    Deleted,
    UpdatedThenDeleted,
}

/// A row's versions, oldest first: when each was written, and its values,
/// or `None` for a delete.
type History = Vec<(u64, Option<Vec<Value>>)>;

#[test]
fn a_scan_at_ts_sees_each_row_as_it_was() {
    let (_dir, heap) = setup(4);
    let undo = UndoLog::new();
    let name = |id: i32, ts: u64| format!("row {id} written at {ts} {}", "-".repeat(60));
    let value = |id: i32, ts: u64| row(id, Some(ts as i64), Some(&name(id, ts)), None);

    let mut model: Vec<(Rid, History)> = Vec::new();
    for id in 0..300 {
        let rid = insert(&heap, &value(id, 10), 10);
        model.push((rid, vec![(10, Some(value(id, 10)))]));
    }
    let pages: HashSet<PageId> = model.iter().map(|(rid, _)| rid.page_id).collect();
    assert!(pages.len() > 4, "more pages than frames: {}", pages.len());

    for (id, (rid, versions)) in model.iter_mut().enumerate() {
        let id = id as i32;
        let fate = [
            Fate::Kept,
            Fate::Updated,
            Fate::Deleted,
            Fate::UpdatedThenDeleted,
        ][id as usize % 4];
        if matches!(fate, Fate::Updated | Fate::UpdatedThenDeleted) {
            update(&heap, &undo, *rid, &value(id, 20), 20);
            versions.push((20, Some(value(id, 20))));
        }
        if matches!(fate, Fate::Deleted | Fate::UpdatedThenDeleted) {
            heap.delete_versioned(*rid, 30, &undo, &ReadTimestamps::new())
                .unwrap();
            versions.push((30, None));
        }
    }
    for id in 300..350 {
        let rid = insert(&heap, &value(id, 40), 40);
        model.push((rid, vec![(40, Some(value(id, 40)))]));
    }

    for ts in [5, 10, 15, 20, 25, 30, 35, 40, 45] {
        let expected: Vec<(Rid, Vec<Value>)> = model
            .iter()
            .filter_map(|(rid, versions)| {
                let (_, values) = versions.iter().rev().find(|(written, _)| *written <= ts)?;
                Some((*rid, values.clone()?))
            })
            .collect();

        let mut iter = heap.iter().unwrap();
        let mut scanned = Vec::new();
        while let Some(batch) = iter.next_batch_at(ts, &schema(), &undo, None).unwrap() {
            assert!(!batch.is_empty());
            for (meta, tuple) in batch {
                assert!(!meta.is_deleted && meta.ts <= ts);
                let rid = tuple.rid().unwrap();
                scanned.push((rid, tuple.decode(&schema()).unwrap()));
            }
        }
        assert_eq!(scanned, expected, "at {ts}");
        for (rid, values) in &expected {
            assert_eq!(read(&heap, &undo, *rid, ts).as_ref(), Some(values));
        }
    }

    // The plain scan still sees the heads that are live now.
    let live = heap.iter().unwrap().map(Result::unwrap).count();
    assert_eq!(live, 150 + 50);
}

/// Readers at fixed timestamps keep seeing the same version while a writer
/// keeps adding newer ones and garbage collection runs at the watermark.
#[test]
fn readers_see_their_version_while_a_writer_updates() {
    let (_dir, heap) = setup(4);
    let undo = UndoLog::new();
    let rid = insert(&heap, &version(1, 10), 10);
    for ts in (20..=100).step_by(10) {
        update(&heap, &undo, rid, &version(1, ts), ts);
    }
    let watermark = 25;
    let done = AtomicBool::new(false);

    thread::scope(|s| {
        for ts in [25, 55, 95, 100, 105] {
            let (heap, undo, done) = (&heap, &undo, &done);
            s.spawn(move || {
                let expected = version(1, ts / 10 * 10);
                let mut reads = 0;
                while !done.load(Ordering::Acquire) || reads < 100 {
                    assert_eq!(
                        read(heap, undo, rid, ts).as_ref(),
                        Some(&expected),
                        "at {ts}"
                    );
                    reads += 1;
                }
            });
        }
        let (undo, done) = (&undo, &done);
        s.spawn(move || {
            while !done.load(Ordering::Acquire) {
                undo.collect(watermark);
            }
        });

        for ts in (110..=2000).step_by(10) {
            update(&heap, undo, rid, &version(1, ts), ts);
        }
        done.store(true, Ordering::Release);
    });

    undo.collect(watermark);
    // Every record from 30 on is still there.
    assert_eq!(undo.len(), (30..=2000).step_by(10).count());
    assert_eq!(read(&heap, &undo, rid, 2000), Some(version(1, 2000)));
    assert_ne!(heap.get(rid).unwrap().0.undo_ptr, INVALID_UNDO_PTR);
}
