use std::fs::{self, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;
use std::thread;

use common::{INVALID_LSN, Lsn};
use storage::StorageError;
use storage::wal::log::{FIRST_LSN, Log};
use storage::wal::record::{HEADER_SIZE, Kind, RecordBuf};
use tempfile::TempDir;

fn record(buf: &mut RecordBuf, kind: Kind, tid: u64, body: &[u8]) {
    buf.begin(kind, tid);
    buf.body().extend_from_slice(body);
    buf.finish();
}

fn append(log: &Log, kind: Kind, tid: u64, prev: Lsn, body: &[u8]) -> Lsn {
    let mut buf = RecordBuf::new();
    record(&mut buf, kind, tid, body);
    log.append(&buf, |_| prev).unwrap()
}

/// Every durable record, as (lsn, prev_lsn, tid, kind, body).
fn scan_all(log: &Log) -> Vec<(Lsn, Lsn, u64, Kind, Vec<u8>)> {
    let mut scan = log.scan(FIRST_LSN);
    let mut out = Vec::new();
    while let Some(r) = scan.next_record().unwrap() {
        out.push((r.lsn, r.prev_lsn, r.tid, r.kind, r.body.to_vec()));
    }
    out
}

fn setup() -> (TempDir, Log) {
    let dir = TempDir::new().unwrap();
    let log = Log::open(dir.path().join("t.log")).unwrap();
    (dir, log)
}

fn log_path(dir: &TempDir) -> std::path::PathBuf {
    dir.path().join("t.log")
}

fn file_len(path: &Path) -> u64 {
    fs::metadata(path).unwrap().len()
}

#[test]
fn a_new_log_is_empty() {
    let (_dir, log) = setup();
    assert_eq!(log.end(), FIRST_LSN);
    assert_eq!(log.durable_end(), FIRST_LSN);
    assert!(scan_all(&log).is_empty());
}

#[test]
fn lsns_are_byte_positions() {
    let (_dir, log) = setup();
    let a = append(&log, Kind::Update, 1, INVALID_LSN, b"abc");
    let b = append(&log, Kind::Commit, 1, a, b"");
    assert_eq!(a, FIRST_LSN);
    assert_eq!(b, a + (HEADER_SIZE + 3) as Lsn);
    assert_eq!(log.end(), b + HEADER_SIZE as Lsn);
}

#[test]
fn records_are_read_back_from_memory_and_from_the_file() {
    let (_dir, log) = setup();
    let a = append(&log, Kind::Update, 7, INVALID_LSN, b"first");
    let mut buf = Vec::new();
    let r = log.read(a, &mut buf).unwrap();
    assert_eq!(
        (r.kind, r.tid, r.prev_lsn, r.body),
        (Kind::Update, 7, INVALID_LSN, &b"first"[..])
    );

    log.flush_to(a).unwrap();
    let b = append(&log, Kind::Clr, 7, a, b"second");
    let r = log.read(a, &mut buf).unwrap();
    assert_eq!(r.body, b"first");
    let r = log.read(b, &mut buf).unwrap();
    assert_eq!((r.kind, r.prev_lsn, r.body), (Kind::Clr, a, &b"second"[..]));
    assert!(r.has_prev());
}

#[test]
fn reading_where_no_record_starts_fails() {
    let (_dir, log) = setup();
    let a = append(&log, Kind::Update, 1, INVALID_LSN, b"payload");
    log.flush_all().unwrap();
    let mut buf = Vec::new();
    assert!(matches!(
        log.read(a + 1, &mut buf),
        Err(StorageError::CorruptLog(_))
    ));
    assert!(matches!(
        log.read(log.end(), &mut buf),
        Err(StorageError::CorruptLog(_))
    ));
}

#[test]
fn link_runs_with_the_lsn_and_sets_prev_lsn() {
    let (_dir, log) = setup();
    let mut buf = RecordBuf::new();
    let mut last = INVALID_LSN;
    for i in 0..3u8 {
        record(&mut buf, Kind::Update, 9, &[i]);
        let lsn = log
            .append(&buf, |lsn| std::mem::replace(&mut last, lsn))
            .unwrap();
        assert_eq!(lsn, last);
    }
    log.flush_all().unwrap();
    let records = scan_all(&log);
    assert_eq!(records[0].1, INVALID_LSN);
    assert_eq!(records[1].1, records[0].0);
    assert_eq!(records[2].1, records[1].0);
}

#[test]
fn only_durable_records_survive_reopening() {
    let (dir, log) = setup();
    let a = append(&log, Kind::Update, 1, INVALID_LSN, b"kept");
    log.flush_to(a).unwrap();
    append(&log, Kind::Update, 1, a, b"lost");
    drop(log);

    let log = Log::open(log_path(&dir)).unwrap();
    let records = scan_all(&log);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].4, b"kept");
    assert_eq!(log.end(), records[0].0 + (HEADER_SIZE + 4) as Lsn);
}

#[test]
fn one_sync_covers_every_record_before_it() {
    let (_dir, log) = setup();
    let first = append(&log, Kind::Update, 1, INVALID_LSN, b"a");
    for _ in 0..9 {
        append(&log, Kind::Update, 1, INVALID_LSN, b"b");
    }
    log.flush_to(first).unwrap();
    assert_eq!(log.num_syncs(), 1);
    assert_eq!(log.durable_end(), log.end());
    log.flush_all().unwrap();
    assert_eq!(log.num_syncs(), 1);
}

#[test]
fn flushing_the_invalid_lsn_does_nothing() {
    let (_dir, log) = setup();
    append(&log, Kind::Update, 1, INVALID_LSN, b"a");
    log.flush_to(INVALID_LSN).unwrap();
    assert_eq!(log.num_syncs(), 0);
}

#[test]
fn a_full_tail_is_flushed_by_the_append_that_overflows_it() {
    let (dir, log) = setup();
    let body = vec![0xab; 64 << 10];
    let lsns: Vec<_> = (0..40)
        .map(|i| append(&log, Kind::System, i, INVALID_LSN, &body))
        .collect();
    assert!(log.durable_end() > FIRST_LSN, "nothing was flushed");
    let mut buf = Vec::new();
    for &lsn in &lsns {
        assert_eq!(log.read(lsn, &mut buf).unwrap().body, &body[..]);
    }
    log.flush_all().unwrap();
    drop(log);
    let log = Log::open(log_path(&dir)).unwrap();
    assert_eq!(scan_all(&log).len(), 40);
}

#[test]
fn a_record_larger_than_the_tail_still_goes_in() {
    let (_dir, log) = setup();
    append(&log, Kind::System, 1, INVALID_LSN, b"small");
    let body = vec![7u8; 3 << 20];
    let big = append(&log, Kind::System, 2, INVALID_LSN, &body);
    log.flush_all().unwrap();
    let mut buf = Vec::new();
    assert_eq!(log.read(big, &mut buf).unwrap().body.len(), 3 << 20);
    assert_eq!(scan_all(&log).len(), 2);
}

#[test]
fn a_torn_record_ends_the_log_and_is_cut_off() {
    let (dir, log) = setup();
    let a = append(&log, Kind::Update, 1, INVALID_LSN, b"one");
    let b = append(&log, Kind::Update, 1, a, b"two");
    let c = append(&log, Kind::Commit, 1, b, b"three");
    log.flush_all().unwrap();
    drop(log);

    let path = log_path(&dir);
    OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(c + 10)
        .unwrap();
    let log = Log::open(&path).unwrap();
    assert_eq!(scan_all(&log).len(), 2);
    assert_eq!(log.end(), c);
    assert_eq!(file_len(&path), c);

    let d = append(&log, Kind::Abort, 1, b, b"");
    assert_eq!(d, c);
    log.flush_all().unwrap();
    drop(log);
    let log = Log::open(&path).unwrap();
    let kinds: Vec<_> = scan_all(&log).into_iter().map(|r| r.3).collect();
    assert_eq!(kinds, [Kind::Update, Kind::Update, Kind::Abort]);
}

#[test]
fn a_record_that_fails_its_checksum_ends_the_log() {
    let (dir, log) = setup();
    let a = append(&log, Kind::Update, 1, INVALID_LSN, b"one");
    let b = append(&log, Kind::Update, 1, a, b"two");
    append(&log, Kind::Update, 1, b, b"three");
    log.flush_all().unwrap();
    drop(log);

    let path = log_path(&dir);
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    let at = b + HEADER_SIZE as Lsn;
    let mut byte = [0u8];
    file.read_exact_at(&mut byte, at).unwrap();
    file.write_all_at(&[byte[0] ^ 1], at).unwrap();
    drop(file);

    let log = Log::open(&path).unwrap();
    assert_eq!(scan_all(&log).len(), 1);
    assert_eq!(log.end(), b);
}

#[test]
fn a_record_copied_to_another_position_is_not_a_record() {
    let (dir, log) = setup();
    let a = append(&log, Kind::Update, 1, INVALID_LSN, b"one");
    log.flush_all().unwrap();
    let end = log.end();
    drop(log);

    // Intact bytes, but they name another position: stale bytes from an
    // older log never pass for a record.
    let path = log_path(&dir);
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    let mut bytes = vec![0u8; (end - a) as usize];
    file.read_exact_at(&mut bytes, a).unwrap();
    file.write_all_at(&bytes, end).unwrap();
    drop(file);

    let log = Log::open(&path).unwrap();
    assert_eq!(scan_all(&log).len(), 1);
    assert_eq!(log.end(), end);
}

#[test]
fn an_intact_record_of_an_unknown_kind_is_an_error() {
    let (dir, log) = setup();
    let a = append(&log, Kind::Update, 1, INVALID_LSN, b"one");
    log.flush_all().unwrap();
    drop(log);

    let path = log_path(&dir);
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    let len = HEADER_SIZE + 3;
    let mut bytes = vec![0u8; len];
    file.read_exact_at(&mut bytes, a).unwrap();
    bytes[32] = 99;
    let crc = crc32c::crc32c(&bytes[8..]);
    bytes[4..8].copy_from_slice(&crc.to_le_bytes());
    file.write_all_at(&bytes, a).unwrap();
    drop(file);

    assert!(matches!(Log::open(&path), Err(StorageError::CorruptLog(_))));
}

#[test]
fn a_file_that_is_not_a_log_is_refused() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("t.log");
    fs::write(&path, [1u8; 64]).unwrap();
    assert!(matches!(Log::open(&path), Err(StorageError::CorruptLog(_))));
}

#[test]
fn concurrent_appenders_and_flushers_lose_nothing() {
    let (dir, log) = setup();
    let log = Arc::new(log);
    let threads: Vec<_> = (0..8u64)
        .map(|t| {
            let log = Arc::clone(&log);
            thread::spawn(move || {
                let mut buf = RecordBuf::new();
                let mut prev = INVALID_LSN;
                for i in 0..200u64 {
                    record(&mut buf, Kind::Update, t + 1, &i.to_le_bytes());
                    prev = log.append(&buf, |_| prev).unwrap();
                    if i % 10 == 9 {
                        log.flush_to(prev).unwrap();
                        assert!(log.durable_end() > prev);
                    }
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    assert!(log.num_syncs() <= 8 * 20);
    drop(log);

    let log = Log::open(log_path(&dir)).unwrap();
    let records = scan_all(&log);
    assert_eq!(records.len(), 8 * 200);
    for t in 1..=8u64 {
        let mine: Vec<_> = records.iter().filter(|r| r.2 == t).collect();
        let bodies: Vec<u64> = mine
            .iter()
            .map(|r| u64::from_le_bytes(r.4[..].try_into().unwrap()))
            .collect();
        assert_eq!(bodies, (0..200).collect::<Vec<_>>());
        for pair in mine.windows(2) {
            assert_eq!(pair[1].1, pair[0].0, "prev_lsn chain of transaction {t}");
        }
    }
}
