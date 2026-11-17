use std::fs::{self, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;

use common::{INVALID_LSN, Lsn};
use storage::StorageError;
use storage::disk::fs::OsFs;
use storage::wal::log::{FIRST_LSN, Log};
use storage::wal::record::{HEADER_SIZE, Kind, RecordBuf};
use tempfile::TempDir;

/// Large enough that a log stays in its first segment.
const ONE_SEGMENT: u64 = u64::MAX;

/// Small enough that a few records fill a segment.
const SMALL: u64 = 1000;

fn record(buf: &mut RecordBuf, kind: Kind, tid: u64, body: &[u8]) {
    buf.begin(kind, tid);
    buf.body().extend_from_slice(body);
    buf.finish();
}

fn append(log: &Log, kind: Kind, tid: u64, body: &[u8]) -> Lsn {
    let mut buf = RecordBuf::new();
    record(&mut buf, kind, tid, body);
    log.append(&buf, |_| {}).unwrap()
}

/// Every durable record, as (lsn, prev_lsn, tid, kind, body).
fn scan_all(log: &Log) -> Vec<(Lsn, Lsn, u64, Kind, Vec<u8>)> {
    let mut scan = log.scan(log.first_lsn());
    let mut out = Vec::new();
    while let Some(r) = scan.next_record().unwrap() {
        out.push((r.lsn, r.prev_lsn, r.tid, r.kind, r.body.to_vec()));
    }
    out
}

fn open(dir: &TempDir, segment_size: u64) -> Result<Log, StorageError> {
    Log::open(Arc::new(OsFs), dir.path().join("t.log"), segment_size)
}

fn setup() -> (TempDir, Log) {
    let dir = TempDir::new().unwrap();
    let log = open(&dir, ONE_SEGMENT).unwrap();
    (dir, log)
}

/// The file of segment `n`. In the first, a record's LSN is its offset.
fn segment(dir: &TempDir, n: u64) -> PathBuf {
    dir.path().join(format!("t.log.{n:08}"))
}

/// The names of the segment files, oldest first.
fn segment_names(dir: &TempDir) -> Vec<String> {
    let mut names: Vec<String> = fs::read_dir(dir.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .filter(|name| name.starts_with("t.log."))
        .collect();
    names.sort();
    names
}

/// Appends `n` records with a 200-byte body, each flushed on its own, so
/// that a segment of `SMALL` bytes fills every few records.
fn append_flushed(log: &Log, n: u64) -> Vec<Lsn> {
    (0..n)
        .map(|i| {
            let lsn = append(log, Kind::System, i, &[i as u8; 200]);
            log.flush_to(lsn).unwrap();
            lsn
        })
        .collect()
}

/// The LSN each segment's first record has, oldest first.
fn segment_bases(dir: &TempDir) -> Vec<Lsn> {
    let mut base = FIRST_LSN;
    (1..=segment_names(dir).len() as u64)
        .map(|n| {
            let first = base;
            base += file_len(&segment(dir, n)) - FIRST_LSN;
            first
        })
        .collect()
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
    let a = append(&log, Kind::Update, 1, b"abc");
    let b = append(&log, Kind::Commit, 1, b"");
    assert_eq!(a, FIRST_LSN);
    assert_eq!(b, a + (HEADER_SIZE + 3) as Lsn);
    assert_eq!(log.end(), b + HEADER_SIZE as Lsn);
}

#[test]
fn records_are_read_back_from_memory_and_from_the_file() {
    let (_dir, log) = setup();
    let a = append(&log, Kind::Update, 7, b"first");
    let mut buf = Vec::new();
    let r = log.read(a, &mut buf).unwrap();
    assert_eq!(
        (r.kind, r.tid, r.prev_lsn, r.body),
        (Kind::Update, 7, INVALID_LSN, &b"first"[..])
    );

    log.flush_to(a).unwrap();
    let b = append(&log, Kind::Clr, 7, b"second");
    let r = log.read(a, &mut buf).unwrap();
    assert_eq!(r.body, b"first");
    let r = log.read(b, &mut buf).unwrap();
    assert_eq!((r.kind, r.prev_lsn, r.body), (Kind::Clr, a, &b"second"[..]));
    assert!(r.has_prev());
}

#[test]
fn reading_where_no_record_starts_fails() {
    let (_dir, log) = setup();
    let a = append(&log, Kind::Update, 1, b"payload");
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
fn each_record_links_to_its_transactions_last() {
    let (_dir, log) = setup();
    let mut buf = RecordBuf::new();
    let mut seen = Vec::new();
    for (kind, tid) in [
        (Kind::Update, 9),
        (Kind::System, 0),
        (Kind::Update, 4),
        (Kind::Update, 9),
        (Kind::Commit, 9),
        (Kind::End, 9),
        (Kind::Update, 9),
    ] {
        record(&mut buf, kind, tid, b"");
        let lsn = log.append(&buf, |lsn| seen.push(lsn)).unwrap();
        assert_eq!(seen.last(), Some(&lsn), "on_append runs with the LSN");
    }
    log.flush_all().unwrap();
    let prevs: Vec<_> = scan_all(&log).iter().map(|r| r.1).collect();
    let none = INVALID_LSN;
    assert_eq!(prevs, [none, none, none, seen[0], seen[3], seen[4], none]);
    assert_eq!(log.last_lsn(9), Some(seen[6]));
    assert_eq!(log.last_lsn(4), Some(seen[2]));
}

#[test]
fn only_durable_records_survive_reopening() {
    let (dir, log) = setup();
    let a = append(&log, Kind::Update, 1, b"kept");
    log.flush_to(a).unwrap();
    append(&log, Kind::Update, 1, b"lost");
    drop(log);

    let log = open(&dir, ONE_SEGMENT).unwrap();
    let records = scan_all(&log);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].4, b"kept");
    assert_eq!(log.end(), records[0].0 + (HEADER_SIZE + 4) as Lsn);
}

#[test]
fn one_sync_covers_every_record_before_it() {
    let (_dir, log) = setup();
    let first = append(&log, Kind::Update, 1, b"a");
    for _ in 0..9 {
        append(&log, Kind::Update, 1, b"b");
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
    append(&log, Kind::Update, 1, b"a");
    log.flush_to(INVALID_LSN).unwrap();
    assert_eq!(log.num_syncs(), 0);
}

#[test]
fn a_full_tail_is_flushed_by_the_append_that_overflows_it() {
    let (dir, log) = setup();
    let body = vec![0xab; 64 << 10];
    let lsns: Vec<_> = (0..40)
        .map(|i| append(&log, Kind::System, i, &body))
        .collect();
    assert!(log.durable_end() > FIRST_LSN, "nothing was flushed");
    let mut buf = Vec::new();
    for &lsn in &lsns {
        assert_eq!(log.read(lsn, &mut buf).unwrap().body, &body[..]);
    }
    log.flush_all().unwrap();
    drop(log);
    let log = open(&dir, ONE_SEGMENT).unwrap();
    assert_eq!(scan_all(&log).len(), 40);
}

#[test]
fn a_record_larger_than_the_tail_still_goes_in() {
    let (_dir, log) = setup();
    append(&log, Kind::System, 1, b"small");
    let body = vec![7u8; 3 << 20];
    let big = append(&log, Kind::System, 2, &body);
    log.flush_all().unwrap();
    let mut buf = Vec::new();
    assert_eq!(log.read(big, &mut buf).unwrap().body.len(), 3 << 20);
    assert_eq!(scan_all(&log).len(), 2);
}

#[test]
fn a_torn_record_ends_the_log_and_is_cut_off() {
    let (dir, log) = setup();
    append(&log, Kind::Update, 1, b"one");
    append(&log, Kind::Update, 1, b"two");
    let c = append(&log, Kind::Commit, 1, b"three");
    log.flush_all().unwrap();
    drop(log);

    let path = segment(&dir, 1);
    OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(c + 10)
        .unwrap();
    let log = open(&dir, ONE_SEGMENT).unwrap();
    assert_eq!(scan_all(&log).len(), 2);
    assert_eq!(log.end(), c);
    assert_eq!(file_len(&path), c);

    let d = append(&log, Kind::Abort, 1, b"");
    assert_eq!(d, c);
    log.flush_all().unwrap();
    drop(log);
    let log = open(&dir, ONE_SEGMENT).unwrap();
    let kinds: Vec<_> = scan_all(&log).into_iter().map(|r| r.3).collect();
    assert_eq!(kinds, [Kind::Update, Kind::Update, Kind::Abort]);
}

#[test]
fn a_record_that_fails_its_checksum_ends_the_log() {
    let (dir, log) = setup();
    append(&log, Kind::Update, 1, b"one");
    let b = append(&log, Kind::Update, 1, b"two");
    append(&log, Kind::Update, 1, b"three");
    log.flush_all().unwrap();
    drop(log);

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(segment(&dir, 1))
        .unwrap();
    let at = b + HEADER_SIZE as Lsn;
    let mut byte = [0u8];
    file.read_exact_at(&mut byte, at).unwrap();
    file.write_all_at(&[byte[0] ^ 1], at).unwrap();
    drop(file);

    let log = open(&dir, ONE_SEGMENT).unwrap();
    assert_eq!(scan_all(&log).len(), 1);
    assert_eq!(log.end(), b);
}

#[test]
fn a_record_copied_to_another_position_is_not_a_record() {
    let (dir, log) = setup();
    let a = append(&log, Kind::Update, 1, b"one");
    log.flush_all().unwrap();
    let end = log.end();
    drop(log);

    // Intact bytes, but they name another position: stale bytes from an
    // older log never pass for a record.
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(segment(&dir, 1))
        .unwrap();
    let mut bytes = vec![0u8; (end - a) as usize];
    file.read_exact_at(&mut bytes, a).unwrap();
    file.write_all_at(&bytes, end).unwrap();
    drop(file);

    let log = open(&dir, ONE_SEGMENT).unwrap();
    assert_eq!(scan_all(&log).len(), 1);
    assert_eq!(log.end(), end);
}

#[test]
fn an_intact_record_of_an_unknown_kind_is_an_error() {
    let (dir, log) = setup();
    let a = append(&log, Kind::Update, 1, b"one");
    log.flush_all().unwrap();
    drop(log);

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(segment(&dir, 1))
        .unwrap();
    let len = HEADER_SIZE + 3;
    let mut bytes = vec![0u8; len];
    file.read_exact_at(&mut bytes, a).unwrap();
    bytes[32] = 99;
    let crc = crc32c::crc32c(&bytes[8..]);
    bytes[4..8].copy_from_slice(&crc.to_le_bytes());
    file.write_all_at(&bytes, a).unwrap();
    drop(file);

    assert!(matches!(
        open(&dir, ONE_SEGMENT),
        Err(StorageError::CorruptLog(_))
    ));
}

#[test]
fn a_file_that_is_not_a_log_is_refused() {
    let dir = TempDir::new().unwrap();
    fs::write(segment(&dir, 1), [1u8; 64]).unwrap();
    assert!(matches!(
        open(&dir, ONE_SEGMENT),
        Err(StorageError::CorruptLog(_))
    ));
}

#[test]
fn a_single_file_log_from_before_segments_is_refused() {
    let dir = TempDir::new().unwrap();
    fs::write(dir.path().join("t.log"), [0u8; 64]).unwrap();
    assert!(matches!(
        open(&dir, ONE_SEGMENT),
        Err(StorageError::CorruptLog(_))
    ));
}

#[test]
fn concurrent_appenders_and_flushers_lose_nothing() {
    let dir = TempDir::new().unwrap();
    let log = Arc::new(open(&dir, 4 << 10).unwrap());
    let threads: Vec<_> = (0..8u64)
        .map(|t| {
            let log = Arc::clone(&log);
            thread::spawn(move || {
                let mut buf = RecordBuf::new();
                for i in 0..200u64 {
                    record(&mut buf, Kind::Update, t + 1, &i.to_le_bytes());
                    let lsn = log.append(&buf, |_| {}).unwrap();
                    if i % 10 == 9 {
                        log.flush_to(lsn).unwrap();
                        assert!(log.durable_end() > lsn);
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

    assert!(
        segment_names(&dir).len() > 3,
        "the appends started segments"
    );
    let log = open(&dir, 4 << 10).unwrap();
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

#[test]
fn a_full_segment_is_followed_by_a_new_one_and_reads_cross_them() {
    let dir = TempDir::new().unwrap();
    let log = open(&dir, SMALL).unwrap();
    let lsns = append_flushed(&log, 20);

    let names = segment_names(&dir);
    assert!(names.len() >= 3, "{names:?}");
    for (n, name) in names.iter().enumerate() {
        assert_eq!(*name, format!("t.log.{:08}", n + 1));
    }
    for n in 1..names.len() as u64 {
        let records = file_len(&segment(&dir, n)) - FIRST_LSN;
        assert!(records >= SMALL, "segment {n} holds {records} bytes");
    }
    let mut buf = Vec::new();
    for (i, &lsn) in lsns.iter().enumerate() {
        assert_eq!(log.read(lsn, &mut buf).unwrap().tid, i as u64);
    }
    assert_eq!(scan_all(&log).len(), 20);
    let end = log.end();
    drop(log);

    let log = open(&dir, SMALL).unwrap();
    assert_eq!(log.first_lsn(), FIRST_LSN);
    assert_eq!(log.end(), end);
    let tids: Vec<u64> = scan_all(&log).into_iter().map(|r| r.2).collect();
    assert_eq!(tids, (0..20).collect::<Vec<_>>());
    let next = append(&log, Kind::Commit, 99, b"");
    assert_eq!(next, end);
}

#[test]
fn a_missing_segment_is_corruption() {
    let dir = TempDir::new().unwrap();
    append_flushed(&open(&dir, SMALL).unwrap(), 20);
    assert!(segment_names(&dir).len() >= 3);

    fs::remove_file(segment(&dir, 2)).unwrap();
    assert!(matches!(
        open(&dir, SMALL),
        Err(StorageError::CorruptLog(msg)) if msg.contains("segment 2 is missing")
    ));
}

#[test]
fn a_segment_cut_short_before_the_last_is_corruption() {
    let dir = TempDir::new().unwrap();
    append_flushed(&open(&dir, SMALL).unwrap(), 20);
    assert!(segment_names(&dir).len() >= 3);

    let path = segment(&dir, 2);
    let len = file_len(&path);
    OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(len - 5)
        .unwrap();
    assert!(matches!(
        open(&dir, SMALL),
        Err(StorageError::CorruptLog(_))
    ));
}

#[test]
fn a_torn_record_is_cut_off_the_last_segment() {
    let dir = TempDir::new().unwrap();
    let lsns = append_flushed(&open(&dir, SMALL).unwrap(), 20);
    let last = segment_names(&dir).len() as u64;
    assert!(last >= 3);

    let path = segment(&dir, last);
    let len = file_len(&path);
    OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(len - 5)
        .unwrap();
    let log = open(&dir, SMALL).unwrap();
    assert_eq!(log.end(), lsns[19]);
    assert_eq!(scan_all(&log).len(), 19);
    assert_eq!(file_len(&path), len - (HEADER_SIZE as u64 + 200));
}

#[test]
fn a_damaged_record_before_the_last_segment_fails_the_scan() {
    let dir = TempDir::new().unwrap();
    let lsns = append_flushed(&open(&dir, SMALL).unwrap(), 20);
    assert!(segment_names(&dir).len() >= 3);

    // In the first segment a record's LSN is its offset.
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(segment(&dir, 1))
        .unwrap();
    let at = lsns[1] + HEADER_SIZE as Lsn;
    let mut byte = [0u8];
    file.read_exact_at(&mut byte, at).unwrap();
    file.write_all_at(&[byte[0] ^ 1], at).unwrap();
    drop(file);

    // Open reads only the last segment; the damage shows once read.
    let log = open(&dir, SMALL).unwrap();
    let mut scan = log.scan(log.first_lsn());
    assert!(scan.next_record().unwrap().is_some());
    assert!(matches!(
        scan.next_record(),
        Err(StorageError::CorruptLog(_))
    ));
    let mut buf = Vec::new();
    assert!(matches!(
        log.read(lsns[1], &mut buf),
        Err(StorageError::CorruptLog(_))
    ));
}

#[test]
fn removing_segments_keeps_every_one_with_a_record_from_the_lsn_on() {
    let dir = TempDir::new().unwrap();
    let log = open(&dir, SMALL).unwrap();
    let lsns = append_flushed(&log, 30);
    let bases = segment_bases(&dir);
    assert!(bases.len() >= 5, "{bases:?}");

    // A record inside the third segment: its segment stays, older ones go.
    let lsn = *lsns.iter().find(|&&lsn| lsn > bases[2]).unwrap();
    assert!(lsn < bases[3]);
    log.remove_segments_before(lsn).unwrap();
    assert_eq!(segment_names(&dir)[0], "t.log.00000003");
    assert_eq!(log.first_lsn(), bases[2]);
    let mut buf = Vec::new();
    assert_eq!(log.read(lsn, &mut buf).unwrap().lsn, lsn);
    assert!(matches!(
        log.read(lsns[0], &mut buf),
        Err(StorageError::CorruptLog(_))
    ));
    let kept = scan_all(&log).len();
    assert_eq!(kept, lsns.iter().filter(|&&l| l >= bases[2]).count());

    // At a segment's first record, the segment before it holds only older
    // ones.
    log.remove_segments_before(bases[3]).unwrap();
    assert_eq!(segment_names(&dir)[0], "t.log.00000004");

    // The segment being written stays, however far the LSN is.
    log.remove_segments_before(Lsn::MAX).unwrap();
    let names = segment_names(&dir);
    assert_eq!(names, [format!("t.log.{:08}", bases.len())]);
    let end = log.end();
    drop(log);

    let log = open(&dir, SMALL).unwrap();
    assert_eq!(log.first_lsn(), bases[bases.len() - 1]);
    assert_eq!(log.end(), end);
    let mut scan = log.scan(FIRST_LSN);
    assert!(matches!(
        scan.next_record(),
        Err(StorageError::CorruptLog(_))
    ));
}
