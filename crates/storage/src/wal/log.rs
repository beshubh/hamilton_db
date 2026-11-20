//! The write-ahead log: an in-memory tail, and segment files on disk.
//!
//! An LSN is the byte position of a record's first byte in the log as if it
//! were one file without headers, starting at `FIRST_LSN`. On disk the log
//! is cut into segments, `<path>.00000001` on, each starting with a header
//! that names its first record's LSN, which is where the previous segment
//! ends. No record spans two segments, and none is at `INVALID_LSN`.
//! Flushes cut at record boundaries, so the record at `lsn` is durable
//! exactly when `lsn < durable_end()`.

use std::ffi::OsString;
use std::io;
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use common::{FIRST_TXN_ID, Lsn, TxnId};
use parking_lot::{Mutex, RwLock};

use crate::disk::fs::{Fs, FsFile};
use crate::error::{Result, StorageError};
use crate::wal::fatal;
use crate::wal::record::{HEADER_SIZE, Record, RecordBuf, record_len, seal};
use crate::wal::txn_log::Att;

/// Bytes before the first record in a segment: magic, version, padding, the
/// first record's LSN, and reserved space.
const FILE_HEADER_SIZE: u64 = 32;

/// The LSN of the first record a log ever holds.
pub const FIRST_LSN: Lsn = FILE_HEADER_SIZE;

const MAGIC: u64 = u64::from_le_bytes(*b"HAMDBWAL");
/// 2: `Update` and `Clr` records carry undo, and `HeapSetTuple` its padding.
/// 3: an `Update` can carry a create's undo.
/// 4: an `Abort` comes before every `Clr` of its transaction.
const VERSION: u32 = 5;

/// Appends stop to flush once the tail holds this much. A record larger
/// than this still goes in, alone.
const BUFFER_SIZE: usize = 1 << 20;

/// How much a scan reads from a segment at a time.
const SCAN_CHUNK: usize = 64 << 10;

/// The log. `flush_to` is group commit without a thread: the first caller
/// whose record is not durable writes and syncs the whole tail for everyone.
/// Lock order is `io`, then `tail`, then `segments`; `append` never holds
/// `tail` while it waits for `io`.
pub struct Log {
    fs: Arc<dyn Fs>,
    /// Segment `n` is this path with `.n`, eight digits at least, appended.
    path: PathBuf,
    /// A batch starts a new segment once the last holds this many bytes.
    segment_size: u64,
    tail: Mutex<Tail>,
    /// Held by the caller writing a batch.
    io: Mutex<Io>,
    /// Every segment, oldest first. Only a caller holding `io` adds one, and
    /// the last, which batches go to, is never removed.
    segments: RwLock<Vec<Segment>>,
    durable_end: AtomicU64,
    /// Set when a write or sync failed. The batch it held is lost, so no
    /// later record can be made durable after it.
    failed: AtomicBool,
    num_syncs: AtomicUsize,
}

struct Tail {
    /// LSN of `buf[0]`.
    start: Lsn,
    buf: Vec<u8>,
    /// Changed only as records are appended, so it always agrees with them.
    att: Att,
    /// See `next_tid`.
    next_tid: TxnId,
}

struct Io {
    /// The buffer a batch is written from, swapped with the tail's so
    /// neither allocates after the first batch.
    batch: Vec<u8>,
    /// The last segment.
    segment: Segment,
}

#[derive(Clone)]
struct Segment {
    seq: u64,
    /// LSN of its first record.
    base: Lsn,
    file: Arc<dyn FsFile>,
}

impl Segment {
    /// One past the LSN of the last byte in the file.
    fn file_end(&self) -> Result<Lsn> {
        Ok(self.base + self.file.size()? - FILE_HEADER_SIZE)
    }
}

impl Log {
    /// Opens the log whose segments are `path` with a number appended,
    /// creating the first if there is none, and cuts the last segment at its
    /// first torn record. Fails with `CorruptLog` if a segment is missing or
    /// does not go on from where the one before it ends.
    pub fn open(fs: Arc<dyn Fs>, path: impl AsRef<Path>, segment_size: u64) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let mut segments: Vec<Segment> = Vec::new();
        for seq in segment_numbers(&*fs, &path)? {
            let file: Arc<dyn FsFile> = Arc::from(fs.open(&segment_path(&path, seq))?);
            let base = read_header(&*file, seq)?;
            if let Some(prev) = segments.last() {
                if seq != prev.seq + 1 {
                    return Err(corrupt(format!("log segment {} is missing", prev.seq + 1)));
                }
                let prev_end = prev.file_end()?;
                if base != prev_end {
                    return Err(corrupt(format!(
                        "log segment {seq} starts at {base}, but segment {} ends at {prev_end}",
                        prev.seq
                    )));
                }
            }
            segments.push(Segment { seq, base, file });
        }
        if segments.is_empty() {
            segments.push(create_segment(&*fs, &path, 1, FIRST_LSN)?);
        }

        let last = segments.last().expect("the log has a segment").clone();
        let file_end = last.file_end()?;
        let end = {
            let mut scan = FileScan::new(vec![last.clone()], last.base, file_end);
            while scan.next_record()?.is_some() {}
            scan.pos
        };
        if end < file_end {
            last.file.set_size(offset_of(last.base, end))?;
            last.file.sync()?;
        }
        Ok(Self {
            fs,
            path,
            segment_size,
            tail: Mutex::new(Tail {
                start: end,
                buf: Vec::with_capacity(BUFFER_SIZE),
                att: Att::default(),
                next_tid: FIRST_TXN_ID,
            }),
            io: Mutex::new(Io {
                batch: Vec::with_capacity(BUFFER_SIZE),
                segment: last,
            }),
            segments: RwLock::new(segments),
            durable_end: AtomicU64::new(end),
            failed: AtomicBool::new(false),
            num_syncs: AtomicUsize::new(0),
        })
    }

    /// Appends `record`, which `RecordBuf::finish` has ended, and returns
    /// its LSN. The record's `prev_lsn` is its transaction's last record,
    /// from the active transaction table, which the record then updates.
    /// `on_append(lsn)` runs inside the critical section: whatever it
    /// records about `lsn` is seen by anyone who appends after. Flushes
    /// first if the tail is full. Fails only if an earlier flush failed.
    ///
    /// Stops the process if the record's transaction may not log it now
    /// (`TxnStatus::after`). `TxnLog` takes only the steps that table
    /// allows, so only a bug appends such a record.
    pub fn append(&self, record: &RecordBuf, on_append: impl FnOnce(Lsn)) -> Result<Lsn> {
        debug_assert!(
            record.len() >= HEADER_SIZE,
            "append of an unfinished record"
        );
        loop {
            self.check_failed()?;
            let mut tail = self.tail.lock();
            if !tail.buf.is_empty() && tail.buf.len() + record.len() > BUFFER_SIZE {
                let last = tail.start + tail.buf.len() as Lsn - 1;
                drop(tail);
                self.flush_to(last)?;
                continue;
            }
            let lsn = tail.start + tail.buf.len() as Lsn;
            let prev_lsn = tail
                .att
                .link(record.tid(), record.kind(), lsn)
                .unwrap_or_else(|e| fatal(format_args!("cannot log a record out of order: {e}")));
            let at = tail.buf.len();
            tail.buf.extend_from_slice(record.bytes());
            tail.next_tid = tail.next_tid.max(record.tid() + 1);
            on_append(lsn);
            seal(&mut tail.buf[at..], lsn, prev_lsn, record.tail_crc());
            return Ok(lsn);
        }
    }

    /// Returns once the record at `lsn` is written and synced, along with
    /// every record before it. `lsn` must be a record already appended, or
    /// `INVALID_LSN`, which is always durable.
    pub fn flush_to(&self, lsn: Lsn) -> Result<()> {
        self.check_failed()?;
        if self.durable_end.load(Ordering::Acquire) > lsn {
            return Ok(());
        }
        let mut io = self.io.lock();
        self.check_failed()?;
        if self.durable_end.load(Ordering::Acquire) > lsn {
            return Ok(());
        }
        let io = &mut *io;
        let start = {
            let mut tail = self.tail.lock();
            debug_assert!(
                lsn < tail.start + tail.buf.len() as Lsn,
                "flush_to({lsn}) past the end of the log"
            );
            mem::swap(&mut tail.buf, &mut io.batch);
            let start = tail.start;
            tail.start += io.batch.len() as Lsn;
            start
        };
        if let Err(e) = self.write_batch(io, start) {
            self.failed.store(true, Ordering::Release);
            return Err(e);
        }
        self.durable_end
            .store(start + io.batch.len() as Lsn, Ordering::Release);
        io.batch.clear();
        self.num_syncs.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Writes and syncs the batch, whose first record is at `start`. Starts
    /// a new segment first if the last is full: every record before `start`
    /// is durable by now, so a segment is never left behind with a torn end.
    fn write_batch(&self, io: &mut Io, start: Lsn) -> Result<()> {
        if start - io.segment.base >= self.segment_size {
            let segment = create_segment(&*self.fs, &self.path, io.segment.seq + 1, start)?;
            self.segments.write().push(segment.clone());
            io.segment = segment;
        }
        let segment = &io.segment;
        segment
            .file
            .write_at(&io.batch, offset_of(segment.base, start))?;
        segment.file.sync()?;
        Ok(())
    }

    /// Makes every record appended so far durable.
    pub fn flush_all(&self) -> Result<()> {
        let end = self.end();
        if end > self.durable_end() {
            self.flush_to(end - 1)?;
        }
        Ok(())
    }

    /// The LSN the next record will get.
    pub fn end(&self) -> Lsn {
        let tail = self.tail.lock();
        tail.start + tail.buf.len() as Lsn
    }

    /// Runs `f` with the LSN the next record will get, the active
    /// transaction table and `next_tid`, holding off appends meanwhile.
    pub(crate) fn at_end<R>(&self, f: impl FnOnce(Lsn, &Att, TxnId) -> R) -> R {
        let tail = self.tail.lock();
        f(tail.start + tail.buf.len() as Lsn, &tail.att, tail.next_tid)
    }

    /// A Tid above that of every record the log holds or held, which a
    /// restart hands out first. A transaction that logs nothing leaves no
    /// trace, so after a crash its Tid may be handed out again.
    pub fn next_tid(&self) -> TxnId {
        self.tail.lock().next_tid
    }

    /// The last record of transaction `tid`, unless it has none or has
    /// ended.
    pub fn last_lsn(&self, tid: TxnId) -> Option<Lsn> {
        self.tail.lock().att.get(tid).map(|entry| entry.last_lsn)
    }

    /// Makes `att` and `next_tid`, which recovery rebuilt from the
    /// checkpoint and the records after it, the log's own, so the records
    /// that finish its transactions chain onto theirs.
    pub(crate) fn restore(&self, att: Att, next_tid: TxnId) {
        let mut tail = self.tail.lock();
        tail.att = att;
        tail.next_tid = tail.next_tid.max(next_tid);
    }

    /// One past the last durable byte.
    pub fn durable_end(&self) -> Lsn {
        self.durable_end.load(Ordering::Acquire)
    }

    /// Copies the record at `lsn` into `buf` and views it there. The record
    /// may still be in memory or already in a segment.
    pub fn read<'b>(&self, lsn: Lsn, buf: &'b mut Vec<u8>) -> Result<Record<'b>> {
        buf.clear();
        {
            let tail = self.tail.lock();
            if lsn >= tail.start {
                let bytes = &tail.buf[(lsn - tail.start) as usize..];
                let len = record_len(bytes).ok_or_else(|| no_record(lsn))?;
                buf.extend_from_slice(&bytes[..len]);
            }
        }
        if buf.is_empty() {
            // Already swapped out of the tail: once the batch holding it is
            // done, it is in its segment.
            drop(self.io.lock());
            self.check_failed()?;
            let (segment, end) = self.segment_holding(lsn).ok_or_else(|| no_record(lsn))?;
            if lsn + HEADER_SIZE as Lsn > end {
                return Err(no_record(lsn));
            }
            buf.resize(HEADER_SIZE, 0);
            let at = offset_of(segment.base, lsn);
            read_exact_at(&*segment.file, buf, at)?;
            let len = u32::from_le_bytes(buf[..4].try_into().unwrap()) as usize;
            if len < HEADER_SIZE || lsn + len as Lsn > end {
                return Err(no_record(lsn));
            }
            buf.resize(len, 0);
            read_exact_at(
                &*segment.file,
                &mut buf[HEADER_SIZE..],
                at + HEADER_SIZE as u64,
            )?;
        }
        Record::decode(lsn, buf)?.ok_or_else(|| no_record(lsn))
    }

    /// The segment holding the durable byte at `lsn`, and where its durable
    /// part ends.
    fn segment_holding(&self, lsn: Lsn) -> Option<(Segment, Lsn)> {
        let durable_end = self.durable_end();
        let segments = self.segments.read();
        let next = segments.partition_point(|segment| segment.base <= lsn);
        let end = segments.get(next).map_or(durable_end, |s| s.base);
        let segment = segments.get(next.checked_sub(1)?)?;
        (lsn < end).then(|| (segment.clone(), end))
    }

    /// Reads the durable records from `from` on, in order. Records still in
    /// memory are not seen; flush first if they matter.
    pub fn scan(&self, from: Lsn) -> LogScan {
        let segments = self.segments.read();
        let first = segments.partition_point(|segment| segment.base <= from);
        // A scan from before the first segment finds no record where it
        // starts.
        let segments = match first.checked_sub(1) {
            Some(first) => segments[first..].to_vec(),
            None => Vec::new(),
        };
        LogScan(FileScan::new(segments, from, self.durable_end()))
    }

    /// Removes, oldest first, every segment whose records all come before
    /// `lsn`, except the last. Each removal is durable before the next
    /// starts, so a crash leaves the rest numbered without a gap.
    pub fn remove_segments_before(&self, lsn: Lsn) -> Result<()> {
        loop {
            let oldest = match &self.segments.read()[..] {
                [oldest, next, ..] if next.base <= lsn => oldest.seq,
                _ => return Ok(()),
            };
            self.fs.remove(&segment_path(&self.path, oldest))?;
            let mut segments = self.segments.write();
            if segments.first().is_some_and(|s| s.seq == oldest) {
                segments.remove(0);
            }
        }
    }

    /// LSN of the oldest record the log can hold.
    pub fn first_lsn(&self) -> Lsn {
        self.segments.read()[0].base
    }

    /// Number of syncs of the log since it was opened.
    pub fn num_syncs(&self) -> usize {
        self.num_syncs.load(Ordering::Relaxed)
    }

    fn check_failed(&self) -> Result<()> {
        if self.failed.load(Ordering::Acquire) {
            return Err(StorageError::LogFailed);
        }
        Ok(())
    }
}

/// A forward scan over durable records. See `Log::scan`.
pub struct LogScan(FileScan);

impl LogScan {
    /// The next record, or `None` at the end of the log. Fails with
    /// `CorruptLog` where a record should be and none is, such as a
    /// damaged record in a segment before the last.
    pub fn next_record(&mut self) -> Result<Option<Record<'_>>> {
        let pos = self.0.pos;
        if pos >= self.0.limit {
            return Ok(None);
        }
        match self.0.next_record()? {
            Some(record) => Ok(Some(record)),
            None => Err(no_record(pos)),
        }
    }
}

/// Reads records from segments in chunks, up to `limit`.
struct FileScan {
    /// The segments it reads, oldest first. The first holds `pos`.
    segments: Vec<Segment>,
    /// Index in `segments` of the one holding `pos`.
    at: usize,
    /// LSN of the next record.
    pos: Lsn,
    limit: Lsn,
    buf: Vec<u8>,
    /// LSN of `buf[0]`.
    buf_start: Lsn,
}

impl FileScan {
    fn new(segments: Vec<Segment>, from: Lsn, limit: Lsn) -> Self {
        Self {
            segments,
            at: 0,
            pos: from,
            limit,
            buf: Vec::new(),
            buf_start: from,
        }
    }

    /// The record at `pos`, or `None` if no whole, intact one starts there.
    fn next_record(&mut self) -> Result<Option<Record<'_>>> {
        while self
            .segments
            .get(self.at + 1)
            .is_some_and(|next| next.base <= self.pos)
        {
            self.at += 1;
        }
        if !self.fill(HEADER_SIZE)? {
            return Ok(None);
        }
        let at = (self.pos - self.buf_start) as usize;
        let claimed = u32::from_le_bytes(self.buf[at..at + 4].try_into().unwrap()) as usize;
        if claimed < HEADER_SIZE || !self.fill(claimed)? {
            return Ok(None);
        }
        let at = (self.pos - self.buf_start) as usize;
        let Some(record) = Record::decode(self.pos, &self.buf[at..at + claimed])? else {
            return Ok(None);
        };
        self.pos = record.end();
        Ok(Some(record))
    }

    /// Makes `buf` hold `n` bytes from `pos`, reading more of the segment if
    /// needed. Returns `false` if the segment, or the scan, ends first.
    fn fill(&mut self, n: usize) -> Result<bool> {
        let Some(segment) = self.segments.get(self.at) else {
            return Ok(false);
        };
        let end = self
            .segments
            .get(self.at + 1)
            .map_or(self.limit, |next| next.base.min(self.limit));
        if self.pos + n as Lsn > end {
            return Ok(false);
        }
        let have = (self.buf_start + self.buf.len() as Lsn).saturating_sub(self.pos) as usize;
        if self.pos >= self.buf_start && have >= n {
            return Ok(true);
        }
        let want = n.max(SCAN_CHUNK).min((end - self.pos) as usize);
        self.buf.resize(want, 0);
        read_exact_at(
            &*segment.file,
            &mut self.buf,
            offset_of(segment.base, self.pos),
        )?;
        self.buf_start = self.pos;
        Ok(true)
    }
}

/// Where the byte at `lsn` is in a segment whose first record is at `base`.
fn offset_of(base: Lsn, lsn: Lsn) -> u64 {
    lsn - base + FILE_HEADER_SIZE
}

fn segment_path(path: &Path, seq: u64) -> PathBuf {
    let mut name = OsString::from(path.as_os_str());
    name.push(format!(".{seq:08}"));
    PathBuf::from(name)
}

/// The numbers of the segments of the log at `path`, in order.
fn segment_numbers(fs: &dyn Fs, path: &Path) -> Result<Vec<u64>> {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| corrupt(format!("bad log path {}", path.display())))?;
    let prefix = format!("{name}.");
    let files = fs.list(path.parent().unwrap_or(Path::new("")))?;
    // A single-file log from before segments would otherwise be ignored,
    // and the changes in it lost.
    if files.iter().any(|file| file.to_str() == Some(name)) {
        return Err(corrupt(format!(
            "{} is a log from an older version; start the database fresh",
            path.display()
        )));
    }
    let mut numbers: Vec<u64> = files
        .iter()
        .filter_map(|file| {
            let number = file.to_str()?.strip_prefix(&prefix)?;
            number.bytes().all(|b| b.is_ascii_digit()).then_some(())?;
            number.parse().ok()
        })
        .collect();
    numbers.sort_unstable();
    Ok(numbers)
}

/// Creates segment `seq`, whose first record will be at `base`. The file
/// appears whole with its header, or not at all.
fn create_segment(fs: &dyn Fs, path: &Path, seq: u64, base: Lsn) -> Result<Segment> {
    let path = segment_path(path, seq);
    fs.replace(&path, &file_header(base))?;
    let file = Arc::from(fs.open(&path)?);
    Ok(Segment { seq, base, file })
}

/// The LSN of the first record in segment `seq`, from its header.
fn read_header(file: &dyn FsFile, seq: u64) -> Result<Lsn> {
    let mut header = [0u8; FILE_HEADER_SIZE as usize];
    if file.read_at(&mut header, 0)? < header.len() {
        return Err(corrupt(format!("log segment {seq} has no header")));
    }
    if u64::from_le_bytes(header[..8].try_into().unwrap()) != MAGIC {
        return Err(corrupt(format!(
            "log segment {seq} is not a hamilton_db log file"
        )));
    }
    let version = u32::from_le_bytes(header[8..12].try_into().unwrap());
    if version != VERSION {
        return Err(corrupt(format!("unsupported log version {version}")));
    }
    let base = u64::from_le_bytes(header[16..24].try_into().unwrap());
    if base < FIRST_LSN {
        return Err(corrupt(format!("first lsn {base} is inside the header")));
    }
    Ok(base)
}

/// The header of a segment whose first record is at `base`.
fn file_header(base: Lsn) -> [u8; FILE_HEADER_SIZE as usize] {
    let mut header = [0u8; FILE_HEADER_SIZE as usize];
    header[..8].copy_from_slice(&MAGIC.to_le_bytes());
    header[8..12].copy_from_slice(&VERSION.to_le_bytes());
    header[16..24].copy_from_slice(&base.to_le_bytes());
    header
}

fn read_exact_at(file: &dyn FsFile, buf: &mut [u8], offset: u64) -> Result<()> {
    if file.read_at(buf, offset)? < buf.len() {
        return Err(io::Error::from(io::ErrorKind::UnexpectedEof).into());
    }
    Ok(())
}

fn no_record(lsn: Lsn) -> StorageError {
    corrupt(format!("no record at {lsn}"))
}

fn corrupt(msg: impl Into<String>) -> StorageError {
    StorageError::CorruptLog(msg.into())
}
