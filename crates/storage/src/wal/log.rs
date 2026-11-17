//! The write-ahead log file and its in-memory tail.
//!
//! An LSN is the byte position of a record's first byte in the log as if it
//! had never been cut: the file header names the LSN its first record has,
//! so a log can start over in a new file without page LSNs on disk going
//! stale. No record is at `INVALID_LSN`. Flushes cut
//! at record boundaries, so the record at `lsn` is durable exactly when
//! `lsn < durable_end()`.

use std::fs::{File, OpenOptions};
use std::io;
use std::mem;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use common::Lsn;
use parking_lot::Mutex;

use crate::disk::disk_manager::{read_full_at, sync_parent_dir, write_all_at};
use crate::error::{Result, StorageError};
use crate::wal::record::{HEADER_SIZE, Record, RecordBuf, record_len, seal};

/// Bytes before the first record in the file: magic, version, padding, the
/// first record's LSN, and reserved space.
const FILE_HEADER_SIZE: u64 = 32;

/// The first LSN of a log that has never started over.
pub const FIRST_LSN: Lsn = FILE_HEADER_SIZE;

const MAGIC: u64 = u64::from_le_bytes(*b"HAMDBWAL");
const VERSION: u32 = 1;

/// Appends stop to flush once the tail holds this much. A record larger
/// than this still goes in, alone.
const BUFFER_SIZE: usize = 1 << 20;

/// How much a scan reads from the file at a time.
const SCAN_CHUNK: usize = 64 << 10;

/// The log: records are appended to an in-memory tail and reach the file
/// when someone needs them durable.
///
/// `flush_to` is group commit without a thread: the first caller to find its
/// record not yet durable takes the I/O lock, swaps out the whole tail and
/// writes and syncs it, and every caller whose record was in that batch
/// returns when it is done. Lock order is `io`, then `tail`; `append` never
/// holds `tail` while it waits for `io`.
pub struct Log {
    file: File,
    /// LSN of the first record in the file.
    base: Lsn,
    tail: Mutex<Tail>,
    /// Held by the caller writing a batch. Holds the buffer it writes from,
    /// swapped with the tail's so neither allocates after the first batch.
    io: Mutex<Vec<u8>>,
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
}

impl Log {
    /// Opens the log at `path`, creating it if needed. The log ends at the
    /// first record that is cut short or fails its checksum, which is where
    /// a crash in the middle of a write leaves it; anything after that is
    /// cut off the file.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        let len = file.metadata()?.len();
        let base = if len < FILE_HEADER_SIZE {
            let mut header = [0u8; FILE_HEADER_SIZE as usize];
            header[..8].copy_from_slice(&MAGIC.to_le_bytes());
            header[8..12].copy_from_slice(&VERSION.to_le_bytes());
            header[16..24].copy_from_slice(&FIRST_LSN.to_le_bytes());
            file.set_len(0)?;
            write_all_at(&file, &header, 0)?;
            file.sync_all()?;
            sync_parent_dir(path)?;
            FIRST_LSN
        } else {
            let mut header = [0u8; FILE_HEADER_SIZE as usize];
            read_full_at(&file, &mut header, 0)?;
            if u64::from_le_bytes(header[..8].try_into().unwrap()) != MAGIC {
                return Err(corrupt("not a hamilton_db log file"));
            }
            let version = u32::from_le_bytes(header[8..12].try_into().unwrap());
            if version != VERSION {
                return Err(corrupt(format!("unsupported log version {version}")));
            }
            let base = u64::from_le_bytes(header[16..24].try_into().unwrap());
            if base < FIRST_LSN {
                return Err(corrupt(format!("first lsn {base} is inside the header")));
            }
            base
        };

        let file_lsns = base + (file.metadata()?.len() - FILE_HEADER_SIZE);
        let end = {
            let mut scan = FileScan::new(&file, base, base, file_lsns);
            while scan.next_record()?.is_some() {}
            scan.pos
        };
        if end < file_lsns {
            file.set_len(offset_of(base, end))?;
            file.sync_all()?;
        }
        Ok(Self {
            file,
            base,
            tail: Mutex::new(Tail {
                start: end,
                buf: Vec::with_capacity(BUFFER_SIZE),
            }),
            io: Mutex::new(Vec::with_capacity(BUFFER_SIZE)),
            durable_end: AtomicU64::new(end),
            failed: AtomicBool::new(false),
            num_syncs: AtomicUsize::new(0),
        })
    }

    /// Creates an empty log at `path`, replacing any log there.
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        File::create(path.as_ref())?;
        Self::open(path)
    }

    /// Appends `record`, which `RecordBuf::finish` has ended, and returns
    /// its LSN. `link(lsn)` runs inside the critical section and returns the
    /// record's `prev_lsn`: whatever it records about `lsn` is seen by
    /// anyone who appends after. Flushes first if the tail is full. Fails
    /// only if an earlier flush failed.
    pub fn append(&self, record: &RecordBuf, link: impl FnOnce(Lsn) -> Lsn) -> Result<Lsn> {
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
            let at = tail.buf.len();
            tail.buf.extend_from_slice(record.bytes());
            let prev_lsn = link(lsn);
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
        let mut batch = self.io.lock();
        self.check_failed()?;
        if self.durable_end.load(Ordering::Acquire) > lsn {
            return Ok(());
        }
        let start = {
            let mut tail = self.tail.lock();
            debug_assert!(
                lsn < tail.start + tail.buf.len() as Lsn,
                "flush_to({lsn}) past the end of the log"
            );
            mem::swap(&mut tail.buf, &mut *batch);
            let start = tail.start;
            tail.start += batch.len() as Lsn;
            start
        };
        let written = write_all_at(&self.file, &batch, offset_of(self.base, start))
            .and_then(|()| self.file.sync_data());
        if let Err(e) = written {
            self.failed.store(true, Ordering::Release);
            return Err(e.into());
        }
        self.durable_end
            .store(start + batch.len() as Lsn, Ordering::Release);
        batch.clear();
        self.num_syncs.fetch_add(1, Ordering::Relaxed);
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

    /// One past the last durable byte.
    pub fn durable_end(&self) -> Lsn {
        self.durable_end.load(Ordering::Acquire)
    }

    /// Copies the record at `lsn` into `buf` and views it there. The record
    /// may still be in memory or already in the file.
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
            // done, it is in the file.
            drop(self.io.lock());
            self.check_failed()?;
            if lsn < self.base || lsn >= self.durable_end() {
                return Err(no_record(lsn));
            }
            buf.resize(HEADER_SIZE, 0);
            read_exact_at(&self.file, buf, offset_of(self.base, lsn))?;
            let len = u32::from_le_bytes(buf[..4].try_into().unwrap()) as usize;
            if len < HEADER_SIZE || lsn + len as Lsn > self.durable_end() {
                return Err(no_record(lsn));
            }
            buf.resize(len, 0);
            let body = offset_of(self.base, lsn) + HEADER_SIZE as u64;
            read_exact_at(&self.file, &mut buf[HEADER_SIZE..], body)?;
        }
        Record::decode(lsn, buf)?.ok_or_else(|| no_record(lsn))
    }

    /// Reads the durable records from `from` on, in order. Records still in
    /// memory are not seen; flush first if they matter.
    pub fn scan(&self, from: Lsn) -> LogScan<'_> {
        LogScan(FileScan::new(
            &self.file,
            self.base,
            from,
            self.durable_end(),
        ))
    }

    /// LSN of the oldest record the file can hold.
    pub fn first_lsn(&self) -> Lsn {
        self.base
    }

    /// Number of syncs of the log file since it was opened.
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
pub struct LogScan<'a>(FileScan<'a>);

impl LogScan<'_> {
    /// The next record, or `None` at the end of the log.
    pub fn next_record(&mut self) -> Result<Option<Record<'_>>> {
        self.0.next_record()
    }
}

/// Reads records from the file in chunks, up to `limit`.
struct FileScan<'a> {
    file: &'a File,
    /// LSN at the end of the file header.
    base: Lsn,
    /// LSN of the next record.
    pos: Lsn,
    limit: Lsn,
    buf: Vec<u8>,
    /// LSN of `buf[0]`.
    buf_start: Lsn,
}

impl<'a> FileScan<'a> {
    fn new(file: &'a File, base: Lsn, from: Lsn, limit: Lsn) -> Self {
        Self {
            file,
            base,
            pos: from,
            limit,
            buf: Vec::new(),
            buf_start: from,
        }
    }

    fn next_record(&mut self) -> Result<Option<Record<'_>>> {
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

    /// Makes `buf` hold `n` bytes from `pos`, reading more of the file if
    /// needed. Returns `false` if the log ends first.
    fn fill(&mut self, n: usize) -> Result<bool> {
        if self.pos + n as Lsn > self.limit {
            return Ok(false);
        }
        let have = (self.buf_start + self.buf.len() as Lsn).saturating_sub(self.pos) as usize;
        if self.pos >= self.buf_start && have >= n {
            return Ok(true);
        }
        let want = n.max(SCAN_CHUNK).min((self.limit - self.pos) as usize);
        self.buf.resize(want, 0);
        read_exact_at(self.file, &mut self.buf, offset_of(self.base, self.pos))?;
        self.buf_start = self.pos;
        Ok(true)
    }
}

/// Where the byte at `lsn` is in a file whose first record is at `base`.
fn offset_of(base: Lsn, lsn: Lsn) -> u64 {
    lsn - base + FILE_HEADER_SIZE
}

fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> Result<()> {
    if read_full_at(file, buf, offset)? < buf.len() {
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
