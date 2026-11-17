//! The framing every log record shares.
//!
//! ```text
//! len u32 | crc u32 | lsn u64 | prev_lsn u64 | tid u64 | kind u8 | body
//! ```
//!
//! All little-endian. `len` counts the whole record. `crc` is CRC-32C of
//! everything after it. `lsn` is the record's own position, so bytes left
//! over from an older log never pass for a record. `lsn` and `prev_lsn` are
//! only known inside the log's critical section, so `RecordBuf` checksums the
//! rest up front and `Log::append` folds them in with `crc32c_combine`.

use common::{INVALID_LSN, Lsn, PageId, TxnId};

use crate::error::{Result, StorageError};
use crate::page::PAGE_SIZE;

pub const HEADER_SIZE: usize = 33;

const LEN: usize = 0;
const CRC: usize = 4;
const LSN: usize = 8;
const PREV_LSN: usize = 16;
/// Start of the part `RecordBuf` checksums before the record has an LSN.
const TID: usize = 24;
const KIND: usize = 32;

/// What a record is. The values are on disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Kind {
    /// An undoable change by a transaction.
    Update = 1,
    /// A compensation: the undo of one `Update`. Never undone itself.
    Clr = 2,
    /// Work a committed transaction does after its `Commit`, such as
    /// stamping its tuples. Redo-only.
    Completion = 3,
    /// A change that belongs to no transaction. Redo-only.
    System = 4,
    Commit = 5,
    Abort = 6,
    /// The transaction has nothing left in the log to do.
    End = 7,
}

impl TryFrom<u8> for Kind {
    type Error = u8;

    fn try_from(value: u8) -> Result<Self, u8> {
        Ok(match value {
            1 => Self::Update,
            2 => Self::Clr,
            3 => Self::Completion,
            4 => Self::System,
            5 => Self::Commit,
            6 => Self::Abort,
            7 => Self::End,
            other => return Err(other),
        })
    }
}

/// A record being built, reused from one record to the next so encoding
/// does not allocate.
#[derive(Default)]
pub struct RecordBuf {
    bytes: Vec<u8>,
    /// CRC-32C of `bytes[TID..]`, set by `finish`.
    tail_crc: u32,
}

impl RecordBuf {
    pub fn new() -> Self {
        Self::default()
    }

    /// Starts a record, dropping whatever the buffer held. Write the body
    /// into `body`, then call `finish`.
    pub fn begin(&mut self, kind: Kind, tid: TxnId) {
        self.bytes.clear();
        self.bytes.resize(HEADER_SIZE, 0);
        self.bytes[TID..KIND].copy_from_slice(&tid.to_le_bytes());
        self.bytes[KIND] = kind as u8;
    }

    /// The body written so far, to append to.
    pub fn body(&mut self) -> &mut Vec<u8> {
        &mut self.bytes
    }

    /// Ends the record: sets its length and checksums the part that does
    /// not depend on where it lands in the log.
    ///
    /// # Panics
    /// If the record is longer than `u32::MAX` bytes.
    pub fn finish(&mut self) {
        assert!(self.bytes.len() >= HEADER_SIZE, "finish without begin");
        let len = u32::try_from(self.bytes.len()).expect("log record too long");
        self.bytes[LEN..CRC].copy_from_slice(&len.to_le_bytes());
        self.tail_crc = crc32c::crc32c(&self.bytes[TID..]);
    }

    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(crate) fn tail_crc(&self) -> u32 {
        self.tail_crc
    }
}

/// Fills in `lsn` and `prev_lsn` of a record copied into the log buffer, and
/// its checksum. `tail_crc` is the one `RecordBuf::finish` computed.
pub(crate) fn seal(record: &mut [u8], lsn: Lsn, prev_lsn: Lsn, tail_crc: u32) {
    record[LSN..PREV_LSN].copy_from_slice(&lsn.to_le_bytes());
    record[PREV_LSN..TID].copy_from_slice(&prev_lsn.to_le_bytes());
    let linked = crc32c::crc32c(&record[LSN..TID]);
    let crc = crc32c::crc32c_combine(linked, tail_crc, record.len() - TID);
    record[CRC..LSN].copy_from_slice(&crc.to_le_bytes());
}

/// A record read back from the log, borrowing its bytes.
#[derive(Debug, Clone, Copy)]
pub struct Record<'a> {
    pub lsn: Lsn,
    /// The previous record of the same transaction, or `INVALID_LSN`.
    pub prev_lsn: Lsn,
    pub tid: TxnId,
    pub kind: Kind,
    pub body: &'a [u8],
}

impl<'a> Record<'a> {
    /// Checks and views the record at the start of `bytes`, which was read
    /// from position `lsn`. Returns `None` if no whole, intact record starts
    /// there, which is how the end of the log looks. A record that is intact
    /// but of a kind we don't know is an error, not the end.
    pub fn decode(lsn: Lsn, bytes: &'a [u8]) -> Result<Option<Self>> {
        let Some(len) = record_len(bytes) else {
            return Ok(None);
        };
        let record = &bytes[..len];
        let stored = u32::from_le_bytes(record[CRC..LSN].try_into().unwrap());
        if crc32c::crc32c(&record[LSN..]) != stored || read_u64(record, LSN) != lsn {
            return Ok(None);
        }
        let kind = Kind::try_from(record[KIND])
            .map_err(|kind| StorageError::CorruptLog(format!("unknown kind {kind} at {lsn}")))?;
        Ok(Some(Self {
            lsn,
            prev_lsn: read_u64(record, PREV_LSN),
            tid: read_u64(record, TID),
            kind,
            body: &record[HEADER_SIZE..],
        }))
    }

    /// Position of the record after this one.
    pub fn end(&self) -> Lsn {
        self.lsn + (HEADER_SIZE + self.body.len()) as Lsn
    }

    pub fn has_prev(&self) -> bool {
        self.prev_lsn != INVALID_LSN
    }
}

/// The body of a record that changes pages (`Update`, `Clr`, `Completion`
/// or `System`): how to redo each page it changed, and the pages it freed.
///
/// ```text
/// num_pages u16 | (page_id i32 | form u8 | form body)* | num_frees u32 | page_id i32*
/// ```
///
/// The form says how the page is redone; it is `IMAGE`, followed by the
/// whole page, for every page so far.
#[derive(Debug, Clone, Copy)]
pub struct PageChanges<'a> {
    num_pages: u16,
    pages: &'a [u8],
    frees: &'a [u8],
}

/// How one page of a `PageChanges` is redone.
#[derive(Debug, Clone, Copy)]
pub enum PageRedo<'a> {
    /// The page after the change, except for its LSN, which is the record's.
    Image(&'a [u8; PAGE_SIZE]),
}

const IMAGE: u8 = 1;
const PAGE_ENTRY_HEADER: usize = 5;

impl<'a> PageChanges<'a> {
    /// Checks and views the body of a page-change record.
    pub fn decode(body: &'a [u8]) -> Result<Self> {
        let corrupt = |what: &str| StorageError::CorruptLog(format!("page changes: {what}"));
        let (num_pages, entries) = body
            .split_first_chunk::<2>()
            .ok_or_else(|| corrupt("truncated"))?;
        let num_pages = u16::from_le_bytes(*num_pages);
        let mut rest = entries;
        for _ in 0..num_pages {
            if rest.get(4) != Some(&IMAGE) {
                return Err(corrupt("unknown page form"));
            }
            rest = rest
                .get(PAGE_ENTRY_HEADER + PAGE_SIZE..)
                .ok_or_else(|| corrupt("truncated"))?;
        }
        let pages = &entries[..entries.len() - rest.len()];
        let (num_frees, frees) = rest
            .split_first_chunk::<4>()
            .ok_or_else(|| corrupt("truncated"))?;
        if frees.len() != u32::from_le_bytes(*num_frees) as usize * size_of::<PageId>() {
            return Err(corrupt("bad length"));
        }
        Ok(Self {
            num_pages,
            pages,
            frees,
        })
    }

    pub fn pages(&self) -> impl Iterator<Item = (PageId, PageRedo<'a>)> + 'a {
        let mut rest = self.pages;
        (0..self.num_pages).map(move |_| {
            let (entry, next) = rest.split_at(PAGE_ENTRY_HEADER + PAGE_SIZE);
            rest = next;
            let page_id = PageId::from_le_bytes(entry[..4].try_into().unwrap());
            let image = entry[PAGE_ENTRY_HEADER..].try_into().unwrap();
            (page_id, PageRedo::Image(image))
        })
    }

    pub fn frees(&self) -> impl Iterator<Item = PageId> + 'a {
        self.frees
            .chunks_exact(size_of::<PageId>())
            .map(|id| PageId::from_le_bytes(id.try_into().unwrap()))
    }
}

/// Appends a `PageChanges` body to `body`: an image of each page in
/// `images`, then `frees`.
///
/// # Panics
/// If there are more than `u16::MAX` images.
pub(crate) fn encode_page_changes<'p>(
    body: &mut Vec<u8>,
    images: impl Iterator<Item = (PageId, &'p [u8; PAGE_SIZE])>,
    frees: &[PageId],
) {
    let num_pages_at = body.len();
    body.extend_from_slice(&[0; 2]);
    let mut num_pages: u16 = 0;
    for (page_id, data) in images {
        num_pages = num_pages
            .checked_add(1)
            .expect("too many pages in one record");
        body.extend_from_slice(&page_id.to_le_bytes());
        body.push(IMAGE);
        body.extend_from_slice(data);
    }
    body[num_pages_at..num_pages_at + 2].copy_from_slice(&num_pages.to_le_bytes());
    body.extend_from_slice(&(frees.len() as u32).to_le_bytes());
    for page_id in frees {
        body.extend_from_slice(&page_id.to_le_bytes());
    }
}

/// The length a record at the start of `bytes` claims, if `bytes` holds
/// that much and the claim is possible.
pub(crate) fn record_len(bytes: &[u8]) -> Option<usize> {
    let len = u32::from_le_bytes(bytes.get(LEN..CRC)?.try_into().unwrap()) as usize;
    (len >= HEADER_SIZE && len <= bytes.len()).then_some(len)
}

fn read_u64(bytes: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(bytes[at..at + 8].try_into().unwrap())
}
