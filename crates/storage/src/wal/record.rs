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
//!
//! The body depends on the kind:
//!
//! ```text
//! Update                undo | page changes
//! Clr                   undo_next u64 | page changes
//! Completion, System    page changes
//! Commit, Abort, End    (empty)
//! ```

use common::{INVALID_LSN, Lsn, PageId, Rid, TxnId};

use crate::error::{Result, StorageError};
use crate::page::PAGE_SIZE;
use crate::table::table_page::TupleMeta;
use crate::wal::op::{Ops, Reader, encode_meta};

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

    pub(crate) fn kind(&self) -> Kind {
        Kind::try_from(self.bytes[KIND]).expect("set by begin")
    }

    pub(crate) fn tid(&self) -> TxnId {
        read_u64(&self.bytes, TID)
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

    /// The pages the record changes and frees, or `None` for a kind that
    /// changes none.
    pub fn page_changes(&self) -> Result<Option<PageChanges<'a>>> {
        let changes = match self.kind {
            Kind::Update => Undo::decode(self.body)?.1,
            Kind::Clr => self.split_undo_next()?.1,
            Kind::Completion | Kind::System => self.body,
            Kind::Commit | Kind::Abort | Kind::End => return Ok(None),
        };
        PageChanges::decode(changes).map(Some)
    }

    /// How to take back the change of an `Update` record.
    pub fn undo(&self) -> Result<Undo<'a>> {
        if self.kind != Kind::Update {
            return Err(StorageError::CorruptLog(format!(
                "{:?} at {} has no undo",
                self.kind, self.lsn
            )));
        }
        Ok(Undo::decode(self.body)?.0)
    }

    /// The record a `Clr` leaves to undo next: the one before the record it
    /// compensates, in its transaction.
    pub fn undo_next(&self) -> Result<Lsn> {
        if self.kind != Kind::Clr {
            return Err(StorageError::CorruptLog(format!(
                "{:?} at {} is not a compensation",
                self.kind, self.lsn
            )));
        }
        Ok(self.split_undo_next()?.0)
    }

    fn split_undo_next(&self) -> Result<(Lsn, &'a [u8])> {
        let (undo_next, rest) = self
            .body
            .split_first_chunk::<8>()
            .ok_or_else(|| StorageError::CorruptLog(format!("truncated record at {}", self.lsn)))?;
        Ok((u64::from_le_bytes(*undo_next), rest))
    }
}

/// How to take back one change of a transaction, which its `Update` record
/// carries. A heap change is undone in place, by slot: the transaction
/// keeps the tuple locked until it ends, so nothing else changes it, and a
/// tuple never grows by undoing (see `TableHeap`'s padding rule). An index
/// entry is undone by key, wherever splits have moved it since.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Undo<'a> {
    Heap {
        rid: Rid,
        /// The payload length the tuple's commit keeps, for a change that
        /// gave the tuple its payload: less than the tuple's length when a
        /// shrinking update padded it. `None` for a change that left the
        /// payload as it was.
        keep_len: Option<usize>,
        before: Before<'a>,
    },
    /// Remove `entry`, if it is there, from the B+ tree whose header page
    /// is `header`.
    IndexEntry { header: PageId, entry: &'a [u8] },
    /// A truncate gave a table the storage `new` in place of `old`. Undo
    /// frees every page of `new`; finishing the commit frees `old`'s.
    Truncate { old: Roots<'a>, new: Roots<'a> },
}

/// Where a table's storage starts: the first page of its heap, then the
/// header page of each of its indexes' B+ trees. The rest of its pages
/// are reached from these.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Roots<'a>(&'a [u8]);

impl<'a> Roots<'a> {
    /// The heap's first page, then the trees' header pages.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = PageId> + 'a {
        self.0
            .chunks_exact(size_of::<PageId>())
            .map(|id| PageId::from_le_bytes(id.try_into().unwrap()))
    }
}

/// A heap tuple before the change an `Undo::Heap` takes back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Before<'a> {
    /// The change inserted the tuple.
    Inserted,
    /// The change set only the tuple's meta, which was this.
    Meta(TupleMeta),
    /// The tuple's meta and stored payload, padding included.
    Tuple { meta: TupleMeta, payload: &'a [u8] },
}

const UNDO_HEAP: u8 = 1;
const UNDO_INDEX_ENTRY: u8 = 2;
const UNDO_TRUNCATE: u8 = 3;
const BEFORE_INSERTED: u8 = 1;
const BEFORE_META: u8 = 2;
const BEFORE_TUPLE: u8 = 3;
const KEEP_ALL: u16 = u16::MAX;

impl<'a> Undo<'a> {
    /// Appends the undo to `buf`:
    ///
    /// ```text
    /// heap          1 | page_id i32 | slot u16 | keep_len u16 | before
    /// index entry   2 | header i32 | len u16 | entry
    /// truncate      3 | num u16 | old page_id i32 * num | new page_id i32 * num
    ///
    /// before: 1 (inserted) | 2 meta | 3 meta len u16 payload
    /// ```
    ///
    /// `keep_len` is `u16::MAX` for `None`.
    pub(crate) fn encode(&self, buf: &mut Vec<u8>) {
        let u16_of = |n: usize| u16::try_from(n).expect("larger than a page");
        match *self {
            Self::Heap {
                rid,
                keep_len,
                before,
            } => {
                buf.push(UNDO_HEAP);
                buf.extend_from_slice(&rid.page_id.to_le_bytes());
                buf.extend_from_slice(&rid.slot.to_le_bytes());
                let keep_len = keep_len.map_or(KEEP_ALL, u16_of);
                buf.extend_from_slice(&keep_len.to_le_bytes());
                match before {
                    Before::Inserted => buf.push(BEFORE_INSERTED),
                    Before::Meta(meta) => {
                        buf.push(BEFORE_META);
                        encode_meta(buf, &meta);
                    }
                    Before::Tuple { meta, payload } => {
                        buf.push(BEFORE_TUPLE);
                        encode_meta(buf, &meta);
                        buf.extend_from_slice(&u16_of(payload.len()).to_le_bytes());
                        buf.extend_from_slice(payload);
                    }
                }
            }
            Self::IndexEntry { header, entry } => {
                buf.push(UNDO_INDEX_ENTRY);
                buf.extend_from_slice(&header.to_le_bytes());
                buf.extend_from_slice(&u16_of(entry.len()).to_le_bytes());
                buf.extend_from_slice(entry);
            }
            Self::Truncate { old, new } => Self::encode_truncate(buf, old.iter(), new.iter()),
        }
    }

    /// Appends a `Truncate` from `old` to `new`, each the heap's first page
    /// and then the trees' header pages.
    ///
    /// # Panics
    /// If `old` and `new` differ in length, or are empty.
    pub(crate) fn encode_truncate(
        buf: &mut Vec<u8>,
        old: impl ExactSizeIterator<Item = PageId>,
        new: impl ExactSizeIterator<Item = PageId>,
    ) {
        assert!(
            old.len() > 0 && old.len() == new.len(),
            "a truncate replaces a heap and each of its trees"
        );
        buf.push(UNDO_TRUNCATE);
        let num = u16::try_from(old.len()).expect("a table has few indexes");
        buf.extend_from_slice(&num.to_le_bytes());
        for page_id in old.chain(new) {
            buf.extend_from_slice(&page_id.to_le_bytes());
        }
    }

    /// The undo at the start of `bytes`, and the bytes after it.
    fn decode(bytes: &'a [u8]) -> Result<(Self, &'a [u8])> {
        let mut r = Reader(bytes);
        let undo = match r.u8()? {
            UNDO_HEAP => {
                let rid = Rid::new(r.i32()?, r.u16()?);
                let keep_len = match r.u16()? {
                    KEEP_ALL => None,
                    len => Some(len.into()),
                };
                let before = match r.u8()? {
                    BEFORE_INSERTED => Before::Inserted,
                    BEFORE_META => Before::Meta(r.meta()?),
                    BEFORE_TUPLE => {
                        let meta = r.meta()?;
                        let len = r.u16()?.into();
                        Before::Tuple {
                            meta,
                            payload: r.bytes(len)?,
                        }
                    }
                    other => return Err(corrupt_undo(other)),
                };
                Self::Heap {
                    rid,
                    keep_len,
                    before,
                }
            }
            UNDO_INDEX_ENTRY => {
                let header = r.i32()?;
                let len = r.u16()?.into();
                Self::IndexEntry {
                    header,
                    entry: r.bytes(len)?,
                }
            }
            UNDO_TRUNCATE => {
                let len = usize::from(r.u16()?) * size_of::<PageId>();
                if len == 0 {
                    return Err(corrupt_undo(UNDO_TRUNCATE));
                }
                Self::Truncate {
                    old: Roots(r.bytes(len)?),
                    new: Roots(r.bytes(len)?),
                }
            }
            other => return Err(corrupt_undo(other)),
        };
        Ok((undo, r.0))
    }
}

fn corrupt_undo(tag: u8) -> StorageError {
    StorageError::CorruptLog(format!("unknown undo {tag}"))
}

/// The body of a record that changes pages (`Update`, `Clr`, `Completion`
/// or `System`): how to redo each page it changed, and the pages it freed.
///
/// ```text
/// num_pages u16 | (page_id i32 | form u8 | form body)* | num_frees u32 | page_id i32*
/// ```
///
/// The form says how the page is redone: `IMAGE`, followed by the whole
/// page, or `OPS`, followed by `len u32` and that many bytes of ops (see
/// `op`).
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
    /// Row changes to make, in order, to the page as the records before
    /// left it.
    Ops(Ops<'a>),
}

impl PageRedo<'_> {
    /// Whether redo needs nothing of the page from before this change.
    pub fn replaces_page(&self) -> bool {
        match self {
            Self::Image(_) => true,
            Self::Ops(ops) => ops.replace_page(),
        }
    }
}

const IMAGE: u8 = 1;
const OPS: u8 = 2;
const PAGE_ENTRY_HEADER: usize = 5;

/// The page entry at the start of `bytes`, and the bytes after it.
fn split_page_entry(bytes: &[u8]) -> Option<((PageId, PageRedo<'_>), &[u8])> {
    let (header, rest) = bytes.split_first_chunk::<PAGE_ENTRY_HEADER>()?;
    let page_id = PageId::from_le_bytes(header[..4].try_into().unwrap());
    match header[4] {
        IMAGE => {
            let (image, rest) = rest.split_first_chunk::<PAGE_SIZE>()?;
            Some(((page_id, PageRedo::Image(image)), rest))
        }
        OPS => {
            let (len, rest) = rest.split_first_chunk::<4>()?;
            let (ops, rest) = rest.split_at_checked(u32::from_le_bytes(*len) as usize)?;
            Some(((page_id, PageRedo::Ops(Ops::new(ops))), rest))
        }
        _ => None,
    }
}

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
            let ((_, redo), next) =
                split_page_entry(rest).ok_or_else(|| corrupt("bad page entry"))?;
            // Checked here so redo never applies part of a record.
            if let PageRedo::Ops(ops) = redo {
                ops.iter().try_for_each(|op| op.map(drop))?;
            }
            rest = next;
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
            let (entry, next) = split_page_entry(rest).expect("checked by decode");
            rest = next;
            entry
        })
    }

    pub fn frees(&self) -> impl Iterator<Item = PageId> + 'a {
        self.frees
            .chunks_exact(size_of::<PageId>())
            .map(|id| PageId::from_le_bytes(id.try_into().unwrap()))
    }
}

/// Writes a `PageChanges` body: `image` or `ops` for each page, then
/// `finish` with the frees.
pub(crate) struct PageChangesWriter<'b> {
    body: &'b mut Vec<u8>,
    num_pages_at: usize,
    num_pages: u16,
}

impl<'b> PageChangesWriter<'b> {
    pub(crate) fn new(body: &'b mut Vec<u8>) -> Self {
        let num_pages_at = body.len();
        body.extend_from_slice(&[0; 2]);
        Self {
            body,
            num_pages_at,
            num_pages: 0,
        }
    }

    pub(crate) fn image(&mut self, page_id: PageId, data: &[u8; PAGE_SIZE]) {
        self.entry(page_id, IMAGE);
        self.body.extend_from_slice(data);
    }

    /// Logs the page as its ops, the concatenation of `runs`.
    pub(crate) fn ops<'o>(&mut self, page_id: PageId, runs: impl IntoIterator<Item = &'o [u8]>) {
        self.entry(page_id, OPS);
        let len_at = self.body.len();
        self.body.extend_from_slice(&[0; 4]);
        for run in runs {
            self.body.extend_from_slice(run);
        }
        let len = u32::try_from(self.body.len() - len_at - 4).expect("ops too long");
        self.body[len_at..len_at + 4].copy_from_slice(&len.to_le_bytes());
    }

    pub(crate) fn finish(self, frees: &[PageId]) {
        self.body[self.num_pages_at..self.num_pages_at + 2]
            .copy_from_slice(&self.num_pages.to_le_bytes());
        self.body
            .extend_from_slice(&(frees.len() as u32).to_le_bytes());
        for page_id in frees {
            self.body.extend_from_slice(&page_id.to_le_bytes());
        }
    }

    /// # Panics
    /// If there are more than `u16::MAX` pages.
    fn entry(&mut self, page_id: PageId, form: u8) {
        self.num_pages = self
            .num_pages
            .checked_add(1)
            .expect("too many pages in one record");
        self.body.extend_from_slice(&page_id.to_le_bytes());
        self.body.push(form);
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
