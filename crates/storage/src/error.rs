use std::io;
use std::path::PathBuf;

use common::{PageId, SlotId};
use thiserror::Error;

use crate::page::page_header::PageType;

pub type Result<T, E = StorageError> = std::result::Result<T, E>;

/// Everything that can go wrong in the storage layer.
#[derive(Debug, Error)]
pub enum StorageError {
    /// The operating system failed a read, write, sync or open.
    #[error(transparent)]
    Io(#[from] io::Error),

    /// The page id is negative, or was never handed out by the buffer pool.
    #[error("invalid page id {0}")]
    InvalidPageId(PageId),

    /// The page has no slot in the db file.
    #[error("page {0} is not allocated")]
    PageNotAllocated(PageId),

    /// The page cannot be deleted while a guard holds it.
    #[error("page {0} is pinned")]
    PagePinned(PageId),

    /// Every frame in the buffer pool is pinned, so nothing can be evicted.
    #[error("no free frame in the buffer pool")]
    NoFreeFrame,

    #[error("free space {0} is larger than a page")]
    InvalidFreeSpace(usize),

    /// A page read from disk does not match its checksum.
    #[error("page {page_id} is corrupted: checksum is {stored:#010x}, expected {expected:#010x}")]
    ChecksumMismatch {
        page_id: PageId,
        stored: u32,
        expected: u32,
    },

    /// The db file has data but its `.meta` file is gone.
    #[error("{} is missing", .0.display())]
    MissingMetadata(PathBuf),

    #[error("corrupt metadata file: {0}")]
    CorruptMetadata(String),

    #[error("unsupported metadata version {0}")]
    UnsupportedMetadataVersion(u32),

    /// The page's header names a different page type than the caller expects.
    #[error("expected a {expected:?} page, found page type {found}")]
    UnexpectedPageType { expected: PageType, found: u32 },

    /// The page does not have room for the tuple right now.
    #[error("page is full")]
    PageFull,

    /// The index already holds the key.
    #[error("duplicate key")]
    DuplicateKey,

    /// The tuple is too large to fit on any page.
    #[error("tuple of {size} bytes is larger than the maximum of {max}")]
    TupleTooLarge { size: usize, max: usize },

    /// The slot is past the end of the slot array, or holds no tuple.
    #[error("invalid slot {0}")]
    InvalidSlot(SlotId),

    /// The disk scheduler's worker thread exited before finishing a request.
    #[error("disk scheduler worker exited")]
    DiskSchedulerShutDown,
}
