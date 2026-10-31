/// Identifies a page of the database. Pages are numbered from 0.
pub type PageId = i32;

pub const INVALID_PAGE_ID: PageId = -1;

/// Index of a frame in the buffer pool.
pub type FrameId = i32;

/// Log sequence number: the position of a record in the write-ahead log.
pub type Lsn = u64;

pub const INVALID_LSN: Lsn = 0;

/// Identifies a transaction.
pub type TxnId = u64;

pub const INVALID_TXN_ID: TxnId = 0;

/// Set in a tuple's `ts` while the transaction that wrote it is running: the
/// version is uncommitted and write-locked by the Tid in the lower bits.
/// Tids stay below it.
pub const TXN_UNCOMMITTED_BIT: TxnId = 1 << 63;
