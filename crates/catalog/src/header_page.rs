//! Page 0 of the database, which locates the system catalog and keeps the
//! next transaction id.
//!
//! ```text
//! +----------------------------------------------------------------+
//! | PageHeader (lsn, checksum, page_type)                     16 B |
//! | CatalogHeader (magic, version, first page of each system       |
//! |                table: tables, columns, indexes, index_columns, |
//! |                next_tid)                                  36 B |
//! +----------------------------------------------------------------+
//! |                            unused                              |
//! +----------------------------------------------------------------+
//! ```
//!
//! All multi-byte fields are little-endian and unaligned.

use common::{PageId, TxnId};
use storage::page::PAGE_SIZE;
use storage::page::page_header::{PageHeader, PageType};
use zerocopy::byteorder::little_endian::{I32, U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::error::{CatalogError, Result};

/// The page that holds the catalog header. It is the first page a new
/// database allocates.
pub const CATALOG_PAGE_ID: PageId = 0;

const MAGIC: u64 = u64::from_le_bytes(*b"HAMDBCAT");
/// Bump when the on-disk format changes, so old files fail to open instead
/// of being misread. 3: secondary indexes store the primary key, on tables
/// that have one. 4: the header keeps the next transaction id. 5: primary
/// key index entries hold the rid after the key.
const VERSION: u32 = 5;

/// The Tid a new database's first transaction gets. 0 is `INVALID_TXN_ID`,
/// the `ts` of rows written outside transactions.
const FIRST_TID: TxnId = 1;

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct CatalogHeader {
    magic: U64,
    version: U32,
    tables: I32,
    columns: I32,
    indexes: I32,
    index_columns: I32,
    /// Above the `ts` of every committed version on disk; see `watermark`.
    next_tid: U64,
}

const _: () = assert!(size_of::<CatalogHeader>() == 36);

/// The first page of each system table's heap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SystemTableRoots {
    pub tables: PageId,
    pub columns: PageId,
    pub indexes: PageId,
    pub index_columns: PageId,
}

impl SystemTableRoots {
    /// Formats `data` as the catalog header page pointing at these tables,
    /// for a new database.
    pub fn write_to(&self, data: &mut [u8; PAGE_SIZE]) {
        PageHeader::init(data, PageType::CatalogHeader);
        let header = CatalogHeader {
            magic: MAGIC.into(),
            version: VERSION.into(),
            tables: self.tables.into(),
            columns: self.columns.into(),
            indexes: self.indexes.into(),
            index_columns: self.index_columns.into(),
            next_tid: FIRST_TID.into(),
        };
        header
            .write_to_prefix(&mut data[PageHeader::SIZE..])
            .unwrap();
    }

    /// Reads the roots from the catalog header page, checking that it is one.
    pub fn read_from(data: &[u8; PAGE_SIZE]) -> Result<Self> {
        let (page_header, _) = PageHeader::read_from_prefix(&data[..]).unwrap();
        if page_header.page_type() != Ok(PageType::CatalogHeader) {
            return Err(corrupt("page 0 is not a catalog header page"));
        }
        let (header, _) = CatalogHeader::ref_from_prefix(&data[PageHeader::SIZE..]).unwrap();
        if header.magic.get() != MAGIC {
            return Err(corrupt("bad magic on the catalog header page"));
        }
        if header.version.get() != VERSION {
            return Err(corrupt(format!(
                "unsupported catalog version {}",
                header.version.get()
            )));
        }
        Ok(Self {
            tables: header.tables.get(),
            columns: header.columns.get(),
            indexes: header.indexes.get(),
            index_columns: header.index_columns.get(),
        })
    }
}

/// The next Tid recorded in `data`, a catalog header page `read_from` has
/// accepted.
pub fn read_next_tid(data: &[u8; PAGE_SIZE]) -> TxnId {
    let (header, _) = CatalogHeader::ref_from_prefix(&data[PageHeader::SIZE..]).unwrap();
    header.next_tid.get()
}

/// Records `tid` as the next Tid in `data`, a catalog header page.
pub fn write_next_tid(data: &mut [u8; PAGE_SIZE], tid: TxnId) {
    let (header, _) = CatalogHeader::mut_from_prefix(&mut data[PageHeader::SIZE..]).unwrap();
    header.next_tid = tid.into();
}

fn corrupt(msg: impl Into<String>) -> CatalogError {
    CatalogError::Corrupt(msg.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use storage::page::PageBuf;

    #[test]
    fn write_and_read_back() {
        let roots = SystemTableRoots {
            tables: 1,
            columns: 2,
            indexes: 3,
            index_columns: 4,
        };
        let mut buf = PageBuf::zeroed();
        roots.write_to(&mut buf);
        assert_eq!(SystemTableRoots::read_from(&buf).unwrap(), roots);
        assert_eq!(read_next_tid(&buf), FIRST_TID);

        write_next_tid(&mut buf, 42);
        assert_eq!(read_next_tid(&buf), 42);
        assert_eq!(SystemTableRoots::read_from(&buf).unwrap(), roots);
    }

    #[test]
    fn rejects_other_pages() {
        let buf = PageBuf::zeroed();
        assert!(matches!(
            SystemTableRoots::read_from(&buf),
            Err(CatalogError::Corrupt(_))
        ));

        let mut buf = PageBuf::zeroed();
        PageHeader::init(&mut buf, PageType::CatalogHeader);
        assert!(matches!(
            SystemTableRoots::read_from(&buf),
            Err(CatalogError::Corrupt(_))
        ));
    }
}
