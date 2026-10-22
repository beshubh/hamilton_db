//! B+tree header page, the fixed entry point of a tree.
//!
//! ```text
//! +----------------------------------------------------------------+
//! | PageHeader (lsn, checksum, page_type)                     16 B |
//! | BtreeHeader (root_page_id)                                 4 B |
//! +----------------------------------------------------------------+
//! |                            unused                              |
//! +----------------------------------------------------------------+
//! ```
//!
//! The root moves when it splits or the tree shrinks, so the catalog records
//! the header page instead, whose id never changes. An empty tree has
//! `INVALID_PAGE_ID` as its root. The root page id is little-endian and
//! nothing is aligned, so a page can be viewed from any byte buffer.

use std::ops::{Deref, DerefMut};

use common::{INVALID_PAGE_ID, PageId};
use zerocopy::byteorder::little_endian::I32;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::error::{Result, StorageError};
use crate::page::PAGE_SIZE;
use crate::page::page_header::{PageHeader, PageType};

const BTREE_HEADER_OFFSET: usize = PageHeader::SIZE;
const BTREE_HEADER_END: usize = BTREE_HEADER_OFFSET + size_of::<BtreeHeader>();

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct BtreeHeader {
    root_page_id: I32,
}

const _: () = assert!(size_of::<BtreeHeader>() == 4);

/// A view of a B+tree header page over borrowed page bytes `BufT`, such as
/// `guard.get_data()` (read-only) or `guard.get_data_mut()` (read-write).
pub struct BtreeHeaderPage<BufT> {
    data: BufT,
}

impl<BufT: Deref<Target = [u8; PAGE_SIZE]>> BtreeHeaderPage<BufT> {
    /// Views `data` as a header page, checking its page type.
    pub fn new(data: BufT) -> Result<Self> {
        let (header, _) = PageHeader::read_from_prefix(&data[..]).unwrap();
        match header.page_type() {
            Ok(PageType::BTreeHeader) => Ok(Self { data }),
            Ok(other) => Err(StorageError::UnexpectedPageType {
                expected: PageType::BTreeHeader,
                found: other as u32,
            }),
            Err(found) => Err(StorageError::UnexpectedPageType {
                expected: PageType::BTreeHeader,
                found,
            }),
        }
    }

    /// The tree's root page, or `INVALID_PAGE_ID` if the tree is empty.
    pub fn root_page_id(&self) -> PageId {
        self.header().root_page_id.get()
    }

    fn header(&self) -> &BtreeHeader {
        BtreeHeader::ref_from_bytes(&self.data[BTREE_HEADER_OFFSET..BTREE_HEADER_END]).unwrap()
    }
}

impl<BufT: DerefMut<Target = [u8; PAGE_SIZE]>> BtreeHeaderPage<BufT> {
    /// Formats `data` as the header page of an empty tree.
    pub fn init(mut data: BufT) -> Self {
        PageHeader::init(&mut data, PageType::BTreeHeader);
        let header = BtreeHeader {
            root_page_id: INVALID_PAGE_ID.into(),
        };
        header
            .write_to(&mut data[BTREE_HEADER_OFFSET..BTREE_HEADER_END])
            .unwrap();
        Self { data }
    }

    pub fn set_root_page_id(&mut self, page_id: PageId) {
        self.header_mut().root_page_id = page_id.into();
    }

    fn header_mut(&mut self) -> &mut BtreeHeader {
        BtreeHeader::mut_from_bytes(&mut self.data[BTREE_HEADER_OFFSET..BTREE_HEADER_END]).unwrap()
    }
}
