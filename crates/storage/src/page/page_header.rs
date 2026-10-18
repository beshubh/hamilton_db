use std::mem::offset_of;

use common::{INVALID_LSN, Lsn, PageId};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::error::{Result, StorageError};
use crate::page::PAGE_SIZE;

/// What a page holds. Stored in the header as a `u32`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum PageType {
    /// A page that has never been initialized; it reads as all zeroes.
    Invalid = 0,
    Table = 1,
    BTreeInternal = 2,
    BTreeLeaf = 3,
    Directory = 4,
    /// Page 0, which locates the system catalog.
    CatalogHeader = 5,
}

impl TryFrom<u32> for PageType {
    type Error = u32;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        Ok(match value {
            0 => Self::Invalid,
            1 => Self::Table,
            2 => Self::BTreeInternal,
            3 => Self::BTreeLeaf,
            4 => Self::Directory,
            5 => Self::CatalogHeader,
            other => return Err(other),
        })
    }
}

/// The header at the start of every page. Each page format puts its own
/// header right after this one, typically by making `PageHeader` the first
/// field of its header struct.
///
/// The checksum only means something on disk: it is stamped when the page is
/// written out (`stamp_checksum`) and checked when it is read back
/// (`verify_checksum`). While the page is in memory the field is stale.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct PageHeader {
    lsn: Lsn,
    checksum: u32,
    page_type: u32,
}

const _: () = assert!(size_of::<PageHeader>() == PageHeader::SIZE);

const CHECKSUM_OFFSET: usize = offset_of!(PageHeader, checksum);
const CHECKSUM_END: usize = CHECKSUM_OFFSET + size_of::<u32>();

impl PageHeader {
    pub const SIZE: usize = 16;

    pub fn new(page_type: PageType) -> Self {
        Self {
            lsn: INVALID_LSN,
            checksum: 0,
            page_type: page_type as u32,
        }
    }

    /// Writes a fresh header for `page_type` into an in-memory page.
    pub fn init(data: &mut [u8; PAGE_SIZE], page_type: PageType) {
        Self::new(page_type).write_to_prefix(&mut data[..]).unwrap();
    }

    /// The page type, or the raw value if it is not one we know.
    pub fn page_type(&self) -> Result<PageType, u32> {
        PageType::try_from(self.page_type)
    }

    pub fn set_page_type(&mut self, page_type: PageType) {
        self.page_type = page_type as u32;
    }

    pub fn lsn(&self) -> Lsn {
        self.lsn
    }

    pub fn set_lsn(&mut self, lsn: Lsn) {
        self.lsn = lsn;
    }

    /// The checksum as of the last time the page was read from or written to
    /// disk.
    pub fn checksum(&self) -> u32 {
        self.checksum
    }
}

/// CRC-32C of the page with the checksum field left out. The page id is mixed
/// in so a page written to the wrong slot is caught too.
fn compute_checksum(page_id: PageId, data: &[u8; PAGE_SIZE]) -> u32 {
    let crc = crc32c::crc32c(&page_id.to_le_bytes());
    let crc = crc32c::crc32c_append(crc, &data[..CHECKSUM_OFFSET]);
    crc32c::crc32c_append(crc, &data[CHECKSUM_END..])
}

/// Sets the header's checksum. Call on the copy of the page that is about to
/// be written to disk.
pub fn stamp_checksum(page_id: PageId, data: &mut [u8; PAGE_SIZE]) {
    let checksum = compute_checksum(page_id, data);
    data[CHECKSUM_OFFSET..CHECKSUM_END].copy_from_slice(checksum.as_bytes());
}

/// Checks a page just read from disk against its checksum. A page of all
/// zeroes has never been written and is accepted as is.
pub fn verify_checksum(page_id: PageId, data: &[u8; PAGE_SIZE]) -> Result<()> {
    let header = PageHeader::ref_from_prefix(&data[..]).unwrap().0;
    if header.checksum == 0 && data.iter().all(|&b| b == 0) {
        return Ok(());
    }
    let expected = compute_checksum(page_id, data);
    if header.checksum != expected {
        return Err(StorageError::ChecksumMismatch {
            page_id,
            stored: header.checksum,
            expected,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn page() -> Box<[u8; PAGE_SIZE]> {
        Box::new([0u8; PAGE_SIZE])
    }

    #[test]
    fn layout() {
        assert_eq!(size_of::<PageHeader>(), 16);
        assert_eq!(offset_of!(PageHeader, lsn), 0);
        assert_eq!(offset_of!(PageHeader, checksum), 8);
        assert_eq!(offset_of!(PageHeader, page_type), 12);
    }

    #[test]
    fn init_and_read_back() {
        let mut data = page();
        PageHeader::init(&mut data, PageType::BTreeLeaf);
        let header = PageHeader::mut_from_prefix(&mut data[..]).unwrap().0;
        assert_eq!(header.page_type(), Ok(PageType::BTreeLeaf));
        assert_eq!(header.lsn(), INVALID_LSN);

        header.set_lsn(42);
        header.set_page_type(PageType::Table);
        let header = PageHeader::ref_from_prefix(&data[..]).unwrap().0;
        assert_eq!(header.lsn(), 42);
        assert_eq!(header.page_type(), Ok(PageType::Table));
    }

    #[test]
    fn unknown_page_type() {
        let mut data = page();
        data[12..16].copy_from_slice(&99u32.to_ne_bytes());
        let header = PageHeader::ref_from_prefix(&data[..]).unwrap().0;
        assert_eq!(header.page_type(), Err(99));
    }

    #[test]
    fn stamped_page_verifies() {
        let mut data = page();
        PageHeader::init(&mut data, PageType::Table);
        data[100..105].copy_from_slice(b"hello");
        stamp_checksum(3, &mut data);
        assert_ne!(
            PageHeader::ref_from_prefix(&data[..]).unwrap().0.checksum(),
            0
        );
        verify_checksum(3, &data).unwrap();
    }

    #[test]
    fn zeroed_page_verifies() {
        verify_checksum(0, &page()).unwrap();
    }

    #[test]
    fn flipped_bit_is_detected() {
        let mut data = page();
        PageHeader::init(&mut data, PageType::Table);
        stamp_checksum(3, &mut data);
        data[PAGE_SIZE - 1] ^= 1;
        let err = verify_checksum(3, &data).unwrap_err();
        assert!(matches!(
            err,
            StorageError::ChecksumMismatch { page_id: 3, .. }
        ));
    }

    #[test]
    fn corrupted_header_is_detected() {
        let mut data = page();
        PageHeader::init(&mut data, PageType::Table);
        stamp_checksum(3, &mut data);
        data[0] ^= 1;
        assert!(verify_checksum(3, &data).is_err());
    }

    #[test]
    fn wrong_page_id_is_detected() {
        let mut data = page();
        PageHeader::init(&mut data, PageType::Table);
        stamp_checksum(3, &mut data);
        assert!(verify_checksum(4, &data).is_err());
    }

    #[test]
    fn torn_write_is_detected() {
        // The first half of a new version landed, the second half is old.
        let mut old = page();
        PageHeader::init(&mut old, PageType::Table);
        old[3000] = 1;
        stamp_checksum(3, &mut old);

        let mut new = old.clone();
        new[3000] = 2;
        new[100] = 7;
        stamp_checksum(3, &mut new);

        let mut torn = old.clone();
        torn[..PAGE_SIZE / 2].copy_from_slice(&new[..PAGE_SIZE / 2]);
        assert!(verify_checksum(3, &torn).is_err());
    }
}
