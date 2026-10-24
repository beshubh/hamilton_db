//! Page holding rows an operator writes out while it runs, such as the
//! partitions of a hash join.
//!
//! ```text
//! +----------------------------------------------------------------+
//! | PageHeader (lsn, checksum, page_type)                     16 B |
//! | IntermediateHeader (next_page_id, tuple_count,                 |
//! |                     free_space_end)                        8 B |
//! +----------------------------------------------------------------+
//! | slot array, grows toward the end →  [off | len] ...            |
//! +----------------------------------------------------------------+
//! |                          free space                            |
//! +----------------------------------------------------------------+
//! |                   ← tuples, grow toward the start              |
//! +----------------------------------------------------------------+
//! ```
//!
//! Tuples are only appended: they are never updated or deleted, and the page
//! is freed once the operator is done with it. `free_space_end` is where the
//! tuple area starts. The pages of one result are linked by `next_page_id`.
//!
//! All multi-byte fields are little-endian and unaligned, so a page can be
//! viewed from any byte buffer.

use std::ops::{Deref, DerefMut};

use common::{INVALID_PAGE_ID, PageId};
use zerocopy::byteorder::little_endian::{I32, U16};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::error::{Result, StorageError};
use crate::page::PAGE_SIZE;
use crate::page::page_header::{PageHeader, PageType};

const _: () = assert!(PAGE_SIZE <= u16::MAX as usize);

const HEADER_OFFSET: usize = PageHeader::SIZE;
const SLOTS_OFFSET: usize = HEADER_OFFSET + size_of::<IntermediateHeader>();
const SLOT_SIZE: usize = size_of::<TupleSlot>();

/// The largest tuple a page holds: what fits in an empty page.
pub const MAX_TUPLE_SIZE: usize = PAGE_SIZE - SLOTS_OFFSET - SLOT_SIZE;

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct IntermediateHeader {
    next_page_id: I32,
    tuple_count: U16,
    free_space_end: U16,
}

const _: () = assert!(size_of::<IntermediateHeader>() == 8);

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct TupleSlot {
    offset: U16,
    len: U16,
}

const _: () = assert!(size_of::<TupleSlot>() == 4);

/// A view of an intermediate result page over borrowed page bytes, such as
/// `guard.get_data()` (read-only) or `guard.get_data_mut()` (read-write).
pub struct IntermediateResultPage<B> {
    data: B,
}

impl<B: Deref<Target = [u8; PAGE_SIZE]>> IntermediateResultPage<B> {
    /// Views `data` as an intermediate result page, checking its page type.
    pub fn new(data: B) -> Result<Self> {
        let (header, _) = PageHeader::read_from_prefix(&data[..]).unwrap();
        match header.page_type() {
            Ok(PageType::IntermediateResult) => Ok(Self { data }),
            Ok(other) => Err(StorageError::UnexpectedPageType {
                expected: PageType::IntermediateResult,
                found: other as u32,
            }),
            Err(found) => Err(StorageError::UnexpectedPageType {
                expected: PageType::IntermediateResult,
                found,
            }),
        }
    }

    /// The next page of the result, or `INVALID_PAGE_ID` if this is the last.
    pub fn next_page_id(&self) -> PageId {
        self.header().next_page_id.get()
    }

    pub fn tuple_count(&self) -> usize {
        self.header().tuple_count.get() as usize
    }

    /// Bytes left for new tuples and their slots.
    pub fn free_space(&self) -> usize {
        self.header().free_space_end.get() as usize - self.slots_end()
    }

    /// Whether a tuple of `len` bytes fits, along with its slot.
    pub fn has_room_for(&self, len: usize) -> bool {
        SLOT_SIZE + len <= self.free_space()
    }

    /// The tuple at `index`, in the order they were inserted, or `None` if
    /// the page has fewer tuples.
    pub fn get_tuple(&self, index: usize) -> Option<&[u8]> {
        if index >= self.tuple_count() {
            return None;
        }
        let slot = self.slot(index);
        Some(&self.data[slot.offset.get() as usize..][..slot.len.get() as usize])
    }

    /// The tuples in the order they were inserted.
    pub fn tuples(&self) -> impl Iterator<Item = &[u8]> {
        (0..self.tuple_count()).map(|i| self.get_tuple(i).unwrap())
    }

    fn slots_end(&self) -> usize {
        SLOTS_OFFSET + self.tuple_count() * SLOT_SIZE
    }

    fn header(&self) -> &IntermediateHeader {
        IntermediateHeader::ref_from_bytes(&self.data[HEADER_OFFSET..SLOTS_OFFSET]).unwrap()
    }

    fn slot(&self, index: usize) -> &TupleSlot {
        let start = SLOTS_OFFSET + index * SLOT_SIZE;
        TupleSlot::ref_from_bytes(&self.data[start..start + SLOT_SIZE]).unwrap()
    }
}

impl<B: DerefMut<Target = [u8; PAGE_SIZE]>> IntermediateResultPage<B> {
    /// Formats `data` as an empty page with no next page.
    pub fn init(mut data: B) -> Self {
        PageHeader::init(&mut data, PageType::IntermediateResult);
        let header = IntermediateHeader {
            next_page_id: INVALID_PAGE_ID.into(),
            tuple_count: 0.into(),
            free_space_end: (PAGE_SIZE as u16).into(),
        };
        header
            .write_to(&mut data[HEADER_OFFSET..SLOTS_OFFSET])
            .unwrap();
        Self { data }
    }

    pub fn set_next_page_id(&mut self, page_id: PageId) {
        self.header_mut().next_page_id = page_id.into();
    }

    /// Appends `tuple` and returns its index. Fails with `PageFull` if it
    /// does not fit, or `TupleTooLarge` if it would not fit in any page.
    pub fn insert_tuple(&mut self, tuple: &[u8]) -> Result<usize> {
        if tuple.len() > MAX_TUPLE_SIZE {
            return Err(StorageError::TupleTooLarge {
                size: tuple.len(),
                max: MAX_TUPLE_SIZE,
            });
        }
        if !self.has_room_for(tuple.len()) {
            return Err(StorageError::PageFull);
        }

        let index = self.tuple_count();
        let slot_start = self.slots_end();
        let offset = self.header().free_space_end.get() as usize - tuple.len();
        self.data[offset..offset + tuple.len()].copy_from_slice(tuple);
        let slot = TupleSlot {
            offset: (offset as u16).into(),
            len: (tuple.len() as u16).into(),
        };
        slot.write_to(&mut self.data[slot_start..slot_start + SLOT_SIZE])
            .unwrap();

        let header = self.header_mut();
        header.tuple_count += 1;
        header.free_space_end = (offset as u16).into();
        Ok(index)
    }

    fn header_mut(&mut self) -> &mut IntermediateHeader {
        IntermediateHeader::mut_from_bytes(&mut self.data[HEADER_OFFSET..SLOTS_OFFSET]).unwrap()
    }
}
