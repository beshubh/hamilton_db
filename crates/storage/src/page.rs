use common::{INVALID_PAGE_ID, PageId};
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

pub mod page_guard;
pub mod page_header;
pub mod btree_internal_page;
pub mod btree_leaf_page;

pub const PAGE_SIZE: usize = 4096;

pub type PageData = Box<[u8; PAGE_SIZE]>;
pub type PageReadGuard<'a> = RwLockReadGuard<'a, PageData>;
pub type PageWriteGuard<'a> = RwLockWriteGuard<'a, PageData>;

/// A page-sized buffer aligned to 8 bytes, so its bytes can be viewed as any
/// on-disk struct with alignment up to 8 (see `ReadPageGuard::as_ref`).
#[derive(Clone)]
#[repr(C, align(8))]
pub struct PageBuf(pub [u8; PAGE_SIZE]);

impl PageBuf {
    pub fn zeroed() -> Self {
        Self([0u8; PAGE_SIZE])
    }
}

impl Default for PageBuf {
    fn default() -> Self {
        Self::zeroed()
    }
}

impl Deref for PageBuf {
    type Target = [u8; PAGE_SIZE];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for PageBuf {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// An in-memory frame in the buffer pool holding one on-disk page.
///
/// The page contents sit behind the read/write latch, so they can only be
/// reached through a guard. The pin count and dirty flag are atomics so they
/// can be updated through a shared reference without taking the latch;
/// changing the page id or resetting the frame needs exclusive access.
pub struct Page {
    data: RwLock<PageData>,
    page_id: PageId,
    pin_count: AtomicUsize,
    is_dirty: AtomicBool,
}

impl Page {
    /// Creates an empty frame that does not hold any page yet.
    pub fn new() -> Self {
        Self::with_id(INVALID_PAGE_ID)
    }

    pub fn with_id(page_id: PageId) -> Self {
        Self {
            data: RwLock::new(Box::new([0u8; PAGE_SIZE])),
            page_id,
            pin_count: AtomicUsize::new(0),
            is_dirty: AtomicBool::new(false),
        }
    }

    pub fn page_id(&self) -> PageId {
        self.page_id
    }

    pub fn set_page_id(&mut self, page_id: PageId) {
        self.page_id = page_id;
    }

    pub fn pin_count(&self) -> usize {
        self.pin_count.load(Ordering::Acquire)
    }

    /// Increments the pin count and returns the new value.
    pub fn pin(&self) -> usize {
        self.pin_count.fetch_add(1, Ordering::AcqRel) + 1
    }

    /// Decrements the pin count and returns the new value, or `None` if the
    /// page was not pinned.
    pub fn unpin(&self) -> Option<usize> {
        self.pin_count
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |c| c.checked_sub(1))
            .ok()
            .map(|prev| prev - 1)
    }

    pub fn is_dirty(&self) -> bool {
        self.is_dirty.load(Ordering::Acquire)
    }

    pub fn set_dirty(&self, dirty: bool) {
        self.is_dirty.store(dirty, Ordering::Release);
    }

    /// Takes the read latch. While it is held, any number of readers can look
    /// at the page and no writer can change it.
    pub fn read(&self) -> PageReadGuard<'_> {
        self.data.read()
    }

    /// Takes the write latch. This does not mark the page dirty; whoever
    /// changes the contents should call `set_dirty(true)`.
    pub fn write(&self) -> PageWriteGuard<'_> {
        self.data.write()
    }

    pub fn try_read(&self) -> Option<PageReadGuard<'_>> {
        self.data.try_read()
    }

    pub fn try_write(&self) -> Option<PageWriteGuard<'_>> {
        self.data.try_write()
    }

    /// Zeroes the data and clears all metadata, leaving an empty frame.
    /// `&mut self` already rules out any outstanding guards, so no latch is taken.
    pub fn reset_memory(&mut self) {
        self.data.get_mut().fill(0);
        self.page_id = INVALID_PAGE_ID;
        self.pin_count.store(0, Ordering::Release);
        self.is_dirty.store(false, Ordering::Release);
    }
}

impl Default for Page {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn new_page_is_empty() {
        let page = Page::new();
        assert_eq!(page.page_id(), INVALID_PAGE_ID);
        assert_eq!(page.pin_count(), 0);
        assert!(!page.is_dirty());
        assert!(page.read().iter().all(|&b| b == 0));
    }

    #[test]
    fn pin_and_unpin() {
        let page = Page::with_id(7);
        assert_eq!(page.pin(), 1);
        assert_eq!(page.pin(), 2);
        assert_eq!(page.unpin(), Some(1));
        assert_eq!(page.unpin(), Some(0));
        assert_eq!(page.unpin(), None);
        assert_eq!(page.pin_count(), 0);
    }

    #[test]
    fn write_then_read() {
        let page = Page::with_id(1);
        {
            let mut data = page.write();
            data[..5].copy_from_slice(b"hello");
            page.set_dirty(true);
        }
        assert_eq!(&page.read()[..5], b"hello");
        assert!(page.is_dirty());
    }

    #[test]
    fn latch_excludes_writer_while_reading() {
        let page = Page::new();
        let r1 = page.read();
        let r2 = page.try_read();
        assert!(r2.is_some());
        assert!(page.try_write().is_none());
        drop(r1);
        drop(r2);
        assert!(page.try_write().is_some());
    }

    #[test]
    fn reset_clears_state() {
        let mut page = Page::with_id(3);
        page.pin();
        page.set_dirty(true);
        page.write()[0] = 0xff;

        page.reset_memory();
        assert_eq!(page.page_id(), INVALID_PAGE_ID);
        assert_eq!(page.pin_count(), 0);
        assert!(!page.is_dirty());
        assert_eq!(page.read()[0], 0);
    }

    #[test]
    fn set_page_id() {
        let mut page = Page::new();
        page.set_page_id(42);
        assert_eq!(page.page_id(), 42);
    }

    #[test]
    fn concurrent_pins() {
        let page = Arc::new(Page::new());
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let page = Arc::clone(&page);
                thread::spawn(move || {
                    for _ in 0..1000 {
                        page.pin();
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(page.pin_count(), 8000);
    }
}
