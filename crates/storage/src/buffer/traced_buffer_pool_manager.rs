use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use common::PageId;
use parking_lot::Mutex;

use crate::buffer::arc_replacer::AccessType;
use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::error::Result;
use crate::page::page_guard::{ReadPageGuard, WritePageGuard};

/// One page operation that went through a `TracedBufferPoolManager`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageAccess {
    New(PageId),
    Delete(PageId),
    Read(PageId),
    Write(PageId),
}

/// A buffer pool that can record which pages its caller touches, and how.
///
/// The B+tree goes through one so tests can check what an operation latched:
/// that a lookup reads one page per level and writes none, or that a delete
/// leaves the siblings of a leaf alone. Recording is off until
/// `start_tracing`; until then every call just forwards to the buffer pool.
///
/// Only operations that succeed are recorded, in the order they returned.
pub struct TracedBufferPoolManager {
    bpm: Arc<BufferPoolManager>,
    tracing: AtomicBool,
    trace: Mutex<Vec<PageAccess>>,
}

impl TracedBufferPoolManager {
    pub fn new(bpm: Arc<BufferPoolManager>) -> Self {
        Self {
            bpm,
            tracing: AtomicBool::new(false),
            trace: Mutex::new(Vec::new()),
        }
    }

    /// The buffer pool underneath. Calls made on it directly are not traced.
    pub fn bpm(&self) -> &Arc<BufferPoolManager> {
        &self.bpm
    }

    /// Starts recording page operations. Does not clear what was recorded
    /// before.
    pub fn start_tracing(&self) {
        self.tracing.store(true, Ordering::Release);
    }

    pub fn stop_tracing(&self) {
        self.tracing.store(false, Ordering::Release);
    }

    pub fn is_tracing(&self) -> bool {
        self.tracing.load(Ordering::Acquire)
    }

    /// The operations recorded so far, oldest first, leaving the trace empty.
    pub fn take_trace(&self) -> Vec<PageAccess> {
        std::mem::take(&mut *self.trace.lock())
    }

    /// See `BufferPoolManager::new_page`.
    pub fn new_page(&self) -> PageId {
        let page_id = self.bpm.new_page();
        self.record(PageAccess::New(page_id));
        page_id
    }

    /// See `BufferPoolManager::delete_page`.
    pub fn delete_page(&self, page_id: PageId) -> Result<()> {
        self.bpm.delete_page(page_id)?;
        self.record(PageAccess::Delete(page_id));
        Ok(())
    }

    /// See `BufferPoolManager::checked_read_page`.
    pub fn checked_read_page(
        &self,
        page_id: PageId,
        access_type: AccessType,
    ) -> Result<ReadPageGuard> {
        let guard = self.bpm.checked_read_page(page_id, access_type)?;
        self.record(PageAccess::Read(page_id));
        Ok(guard)
    }

    /// See `BufferPoolManager::checked_write_page`.
    pub fn checked_write_page(
        &self,
        page_id: PageId,
        access_type: AccessType,
    ) -> Result<WritePageGuard> {
        let guard = self.bpm.checked_write_page(page_id, access_type)?;
        self.record(PageAccess::Write(page_id));
        Ok(guard)
    }

    /// Pin count of the page, or `None` if it is not in memory. Not traced.
    pub fn get_pin_count(&self, page_id: PageId) -> Option<usize> {
        self.bpm.get_pin_count(page_id)
    }

    fn record(&self, access: PageAccess) {
        if self.is_tracing() {
            self.trace.lock().push(access);
        }
    }
}
