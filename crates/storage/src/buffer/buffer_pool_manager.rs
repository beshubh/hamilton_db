use crate::{
    buffer::arc_replacer::{AccessType, ArcReplacer},
    disk::{disk_manager::DiskManager, disk_scheduler::DiskScheduler},
    error::{Result, StorageError},
    page::{
        PageBuf,
        page_guard::{ReadPageGuard, WritePageGuard, flush_page},
        page_header::{stamp_checksum, verify_checksum},
    },
};
use common::{FrameId, INVALID_PAGE_ID, PageId};
use parking_lot::{ArcRwLockReadGuard, ArcRwLockWriteGuard, Mutex, MutexGuard, RawRwLock, RwLock};
use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicI32, AtomicUsize, Ordering},
    },
};

/// Frames in a buffer pool whose user doesn't choose: 64 MB of pages. The
/// pool is allocated up front, so this is what it costs even when the
/// database is smaller.
pub const DEFAULT_POOL_SIZE: usize = 16_384;

pub type FrameReadLatch = ArcRwLockReadGuard<RawRwLock, PageBuf>;
pub type FrameWriteLatch = ArcRwLockWriteGuard<RawRwLock, PageBuf>;

/// One slot of the buffer pool. Frames are shared as `Arc<FrameHeader>`, so
/// everything that changes is either behind the latch (the data) or atomic.
pub struct FrameHeader {
    frame_id: FrameId,
    /// The frame's read/write latch together with the data it protects. It is
    /// in its own `Arc` so page guards can own the lock guard.
    data: Arc<RwLock<PageBuf>>,
    pin_count: AtomicUsize,
    is_dirty: AtomicBool,
    /// Page currently stored in the frame, or `INVALID_PAGE_ID` if empty.
    page_id: AtomicI32,
}

impl FrameHeader {
    pub fn new(frame_id: FrameId) -> Self {
        Self {
            frame_id,
            data: Arc::new(RwLock::new(PageBuf::zeroed())),
            pin_count: AtomicUsize::new(0),
            is_dirty: AtomicBool::new(false),
            page_id: AtomicI32::new(INVALID_PAGE_ID),
        }
    }

    pub fn frame_id(&self) -> FrameId {
        self.frame_id
    }

    pub fn page_id(&self) -> Option<PageId> {
        let page_id = self.page_id.load(Ordering::Acquire);
        (page_id != INVALID_PAGE_ID).then_some(page_id)
    }

    pub fn set_page_id(&self, page_id: Option<PageId>) {
        self.page_id
            .store(page_id.unwrap_or(INVALID_PAGE_ID), Ordering::Release);
    }

    pub fn pin_count(&self) -> usize {
        self.pin_count.load(Ordering::Acquire)
    }

    /// Increments the pin count and returns the new value.
    pub fn pin(&self) -> usize {
        self.pin_count.fetch_add(1, Ordering::AcqRel) + 1
    }

    /// Decrements the pin count and returns the new value, or `None` if the
    /// frame was not pinned.
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

    /// Takes the frame latch in shared mode. The returned guard keeps the data
    /// alive on its own, so it can be stored in a page guard.
    pub fn get_data(&self) -> FrameReadLatch {
        self.data.read_arc()
    }

    /// Takes the frame latch in exclusive mode. This does not mark the frame
    /// dirty; callers that change the data should call `set_dirty(true)`.
    pub fn get_data_mut(&self) -> FrameWriteLatch {
        self.data.write_arc()
    }

    pub fn try_get_data(&self) -> Option<FrameReadLatch> {
        self.data.try_read_arc()
    }

    pub fn try_get_data_mut(&self) -> Option<FrameWriteLatch> {
        self.data.try_write_arc()
    }

    /// Zeroes the data and clears all metadata so the frame can be reused.
    /// Waits for the write latch, so no guard can be looking at the data.
    pub fn reset(&self) {
        let mut data = self.data.write();
        data.fill(0);
        self.pin_count.store(0, Ordering::Release);
        self.is_dirty.store(false, Ordering::Release);
        self.page_id.store(INVALID_PAGE_ID, Ordering::Release);
    }
}

/// The part of the buffer pool that changes as pages move in and out of
/// memory. Only touched while holding the buffer pool latch.
#[derive(Default)]
pub(crate) struct BufferPoolManagerState {
    page_table: HashMap<PageId, FrameId>,
    free_frames: VecDeque<FrameId>,
}

/// The buffer pool latch. Page guards hold a clone so they can unpin under it.
pub(crate) type BpmLatch = Arc<Mutex<BufferPoolManagerState>>;

/// Caches disk pages in a fixed number of in-memory frames.
///
/// Callers get pages through page guards, which keep the frame pinned and
/// latched for as long as they live. When a page is needed and no frame is
/// free, the ARC replacer picks an unpinned frame to evict, writing it back
/// first if it is dirty.
///
/// Lock order is always buffer pool latch, then frame latch. Page guards
/// release their frame latch before taking the buffer pool latch to unpin.
///
/// The buffer pool latch is never held during disk I/O. A miss holds only the
/// frame latch while it writes back the victim and reads the new page, so
/// threads that want other pages are not held up (see `pin_page`).
pub struct BufferPoolManager {
    num_frames: usize,
    next_page_id: AtomicI32,
    frames: Vec<Arc<FrameHeader>>,
    latch: BpmLatch,
    replacer: Arc<ArcReplacer>,
    disk_scheduler: Arc<DiskScheduler>,
}

impl BufferPoolManager {
    /// Page ids continue after the highest page already in `disk_manager`'s
    /// db file, so reopening a database never hands out a page id in use.
    pub fn new(num_frames: usize, disk_manager: Arc<DiskManager>) -> Self {
        let frames = (0..num_frames)
            .map(|i| Arc::new(FrameHeader::new(i as FrameId)))
            .collect();
        let state = BufferPoolManagerState {
            page_table: HashMap::new(),
            free_frames: (0..num_frames as FrameId).collect(),
        };
        let next_page_id = disk_manager.next_page_id();
        Self {
            num_frames,
            next_page_id: AtomicI32::new(next_page_id),
            frames,
            latch: Arc::new(Mutex::new(state)),
            replacer: Arc::new(ArcReplacer::new(num_frames)),
            disk_scheduler: Arc::new(DiskScheduler::new(disk_manager)),
        }
    }

    /// Number of frames in the pool.
    pub fn size(&self) -> usize {
        self.num_frames
    }

    /// Allocates a new page id. Space on disk is allocated lazily by the disk
    /// manager, and the page is only brought into memory when first accessed.
    pub fn new_page(&self) -> PageId {
        self.next_page_id.fetch_add(1, Ordering::AcqRel)
    }

    /// Removes a page from the buffer pool and frees it on disk. Fails with
    /// `PagePinned` if a guard holds the page. A page that is not in memory is
    /// still freed on disk.
    pub fn delete_page(&self, page_id: PageId) -> Result<()> {
        {
            let mut state = self.latch.lock();
            if let Some(&frame_id) = state.page_table.get(&page_id) {
                let frame = self.frame(frame_id);
                if frame.pin_count() > 0 {
                    return Err(StorageError::PagePinned(page_id));
                }
                state.page_table.remove(&page_id);
                self.replacer.remove(frame_id);
                frame.reset();
                state.free_frames.push_back(frame_id);
            }
        }
        self.disk_scheduler.deallocate_page(page_id).wait()
    }

    /// Returns a shared guard on the page, bringing it into memory if needed.
    /// Fails with `NoFreeFrame` if every frame is pinned, or `InvalidPageId`
    /// if the page id was never allocated.
    pub fn checked_read_page(
        &self,
        page_id: PageId,
        access_type: AccessType,
    ) -> Result<ReadPageGuard> {
        loop {
            let frame = self.pin_page(page_id, access_type)?;
            let guard = ReadPageGuard::new(
                page_id,
                Arc::clone(&frame),
                Arc::clone(&self.replacer),
                Arc::clone(&self.latch),
                Arc::clone(&self.disk_scheduler),
            );
            // The load we waited on failed and left the frame empty; dropping
            // the guard unpins it, and the next try reads the page itself.
            if frame.page_id() == Some(page_id) {
                return Ok(guard);
            }
        }
    }

    /// Returns an exclusive guard on the page, bringing it into memory if
    /// needed. Fails with `NoFreeFrame` if every frame is pinned, or
    /// `InvalidPageId` if the page id was never allocated.
    pub fn checked_write_page(
        &self,
        page_id: PageId,
        access_type: AccessType,
    ) -> Result<WritePageGuard> {
        loop {
            let frame = self.pin_page(page_id, access_type)?;
            let guard = WritePageGuard::new(
                page_id,
                Arc::clone(&frame),
                Arc::clone(&self.replacer),
                Arc::clone(&self.latch),
                Arc::clone(&self.disk_scheduler),
            );
            // See `checked_read_page`.
            if frame.page_id() == Some(page_id) {
                return Ok(guard);
            }
        }
    }

    /// Like `checked_read_page`, for callers that know the page can be loaded.
    ///
    /// # Panics
    /// If no frame is available, the page id is invalid, or the read fails.
    pub fn read_page(&self, page_id: PageId, access_type: AccessType) -> ReadPageGuard {
        self.checked_read_page(page_id, access_type)
            .unwrap_or_else(|e| panic!("cannot fetch page {page_id}: {e}"))
    }

    /// Like `checked_write_page`, for callers that know the page can be loaded.
    ///
    /// # Panics
    /// If no frame is available, the page id is invalid, or the read fails.
    pub fn write_page(&self, page_id: PageId, access_type: AccessType) -> WritePageGuard {
        self.checked_write_page(page_id, access_type)
            .unwrap_or_else(|e| panic!("cannot fetch page {page_id}: {e}"))
    }

    /// Writes the page to disk if it is in memory and dirty, then marks it
    /// clean. Takes the page's latch in shared mode, so it waits for any
    /// writer to finish. Returns `false` if the page is not in memory.
    pub fn flush_page(&self, page_id: PageId) -> Result<bool> {
        let frame = {
            let state = self.latch.lock();
            let Some(&frame_id) = state.page_table.get(&page_id) else {
                return Ok(false);
            };
            // Pin without recording an access, so flushing does not count as
            // a use of the page for the replacement policy.
            let frame = self.frame(frame_id);
            frame.pin();
            self.replacer.set_evictable(frame_id, false);
            Arc::clone(frame)
        };
        // The guard waits for the latch outside the buffer pool latch and
        // unpins when it goes out of scope.
        let guard = ReadPageGuard::new(
            page_id,
            Arc::clone(&frame),
            Arc::clone(&self.replacer),
            Arc::clone(&self.latch),
            Arc::clone(&self.disk_scheduler),
        );
        // The page was still loading and its read failed.
        if frame.page_id() != Some(page_id) {
            return Ok(false);
        }
        guard.flush()?;
        Ok(true)
    }

    /// Like `flush_page`, but reads the page without taking its latch.
    /// Returns `false` if the page is not in memory.
    ///
    /// # Safety
    /// No thread may be writing the page (holding a `WritePageGuard` on it)
    /// while this runs; that would be a data race. Meant for shutdown and
    /// tests, when the caller knows the pool is quiet.
    pub unsafe fn flush_page_unsafe(&self, page_id: PageId) -> Result<bool> {
        // Holding the buffer pool latch keeps the frame from being evicted.
        let state = self.latch.lock();
        let Some(&frame_id) = state.page_table.get(&page_id) else {
            return Ok(false);
        };
        let frame = self.frame(frame_id);
        if frame.is_dirty() {
            // SAFETY: the caller guarantees nobody holds the write latch, so
            // nothing is mutating the data while we copy it.
            let mut data = Box::new(unsafe { (*frame.data.data_ptr()).0 });
            stamp_checksum(page_id, &mut data);
            self.disk_scheduler.write_page(page_id, data).wait()?;
            frame.set_dirty(false);
        }
        Ok(true)
    }

    /// Flushes every page currently in memory. Pages evicted while this runs
    /// were already written back by the eviction.
    pub fn flush_all_pages(&self) -> Result<()> {
        let page_ids: Vec<PageId> = self.latch.lock().page_table.keys().copied().collect();
        for page_id in page_ids {
            self.flush_page(page_id)?;
        }
        Ok(())
    }

    /// Pin count of the page, or `None` if it is not in memory.
    pub fn get_pin_count(&self, page_id: PageId) -> Option<usize> {
        let state = self.latch.lock();
        let &frame_id = state.page_table.get(&page_id)?;
        Some(self.frame(frame_id).pin_count())
    }

    fn frame(&self, frame_id: FrameId) -> &Arc<FrameHeader> {
        &self.frames[frame_id as usize]
    }

    /// Finds or loads the page into a frame and pins it. The caller then
    /// takes the frame latch, which waits for a load in progress to finish.
    ///
    /// The buffer pool latch is only held to look up and change the page
    /// table. A miss that picks a dirty victim writes it back first and then
    /// starts over, since the page may have been loaded in the meantime.
    fn pin_page(&self, page_id: PageId, access_type: AccessType) -> Result<Arc<FrameHeader>> {
        if page_id < 0 || page_id >= self.next_page_id.load(Ordering::Acquire) {
            return Err(StorageError::InvalidPageId(page_id));
        }
        loop {
            let mut state = self.latch.lock();

            if let Some(&frame_id) = state.page_table.get(&page_id) {
                let frame = self.frame(frame_id);
                if frame.page_id() == Some(page_id) {
                    frame.pin();
                    self.replacer.record_access(frame_id, page_id, access_type);
                    self.replacer.set_evictable(frame_id, false);
                    return Ok(Arc::clone(frame));
                }
                // A load that failed; its loader has not removed it yet.
                state.page_table.remove(&page_id);
            }

            let Some(frame_id) = state
                .free_frames
                .pop_front()
                .or_else(|| self.replacer.evict())
            else {
                return Err(StorageError::NoFreeFrame);
            };
            if self.frame(frame_id).is_dirty() {
                self.write_back_victim(frame_id, state)?;
                continue;
            }
            return self.load_page(page_id, frame_id, access_type, state);
        }
    }

    /// Writes an evicted dirty frame to disk with only its frame latch held.
    /// The page stays in the page table meanwhile, so nobody can read the
    /// stale copy from disk, and other threads may keep using it. If none
    /// did, the frame goes on the free list; otherwise the page stays.
    fn write_back_victim(
        &self,
        frame_id: FrameId,
        state: MutexGuard<'_, BufferPoolManagerState>,
    ) -> Result<()> {
        let frame = self.frame(frame_id);
        let page_id = frame.page_id().expect("a dirty frame holds a page");
        frame.pin();
        drop(state);

        // Only try the latch: our caller may hold other page latches, and
        // waiting here could deadlock with a thread that holds this one and
        // wants them. A page in use is a poor victim anyway.
        let written = match frame.try_get_data() {
            Some(data) => flush_page(page_id, frame, &data, &self.disk_scheduler),
            None => Ok(()),
        };

        let mut state = self.latch.lock();
        let unused = frame.unpin() == Some(0);
        if unused && !frame.is_dirty() && written.is_ok() {
            state.page_table.remove(&page_id);
            // A thread that used the page meanwhile put it back in the
            // replacer; `remove` ignores it otherwise.
            self.replacer.set_evictable(frame_id, true);
            self.replacer.remove(frame_id);
            frame.reset();
            state.free_frames.push_front(frame_id);
        } else if unused {
            // Keep the page: it changed again, or was not written.
            self.replacer
                .record_access(frame_id, page_id, AccessType::Unknown);
            self.replacer.set_evictable(frame_id, true);
        }
        written
    }

    /// Puts `page_id` in a clean, unpinned frame and reads it from disk.
    ///
    /// The page goes into the page table before the read, with the frame's
    /// write latch held, and the buffer pool latch is released. Threads that
    /// want this page pin it and wait on the frame latch; if the read fails,
    /// they find the frame empty and try again.
    fn load_page(
        &self,
        page_id: PageId,
        frame_id: FrameId,
        access_type: AccessType,
        mut state: MutexGuard<'_, BufferPoolManagerState>,
    ) -> Result<Arc<FrameHeader>> {
        let frame = Arc::clone(self.frame(frame_id));
        if let Some(old_page_id) = frame.page_id() {
            state.page_table.remove(&old_page_id);
        }
        frame.set_page_id(Some(page_id));
        frame.pin();
        state.page_table.insert(page_id, frame_id);
        self.replacer.record_access(frame_id, page_id, access_type);
        self.replacer.set_evictable(frame_id, false);
        // Nobody else had the frame pinned, so the latch is free.
        let mut data = frame.get_data_mut();
        drop(state);

        let read = self
            .disk_scheduler
            .read_page(page_id)
            .wait()
            .and_then(|bytes| verify_checksum(page_id, &bytes).map(|()| bytes));
        let err = match read {
            Ok(bytes) => {
                data.0 = *bytes;
                drop(data);
                return Ok(frame);
            }
            Err(e) => e,
        };

        data.fill(0);
        frame.set_page_id(None);
        drop(data);
        let mut state = self.latch.lock();
        if state.page_table.get(&page_id) == Some(&frame_id) {
            state.page_table.remove(&page_id);
        }
        // Waiters still pinning it unpin it later, and it is evicted as an
        // empty frame.
        if frame.unpin() == Some(0) {
            self.replacer.set_evictable(frame_id, true);
            self.replacer.remove(frame_id);
            state.free_frames.push_back(frame_id);
        }
        Err(err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::PAGE_SIZE;
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;
    use tempfile::TempDir;

    fn setup(num_frames: usize) -> (TempDir, Arc<DiskManager>, BufferPoolManager) {
        let dir = TempDir::new().unwrap();
        let dm = Arc::new(DiskManager::new(dir.path().join("test.db")).unwrap());
        let bpm = BufferPoolManager::new(num_frames, Arc::clone(&dm));
        (dir, dm, bpm)
    }

    fn write_bytes(bpm: &BufferPoolManager, page_id: PageId, bytes: &[u8]) {
        let mut guard = bpm.write_page(page_id, AccessType::Unknown);
        guard.get_data_mut()[..bytes.len()].copy_from_slice(bytes);
    }

    fn read_bytes(bpm: &BufferPoolManager, page_id: PageId, len: usize) -> Vec<u8> {
        bpm.read_page(page_id, AccessType::Unknown).get_data()[..len].to_vec()
    }

    /// Spins until the page has `pins` pins, i.e. a thread got that far.
    fn wait_for_pins(bpm: &BufferPoolManager, page_id: PageId, pins: usize) {
        while bpm.get_pin_count(page_id) != Some(pins) {
            thread::yield_now();
        }
    }

    /// Runs `f` on another thread and fails if it does not finish soon, so a
    /// thread stuck behind paused disk I/O fails the test instead of hanging.
    fn within_timeout(f: impl FnOnce() + Send + 'static) {
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            f();
            let _ = tx.send(());
        });
        rx.recv_timeout(Duration::from_secs(5))
            .expect("did not finish while another page's disk I/O was paused");
    }

    /// Writes `bytes` to a new page on disk and returns a pool over `dm` that
    /// has nothing cached.
    fn page_on_disk(
        dm: &Arc<DiskManager>,
        bpm: BufferPoolManager,
        bytes: &[u8],
    ) -> (PageId, Arc<BufferPoolManager>) {
        let pid = bpm.new_page();
        write_bytes(&bpm, pid, bytes);
        bpm.flush_all_pages().unwrap();
        let frames = bpm.size();
        drop(bpm);
        (
            pid,
            Arc::new(BufferPoolManager::new(frames, Arc::clone(dm))),
        )
    }

    #[test]
    fn new_page_allocates_increasing_ids() {
        let (_dir, _dm, bpm) = setup(2);
        assert_eq!(bpm.size(), 2);
        assert_eq!(bpm.new_page(), 0);
        assert_eq!(bpm.new_page(), 1);
        assert_eq!(bpm.new_page(), 2);
    }

    #[test]
    fn new_page_ids_continue_after_reopen() {
        let (dir, dm, bpm) = setup(2);
        for _ in 0..3 {
            let pid = bpm.new_page();
            write_bytes(&bpm, pid, b"x");
        }
        bpm.flush_all_pages().unwrap();
        dm.shut_down().unwrap();
        drop((bpm, dm));

        let dm = Arc::new(DiskManager::new(dir.path().join("test.db")).unwrap());
        let bpm = BufferPoolManager::new(2, dm);
        assert_eq!(bpm.new_page(), 3);
        assert_eq!(read_bytes(&bpm, 2, 1), b"x");
    }

    #[test]
    fn write_then_read() {
        let (_dir, _dm, bpm) = setup(2);
        let pid = bpm.new_page();
        write_bytes(&bpm, pid, b"hello");
        assert_eq!(read_bytes(&bpm, pid, 5), b"hello");
    }

    #[test]
    fn rejects_unallocated_page_ids() {
        let (_dir, _dm, bpm) = setup(2);
        assert!(matches!(
            bpm.checked_read_page(0, AccessType::Unknown),
            Err(StorageError::InvalidPageId(0))
        ));
        assert!(matches!(
            bpm.checked_read_page(-1, AccessType::Unknown),
            Err(StorageError::InvalidPageId(-1))
        ));
    }

    #[test]
    fn evicted_dirty_pages_are_written_back() {
        let (_dir, dm, bpm) = setup(2);
        let pids: Vec<_> = (0..5).map(|_| bpm.new_page()).collect();
        for &pid in &pids {
            write_bytes(&bpm, pid, &[pid as u8; 8]);
        }
        // Only 2 frames, so at least 3 pages were evicted and written.
        assert!(dm.num_writes() >= 3);
        for &pid in &pids {
            assert_eq!(read_bytes(&bpm, pid, 8), [pid as u8; 8]);
        }
    }

    #[test]
    fn fails_when_all_frames_pinned() {
        let (_dir, _dm, bpm) = setup(2);
        let (a, b, c) = (bpm.new_page(), bpm.new_page(), bpm.new_page());
        let _ga = bpm.read_page(a, AccessType::Unknown);
        let gb = bpm.write_page(b, AccessType::Unknown);
        assert!(matches!(
            bpm.checked_read_page(c, AccessType::Unknown),
            Err(StorageError::NoFreeFrame)
        ));

        drop(gb);
        bpm.checked_read_page(c, AccessType::Unknown).unwrap();
    }

    #[test]
    fn pin_count_follows_guards() {
        let (_dir, _dm, bpm) = setup(2);
        let pid = bpm.new_page();
        assert_eq!(bpm.get_pin_count(pid), None);

        let g1 = bpm.read_page(pid, AccessType::Unknown);
        let g2 = bpm.read_page(pid, AccessType::Unknown);
        assert_eq!(bpm.get_pin_count(pid), Some(2));
        drop(g1);
        assert_eq!(bpm.get_pin_count(pid), Some(1));
        drop(g2);
        assert_eq!(bpm.get_pin_count(pid), Some(0));
    }

    #[test]
    fn delete_page() {
        let (_dir, dm, bpm) = setup(1);
        let pid = bpm.new_page();
        let guard = bpm.read_page(pid, AccessType::Unknown);
        assert!(matches!(
            bpm.delete_page(pid),
            Err(StorageError::PagePinned(p)) if p == pid
        ));
        drop(guard);

        bpm.delete_page(pid).unwrap();
        assert_eq!(bpm.get_pin_count(pid), None);
        assert_eq!(dm.num_deletes(), 0); // never written, so it had no slot on disk

        // The freed frame can hold another page.
        let other = bpm.new_page();
        bpm.checked_read_page(other, AccessType::Unknown).unwrap();
    }

    #[test]
    fn flush_page_writes_and_cleans() {
        let (_dir, dm, bpm) = setup(2);
        let pid = bpm.new_page();
        assert!(!bpm.flush_page(pid).unwrap());

        write_bytes(&bpm, pid, b"data");
        assert!(bpm.flush_page(pid).unwrap());
        assert_eq!(dm.num_writes(), 1);
        assert_eq!(bpm.get_pin_count(pid), Some(0));

        // Clean now, so a second flush writes nothing.
        assert!(bpm.flush_page(pid).unwrap());
        assert_eq!(dm.num_writes(), 1);

        let mut buf = [0u8; PAGE_SIZE];
        dm.read_page(pid, &mut buf).unwrap();
        assert_eq!(&buf[..4], b"data");
    }

    #[test]
    fn flush_page_unsafe_writes_dirty_page() {
        let (_dir, dm, bpm) = setup(2);
        let pid = bpm.new_page();
        write_bytes(&bpm, pid, b"raw");
        // SAFETY: no other thread is using the pool.
        assert!(unsafe { bpm.flush_page_unsafe(pid) }.unwrap());
        assert!(!unsafe { bpm.flush_page_unsafe(pid + 1) }.unwrap());

        let mut buf = [0u8; PAGE_SIZE];
        dm.read_page(pid, &mut buf).unwrap();
        assert_eq!(&buf[..3], b"raw");
    }

    #[test]
    fn corrupted_page_fails_to_load() {
        let (_dir, dm, bpm) = setup(1);
        let pid = bpm.new_page();
        let other = bpm.new_page();
        write_bytes(&bpm, pid, b"data");
        bpm.flush_page(pid).unwrap();

        let mut buf = [0u8; PAGE_SIZE];
        dm.read_page(pid, &mut buf).unwrap();
        verify_checksum(pid, &buf).unwrap();
        buf[PAGE_SIZE - 1] ^= 1;
        dm.write_page(pid, &buf).unwrap();

        // Evict `pid` so the next access reads it from disk.
        drop(bpm.read_page(other, AccessType::Unknown));
        let err = bpm
            .checked_read_page(pid, AccessType::Unknown)
            .err()
            .expect("corruption should be detected");
        assert!(matches!(
            err,
            StorageError::ChecksumMismatch { page_id, .. } if page_id == pid
        ));
    }

    #[test]
    fn flush_all_pages() {
        let (_dir, dm, bpm) = setup(4);
        let pids: Vec<_> = (0..3).map(|_| bpm.new_page()).collect();
        for &pid in &pids {
            write_bytes(&bpm, pid, &[pid as u8 + 1; 4]);
        }
        bpm.flush_all_pages().unwrap();
        assert_eq!(dm.num_writes(), 3);

        let mut buf = [0u8; PAGE_SIZE];
        for &pid in &pids {
            dm.read_page(pid, &mut buf).unwrap();
            assert_eq!(&buf[..4], [pid as u8 + 1; 4]);
        }
    }

    #[test]
    fn concurrent_access_with_eviction() {
        let (_dir, _dm, bpm) = setup(4);
        let bpm = Arc::new(bpm);
        let pids: Vec<_> = (0..16).map(|_| bpm.new_page()).collect();

        let handles: Vec<_> = (0..4)
            .map(|t| {
                let bpm = Arc::clone(&bpm);
                let pids = pids.clone();
                thread::spawn(move || {
                    for round in 0..50u8 {
                        let pid = pids[(t * 4 + round as usize) % pids.len()];
                        let mut guard = bpm.write_page(pid, AccessType::Unknown);
                        let counter = guard.as_mut::<u64>();
                        *counter += 1;
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }

        let total: u64 = pids
            .iter()
            .map(|&pid| *bpm.read_page(pid, AccessType::Unknown).as_ref::<u64>())
            .sum();
        assert_eq!(total, 4 * 50);
        for &pid in &pids {
            assert_eq!(bpm.get_pin_count(pid).unwrap_or(0), 0);
        }
    }

    #[test]
    fn miss_does_not_block_other_pages() {
        let (_dir, dm, bpm) = setup(2);
        let bpm = Arc::new(bpm);
        let (cached, missing) = (bpm.new_page(), bpm.new_page());
        drop(bpm.read_page(cached, AccessType::Unknown));

        let io = dm.pause_io();
        let loader = {
            let bpm = Arc::clone(&bpm);
            thread::spawn(move || drop(bpm.read_page(missing, AccessType::Unknown)))
        };
        let other = Arc::clone(&bpm);
        within_timeout(move || {
            wait_for_pins(&other, missing, 1); // the loader is in its read
            drop(other.read_page(cached, AccessType::Unknown));
        });
        drop(io);
        loader.join().unwrap();
    }

    #[test]
    fn threads_wanting_a_loading_page_share_its_read() {
        let (_dir, dm, bpm) = setup(2);
        let (pid, bpm) = page_on_disk(&dm, bpm, b"shared");
        let reads = dm.num_reads();

        let io = dm.pause_io();
        let readers: Vec<_> = (1..=2)
            .map(|pins| {
                let reader = Arc::clone(&bpm);
                let handle = thread::spawn(move || read_bytes(&reader, pid, 6));
                let bpm = Arc::clone(&bpm);
                within_timeout(move || wait_for_pins(&bpm, pid, pins));
                handle
            })
            .collect();
        drop(io);

        for reader in readers {
            assert_eq!(reader.join().unwrap(), b"shared");
        }
        assert_eq!(dm.num_reads(), reads + 1);
    }

    #[test]
    fn failed_load_lets_waiters_retry() {
        let (_dir, dm, bpm) = setup(2);
        let (pid, bpm) = page_on_disk(&dm, bpm, b"data");
        let mut buf = [0u8; PAGE_SIZE];
        dm.read_page(pid, &mut buf).unwrap();
        buf[PAGE_SIZE - 1] ^= 1;
        dm.write_page(pid, &buf).unwrap();

        let io = dm.pause_io();
        let readers: Vec<_> = (1..=2)
            .map(|pins| {
                let reader = Arc::clone(&bpm);
                let handle = thread::spawn(move || {
                    matches!(
                        reader.checked_read_page(pid, AccessType::Unknown),
                        Err(StorageError::ChecksumMismatch { .. })
                    )
                });
                let bpm = Arc::clone(&bpm);
                within_timeout(move || wait_for_pins(&bpm, pid, pins));
                handle
            })
            .collect();
        drop(io);

        for reader in readers {
            assert!(reader.join().unwrap(), "expected a checksum mismatch");
        }
        assert_eq!(bpm.get_pin_count(pid), None);
        // Both frames are usable again.
        let (a, b) = (bpm.new_page(), bpm.new_page());
        let _a = bpm.read_page(a, AccessType::Unknown);
        let _b = bpm.read_page(b, AccessType::Unknown);
    }

    #[test]
    fn dirty_victim_stays_readable_during_write_back() {
        let (_dir, dm, bpm) = setup(1);
        let bpm = Arc::new(bpm);
        let (victim, wanted) = (bpm.new_page(), bpm.new_page());
        write_bytes(&bpm, victim, b"dirty");

        let io = dm.pause_io();
        let loader = {
            let bpm = Arc::clone(&bpm);
            thread::spawn(move || read_bytes(&bpm, wanted, 1))
        };
        let other = Arc::clone(&bpm);
        within_timeout(move || {
            wait_for_pins(&other, victim, 1); // the loader is writing it back
            assert_eq!(read_bytes(&other, victim, 5), b"dirty");
        });
        drop(io);

        assert_eq!(loader.join().unwrap(), [0]);
        assert_eq!(dm.num_writes(), 1);
        // Evicts `wanted` and reads the victim back from disk.
        assert_eq!(read_bytes(&bpm, victim, 5), b"dirty");
    }

    #[test]
    fn concurrent_misses_keep_every_write() {
        let (_dir, _dm, bpm) = setup(4);
        let pids: Vec<_> = (0..64).map(|_| bpm.new_page()).collect();
        let (bpm, pids) = (&bpm, &pids);

        thread::scope(|s| {
            for t in 0..4 {
                s.spawn(move || {
                    for round in 0..500 {
                        let pid = pids[(t * 17 + round * 7) % pids.len()];
                        *bpm.write_page(pid, AccessType::Unknown).as_mut::<u64>() += 1;
                        let other = pids[(t + round * 13) % pids.len()];
                        drop(bpm.read_page(other, AccessType::Unknown));
                    }
                });
            }
        });

        let total: u64 = pids
            .iter()
            .map(|&pid| *bpm.read_page(pid, AccessType::Unknown).as_ref::<u64>())
            .sum();
        assert_eq!(total, 4 * 500);
    }
}
