use std::sync::Arc;

use common::PageId;
use parking_lot::Mutex;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::{
    buffer::{
        arc_replacer::ArcReplacer,
        buffer_pool_manager::{
            BpmLatch, BufferPoolManagerState, FrameHeader, FrameReadLatch, FrameWriteLatch,
        },
    },
    disk::disk_scheduler::DiskScheduler,
    error::Result,
    page::{PAGE_SIZE, PageBuf, page_header::stamp_checksum},
};

/// Shared, latched access to a page in the buffer pool.
///
/// Holds the frame's latch in shared mode for as long as it lives. Dropping it
/// releases the latch and unpins the frame; when the last pin goes away the
/// frame becomes evictable again.
///
/// The buffer pool must pin the frame before creating the guard. There is no
/// `is_valid` flag as in BusTub: a moved-from guard no longer exists in Rust,
/// so `Drop` runs exactly once per guard.
pub struct ReadPageGuard {
    page_id: PageId,
    frame: Arc<FrameHeader>,
    replacer: Arc<ArcReplacer>,
    bpm_latch: BpmLatch,
    disk_scheduler: Arc<DiskScheduler>,
    /// `Some` until `drop`, which releases it before unpinning.
    latch: Option<FrameReadLatch>,
}

impl ReadPageGuard {
    /// Blocks until the frame's latch can be taken in shared mode.
    pub(crate) fn new(
        page_id: PageId,
        frame: Arc<FrameHeader>,
        replacer: Arc<ArcReplacer>,
        bpm_latch: BpmLatch,
        disk_scheduler: Arc<DiskScheduler>,
    ) -> Self {
        debug_assert!(
            frame.pin_count() > 0,
            "frame must be pinned before guarding it"
        );
        let latch = Some(frame.get_data());
        Self {
            page_id,
            frame,
            replacer,
            bpm_latch,
            disk_scheduler,
            latch,
        }
    }

    pub fn page_id(&self) -> PageId {
        self.page_id
    }

    pub fn get_data(&self) -> &[u8; PAGE_SIZE] {
        self.buf()
    }

    /// Views the start of the page as a `T`, without copying.
    ///
    /// # Panics
    /// If `T` is larger than a page or needs more than 8-byte alignment.
    pub fn as_ref<T: FromBytes + KnownLayout + Immutable>(&self) -> &T {
        view_as(self.buf())
    }

    pub fn is_dirty(&self) -> bool {
        self.frame.is_dirty()
    }

    /// Writes the page to disk if it is dirty, then marks it clean.
    pub fn flush(&self) -> Result<()> {
        flush_page(self.page_id, &self.frame, self.buf(), &self.disk_scheduler)
    }

    fn buf(&self) -> &PageBuf {
        self.latch.as_ref().expect("latch is held until drop")
    }
}

impl Drop for ReadPageGuard {
    fn drop(&mut self) {
        drop(self.latch.take());
        unpin_frame(&self.frame, &self.replacer, &self.bpm_latch);
    }
}

/// Exclusive, latched access to a page in the buffer pool.
///
/// Same lifecycle as `ReadPageGuard`, but holds the frame's latch in exclusive
/// mode. Any mutable access marks the page dirty.
pub struct WritePageGuard {
    page_id: PageId,
    frame: Arc<FrameHeader>,
    replacer: Arc<ArcReplacer>,
    bpm_latch: BpmLatch,
    disk_scheduler: Arc<DiskScheduler>,
    /// `Some` until `drop`, which releases it before unpinning.
    latch: Option<FrameWriteLatch>,
}

impl WritePageGuard {
    /// Blocks until the frame's latch can be taken in exclusive mode.
    pub(crate) fn new(
        page_id: PageId,
        frame: Arc<FrameHeader>,
        replacer: Arc<ArcReplacer>,
        bpm_latch: BpmLatch,
        disk_scheduler: Arc<DiskScheduler>,
    ) -> Self {
        debug_assert!(
            frame.pin_count() > 0,
            "frame must be pinned before guarding it"
        );
        let latch = Some(frame.get_data_mut());
        Self {
            page_id,
            frame,
            replacer,
            bpm_latch,
            disk_scheduler,
            latch,
        }
    }

    pub fn page_id(&self) -> PageId {
        self.page_id
    }

    pub fn get_data(&self) -> &[u8; PAGE_SIZE] {
        self.buf()
    }

    /// Mutable access to the page bytes. Marks the page dirty.
    pub fn get_data_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        self.buf_mut()
    }

    /// Views the start of the page as a `T`, without copying.
    ///
    /// # Panics
    /// If `T` is larger than a page or needs more than 8-byte alignment.
    pub fn as_ref<T: FromBytes + KnownLayout + Immutable>(&self) -> &T {
        view_as(self.buf())
    }

    /// Views the start of the page as a mutable `T`, without copying. Marks
    /// the page dirty.
    ///
    /// # Panics
    /// If `T` is larger than a page or needs more than 8-byte alignment.
    pub fn as_mut<T: FromBytes + IntoBytes + KnownLayout>(&mut self) -> &mut T {
        match T::mut_from_prefix(&mut self.buf_mut()[..]) {
            Ok((value, _rest)) => value,
            Err(_) => panic!("{}", cast_error::<T>()),
        }
    }

    pub fn is_dirty(&self) -> bool {
        self.frame.is_dirty()
    }

    /// Writes the page to disk if it is dirty, then marks it clean.
    pub fn flush(&self) -> Result<()> {
        flush_page(self.page_id, &self.frame, self.buf(), &self.disk_scheduler)
    }

    fn buf(&self) -> &PageBuf {
        self.latch.as_ref().expect("latch is held until drop")
    }

    fn buf_mut(&mut self) -> &mut PageBuf {
        self.frame.set_dirty(true);
        self.latch.as_mut().expect("latch is held until drop")
    }
}

impl Drop for WritePageGuard {
    fn drop(&mut self) {
        drop(self.latch.take());
        unpin_frame(&self.frame, &self.replacer, &self.bpm_latch);
    }
}

fn view_as<T: FromBytes + KnownLayout + Immutable>(buf: &PageBuf) -> &T {
    match T::ref_from_prefix(&buf[..]) {
        Ok((value, _rest)) => value,
        Err(_) => panic!("{}", cast_error::<T>()),
    }
}

fn cast_error<T>() -> String {
    format!(
        "cannot view a page as {} (size {}, align {}): page is {} bytes, aligned to {}",
        std::any::type_name::<T>(),
        size_of::<T>(),
        align_of::<T>(),
        PAGE_SIZE,
        align_of::<PageBuf>(),
    )
}

/// Drops one pin. Done under the buffer pool latch so that "pin count hit 0"
/// and "mark evictable" happen together: otherwise another thread could pin
/// the frame in between and we would mark a pinned frame evictable.
///
/// Called after the frame latch is released, so this never holds the frame
/// latch while waiting for the buffer pool latch.
fn unpin_frame(
    frame: &FrameHeader,
    replacer: &ArcReplacer,
    bpm_latch: &Mutex<BufferPoolManagerState>,
) {
    let _bpm = bpm_latch.lock();
    if frame.unpin() == Some(0) {
        replacer.set_evictable(frame.frame_id(), true);
    }
}

fn flush_page(
    page_id: PageId,
    frame: &FrameHeader,
    data: &PageBuf,
    disk_scheduler: &DiskScheduler,
) -> Result<()> {
    if !frame.is_dirty() {
        return Ok(());
    }
    let mut data = Box::new(data.0);
    stamp_checksum(page_id, &mut data);
    disk_scheduler.write_page(page_id, data).wait()?;
    frame.set_dirty(false);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::arc_replacer::AccessType;
    use crate::disk::disk_manager::DiskManager;
    use tempfile::TempDir;
    use zerocopy::{Immutable, IntoBytes, KnownLayout};

    #[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Debug, PartialEq)]
    #[repr(C)]
    struct Header {
        page_type: u32,
        size: u32,
        next_page_id: i64,
    }

    #[derive(FromBytes, KnownLayout, Immutable)]
    #[repr(C)]
    struct TooBig([u8; PAGE_SIZE + 1]);

    struct Fixture {
        _dir: TempDir,
        frame: Arc<FrameHeader>,
        replacer: Arc<ArcReplacer>,
        bpm_latch: BpmLatch,
        disk_scheduler: Arc<DiskScheduler>,
    }

    const PAGE: PageId = 7;

    impl Fixture {
        fn new() -> Self {
            let dir = TempDir::new().unwrap();
            let dm = Arc::new(DiskManager::new(dir.path().join("test.db")).unwrap());
            let frame = Arc::new(FrameHeader::new(0));
            frame.set_page_id(Some(PAGE));
            let replacer = Arc::new(ArcReplacer::new(4));
            replacer.record_access(0, PAGE, AccessType::Unknown);
            Self {
                _dir: dir,
                frame,
                replacer,
                bpm_latch: Arc::new(Mutex::new(BufferPoolManagerState::default())),
                disk_scheduler: Arc::new(DiskScheduler::new(dm)),
            }
        }

        /// Pins the frame and guards it, as the buffer pool would.
        fn read(&self) -> ReadPageGuard {
            self.frame.pin();
            ReadPageGuard::new(
                PAGE,
                Arc::clone(&self.frame),
                Arc::clone(&self.replacer),
                Arc::clone(&self.bpm_latch),
                Arc::clone(&self.disk_scheduler),
            )
        }

        fn write(&self) -> WritePageGuard {
            self.frame.pin();
            WritePageGuard::new(
                PAGE,
                Arc::clone(&self.frame),
                Arc::clone(&self.replacer),
                Arc::clone(&self.bpm_latch),
                Arc::clone(&self.disk_scheduler),
            )
        }
    }

    #[test]
    fn write_struct_then_read_it_back() {
        let f = Fixture::new();
        {
            let mut guard = f.write();
            let header = guard.as_mut::<Header>();
            header.page_type = 1;
            header.size = 42;
            header.next_page_id = -1;
        }
        let guard = f.read();
        assert_eq!(guard.page_id(), PAGE);
        assert_eq!(
            guard.as_ref::<Header>(),
            &Header {
                page_type: 1,
                size: 42,
                next_page_id: -1
            }
        );
        assert_eq!(&guard.get_data()[..4], &1u32.to_ne_bytes());
    }

    #[test]
    fn mutable_access_marks_dirty() {
        let f = Fixture::new();
        let mut guard = f.write();
        let _ = guard.as_ref::<Header>();
        assert!(!guard.is_dirty());
        guard.get_data_mut()[0] = 1;
        assert!(guard.is_dirty());
    }

    #[test]
    fn readers_share_and_writer_excludes() {
        let f = Fixture::new();
        let r1 = f.read();
        let r2 = f.read();
        assert_eq!(f.frame.pin_count(), 2);
        assert!(f.frame.try_get_data_mut().is_none());
        drop(r1);
        drop(r2);

        let w = f.write();
        assert!(f.frame.try_get_data().is_none());
        drop(w);
        assert!(f.frame.try_get_data().is_some());
    }

    #[test]
    fn drop_unpins_and_last_guard_makes_evictable() {
        let f = Fixture::new();
        let r1 = f.read();
        let r2 = f.read();
        drop(r1);
        assert_eq!(f.frame.pin_count(), 1);
        assert_eq!(f.replacer.size(), 0);

        drop(r2);
        assert_eq!(f.frame.pin_count(), 0);
        assert_eq!(f.replacer.size(), 1);
        assert_eq!(f.replacer.evict(), Some(0));
    }

    #[test]
    fn flush_writes_dirty_page_to_disk() {
        let f = Fixture::new();
        let mut guard = f.write();
        guard.get_data_mut()[..5].copy_from_slice(b"hello");
        guard.flush().unwrap();
        assert!(!guard.is_dirty());
        drop(guard);

        let on_disk = f.disk_scheduler.read_page(PAGE).wait().unwrap();
        assert_eq!(&on_disk[..5], b"hello");
    }

    #[test]
    fn flush_skips_clean_page() {
        let f = Fixture::new();
        let guard = f.read();
        guard.flush().unwrap();
        // Nothing was written, so the page still reads as zeroes.
        let on_disk = f.disk_scheduler.read_page(PAGE).wait().unwrap();
        assert!(on_disk.iter().all(|&b| b == 0));
    }

    #[test]
    #[should_panic(expected = "cannot view a page as")]
    fn oversized_type_panics() {
        let f = Fixture::new();
        let guard = f.read();
        let _ = guard.as_ref::<TooBig>();
    }

    #[test]
    fn guard_can_move_across_threads() {
        let f = Fixture::new();
        let mut guard = f.write();
        let handle = std::thread::spawn(move || {
            guard.as_mut::<Header>().size = 9;
        });
        handle.join().unwrap();
        assert_eq!(f.read().as_ref::<Header>().size, 9);
        assert_eq!(f.frame.pin_count(), 0);
    }
}
