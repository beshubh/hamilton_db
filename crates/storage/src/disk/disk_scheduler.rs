use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::{self, JoinHandle};

use common::PageId;

use crate::disk::disk_manager::DiskManager;
use crate::error::{Result, StorageError};
use crate::page::{PAGE_SIZE, PageData};

/// A unit of work for the background worker. Each request carries the sending
/// half of a one-shot channel that the worker uses to report the result.
enum DiskRequest {
    Read {
        page_id: PageId,
        done: Sender<Result<PageData>>,
    },
    Write {
        page_id: PageId,
        data: PageData,
        done: Sender<Result<()>>,
    },
    Delete {
        page_id: PageId,
        done: Sender<Result<()>>,
    },
}

/// Handle to the result of a scheduled request.
#[must_use = "a DiskFuture does nothing unless you `wait` on it"]
pub struct DiskFuture<T> {
    rx: Receiver<Result<T>>,
}

impl<T> DiskFuture<T> {
    /// Blocks until the worker has finished the request.
    pub fn wait(self) -> Result<T> {
        self.rx
            .recv()
            .unwrap_or(Err(StorageError::DiskSchedulerShutDown))
    }
}

/// Runs disk I/O on a background thread so callers such as the buffer pool can
/// queue a request and wait on it without holding their own locks.
///
/// Requests are handled one at a time in the order they were scheduled, so a
/// read always sees an earlier write to the same page.
pub struct DiskScheduler {
    /// `None` only while dropping, to close the channel before joining.
    sender: Option<Sender<DiskRequest>>,
    worker: Option<JoinHandle<()>>,
}

impl DiskScheduler {
    pub fn new(disk_manager: Arc<DiskManager>) -> Self {
        let (sender, receiver) = mpsc::channel();
        let worker = thread::Builder::new()
            .name("disk-scheduler".into())
            .spawn(move || run_worker(&disk_manager, receiver))
            .expect("failed to spawn disk scheduler thread");
        Self {
            sender: Some(sender),
            worker: Some(worker),
        }
    }

    /// Queues a read of `page_id`. The future yields the page contents.
    pub fn read_page(&self, page_id: PageId) -> DiskFuture<PageData> {
        self.schedule(|done| DiskRequest::Read { page_id, done })
    }

    /// Queues a write of `data` to `page_id`.
    pub fn write_page(&self, page_id: PageId, data: PageData) -> DiskFuture<()> {
        self.schedule(|done| DiskRequest::Write {
            page_id,
            data,
            done,
        })
    }

    /// Queues freeing `page_id` on disk. Goes through the queue so it cannot
    /// overtake an earlier write to the same page.
    pub fn deallocate_page(&self, page_id: PageId) -> DiskFuture<()> {
        self.schedule(|done| DiskRequest::Delete { page_id, done })
    }

    fn schedule<T>(&self, make: impl FnOnce(Sender<Result<T>>) -> DiskRequest) -> DiskFuture<T> {
        let (done, rx) = mpsc::channel();
        let sender = self.sender.as_ref().expect("disk scheduler is shut down");
        // If the worker has died, `done` is dropped with the request and
        // `wait` reports the error.
        let _ = sender.send(make(done));
        DiskFuture { rx }
    }
}

impl Drop for DiskScheduler {
    /// Lets the worker finish every queued request, then joins it.
    fn drop(&mut self) {
        drop(self.sender.take());
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn run_worker(disk_manager: &DiskManager, receiver: Receiver<DiskRequest>) {
    // `recv` fails once every sender is gone, i.e. the scheduler was dropped.
    while let Ok(request) = receiver.recv() {
        // The caller may have dropped its future; that is not an error.
        match request {
            DiskRequest::Read { page_id, done } => {
                let mut data: PageData = Box::new([0u8; PAGE_SIZE]);
                let result = disk_manager.read_page(page_id, &mut data).map(|()| data);
                let _ = done.send(result);
            }
            DiskRequest::Write {
                page_id,
                data,
                done,
            } => {
                let _ = done.send(disk_manager.write_page(page_id, &data));
            }
            DiskRequest::Delete { page_id, done } => {
                disk_manager.delete_page(page_id);
                let _ = done.send(Ok(()));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::INVALID_PAGE_ID;
    use tempfile::TempDir;

    fn setup() -> (TempDir, Arc<DiskManager>, DiskScheduler) {
        let dir = TempDir::new().unwrap();
        let dm = Arc::new(DiskManager::new(dir.path().join("test.db")).unwrap());
        let scheduler = DiskScheduler::new(Arc::clone(&dm));
        (dir, dm, scheduler)
    }

    fn page_filled(byte: u8) -> PageData {
        Box::new([byte; PAGE_SIZE])
    }

    #[test]
    fn write_then_read() {
        let (_dir, _dm, scheduler) = setup();
        let mut data = page_filled(0);
        data[..5].copy_from_slice(b"hello");

        scheduler.write_page(0, data.clone()).wait().unwrap();
        let read = scheduler.read_page(0).wait().unwrap();
        assert_eq!(read, data);
    }

    #[test]
    fn requests_run_in_order() {
        let (_dir, _dm, scheduler) = setup();
        // Queue everything before waiting on anything.
        let w1 = scheduler.write_page(1, page_filled(1));
        let w2 = scheduler.write_page(1, page_filled(2));
        let r = scheduler.read_page(1);

        w1.wait().unwrap();
        w2.wait().unwrap();
        assert_eq!(r.wait().unwrap(), page_filled(2));
    }

    #[test]
    fn deallocate_after_write_frees_slot() {
        let (_dir, dm, scheduler) = setup();
        let w = scheduler.write_page(3, page_filled(3));
        let d = scheduler.deallocate_page(3);
        w.wait().unwrap();
        d.wait().unwrap();
        assert_eq!(dm.num_writes(), 1);
        assert_eq!(dm.num_deletes(), 1);
    }

    #[test]
    fn errors_are_reported() {
        let (_dir, _dm, scheduler) = setup();
        let err = scheduler.read_page(INVALID_PAGE_ID).wait().unwrap_err();
        assert!(matches!(err, StorageError::InvalidPageId(INVALID_PAGE_ID)));
    }

    #[test]
    fn drop_finishes_pending_requests() {
        let (_dir, dm, scheduler) = setup();
        for i in 0..32 {
            // Futures dropped on purpose: the work must still happen.
            let _ = scheduler.write_page(i, page_filled(i as u8));
        }
        drop(scheduler);

        assert_eq!(dm.num_writes(), 32);
        let mut buf = [0u8; PAGE_SIZE];
        dm.read_page(31, &mut buf).unwrap();
        assert_eq!(buf, [31u8; PAGE_SIZE]);
    }

    #[test]
    fn shared_across_threads() {
        let (_dir, _dm, scheduler) = setup();
        let scheduler = Arc::new(scheduler);
        let handles: Vec<_> = (0..4)
            .map(|t| {
                let scheduler = Arc::clone(&scheduler);
                thread::spawn(move || {
                    for i in 0..10 {
                        let id = t * 10 + i;
                        scheduler
                            .write_page(id, page_filled(id as u8))
                            .wait()
                            .unwrap();
                        let read = scheduler.read_page(id).wait().unwrap();
                        assert_eq!(read, page_filled(id as u8));
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
    }
}
