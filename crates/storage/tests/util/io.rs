//! Helpers for tests that check one page's disk I/O does not wait for
//! another's.

use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;
use std::time::Duration;

use common::PageId;
use parking_lot::Mutex;

use crate::disk::disk_manager::DiskManager;

const TIMEOUT: Duration = Duration::from_secs(5);

/// Runs `f` on its own thread and fails if it takes longer than 5 seconds.
pub(crate) fn within_timeout(f: impl FnOnce() + Send + 'static) {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        f();
        let _ = tx.send(());
    });
    rx.recv_timeout(TIMEOUT)
        .expect("did not finish while another page's disk I/O was paused");
}

/// A page whose reads and writes stop just before their `pread`/`pwrite`,
/// with the disk manager's lock released, until the stall is dropped.
pub(crate) struct Stall {
    started: Receiver<()>,
    _release: Sender<()>,
}

impl Stall {
    /// Waits until a request for the page has stopped.
    pub(crate) fn wait_started(&self) {
        self.started
            .recv_timeout(TIMEOUT)
            .expect("no request for the stalled page started");
    }
}

pub(crate) fn stall_io(dm: &DiskManager, page_id: PageId) -> Stall {
    let (started, started_rx) = mpsc::channel();
    let (release, release_rx) = mpsc::channel::<()>();
    let release_rx = Mutex::new(release_rx);
    dm.set_before_io(move |pid| {
        if pid == page_id {
            let _ = started.send(());
            // Returns once the stall is dropped.
            let _ = release_rx.lock().recv();
        }
    });
    Stall {
        started: started_rx,
        _release: release,
    }
}
