use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use parking_lot::Mutex;

use crate::page::{PAGE_SIZE, PageId};

/// Number of pages the db file has room for when first created.
pub const DEFAULT_DB_IO_SIZE: usize = 16;

/// Handles allocating and freeing pages in the database file, and reading and
/// writing them.
///
/// Space is allocated lazily: a page only gets an offset in the file the first
/// time it is read or written. Offsets of deleted pages are reused by later
/// allocations.
///
/// The page table lives only in memory, so pages written by an earlier
/// `DiskManager` cannot be found after the file is reopened.
pub struct DiskManager {
    db_file_name: PathBuf,
    log_file_name: PathBuf,
    state: Mutex<DiskState>,
    log: Mutex<LogState>,
    flush_log: AtomicBool,
}

struct DiskState {
    db_io: File,
    /// Offset of each page in the db file.
    pages: HashMap<PageId, u64>,
    /// Offsets left behind by deleted pages, ready for reuse.
    free_slots: Vec<u64>,
    /// Number of pages the db file currently has room for.
    page_capacity: usize,
    num_writes: usize,
    num_deletes: usize,
}

struct LogState {
    log_io: File,
    num_flushes: usize,
}

impl DiskManager {
    /// Opens (or creates) `db_file` and a matching `.log` file next to it.
    pub fn new(db_file: impl AsRef<Path>) -> io::Result<Self> {
        let db_file_name = db_file.as_ref().to_path_buf();
        let log_file_name = db_file_name.with_extension("log");

        let db_io = open_rw(&db_file_name)?;
        let log_io = open_rw(&log_file_name)?;

        let page_capacity = DEFAULT_DB_IO_SIZE;
        db_io.set_len(file_len_for(page_capacity))?;

        Ok(Self {
            db_file_name,
            log_file_name,
            state: Mutex::new(DiskState {
                db_io,
                pages: HashMap::new(),
                free_slots: Vec::new(),
                page_capacity,
                num_writes: 0,
                num_deletes: 0,
            }),
            log: Mutex::new(LogState {
                log_io,
                num_flushes: 0,
            }),
            flush_log: AtomicBool::new(false),
        })
    }

    /// Flushes both files to stable storage. The files are closed when the
    /// `DiskManager` is dropped.
    pub fn shut_down(&self) -> io::Result<()> {
        self.state.lock().db_io.sync_all()?;
        self.log.lock().log_io.sync_all()
    }

    /// Writes `page_data` to the page's slot in the db file, giving the page
    /// a slot first if it does not have one yet.
    pub fn write_page(&self, page_id: PageId, page_data: &[u8; PAGE_SIZE]) -> io::Result<()> {
        check_page_id(page_id)?;
        let mut state = self.state.lock();
        let offset = state.offset_or_allocate(page_id)?;

        state.db_io.seek(SeekFrom::Start(offset))?;
        state.db_io.write_all(page_data)?;
        state.db_io.flush()?;
        state.num_writes += 1;
        Ok(())
    }

    /// Reads the page into `page_data`. A page that has never been written
    /// reads as all zeroes.
    pub fn read_page(&self, page_id: PageId, page_data: &mut [u8; PAGE_SIZE]) -> io::Result<()> {
        check_page_id(page_id)?;
        let mut state = self.state.lock();
        let offset = state.offset_or_allocate(page_id)?;

        state.db_io.seek(SeekFrom::Start(offset))?;
        let n = read_full(&mut state.db_io, page_data)?;
        page_data[n..].fill(0);
        Ok(())
    }

    /// Frees the page's slot so a later allocation can reuse it. Unknown pages
    /// are ignored.
    pub fn delete_page(&self, page_id: PageId) {
        let mut state = self.state.lock();
        if let Some(offset) = state.pages.remove(&page_id) {
            state.free_slots.push(offset);
            state.num_deletes += 1;
        }
    }

    /// Appends `log_data` to the log file and flushes it.
    pub fn write_log(&self, log_data: &[u8]) -> io::Result<()> {
        if log_data.is_empty() {
            return Ok(());
        }
        let mut log = self.log.lock();
        self.flush_log.store(true, Ordering::Release);
        let result = log
            .log_io
            .seek(SeekFrom::End(0))
            .and_then(|_| log.log_io.write_all(log_data))
            .and_then(|_| log.log_io.flush());
        log.num_flushes += 1;
        self.flush_log.store(false, Ordering::Release);
        result
    }

    /// Reads `log_data.len()` bytes from the log file starting at `offset`.
    /// Returns `false` if `offset` is past the end of the log; a short read
    /// leaves the rest of the buffer zeroed.
    pub fn read_log(&self, log_data: &mut [u8], offset: u64) -> io::Result<bool> {
        let mut log = self.log.lock();
        if offset >= log.log_io.metadata()?.len() {
            return Ok(false);
        }
        log.log_io.seek(SeekFrom::Start(offset))?;
        let n = read_full(&mut log.log_io, log_data)?;
        log_data[n..].fill(0);
        Ok(true)
    }

    pub fn num_flushes(&self) -> usize {
        self.log.lock().num_flushes
    }

    /// Whether a log flush is in progress.
    pub fn flush_state(&self) -> bool {
        self.flush_log.load(Ordering::Acquire)
    }

    pub fn num_writes(&self) -> usize {
        self.state.lock().num_writes
    }

    pub fn num_deletes(&self) -> usize {
        self.state.lock().num_deletes
    }

    pub fn db_file_name(&self) -> &Path {
        &self.db_file_name
    }

    pub fn log_file_name(&self) -> &Path {
        &self.log_file_name
    }

    /// Size of the db file on disk, in bytes.
    pub fn db_file_size(&self) -> io::Result<u64> {
        Ok(self.state.lock().db_io.metadata()?.len())
    }
}

impl DiskState {
    fn offset_or_allocate(&mut self, page_id: PageId) -> io::Result<u64> {
        if let Some(&offset) = self.pages.get(&page_id) {
            return Ok(offset);
        }
        let offset = self.allocate_page()?;
        self.pages.insert(page_id, offset);
        Ok(offset)
    }

    /// Returns a free offset, doubling the db file when it is full.
    fn allocate_page(&mut self) -> io::Result<u64> {
        if let Some(offset) = self.free_slots.pop() {
            return Ok(offset);
        }
        // With no free slots, every slot below `pages.len()` is in use.
        if self.pages.len() + 1 >= self.page_capacity {
            self.page_capacity *= 2;
            self.db_io.set_len(file_len_for(self.page_capacity))?;
        }
        Ok((self.pages.len() * PAGE_SIZE) as u64)
    }
}

fn open_rw(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
}

fn file_len_for(page_capacity: usize) -> u64 {
    ((page_capacity + 1) * PAGE_SIZE) as u64
}

fn check_page_id(page_id: PageId) -> io::Result<()> {
    if page_id < 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid page id {page_id}"),
        ));
    }
    Ok(())
}

/// Reads until `buf` is full or EOF, returning the number of bytes read.
fn read_full(file: &mut File, buf: &mut [u8]) -> io::Result<usize> {
    let mut n = 0;
    while n < buf.len() {
        match file.read(&mut buf[n..]) {
            Ok(0) => break,
            Ok(k) => n += k,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::INVALID_PAGE_ID;
    use std::sync::Arc;
    use std::thread;
    use tempfile::TempDir;

    fn setup() -> (TempDir, DiskManager) {
        let dir = TempDir::new().unwrap();
        let dm = DiskManager::new(dir.path().join("test.db")).unwrap();
        (dir, dm)
    }

    fn page_filled(byte: u8) -> [u8; PAGE_SIZE] {
        [byte; PAGE_SIZE]
    }

    #[test]
    fn creates_db_and_log_files() {
        let (dir, dm) = setup();
        assert!(dir.path().join("test.db").exists());
        assert!(dir.path().join("test.log").exists());
        assert_eq!(dm.log_file_name(), dir.path().join("test.log"));
        assert_eq!(
            dm.db_file_size().unwrap(),
            ((DEFAULT_DB_IO_SIZE + 1) * PAGE_SIZE) as u64
        );
    }

    #[test]
    fn write_then_read_page() {
        let (_dir, dm) = setup();
        let mut data = page_filled(0);
        data[..5].copy_from_slice(b"hello");
        dm.write_page(0, &data).unwrap();
        dm.write_page(5, &page_filled(7)).unwrap();

        let mut buf = page_filled(0xff);
        dm.read_page(0, &mut buf).unwrap();
        assert_eq!(buf, data);
        dm.read_page(5, &mut buf).unwrap();
        assert_eq!(buf, page_filled(7));
        assert_eq!(dm.num_writes(), 2);
    }

    #[test]
    fn unwritten_page_reads_as_zeroes() {
        let (_dir, dm) = setup();
        let mut buf = page_filled(0xff);
        dm.read_page(3, &mut buf).unwrap();
        assert_eq!(buf, page_filled(0));
    }

    #[test]
    fn overwrite_keeps_same_slot() {
        let (_dir, dm) = setup();
        dm.write_page(1, &page_filled(1)).unwrap();
        dm.write_page(1, &page_filled(2)).unwrap();
        let mut buf = page_filled(0);
        dm.read_page(1, &mut buf).unwrap();
        assert_eq!(buf, page_filled(2));
        assert_eq!(dm.state.lock().pages.len(), 1);
    }

    #[test]
    fn delete_reuses_slot() {
        let (_dir, dm) = setup();
        dm.write_page(0, &page_filled(1)).unwrap();
        dm.write_page(1, &page_filled(2)).unwrap();
        let offset = dm.state.lock().pages[&0];

        dm.delete_page(0);
        dm.delete_page(42); // unknown page is a no-op
        assert_eq!(dm.num_deletes(), 1);

        dm.write_page(2, &page_filled(3)).unwrap();
        assert_eq!(dm.state.lock().pages[&2], offset);

        let mut buf = page_filled(0);
        dm.read_page(1, &mut buf).unwrap();
        assert_eq!(buf, page_filled(2));
    }

    #[test]
    fn grows_file_when_full() {
        let (_dir, dm) = setup();
        let n = DEFAULT_DB_IO_SIZE * 3;
        for i in 0..n {
            dm.write_page(i as PageId, &page_filled(i as u8)).unwrap();
        }
        assert!(dm.db_file_size().unwrap() >= (n * PAGE_SIZE) as u64);

        let mut buf = page_filled(0);
        for i in 0..n {
            dm.read_page(i as PageId, &mut buf).unwrap();
            assert_eq!(buf, page_filled(i as u8), "page {i}");
        }
    }

    #[test]
    fn rejects_invalid_page_id() {
        let (_dir, dm) = setup();
        let err = dm.write_page(INVALID_PAGE_ID, &page_filled(0)).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn log_roundtrip() {
        let (_dir, dm) = setup();
        dm.write_log(b"hello ").unwrap();
        dm.write_log(b"world").unwrap();
        dm.write_log(b"").unwrap(); // ignored
        assert_eq!(dm.num_flushes(), 2);
        assert!(!dm.flush_state());

        let mut buf = [0xffu8; 8];
        assert!(dm.read_log(&mut buf, 6).unwrap());
        assert_eq!(&buf, b"world\0\0\0");
        assert!(!dm.read_log(&mut buf, 100).unwrap());
    }

    #[test]
    fn concurrent_writes() {
        let (_dir, dm) = setup();
        let dm = Arc::new(dm);
        let handles: Vec<_> = (0..8)
            .map(|t| {
                let dm = Arc::clone(&dm);
                thread::spawn(move || {
                    for i in 0..10 {
                        let id = t * 10 + i;
                        dm.write_page(id, &page_filled(id as u8)).unwrap();
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }

        let mut buf = page_filled(0);
        for id in 0..80 {
            dm.read_page(id, &mut buf).unwrap();
            assert_eq!(buf, page_filled(id as u8));
        }
        dm.shut_down().unwrap();
    }
}
