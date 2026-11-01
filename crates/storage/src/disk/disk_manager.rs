use std::collections::HashMap;
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use common::PageId;
use parking_lot::Mutex;

use crate::error::{Result, StorageError};
use crate::page::PAGE_SIZE;

/// Number of pages the db file has room for when first created.
pub const DEFAULT_DB_IO_SIZE: usize = 16;

const META_MAGIC: u64 = u64::from_le_bytes(*b"HAMDBMET");
const META_VERSION: u32 = 1;

/// Handles allocating and freeing pages in the database file, and reading and
/// writing them.
///
/// Space is allocated lazily: a page only gets an offset in the file the first
/// time it is written. Offsets of deleted pages are reused by later
/// allocations.
///
/// Reads and writes hold the lock only to look up or allocate the page's
/// slot, and run the `pread`/`pwrite` without it, so I/O on different pages
/// runs in parallel. Callers must not run two requests for the same page at
/// once, or delete a page while a request for it is running, since a freed
/// slot can be given to another page. `DiskScheduler` keeps each page's
/// requests in order.
///
/// The page directory (each page's offset and free space) and the free-slot
/// list are kept in memory and saved to a separate `.meta` file by
/// `checkpoint` and `shut_down`. Changes since the last save are lost if the
/// process stops without one.
pub struct DiskManager {
    db_file_name: PathBuf,
    log_file_name: PathBuf,
    meta_file_name: PathBuf,
    /// Only read and written with positioned I/O, so it needs no lock.
    db_io: File,
    state: Mutex<DiskState>,
    log: Mutex<LogState>,
    flush_log: AtomicBool,
    num_reads: AtomicUsize,
    num_writes: AtomicUsize,
    num_deletes: AtomicUsize,
    #[cfg(test)]
    before_io: Mutex<Option<IoHook>>,
}

#[cfg(test)]
type IoHook = Arc<dyn Fn(PageId) + Send + Sync>;

struct DiskState {
    /// Where each written page lives in the db file.
    pages: HashMap<PageId, PageEntry>,
    /// Offsets left behind by deleted pages, ready for reuse.
    free_slots: Vec<u64>,
    /// Slots taken by first writes still in progress. The page is added to
    /// `pages` only once its write is done, so a checkpoint never saves a
    /// slot that does not hold the page yet.
    first_writes: usize,
    /// Number of pages the db file currently has room for.
    page_capacity: usize,
    /// Whether the metadata changed since it was last saved.
    meta_dirty: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PageEntry {
    /// Offset of the page in the db file.
    offset: u64,
    /// Free bytes in the page, as last reported with `set_free_space`.
    free_space: u32,
}

struct LogState {
    log_io: File,
    num_flushes: usize,
}

impl DiskManager {
    /// Opens (or creates) `db_file` with matching `.log` and `.meta` files
    /// next to it. The page directory of an existing database is loaded from
    /// its `.meta` file. Fails with `DatabaseLocked` while another manager
    /// has `db_file` open.
    pub fn new(db_file: impl AsRef<Path>) -> Result<Self> {
        let db_file_name = db_file.as_ref().to_path_buf();
        let log_file_name = db_file_name.with_extension("log");
        let meta_file_name = db_file_name.with_extension("meta");

        let db_io = open_rw(&db_file_name)?;
        // Two managers on one file would hand out the same slots and
        // overwrite each other's `.meta`. The lock goes when `db_io` closes.
        db_io.try_lock().map_err(|e| match e {
            TryLockError::WouldBlock => StorageError::DatabaseLocked(db_file_name.clone()),
            TryLockError::Error(e) => e.into(),
        })?;
        let log_io = open_rw(&log_file_name)?;

        // The capacity is not saved; it follows from the db file's length.
        let db_len = db_io.metadata()?.len() as usize;
        let mut state = DiskState {
            pages: HashMap::new(),
            free_slots: Vec::new(),
            first_writes: 0,
            page_capacity: (db_len / PAGE_SIZE).max(DEFAULT_DB_IO_SIZE),
            meta_dirty: false,
        };
        match fs::read(&meta_file_name) {
            Ok(bytes) => state.load_meta(&bytes)?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                // The meta file is saved before the db file is grown, so a
                // db file with data but no meta file was not made by us.
                if db_len != 0 {
                    return Err(StorageError::MissingMetadata(meta_file_name));
                }
                state.meta_dirty = true;
                state.save_meta(&meta_file_name)?;
            }
            Err(e) => return Err(e.into()),
        }
        let file_len = file_len_for(state.page_capacity);
        if (db_len as u64) < file_len {
            db_io.set_len(file_len)?;
        }

        Ok(Self {
            db_file_name,
            log_file_name,
            meta_file_name,
            db_io,
            state: Mutex::new(state),
            log: Mutex::new(LogState {
                log_io,
                num_flushes: 0,
            }),
            flush_log: AtomicBool::new(false),
            num_reads: AtomicUsize::new(0),
            num_writes: AtomicUsize::new(0),
            num_deletes: AtomicUsize::new(0),
            #[cfg(test)]
            before_io: Mutex::new(None),
        })
    }

    /// Saves the metadata and flushes all files to stable storage. The files
    /// are closed when the `DiskManager` is dropped.
    pub fn shut_down(&self) -> Result<()> {
        self.checkpoint()?;
        self.log.lock().log_io.sync_all()?;
        Ok(())
    }

    /// Flushes the db file and saves the metadata file if it changed.
    pub fn checkpoint(&self) -> Result<()> {
        // Held throughout so no first write is added to `pages` after the
        // sync and saved without its data on disk.
        let mut state = self.state.lock();
        self.db_io.sync_all()?;
        state.save_meta(&self.meta_file_name)
    }

    /// Free bytes in the page as last set with `set_free_space`, or `None`
    /// if the page has no slot. A newly allocated page has `PAGE_SIZE`.
    pub fn free_space(&self, page_id: PageId) -> Option<usize> {
        let state = self.state.lock();
        state.pages.get(&page_id).map(|e| e.free_space as usize)
    }

    /// One past the highest page id that has a slot in the db file, or 0 for
    /// an empty database. Ids handed out but never written are not counted.
    pub fn next_page_id(&self) -> PageId {
        let state = self.state.lock();
        state.pages.keys().max().map_or(0, |&max| max + 1)
    }

    /// Records how many bytes are free in the page.
    pub fn set_free_space(&self, page_id: PageId, free_space: usize) -> Result<()> {
        if free_space > PAGE_SIZE {
            return Err(StorageError::InvalidFreeSpace(free_space));
        }
        let mut state = self.state.lock();
        let entry = state
            .pages
            .get_mut(&page_id)
            .ok_or(StorageError::PageNotAllocated(page_id))?;
        if entry.free_space != free_space as u32 {
            entry.free_space = free_space as u32;
            state.meta_dirty = true;
        }
        Ok(())
    }

    /// Writes `page_data` to the page's slot in the db file, giving the page
    /// a slot first if it does not have one yet.
    pub fn write_page(&self, page_id: PageId, page_data: &[u8; PAGE_SIZE]) -> Result<()> {
        check_page_id(page_id)?;
        let (offset, first_write) = self.state.lock().slot_for_write(page_id, &self.db_io)?;

        #[cfg(test)]
        self.run_before_io(page_id);
        let written = write_all_at(&self.db_io, page_data, offset);
        if first_write {
            let mut state = self.state.lock();
            state.finish_first_write(page_id, offset, written.is_ok());
        }
        written?;
        self.num_writes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Reads the page into `page_data`. A page that has never been written
    /// reads as all zeroes. It gets no slot until it is written: a slot
    /// freed by `delete_page` still holds the deleted page's bytes.
    pub fn read_page(&self, page_id: PageId, page_data: &mut [u8; PAGE_SIZE]) -> Result<()> {
        check_page_id(page_id)?;
        let offset = self.state.lock().pages.get(&page_id).map(|e| e.offset);
        let Some(offset) = offset else {
            page_data.fill(0);
            return Ok(());
        };

        #[cfg(test)]
        self.run_before_io(page_id);
        let n = read_full_at(&self.db_io, page_data, offset)?;
        page_data[n..].fill(0);
        self.num_reads.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Frees the page's slot so a later allocation can reuse it. Unknown pages
    /// are ignored.
    /// Whether `page_id` has been written and not deleted since: whether it
    /// has a slot on disk.
    pub fn has_page(&self, page_id: PageId) -> bool {
        self.state.lock().pages.contains_key(&page_id)
    }

    pub fn delete_page(&self, page_id: PageId) {
        let mut state = self.state.lock();
        if let Some(entry) = state.pages.remove(&page_id) {
            state.free_slots.push(entry.offset);
            state.meta_dirty = true;
            self.num_deletes.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Appends `log_data` to the log file and flushes it.
    pub fn write_log(&self, log_data: &[u8]) -> Result<()> {
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
        Ok(result?)
    }

    /// Reads `log_data.len()` bytes from the log file starting at `offset`.
    /// Returns `false` if `offset` is past the end of the log; a short read
    /// leaves the rest of the buffer zeroed.
    pub fn read_log(&self, log_data: &mut [u8], offset: u64) -> Result<bool> {
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

    /// Pages read from the db file. Reads of pages never written are not
    /// counted, as they do not touch the file.
    pub fn num_reads(&self) -> usize {
        self.num_reads.load(Ordering::Relaxed)
    }

    pub fn num_writes(&self) -> usize {
        self.num_writes.load(Ordering::Relaxed)
    }

    pub fn num_deletes(&self) -> usize {
        self.num_deletes.load(Ordering::Relaxed)
    }

    pub fn db_file_name(&self) -> &Path {
        &self.db_file_name
    }

    pub fn log_file_name(&self) -> &Path {
        &self.log_file_name
    }

    pub fn meta_file_name(&self) -> &Path {
        &self.meta_file_name
    }

    /// Keeps every page read and write from starting until the returned guard
    /// is dropped, so tests can catch a request in the middle of its I/O.
    #[cfg(test)]
    pub(crate) fn pause_io(&self) -> impl Sized + '_ {
        self.state.lock()
    }

    /// Runs `hook` with the page id just before each `pread`/`pwrite`, after
    /// the lock is released. A hook that blocks stalls only that request.
    #[cfg(test)]
    pub(crate) fn set_before_io(&self, hook: impl Fn(PageId) + Send + Sync + 'static) {
        *self.before_io.lock() = Some(Arc::new(hook));
    }

    #[cfg(test)]
    fn run_before_io(&self, page_id: PageId) {
        // Cloned out so a blocked hook does not hold up other requests.
        let hook = self.before_io.lock().clone();
        if let Some(hook) = hook {
            hook(page_id);
        }
    }

    /// Size of the db file on disk, in bytes.
    pub fn db_file_size(&self) -> Result<u64> {
        Ok(self.db_io.metadata()?.len())
    }
}

impl DiskState {
    /// Returns the page's offset, and whether this is its first write. A
    /// first write gets a new slot, and must be ended with
    /// `finish_first_write`.
    fn slot_for_write(&mut self, page_id: PageId, db_io: &File) -> Result<(u64, bool)> {
        if let Some(entry) = self.pages.get(&page_id) {
            return Ok((entry.offset, false));
        }
        let offset = self.allocate_slot(db_io)?;
        self.first_writes += 1;
        Ok((offset, true))
    }

    /// Adds the page at `offset` to `pages`, or frees the slot if the write
    /// failed.
    fn finish_first_write(&mut self, page_id: PageId, offset: u64, written: bool) {
        self.first_writes -= 1;
        if written {
            let free_space = PAGE_SIZE as u32;
            self.pages.insert(page_id, PageEntry { offset, free_space });
            self.meta_dirty = true;
        } else {
            self.free_slots.push(offset);
        }
    }

    /// Returns a free offset, doubling the db file when it is full.
    fn allocate_slot(&mut self, db_io: &File) -> Result<u64> {
        if let Some(offset) = self.free_slots.pop() {
            return Ok(offset);
        }
        // With no free slots, every slot below this one holds a page or a
        // first write in progress.
        let slot = self.pages.len() + self.first_writes;
        if slot >= self.page_capacity {
            self.page_capacity *= 2;
            db_io.set_len(file_len_for(self.page_capacity))?;
        }
        Ok((slot * PAGE_SIZE) as u64)
    }

    /// Saves the metadata to `path` if it changed since the last save.
    ///
    /// It is written to a temporary file that is then renamed over `path`,
    /// so a crash leaves either the old or the new metadata in place.
    fn save_meta(&mut self, path: &Path) -> Result<()> {
        if !self.meta_dirty {
            return Ok(());
        }
        let tmp_path = path.with_extension("meta.tmp");
        let mut tmp = File::create(&tmp_path)?;
        tmp.write_all(&self.encode_meta())?;
        tmp.sync_all()?;
        drop(tmp);
        fs::rename(&tmp_path, path)?;
        sync_parent_dir(path)?;
        self.meta_dirty = false;
        Ok(())
    }

    /// Lays the metadata out as
    /// `magic: u64, version: u32, _pad: u32, num_pages: u64, (page_id: i32, free_space: u32, offset: u64)*,
    /// num_free: u64, offset: u64*, checksum: u64`, all little-endian.
    fn encode_meta(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(40 + self.pages.len() * 16 + self.free_slots.len() * 8);
        bytes.extend_from_slice(&META_MAGIC.to_le_bytes());
        bytes.extend_from_slice(&META_VERSION.to_le_bytes());
        bytes.extend_from_slice(&0u32.to_le_bytes());
        bytes.extend_from_slice(&(self.pages.len() as u64).to_le_bytes());
        for (&page_id, entry) in &self.pages {
            bytes.extend_from_slice(&page_id.to_le_bytes());
            bytes.extend_from_slice(&entry.free_space.to_le_bytes());
            bytes.extend_from_slice(&entry.offset.to_le_bytes());
        }
        bytes.extend_from_slice(&(self.free_slots.len() as u64).to_le_bytes());
        for &offset in &self.free_slots {
            bytes.extend_from_slice(&offset.to_le_bytes());
        }
        let checksum = fnv1a(&bytes);
        bytes.extend_from_slice(&checksum.to_le_bytes());
        bytes
    }

    /// Loads the metadata, checking that every offset is a whole slot within
    /// `page_capacity`.
    fn load_meta(&mut self, bytes: &[u8]) -> Result<()> {
        let (body, checksum) = bytes
            .split_last_chunk::<8>()
            .ok_or_else(|| corrupt_meta("file is truncated"))?;
        if fnv1a(body) != u64::from_le_bytes(*checksum) {
            return Err(corrupt_meta("checksum mismatch"));
        }
        let mut r = Reader(body);
        if r.u64()? != META_MAGIC {
            return Err(corrupt_meta("not a hamilton_db metadata file"));
        }
        let version = r.u32()?;
        if version != META_VERSION {
            return Err(StorageError::UnsupportedMetadataVersion(version));
        }
        r.u32()?;
        let slot_end = file_len_for(self.page_capacity);
        let check_offset = |offset: u64| {
            if !offset.is_multiple_of(PAGE_SIZE as u64) || offset >= slot_end {
                return Err(corrupt_meta(format!("bad page offset {offset}")));
            }
            Ok(offset)
        };

        let mut pages = HashMap::new();
        for _ in 0..r.u64()? {
            let page_id = r.u32()? as PageId;
            let free_space = r.u32()?;
            let offset = check_offset(r.u64()?)?;
            let entry = PageEntry { offset, free_space };
            if page_id < 0
                || free_space as usize > PAGE_SIZE
                || pages.insert(page_id, entry).is_some()
            {
                return Err(corrupt_meta(format!("bad entry for page {page_id}")));
            }
        }
        let mut free_slots = Vec::new();
        for _ in 0..r.u64()? {
            free_slots.push(check_offset(r.u64()?)?);
        }
        if !r.0.is_empty() {
            return Err(corrupt_meta("trailing bytes"));
        }

        self.pages = pages;
        self.free_slots = free_slots;
        self.meta_dirty = false;
        Ok(())
    }
}

/// Reads little-endian integers off the front of a byte slice.
struct Reader<'a>(&'a [u8]);

impl Reader<'_> {
    fn take<const N: usize>(&mut self) -> Result<[u8; N]> {
        let (head, rest) = self
            .0
            .split_first_chunk::<N>()
            .ok_or_else(|| corrupt_meta("file is truncated"))?;
        self.0 = rest;
        Ok(*head)
    }

    fn u32(&mut self) -> Result<u32> {
        self.take().map(u32::from_le_bytes)
    }

    fn u64(&mut self) -> Result<u64> {
        self.take().map(u64::from_le_bytes)
    }
}

fn fnv1a(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |hash, &b| {
        (hash ^ b as u64).wrapping_mul(0x0100_0000_01b3)
    })
}

/// Makes a rename inside the directory durable.
#[cfg(unix)]
fn sync_parent_dir(path: &Path) -> io::Result<()> {
    match path.parent() {
        Some(dir) if !dir.as_os_str().is_empty() => File::open(dir)?.sync_all(),
        _ => File::open(".")?.sync_all(),
    }
}

#[cfg(not(unix))]
fn sync_parent_dir(_path: &Path) -> io::Result<()> {
    Ok(())
}

fn corrupt_meta(msg: impl Into<String>) -> StorageError {
    StorageError::CorruptMetadata(msg.into())
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
    (page_capacity * PAGE_SIZE) as u64
}

fn check_page_id(page_id: PageId) -> Result<()> {
    if page_id < 0 {
        return Err(StorageError::InvalidPageId(page_id));
    }
    Ok(())
}

/// Writes all of `buf` at `offset` in one positioned write (`pwrite`),
/// without moving the file's cursor.
#[cfg(unix)]
fn write_all_at(file: &File, buf: &[u8], offset: u64) -> io::Result<()> {
    std::os::unix::fs::FileExt::write_all_at(file, buf, offset)
}

/// Moving the shared cursor and then writing is only safe one at a time.
#[cfg(not(unix))]
static CURSOR: Mutex<()> = Mutex::new(());

#[cfg(not(unix))]
fn write_all_at(mut file: &File, buf: &[u8], offset: u64) -> io::Result<()> {
    let _cursor = CURSOR.lock();
    file.seek(SeekFrom::Start(offset))?;
    file.write_all(buf)
}

/// Reads at `offset` until `buf` is full or EOF, with positioned reads
/// (`pread`), returning the number of bytes read.
#[cfg(unix)]
fn read_full_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    use std::os::unix::fs::FileExt;
    let mut n = 0;
    while n < buf.len() {
        match file.read_at(&mut buf[n..], offset + n as u64) {
            Ok(0) => break,
            Ok(k) => n += k,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(n)
}

#[cfg(not(unix))]
fn read_full_at(mut file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    let _cursor = CURSOR.lock();
    file.seek(SeekFrom::Start(offset))?;
    read_full(file, buf)
}

/// Reads until `buf` is full or EOF, returning the number of bytes read.
fn read_full(mut file: impl Read, buf: &mut [u8]) -> io::Result<usize> {
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
    use crate::test_util::{stall_io, within_timeout};
    use common::INVALID_PAGE_ID;
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
        assert!(dir.path().join("test.meta").exists());
        assert_eq!(dm.log_file_name(), dir.path().join("test.log"));
        assert_eq!(
            dm.db_file_size().unwrap(),
            (DEFAULT_DB_IO_SIZE * PAGE_SIZE) as u64
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
        let offset = dm.state.lock().pages[&0].offset;

        dm.delete_page(0);
        dm.delete_page(42); // unknown page is a no-op
        assert_eq!(dm.num_deletes(), 1);

        dm.write_page(2, &page_filled(3)).unwrap();
        assert_eq!(dm.state.lock().pages[&2].offset, offset);

        let mut buf = page_filled(0);
        dm.read_page(1, &mut buf).unwrap();
        assert_eq!(buf, page_filled(2));
    }

    #[test]
    fn unwritten_page_reads_as_zeroes_after_a_delete() {
        let (_dir, dm) = setup();
        dm.write_page(0, &page_filled(1)).unwrap();
        dm.delete_page(0);
        let mut data = page_filled(9);
        dm.read_page(1, &mut data).unwrap();
        assert_eq!(data, [0; PAGE_SIZE]);
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
    fn fills_every_slot_before_growing() {
        let (_dir, dm) = setup();
        for id in 0..DEFAULT_DB_IO_SIZE as PageId {
            dm.write_page(id, &page_filled(0)).unwrap();
        }
        let full = (DEFAULT_DB_IO_SIZE * PAGE_SIZE) as u64;
        assert_eq!(dm.db_file_size().unwrap(), full);
        dm.write_page(DEFAULT_DB_IO_SIZE as PageId, &page_filled(0))
            .unwrap();
        assert_eq!(dm.db_file_size().unwrap(), 2 * full);
    }

    #[test]
    fn rejects_invalid_page_id() {
        let (_dir, dm) = setup();
        let err = dm.write_page(INVALID_PAGE_ID, &page_filled(0)).unwrap_err();
        assert!(matches!(err, StorageError::InvalidPageId(INVALID_PAGE_ID)));
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

    /// The page directory the last save left in the `.meta` file, read
    /// without opening a second manager on the database.
    fn saved_pages(dm: &DiskManager) -> HashMap<PageId, PageEntry> {
        let mut saved = DiskState {
            pages: HashMap::new(),
            free_slots: Vec::new(),
            first_writes: 0,
            page_capacity: dm.state.lock().page_capacity,
            meta_dirty: false,
        };
        saved
            .load_meta(&fs::read(dm.meta_file_name()).unwrap())
            .unwrap();
        saved.pages
    }

    fn reopen(dir: &TempDir, dm: DiskManager) -> DiskManager {
        dm.shut_down().unwrap();
        drop(dm);
        DiskManager::new(dir.path().join("test.db")).unwrap()
    }

    #[test]
    fn a_second_manager_on_the_same_file_is_refused() {
        let (dir, dm) = setup();
        let path = dir.path().join("test.db");
        assert!(matches!(
            DiskManager::new(&path),
            Err(StorageError::DatabaseLocked(locked)) if locked == path
        ));
        drop(dm);
        DiskManager::new(&path).unwrap();
    }

    #[test]
    fn pages_survive_reopen() {
        let (dir, dm) = setup();
        let n = DEFAULT_DB_IO_SIZE * 3;
        for id in 0..n as PageId {
            dm.write_page(id, &page_filled(id as u8)).unwrap();
        }
        let size = dm.db_file_size().unwrap();
        let dm = reopen(&dir, dm);

        assert_eq!(dm.db_file_size().unwrap(), size);
        let mut buf = page_filled(0xff);
        for id in 0..n as PageId {
            dm.read_page(id, &mut buf).unwrap();
            assert_eq!(buf, page_filled(id as u8), "page {id}");
        }
    }

    #[test]
    fn free_slots_survive_reopen() {
        let (dir, dm) = setup();
        dm.write_page(0, &page_filled(1)).unwrap();
        dm.write_page(1, &page_filled(2)).unwrap();
        let offset = dm.state.lock().pages[&0].offset;
        dm.delete_page(0);
        let dm = reopen(&dir, dm);

        assert_eq!(dm.free_space(0), None);
        dm.write_page(2, &page_filled(3)).unwrap();
        assert_eq!(dm.state.lock().pages[&2].offset, offset);
        let mut buf = page_filled(0);
        dm.read_page(1, &mut buf).unwrap();
        assert_eq!(buf, page_filled(2));
    }

    #[test]
    fn free_space_survives_reopen() {
        let (dir, dm) = setup();
        dm.write_page(0, &page_filled(0)).unwrap();
        dm.write_page(1, &page_filled(0)).unwrap();
        assert_eq!(dm.free_space(0), Some(PAGE_SIZE));
        dm.set_free_space(0, 100).unwrap();
        assert!(matches!(
            dm.set_free_space(0, PAGE_SIZE + 1),
            Err(StorageError::InvalidFreeSpace(_))
        ));
        assert!(matches!(
            dm.set_free_space(9, 0),
            Err(StorageError::PageNotAllocated(9))
        ));
        let dm = reopen(&dir, dm);

        assert_eq!(dm.free_space(0), Some(100));
        assert_eq!(dm.free_space(1), Some(PAGE_SIZE));
    }

    #[test]
    fn unsaved_changes_are_lost_without_checkpoint() {
        let (dir, dm) = setup();
        dm.write_page(0, &page_filled(1)).unwrap();
        dm.checkpoint().unwrap();
        dm.write_page(1, &page_filled(2)).unwrap();
        drop(dm);

        let dm = DiskManager::new(dir.path().join("test.db")).unwrap();
        assert_eq!(dm.free_space(0), Some(PAGE_SIZE));
        assert_eq!(dm.free_space(1), None);
    }

    #[test]
    fn rejects_corrupt_meta_file() {
        let (dir, dm) = setup();
        dm.write_page(0, &page_filled(1)).unwrap();
        dm.shut_down().unwrap();
        drop(dm);

        let meta = dir.path().join("test.meta");
        let mut bytes = fs::read(&meta).unwrap();
        bytes[20] ^= 1;
        fs::write(&meta, bytes).unwrap();
        let err = DiskManager::new(dir.path().join("test.db")).err().unwrap();
        assert!(matches!(err, StorageError::CorruptMetadata(_)));
    }

    #[test]
    fn rejects_db_file_without_meta_file() {
        let (dir, dm) = setup();
        dm.shut_down().unwrap();
        drop(dm);

        fs::remove_file(dir.path().join("test.meta")).unwrap();
        let err = DiskManager::new(dir.path().join("test.db")).err().unwrap();
        assert!(matches!(err, StorageError::MissingMetadata(_)));
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

    #[test]
    fn io_on_one_page_does_not_block_others() {
        let (_dir, dm) = setup();
        let dm = Arc::new(dm);
        dm.write_page(0, &page_filled(1)).unwrap();
        dm.write_page(1, &page_filled(2)).unwrap();

        let stall = stall_io(&dm, 0);
        let stalled = {
            let dm = Arc::clone(&dm);
            thread::spawn(move || dm.write_page(0, &page_filled(3)).unwrap())
        };
        stall.wait_started();

        let other = Arc::clone(&dm);
        within_timeout(move || {
            let mut buf = page_filled(0);
            other.read_page(1, &mut buf).unwrap();
            assert_eq!(buf, page_filled(2));
            other.write_page(2, &page_filled(4)).unwrap();
            other.checkpoint().unwrap();
        });
        drop(stall);
        stalled.join().unwrap();

        let mut buf = page_filled(0);
        dm.read_page(0, &mut buf).unwrap();
        assert_eq!(buf, page_filled(3));
    }

    #[test]
    fn first_write_is_saved_only_once_done() {
        let (_dir, dm) = setup();
        let dm = Arc::new(dm);
        let stall = stall_io(&dm, 0);
        let stalled = {
            let dm = Arc::clone(&dm);
            thread::spawn(move || dm.write_page(0, &page_filled(1)).unwrap())
        };
        stall.wait_started();

        // The in-progress write keeps its slot from other pages, but is not
        // in the metadata a checkpoint saves.
        dm.write_page(1, &page_filled(2)).unwrap();
        dm.checkpoint().unwrap();
        assert_eq!(dm.free_space(0), None);
        let saved = saved_pages(&dm);
        assert!(!saved.contains_key(&0));
        assert_eq!(saved[&1].free_space, PAGE_SIZE as u32);

        drop(stall);
        stalled.join().unwrap();
        let state = dm.state.lock();
        assert_ne!(state.pages[&0].offset, state.pages[&1].offset);
        assert_eq!(state.first_writes, 0);
    }
}
