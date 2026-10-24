use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

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
/// time it is read or written. Offsets of deleted pages are reused by later
/// allocations.
///
/// The page directory (each page's offset and free space) and the free-slot
/// list are kept in memory and saved to a separate `.meta` file by
/// `checkpoint` and `shut_down`. Changes since the last save are lost if the
/// process stops without one.
pub struct DiskManager {
    db_file_name: PathBuf,
    log_file_name: PathBuf,
    meta_file_name: PathBuf,
    state: Mutex<DiskState>,
    log: Mutex<LogState>,
    flush_log: AtomicBool,
}

struct DiskState {
    db_io: File,
    /// Where each allocated page lives in the db file.
    pages: HashMap<PageId, PageEntry>,
    /// Offsets left behind by deleted pages, ready for reuse.
    free_slots: Vec<u64>,
    /// Number of pages the db file currently has room for.
    page_capacity: usize,
    /// Whether the metadata changed since it was last saved.
    meta_dirty: bool,
    num_reads: usize,
    num_writes: usize,
    num_deletes: usize,
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
    /// its `.meta` file.
    pub fn new(db_file: impl AsRef<Path>) -> Result<Self> {
        let db_file_name = db_file.as_ref().to_path_buf();
        let log_file_name = db_file_name.with_extension("log");
        let meta_file_name = db_file_name.with_extension("meta");

        let db_io = open_rw(&db_file_name)?;
        let log_io = open_rw(&log_file_name)?;

        // The capacity is not saved; it follows from the db file's length.
        let db_len = db_io.metadata()?.len() as usize;
        let mut state = DiskState {
            db_io,
            pages: HashMap::new(),
            free_slots: Vec::new(),
            page_capacity: (db_len / PAGE_SIZE).max(DEFAULT_DB_IO_SIZE),
            meta_dirty: false,
            num_reads: 0,
            num_writes: 0,
            num_deletes: 0,
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
            state.db_io.set_len(file_len)?;
        }

        Ok(Self {
            db_file_name,
            log_file_name,
            meta_file_name,
            state: Mutex::new(state),
            log: Mutex::new(LogState {
                log_io,
                num_flushes: 0,
            }),
            flush_log: AtomicBool::new(false),
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
        let mut state = self.state.lock();
        state.db_io.sync_all()?;
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
        let mut state = self.state.lock();
        let offset = state.offset_or_allocate(page_id)?;

        state.db_io.seek(SeekFrom::Start(offset))?;
        state.db_io.write_all(page_data)?;
        state.db_io.flush()?;
        state.num_writes += 1;
        Ok(())
    }

    /// Reads the page into `page_data`. A page that has never been written
    /// reads as all zeroes. It gets no slot until it is written: a slot
    /// freed by `delete_page` still holds the deleted page's bytes.
    pub fn read_page(&self, page_id: PageId, page_data: &mut [u8; PAGE_SIZE]) -> Result<()> {
        check_page_id(page_id)?;
        let mut state = self.state.lock();
        let Some(offset) = state.pages.get(&page_id).map(|entry| entry.offset) else {
            page_data.fill(0);
            return Ok(());
        };

        state.db_io.seek(SeekFrom::Start(offset))?;
        let n = read_full(&mut state.db_io, page_data)?;
        page_data[n..].fill(0);
        state.num_reads += 1;
        Ok(())
    }

    /// Frees the page's slot so a later allocation can reuse it. Unknown pages
    /// are ignored.
    pub fn delete_page(&self, page_id: PageId) {
        let mut state = self.state.lock();
        if let Some(entry) = state.pages.remove(&page_id) {
            state.free_slots.push(entry.offset);
            state.meta_dirty = true;
            state.num_deletes += 1;
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
        self.state.lock().num_reads
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

    pub fn meta_file_name(&self) -> &Path {
        &self.meta_file_name
    }

    /// Size of the db file on disk, in bytes.
    pub fn db_file_size(&self) -> Result<u64> {
        Ok(self.state.lock().db_io.metadata()?.len())
    }
}

impl DiskState {
    fn offset_or_allocate(&mut self, page_id: PageId) -> Result<u64> {
        if let Some(entry) = self.pages.get(&page_id) {
            return Ok(entry.offset);
        }
        let offset = self.allocate_page()?;
        let free_space = PAGE_SIZE as u32;
        self.pages.insert(page_id, PageEntry { offset, free_space });
        self.meta_dirty = true;
        Ok(offset)
    }

    /// Returns a free offset, doubling the db file when it is full.
    fn allocate_page(&mut self) -> Result<u64> {
        if let Some(offset) = self.free_slots.pop() {
            return Ok(offset);
        }
        // With no free slots, every slot below `pages.len()` is in use.
        if self.pages.len() >= self.page_capacity {
            self.page_capacity *= 2;
            self.db_io.set_len(file_len_for(self.page_capacity))?;
        }
        Ok((self.pages.len() * PAGE_SIZE) as u64)
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

    fn reopen(dir: &TempDir, dm: DiskManager) -> DiskManager {
        dm.shut_down().unwrap();
        drop(dm);
        DiskManager::new(dir.path().join("test.db")).unwrap()
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
}
