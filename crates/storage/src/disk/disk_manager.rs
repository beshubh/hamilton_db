use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use common::PageId;
use parking_lot::Mutex;

use crate::disk::fs::{Fs, FsFile, OsFs};
use crate::error::{Result, StorageError};
use crate::page::{PAGE_SIZE, TEMP_PAGE_BASE};

/// Number of pages the db file has room for when first created.
pub const DEFAULT_DB_IO_SIZE: usize = 16;

const META_MAGIC: u64 = u64::from_le_bytes(*b"HAMDBMET");
const META_VERSION: u32 = 2;

/// Handles allocating and freeing pages in the database file, and reading and
/// writing them.
///
/// Space is allocated lazily: a page only gets a slot in the file the first
/// time it is written.
///
/// Reads and writes hold the lock only to look up or allocate the page's
/// slot, and run the `pread`/`pwrite` without it, so I/O on different pages
/// runs in parallel. Callers must not run two requests for the same page at
/// once, or delete a page while a request for it is running, since a freed
/// slot can be given to another page. `DiskScheduler` keeps each page's
/// requests in order.
///
/// A slot a durable page frees is quarantined until a `.meta` that no
/// longer lists the page is durable, so a saved directory never maps a page
/// to bytes another page wrote (docs/internals/recovery.md).
pub struct DiskManager {
    db_file_name: PathBuf,
    log_file_name: PathBuf,
    meta_file_name: PathBuf,
    fs: Arc<dyn Fs>,
    /// Only read and written with positioned I/O, so it needs no lock.
    db_io: Box<dyn FsFile>,
    state: Mutex<DiskState>,
    /// The checkpoint the `.meta` file held when it was opened, empty for a
    /// new database.
    opened_checkpoint: Vec<u8>,
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
    pages: HashMap<PageId, u64>,
    /// Offsets no page holds, ready for reuse.
    free_slots: Vec<u64>,
    /// Offsets of durable pages deleted since the directory was last copied
    /// for a save. The `.meta` on disk may still map them to their pages.
    quarantine: Vec<u64>,
    /// Slots handed out so far. Each slot below it is held by a page, free,
    /// quarantined, or taken by a first write still in progress.
    slot_end: usize,
    /// Number of pages the db file currently has room for.
    page_capacity: usize,
}

/// The durable pages' slots at one moment, for `save_meta`.
pub(crate) struct Directory {
    pages: Vec<(PageId, u64)>,
    slot_end: usize,
    /// How many slots were quarantined when the copy was taken. None of
    /// their pages is in the copy, so saving it releases them.
    quarantined: usize,
}

impl DiskManager {
    /// Opens (or creates) `db_file` on the real file system. See `open`.
    pub fn new(db_file: impl AsRef<Path>) -> Result<Self> {
        Self::open(Arc::new(OsFs), db_file)
    }

    /// Opens (or creates) `db_file` on `fs`, with a `.meta` file next to it.
    /// The page directory of an existing database is the one its last
    /// checkpoint saved: pages written since are found by recovery
    /// (`storage::open`). Fails with `DatabaseLocked` while another manager
    /// has `db_file` open.
    pub fn open(fs: Arc<dyn Fs>, db_file: impl AsRef<Path>) -> Result<Self> {
        let db_file_name = db_file.as_ref().to_path_buf();
        let log_file_name = db_file_name.with_extension("log");
        let meta_file_name = db_file_name.with_extension("meta");

        let db_io = fs.open(&db_file_name)?;
        // Two managers on one file would hand out the same slots and
        // overwrite each other's `.meta`. The lock goes when `db_io` closes.
        if !db_io.try_lock()? {
            return Err(StorageError::DatabaseLocked(db_file_name));
        }

        let db_size = db_io.size()?;
        let (mut state, opened_checkpoint) = match fs.read(&meta_file_name)? {
            Some(bytes) => DiskState::load(&bytes)?,
            // The meta file is saved before any page is written, so a db
            // file with data but no meta file was not made by us.
            None if db_size != 0 => return Err(StorageError::MissingMetadata(meta_file_name)),
            None => {
                let state = DiskState::new();
                fs.replace(&meta_file_name, &encode_meta(&state.directory(), &[]))?;
                (state, Vec::new())
            }
        };
        state.page_capacity = (db_size as usize / PAGE_SIZE)
            .max(state.slot_end)
            .max(DEFAULT_DB_IO_SIZE);
        let file_size = file_size_for(state.page_capacity);
        if db_size < file_size {
            db_io.set_size(file_size)?;
        }

        Ok(Self {
            db_file_name,
            log_file_name,
            meta_file_name,
            fs,
            db_io,
            state: Mutex::new(state),
            opened_checkpoint,
            num_reads: AtomicUsize::new(0),
            num_writes: AtomicUsize::new(0),
            num_deletes: AtomicUsize::new(0),
            #[cfg(test)]
            before_io: Mutex::new(None),
        })
    }

    /// The checkpoint the `.meta` file held when it was opened, empty for a
    /// new database. Only `wal::checkpoint` knows what is in it.
    pub(crate) fn opened_checkpoint(&self) -> &[u8] {
        &self.opened_checkpoint
    }

    /// Copies the durable pages' slots, for `save_meta`.
    pub(crate) fn directory(&self) -> Directory {
        self.state.lock().directory()
    }

    /// Replaces the `.meta` file with `directory` and `checkpoint`, then
    /// releases the slots quarantined when `directory` was copied. The copy
    /// must be newer than any saved before, and its pages' writes durable
    /// (`sync`).
    pub(crate) fn save_meta(&self, directory: &Directory, checkpoint: &[u8]) -> Result<()> {
        self.fs
            .replace(&self.meta_file_name, &encode_meta(directory, checkpoint))?;
        let mut state = self.state.lock();
        let DiskState {
            quarantine,
            free_slots,
            ..
        } = &mut *state;
        free_slots.extend(quarantine.drain(..directory.quarantined));
        Ok(())
    }

    /// Makes every page write so far durable.
    pub(crate) fn sync(&self) -> Result<()> {
        Ok(self.db_io.sync()?)
    }

    /// One past the highest durable page id that has a slot in the db file,
    /// or 0 for an empty database. Ids handed out but never written are not
    /// counted.
    pub fn next_page_id(&self) -> PageId {
        let state = self.state.lock();
        let durable = state.pages.keys().filter(|&&id| id < TEMP_PAGE_BASE);
        durable.max().map_or(0, |&max| max + 1)
    }

    /// Writes `page_data` to the page's slot in the db file, giving the page
    /// a slot first if it does not have one yet.
    pub fn write_page(&self, page_id: PageId, page_data: &[u8; PAGE_SIZE]) -> Result<()> {
        check_page_id(page_id)?;
        let (offset, first_write) = self.state.lock().slot_for_write(page_id, &*self.db_io)?;

        #[cfg(test)]
        self.run_before_io(page_id);
        let written = self.db_io.write_at(page_data, offset);
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
        let offset = self.state.lock().pages.get(&page_id).copied();
        let Some(offset) = offset else {
            page_data.fill(0);
            return Ok(());
        };

        #[cfg(test)]
        self.run_before_io(page_id);
        let n = self.db_io.read_at(page_data, offset)?;
        page_data[n..].fill(0);
        self.num_reads.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Whether `page_id` has been written and not deleted since: whether it
    /// has a slot on disk.
    pub fn has_page(&self, page_id: PageId) -> bool {
        self.state.lock().pages.contains_key(&page_id)
    }

    /// Frees the page's slot so a later page can have it. Unknown pages are
    /// ignored.
    pub fn delete_page(&self, page_id: PageId) {
        let mut state = self.state.lock();
        if let Some(offset) = state.pages.remove(&page_id) {
            if page_id < TEMP_PAGE_BASE {
                state.quarantine.push(offset);
            } else {
                state.free_slots.push(offset);
            }
            self.num_deletes.fetch_add(1, Ordering::Relaxed);
        }
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

    pub(crate) fn fs(&self) -> &dyn Fs {
        &*self.fs
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
        Ok(self.db_io.size()?)
    }
}

impl DiskState {
    fn new() -> Self {
        Self {
            pages: HashMap::new(),
            free_slots: Vec::new(),
            quarantine: Vec::new(),
            slot_end: 0,
            page_capacity: DEFAULT_DB_IO_SIZE,
        }
    }

    fn directory(&self) -> Directory {
        let durable = self.pages.iter().filter(|&(&id, _)| id < TEMP_PAGE_BASE);
        Directory {
            pages: durable.map(|(&id, &offset)| (id, offset)).collect(),
            slot_end: self.slot_end,
            quarantined: self.quarantine.len(),
        }
    }

    /// Returns the page's offset, and whether this is its first write. A
    /// first write gets a new slot, and must be ended with
    /// `finish_first_write`.
    fn slot_for_write(&mut self, page_id: PageId, db_io: &dyn FsFile) -> Result<(u64, bool)> {
        if let Some(&offset) = self.pages.get(&page_id) {
            return Ok((offset, false));
        }
        Ok((self.allocate_slot(db_io)?, true))
    }

    /// Adds the page at `offset` to `pages`, or frees the slot if the write
    /// failed. Until then a checkpoint does not save the slot as the page's,
    /// since it may not hold the page yet.
    fn finish_first_write(&mut self, page_id: PageId, offset: u64, written: bool) {
        if written {
            self.pages.insert(page_id, offset);
        } else {
            self.free_slots.push(offset);
        }
    }

    /// Returns a free offset, doubling the db file when it is full.
    fn allocate_slot(&mut self, db_io: &dyn FsFile) -> Result<u64> {
        if let Some(offset) = self.free_slots.pop() {
            return Ok(offset);
        }
        let slot = self.slot_end;
        if slot >= self.page_capacity {
            db_io.set_size(file_size_for(self.page_capacity * 2))?;
            self.page_capacity *= 2;
        }
        self.slot_end += 1;
        Ok((slot * PAGE_SIZE) as u64)
    }

    /// Loads the directory saved in a `.meta` file, and the checkpoint saved
    /// with it. Every slot below the high-water mark that no page holds is
    /// free.
    fn load(bytes: &[u8]) -> Result<(Self, Vec<u8>)> {
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
        let slot_end = usize::try_from(r.u64()?).map_err(|_| corrupt_meta("too many slots"))?;

        let mut held = vec![false; slot_end];
        let mut pages = HashMap::new();
        for _ in 0..r.u64()? {
            let page_id = r.u32()? as PageId;
            let offset = r.u64()?;
            let slot = (offset / PAGE_SIZE as u64) as usize;
            let bad = !(0..TEMP_PAGE_BASE).contains(&page_id)
                || !offset.is_multiple_of(PAGE_SIZE as u64)
                || slot >= slot_end
                || std::mem::replace(&mut held[slot], true)
                || pages.insert(page_id, offset).is_some();
            if bad {
                return Err(corrupt_meta(format!("bad entry for page {page_id}")));
            }
        }
        let checkpoint_len = usize::try_from(r.u64()?).map_err(|_| corrupt_meta("bad length"))?;
        let checkpoint = r.bytes(checkpoint_len)?.to_vec();
        if !r.0.is_empty() {
            return Err(corrupt_meta("trailing bytes"));
        }

        let free_slots = (0..slot_end)
            .rev()
            .filter(|&slot| !held[slot])
            .map(|slot| (slot * PAGE_SIZE) as u64)
            .collect();
        let state = Self {
            pages,
            free_slots,
            quarantine: Vec::new(),
            slot_end,
            page_capacity: DEFAULT_DB_IO_SIZE,
        };
        Ok((state, checkpoint))
    }
}

/// Lays a `.meta` file out as `magic: u64, version: u32, _pad: u32,
/// slot_end: u64, num_pages: u64, (page_id: i32, offset: u64)*,
/// checkpoint_len: u64, checkpoint, checksum: u64`, all little-endian.
fn encode_meta(directory: &Directory, checkpoint: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(48 + directory.pages.len() * 12 + checkpoint.len());
    bytes.extend_from_slice(&META_MAGIC.to_le_bytes());
    bytes.extend_from_slice(&META_VERSION.to_le_bytes());
    bytes.extend_from_slice(&0u32.to_le_bytes());
    bytes.extend_from_slice(&(directory.slot_end as u64).to_le_bytes());
    bytes.extend_from_slice(&(directory.pages.len() as u64).to_le_bytes());
    for &(page_id, offset) in &directory.pages {
        bytes.extend_from_slice(&page_id.to_le_bytes());
        bytes.extend_from_slice(&offset.to_le_bytes());
    }
    bytes.extend_from_slice(&(checkpoint.len() as u64).to_le_bytes());
    bytes.extend_from_slice(checkpoint);
    let checksum = fnv1a(&bytes);
    bytes.extend_from_slice(&checksum.to_le_bytes());
    bytes
}

/// Reads little-endian integers off the front of a byte slice.
struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn bytes(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.0.len() < n {
            return Err(corrupt_meta("file is truncated"));
        }
        let (head, rest) = self.0.split_at(n);
        self.0 = rest;
        Ok(head)
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.bytes(4)?.try_into().unwrap()))
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.bytes(8)?.try_into().unwrap()))
    }
}

fn fnv1a(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |hash, &b| {
        (hash ^ b as u64).wrapping_mul(0x0100_0000_01b3)
    })
}

fn corrupt_meta(msg: impl Into<String>) -> StorageError {
    StorageError::CorruptMetadata(msg.into())
}

fn file_size_for(page_capacity: usize) -> u64 {
    (page_capacity * PAGE_SIZE) as u64
}

fn check_page_id(page_id: PageId) -> Result<()> {
    if page_id < 0 {
        return Err(StorageError::InvalidPageId(page_id));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::{stall_io, within_timeout};
    use common::INVALID_PAGE_ID;
    use std::fs;
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

    fn offset(dm: &DiskManager, page_id: PageId) -> u64 {
        dm.state.lock().pages[&page_id]
    }

    /// Saves the directory as a checkpoint does, with `checkpoint` as its
    /// record.
    fn save(dm: &DiskManager, checkpoint: &[u8]) {
        dm.sync().unwrap();
        dm.save_meta(&dm.directory(), checkpoint).unwrap();
    }

    /// The directory and checkpoint the last save left in the `.meta` file,
    /// read without opening a second manager on the database.
    fn saved(dm: &DiskManager) -> (HashMap<PageId, u64>, Vec<u8>) {
        let (state, checkpoint) = DiskState::load(&fs::read(dm.meta_file_name()).unwrap()).unwrap();
        (state.pages, checkpoint)
    }

    fn reopen(dir: &TempDir, dm: DiskManager) -> DiskManager {
        save(&dm, b"");
        drop(dm);
        DiskManager::new(dir.path().join("test.db")).unwrap()
    }

    #[test]
    fn creates_db_and_meta_files() {
        let (dir, dm) = setup();
        assert!(dir.path().join("test.db").exists());
        assert!(dir.path().join("test.meta").exists());
        assert_eq!(dm.log_file_name(), dir.path().join("test.log"));
        assert_eq!(
            dm.db_file_size().unwrap(),
            (DEFAULT_DB_IO_SIZE * PAGE_SIZE) as u64
        );
        assert!(dm.opened_checkpoint().is_empty());
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
    fn a_temporary_pages_slot_is_reused_at_once() {
        let (_dir, dm) = setup();
        dm.write_page(TEMP_PAGE_BASE, &page_filled(1)).unwrap();
        dm.write_page(1, &page_filled(2)).unwrap();
        let temp = offset(&dm, TEMP_PAGE_BASE);

        dm.delete_page(TEMP_PAGE_BASE);
        dm.delete_page(42); // unknown page is a no-op
        assert_eq!(dm.num_deletes(), 1);

        dm.write_page(2, &page_filled(3)).unwrap();
        assert_eq!(offset(&dm, 2), temp);
        let mut buf = page_filled(0);
        dm.read_page(1, &mut buf).unwrap();
        assert_eq!(buf, page_filled(2));
    }

    #[test]
    fn a_freed_slot_waits_for_a_save_that_no_longer_lists_its_page() {
        let (_dir, dm) = setup();
        dm.write_page(0, &page_filled(1)).unwrap();
        save(&dm, b"");
        let freed = offset(&dm, 0);
        dm.delete_page(0);

        // The `.meta` on disk still maps page 0 to its slot.
        dm.write_page(1, &page_filled(2)).unwrap();
        assert_ne!(offset(&dm, 1), freed);
        // Copied before this save, so still listed, until the next.
        dm.write_page(2, &page_filled(3)).unwrap();
        let listing_two = dm.directory();
        dm.delete_page(2);
        dm.sync().unwrap();
        dm.save_meta(&listing_two, b"").unwrap();
        dm.write_page(3, &page_filled(4)).unwrap();
        assert_eq!(offset(&dm, 3), freed);
        assert!(saved(&dm).0.contains_key(&2));
        dm.write_page(4, &page_filled(5)).unwrap();
        let two = listing_two
            .pages
            .iter()
            .find(|&&(id, _)| id == 2)
            .unwrap()
            .1;
        assert_ne!(offset(&dm, 4), two);

        save(&dm, b"");
        dm.write_page(5, &page_filled(6)).unwrap();
        assert_eq!(offset(&dm, 5), two);
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
    fn pages_and_the_checkpoint_survive_reopen() {
        let (dir, dm) = setup();
        let n = DEFAULT_DB_IO_SIZE * 3;
        for id in 0..n as PageId {
            dm.write_page(id, &page_filled(id as u8)).unwrap();
        }
        let size = dm.db_file_size().unwrap();
        save(&dm, b"checkpoint");
        drop(dm);
        let dm = DiskManager::new(dir.path().join("test.db")).unwrap();

        assert_eq!(dm.opened_checkpoint(), b"checkpoint");
        assert_eq!(dm.db_file_size().unwrap(), size);
        let mut buf = page_filled(0xff);
        for id in 0..n as PageId {
            dm.read_page(id, &mut buf).unwrap();
            assert_eq!(buf, page_filled(id as u8), "page {id}");
        }
    }

    #[test]
    fn slots_no_saved_page_holds_are_free_after_reopen() {
        let (dir, dm) = setup();
        for id in 0..3 {
            dm.write_page(id, &page_filled(id as u8)).unwrap();
        }
        let freed = offset(&dm, 1);
        dm.delete_page(1);
        let dm = reopen(&dir, dm);

        assert!(!dm.has_page(1));
        dm.write_page(3, &page_filled(3)).unwrap();
        assert_eq!(offset(&dm, 3), freed);
        dm.write_page(4, &page_filled(4)).unwrap();
        assert_eq!(
            offset(&dm, 4),
            3 * PAGE_SIZE as u64,
            "past the high-water mark"
        );
        let mut buf = page_filled(0);
        dm.read_page(2, &mut buf).unwrap();
        assert_eq!(buf, page_filled(2));
    }

    #[test]
    fn temporary_pages_are_never_saved() {
        let (dir, dm) = setup();
        dm.write_page(TEMP_PAGE_BASE, &page_filled(1)).unwrap();
        dm.write_page(0, &page_filled(2)).unwrap();
        let temp_offset = offset(&dm, TEMP_PAGE_BASE);
        assert_eq!(dm.next_page_id(), 1);
        let dm = reopen(&dir, dm);

        assert!(!dm.has_page(TEMP_PAGE_BASE));
        assert_eq!(dm.next_page_id(), 1);
        dm.write_page(1, &page_filled(3)).unwrap();
        assert_eq!(offset(&dm, 1), temp_offset);
    }

    #[test]
    fn unsaved_changes_are_lost_without_a_save() {
        let (dir, dm) = setup();
        dm.write_page(0, &page_filled(1)).unwrap();
        save(&dm, b"");
        dm.write_page(1, &page_filled(2)).unwrap();
        drop(dm);

        let dm = DiskManager::new(dir.path().join("test.db")).unwrap();
        assert!(dm.has_page(0));
        assert!(!dm.has_page(1));
    }

    #[test]
    fn rejects_corrupt_meta_file() {
        let (dir, dm) = setup();
        dm.write_page(0, &page_filled(1)).unwrap();
        save(&dm, b"");
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
            save(&other, b"");
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
        // in the directory a save copies.
        dm.write_page(1, &page_filled(2)).unwrap();
        save(&dm, b"");
        assert!(!dm.has_page(0));
        let (saved, _) = saved(&dm);
        assert!(!saved.contains_key(&0));
        assert!(saved.contains_key(&1));

        drop(stall);
        stalled.join().unwrap();
        assert_ne!(offset(&dm, 0), offset(&dm, 1));
    }
}
