//! Crash recovery: whatever moment a crash comes at, and whatever unsynced
//! writes it loses or tears, opening the database again leaves every page
//! exactly as the records that survived in the log say: its last image,
//! with the ops logged after it made on it.

#[path = "util/crash_fs.rs"]
mod crash_fs;

use std::collections::BTreeMap;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use common::{Lsn, PageId, Rid, TXN_UNCOMMITTED_BIT};
use crash_fs::{CrashFs, Lost, next};
use storage::Storage;
use storage::buffer::arc_replacer::AccessType;
use storage::buffer::buffer_pool_manager::BufferPoolManager;
use storage::disk::disk_manager::DiskManager;
use storage::page::PAGE_SIZE;
use storage::page::btree_leaf_page::BtreeLeafPage;
use storage::page::btree_page::BytewiseComparator;
use storage::page::page_header::{PageHeader, page_lsn, verify_checksum};
use storage::table::table_page::{TablePage, TupleMeta};
use storage::wal::checkpoint;
use storage::wal::log::{FIRST_LSN, Log};
use storage::wal::record::{Kind, PageChanges, PageRedo};
use storage::wal::{Logged, MtrPage, PageOp};

const DIR: &str = "/db";
const DB: &str = "/db/test.db";
const LOG: &str = "/db/test.log";
const META: &str = "/db/test.meta";

/// Small, so a few records fill a segment and the workloads here start and
/// remove many.
const SEGMENT: u64 = 32 << 10;

/// Where tests put their bytes: past the page header.
const AT: usize = PageHeader::SIZE;

/// The checksum is stamped on the copy of a page written to disk, so it is
/// the one part of a page its image need not match.
const CHECKSUM: Range<usize> = 8..12;

fn open(fs: &CrashFs, frames: usize) -> Storage {
    storage::open_with(Arc::new(fs.clone()), DB, frames, None, SEGMENT).unwrap()
}

/// The numbers of the log's segments in `fs`, oldest first.
fn segments(fs: &CrashFs) -> Vec<u64> {
    let prefix = format!("{LOG}.");
    fs.files_in(DIR.as_ref())
        .iter()
        .filter_map(|path| path.to_str()?.strip_prefix(&prefix)?.parse().ok())
        .collect()
}

fn segment_path(n: u64) -> PathBuf {
    PathBuf::from(format!("{LOG}.{n:08}"))
}

/// Each page the log names: the page its records leave and the last one's
/// LSN, or `None` once a record frees it.
type Pages = BTreeMap<PageId, Option<(Lsn, Box<[u8; PAGE_SIZE]>)>>;

/// The pages the records in `fs`'s log leave, and where the log ends: what
/// recovery must bring back. Read from every segment the log ever had,
/// removed ones too, so a page whose last record a checkpoint removed is
/// still checked.
fn logged_pages(fs: &CrashFs) -> (Pages, Lsn) {
    let log = Log::open(Arc::new(fs.with_removed()), LOG, SEGMENT).unwrap();
    assert_eq!(log.first_lsn(), FIRST_LSN, "a segment is missing");
    let mut pages = Pages::new();
    let mut scan = log.scan(FIRST_LSN);
    while let Some(record) = scan.next_record().unwrap() {
        assert_eq!(record.kind, Kind::System);
        let changes = PageChanges::decode(record.body).unwrap();
        for (page_id, redo) in changes.pages() {
            let page = pages.entry(page_id).or_insert(None);
            let data = match page {
                Some((lsn, data)) => {
                    *lsn = record.lsn;
                    data
                }
                None => {
                    // Whatever was there before a page's first record does
                    // not matter: it replaces the whole page.
                    assert!(
                        redo.replaces_page(),
                        "page {page_id} starts at {} with ops",
                        record.lsn
                    );
                    &mut page.insert((record.lsn, Box::new([0; PAGE_SIZE]))).1
                }
            };
            match redo {
                PageRedo::Image(image) => **data = *image,
                PageRedo::Ops(ops) => ops.apply(data).unwrap(),
            }
        }
        for page_id in changes.frees() {
            pages.insert(page_id, None);
        }
    }
    (pages, log.end())
}

/// How many page entries in `fs`'s log, every segment it ever had, are
/// images and how many are ops.
fn forms(fs: &CrashFs) -> (usize, usize) {
    let log = Log::open(Arc::new(fs.with_removed()), LOG, SEGMENT).unwrap();
    let (mut images, mut ops) = (0, 0);
    let mut scan = log.scan(FIRST_LSN);
    while let Some(record) = scan.next_record().unwrap() {
        for (_, redo) in PageChanges::decode(record.body).unwrap().pages() {
            match redo {
                PageRedo::Image(_) => images += 1,
                PageRedo::Ops(_) => ops += 1,
            }
        }
    }
    (images, ops)
}

/// Asserts that `bpm` holds exactly `pages`: each logged page as its
/// records leave it, no slot for a freed page or one below `allocated` the
/// log never named, and page ids handed out from now on past all of them.
fn assert_recovered(bpm: &BufferPoolManager, pages: &Pages, allocated: PageId, at: &str) {
    for (&page_id, expected) in pages {
        let Some((lsn, image)) = expected else {
            assert!(
                !bpm.disk_manager().has_page(page_id),
                "{at}: freed page {page_id} is back"
            );
            continue;
        };
        let guard = bpm
            .checked_read_page(page_id, AccessType::Unknown)
            .unwrap_or_else(|e| panic!("{at}: page {page_id}: {e}"));
        let data = guard.get_data();
        assert_eq!(page_lsn(data), *lsn, "{at}: LSN of page {page_id}");
        assert!(
            data[CHECKSUM.end..] == image[CHECKSUM.end..],
            "{at}: page {page_id} is not as the records up to {lsn} leave it"
        );
    }
    for page_id in (0..allocated).filter(|id| !pages.contains_key(id)) {
        assert!(
            !bpm.disk_manager().has_page(page_id),
            "{at}: page {page_id} was never logged but has a slot"
        );
    }
    let highest = pages.keys().max().copied().unwrap_or(-1);
    assert!(bpm.new_page() > highest, "{at}: page ids went backwards");
}

/// Recovers a copy of `image` and checks it against what its log says,
/// then, if `reopen`, closes and opens it again and checks that too.
fn recover_and_check(image: &CrashFs, allocated: PageId, reopen: bool, at: &str) {
    let (pages, end) = logged_pages(image);
    let storage = open(image, 4);
    assert_eq!(storage.bpm.log().end(), end, "{at}: log end");
    assert_eq!(segments(image).len(), 1, "{at}: segments left");
    assert_recovered(&storage.bpm, &pages, allocated, at);
    if reopen {
        storage.close().unwrap();
        let storage = open(image, 4);
        assert_recovered(&storage.bpm, &pages, allocated, &format!("{at}, reopened"));
    }
}

/// How the workload changes a page: raw bytes, logged as images, or the
/// ops of a table page or a B+tree leaf.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Shape {
    Raw,
    Heap,
    Leaf,
}

impl Shape {
    fn of(data: &[u8; PAGE_SIZE]) -> Self {
        if TablePage::new(data).is_ok() {
            Self::Heap
        } else if Leaf::new(data).is_ok() {
            Self::Leaf
        } else {
            Self::Raw
        }
    }
}

/// Leaves keep tombstones, so the leaf ops revive and give them up too.
const NUM_TOMBS: usize = 2;

type Leaf<'a> = BtreeLeafPage<&'a [u8; PAGE_SIZE], NUM_TOMBS>;

/// Pages changed, created, freed, flushed and checkpointed at random,
/// through a pool too small to hold them, so pages are written before
/// their changes are durable and slots are freed and reused.
struct Workload {
    storage: Storage,
    rng: u64,
    live: Vec<(PageId, Shape)>,
}

impl Workload {
    fn new(fs: &CrashFs, seed: u64) -> Self {
        Self {
            storage: open(fs, 4),
            rng: seed,
            live: Vec::new(),
        }
    }

    /// Opens the database in `fs` again and goes on with the pages its log
    /// left.
    fn resume(fs: &CrashFs, seed: u64) -> Self {
        let (pages, _) = logged_pages(fs);
        let live = pages
            .iter()
            .filter_map(|(&page_id, page)| Some((page_id, Shape::of(&page.as_ref()?.1))));
        Self {
            live: live.collect(),
            ..Self::new(fs, seed)
        }
    }

    fn bpm(&self) -> &BufferPoolManager {
        &self.storage.bpm
    }

    fn below(&mut self, n: usize) -> usize {
        next(&mut self.rng) as usize % n.max(1)
    }

    fn pick(&mut self) -> (PageId, Shape) {
        let at = self.below(self.live.len());
        self.live[at]
    }

    fn scribble(&mut self, data: &mut [u8; PAGE_SIZE]) {
        let at = AT + self.below(PAGE_SIZE - AT - 8);
        data[at..at + 8].copy_from_slice(&next(&mut self.rng).to_le_bytes());
    }

    fn run(&mut self, steps: usize) {
        for _ in 0..steps {
            self.step();
        }
    }

    fn step(&mut self) {
        let choice = next(&mut self.rng) % 20;
        match choice {
            _ if self.live.len() < 3 || choice < 3 => self.create(),
            3..=9 => self.change(),
            10..=11 => self.free(),
            12..=14 => {
                let (page_id, _) = self.pick();
                self.bpm().flush_page(page_id).unwrap();
            }
            15 => checkpoint::fuzzy(self.bpm()).unwrap(),
            16 => self.bpm().checkpoint().unwrap(),
            17 => {
                // A page id handed out that nothing ever writes.
                self.bpm().new_page();
            }
            _ => self.bpm().log().flush_all().unwrap(),
        }
    }

    /// Creates, changes or frees a page: one mtr, and nothing else. Creates
    /// more than it frees, so the pages soon outgrow the pool.
    fn write(&mut self) {
        match next(&mut self.rng) % 5 {
            _ if self.live.len() < 3 => self.create(),
            0 | 1 => self.create(),
            2 => self.free(),
            _ => self.change(),
        }
    }

    fn create(&mut self) {
        let bpm = Arc::clone(&self.storage.bpm);
        let mtr = bpm.mtr(Logged::System);
        let mut page = mtr.new_page().unwrap();
        let shape = [Shape::Raw, Shape::Heap, Shape::Leaf][self.below(3)];
        match shape {
            Shape::Raw => self.scribble(page.raw_mut()),
            Shape::Heap => page.apply(PageOp::HeapInit).unwrap(),
            Shape::Leaf => drop(BtreeLeafPage::<_, NUM_TOMBS>::init(page.raw_mut())),
        }
        for _ in 0..self.below(3) {
            self.change_page(&mut page, shape);
        }
        self.live.push((page.page_id(), shape));
    }

    /// Changes up to three pages in one mtr, most often ones of the last
    /// few it changed, which are then still dirty and log their ops.
    fn change(&mut self) {
        let bpm = Arc::clone(&self.storage.bpm);
        let mtr = bpm.mtr(Logged::System);
        let mut changed = Vec::new();
        for _ in 0..1 + self.below(3) {
            let (page_id, shape) = if self.below(4) > 0 {
                let recent = self.live.len().saturating_sub(3);
                let at = recent + self.below(self.live.len() - recent);
                self.live[at]
            } else {
                self.pick()
            };
            // Keeps `live` in the order pages last changed.
            self.live.retain(|&(id, _)| id != page_id);
            self.live.push((page_id, shape));
            if !changed.contains(&page_id) {
                changed.push(page_id);
                let mut page = mtr.write_page(page_id).unwrap();
                for _ in 0..1 + self.below(3) {
                    self.change_page(&mut page, shape);
                }
            }
        }
    }

    /// Frees a page and changes another in the same mtr, as a merge does.
    fn free(&mut self) {
        let (freed, _) = self.pick();
        self.live.retain(|&(id, _)| id != freed);
        let (other, shape) = self.pick();
        let bpm = Arc::clone(&self.storage.bpm);
        let mtr = bpm.mtr(Logged::System);
        self.change_page(&mut mtr.write_page(other).unwrap(), shape);
        mtr.free_page(freed);
    }

    /// Makes one change to `page`. A raw page gets a scribble, and a table
    /// page or a leaf gets an op, or now and then a change through
    /// `raw_mut`. An op the page has no room for is left out.
    fn change_page(&mut self, page: &mut MtrPage<'_>, shape: Shape) {
        if shape == Shape::Raw {
            return self.scribble(page.raw_mut());
        }
        if self.below(10) == 0 {
            let payload = vec![self.below(256) as u8; self.below(64)];
            let data = page.raw_mut();
            return match shape {
                Shape::Heap => drop(TablePage::new(data).unwrap().insert(&self.meta(), &payload)),
                _ => drop(self.leaf_insert(data)),
            };
        }
        let op_rng = next(&mut self.rng);
        let mut payload = Vec::new();
        let op = match shape {
            Shape::Heap => self.heap_op(page.data(), op_rng, &mut payload),
            _ => self.leaf_op(page.data(), op_rng, &mut payload),
        };
        if let Some(op) = op {
            match page.apply(op) {
                Ok(()) | Err(storage::StorageError::PageFull) => {}
                Err(e) => panic!("{op:?}: {e}"),
            }
        }
    }

    fn meta(&mut self) -> TupleMeta {
        let locked = if self.below(2) == 0 {
            TXN_UNCOMMITTED_BIT
        } else {
            0
        };
        TupleMeta {
            ts: (next(&mut self.rng) % 1000) | locked,
            undo_ptr: next(&mut self.rng) % 3,
            is_deleted: self.below(4) == 0,
            has_nulls: self.below(2) == 0,
        }
    }

    /// A heap op that `data` can take, its payload put in `payload`.
    fn heap_op<'p>(
        &mut self,
        data: &[u8; PAGE_SIZE],
        choice: u64,
        payload: &'p mut Vec<u8>,
    ) -> Option<PageOp<'p>> {
        let page = TablePage::new(data).unwrap();
        let live: Vec<_> = page
            .iter()
            .map(|(slot, meta, bytes)| (slot, meta, bytes.len()))
            .collect();
        payload.resize(1 + self.below(200), self.below(256) as u8);
        let padded_to = payload.len() + self.below(4);
        let meta = self.meta();
        if live.is_empty() || choice.is_multiple_of(6) {
            return Some(PageOp::HeapInsert {
                slot: page.insert_slot(),
                meta,
                payload,
            });
        }
        let (slot, old, len) = live[self.below(live.len())];
        Some(match choice % 6 {
            1 => PageOp::HeapSetTuple {
                slot,
                meta,
                payload,
                padded_to,
            },
            2 => PageOp::HeapSetMeta { slot, meta },
            3 if old.ts & TXN_UNCOMMITTED_BIT != 0 => PageOp::HeapStamp {
                slot,
                keep_len: len - self.below(len + 1),
            },
            4 => PageOp::HeapMarkDead { slot },
            _ => PageOp::HeapSetNext {
                next: self.below(1000) as PageId,
            },
        })
    }

    /// A leaf op that `data` can take, with its key put in `key`. It
    /// removes a key, or inserts one from a small range, so some are there
    /// already or tombstoned.
    fn leaf_op<'k>(
        &mut self,
        data: &[u8; PAGE_SIZE],
        choice: u64,
        key: &'k mut Vec<u8>,
    ) -> Option<PageOp<'k>> {
        let leaf = Leaf::new(data).unwrap();
        let live: Vec<usize> = (0..leaf.size())
            .filter(|&i| !leaf.is_tombstone(i))
            .collect();
        if choice.is_multiple_of(3) && !live.is_empty() {
            let index = live[self.below(live.len())];
            return Some(PageOp::LeafRemove {
                num_tombs: NUM_TOMBS,
                index,
            });
        }
        key.extend_from_slice(&(self.below(300) as u16).to_be_bytes());
        key.resize(2 + self.below(40), b'k');
        let position = leaf.insert_position(key, &BytewiseComparator).ok()?;
        Some(PageOp::LeafInsert {
            num_tombs: NUM_TOMBS,
            position,
            key,
            rid: Rid::new(self.below(100) as PageId, self.below(100) as u16),
        })
    }

    fn leaf_insert(&mut self, data: &mut [u8; PAGE_SIZE]) -> storage::Result<usize> {
        let key = (self.below(300) as u16).to_be_bytes();
        let rid = Rid::new(1, self.below(100) as u16);
        BtreeLeafPage::<_, NUM_TOMBS>::new(data)?.insert(&key, rid, &BytewiseComparator)
    }

    /// One past the highest page id handed out so far.
    fn allocated(&self) -> PageId {
        self.bpm().new_page()
    }
}

#[test]
fn every_crash_point_recovers_exactly_the_logged_pages() {
    let fs = CrashFs::new();
    let mut workload = Workload::new(&fs, 0x5eed);
    workload.run(250);
    let allocated = workload.allocated();
    drop(workload);

    let (images, ops) = forms(&fs);
    assert!(images > 50 && ops > 50, "{images} images and {ops} ops");
    let started = segments(&fs);
    assert!(started[0] > 1, "no checkpoint removed a segment");
    assert!(
        *started.last().unwrap() > 8,
        "only {started:?} were started"
    );
    let mut crashes = 0;
    let mut most_segments = 0;
    fs.for_each_moment(|n, moment| {
        for lost in [Lost::Nothing, Lost::Everything, Lost::Some(n as u64)] {
            let at = format!("crash after event {n}, losing {lost:?}");
            let image = moment.crash(lost);
            most_segments = most_segments.max(segments(&image).len());
            recover_and_check(&image, allocated, n % 8 == 0, &at);
            crashes += 1;
        }
    });
    assert!(crashes > 600, "only {crashes} crash points");
    assert!(
        most_segments * 2 < *started.last().unwrap() as usize,
        "the log grew to {most_segments} segments"
    );
}

/// Recovery leaves pages written before the log's first record, slots freed
/// in a `.meta` it did not save, and a log that is one segment: a second
/// crash must find all of that in order.
#[test]
fn every_crash_point_after_a_recovery_recovers_exactly_the_logged_pages() {
    let fs = CrashFs::new();
    let mut workload = Workload::new(&fs, 0x9e11);
    workload.run(150);
    drop(workload);

    let events = fs.num_events();
    for (first_crash, lost) in [(events / 2, Lost::Some(3)), (events, Lost::Everything)] {
        let image = fs.crash_after(first_crash, lost);
        let (pages, _) = logged_pages(&image);
        let mut workload = Workload::resume(&image, first_crash as u64);
        let base = workload.bpm().log().first_lsn();
        assert!(
            pages.values().flatten().any(|&(lsn, _)| lsn < base),
            "no page was last logged before the log's first segment"
        );
        workload.run(150);
        let allocated = workload.allocated();
        drop(workload);

        let mut crashes = 0;
        image.for_each_moment(|n, moment| {
            for lost in [Lost::Nothing, Lost::Everything, Lost::Some(n as u64)] {
                let at =
                    format!("crash after event {first_crash}, then after {n}, losing {lost:?}");
                recover_and_check(&moment.crash(lost), allocated, n % 16 == 0, &at);
                crashes += 1;
            }
        });
        assert!(crashes > 300, "only {crashes} crash points");
    }
}

#[test]
fn a_crash_in_any_step_of_a_checkpoint_recovers() {
    let fs = CrashFs::new();
    let mut workload = Workload::new(&fs, 0xc4ec);
    workload.run(60);
    let before = fs.num_events();
    checkpoint::fuzzy(workload.bpm()).unwrap();
    let after = fs.num_events();
    workload.bpm().checkpoint().unwrap();
    let end = fs.num_events();
    let allocated = workload.allocated();
    drop(workload);

    assert!(
        after - before >= 3,
        "a checkpoint syncs the log and the db file, then saves"
    );
    fs.for_each_moment(|n, moment| {
        if n < before {
            return;
        }
        for lost in [Lost::Nothing, Lost::Everything, Lost::Some(n as u64)] {
            let at = format!(
                "crash after event {n} of checkpoints {before}..{after}..{end}, losing {lost:?}"
            );
            recover_and_check(&moment.crash(lost), allocated, true, &at);
        }
    });
}

#[test]
fn a_crash_during_recovery_recovers_the_same_pages() {
    let fs = CrashFs::new();
    let mut workload = Workload::new(&fs, 0xdead);
    workload.run(80);
    let allocated = workload.allocated();
    drop(workload);

    let points = fs.num_events();
    for crash_at in [points / 3, points / 2, points] {
        let image = fs.crash_after(crash_at, Lost::Some(crash_at as u64));
        let (pages, _) = logged_pages(&image);
        drop(open(&image, 4));
        let recovering = image.num_events();
        assert!(recovering > 0, "recovery wrote nothing");
        image.for_each_moment(|n, moment| {
            for lost in [Lost::Nothing, Lost::Everything, Lost::Some(n as u64)] {
                let again = moment.crash(lost);
                let storage = open(&again, 4);
                let at = format!(
                    "crash after event {crash_at}, then after event {n} of {recovering} in recovery, losing {lost:?}"
                );
                assert_recovered(&storage.bpm, &pages, allocated, &at);
            }
        });
    }
}

/// Without page writes the log is all there is, so the records below any
/// cut decide every page.
#[test]
fn every_prefix_of_the_log_recovers_the_pages_its_records_leave() {
    let fs = CrashFs::new();
    // One segment, so a cut is a length of its file.
    let storage = storage::open_with(Arc::new(fs.clone()), DB, 256, None, u64::MAX).unwrap();
    let mut workload = Workload {
        storage,
        rng: 0x106,
        live: Vec::new(),
    };
    for _ in 0..60 {
        workload.write();
    }
    let allocated = workload.allocated();
    workload.bpm().log().flush_all().unwrap();
    drop(workload);

    let first = segment_path(1);
    let log_bytes = fs.contents(&first);
    let (_, end) = logged_pages(&fs);
    let mut cuts = vec![end];
    let log = Log::open(Arc::new(fs.crash_now()), LOG, u64::MAX).unwrap();
    let mut scan = log.scan(FIRST_LSN);
    while let Some(record) = scan.next_record().unwrap() {
        cuts.push(record.lsn);
    }
    assert!(cuts.len() > 60, "{} records", cuts.len());

    for cut in cuts {
        // In the first segment, an LSN is its byte offset.
        let image = fs.with_contents(&first, &log_bytes[..cut as usize]);
        let (_, recovered_end) = logged_pages(&image);
        assert_eq!(recovered_end, cut);
        recover_and_check(&image, allocated, false, &format!("log cut at {cut}"));
    }
}

/// A writer's mtrs and evictions race checkpoints that save the dirty page
/// table, flush the log and remove segments as they go.
#[test]
fn pages_changed_during_fuzzy_checkpoints_recover_as_logged() {
    let fs = CrashFs::new();
    // Room for an mtr's three pages and a page a checkpoint is writing.
    let mut workload = Workload {
        storage: open(&fs, 8),
        rng: 0xf022,
        live: Vec::new(),
    };
    let done = Arc::new(AtomicBool::new(false));
    let checkpoints = {
        let bpm = Arc::clone(&workload.storage.bpm);
        let done = Arc::clone(&done);
        thread::spawn(move || {
            let mut taken = 0;
            while !done.load(Ordering::Acquire) {
                checkpoint::fuzzy(&bpm).unwrap();
                taken += 1;
                thread::yield_now();
            }
            taken
        })
    };
    let writer = thread::spawn(move || {
        for _ in 0..600 {
            workload.write();
        }
        workload
    });
    let workload = writer.join().unwrap();
    done.store(true, Ordering::Release);
    let taken = checkpoints.join().unwrap();
    let allocated = workload.allocated();
    drop(workload);

    assert!(
        taken >= 10,
        "only {taken} checkpoints ran during the writes"
    );
    assert!(segments(&fs)[0] > 1, "no checkpoint removed a segment");
    let events = fs.num_events();
    let stride = (events / 400).max(1);
    fs.for_each_moment(|n, moment| {
        if n % stride == 0 || n == events {
            for lost in [Lost::Nothing, Lost::Everything, Lost::Some(n as u64)] {
                let at = format!("crash after event {n} of {events}, losing {lost:?}");
                recover_and_check(&moment.crash(lost), allocated, false, &at);
            }
        }
    });
}

#[test]
fn a_crash_while_starting_a_segment_recovers() {
    let fs = CrashFs::new();
    let storage = open(&fs, 4);
    let page_id = storage.bpm.new_page();
    let mut starts = Vec::new();
    for i in 0..40u8 {
        write(&storage.bpm, page_id, &[i; 64]);
        let (before, last) = (fs.num_events(), segments(&fs).len());
        storage.bpm.log().flush_all().unwrap();
        if segments(&fs).len() > last {
            starts.push(before..fs.num_events());
        }
    }
    let allocated = storage.bpm.new_page();
    drop(storage);

    assert!(starts.len() >= 3, "only {} segments started", starts.len());
    for flush in starts {
        for n in flush.start..=flush.end {
            for lost in [Lost::Nothing, Lost::Everything, Lost::Some(n as u64)] {
                let at = format!(
                    "crash after event {n} of a flush that started a segment, losing {lost:?}"
                );
                recover_and_check(&fs.crash_after(n, lost), allocated, false, &at);
            }
        }
    }
}

#[test]
fn a_crash_between_saving_a_checkpoint_and_removing_its_segments_recovers() {
    let fs = CrashFs::new();
    let mut workload = Workload::new(&fs, 0x5e9);
    for _ in 0..60 {
        workload.write();
        workload.bpm().log().flush_all().unwrap();
    }
    let segments_before = segments(&fs).len();
    let before = fs.num_events();
    workload.bpm().checkpoint().unwrap();
    let after = fs.num_events();
    let allocated = workload.allocated();
    drop(workload);

    assert!(segments_before >= 3, "only {segments_before} segments");
    assert_eq!(segments(&fs).len(), 1, "the checkpoint removed the rest");
    for n in before..=after {
        for lost in [Lost::Nothing, Lost::Everything, Lost::Some(n as u64)] {
            let at =
                format!("crash after event {n} of a checkpoint {before}..{after}, losing {lost:?}");
            recover_and_check(&fs.crash_after(n, lost), allocated, false, &at);
        }
    }
}

/// Changes `other` until the log has started `count` more segments.
fn fill_segments(fs: &CrashFs, bpm: &BufferPoolManager, other: PageId, count: usize) {
    let target = segments(fs).len() + count;
    for i in 0u64.. {
        if segments(fs).len() >= target {
            return;
        }
        write(bpm, other, &i.to_le_bytes());
        bpm.log().flush_all().unwrap();
    }
}

#[test]
fn a_checkpoint_keeps_every_segment_from_its_oldest_dirty_page_on() {
    let fs = CrashFs::new();
    let storage = open(&fs, 4);
    let bpm = &storage.bpm;
    let (kept, other) = (bpm.new_page(), bpm.new_page());
    fill_segments(&fs, bpm, other, 2);
    bpm.flush_page(other).unwrap();
    write(bpm, kept, b"kept page");
    let pinned = bpm.read_page(kept, AccessType::Unknown);
    let rec_lsn = page_lsn(pinned.get_data());
    fill_segments(&fs, bpm, other, 2);

    let before = bpm.log().first_lsn();
    checkpoint::fuzzy(bpm).unwrap();
    let first = bpm.log().first_lsn();
    assert!(first > before, "the checkpoint removed no segment");
    assert!(first <= rec_lsn, "the checkpoint removed {kept}'s change");
    let mut buf = Vec::new();
    assert_eq!(bpm.log().read(rec_lsn, &mut buf).unwrap().lsn, rec_lsn);
    drop(pinned);
    let image = fs.crash_now();
    drop(storage);

    let storage = open(&image, 4);
    assert_eq!(read(&storage.bpm, kept, 9), b"kept page");
}

#[test]
fn a_checkpoint_that_needs_removed_log_is_not_saved() {
    let fs = CrashFs::new();
    let storage = open(&fs, 4);
    let bpm = &storage.bpm;
    let (dirty, other) = (bpm.new_page(), bpm.new_page());
    write(bpm, dirty, b"dirty page");
    let _pinned = bpm.read_page(dirty, AccessType::Unknown);
    fill_segments(&fs, bpm, other, 2);
    // As a bound that moved back would have: the dirty page's change goes.
    bpm.log().remove_segments_before(bpm.log().end()).unwrap();

    let saved = fs.contents(META.as_ref());
    assert!(matches!(
        checkpoint::fuzzy(bpm),
        Err(storage::StorageError::CorruptLog(_))
    ));
    assert_eq!(fs.contents(META.as_ref()), saved, "the .meta changed");
}

/// Changes a page through an mtr of its own, writing `bytes` at `AT`.
fn write(bpm: &BufferPoolManager, page_id: PageId, bytes: &[u8]) {
    let mtr = bpm.mtr(Logged::System);
    mtr.write_page(page_id).unwrap().raw_mut()[AT..][..bytes.len()].copy_from_slice(bytes);
}

fn read(bpm: &BufferPoolManager, page_id: PageId, len: usize) -> Vec<u8> {
    bpm.read_page(page_id, AccessType::Unknown).get_data()[AT..][..len].to_vec()
}

/// The offset of the slot in the db file whose page has `bytes` at `AT`.
fn slot_holding(db_file: &[u8], bytes: &[u8]) -> usize {
    (0..db_file.len() / PAGE_SIZE)
        .map(|slot| slot * PAGE_SIZE)
        .find(|&offset| db_file[offset + AT..].starts_with(bytes))
        .expect("no slot holds the page")
}

#[test]
fn a_torn_page_write_is_repaired_from_the_log() {
    let fs = CrashFs::new();
    let storage = open(&fs, 4);
    let page_id = storage.bpm.new_page();
    write(&storage.bpm, page_id, &[b'a'; PAGE_SIZE - AT]);
    storage.bpm.checkpoint().unwrap();
    let before = fs.contents(DB.as_ref());
    write(&storage.bpm, page_id, &[b'b'; PAGE_SIZE - AT]);
    storage.bpm.flush_page(page_id).unwrap();
    let after = fs.contents(DB.as_ref());
    drop(storage);

    // Half the new page reached the disk, half did not.
    let offset = slot_holding(&before, b"aaaa");
    let mut torn = after.clone();
    torn[offset + PAGE_SIZE / 2..offset + PAGE_SIZE]
        .copy_from_slice(&before[offset + PAGE_SIZE / 2..offset + PAGE_SIZE]);
    let image = fs.with_contents(DB.as_ref(), &torn);
    let page: &[u8; PAGE_SIZE] = torn[offset..offset + PAGE_SIZE].try_into().unwrap();
    assert!(verify_checksum(page_id, page).is_err(), "the page is torn");

    let storage = open(&image, 4);
    assert_eq!(
        read(&storage.bpm, page_id, PAGE_SIZE - AT),
        [b'b'; PAGE_SIZE - AT]
    );
}

#[test]
fn a_freed_slot_is_not_reused_while_the_saved_directory_maps_it() {
    let fs = CrashFs::new();
    let storage = open(&fs, 4);
    let (freed, other) = (storage.bpm.new_page(), storage.bpm.new_page());
    write(&storage.bpm, freed, b"freed page");
    write(&storage.bpm, other, b"other page");
    storage.bpm.checkpoint().unwrap();
    {
        let mtr = storage.bpm.mtr(Logged::System);
        mtr.free_page(freed);
    }
    let reused = storage.bpm.new_page();
    write(&storage.bpm, reused, b"newer page");
    storage.bpm.flush_page(reused).unwrap();
    storage.bpm.log().flush_all().unwrap();
    let image = fs.crash_now();
    drop(storage);

    // The `.meta` on disk is the checkpoint's, which still maps the freed
    // page to its slot: that slot must still hold the freed page.
    let dm = DiskManager::open(Arc::new(image.crash_now()), DB).unwrap();
    let mut data = [0; PAGE_SIZE];
    dm.read_page(freed, &mut data).unwrap();
    assert_eq!(&data[AT..][..10], b"freed page");
    drop(dm);

    let storage = open(&image, 4);
    assert!(!storage.bpm.disk_manager().has_page(freed));
    assert_eq!(read(&storage.bpm, reused, 10), b"newer page");
    assert_eq!(read(&storage.bpm, other, 10), b"other page");
}

#[test]
fn a_page_first_written_after_the_last_checkpoint_is_recovered() {
    for lost in [Lost::Nothing, Lost::Everything] {
        let fs = CrashFs::new();
        let storage = open(&fs, 4);
        let old = storage.bpm.new_page();
        write(&storage.bpm, old, b"old page");
        storage.bpm.checkpoint().unwrap();
        let new = storage.bpm.new_page();
        write(&storage.bpm, new, b"new page");
        // Its first write takes a slot the saved directory does not know.
        storage.bpm.flush_page(new).unwrap();
        let image = fs.crash_after(fs.num_events(), lost);
        drop(storage);

        let storage = open(&image, 4);
        assert_eq!(read(&storage.bpm, new, 8), b"new page", "{lost:?}");
        assert_eq!(read(&storage.bpm, old, 8), b"old page", "{lost:?}");
    }
}

#[test]
fn page_ids_handed_out_before_a_checkpoint_are_never_handed_out_again() {
    let fs = CrashFs::new();
    let storage = open(&fs, 4);
    let written = storage.bpm.new_page();
    write(&storage.bpm, written, b"written");
    let unwritten: Vec<PageId> = (0..5).map(|_| storage.bpm.new_page()).collect();
    storage.bpm.checkpoint().unwrap();
    let image = fs.crash_now();
    drop(storage);

    let storage = open(&image, 4);
    assert!(storage.bpm.new_page() > *unwritten.last().unwrap());
    drop(storage);
    // Nor once recovery has taken a checkpoint of its own.
    let storage = open(&image, 4);
    assert!(storage.bpm.new_page() > *unwritten.last().unwrap());
}

#[test]
fn a_log_that_ends_before_the_checkpoint_is_refused() {
    let fs = CrashFs::new();
    let storage = open(&fs, 4);
    let page_id = storage.bpm.new_page();
    write(&storage.bpm, page_id, b"page");
    storage.close().unwrap();
    let storage = open(&fs, 4);
    write(&storage.bpm, page_id, b"more");
    storage.bpm.checkpoint().unwrap();
    drop(storage);

    let image = segments(&fs)
        .into_iter()
        .fold(fs.crash_now(), |image, n| image.without(&segment_path(n)));
    let opened = storage::open_with(Arc::new(image), DB, 4, None, SEGMENT);
    assert!(matches!(opened, Err(storage::StorageError::CorruptLog(_))));
}

#[test]
fn a_log_left_from_another_database_is_not_replayed() {
    let fs = CrashFs::new();
    let storage = open(&fs, 4);
    let page_id = storage.bpm.new_page();
    write(&storage.bpm, page_id, b"old database");
    storage.close().unwrap();
    let fresh = fs.without(DB.as_ref()).without(META.as_ref());

    let storage = open(&fresh, 4);
    assert_eq!(storage.bpm.new_page(), 0, "the new database has no pages");
}

#[test]
fn a_segment_missing_from_the_middle_of_the_log_is_refused() {
    let fs = CrashFs::new();
    let storage = open(&fs, 4);
    let (page_id, other) = (storage.bpm.new_page(), storage.bpm.new_page());
    write(&storage.bpm, page_id, b"page");
    storage.bpm.checkpoint().unwrap();
    fill_segments(&fs, &storage.bpm, other, 3);
    drop(storage);

    let present = segments(&fs);
    let image = fs.crash_now().without(&segment_path(present[1]));
    let opened = storage::open_with(Arc::new(image), DB, 4, None, SEGMENT);
    assert!(matches!(opened, Err(storage::StorageError::CorruptLog(_))));
}
