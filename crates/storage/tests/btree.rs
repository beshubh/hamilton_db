//! The B+tree: lookups and scans on trees built page by page, inserts and
//! removes.

use std::cmp::Ordering;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

use common::{INVALID_PAGE_ID, PageId, Rid};
use storage::StorageError;
use storage::buffer::arc_replacer::AccessType;
use storage::buffer::buffer_pool_manager::BufferPoolManager;
use storage::buffer::traced_buffer_pool_manager::{PageAccess, TracedBufferPoolManager};
use storage::disk::disk_manager::DiskManager;
use storage::index::btree::Btree;
use storage::index::index_iter::IndexIterator;
use storage::page::PAGE_SIZE;
use storage::page::btree_header_page::BtreeHeaderPage;
use storage::page::btree_internal_page::BtreeInternalPage;
use storage::page::btree_leaf_page::BtreeLeafPage;
use storage::page::btree_page::{BytewiseComparator, KeyComparator};
use storage::page::page_header::{PageHeader, PageType};
use tempfile::TempDir;
use zerocopy::FromBytes;

type Tree = Btree<BytewiseComparator, 2>;
type Leaf<'a> = BtreeLeafPage<&'a mut [u8; PAGE_SIZE], 2>;
type Internal<'a> = BtreeInternalPage<&'a mut [u8; PAGE_SIZE]>;

const CMP: BytewiseComparator = BytewiseComparator;

/// Big-endian, so byte order matches numeric order.
fn key(n: u64) -> [u8; 8] {
    n.to_be_bytes()
}

fn rid(n: u64) -> Rid {
    Rid::new(n as i32, n as u16 + 1)
}

/// The keys `iter` yields, as numbers, checking each one's record id.
fn collect<const NUM_TOMBS: usize>(iter: IndexIterator<NUM_TOMBS>) -> Vec<u64> {
    iter.map(|entry| {
        let (k, r) = entry.unwrap();
        let k = u64::from_be_bytes(k[..8].try_into().unwrap());
        assert_eq!(r, rid(k));
        k
    })
    .collect()
}

/// A tree whose leaves keep `NUM_TOMBS` tombstones.
struct Fixture<const NUM_TOMBS: usize = 2> {
    _dir: TempDir,
    bpm: Arc<TracedBufferPoolManager>,
    header_page_id: PageId,
    tree: Btree<BytewiseComparator, NUM_TOMBS>,
}

/// Key `n` padded with filler to `len` bytes. Ordered as `n`.
fn long_key(n: u64, len: usize) -> Vec<u8> {
    let mut k = key(n).to_vec();
    k.resize(len, b'x');
    k
}

/// `0..n` in an order that jumps all over, the same every run.
fn shuffled(n: u64) -> Vec<u64> {
    let mut keys: Vec<u64> = (0..n).collect();
    let mut state = 0x2545_f491_4f6c_dd1du64;
    for i in (1..keys.len()).rev() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        keys.swap(i, (state % (i as u64 + 1)) as usize);
    }
    keys
}

/// An empty tree.
fn setup() -> Fixture {
    setup_with_frames(16)
}

/// An empty tree over a buffer pool of `frames` frames.
fn setup_with_frames(frames: usize) -> Fixture {
    setup_with(frames)
}

/// An empty tree whose leaves keep no tombstones, so every remove takes its
/// key out right away.
fn setup_no_tombs(frames: usize) -> Fixture<0> {
    setup_with(frames)
}

fn setup_with<const NUM_TOMBS: usize>(frames: usize) -> Fixture<NUM_TOMBS> {
    let dir = TempDir::new().unwrap();
    let dm = Arc::new(DiskManager::new(dir.path().join("test.db")).unwrap());
    let bpm = Arc::new(TracedBufferPoolManager::new(Arc::new(
        BufferPoolManager::new(frames, dm),
    )));
    let header_page_id = bpm.new_page();
    let tree = Btree::new("idx", header_page_id, Arc::clone(&bpm), CMP).unwrap();
    Fixture {
        _dir: dir,
        bpm,
        header_page_id,
        tree,
    }
}

impl<const NUM_TOMBS: usize> Fixture<NUM_TOMBS> {
    /// Writes a leaf holding `keys` with their `rid`s, linked to `next`.
    fn leaf(&self, keys: &[u64], next: PageId) -> PageId {
        let page_id = self.bpm.new_page();
        let mut guard = self
            .bpm
            .checked_write_page(page_id, AccessType::Unknown)
            .unwrap();
        let mut page = BtreeLeafPage::<_, NUM_TOMBS>::init(guard.get_data_mut());
        for &k in keys {
            page.insert(&key(k), rid(k), &CMP).unwrap();
        }
        page.set_next_page_id(next);
        page_id
    }

    fn set_root(&self, root: PageId) {
        let mut guard = self
            .bpm
            .checked_write_page(self.header_page_id, AccessType::Unknown)
            .unwrap();
        BtreeHeaderPage::new(guard.get_data_mut())
            .unwrap()
            .set_root_page_id(root);
    }

    /// Builds this tree and returns its root and leaves:
    ///
    /// ```text
    ///               [ . 40 60 ]
    ///             /     |      \
    ///  [10 20 30] -> [40 50] -> [60 70]
    /// ```
    fn two_levels(&self) -> (PageId, [PageId; 3]) {
        let l3 = self.leaf(&[60, 70], INVALID_PAGE_ID);
        let l2 = self.leaf(&[40, 50], l3);
        let l1 = self.leaf(&[10, 20, 30], l2);
        let root = self.bpm.new_page();
        {
            let mut guard = self
                .bpm
                .checked_write_page(root, AccessType::Unknown)
                .unwrap();
            let mut page = Internal::init(guard.get_data_mut());
            page.populate_new_root(l1, &key(40), l2).unwrap();
            page.insert(&key(60), l3, &CMP).unwrap();
        }
        self.set_root(root);
        (root, [l1, l2, l3])
    }

    fn remove(&self, leaf: PageId, k: u64) {
        let mut guard = self
            .bpm
            .checked_write_page(leaf, AccessType::Unknown)
            .unwrap();
        BtreeLeafPage::<_, NUM_TOMBS>::new(guard.get_data_mut())
            .unwrap()
            .remove(&key(k), &CMP)
            .unwrap();
    }

    fn get(&self, k: u64) -> Option<Rid> {
        self.get_key(&key(k))
    }

    fn get_key(&self, k: &[u8]) -> Option<Rid> {
        let mut result = Vec::new();
        let found = self.tree.get_value(k, &mut result).unwrap();
        assert_eq!(found, !result.is_empty());
        assert!(result.len() <= 1);
        result.pop()
    }

    fn scan(&self) -> Vec<u64> {
        collect(self.tree.begin().unwrap())
    }

    /// The keys `>= k`, by `begin_at`.
    fn scan_from(&self, k: u64) -> Vec<u64> {
        collect(self.tree.begin_at(&key(k)).unwrap())
    }

    /// The root's children, or `None` if it is a leaf.
    fn root_children(&self) -> Option<Vec<PageId>> {
        let root = self.tree.get_root_page_id().unwrap();
        if root == INVALID_PAGE_ID {
            return None;
        }
        let guard = self
            .bpm
            .checked_read_page(root, AccessType::Unknown)
            .unwrap();
        let page = BtreeInternalPage::new(guard.get_data()).ok()?;
        Some((0..page.size()).map(|i| page.value_at(i)).collect())
    }

    /// Bytes the entries of leaf `page_id` take.
    fn leaf_used_space(&self, page_id: PageId) -> usize {
        let guard = self
            .bpm
            .checked_read_page(page_id, AccessType::Unknown)
            .unwrap();
        BtreeLeafPage::<_, NUM_TOMBS>::new(guard.get_data())
            .unwrap()
            .used_space()
    }

    /// Checks that the tree is a well-formed B+tree and returns its height,
    /// 0 for an empty tree and 1 for a root leaf:
    /// - every page's keys are in increasing order,
    /// - every key lies within the bounds its parents' separators set,
    /// - every internal page has at least 2 children,
    /// - every leaf is at the same depth, and
    /// - the leaves are linked left to right in the same order.
    ///
    /// Also checks that no page is left pinned.
    fn check(&self) -> usize {
        let root = self.tree.get_root_page_id().unwrap();
        if root == INVALID_PAGE_ID {
            return 0;
        }
        let mut leaves = Vec::new();
        let height = self.check_page(root, None, None, &mut leaves);

        let mut chain = vec![leaves[0]];
        loop {
            let guard = self
                .bpm
                .checked_read_page(*chain.last().unwrap(), AccessType::Unknown)
                .unwrap();
            let next = BtreeLeafPage::<_, NUM_TOMBS>::new(guard.get_data())
                .unwrap()
                .next_page_id();
            if next == INVALID_PAGE_ID {
                break;
            }
            chain.push(next);
        }
        assert_eq!(chain, leaves, "leaf chain");

        assert_eq!(self.bpm.get_pin_count(self.header_page_id).unwrap_or(0), 0);
        for &page_id in &leaves {
            assert_eq!(self.bpm.get_pin_count(page_id).unwrap_or(0), 0);
        }
        height
    }

    /// Checks the subtree under `page_id`, whose keys must be in `lo..hi`,
    /// appends its leaves and returns its height.
    fn check_page(
        &self,
        page_id: PageId,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        leaves: &mut Vec<PageId>,
    ) -> usize {
        let guard = self
            .bpm
            .checked_read_page(page_id, AccessType::Unknown)
            .unwrap();
        let data = guard.get_data();
        let check_keys = |keys: &[&[u8]]| {
            for pair in keys.windows(2) {
                assert!(pair[0] < pair[1], "page {page_id}: keys out of order");
            }
            for &k in keys {
                assert!(
                    lo.is_none_or(|lo| lo <= k) && hi.is_none_or(|hi| k < hi),
                    "page {page_id}: key out of its parent's bounds"
                );
            }
        };
        let (header, _) = PageHeader::ref_from_prefix(&data[..]).unwrap();
        if header.page_type() == Ok(PageType::BTreeLeaf) {
            let page = BtreeLeafPage::<_, NUM_TOMBS>::new(data).unwrap();
            assert!(page.size() > 0, "page {page_id}: empty leaf");
            check_keys(&(0..page.size()).map(|i| page.key_at(i)).collect::<Vec<_>>());
            leaves.push(page_id);
            return 1;
        }
        let page = BtreeInternalPage::new(data).unwrap();
        let size = page.size();
        assert!(
            size >= 2,
            "page {page_id}: internal page with {size} children"
        );
        check_keys(&(1..size).map(|i| page.key_at(i)).collect::<Vec<_>>());
        let heights: Vec<usize> = (0..size)
            .map(|i| {
                let child_lo = if i == 0 { lo } else { Some(page.key_at(i)) };
                let child_hi = if i + 1 < size {
                    Some(page.key_at(i + 1))
                } else {
                    hi
                };
                self.check_page(page.value_at(i), child_lo, child_hi, leaves)
            })
            .collect();
        assert!(
            heights.iter().all(|&h| h == heights[0]),
            "page {page_id}: leaves at different depths"
        );
        heights[0] + 1
    }
}

#[test]
fn new_tree_is_empty() {
    let f = setup();
    assert!(f.tree.is_empty().unwrap());
    assert_eq!(f.tree.get_root_page_id().unwrap(), INVALID_PAGE_ID);
    assert_eq!(f.get(10), None);
    assert!(f.tree.begin().unwrap().is_end());
    assert!(f.scan().is_empty());
    assert!(f.tree.end().next().is_none());
}

#[test]
fn reopen_keeps_root() {
    let f = setup();
    let (root, _) = f.two_levels();
    let tree = Tree::new("idx", f.header_page_id, Arc::clone(&f.bpm), CMP).unwrap();
    assert_eq!(tree.get_root_page_id().unwrap(), root);
    assert!(!tree.is_empty().unwrap());
}

#[test]
fn new_rejects_other_page_types() {
    let f = setup();
    let leaf = f.leaf(&[], INVALID_PAGE_ID);
    let Err(err) = Tree::new("idx", leaf, Arc::clone(&f.bpm), CMP) else {
        panic!("a leaf page opened as a header page");
    };
    assert!(matches!(
        err,
        StorageError::UnexpectedPageType {
            expected: PageType::BTreeHeader,
            found: 3
        }
    ));
    // The leaf is left as it was.
    let guard = f.bpm.checked_read_page(leaf, AccessType::Unknown).unwrap();
    let (header, _) = PageHeader::ref_from_prefix(&guard.get_data()[..]).unwrap();
    assert_eq!(header.page_type(), Ok(PageType::BTreeLeaf));
}

#[test]
fn root_can_be_a_leaf() {
    let f = setup();
    let leaf = f.leaf(&[10, 20], INVALID_PAGE_ID);
    f.set_root(leaf);
    assert_eq!(f.get(10), Some(rid(10)));
    assert_eq!(f.get(15), None);
    assert_eq!(f.scan(), [10, 20]);
}

#[test]
fn get_value_finds_every_key() {
    let f = setup();
    f.two_levels();
    for k in [10, 20, 30, 40, 50, 60, 70] {
        assert_eq!(f.get(k), Some(rid(k)), "key {k}");
    }
    for k in [0, 15, 35, 39, 41, 65, 80] {
        assert_eq!(f.get(k), None, "key {k}");
    }
}

#[test]
fn begin_scans_every_leaf_in_order() {
    let f = setup();
    f.two_levels();
    assert_eq!(f.scan(), [10, 20, 30, 40, 50, 60, 70]);
}

#[test]
fn tombstoned_keys_are_skipped() {
    let f = setup();
    let (_, [l1, l2, _]) = f.two_levels();
    f.remove(l1, 20);
    // Every key of the middle leaf is tombstoned.
    f.remove(l2, 40);
    f.remove(l2, 50);

    assert_eq!(f.scan(), [10, 30, 60, 70]);
    assert_eq!(f.get(20), None);
    assert_eq!(f.get(40), None);
    assert_eq!(f.get(30), Some(rid(30)));
}

#[test]
fn lookup_reads_one_page_per_level() {
    let f = setup();
    let (root, [_, l2, _]) = f.two_levels();
    f.bpm.start_tracing();
    assert_eq!(f.get(50), Some(rid(50)));
    assert_eq!(
        f.bpm.take_trace(),
        [
            PageAccess::Read(f.header_page_id),
            PageAccess::Read(root),
            PageAccess::Read(l2),
        ]
    );
}

#[test]
fn scan_reads_leaves_left_to_right() {
    let f = setup();
    let (root, [l1, l2, l3]) = f.two_levels();
    f.bpm.start_tracing();
    f.scan();
    assert_eq!(
        f.bpm.take_trace(),
        [
            PageAccess::Read(f.header_page_id),
            PageAccess::Read(root),
            PageAccess::Read(l1),
            PageAccess::Read(l2),
            PageAccess::Read(l3),
        ]
    );
}

#[test]
fn iterator_pins_only_its_current_leaf() {
    let f = setup();
    let (root, [l1, l2, l3]) = f.two_levels();
    let pins = |page_id| f.bpm.get_pin_count(page_id).unwrap_or(0);

    let mut iter = f.tree.begin().unwrap();
    assert_eq!(pins(f.header_page_id), 0);
    assert_eq!(pins(root), 0);
    assert_eq!(pins(l1), 1);

    // Past the first leaf.
    for _ in 0..4 {
        iter.next().unwrap().unwrap();
    }
    assert_eq!(pins(l1), 0);
    assert_eq!(pins(l2), 1);

    drop(iter);
    for page_id in [f.header_page_id, root, l1, l2, l3] {
        assert_eq!(pins(page_id), 0, "page {page_id}");
    }
}

#[test]
fn begin_at_starts_at_the_first_key_not_below() {
    let f = setup();
    f.two_levels();
    let all = [10, 20, 30, 40, 50, 60, 70];
    assert_eq!(f.scan_from(0), all);
    assert_eq!(f.scan_from(10), all);
    assert_eq!(f.scan_from(20), all[1..]);
    // Between keys of a leaf.
    assert_eq!(f.scan_from(15), all[1..]);
    // The separator itself, on the right leaf.
    assert_eq!(f.scan_from(40), all[3..]);
    assert_eq!(f.scan_from(70), [70]);
    assert!(f.scan_from(71).is_empty());
}

#[test]
fn begin_at_past_a_leafs_last_key_moves_to_the_next_leaf() {
    let f = setup();
    let (root, [l1, l2, _]) = f.two_levels();
    f.bpm.start_tracing();
    // 35 belongs in the first leaf, which has nothing `>= 35`.
    let mut iter = f.tree.begin_at(&key(35)).unwrap();
    assert_eq!(
        f.bpm.take_trace(),
        [
            PageAccess::Read(f.header_page_id),
            PageAccess::Read(root),
            PageAccess::Read(l1),
        ]
    );
    assert_eq!(iter.next().unwrap().unwrap(), (key(40).to_vec(), rid(40)));
    assert_eq!(f.bpm.take_trace(), [PageAccess::Read(l2)]);
}

#[test]
fn begin_at_skips_tombstoned_keys() {
    let f = setup();
    let (_, [l1, l2, _]) = f.two_levels();
    f.remove(l1, 20);
    f.remove(l2, 40);
    f.remove(l2, 50);
    assert_eq!(f.scan_from(20), [30, 60, 70]);
    assert_eq!(f.scan_from(40), [60, 70]);
}

#[test]
fn begin_at_on_empty_tree() {
    let f = setup();
    assert!(f.tree.begin_at(&key(10)).unwrap().is_end());
}

#[test]
fn begin_at_on_a_large_tree() {
    let f = setup_no_tombs(16);
    for k in shuffled(2000) {
        f.tree.insert(&long_key(k * 2, 300), rid(k * 2)).unwrap();
    }
    assert!(f.check() >= 3);
    for k in (0..4001u64).step_by(37) {
        // The keys are the even numbers below 4000.
        let first = k.next_multiple_of(2);
        let keys = collect(f.tree.begin_at(&long_key(k, 300)).unwrap());
        assert_eq!(
            keys,
            (first..4000).step_by(2).collect::<Vec<_>>(),
            "from {k}"
        );
    }
}

/// Orders little-endian `u32`s numerically, which their bytes do not.
struct LittleEndian;

impl KeyComparator for LittleEndian {
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        let num = |k: &[u8]| u32::from_le_bytes(k.try_into().unwrap());
        num(a).cmp(&num(b))
    }
}

#[test]
fn tree_orders_keys_with_its_comparator() {
    let f = setup();
    let header_page_id = f.bpm.new_page();
    let tree =
        Btree::<LittleEndian>::new("le", header_page_id, Arc::clone(&f.bpm), LittleEndian).unwrap();
    let write = |page_id| {
        f.bpm
            .checked_write_page(page_id, AccessType::Unknown)
            .unwrap()
    };

    // By bytes, 256 ([0, 1, 0, 0]) sorts before 2 ([2, 0, 0, 0]).
    //
    //          [ . 256 ]
    //          /       \
    //    [1 2]    ->    [256 300]
    let right = f.bpm.new_page();
    let left = f.bpm.new_page();
    for (page_id, keys, next) in [
        (right, [256u32, 300], INVALID_PAGE_ID),
        (left, [2, 1], right),
    ] {
        let mut guard = write(page_id);
        let mut page = BtreeLeafPage::<_, 0>::init(guard.get_data_mut());
        for k in keys {
            page.insert(&k.to_le_bytes(), rid(k.into()), &LittleEndian)
                .unwrap();
        }
        page.set_next_page_id(next);
    }
    let root = f.bpm.new_page();
    BtreeInternalPage::init(write(root).get_data_mut())
        .populate_new_root(left, &256u32.to_le_bytes(), right)
        .unwrap();
    BtreeHeaderPage::new(write(header_page_id).get_data_mut())
        .unwrap()
        .set_root_page_id(root);

    let keys: Vec<u32> = tree
        .begin()
        .unwrap()
        .map(|entry| u32::from_le_bytes(entry.unwrap().0.try_into().unwrap()))
        .collect();
    assert_eq!(keys, [1, 2, 256, 300]);
    for k in [1u32, 2, 256, 300] {
        let mut result = Vec::new();
        assert!(
            tree.get_value(&k.to_le_bytes(), &mut result).unwrap(),
            "key {k}"
        );
        assert_eq!(result, [rid(k.into())]);
    }
    assert!(
        !tree
            .get_value(&3u32.to_le_bytes(), &mut Vec::new())
            .unwrap()
    );
}

#[test]
fn keys_of_different_lengths() {
    let f = setup();
    let leaf = f.bpm.new_page();
    {
        let mut guard = f.bpm.checked_write_page(leaf, AccessType::Unknown).unwrap();
        let mut page = Leaf::init(guard.get_data_mut());
        for (n, k) in ["pear", "", "apple", "app"].into_iter().enumerate() {
            page.insert(k.as_bytes(), rid(n as u64), &CMP).unwrap();
        }
    }
    f.set_root(leaf);

    let keys: Vec<Vec<u8>> = f.tree.begin().unwrap().map(|e| e.unwrap().0).collect();
    assert_eq!(keys, [&b""[..], b"app", b"apple", b"pear"]);
    let mut result = Vec::new();
    assert!(f.tree.get_value(b"apple", &mut result).unwrap());
    assert_eq!(result, [rid(2)]);
    assert!(!f.tree.get_value(b"appl", &mut Vec::new()).unwrap());
}

/// Builds a tree the way an insert that splits the root leaf will: fill a
/// leaf, split it by bytes, and put the right half's first key in a new root.
#[test]
fn split_leaf_under_new_root() {
    let f = setup();
    let write = |page_id| {
        f.bpm
            .checked_write_page(page_id, AccessType::Unknown)
            .unwrap()
    };
    // Keys of 8 to 200 bytes that order as their number.
    let long_key = |n: u64| {
        let mut k = key(n).to_vec();
        k.resize(8 + (n as usize * 37) % 193, b'x');
        k
    };

    let (left, right) = (f.bpm.new_page(), f.bpm.new_page());
    let mut count = 0;
    let mut left_guard = write(left);
    let mut left_page = Leaf::init(left_guard.get_data_mut());
    while left_page.insert(&long_key(count), rid(count), &CMP).is_ok() {
        count += 1;
    }
    let mut right_guard = write(right);
    let mut right_page = Leaf::init(right_guard.get_data_mut());
    left_page.split_to(&mut right_page, right);
    let separator = right_page.key_at(0).to_vec();
    drop((left_guard, right_guard));

    let root = f.bpm.new_page();
    Internal::init(write(root).get_data_mut())
        .populate_new_root(left, &separator, right)
        .unwrap();
    f.set_root(root);

    for n in 0..count {
        let mut result = Vec::new();
        assert!(
            f.tree.get_value(&long_key(n), &mut result).unwrap(),
            "key {n}"
        );
        assert_eq!(result, [rid(n)]);
    }
    let keys: Vec<Vec<u8>> = f.tree.begin().unwrap().map(|e| e.unwrap().0).collect();
    assert_eq!(keys, (0..count).map(long_key).collect::<Vec<_>>());
}

#[test]
fn insert_into_empty_tree_starts_a_root_leaf() {
    let f = setup();
    f.bpm.start_tracing();
    assert!(f.tree.insert(&key(10), rid(10)).unwrap());
    let root = f.tree.get_root_page_id().unwrap();
    assert_eq!(
        f.bpm.take_trace(),
        [
            // The optimistic attempt finds the tree empty.
            PageAccess::Read(f.header_page_id),
            PageAccess::Write(f.header_page_id),
            PageAccess::New(root),
            PageAccess::Write(root),
            // `get_root_page_id`.
            PageAccess::Read(f.header_page_id),
        ]
    );
    let guard = f.bpm.checked_read_page(root, AccessType::Unknown).unwrap();
    let (header, _) = PageHeader::ref_from_prefix(&guard.get_data()[..]).unwrap();
    assert_eq!(header.page_type(), Ok(PageType::BTreeLeaf));
    drop(guard);
    assert_eq!(f.get(10), Some(rid(10)));
    assert_eq!(f.scan(), [10]);
}

#[test]
fn inserts_keep_keys_in_order() {
    let f = setup();
    for k in [50, 10, 40, 20, 30] {
        assert!(f.tree.insert(&key(k), rid(k)).unwrap(), "key {k}");
    }
    assert_eq!(f.scan(), [10, 20, 30, 40, 50]);
    for k in [10, 20, 30, 40, 50] {
        assert_eq!(f.get(k), Some(rid(k)), "key {k}");
    }
    assert_eq!(f.get(25), None);
}

#[test]
fn insert_of_existing_key_changes_nothing() {
    let f = setup();
    assert!(f.tree.insert(&key(10), rid(10)).unwrap());
    assert!(!f.tree.insert(&key(10), rid(99)).unwrap());
    assert_eq!(f.get(10), Some(rid(10)));
    assert_eq!(f.scan(), [10]);
}

#[test]
fn insert_rejects_keys_over_max_size() {
    let f = setup();
    f.bpm.start_tracing();
    let too_long = vec![0; Tree::MAX_KEY_SIZE + 1];
    let err = f.tree.insert(&too_long, rid(1)).unwrap_err();
    assert!(matches!(
        err,
        StorageError::KeyTooLarge { size, max }
            if size == Tree::MAX_KEY_SIZE + 1 && max == Tree::MAX_KEY_SIZE
    ));
    // Rejected before touching any page.
    assert!(f.bpm.take_trace().is_empty());
    assert!(f.tree.is_empty().unwrap());

    let longest = vec![0; Tree::MAX_KEY_SIZE];
    assert!(f.tree.insert(&longest, rid(1)).unwrap());
    let mut result = Vec::new();
    assert!(f.tree.get_value(&longest, &mut result).unwrap());
    assert_eq!(result, [rid(1)]);
}

#[test]
fn insert_write_latches_only_the_leaf() {
    let f = setup();
    let (root, [_, l2, _]) = f.two_levels();
    f.bpm.start_tracing();
    assert!(f.tree.insert(&key(45), rid(45)).unwrap());
    assert_eq!(
        f.bpm.take_trace(),
        [
            PageAccess::Read(f.header_page_id),
            PageAccess::Read(root),
            PageAccess::Read(l2),
            PageAccess::Write(l2),
        ]
    );
    assert_eq!(f.scan(), [10, 20, 30, 40, 45, 50, 60, 70]);
}

#[test]
fn insert_into_root_leaf() {
    let f = setup();
    let leaf = f.leaf(&[10, 30], INVALID_PAGE_ID);
    f.set_root(leaf);
    f.bpm.start_tracing();
    assert!(f.tree.insert(&key(20), rid(20)).unwrap());
    assert_eq!(
        f.bpm.take_trace(),
        [
            PageAccess::Read(f.header_page_id),
            PageAccess::Read(leaf),
            PageAccess::Write(leaf),
        ]
    );
    assert_eq!(f.scan(), [10, 20, 30]);
}

#[test]
fn insert_revives_tombstoned_key() {
    let f = setup();
    let (_, [l1, _, _]) = f.two_levels();
    f.remove(l1, 20);
    assert_eq!(f.get(20), None);
    assert!(f.tree.insert(&key(20), rid(20)).unwrap());
    assert_eq!(f.get(20), Some(rid(20)));
    assert_eq!(f.scan(), [10, 20, 30, 40, 50, 60, 70]);
}

#[test]
fn insert_releases_every_page() {
    let f = setup();
    let (root, [l1, l2, l3]) = f.two_levels();
    for k in [15, 45, 65] {
        assert!(f.tree.insert(&key(k), rid(k)).unwrap());
    }
    for page_id in [f.header_page_id, root, l1, l2, l3] {
        assert_eq!(
            f.bpm.get_pin_count(page_id).unwrap_or(0),
            0,
            "page {page_id}"
        );
    }
}

#[test]
fn concurrent_inserts_into_one_leaf() {
    const THREADS: u64 = 8;
    const PER_THREAD: u64 = 20;
    let f = setup();
    std::thread::scope(|s| {
        for t in 0..THREADS {
            let tree = &f.tree;
            s.spawn(move || {
                for i in 0..PER_THREAD {
                    // Interleaved, so every thread inserts all over the leaf.
                    let k = i * THREADS + t;
                    assert!(tree.insert(&key(k), rid(k)).unwrap(), "key {k}");
                }
            });
        }
    });
    let all: Vec<u64> = (0..THREADS * PER_THREAD).collect();
    assert_eq!(f.scan(), all);
}

#[test]
fn full_root_leaf_splits_under_a_new_root() {
    let f = setup();
    let mut n = 0;
    while f.tree.get_root_page_id().unwrap() == INVALID_PAGE_ID || f.check() == 1 {
        assert!(f.tree.insert(&key(n), rid(n)).unwrap());
        n += 1;
    }
    assert_eq!(f.check(), 2);
    let root = f.tree.get_root_page_id().unwrap();
    let guard = f.bpm.checked_read_page(root, AccessType::Unknown).unwrap();
    assert_eq!(BtreeInternalPage::new(guard.get_data()).unwrap().size(), 2);
    drop(guard);
    assert_eq!(f.scan(), (0..n).collect::<Vec<_>>());
    for k in 0..n {
        assert_eq!(f.get(k), Some(rid(k)), "key {k}");
    }
}

#[test]
fn split_latches_from_the_header_down() {
    let f = setup();
    let leaf = f.leaf(&[], INVALID_PAGE_ID);
    f.set_root(leaf);
    let mut n = 0;
    {
        let mut guard = f.bpm.checked_write_page(leaf, AccessType::Unknown).unwrap();
        let mut page = Leaf::init(guard.get_data_mut());
        while page.insert(&key(n), rid(n), &CMP).is_ok() {
            n += 1;
        }
    }
    f.bpm.start_tracing();
    assert!(f.tree.insert(&key(n), rid(n)).unwrap());
    let trace = f.bpm.take_trace();
    let root = f.tree.get_root_page_id().unwrap();
    let PageAccess::New(right) = trace[5] else {
        panic!("no new leaf in {trace:?}");
    };
    assert_eq!(
        trace,
        [
            // The optimistic attempt finds the leaf full.
            PageAccess::Read(f.header_page_id),
            PageAccess::Read(leaf),
            PageAccess::Write(leaf),
            PageAccess::Write(f.header_page_id),
            PageAccess::Write(leaf),
            PageAccess::New(right),
            PageAccess::Write(right),
            PageAccess::New(root),
            PageAccess::Write(root),
        ]
    );
    assert_eq!(f.check(), 2);
    assert_eq!(f.scan(), (0..=n).collect::<Vec<_>>());
}

#[test]
fn split_stops_at_a_parent_with_room() {
    let f = setup();
    let (root, [l1, ..]) = f.two_levels();
    // 16-byte keys between 10 and 20, so they all belong in the first leaf.
    let between = |n: u64| [key(10), key(n)].concat();
    let mut n = 0;
    {
        let mut guard = f.bpm.checked_write_page(l1, AccessType::Unknown).unwrap();
        let mut page = Leaf::new(guard.get_data_mut()).unwrap();
        while page.insert(&between(n), rid(n), &CMP).is_ok() {
            n += 1;
        }
    }
    f.bpm.start_tracing();
    assert!(f.tree.insert(&between(n), rid(n)).unwrap());
    let trace = f.bpm.take_trace();
    let PageAccess::New(right) = trace[7] else {
        panic!("no new leaf in {trace:?}");
    };
    assert_eq!(
        trace,
        [
            PageAccess::Read(f.header_page_id),
            PageAccess::Read(root),
            PageAccess::Read(l1),
            PageAccess::Write(l1),
            // The root has room for a separator, so the header page is
            // released once the root is latched and it stays the root.
            PageAccess::Write(f.header_page_id),
            PageAccess::Write(root),
            PageAccess::Write(l1),
            PageAccess::New(right),
            PageAccess::Write(right),
        ]
    );
    assert_eq!(f.tree.get_root_page_id().unwrap(), root);
    assert_eq!(f.check(), 2);
    let guard = f.bpm.checked_read_page(root, AccessType::Unknown).unwrap();
    assert_eq!(BtreeInternalPage::new(guard.get_data()).unwrap().size(), 4);
    drop(guard);
    for k in 0..=n {
        assert_eq!(f.get_key(&between(k)), Some(rid(k)), "key {k}");
    }
    for k in [10, 20, 30, 40, 50, 60, 70] {
        assert_eq!(f.get(k), Some(rid(k)), "key {k}");
    }
}

#[test]
fn many_inserts_build_a_valid_tree() {
    for (name, order) in [
        ("ascending", (0..2000).collect::<Vec<u64>>()),
        ("descending", (0..2000).rev().collect()),
        ("shuffled", shuffled(2000)),
    ] {
        let f = setup();
        for &k in &order {
            assert!(
                f.tree.insert(&long_key(k, 300), rid(k)).unwrap(),
                "{name}: key {k}"
            );
        }
        let height = f.check();
        assert!(height >= 3, "{name}: height {height}");
        assert_eq!(f.scan(), (0..2000).collect::<Vec<_>>(), "{name}");
        for k in 0..2000 {
            assert_eq!(
                f.get_key(&long_key(k, 300)),
                Some(rid(k)),
                "{name}: key {k}"
            );
            assert!(!f.tree.insert(&long_key(k, 300), rid(k + 1)).unwrap());
        }
    }
}

#[test]
fn inserts_of_mixed_key_lengths() {
    let f = setup();
    let len = |k: u64| 8 + (k as usize * 7919) % (Tree::MAX_KEY_SIZE - 7);
    for k in shuffled(1000) {
        assert!(
            f.tree.insert(&long_key(k, len(k)), rid(k)).unwrap(),
            "key {k}"
        );
    }
    assert!(f.check() >= 3);
    assert_eq!(f.scan(), (0..1000).collect::<Vec<_>>());
    for k in 0..1000 {
        assert_eq!(f.get_key(&long_key(k, len(k))), Some(rid(k)), "key {k}");
    }
}

#[test]
fn inserts_of_max_size_keys() {
    let f = setup();
    for k in shuffled(300) {
        assert!(
            f.tree
                .insert(&long_key(k, Tree::MAX_KEY_SIZE), rid(k))
                .unwrap()
        );
    }
    assert!(f.check() >= 4);
    assert_eq!(f.scan(), (0..300).collect::<Vec<_>>());
}

#[test]
fn concurrent_inserts_with_splits() {
    const THREADS: u64 = 8;
    const PER_THREAD: u64 = 400;
    let f = setup_with_frames(128);
    std::thread::scope(|s| {
        for t in 0..THREADS {
            let f = &f;
            s.spawn(move || {
                for i in 0..PER_THREAD {
                    let k = i * THREADS + t;
                    let key = long_key(k, 200);
                    assert!(f.tree.insert(&key, rid(k)).unwrap(), "key {k}");
                    assert_eq!(f.get_key(&key), Some(rid(k)), "key {k}");
                }
            });
        }
    });
    assert!(f.check() >= 3);
    assert_eq!(f.scan(), (0..THREADS * PER_THREAD).collect::<Vec<_>>());
}

#[test]
fn remove_from_empty_tree_does_nothing() {
    let f = setup_no_tombs(16);
    f.bpm.start_tracing();
    f.tree.remove(&key(10)).unwrap();
    assert_eq!(f.bpm.take_trace(), [PageAccess::Read(f.header_page_id)]);
    assert!(f.tree.is_empty().unwrap());
}

#[test]
fn remove_of_missing_key_changes_nothing() {
    let f = setup_no_tombs(16);
    for k in [10, 20, 30] {
        f.tree.insert(&key(k), rid(k)).unwrap();
    }
    f.tree.remove(&key(15)).unwrap();
    f.tree.remove(&key(40)).unwrap();
    assert_eq!(f.scan(), [10, 20, 30]);
}

#[test]
fn remove_from_root_leaf_latches_only_the_leaf() {
    let f = setup_no_tombs(16);
    for k in [10, 20, 30] {
        f.tree.insert(&key(k), rid(k)).unwrap();
    }
    let root = f.tree.get_root_page_id().unwrap();
    f.bpm.start_tracing();
    // A root leaf may be underfull: it only has to keep a key.
    f.tree.remove(&key(20)).unwrap();
    assert_eq!(
        f.bpm.take_trace(),
        [
            PageAccess::Read(f.header_page_id),
            PageAccess::Read(root),
            PageAccess::Write(root),
        ]
    );
    assert_eq!(f.scan(), [10, 30]);
    assert_eq!(f.get(20), None);
}

#[test]
fn removing_the_last_key_empties_the_tree() {
    let f = setup_no_tombs(16);
    f.tree.insert(&key(10), rid(10)).unwrap();
    let root = f.tree.get_root_page_id().unwrap();
    f.bpm.start_tracing();
    f.tree.remove(&key(10)).unwrap();
    assert_eq!(
        f.bpm.take_trace(),
        [
            // The optimistic attempt finds the root leaf would be left empty.
            PageAccess::Read(f.header_page_id),
            PageAccess::Read(root),
            PageAccess::Write(root),
            PageAccess::Write(f.header_page_id),
            PageAccess::Write(root),
            PageAccess::Delete(root),
        ]
    );
    assert!(f.tree.is_empty().unwrap());
    assert!(f.tree.begin().unwrap().is_end());

    assert!(f.tree.insert(&key(20), rid(20)).unwrap());
    assert_eq!(f.scan(), [20]);
}

#[test]
fn tombstoning_remove_latches_only_the_leaf() {
    let f = setup();
    let (root, [l1, ..]) = f.two_levels();
    f.bpm.start_tracing();
    // The leaf is underfull, but a tombstone frees nothing.
    f.tree.remove(&key(20)).unwrap();
    assert_eq!(
        f.bpm.take_trace(),
        [
            PageAccess::Read(f.header_page_id),
            PageAccess::Read(root),
            PageAccess::Read(l1),
            PageAccess::Write(l1),
        ]
    );
    assert_eq!(f.scan(), [10, 30, 40, 50, 60, 70]);
}

/// Inserts `0..` into an empty tree until its root leaf splits, and returns
/// how many keys that took and the two leaves.
fn two_leaves(f: &Fixture<0>) -> (u64, PageId, PageId) {
    let mut n = 0;
    while f.root_children().is_none() {
        f.tree.insert(&key(n), rid(n)).unwrap();
        n += 1;
    }
    let children = f.root_children().unwrap();
    assert_eq!(children.len(), 2);
    (n, children[0], children[1])
}

#[test]
fn underfull_leaf_merges_with_right_sibling() {
    let f = setup_no_tombs(16);
    let (n, left, right) = two_leaves(&f);
    let mut k = 0;
    while f.root_children().is_some() {
        f.tree.remove(&key(k)).unwrap();
        k += 1;
        f.check();
    }
    // The right leaf merged into the left one, which is the root now.
    assert_eq!(f.tree.get_root_page_id().unwrap(), left);
    assert_eq!(f.bpm.get_pin_count(right), None, "right leaf not deleted");
    assert_eq!(f.scan(), (k..n).collect::<Vec<_>>());
}

#[test]
fn last_leaf_merges_with_left_sibling() {
    let f = setup_no_tombs(16);
    let (n, left, right) = two_leaves(&f);
    let mut k = n;
    while f.root_children().is_some() {
        k -= 1;
        f.tree.remove(&key(k)).unwrap();
        f.check();
    }
    assert_eq!(f.tree.get_root_page_id().unwrap(), left);
    assert_eq!(f.bpm.get_pin_count(right), None, "right leaf not deleted");
    assert_eq!(f.scan(), (0..k).collect::<Vec<_>>());
}

#[test]
fn underfull_leaf_takes_keys_from_a_fuller_sibling() {
    let f = setup_no_tombs(16);
    let (n, left, right) = two_leaves(&f);
    // Fill the right leaf most of the way, so the two no longer fit in one.
    let mut m = n;
    while f.leaf_used_space(right) < 3 * BtreeLeafPage::<&[u8; PAGE_SIZE], 0>::CAPACITY / 4 {
        f.tree.insert(&key(m), rid(m)).unwrap();
        m += 1;
    }
    // Remove from the left leaf down to half full, then once more.
    let half = BtreeLeafPage::<&[u8; PAGE_SIZE], 0>::CAPACITY / 2;
    let entry_size = BtreeLeafPage::<&[u8; PAGE_SIZE], 0>::entry_size(8);
    let mut k = 0;
    while f.leaf_used_space(left) - entry_size >= half {
        f.tree.remove(&key(k)).unwrap();
        k += 1;
    }
    f.tree.remove(&key(k)).unwrap();
    k += 1;
    assert_eq!(f.check(), 2);
    assert_eq!(f.root_children().unwrap(), [left, right]);
    // Keys moved left until the two were about even.
    let (l, r) = (f.leaf_used_space(left), f.leaf_used_space(right));
    assert!(l >= half, "left {l} bytes");
    assert!(
        l.abs_diff(r) <= 2 * entry_size,
        "left {l} bytes, right {r} bytes"
    );
    assert_eq!(f.scan(), (k..m).collect::<Vec<_>>());
}

/// Removes every key of a tree of 2,000 keys of 300 bytes, in `order`,
/// checking the tree as it shrinks.
fn remove_all<const NUM_TOMBS: usize>(f: &Fixture<NUM_TOMBS>, order: &[u64], name: &str) {
    let n = order.len() as u64;
    for k in 0..n {
        f.tree.insert(&long_key(k, 300), rid(k)).unwrap();
    }
    assert!(f.check() >= 3, "{name}");
    let mut left: Vec<u64> = (0..n).collect();
    for (i, &k) in order.iter().enumerate() {
        f.tree.remove(&long_key(k, 300)).unwrap();
        assert_eq!(f.get_key(&long_key(k, 300)), None, "{name}: key {k}");
        if i % 100 == 99 {
            left.retain(|&x| !order[..=i].contains(&x));
            f.check();
            assert_eq!(f.scan(), left, "{name}: after {} removes", i + 1);
        }
    }
    assert!(f.scan().is_empty(), "{name}");
}

#[test]
fn many_removes_keep_the_tree_valid() {
    for (name, order) in [
        ("ascending", (0..2000).collect::<Vec<u64>>()),
        ("descending", (0..2000).rev().collect()),
        ("shuffled", shuffled(2000)),
    ] {
        let f = setup_no_tombs(16);
        remove_all(&f, &order, name);
        assert!(f.tree.is_empty().unwrap(), "{name}");
    }
}

#[test]
fn many_removes_with_tombstones() {
    let f = setup();
    remove_all(&f, &shuffled(2000), "shuffled");
    // Tombstones may keep keys in a root leaf, but none is seen.
    assert_eq!(f.check(), 1);
}

#[test]
fn removes_of_mixed_key_lengths() {
    let f = setup_no_tombs(16);
    let len = |k: u64| 8 + (k as usize * 7919) % (Tree::MAX_KEY_SIZE - 7);
    for k in 0..1000 {
        f.tree.insert(&long_key(k, len(k)), rid(k)).unwrap();
    }
    let order = shuffled(1000);
    for (i, &k) in order.iter().enumerate() {
        f.tree.remove(&long_key(k, len(k))).unwrap();
        if i % 50 == 49 {
            f.check();
        }
    }
    assert!(f.tree.is_empty().unwrap());
}

#[test]
fn removed_keys_can_be_inserted_again() {
    let f = setup_no_tombs(16);
    for k in 0..1000 {
        f.tree.insert(&long_key(k, 300), rid(k)).unwrap();
    }
    for k in (0..1000).filter(|k| k % 3 != 0) {
        f.tree.remove(&long_key(k, 300)).unwrap();
    }
    f.check();
    assert_eq!(
        f.scan(),
        (0..1000).filter(|k| k % 3 == 0).collect::<Vec<_>>()
    );
    for k in (0..1000).filter(|k| k % 3 != 0) {
        assert!(f.tree.insert(&long_key(k, 300), rid(k)).unwrap());
    }
    f.check();
    assert_eq!(f.scan(), (0..1000).collect::<Vec<_>>());
}

#[test]
fn concurrent_removes_inserts_and_scans() {
    const N: u64 = 3000;
    let f = setup_no_tombs(256);
    for k in 0..N {
        f.tree.insert(&long_key(k, 200), rid(k)).unwrap();
    }
    let done = AtomicBool::new(false);
    /// Stops the scans when dropped, so a writer that panics does not leave
    /// them running forever.
    struct Stop<'a>(&'a AtomicBool);
    impl Drop for Stop<'_> {
        fn drop(&mut self) {
            self.0.store(true, AtomicOrdering::Release);
        }
    }
    std::thread::scope(|s| {
        let stop = Stop(&done);
        let mut writers = Vec::new();
        // Keys with `k % 8` in 0..4 are removed, 4..6 inserted again after
        // being removed, and 6..8 stay.
        for t in 0..6 {
            let f = &f;
            writers.push(s.spawn(move || {
                for k in (0..N).filter(|k| k % 8 == t) {
                    f.tree.remove(&long_key(k, 200)).unwrap();
                    assert_eq!(f.get_key(&long_key(k, 200)), None, "key {k}");
                }
                if t >= 4 {
                    for k in (0..N).filter(|k| k % 8 == t) {
                        assert!(f.tree.insert(&long_key(k, 200), rid(k)).unwrap());
                    }
                }
            }));
        }
        for _ in 0..2 {
            let (f, done) = (&f, &done);
            s.spawn(move || {
                while !done.load(AtomicOrdering::Acquire) {
                    let keys = f.scan();
                    assert!(keys.windows(2).all(|w| w[0] < w[1]), "scan out of order");
                    let staying = keys.iter().filter(|k| *k % 8 >= 6).count() as u64;
                    assert_eq!(staying, (0..N).filter(|k| k % 8 >= 6).count() as u64);
                }
            });
        }
        for w in writers {
            w.join().unwrap();
        }
        drop(stop);
    });
    f.check();
    assert_eq!(f.scan(), (0..N).filter(|k| k % 8 >= 4).collect::<Vec<_>>());
}
