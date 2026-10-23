//! The read side of the B+tree, on trees built page by page since `insert`
//! is not written yet.

use std::cmp::Ordering;
use std::sync::Arc;

use common::{INVALID_PAGE_ID, PageId, Rid};
use storage::StorageError;
use storage::buffer::arc_replacer::AccessType;
use storage::buffer::buffer_pool_manager::BufferPoolManager;
use storage::buffer::traced_buffer_pool_manager::{PageAccess, TracedBufferPoolManager};
use storage::disk::disk_manager::DiskManager;
use storage::index::btree::Btree;
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

struct Fixture {
    _dir: TempDir,
    bpm: Arc<TracedBufferPoolManager>,
    header_page_id: PageId,
    tree: Tree,
}

/// An empty tree.
fn setup() -> Fixture {
    let dir = TempDir::new().unwrap();
    let dm = Arc::new(DiskManager::new(dir.path().join("test.db")).unwrap());
    let bpm = Arc::new(TracedBufferPoolManager::new(Arc::new(
        BufferPoolManager::new(16, dm),
    )));
    let header_page_id = bpm.new_page();
    let tree = Tree::new("idx", header_page_id, Arc::clone(&bpm), CMP).unwrap();
    Fixture {
        _dir: dir,
        bpm,
        header_page_id,
        tree,
    }
}

impl Fixture {
    /// Writes a leaf holding `keys` with their `rid`s, linked to `next`.
    fn leaf(&self, keys: &[u64], next: PageId) -> PageId {
        let page_id = self.bpm.new_page();
        let mut guard = self
            .bpm
            .checked_write_page(page_id, AccessType::Unknown)
            .unwrap();
        let mut page = Leaf::init(guard.get_data_mut());
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
        Leaf::new(guard.get_data_mut())
            .unwrap()
            .remove(&key(k), &CMP)
            .unwrap();
    }

    fn get(&self, k: u64) -> Option<Rid> {
        let mut result = Vec::new();
        let found = self.tree.get_value(&key(k), &mut result).unwrap();
        assert_eq!(found, !result.is_empty());
        assert!(result.len() <= 1);
        result.pop()
    }

    fn scan(&self) -> Vec<u64> {
        self.tree
            .begin()
            .unwrap()
            .map(|entry| {
                let (k, r) = entry.unwrap();
                let k = u64::from_be_bytes(k.try_into().unwrap());
                assert_eq!(r, rid(k));
                k
            })
            .collect()
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
