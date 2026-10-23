use common::{INVALID_PAGE_ID, Rid};
use storage::StorageError;
use storage::page::btree_leaf_page::BtreeLeafPage;
use storage::page::btree_page::BytewiseComparator;
use storage::page::page_header::{PageHeader, PageType};
use storage::page::{PAGE_SIZE, PageBuf};

type Page<'a, const NUM_TOMBS: usize = 2> = BtreeLeafPage<&'a mut [u8; PAGE_SIZE], NUM_TOMBS>;

const CMP: BytewiseComparator = BytewiseComparator;

/// Big-endian, so byte order matches numeric order.
fn key(n: u64) -> [u8; 8] {
    n.to_be_bytes()
}

/// A `len`-byte key that orders as `n`: `key(n)` followed by zeroes.
fn long_key(n: u64, len: usize) -> Vec<u8> {
    let mut key = key(n).to_vec();
    key.resize(len, 0);
    key
}

fn num(key: &[u8]) -> u64 {
    u64::from_be_bytes(key[..8].try_into().unwrap())
}

fn rid(n: u64) -> Rid {
    Rid::new(n as i32, n as u16 + 1)
}

/// Every key on the page, tombstoned ones included.
fn keys<const N: usize>(page: &Page<'_, N>) -> Vec<u64> {
    (0..page.size()).map(|i| num(page.key_at(i))).collect()
}

fn tombstoned_keys<const N: usize>(page: &Page<'_, N>) -> Vec<u64> {
    page.tombstones().map(|i| num(page.key_at(i))).collect()
}

/// A page holding keys 10, 20, ... with their `rid`s.
fn page_with<const N: usize>(buf: &mut PageBuf, count: u64) -> Page<'_, N> {
    let mut page = Page::<N>::init(&mut *buf);
    for i in 1..=count {
        page.insert(&key(10 * i), rid(10 * i), &CMP).unwrap();
    }
    page
}

/// Inserts `len`-byte keys 1, 2, ... until the page is full, and returns
/// how many fit.
fn fill<const N: usize>(page: &mut Page<'_, N>, len: usize) -> u64 {
    let mut n = 0;
    loop {
        match page.insert(&long_key(n + 1, len), rid(n + 1), &CMP) {
            Ok(_) => n += 1,
            Err(StorageError::PageFull) => return n,
            Err(e) => panic!("{e}"),
        }
    }
}

#[test]
fn init_and_reopen() {
    let mut buf = PageBuf::zeroed();
    let page = Page::<2>::init(&mut *buf);
    assert_eq!(page.size(), 0);
    assert_eq!(page.used_space(), 0);
    assert_eq!(page.free_space(), Page::<2>::CAPACITY);
    assert_eq!(page.next_page_id(), INVALID_PAGE_ID);
    assert_eq!(page.num_tombstones(), 0);

    let mut page = page_with::<2>(&mut buf, 3);
    page.set_next_page_id(42);
    page.remove(&key(20), &CMP);
    let page = BtreeLeafPage::<_, 2>::new(&*buf).unwrap();
    assert_eq!(page.size(), 3);
    assert_eq!(page.next_page_id(), 42);
    assert_eq!(page.lookup(&key(30), &CMP), Some(rid(30)));
    assert_eq!(page.lookup(&key(20), &CMP), None);
    assert_eq!(page.tombstones().collect::<Vec<_>>(), [1]);
}

#[test]
fn new_rejects_other_page_types() {
    let mut buf = PageBuf::zeroed();
    PageHeader::init(&mut buf, PageType::BTreeInternal);
    let Err(err) = BtreeLeafPage::<_, 2>::new(&*buf) else {
        panic!("an internal page opened as a leaf page");
    };
    assert!(matches!(
        err,
        StorageError::UnexpectedPageType {
            expected: PageType::BTreeLeaf,
            found: 2
        }
    ));
}

#[test]
fn insert_keeps_keys_sorted() {
    let mut buf = PageBuf::zeroed();
    let mut page = Page::<2>::init(&mut *buf);
    assert_eq!(page.insert(&key(30), rid(30), &CMP).unwrap(), 0);
    assert_eq!(page.insert(&key(10), rid(10), &CMP).unwrap(), 0);
    assert_eq!(page.insert(&key(50), rid(50), &CMP).unwrap(), 2);
    assert_eq!(page.insert(&key(20), rid(20), &CMP).unwrap(), 1);

    assert_eq!(keys(&page), [10, 20, 30, 50]);
    for k in [10, 20, 30, 50] {
        assert_eq!(page.lookup(&key(k), &CMP), Some(rid(k)));
    }
    assert_eq!(page.lookup(&key(40), &CMP), None);
    assert_eq!(page.lookup(&key(60), &CMP), None);
    assert_eq!(page.key_index(&key(25), &CMP), 2);
    assert_eq!(page.key_index(&key(30), &CMP), 2);
    assert_eq!(page.key_index(&key(99), &CMP), 4);
}

#[test]
fn keys_of_different_lengths() {
    let mut buf = PageBuf::zeroed();
    let mut page = Page::<2>::init(&mut *buf);
    for (n, k) in [&b"b"[..], b"", b"abc", b"ab"].into_iter().enumerate() {
        page.insert(k, rid(n as u64), &CMP).unwrap();
    }
    let keys: Vec<&[u8]> = (0..page.size()).map(|i| page.key_at(i)).collect();
    assert_eq!(keys, [&b""[..], b"ab", b"abc", b"b"]);
    assert_eq!(page.lookup(b"abc", &CMP), Some(rid(2)));
    assert_eq!(page.lookup(b"", &CMP), Some(rid(1)));
    assert_eq!(page.lookup(b"a", &CMP), None);
}

#[test]
fn space_is_counted_in_bytes() {
    let mut buf = PageBuf::zeroed();
    let mut page = Page::<0>::init(&mut *buf);
    page.insert(&long_key(1, 100), rid(1), &CMP).unwrap();
    page.insert(&key(2), rid(2), &CMP).unwrap();
    let used = Page::<0>::entry_size(100) + Page::<0>::entry_size(8);
    assert_eq!(page.used_space(), used);
    assert_eq!(page.free_space(), Page::<0>::CAPACITY - used);

    page.remove(&long_key(1, 100), &CMP);
    assert_eq!(page.used_space(), Page::<0>::entry_size(8));
}

#[test]
fn insert_duplicate_fails() {
    let mut buf = PageBuf::zeroed();
    let mut page = page_with::<2>(&mut buf, 3);
    let err = page.insert(&key(20), rid(99), &CMP).unwrap_err();
    assert!(matches!(err, StorageError::DuplicateKey));
    assert_eq!(page.lookup(&key(20), &CMP), Some(rid(20)));
}

#[test]
fn insert_into_full_page_fails() {
    let mut buf = PageBuf::zeroed();
    let mut page = Page::<2>::init(&mut *buf);
    let count = fill(&mut page, 8);
    assert_eq!(
        count as usize,
        Page::<2>::CAPACITY / Page::<2>::entry_size(8)
    );
    assert!(!page.has_room_for(8));
    assert!(page.free_space() < Page::<2>::entry_size(8));
    assert_eq!(keys(&page), (1..=count).collect::<Vec<_>>());
}

#[test]
fn key_too_large() {
    let max = Page::<2>::MAX_KEY_SIZE;
    let mut buf = PageBuf::zeroed();
    let mut page = Page::<2>::init(&mut *buf);
    let err = page
        .insert(&long_key(1, max + 1), rid(1), &CMP)
        .unwrap_err();
    assert!(
        matches!(err, StorageError::KeyTooLarge { size, max: m } if size == max + 1 && m == max)
    );
    page.insert(&long_key(1, max), rid(1), &CMP).unwrap();
}

#[test]
fn holes_are_reused() {
    let mut buf = PageBuf::zeroed();
    let mut page = Page::<0>::init(&mut *buf);
    let count = fill(&mut page, 200);
    for n in (1..=count).step_by(2) {
        page.remove(&long_key(n, 200), &CMP);
    }
    // The removed keys left holes in the middle of the key area, so these
    // only fit once it is compacted.
    let free = page.free_space();
    let added = (free / Page::<0>::entry_size(300)) as u64;
    assert!(added > 1);
    for n in 0..added {
        page.insert(&long_key(1000 + n, 300), rid(1000 + n), &CMP)
            .unwrap();
    }
    for n in (2..=count).step_by(2) {
        assert_eq!(page.lookup(&long_key(n, 200), &CMP), Some(rid(n)));
    }
    for n in 0..added {
        let index = page.key_index(&long_key(1000 + n, 300), &CMP);
        assert_eq!(page.key_at(index), long_key(1000 + n, 300));
    }
}

#[test]
fn remove_tombstones_the_key() {
    let mut buf = PageBuf::zeroed();
    let mut page = page_with::<2>(&mut buf, 4);
    let used = page.used_space();
    assert_eq!(page.remove(&key(20), &CMP), Some(rid(20)));
    assert_eq!(page.remove(&key(20), &CMP), None);
    assert_eq!(page.remove(&key(25), &CMP), None);

    // The key keeps its slot and bytes, but lookups skip it.
    assert_eq!(keys(&page), [10, 20, 30, 40]);
    assert_eq!(page.size(), 4);
    assert_eq!(page.live_size(), 3);
    assert_eq!(page.used_space(), used);
    assert!(page.is_tombstone(1));
    assert_eq!(page.lookup(&key(20), &CMP), None);
}

#[test]
fn tombstones_follow_their_keys() {
    let mut buf = PageBuf::zeroed();
    let mut page = page_with::<2>(&mut buf, 4);
    page.remove(&key(30), &CMP);
    page.insert(&key(5), rid(5), &CMP).unwrap();
    page.insert(&key(25), rid(25), &CMP).unwrap();
    assert_eq!(tombstoned_keys(&page), [30]);
    page.remove_at(0);
    assert_eq!(tombstoned_keys(&page), [30]);
    assert_eq!(page.lookup(&key(30), &CMP), None);
    assert_eq!(page.lookup(&key(40), &CMP), Some(rid(40)));
}

#[test]
fn full_tombstone_buffer_removes_oldest() {
    let mut buf = PageBuf::zeroed();
    let mut page = page_with::<2>(&mut buf, 5);
    page.remove(&key(20), &CMP);
    page.remove(&key(40), &CMP);
    assert_eq!(tombstoned_keys(&page), [20, 40]);

    // 20 is the oldest, so it goes for good and 50 takes its place.
    assert_eq!(page.remove(&key(50), &CMP), Some(rid(50)));
    assert_eq!(keys(&page), [10, 30, 40, 50]);
    assert_eq!(tombstoned_keys(&page), [40, 50]);

    // Here the oldest tombstone sits after the removed key.
    assert_eq!(page.remove(&key(10), &CMP), Some(rid(10)));
    assert_eq!(keys(&page), [10, 30, 50]);
    assert_eq!(tombstoned_keys(&page), [50, 10]);
    assert_eq!(page.live_size(), 1);
    assert_eq!(page.lookup(&key(30), &CMP), Some(rid(30)));
}

#[test]
fn no_tombstones_removes_at_once() {
    let mut buf = PageBuf::zeroed();
    let mut page = page_with::<0>(&mut buf, 3);
    assert_eq!(page.remove(&key(20), &CMP), Some(rid(20)));
    assert_eq!(keys(&page), [10, 30]);
    assert_eq!(page.num_tombstones(), 0);
}

#[test]
fn insert_revives_tombstoned_key() {
    let mut buf = PageBuf::zeroed();
    let mut page = Page::<2>::init(&mut *buf);
    let count = fill(&mut page, 8);
    page.remove(&long_key(2, 8), &CMP);
    // The page is full, but reviving reuses the key's slot.
    assert_eq!(page.insert(&key(2), rid(99), &CMP).unwrap(), 1);
    assert_eq!(page.lookup(&key(2), &CMP), Some(rid(99)));
    assert_eq!(page.num_tombstones(), 0);
    assert_eq!(page.size() as u64, count);
}

#[test]
fn remove_at_returns_key_and_rid() {
    let mut buf = PageBuf::zeroed();
    let mut page = page_with::<2>(&mut buf, 3);
    page.remove(&key(20), &CMP);
    assert_eq!(page.remove_at(1), (key(20).to_vec(), rid(20)));
    assert_eq!(page.num_tombstones(), 0);
    assert_eq!(page.remove_at(1), (key(30).to_vec(), rid(30)));
    assert_eq!(keys(&page), [10]);
    assert_eq!(page.used_space(), Page::<2>::entry_size(8));
}

#[test]
fn purge_tombstones() {
    let mut buf = PageBuf::zeroed();
    let mut page = page_with::<3>(&mut buf, 6);
    page.remove(&key(50), &CMP);
    page.remove(&key(10), &CMP);
    page.remove(&key(30), &CMP);
    page.purge_tombstones();
    assert_eq!(keys(&page), [20, 40, 60]);
    assert_eq!(page.num_tombstones(), 0);
    for k in [20, 40, 60] {
        assert_eq!(page.lookup(&key(k), &CMP), Some(rid(k)));
    }
}

#[test]
fn fill_with_small_keys() {
    let mut buf = PageBuf::zeroed();
    let mut page = Page::<16>::init(&mut *buf);
    let count = (Page::<16>::CAPACITY / Page::<16>::entry_size(8)) as u64;
    assert!(count > 200);
    // Insert in descending order, so every insert shifts the whole page.
    for i in (0..count).rev() {
        page.insert(&key(i), rid(i), &CMP).unwrap();
    }
    for i in (0..16).map(|i| i * 13) {
        page.remove(&key(i), &CMP);
    }
    assert_eq!(page.num_tombstones(), 16);
    assert!(matches!(
        page.insert(&key(u64::MAX), rid(0), &CMP).unwrap_err(),
        StorageError::PageFull
    ));
    for i in 0..count {
        let expected = (!page.is_tombstone(i as usize)).then(|| rid(i));
        assert_eq!(page.lookup(&key(i), &CMP), expected);
    }
    assert_eq!(keys(&page), (0..count).collect::<Vec<_>>());
}

#[test]
fn split_balances_bytes() {
    let mut left_buf = PageBuf::zeroed();
    let mut right_buf = PageBuf::zeroed();
    let mut left = Page::<0>::init(&mut *left_buf);
    left.set_next_page_id(42);
    // A few large keys up front and many small ones after: halving by count
    // would leave the left page much fuller.
    let lens = [600, 500, 400]
        .into_iter()
        .chain(std::iter::repeat_n(20, 70));
    for (n, len) in lens.enumerate() {
        left.insert(&long_key(n as u64, len), rid(n as u64), &CMP)
            .unwrap();
    }
    let size = left.size();
    let used = left.used_space();

    let mut right = Page::<0>::init(&mut *right_buf);
    left.split_to(&mut right, 7);

    assert_eq!(left.size() + right.size(), size);
    assert_eq!(left.used_space() + right.used_space(), used);
    let largest = Page::<0>::entry_size(600);
    assert!(left.used_space().abs_diff(right.used_space()) <= largest);
    assert!(left.size() < right.size());

    let all: Vec<u64> = keys(&left).into_iter().chain(keys(&right)).collect();
    assert_eq!(all, (0..size as u64).collect::<Vec<_>>());
    assert_eq!(left.next_page_id(), 7);
    assert_eq!(right.next_page_id(), 42);
}

#[test]
fn split_moves_tombstones() {
    let mut left_buf = PageBuf::zeroed();
    let mut right_buf = PageBuf::zeroed();
    let mut left = page_with::<4>(&mut left_buf, 10);
    for k in [90, 20, 80] {
        left.remove(&key(k), &CMP);
    }
    let mut right = Page::<4>::init(&mut *right_buf);
    left.split_to(&mut right, 7);

    assert_eq!(keys(&left), [10, 20, 30, 40, 50]);
    assert_eq!(keys(&right), [60, 70, 80, 90, 100]);
    assert_eq!(tombstoned_keys(&left), [20]);
    assert_eq!(tombstoned_keys(&right), [90, 80]);
    assert_eq!(right.lookup(&key(90), &CMP), None);
    assert_eq!(right.lookup(&key(100), &CMP), Some(rid(100)));
}

#[test]
fn split_leaves_room_for_the_largest_key() {
    let max = Page::<2>::MAX_KEY_SIZE;
    for len in [max, max / 2 + 1, 8] {
        let mut left_buf = PageBuf::zeroed();
        let mut right_buf = PageBuf::zeroed();
        let mut left = Page::<2>::init(&mut *left_buf);
        let count = fill(&mut left, len);
        assert!(count >= 4, "{count} keys of {len} bytes");
        assert!(!left.has_room_for(max));

        let mut right = Page::<2>::init(&mut *right_buf);
        left.split_to(&mut right, 7);
        assert!(left.has_room_for(max), "keys of {len} bytes");
        assert!(right.has_room_for(max), "keys of {len} bytes");
    }
}

#[test]
fn underfull_below_half() {
    let mut buf = PageBuf::zeroed();
    let mut page = Page::<0>::init(&mut *buf);
    assert!(page.is_underfull());
    let mut n = 0;
    while page.is_underfull() {
        n += 1;
        page.insert(&long_key(n, 100), rid(n), &CMP).unwrap();
    }
    assert!(page.used_space() >= Page::<0>::CAPACITY / 2);
    page.remove(&long_key(1, 100), &CMP);
    assert!(page.is_underfull());
}

#[test]
#[should_panic(expected = "key index 3 out of range")]
fn key_past_size_panics() {
    let mut buf = PageBuf::zeroed();
    let page = page_with::<2>(&mut buf, 3);
    page.key_at(3);
}

#[test]
#[should_panic(expected = "not empty")]
fn split_into_non_empty_page_panics() {
    let mut left_buf = PageBuf::zeroed();
    let mut right_buf = PageBuf::zeroed();
    let mut left = page_with::<2>(&mut left_buf, 4);
    let mut right = page_with::<2>(&mut right_buf, 1);
    left.split_to(&mut right, 7);
}
