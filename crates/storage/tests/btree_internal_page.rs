use common::PageId;
use storage::StorageError;
use storage::page::btree_internal_page::BtreeInternalPage;
use storage::page::btree_page::BytewiseComparator;
use storage::page::page_header::{PageHeader, PageType};
use storage::page::{PAGE_SIZE, PageBuf};

type Page<'a> = BtreeInternalPage<&'a mut [u8; PAGE_SIZE]>;

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

fn keys(page: &Page) -> Vec<u64> {
    (1..page.size()).map(|i| num(page.key_at(i))).collect()
}

fn values(page: &Page) -> Vec<PageId> {
    (0..page.size()).map(|i| page.value_at(i)).collect()
}

/// A page with children 100, 110, 120, ... split by keys 10, 20, ...
fn page_with(buf: &mut PageBuf, children: usize) -> Page<'_> {
    let mut page = Page::init(&mut *buf);
    page.populate_new_root(100, &key(10), 110).unwrap();
    for i in 2..children {
        page.insert(&key(10 * i as u64), 100 + 10 * i as PageId, &CMP)
            .unwrap();
    }
    page
}

/// A root over child 0 with `len`-byte keys 1, 2, ... and children 1, 2, ...
/// added until the page is full. Returns how many children it has.
fn fill(page: &mut Page<'_>, len: usize) -> usize {
    page.push_front(&[], 0).unwrap();
    let mut n = 0;
    loop {
        match page.insert(&long_key(n + 1, len), n as PageId + 1, &CMP) {
            Ok(_) => n += 1,
            Err(StorageError::PageFull) => return page.size(),
            Err(e) => panic!("{e}"),
        }
    }
}

#[test]
fn init_and_reopen() {
    let mut buf = PageBuf::zeroed();
    let page = Page::init(&mut *buf);
    assert_eq!(page.size(), 0);
    assert_eq!(page.used_space(), 0);
    assert_eq!(page.free_space(), Page::CAPACITY);

    page_with(&mut buf, 3);
    let page = BtreeInternalPage::new(&*buf).unwrap();
    assert_eq!(page.size(), 3);
    assert_eq!(page.key_at(2), key(20));
    assert_eq!(page.value_at(2), 120);
    // Key 0 is empty, so it takes only its slot.
    assert_eq!(
        page.used_space(),
        Page::entry_size(0) + 2 * Page::entry_size(8)
    );
}

#[test]
fn new_rejects_other_page_types() {
    let mut buf = PageBuf::zeroed();
    PageHeader::init(&mut buf, PageType::BTreeLeaf);
    let Err(err) = BtreeInternalPage::new(&*buf) else {
        panic!("a leaf page opened as an internal page");
    };
    assert!(matches!(
        err,
        StorageError::UnexpectedPageType {
            expected: PageType::BTreeInternal,
            found: 3
        }
    ));
}

#[test]
fn lookup_ignores_first_key() {
    let mut buf = PageBuf::zeroed();
    let page = page_with(&mut buf, 4);
    assert_eq!(keys(&page), [10, 20, 30]);
    assert_eq!(values(&page), [100, 110, 120, 130]);

    assert_eq!(page.lookup(&key(0), &CMP), 100);
    assert_eq!(page.lookup(&key(9), &CMP), 100);
    assert_eq!(page.lookup(&key(10), &CMP), 110);
    assert_eq!(page.lookup(&key(19), &CMP), 110);
    assert_eq!(page.lookup(&key(20), &CMP), 120);
    assert_eq!(page.lookup(&key(30), &CMP), 130);
    assert_eq!(page.lookup(&key(u64::MAX), &CMP), 130);
    assert_eq!(page.lookup(b"", &CMP), 100);
}

#[test]
fn lookup_with_one_child() {
    let mut buf = PageBuf::zeroed();
    let mut page = Page::init(&mut *buf);
    page.push_front(&key(99), 7).unwrap();
    assert_eq!(page.size(), 1);
    assert_eq!(page.lookup(&key(0), &CMP), 7);
    assert_eq!(page.lookup(&key(u64::MAX), &CMP), 7);
}

#[test]
fn insert_keeps_keys_sorted() {
    let mut buf = PageBuf::zeroed();
    let mut page = Page::init(&mut *buf);
    page.populate_new_root(1, &key(40), 2).unwrap();
    assert_eq!(page.insert(&key(20), 3, &CMP).unwrap(), 1);
    assert_eq!(page.insert(&key(60), 4, &CMP).unwrap(), 3);
    assert_eq!(page.insert(&key(30), 5, &CMP).unwrap(), 2);
    assert_eq!(page.insert(&key(10), 6, &CMP).unwrap(), 1);

    assert_eq!(keys(&page), [10, 20, 30, 40, 60]);
    assert_eq!(values(&page), [1, 6, 3, 5, 2, 4]);
    assert_eq!(page.lookup(&key(35), &CMP), 5);
}

#[test]
fn keys_of_different_lengths() {
    let mut buf = PageBuf::zeroed();
    let mut page = Page::init(&mut *buf);
    page.populate_new_root(1, b"m", 2).unwrap();
    page.insert(b"abc", 3, &CMP).unwrap();
    page.insert(b"ab", 4, &CMP).unwrap();
    let keys: Vec<&[u8]> = (1..page.size()).map(|i| page.key_at(i)).collect();
    assert_eq!(keys, [&b"ab"[..], b"abc", b"m"]);
    assert_eq!(page.lookup(b"a", &CMP), 1);
    assert_eq!(page.lookup(b"abb", &CMP), 4);
    assert_eq!(page.lookup(b"abcd", &CMP), 3);
    assert_eq!(page.lookup(b"z", &CMP), 2);
}

#[test]
fn insert_equal_key_goes_after() {
    let mut buf = PageBuf::zeroed();
    let mut page = page_with(&mut buf, 3);
    assert_eq!(page.insert(&key(10), 999, &CMP).unwrap(), 2);
    assert_eq!(keys(&page), [10, 10, 20]);
    assert_eq!(values(&page), [100, 110, 999, 120]);
}

#[test]
fn insert_into_full_page_fails() {
    let mut buf = PageBuf::zeroed();
    let mut page = Page::init(&mut *buf);
    let children = fill(&mut page, 8);
    assert_eq!(
        Page::entry_size(0) + (children - 1) * Page::entry_size(8),
        page.used_space()
    );
    assert!(!page.has_room_for(8));
    assert!(matches!(
        page.push_front(&key(0), 1).unwrap_err(),
        StorageError::PageFull
    ));
    assert!(page.free_space() < Page::entry_size(8));
    assert_eq!(keys(&page), (1..children as u64).collect::<Vec<_>>());
}

#[test]
fn key_too_large() {
    let max = Page::MAX_KEY_SIZE;
    let too_long = long_key(1, max + 1);
    let mut buf = PageBuf::zeroed();
    let mut page = Page::init(&mut *buf);
    assert!(matches!(
        page.populate_new_root(1, &too_long, 2),
        Err(StorageError::KeyTooLarge { .. })
    ));
    page.populate_new_root(1, &long_key(1, max), 2).unwrap();
    assert!(matches!(
        page.insert(&too_long, 3, &CMP),
        Err(StorageError::KeyTooLarge { .. })
    ));
    assert!(matches!(
        page.set_key_at(1, &too_long),
        Err(StorageError::KeyTooLarge { .. })
    ));
    assert!(matches!(
        page.push_front(&too_long, 3),
        Err(StorageError::KeyTooLarge { .. })
    ));
}

#[test]
fn remove_at() {
    let mut buf = PageBuf::zeroed();
    let mut page = page_with(&mut buf, 5);

    page.remove_at(2);
    assert_eq!(keys(&page), [10, 30, 40]);
    assert_eq!(values(&page), [100, 110, 130, 140]);

    page.remove_at(3);
    assert_eq!(keys(&page), [10, 30]);
    assert_eq!(values(&page), [100, 110, 130]);

    // Removing the leftmost child drops the key after it too, freeing its
    // bytes.
    page.remove_at(0);
    assert_eq!(keys(&page), [30]);
    assert_eq!(values(&page), [110, 130]);
    assert_eq!(page.lookup(&key(0), &CMP), 110);
    assert_eq!(page.used_space(), Page::entry_size(0) + Page::entry_size(8));
}

#[test]
fn remove_first_returns_key_and_child() {
    let mut buf = PageBuf::zeroed();
    let mut page = page_with(&mut buf, 3);
    assert_eq!(page.remove_first(), (key(10).to_vec(), 100));
    assert_eq!(keys(&page), [20]);
    assert_eq!(values(&page), [110, 120]);
}

#[test]
fn push_front_adds_leftmost_child() {
    let mut buf = PageBuf::zeroed();
    let mut page = page_with(&mut buf, 3);
    page.push_front(&key(5), 90).unwrap();
    assert_eq!(keys(&page), [5, 10, 20]);
    assert_eq!(values(&page), [90, 100, 110, 120]);
    assert_eq!(page.lookup(&key(4), &CMP), 90);
    assert_eq!(page.lookup(&key(5), &CMP), 100);
}

#[test]
fn set_key_and_value() {
    let mut buf = PageBuf::zeroed();
    let mut page = page_with(&mut buf, 3);
    page.set_key_at(2, &key(25)).unwrap();
    page.set_value_at(0, 7);
    assert_eq!(keys(&page), [10, 25]);
    assert_eq!(values(&page), [7, 110, 120]);

    page.set_key_at(1, &long_key(10, 100)).unwrap();
    assert_eq!(page.key_at(1), long_key(10, 100));
    assert_eq!(
        page.used_space(),
        Page::entry_size(0) + Page::entry_size(100) + Page::entry_size(8)
    );
}

#[test]
fn set_key_reuses_the_old_key_bytes() {
    let mut buf = PageBuf::zeroed();
    let mut page = Page::init(&mut *buf);
    fill(&mut page, 100);
    let free = page.free_space();
    // Fits only because the old key's 100 bytes count.
    page.set_key_at(1, &long_key(1, 100 + free)).unwrap();
    assert_eq!(page.free_space(), 0);
    assert_eq!(page.key_at(1), long_key(1, 100 + free));
    assert_eq!(page.key_at(2), long_key(2, 100));

    assert!(matches!(
        page.set_key_at(2, &long_key(2, 101)),
        Err(StorageError::PageFull)
    ));
    assert_eq!(page.key_at(2), long_key(2, 100));
}

#[test]
fn holes_are_reused() {
    let mut buf = PageBuf::zeroed();
    let mut page = Page::init(&mut *buf);
    let children = fill(&mut page, 200);
    // Remove every other key and its child.
    for index in (1..children).rev().step_by(2) {
        page.remove_at(index);
    }
    let remaining = keys(&page);
    let added = (page.free_space() / Page::entry_size(300)) as u64;
    assert!(added > 1);
    for n in 0..added {
        page.insert(&long_key(1000 + n, 300), 1000 + n as PageId, &CMP)
            .unwrap();
    }
    for n in remaining {
        assert_eq!(page.lookup(&long_key(n, 200), &CMP), n as PageId);
    }
    for n in 0..added {
        assert_eq!(
            page.lookup(&long_key(1000 + n, 300), &CMP),
            1000 + n as PageId
        );
    }
}

#[test]
fn split_moves_upper_half_and_returns_separator() {
    let mut left_buf = PageBuf::zeroed();
    let mut right_buf = PageBuf::zeroed();
    let mut left = page_with(&mut left_buf, 8);
    let mut right = Page::init(&mut *right_buf);

    let separator = left.split_to(&mut right);
    assert_eq!(separator, key(40));
    // 40 went up: it is in neither page, and its right child leads `right`.
    assert_eq!(keys(&left), [10, 20, 30]);
    assert_eq!(values(&left), [100, 110, 120, 130]);
    assert_eq!(keys(&right), [50, 60, 70]);
    assert_eq!(values(&right), [140, 150, 160, 170]);
    assert_eq!(right.lookup(&key(40), &CMP), 140);
    assert_eq!(left.used_space(), right.used_space());
}

#[test]
fn split_balances_bytes() {
    let mut left_buf = PageBuf::zeroed();
    let mut right_buf = PageBuf::zeroed();
    let mut left = Page::init(&mut *left_buf);
    left.push_front(&[], 0).unwrap();
    // Small keys first and a few large ones at the end.
    let lens = std::iter::repeat_n(20, 70).chain([400, 500, 600]);
    for (n, len) in lens.enumerate() {
        let n = n as u64 + 1;
        left.insert(&long_key(n, len), n as PageId, &CMP).unwrap();
    }
    let children = left.size();

    let mut right = Page::init(&mut *right_buf);
    let separator = left.split_to(&mut right);
    assert_eq!(left.size() + right.size(), children);
    assert!(left.size() > right.size());
    let largest = Page::entry_size(600);
    assert!(left.used_space().abs_diff(right.used_space()) <= largest);
    assert_eq!(num(&separator), left.size() as u64);
    assert_eq!(right.value_at(0), left.size() as PageId);
}

#[test]
fn split_leaves_room_for_the_largest_key() {
    let max = Page::MAX_KEY_SIZE;
    for len in [max, max / 2 + 1, 8] {
        let mut left_buf = PageBuf::zeroed();
        let mut right_buf = PageBuf::zeroed();
        let mut left = Page::init(&mut *left_buf);
        let children = fill(&mut left, len);
        assert!(children >= 4, "{children} children with {len}-byte keys");
        assert!(!left.has_room_for(max));

        let mut right = Page::init(&mut *right_buf);
        left.split_to(&mut right);
        assert!(left.has_room_for(max), "keys of {len} bytes");
        assert!(right.has_room_for(max), "keys of {len} bytes");
    }
}

#[test]
fn value_index() {
    let mut buf = PageBuf::zeroed();
    let page = page_with(&mut buf, 4);
    assert_eq!(page.value_index(100), Some(0));
    assert_eq!(page.value_index(130), Some(3));
    assert_eq!(page.value_index(5), None);
}

#[test]
#[should_panic(expected = "key index 0 out of range")]
fn first_key_is_invalid() {
    let mut buf = PageBuf::zeroed();
    let page = page_with(&mut buf, 3);
    page.key_at(0);
}

#[test]
#[should_panic(expected = "child index 3 out of range")]
fn value_past_size_panics() {
    let mut buf = PageBuf::zeroed();
    let page = page_with(&mut buf, 3);
    page.value_at(3);
}
