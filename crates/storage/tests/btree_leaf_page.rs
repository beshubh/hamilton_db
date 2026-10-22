use common::{INVALID_PAGE_ID, Rid};
use storage::StorageError;
use storage::page::btree_leaf_page::BtreeLeafPage;
use storage::page::page_header::{PageHeader, PageType};
use storage::page::{PAGE_SIZE, PageBuf};

/// Big-endian, so byte order matches numeric order.
type Key = [u8; 8];
type Page<'a, const NUM_TOMBS: usize = 2> = BtreeLeafPage<&'a mut [u8; PAGE_SIZE], Key, NUM_TOMBS>;

fn key(n: u64) -> Key {
    n.to_be_bytes()
}

fn rid(n: u64) -> Rid {
    Rid::new(n as i32, n as u16 + 1)
}

/// Every key on the page, tombstoned ones included.
fn keys<const N: usize>(page: &Page<'_, N>) -> Vec<u64> {
    (0..page.size())
        .map(|i| u64::from_be_bytes(page.key_at(i)))
        .collect()
}

fn tombstoned_keys<const N: usize>(page: &Page<'_, N>) -> Vec<u64> {
    page.tombstones()
        .map(|i| u64::from_be_bytes(page.key_at(i)))
        .collect()
}

/// A page holding keys 10, 20, ... with their `rid`s.
fn page_with<const N: usize>(buf: &mut PageBuf, max_size: usize, count: u64) -> Page<'_, N> {
    let mut page = Page::<N>::init(&mut *buf, max_size);
    for i in 1..=count {
        page.insert(key(10 * i), rid(10 * i)).unwrap();
    }
    page
}

#[test]
fn init_and_reopen() {
    let mut buf = PageBuf::zeroed();
    let mut page = Page::<2>::init(&mut *buf, 5);
    assert_eq!(page.size(), 0);
    assert_eq!(page.max_size(), 5);
    assert_eq!(page.min_size(), 3);
    assert_eq!(page.next_page_id(), INVALID_PAGE_ID);
    assert_eq!(page.num_tombstones(), 0);
    page.set_next_page_id(42);

    let mut page = page_with::<2>(&mut buf, 5, 3);
    page.set_next_page_id(42);
    page.remove(&key(20));
    let page = BtreeLeafPage::<_, Key, 2>::new(&*buf).unwrap();
    assert_eq!(page.size(), 3);
    assert_eq!(page.next_page_id(), 42);
    assert_eq!(page.lookup(&key(30)), Some(rid(30)));
    assert_eq!(page.lookup(&key(20)), None);
    assert_eq!(page.tombstones().collect::<Vec<_>>(), [1]);
}

#[test]
fn new_rejects_other_page_types() {
    let mut buf = PageBuf::zeroed();
    PageHeader::init(&mut buf, PageType::BTreeInternal);
    let Err(err) = BtreeLeafPage::<_, Key, 2>::new(&*buf) else {
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
    let mut page = Page::<2>::init(&mut *buf, 8);
    assert_eq!(page.insert(key(30), rid(30)).unwrap(), 0);
    assert_eq!(page.insert(key(10), rid(10)).unwrap(), 0);
    assert_eq!(page.insert(key(50), rid(50)).unwrap(), 2);
    assert_eq!(page.insert(key(20), rid(20)).unwrap(), 1);

    assert_eq!(keys(&page), [10, 20, 30, 50]);
    for k in [10, 20, 30, 50] {
        assert_eq!(page.lookup(&key(k)), Some(rid(k)));
    }
    assert_eq!(page.lookup(&key(40)), None);
    assert_eq!(page.lookup(&key(60)), None);
    assert_eq!(page.key_index(&key(25)), 2);
    assert_eq!(page.key_index(&key(30)), 2);
    assert_eq!(page.key_index(&key(99)), 4);
}

#[test]
fn insert_duplicate_fails() {
    let mut buf = PageBuf::zeroed();
    let mut page = page_with::<2>(&mut buf, 8, 3);
    let err = page.insert(key(20), rid(99)).unwrap_err();
    assert!(matches!(err, StorageError::DuplicateKey));
    assert_eq!(page.lookup(&key(20)), Some(rid(20)));
}

#[test]
fn insert_into_full_page_fails() {
    let mut buf = PageBuf::zeroed();
    let mut page = page_with::<2>(&mut buf, 3, 3);
    assert!(page.is_full());
    let err = page.insert(key(5), rid(5)).unwrap_err();
    assert!(matches!(err, StorageError::PageFull));
    assert_eq!(keys(&page), [10, 20, 30]);
}

#[test]
fn remove_tombstones_the_key() {
    let mut buf = PageBuf::zeroed();
    let mut page = page_with::<2>(&mut buf, 8, 4);
    assert_eq!(page.remove(&key(20)), Some(rid(20)));
    assert_eq!(page.remove(&key(20)), None);
    assert_eq!(page.remove(&key(25)), None);

    // The key keeps its slot but lookups skip it.
    assert_eq!(keys(&page), [10, 20, 30, 40]);
    assert_eq!(page.size(), 4);
    assert_eq!(page.live_size(), 3);
    assert!(page.is_tombstone(1));
    assert_eq!(page.lookup(&key(20)), None);
}

#[test]
fn tombstones_follow_their_keys() {
    let mut buf = PageBuf::zeroed();
    let mut page = page_with::<2>(&mut buf, 8, 4);
    page.remove(&key(30));
    page.insert(key(5), rid(5)).unwrap();
    page.insert(key(25), rid(25)).unwrap();
    assert_eq!(tombstoned_keys(&page), [30]);
    page.remove_at(0);
    assert_eq!(tombstoned_keys(&page), [30]);
    assert_eq!(page.lookup(&key(30)), None);
    assert_eq!(page.lookup(&key(40)), Some(rid(40)));
}

#[test]
fn full_tombstone_buffer_removes_oldest() {
    let mut buf = PageBuf::zeroed();
    let mut page = page_with::<2>(&mut buf, 8, 5);
    page.remove(&key(20));
    page.remove(&key(40));
    assert_eq!(tombstoned_keys(&page), [20, 40]);

    // 20 is the oldest, so it goes for good and 50 takes its place.
    assert_eq!(page.remove(&key(50)), Some(rid(50)));
    assert_eq!(keys(&page), [10, 30, 40, 50]);
    assert_eq!(tombstoned_keys(&page), [40, 50]);

    // Here the oldest tombstone sits after the removed key.
    assert_eq!(page.remove(&key(10)), Some(rid(10)));
    assert_eq!(keys(&page), [10, 30, 50]);
    assert_eq!(tombstoned_keys(&page), [50, 10]);
    assert_eq!(page.live_size(), 1);
    assert_eq!(page.lookup(&key(30)), Some(rid(30)));
}

#[test]
fn no_tombstones_removes_at_once() {
    let mut buf = PageBuf::zeroed();
    let mut page = page_with::<0>(&mut buf, 8, 3);
    assert_eq!(page.remove(&key(20)), Some(rid(20)));
    assert_eq!(keys(&page), [10, 30]);
    assert_eq!(page.num_tombstones(), 0);
}

#[test]
fn insert_revives_tombstoned_key() {
    let mut buf = PageBuf::zeroed();
    let mut page = page_with::<2>(&mut buf, 3, 3);
    page.remove(&key(20));
    // The page is full, but reviving reuses the key's slot.
    assert_eq!(page.insert(key(20), rid(99)).unwrap(), 1);
    assert_eq!(page.lookup(&key(20)), Some(rid(99)));
    assert_eq!(page.num_tombstones(), 0);
    assert_eq!(keys(&page), [10, 20, 30]);
}

#[test]
fn remove_at_returns_key_and_rid() {
    let mut buf = PageBuf::zeroed();
    let mut page = page_with::<2>(&mut buf, 8, 3);
    page.remove(&key(20));
    assert_eq!(page.remove_at(1), (key(20), rid(20)));
    assert_eq!(page.num_tombstones(), 0);
    assert_eq!(page.remove_at(1), (key(30), rid(30)));
    assert_eq!(keys(&page), [10]);
}

#[test]
fn purge_tombstones() {
    let mut buf = PageBuf::zeroed();
    let mut page = page_with::<3>(&mut buf, 8, 6);
    page.remove(&key(50));
    page.remove(&key(10));
    page.remove(&key(30));
    page.purge_tombstones();
    assert_eq!(keys(&page), [20, 40, 60]);
    assert_eq!(page.num_tombstones(), 0);
    for k in [20, 40, 60] {
        assert_eq!(page.lookup(&key(k)), Some(rid(k)));
    }
}

#[test]
fn fill_to_slot_cnt() {
    type Big<'a> = Page<'a, 16>;
    let slot_cnt = Big::SLOT_CNT;
    assert!(slot_cnt > 250);
    assert!(slot_cnt < Page::<0>::SLOT_CNT);
    let mut buf = PageBuf::zeroed();
    // Insert in descending order, so every insert shifts the whole page.
    let mut page = Big::init(&mut *buf, slot_cnt);
    for i in (0..slot_cnt as u64).rev() {
        page.insert(key(i), rid(i)).unwrap();
    }
    for i in (0..16).map(|i| i * 17) {
        page.remove(&key(i));
    }
    assert_eq!(page.num_tombstones(), 16);
    assert!(matches!(
        page.insert(key(u64::MAX), rid(0)).unwrap_err(),
        StorageError::PageFull
    ));
    for i in 0..slot_cnt as u64 {
        let expected = (!page.is_tombstone(i as usize)).then(|| rid(i));
        assert_eq!(page.lookup(&key(i)), expected);
    }
    assert_eq!(keys(&page), (0..slot_cnt as u64).collect::<Vec<_>>());
}

#[test]
#[should_panic(expected = "key index 3 out of range")]
fn key_past_size_panics() {
    let mut buf = PageBuf::zeroed();
    let page = page_with::<2>(&mut buf, 8, 3);
    page.key_at(3);
}

#[test]
#[should_panic(expected = "max size")]
fn max_size_over_slot_cnt_panics() {
    let mut buf = PageBuf::zeroed();
    Page::<2>::init(&mut *buf, Page::<2>::SLOT_CNT + 1);
}
