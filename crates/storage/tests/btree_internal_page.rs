use common::PageId;
use storage::StorageError;
use storage::page::btree_internal_page::BtreeInternalPage;
use storage::page::page_header::{PageHeader, PageType};
use storage::page::{PAGE_SIZE, PageBuf};

/// Big-endian, so byte order matches numeric order.
type Key = [u8; 8];
type Page<'a> = BtreeInternalPage<&'a mut [u8; PAGE_SIZE], Key>;

fn key(n: u64) -> Key {
    n.to_be_bytes()
}

fn keys(page: &Page) -> Vec<u64> {
    (1..page.size())
        .map(|i| u64::from_be_bytes(page.key_at(i)))
        .collect()
}

fn values(page: &Page) -> Vec<PageId> {
    (0..page.size()).map(|i| page.value_at(i)).collect()
}

/// A page with children 100, 110, 120, ... split by keys 10, 20, ...
fn page_with(buf: &mut PageBuf, max_size: usize, children: usize) -> Page<'_> {
    let mut page = Page::init(&mut *buf, max_size);
    page.populate_new_root(100, key(10), 110);
    for i in 2..children {
        page.insert(key(10 * i as u64), 100 + 10 * i as PageId)
            .unwrap();
    }
    page
}

#[test]
fn init_and_reopen() {
    let mut buf = PageBuf::zeroed();
    let page = Page::init(&mut *buf, 5);
    assert_eq!(page.size(), 0);
    assert_eq!(page.max_size(), 5);
    assert_eq!(page.min_size(), 3);

    let mut page = page_with(&mut buf, 5, 3);
    page.set_max_size(4);
    let page = BtreeInternalPage::<_, Key>::new(&*buf).unwrap();
    assert_eq!(page.size(), 3);
    assert_eq!(page.max_size(), 4);
    assert_eq!(page.key_at(2), key(20));
    assert_eq!(page.value_at(2), 120);
}

#[test]
fn new_rejects_other_page_types() {
    let mut buf = PageBuf::zeroed();
    PageHeader::init(&mut buf, PageType::BTreeLeaf);
    let Err(err) = BtreeInternalPage::<_, Key>::new(&*buf) else {
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
    let page = page_with(&mut buf, 8, 4);
    assert_eq!(keys(&page), [10, 20, 30]);
    assert_eq!(values(&page), [100, 110, 120, 130]);

    assert_eq!(page.lookup(&key(0)), 100);
    assert_eq!(page.lookup(&key(9)), 100);
    assert_eq!(page.lookup(&key(10)), 110);
    assert_eq!(page.lookup(&key(19)), 110);
    assert_eq!(page.lookup(&key(20)), 120);
    assert_eq!(page.lookup(&key(30)), 130);
    assert_eq!(page.lookup(&key(u64::MAX)), 130);
}

#[test]
fn lookup_with_one_child() {
    let mut buf = PageBuf::zeroed();
    let mut page = Page::init(&mut *buf, 4);
    page.push_front(key(99), 7).unwrap();
    assert_eq!(page.size(), 1);
    assert_eq!(page.lookup(&key(0)), 7);
    assert_eq!(page.lookup(&key(u64::MAX)), 7);
}

#[test]
fn insert_keeps_keys_sorted() {
    let mut buf = PageBuf::zeroed();
    let mut page = Page::init(&mut *buf, 8);
    page.populate_new_root(1, key(40), 2);
    assert_eq!(page.insert(key(20), 3).unwrap(), 1);
    assert_eq!(page.insert(key(60), 4).unwrap(), 3);
    assert_eq!(page.insert(key(30), 5).unwrap(), 2);
    assert_eq!(page.insert(key(10), 6).unwrap(), 1);

    assert_eq!(keys(&page), [10, 20, 30, 40, 60]);
    assert_eq!(values(&page), [1, 6, 3, 5, 2, 4]);
    assert_eq!(page.lookup(&key(35)), 5);
}

#[test]
fn insert_equal_key_goes_after() {
    let mut buf = PageBuf::zeroed();
    let mut page = page_with(&mut buf, 8, 3);
    assert_eq!(page.insert(key(10), 999).unwrap(), 2);
    assert_eq!(keys(&page), [10, 10, 20]);
    assert_eq!(values(&page), [100, 110, 999, 120]);
}

#[test]
fn insert_into_full_page_fails() {
    let mut buf = PageBuf::zeroed();
    let mut page = page_with(&mut buf, 3, 3);
    assert!(page.is_full());
    let err = page.insert(key(5), 1).unwrap_err();
    assert!(matches!(err, StorageError::PageFull));
    assert!(matches!(
        page.push_front(key(5), 1).unwrap_err(),
        StorageError::PageFull
    ));
    assert_eq!(keys(&page), [10, 20]);
    assert_eq!(values(&page), [100, 110, 120]);
}

#[test]
fn fill_to_slot_cnt() {
    let slot_cnt = Page::SLOT_CNT;
    assert!(slot_cnt > 300);
    let mut buf = PageBuf::zeroed();
    // Insert in descending order, so every insert shifts the whole page.
    let mut page = Page::init(&mut *buf, slot_cnt);
    page.populate_new_root(0, key(slot_cnt as u64 - 1), slot_cnt as PageId - 1);
    for i in (1..slot_cnt - 1).rev() {
        page.insert(key(i as u64), i as PageId).unwrap();
    }
    assert_eq!(page.size(), slot_cnt);
    assert!(matches!(
        page.insert(key(0), 0).unwrap_err(),
        StorageError::PageFull
    ));
    for i in 0..slot_cnt {
        assert_eq!(page.value_at(i), i as PageId);
        assert_eq!(page.lookup(&key(i as u64)), i as PageId);
    }
    assert_eq!(keys(&page), (1..slot_cnt as u64).collect::<Vec<_>>());
}

#[test]
fn remove_at() {
    let mut buf = PageBuf::zeroed();
    let mut page = page_with(&mut buf, 8, 5);

    page.remove_at(2);
    assert_eq!(keys(&page), [10, 30, 40]);
    assert_eq!(values(&page), [100, 110, 130, 140]);

    page.remove_at(3);
    assert_eq!(keys(&page), [10, 30]);
    assert_eq!(values(&page), [100, 110, 130]);

    // Removing the leftmost child drops the key after it too.
    page.remove_at(0);
    assert_eq!(keys(&page), [30]);
    assert_eq!(values(&page), [110, 130]);
    assert_eq!(page.lookup(&key(0)), 110);
}

#[test]
fn remove_first_returns_key_and_child() {
    let mut buf = PageBuf::zeroed();
    let mut page = page_with(&mut buf, 8, 3);
    assert_eq!(page.remove_first(), (key(10), 100));
    assert_eq!(keys(&page), [20]);
    assert_eq!(values(&page), [110, 120]);
}

#[test]
fn push_front_adds_leftmost_child() {
    let mut buf = PageBuf::zeroed();
    let mut page = page_with(&mut buf, 8, 3);
    page.push_front(key(5), 90).unwrap();
    assert_eq!(keys(&page), [5, 10, 20]);
    assert_eq!(values(&page), [90, 100, 110, 120]);
    assert_eq!(page.lookup(&key(4)), 90);
    assert_eq!(page.lookup(&key(5)), 100);
}

#[test]
fn set_key_and_value() {
    let mut buf = PageBuf::zeroed();
    let mut page = page_with(&mut buf, 8, 3);
    page.set_key_at(2, key(25));
    page.set_value_at(0, 7);
    assert_eq!(keys(&page), [10, 25]);
    assert_eq!(values(&page), [7, 110, 120]);
}

#[test]
fn value_index() {
    let mut buf = PageBuf::zeroed();
    let page = page_with(&mut buf, 8, 4);
    assert_eq!(page.value_index(100), Some(0));
    assert_eq!(page.value_index(130), Some(3));
    assert_eq!(page.value_index(5), None);
}

#[test]
fn change_size_by() {
    let mut buf = PageBuf::zeroed();
    let mut page = page_with(&mut buf, 8, 4);
    page.change_size_by(-2);
    assert_eq!(values(&page), [100, 110]);
    page.change_size_by(1);
    assert_eq!(values(&page), [100, 110, 120]);
}

#[test]
#[should_panic(expected = "key index 0 out of range")]
fn first_key_is_invalid() {
    let mut buf = PageBuf::zeroed();
    let page = page_with(&mut buf, 8, 3);
    page.key_at(0);
}

#[test]
#[should_panic(expected = "child index 3 out of range")]
fn value_past_size_panics() {
    let mut buf = PageBuf::zeroed();
    let page = page_with(&mut buf, 8, 3);
    page.value_at(3);
}

#[test]
#[should_panic(expected = "max size")]
fn max_size_over_slot_cnt_panics() {
    let mut buf = PageBuf::zeroed();
    Page::init(&mut *buf, Page::SLOT_CNT + 1);
}
