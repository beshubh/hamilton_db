use common::INVALID_PAGE_ID;
use storage::StorageError;
use storage::page::PageBuf;
use storage::page::intermediate_result_page::{IntermediateResultPage, MAX_TUPLE_SIZE};
use storage::page::page_header::{PageHeader, PageType};
use zerocopy::FromBytes;

#[test]
fn init_makes_an_empty_page() {
    let mut buf = PageBuf::zeroed();
    let page = IntermediateResultPage::init(&mut *buf);
    assert_eq!(page.tuple_count(), 0);
    assert_eq!(page.next_page_id(), INVALID_PAGE_ID);
    assert_eq!(page.get_tuple(0), None);
    assert!(page.has_room_for(MAX_TUPLE_SIZE));
    assert!(!page.has_room_for(MAX_TUPLE_SIZE + 1));

    let (header, _) = PageHeader::ref_from_prefix(&buf[..]).unwrap();
    assert_eq!(header.page_type(), Ok(PageType::IntermediateResult));
}

#[test]
fn tuples_come_back_in_insert_order() {
    let mut buf = PageBuf::zeroed();
    let mut page = IntermediateResultPage::init(&mut *buf);
    assert_eq!(page.insert_tuple(b"first").unwrap(), 0);
    assert_eq!(page.insert_tuple(b"").unwrap(), 1);
    assert_eq!(page.insert_tuple(b"third one").unwrap(), 2);

    let page = IntermediateResultPage::new(&*buf).unwrap();
    assert_eq!(page.tuple_count(), 3);
    assert_eq!(page.get_tuple(0), Some(&b"first"[..]));
    assert_eq!(page.get_tuple(1), Some(&b""[..]));
    assert_eq!(page.get_tuple(3), None);
    let tuples: Vec<_> = page.tuples().collect();
    assert_eq!(tuples, [&b"first"[..], b"", b"third one"]);
}

/// Each tuple takes its bytes and a 4-byte slot, until the page is full.
#[test]
fn inserts_until_full() {
    let mut buf = PageBuf::zeroed();
    let mut page = IntermediateResultPage::init(&mut *buf);
    let free = page.free_space();
    let tuple = [7u8; 96];
    let fits = free / (tuple.len() + 4);
    for i in 0..fits {
        assert_eq!(page.insert_tuple(&tuple).unwrap(), i);
    }
    assert_eq!(page.free_space(), free - fits * (tuple.len() + 4));
    assert!(matches!(
        page.insert_tuple(&tuple),
        Err(StorageError::PageFull)
    ));
    assert_eq!(page.tuple_count(), fits);
    assert!(page.tuples().all(|t| t == tuple));
}

#[test]
fn a_tuple_as_large_as_the_page_allows() {
    let mut buf = PageBuf::zeroed();
    let mut page = IntermediateResultPage::init(&mut *buf);
    assert!(matches!(
        page.insert_tuple(&vec![1; MAX_TUPLE_SIZE + 1]),
        Err(StorageError::TupleTooLarge { size, max: MAX_TUPLE_SIZE }) if size == MAX_TUPLE_SIZE + 1
    ));
    page.insert_tuple(&vec![1; MAX_TUPLE_SIZE]).unwrap();
    assert_eq!(page.free_space(), 0);
    assert!(matches!(
        page.insert_tuple(b""),
        Err(StorageError::PageFull)
    ));
}

#[test]
fn set_next_page_and_reopen() {
    let mut buf = PageBuf::zeroed();
    let mut page = IntermediateResultPage::init(&mut *buf);
    page.insert_tuple(b"row").unwrap();
    page.set_next_page_id(42);

    let page = IntermediateResultPage::new(&*buf).unwrap();
    assert_eq!(page.next_page_id(), 42);
    assert_eq!(page.get_tuple(0), Some(&b"row"[..]));
}

#[test]
fn new_rejects_other_page_types() {
    let mut buf = PageBuf::zeroed();
    PageHeader::init(&mut buf, PageType::Table);
    assert!(matches!(
        IntermediateResultPage::new(&*buf),
        Err(StorageError::UnexpectedPageType {
            expected: PageType::IntermediateResult,
            found: 1
        })
    ));
    let buf = PageBuf::zeroed();
    assert!(matches!(
        IntermediateResultPage::new(&*buf),
        Err(StorageError::UnexpectedPageType { found: 0, .. })
    ));
}
