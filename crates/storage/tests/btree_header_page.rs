use common::INVALID_PAGE_ID;
use storage::StorageError;
use storage::page::PageBuf;
use storage::page::btree_header_page::BtreeHeaderPage;
use storage::page::page_header::{PageHeader, PageType};
use zerocopy::FromBytes;

#[test]
fn init_makes_an_empty_tree() {
    let mut buf = PageBuf::zeroed();
    let page = BtreeHeaderPage::init(&mut *buf);
    assert_eq!(page.root_page_id(), INVALID_PAGE_ID);

    let (header, _) = PageHeader::ref_from_prefix(&buf[..]).unwrap();
    assert_eq!(header.page_type(), Ok(PageType::BTreeHeader));
}

#[test]
fn set_root_and_reopen() {
    let mut buf = PageBuf::zeroed();
    let mut page = BtreeHeaderPage::init(&mut *buf);
    page.set_root_page_id(42);
    assert_eq!(page.root_page_id(), 42);

    let page = BtreeHeaderPage::new(&*buf).unwrap();
    assert_eq!(page.root_page_id(), 42);
}

#[test]
fn new_rejects_other_page_types() {
    let mut buf = PageBuf::zeroed();
    PageHeader::init(&mut buf, PageType::BTreeLeaf);
    let Err(err) = BtreeHeaderPage::new(&*buf) else {
        panic!("a leaf page opened as a header page");
    };
    assert!(matches!(
        err,
        StorageError::UnexpectedPageType {
            expected: PageType::BTreeHeader,
            found: 3
        }
    ));
}

#[test]
fn new_rejects_unwritten_page() {
    let buf = PageBuf::zeroed();
    assert!(matches!(
        BtreeHeaderPage::new(&*buf),
        Err(StorageError::UnexpectedPageType { found: 0, .. })
    ));
}
