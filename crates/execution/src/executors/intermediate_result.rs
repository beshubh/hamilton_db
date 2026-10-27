//! Rows an executor writes out while it runs, such as the partitions of a
//! hash join or the runs of a sort, on linked lists of intermediate result
//! pages.

use std::collections::HashSet;
use std::sync::Arc;

use common::{INVALID_PAGE_ID, PageId, Tuple};
use storage::StorageError;
use storage::buffer::arc_replacer::AccessType;
use storage::buffer::buffer_pool_manager::BufferPoolManager;
use storage::page::intermediate_result_page::{IntermediateResultPage, TUPLE_SPACE, space_for};

use crate::Result;
use crate::execution_context::ExecutionContext;

/// Rows on a linked list of intermediate result pages.
pub(super) struct IntermediateResult {
    /// `INVALID_PAGE_ID` while the result is empty.
    pub first_page_id: PageId,
    /// The page new rows go to, or `INVALID_PAGE_ID`.
    pub last_page_id: PageId,
    pub page_count: usize,
    pub tuple_count: usize,
}

impl IntermediateResult {
    pub fn new() -> Self {
        Self {
            first_page_id: INVALID_PAGE_ID,
            last_page_id: INVALID_PAGE_ID,
            page_count: 0,
            tuple_count: 0,
        }
    }
}

/// Counts the pages rows would fill as an intermediate result, without
/// writing them, for an executor that keeps rows in memory while they would
/// fit in its frames.
pub(super) struct PageCount {
    pages: usize,
    /// The room left on the last page.
    free: usize,
}

impl PageCount {
    pub fn new() -> Self {
        Self { pages: 0, free: 0 }
    }

    /// Counts `tuple` on the last page if it fits there, and on a new page
    /// if not, as [`IntermediatePages::append`] would put it.
    pub fn add(&mut self, tuple: &Tuple) {
        let space = space_for(tuple.len());
        if space > self.free {
            self.pages += 1;
            self.free = TUPLE_SPACE;
        }
        self.free = self.free.saturating_sub(space);
    }

    pub fn pages(&self) -> usize {
        self.pages
    }
}

/// The intermediate result pages one executor has allocated. Any it has not
/// freed are freed when it is dropped.
pub(super) struct IntermediatePages {
    exec_ctx: Arc<ExecutionContext>,
    page_ids: HashSet<PageId>,
}

impl IntermediatePages {
    pub fn new(exec_ctx: Arc<ExecutionContext>) -> Self {
        Self {
            exec_ctx,
            page_ids: HashSet::new(),
        }
    }

    fn bpm(&self) -> &BufferPoolManager {
        self.exec_ctx.catalog().bpm()
    }

    /// The pages allocated and not freed yet.
    #[cfg(test)]
    pub fn page_ids(&self) -> impl Iterator<Item = PageId> + '_ {
        self.page_ids.iter().copied()
    }

    /// Appends `tuple` to the last page of `result`, or to a new page linked
    /// after it when that one is full or the result has no pages.
    pub fn append(&mut self, result: &mut IntermediateResult, tuple: &Tuple) -> Result<()> {
        if self.try_append(result, tuple)? {
            return Ok(());
        }
        let page_id = self.bpm().new_page();
        self.page_ids.insert(page_id);
        let mut guard = self
            .bpm()
            .checked_write_page(page_id, AccessType::Unknown)?;
        IntermediateResultPage::init(guard.get_data_mut()).insert_tuple(tuple.data())?;
        drop(guard);

        if result.last_page_id == INVALID_PAGE_ID {
            result.first_page_id = page_id;
        } else {
            let mut last = self
                .bpm()
                .checked_write_page(result.last_page_id, AccessType::Unknown)?;
            IntermediateResultPage::new(last.get_data_mut())?.set_next_page_id(page_id);
        }
        result.last_page_id = page_id;
        result.page_count += 1;
        result.tuple_count += 1;
        Ok(())
    }

    /// Appends `tuple` to the last page of `result` if it fits there. False,
    /// adding nothing, if that page is full or the result has no pages.
    pub fn try_append(&mut self, result: &mut IntermediateResult, tuple: &Tuple) -> Result<bool> {
        if result.last_page_id == INVALID_PAGE_ID {
            return Ok(false);
        }
        let mut guard = self
            .bpm()
            .checked_write_page(result.last_page_id, AccessType::Unknown)?;
        match IntermediateResultPage::new(guard.get_data_mut())?.insert_tuple(tuple.data()) {
            Ok(_) => {
                result.tuple_count += 1;
                Ok(true)
            }
            Err(StorageError::PageFull) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// The rows on page `page_id`, and the page after it.
    pub fn read_page(&self, page_id: PageId) -> Result<(Vec<Tuple>, PageId)> {
        let guard = self.bpm().checked_read_page(page_id, AccessType::Scan)?;
        let page = IntermediateResultPage::new(guard.get_data())?;
        let tuples = page
            .tuples()
            .map(|data| Tuple::from_bytes(data.to_vec()))
            .collect();
        Ok((tuples, page.next_page_id()))
    }

    /// Replaces the rows on page `page_id` with `tuples`, which must fit,
    /// keeping its link to the next page.
    pub fn rewrite_page(&self, page_id: PageId, tuples: &[Tuple]) -> Result<()> {
        let mut guard = self
            .bpm()
            .checked_write_page(page_id, AccessType::Unknown)?;
        let next_page_id = IntermediateResultPage::new(guard.get_data())?.next_page_id();
        let mut page = IntermediateResultPage::init(guard.get_data_mut());
        page.set_next_page_id(next_page_id);
        for tuple in tuples {
            page.insert_tuple(tuple.data())?;
        }
        Ok(())
    }

    /// Frees page `page_id`, which is never read again.
    pub fn free_page(&mut self, page_id: PageId) -> Result<()> {
        self.bpm().delete_page(page_id)?;
        self.page_ids.remove(&page_id);
        Ok(())
    }

    /// Frees every page not freed yet.
    pub fn free_all(&mut self) -> Result<()> {
        for page_id in std::mem::take(&mut self.page_ids) {
            self.bpm().delete_page(page_id)?;
        }
        Ok(())
    }
}

impl Drop for IntermediatePages {
    fn drop(&mut self) {
        // Nothing can be done about a page that cannot be freed, and it is
        // never read again.
        let _ = self.free_all();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common::Tuple;

    use super::super::test_util::Db;
    use super::{IntermediatePages, IntermediateResult, PageCount};

    /// Counting rows gives the pages appending them fills.
    #[test]
    fn counts_the_pages_appending_fills() {
        let db = Db::new();
        let mut pages = IntermediatePages::new(Arc::clone(&db.ctx));
        let (mut result, mut count) = (IntermediateResult::new(), PageCount::new());
        assert_eq!(count.pages(), 0);
        for len in (0..500).map(|i| i * 7 % 300) {
            let tuple = Tuple::from_bytes(vec![1; len]);
            pages.append(&mut result, &tuple).unwrap();
            count.add(&tuple);
            assert_eq!(count.pages(), result.page_count);
        }
        assert!(result.page_count > 1);
    }
}
