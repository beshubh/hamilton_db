//! Rows an executor writes out while it runs, such as the partitions of a
//! hash join or the runs of a sort, on linked lists of intermediate result
//! pages.

use std::collections::HashSet;
use std::sync::Arc;

use common::hash::IntKeys;
use common::{INVALID_PAGE_ID, PageId, Tuple};
use storage::StorageError;
use storage::buffer::arc_replacer::AccessType;
use storage::buffer::buffer_pool_manager::BufferPoolManager;
use storage::page::PAGE_SIZE;
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

/// Appends rows to several intermediate results at once, as an executor
/// splitting rows into partitions does, giving the same pages as
/// [`IntermediatePages::append`]. Each result's last page is filled in
/// memory, the frame that result writes to, and goes to the buffer pool once
/// full, so a page is pinned once rather than once per row.
/// [`finish`](Self::finish) writes the pages still being filled.
pub(super) struct PartitionWriter {
    results: Vec<IntermediateResult>,
    /// The last page of each result, while it is being filled.
    filling: Vec<Option<Box<[u8; PAGE_SIZE]>>>,
}

impl PartitionWriter {
    pub fn new(count: usize) -> Self {
        Self {
            results: (0..count).map(|_| IntermediateResult::new()).collect(),
            filling: (0..count).map(|_| None).collect(),
        }
    }

    /// Appends `tuple` to result `i`.
    pub fn append(&mut self, pages: &mut IntermediatePages, i: usize, tuple: &Tuple) -> Result<()> {
        let result = &mut self.results[i];
        if let Some(page) = &mut self.filling[i] {
            match IntermediateResultPage::new(&mut **page)?.insert_tuple(tuple.data()) {
                Ok(_) => {
                    result.tuple_count += 1;
                    return Ok(());
                }
                Err(StorageError::PageFull) => {}
                Err(e) => return Err(e.into()),
            }
            let next_page_id = pages.allocate();
            IntermediateResultPage::new(&mut **page)?.set_next_page_id(next_page_id);
            pages.write(result.last_page_id, page)?;
            page.fill(0);
            result.last_page_id = next_page_id;
        } else {
            let page_id = pages.allocate();
            result.first_page_id = page_id;
            result.last_page_id = page_id;
            self.filling[i] = Some(Box::new([0; PAGE_SIZE]));
        }
        let page = self.filling[i].as_mut().expect("a page is being filled");
        IntermediateResultPage::init(&mut **page).insert_tuple(tuple.data())?;
        result.page_count += 1;
        result.tuple_count += 1;
        Ok(())
    }

    /// Writes the pages still being filled, and returns the results.
    pub fn finish(self, pages: &mut IntermediatePages) -> Result<Vec<IntermediateResult>> {
        for (result, page) in self.results.iter().zip(&self.filling) {
            if let Some(page) = page {
                pages.write(result.last_page_id, page)?;
            }
        }
        Ok(self.results)
    }
}

/// The intermediate result pages one executor has allocated. Any it has not
/// freed are freed when it is dropped.
pub(super) struct IntermediatePages {
    exec_ctx: Arc<ExecutionContext>,
    page_ids: HashSet<PageId, IntKeys>,
}

impl IntermediatePages {
    pub fn new(exec_ctx: Arc<ExecutionContext>) -> Self {
        Self {
            exec_ctx,
            page_ids: HashSet::default(),
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

    /// A new page, for a [`PartitionWriter`] to fill.
    fn allocate(&mut self) -> PageId {
        let page_id = self.bpm().new_page();
        self.page_ids.insert(page_id);
        page_id
    }

    /// Writes `data`, a whole page, to page `page_id`.
    fn write(&self, page_id: PageId, data: &[u8; PAGE_SIZE]) -> Result<()> {
        let mut guard = self
            .bpm()
            .checked_write_page(page_id, AccessType::Unknown)?;
        *guard.get_data_mut() = *data;
        Ok(())
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

    /// Calls `f` with each row on page `page_id`, in order, and returns the
    /// page after it. The page stays latched meanwhile.
    pub fn for_each_tuple(
        &self,
        page_id: PageId,
        mut f: impl FnMut(&[u8]) -> Result<()>,
    ) -> Result<PageId> {
        let guard = self.bpm().checked_read_page(page_id, AccessType::Scan)?;
        let page = IntermediateResultPage::new(guard.get_data())?;
        for data in page.tuples() {
            f(data)?;
        }
        Ok(page.next_page_id())
    }

    /// [`read_page`](Self::read_page) into `tuples`, replacing what it holds
    /// and writing over its tuples' buffers rather than allocating new ones.
    pub fn read_page_into(&self, page_id: PageId, tuples: &mut Vec<Tuple>) -> Result<PageId> {
        let mut n = 0;
        let next_page_id = self.for_each_tuple(page_id, |data| {
            if n == tuples.len() {
                tuples.push(Tuple::default());
            }
            tuples[n].set_bytes(data);
            n += 1;
            Ok(())
        })?;
        tuples.truncate(n);
        Ok(next_page_id)
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
    use common::Tuple;

    use super::super::test_util::Db;
    use super::{IntermediatePages, IntermediateResult, PageCount};

    /// Counting rows gives the pages appending them fills.
    #[test]
    fn counts_the_pages_appending_fills() {
        let db = Db::new();
        let mut pages = IntermediatePages::new(db.ctx());
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
