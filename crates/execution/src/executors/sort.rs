use std::cmp::Ordering;
use std::collections::VecDeque;
use std::sync::Arc;

use common::{INVALID_PAGE_ID, PageId, Rid, Schema, Tuple, Value};

use super::intermediate_result::{IntermediatePages, IntermediateResult};
use super::{AbstractExecutor, Executor};
use crate::Result;
use crate::execution_context::ExecutionContext;
use crate::expressions::Expr;
use crate::plans::{OrderBy, PlanNode, SortPlan};

/// How many rows `init` asks the child for at a time while filling pages.
/// Each batch goes straight to a page, so this only bounds the rows in
/// flight.
const CHILD_BATCH_SIZE: usize = 128;

/// A two-way external merge sort, using three page buffers: two to read
/// runs from and one to write the merged run to.
///
/// A run is a sorted list of intermediate result pages. With the child's rows
/// on `N` pages, the sort makes `1 + ⌈log₂ N⌉` passes over them, each reading
/// and writing every page, for `2N × (1 + ⌈log₂ N⌉)` page IOs:
///
/// ```text
/// pass 0: fill pages with the child's rows; as each page fills up, sort its
///     rows and write them back, making it a run of one page
/// while there is more than one run:
///     merge the runs in pairs, the first with the second and so on:
///         read the first page of each run
///         repeatedly move the smaller of the two first rows to the output
///             page, writing it out and starting an empty one when it is full,
///             and reading a run's next page when its page is used up
///     a run left without a pair is carried to the next pass as it is
/// ```
///
/// All this happens in `init`, and `next` returns the rows of the last run.
/// Each page is freed once it has been read, so the sort holds at most about
/// `2N` pages at a time.
///
/// Rows are ordered by the plan's keys, the first deciding first. NULL sorts
/// after every other value, so it comes last in ascending order and first in
/// descending order, as in PostgreSQL. The sort is stable: each page is
/// sorted stably, and a merge takes the row from the earlier run on a tie, so
/// rows with equal keys keep the child's order.
///
/// Written to the buffer pool, a page reaches disk when it is evicted, so
/// fewer IOs are made when the runs fit in the pool. The sorted rows are not
/// stored anywhere, so they have no rids.
pub struct SortExecutor {
    plan: SortPlan,
    child: Box<AbstractExecutor>,
    /// The pages of the runs, freed as they are read.
    pages: IntermediatePages,
    /// The rows of the last run's page being returned, not returned yet.
    output: VecDeque<Tuple>,
    /// The last run's page after `output`, or `INVALID_PAGE_ID`.
    next_output_page: PageId,
}

/// A sorted list of pages.
type Run = IntermediateResult;

/// A row with the values of its sort keys.
struct SortRow {
    keys: Vec<Value>,
    tuple: Tuple,
}

/// The page buffer a merge reads one of its runs through.
struct RunReader {
    /// The rows of the page being read, not merged yet.
    rows: VecDeque<SortRow>,
    /// The run's page after `rows`, or `INVALID_PAGE_ID`.
    next_page_id: PageId,
}

impl SortExecutor {
    pub fn new(ctx: Arc<ExecutionContext>, plan: SortPlan, child: Box<AbstractExecutor>) -> Self {
        Self {
            plan,
            child,
            pages: IntermediatePages::new(ctx),
            output: VecDeque::new(),
            next_output_page: INVALID_PAGE_ID,
        }
    }

    /// `tuple`, a row of the child, with the values of its sort keys.
    fn sort_row(&self, tuple: Tuple) -> Result<SortRow> {
        let schema = self.child.get_output_schema();
        let keys = self
            .plan
            .order_bys()
            .iter()
            .map(|order_by| order_by.expr.evaluate(&tuple, schema))
            .collect::<Result<_>>()?;
        Ok(SortRow { keys, tuple })
    }

    /// Pass 0: fills pages with the child's rows, sorting each one once it is
    /// full, so that each page is a run.
    fn sort_pages(&mut self) -> Result<Vec<Run>> {
        let mut runs = Vec::new();
        let mut run = Run::new();
        let (mut tuples, mut rids) = (Vec::new(), Vec::new());
        while self.child.next(&mut tuples, &mut rids, CHILD_BATCH_SIZE)? {
            for tuple in &tuples {
                if self.pages.try_append(&mut run, tuple)? {
                    continue;
                }
                if run.page_count > 0 {
                    self.sort_page(run.first_page_id)?;
                    runs.push(std::mem::replace(&mut run, Run::new()));
                }
                self.pages.append(&mut run, tuple)?;
            }
        }
        if run.page_count > 0 {
            self.sort_page(run.first_page_id)?;
            runs.push(run);
        }
        Ok(runs)
    }

    /// Sorts the rows on page `page_id` and writes them back to it.
    fn sort_page(&mut self, page_id: PageId) -> Result<()> {
        let (tuples, _) = self.pages.read_page(page_id)?;
        let mut rows = tuples
            .into_iter()
            .map(|tuple| self.sort_row(tuple))
            .collect::<Result<Vec<_>>>()?;
        let mut error = None;
        rows.sort_by(|a, b| {
            compare_keys(self.plan.order_bys(), &a.keys, &b.keys).unwrap_or_else(|e| {
                error.get_or_insert(e);
                Ordering::Equal
            })
        });
        if let Some(e) = error {
            return Err(e);
        }
        let tuples: Vec<_> = rows.into_iter().map(|row| row.tuple).collect();
        self.pages.rewrite_page(page_id, &tuples)
    }

    /// One merge pass: merges `runs` in pairs, carrying the last one over as
    /// it is when there is an odd number of them.
    fn merge_pass(&mut self, runs: Vec<Run>) -> Result<Vec<Run>> {
        let mut merged = Vec::with_capacity(runs.len().div_ceil(2));
        let mut runs = runs.into_iter();
        while let Some(first) = runs.next() {
            match runs.next() {
                Some(second) => merged.push(self.merge(&first, &second)?),
                None => merged.push(first),
            }
        }
        Ok(merged)
    }

    /// Merges `first` and `second` into a new run, freeing their pages.
    fn merge(&mut self, first: &Run, second: &Run) -> Result<Run> {
        let mut output = Run::new();
        let mut first = RunReader::new(first);
        let mut second = RunReader::new(second);
        loop {
            self.fill(&mut first)?;
            self.fill(&mut second)?;
            let from_first = match (first.rows.front(), second.rows.front()) {
                (Some(a), Some(b)) => {
                    // On a tie the row from the first run goes first, which
                    // keeps the sort stable.
                    compare_keys(self.plan.order_bys(), &a.keys, &b.keys)?.is_le()
                }
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (None, None) => break,
            };
            let reader = if from_first { &mut first } else { &mut second };
            let row = reader.rows.pop_front().unwrap();
            self.pages.append(&mut output, &row.tuple)?;
        }
        Ok(output)
    }

    /// Reads the next page of `reader`'s run into it, and frees the page,
    /// when its rows are all merged and the run has more pages.
    fn fill(&mut self, reader: &mut RunReader) -> Result<()> {
        if !reader.rows.is_empty() || reader.next_page_id == INVALID_PAGE_ID {
            return Ok(());
        }
        let (tuples, next_page_id) = self.pages.read_page(reader.next_page_id)?;
        self.pages.free_page(reader.next_page_id)?;
        for tuple in tuples {
            reader.rows.push_back(self.sort_row(tuple)?);
        }
        reader.next_page_id = next_page_id;
        Ok(())
    }

    /// Reads the last run's next page into `output`, and frees the page.
    fn read_output_page(&mut self) -> Result<()> {
        let (tuples, next_page_id) = self.pages.read_page(self.next_output_page)?;
        self.pages.free_page(self.next_output_page)?;
        self.output.extend(tuples);
        self.next_output_page = next_page_id;
        Ok(())
    }
}

impl RunReader {
    fn new(run: &Run) -> Self {
        Self {
            rows: VecDeque::new(),
            next_page_id: run.first_page_id,
        }
    }
}

/// How the row with sort keys `left` compares with the one with `right`.
fn compare_keys(order_bys: &[OrderBy], left: &[Value], right: &[Value]) -> Result<Ordering> {
    for ((l, r), order_by) in left.iter().zip(right).zip(order_bys) {
        let ordering = match (l.is_null(), r.is_null()) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Greater,
            (false, true) => Ordering::Less,
            (false, false) => l.compare(r)?.expect("neither value is NULL"),
        };
        let ordering = if order_by.desc {
            ordering.reverse()
        } else {
            ordering
        };
        if ordering.is_ne() {
            return Ok(ordering);
        }
    }
    Ok(Ordering::Equal)
}

impl Executor for SortExecutor {
    /// Sorts the child's rows, so that `next` can return them.
    fn init(&mut self) -> Result<()> {
        self.child.init()?;
        self.pages.free_all()?;
        self.output.clear();
        self.next_output_page = INVALID_PAGE_ID;

        let mut runs = self.sort_pages()?;
        while runs.len() > 1 {
            runs = self.merge_pass(runs)?;
        }
        if let Some(run) = runs.pop() {
            self.next_output_page = run.first_page_id;
        }
        Ok(())
    }

    fn next(
        &mut self,
        tuples: &mut Vec<Tuple>,
        rids: &mut Vec<Rid>,
        batch_size: usize,
    ) -> Result<bool> {
        tuples.clear();
        rids.clear();
        while tuples.len() < batch_size {
            if let Some(row) = self.output.pop_front() {
                tuples.push(row);
            } else if self.next_output_page != INVALID_PAGE_ID {
                self.read_output_page()?;
            } else {
                break;
            }
        }
        Ok(!tuples.is_empty())
    }

    fn get_output_schema(&self) -> &Schema {
        self.plan.get_output_schema()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common::{DataType, INVALID_PAGE_ID, Value};

    use super::super::test_util::{
        Db, column, constant, int, is_intermediate_page, user, users_schema,
    };
    use super::super::{Executor, create_executor};
    use super::{Run, SortExecutor};
    use crate::expressions::{AbstractExpr, ArithmeticExpr, ArithmeticOp};
    use crate::plans::{AbstractPlanNode, OrderBy, SortPlan, ValuesPlan};

    fn asc(expr: AbstractExpr) -> OrderBy {
        OrderBy { expr, desc: false }
    }

    fn desc(expr: AbstractExpr) -> OrderBy {
        OrderBy { expr, desc: true }
    }

    /// `users` rows, as a VALUES list.
    fn users(rows: &[(i32, Option<&str>)]) -> Arc<AbstractPlanNode> {
        let rows = rows
            .iter()
            .map(|&(id, name)| {
                vec![
                    int(id),
                    constant(name.map_or(Value::Null, Value::from), DataType::Varchar(32)),
                ]
            })
            .collect();
        Arc::new(ValuesPlan::new(users_schema(), rows).into())
    }

    /// Inserts users `1..=n` in a shuffled order, so that their ids are out
    /// of order across pages.
    fn insert_shuffled(db: &Db, n: i32) {
        // 7919 is prime, so it shuffles any `n` it does not divide.
        assert_ne!(n % 7919, 0);
        let names: Vec<_> = (1..=n).map(|id| format!("user {id}")).collect();
        let rows: Vec<_> = (0..n)
            .map(|i| {
                let id = (i * 7919) % n + 1;
                (id, Some(names[id as usize - 1].as_str()))
            })
            .collect();
        assert_eq!(db.insert(&rows), i64::from(n));
    }

    /// A sort of the users table, not initialized yet.
    fn sort_users(db: &Db, order_bys: Vec<OrderBy>) -> SortExecutor {
        let (ctx, scan) = (db.ctx(), db.scan(None));
        let child = create_executor(&ctx, &scan).unwrap();
        let plan = SortPlan::new(order_bys, scan);
        SortExecutor::new(ctx, plan, Box::new(child))
    }

    /// The ids on the pages of `run`, which must be sorted by id, checking
    /// its counts on the way.
    fn run_ids(sort: &SortExecutor, run: &Run) -> Vec<i32> {
        let (mut ids, mut pages) = (Vec::new(), 0);
        let mut page_id = run.first_page_id;
        while page_id != INVALID_PAGE_ID {
            let (tuples, next_page_id) = sort.pages.read_page(page_id).unwrap();
            for tuple in tuples {
                match tuple.value(&users_schema(), 0).unwrap() {
                    Value::Int(id) => ids.push(id),
                    value => panic!("{value:?}"),
                }
            }
            pages += 1;
            page_id = next_page_id;
        }
        assert_eq!((pages, ids.len()), (run.page_count, run.tuple_count));
        assert!(ids.is_sorted(), "{ids:?}");
        ids
    }

    #[test]
    fn orders_by_each_key_in_turn() {
        let db = Db::new();
        let rows = users(&[
            (3, Some("b")),
            (1, Some("a")),
            (2, Some("b")),
            (4, Some("a")),
        ]);
        let sort = SortPlan::new(vec![desc(column(1)), asc(column(0))], rows);
        assert_eq!(
            db.run(sort.into()).unwrap(),
            [
                user(2, Some("b")),
                user(3, Some("b")),
                user(1, Some("a")),
                user(4, Some("a")),
            ]
        );
    }

    #[test]
    fn nulls_sort_after_every_value() {
        let db = Db::new();
        let rows = || users(&[(1, None), (2, Some("b")), (3, None), (4, Some("a"))]);
        let ascending = SortPlan::new(vec![asc(column(1)), asc(column(0))], rows());
        assert_eq!(
            db.run(ascending.into()).unwrap(),
            [
                user(4, Some("a")),
                user(2, Some("b")),
                user(1, None),
                user(3, None),
            ]
        );
        let descending = SortPlan::new(vec![desc(column(1)), asc(column(0))], rows());
        assert_eq!(
            db.run(descending.into()).unwrap(),
            [
                user(1, None),
                user(3, None),
                user(2, Some("b")),
                user(4, Some("a")),
            ]
        );
    }

    #[test]
    fn sorts_nothing() {
        let db = Db::new();
        let sort = SortPlan::new(vec![asc(column(0))], users(&[]));
        assert!(db.run(sort.into()).unwrap().is_empty());
    }

    #[test]
    fn sorts_across_many_pages() {
        let db = Db::new();
        insert_shuffled(&db, 1000);
        let sort = SortPlan::new(vec![desc(column(0))], db.scan(None));
        let expected: Vec<_> = (1..=1000)
            .rev()
            .map(|id| user(id, Some(&format!("user {id}"))))
            .collect();
        assert_eq!(db.run(sort.into()).unwrap(), expected);
    }

    /// Sorted by `id % 2`, the even users come first and the odd ones after,
    /// each in the order the scan read them, through every merge.
    #[test]
    fn rows_with_equal_keys_keep_their_order() {
        let db = Db::new();
        insert_shuffled(&db, 1000);
        let parity = ArithmeticExpr::new(ArithmeticOp::Rem, column(0), int(2)).unwrap();
        let sort = SortPlan::new(vec![asc(parity.into())], db.scan(None));
        let scanned = db.users_rows();
        let (even, odd): (Vec<_>, Vec<_>) = scanned
            .into_iter()
            .partition(|row| matches!(row[0], Value::Int(id) if id % 2 == 0));
        let expected: Vec<_> = even.into_iter().chain(odd).collect();
        assert_eq!(db.run(sort.into()).unwrap(), expected);
    }

    /// Pass 0 makes one run per page, and each merge pass halves the number
    /// of runs, rounding up, so there are `1 + ⌈log₂ N⌉` passes.
    #[test]
    fn each_merge_pass_halves_the_runs() {
        let db = Db::new();
        insert_shuffled(&db, 1000);
        let mut sort = sort_users(&db, vec![asc(column(0))]);
        sort.child.init().unwrap();

        let mut runs = sort.sort_pages().unwrap();
        let n = runs.len();
        assert!(runs.iter().all(|run| run.page_count == 1));
        assert!(n > 2 && !n.is_power_of_two(), "{n} pages");
        let mut passes = 1;
        while runs.len() > 1 {
            let before = runs.len();
            let all: Vec<_> = runs.iter().flat_map(|run| run_ids(&sort, run)).collect();
            assert_eq!(all.len(), 1000);
            runs = sort.merge_pass(runs).unwrap();
            assert_eq!(runs.len(), before.div_ceil(2));
            passes += 1;
        }
        assert_eq!(run_ids(&sort, &runs[0]), (1..=1000).collect::<Vec<_>>());
        assert_eq!(passes, 1 + n.next_power_of_two().ilog2() as usize);
    }

    /// Each pass reads and writes each of the `N` pages at most once, on top
    /// of the `M` pages the scan reads.
    #[test]
    fn makes_at_most_2n_page_ios_per_pass() {
        let db = Db::new();
        insert_shuffled(&db, 5000);
        let m = {
            let mut iter = db.users.heap.iter().unwrap();
            let mut pages = 0;
            while iter.next_batch().unwrap().is_some() {
                pages += 1;
            }
            pages
        };
        let n = {
            let mut sort = sort_users(&db, vec![asc(column(0))]);
            sort.child.init().unwrap();
            sort.sort_pages().unwrap().len()
        };
        assert!(n > 8, "the runs do not fit in the buffer pool: {n} pages");
        let passes = 1 + n.next_power_of_two().ilog2() as usize;

        db.bpm.flush_all_pages().unwrap();
        let (reads, writes) = (db.disk.num_reads(), db.disk.num_writes());
        let mut sort = sort_users(&db, vec![asc(column(0))]);
        sort.init().unwrap();
        let ios = (db.disk.num_reads() - reads) + (db.disk.num_writes() - writes);
        assert!(
            ios <= m + 2 * n * passes,
            "{ios} > {m} + 2 × {n} × {passes}"
        );
        assert!(ios > m + n, "the runs were written out: {ios}");
    }

    /// Pages are freed once read, so returning every row frees them all, and
    /// the rest are freed when the sort starts over or is dropped.
    #[test]
    fn frees_its_pages() {
        let db = Db::new();
        insert_shuffled(&db, 600);
        let mut sort = sort_users(&db, vec![asc(column(0))]);
        sort.init().unwrap();
        let first: Vec<_> = sort.pages.page_ids().collect();
        assert!(!first.is_empty());
        assert!(first.iter().all(|&id| is_intermediate_page(&db, id)));

        sort.init().unwrap();
        assert!(first.iter().all(|&id| !is_intermediate_page(&db, id)));
        let second: Vec<_> = sort.pages.page_ids().collect();
        drop(sort);
        assert!(second.iter().all(|&id| !is_intermediate_page(&db, id)));

        let mut sort = sort_users(&db, vec![asc(column(0))]);
        sort.init().unwrap();
        let (mut tuples, mut rids) = (Vec::new(), Vec::new());
        let mut rows = 0;
        while sort.next(&mut tuples, &mut rids, 100).unwrap() {
            rows += tuples.len();
        }
        assert_eq!(rows, 600);
        assert_eq!(sort.pages.page_ids().count(), 0);
    }
}
