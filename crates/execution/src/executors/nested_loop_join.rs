use std::collections::VecDeque;
use std::sync::Arc;

use common::{Rid, Schema, Tuple, Value};
use storage::page::PAGE_SIZE;

use super::{AbstractExecutor, Executor};
use crate::Result;
use crate::execution_context::ExecutionContext;
use crate::expressions::Expr;
use crate::plans::{JoinType, NestedLoopJoinPlan, PlanNode};

/// A block nested loop join: for each block of left (outer) rows, the right
/// (inner) child is scanned from the start a block at a time, and every row
/// of the outer block is compared with every row of the inner block.
///
/// With `B` join frames ([`ExecutionContext::join_frames`]), an outer block
/// holds `B - 2` pages of rows, an inner block one page, and the output one
/// page. The inner side is scanned once per outer block rather than once per
/// outer row, so for an outer side of `M` pages and an inner side of `N`, the
/// join reads `M + ⌈M / (B - 2)⌉ × N` pages. A page here is `PAGE_SIZE` bytes
/// of tuple data. A table page holds less, as it also has a header and slots,
/// so an outer block covers at least `B - 2` table pages and the join reads
/// no more than that.
///
/// The joined rows are new, so they have no rids. They come out an inner
/// block at a time: each row of the outer block with its matches in the inner
/// block. For a LEFT join, the outer block's rows that matched no inner row
/// follow the last inner block, padded with NULLs.
///
/// [`ExecutionContext::join_frames`]: crate::execution_context::ExecutionContext::join_frames
pub struct NestedLoopJoinExecutor {
    plan: NestedLoopJoinPlan,
    outer: Box<Input>,
    inner: Box<Input>,
    /// Bytes of rows an outer block holds: `B - 2` pages.
    outer_block_size: usize,
    outer_block: Vec<Tuple>,
    /// Whether each row of `outer_block` has matched an inner row yet.
    matched: Vec<bool>,
    inner_block: Vec<Tuple>,
    state: State,
}

/// Where the join is, between calls to `next`.
#[derive(Clone, Copy)]
enum State {
    /// Read the next outer block and start the inner side over.
    OuterBlock,
    /// Read the next inner block for the current outer block.
    InnerBlock,
    /// Comparing the two blocks; `outer` and `inner` are the next pair.
    Join {
        outer: usize,
        inner: usize,
    },
    /// The inner side is done for this outer block; the outer rows from
    /// `outer` on that matched nothing are yet to be produced.
    Unmatched {
        outer: usize,
    },
    Done,
}

/// A child of the join, read a block at a time.
struct Input {
    executor: Box<AbstractExecutor>,
    /// Rows pulled from `executor` that did not fit in the last block, the
    /// first rows of the next one.
    carry: VecDeque<Tuple>,
    /// Whether `executor` has run out since it was last initialized.
    exhausted: bool,
    tuples: Vec<Tuple>,
    rids: Vec<Rid>,
}

impl Input {
    fn new(executor: Box<AbstractExecutor>) -> Self {
        Self {
            executor,
            carry: VecDeque::new(),
            exhausted: false,
            tuples: Vec::new(),
            rids: Vec::new(),
        }
    }

    fn init(&mut self) -> Result<()> {
        self.executor.init()?;
        self.carry.clear();
        self.exhausted = false;
        Ok(())
    }

    /// Replaces the contents of `block` with the next rows, as many as fit
    /// in `block_size` bytes, but at least one. Returns `false`, leaving
    /// `block` empty, once there are no more rows.
    fn next_block(
        &mut self,
        block: &mut Vec<Tuple>,
        block_size: usize,
        batch_size: usize,
    ) -> Result<bool> {
        block.clear();
        let mut bytes = 0;
        loop {
            while let Some(tuple) = self.carry.front() {
                if !block.is_empty() && bytes + tuple.len() > block_size {
                    return Ok(true);
                }
                bytes += tuple.len();
                block.extend(self.carry.pop_front());
            }
            if self.exhausted
                || !self
                    .executor
                    .next(&mut self.tuples, &mut self.rids, batch_size)?
            {
                self.exhausted = true;
                return Ok(!block.is_empty());
            }
            self.carry.extend(self.tuples.drain(..));
        }
    }

    fn schema(&self) -> &Schema {
        self.executor.get_output_schema()
    }
}

/// The batch `next` fills, which is full at `batch_size` rows or once it
/// holds a page of them.
struct Output<'a> {
    tuples: &'a mut Vec<Tuple>,
    bytes: usize,
    batch_size: usize,
}

impl Output<'_> {
    fn is_full(&self) -> bool {
        self.tuples.len() >= self.batch_size || self.bytes >= PAGE_SIZE
    }

    fn push(&mut self, schema: &Schema, values: &[Value]) -> Result<()> {
        let tuple = Tuple::encode(schema, values)?;
        self.bytes += tuple.len();
        self.tuples.push(tuple);
        Ok(())
    }
}

impl NestedLoopJoinExecutor {
    pub fn new(
        ctx: Arc<ExecutionContext>,
        plan: NestedLoopJoinPlan,
        left: Box<AbstractExecutor>,
        right: Box<AbstractExecutor>,
    ) -> Self {
        Self {
            plan,
            outer: Box::new(Input::new(left)),
            inner: Box::new(Input::new(right)),
            outer_block_size: (ctx.join_frames() - 2) * PAGE_SIZE,
            outer_block: Vec::new(),
            matched: Vec::new(),
            inner_block: Vec::new(),
            state: State::Done,
        }
    }

    /// Compares the blocks from the pair at `start` on, until `output` is
    /// full or every pair is done. Returns the state to go on from.
    fn join_blocks(&mut self, start: (usize, usize), output: &mut Output) -> Result<State> {
        let (outer_schema, inner_schema) = (self.outer.schema(), self.inner.schema());
        for o in start.0..self.outer_block.len() {
            let first = if o == start.0 { start.1 } else { 0 };
            for i in first..self.inner_block.len() {
                if output.is_full() {
                    return Ok(State::Join { outer: o, inner: i });
                }
                let (outer, inner) = (&self.outer_block[o], &self.inner_block[i]);
                if let Some(predicate) = self.plan.predicate()
                    && predicate.evaluate_join(outer, outer_schema, inner, inner_schema)?
                        != Value::Bool(true)
                {
                    continue;
                }
                self.matched[o] = true;
                let mut values = outer.decode(outer_schema)?;
                values.extend(inner.decode(inner_schema)?);
                output.push(self.plan.get_output_schema(), &values)?;
            }
        }
        Ok(State::InnerBlock)
    }

    /// Produces the rows of the outer block from `start` on that matched
    /// nothing, padded with NULLs, until `output` is full or they are done.
    /// Returns the state to go on from.
    fn pad_unmatched(&mut self, start: usize, output: &mut Output) -> Result<State> {
        let (outer_schema, nulls) = (self.outer.schema(), self.inner.schema().len());
        for o in start..self.outer_block.len() {
            if output.is_full() {
                return Ok(State::Unmatched { outer: o });
            }
            if !self.matched[o] {
                let mut values = self.outer_block[o].decode(outer_schema)?;
                values.extend(std::iter::repeat_n(Value::Null, nulls));
                output.push(self.plan.get_output_schema(), &values)?;
            }
        }
        Ok(State::OuterBlock)
    }
}

impl Executor for NestedLoopJoinExecutor {
    /// The inner child is started over for each outer block, so it is
    /// initialized by `next`.
    fn init(&mut self) -> Result<()> {
        self.outer.init()?;
        self.state = State::OuterBlock;
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
        let mut output = Output {
            tuples,
            bytes: 0,
            batch_size,
        };
        while !output.is_full() {
            self.state = match self.state {
                State::OuterBlock => {
                    let more = self.outer.next_block(
                        &mut self.outer_block,
                        self.outer_block_size,
                        batch_size,
                    )?;
                    if !more {
                        State::Done
                    } else {
                        self.matched = vec![false; self.outer_block.len()];
                        self.inner.init()?;
                        State::InnerBlock
                    }
                }
                State::InnerBlock => {
                    if self
                        .inner
                        .next_block(&mut self.inner_block, PAGE_SIZE, batch_size)?
                    {
                        State::Join { outer: 0, inner: 0 }
                    } else if self.plan.join_type() == JoinType::Left {
                        State::Unmatched { outer: 0 }
                    } else {
                        State::OuterBlock
                    }
                }
                State::Join { outer, inner } => self.join_blocks((outer, inner), &mut output)?,
                State::Unmatched { outer } => self.pad_unmatched(outer, &mut output)?,
                State::Done => break,
            };
        }
        Ok(!output.tuples.is_empty())
    }

    fn get_output_schema(&self) -> &Schema {
        self.plan.get_output_schema()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common::{Column, DataType, Schema, Value};
    use storage::page::PAGE_SIZE;

    use super::super::test_util::{Db, column, compare, constant, int, users_schema};
    use crate::expressions::{
        AbstractExpr, ArithmeticExpr, ArithmeticOp, ColumnValueExpr, ComparisonOp,
    };
    use crate::plans::{
        AbstractPlanNode, FilterPlan, InsertPlan, JoinType, NestedLoopJoinPlan, SeqScanPlan,
        ValuesPlan,
    };

    /// The fewest frames a join can have: outer blocks of one page.
    const MIN_FRAMES: usize = 3;

    /// A database whose joins get the default frames, and one whose joins
    /// get the fewest.
    fn dbs() -> [Db; 2] {
        let mut small = Db::new();
        small.set_join_frames(MIN_FRAMES);
        [Db::new(), small]
    }

    fn orders_schema() -> Schema {
        Schema::new(vec![
            Column::new("user_id", DataType::Int, true),
            Column::new("item", DataType::Varchar(32), false),
        ])
    }

    /// An `orders` table of `(user_id, item)` rows, as a VALUES list.
    fn orders(rows: &[(Option<i32>, &str)]) -> Arc<AbstractPlanNode> {
        let rows = rows
            .iter()
            .map(|&(user_id, item)| {
                vec![
                    constant(user_id.map_or(Value::Null, Value::Int), DataType::Int),
                    constant(Value::from(item), DataType::Varchar(32)),
                ]
            })
            .collect();
        Arc::new(ValuesPlan::new(orders_schema(), rows).into())
    }

    /// Column `idx` of the right side.
    fn right_column(idx: usize, schema: &Schema) -> AbstractExpr {
        ColumnValueExpr::new(1, idx, schema.column(idx).unwrap().clone()).into()
    }

    /// `users.id = orders.user_id`.
    fn on_user_id() -> AbstractExpr {
        compare(
            ComparisonOp::Eq,
            column(0),
            right_column(0, &orders_schema()),
        )
    }

    /// A joined `users` and `orders` row.
    fn row(id: i32, name: &str, user_id: Option<i32>, item: Option<&str>) -> Vec<Value> {
        vec![
            Value::Int(id),
            Value::from(name),
            user_id.map_or(Value::Null, Value::Int),
            item.map_or(Value::Null, Value::from),
        ]
    }

    /// The order of the joined rows depends on the block sizes, so tests
    /// compare them sorted.
    fn sorted(mut rows: Vec<Vec<Value>>) -> Vec<Vec<Value>> {
        rows.sort_by_key(|row| format!("{row:?}"));
        rows
    }

    fn some_orders() -> Arc<AbstractPlanNode> {
        orders(&[
            (Some(1), "pen"),
            (Some(3), "ink"),
            (None, "box"),
            (Some(1), "cap"),
            (Some(9), "mug"),
        ])
    }

    #[test]
    fn inner_join_keeps_the_matching_pairs() {
        for db in dbs() {
            db.insert_numbered(4);
            let join = NestedLoopJoinPlan::new(
                JoinType::Inner,
                db.scan(None),
                some_orders(),
                Some(on_user_id()),
            );
            assert_eq!(
                sorted(db.run(join.into()).unwrap()),
                sorted(vec![
                    row(1, "user 1", Some(1), Some("pen")),
                    row(1, "user 1", Some(1), Some("cap")),
                    row(3, "user 3", Some(3), Some("ink")),
                ])
            );
        }
    }

    /// Users 2 and 4 have no orders, and the order without a user matches
    /// no one, as NULL = 1 is not TRUE.
    #[test]
    fn left_join_pads_rows_without_matches() {
        for db in dbs() {
            db.insert_numbered(4);
            let join = NestedLoopJoinPlan::new(
                JoinType::Left,
                db.scan(None),
                some_orders(),
                Some(on_user_id()),
            );
            assert_eq!(
                sorted(db.run(join.into()).unwrap()),
                sorted(vec![
                    row(1, "user 1", Some(1), Some("pen")),
                    row(1, "user 1", Some(1), Some("cap")),
                    row(2, "user 2", None, None),
                    row(3, "user 3", Some(3), Some("ink")),
                    row(4, "user 4", None, None),
                ])
            );
        }
    }

    #[test]
    fn cross_join_pairs_every_row() {
        for db in dbs() {
            db.insert_numbered(3);
            let items = orders(&[(Some(7), "pen"), (None, "ink")]);
            let join = NestedLoopJoinPlan::new(JoinType::Inner, db.scan(None), items, None);
            let mut expected = Vec::new();
            for id in 1..=3 {
                let name = format!("user {id}");
                expected.push(row(id, &name, Some(7), Some("pen")));
                expected.push(row(id, &name, None, Some("ink")));
            }
            assert_eq!(sorted(db.run(join.into()).unwrap()), sorted(expected));
        }
    }

    #[test]
    fn empty_sides() {
        for db in dbs() {
            let left = NestedLoopJoinPlan::new(JoinType::Left, db.scan(None), some_orders(), None);
            assert_eq!(db.run(left.into()).unwrap(), Vec::<Vec<Value>>::new());

            db.insert_numbered(3);
            let inner = NestedLoopJoinPlan::new(JoinType::Inner, db.scan(None), orders(&[]), None);
            assert_eq!(db.run(inner.into()).unwrap(), Vec::<Vec<Value>>::new());
            let left = NestedLoopJoinPlan::new(JoinType::Left, db.scan(None), orders(&[]), None);
            assert_eq!(
                db.run(left.into()).unwrap(),
                [
                    row(1, "user 1", None, None),
                    row(2, "user 2", None, None),
                    row(3, "user 3", None, None),
                ]
            );
        }
    }

    /// The bytes of the `users` rows, from which the outer blocks are cut.
    fn users_bytes(db: &Db) -> usize {
        let mut iter = db.users.heap.iter().unwrap();
        let mut bytes = 0;
        while let Some(rows) = iter.next_batch().unwrap() {
            bytes += rows.iter().map(|(_, tuple)| tuple.len()).sum::<usize>();
        }
        bytes
    }

    /// Joins many outer blocks with a table scan on the right, read from the
    /// start for each: users.id + 1 = users.id pairs each user with the next.
    /// The cross join's output is many batches per pair of blocks.
    #[test]
    fn joins_across_many_blocks() {
        let mut db = Db::new();
        db.set_join_frames(MIN_FRAMES);
        db.insert_numbered(600);
        assert!(
            users_bytes(&db) > 2 * PAGE_SIZE,
            "users span 3 outer blocks"
        );

        let id = |tuple_idx| -> AbstractExpr {
            let id = users_schema().column(0).unwrap().clone();
            ColumnValueExpr::new(tuple_idx, 0, id).into()
        };
        let next = ArithmeticExpr::new(ArithmeticOp::Add, id(0), int(1)).unwrap();
        let predicate = compare(ComparisonOp::Eq, next.into(), id(1));
        let join = NestedLoopJoinPlan::new(
            JoinType::Inner,
            db.scan(None),
            db.scan(None),
            Some(predicate),
        );
        let rows = db.run(join.into()).unwrap();
        assert_eq!(rows.len(), 599);
        assert!(rows.iter().all(|row| match (&row[0], &row[2]) {
            (Value::Int(left), Value::Int(right)) => left + 1 == *right,
            pair => panic!("{pair:?}"),
        }));

        // Every user in 1..=600 with users 1..=200: 120,000 rows.
        let few = Arc::new(
            FilterPlan::new(
                compare(ComparisonOp::LtEq, column(0), int(200)),
                db.scan(None),
            )
            .into(),
        );
        let cross = NestedLoopJoinPlan::new(JoinType::Inner, db.scan(None), few, None);
        assert_eq!(db.run(cross.into()).unwrap().len(), 600 * 200);
    }

    /// Each outer block finds out which of its own rows matched.
    #[test]
    fn left_join_across_blocks() {
        let mut db = Db::new();
        db.set_join_frames(MIN_FRAMES);
        db.insert_numbered(600);
        let some = orders(&[(Some(1), "pen"), (Some(300), "ink"), (Some(600), "cap")]);
        let join = NestedLoopJoinPlan::new(JoinType::Left, db.scan(None), some, Some(on_user_id()));
        let rows = db.run(join.into()).unwrap();
        let expected: Vec<_> = (1..=600)
            .map(|id| {
                let name = format!("user {id}");
                match id {
                    1 => row(id, &name, Some(1), Some("pen")),
                    300 => row(id, &name, Some(300), Some("ink")),
                    600 => row(id, &name, Some(600), Some("cap")),
                    _ => row(id, &name, None, None),
                }
            })
            .collect();
        assert_eq!(sorted(rows), sorted(expected));
    }

    /// `items (id INT NOT NULL, label VARCHAR(32))`, with rows `1..=n`
    /// whose labels fill the column.
    fn create_items(db: &Db, n: i32) -> Arc<AbstractPlanNode> {
        let schema = Schema::new(vec![
            Column::new("id", DataType::Int, false),
            Column::new("label", DataType::Varchar(32), false),
        ]);
        let items = db
            .catalog()
            .create_table("items", schema.clone(), &[])
            .unwrap();
        let rows = (1..=n)
            .map(|id| {
                let label = format!("{id:0>32}");
                vec![
                    int(id),
                    constant(Value::from(label.as_str()), DataType::Varchar(32)),
                ]
            })
            .collect();
        let values = ValuesPlan::new(schema.clone(), rows);
        db.run(InsertPlan::new(items.oid, Arc::new(values.into())).into())
            .unwrap();
        Arc::new(SeqScanPlan::new(items.oid, "items", schema, None).into())
    }

    fn pages(db: &Db, table: &str) -> usize {
        let mut iter = db.catalog().get_table(table).unwrap().heap.iter().unwrap();
        let mut pages = 0;
        while iter.next_batch().unwrap().is_some() {
            pages += 1;
        }
        pages
    }

    /// The pages the join of `users` with `items` on their ids reads from
    /// disk, with `frames` join frames.
    fn join_reads(db: &mut Db, items: &Arc<AbstractPlanNode>, frames: usize) -> usize {
        db.set_join_frames(frames);
        let on_id = compare(
            ComparisonOp::Eq,
            column(0),
            ColumnValueExpr::new(1, 0, Column::new("id", DataType::Int, false)).into(),
        );
        let join = NestedLoopJoinPlan::new(
            JoinType::Inner,
            db.scan(None),
            Arc::clone(items),
            Some(on_id),
        );
        db.bpm.flush_all_pages().unwrap();
        let before = db.disk.num_reads();
        assert_eq!(db.run(join.into()).unwrap().len(), 1000);
        db.disk.num_reads() - before
    }

    /// The inner table, too big for the buffer pool, is read from disk once
    /// per outer block, so the join reads the `M` outer pages and, for each
    /// block, the `N` inner pages, or fewer when some are still in the pool.
    #[test]
    fn reads_the_inner_side_once_per_outer_block() {
        let mut db = Db::new();
        db.insert_numbered(1000);
        let items = create_items(&db, 1000);
        let (m, n) = (pages(&db, "users"), pages(&db, "items"));
        assert!(n > 8, "items does not fit in the buffer pool");

        let one_block = join_reads(&mut db, &items, m + 2);
        assert!(one_block <= m + n, "{one_block} > {m} + {n}");

        let one_page_blocks = join_reads(&mut db, &items, MIN_FRAMES);
        let blocks = users_bytes(&db).div_ceil(PAGE_SIZE);
        assert!(blocks > 1 && blocks <= m);
        assert!(
            one_page_blocks <= m + blocks * n,
            "{one_page_blocks} > {m} + {blocks} × {n}"
        );
        assert!(
            one_page_blocks > one_block + n,
            "the inner side is read again for each block: {one_page_blocks} vs {one_block}"
        );
    }
}
