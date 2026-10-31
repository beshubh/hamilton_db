use std::cmp::Ordering;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use common::hash::{ValuesHasher, partition_index};
use common::{INVALID_PAGE_ID, PageId, Rid, Schema, Tuple, Value};

use super::intermediate_result::{IntermediatePages, IntermediateResult, PageCount};
use super::{AbstractExecutor, Executor};
use crate::Result;
use crate::execution_context::ExecutionContext;
use crate::expressions::{AbstractExpr, Expr, evaluate_in_place};
use crate::plans::{HashJoinPlan, JoinType, PlanNode};

/// How many rows a child is asked for at a time: while partitioning it,
/// while reading the right child into memory, and while probing with the
/// left child. Each batch goes straight to the partitions' pages or the hash
/// table, or is probed with, so this only bounds the rows in flight.
const CHILD_BATCH_SIZE: usize = 128;

/// The seed the hash table hashes keys with. Partitioning uses seeds from 0
/// up, but the table's buckets don't depend on the partitions, so any seed
/// will do.
const HASH_TABLE_SEED: u64 = 0;

/// A hash join: the right child is the build side and the left child the
/// probe side. With `B` join frames ([`ExecutionContext::join_frames`]), a
/// hash table gets `B - 2` frames, leaving one to read left rows and one for
/// output.
///
/// The right child is read first. If its rows fit in the hash table, they
/// make the only one, and the left child is probed with as it is read, so
/// neither side is written out. That is the simple in-memory hash join.
///
/// If they do not fit, it is a grace hash join: both children are split
/// into `B - 1` partitions by a hash of their keys, written to intermediate
/// pages: one frame reads the input and each partition has one to write to.
/// Rows with equal keys land in partitions with the same number, so each
/// left partition only has to be joined with the matching right partition,
/// through a hash table of that right partition. A right partition of more
/// than `B - 2` pages is partitioned again, along with its left partition,
/// with another hash function, until it fits. Unlike a hybrid hash join, no
/// partition is kept in memory while the others are written out: once the
/// right side does not fit, all of it is.
///
/// ```text
/// read right rows while they fit in B - 2 pages
/// if that is all of them:
///     build a hash table of them by their right_keys
///     for each left row, as the left child gives it:
///         join it with the hash table (below)
/// else:
///     partition the right rows and the left rows by hash(keys, seed 0)
///     for each pair of partitions:
///         while the right partition is more than B - 2 pages:
///             partition both again by hash(keys, seed + 1)
///         for each chunk of B - 2 pages of the right partition:
///             build a hash table of the chunk's rows by their right_keys
///             for each left row of the left partition:
///                 join it with the hash table (below)
///
/// join a left row with the hash table:
///     for each right row in the hash table under its left_keys:
///         if the keys are equal and the pair passes the predicate, output it
///     LEFT join, last chunk: if no chunk had a match, output the row padded
///         with NULLs
/// ```
///
/// A right partition that fits is a single chunk. If partitioning a right
/// partition sends all of its rows to the same partition, they probably all
/// have the same keys, and partitioning again would not split them either.
/// Such a pair is kept as it is, and its left partition is read once per
/// chunk, as in a block nested loop join. `matched` remembers which left
/// rows matched in an earlier chunk, so only the last chunk pads them.
///
/// A NULL key matches nothing, so right rows with one are dropped, and so
/// are left rows, except in a LEFT join, which pads them with NULLs. When
/// partitioning, those left rows get their own partition, paired with an
/// empty right partition.
///
/// Keys are read from the rows in place, so a VARCHAR key is hashed and
/// compared without being copied, and a joined row is made by copying the
/// bytes of the two rows ([`Tuple::join`]) without decoding them.
///
/// One left row can have more matches than fit in a batch, so its joined
/// rows wait in `output` until `next` returns them. That holds every joined
/// row of one left row and one chunk in memory at once.
///
/// [`ExecutionContext::join_frames`]: crate::execution_context::ExecutionContext::join_frames
pub struct HashJoinExecutor {
    exec_ctx: Arc<ExecutionContext>,
    plan: HashJoinPlan,
    /// The probe side.
    left: Box<AbstractExecutor>,
    /// The build side.
    right: Box<AbstractExecutor>,
    /// Where `next_left_row` reads left rows from.
    left_input: LeftInput,
    /// The pairs of partitions to join, whose right partitions fit in memory
    /// unless partitioning could not split them. Empty unless the right side
    /// did not fit.
    partition_pairs: Vec<PartitionPair>,
    /// The pages the partitions were written to, freed when the join is
    /// initialized again or dropped.
    pages: IntermediatePages,
    /// The pair after the one being joined.
    next_pair: usize,
    /// The right rows being joined with, by the hash of their keys: all of
    /// them, or the current chunk of the pair's right partition.
    hash_table: HashMap<u64, Vec<Tuple>>,
    /// The first page of the right partition's next chunk, or
    /// `INVALID_PAGE_ID` if `hash_table` holds its last chunk, or all the
    /// right rows.
    next_right_page: PageId,
    /// The rows of the left page or batch being probed with.
    left_rows: Vec<Tuple>,
    left_rids: Vec<Rid>,
    /// The next row of `left_rows` to probe with.
    next_left: usize,
    /// The left page after `left_rows`, or `INVALID_PAGE_ID`.
    next_left_page: PageId,
    /// Where the next left row is in its partition, or in its batch.
    left_position: usize,
    /// Whether each row of the pair's left partition has matched a right row
    /// in any chunk so far. When the right side fit in memory, there is one
    /// chunk, and this only covers the batch of left rows being probed with.
    matched: Vec<bool>,
    /// Joined rows not returned by `next` yet.
    output: VecDeque<Tuple>,
}

/// Rows with keys that hash alike. Its page count decides whether a hash
/// table of the rows fits in memory.
type Partition = IntermediateResult;

/// A left partition and the right partition whose rows its rows can match.
struct PartitionPair {
    left: Partition,
    right: Partition,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Side {
    Left,
    Right,
}

/// Where the left rows come from.
#[derive(Clone, Copy, PartialEq, Eq)]
enum LeftInput {
    /// The left child, probed with as it is read: the right side fit in
    /// memory.
    Child,
    /// The left partition of each pair in turn.
    Partitions,
}

impl HashJoinExecutor {
    pub fn new(
        ctx: Arc<ExecutionContext>,
        plan: HashJoinPlan,
        left: Box<AbstractExecutor>,
        right: Box<AbstractExecutor>,
    ) -> Self {
        Self {
            pages: IntermediatePages::new(Arc::clone(&ctx)),
            exec_ctx: ctx,
            plan,
            left,
            right,
            left_input: LeftInput::Partitions,
            partition_pairs: Vec::new(),
            next_pair: 0,
            hash_table: HashMap::new(),
            next_right_page: INVALID_PAGE_ID,
            left_rows: Vec::new(),
            left_rids: Vec::new(),
            next_left: 0,
            next_left_page: INVALID_PAGE_ID,
            left_position: 0,
            matched: Vec::new(),
            output: VecDeque::new(),
        }
    }

    /// How many pages of right rows a hash table may hold: `B - 2`.
    fn build_pages(&self) -> usize {
        self.exec_ctx.join_frames() - 2
    }

    fn keys(&self, side: Side) -> &[AbstractExpr] {
        match side {
            Side::Left => self.plan.left_keys(),
            Side::Right => self.plan.right_keys(),
        }
    }

    fn schema(&self, side: Side) -> &Schema {
        match side {
            Side::Left => self.left.get_output_schema(),
            Side::Right => self.right.get_output_schema(),
        }
    }

    /// Hashes the values of `side`'s keys on `tuple` with seed `seed`: each
    /// round of partitioning uses another one. `None` if a key is NULL, as
    /// the row then matches nothing.
    fn hash_keys(&self, tuple: &Tuple, side: Side, seed: u64) -> Result<Option<u64>> {
        let mut hasher = ValuesHasher::new(seed);
        for key in self.keys(side) {
            let value = evaluate_in_place(key, tuple, self.schema(side))?;
            if value.as_value_ref().is_null() {
                return Ok(None);
            }
            hasher.add(value.as_value_ref());
        }
        Ok(Some(hasher.finish()))
    }

    /// Whether the keys of `left` and `right`, neither of them NULL, are
    /// equal under `=`, which compares an INT and a BIGINT by value.
    fn keys_equal(&self, left: &Tuple, right: &Tuple) -> Result<bool> {
        let keys = self.plan.left_keys().iter().zip(self.plan.right_keys());
        for (left_key, right_key) in keys {
            let l = evaluate_in_place(left_key, left, self.schema(Side::Left))?;
            let r = evaluate_in_place(right_key, right, self.schema(Side::Right))?;
            if l.as_value_ref().compare(r.as_value_ref())? != Some(Ordering::Equal) {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

/// Reading the right side and partitioning, done by `init`.
impl HashJoinExecutor {
    /// How many partitions each round of partitioning splits rows into:
    /// `B - 1`, one output page each and one frame for the input.
    fn fan_out(&self) -> usize {
        self.exec_ctx.join_frames() - 1
    }

    /// Reads the right child while its rows fit on
    /// [`build_pages`](Self::build_pages) pages. Returns the rows read, and
    /// whether they are all of them.
    fn read_right_while_it_fits(&mut self) -> Result<(Vec<Tuple>, bool)> {
        let (mut rows, mut pages) = (Vec::new(), PageCount::new());
        let (mut tuples, mut rids) = (Vec::new(), Vec::new());
        while pages.pages() <= self.build_pages() {
            if !self.right.next(&mut tuples, &mut rids, CHILD_BATCH_SIZE)? {
                return Ok((rows, true));
            }
            for tuple in tuples.drain(..) {
                pages.add(&tuple);
                rows.push(tuple);
            }
        }
        Ok((rows, false))
    }

    /// Partitions both children, for a right side too big for memory:
    /// `right_rows`, the right rows read already, along with the rest of the
    /// right child, then the left child. Then pairs up the partitions.
    fn partition_both(&mut self, right_rows: Vec<Tuple>) -> Result<()> {
        let (rights, _) = self.partition_child(Side::Right, right_rows)?;
        let (lefts, null_key_rows) = self.partition_child(Side::Left, Vec::new())?;
        for (left, right) in lefts.into_iter().zip(rights) {
            self.prepare_partition_pair(left, right, 1)?;
        }
        if null_key_rows.tuple_count > 0 {
            self.partition_pairs.push(PartitionPair {
                left: null_key_rows,
                right: Partition::new(),
            });
        }
        Ok(())
    }

    /// Splits `read`, rows of `side`'s child read already, and then the rest
    /// of the child, into [`fan_out`](Self::fan_out) partitions with hash
    /// function 0. Also returns the rows with a NULL key, which are only
    /// kept for the left side of a LEFT join.
    fn partition_child(
        &mut self,
        side: Side,
        read: Vec<Tuple>,
    ) -> Result<(Vec<Partition>, Partition)> {
        let keep_null_keys = side == Side::Left && self.plan.join_type() == JoinType::Left;
        let mut partitions: Vec<_> = (0..self.fan_out()).map(|_| Partition::new()).collect();
        let mut null_key_rows = Partition::new();
        let (mut tuples, mut rids) = (read, Vec::new());
        loop {
            for tuple in &tuples {
                match self.hash_keys(tuple, side, 0)? {
                    Some(hash) => {
                        let i = partition_index(hash, partitions.len());
                        self.pages.append(&mut partitions[i], tuple)?;
                    }
                    None if keep_null_keys => self.pages.append(&mut null_key_rows, tuple)?,
                    None => {}
                }
            }
            let child = match side {
                Side::Left => &mut self.left,
                Side::Right => &mut self.right,
            };
            if !child.next(&mut tuples, &mut rids, CHILD_BATCH_SIZE)? {
                break;
            }
        }
        Ok((partitions, null_key_rows))
    }

    /// Splits the rows of `partition`, from `side`, into
    /// [`fan_out`](Self::fan_out) partitions with hash function `seed`.
    fn repartition(
        &mut self,
        partition: &Partition,
        side: Side,
        seed: u64,
    ) -> Result<Vec<Partition>> {
        let mut partitions: Vec<_> = (0..self.fan_out()).map(|_| Partition::new()).collect();
        let mut page_id = partition.first_page_id;
        while page_id != INVALID_PAGE_ID {
            let (tuples, next_page_id) = self.pages.read_page(page_id)?;
            for tuple in &tuples {
                let hash = self
                    .hash_keys(tuple, side, seed)?
                    .expect("rows with a NULL key are not partitioned");
                let i = partition_index(hash, partitions.len());
                self.pages.append(&mut partitions[i], tuple)?;
            }
            page_id = next_page_id;
        }
        Ok(partitions)
    }

    /// Adds `left` and `right` to the pairs to join, first partitioning both
    /// again with hash function `seed` if `right` does not fit in memory, and
    /// so on for each pair that gives.
    fn prepare_partition_pair(
        &mut self,
        left: Partition,
        right: Partition,
        seed: u64,
    ) -> Result<()> {
        // Without left rows there is nothing to output, and without right
        // rows only a LEFT join outputs anything.
        if left.tuple_count == 0
            || (right.tuple_count == 0 && self.plan.join_type() == JoinType::Inner)
        {
            return Ok(());
        }
        if right.page_count <= self.build_pages() {
            self.partition_pairs.push(PartitionPair { left, right });
            return Ok(());
        }

        let rights = self.repartition(&right, Side::Right, seed)?;
        if rights.iter().any(|p| p.tuple_count == right.tuple_count) {
            // All the rows went to one partition, so another round would not
            // split them either.
            self.partition_pairs.push(PartitionPair { left, right });
            return Ok(());
        }
        let lefts = self.repartition(&left, Side::Left, seed)?;
        for (left, right) in lefts.into_iter().zip(rights) {
            self.prepare_partition_pair(left, right, seed + 1)?;
        }
        Ok(())
    }
}

/// Building and probing, done by `next`.
impl HashJoinExecutor {
    /// Adds a right row to the hash table, under the hash of its keys. A row
    /// with a NULL key matches nothing, so it is left out.
    fn add_to_hash_table(&mut self, tuple: Tuple) -> Result<()> {
        if let Some(hash) = self.hash_keys(&tuple, Side::Right, HASH_TABLE_SEED)? {
            self.hash_table.entry(hash).or_default().push(tuple);
        }
        Ok(())
    }

    /// Starts another pass over the current pair's left partition, with the
    /// next chunk of its right partition, or with the next pair's first chunk
    /// once that one is done. False once every pair is done, and at once
    /// when the right side fit in memory, as there are no pairs.
    fn start_next_chunk(&mut self) -> Result<bool> {
        if self.next_right_page == INVALID_PAGE_ID {
            let Some(pair) = self.partition_pairs.get(self.next_pair) else {
                return Ok(false);
            };
            self.next_right_page = pair.right.first_page_id;
            self.matched = vec![false; pair.left.tuple_count];
            self.next_pair += 1;
        }
        self.build_hash_table()?;

        self.left_rows.clear();
        self.next_left = 0;
        self.next_left_page = self.partition_pairs[self.next_pair - 1].left.first_page_id;
        self.left_position = 0;
        Ok(true)
    }

    /// Replaces the hash table with the right rows on the next
    /// [`build_pages`](Self::build_pages) pages from `next_right_page` on.
    fn build_hash_table(&mut self) -> Result<()> {
        self.hash_table.clear();
        for _ in 0..self.build_pages() {
            if self.next_right_page == INVALID_PAGE_ID {
                break;
            }
            let (rows, next_page_id) = self.pages.read_page(self.next_right_page)?;
            for tuple in rows {
                self.add_to_hash_table(tuple)?;
            }
            self.next_right_page = next_page_id;
        }
        Ok(())
    }

    /// The next left row and where it is among the rows `matched` covers,
    /// reading the next batch of the left child, or page of the pair's left
    /// partition, when this one is done. None once they are done.
    fn next_left_row(&mut self) -> Result<Option<(usize, Tuple)>> {
        while self.next_left == self.left_rows.len() {
            match self.left_input {
                LeftInput::Child => {
                    // The child empties `left_rows` even when it has no more.
                    self.next_left = 0;
                    let more = self.left.next(
                        &mut self.left_rows,
                        &mut self.left_rids,
                        CHILD_BATCH_SIZE,
                    )?;
                    if !more {
                        return Ok(None);
                    }
                    self.matched.clear();
                    self.matched.resize(self.left_rows.len(), false);
                    self.left_position = 0;
                }
                LeftInput::Partitions => {
                    if self.next_left_page == INVALID_PAGE_ID {
                        return Ok(None);
                    }
                    (self.left_rows, self.next_left_page) =
                        self.pages.read_page(self.next_left_page)?;
                    self.next_left = 0;
                }
            }
        }
        let row = std::mem::take(&mut self.left_rows[self.next_left]);
        self.next_left += 1;
        self.left_position += 1;
        Ok(Some((self.left_position - 1, row)))
    }

    /// Joins `left`, the row at `position` among the rows `matched` covers,
    /// with the right rows in the hash table under its keys, adding the
    /// joined rows to `output`. In a LEFT join, once the last chunk is built,
    /// a row that never matched is added padded with NULLs.
    fn join_left_row(&mut self, position: usize, left: &Tuple) -> Result<()> {
        let left_schema = self.left.get_output_schema();
        let right_schema = self.right.get_output_schema();
        let schema = self.plan.get_output_schema();
        if let Some(hash) = self.hash_keys(left, Side::Left, HASH_TABLE_SEED)? {
            for right in self.hash_table.get(&hash).into_iter().flatten() {
                if !self.keys_equal(left, right)? {
                    continue;
                }
                if let Some(predicate) = self.plan.predicate()
                    && predicate.evaluate_join(left, left_schema, right, right_schema)?
                        != Value::Bool(true)
                {
                    continue;
                }
                let joined = Tuple::join(schema, left, left_schema, Some(right), right_schema)?;
                self.output.push_back(joined);
                self.matched[position] = true;
            }
        }

        let last_chunk = self.next_right_page == INVALID_PAGE_ID;
        if last_chunk && !self.matched[position] && self.plan.join_type() == JoinType::Left {
            let padded = Tuple::join(schema, left, left_schema, None, right_schema)?;
            self.output.push_back(padded);
        }
        Ok(())
    }
}

impl Executor for HashJoinExecutor {
    /// Reads the right child into the hash table if it fits, or else
    /// partitions both children, so that `next` can join the pairs of
    /// partitions one at a time.
    fn init(&mut self) -> Result<()> {
        self.left.init()?;
        self.right.init()?;
        self.left_input = LeftInput::Partitions;
        self.partition_pairs.clear();
        self.pages.free_all()?;
        self.next_pair = 0;
        self.hash_table.clear();
        self.next_right_page = INVALID_PAGE_ID;
        self.left_rows.clear();
        self.next_left = 0;
        self.next_left_page = INVALID_PAGE_ID;
        self.left_position = 0;
        self.matched.clear();
        self.output.clear();

        let (right_rows, all_read) = self.read_right_while_it_fits()?;
        if !all_read {
            return self.partition_both(right_rows);
        }
        for tuple in right_rows {
            self.add_to_hash_table(tuple)?;
        }
        // Without right rows to match, only a LEFT join outputs anything.
        // An inner join is left with no pairs to join, so it is done at once.
        if !self.hash_table.is_empty() || self.plan.join_type() == JoinType::Left {
            self.left_input = LeftInput::Child;
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
            } else if let Some((position, left)) = self.next_left_row()? {
                self.join_left_row(position, &left)?;
            } else if !self.start_next_chunk()? {
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
    use std::collections::BTreeSet;
    use std::fmt::Debug;
    use std::sync::Arc;

    use common::{Column, DataType, INVALID_PAGE_ID, Schema, Value};

    use super::super::test_util::{
        Db, column, compare, constant, decode, int, is_intermediate_page, users_schema,
    };
    use super::super::{Executor, create_executor};
    use super::{HashJoinExecutor, LeftInput, Partition};
    use crate::expressions::{
        AbstractExpr, ArithmeticExpr, ArithmeticOp, ColumnValueExpr, ComparisonOp,
    };
    use crate::plans::{AbstractPlanNode, HashJoinPlan, JoinType, PlanNode, ValuesPlan};

    /// The fewest frames a join can have: 2 partitions per round, and right
    /// partitions of one page.
    const MIN_FRAMES: usize = 3;

    /// A database whose joins get the default frames, and one whose joins
    /// get the fewest.
    fn dbs() -> [Db; 2] {
        let mut small = Db::new();
        small.set_join_frames(MIN_FRAMES);
        [Db::new(), small]
    }

    /// A hash join of `left` and `right` on `left_key = right_key`, not
    /// initialized yet.
    fn hash_join(
        db: &Db,
        join_type: JoinType,
        left: Arc<AbstractPlanNode>,
        right: Arc<AbstractPlanNode>,
        left_key: AbstractExpr,
        right_key: AbstractExpr,
    ) -> HashJoinExecutor {
        let ctx = db.ctx();
        let left_executor = create_executor(&ctx, &left).unwrap();
        let right_executor = create_executor(&ctx, &right).unwrap();
        let plan = HashJoinPlan::new(
            join_type,
            left,
            right,
            vec![left_key],
            vec![right_key],
            None,
        );
        HashJoinExecutor::new(ctx, plan, Box::new(left_executor), Box::new(right_executor))
    }

    /// [`hash_join`] with both sides partitioned, as `init` does when the
    /// right side does not fit in memory.
    fn partitioned(
        db: &Db,
        join_type: JoinType,
        left: Arc<AbstractPlanNode>,
        right: Arc<AbstractPlanNode>,
        left_key: AbstractExpr,
        right_key: AbstractExpr,
    ) -> HashJoinExecutor {
        let mut join = hash_join(db, join_type, left, right, left_key, right_key);
        join.left.init().unwrap();
        join.right.init().unwrap();
        join.partition_both(Vec::new()).unwrap();
        join
    }

    /// The first column of the rows of `partition`, checking the partition's
    /// counts on the way.
    fn ids(join: &HashJoinExecutor, partition: &Partition, schema: &Schema) -> Vec<Value> {
        let (mut ids, mut pages) = (Vec::new(), 0);
        let (mut page_id, mut last) = (partition.first_page_id, INVALID_PAGE_ID);
        while page_id != INVALID_PAGE_ID {
            let (tuples, next_page_id) = join.pages.read_page(page_id).unwrap();
            for tuple in tuples {
                ids.push(tuple.value(schema, 0).unwrap());
            }
            pages += 1;
            last = page_id;
            page_id = next_page_id;
        }
        assert_eq!(
            (pages, ids.len(), last),
            (
                partition.page_count,
                partition.tuple_count,
                partition.last_page_id
            )
        );
        ids
    }

    fn ints(ids: impl IntoIterator<Item = i32>) -> Vec<Value> {
        ids.into_iter().map(Value::Int).collect()
    }

    /// The order of the rows depends on the partitions, so tests compare
    /// them sorted.
    fn sorted<T: Debug>(mut values: Vec<T>) -> Vec<T> {
        values.sort_by_key(|value| format!("{value:?}"));
        values
    }

    /// `(user_id INT)` rows, as a VALUES list.
    fn user_ids(ids: &[Option<i32>]) -> Arc<AbstractPlanNode> {
        let schema = Schema::new(vec![Column::new("user_id", DataType::Int, true)]);
        let rows = ids
            .iter()
            .map(|id| vec![constant(id.map_or(Value::Null, Value::Int), DataType::Int)])
            .collect();
        Arc::new(ValuesPlan::new(schema, rows).into())
    }

    fn user_id() -> AbstractExpr {
        ColumnValueExpr::new(0, 0, Column::new("user_id", DataType::Int, true)).into()
    }

    /// Joining users with themselves on their ids, each pair of partitions
    /// holds the same users on both sides, and every user is in one pair.
    /// With the fewest frames, the users take several pages, so partitions
    /// are split again until their right sides fit in one.
    #[test]
    fn partitions_rows_with_their_matches() {
        for db in dbs() {
            db.insert_numbered(600);
            let join = partitioned(
                &db,
                JoinType::Inner,
                db.scan(None),
                db.scan(None),
                column(0),
                column(0),
            );
            let mut all = Vec::new();
            for pair in &join.partition_pairs {
                let left = sorted(ids(&join, &pair.left, &users_schema()));
                let right = sorted(ids(&join, &pair.right, &users_schema()));
                assert!(!left.is_empty());
                assert_eq!(left, right);
                assert!(pair.right.page_count <= join.build_pages());
                all.extend(left);
            }
            assert_eq!(sorted(all), sorted(ints(1..=600)));
            if db.join_frames() == MIN_FRAMES {
                assert!(
                    join.partition_pairs.len() > MIN_FRAMES - 1,
                    "the partitions were split again"
                );
            }
        }
    }

    /// A NULL key matches nothing, so the rows with one are only kept on the
    /// left of a LEFT join, paired with an empty right partition.
    #[test]
    fn rows_with_null_keys() {
        for db in dbs() {
            let left = user_ids(&[Some(1), None, Some(3), None]);
            let right = user_ids(&[None, Some(3), Some(1)]);
            for join_type in [JoinType::Inner, JoinType::Left] {
                let join = partitioned(
                    &db,
                    join_type,
                    Arc::clone(&left),
                    Arc::clone(&right),
                    user_id(),
                    user_id(),
                );
                let schema = left.get_output_schema();
                let (mut lefts, mut rights) = (Vec::new(), Vec::new());
                for pair in &join.partition_pairs {
                    lefts.extend(ids(&join, &pair.left, schema));
                    rights.extend(ids(&join, &pair.right, schema));
                }
                let mut expected = ints([1, 3]);
                if join_type == JoinType::Left {
                    expected.extend([Value::Null, Value::Null]);
                }
                assert_eq!(sorted(lefts), sorted(expected));
                assert_eq!(sorted(rights), ints([1, 3]));
            }
        }
    }

    /// Without right rows nothing matches: an inner join has no pairs to
    /// join, and a LEFT join keeps every left row.
    #[test]
    fn empty_right_side() {
        for db in dbs() {
            db.insert_numbered(100);
            let inner = partitioned(
                &db,
                JoinType::Inner,
                db.scan(None),
                user_ids(&[]),
                column(0),
                user_id(),
            );
            assert!(inner.partition_pairs.is_empty());

            let left = partitioned(
                &db,
                JoinType::Left,
                db.scan(None),
                user_ids(&[]),
                column(0),
                user_id(),
            );
            let mut all = Vec::new();
            for pair in &left.partition_pairs {
                assert_eq!(pair.right.tuple_count, 0);
                all.extend(ids(&left, &pair.left, &users_schema()));
            }
            assert_eq!(sorted(all), sorted(ints(1..=100)));
        }
    }

    /// Right rows that all have the same key cannot be split, so their
    /// partition is kept whole although it does not fit.
    #[test]
    fn equal_keys_are_not_split() {
        let mut db = Db::new();
        db.set_join_frames(MIN_FRAMES);
        db.insert_numbered(600);
        let join = partitioned(
            &db,
            JoinType::Inner,
            db.scan(None),
            db.scan(None),
            column(0),
            int(1),
        );
        let [pair] = join.partition_pairs.as_slice() else {
            panic!("{} pairs", join.partition_pairs.len());
        };
        assert_eq!(pair.right.tuple_count, 600);
        assert!(pair.right.page_count > join.build_pages());
        assert!(ids(&join, &pair.left, &users_schema()).contains(&Value::Int(1)));
    }

    /// A right side that fits in the hash table is joined in memory, and
    /// nothing is written out. With the fewest frames, the 600 users do not
    /// fit, and both sides are partitioned. Either way, the rows are the
    /// same, padded rows with NULL keys included.
    #[test]
    fn joins_in_memory_when_the_right_side_fits() {
        let left_ids: Vec<_> = (0..300)
            .map(|i| (i % 10 != 0).then_some(i * 3 % 700))
            .collect();
        for db in dbs() {
            db.insert_numbered(600);
            let mut join = hash_join(
                &db,
                JoinType::Left,
                user_ids(&left_ids),
                db.scan(None),
                user_id(),
                column(0),
            );
            join.init().unwrap();
            let fits = db.join_frames() != MIN_FRAMES;
            assert_eq!(join.left_input == LeftInput::Child, fits);
            assert_eq!(join.pages.page_ids().next().is_none(), fits);
            assert_eq!(join.partition_pairs.is_empty(), fits);

            let (mut tuples, mut rids, mut rows) = (Vec::new(), Vec::new(), Vec::new());
            while join.next(&mut tuples, &mut rids, 7).unwrap() {
                rows.extend(decode(&tuples, join.get_output_schema()));
            }
            let expected: Vec<_> = left_ids
                .iter()
                .map(|&id| match id {
                    Some(id @ 1..=600) => {
                        vec![
                            Value::Int(id),
                            Value::Int(id),
                            Value::from(format!("user {id}")),
                        ]
                    }
                    _ => vec![id.map_or(Value::Null, Value::Int), Value::Null, Value::Null],
                })
                .collect();
            assert_eq!(sorted(rows), sorted(expected));
        }
    }

    /// The partitions' pages are freed when the join starts over and when it
    /// is dropped.
    #[test]
    fn frees_the_partition_pages() {
        let mut db = Db::new();
        db.set_join_frames(MIN_FRAMES);
        db.insert_numbered(600);
        let mut join = partitioned(
            &db,
            JoinType::Inner,
            db.scan(None),
            db.scan(None),
            column(0),
            column(0),
        );
        let first: BTreeSet<_> = join.pages.page_ids().collect();
        assert!(!first.is_empty());
        assert!(first.iter().all(|&id| is_intermediate_page(&db, id)));

        join.init().unwrap();
        let second: Vec<_> = join.pages.page_ids().collect();
        assert!(first.iter().all(|&id| !is_intermediate_page(&db, id)));
        assert!(second.iter().all(|&id| is_intermediate_page(&db, id)));

        drop(join);
        assert!(second.iter().all(|&id| !is_intermediate_page(&db, id)));
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

    fn some_orders() -> Arc<AbstractPlanNode> {
        orders(&[
            (Some(1), "pen"),
            (Some(3), "ink"),
            (None, "box"),
            (Some(1), "cap"),
            (Some(9), "mug"),
        ])
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

    /// Runs a hash join of `left` and `right` on `left_key = right_key`.
    fn run_join(
        db: &Db,
        join_type: JoinType,
        left: Arc<AbstractPlanNode>,
        right: Arc<AbstractPlanNode>,
        (left_key, right_key): (AbstractExpr, AbstractExpr),
        predicate: Option<AbstractExpr>,
    ) -> Vec<Vec<Value>> {
        let join = HashJoinPlan::new(
            join_type,
            left,
            right,
            vec![left_key],
            vec![right_key],
            predicate,
        );
        db.run(join.into()).unwrap()
    }

    /// `users.id = orders.user_id`.
    fn on_user_id() -> (AbstractExpr, AbstractExpr) {
        (column(0), user_id())
    }

    #[test]
    fn inner_join_keeps_the_matching_pairs() {
        for db in dbs() {
            db.insert_numbered(4);
            let rows = run_join(
                &db,
                JoinType::Inner,
                db.scan(None),
                some_orders(),
                on_user_id(),
                None,
            );
            assert_eq!(
                sorted(rows),
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
            let rows = run_join(
                &db,
                JoinType::Left,
                db.scan(None),
                some_orders(),
                on_user_id(),
                None,
            );
            assert_eq!(
                sorted(rows),
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

    /// The order without a user has a NULL key, and still comes out of a
    /// LEFT join with orders on the left.
    #[test]
    fn left_rows_with_null_keys_are_padded() {
        for db in dbs() {
            db.insert_numbered(4);
            let rows = run_join(
                &db,
                JoinType::Left,
                some_orders(),
                db.scan(None),
                (user_id(), column(0)),
                None,
            );
            let order = |user_id: Option<i32>, item: &str, user: Option<i32>| {
                let mut row = vec![user_id.map_or(Value::Null, Value::Int), Value::from(item)];
                match user {
                    Some(id) => row.extend([Value::Int(id), Value::from(format!("user {id}"))]),
                    None => row.extend([Value::Null, Value::Null]),
                }
                row
            };
            assert_eq!(
                sorted(rows),
                sorted(vec![
                    order(Some(1), "pen", Some(1)),
                    order(Some(3), "ink", Some(3)),
                    order(None, "box", None),
                    order(Some(1), "cap", Some(1)),
                    order(Some(9), "mug", None),
                ])
            );
        }
    }

    /// The predicate is checked on pairs whose keys match: user 1's cap is
    /// left out, and user 1 is not padded, as the pen matched.
    #[test]
    fn predicate_on_top_of_the_keys() {
        for db in dbs() {
            db.insert_numbered(4);
            let item = ColumnValueExpr::new(1, 1, orders_schema().column(1).unwrap().clone());
            let not_cap = compare(
                ComparisonOp::NotEq,
                item.into(),
                constant(Value::from("cap"), DataType::Varchar(32)),
            );
            let rows = run_join(
                &db,
                JoinType::Left,
                db.scan(None),
                some_orders(),
                on_user_id(),
                Some(not_cap),
            );
            assert_eq!(
                sorted(rows),
                sorted(vec![
                    row(1, "user 1", Some(1), Some("pen")),
                    row(2, "user 2", None, None),
                    row(3, "user 3", Some(3), Some("ink")),
                    row(4, "user 4", None, None),
                ])
            );
        }
    }

    /// An INT key matches a BIGINT key with the same value, as with `=`.
    #[test]
    fn int_keys_match_bigint_keys() {
        for db in dbs() {
            db.insert_numbered(3);
            let schema = Schema::new(vec![Column::new("id", DataType::BigInt, false)]);
            let rows = [1, 3, 7]
                .into_iter()
                .map(|id| vec![constant(Value::BigInt(id), DataType::BigInt)])
                .collect();
            let big_ids = Arc::new(ValuesPlan::new(schema.clone(), rows).into());
            let big_id = ColumnValueExpr::new(0, 0, schema.column(0).unwrap().clone());
            let rows = run_join(
                &db,
                JoinType::Inner,
                db.scan(None),
                big_ids,
                (column(0), big_id.into()),
                None,
            );
            assert_eq!(
                sorted(rows),
                vec![
                    vec![Value::Int(1), Value::from("user 1"), Value::BigInt(1)],
                    vec![Value::Int(3), Value::from("user 3"), Value::BigInt(3)],
                ]
            );
        }
    }

    #[test]
    fn empty_sides() {
        for db in dbs() {
            let rows = run_join(
                &db,
                JoinType::Left,
                db.scan(None),
                some_orders(),
                on_user_id(),
                None,
            );
            assert!(rows.is_empty());

            db.insert_numbered(3);
            let rows = run_join(
                &db,
                JoinType::Inner,
                db.scan(None),
                orders(&[]),
                on_user_id(),
                None,
            );
            assert!(rows.is_empty());
            let rows = run_join(
                &db,
                JoinType::Left,
                db.scan(None),
                orders(&[]),
                on_user_id(),
                None,
            );
            assert_eq!(rows.len(), 3, "every user padded: {rows:?}");
            assert!(
                rows.iter()
                    .all(|row| row[2..] == [Value::Null, Value::Null])
            );
        }
    }

    /// Joins many partitions, split again with the fewest frames:
    /// users.id + 1 = users.id pairs each user with the next.
    #[test]
    fn joins_across_many_partitions() {
        for db in dbs() {
            db.insert_numbered(600);
            let next = ArithmeticExpr::new(ArithmeticOp::Add, column(0), int(1)).unwrap();
            let rows = run_join(
                &db,
                JoinType::Inner,
                db.scan(None),
                db.scan(None),
                (next.into(), column(0)),
                None,
            );
            assert_eq!(rows.len(), 599);
            assert!(rows.iter().all(|row| match (&row[0], &row[2]) {
                (Value::Int(left), Value::Int(right)) => left + 1 == *right,
                pair => panic!("{pair:?}"),
            }));
        }
    }

    /// With every key equal, the right partition is built a chunk at a time
    /// and the left partition read once per chunk. Each user matches itself
    /// in one chunk only, and must not be padded in the others.
    #[test]
    fn equal_keys_are_joined_a_chunk_at_a_time() {
        let mut db = Db::new();
        db.set_join_frames(MIN_FRAMES);
        db.insert_numbered(600);
        let right_id = ColumnValueExpr::new(1, 0, users_schema().column(0).unwrap().clone());
        let same_id = compare(ComparisonOp::Eq, column(0), right_id.into());
        let rows = run_join(
            &db,
            JoinType::Left,
            db.scan(None),
            db.scan(None),
            (int(1), int(1)),
            Some(same_id),
        );
        let expected: Vec<_> = (1..=600)
            .map(|id| {
                let name = format!("user {id}");
                row(id, &name, Some(id), Some(&name))
            })
            .collect();
        assert_eq!(sorted(rows), sorted(expected));

        // User 1 matches every user, in every chunk, and the rest none.
        let rows = run_join(
            &db,
            JoinType::Left,
            db.scan(None),
            db.scan(None),
            (column(0), int(1)),
            None,
        );
        let mut expected: Vec<_> = (1..=600)
            .map(|id| row(1, "user 1", Some(id), Some(&format!("user {id}"))))
            .collect();
        expected.extend((2..=600).map(|id| row(id, &format!("user {id}"), None, None)));
        assert_eq!(sorted(rows), sorted(expected));
    }

    /// Initializing the join again starts it over.
    #[test]
    fn init_starts_over() {
        let mut db = Db::new();
        db.set_join_frames(MIN_FRAMES);
        db.insert_numbered(300);
        let plan = HashJoinPlan::new(
            JoinType::Inner,
            db.scan(None),
            db.scan(None),
            vec![column(0)],
            vec![column(0)],
            None,
        );
        let mut join = create_executor(&db.ctx(), &plan.into()).unwrap();
        let (mut tuples, mut rids) = (Vec::new(), Vec::new());
        for _ in 0..2 {
            join.init().unwrap();
            let mut count = 0;
            while join.next(&mut tuples, &mut rids, 7).unwrap() {
                assert!(!tuples.is_empty() && tuples.len() <= 7);
                count += tuples.len();
            }
            assert_eq!(count, 300);
        }
    }
}
