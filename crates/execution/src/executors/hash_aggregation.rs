use std::cmp::Ordering;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use common::hash::{hash_values, partition_index};
use common::{Column, DataType, INVALID_PAGE_ID, Rid, Schema, Tuple, Value};
use storage::page::PAGE_SIZE;

use super::intermediate_result::{IntermediatePages, IntermediateResult};
use super::{AbstractExecutor, Executor};
use crate::Result;
use crate::execution_context::ExecutionContext;
use crate::expressions::Expr;
use crate::plans::{Aggregate, AggregationPlan, PlanNode};

/// How many rows `init` asks the child for at a time. Each batch is folded
/// into the hash table or written to partitions straight away, so this only
/// bounds the rows in flight.
const CHILD_BATCH_SIZE: usize = 128;

/// A hash aggregation: groups rows in a hash table of
/// `(group-by values -> running values)`, spilling to partitions on disk
/// when the table outgrows memory.
///
/// With `B` aggregation frames ([`ExecutionContext::aggregation_frames`]),
/// the hash table may hold `B - 1` pages of groups, leaving one frame to read
/// input. For each row, the table either has its group, whose running values
/// it updates, or has room for a new group. Once it has neither, every group
/// in it is written out to `B - 1` partitions by a hash of its group-by
/// values, which gives each partition its own frame to write to, and so do
/// all later rows. Rows of the same group land in the same partition, so
/// each partition is then aggregated on its own in the same way, with
/// another hash function. One whose groups still don't fit is partitioned
/// again.
///
/// ```text
/// aggregate(input, seed):
///     for each row of input:
///         if spilled: write the row to partition hash(group, seed)
///         elif the table has the row's group: update its running values
///         elif the table has room: insert the group
///         else:
///             write each group in the table to partition hash(group, seed)
///             spilled = true
///     if spilled: aggregate(partition, seed + 1) for each partition
///     else: output the table's groups
/// ```
///
/// `init` aggregates the child; `next` outputs groups and aggregates the
/// partitions left.
///
/// Partitions hold groups, not the child's rows: a group-by value and a
/// running value per aggregate, which for a single row is that row's
/// contribution (see [`RunningValue::of_row`]). That way the table's groups
/// can be spilled along with the rows still to come, without reading the
/// child again, and aggregating a partition merges running values. It also
/// means that whether a partition fits depends on how many groups it has,
/// not how many pages: a partition of one group fits however many rows it
/// has.
///
/// The table's size is the bytes its groups take encoded as rows, measured
/// when each is inserted. It leaves out the hash table's own overhead and
/// MIN/MAX of a VARCHAR growing.
///
/// Without group-bys all rows form one group, which always fits. It is
/// output even if the child has no rows, with a count of 0 and NULL for the
/// other aggregates.
///
/// [`ExecutionContext::aggregation_frames`]: crate::execution_context::ExecutionContext::aggregation_frames
pub struct HashAggregationExecutor {
    exec_ctx: Arc<ExecutionContext>,
    plan: AggregationPlan,
    child: Box<AbstractExecutor>,
    /// A group written to a partition: the group-by values, then each
    /// aggregate's running value (see [`RunningValue::to_values`]).
    group_schema: Schema,
    /// The groups being aggregated, by the hash of their group-by values.
    table: HashMap<u64, Vec<Group>>,
    /// The bytes the table's groups take; see the type's docs.
    table_bytes: usize,
    /// Partitions not aggregated yet, each with the seed to hash its groups
    /// with.
    pending: Vec<(Partition, u64)>,
    /// The pages of the partitions, each freed once it is read.
    pages: IntermediatePages,
    /// Output rows not returned by `next` yet.
    output: VecDeque<Tuple>,
}

/// Groups whose group-by values hash alike.
type Partition = IntermediateResult;

/// The running values of one group.
struct Group {
    keys: Vec<Value>,
    /// One per aggregate, in order.
    values: Vec<RunningValue>,
}

/// What an aggregate has accumulated so far for a group.
enum RunningValue {
    /// COUNT(*) and COUNT(expr).
    Count(i64),
    /// NULL until there is a non-NULL value.
    Sum(Value),
    /// NULL until there is a non-NULL value.
    Min(Value),
    /// NULL until there is a non-NULL value.
    Max(Value),
    /// The sum and count of the non-NULL values.
    Avg { sum: Value, count: i64 },
}

impl RunningValue {
    /// The running value of a group with no rows.
    fn empty(aggregate: &Aggregate) -> Self {
        match aggregate {
            Aggregate::CountStar | Aggregate::Count(_) => RunningValue::Count(0),
            Aggregate::Sum(_) => RunningValue::Sum(Value::Null),
            Aggregate::Min(_) => RunningValue::Min(Value::Null),
            Aggregate::Max(_) => RunningValue::Max(Value::Null),
            Aggregate::Avg(_) => RunningValue::Avg {
                sum: Value::Null,
                count: 0,
            },
        }
    }

    /// The running value of a group with only `tuple`, a row of `schema`.
    fn of_row(aggregate: &Aggregate, tuple: &Tuple, schema: &Schema) -> Result<Self> {
        let arg = match aggregate.arg() {
            Some(expr) => expr.evaluate(tuple, schema)?,
            None => Value::Null,
        };
        let count = i64::from(!arg.is_null());
        Ok(match aggregate {
            Aggregate::CountStar => RunningValue::Count(1),
            Aggregate::Count(_) => RunningValue::Count(count),
            Aggregate::Sum(_) => RunningValue::Sum(arg.cast_number(sum_type(aggregate))?),
            Aggregate::Min(_) => RunningValue::Min(arg),
            Aggregate::Max(_) => RunningValue::Max(arg),
            Aggregate::Avg(_) => RunningValue::Avg {
                sum: arg.cast_number(sum_type(aggregate))?,
                count,
            },
        })
    }

    /// Adds `other`'s rows, of the same group and aggregate, to this.
    fn merge(&mut self, other: RunningValue) -> Result<()> {
        match (self, other) {
            (RunningValue::Count(count), RunningValue::Count(other)) => *count += other,
            (RunningValue::Sum(sum), RunningValue::Sum(other)) => *sum = add(sum, &other)?,
            (RunningValue::Min(min), RunningValue::Min(other)) => {
                if replaces(&other, min, Ordering::Less)? {
                    *min = other;
                }
            }
            (RunningValue::Max(max), RunningValue::Max(other)) => {
                if replaces(&other, max, Ordering::Greater)? {
                    *max = other;
                }
            }
            (
                RunningValue::Avg { sum, count },
                RunningValue::Avg {
                    sum: other_sum,
                    count: other_count,
                },
            ) => {
                *sum = add(sum, &other_sum)?;
                *count += other_count;
            }
            _ => unreachable!("a group's running values line up with the aggregates"),
        }
        Ok(())
    }

    /// The aggregate's result.
    fn finish(self) -> Result<Value> {
        Ok(match self {
            RunningValue::Count(count) => Value::BigInt(count),
            RunningValue::Sum(value) | RunningValue::Min(value) | RunningValue::Max(value) => value,
            RunningValue::Avg { count: 0, .. } => Value::Null,
            RunningValue::Avg { sum, count } => sum
                .cast_number(DataType::Double)?
                .checked_div(&Value::BigInt(count))?,
        })
    }

    /// The columns this running value is written to a partition as.
    fn columns(aggregate: &Aggregate) -> Vec<Column> {
        let count = Column::new("count", DataType::BigInt, false);
        match aggregate {
            Aggregate::CountStar | Aggregate::Count(_) => vec![count],
            Aggregate::Sum(_) | Aggregate::Min(_) | Aggregate::Max(_) => {
                vec![aggregate.return_type()]
            }
            Aggregate::Avg(_) => vec![Column::new("sum", sum_type(aggregate), true), count],
        }
    }

    /// The values of [`columns`](Self::columns).
    fn to_values(&self) -> Vec<Value> {
        match self {
            RunningValue::Count(count) => vec![Value::BigInt(*count)],
            RunningValue::Sum(value) | RunningValue::Min(value) | RunningValue::Max(value) => {
                vec![value.clone()]
            }
            RunningValue::Avg { sum, count } => vec![sum.clone(), Value::BigInt(*count)],
        }
    }

    /// Takes the running value of `aggregate` off the front of `values`, as
    /// written by [`to_values`](Self::to_values).
    fn from_values(aggregate: &Aggregate, values: &mut impl Iterator<Item = Value>) -> Self {
        let mut next = || values.next().expect("a group row has every column");
        let count = |value| match value {
            Value::BigInt(count) => count,
            value => unreachable!("a count is a BIGINT, not {value:?}"),
        };
        match aggregate {
            Aggregate::CountStar | Aggregate::Count(_) => RunningValue::Count(count(next())),
            Aggregate::Sum(_) => RunningValue::Sum(next()),
            Aggregate::Min(_) => RunningValue::Min(next()),
            Aggregate::Max(_) => RunningValue::Max(next()),
            Aggregate::Avg(_) => RunningValue::Avg {
                sum: next(),
                count: count(next()),
            },
        }
    }
}

/// The type SUM and AVG add their values up in: DOUBLE for doubles and
/// BIGINT for integers.
fn sum_type(aggregate: &Aggregate) -> DataType {
    match aggregate.arg().map(|arg| arg.return_type().data_type) {
        Some(DataType::Double) => DataType::Double,
        _ => DataType::BigInt,
    }
}

/// `sum + value`, where a NULL on either side adds nothing.
fn add(sum: &Value, value: &Value) -> Result<Value> {
    Ok(match (sum.is_null(), value.is_null()) {
        (_, true) => sum.clone(),
        (true, false) => value.clone(),
        (false, false) => sum.checked_add(value)?,
    })
}

/// Whether `candidate` should replace `current` as a MIN (`Less`) or MAX
/// (`Greater`). NULLs are skipped.
fn replaces(candidate: &Value, current: &Value, wins: Ordering) -> Result<bool> {
    if candidate.is_null() {
        return Ok(false);
    }
    Ok(current.is_null() || candidate.compare(current)? == Some(wins))
}

/// Whether two rows have the same group-by values. Unlike `=`, NULL
/// matches NULL: GROUP BY puts all NULLs in one group.
fn same_group(left: &[Value], right: &[Value]) -> Result<bool> {
    for (l, r) in left.iter().zip(right) {
        let equal = match (l.is_null(), r.is_null()) {
            (true, true) => true,
            (false, false) => l.compare(r)? == Some(Ordering::Equal),
            _ => false,
        };
        if !equal {
            return Ok(false);
        }
    }
    Ok(true)
}

impl HashAggregationExecutor {
    pub fn new(
        ctx: Arc<ExecutionContext>,
        plan: AggregationPlan,
        child: Box<AbstractExecutor>,
    ) -> Self {
        let group_columns = plan
            .group_bys()
            .iter()
            .map(|expr| expr.return_type().clone());
        let value_columns = plan.aggregates().iter().flat_map(RunningValue::columns);
        let group_schema = Schema::new(group_columns.chain(value_columns).collect());
        Self {
            pages: IntermediatePages::new(Arc::clone(&ctx)),
            exec_ctx: ctx,
            plan,
            child,
            group_schema,
            table: HashMap::new(),
            table_bytes: 0,
            pending: Vec::new(),
            output: VecDeque::new(),
        }
    }

    /// How many partitions a spill splits groups into: `B - 1`, one output
    /// page each and one frame for the input.
    fn fan_out(&self) -> usize {
        self.exec_ctx.aggregation_frames() - 1
    }

    /// How many bytes of groups the hash table may hold: `B - 1` pages.
    fn table_capacity(&self) -> usize {
        (self.exec_ctx.aggregation_frames() - 1) * PAGE_SIZE
    }

    /// The group of `tuple`, a row of the child, holding just that row.
    fn group_of_row(&self, tuple: &Tuple) -> Result<Group> {
        let schema = self.child.get_output_schema();
        let keys = self
            .plan
            .group_bys()
            .iter()
            .map(|expr| expr.evaluate(tuple, schema))
            .collect::<Result<_>>()?;
        let values = self
            .plan
            .aggregates()
            .iter()
            .map(|aggregate| RunningValue::of_row(aggregate, tuple, schema))
            .collect::<Result<_>>()?;
        Ok(Group { keys, values })
    }

    /// `group` as a row of `group_schema`.
    fn encode_group(&self, group: &Group) -> Result<Tuple> {
        let mut values = group.keys.clone();
        values.extend(group.values.iter().flat_map(RunningValue::to_values));
        Ok(Tuple::encode(&self.group_schema, &values)?)
    }

    /// The group in `tuple`, a row of `group_schema`.
    fn decode_group(&self, tuple: &Tuple) -> Result<Group> {
        let mut values = tuple.decode(&self.group_schema)?.into_iter();
        let keys = values.by_ref().take(self.plan.group_bys().len()).collect();
        let values = self
            .plan
            .aggregates()
            .iter()
            .map(|aggregate| RunningValue::from_values(aggregate, &mut values))
            .collect();
        Ok(Group { keys, values })
    }

    /// Reads the child to the end, aggregating its rows with seed 0.
    fn aggregate_child(&mut self) -> Result<()> {
        let mut spilled = None;
        let (mut tuples, mut rids) = (Vec::new(), Vec::new());
        while self.child.next(&mut tuples, &mut rids, CHILD_BATCH_SIZE)? {
            for tuple in &tuples {
                let group = self.group_of_row(tuple)?;
                self.add_group(group, 0, &mut spilled)?;
            }
        }
        self.finish_input(spilled, 0)
    }

    /// Reads `partition` to the end, freeing its pages, and aggregates its
    /// groups with `seed`.
    fn aggregate_partition(&mut self, partition: Partition, seed: u64) -> Result<()> {
        let mut spilled = None;
        let mut page_id = partition.first_page_id;
        while page_id != INVALID_PAGE_ID {
            let (tuples, next_page_id) = self.pages.read_page(page_id)?;
            self.pages.free_page(page_id)?;
            for tuple in &tuples {
                let group = self.decode_group(tuple)?;
                self.add_group(group, seed, &mut spilled)?;
            }
            page_id = next_page_id;
        }
        self.finish_input(spilled, seed)
    }

    /// Adds `group` to the hash table, merging it into the table's group
    /// with the same group-by values if there is one. When the table is full,
    /// spills it to partitions by a hash of the group-by values with `seed`,
    /// setting `spilled`; after that, groups go straight to the partitions.
    fn add_group(
        &mut self,
        group: Group,
        seed: u64,
        spilled: &mut Option<Vec<Partition>>,
    ) -> Result<()> {
        if let Some(partitions) = spilled {
            return self.write_to_partition(partitions, &group, seed);
        }

        let hash = hash_values(&group.keys, seed);
        if let Some(bucket) = self.table.get_mut(&hash) {
            for existing in bucket.iter_mut() {
                if same_group(&existing.keys, &group.keys)? {
                    for (value, other) in existing.values.iter_mut().zip(group.values) {
                        value.merge(other)?;
                    }
                    return Ok(());
                }
            }
        }

        let size = self.encode_group(&group)?.data().len();
        if self.table_bytes + size <= self.table_capacity() {
            self.table.entry(hash).or_default().push(group);
            self.table_bytes += size;
            return Ok(());
        }
        let mut partitions = self.spill_table(seed)?;
        self.write_to_partition(&mut partitions, &group, seed)?;
        *spilled = Some(partitions);
        Ok(())
    }

    /// Empties the hash table into [`fan_out`](Self::fan_out) new partitions
    /// by a hash of each group's group-by values with `seed`.
    fn spill_table(&mut self, seed: u64) -> Result<Vec<Partition>> {
        let mut partitions: Vec<_> = (0..self.fan_out()).map(|_| Partition::new()).collect();
        for group in std::mem::take(&mut self.table).into_values().flatten() {
            self.write_to_partition(&mut partitions, &group, seed)?;
        }
        self.table_bytes = 0;
        Ok(partitions)
    }

    fn write_to_partition(
        &mut self,
        partitions: &mut [Partition],
        group: &Group,
        seed: u64,
    ) -> Result<()> {
        let i = partition_index(hash_values(&group.keys, seed), partitions.len());
        let tuple = self.encode_group(group)?;
        self.pages.append(&mut partitions[i], &tuple)
    }

    /// Done with an input aggregated with `seed`: outputs the hash table's
    /// groups, or if it spilled, leaves its partitions for `next` to
    /// aggregate with the next seed.
    fn finish_input(&mut self, spilled: Option<Vec<Partition>>, seed: u64) -> Result<()> {
        match spilled {
            None => self.output_table(),
            Some(partitions) => {
                let partitions = partitions.into_iter().filter(|p| p.tuple_count > 0);
                self.pending.extend(partitions.map(|p| (p, seed + 1)));
                Ok(())
            }
        }
    }

    /// Moves the hash table's groups to `output` as finished rows.
    fn output_table(&mut self) -> Result<()> {
        for group in std::mem::take(&mut self.table).into_values().flatten() {
            let row = self.output_row(group)?;
            self.output.push_back(row);
        }
        self.table_bytes = 0;
        Ok(())
    }

    /// The output row of `group`: its group-by values, then each aggregate's
    /// result.
    fn output_row(&self, group: Group) -> Result<Tuple> {
        let mut values = group.keys;
        for value in group.values {
            values.push(value.finish()?);
        }
        Ok(Tuple::encode(self.plan.get_output_schema(), &values)?)
    }
}

impl Executor for HashAggregationExecutor {
    /// Aggregates the child, leaving `next` the groups to output and the
    /// partitions it spilled.
    fn init(&mut self) -> Result<()> {
        self.child.init()?;
        self.table.clear();
        self.table_bytes = 0;
        self.pending.clear();
        self.pages.free_all()?;
        self.output.clear();

        self.aggregate_child()?;
        if self.plan.group_bys().is_empty() && self.output.is_empty() {
            // No rows: the one group is still output.
            let values = self.plan.aggregates().iter().map(RunningValue::empty);
            let group = Group {
                keys: Vec::new(),
                values: values.collect(),
            };
            let row = self.output_row(group)?;
            self.output.push_back(row);
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
            } else if let Some((partition, seed)) = self.pending.pop() {
                self.aggregate_partition(partition, seed)?;
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
    use std::collections::BTreeSet;
    use std::fmt::Debug;
    use std::sync::Arc;

    use common::{Column, DataType, Schema, Value, ValueError};

    use super::super::test_util::{Db, constant, is_intermediate_page};
    use super::super::{Executor, create_executor};
    use super::HashAggregationExecutor;
    use crate::ExecutionError;
    use crate::expressions::{AbstractExpr, ColumnValueExpr};
    use crate::plans::{AbstractPlanNode, Aggregate, AggregationPlan, ValuesPlan};

    /// The fewest frames an aggregation can have: a table of 2 pages, and 2
    /// partitions per spill.
    const MIN_FRAMES: usize = 3;

    /// A database whose aggregations get the default frames, and one whose
    /// aggregations get the fewest.
    fn dbs() -> [Db; 2] {
        let mut small = Db::new();
        small.set_aggregation_frames(MIN_FRAMES);
        [Db::new(), small]
    }

    /// `(k INT, v INT, d DOUBLE)`, all nullable.
    fn schema() -> Schema {
        Schema::new(vec![
            Column::new("k", DataType::Int, true),
            Column::new("v", DataType::Int, true),
            Column::new("d", DataType::Double, true),
        ])
    }

    /// Column `idx` of [`schema`].
    fn col(idx: usize) -> AbstractExpr {
        ColumnValueExpr::new(0, idx, schema().column(idx).unwrap().clone()).into()
    }

    /// Rows of [`schema`] as a VALUES list.
    fn rows(rows: Vec<[Value; 3]>) -> Arc<AbstractPlanNode> {
        let schema = schema();
        let rows = rows
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .zip(schema.columns())
                    .map(|(value, column)| constant(value, column.data_type))
                    .collect()
            })
            .collect();
        Arc::new(ValuesPlan::new(schema, rows).into())
    }

    /// Rows `(k, v, NULL)`.
    fn ints(pairs: impl IntoIterator<Item = (Option<i32>, Option<i32>)>) -> Arc<AbstractPlanNode> {
        let value = |v: Option<i32>| v.map_or(Value::Null, Value::Int);
        rows(
            pairs
                .into_iter()
                .map(|(k, v)| [value(k), value(v), Value::Null])
                .collect(),
        )
    }

    fn plan(
        group_bys: Vec<AbstractExpr>,
        aggregates: Vec<Aggregate>,
        child: Arc<AbstractPlanNode>,
    ) -> AggregationPlan {
        AggregationPlan::new(group_bys, aggregates, child)
    }

    /// The executor for `plan`, initialized.
    fn aggregation(db: &Db, plan: AggregationPlan) -> HashAggregationExecutor {
        let child = create_executor(&db.ctx, plan.child()).unwrap();
        let mut aggregation =
            HashAggregationExecutor::new(Arc::clone(&db.ctx), plan, Box::new(child));
        aggregation.init().unwrap();
        aggregation
    }

    /// The order of the groups depends on the hash table, so tests compare
    /// them sorted.
    fn sorted<T: Debug>(mut values: Vec<T>) -> Vec<T> {
        values.sort_by_key(|value| format!("{value:?}"));
        values
    }

    fn run(db: &Db, plan: AggregationPlan) -> Vec<Vec<Value>> {
        sorted(db.run(plan.into()).unwrap())
    }

    fn big(v: i64) -> Value {
        Value::BigInt(v)
    }

    #[test]
    fn computes_each_aggregate_per_group() {
        let input = || {
            rows(vec![
                [1.into(), 10.into(), 1.5.into()],
                [1.into(), Value::Null, Value::Null],
                [2.into(), 5.into(), 2.0.into()],
                [Value::Null, 7.into(), Value::Null],
                [Value::Null, 3.into(), 0.5.into()],
                [1.into(), 20.into(), Value::Null],
            ])
        };
        let aggregates = || {
            vec![
                Aggregate::CountStar,
                Aggregate::Count(col(1)),
                Aggregate::Sum(col(1)),
                Aggregate::Min(col(1)),
                Aggregate::Max(col(1)),
                Aggregate::Avg(col(1)),
                Aggregate::Sum(col(2)),
                Aggregate::Avg(col(2)),
            ]
        };
        // All NULL keys are one group.
        let expected = sorted(vec![
            vec![
                1.into(),
                big(3),
                big(2),
                big(30),
                10.into(),
                20.into(),
                15.0.into(),
                1.5.into(),
                1.5.into(),
            ],
            vec![
                2.into(),
                big(1),
                big(1),
                big(5),
                5.into(),
                5.into(),
                5.0.into(),
                2.0.into(),
                2.0.into(),
            ],
            vec![
                Value::Null,
                big(2),
                big(2),
                big(10),
                3.into(),
                7.into(),
                5.0.into(),
                0.5.into(),
                0.5.into(),
            ],
        ]);
        for db in dbs() {
            assert_eq!(
                run(&db, plan(vec![col(0)], aggregates(), input())),
                expected
            );
        }
    }

    #[test]
    fn a_group_of_only_nulls_has_null_aggregates_but_counts() {
        let db = Db::new();
        let aggregates = vec![
            Aggregate::CountStar,
            Aggregate::Count(col(1)),
            Aggregate::Sum(col(1)),
            Aggregate::Min(col(1)),
            Aggregate::Avg(col(1)),
        ];
        let input = ints([(Some(1), None), (Some(1), None)]);
        assert_eq!(
            run(&db, plan(vec![col(0)], aggregates, input)),
            [vec![
                1.into(),
                big(2),
                big(0),
                Value::Null,
                Value::Null,
                Value::Null
            ]]
        );
    }

    #[test]
    fn avg_is_a_double() {
        let db = Db::new();
        let input = ints([(Some(1), Some(1)), (Some(1), Some(2))]);
        assert_eq!(
            run(&db, plan(vec![], vec![Aggregate::Avg(col(1))], input)),
            [vec![1.5.into()]]
        );
    }

    #[test]
    fn without_group_bys_outputs_one_row_even_for_no_rows() {
        let aggregates = || {
            vec![
                Aggregate::CountStar,
                Aggregate::Count(col(1)),
                Aggregate::Sum(col(1)),
                Aggregate::Max(col(1)),
                Aggregate::Avg(col(1)),
            ]
        };
        for db in dbs() {
            assert_eq!(
                run(&db, plan(vec![], aggregates(), ints([]))),
                [vec![big(0), big(0), Value::Null, Value::Null, Value::Null]]
            );
            let input = ints([(Some(1), Some(4)), (Some(2), None), (None, Some(6))]);
            assert_eq!(
                run(&db, plan(vec![], aggregates(), input)),
                [vec![big(3), big(2), big(10), 6.into(), 5.0.into()]]
            );
        }
    }

    #[test]
    fn with_group_bys_outputs_nothing_for_no_rows() {
        let db = Db::new();
        let plan = plan(vec![col(0)], vec![Aggregate::CountStar], ints([]));
        assert!(run(&db, plan).is_empty());
    }

    #[test]
    fn groups_by_several_columns() {
        let db = Db::new();
        let input = rows(vec![
            [1.into(), 1.into(), 0.0.into()],
            [1.into(), 1.into(), (-0.0).into()],
            [1.into(), 2.into(), 0.0.into()],
            [1.into(), 1.into(), 2.5.into()],
            [1.into(), Value::Null, 2.5.into()],
            [1.into(), Value::Null, 2.5.into()],
        ]);
        let rows = run(
            &db,
            plan(vec![col(0), col(2)], vec![Aggregate::CountStar], input),
        );
        // -0 = 0, so they are one group, shown as the first row's value.
        assert_eq!(
            rows,
            sorted(vec![
                vec![1.into(), 0.0.into(), big(3)],
                vec![1.into(), 2.5.into(), big(3)],
            ])
        );
    }

    #[test]
    fn sum_overflow_fails() {
        let db = Db::new();
        let max = constant(big(i64::MAX), DataType::BigInt);
        let input = ints([(Some(1), None), (Some(1), None)]);
        let plan = plan(vec![col(0)], vec![Aggregate::Sum(max)], input);
        assert!(matches!(
            db.run(plan.into()),
            Err(ExecutionError::Value(ValueError::Overflow))
        ));
    }

    /// `n` groups `0..n`, each with the rows `(k, k)` and `(k, 1)`.
    fn numbered_groups(n: i32) -> Arc<AbstractPlanNode> {
        ints((0..n).flat_map(|k| [(Some(k), Some(k)), (Some(k), Some(1))]))
    }

    fn count_and_sum(child: Arc<AbstractPlanNode>) -> AggregationPlan {
        plan(
            vec![col(0)],
            vec![Aggregate::CountStar, Aggregate::Sum(col(1))],
            child,
        )
    }

    #[test]
    fn spills_groups_that_do_not_fit() {
        let n = 2000;
        let expected = sorted(
            (0..n)
                .map(|k| vec![k.into(), big(2), big(i64::from(k) + 1)])
                .collect(),
        );
        let [db, small] = dbs();

        let in_memory = aggregation(&db, count_and_sum(numbered_groups(n)));
        assert!(in_memory.pending.is_empty());
        assert_eq!(in_memory.output.len(), n as usize);
        assert_eq!(run(&db, count_and_sum(numbered_groups(n))), expected);

        // 2 pages hold a few hundred groups, so the child's groups are split
        // into 2 partitions, which are each still too big and are split
        // again.
        let mut spilled = aggregation(&small, count_and_sum(numbered_groups(n)));
        assert!(spilled.output.is_empty());
        assert_eq!(
            spilled
                .pending
                .iter()
                .map(|(_, seed)| *seed)
                .collect::<Vec<_>>(),
            [1, 1]
        );
        let (partition, seed) = spilled.pending.pop().unwrap();
        spilled.aggregate_partition(partition, seed).unwrap();
        assert!(spilled.output.is_empty());
        assert_eq!(
            spilled
                .pending
                .iter()
                .map(|(_, seed)| *seed)
                .collect::<Vec<_>>(),
            [1, 2, 2]
        );
        assert_eq!(run(&small, count_and_sum(numbered_groups(n))), expected);
    }

    #[test]
    fn few_groups_fit_however_many_rows() {
        let [_, small] = dbs();
        let input = ints((0..3000).map(|i| (Some(i % 3), Some(1))));
        let aggregation = aggregation(&small, count_and_sum(input));
        assert!(aggregation.pending.is_empty());
        assert_eq!(aggregation.pages.page_ids().count(), 0);
        assert_eq!(aggregation.output.len(), 3);
    }

    #[test]
    fn frees_its_pages() {
        let [_, db] = dbs();
        let mut aggregation = aggregation(&db, count_and_sum(numbered_groups(2000)));
        let first: BTreeSet<_> = aggregation.pages.page_ids().collect();
        assert!(!first.is_empty());
        assert!(first.iter().all(|&id| is_intermediate_page(&db, id)));

        // Initializing again frees the partitions not read yet.
        aggregation.init().unwrap();
        let second: BTreeSet<_> = aggregation.pages.page_ids().collect();
        assert!(
            first
                .difference(&second)
                .all(|&id| !is_intermediate_page(&db, id))
        );
        assert!(second.iter().all(|&id| is_intermediate_page(&db, id)));

        // Each page is freed once it is read.
        let (mut tuples, mut rids, mut rows) = (Vec::new(), Vec::new(), 0);
        while aggregation.next(&mut tuples, &mut rids, 64).unwrap() {
            rows += tuples.len();
        }
        assert_eq!(rows, 2000);
        assert_eq!(aggregation.pages.page_ids().count(), 0);
        assert!(second.iter().all(|&id| !is_intermediate_page(&db, id)));
    }

    #[test]
    fn frees_unread_pages_when_dropped() {
        let [_, db] = dbs();
        let aggregation = aggregation(&db, count_and_sum(numbered_groups(2000)));
        let pages: Vec<_> = aggregation.pages.page_ids().collect();
        assert!(!pages.is_empty());
        drop(aggregation);
        assert!(pages.iter().all(|&id| !is_intermediate_page(&db, id)));
    }
}
