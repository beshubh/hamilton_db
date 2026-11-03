//! Executors: the operators that run a plan tree.
//!
//! [`create_executor`] builds one executor per plan node, each owning the
//! executors of its children. An executor is built from the statement's
//! [`ExecutionContext`], its plan and its children's executors, and finds
//! anything else it needs, such as the tables and indexes its plan names,
//! through the context when it runs. Like plans and expressions, every
//! executor is a variant of [`AbstractExecutor`], and `enum_dispatch`
//! forwards [`Executor`] calls to the variant. Each executor lives in its own
//! module under `executors/`.
//!
//! Rows are pulled from the root in batches: each call to [`Executor::next`]
//! pulls as many batches from the children as it needs to produce one of its
//! own.
//!
//! So far there are executors for CREATE TABLE, CREATE INDEX, INSERT ...
//! VALUES, UPDATE, DELETE, SELECT ... WHERE through a table or an index
//! scan, ORDER BY, LIMIT, nested loop, index nested loop and hash joins, and
//! hash aggregation for GROUP BY and aggregates; other plans fail with
//! [`ExecutionError::NoExecutor`].
//!
//! Executors read and write tables and indexes through the statement's
//! transaction ([`ExecutionContext::txn`]). They never commit or abort it:
//! whoever runs the statement does, so a statement that fails is undone by
//! the abort that follows.

mod create_index;
mod create_table;
mod delete;
mod explain;
mod filter;
mod hash_aggregation;
mod hash_join;
mod index_scan;
mod insert;
mod intermediate_result;
mod limit;
mod nested_index_join;
mod nested_loop_join;
mod projection;
mod seq_scan;
mod sort;
#[cfg(test)]
#[path = "../tests/util/executors.rs"]
pub(crate) mod test_util;
mod truncate;
mod update;
mod values;

pub use create_index::CreateIndexExecutor;
pub use create_table::CreateTableExecutor;
pub use delete::DeleteExecutor;
pub use explain::ExplainExecutor;
pub use filter::FilterExecutor;
pub use hash_aggregation::HashAggregationExecutor;
pub use hash_join::HashJoinExecutor;
pub use index_scan::IndexScanExecutor;
pub use insert::InsertExecutor;
pub use limit::LimitExecutor;
pub use nested_index_join::NestedIndexJoinExecutor;
pub use nested_loop_join::NestedLoopJoinExecutor;
pub use projection::ProjectionExecutor;
pub use seq_scan::SeqScanExecutor;
pub use sort::SortExecutor;
pub use truncate::TruncateExecutor;
pub use update::UpdateExecutor;
pub use values::ValuesExecutor;

use std::sync::Arc;

use catalog::{Catalog, IndexInfo, LockMode, TableInfo, TableWriteGuard};
use common::{ColumnReader, Rid, Schema, Tuple, Value};
use enum_dispatch::enum_dispatch;
use storage::concurrency::transaction::Transaction;
use storage::index::{IndexMetadata, LocatorKind, RowLocator};

use crate::error::{ExecutionError, Result};
use crate::execution_context::ExecutionContext;
use crate::expressions::{AbstractExpr, ComparisonOp, Expr, LogicOp};
use crate::plans::{AbstractPlanNode, PlanNode};

#[enum_dispatch]
pub trait Executor {
    /// Prepares the executor, and its children, to produce their rows from
    /// the start. Must be called before the first [`next`](Self::next).
    fn init(&mut self) -> Result<()>;

    /// Replaces the contents of `tuples` with the next rows, at most
    /// `batch_size` of them, and of `rids` with where those rows are stored,
    /// for rows read from a table. Returns `false`, leaving both empty, once
    /// there are no more rows.
    ///
    /// The tuples already in `tuples` may be written over to reuse their
    /// buffers, so a caller that keeps the vector between calls saves an
    /// allocation per row.
    fn next(
        &mut self,
        tuples: &mut Vec<Tuple>,
        rids: &mut Vec<Rid>,
        batch_size: usize,
    ) -> Result<bool>;

    /// The layout of the tuples [`next`](Self::next) produces.
    fn get_output_schema(&self) -> &Schema;
}

#[enum_dispatch(Executor)]
pub enum AbstractExecutor {
    CreateTableExecutor,
    CreateIndexExecutor,
    SeqScanExecutor,
    IndexScanExecutor,
    ValuesExecutor,
    InsertExecutor,
    UpdateExecutor,
    DeleteExecutor,
    TruncateExecutor,
    FilterExecutor,
    ProjectionExecutor,
    SortExecutor,
    LimitExecutor,
    NestedLoopJoinExecutor,
    NestedIndexJoinExecutor,
    HashJoinExecutor,
    HashAggregationExecutor,
    ExplainExecutor,
}

/// The executor for `plan` and, below it, for each of its children.
pub fn create_executor(
    ctx: &Arc<ExecutionContext>,
    plan: &AbstractPlanNode,
) -> Result<AbstractExecutor> {
    let child = |plan: &Arc<AbstractPlanNode>| create_executor(ctx, plan).map(Box::new);
    let executor = match plan {
        AbstractPlanNode::CreateTablePlan(plan) => {
            CreateTableExecutor::new(Arc::clone(ctx), plan.clone()).into()
        }
        AbstractPlanNode::CreateIndexPlan(plan) => {
            CreateIndexExecutor::new(Arc::clone(ctx), plan.clone()).into()
        }
        AbstractPlanNode::SeqScanPlan(plan) => {
            SeqScanExecutor::new(Arc::clone(ctx), plan.clone()).into()
        }
        AbstractPlanNode::IndexScanPlan(plan) => {
            IndexScanExecutor::new(Arc::clone(ctx), plan.clone()).into()
        }
        AbstractPlanNode::ValuesPlan(plan) => {
            ValuesExecutor::new(Arc::clone(ctx), plan.clone()).into()
        }
        AbstractPlanNode::InsertPlan(plan) => {
            InsertExecutor::new(Arc::clone(ctx), plan.clone(), child(plan.child())?).into()
        }
        AbstractPlanNode::UpdatePlan(plan) => {
            UpdateExecutor::new(Arc::clone(ctx), plan.clone(), child(plan.child())?).into()
        }
        AbstractPlanNode::DeletePlan(plan) => {
            DeleteExecutor::new(Arc::clone(ctx), plan.clone(), child(plan.child())?).into()
        }
        AbstractPlanNode::TruncatePlan(plan) => {
            TruncateExecutor::new(Arc::clone(ctx), plan.clone()).into()
        }
        AbstractPlanNode::FilterPlan(plan) => {
            FilterExecutor::new(Arc::clone(ctx), plan.clone(), child(plan.child())?).into()
        }
        AbstractPlanNode::ProjectionPlan(plan) => {
            ProjectionExecutor::new(Arc::clone(ctx), plan.clone(), child(plan.child())?).into()
        }
        AbstractPlanNode::SortPlan(plan) => {
            SortExecutor::new(Arc::clone(ctx), plan.clone(), child(plan.child())?).into()
        }
        AbstractPlanNode::LimitPlan(plan) => {
            LimitExecutor::new(Arc::clone(ctx), plan.clone(), child(plan.child())?).into()
        }
        AbstractPlanNode::NestedLoopJoinPlan(plan) => {
            let (left, right) = (child(plan.left())?, child(plan.right())?);
            NestedLoopJoinExecutor::new(Arc::clone(ctx), plan.clone(), left, right).into()
        }
        AbstractPlanNode::NestedIndexJoinPlan(plan) => {
            NestedIndexJoinExecutor::new(Arc::clone(ctx), plan.clone(), child(plan.outer())?).into()
        }
        AbstractPlanNode::HashJoinPlan(plan) => {
            let (left, right) = (child(plan.left())?, child(plan.right())?);
            HashJoinExecutor::new(Arc::clone(ctx), plan.clone(), left, right).into()
        }
        AbstractPlanNode::AggregationPlan(plan) => {
            HashAggregationExecutor::new(Arc::clone(ctx), plan.clone(), child(plan.child())?).into()
        }
        AbstractPlanNode::ExplainPlan(plan) => {
            ExplainExecutor::new(Arc::clone(ctx), plan.clone()).into()
        }
        AbstractPlanNode::WindowPlan(_) => return Err(ExecutionError::NoExecutor("Window")),
        AbstractPlanNode::TopNPlan(_) => return Err(ExecutionError::NoExecutor("TopN")),
        AbstractPlanNode::TopNPerGroupPlan(_) => {
            return Err(ExecutionError::NoExecutor("TopNPerGroup"));
        }
    };
    Ok(executor)
}

/// Locks the tables `plan` uses for `txn`, before any of its executors
/// run, so that an INSERT that can't get its lock takes no identity value.
/// EXPLAIN runs nothing, so locks nothing.
pub fn lock_tables(catalog: &Catalog, txn: &Transaction, plan: &AbstractPlanNode) -> Result<()> {
    let lock = |oid, mode| catalog.lock_table(txn, oid, mode);
    match plan {
        AbstractPlanNode::ExplainPlan(_) | AbstractPlanNode::CreateTablePlan(_) => return Ok(()),
        AbstractPlanNode::SeqScanPlan(plan) => lock(plan.table_oid(), LockMode::AccessShare)?,
        AbstractPlanNode::IndexScanPlan(plan) => lock(plan.table_oid(), LockMode::AccessShare)?,
        AbstractPlanNode::NestedIndexJoinPlan(plan) => {
            lock(plan.inner_table_oid(), LockMode::AccessShare)?;
        }
        AbstractPlanNode::InsertPlan(plan) => lock(plan.table_oid(), LockMode::RowExclusive)?,
        AbstractPlanNode::UpdatePlan(plan) => lock(plan.table_oid(), LockMode::RowExclusive)?,
        AbstractPlanNode::DeletePlan(plan) => lock(plan.table_oid(), LockMode::RowExclusive)?,
        AbstractPlanNode::CreateIndexPlan(plan) => {
            if let Some(table) = catalog.get_table(plan.table_name()) {
                lock(table.oid, LockMode::RowExclusive)?;
            }
        }
        AbstractPlanNode::TruncatePlan(plan) => {
            for &oid in plan.tables() {
                lock(oid, LockMode::AccessExclusive)?;
            }
        }
        // These read only their children's rows.
        AbstractPlanNode::ValuesPlan(_)
        | AbstractPlanNode::FilterPlan(_)
        | AbstractPlanNode::ProjectionPlan(_)
        | AbstractPlanNode::NestedLoopJoinPlan(_)
        | AbstractPlanNode::HashJoinPlan(_)
        | AbstractPlanNode::AggregationPlan(_)
        | AbstractPlanNode::WindowPlan(_)
        | AbstractPlanNode::SortPlan(_)
        | AbstractPlanNode::LimitPlan(_)
        | AbstractPlanNode::TopNPlan(_)
        | AbstractPlanNode::TopNPerGroupPlan(_) => {}
    }
    plan.get_children()
        .iter()
        .try_for_each(|child| lock_tables(catalog, txn, child))
}

/// The indexes of `table` that INSERT, UPDATE and DELETE write, primary key
/// first: a row's primary key entry must go in before its other entries
/// (see `storage::concurrency::index_versions`). Returned with the table's
/// write lock (`TableInfo::lock_writes`), which the statement holds until
/// its last write so that no index is created in between.
fn write_indexes<'a>(
    ctx: &ExecutionContext,
    table: &'a TableInfo,
) -> (TableWriteGuard<'a>, Vec<Arc<IndexInfo>>) {
    let guard = table.lock_writes();
    let mut indexes = ctx.catalog().table_indexes(&table.name);
    indexes.sort_by_key(|index| !index.kind.is_primary_key());
    (guard, indexes)
}

/// The key of `tuple`, a row of `table`, in each of `indexes`.
fn index_keys(table: &TableInfo, indexes: &[Arc<IndexInfo>], tuple: &Tuple) -> Result<Vec<Tuple>> {
    indexes
        .iter()
        .map(|index| Ok(tuple.to_key(&table.schema, &index.key_schema, &index.key_columns)?))
        .collect()
}

/// The `RowLocator` each of `indexes` (all of a table's indexes) stores for a
/// row, given the row's key in each index and its rid.
fn locators(indexes: &[Arc<IndexInfo>], keys: &[Tuple], rid: Rid) -> Vec<RowLocator> {
    let primary_key = indexes.iter().position(|index| index.kind.is_primary_key());
    indexes
        .iter()
        .map(|index| match index.locator {
            LocatorKind::Rid => RowLocator::Rid(rid),
            LocatorKind::PrimaryKey => {
                let pk =
                    primary_key.expect("only a table with a primary key has indexes that store it");
                RowLocator::PrimaryKey(keys[pk].clone())
            }
        })
        .collect()
}

/// The error for `key` already being in the unique `index`.
fn duplicate_key(index: &IndexMetadata, key: &Tuple) -> ExecutionError {
    let columns = index.key_schema().columns().iter().map(|c| c.name.as_str());
    let values = match key.decode(index.key_schema()) {
        Ok(values) => values.iter().map(ToString::to_string).collect(),
        Err(_) => vec!["?".to_owned()],
    };
    ExecutionError::DuplicateKey {
        index: index.name().to_owned(),
        key: format!(
            "({})=({})",
            columns.collect::<Vec<_>>().join(", "),
            values.join(", ")
        ),
    }
}

/// Whether `tuple` passes `predicate`: only TRUE does, not FALSE or NULL.
fn passes(predicate: &AbstractExpr, tuple: &Tuple, schema: &Schema) -> Result<bool> {
    Ok(predicate.evaluate(tuple, schema)? == Value::Bool(true))
}

/// A predicate made ready to test many rows of one schema, passing the rows
/// [`passes`] does. A column compared with a constant, or tested for NULL,
/// is read in place, and AND of those combines them; anything else is
/// evaluated as an expression.
enum RowPredicate {
    Compare {
        column: ColumnReader,
        op: ComparisonOp,
        constant: Value,
        /// Whether the column is the comparison's right operand.
        flipped: bool,
    },
    IsNull {
        column: ColumnReader,
        negated: bool,
    },
    And(Box<RowPredicate>, Box<RowPredicate>),
    Expr(AbstractExpr),
}

impl RowPredicate {
    fn new(predicate: &AbstractExpr, schema: &Schema) -> Self {
        Self::compile(predicate, schema).unwrap_or_else(|| RowPredicate::Expr(predicate.clone()))
    }

    fn compile(predicate: &AbstractExpr, schema: &Schema) -> Option<Self> {
        let column = |expr: &AbstractExpr| match expr {
            AbstractExpr::ColumnValueExpr(column) if column.tuple_idx() == 0 => {
                ColumnReader::new(schema, column.col_idx())
            }
            _ => None,
        };
        Some(match predicate {
            AbstractExpr::ComparisonExpr(comparison) => {
                let (column, constant, flipped) = match comparison.children() {
                    [left, AbstractExpr::ConstantExpr(constant)] => {
                        (column(left)?, constant, false)
                    }
                    [AbstractExpr::ConstantExpr(constant), right] => {
                        (column(right)?, constant, true)
                    }
                    _ => return None,
                };
                RowPredicate::Compare {
                    column,
                    op: comparison.op(),
                    constant: constant.value().clone(),
                    flipped,
                }
            }
            AbstractExpr::IsNullExpr(is_null) => RowPredicate::IsNull {
                column: column(&is_null.children()[0])?,
                negated: is_null.negated(),
            },
            AbstractExpr::LogicExpr(logic) if logic.op() == LogicOp::And => {
                let [left, right] = logic.children() else {
                    return None;
                };
                RowPredicate::And(
                    Box::new(Self::compile(left, schema)?),
                    Box::new(Self::compile(right, schema)?),
                )
            }
            _ => return None,
        })
    }

    /// Whether `tuple`, a row of `schema`, passes.
    #[inline]
    fn passes(&self, tuple: &Tuple, schema: &Schema) -> Result<bool> {
        Ok(self.test(tuple, schema)? == Some(true))
    }

    /// The predicate's value on `tuple`: TRUE, FALSE or NULL (`None`).
    fn test(&self, tuple: &Tuple, schema: &Schema) -> Result<Option<bool>> {
        Ok(match self {
            RowPredicate::Compare {
                column,
                op,
                constant,
                flipped,
            } => {
                let value = column.read(tuple)?;
                let ordering = if *flipped {
                    constant.as_value_ref().compare(value)?
                } else {
                    value.compare(constant.as_value_ref())?
                };
                ordering.map(|ordering| op.holds(ordering))
            }
            RowPredicate::IsNull { column, negated } => {
                Some(column.read(tuple)?.is_null() != *negated)
            }
            // As AND evaluates: FALSE on the left decides it.
            RowPredicate::And(left, right) => match left.test(tuple, schema)? {
                Some(false) => Some(false),
                left => match (left, right.test(tuple, schema)?) {
                    (_, Some(false)) => Some(false),
                    (Some(true), Some(true)) => Some(true),
                    _ => None,
                },
            },
            RowPredicate::Expr(expr) => match expr.evaluate(tuple, schema)? {
                Value::Bool(value) => Some(value),
                _ => None,
            },
        })
    }
}

/// The output of INSERT, UPDATE and DELETE: one row holding `count`.
fn count_row(schema: &Schema, count: usize) -> Result<Tuple> {
    let count = i64::try_from(count).expect("a row count fits in BIGINT");
    Ok(Tuple::encode(schema, &[Value::BigInt(count)])?)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::create_executor;
    use super::test_util::{Db, users_schema};
    use crate::ExecutionError;
    use crate::plans::{SeqScanPlan, TopNPlan, ValuesPlan};

    #[test]
    fn plans_without_executors_fail() {
        let db = Db::new();
        let values = Arc::new(ValuesPlan::new(users_schema(), vec![]).into());
        let topn = TopNPlan::new(vec![], 1, values).into();
        assert!(matches!(
            create_executor(&db.ctx(), &topn),
            Err(ExecutionError::NoExecutor("TopN"))
        ));
    }

    /// Executors find their tables when they run, not when they are built.
    #[test]
    fn unknown_table_fails() {
        let db = Db::new();
        let scan = SeqScanPlan::new(999, "gone", users_schema(), None).into();
        assert!(matches!(
            db.run(scan),
            Err(ExecutionError::TableNotFound(999))
        ));
    }
}
