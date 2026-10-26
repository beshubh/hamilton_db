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
//! [`ExecutionError::NoExecutor`]. There are no transactions yet, so every
//! write is committed as soon as it is made.

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
mod test_util;
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
pub use update::UpdateExecutor;
pub use values::ValuesExecutor;

use std::sync::Arc;

use catalog::{IndexInfo, TableInfo};
use common::{Rid, Schema, Tuple, Value};
use enum_dispatch::enum_dispatch;

use crate::error::{ExecutionError, Result};
use crate::execution_context::ExecutionContext;
use crate::expressions::{AbstractExpr, Expr};
use crate::plans::AbstractPlanNode;

/// The timestamp every write is stamped with until there are transactions,
/// the same one the catalog's own rows carry.
const COMMIT_TS: u64 = 0;

#[enum_dispatch]
pub trait Executor {
    /// Prepares the executor, and its children, to produce their rows from
    /// the start. Must be called before the first [`next`](Self::next).
    fn init(&mut self) -> Result<()>;

    /// Replaces the contents of `tuples` with the next rows, at most
    /// `batch_size` of them, and of `rids` with where those rows are stored,
    /// for rows read from a table. Returns `false`, leaving both empty, once
    /// there are no more rows.
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

/// The key of `tuple`, a row of `table`, in each of `indexes`.
fn index_keys(table: &TableInfo, indexes: &[Arc<IndexInfo>], tuple: &Tuple) -> Result<Vec<Tuple>> {
    indexes
        .iter()
        .map(|index| Ok(tuple.to_key(&table.schema, &index.key_schema, &index.key_columns)?))
        .collect()
}

/// The error for `key` already being in the primary key `index`.
fn duplicate_key(index: &IndexInfo, key: &Tuple) -> ExecutionError {
    let columns = index.key_schema.columns().iter().map(|c| c.name.as_str());
    let values = match key.decode(&index.key_schema) {
        Ok(values) => values.iter().map(ToString::to_string).collect(),
        Err(_) => vec!["?".to_owned()],
    };
    ExecutionError::DuplicateKey {
        index: index.name.clone(),
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
            create_executor(&db.ctx, &topn),
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
