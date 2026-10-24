//! Executors: the operators that run a plan tree.
//!
//! [`create_executor`] builds one executor per plan node, each owning the
//! executors of its children. Like plans and expressions, every executor is a
//! variant of [`AbstractExecutor`], and `enum_dispatch` forwards
//! [`Executor`] calls to the variant. Each executor lives in its own module
//! under `executors/`.
//!
//! Rows are pulled from the root in batches: each call to [`Executor::next`]
//! pulls as many batches from the children as it needs to produce one of its
//! own.
//!
//! So far there are executors for CREATE TABLE, CREATE INDEX, INSERT ...
//! VALUES, UPDATE, DELETE, SELECT ... WHERE through a table or an index
//! scan, and nested loop and index nested loop joins; other plans fail with
//! [`ExecutionError::NoExecutor`]. There are no transactions yet, so every
//! write is committed as soon as it is made.

mod create_index;
mod create_table;
mod delete;
mod filter;
mod index_scan;
mod insert;
mod nested_index_join;
mod nested_loop_join;
mod projection;
mod seq_scan;
#[cfg(test)]
#[path = "../tests/util/executors.rs"]
mod test_util;
mod update;
mod values;

pub use create_index::CreateIndexExecutor;
pub use create_table::CreateTableExecutor;
pub use delete::DeleteExecutor;
pub use filter::FilterExecutor;
pub use index_scan::IndexScanExecutor;
pub use insert::InsertExecutor;
pub use nested_index_join::NestedIndexJoinExecutor;
pub use nested_loop_join::NestedLoopJoinExecutor;
pub use projection::ProjectionExecutor;
pub use seq_scan::SeqScanExecutor;
pub use update::UpdateExecutor;
pub use values::ValuesExecutor;

use std::sync::Arc;

use catalog::{IndexInfo, IndexOid, TableInfo, TableOid};
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
    NestedLoopJoinExecutor,
    NestedIndexJoinExecutor,
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
            SeqScanExecutor::new(plan.clone(), table(ctx, plan.table_oid())?).into()
        }
        AbstractPlanNode::ValuesPlan(plan) => ValuesExecutor::new(plan.clone()).into(),
        AbstractPlanNode::InsertPlan(plan) => {
            let table = table(ctx, plan.table_oid())?;
            let indexes = ctx.catalog().table_indexes(&table.name);
            InsertExecutor::new(plan.clone(), table, indexes, child(plan.child())?).into()
        }
        AbstractPlanNode::DeletePlan(plan) => {
            let table = table(ctx, plan.table_oid())?;
            let indexes = ctx.catalog().table_indexes(&table.name);
            DeleteExecutor::new(plan.clone(), table, indexes, child(plan.child())?).into()
        }
        AbstractPlanNode::FilterPlan(plan) => {
            FilterExecutor::new(plan.clone(), child(plan.child())?).into()
        }
        AbstractPlanNode::ProjectionPlan(plan) => {
            ProjectionExecutor::new(plan.clone(), child(plan.child())?).into()
        }
        AbstractPlanNode::IndexScanPlan(plan) => {
            let table = table(ctx, plan.table_oid())?;
            IndexScanExecutor::new(plan.clone(), table, index(ctx, plan.index_oid())?).into()
        }
        AbstractPlanNode::UpdatePlan(plan) => {
            let table = table(ctx, plan.table_oid())?;
            let indexes = ctx.catalog().table_indexes(&table.name);
            UpdateExecutor::new(plan.clone(), table, indexes, child(plan.child())?).into()
        }
        AbstractPlanNode::NestedLoopJoinPlan(plan) => {
            let (left, right) = (child(plan.left())?, child(plan.right())?);
            NestedLoopJoinExecutor::new(plan.clone(), left, right, ctx.join_frames()).into()
        }
        AbstractPlanNode::HashJoinPlan(_) => return Err(ExecutionError::NoExecutor("HashJoin")),
        AbstractPlanNode::NestedIndexJoinPlan(plan) => {
            let table = table(ctx, plan.inner_table_oid())?;
            let index = index(ctx, plan.index_oid())?;
            NestedIndexJoinExecutor::new(plan.clone(), child(plan.outer())?, table, index).into()
        }
        AbstractPlanNode::AggregationPlan(_) => {
            return Err(ExecutionError::NoExecutor("Aggregation"));
        }
        AbstractPlanNode::WindowPlan(_) => return Err(ExecutionError::NoExecutor("Window")),
        AbstractPlanNode::SortPlan(_) => return Err(ExecutionError::NoExecutor("Sort")),
        AbstractPlanNode::LimitPlan(_) => return Err(ExecutionError::NoExecutor("Limit")),
        AbstractPlanNode::TopNPlan(_) => return Err(ExecutionError::NoExecutor("TopN")),
        AbstractPlanNode::TopNPerGroupPlan(_) => {
            return Err(ExecutionError::NoExecutor("TopNPerGroup"));
        }
    };
    Ok(executor)
}

fn table(ctx: &ExecutionContext, oid: TableOid) -> Result<Arc<TableInfo>> {
    ctx.catalog()
        .get_table_by_oid(oid)
        .ok_or(ExecutionError::TableNotFound(oid))
}

fn index(ctx: &ExecutionContext, oid: IndexOid) -> Result<Arc<IndexInfo>> {
    ctx.catalog()
        .get_index_by_oid(oid)
        .ok_or(ExecutionError::IndexNotFound(oid))
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
    use crate::plans::{LimitPlan, SeqScanPlan, ValuesPlan};

    #[test]
    fn plans_without_executors_fail() {
        let db = Db::new();
        let values = Arc::new(ValuesPlan::new(users_schema(), vec![]).into());
        let limit = LimitPlan::new(1, values).into();
        assert!(matches!(
            create_executor(&db.ctx, &limit),
            Err(ExecutionError::NoExecutor("Limit"))
        ));
    }

    #[test]
    fn unknown_table_fails() {
        let db = Db::new();
        let scan = SeqScanPlan::new(999, "gone", users_schema(), None).into();
        assert!(matches!(
            create_executor(&db.ctx, &scan),
            Err(ExecutionError::TableNotFound(999))
        ));
    }
}
