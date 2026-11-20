use std::sync::Arc;

use catalog::CatalogError;
use common::{Rid, Schema, Tuple};

use super::{Executor, count_row};
use crate::execution_context::ExecutionContext;
use crate::plans::{DropTablePlan, PlanNode};
use crate::{ExecutionError, Result};

/// Drops the plan's tables on the first call to `next`, and produces one
/// row, which the engine ignores.
pub struct DropTableExecutor {
    ctx: Arc<ExecutionContext>,
    plan: DropTablePlan,
    done: bool,
}

impl DropTableExecutor {
    pub fn new(ctx: Arc<ExecutionContext>, plan: DropTablePlan) -> Self {
        Self {
            ctx,
            plan,
            done: false,
        }
    }
}

impl Executor for DropTableExecutor {
    fn init(&mut self) -> Result<()> {
        self.done = false;
        Ok(())
    }

    fn next(
        &mut self,
        tuples: &mut Vec<Tuple>,
        rids: &mut Vec<Rid>,
        _batch_size: usize,
    ) -> Result<bool> {
        tuples.clear();
        rids.clear();
        if self.done {
            return Ok(false);
        }
        self.done = true;
        for &oid in self.plan.tables() {
            match self.ctx.catalog().drop_table(self.ctx.txn(), oid) {
                Err(CatalogError::TableNotFound(_)) if self.plan.if_exists() => {}
                Err(CatalogError::TableNotFound(_)) => {
                    return Err(ExecutionError::TableNotFound(oid));
                }
                result => result?,
            }
        }
        tuples.push(count_row(self.plan.get_output_schema(), 0)?);
        Ok(true)
    }

    fn get_output_schema(&self) -> &Schema {
        self.plan.get_output_schema()
    }
}
