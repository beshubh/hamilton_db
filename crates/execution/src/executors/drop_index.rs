use std::sync::Arc;

use catalog::CatalogError;
use common::{Rid, Schema, Tuple};

use super::{Executor, count_row};
use crate::Result;
use crate::execution_context::ExecutionContext;
use crate::plans::{DropIndexPlan, PlanNode};

pub struct DropIndexExecutor {
    ctx: Arc<ExecutionContext>,
    plan: DropIndexPlan,
    done: bool,
}

impl DropIndexExecutor {
    pub fn new(ctx: Arc<ExecutionContext>, plan: DropIndexPlan) -> Self {
        Self {
            ctx,
            plan,
            done: false,
        }
    }
}

impl Executor for DropIndexExecutor {
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
        let catalog = self.ctx.catalog();
        for index in self.plan.indexes() {
            // Dropped by another transaction between binding and locking.
            if catalog.get_index_by_oid(index.oid).is_none() {
                if self.plan.if_exists() {
                    continue;
                }
                return Err(CatalogError::IndexNotFound(index.name.clone()).into());
            }
            catalog.drop_index(self.ctx.txn(), index.oid)?;
        }
        tuples.push(count_row(self.plan.get_output_schema(), 0)?);
        Ok(true)
    }

    fn get_output_schema(&self) -> &Schema {
        self.plan.get_output_schema()
    }
}
