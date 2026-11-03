use std::sync::Arc;

use common::{Rid, Schema, Tuple};

use super::{Executor, count_row};
use crate::Result;
use crate::execution_context::ExecutionContext;
use crate::plans::{PlanNode, TruncatePlan};

/// Truncates the plan's tables on the first call to `next`, and produces
/// one row, which the engine ignores.
pub struct TruncateExecutor {
    ctx: Arc<ExecutionContext>,
    plan: TruncatePlan,
    done: bool,
}

impl TruncateExecutor {
    pub fn new(ctx: Arc<ExecutionContext>, plan: TruncatePlan) -> Self {
        Self {
            ctx,
            plan,
            done: false,
        }
    }
}

impl Executor for TruncateExecutor {
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
            self.ctx
                .catalog()
                .truncate(self.ctx.txn(), oid, self.plan.restart_identity())?;
        }
        tuples.push(count_row(self.plan.get_output_schema(), 0)?);
        Ok(true)
    }

    fn get_output_schema(&self) -> &Schema {
        self.plan.get_output_schema()
    }
}
