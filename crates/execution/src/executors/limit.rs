use std::sync::Arc;

use common::{Rid, Schema, Tuple};

use super::{AbstractExecutor, Executor};
use crate::Result;
use crate::execution_context::ExecutionContext;
use crate::plans::{LimitPlan, PlanNode};

/// Produces the child's first `limit` rows. It asks the child for no more
/// rows than are still wanted, and stops pulling once it has them all.
pub struct LimitExecutor {
    plan: LimitPlan,
    child: Box<AbstractExecutor>,
    /// How many more rows to produce.
    remaining: u64,
}

impl LimitExecutor {
    pub fn new(_ctx: Arc<ExecutionContext>, plan: LimitPlan, child: Box<AbstractExecutor>) -> Self {
        Self {
            remaining: plan.limit(),
            plan,
            child,
        }
    }
}

impl Executor for LimitExecutor {
    fn init(&mut self) -> Result<()> {
        self.remaining = self.plan.limit();
        self.child.init()
    }

    fn next(
        &mut self,
        tuples: &mut Vec<Tuple>,
        rids: &mut Vec<Rid>,
        batch_size: usize,
    ) -> Result<bool> {
        if self.remaining == 0 {
            tuples.clear();
            rids.clear();
            return Ok(false);
        }
        let wanted = usize::try_from(self.remaining).unwrap_or(usize::MAX);
        let more = self.child.next(tuples, rids, batch_size.min(wanted))?;
        self.remaining -= tuples.len() as u64;
        Ok(more)
    }

    fn get_output_schema(&self) -> &Schema {
        self.plan.get_output_schema()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common::{DataType, Value};

    use super::super::test_util::{Db, constant, int, user, users_schema};
    use crate::plans::{LimitPlan, ValuesPlan};

    #[test]
    fn stops_after_the_limit() {
        let db = Db::new();
        db.insert_numbered(9);
        let limit = LimitPlan::new(3, db.scan(None));
        assert_eq!(
            db.run(limit.into()).unwrap(),
            [
                user(1, Some("user 1")),
                user(2, Some("user 2")),
                user(3, Some("user 3"))
            ]
        );
    }

    #[test]
    fn a_limit_past_the_end_produces_every_row() {
        let db = Db::new();
        db.insert_numbered(3);
        let limit = LimitPlan::new(10, db.scan(None));
        assert_eq!(db.run(limit.into()).unwrap(), db.users_rows());
    }

    #[test]
    fn limit_zero_produces_nothing() {
        let db = Db::new();
        db.insert_numbered(3);
        let limit = LimitPlan::new(0, db.scan(None));
        assert!(db.run(limit.into()).unwrap().is_empty());
    }

    /// Rows that are not stored anywhere have no rids to pass on.
    #[test]
    fn over_rows_without_rids() {
        let db = Db::new();
        let rows = (1..=3)
            .map(|id| vec![int(id), constant(Value::Null, DataType::Varchar(32))])
            .collect();
        let values = Arc::new(ValuesPlan::new(users_schema(), rows).into());
        let limit = LimitPlan::new(1, values);
        assert_eq!(db.run(limit.into()).unwrap(), [user(1, None)]);
    }
}
