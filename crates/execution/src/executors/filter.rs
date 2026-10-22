use common::{Rid, Schema, Tuple};

use super::{AbstractExecutor, Executor, passes};
use crate::Result;
use crate::plans::{FilterPlan, PlanNode};

/// Produces the child's rows that pass the plan's predicate.
pub struct FilterExecutor {
    plan: FilterPlan,
    child: Box<AbstractExecutor>,
    child_tuples: Vec<Tuple>,
    child_rids: Vec<Rid>,
}

impl FilterExecutor {
    pub fn new(plan: FilterPlan, child: Box<AbstractExecutor>) -> Self {
        Self {
            plan,
            child,
            child_tuples: Vec::new(),
            child_rids: Vec::new(),
        }
    }
}

impl Executor for FilterExecutor {
    fn init(&mut self) -> Result<()> {
        self.child.init()
    }

    /// Pulls child batches until one has a row that passes, so a batch is
    /// never empty before the child runs out.
    fn next(
        &mut self,
        tuples: &mut Vec<Tuple>,
        rids: &mut Vec<Rid>,
        batch_size: usize,
    ) -> Result<bool> {
        tuples.clear();
        rids.clear();
        while tuples.is_empty() {
            let more = self
                .child
                .next(&mut self.child_tuples, &mut self.child_rids, batch_size)?;
            if !more {
                return Ok(false);
            }
            let schema = self.child.get_output_schema();
            for tuple in self.child_tuples.drain(..) {
                if passes(self.plan.predicate(), &tuple, schema)? {
                    rids.extend(tuple.rid());
                    tuples.push(tuple);
                }
            }
        }
        Ok(true)
    }

    fn get_output_schema(&self) -> &Schema {
        self.plan.get_output_schema()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common::{DataType, Value};

    use super::super::test_util::{Db, column, compare, constant, int, user, users_schema};
    use crate::expressions::{ComparisonOp, LogicExpr, LogicOp};
    use crate::plans::{FilterPlan, ValuesPlan};

    #[test]
    fn keeps_rows_where_the_predicate_is_true() {
        let db = Db::new();
        db.insert(&[
            (1, Some("a")),
            (2, None),
            (3, Some("b")),
            (4, Some("a")),
            (5, Some("a")),
        ]);
        // name = 'a' AND id > 1: NULL for user 2, which is dropped too.
        let name_is_a = compare(
            ComparisonOp::Eq,
            column(1),
            constant(Value::from("a"), DataType::Varchar(32)),
        );
        let id_above_1 = compare(ComparisonOp::Gt, column(0), int(1));
        let predicate = LogicExpr::new(LogicOp::And, name_is_a, id_above_1).unwrap();
        let filter = FilterPlan::new(predicate.into(), db.scan(None));
        assert_eq!(
            db.run(filter.into()).unwrap(),
            [user(4, Some("a")), user(5, Some("a"))]
        );
    }

    /// The child's batches pass nothing until the last one.
    #[test]
    fn skips_batches_without_matches() {
        let db = Db::new();
        db.insert_numbered(9);
        let filter = FilterPlan::new(compare(ComparisonOp::Eq, column(0), int(9)), db.scan(None));
        assert_eq!(db.run(filter.into()).unwrap(), [user(9, Some("user 9"))]);
    }

    /// Rows that are not stored anywhere have no rids to pass on.
    #[test]
    fn over_rows_without_rids() {
        let db = Db::new();
        let rows = vec![
            vec![int(1), constant(Value::Null, DataType::Varchar(32))],
            vec![int(2), constant(Value::Null, DataType::Varchar(32))],
        ];
        let values = Arc::new(ValuesPlan::new(users_schema(), rows).into());
        let filter = FilterPlan::new(compare(ComparisonOp::Eq, column(0), int(2)), values);
        assert_eq!(db.run(filter.into()).unwrap(), [user(2, None)]);
    }
}
