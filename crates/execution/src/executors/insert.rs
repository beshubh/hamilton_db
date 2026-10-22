use std::sync::Arc;

use catalog::TableInfo;
use common::{Rid, Schema, Tuple};
use storage::table::table_page::TupleMeta;

use super::{AbstractExecutor, COMMIT_TS, Executor, count_row};
use crate::Result;
use crate::plans::{InsertPlan, PlanNode};

/// Inserts all of the child's rows on the first call to `next`, and produces
/// one row holding how many there were.
///
/// There are no transactions to roll back yet, so it reads every row before
/// inserting any: a row that fails to evaluate inserts nothing.
///
/// The table's indexes are not updated: storage has no B+ tree yet.
pub struct InsertExecutor {
    plan: InsertPlan,
    table: Arc<TableInfo>,
    child: Box<AbstractExecutor>,
    child_tuples: Vec<Tuple>,
    child_rids: Vec<Rid>,
    done: bool,
}

impl InsertExecutor {
    pub fn new(plan: InsertPlan, table: Arc<TableInfo>, child: Box<AbstractExecutor>) -> Self {
        Self {
            plan,
            table,
            child,
            child_tuples: Vec::new(),
            child_rids: Vec::new(),
            done: false,
        }
    }
}

impl Executor for InsertExecutor {
    fn init(&mut self) -> Result<()> {
        self.done = false;
        self.child.init()
    }

    fn next(
        &mut self,
        tuples: &mut Vec<Tuple>,
        rids: &mut Vec<Rid>,
        batch_size: usize,
    ) -> Result<bool> {
        tuples.clear();
        rids.clear();
        if self.done {
            return Ok(false);
        }
        self.done = true;
        let mut rows = Vec::new();
        while self
            .child
            .next(&mut self.child_tuples, &mut self.child_rids, batch_size)?
        {
            rows.append(&mut self.child_tuples);
        }
        // The child's rows are already in the table's layout.
        for tuple in &rows {
            self.table.heap.insert(&TupleMeta::new(COMMIT_TS), tuple)?;
        }
        tuples.push(count_row(self.plan.get_output_schema(), rows.len())?);
        Ok(true)
    }

    fn get_output_schema(&self) -> &Schema {
        self.plan.get_output_schema()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common::{DataType, Value, ValueError};

    use super::super::test_util::{Db, constant, int, user, users_schema};
    use crate::ExecutionError;
    use crate::plans::{InsertPlan, ValuesPlan};

    #[test]
    fn inserts_the_rows_and_counts_them() {
        let db = Db::new();
        assert_eq!(db.insert(&[(1, Some("a")), (2, None), (3, Some("c"))]), 3);
        assert_eq!(db.insert(&[(4, Some("d"))]), 1);
        assert_eq!(
            db.users_rows(),
            [
                user(1, Some("a")),
                user(2, None),
                user(3, Some("c")),
                user(4, Some("d"))
            ]
        );
    }

    #[test]
    fn a_failing_row_inserts_nothing() {
        let db = Db::new();
        let rows = vec![
            vec![int(1), constant(Value::from("a"), DataType::Varchar(32))],
            vec![
                constant(Value::Null, DataType::Int),
                constant(Value::from("b"), DataType::Varchar(32)),
            ],
        ];
        let values = ValuesPlan::new(users_schema(), rows);
        let insert = InsertPlan::new(db.users.oid, Arc::new(values.into()));
        assert!(matches!(
            db.run(insert.into()),
            Err(ExecutionError::Value(ValueError::NullNotAllowed(_)))
        ));
        assert!(db.users_rows().is_empty());
    }

    #[test]
    fn inserts_across_pages() {
        let db = Db::new();
        db.insert_numbered(500);
        assert_eq!(db.users_rows().len(), 500);
    }
}
