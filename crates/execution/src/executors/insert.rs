use std::sync::Arc;

use common::{Rid, Schema, Tuple};

use super::{
    AbstractExecutor, Executor, count_row, duplicate_key, index_keys, locators, write_indexes,
};
use crate::Result;
use crate::execution_context::ExecutionContext;
use crate::plans::{InsertPlan, PlanNode};

/// Inserts all of the child's rows, and their entries in each of the table's
/// indexes, on the first call to `next`, and produces one row holding how
/// many there were. A row whose primary key is taken, by a row already in
/// the table or one inserted before it, fails the statement.
pub struct InsertExecutor {
    ctx: Arc<ExecutionContext>,
    plan: InsertPlan,
    child: Box<AbstractExecutor>,
    child_tuples: Vec<Tuple>,
    child_rids: Vec<Rid>,
    done: bool,
}

impl InsertExecutor {
    pub fn new(ctx: Arc<ExecutionContext>, plan: InsertPlan, child: Box<AbstractExecutor>) -> Self {
        Self {
            ctx,
            plan,
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
        let table = self.ctx.table(self.plan.table_oid())?;
        let indexes = write_indexes(&self.ctx, &table);
        let indexed = self.ctx.indexed_table(&table);
        let mut rows = Vec::new();
        while self
            .child
            .next(&mut self.child_tuples, &mut self.child_rids, batch_size)?
        {
            rows.append(&mut self.child_tuples);
        }
        let txn = self.ctx.txn();
        // The child's rows are already in the table's layout.
        for tuple in &rows {
            let keys = index_keys(&table, &indexes, tuple)?;
            let rid = txn.insert(&table.heap, &table.schema, tuple)?;
            let locators = locators(&indexes, &keys, rid);
            for ((index, key), locator) in indexes.iter().zip(&keys).zip(&locators) {
                if !txn.insert_entry(&indexed, &index.index, key, locator)? {
                    return Err(duplicate_key(index, key));
                }
            }
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
        let insert = InsertPlan::new(db.users.oid, &db.users.name, Arc::new(values.into()));
        assert!(matches!(
            db.run(insert.into()),
            Err(ExecutionError::Value(ValueError::NullNotAllowed(_)))
        ));
        assert!(db.users_rows().is_empty());
    }

    #[test]
    fn adds_the_rows_to_the_indexes() {
        let db = Db::new();
        db.catalog()
            .create_index("users_name", "users", &["name"])
            .unwrap();
        db.insert(&[(1, Some("a")), (2, Some("b")), (3, Some("a"))]);
        assert_eq!(db.index_lookup("users_name", &["a".into()]).len(), 2);
        assert_eq!(db.index_lookup("users_name", &["b".into()]).len(), 1);
        assert!(db.index_lookup("users_name", &["c".into()]).is_empty());
    }

    #[test]
    fn inserts_across_pages() {
        let db = Db::new();
        db.insert_numbered(500);
        assert_eq!(db.users_rows().len(), 500);
    }
}
