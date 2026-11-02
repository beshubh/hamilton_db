use std::sync::Arc;

use common::{Rid, Schema, Tuple};
use storage::index::IndexTrait;

use super::{Executor, duplicate_key};
use crate::Result;
use crate::execution_context::ExecutionContext;
use crate::plans::{CreateIndexPlan, PlanNode};

/// Creates the index on the first call to `next`, with an entry for every
/// row already in the table (`Transaction::build_index`). A unique index
/// fails, and is not created, if two of the rows share a key. Produces no
/// rows.
pub struct CreateIndexExecutor {
    ctx: Arc<ExecutionContext>,
    plan: CreateIndexPlan,
    done: bool,
}

impl CreateIndexExecutor {
    pub fn new(ctx: Arc<ExecutionContext>, plan: CreateIndexPlan) -> Self {
        Self {
            ctx,
            plan,
            done: false,
        }
    }
}

impl Executor for CreateIndexExecutor {
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
        if !self.done {
            self.done = true;
            let key_columns: Vec<&str> =
                self.plan.key_columns().iter().map(String::as_str).collect();
            let txn = self.ctx.txn();
            self.ctx.catalog().create_index_with(
                self.plan.index_name(),
                self.plan.table_name(),
                &key_columns,
                self.plan.kind(),
                |table, index| {
                    let table = self.ctx.indexed_table(table);
                    match txn.build_index(&table, index)? {
                        Some(key) => Err(duplicate_key(index.metadata(), &key)),
                        None => Ok(()),
                    }
                },
            )?;
        }
        Ok(false)
    }

    fn get_output_schema(&self) -> &Schema {
        self.plan.get_output_schema()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use catalog::CatalogError;
    use common::{DataType, IndexKind, Value};

    use super::super::test_util::{Db, constant, int, user, users_schema};
    use crate::ExecutionError;
    use crate::plans::{CreateIndexPlan, InsertPlan, ValuesPlan};

    fn plan(name: &str, table: &str, columns: &[&str]) -> CreateIndexPlan {
        let columns = columns.iter().map(|&c| c.to_owned()).collect();
        CreateIndexPlan::new(name, table, columns)
    }

    #[test]
    fn creates_the_index() {
        let db = Db::new();
        let rows = db.run(plan("users_name_id", "users", &["name", "id"]).into());
        assert!(rows.unwrap().is_empty());
        let index = db.catalog().get_index("users_name_id").unwrap();
        assert_eq!(index.table_oid, db.users.oid);
        assert_eq!(index.key_columns, [1, 0]);
        assert_eq!(index.kind, IndexKind::Plain);
    }

    #[test]
    fn existing_index_fails() {
        let db = Db::new();
        db.run(plan("i", "users", &["id"]).into()).unwrap();
        let err = db.run(plan("i", "users", &["name"]).into()).unwrap_err();
        assert!(matches!(
            err,
            ExecutionError::Catalog(CatalogError::IndexExists(name)) if name == "i"
        ));
    }

    #[test]
    fn adds_the_rows_already_in_the_table() {
        let db = Db::new();
        db.insert(&[(1, Some("a")), (2, Some("b")), (3, Some("a")), (4, None)]);
        db.run(plan("users_name", "users", &["name"]).into())
            .unwrap();
        assert_eq!(db.index_entries("users_name").len(), 4);
        assert_eq!(
            db.index_rows("users_name", &[Value::from("a")]),
            [user(1, Some("a")), user(3, Some("a"))]
        );
        assert_eq!(
            db.index_rows("users_name", &[Value::from("b")]),
            [user(2, Some("b"))]
        );
    }

    /// A transaction newer than CREATE INDEX's can still have written the
    /// table before the index existed, so it never touches the index. The
    /// entry the index got for its row is removed once it rolls back, not
    /// as soon as CREATE INDEX's transaction is collected.
    #[test]
    fn a_newer_writer_that_rolls_back_leaves_no_entry() {
        let db = Db::new();
        db.insert(&[(1, Some("a"))]);
        let create = db.tm.begin();
        let writer = db.tm.begin();
        let row = vec![int(2), constant(Value::from("b"), DataType::Varchar(32))];
        let values = ValuesPlan::new(users_schema(), vec![row]);
        let insert = InsertPlan::new(db.users.oid, &db.users.name, Arc::new(values.into()));
        db.run_in(&writer, insert.into()).unwrap();

        db.run_in(&create, plan("users_name", "users", &["name"]).into())
            .unwrap();
        db.tm.commit(&create).unwrap();
        db.tm.wait_for_gc();
        assert_eq!(db.index_entries("users_name").len(), 2);

        db.tm.abort(&writer).unwrap();
        db.tm.wait_for_gc();
        assert_eq!(db.index_entries("users_name").len(), 1);
        assert_eq!(
            db.index_rows("users_name", &[Value::from("a")]),
            [user(1, Some("a"))]
        );
    }
}
