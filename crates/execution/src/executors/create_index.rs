use std::sync::Arc;

use common::{Rid, Schema, Tuple};

use super::Executor;
use crate::Result;
use crate::execution_context::ExecutionContext;
use crate::plans::{CreateIndexPlan, PlanNode};

/// Creates the index on the first call to `next`. Produces no rows.
///
/// The index starts out empty: rows already in the table are not added to
/// it.
// TODO: fill the index from the table once executors maintain indexes.
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
            self.ctx.catalog().create_index(
                self.plan.index_name(),
                self.plan.table_name(),
                &key_columns,
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
    use catalog::CatalogError;

    use super::super::test_util::Db;
    use crate::ExecutionError;
    use crate::plans::CreateIndexPlan;

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
        assert!(!index.is_primary_key);
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
}
