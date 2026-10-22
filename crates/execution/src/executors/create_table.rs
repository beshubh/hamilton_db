use std::sync::Arc;

use common::{Rid, Schema, Tuple};

use super::Executor;
use crate::Result;
use crate::execution_context::ExecutionContext;
use crate::plans::{CreateTablePlan, PlanNode};

/// Creates the table on the first call to `next`. Produces no rows.
pub struct CreateTableExecutor {
    ctx: Arc<ExecutionContext>,
    plan: CreateTablePlan,
    done: bool,
}

impl CreateTableExecutor {
    pub fn new(ctx: Arc<ExecutionContext>, plan: CreateTablePlan) -> Self {
        Self {
            ctx,
            plan,
            done: false,
        }
    }
}

impl Executor for CreateTableExecutor {
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
            self.ctx
                .catalog()
                .create_table(self.plan.table_name(), self.plan.table_schema().clone())?;
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

    use super::super::test_util::{Db, users_schema};
    use crate::ExecutionError;
    use crate::plans::CreateTablePlan;

    #[test]
    fn creates_the_table() {
        let db = Db::new();
        let rows = db.run(CreateTablePlan::new("t", users_schema()).into());
        assert!(rows.unwrap().is_empty());
        let table = db.catalog().get_table("t").unwrap();
        assert_eq!(table.schema, users_schema());
        assert_eq!(table.heap.iter().unwrap().count(), 0);
    }

    #[test]
    fn existing_table_fails() {
        let db = Db::new();
        let err = db
            .run(CreateTablePlan::new("users", users_schema()).into())
            .unwrap_err();
        assert!(matches!(
            err,
            ExecutionError::Catalog(CatalogError::TableExists(name)) if name == "users"
        ));
    }
}
