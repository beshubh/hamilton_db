use std::sync::Arc;

use common::{Rid, Schema, Tuple};

use super::Executor;
use crate::Result;
use crate::execution_context::ExecutionContext;
use crate::plans::{CreateTablePlan, PlanNode};

/// Creates the table, and its primary key index, on the first call to
/// `next`. Produces no rows.
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
            let primary_key: Vec<&str> =
                self.plan.primary_key().iter().map(String::as_str).collect();
            self.ctx.catalog().create_table(
                self.plan.table_name(),
                self.plan.table_schema().clone(),
                &primary_key,
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

    use super::super::test_util::{Db, users_schema};
    use crate::ExecutionError;
    use crate::plans::CreateTablePlan;

    #[test]
    fn creates_the_table() {
        let db = Db::new();
        let rows = db.run(CreateTablePlan::new("t", users_schema(), vec![]).into());
        assert!(rows.unwrap().is_empty());
        let table = db.catalog().get_table("t").unwrap();
        assert_eq!(table.schema, users_schema());
        assert_eq!(table.heap.iter().unwrap().count(), 0);
        assert!(db.catalog().table_indexes("t").is_empty());
    }

    #[test]
    fn creates_the_primary_key_index() {
        let db = Db::new();
        let plan = CreateTablePlan::new("t", users_schema(), vec!["id".into()]);
        db.run(plan.into()).unwrap();
        let [pkey] = db.catalog().table_indexes("t").try_into().unwrap();
        assert_eq!(pkey.name, "t_pkey");
        assert!(pkey.is_primary_key);
        assert_eq!(pkey.key_columns, [0]);
    }

    #[test]
    fn existing_table_fails() {
        let db = Db::new();
        let err = db
            .run(CreateTablePlan::new("users", users_schema(), vec![]).into())
            .unwrap_err();
        assert!(matches!(
            err,
            ExecutionError::Catalog(CatalogError::TableExists(name)) if name == "users"
        ));
    }
}
