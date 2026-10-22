use std::sync::Arc;

use common::Schema;

use super::{AbstractPlanNode, PlanNode, expect_children};

/// Creates a table in the catalog. Produces no rows.
#[derive(Clone, Debug)]
pub struct CreateTablePlan {
    /// Always empty.
    output_schema: Schema,
    table_name: String,
    /// The new table's columns.
    table_schema: Schema,
}

impl CreateTablePlan {
    pub fn new(table_name: impl Into<String>, table_schema: Schema) -> Self {
        Self {
            output_schema: Schema::default(),
            table_name: table_name.into(),
            table_schema,
        }
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn table_schema(&self) -> &Schema {
        &self.table_schema
    }
}

impl PlanNode for CreateTablePlan {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> &[Arc<AbstractPlanNode>] {
        &[]
    }

    fn clone_with_children(&self, children: Vec<Arc<AbstractPlanNode>>) -> AbstractPlanNode {
        expect_children("CreateTable", children, 0);
        self.clone().into()
    }
}
