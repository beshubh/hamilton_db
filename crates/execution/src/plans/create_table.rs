use std::sync::Arc;

use common::Schema;

use super::{AbstractPlanNode, PlanNode, expect_children};

/// Creates a table in the catalog, with its primary key index if it has a
/// primary key. Produces no rows.
#[derive(Clone, Debug)]
pub struct CreateTablePlan {
    /// Always empty.
    output_schema: Schema,
    table_name: String,
    /// The new table's columns.
    table_schema: Schema,
    /// The primary key columns, in key order. Empty for no primary key.
    primary_key: Vec<String>,
}

impl CreateTablePlan {
    pub fn new(
        table_name: impl Into<String>,
        table_schema: Schema,
        primary_key: Vec<String>,
    ) -> Self {
        Self {
            output_schema: Schema::default(),
            table_name: table_name.into(),
            table_schema,
            primary_key,
        }
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn table_schema(&self) -> &Schema {
        &self.table_schema
    }

    pub fn primary_key(&self) -> &[String] {
        &self.primary_key
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
