use std::sync::Arc;

use common::Schema;

use super::{AbstractPlanNode, PlanNode, expect_children};

/// Creates an index in the catalog. Produces no rows.
#[derive(Clone, Debug)]
pub struct CreateIndexPlan {
    /// Always empty.
    output_schema: Schema,
    index_name: String,
    table_name: String,
    /// The key columns, in key order.
    key_columns: Vec<String>,
}

impl CreateIndexPlan {
    pub fn new(
        index_name: impl Into<String>,
        table_name: impl Into<String>,
        key_columns: Vec<String>,
    ) -> Self {
        Self {
            output_schema: Schema::default(),
            index_name: index_name.into(),
            table_name: table_name.into(),
            key_columns,
        }
    }

    pub fn index_name(&self) -> &str {
        &self.index_name
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn key_columns(&self) -> &[String] {
        &self.key_columns
    }
}

impl PlanNode for CreateIndexPlan {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> &[Arc<AbstractPlanNode>] {
        &[]
    }

    fn clone_with_children(&self, children: Vec<Arc<AbstractPlanNode>>) -> AbstractPlanNode {
        expect_children("CreateIndex", children, 0);
        self.clone().into()
    }
}
