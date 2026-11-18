use std::sync::Arc;

use catalog::TableOid;
use common::Schema;

use super::{AbstractPlanNode, PlanNode, dml_output_schema};

/// Drops each table with its indexes and sequences (`Catalog::drop_table`).
/// With IF EXISTS, a table dropped since the statement was bound is
/// skipped.
#[derive(Clone, Debug)]
pub struct DropTablePlan {
    /// See [`dml_output_schema`]. The engine reports no count.
    output_schema: Schema,
    tables: Vec<TableOid>,
    if_exists: bool,
}

impl DropTablePlan {
    pub fn new(tables: Vec<TableOid>, if_exists: bool) -> Self {
        Self {
            output_schema: dml_output_schema(),
            tables,
            if_exists,
        }
    }

    pub fn tables(&self) -> &[TableOid] {
        &self.tables
    }

    pub fn if_exists(&self) -> bool {
        self.if_exists
    }
}

impl PlanNode for DropTablePlan {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> &[Arc<AbstractPlanNode>] {
        &[]
    }

    fn clone_with_children(&self, children: Vec<Arc<AbstractPlanNode>>) -> AbstractPlanNode {
        assert!(children.is_empty(), "DropTable takes no children");
        self.clone().into()
    }
}
