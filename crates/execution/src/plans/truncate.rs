use std::sync::Arc;

use catalog::TableOid;
use common::Schema;

use super::{AbstractPlanNode, PlanNode, dml_output_schema};

/// Empties each table by giving it new storage (`Catalog::truncate`), and
/// with RESTART IDENTITY restarts its sequences.
#[derive(Clone, Debug)]
pub struct TruncatePlan {
    /// See [`dml_output_schema`]. The engine reports no count.
    output_schema: Schema,
    tables: Vec<TableOid>,
    restart_identity: bool,
}

impl TruncatePlan {
    pub fn new(tables: Vec<TableOid>, restart_identity: bool) -> Self {
        Self {
            output_schema: dml_output_schema(),
            tables,
            restart_identity,
        }
    }

    pub fn tables(&self) -> &[TableOid] {
        &self.tables
    }

    pub fn restart_identity(&self) -> bool {
        self.restart_identity
    }
}

impl PlanNode for TruncatePlan {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> &[Arc<AbstractPlanNode>] {
        &[]
    }

    fn clone_with_children(&self, children: Vec<Arc<AbstractPlanNode>>) -> AbstractPlanNode {
        assert!(children.is_empty(), "Truncate takes no children");
        self.clone().into()
    }
}
