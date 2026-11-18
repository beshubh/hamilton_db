use std::sync::Arc;

use catalog::{IndexOid, TableOid};
use common::Schema;

use super::{AbstractPlanNode, PlanNode, dml_output_schema};

/// Drops each index (`Catalog::drop_index`), holding its table in
/// `AccessExclusive`.
#[derive(Clone, Debug)]
pub struct DropIndexPlan {
    output_schema: Schema,
    indexes: Vec<DroppedIndex>,
    if_exists: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DroppedIndex {
    pub name: String,
    pub oid: IndexOid,
    pub table_oid: TableOid,
}

impl DropIndexPlan {
    /// With `if_exists`, an index gone by the time the plan runs is skipped.
    pub fn new(indexes: Vec<DroppedIndex>, if_exists: bool) -> Self {
        Self {
            output_schema: dml_output_schema(),
            indexes,
            if_exists,
        }
    }

    pub fn indexes(&self) -> &[DroppedIndex] {
        &self.indexes
    }

    pub fn if_exists(&self) -> bool {
        self.if_exists
    }
}

impl PlanNode for DropIndexPlan {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> &[Arc<AbstractPlanNode>] {
        &[]
    }

    fn clone_with_children(&self, children: Vec<Arc<AbstractPlanNode>>) -> AbstractPlanNode {
        assert!(children.is_empty(), "DropIndex takes no children");
        self.clone().into()
    }
}
