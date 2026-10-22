use std::sync::Arc;

use catalog::TableOid;
use common::Schema;

use super::{AbstractPlanNode, PlanNode, dml_output_schema, expect_children};

/// Deletes every row its child produces, which are the table's rows that
/// match the DELETE's condition.
#[derive(Clone, Debug)]
pub struct DeletePlan {
    /// See [`dml_output_schema`].
    output_schema: Schema,
    /// Always `[child]`.
    children: Vec<Arc<AbstractPlanNode>>,
    table_oid: TableOid,
}

impl DeletePlan {
    pub fn new(table_oid: TableOid, child: Arc<AbstractPlanNode>) -> Self {
        Self {
            output_schema: dml_output_schema(),
            children: vec![child],
            table_oid,
        }
    }

    pub fn table_oid(&self) -> TableOid {
        self.table_oid
    }

    pub fn child(&self) -> &Arc<AbstractPlanNode> {
        &self.children[0]
    }
}

impl PlanNode for DeletePlan {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> &[Arc<AbstractPlanNode>] {
        &self.children
    }

    fn clone_with_children(&self, children: Vec<Arc<AbstractPlanNode>>) -> AbstractPlanNode {
        Self {
            children: expect_children("Delete", children, 1),
            ..self.clone()
        }
        .into()
    }
}
