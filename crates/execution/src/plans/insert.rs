use std::sync::Arc;

use catalog::TableOid;
use common::Schema;

use super::{AbstractPlanNode, PlanNode, dml_output_schema, expect_children};

/// Inserts every row its child produces into a table. The child's rows are
/// already in table column order and of the columns' types.
#[derive(Clone, Debug)]
pub struct InsertPlan {
    /// See [`dml_output_schema`].
    output_schema: Schema,
    /// Always `[child]`.
    children: Vec<Arc<AbstractPlanNode>>,
    table_oid: TableOid,
}

impl InsertPlan {
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

impl PlanNode for InsertPlan {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> &[Arc<AbstractPlanNode>] {
        &self.children
    }

    fn clone_with_children(&self, children: Vec<Arc<AbstractPlanNode>>) -> AbstractPlanNode {
        Self {
            children: expect_children("Insert", children, 1),
            ..self.clone()
        }
        .into()
    }
}
