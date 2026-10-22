use std::sync::Arc;

use catalog::TableOid;
use common::Schema;

use super::{AbstractPlanNode, PlanNode, dml_output_schema, expect_children};
use crate::expressions::AbstractExpr;

/// Updates every row its child produces, which are the table's rows that
/// match the UPDATE's condition.
#[derive(Clone, Debug)]
pub struct UpdatePlan {
    /// See [`dml_output_schema`].
    output_schema: Schema,
    /// Always `[child]`.
    children: Vec<Arc<AbstractPlanNode>>,
    table_oid: TableOid,
    /// The new row: one expression per table column, in table order and of
    /// the column's type, evaluated on the old row. A column the UPDATE does
    /// not set reads itself.
    target_exprs: Vec<AbstractExpr>,
}

impl UpdatePlan {
    pub fn new(
        table_oid: TableOid,
        target_exprs: Vec<AbstractExpr>,
        child: Arc<AbstractPlanNode>,
    ) -> Self {
        Self {
            output_schema: dml_output_schema(),
            children: vec![child],
            table_oid,
            target_exprs,
        }
    }

    pub fn table_oid(&self) -> TableOid {
        self.table_oid
    }

    pub fn target_exprs(&self) -> &[AbstractExpr] {
        &self.target_exprs
    }

    pub fn child(&self) -> &Arc<AbstractPlanNode> {
        &self.children[0]
    }
}

impl PlanNode for UpdatePlan {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> &[Arc<AbstractPlanNode>] {
        &self.children
    }

    fn clone_with_children(&self, children: Vec<Arc<AbstractPlanNode>>) -> AbstractPlanNode {
        Self {
            children: expect_children("Update", children, 1),
            ..self.clone()
        }
        .into()
    }
}
