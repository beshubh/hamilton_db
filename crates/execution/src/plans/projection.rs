use std::sync::Arc;

use common::Schema;

use super::{AbstractPlanNode, PlanNode, only_child};
use crate::expressions::AbstractExpr;

/// Computes one output row per child row, one expression per output column.
#[derive(Clone, Debug)]
pub struct ProjectionPlan {
    /// One column per expression, named by the select list.
    output_schema: Schema,
    /// Always `[child]`.
    children: Vec<Arc<AbstractPlanNode>>,
    /// Expressions on the child's row.
    exprs: Vec<AbstractExpr>,
}

impl ProjectionPlan {
    pub fn new(
        output_schema: Schema,
        exprs: Vec<AbstractExpr>,
        child: Arc<AbstractPlanNode>,
    ) -> Self {
        debug_assert_eq!(output_schema.len(), exprs.len());
        Self {
            output_schema,
            children: vec![child],
            exprs,
        }
    }

    pub fn exprs(&self) -> &[AbstractExpr] {
        &self.exprs
    }

    pub fn child(&self) -> &Arc<AbstractPlanNode> {
        &self.children[0]
    }
}

impl PlanNode for ProjectionPlan {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> &[Arc<AbstractPlanNode>] {
        &self.children
    }

    fn clone_with_children(&self, children: Vec<Arc<AbstractPlanNode>>) -> AbstractPlanNode {
        Self {
            children: vec![only_child("Projection", children)],
            ..self.clone()
        }
        .into()
    }
}
