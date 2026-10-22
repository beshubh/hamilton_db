use std::sync::Arc;

use common::Schema;

use super::{AbstractPlanNode, PlanNode, only_child};
use crate::expressions::AbstractExpr;

/// Keeps the child's rows for which `predicate` is true; false and NULL drop
/// the row.
#[derive(Clone, Debug)]
pub struct FilterPlan {
    /// The child's schema.
    output_schema: Schema,
    /// Always `[child]`.
    children: Vec<Arc<AbstractPlanNode>>,
    /// A BOOL condition on the child's row.
    predicate: AbstractExpr,
}

impl FilterPlan {
    pub fn new(predicate: AbstractExpr, child: Arc<AbstractPlanNode>) -> Self {
        Self {
            output_schema: child.get_output_schema().clone(),
            children: vec![child],
            predicate,
        }
    }

    pub fn predicate(&self) -> &AbstractExpr {
        &self.predicate
    }

    pub fn child(&self) -> &Arc<AbstractPlanNode> {
        &self.children[0]
    }
}

impl PlanNode for FilterPlan {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> &[Arc<AbstractPlanNode>] {
        &self.children
    }

    fn clone_with_children(&self, children: Vec<Arc<AbstractPlanNode>>) -> AbstractPlanNode {
        Self::new(self.predicate.clone(), only_child("Filter", children)).into()
    }
}
