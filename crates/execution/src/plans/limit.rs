use std::sync::Arc;

use common::Schema;

use super::{AbstractPlanNode, PlanNode, only_child};

/// Outputs the first `limit` rows of the child.
#[derive(Clone, Debug)]
pub struct LimitPlan {
    /// The child's schema.
    output_schema: Schema,
    /// Always `[child]`.
    children: Vec<Arc<AbstractPlanNode>>,
    limit: u64,
}

impl LimitPlan {
    pub fn new(limit: u64, child: Arc<AbstractPlanNode>) -> Self {
        Self {
            output_schema: child.get_output_schema().clone(),
            children: vec![child],
            limit,
        }
    }

    pub fn limit(&self) -> u64 {
        self.limit
    }

    pub fn child(&self) -> &Arc<AbstractPlanNode> {
        &self.children[0]
    }
}

impl PlanNode for LimitPlan {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> &[Arc<AbstractPlanNode>] {
        &self.children
    }

    fn clone_with_children(&self, children: Vec<Arc<AbstractPlanNode>>) -> AbstractPlanNode {
        Self::new(self.limit, only_child("Limit", children)).into()
    }
}
