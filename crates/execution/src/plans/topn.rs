use std::sync::Arc;

use common::Schema;

use super::{AbstractPlanNode, OrderBy, PlanNode, only_child};

/// Outputs the first `n` of the child's rows in `order_bys` order: a sort
/// followed by a limit, done by keeping only `n` rows at a time.
#[derive(Clone, Debug)]
pub struct TopNPlan {
    /// The child's schema.
    output_schema: Schema,
    /// Always `[child]`.
    children: Vec<Arc<AbstractPlanNode>>,
    order_bys: Vec<OrderBy>,
    n: u64,
}

impl TopNPlan {
    pub fn new(order_bys: Vec<OrderBy>, n: u64, child: Arc<AbstractPlanNode>) -> Self {
        Self {
            output_schema: child.get_output_schema().clone(),
            children: vec![child],
            order_bys,
            n,
        }
    }

    pub fn order_bys(&self) -> &[OrderBy] {
        &self.order_bys
    }

    pub fn n(&self) -> u64 {
        self.n
    }

    pub fn child(&self) -> &Arc<AbstractPlanNode> {
        &self.children[0]
    }
}

impl PlanNode for TopNPlan {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> &[Arc<AbstractPlanNode>] {
        &self.children
    }

    fn clone_with_children(&self, children: Vec<Arc<AbstractPlanNode>>) -> AbstractPlanNode {
        Self::new(self.order_bys.clone(), self.n, only_child("TopN", children)).into()
    }
}
