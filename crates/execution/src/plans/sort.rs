use std::sync::Arc;

use common::Schema;

use super::{AbstractPlanNode, PlanNode, only_child};
use crate::expressions::AbstractExpr;

/// One sort key.
#[derive(Clone, Debug)]
pub struct OrderBy {
    /// An expression on the child's row.
    pub expr: AbstractExpr,
    pub desc: bool,
}

/// Outputs all of the child's rows ordered by `order_bys`, the first key
/// deciding first.
#[derive(Clone, Debug)]
pub struct SortPlan {
    /// The child's schema.
    output_schema: Schema,
    /// Always `[child]`.
    children: Vec<Arc<AbstractPlanNode>>,
    order_bys: Vec<OrderBy>,
}

impl SortPlan {
    pub fn new(order_bys: Vec<OrderBy>, child: Arc<AbstractPlanNode>) -> Self {
        Self {
            output_schema: child.get_output_schema().clone(),
            children: vec![child],
            order_bys,
        }
    }

    pub fn order_bys(&self) -> &[OrderBy] {
        &self.order_bys
    }

    pub fn child(&self) -> &Arc<AbstractPlanNode> {
        &self.children[0]
    }
}

impl PlanNode for SortPlan {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> &[Arc<AbstractPlanNode>] {
        &self.children
    }

    fn clone_with_children(&self, children: Vec<Arc<AbstractPlanNode>>) -> AbstractPlanNode {
        Self::new(self.order_bys.clone(), only_child("Sort", children)).into()
    }
}
