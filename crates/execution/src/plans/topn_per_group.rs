use std::sync::Arc;

use common::Schema;

use super::{AbstractPlanNode, OrderBy, PlanNode, only_child};
use crate::expressions::AbstractExpr;

/// Groups the child's rows by `group_bys` and keeps the rows of each group
/// that rank at most `n` in `order_bys` order: the rewrite of a filter
/// `rank() OVER (PARTITION BY ... ORDER BY ...) <= n`. Rows tied on the sort
/// keys share a rank, so a group can keep more than `n` rows.
#[derive(Clone, Debug)]
pub struct TopNPerGroupPlan {
    /// The child's schema.
    output_schema: Schema,
    /// Always `[child]`.
    children: Vec<Arc<AbstractPlanNode>>,
    /// Expressions on the child's row.
    group_bys: Vec<AbstractExpr>,
    order_bys: Vec<OrderBy>,
    n: u64,
}

impl TopNPerGroupPlan {
    pub fn new(
        group_bys: Vec<AbstractExpr>,
        order_bys: Vec<OrderBy>,
        n: u64,
        child: Arc<AbstractPlanNode>,
    ) -> Self {
        Self {
            output_schema: child.get_output_schema().clone(),
            children: vec![child],
            group_bys,
            order_bys,
            n,
        }
    }

    pub fn group_bys(&self) -> &[AbstractExpr] {
        &self.group_bys
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

impl PlanNode for TopNPerGroupPlan {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> &[Arc<AbstractPlanNode>] {
        &self.children
    }

    fn clone_with_children(&self, children: Vec<Arc<AbstractPlanNode>>) -> AbstractPlanNode {
        Self::new(
            self.group_bys.clone(),
            self.order_bys.clone(),
            self.n,
            only_child("TopNPerGroup", children),
        )
        .into()
    }
}
