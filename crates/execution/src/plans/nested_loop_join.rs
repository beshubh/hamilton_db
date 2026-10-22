use std::sync::Arc;

use common::Schema;

use super::{AbstractPlanNode, JoinType, PlanNode, expect_children, join_schema};
use crate::expressions::AbstractExpr;

/// Joins its two children by comparing every left row with every right row.
#[derive(Clone, Debug)]
pub struct NestedLoopJoinPlan {
    /// See [`join_schema`].
    output_schema: Schema,
    /// Always `[left, right]`.
    children: Vec<Arc<AbstractPlanNode>>,
    join_type: JoinType,
    /// A BOOL condition on a pair of rows, reading the left row through
    /// `tuple_idx` 0 and the right through 1. `None` matches every pair.
    predicate: Option<AbstractExpr>,
}

impl NestedLoopJoinPlan {
    pub fn new(
        join_type: JoinType,
        left: Arc<AbstractPlanNode>,
        right: Arc<AbstractPlanNode>,
        predicate: Option<AbstractExpr>,
    ) -> Self {
        Self {
            output_schema: join_schema(
                left.get_output_schema(),
                right.get_output_schema(),
                join_type,
            ),
            children: vec![left, right],
            join_type,
            predicate,
        }
    }

    pub fn join_type(&self) -> JoinType {
        self.join_type
    }

    pub fn predicate(&self) -> Option<&AbstractExpr> {
        self.predicate.as_ref()
    }

    pub fn left(&self) -> &Arc<AbstractPlanNode> {
        &self.children[0]
    }

    pub fn right(&self) -> &Arc<AbstractPlanNode> {
        &self.children[1]
    }
}

impl PlanNode for NestedLoopJoinPlan {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> &[Arc<AbstractPlanNode>] {
        &self.children
    }

    fn clone_with_children(&self, children: Vec<Arc<AbstractPlanNode>>) -> AbstractPlanNode {
        let mut children = expect_children("NestedLoopJoin", children, 2);
        let right = children.pop().unwrap();
        let left = children.pop().unwrap();
        Self::new(self.join_type, left, right, self.predicate.clone()).into()
    }
}
