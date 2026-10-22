use std::sync::Arc;

use common::Schema;

use super::{AbstractPlanNode, JoinType, PlanNode, expect_children, join_schema};
use crate::expressions::AbstractExpr;

/// Joins its two children on equal keys: builds a hash table of the right
/// rows by `right_keys`, then probes it with each left row's `left_keys`.
/// A NULL key matches nothing, as with `=`.
#[derive(Clone, Debug)]
pub struct HashJoinPlan {
    /// See [`join_schema`].
    output_schema: Schema,
    /// Always `[left, right]`.
    children: Vec<Arc<AbstractPlanNode>>,
    join_type: JoinType,
    /// Expressions on the left row, paired by position with `right_keys`.
    left_keys: Vec<AbstractExpr>,
    /// Expressions on the right row.
    right_keys: Vec<AbstractExpr>,
    /// The rest of the ON condition, checked on each pair whose keys match,
    /// reading the left row through `tuple_idx` 0 and the right through 1.
    predicate: Option<AbstractExpr>,
}

impl HashJoinPlan {
    pub fn new(
        join_type: JoinType,
        left: Arc<AbstractPlanNode>,
        right: Arc<AbstractPlanNode>,
        left_keys: Vec<AbstractExpr>,
        right_keys: Vec<AbstractExpr>,
        predicate: Option<AbstractExpr>,
    ) -> Self {
        debug_assert_eq!(left_keys.len(), right_keys.len());
        Self {
            output_schema: join_schema(
                left.get_output_schema(),
                right.get_output_schema(),
                join_type,
            ),
            children: vec![left, right],
            join_type,
            left_keys,
            right_keys,
            predicate,
        }
    }

    pub fn join_type(&self) -> JoinType {
        self.join_type
    }

    pub fn left_keys(&self) -> &[AbstractExpr] {
        &self.left_keys
    }

    pub fn right_keys(&self) -> &[AbstractExpr] {
        &self.right_keys
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

impl PlanNode for HashJoinPlan {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> &[Arc<AbstractPlanNode>] {
        &self.children
    }

    fn clone_with_children(&self, children: Vec<Arc<AbstractPlanNode>>) -> AbstractPlanNode {
        let mut children = expect_children("HashJoin", children, 2);
        let right = children.pop().unwrap();
        let left = children.pop().unwrap();
        Self::new(
            self.join_type,
            left,
            right,
            self.left_keys.clone(),
            self.right_keys.clone(),
            self.predicate.clone(),
        )
        .into()
    }
}
