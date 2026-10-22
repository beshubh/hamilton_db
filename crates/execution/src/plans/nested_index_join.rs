use std::sync::Arc;

use catalog::{IndexOid, TableOid};
use common::Schema;

use super::{AbstractPlanNode, JoinType, PlanNode, join_schema, only_child};
use crate::expressions::AbstractExpr;

/// Joins its child (the outer side) with a table (the inner side) by looking
/// up each outer row's key in an index on the table. A key with a NULL
/// matches nothing, as with `=`.
#[derive(Clone, Debug)]
pub struct NestedIndexJoinPlan {
    /// See [`join_schema`]; the inner table is the right side.
    output_schema: Schema,
    /// Always `[outer]`.
    children: Vec<Arc<AbstractPlanNode>>,
    join_type: JoinType,
    inner_table_oid: TableOid,
    /// The inner table's schema.
    inner_schema: Schema,
    index_oid: IndexOid,
    index_name: String,
    /// The lookup key: one expression per key column of the index, in key
    /// order, on the outer row.
    key_exprs: Vec<AbstractExpr>,
    /// The rest of the ON condition, checked on each pair the index finds,
    /// reading the outer row through `tuple_idx` 0 and the inner through 1.
    predicate: Option<AbstractExpr>,
}

impl NestedIndexJoinPlan {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        join_type: JoinType,
        outer: Arc<AbstractPlanNode>,
        inner_table_oid: TableOid,
        inner_schema: Schema,
        index_oid: IndexOid,
        index_name: impl Into<String>,
        key_exprs: Vec<AbstractExpr>,
        predicate: Option<AbstractExpr>,
    ) -> Self {
        Self {
            output_schema: join_schema(outer.get_output_schema(), &inner_schema, join_type),
            children: vec![outer],
            join_type,
            inner_table_oid,
            inner_schema,
            index_oid,
            index_name: index_name.into(),
            key_exprs,
            predicate,
        }
    }

    pub fn join_type(&self) -> JoinType {
        self.join_type
    }

    pub fn inner_table_oid(&self) -> TableOid {
        self.inner_table_oid
    }

    pub fn inner_schema(&self) -> &Schema {
        &self.inner_schema
    }

    pub fn index_oid(&self) -> IndexOid {
        self.index_oid
    }

    pub fn index_name(&self) -> &str {
        &self.index_name
    }

    pub fn key_exprs(&self) -> &[AbstractExpr] {
        &self.key_exprs
    }

    pub fn predicate(&self) -> Option<&AbstractExpr> {
        self.predicate.as_ref()
    }

    pub fn outer(&self) -> &Arc<AbstractPlanNode> {
        &self.children[0]
    }
}

impl PlanNode for NestedIndexJoinPlan {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> &[Arc<AbstractPlanNode>] {
        &self.children
    }

    fn clone_with_children(&self, children: Vec<Arc<AbstractPlanNode>>) -> AbstractPlanNode {
        let outer = only_child("NestedIndexJoin", children);
        Self {
            output_schema: join_schema(
                outer.get_output_schema(),
                &self.inner_schema,
                self.join_type,
            ),
            children: vec![outer],
            ..self.clone()
        }
        .into()
    }
}
