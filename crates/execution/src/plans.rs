//! Query plans: the tree of operators the planner builds and the executors
//! run.
//!
//! Every plan node is a variant of [`AbstractPlanNode`], and `enum_dispatch`
//! forwards [`PlanNode`] calls to the variant. Children are shared through
//! `Arc`, so an optimizer can rebuild one node with
//! [`PlanNode::clone_with_children`] without copying the subtrees below it.
//! Each plan lives in its own module under `plans/`.
//!
//! A plan's expressions read its input row: the child's output for a plan
//! with one child, and `tuple_idx` 0 (left) or 1 (right) for a join.

mod aggregation;
mod create_index;
mod create_table;
mod delete;
mod filter;
mod hash_join;
mod index_scan;
mod insert;
mod limit;
mod nested_index_join;
mod nested_loop_join;
mod projection;
mod seq_scan;
mod sort;
mod topn;
mod topn_per_group;
mod update;
mod values;
mod window;

pub use aggregation::{Aggregate, AggregationPlan};
pub use create_index::CreateIndexPlan;
pub use create_table::CreateTablePlan;
pub use delete::DeletePlan;
pub use filter::FilterPlan;
pub use hash_join::HashJoinPlan;
pub use index_scan::{IndexScanPlan, IndexScanRange, KeyBound};
pub use insert::InsertPlan;
pub use limit::LimitPlan;
pub use nested_index_join::NestedIndexJoinPlan;
pub use nested_loop_join::NestedLoopJoinPlan;
pub use projection::ProjectionPlan;
pub use seq_scan::SeqScanPlan;
pub use sort::{OrderBy, SortPlan};
pub use topn::TopNPlan;
pub use topn_per_group::TopNPerGroupPlan;
pub use update::UpdatePlan;
pub use values::ValuesPlan;
pub use window::{WindowColumn, WindowFunction, WindowFunctionKind, WindowPlan};

use common::{Column, DataType, Schema};
use enum_dispatch::enum_dispatch;
use std::sync::Arc;

#[enum_dispatch]
pub trait PlanNode {
    /// The layout of the tuples this plan produces.
    fn get_output_schema(&self) -> &Schema;

    /// Panics if there is no child `child_idx`.
    fn get_child_at(&self, child_idx: usize) -> Arc<AbstractPlanNode> {
        Arc::clone(&self.get_children()[child_idx])
    }

    fn get_children(&self) -> &[Arc<AbstractPlanNode>];

    /// A copy of this plan with its children replaced. Panics if `children`
    /// does not have as many plans as this plan takes.
    fn clone_with_children(&self, children: Vec<Arc<AbstractPlanNode>>) -> AbstractPlanNode;
}

#[enum_dispatch(PlanNode)]
#[derive(Clone, Debug)]
pub enum AbstractPlanNode {
    CreateTablePlan,
    CreateIndexPlan,
    SeqScanPlan,
    IndexScanPlan,
    ValuesPlan,
    InsertPlan,
    UpdatePlan,
    DeletePlan,
    FilterPlan,
    ProjectionPlan,
    NestedLoopJoinPlan,
    HashJoinPlan,
    NestedIndexJoinPlan,
    AggregationPlan,
    WindowPlan,
    SortPlan,
    LimitPlan,
    TopNPlan,
    TopNPerGroupPlan,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinType {
    /// Only the pairs that match. A cross join is an inner join without a
    /// condition.
    Inner,
    /// The pairs that match, plus each left row that matches nothing,
    /// padded with NULLs on the right.
    Left,
}

/// The output of a join: the left columns, then the right ones, which are
/// nullable for a LEFT join.
fn join_schema(left: &Schema, right: &Schema, join_type: JoinType) -> Schema {
    let right_columns = right.columns().iter().map(|column| {
        let mut column = column.clone();
        column.nullable |= join_type == JoinType::Left;
        column
    });
    Schema::new(
        left.columns()
            .iter()
            .cloned()
            .chain(right_columns)
            .collect(),
    )
}

/// The output of INSERT, UPDATE and DELETE: one row holding the number of
/// rows changed.
pub fn dml_output_schema() -> Schema {
    Schema::new(vec![Column::new("count", DataType::BigInt, false)])
}

/// Returns `children` after checking that `plan` takes that many.
fn expect_children(
    plan: &str,
    children: Vec<Arc<AbstractPlanNode>>,
    expected: usize,
) -> Vec<Arc<AbstractPlanNode>> {
    assert_eq!(
        children.len(),
        expected,
        "{plan} takes {expected} child plan(s), got {}",
        children.len()
    );
    children
}

/// The one child of a plan that takes one, for `clone_with_children`.
fn only_child(plan: &str, children: Vec<Arc<AbstractPlanNode>>) -> Arc<AbstractPlanNode> {
    let mut children = expect_children(plan, children, 1);
    children.pop().unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::{AbstractExpr, ColumnValueExpr, ConstantExpr, Expr};
    use common::Value;

    fn table_schema() -> Schema {
        Schema::new(vec![
            Column::new("id", DataType::Int, false),
            Column::new("name", DataType::Varchar(16), true),
        ])
    }

    fn scan() -> Arc<AbstractPlanNode> {
        Arc::new(SeqScanPlan::new(1, "users", table_schema(), None).into())
    }

    fn column(idx: usize) -> AbstractExpr {
        ColumnValueExpr::new(0, idx, table_schema().column(idx).unwrap().clone()).into()
    }

    fn truth() -> AbstractExpr {
        ConstantExpr::new(
            Value::Bool(true),
            Column::new("?column?", DataType::Bool, false),
        )
        .into()
    }

    #[test]
    fn seq_scan_outputs_the_table() {
        let plan = scan();
        assert_eq!(plan.get_output_schema(), &table_schema());
        assert!(plan.get_children().is_empty());
    }

    #[test]
    fn filter_sort_and_limit_keep_the_child_schema() {
        let filter: AbstractPlanNode = FilterPlan::new(truth(), scan()).into();
        let sort: AbstractPlanNode = SortPlan::new(
            vec![OrderBy {
                expr: column(0),
                desc: true,
            }],
            Arc::new(filter),
        )
        .into();
        let limit: AbstractPlanNode = LimitPlan::new(10, Arc::new(sort)).into();
        assert_eq!(limit.get_output_schema(), &table_schema());
    }

    #[test]
    fn inner_join_concatenates_both_sides() {
        let join = NestedLoopJoinPlan::new(JoinType::Inner, scan(), scan(), Some(truth()));
        let columns = join.get_output_schema().columns();
        let names: Vec<_> = columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["id", "name", "id", "name"]);
        assert!(!columns[2].nullable);
    }

    #[test]
    fn left_join_makes_the_right_side_nullable() {
        let join = NestedLoopJoinPlan::new(JoinType::Left, scan(), scan(), None);
        let nullable: Vec<_> = join
            .get_output_schema()
            .columns()
            .iter()
            .map(|c| c.nullable)
            .collect();
        assert_eq!(nullable, [false, true, true, true]);
    }

    #[test]
    fn aggregation_outputs_groups_then_aggregates() {
        let plan = AggregationPlan::new(
            vec![column(1)],
            vec![
                Aggregate::CountStar,
                Aggregate::Sum(column(0)),
                Aggregate::Max(column(0)),
            ],
            scan(),
        );
        let expected = Schema::new(vec![
            Column::new("name", DataType::Varchar(16), true),
            Column::new("count", DataType::BigInt, false),
            Column::new("sum", DataType::BigInt, true),
            Column::new("max", DataType::Int, true),
        ]);
        assert_eq!(plan.get_output_schema(), &expected);
    }

    #[test]
    fn dml_outputs_a_count() {
        let delete = DeletePlan::new(1, scan());
        assert_eq!(delete.get_output_schema(), &dml_output_schema());
        let update = UpdatePlan::new(1, vec![column(0), column(1)], scan());
        assert_eq!(update.get_output_schema(), &dml_output_schema());
    }

    #[test]
    fn clone_with_children_replaces_the_child() {
        let projection: AbstractPlanNode =
            ProjectionPlan::new(table_schema(), vec![column(0), column(1)], scan()).into();
        let values: Arc<AbstractPlanNode> =
            Arc::new(ValuesPlan::new(table_schema(), vec![]).into());
        let cloned = projection.clone_with_children(vec![Arc::clone(&values)]);
        assert!(Arc::ptr_eq(&cloned.get_child_at(0), &values));
        assert_eq!(cloned.get_output_schema(), projection.get_output_schema());
        let AbstractPlanNode::ProjectionPlan(cloned) = cloned else {
            panic!("expected a projection");
        };
        assert_eq!(cloned.exprs().len(), 2);
        assert_eq!(cloned.exprs()[1].return_type().name, "name");
    }

    fn other_schema() -> Schema {
        Schema::new(vec![Column::new("user_id", DataType::Int, false)])
    }

    fn other_scan() -> Arc<AbstractPlanNode> {
        Arc::new(SeqScanPlan::new(2, "orders", other_schema(), None).into())
    }

    fn nullable(schema: &Schema) -> Vec<bool> {
        schema.columns().iter().map(|c| c.nullable).collect()
    }

    #[test]
    fn index_scan_outputs_the_table() {
        let range = IndexScanRange::Range {
            lower: Some(KeyBound {
                key: vec![column(0)],
                inclusive: true,
            }),
            upper: None,
        };
        let plan = IndexScanPlan::new(1, 7, "users_id", table_schema(), range, None);
        assert_eq!(plan.get_output_schema(), &table_schema());
        assert!(plan.get_children().is_empty());
        assert_eq!(plan.index_oid(), 7);
    }

    #[test]
    fn hash_join_concatenates_both_sides() {
        let join = HashJoinPlan::new(
            JoinType::Left,
            scan(),
            other_scan(),
            vec![column(0)],
            vec![column(0)],
            None,
        );
        let expected = join_schema(&table_schema(), &other_schema(), JoinType::Left);
        assert_eq!(join.get_output_schema(), &expected);
        assert_eq!(nullable(join.get_output_schema()), [false, true, true]);
    }

    #[test]
    fn nested_index_join_appends_the_inner_table() {
        let join: AbstractPlanNode = NestedIndexJoinPlan::new(
            JoinType::Inner,
            scan(),
            2,
            other_schema(),
            9,
            "orders_user_id",
            vec![column(0)],
            None,
        )
        .into();
        let expected = join_schema(&table_schema(), &other_schema(), JoinType::Inner);
        assert_eq!(join.get_output_schema(), &expected);

        // A new outer side changes the outer columns and keeps the inner ones.
        let cloned = join.clone_with_children(vec![other_scan()]);
        let expected = join_schema(&other_schema(), &other_schema(), JoinType::Inner);
        assert_eq!(cloned.get_output_schema(), &expected);
    }

    #[test]
    fn topn_plans_keep_the_child_schema() {
        let order_by = || OrderBy {
            expr: column(0),
            desc: false,
        };
        let topn = TopNPlan::new(vec![order_by()], 3, scan());
        assert_eq!(topn.get_output_schema(), &table_schema());
        let per_group = TopNPerGroupPlan::new(vec![column(1)], vec![order_by()], 3, scan());
        assert_eq!(per_group.get_output_schema(), &table_schema());
    }

    #[test]
    fn window_function_types() {
        let window = |function| WindowFunction {
            function,
            partition_by: vec![column(1)],
            order_by: vec![],
        };
        let rank = window(WindowFunctionKind::Rank).return_type();
        assert_eq!(rank, Column::new("rank", DataType::BigInt, false));
        let max = window(WindowFunctionKind::Aggregate(Aggregate::Max(column(0))));
        assert_eq!(max.return_type(), Column::new("max", DataType::Int, true));
        let plan = WindowPlan::new(
            table_schema(),
            vec![
                WindowColumn::Expr(column(0)),
                WindowColumn::Window(window(WindowFunctionKind::RowNumber)),
            ],
            scan(),
        );
        assert_eq!(
            plan.columns()[0].return_type(),
            column(0).return_type().clone()
        );
    }

    #[test]
    #[should_panic(expected = "takes 2 child plan(s), got 1")]
    fn clone_with_wrong_number_of_children_panics() {
        let join: AbstractPlanNode =
            NestedLoopJoinPlan::new(JoinType::Inner, scan(), scan(), None).into();
        join.clone_with_children(vec![scan()]);
    }
}
