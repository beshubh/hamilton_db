//! Removes identity projections, like the one the planner puts on `SELECT *`.
//! Renames count as changes: the root's column names are the output headers.

use std::sync::Arc;

use crate::expressions::AbstractExpr;
use crate::optimizer::Optimizer;
use crate::plans::{AbstractPlanNode, PlanNode, ProjectionPlan};

impl<'a> Optimizer<'a> {
    pub(crate) fn optimize_merge_projection(&self, plan: &AbstractPlanNode) -> AbstractPlanNode {
        let children = plan
            .get_children()
            .iter()
            .map(|child| Arc::new(self.optimize_merge_projection(child)))
            .collect();
        let plan = plan.clone_with_children(children);
        match &plan {
            AbstractPlanNode::ProjectionPlan(projection) if is_identity(projection) => {
                projection.child().as_ref().clone()
            }
            _ => plan,
        }
    }
}

fn is_identity(projection: &ProjectionPlan) -> bool {
    projection.get_output_schema() == projection.child().get_output_schema()
        && projection.exprs().iter().enumerate().all(|(idx, expr)| {
            matches!(expr, AbstractExpr::ColumnValueExpr(column)
                if column.tuple_idx() == 0 && column.col_idx() == idx)
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executors::test_util::Db;
    use crate::expressions::{ArithmeticExpr, ArithmeticOp, ColumnValueExpr, Expr};
    use crate::plans::{LimitPlan, SeqScanPlan};
    use common::{Column, DataType, Schema};

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

    fn project(exprs: Vec<AbstractExpr>, child: Arc<AbstractPlanNode>) -> AbstractPlanNode {
        let columns = exprs.iter().map(|e| e.return_type().clone()).collect();
        ProjectionPlan::new(Schema::new(columns), exprs, child).into()
    }

    fn select_star(child: Arc<AbstractPlanNode>) -> AbstractPlanNode {
        project(vec![column(0), column(1)], child)
    }

    fn optimize(plan: &AbstractPlanNode) -> AbstractPlanNode {
        let db = Db::new();
        Optimizer::new(db.catalog()).optimize_merge_projection(plan)
    }

    #[test]
    fn removes_a_projection_of_every_column_in_order() {
        let plan = optimize(&select_star(scan()));
        assert!(matches!(plan, AbstractPlanNode::SeqScanPlan(_)));
        assert_eq!(plan.get_output_schema(), &table_schema());
    }

    #[test]
    fn removes_projections_below_other_plans() {
        // `SELECT * FROM (SELECT * FROM users) LIMIT 1`, with the limit
        // under the outer projection.
        let inner = Arc::new(select_star(scan()));
        let limit = Arc::new(LimitPlan::new(1, inner).into());
        let plan = optimize(&select_star(limit));
        let AbstractPlanNode::LimitPlan(limit) = plan else {
            panic!("expected a limit, got {plan:?}");
        };
        assert!(matches!(
            *limit.get_child_at(0),
            AbstractPlanNode::SeqScanPlan(_)
        ));
    }

    #[test]
    fn keeps_a_projection_that_renames_a_column() {
        let schema = Schema::new(vec![
            Column::new("user_id", DataType::Int, false),
            Column::new("name", DataType::Varchar(16), true),
        ]);
        let plan: AbstractPlanNode =
            ProjectionPlan::new(schema, vec![column(0), column(1)], scan()).into();
        assert!(matches!(
            optimize(&plan),
            AbstractPlanNode::ProjectionPlan(_)
        ));
    }

    #[test]
    fn keeps_a_projection_that_drops_or_reorders_columns() {
        for exprs in [vec![column(0)], vec![column(1), column(0)]] {
            let plan = optimize(&project(exprs, scan()));
            assert!(matches!(plan, AbstractPlanNode::ProjectionPlan(_)));
        }
    }

    #[test]
    fn keeps_a_projection_that_computes_a_column() {
        // `SELECT id + id AS id, name`: the same schema as the scan, but not
        // the same values.
        let doubled = ArithmeticExpr::new(ArithmeticOp::Add, column(0), column(0)).unwrap();
        let schema = table_schema();
        let plan: AbstractPlanNode =
            ProjectionPlan::new(schema, vec![doubled.into(), column(1)], scan()).into();
        assert!(matches!(
            optimize(&plan),
            AbstractPlanNode::ProjectionPlan(_)
        ));
    }
}
