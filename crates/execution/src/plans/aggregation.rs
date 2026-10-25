use std::sync::Arc;

use common::{Column, DataType, Schema};

use super::{AbstractPlanNode, PlanNode, only_child};
use crate::expressions::{AbstractExpr, Expr};

/// An aggregate function over the rows of a group. All but `COUNT(*)` skip
/// rows where their argument is NULL.
#[derive(Clone, Debug)]
pub enum Aggregate {
    /// `COUNT(*)`: the number of rows.
    CountStar,
    /// `COUNT(expr)`: the number of non-NULL values.
    Count(AbstractExpr),
    /// `SUM(expr)`: BIGINT for integers, DOUBLE for doubles.
    Sum(AbstractExpr),
    Min(AbstractExpr),
    Max(AbstractExpr),
    /// `AVG(expr)` of numbers, as DOUBLE.
    Avg(AbstractExpr),
}

impl Aggregate {
    /// The expression aggregated, or `None` for `COUNT(*)`.
    pub fn arg(&self) -> Option<&AbstractExpr> {
        match self {
            Aggregate::CountStar => None,
            Aggregate::Count(expr)
            | Aggregate::Sum(expr)
            | Aggregate::Min(expr)
            | Aggregate::Max(expr)
            | Aggregate::Avg(expr) => Some(expr),
        }
    }

    /// The column this aggregate produces. The counts are never NULL; the
    /// others are NULL for a group with no non-NULL values.
    pub fn return_type(&self) -> Column {
        match self {
            Aggregate::CountStar | Aggregate::Count(_) => {
                Column::new("count", DataType::BigInt, false)
            }
            Aggregate::Sum(expr) => {
                let data_type = match expr.return_type().data_type {
                    DataType::Double => DataType::Double,
                    _ => DataType::BigInt,
                };
                Column::new("sum", data_type, true)
            }
            Aggregate::Avg(_) => Column::new("avg", DataType::Double, true),
            Aggregate::Min(expr) => Column::new("min", expr.return_type().data_type, true),
            Aggregate::Max(expr) => Column::new("max", expr.return_type().data_type, true),
        }
    }
}

/// Groups the child's rows by `group_bys` and computes `aggregates` for each
/// group. Without group-bys all rows form one group, so it outputs one row
/// even when the child produces none.
#[derive(Clone, Debug)]
pub struct AggregationPlan {
    /// The group-by values, then the aggregates, in order.
    output_schema: Schema,
    /// Always `[child]`.
    children: Vec<Arc<AbstractPlanNode>>,
    /// Expressions on the child's row.
    group_bys: Vec<AbstractExpr>,
    aggregates: Vec<Aggregate>,
}

impl AggregationPlan {
    pub fn new(
        group_bys: Vec<AbstractExpr>,
        aggregates: Vec<Aggregate>,
        child: Arc<AbstractPlanNode>,
    ) -> Self {
        let group_columns = group_bys.iter().map(|expr| expr.return_type().clone());
        let aggregate_columns = aggregates.iter().map(Aggregate::return_type);
        Self {
            output_schema: Schema::new(group_columns.chain(aggregate_columns).collect()),
            children: vec![child],
            group_bys,
            aggregates,
        }
    }

    pub fn group_bys(&self) -> &[AbstractExpr] {
        &self.group_bys
    }

    pub fn aggregates(&self) -> &[Aggregate] {
        &self.aggregates
    }

    pub fn child(&self) -> &Arc<AbstractPlanNode> {
        &self.children[0]
    }
}

impl PlanNode for AggregationPlan {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> &[Arc<AbstractPlanNode>] {
        &self.children
    }

    fn clone_with_children(&self, children: Vec<Arc<AbstractPlanNode>>) -> AbstractPlanNode {
        Self {
            children: vec![only_child("Aggregation", children)],
            ..self.clone()
        }
        .into()
    }
}
