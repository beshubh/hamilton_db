use std::sync::Arc;

use common::{Column, DataType, Schema};

use super::{AbstractPlanNode, Aggregate, OrderBy, PlanNode, only_child};
use crate::expressions::{AbstractExpr, Expr};

#[derive(Clone, Debug)]
pub enum WindowFunctionKind {
    /// An aggregate over the row's window frame.
    Aggregate(Aggregate),
    /// `RANK()`: one more than the number of rows of the partition that sort
    /// before this one, so rows tied on the sort keys share a rank.
    Rank,
    /// `ROW_NUMBER()`: the row's position in the partition, from 1.
    RowNumber,
}

/// `function OVER (PARTITION BY partition_by ORDER BY order_by)`.
///
/// Without ORDER BY an aggregate covers the whole partition. With it, the
/// frame runs from the start of the partition to the current row and the
/// rows tied with it, as PostgreSQL's default frame does.
#[derive(Clone, Debug)]
pub struct WindowFunction {
    pub function: WindowFunctionKind,
    /// Expressions on the child's row.
    pub partition_by: Vec<AbstractExpr>,
    pub order_by: Vec<OrderBy>,
}

impl WindowFunction {
    /// The column this function produces. Ranks and row numbers are BIGINT
    /// and never NULL; aggregates are as in [`Aggregate::return_type`].
    pub fn return_type(&self) -> Column {
        match &self.function {
            WindowFunctionKind::Aggregate(aggregate) => aggregate.return_type(),
            WindowFunctionKind::Rank => Column::new("rank", DataType::BigInt, false),
            WindowFunctionKind::RowNumber => Column::new("row_number", DataType::BigInt, false),
        }
    }
}

/// One output column of a [`WindowPlan`].
#[derive(Clone, Debug)]
pub enum WindowColumn {
    /// An expression on the child's row.
    Expr(AbstractExpr),
    Window(WindowFunction),
}

impl WindowColumn {
    /// The column this produces, before the select list names it.
    pub fn return_type(&self) -> Column {
        match self {
            WindowColumn::Expr(expr) => expr.return_type().clone(),
            WindowColumn::Window(function) => function.return_type(),
        }
    }
}

/// A projection whose columns can be window functions: one output row per
/// child row, with each window function computed over the row's partition.
#[derive(Clone, Debug)]
pub struct WindowPlan {
    /// One column per entry of `columns`, named by the select list.
    output_schema: Schema,
    /// Always `[child]`.
    children: Vec<Arc<AbstractPlanNode>>,
    columns: Vec<WindowColumn>,
}

impl WindowPlan {
    pub fn new(
        output_schema: Schema,
        columns: Vec<WindowColumn>,
        child: Arc<AbstractPlanNode>,
    ) -> Self {
        debug_assert_eq!(output_schema.len(), columns.len());
        Self {
            output_schema,
            children: vec![child],
            columns,
        }
    }

    pub fn columns(&self) -> &[WindowColumn] {
        &self.columns
    }

    pub fn child(&self) -> &Arc<AbstractPlanNode> {
        &self.children[0]
    }
}

impl PlanNode for WindowPlan {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> &[Arc<AbstractPlanNode>] {
        &self.children
    }

    fn clone_with_children(&self, children: Vec<Arc<AbstractPlanNode>>) -> AbstractPlanNode {
        Self {
            children: vec![only_child("Window", children)],
            ..self.clone()
        }
        .into()
    }
}
