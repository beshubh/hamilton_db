use std::sync::Arc;

use common::Schema;

use super::{AbstractPlanNode, PlanNode, expect_children};
use crate::expressions::AbstractExpr;

/// Produces a fixed list of rows, each expression evaluated without an input
/// row: the rows of `INSERT ... VALUES`, or the one empty row of a SELECT
/// without FROM.
#[derive(Clone, Debug)]
pub struct ValuesPlan {
    output_schema: Schema,
    /// One expression per output column in every row.
    rows: Vec<Vec<AbstractExpr>>,
}

impl ValuesPlan {
    pub fn new(output_schema: Schema, rows: Vec<Vec<AbstractExpr>>) -> Self {
        Self {
            output_schema,
            rows,
        }
    }

    pub fn rows(&self) -> &[Vec<AbstractExpr>] {
        &self.rows
    }
}

impl PlanNode for ValuesPlan {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> &[Arc<AbstractPlanNode>] {
        &[]
    }

    fn clone_with_children(&self, children: Vec<Arc<AbstractPlanNode>>) -> AbstractPlanNode {
        expect_children("Values", children, 0);
        self.clone().into()
    }
}
