use std::sync::Arc;

use catalog::TableOid;
use common::Schema;

use super::{AbstractPlanNode, PlanNode, expect_children};
use crate::expressions::AbstractExpr;

/// Reads every visible tuple of a table, keeping those that pass `filter`.
#[derive(Clone, Debug)]
pub struct SeqScanPlan {
    /// The table's schema.
    output_schema: Schema,
    table_oid: TableOid,
    table_name: String,
    /// A BOOL condition on the table's row; `None` keeps every tuple.
    filter: Option<AbstractExpr>,
}

impl SeqScanPlan {
    pub fn new(
        table_oid: TableOid,
        table_name: impl Into<String>,
        table_schema: Schema,
        filter: Option<AbstractExpr>,
    ) -> Self {
        Self {
            output_schema: table_schema,
            table_oid,
            table_name: table_name.into(),
            filter,
        }
    }

    pub fn table_oid(&self) -> TableOid {
        self.table_oid
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn filter(&self) -> Option<&AbstractExpr> {
        self.filter.as_ref()
    }
}

impl PlanNode for SeqScanPlan {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> &[Arc<AbstractPlanNode>] {
        &[]
    }

    fn clone_with_children(&self, children: Vec<Arc<AbstractPlanNode>>) -> AbstractPlanNode {
        expect_children("SeqScan", children, 0);
        self.clone().into()
    }
}
