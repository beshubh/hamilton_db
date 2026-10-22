use std::sync::Arc;

use catalog::{IndexOid, TableOid};
use common::Schema;

use super::{AbstractPlanNode, PlanNode, expect_children};
use crate::expressions::AbstractExpr;

/// Which entries of the index to read. Key expressions are evaluated without
/// an input row and have one expression per key column, in key order, unless
/// stated otherwise.
#[derive(Clone, Debug)]
pub enum IndexScanRange {
    /// Every entry.
    Full,
    /// The entries equal to one of these keys, each key looked up once.
    Points(Vec<Vec<AbstractExpr>>),
    /// The entries between two bounds; `None` leaves that end open.
    Range {
        lower: Option<KeyBound>,
        upper: Option<KeyBound>,
    },
}

/// One end of an [`IndexScanRange::Range`].
#[derive(Clone, Debug)]
pub struct KeyBound {
    /// Values for a prefix of the key columns; entries are compared on that
    /// prefix only.
    pub key: Vec<AbstractExpr>,
    pub inclusive: bool,
}

/// Reads the table rows that an index points to, in key order, keeping
/// those that pass `filter`.
#[derive(Clone, Debug)]
pub struct IndexScanPlan {
    /// The table's schema.
    output_schema: Schema,
    table_oid: TableOid,
    index_oid: IndexOid,
    index_name: String,
    range: IndexScanRange,
    /// A BOOL condition on the table's row, for the part of the WHERE that
    /// `range` does not cover; `None` keeps every row.
    filter: Option<AbstractExpr>,
}

impl IndexScanPlan {
    pub fn new(
        table_oid: TableOid,
        index_oid: IndexOid,
        index_name: impl Into<String>,
        table_schema: Schema,
        range: IndexScanRange,
        filter: Option<AbstractExpr>,
    ) -> Self {
        Self {
            output_schema: table_schema,
            table_oid,
            index_oid,
            index_name: index_name.into(),
            range,
            filter,
        }
    }

    pub fn table_oid(&self) -> TableOid {
        self.table_oid
    }

    pub fn index_oid(&self) -> IndexOid {
        self.index_oid
    }

    pub fn index_name(&self) -> &str {
        &self.index_name
    }

    pub fn range(&self) -> &IndexScanRange {
        &self.range
    }

    pub fn filter(&self) -> Option<&AbstractExpr> {
        self.filter.as_ref()
    }
}

impl PlanNode for IndexScanPlan {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> &[Arc<AbstractPlanNode>] {
        &[]
    }

    fn clone_with_children(&self, children: Vec<Arc<AbstractPlanNode>>) -> AbstractPlanNode {
        expect_children("IndexScan", children, 0);
        self.clone().into()
    }
}
