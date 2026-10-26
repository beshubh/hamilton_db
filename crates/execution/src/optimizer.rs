use catalog::Catalog;

use crate::plans::AbstractPlanNode;

mod expr;
pub mod merge_projection;
pub mod nlj_as_hash_join;
pub mod orderby_as_indexscan;
pub mod predicate_pushdown;
pub mod projection_pushdown;
pub mod seqscan_as_indexscan;
#[cfg(test)]
#[path = "../tests/util/optimizer.rs"]
mod test_util;

pub struct Optimizer<'a> {
    catalog: &'a Catalog,
}

impl<'a> Optimizer<'a> {
    pub fn new(catalog: &'a Catalog) -> Self {
        Self { catalog }
    }

    /// Order matters:
    /// - Predicate pushdown first, so later rules see the WHERE in scans and joins.
    /// - WHERE picks the index before ORDER BY: narrowing beats skipping a sort.
    /// - Projection pushdown last: its projections hide scans from the index rules.
    pub fn optimize(&self, plan: &AbstractPlanNode) -> AbstractPlanNode {
        let plan = self.optimize_merge_projection(plan);
        let plan = self.optimize_predicate_pushdown(&plan);
        let plan = self.optimize_nlj_as_hash_join(&plan);
        let plan = self.optimize_seqscan_as_indexscan(&plan);
        let plan = self.optimize_orderby_as_indexscan(&plan);
        self.optimize_projection_pushdown(&plan)
    }
}
