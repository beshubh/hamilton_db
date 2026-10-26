//! Drops a sort when an index yields rows in its order:
//! `Sort(IndexScan)` -> the index scan; `Sort([Filter](SeqScan))` -> a full index scan.
//!
//! In order: every sort key is an ascending column, matching the index keys
//! after those the range fixes (fixed ones may appear anywhere).
//! The index puts NULLs first, a sort last, so a nullable key needs its NULLs out of range.

use std::cmp::Ordering;
use std::sync::Arc;

use catalog::IndexInfo;
use common::{Schema, Value};

use super::seqscan_as_indexscan::TableScan;
use crate::expressions::AbstractExpr;
use crate::optimizer::Optimizer;
use crate::plans::{AbstractPlanNode, IndexScanRange, KeyBound, OrderBy, PlanNode};

impl<'a> Optimizer<'a> {
    pub(crate) fn optimize_orderby_as_indexscan(
        &self,
        plan: &AbstractPlanNode,
    ) -> AbstractPlanNode {
        let children = plan
            .get_children()
            .iter()
            .map(|child| Arc::new(self.optimize_orderby_as_indexscan(child)))
            .collect();
        let plan = plan.clone_with_children(children);
        let AbstractPlanNode::SortPlan(sort) = &plan else {
            return plan;
        };
        let order_bys = sort.order_bys();
        let child = sort.child().as_ref();
        if let AbstractPlanNode::IndexScanPlan(index_scan) = child {
            let Some(index) = self.catalog.get_index_by_oid(index_scan.index_oid()) else {
                return plan;
            };
            let fixed = FixedKeys::of(index_scan.range(), index.key_columns.len());
            if reads_in_order(&index, child.get_output_schema(), &fixed, order_bys) {
                return child.clone();
            }
        } else if let Some(scan) = TableScan::of(child) {
            let schema = child.get_output_schema();
            for index in self.catalog.table_indexes(scan.seq_scan.table_name()) {
                if reads_in_order(&index, schema, &FixedKeys::NONE, order_bys) {
                    let conjuncts = scan.conjuncts.iter().copied();
                    return scan
                        .index_scan(&index, IndexScanRange::Full, conjuncts)
                        .into();
                }
            }
        }
        plan
    }
}

/// Key columns an index scan's range fixes.
struct FixedKeys {
    /// Leading key columns with one value in every row.
    len: usize,
    /// The key column after them has no NULLs in range.
    next_not_null: bool,
}

impl FixedKeys {
    const NONE: Self = Self {
        len: 0,
        next_not_null: false,
    };

    fn of(range: &IndexScanRange, key_len: usize) -> Self {
        match range {
            IndexScanRange::Full => Self::NONE,
            IndexScanRange::Points(points) if points.len() == 1 => Self {
                len: key_len,
                next_not_null: false,
            },
            // Points are read in key order.
            IndexScanRange::Points(points) => Self {
                len: 0,
                next_not_null: points.iter().all(|point| {
                    point
                        .first()
                        .and_then(constant)
                        .is_some_and(|value| !value.is_null())
                }),
            },
            IndexScanRange::Range { lower, upper } => Self::of_range(lower, upper),
        }
    }

    /// Values shared by both bounds' leading keys are fixed.
    fn of_range(lower: &Option<KeyBound>, upper: &Option<KeyBound>) -> Self {
        let Some(lower) = lower else {
            return Self::NONE;
        };
        let len = match upper {
            Some(upper) => lower
                .key
                .iter()
                .zip(&upper.key)
                .take_while(|(l, u)| match (constant(l), constant(u)) {
                    (Some(l), Some(u)) => matches!(l.compare(u), Ok(Some(Ordering::Equal))),
                    _ => false,
                })
                .count(),
            None => 0,
        };
        // NULLs come first, so a lower bound excludes them; a NULL bound only as `> NULL`.
        let next_not_null = match lower.key.get(len).map(constant) {
            Some(Some(Value::Null)) => !lower.inclusive && lower.key.len() == len + 1,
            Some(Some(_)) => true,
            _ => false,
        };
        Self { len, next_not_null }
    }
}

fn constant(expr: &AbstractExpr) -> Option<&Value> {
    match expr {
        AbstractExpr::ConstantExpr(constant) => Some(constant.value()),
        _ => None,
    }
}

/// Whether `index`, with `fixed` keys, yields rows in `order_bys` order.
fn reads_in_order(
    index: &IndexInfo,
    schema: &Schema,
    fixed: &FixedKeys,
    order_bys: &[OrderBy],
) -> bool {
    let fixed_columns = &index.key_columns[..fixed.len];
    let mut next_key = fixed.len;
    for order_by in order_bys {
        let AbstractExpr::ColumnValueExpr(column) = &order_by.expr else {
            return false;
        };
        if order_by.desc || column.tuple_idx() != 0 {
            return false;
        }
        let col_idx = column.col_idx();
        if fixed_columns.contains(&col_idx) {
            continue;
        }
        if index.key_columns.get(next_key) != Some(&col_idx) {
            return false;
        }
        let nullable = schema.column(col_idx).is_some_and(|column| column.nullable);
        if nullable && !(next_key == fixed.len && fixed.next_not_null) {
            return false;
        }
        next_key += 1;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executors::test_util::{Db, column, compare, int};
    use crate::expressions::ComparisonOp;
    use crate::optimizer::test_util::{
        assert_same_rows, db_with_index, db_with_indexes, filter, index_scan, sort, text,
    };
    use crate::plans::LimitPlan;

    /// Both index rules, in the order `Optimizer::optimize` runs them.
    fn optimize(db: &Db, plan: &AbstractPlanNode) -> AbstractPlanNode {
        let optimizer = Optimizer::new(db.catalog());
        let plan = optimizer.optimize_seqscan_as_indexscan(plan);
        optimizer.optimize_orderby_as_indexscan(&plan)
    }

    fn assert_sorts(plan: &AbstractPlanNode) {
        assert!(
            matches!(plan, AbstractPlanNode::SortPlan(_)),
            "expected a sort, got {plan:?}"
        );
    }

    #[test]
    fn an_index_on_the_sort_keys_replaces_the_sort() {
        let db = db_with_index("users_id", &["id"]);
        let plan = sort(db.scan(None), &[(0, false)]);
        let optimized = optimize(&db, &plan);
        let scan = index_scan(&optimized);
        assert!(matches!(scan.range(), IndexScanRange::Full));
        assert!(scan.filter().is_none());
        assert_same_rows(&db, &plan, &optimized);
    }

    #[test]
    fn a_filter_no_index_covers_moves_into_the_index_scan() {
        let db = db_with_index("users_id", &["id"]);
        let not_b = compare(ComparisonOp::NotEq, column(1), text("b"));
        let plan = sort(filter(&db, not_b), &[(0, false)]);
        let optimized = optimize(&db, &plan);
        let scan = index_scan(&optimized);
        assert!(matches!(scan.range(), IndexScanRange::Full));
        assert!(scan.filter().is_some());
        assert_same_rows(&db, &plan, &optimized);
    }

    #[test]
    fn removes_the_sort_above_a_range_on_the_sort_key() {
        let db = db_with_index("users_id", &["id"]);
        let plan = sort(
            filter(&db, compare(ComparisonOp::Gt, column(0), int(2))),
            &[(0, false)],
        );
        let optimized = optimize(&db, &plan);
        assert!(matches!(
            index_scan(&optimized).range(),
            IndexScanRange::Range { .. }
        ));
        assert_same_rows(&db, &plan, &optimized);
    }

    #[test]
    fn keeps_sorts_the_index_order_does_not_match() {
        let db = db_with_index("users_id", &["id"]);
        // Descending, or not on the key.
        for keys in [[(0, true)], [(1, false)]] {
            assert_sorts(&optimize(&db, &sort(db.scan(None), &keys)));
        }
        // The WHERE rule still narrows the scan under the sort.
        let narrowed = filter(&db, compare(ComparisonOp::Gt, column(0), int(2)));
        let plan = sort(narrowed, &[(0, true)]);
        let optimized = optimize(&db, &plan);
        assert_sorts(&optimized);
        index_scan(&optimized.get_child_at(0));
        assert_same_rows(&db, &plan, &optimized);
    }

    #[test]
    fn keeps_the_sort_when_the_where_picks_another_index() {
        let db = db_with_indexes(&[("users_id", &["id"]), ("users_name", &["name"])]);
        // WHERE name = 'b' ORDER BY id: the range on name comes first.
        let plan = sort(
            filter(&db, compare(ComparisonOp::Eq, column(1), text("b"))),
            &[(0, false)],
        );
        let optimized = optimize(&db, &plan);
        assert_sorts(&optimized);
        assert_eq!(
            index_scan(&optimized.get_child_at(0)).index_name(),
            "users_name"
        );
        assert_same_rows(&db, &plan, &optimized);
    }

    #[test]
    fn a_nullable_key_sorts_only_when_its_nulls_are_left_out() {
        let db = db_with_index("users_name", &["name"]);
        assert_sorts(&optimize(&db, &sort(db.scan(None), &[(1, false)])));

        for op in [ComparisonOp::GtEq, ComparisonOp::Lt] {
            let plan = sort(
                filter(&db, compare(op, column(1), text("b"))),
                &[(1, false)],
            );
            let optimized = optimize(&db, &plan);
            index_scan(&optimized);
            assert_same_rows(&db, &plan, &optimized);
        }
    }

    #[test]
    fn sorts_on_the_key_after_an_equality_prefix() {
        let db = db_with_index("users_name_id", &["name", "id"]);
        let name_b = || compare(ComparisonOp::Eq, column(1), text("b"));
        // WHERE name = 'b' ORDER BY id, and ORDER BY name, id.
        for keys in [&[(0, false)][..], &[(1, false), (0, false)]] {
            let plan = sort(filter(&db, name_b()), keys);
            let optimized = optimize(&db, &plan);
            index_scan(&optimized);
            assert_same_rows(&db, &plan, &optimized);
        }
        // id comes after name in the key.
        assert_sorts(&optimize(&db, &sort(db.scan(None), &[(0, false)])));
    }

    #[test]
    fn a_point_holds_every_key_column() {
        let db = db_with_index("users_id", &["id"]);
        // WHERE id = 3 ORDER BY id
        let plan = sort(
            filter(&db, compare(ComparisonOp::Eq, column(0), int(3))),
            &[(0, false)],
        );
        let optimized = optimize(&db, &plan);
        assert!(matches!(
            index_scan(&optimized).range(),
            IndexScanRange::Points(_)
        ));
        assert_same_rows(&db, &plan, &optimized);
    }

    #[test]
    fn rewrites_sorts_below_other_plans() {
        let db = db_with_index("users_id", &["id"]);
        let sorted = Arc::new(sort(db.scan(None), &[(0, false)]));
        let plan: AbstractPlanNode = LimitPlan::new(2, sorted).into();
        let optimized = optimize(&db, &plan);
        index_scan(&optimized.get_child_at(0));
        assert_eq!(db.run(optimized).unwrap(), db.run(plan).unwrap());
    }
}
