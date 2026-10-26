//! `[Filter](SeqScan)` -> index scan when `column <op> constant` conjuncts
//! narrow an index: equalities on a key prefix, then bounds on the next key.
//! Covered conjuncts are dropped, the rest stay as the filter.
//! The index fixing the most key columns wins; no statistics.
//!
//! NULLs come first in the index, so an upper bound alone on a nullable column adds `> NULL`.
//! Constants must convert losslessly to the key type: `int_col < 2.5` stays in the filter.

use std::sync::Arc;

use catalog::IndexInfo;
use common::{Column, DataType, Value};

use super::expr::{conjunction, conjuncts};
use crate::expressions::{AbstractExpr, ComparisonOp, ConstantExpr, Expr};
use crate::optimizer::Optimizer;
use crate::plans::{
    AbstractPlanNode, IndexScanPlan, IndexScanRange, KeyBound, PlanNode, SeqScanPlan,
};

impl<'a> Optimizer<'a> {
    pub(crate) fn optimize_seqscan_as_indexscan(
        &self,
        plan: &AbstractPlanNode,
    ) -> AbstractPlanNode {
        if let Some(scan) = TableScan::of(plan) {
            return match self.choose_index(&scan) {
                Some(index_scan) => index_scan.into(),
                None => plan.clone(),
            };
        }
        let children = plan
            .get_children()
            .iter()
            .map(|child| Arc::new(self.optimize_seqscan_as_indexscan(child)))
            .collect();
        plan.clone_with_children(children)
    }

    fn choose_index(&self, scan: &TableScan) -> Option<IndexScanPlan> {
        let mut best: Option<Candidate> = None;
        for index in self.catalog.table_indexes(scan.seq_scan.table_name()) {
            let candidate = Candidate::new(scan, &index);
            let used = candidate.key_columns_used;
            if used > 0
                && best
                    .as_ref()
                    .is_none_or(|best| used > best.key_columns_used)
            {
                best = Some(candidate);
            }
        }
        best.map(|candidate| candidate.into_plan(scan))
    }
}

/// `SeqScan` or `Filter(SeqScan)`, with both conditions split into conjuncts.
pub(super) struct TableScan<'p> {
    pub(super) seq_scan: &'p SeqScanPlan,
    pub(super) conjuncts: Vec<&'p AbstractExpr>,
}

impl<'p> TableScan<'p> {
    pub(super) fn of(plan: &'p AbstractPlanNode) -> Option<Self> {
        let (seq_scan, predicate) = match plan {
            AbstractPlanNode::SeqScanPlan(seq_scan) => (seq_scan, None),
            AbstractPlanNode::FilterPlan(filter) => match filter.child().as_ref() {
                AbstractPlanNode::SeqScanPlan(seq_scan) => (seq_scan, Some(filter.predicate())),
                _ => return None,
            },
            _ => return None,
        };
        let conjuncts = seq_scan
            .filter()
            .into_iter()
            .chain(predicate)
            .flat_map(conjuncts)
            .collect();
        Some(Self {
            seq_scan,
            conjuncts,
        })
    }

    fn column(&self, col_idx: usize) -> &Column {
        self.seq_scan
            .get_output_schema()
            .column(col_idx)
            .expect("index key columns are table columns")
    }

    pub(super) fn index_scan<'c>(
        &self,
        index: &IndexInfo,
        range: IndexScanRange,
        conjuncts: impl IntoIterator<Item = &'c AbstractExpr>,
    ) -> IndexScanPlan {
        let filter = conjunction(conjuncts.into_iter().cloned());
        IndexScanPlan::new(
            self.seq_scan.table_oid(),
            index.oid,
            &index.name,
            self.seq_scan.get_output_schema().clone(),
            range,
            filter,
        )
    }
}

/// `column <op> non-NULL constant`, either way round, as `(column, op, value)`.
fn column_comparison(conjunct: &AbstractExpr) -> Option<(usize, ComparisonOp, &Value)> {
    let AbstractExpr::ComparisonExpr(comparison) = conjunct else {
        return None;
    };
    let [left, right] = comparison.children() else {
        unreachable!("a comparison has two operands");
    };
    let (column, constant, op) = match (left, right) {
        (AbstractExpr::ColumnValueExpr(column), AbstractExpr::ConstantExpr(constant)) => {
            (column, constant, comparison.op())
        }
        (AbstractExpr::ConstantExpr(constant), AbstractExpr::ColumnValueExpr(column)) => {
            (column, constant, flip(comparison.op()))
        }
        _ => return None,
    };
    if column.tuple_idx() != 0 || constant.value().is_null() {
        return None;
    }
    Some((column.col_idx(), op, constant.value()))
}

/// `a < b` is `b > a`.
fn flip(op: ComparisonOp) -> ComparisonOp {
    match op {
        ComparisonOp::Lt => ComparisonOp::Gt,
        ComparisonOp::LtEq => ComparisonOp::GtEq,
        ComparisonOp::Gt => ComparisonOp::Lt,
        ComparisonOp::GtEq => ComparisonOp::LtEq,
        ComparisonOp::Eq | ComparisonOp::NotEq => op,
    }
}

/// `value` in `column`'s type, if lossless.
fn key_value(value: &Value, column: &Column) -> Option<Value> {
    match (column.data_type, value) {
        (DataType::Int, Value::Int(_))
        | (DataType::BigInt, Value::BigInt(_))
        | (DataType::Double, Value::Double(_))
        | (DataType::Bool, Value::Bool(_)) => Some(value.clone()),
        (DataType::Varchar(max), Value::Varchar(s)) if s.len() <= max as usize => {
            Some(value.clone())
        }
        (DataType::Int, Value::BigInt(v)) => i32::try_from(*v).ok().map(Value::Int),
        (DataType::BigInt, Value::Int(v)) => Some(Value::BigInt(i64::from(*v))),
        (DataType::Double, Value::Int(v)) => Some(Value::Double(f64::from(*v))),
        _ => None,
    }
}

struct ColumnBound {
    conjunct_idx: usize,
    value: Value,
    inclusive: bool,
}

/// The range an index gives a table scan.
struct Candidate {
    index: Arc<IndexInfo>,
    /// `(conjunct_idx, value)` per leading key column.
    equalities: Vec<(usize, Value)>,
    /// Bounds on the key column after the equalities.
    lower: Option<ColumnBound>,
    upper: Option<ColumnBound>,
    range_column_nullable: bool,
    key_columns_used: usize,
}

impl Candidate {
    fn new(scan: &TableScan, index: &Arc<IndexInfo>) -> Self {
        let comparisons: Vec<_> = scan
            .conjuncts
            .iter()
            .map(|conjunct| column_comparison(conjunct))
            .collect();
        // First convertible `key column <op> value` with `wanted(op)`.
        let find = |key_idx: usize, wanted: &dyn Fn(ComparisonOp) -> bool| {
            let col_idx = index.key_columns[key_idx];
            let key_column = index.key_schema.column(key_idx).expect("a key column");
            comparisons
                .iter()
                .enumerate()
                .find_map(|(conjunct_idx, comparison)| match comparison {
                    Some((col, op, value)) if *col == col_idx && wanted(*op) => {
                        let value = key_value(value, key_column)?;
                        Some((conjunct_idx, *op, value))
                    }
                    _ => None,
                })
        };

        let mut equalities = Vec::new();
        while equalities.len() < index.key_columns.len()
            && let Some((conjunct_idx, _, value)) =
                find(equalities.len(), &|op| op == ComparisonOp::Eq)
        {
            equalities.push((conjunct_idx, value));
        }

        let range_key = equalities.len();
        let (mut lower, mut upper, mut range_column_nullable) = (None, None, false);
        if range_key < index.key_columns.len() {
            let is_lower = |op| matches!(op, ComparisonOp::Gt | ComparisonOp::GtEq);
            let is_upper = |op| matches!(op, ComparisonOp::Lt | ComparisonOp::LtEq);
            let bound = |(conjunct_idx, op, value)| ColumnBound {
                conjunct_idx,
                value,
                inclusive: matches!(op, ComparisonOp::GtEq | ComparisonOp::LtEq),
            };
            lower = find(range_key, &is_lower).map(bound);
            upper = find(range_key, &is_upper).map(bound);
            range_column_nullable = scan.column(index.key_columns[range_key]).nullable;
        }
        let key_columns_used = range_key + usize::from(lower.is_some() || upper.is_some());
        Self {
            index: Arc::clone(index),
            equalities,
            lower,
            upper,
            range_column_nullable,
            key_columns_used,
        }
    }

    /// Filters on the conjuncts the range doesn't cover.
    fn into_plan(self, scan: &TableScan) -> IndexScanPlan {
        let mut used = vec![false; scan.conjuncts.len()];
        for &(conjunct_idx, _) in &self.equalities {
            used[conjunct_idx] = true;
        }
        for bound in self.lower.iter().chain(&self.upper) {
            used[bound.conjunct_idx] = true;
        }
        let rest = scan
            .conjuncts
            .iter()
            .zip(&used)
            .filter(|(_, used)| !**used)
            .map(|(conjunct, _)| *conjunct);
        scan.index_scan(&self.index, self.range(), rest)
    }

    fn range(&self) -> IndexScanRange {
        let key_len = self.index.key_schema.len();
        let prefix: Vec<AbstractExpr> = self
            .equalities
            .iter()
            .enumerate()
            .map(|(key_idx, (_, value))| self.key_constant(key_idx, value.clone()))
            .collect();
        if prefix.len() == key_len {
            return IndexScanRange::Points(vec![prefix]);
        }
        if prefix.is_empty() && self.lower.is_none() && self.upper.is_none() {
            return IndexScanRange::Full;
        }
        let range_key = prefix.len();
        // `prefix` + bound, or `prefix` inclusive if unbounded.
        let end = |bound: &Option<ColumnBound>| match bound {
            Some(bound) => {
                let mut key = prefix.clone();
                key.push(self.key_constant(range_key, bound.value.clone()));
                Some(KeyBound {
                    key,
                    inclusive: bound.inclusive,
                })
            }
            None if prefix.is_empty() => None,
            None => Some(KeyBound {
                key: prefix.clone(),
                inclusive: true,
            }),
        };
        let mut lower = end(&self.lower);
        if self.lower.is_none() && self.upper.is_some() && self.range_column_nullable {
            // Skip the NULLs, which come first and fail `column < value`.
            let mut key = prefix.clone();
            key.push(self.key_constant(range_key, Value::Null));
            lower = Some(KeyBound {
                key,
                inclusive: false,
            });
        }
        IndexScanRange::Range {
            lower,
            upper: end(&self.upper),
        }
    }

    fn key_constant(&self, key_idx: usize, value: Value) -> AbstractExpr {
        let column = self.index.key_schema.column(key_idx).expect("a key column");
        ConstantExpr::new(value, column.clone()).into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executors::test_util::{Db, column, compare, constant, int};
    use crate::expressions::{LogicExpr, LogicOp};
    use crate::optimizer::test_util::{
        and, assert_same_rows, db_with_index, db_with_indexes, filter, index_scan, sort, text,
    };

    fn optimize(db: &Db, plan: &AbstractPlanNode) -> AbstractPlanNode {
        Optimizer::new(db.catalog()).optimize_seqscan_as_indexscan(plan)
    }
    #[test]
    fn an_equality_on_the_whole_key_is_a_point() {
        let db = db_with_index("users_id", &["id"]);
        let plan = Arc::unwrap_or_clone(filter(&db, compare(ComparisonOp::Eq, column(0), int(3))));
        let optimized = optimize(&db, &plan);
        let scan = index_scan(&optimized);
        assert!(matches!(scan.range(), IndexScanRange::Points(points) if points.len() == 1));
        assert!(scan.filter().is_none());
        assert_same_rows(&db, &plan, &optimized);
    }

    #[test]
    fn the_constant_can_be_on_either_side() {
        let db = db_with_index("users_id", &["id"]);
        // 2 < id AND id <= 5
        let predicate = and(
            compare(ComparisonOp::Lt, int(2), column(0)),
            compare(ComparisonOp::LtEq, column(0), int(5)),
        );
        let plan = Arc::unwrap_or_clone(filter(&db, predicate));
        let optimized = optimize(&db, &plan);
        let IndexScanRange::Range { lower, upper } = index_scan(&optimized).range() else {
            panic!("expected a range");
        };
        assert!(!lower.as_ref().unwrap().inclusive);
        assert!(upper.as_ref().unwrap().inclusive);
        assert_same_rows(&db, &plan, &optimized);
    }

    #[test]
    fn conjuncts_the_range_does_not_cover_stay_in_the_filter() {
        let db = db_with_index("users_id", &["id"]);
        // id >= 2 AND name <> 'b' AND id > 1
        let predicate = and(
            and(
                compare(ComparisonOp::GtEq, column(0), int(2)),
                compare(ComparisonOp::NotEq, column(1), text("b")),
            ),
            compare(ComparisonOp::Gt, column(0), int(1)),
        );
        let plan = Arc::unwrap_or_clone(filter(&db, predicate));
        let optimized = optimize(&db, &plan);
        let filter = index_scan(&optimized).filter().unwrap();
        assert!(matches!(filter, AbstractExpr::LogicExpr(_)));
        assert_same_rows(&db, &plan, &optimized);
    }

    #[test]
    fn keeps_scans_no_index_applies_to() {
        let db = db_with_index("users_id", &["id"]);
        let not_one = compare(ComparisonOp::NotEq, column(0), int(1));
        let on_name = compare(ComparisonOp::Eq, column(1), text("a"));
        let either = LogicExpr::new(
            LogicOp::Or,
            compare(ComparisonOp::Eq, column(0), int(1)),
            compare(ComparisonOp::Eq, column(0), int(2)),
        )
        .unwrap()
        .into();
        let double = constant(Value::Double(2.5), DataType::Double);
        let not_an_int = compare(ComparisonOp::Lt, column(0), double);
        for predicate in [not_one, on_name, either, not_an_int] {
            let plan = Arc::unwrap_or_clone(filter(&db, predicate));
            assert!(matches!(
                optimize(&db, &plan),
                AbstractPlanNode::FilterPlan(_)
            ));
        }
        let plan = Arc::unwrap_or_clone(db.scan(None));
        assert!(matches!(
            optimize(&db, &plan),
            AbstractPlanNode::SeqScanPlan(_)
        ));
    }

    #[test]
    fn a_bigint_constant_bounds_an_int_column() {
        let db = db_with_index("users_id", &["id"]);
        let three = constant(Value::BigInt(3), DataType::BigInt);
        let plan = Arc::unwrap_or_clone(filter(&db, compare(ComparisonOp::Eq, column(0), three)));
        let optimized = optimize(&db, &plan);
        index_scan(&optimized);
        assert_same_rows(&db, &plan, &optimized);
    }

    #[test]
    fn an_upper_bound_on_a_nullable_column_leaves_out_nulls() {
        let db = db_with_index("users_name", &["name"]);
        let plan =
            Arc::unwrap_or_clone(filter(&db, compare(ComparisonOp::Lt, column(1), text("c"))));
        let optimized = optimize(&db, &plan);
        index_scan(&optimized);
        assert_eq!(db.run(optimized.clone()).unwrap().len(), 3);
        assert_same_rows(&db, &plan, &optimized);
    }

    #[test]
    fn picks_the_index_that_constrains_the_most_key_columns() {
        let db = db_with_indexes(&[
            ("users_name", &["name"]),
            ("users_name_id", &["name", "id"]),
        ]);
        let predicate = and(
            compare(ComparisonOp::Eq, column(1), text("b")),
            compare(ComparisonOp::Eq, column(0), int(3)),
        );
        let plan = Arc::unwrap_or_clone(filter(&db, predicate));
        let optimized = optimize(&db, &plan);
        let scan = index_scan(&optimized);
        assert_eq!(scan.index_name(), "users_name_id");
        assert!(scan.filter().is_none());
        assert_same_rows(&db, &plan, &optimized);
    }

    #[test]
    fn leaves_sorts_to_the_order_by_rule() {
        let db = db_with_index("users_id", &["id"]);
        // An index on the sort key alone does not replace the scan.
        let plan = sort(db.scan(None), &[(0, false)]);
        let optimized = optimize(&db, &plan);
        let AbstractPlanNode::SortPlan(kept) = &optimized else {
            panic!("expected a sort, got {optimized:?}");
        };
        assert!(matches!(
            kept.child().as_ref(),
            AbstractPlanNode::SeqScanPlan(_)
        ));
        // The scan under a sort is narrowed, and the sort kept.
        let narrowed = filter(&db, compare(ComparisonOp::Gt, column(0), int(2)));
        let plan = sort(narrowed, &[(0, false)]);
        let optimized = optimize(&db, &plan);
        let AbstractPlanNode::SortPlan(kept) = &optimized else {
            panic!("expected a sort, got {optimized:?}");
        };
        index_scan(kept.child());
        assert_same_rows(&db, &plan, &optimized);
    }
}
