use std::collections::VecDeque;
use std::ops::Bound;
use std::sync::Arc;

use catalog::{IndexInfo, TableInfo};
use common::{Rid, Schema, Tuple, Value};
use storage::index::btree_index::KeySchemaComparator;
use storage::index::{Index, IndexTrait};
use storage::page::btree_page::KeyComparator;

use super::{Executor, passes};
use crate::Result;
use crate::expressions::{AbstractExpr, Expr};
use crate::plans::{IndexScanPlan, IndexScanRange, KeyBound, PlanNode};

/// Reads the table rows that the plan's index entries point to, in key
/// order, keeping those that pass the plan's filter.
///
/// `init` reads the record ids of every entry in the plan's range up front,
/// and `next` fetches their rows a batch at a time. Reading the index as the
/// rows are produced would keep a leaf latched between calls to `next`,
/// blocking any writer to the index above it, and would let an UPDATE above
/// it see a row again after moving it further along the index.
pub struct IndexScanExecutor {
    plan: IndexScanPlan,
    table: Arc<TableInfo>,
    index: Arc<IndexInfo>,
    /// Record ids of the rows not produced yet, in key order. Set by `init`.
    rids: VecDeque<Rid>,
}

impl IndexScanExecutor {
    pub fn new(plan: IndexScanPlan, table: Arc<TableInfo>, index: Arc<IndexInfo>) -> Self {
        Self {
            plan,
            table,
            index,
            rids: VecDeque::new(),
        }
    }

    /// The record ids of the entries in the plan's range, in key order.
    fn scan_index(&self) -> Result<Vec<Rid>> {
        let Index::BtreeIndex(btree) = &*self.index.index;
        let mut rids = Vec::new();
        match self.plan.range() {
            IndexScanRange::Full => {
                for rid in btree.scan_range(Bound::Unbounded, Bound::Unbounded)? {
                    rids.push(rid?);
                }
            }
            IndexScanRange::Points(points) => {
                let key_schema = &self.index.key_schema;
                let mut keys = points
                    .iter()
                    .map(|key| Ok(Tuple::encode(key_schema, &evaluate(key)?)?))
                    .collect::<Result<Vec<_>>>()?;
                let cmp = KeySchemaComparator::new(Arc::new(key_schema.clone()));
                keys.sort_by(|a, b| cmp.compare(a.data(), b.data()));
                keys.dedup_by(|a, b| cmp.compare(a.data(), b.data()).is_eq());
                for key in &keys {
                    btree.scan_key(key, &mut rids)?;
                }
            }
            IndexScanRange::Range { lower, upper } => {
                let (lower, upper) = (bound_values(lower)?, bound_values(upper)?);
                for rid in btree.scan_range(as_bound(&lower), as_bound(&upper))? {
                    rids.push(rid?);
                }
            }
        }
        Ok(rids)
    }
}

impl Executor for IndexScanExecutor {
    fn init(&mut self) -> Result<()> {
        self.rids = self.scan_index()?.into();
        Ok(())
    }

    fn next(
        &mut self,
        tuples: &mut Vec<Tuple>,
        rids: &mut Vec<Rid>,
        batch_size: usize,
    ) -> Result<bool> {
        tuples.clear();
        rids.clear();
        while tuples.len() < batch_size {
            let Some(rid) = self.rids.pop_front() else {
                break;
            };
            let (meta, tuple) = self.table.heap.get(rid)?;
            if meta.is_deleted {
                continue;
            }
            if let Some(filter) = self.plan.filter()
                && !passes(filter, &tuple, self.plan.get_output_schema())?
            {
                continue;
            }
            rids.push(rid);
            tuples.push(tuple);
        }
        Ok(!tuples.is_empty())
    }

    fn get_output_schema(&self) -> &Schema {
        self.plan.get_output_schema()
    }
}

/// The values of key expressions, which read no input.
fn evaluate(key: &[AbstractExpr]) -> Result<Vec<Value>> {
    let (input, input_schema) = (Tuple::default(), Schema::default());
    key.iter()
        .map(|expr| expr.evaluate(&input, &input_schema))
        .collect()
}

/// The values of `bound`, and whether they are included; `None` for an open
/// end.
fn bound_values(bound: &Option<KeyBound>) -> Result<Bound<Vec<Value>>> {
    Ok(match bound {
        Some(KeyBound {
            key,
            inclusive: true,
        }) => Bound::Included(evaluate(key)?),
        Some(KeyBound {
            key,
            inclusive: false,
        }) => Bound::Excluded(evaluate(key)?),
        None => Bound::Unbounded,
    })
}

fn as_bound(bound: &Bound<Vec<Value>>) -> Bound<&[Value]> {
    bound.as_ref().map(Vec::as_slice)
}

#[cfg(test)]
mod tests {
    use common::{DataType, Value};

    use super::super::test_util::{
        BATCH_SIZE, Db, column, compare, constant, int, user, users_schema,
    };
    use super::super::{Executor, create_executor};
    use crate::ExecutionError;
    use crate::expressions::{AbstractExpr, ComparisonOp};
    use crate::plans::{AbstractPlanNode, IndexScanPlan, IndexScanRange, KeyBound};

    /// A scan of the users table through index `name`, which must exist.
    fn scan(
        db: &Db,
        name: &str,
        range: IndexScanRange,
        filter: Option<AbstractExpr>,
    ) -> AbstractPlanNode {
        let index = db.catalog().get_index(name).unwrap();
        IndexScanPlan::new(db.users.oid, index.oid, name, users_schema(), range, filter).into()
    }

    fn text(s: &str) -> AbstractExpr {
        constant(Value::from(s), DataType::Varchar(32))
    }

    fn bound(key: Vec<AbstractExpr>, inclusive: bool) -> Option<KeyBound> {
        Some(KeyBound { key, inclusive })
    }

    /// A database with an index `users_id` on `id`, and the users with
    /// `ids`, in that order.
    fn with_ids(ids: &[i32]) -> Db {
        let db = Db::new();
        db.catalog()
            .create_index("users_id", "users", &["id"])
            .unwrap();
        let names: Vec<String> = ids.iter().map(|id| format!("user {id}")).collect();
        let rows: Vec<_> = ids
            .iter()
            .zip(&names)
            .map(|(&id, name)| (id, Some(name.as_str())))
            .collect();
        db.insert(&rows);
        db
    }

    fn ids(rows: Vec<Vec<Value>>) -> Vec<i32> {
        rows.into_iter()
            .map(|row| match row[0] {
                Value::Int(id) => id,
                ref v => panic!("{v:?}"),
            })
            .collect()
    }

    #[test]
    fn full_scan_reads_rows_in_key_order() {
        let db = with_ids(&[3, 1, 5, 2, 4]);
        let rows = db
            .run(scan(&db, "users_id", IndexScanRange::Full, None))
            .unwrap();
        assert_eq!(ids(rows), [1, 2, 3, 4, 5]);
    }

    #[test]
    fn reads_rows_across_pages() {
        let ids_desc: Vec<i32> = (1..=300).rev().collect();
        let db = with_ids(&ids_desc);
        let rows = db
            .run(scan(&db, "users_id", IndexScanRange::Full, None))
            .unwrap();
        assert_eq!(ids(rows), (1..=300).collect::<Vec<_>>());
    }

    #[test]
    fn points_are_read_once_each_in_key_order() {
        let db = with_ids(&[1, 2, 3, 4, 5]);
        let points = vec![vec![int(4)], vec![int(2)], vec![int(9)], vec![int(4)]];
        let rows = db
            .run(scan(&db, "users_id", IndexScanRange::Points(points), None))
            .unwrap();
        assert_eq!(rows, [user(2, Some("user 2")), user(4, Some("user 4"))]);
    }

    #[test]
    fn a_point_reads_every_row_with_its_key() {
        let db = Db::new();
        db.catalog()
            .create_index("users_name", "users", &["name"])
            .unwrap();
        db.insert(&[(1, Some("a")), (2, Some("b")), (3, Some("a"))]);
        let points = IndexScanRange::Points(vec![vec![text("a")]]);
        let rows = db.run(scan(&db, "users_name", points, None)).unwrap();
        assert_eq!(rows, [user(1, Some("a")), user(3, Some("a"))]);
    }

    #[test]
    fn ranges_respect_their_bounds() {
        let db = with_ids(&[5, 4, 3, 2, 1]);
        let range = |lower, upper| {
            let rows = db.run(scan(
                &db,
                "users_id",
                IndexScanRange::Range { lower, upper },
                None,
            ));
            ids(rows.unwrap())
        };
        assert_eq!(
            range(bound(vec![int(2)], true), bound(vec![int(4)], true)),
            [2, 3, 4]
        );
        assert_eq!(
            range(bound(vec![int(2)], false), bound(vec![int(4)], false)),
            [3]
        );
        assert_eq!(range(None, bound(vec![int(3)], false)), [1, 2]);
        assert_eq!(range(bound(vec![int(3)], true), None), [3, 4, 5]);
        assert_eq!(range(None, None), [1, 2, 3, 4, 5]);
        assert!(range(bound(vec![int(4)], true), bound(vec![int(2)], true)).is_empty());
    }

    /// Bounds on a two column index give values for the first column only.
    #[test]
    fn ranges_on_a_key_prefix() {
        let db = Db::new();
        db.catalog()
            .create_index("users_name_id", "users", &["name", "id"])
            .unwrap();
        db.insert(&[
            (4, Some("b")),
            (1, Some("a")),
            (3, Some("b")),
            (2, Some("c")),
            (5, Some("a")),
        ]);
        // name = 'b'
        let range = IndexScanRange::Range {
            lower: bound(vec![text("b")], true),
            upper: bound(vec![text("b")], true),
        };
        let rows = db.run(scan(&db, "users_name_id", range, None)).unwrap();
        assert_eq!(rows, [user(3, Some("b")), user(4, Some("b"))]);
        // name > 'a' AND (name, id) < ('c', 2)
        let range = IndexScanRange::Range {
            lower: bound(vec![text("a")], false),
            upper: bound(vec![text("c"), int(2)], false),
        };
        let rows = db.run(scan(&db, "users_name_id", range, None)).unwrap();
        assert_eq!(rows, [user(3, Some("b")), user(4, Some("b"))]);
    }

    #[test]
    fn applies_its_filter() {
        let db = with_ids(&[1, 2, 3, 4, 5, 6]);
        let range = IndexScanRange::Range {
            lower: bound(vec![int(2)], true),
            upper: None,
        };
        let not_user_4 = compare(ComparisonOp::NotEq, column(1), text("user 4"));
        let rows = db
            .run(scan(&db, "users_id", range, Some(not_user_4)))
            .unwrap();
        assert_eq!(ids(rows), [2, 3, 5, 6]);
    }

    #[test]
    fn produces_the_rids_of_its_rows() {
        let db = with_ids(&[3, 1, 2]);
        let plan = scan(&db, "users_id", IndexScanRange::Full, None);
        let mut executor = create_executor(&db.ctx, &plan).unwrap();
        executor.init().unwrap();
        let (mut tuples, mut rids) = (Vec::new(), Vec::new());
        let mut seen = 0;
        while executor.next(&mut tuples, &mut rids, BATCH_SIZE).unwrap() {
            assert_eq!(rids.len(), tuples.len());
            for (tuple, rid) in tuples.iter().zip(&rids) {
                assert_eq!(tuple.rid(), Some(*rid));
                let (_, stored) = db.users.heap.get(*rid).unwrap();
                assert_eq!(stored.data(), tuple.data());
            }
            seen += tuples.len();
        }
        assert_eq!(seen, 3);
        // init starts the scan over.
        executor.init().unwrap();
        assert!(executor.next(&mut tuples, &mut rids, 10).unwrap());
        assert_eq!(tuples.len(), 3);
    }

    #[test]
    fn skips_deleted_rows() {
        let db = with_ids(&[1, 2, 3]);
        let plan = scan(&db, "users_id", IndexScanRange::Full, None);
        let mut executor = create_executor(&db.ctx, &plan).unwrap();
        executor.init().unwrap();
        // Deleted after the index was read, so its entry was still there.
        let rid = db.index_lookup("users_id", &[Value::Int(2)])[0];
        db.users.heap.mark_delete(rid, 0, 0).unwrap();
        let (mut tuples, mut rids) = (Vec::new(), Vec::new());
        let mut rows = Vec::new();
        while executor.next(&mut tuples, &mut rids, BATCH_SIZE).unwrap() {
            rows.extend(tuples.iter().map(|t| t.decode(&users_schema()).unwrap()));
        }
        assert_eq!(ids(rows), [1, 3]);
    }

    #[test]
    fn unknown_index_fails() {
        let db = Db::new();
        let plan = IndexScanPlan::new(
            db.users.oid,
            999,
            "gone",
            users_schema(),
            IndexScanRange::Full,
            None,
        );
        assert!(matches!(
            create_executor(&db.ctx, &plan.into()),
            Err(ExecutionError::IndexNotFound(999))
        ));
    }
}
