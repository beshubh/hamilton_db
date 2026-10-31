use std::cmp::Ordering;
use std::collections::VecDeque;
use std::ops::Bound;
use std::sync::Arc;

use catalog::{IndexInfo, TableInfo};
use common::{Rid, Schema, Tuple, Value};
use storage::index::btree_index::{KeySchemaComparator, compare_key_prefix};
use storage::index::{Index, IndexTrait, RowLocator};
use storage::page::btree_page::KeyComparator;

use super::{Executor, passes};
use crate::Result;
use crate::execution_context::ExecutionContext;
use crate::expressions::{AbstractExpr, Expr};
use crate::plans::{IndexScanPlan, IndexScanRange, KeyBound, PlanNode};

/// Reads the table rows that the plan's index entries point to, in key
/// order, keeping those that pass the plan's filter.
///
/// `init` reads the locators of every entry in the plan's range up front,
/// and `next` fetches their rows a batch at a time. Reading the index as the
/// rows are produced would keep a leaf latched between calls to `next`,
/// blocking any writer to the index above it, and would let an UPDATE above
/// it see a row again after moving it further along the index.
///
/// If the index stores primary keys, `next` looks each one up in the primary
/// key index to find the rid, skipping rows that are gone.
///
/// Rows are read as the statement's transaction sees them, skipping those it
/// doesn't see. Index entries are not versioned, so the version it sees may
/// have a key other than its entry's: a row whose key is no longer in the
/// plan's range is skipped too.
pub struct IndexScanExecutor {
    ctx: Arc<ExecutionContext>,
    plan: IndexScanPlan,
    /// Locators of the rows not produced yet, in key order. Set by `init`.
    locators: VecDeque<RowLocator>,
    /// The keys the rows must have. Set by `init`.
    keys: KeyRange,
}

/// The keys an index scan reads, as values.
enum KeyRange {
    Full,
    Points(Vec<Vec<Value>>),
    Range(Bound<Vec<Value>>, Bound<Vec<Value>>),
}

impl KeyRange {
    /// Whether `key` (a row's key, as values) is in this range.
    fn contains(&self, key: &[Value]) -> bool {
        match self {
            KeyRange::Full => true,
            KeyRange::Points(points) => points
                .iter()
                .any(|point| compare_key_prefix(key, point).is_eq()),
            KeyRange::Range(lower, upper) => {
                let above_lower = match lower {
                    Bound::Included(p) => compare_key_prefix(key, p) != Ordering::Less,
                    Bound::Excluded(p) => compare_key_prefix(key, p) == Ordering::Greater,
                    Bound::Unbounded => true,
                };
                let below_upper = match upper {
                    Bound::Included(p) => compare_key_prefix(key, p) != Ordering::Greater,
                    Bound::Excluded(p) => compare_key_prefix(key, p) == Ordering::Less,
                    Bound::Unbounded => true,
                };
                above_lower && below_upper
            }
        }
    }
}

impl IndexScanExecutor {
    pub fn new(ctx: Arc<ExecutionContext>, plan: IndexScanPlan) -> Self {
        Self {
            ctx,
            plan,
            locators: VecDeque::new(),
            keys: KeyRange::Full,
        }
    }

    /// The plan's range as values, and the locators of the entries in it, in
    /// key order.
    fn scan_index(&self) -> Result<(KeyRange, Vec<RowLocator>)> {
        let index = self.ctx.index(self.plan.index_oid())?;
        let Index::BtreeIndex(btree) = &*index.index;
        let mut locators = Vec::new();
        let keys = match self.plan.range() {
            IndexScanRange::Full => {
                for locator in btree.scan_range(Bound::Unbounded, Bound::Unbounded)? {
                    locators.push(locator?);
                }
                KeyRange::Full
            }
            IndexScanRange::Points(points) => {
                let key_schema = &index.key_schema;
                let mut keys = points
                    .iter()
                    .map(|key| Ok(Tuple::encode(key_schema, &evaluate(key)?)?))
                    .collect::<Result<Vec<_>>>()?;
                let cmp = KeySchemaComparator::new(Arc::new(key_schema.clone()));
                keys.sort_by(|a, b| cmp.compare(a.data(), b.data()));
                keys.dedup_by(|a, b| cmp.compare(a.data(), b.data()).is_eq());
                for key in &keys {
                    btree.scan_key(key, &mut locators)?;
                }
                let values = keys
                    .iter()
                    .map(|key| key.decode(key_schema))
                    .collect::<Result<_, _>>()?;
                KeyRange::Points(values)
            }
            IndexScanRange::Range { lower, upper } => {
                let (lower, upper) = (bound_values(lower)?, bound_values(upper)?);
                for locator in btree.scan_range(as_bound(&lower), as_bound(&upper))? {
                    locators.push(locator?);
                }
                KeyRange::Range(lower, upper)
            }
        };
        Ok((keys, locators))
    }
}

impl Executor for IndexScanExecutor {
    fn init(&mut self) -> Result<()> {
        let (keys, locators) = self.scan_index()?;
        self.keys = keys;
        self.locators = locators.into();
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
        let table = self.ctx.table(self.plan.table_oid())?;
        let index = self.ctx.index(self.plan.index_oid())?;
        while tuples.len() < batch_size {
            let Some(locator) = self.locators.pop_front() else {
                break;
            };
            let Some(rid) = self.ctx.locate(&table, &locator)? else {
                continue;
            };
            let Some((_, tuple)) = self.ctx.txn().get(&table.heap, &table.schema, rid)? else {
                continue;
            };
            if !self.keys.contains(&key_values(&table, &index, &tuple)?) {
                continue;
            }
            if let Some(filter) = self.plan.filter()
                && !passes(filter, &tuple, self.plan.get_output_schema())?
            {
                continue;
            }
            rids.push(rid);
            tuples.push(tuple.with_rid(rid));
        }
        Ok(!tuples.is_empty())
    }

    fn get_output_schema(&self) -> &Schema {
        self.plan.get_output_schema()
    }
}

/// The values of `tuple`'s key in `index`, an index on `table`.
pub(super) fn key_values(
    table: &TableInfo,
    index: &IndexInfo,
    tuple: &Tuple,
) -> Result<Vec<Value>> {
    index
        .key_columns
        .iter()
        .map(|&column| Ok(tuple.value(&table.schema, column)?))
        .collect()
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
    use common::{DataType, Tuple, Value};

    use super::super::test_util::{
        BATCH_SIZE, Db, account, account_column, accounts_schema, column, compare, constant, int,
        user, users_schema,
    };
    use super::super::{Executor, create_executor};
    use crate::ExecutionError;
    use crate::expressions::{AbstractExpr, ComparisonOp};
    use crate::expressions::{ArithmeticExpr, ArithmeticOp};
    use crate::plans::{AbstractPlanNode, IndexScanPlan, IndexScanRange, KeyBound, UpdatePlan};
    use storage::index::{IndexTrait, RowLocator};

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

    /// Accounts 1 to 4, and a function that scans them through
    /// `accounts_name`, which stores primary keys.
    fn accounts_by_name() -> (Db, impl Fn(&Db, IndexScanRange) -> AbstractPlanNode) {
        let db = Db::new();
        db.create_accounts(&[
            (3, Some("b"), 30),
            (1, Some("a"), 10),
            (4, None, 40),
            (2, Some("a"), 20),
        ]);
        let scan = |db: &Db, range| {
            let table = db.catalog().get_table("accounts").unwrap();
            let index = db.catalog().get_index("accounts_name").unwrap();
            let schema = accounts_schema();
            IndexScanPlan::new(table.oid, index.oid, &index.name, schema, range, None).into()
        };
        (db, scan)
    }

    #[test]
    fn reads_through_an_index_that_points_by_primary_key() {
        let (db, scan) = accounts_by_name();
        assert_eq!(
            db.run(scan(&db, IndexScanRange::Full)).unwrap(),
            [
                account(4, None, 40),
                account(1, Some("a"), 10),
                account(2, Some("a"), 20),
                account(3, Some("b"), 30),
            ]
        );
        let points = IndexScanRange::Points(vec![vec![text("a")], vec![text("z")]]);
        assert_eq!(
            db.run(scan(&db, points)).unwrap(),
            [account(1, Some("a"), 10), account(2, Some("a"), 20)]
        );
        let range = IndexScanRange::Range {
            lower: bound(vec![text("a")], false),
            upper: None,
        };
        assert_eq!(
            db.run(scan(&db, range)).unwrap(),
            [account(3, Some("b"), 30)]
        );

        // Every row moves, leaving the index alone; it still finds them.
        let table = db.catalog().get_table("accounts").unwrap();
        let plus_1 = ArithmeticExpr::new(ArithmeticOp::Add, account_column(2), int(1)).unwrap();
        let targets = vec![account_column(0), account_column(1), plus_1.into()];
        let update = UpdatePlan::new(
            table.oid,
            &table.name,
            targets,
            db.scan_accounts(&table, None),
        );
        db.run(update.into()).unwrap();
        let points = IndexScanRange::Points(vec![vec![text("a")]]);
        assert_eq!(
            db.run(scan(&db, points)).unwrap(),
            [account(1, Some("a"), 11), account(2, Some("a"), 21)]
        );
    }

    /// A row whose primary key is missing from the primary key index is
    /// skipped.
    #[test]
    fn primary_keys_not_in_the_primary_key_index_are_skipped() {
        let (db, scan) = accounts_by_name();
        let pkey = db.catalog().get_index("accounts_pkey").unwrap();
        let [rid] = db
            .index_lookup("accounts_pkey", &[Value::Int(1)])
            .try_into()
            .unwrap();
        let key = Tuple::encode(&pkey.key_schema, &[Value::Int(1)]).unwrap();
        pkey.index
            .delete_entry(&key, &RowLocator::Rid(rid))
            .unwrap();
        let points = IndexScanRange::Points(vec![vec![text("a")]]);
        assert_eq!(
            db.run(scan(&db, points)).unwrap(),
            [account(2, Some("a"), 20)]
        );
    }

    /// A newer transaction moves user 1 to id 10 in place, which moves its
    /// index entry at once. An older one still sees id 1, so the entry for
    /// 10 leads it to a row whose key isn't 10: it is skipped.
    #[test]
    fn rows_whose_visible_key_left_the_range_are_skipped() {
        let db = with_ids(&[1, 2, 3]);
        let older = db.tm.begin();
        let id_1 = compare(ComparisonOp::Eq, column(0), int(1));
        let update = UpdatePlan::new(
            db.users.oid,
            &db.users.name,
            vec![int(10), column(1)],
            db.scan(Some(id_1)),
        );
        db.run(update.into()).unwrap();

        let point = |id| IndexScanRange::Points(vec![vec![int(id)]]);
        let at_10 = db.run_in(&older, scan(&db, "users_id", point(10), None));
        assert!(at_10.unwrap().is_empty());
        let range = IndexScanRange::Range {
            lower: bound(vec![int(5)], true),
            upper: None,
        };
        assert!(
            db.run_in(&older, scan(&db, "users_id", range, None))
                .unwrap()
                .is_empty()
        );
        db.tm.commit(&older).unwrap();
        assert_eq!(
            db.run(scan(&db, "users_id", point(10), None)).unwrap(),
            [user(10, Some("user 1"))]
        );
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
        let mut executor = create_executor(&db.ctx(), &plan).unwrap();
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
        let mut executor = create_executor(&db.ctx(), &plan).unwrap();
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
            db.run(plan.into()),
            Err(ExecutionError::IndexNotFound(999))
        ));
    }
}
