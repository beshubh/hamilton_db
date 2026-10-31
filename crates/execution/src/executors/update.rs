use std::collections::HashSet;
use std::sync::Arc;

use catalog::IndexInfo;
use common::{Rid, Schema, Tuple};
use storage::index::{IndexTrait, LocatorKind};
use storage::table::table_page::{INVALID_UNDO_PTR, TupleMeta};

use super::{
    AbstractExecutor, COMMIT_TS, Executor, count_row, duplicate_key, index_keys, locators,
};
use crate::Result;
use crate::execution_context::ExecutionContext;
use crate::expressions::Expr;
use crate::plans::{PlanNode, UpdatePlan};

/// Updates all of the child's rows on the first call to `next`, and produces
/// one row holding how many there were. The child reads the table, so every
/// row it produces has a rid.
///
/// It reads every row before updating any, so a row it moves is not read
/// again by a scan still running below it. There are no transactions to roll
/// back yet, so it also computes every new row and checks the primary keys
/// before writing: a row that fails to evaluate or a duplicate key updates
/// nothing. An error from storage part way through still leaves the rows
/// before it updated.
///
/// An update deletes the old tuple and inserts the new one, so the row gets a
/// new rid. Index entries that store the rid are always rewritten. Entries
/// that store the primary key are rewritten only if the index key or the
/// primary key changed.
///
/// All old entries are removed before new ones are added, so
/// `SET id = id + 1` doesn't collide with itself.
// TODO: update in place, keeping the rid, once there is an undo log.
pub struct UpdateExecutor {
    ctx: Arc<ExecutionContext>,
    plan: UpdatePlan,
    child: Box<AbstractExecutor>,
    child_tuples: Vec<Tuple>,
    child_rids: Vec<Rid>,
    done: bool,
}

impl UpdateExecutor {
    pub fn new(ctx: Arc<ExecutionContext>, plan: UpdatePlan, child: Box<AbstractExecutor>) -> Self {
        Self {
            ctx,
            plan,
            child,
            child_tuples: Vec::new(),
            child_rids: Vec::new(),
            done: false,
        }
    }
}

impl Executor for UpdateExecutor {
    fn init(&mut self) -> Result<()> {
        self.done = false;
        self.child.init()
    }

    fn next(
        &mut self,
        tuples: &mut Vec<Tuple>,
        rids: &mut Vec<Rid>,
        batch_size: usize,
    ) -> Result<bool> {
        tuples.clear();
        rids.clear();
        if self.done {
            return Ok(false);
        }
        self.done = true;
        let table = self.ctx.table(self.plan.table_oid())?;
        let indexes = self.ctx.catalog().table_indexes(&table.name);
        let mut found = Vec::new();
        while self
            .child
            .next(&mut self.child_tuples, &mut self.child_rids, batch_size)?
        {
            debug_assert_eq!(self.child_rids.len(), self.child_tuples.len());
            found.extend(self.child_rids.drain(..).zip(self.child_tuples.drain(..)));
        }
        let child_schema = self.child.get_output_schema();
        let new_rows = found
            .iter()
            .map(|(_, old)| {
                let values = self
                    .plan
                    .target_exprs()
                    .iter()
                    .map(|expr| expr.evaluate(old, child_schema))
                    .collect::<Result<Vec<_>>>()?;
                Ok(Tuple::encode(&table.schema, &values)?)
            })
            .collect::<Result<Vec<_>>>()?;
        let old_keys = found
            .iter()
            .map(|(_, old)| index_keys(&table, &indexes, old))
            .collect::<Result<Vec<_>>>()?;
        let new_keys = new_rows
            .iter()
            .map(|new| index_keys(&table, &indexes, new))
            .collect::<Result<Vec<_>>>()?;
        check_primary_keys(&indexes, &found, &new_keys)?;

        for (((rid, _), old), new) in found.iter().zip(&old_keys).zip(&new_keys) {
            let old_locators = locators(&indexes, old, *rid);
            for (i, index) in indexes.iter().enumerate() {
                if entry_moves(&indexes, i, old, new) {
                    index.index.delete_entry(&old[i], &old_locators[i])?;
                }
            }
        }
        for ((((rid, _), new_row), old), new) in
            found.iter().zip(&new_rows).zip(&old_keys).zip(&new_keys)
        {
            table.heap.mark_delete(*rid, COMMIT_TS, INVALID_UNDO_PTR)?;
            let new_rid = table.heap.insert(&TupleMeta::new(COMMIT_TS), new_row)?;
            let new_locators = locators(&indexes, new, new_rid);
            for (i, index) in indexes.iter().enumerate() {
                if entry_moves(&indexes, i, old, new)
                    && !index.index.insert_entry(&new[i], &new_locators[i])?
                {
                    return Err(duplicate_key(index, &new[i]));
                }
            }
        }
        tuples.push(count_row(self.plan.get_output_schema(), found.len())?);
        Ok(true)
    }

    fn get_output_schema(&self) -> &Schema {
        self.plan.get_output_schema()
    }
}

/// Whether the row's entry in `indexes[i]` must be rewritten. `old_keys` and
/// `new_keys` hold the row's key in each index before and after the update.
fn entry_moves(
    indexes: &[Arc<IndexInfo>],
    i: usize,
    old_keys: &[Tuple],
    new_keys: &[Tuple],
) -> bool {
    match indexes[i].locator {
        LocatorKind::Rid => true,
        LocatorKind::PrimaryKey => {
            let pk = indexes
                .iter()
                .position(|index| index.is_primary_key)
                .expect("only a table with a primary key has indexes that store it");
            old_keys[i] != new_keys[i] || old_keys[pk] != new_keys[pk]
        }
    }
}

/// Fails if, once every row in `found` is updated, two rows would share a
/// key in a primary key index: a new key that is also another updated row's
/// new key, or a row's key that is not being updated. `new_keys` holds each
/// updated row's new key in each of `indexes`.
fn check_primary_keys(
    indexes: &[Arc<IndexInfo>],
    found: &[(Rid, Tuple)],
    new_keys: &[Vec<Tuple>],
) -> Result<()> {
    let updated: HashSet<Rid> = found.iter().map(|(rid, _)| *rid).collect();
    let mut holders = Vec::new();
    for (i, index) in indexes.iter().enumerate() {
        if !index.is_primary_key {
            continue;
        }
        let mut seen = HashSet::new();
        for row_keys in new_keys {
            let key = &row_keys[i];
            if !seen.insert(key.data()) {
                return Err(duplicate_key(index, key));
            }
            holders.clear();
            index.index.scan_key(key, &mut holders)?;
            // The rows being updated give up their old keys.
            if holders.iter().any(|holder| {
                let rid = holder.as_rid().expect("the primary key index stores rids");
                !updated.contains(&rid)
            }) {
                return Err(duplicate_key(index, key));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use catalog::TableInfo;
    use common::{DataType, Rid, Value, ValueError};
    use storage::buffer::traced_buffer_pool_manager::{PageAccess, TracedBufferPoolManager};

    use super::super::test_util::{
        Db, account, account_column, column, compare, constant, int, user, users_schema,
    };
    use crate::expressions::{AbstractExpr, ArithmeticExpr, ArithmeticOp, ComparisonOp};
    use crate::plans::{
        AbstractPlanNode, FilterPlan, IndexScanPlan, IndexScanRange, InsertPlan, KeyBound,
        SeqScanPlan, UpdatePlan, ValuesPlan,
    };
    use crate::{ExecutionError, Result};

    fn text(s: &str) -> AbstractExpr {
        constant(Value::from(s), DataType::Varchar(32))
    }

    fn plus(expr: AbstractExpr, n: i32) -> AbstractExpr {
        ArithmeticExpr::new(ArithmeticOp::Add, expr, int(n))
            .unwrap()
            .into()
    }

    fn scan(table: &TableInfo) -> Arc<AbstractPlanNode> {
        let scan = SeqScanPlan::new(table.oid, &table.name, users_schema(), None);
        Arc::new(scan.into())
    }

    /// `table`'s rows that pass `predicate`.
    fn matching(table: &TableInfo, predicate: AbstractExpr) -> Arc<AbstractPlanNode> {
        Arc::new(FilterPlan::new(predicate, scan(table)).into())
    }

    /// Updates the rows `child` reads from `table` to `targets`, returning
    /// the count UPDATE reports.
    fn update(
        db: &Db,
        table: &TableInfo,
        targets: Vec<AbstractExpr>,
        child: Arc<AbstractPlanNode>,
    ) -> Result<i64> {
        let rows = db.run(UpdatePlan::new(table.oid, &table.name, targets, child).into())?;
        match rows.as_slice() {
            [row] => match row.as_slice() {
                [Value::BigInt(count)] => Ok(*count),
                row => panic!("{row:?}"),
            },
            rows => panic!("{rows:?}"),
        }
    }

    /// `table`'s rows, ordered by id.
    fn rows(db: &Db, table: &TableInfo) -> Vec<Vec<Value>> {
        let mut rows = db.run(Arc::unwrap_or_clone(scan(table))).unwrap();
        rows.sort_by_key(|row| match row[0] {
            Value::Int(id) => id,
            ref v => panic!("{v:?}"),
        });
        rows
    }

    /// A table `accounts`, laid out like `users` with `id` as its primary
    /// key, holding unnamed rows with `ids`.
    fn accounts(db: &Db, ids: &[i32]) -> Arc<TableInfo> {
        let table = db
            .catalog()
            .create_table("accounts", users_schema(), &["id"])
            .unwrap();
        let rows = ids
            .iter()
            .map(|&id| vec![int(id), constant(Value::Null, DataType::Varchar(32))])
            .collect();
        let values = ValuesPlan::new(users_schema(), rows);
        db.run(InsertPlan::new(table.oid, &table.name, Arc::new(values.into())).into())
            .unwrap();
        table
    }

    #[test]
    fn updates_the_matching_rows() {
        let db = Db::new();
        db.insert_numbered(5);
        let child = matching(&db.users, compare(ComparisonOp::Gt, column(0), int(3)));
        let count = update(&db, &db.users, vec![column(0), text("x")], child);
        assert_eq!(count.unwrap(), 2);
        assert_eq!(
            rows(&db, &db.users),
            [
                user(1, Some("user 1")),
                user(2, Some("user 2")),
                user(3, Some("user 3")),
                user(4, Some("x")),
                user(5, Some("x")),
            ]
        );
    }

    #[test]
    fn new_values_are_computed_from_the_old_row() {
        let db = Db::new();
        db.insert(&[(1, Some("a")), (2, None)]);
        let count = update(
            &db,
            &db.users,
            vec![plus(column(0), 10), column(1)],
            scan(&db.users),
        );
        assert_eq!(count.unwrap(), 2);
        assert_eq!(rows(&db, &db.users), [user(11, Some("a")), user(12, None)]);
    }

    #[test]
    fn a_failing_row_updates_nothing() {
        let db = Db::new();
        db.insert_numbered(5);
        // 10 / (id - 3) divides by zero on the third row.
        let diff = ArithmeticExpr::new(ArithmeticOp::Sub, column(0), int(3)).unwrap();
        let quotient = ArithmeticExpr::new(ArithmeticOp::Div, int(10), diff.into()).unwrap();
        let err = update(
            &db,
            &db.users,
            vec![quotient.into(), column(1)],
            scan(&db.users),
        );
        assert!(matches!(
            err,
            Err(ExecutionError::Value(ValueError::DivisionByZero))
        ));
        let null_id = update(
            &db,
            &db.users,
            vec![constant(Value::Null, DataType::Int), column(1)],
            scan(&db.users),
        );
        assert!(matches!(
            null_id,
            Err(ExecutionError::Value(ValueError::NullNotAllowed(_)))
        ));
        assert_eq!(rows(&db, &db.users).len(), 5);
        assert_eq!(rows(&db, &db.users)[2], user(3, Some("user 3")));
    }

    #[test]
    fn moves_the_index_entries() {
        let db = Db::new();
        let catalog = db.catalog();
        catalog.create_index("users_id", "users", &["id"]).unwrap();
        catalog
            .create_index("users_name", "users", &["name"])
            .unwrap();
        db.insert_numbered(3);
        let child = matching(&db.users, compare(ComparisonOp::Eq, column(0), int(2)));
        update(&db, &db.users, vec![column(0), text("z")], child).unwrap();

        assert!(db.index_lookup("users_name", &["user 2".into()]).is_empty());
        let [by_name] = db
            .index_lookup("users_name", &["z".into()])
            .try_into()
            .unwrap();
        // The id did not change, but its entry points at the new tuple.
        let [by_id] = db
            .index_lookup("users_id", &[Value::Int(2)])
            .try_into()
            .unwrap();
        assert_eq!(by_id, by_name);
        let (meta, tuple) = db.users.heap.get(by_id).unwrap();
        assert!(!meta.is_deleted);
        assert_eq!(tuple.decode(&users_schema()).unwrap(), user(2, Some("z")));
        assert_eq!(db.index_lookup("users_name", &["user 1".into()]).len(), 1);
    }

    #[test]
    fn primary_keys_may_shift_onto_each_other() {
        let db = Db::new();
        let table = accounts(&db, &[1, 2, 3, 4, 5]);
        let count = update(
            &db,
            &table,
            vec![plus(column(0), 1), column(1)],
            scan(&table),
        );
        assert_eq!(count.unwrap(), 5);
        assert_eq!(
            rows(&db, &table),
            (2..=6).map(|id| user(id, None)).collect::<Vec<_>>()
        );
        assert!(
            db.index_lookup("accounts_pkey", &[Value::Int(1)])
                .is_empty()
        );
        for id in 2..=6 {
            assert_eq!(db.index_lookup("accounts_pkey", &[Value::Int(id)]).len(), 1);
        }
    }

    #[test]
    fn a_key_taken_by_another_row_updates_nothing() {
        let db = Db::new();
        let table = accounts(&db, &[1, 2, 3]);
        let child = matching(&table, compare(ComparisonOp::Eq, column(0), int(1)));
        let err = update(&db, &table, vec![int(3), text("x")], child).unwrap_err();
        assert!(matches!(
            err,
            ExecutionError::DuplicateKey { index, key }
                if index == "accounts_pkey" && key == "(id)=(3)"
        ));
        assert_eq!(
            rows(&db, &table),
            [user(1, None), user(2, None), user(3, None)]
        );
    }

    #[test]
    fn two_rows_given_one_key_update_nothing() {
        let db = Db::new();
        let table = accounts(&db, &[1, 2]);
        let err = update(&db, &table, vec![int(7), column(1)], scan(&table)).unwrap_err();
        assert!(matches!(err, ExecutionError::DuplicateKey { .. }));
        assert_eq!(rows(&db, &table), [user(1, None), user(2, None)]);
        assert_eq!(db.index_lookup("accounts_pkey", &[Value::Int(1)]).len(), 1);
    }

    /// `accounts` holding two accounts named `a` and one named `b`.
    fn three_accounts(db: &Db) -> Arc<TableInfo> {
        db.create_accounts(&[(1, Some("a"), 10), (2, Some("b"), 20), (3, Some("a"), 30)])
    }

    /// The accounts with `id`.
    fn account_with_id(db: &Db, table: &TableInfo, id: i32) -> Arc<AbstractPlanNode> {
        let predicate = compare(ComparisonOp::Eq, account_column(0), int(id));
        db.scan_accounts(table, Some(predicate))
    }

    /// The record id the primary key index holds for each of `ids`.
    fn pkey_rids(db: &Db, ids: &[i32]) -> Vec<Rid> {
        ids.iter()
            .map(|&id| {
                let [rid] = db
                    .index_lookup("accounts_pkey", &[Value::Int(id)])
                    .try_into()
                    .unwrap();
                rid
            })
            .collect()
    }

    /// Whether `pages` recorded a write.
    fn wrote(pages: &TracedBufferPoolManager) -> bool {
        pages
            .take_trace()
            .iter()
            .any(|access| matches!(access, PageAccess::Write(_)))
    }

    /// The rows move, but `accounts_name`, which stores primary keys, isn't
    /// written at all.
    #[test]
    fn an_update_of_other_columns_leaves_secondary_entries_alone() {
        let db = Db::new();
        let table = three_accounts(&db);
        let entries = db.index_entries("accounts_name");
        let rids = pkey_rids(&db, &[1, 2, 3]);
        let (name_pages, pkey_pages) = (
            db.index_pages("accounts_name"),
            db.index_pages("accounts_pkey"),
        );
        name_pages.start_tracing();
        pkey_pages.start_tracing();

        let targets = vec![
            account_column(0),
            account_column(1),
            plus(account_column(2), 1),
        ];
        let child = db.scan_accounts(&table, None);
        assert_eq!(update(&db, &table, targets, child).unwrap(), 3);

        name_pages.stop_tracing();
        pkey_pages.stop_tracing();
        assert!(!wrote(&name_pages));
        assert!(wrote(&pkey_pages));
        assert_eq!(db.index_entries("accounts_name"), entries);
        // The primary key index points at the rows' new versions.
        let new_rids = pkey_rids(&db, &[1, 2, 3]);
        for (old, new) in rids.iter().zip(&new_rids) {
            assert_ne!(old, new);
            assert!(table.heap.get(*old).unwrap().0.is_deleted);
        }
        assert_eq!(
            db.index_rows("accounts_name", &["a".into()]),
            [account(1, Some("a"), 11), account(3, Some("a"), 31)]
        );
        assert_eq!(
            db.index_rows("accounts_name", &["b".into()]),
            [account(2, Some("b"), 21)]
        );
    }

    #[test]
    fn an_update_of_the_secondary_key_moves_only_that_rows_entry() {
        let db = Db::new();
        let table = three_accounts(&db);
        let entries = db.index_entries("accounts_name");

        let targets = vec![account_column(0), text("c"), account_column(2)];
        update(&db, &table, targets, account_with_id(&db, &table, 2)).unwrap();

        let after = db.index_entries("accounts_name");
        assert_eq!(after.len(), 3);
        let kept: Vec<_> = entries.iter().filter(|e| after.contains(e)).collect();
        // The entries of both `a` accounts, untouched.
        assert_eq!(kept, [&entries[0], &entries[1]]);
        assert!(db.index_rows("accounts_name", &["b".into()]).is_empty());
        assert_eq!(
            db.index_rows("accounts_name", &["c".into()]),
            [account(2, Some("c"), 20)]
        );
    }

    #[test]
    fn an_update_of_the_primary_key_rewrites_that_rows_secondary_entries() {
        let db = Db::new();
        let table = three_accounts(&db);
        let entries = db.index_entries("accounts_name");

        let targets = vec![
            plus(account_column(0), 10),
            account_column(1),
            account_column(2),
        ];
        update(&db, &table, targets, account_with_id(&db, &table, 1)).unwrap();

        let after = db.index_entries("accounts_name");
        assert_eq!(after.len(), 3);
        // Account 1's entry is replaced; those of accounts 2 and 3 are not.
        assert!(!after.contains(&entries[0]));
        assert!(after.contains(&entries[1]) && after.contains(&entries[2]));
        assert_eq!(
            db.index_rows("accounts_name", &["a".into()]),
            [account(3, Some("a"), 30), account(11, Some("a"), 10)]
        );
        assert!(
            db.index_lookup("accounts_pkey", &[Value::Int(1)])
                .is_empty()
        );

        // Shift every primary key up by one: every secondary entry is
        // rewritten, without collisions.
        let targets = vec![
            plus(account_column(0), 1),
            account_column(1),
            account_column(2),
        ];
        let child = db.scan_accounts(&table, None);
        assert_eq!(update(&db, &table, targets, child).unwrap(), 3);
        assert_eq!(
            db.index_rows("accounts_name", &["a".into()]),
            [account(4, Some("a"), 30), account(12, Some("a"), 10)]
        );
        assert_eq!(
            db.index_rows("accounts_name", &["b".into()]),
            [account(3, Some("b"), 20)]
        );
    }

    /// An index scan on the key being raised meets each row once.
    #[test]
    fn rows_moved_ahead_of_the_scan_are_not_updated_again() {
        let db = Db::new();
        db.catalog()
            .create_index("users_id", "users", &["id"])
            .unwrap();
        db.insert_numbered(5);
        let index = db.catalog().get_index("users_id").unwrap();
        let range = IndexScanRange::Range {
            lower: Some(KeyBound {
                key: vec![int(1)],
                inclusive: true,
            }),
            upper: None,
        };
        let scan = IndexScanPlan::new(
            db.users.oid,
            index.oid,
            "users_id",
            users_schema(),
            range,
            None,
        );
        let count = update(
            &db,
            &db.users,
            vec![plus(column(0), 10), column(1)],
            Arc::new(scan.into()),
        );
        assert_eq!(count.unwrap(), 5);
        let ids: Vec<Value> = rows(&db, &db.users)
            .into_iter()
            .map(|row| row[0].clone())
            .collect();
        assert_eq!(ids, (11..=15).map(Value::Int).collect::<Vec<_>>());
    }

    #[test]
    fn updates_every_row_across_pages() {
        let db = Db::new();
        db.insert_numbered(300);
        let count = update(
            &db,
            &db.users,
            vec![column(0), text("renamed")],
            scan(&db.users),
        );
        assert_eq!(count.unwrap(), 300);
        let rows = rows(&db, &db.users);
        assert_eq!(rows.len(), 300);
        assert!(rows.iter().all(|row| row[1] == Value::from("renamed")));
    }
}
