use std::sync::Arc;

use catalog::{IndexInfo, TableInfo};
use common::{Rid, Schema, Tuple};
use storage::index::RowLocator;

use super::{AbstractExecutor, Executor, count_row, duplicate_key, index_keys, locators};
use crate::Result;
use crate::execution_context::ExecutionContext;
use crate::expressions::Expr;
use crate::plans::{PlanNode, UpdatePlan};

/// Updates all of the child's rows on the first call to `next`, and produces
/// one row holding how many there were. The child reads the table, so every
/// row it produces has a rid.
///
/// It reads every row before updating any, so a row it changes is not read
/// again by a scan still running below it.
///
/// Rows are updated in place, keeping their rids unless a new version
/// doesn't fit its page (`Transaction::update`). A row's entry in an index
/// is rewritten only if its key or its locator changed. All old entries are
/// removed before new ones are added, so `SET id = id + 1` doesn't collide
/// with itself.
pub struct UpdateExecutor {
    ctx: Arc<ExecutionContext>,
    plan: UpdatePlan,
    child: Box<AbstractExecutor>,
    child_tuples: Vec<Tuple>,
    child_rids: Vec<Rid>,
    done: bool,
}

/// A row's entry in an index: its key and the locator stored with it.
type Entry = (Tuple, RowLocator);

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

    /// The new version of `old`, a row of `table`.
    fn new_row(&self, table: &TableInfo, old: &Tuple) -> Result<Tuple> {
        let child_schema = self.child.get_output_schema();
        let values = self
            .plan
            .target_exprs()
            .iter()
            .map(|expr| expr.evaluate(old, child_schema))
            .collect::<Result<Vec<_>>>()?;
        Ok(Tuple::encode(&table.schema, &values)?)
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

        let txn = self.ctx.txn();
        // Each row's entries in `indexes`, before and after the update.
        let mut moved: Vec<(Vec<Entry>, Vec<Entry>)> = Vec::with_capacity(found.len());
        for (rid, old) in &found {
            let new = self.new_row(&table, old)?;
            let new_rid = txn.update(&table.heap, &table.schema, *rid, &new)?;
            let old_entries = entries(&table, &indexes, old, *rid)?;
            let new_entries = entries(&table, &indexes, &new, new_rid)?;
            moved.push((old_entries, new_entries));
        }
        for (old_entries, new_entries) in &moved {
            for ((index, old), new) in indexes.iter().zip(old_entries).zip(new_entries) {
                if old != new {
                    txn.delete_entry(&index.index, &old.0, &old.1)?;
                }
            }
        }
        for (old_entries, new_entries) in &moved {
            for ((index, old), new) in indexes.iter().zip(old_entries).zip(new_entries) {
                if old != new && !txn.insert_entry(&index.index, &new.0, &new.1)? {
                    return Err(duplicate_key(index, &new.0));
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

/// The entry in each of `indexes` (all of `table`'s) for `tuple`, stored at
/// `rid`.
fn entries(
    table: &TableInfo,
    indexes: &[Arc<IndexInfo>],
    tuple: &Tuple,
    rid: Rid,
) -> Result<Vec<Entry>> {
    let keys = index_keys(table, indexes, tuple)?;
    let locators = locators(indexes, &keys, rid);
    Ok(keys.into_iter().zip(locators).collect())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use catalog::TableInfo;
    use common::{Column, DataType, Rid, Schema, Value, ValueError};
    use storage::StorageError;
    use storage::buffer::traced_buffer_pool_manager::{PageAccess, TracedBufferPoolManager};

    use super::super::test_util::{
        Db, account, account_column, column, compare, constant, int, user, users_schema,
    };
    use crate::expressions::{
        AbstractExpr, ArithmeticExpr, ArithmeticOp, ColumnValueExpr, ComparisonOp,
    };
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

    /// Rows updated in place keep their rids, so neither index is written.
    #[test]
    fn an_update_in_place_writes_no_index() {
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
        assert!(!wrote(&pkey_pages));
        assert_eq!(db.index_entries("accounts_name"), entries);
        assert_eq!(pkey_rids(&db, &[1, 2, 3]), rids);
        assert_eq!(
            db.index_rows("accounts_name", &["a".into()]),
            [account(1, Some("a"), 11), account(3, Some("a"), 31)]
        );
        assert_eq!(
            db.index_rows("accounts_name", &["b".into()]),
            [account(2, Some("b"), 21)]
        );
    }

    /// A row that no longer fits its page moves to a new rid. The primary
    /// key index follows it, but `notes_tag`, which stores primary keys,
    /// isn't written.
    #[test]
    fn a_moved_row_rewrites_only_entries_that_store_its_rid() {
        let db = Db::new();
        let schema = Schema::new(vec![
            Column::new("id", DataType::Int, false),
            Column::new("tag", DataType::Int, true),
            Column::new("note", DataType::Varchar(2000), true),
        ]);
        let table = db
            .catalog()
            .create_table("notes", schema.clone(), &["id"])
            .unwrap();
        db.catalog()
            .create_index("notes_tag", "notes", &["tag"])
            .unwrap();
        // Row 1 is short; the rest fill most of its page.
        let note = |len: usize| {
            constant(
                Value::from("n".repeat(len).as_str()),
                DataType::Varchar(2000),
            )
        };
        let rows = (1..=4)
            .map(|id| vec![int(id), int(7), note(if id == 1 { 10 } else { 1000 })])
            .collect();
        let values = ValuesPlan::new(schema.clone(), rows);
        db.run(InsertPlan::new(table.oid, &table.name, Arc::new(values.into())).into())
            .unwrap();
        let rids = db.index_lookup("notes_pkey", &[Value::Int(1)]);
        let (tag_pages, pkey_pages) = (db.index_pages("notes_tag"), db.index_pages("notes_pkey"));
        tag_pages.start_tracing();
        pkey_pages.start_tracing();

        let column = |idx| ColumnValueExpr::new(0, idx, schema.column(idx).unwrap().clone());
        let id_1 = compare(ComparisonOp::Eq, column(0).into(), int(1));
        let scan = SeqScanPlan::new(table.oid, &table.name, schema.clone(), Some(id_1));
        let targets = vec![column(0).into(), column(1).into(), note(1500)];
        assert_eq!(
            update(&db, &table, targets, Arc::new(scan.into())).unwrap(),
            1
        );

        tag_pages.stop_tracing();
        pkey_pages.stop_tracing();
        assert!(!wrote(&tag_pages));
        assert!(wrote(&pkey_pages));
        let new_rids = db.index_lookup("notes_pkey", &[Value::Int(1)]);
        assert_ne!(new_rids, rids);
        // The old version is gone for new transactions (and may already be
        // reclaimed).
        let txn = db.tm.begin();
        assert!(txn.get(&table.heap, &schema, rids[0]).unwrap().is_none());
        db.tm.commit(&txn).unwrap();
        let by_tag = db.index_rows("notes_tag", &[Value::Int(7)]);
        assert_eq!(by_tag.len(), 4);
        assert!(by_tag.contains(&vec![
            Value::Int(1),
            Value::Int(7),
            Value::from("n".repeat(1500).as_str())
        ]));
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

    /// Two transactions writing the same row: the later write fails, and
    /// aborting it leaves the first one's write.
    #[test]
    fn a_row_another_transaction_wrote_is_a_conflict() {
        let db = Db::new();
        db.insert_numbered(2);
        let (first, second) = (db.tm.begin(), db.tm.begin());
        let id_1 = || matching(&db.users, compare(ComparisonOp::Eq, column(0), int(1)));
        let set_name =
            |name| UpdatePlan::new(db.users.oid, "users", vec![column(0), text(name)], id_1());
        db.run_in(&first, set_name("first").into()).unwrap();
        let err = db.run_in(&second, set_name("second").into()).unwrap_err();
        assert!(
            matches!(err, ExecutionError::Storage(StorageError::Conflict { .. })),
            "{err:?}"
        );
        db.tm.abort(&second).unwrap();
        db.tm.commit(&first).unwrap();
        assert_eq!(
            rows(&db, &db.users),
            [user(1, Some("first")), user(2, Some("user 2"))]
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
