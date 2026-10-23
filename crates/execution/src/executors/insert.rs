use std::collections::HashSet;
use std::sync::Arc;

use catalog::{IndexInfo, TableInfo};
use common::{Rid, Schema, Tuple};
use storage::index::IndexTrait;
use storage::table::table_page::TupleMeta;

use super::{AbstractExecutor, COMMIT_TS, Executor, count_row, duplicate_key, index_keys};
use crate::Result;
use crate::plans::{InsertPlan, PlanNode};

/// Inserts all of the child's rows, and their entries in each of the table's
/// indexes, on the first call to `next`, and produces one row holding how
/// many there were.
///
/// There are no transactions to roll back yet, so it reads every row and
/// checks that none duplicates a primary key before inserting any: a row
/// that fails to evaluate or has a duplicate key inserts nothing. An error
/// from storage part way through, such as a key too long for its index,
/// still leaves the rows before it inserted.
pub struct InsertExecutor {
    plan: InsertPlan,
    table: Arc<TableInfo>,
    indexes: Vec<Arc<IndexInfo>>,
    child: Box<AbstractExecutor>,
    child_tuples: Vec<Tuple>,
    child_rids: Vec<Rid>,
    done: bool,
}

impl InsertExecutor {
    pub fn new(
        plan: InsertPlan,
        table: Arc<TableInfo>,
        indexes: Vec<Arc<IndexInfo>>,
        child: Box<AbstractExecutor>,
    ) -> Self {
        Self {
            plan,
            table,
            indexes,
            child,
            child_tuples: Vec::new(),
            child_rids: Vec::new(),
            done: false,
        }
    }
}

impl Executor for InsertExecutor {
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
        let mut rows = Vec::new();
        while self
            .child
            .next(&mut self.child_tuples, &mut self.child_rids, batch_size)?
        {
            rows.append(&mut self.child_tuples);
        }
        // The child's rows are already in the table's layout.
        let keys = rows
            .iter()
            .map(|tuple| index_keys(&self.table, &self.indexes, tuple))
            .collect::<Result<Vec<_>>>()?;
        self.check_primary_keys(&keys)?;
        for (tuple, keys) in rows.iter().zip(&keys) {
            let rid = self.table.heap.insert(&TupleMeta::new(COMMIT_TS), tuple)?;
            for (index, key) in self.indexes.iter().zip(keys) {
                if !index.index.insert_entry(key, rid)? {
                    return Err(duplicate_key(index, key));
                }
            }
        }
        tuples.push(count_row(self.plan.get_output_schema(), rows.len())?);
        Ok(true)
    }

    fn get_output_schema(&self) -> &Schema {
        self.plan.get_output_schema()
    }
}

impl InsertExecutor {
    /// Fails if a row's key in a primary key index is already in the index,
    /// or is also an earlier row's. `keys` holds each row's key in each
    /// index.
    fn check_primary_keys(&self, keys: &[Vec<Tuple>]) -> Result<()> {
        let mut rids = Vec::new();
        for (i, index) in self.indexes.iter().enumerate() {
            if !index.is_primary_key {
                continue;
            }
            let mut seen = HashSet::new();
            for row_keys in keys {
                let key = &row_keys[i];
                index.index.scan_key(key, &mut rids)?;
                if !rids.is_empty() || !seen.insert(key.data()) {
                    return Err(duplicate_key(index, key));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common::{DataType, Value, ValueError};

    use super::super::test_util::{Db, constant, int, user, users_schema};
    use crate::ExecutionError;
    use crate::plans::{InsertPlan, ValuesPlan};

    #[test]
    fn inserts_the_rows_and_counts_them() {
        let db = Db::new();
        assert_eq!(db.insert(&[(1, Some("a")), (2, None), (3, Some("c"))]), 3);
        assert_eq!(db.insert(&[(4, Some("d"))]), 1);
        assert_eq!(
            db.users_rows(),
            [
                user(1, Some("a")),
                user(2, None),
                user(3, Some("c")),
                user(4, Some("d"))
            ]
        );
    }

    #[test]
    fn a_failing_row_inserts_nothing() {
        let db = Db::new();
        let rows = vec![
            vec![int(1), constant(Value::from("a"), DataType::Varchar(32))],
            vec![
                constant(Value::Null, DataType::Int),
                constant(Value::from("b"), DataType::Varchar(32)),
            ],
        ];
        let values = ValuesPlan::new(users_schema(), rows);
        let insert = InsertPlan::new(db.users.oid, Arc::new(values.into()));
        assert!(matches!(
            db.run(insert.into()),
            Err(ExecutionError::Value(ValueError::NullNotAllowed(_)))
        ));
        assert!(db.users_rows().is_empty());
    }

    #[test]
    fn adds_the_rows_to_the_indexes() {
        let db = Db::new();
        db.catalog()
            .create_index("users_name", "users", &["name"])
            .unwrap();
        db.insert(&[(1, Some("a")), (2, Some("b")), (3, Some("a"))]);
        assert_eq!(db.index_lookup("users_name", &["a".into()]).len(), 2);
        assert_eq!(db.index_lookup("users_name", &["b".into()]).len(), 1);
        assert!(db.index_lookup("users_name", &["c".into()]).is_empty());
    }

    #[test]
    fn inserts_across_pages() {
        let db = Db::new();
        db.insert_numbered(500);
        assert_eq!(db.users_rows().len(), 500);
    }
}
