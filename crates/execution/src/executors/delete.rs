use std::sync::Arc;

use common::{Rid, Schema, Tuple};
use storage::index::IndexTrait;
use storage::table::table_page::INVALID_UNDO_PTR;

use super::{AbstractExecutor, COMMIT_TS, Executor, count_row, index_keys};
use crate::Result;
use crate::execution_context::ExecutionContext;
use crate::plans::{DeletePlan, PlanNode};

/// Deletes all of the child's rows on the first call to `next`, and produces
/// one row holding how many there were. The child reads the table, so every
/// row it produces has a rid.
///
/// There are no transactions to roll back yet, so it finds every row before
/// deleting any: a WHERE that fails to evaluate deletes nothing.
///
/// A delete marks the tuple deleted and removes its entries from the table's
/// indexes. There is no undo log yet, so the tuple gets no undo entry.
pub struct DeleteExecutor {
    ctx: Arc<ExecutionContext>,
    plan: DeletePlan,
    child: Box<AbstractExecutor>,
    child_tuples: Vec<Tuple>,
    child_rids: Vec<Rid>,
    done: bool,
}

impl DeleteExecutor {
    pub fn new(ctx: Arc<ExecutionContext>, plan: DeletePlan, child: Box<AbstractExecutor>) -> Self {
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

impl Executor for DeleteExecutor {
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
        for (rid, tuple) in &found {
            table.heap.mark_delete(*rid, COMMIT_TS, INVALID_UNDO_PTR)?;
            let keys = index_keys(&table, &indexes, tuple)?;
            for (index, key) in indexes.iter().zip(&keys) {
                index.index.delete_entry(key, *rid)?;
            }
        }
        tuples.push(count_row(self.plan.get_output_schema(), found.len())?);
        Ok(true)
    }

    fn get_output_schema(&self) -> &Schema {
        self.plan.get_output_schema()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common::{Value, ValueError};

    use super::super::test_util::{Db, column, compare, int, user};
    use crate::ExecutionError;
    use crate::expressions::{ArithmeticExpr, ArithmeticOp, ComparisonOp};
    use crate::plans::{DeletePlan, FilterPlan};

    fn delete(db: &Db, filter: Option<FilterPlan>) -> Vec<Vec<Value>> {
        let child = match filter {
            Some(filter) => Arc::new(filter.into()),
            None => db.scan(None),
        };
        db.run(DeletePlan::new(db.users.oid, &db.users.name, child).into())
            .unwrap()
    }

    #[test]
    fn deletes_the_matching_rows() {
        let db = Db::new();
        db.insert_numbered(5);
        let id_above_2 = compare(ComparisonOp::Gt, column(0), int(2));
        let filter = FilterPlan::new(id_above_2, db.scan(None));
        assert_eq!(delete(&db, Some(filter)), [[Value::BigInt(3)]]);
        assert_eq!(
            db.users_rows(),
            [user(1, Some("user 1")), user(2, Some("user 2"))]
        );
    }

    #[test]
    fn removes_the_rows_from_the_indexes() {
        let db = Db::new();
        db.catalog()
            .create_index("users_name", "users", &["name"])
            .unwrap();
        db.insert_numbered(5);
        let id_above_2 = compare(ComparisonOp::Gt, column(0), int(2));
        delete(&db, Some(FilterPlan::new(id_above_2, db.scan(None))));
        assert_eq!(db.index_lookup("users_name", &["user 2".into()]).len(), 1);
        assert!(db.index_lookup("users_name", &["user 3".into()]).is_empty());
    }

    #[test]
    fn deletes_every_row_across_pages() {
        let db = Db::new();
        db.insert_numbered(300);
        assert_eq!(delete(&db, None), [[Value::BigInt(300)]]);
        assert!(db.users_rows().is_empty());
        // Nothing is left to delete.
        assert_eq!(delete(&db, None), [[Value::BigInt(0)]]);
    }

    #[test]
    fn a_failing_condition_deletes_nothing() {
        let db = Db::new();
        db.insert_numbered(5);
        // 10 / (id - 3) divides by zero on the third row.
        let diff = ArithmeticExpr::new(ArithmeticOp::Sub, column(0), int(3)).unwrap();
        let quotient = ArithmeticExpr::new(ArithmeticOp::Div, int(10), diff.into()).unwrap();
        let filter = FilterPlan::new(
            compare(ComparisonOp::Gt, quotient.into(), int(0)),
            db.scan(None),
        );
        let delete = DeletePlan::new(db.users.oid, &db.users.name, Arc::new(filter.into()));
        assert!(matches!(
            db.run(delete.into()),
            Err(ExecutionError::Value(ValueError::DivisionByZero))
        ));
        assert_eq!(db.users_rows().len(), 5);
    }

    #[test]
    fn deleted_rows_stay_on_their_pages() {
        let db = Db::new();
        db.insert_numbered(3);
        let first = db.users.heap.iter().unwrap().next().unwrap().unwrap().1;
        let rid = first.rid().unwrap();
        let filter = FilterPlan::new(compare(ComparisonOp::Eq, column(0), int(1)), db.scan(None));
        delete(&db, Some(filter));
        let (meta, tuple) = db.users.heap.get(rid).unwrap();
        assert!(meta.is_deleted);
        assert_eq!(tuple.data(), first.data());
    }
}
