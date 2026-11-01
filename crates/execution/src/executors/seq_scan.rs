use std::sync::Arc;

use common::{Rid, Schema, Tuple};
use storage::concurrency::transaction::TransactionScan;
use storage::table::table_iterator::TableRow;

use super::{Executor, RowPredicate};
use crate::Result;
use crate::execution_context::ExecutionContext;
use crate::plans::{PlanNode, SeqScanPlan};

/// Reads the rows of a table that the statement's transaction sees, in
/// storage order, a page at a time, keeping those that pass the plan's
/// filter.
pub struct SeqScanExecutor {
    ctx: Arc<ExecutionContext>,
    plan: SeqScanPlan,
    /// Set by `init`.
    iter: Option<TransactionScan>,
    /// Rows of the page read last; those from `next_row` on have not been
    /// produced yet. Produced rows trade buffers with the caller's batch, so
    /// reading the next page writes over buffers instead of allocating.
    rows: Vec<TableRow>,
    next_row: usize,
    /// The plan's filter, made ready to test rows.
    filter: Option<RowPredicate>,
}

impl SeqScanExecutor {
    pub fn new(ctx: Arc<ExecutionContext>, plan: SeqScanPlan) -> Self {
        let filter = plan
            .filter()
            .map(|filter| RowPredicate::new(filter, plan.get_output_schema()));
        Self {
            ctx,
            plan,
            filter,
            iter: None,
            rows: Vec::new(),
            next_row: 0,
        }
    }
}

impl Executor for SeqScanExecutor {
    fn init(&mut self) -> Result<()> {
        let table = self.ctx.table(self.plan.table_oid())?;
        self.iter = Some(self.ctx.txn().scan(&table.heap, &table.schema)?);
        self.rows.clear();
        self.next_row = 0;
        Ok(())
    }

    fn next(
        &mut self,
        tuples: &mut Vec<Tuple>,
        rids: &mut Vec<Rid>,
        batch_size: usize,
    ) -> Result<bool> {
        rids.clear();
        let iter = self.iter.as_mut().expect("init is called before next");
        let mut n = 0;
        while n < batch_size {
            if self.next_row == self.rows.len() {
                self.next_row = 0;
                if !iter.next_batch_into(&mut self.rows)? {
                    break;
                }
            }
            let (_, tuple) = &mut self.rows[self.next_row];
            self.next_row += 1;
            if let Some(filter) = &self.filter
                && !filter.passes(tuple, self.plan.get_output_schema())?
            {
                continue;
            }
            rids.push(tuple.rid().expect("rows read from a table carry their rid"));
            if n == tuples.len() {
                tuples.push(Tuple::default());
            }
            std::mem::swap(&mut tuples[n], tuple);
            n += 1;
        }
        tuples.truncate(n);
        Ok(n > 0)
    }

    fn get_output_schema(&self) -> &Schema {
        self.plan.get_output_schema()
    }
}

#[cfg(test)]
mod tests {
    use common::Value;

    use super::super::test_util::{BATCH_SIZE, Db, column, compare, decode, int, user};
    use super::super::{Executor, create_executor};
    use crate::expressions::ComparisonOp;

    #[test]
    fn reads_every_row_across_pages() {
        let db = Db::new();
        db.insert_numbered(300);
        let first_page = db.users.heap.iter().unwrap().next_batch().unwrap();
        assert!(first_page.unwrap().len() < 300, "rows span several pages");
        let rows = db.users_rows();
        assert_eq!(rows.len(), 300);
        for (row, id) in rows.iter().zip(1..) {
            assert_eq!(*row, user(id, Some(&format!("user {id}"))));
        }
    }

    #[test]
    fn applies_its_filter() {
        let db = Db::new();
        db.insert_numbered(10);
        let filter = compare(ComparisonOp::GtEq, column(0), int(8));
        let scan = std::sync::Arc::unwrap_or_clone(db.scan(Some(filter)));
        let ids: Vec<Value> = db
            .run(scan)
            .unwrap()
            .into_iter()
            .map(|row| row[0].clone())
            .collect();
        assert_eq!(ids, [Value::Int(8), Value::Int(9), Value::Int(10)]);
    }

    #[test]
    fn produces_the_rids_of_its_rows() {
        let db = Db::new();
        db.insert_numbered(5);
        let mut scan = create_executor(&db.ctx(), &db.scan(None)).unwrap();
        scan.init().unwrap();
        let (mut tuples, mut rids) = (Vec::new(), Vec::new());
        let mut seen = 0;
        while scan.next(&mut tuples, &mut rids, BATCH_SIZE).unwrap() {
            assert_eq!(rids.len(), tuples.len());
            for (tuple, rid) in tuples.iter().zip(&rids) {
                assert_eq!(tuple.rid(), Some(*rid));
                let (_, stored) = db.users.heap.get(*rid).unwrap();
                assert_eq!(stored.data(), tuple.data());
            }
            seen += tuples.len();
        }
        assert_eq!(seen, 5);
        // init starts the scan over.
        scan.init().unwrap();
        assert!(scan.next(&mut tuples, &mut rids, 10).unwrap());
        assert_eq!(
            decode(&tuples, scan.get_output_schema()),
            (1..=5)
                .map(|id| user(id, Some(&format!("user {id}"))))
                .collect::<Vec<_>>()
        );
    }
}
