use std::collections::VecDeque;
use std::sync::Arc;

use common::{Rid, Schema, Tuple, Value};
use storage::index::IndexTrait;

use super::{AbstractExecutor, Executor};
use crate::Result;
use crate::execution_context::ExecutionContext;
use crate::expressions::Expr;
use crate::plans::{JoinType, NestedIndexJoinPlan, PlanNode};

/// An index nested loop join:
///
/// ```text
/// for each block of outer rows:
///     for each outer row in the block:
///         look up the row's key in the index on the inner table
///         for each inner row the index points to:
///             if the pair passes the predicate, output it
///         LEFT join: if nothing was output, output the row padded with NULLs
/// ```
///
/// The inner table is never scanned, so the size of an outer block doesn't
/// change how much of it is read: the outer rows are taken in the batches
/// the outer child returns them in.
///
/// One outer row can have more matches than fit in a batch, so its joined
/// rows wait in `output` until `next` returns them. That holds every joined
/// row of one outer row in memory at once.
pub struct NestedIndexJoinExecutor {
    ctx: Arc<ExecutionContext>,
    plan: NestedIndexJoinPlan,
    outer: Box<AbstractExecutor>,
    /// The current block of outer rows.
    outer_block: Vec<Tuple>,
    outer_rids: Vec<Rid>,
    /// The next row of `outer_block` to join.
    next_outer: usize,
    /// Joined rows not returned by `next` yet.
    output: VecDeque<Tuple>,
}

impl NestedIndexJoinExecutor {
    pub fn new(
        ctx: Arc<ExecutionContext>,
        plan: NestedIndexJoinPlan,
        outer: Box<AbstractExecutor>,
    ) -> Self {
        Self {
            ctx,
            plan,
            outer,
            outer_block: Vec::new(),
            outer_rids: Vec::new(),
            next_outer: 0,
            output: VecDeque::new(),
        }
    }

    /// The next outer row, fetching the next block of outer rows when this
    /// one is done. None once the outer side is done.
    fn next_outer_row(&mut self, batch_size: usize) -> Result<Option<Tuple>> {
        if self.next_outer == self.outer_block.len() {
            self.next_outer = 0;
            let more = self
                .outer
                .next(&mut self.outer_block, &mut self.outer_rids, batch_size)?;
            if !more {
                return Ok(None);
            }
        }
        self.next_outer += 1;
        Ok(Some(self.outer_block[self.next_outer - 1].clone()))
    }

    /// Joins `outer` with the inner rows the index finds for it, adding the
    /// joined rows to `output`.
    fn join_outer_row(&mut self, outer: &Tuple) -> Result<()> {
        let outer_schema = self.outer.get_output_schema();
        let inner_schema = self.plan.inner_schema();
        let table = self.ctx.table(self.plan.inner_table_oid())?;
        let mut matched = false;
        for rid in self.index_lookup(outer)? {
            let (meta, inner) = table.heap.get(rid)?;
            if meta.is_deleted {
                continue;
            }
            if let Some(predicate) = self.plan.predicate()
                && predicate.evaluate_join(outer, outer_schema, &inner, inner_schema)?
                    != Value::Bool(true)
            {
                continue;
            }
            let mut values = outer.decode(outer_schema)?;
            values.extend(inner.decode(inner_schema)?);
            self.output
                .push_back(Tuple::encode(self.plan.get_output_schema(), &values)?);
            matched = true;
        }
        if !matched && self.plan.join_type() == JoinType::Left {
            let mut values = outer.decode(outer_schema)?;
            values.extend(std::iter::repeat_n(Value::Null, inner_schema.len()));
            self.output
                .push_back(Tuple::encode(self.plan.get_output_schema(), &values)?);
        }
        Ok(())
    }

    /// The record ids of the inner rows whose key is the plan's key for
    /// `outer`. None if the key has a NULL: NULL = NULL is not TRUE, though
    /// the index holds NULL keys.
    fn index_lookup(&self, outer: &Tuple) -> Result<Vec<Rid>> {
        let values = self
            .plan
            .key_exprs()
            .iter()
            .map(|expr| expr.evaluate(outer, self.outer.get_output_schema()))
            .collect::<Result<Vec<_>>>()?;
        let index = self.ctx.index(self.plan.index_oid())?;
        let mut rids = Vec::new();
        if !values.iter().any(Value::is_null) {
            let key = Tuple::encode(&index.key_schema, &values)?;
            index.index.scan_key(&key, &mut rids)?;
        }
        Ok(rids)
    }
}

impl Executor for NestedIndexJoinExecutor {
    fn init(&mut self) -> Result<()> {
        self.outer.init()?;
        self.outer_block.clear();
        self.next_outer = 0;
        self.output.clear();
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
            if let Some(row) = self.output.pop_front() {
                tuples.push(row);
            } else if let Some(outer) = self.next_outer_row(batch_size)? {
                self.join_outer_row(&outer)?;
            } else {
                break;
            }
        }
        Ok(!tuples.is_empty())
    }

    fn get_output_schema(&self) -> &Schema {
        self.plan.get_output_schema()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use catalog::{IndexInfo, TableInfo};
    use common::{Column, DataType, Schema, Value};

    use super::super::test_util::{Db, column, compare, constant, int};
    use super::super::{Executor, create_executor};
    use crate::ExecutionError;
    use crate::expressions::{AbstractExpr, ColumnValueExpr, ComparisonOp};
    use crate::plans::{AbstractPlanNode, InsertPlan, JoinType, NestedIndexJoinPlan, ValuesPlan};

    fn orders_schema() -> Schema {
        Schema::new(vec![
            Column::new("user_id", DataType::Int, true),
            Column::new("item", DataType::Varchar(32), false),
        ])
    }

    /// An `orders (user_id INT, item VARCHAR(32))` table with an index
    /// `orders_<key>` on the `key` columns, holding `rows`.
    fn create_orders(db: &Db, key: &[&str], rows: &[(Option<i32>, &str)]) -> Inner {
        let table = db
            .catalog()
            .create_table("orders", orders_schema(), &[])
            .unwrap();
        let name = format!("orders_{}", key.join("_"));
        let index = db.catalog().create_index(&name, "orders", key).unwrap();
        let rows = rows
            .iter()
            .map(|&(user_id, item)| {
                vec![
                    constant(user_id.map_or(Value::Null, Value::Int), DataType::Int),
                    constant(Value::from(item), DataType::Varchar(32)),
                ]
            })
            .collect();
        let values = ValuesPlan::new(table.schema.clone(), rows);
        db.run(InsertPlan::new(table.oid, &table.name, Arc::new(values.into())).into())
            .unwrap();
        Inner { table, index }
    }

    /// The inner side of a join: a table and an index on it.
    struct Inner {
        table: Arc<TableInfo>,
        index: Arc<IndexInfo>,
    }

    impl Inner {
        /// A join of `outer` with this table, looking up `key` in its index.
        fn join(
            &self,
            join_type: JoinType,
            outer: Arc<AbstractPlanNode>,
            key: Vec<AbstractExpr>,
            predicate: Option<AbstractExpr>,
        ) -> AbstractPlanNode {
            NestedIndexJoinPlan::new(
                join_type,
                outer,
                self.table.oid,
                self.table.schema.clone(),
                self.index.oid,
                &self.index.name,
                key,
                predicate,
            )
            .into()
        }
    }

    /// Column `idx` of the inner side.
    fn inner_column(idx: usize) -> AbstractExpr {
        ColumnValueExpr::new(1, idx, orders_schema().column(idx).unwrap().clone()).into()
    }

    /// A joined `users` and `orders` row.
    fn row(id: i32, user_id: Option<i32>, item: Option<&str>) -> Vec<Value> {
        vec![
            Value::Int(id),
            Value::from(format!("user {id}").as_str()),
            user_id.map_or(Value::Null, Value::Int),
            item.map_or(Value::Null, Value::from),
        ]
    }

    /// Orders for users 1 and 3, one without a user and one for a user who
    /// doesn't exist.
    const SOME_ORDERS: &[(Option<i32>, &str)] = &[
        (Some(1), "pen"),
        (Some(3), "ink"),
        (None, "box"),
        (Some(1), "cap"),
        (Some(9), "mug"),
    ];

    /// Users 1 to 4, and [`SOME_ORDERS`] indexed on `user_id`.
    fn some_orders() -> (Db, Inner) {
        let db = Db::new();
        db.insert_numbered(4);
        let orders = create_orders(&db, &["user_id"], SOME_ORDERS);
        (db, orders)
    }

    /// Rows come out in outer order, each with its matches in index order.
    #[test]
    fn inner_join_looks_up_each_outer_row() {
        let (db, orders) = some_orders();
        let join = orders.join(JoinType::Inner, db.scan(None), vec![column(0)], None);
        assert_eq!(
            db.run(join).unwrap(),
            [
                row(1, Some(1), Some("pen")),
                row(1, Some(1), Some("cap")),
                row(3, Some(3), Some("ink")),
            ]
        );
    }

    #[test]
    fn left_join_pads_rows_without_matches() {
        let (db, orders) = some_orders();
        let join = orders.join(JoinType::Left, db.scan(None), vec![column(0)], None);
        assert_eq!(
            db.run(join).unwrap(),
            [
                row(1, Some(1), Some("pen")),
                row(1, Some(1), Some("cap")),
                row(2, None, None),
                row(3, Some(3), Some("ink")),
                row(4, None, None),
            ]
        );
    }

    /// The index holds the order without a user, but NULL = NULL is not
    /// TRUE, so an outer NULL key finds nothing.
    #[test]
    fn null_keys_match_nothing() {
        let (db, orders) = some_orders();
        let schema = Schema::new(vec![Column::new("user_id", DataType::Int, true)]);
        let keys = vec![vec![int(3)], vec![constant(Value::Null, DataType::Int)]];
        let outer = Arc::new(ValuesPlan::new(schema.clone(), keys).into());
        let key = ColumnValueExpr::new(0, 0, schema.column(0).unwrap().clone());
        let inner = orders.join(
            JoinType::Inner,
            Arc::clone(&outer),
            vec![key.clone().into()],
            None,
        );
        let ink = vec![Value::Int(3), Value::Int(3), Value::from("ink")];
        assert_eq!(db.run(inner).unwrap(), std::slice::from_ref(&ink));
        let left = orders.join(JoinType::Left, outer, vec![key.into()], None);
        assert_eq!(db.run(left).unwrap(), [ink, vec![Value::Null; 3]]);
    }

    /// The predicate is checked on the pairs the index finds; an outer row
    /// whose matches all fail it is padded in a LEFT join.
    #[test]
    fn checks_the_rest_of_the_condition() {
        let (db, orders) = some_orders();
        let not_pen = compare(
            ComparisonOp::NotEq,
            inner_column(1),
            constant(Value::from("pen"), DataType::Varchar(32)),
        );
        let join = orders.join(
            JoinType::Inner,
            db.scan(None),
            vec![column(0)],
            Some(not_pen),
        );
        assert_eq!(
            db.run(join).unwrap(),
            [row(1, Some(1), Some("cap")), row(3, Some(3), Some("ink"))]
        );

        let ink = compare(
            ComparisonOp::Eq,
            inner_column(1),
            constant(Value::from("ink"), DataType::Varchar(32)),
        );
        let join = orders.join(JoinType::Left, db.scan(None), vec![column(0)], Some(ink));
        assert_eq!(
            db.run(join).unwrap(),
            [
                row(1, None, None),
                row(2, None, None),
                row(3, Some(3), Some("ink")),
                row(4, None, None),
            ]
        );
    }

    /// A key on two columns, the second given by a constant.
    #[test]
    fn looks_up_keys_of_several_columns() {
        let db = Db::new();
        db.insert_numbered(4);
        let orders = create_orders(&db, &["user_id", "item"], SOME_ORDERS);
        let cap = constant(Value::from("cap"), DataType::Varchar(32));
        let join = orders.join(JoinType::Left, db.scan(None), vec![column(0), cap], None);
        assert_eq!(
            db.run(join).unwrap(),
            [
                row(1, Some(1), Some("cap")),
                row(2, None, None),
                row(3, None, None),
                row(4, None, None),
            ]
        );
    }

    /// One outer row with matches across many index leaves and output
    /// batches.
    #[test]
    fn many_matches_for_one_row() {
        let db = Db::new();
        db.insert_numbered(3);
        let items: Vec<String> = (0..400).map(|i| format!("item {i:03}")).collect();
        let rows: Vec<_> = items.iter().map(|item| (Some(2), item.as_str())).collect();
        let orders = create_orders(&db, &["user_id"], &rows);
        let join = orders.join(JoinType::Left, db.scan(None), vec![column(0)], None);
        let mut expected = vec![row(1, None, None)];
        expected.extend(items.iter().map(|item| row(2, Some(2), Some(item))));
        expected.push(row(3, None, None));
        assert_eq!(db.run(join).unwrap(), expected);
    }

    /// A row deleted after it was indexed, its entry still there, is not
    /// joined.
    #[test]
    fn skips_deleted_inner_rows() {
        let (db, orders) = some_orders();
        let rid = db.index_lookup("orders_user_id", &[Value::Int(3)])[0];
        orders.table.heap.mark_delete(rid, 0, 0).unwrap();
        let join = orders.join(JoinType::Left, db.scan(None), vec![column(0)], None);
        let rows = db.run(join).unwrap();
        assert_eq!(rows[3], row(3, None, None));
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn init_starts_over() {
        let (db, orders) = some_orders();
        let join = orders.join(JoinType::Inner, db.scan(None), vec![column(0)], None);
        let mut executor = create_executor(&db.ctx, &join).unwrap();
        let (mut tuples, mut rids) = (Vec::new(), Vec::new());
        for _ in 0..2 {
            executor.init().unwrap();
            assert!(executor.next(&mut tuples, &mut rids, 2).unwrap());
            assert_eq!(tuples.len(), 2);
            assert!(rids.is_empty(), "joined rows have no rids");
        }
        assert!(executor.next(&mut tuples, &mut rids, 2).unwrap());
        assert_eq!(tuples.len(), 1);
        assert!(!executor.next(&mut tuples, &mut rids, 2).unwrap());
    }

    fn pages(db: &Db, table: &str) -> usize {
        let mut iter = db.catalog().get_table(table).unwrap().heap.iter().unwrap();
        let mut pages = 0;
        while iter.next_batch().unwrap().is_some() {
            pages += 1;
        }
        pages
    }

    /// Joining a few users with a table much bigger than the buffer pool
    /// reads the pages on the way to their matches, not the whole table.
    #[test]
    fn reads_only_what_the_index_points_to() {
        let db = Db::new();
        db.insert_numbered(3);
        let items: Vec<String> = (0..2000).map(|i| format!("{i:0>32}")).collect();
        let rows: Vec<_> = (1..)
            .zip(&items)
            .map(|(id, item)| (Some(id), item.as_str()))
            .collect();
        let orders = create_orders(&db, &["user_id"], &rows);
        let n = pages(&db, "orders");
        assert!(n > 20, "orders does not fit in the buffer pool");

        let join = orders.join(JoinType::Inner, db.scan(None), vec![column(0)], None);
        db.bpm.flush_all_pages().unwrap();
        let before = db.disk.num_reads();
        let rows = db.run(join).unwrap();
        let reads = db.disk.num_reads() - before;
        assert_eq!(rows.len(), 3);
        assert!(reads < 10, "{reads} reads, orders has {n} pages");
    }

    /// The index is looked up for each outer row, so the outer side needs
    /// one.
    #[test]
    fn unknown_index_fails() {
        let db = Db::new();
        db.insert_numbered(1);
        let schema = Schema::new(vec![Column::new("id", DataType::Int, false)]);
        let join = NestedIndexJoinPlan::new(
            JoinType::Inner,
            db.scan(None),
            db.users.oid,
            schema,
            999,
            "gone",
            vec![column(0)],
            None,
        );
        assert!(matches!(
            db.run(join.into()),
            Err(ExecutionError::IndexNotFound(999))
        ));
    }
}
