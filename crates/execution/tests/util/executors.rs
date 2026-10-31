//! A database to run executors against, and plans for the executor tests.

use std::sync::Arc;

use catalog::{Catalog, TableInfo};
use common::{Column, DataType, PageId, Rid, Schema, Tuple, Value};
use storage::buffer::arc_replacer::AccessType;
use storage::buffer::buffer_pool_manager::BufferPoolManager;
use storage::buffer::traced_buffer_pool_manager::TracedBufferPoolManager;
use storage::disk::disk_manager::DiskManager;
use storage::index::{Index, IndexTrait};
use storage::page::intermediate_result_page::IntermediateResultPage;
use tempfile::TempDir;

use super::{Executor, create_executor};
use crate::Result;
use crate::execution_context::ExecutionContext;
use crate::expressions::{
    AbstractExpr, ColumnValueExpr, ComparisonExpr, ComparisonOp, ConstantExpr, UNNAMED_COLUMN,
};
use crate::plans::{AbstractPlanNode, InsertPlan, SeqScanPlan, ValuesPlan};

/// Small, so that scans of a few hundred rows evict pages.
const NUM_FRAMES: usize = 8;

/// Small, so that every executor is pulled more than once.
pub const BATCH_SIZE: usize = 2;

/// A database whose catalog has one table:
/// `users (id INT NOT NULL, name VARCHAR(32))`.
pub struct Db {
    _dir: TempDir,
    pub disk: Arc<DiskManager>,
    pub bpm: Arc<BufferPoolManager>,
    catalog: Arc<Catalog>,
    pub ctx: Arc<ExecutionContext>,
    pub users: Arc<TableInfo>,
}

impl Db {
    pub fn new() -> Self {
        let dir = TempDir::new().unwrap();
        let disk = Arc::new(DiskManager::new(dir.path().join("test.db")).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(NUM_FRAMES, Arc::clone(&disk)));
        let catalog = Arc::new(Catalog::open(Arc::clone(&bpm)).unwrap());
        let users = catalog.create_table("users", users_schema(), &[]).unwrap();
        Self {
            _dir: dir,
            disk,
            bpm,
            ctx: Arc::new(ExecutionContext::new(Arc::clone(&catalog))),
            catalog,
            users,
        }
    }

    /// Gives the joins of later plans `frames` buffers.
    pub fn set_join_frames(&mut self, frames: usize) {
        let ctx = ExecutionContext::new(Arc::clone(&self.catalog))
            .with_join_frames(frames)
            .with_aggregation_frames(self.ctx.aggregation_frames());
        self.ctx = Arc::new(ctx);
    }

    /// Gives the aggregations of later plans `frames` buffers.
    pub fn set_aggregation_frames(&mut self, frames: usize) {
        let ctx = ExecutionContext::new(Arc::clone(&self.catalog))
            .with_join_frames(self.ctx.join_frames())
            .with_aggregation_frames(frames);
        self.ctx = Arc::new(ctx);
    }

    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    /// Runs `plan` to the end and decodes the rows it produces.
    pub fn run(&self, plan: AbstractPlanNode) -> Result<Vec<Vec<Value>>> {
        let mut executor = create_executor(&self.ctx, &plan)?;
        executor.init()?;
        let (mut tuples, mut rids) = (Vec::new(), Vec::new());
        let mut rows = Vec::new();
        while executor.next(&mut tuples, &mut rids, BATCH_SIZE)? {
            assert!(!tuples.is_empty() && tuples.len() <= BATCH_SIZE);
            for tuple in &tuples {
                rows.push(tuple.decode(executor.get_output_schema())?);
            }
        }
        assert!(tuples.is_empty() && rids.is_empty());
        Ok(rows)
    }

    /// Inserts `users` rows, returning the count INSERT reports.
    pub fn insert(&self, rows: &[(i32, Option<&str>)]) -> i64 {
        let rows = rows
            .iter()
            .map(|&(id, name)| {
                vec![
                    constant(Value::Int(id), DataType::Int),
                    constant(name.map_or(Value::Null, Value::from), DataType::Varchar(32)),
                ]
            })
            .collect();
        let values = ValuesPlan::new(users_schema(), rows);
        let insert = InsertPlan::new(self.users.oid, &self.users.name, Arc::new(values.into()));
        match self.run(insert.into()).unwrap().as_slice() {
            [row] => match row.as_slice() {
                [Value::BigInt(count)] => *count,
                row => panic!("{row:?}"),
            },
            rows => panic!("{rows:?}"),
        }
    }

    /// Inserts users `1..=n`, named after their ids.
    pub fn insert_numbered(&self, n: i32) {
        let names: Vec<String> = (1..=n).map(|id| format!("user {id}")).collect();
        let rows: Vec<_> = names
            .iter()
            .zip(1..)
            .map(|(name, id)| (id, Some(name.as_str())))
            .collect();
        assert_eq!(self.insert(&rows), i64::from(n));
    }

    pub fn scan(&self, filter: Option<AbstractExpr>) -> Arc<AbstractPlanNode> {
        let scan = SeqScanPlan::new(self.users.oid, "users", users_schema(), filter);
        Arc::new(scan.into())
    }

    /// The rids of the rows index `name` holds for the key `values`.
    pub fn index_lookup(&self, name: &str, values: &[Value]) -> Vec<Rid> {
        let index = self.catalog().get_index(name).unwrap();
        let table = self.catalog().get_table_by_oid(index.table_oid).unwrap();
        let key = Tuple::encode(&index.key_schema, values).unwrap();
        let mut locators = Vec::new();
        index.index.scan_key(&key, &mut locators).unwrap();
        locators
            .iter()
            .map(|locator| self.ctx.locate(&table, locator).unwrap().unwrap())
            .collect()
    }

    /// The rows the index `name` holds for the key made of `values`, in index
    /// order.
    pub fn index_rows(&self, name: &str, values: &[Value]) -> Vec<Vec<Value>> {
        let index = self.catalog().get_index(name).unwrap();
        let table = self.catalog().get_table_by_oid(index.table_oid).unwrap();
        self.index_lookup(name, values)
            .into_iter()
            .map(|rid| {
                let (meta, tuple) = table.heap.get(rid).unwrap();
                assert!(!meta.is_deleted, "index {name} points at a deleted row");
                tuple.decode(&table.schema).unwrap()
            })
            .collect()
    }

    /// Every entry of the index `name`, in order, as its raw bytes.
    pub fn index_entries(&self, name: &str) -> Vec<Vec<u8>> {
        let index = self.catalog().get_index(name).unwrap();
        let Index::BtreeIndex(btree) = &*index.index;
        btree
            .tree()
            .begin()
            .unwrap()
            .map(|entry| entry.unwrap().0)
            .collect()
    }

    /// The buffer pool the index `name` goes through, which can record the
    /// pages a statement reads and writes in the index.
    pub fn index_pages(&self, name: &str) -> Arc<TracedBufferPoolManager> {
        let index = self.catalog().get_index(name).unwrap();
        let Index::BtreeIndex(btree) = &*index.index;
        Arc::clone(btree.tree().bpm())
    }

    /// Creates `accounts`, laid out as [`accounts_schema`] with `id` as its
    /// primary key and an index `accounts_name` on `name`, which stores
    /// primary keys. Inserts `rows`, as `(id, name, balance)`.
    pub fn create_accounts(&self, rows: &[(i32, Option<&str>, i32)]) -> Arc<TableInfo> {
        let table = self
            .catalog()
            .create_table("accounts", accounts_schema(), &["id"])
            .unwrap();
        self.catalog()
            .create_index("accounts_name", "accounts", &["name"])
            .unwrap();
        let rows = rows
            .iter()
            .map(|&(id, name, balance)| {
                account(id, name, balance)
                    .into_iter()
                    .zip(accounts_schema().columns())
                    .map(|(value, column)| constant(value, column.data_type))
                    .collect()
            })
            .collect();
        let values = ValuesPlan::new(accounts_schema(), rows);
        let insert = InsertPlan::new(table.oid, &table.name, Arc::new(values.into()));
        self.run(insert.into()).unwrap();
        table
    }

    /// A scan of `accounts`, keeping the rows that pass `filter`.
    pub fn scan_accounts(
        &self,
        table: &TableInfo,
        filter: Option<AbstractExpr>,
    ) -> Arc<AbstractPlanNode> {
        let scan = SeqScanPlan::new(table.oid, &table.name, accounts_schema(), filter);
        Arc::new(scan.into())
    }

    /// The users table's rows, in storage order.
    pub fn users_rows(&self) -> Vec<Vec<Value>> {
        let scan = Arc::unwrap_or_clone(self.scan(None));
        self.run(scan).unwrap()
    }
}

pub fn users_schema() -> Schema {
    Schema::new(vec![
        Column::new("id", DataType::Int, false),
        Column::new("name", DataType::Varchar(32), true),
    ])
}

/// A `users` row.
pub fn user(id: i32, name: Option<&str>) -> Vec<Value> {
    vec![Value::Int(id), name.map_or(Value::Null, Value::from)]
}

/// `accounts (id INT NOT NULL, name VARCHAR(32), balance INT NOT NULL)`; see
/// [`Db::create_accounts`].
pub fn accounts_schema() -> Schema {
    Schema::new(vec![
        Column::new("id", DataType::Int, false),
        Column::new("name", DataType::Varchar(32), true),
        Column::new("balance", DataType::Int, false),
    ])
}

/// An `accounts` row.
pub fn account(id: i32, name: Option<&str>, balance: i32) -> Vec<Value> {
    vec![
        Value::Int(id),
        name.map_or(Value::Null, Value::from),
        Value::Int(balance),
    ]
}

/// Column `idx` of `accounts`.
pub fn account_column(idx: usize) -> AbstractExpr {
    let schema = accounts_schema();
    ColumnValueExpr::new(0, idx, schema.column(idx).unwrap().clone()).into()
}

/// Column `idx` of `users`.
pub fn column(idx: usize) -> AbstractExpr {
    let schema = users_schema();
    ColumnValueExpr::new(0, idx, schema.column(idx).unwrap().clone()).into()
}

pub fn constant(value: Value, data_type: DataType) -> AbstractExpr {
    let nullable = value.is_null();
    ConstantExpr::new(value, Column::new(UNNAMED_COLUMN, data_type, nullable)).into()
}

pub fn int(v: i32) -> AbstractExpr {
    constant(Value::Int(v), DataType::Int)
}

pub fn compare(op: ComparisonOp, left: AbstractExpr, right: AbstractExpr) -> AbstractExpr {
    ComparisonExpr::new(op, left, right).unwrap().into()
}

/// The rows of `tuples`, decoded with `schema`.
pub fn decode(tuples: &[Tuple], schema: &Schema) -> Vec<Vec<Value>> {
    tuples.iter().map(|t| t.decode(schema).unwrap()).collect()
}

/// Whether `page_id` is still an intermediate result page, rather than the
/// zeroes a freed page reads as.
pub fn is_intermediate_page(db: &Db, page_id: PageId) -> bool {
    let guard = db.bpm.read_page(page_id, AccessType::Unknown);
    IntermediateResultPage::new(guard.get_data()).is_ok()
}
