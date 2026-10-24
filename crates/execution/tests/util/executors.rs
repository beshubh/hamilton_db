//! A database to run executors against, and plans for the executor tests.

use std::sync::Arc;

use catalog::{Catalog, TableInfo};
use common::{Column, DataType, Rid, Schema, Tuple, Value};
use storage::buffer::buffer_pool_manager::BufferPoolManager;
use storage::disk::disk_manager::DiskManager;
use storage::index::IndexTrait;
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
        let ctx = ExecutionContext::new(Arc::clone(&self.catalog)).with_join_frames(frames);
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
        let insert = InsertPlan::new(self.users.oid, Arc::new(values.into()));
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

    /// The record ids index `name` holds for the key made of `values`.
    pub fn index_lookup(&self, name: &str, values: &[Value]) -> Vec<Rid> {
        let index = self.catalog().get_index(name).unwrap();
        let key = Tuple::encode(&index.key_schema, values).unwrap();
        let mut rids = Vec::new();
        index.index.scan_key(&key, &mut rids).unwrap();
        rids
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
