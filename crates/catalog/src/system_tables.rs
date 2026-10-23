//! The system tables that store the catalog, and their row formats.
//!
//! ```text
//! tables         (oid INT, name VARCHAR, first_page_id INT)
//! columns        (table_oid INT, position INT, name VARCHAR, type_id INT,
//!                 max_len INT, nullable BOOL)
//! indexes        (oid INT, name VARCHAR, table_oid INT, header_page_id INT,
//!                 is_primary_key BOOL)
//! index_columns  (index_oid INT, position INT, column INT)
//! ```
//!
//! Each is an ordinary table heap whose first page is recorded on page 0.
//! A table's or index's own row is inserted after the rows that describe it
//! (its columns, its key columns), so a crash part way through a create
//! leaves only rows nothing points to, which loading ignores.

use std::sync::{Arc, LazyLock};

use common::{Column, DataType, PageId, Schema, Tuple, Value};
use storage::buffer::buffer_pool_manager::BufferPoolManager;
use storage::table::table_heap::TableHeap;
use storage::table::table_page::TupleMeta;

use crate::catalog::{IndexOid, MAX_NAME_LEN, TableOid};
use crate::error::{CatalogError, Result};
use crate::header_page::SystemTableRoots;

/// Catalog rows are written outside of any transaction, as if committed at
/// the start of time, so every snapshot sees them.
const CATALOG_TS: u64 = 0;

fn name_column(name: &str) -> Column {
    Column::new(name, DataType::Varchar(MAX_NAME_LEN), false)
}

fn int_column(name: &str) -> Column {
    Column::new(name, DataType::Int, false)
}

static TABLES_SCHEMA: LazyLock<Schema> = LazyLock::new(|| {
    Schema::new(vec![
        int_column("oid"),
        name_column("name"),
        int_column("first_page_id"),
    ])
});

static COLUMNS_SCHEMA: LazyLock<Schema> = LazyLock::new(|| {
    Schema::new(vec![
        int_column("table_oid"),
        int_column("position"),
        name_column("name"),
        int_column("type_id"),
        int_column("max_len"),
        Column::new("nullable", DataType::Bool, false),
    ])
});

static INDEXES_SCHEMA: LazyLock<Schema> = LazyLock::new(|| {
    Schema::new(vec![
        int_column("oid"),
        name_column("name"),
        int_column("table_oid"),
        int_column("header_page_id"),
        Column::new("is_primary_key", DataType::Bool, false),
    ])
});

static INDEX_COLUMNS_SCHEMA: LazyLock<Schema> = LazyLock::new(|| {
    Schema::new(vec![
        int_column("index_oid"),
        int_column("position"),
        int_column("column"),
    ])
});

/// A row of `tables`.
pub(crate) struct TableRow {
    pub oid: TableOid,
    pub name: String,
    pub first_page_id: PageId,
}

/// A row of `columns`: column `position` of table `table_oid`.
pub(crate) struct ColumnRow {
    pub table_oid: TableOid,
    pub position: usize,
    pub column: Column,
}

/// A row of `indexes`.
pub(crate) struct IndexRow {
    pub oid: IndexOid,
    pub name: String,
    pub table_oid: TableOid,
    pub header_page_id: PageId,
    pub is_primary_key: bool,
}

/// A row of `index_columns`: key column `position` of index `index_oid` is
/// column `column` of its table.
pub(crate) struct IndexColumnRow {
    pub index_oid: IndexOid,
    pub position: usize,
    pub column: usize,
}

/// The heaps of the four system tables.
pub(crate) struct SystemTables {
    tables: TableHeap,
    columns: TableHeap,
    indexes: TableHeap,
    index_columns: TableHeap,
}

impl SystemTables {
    /// Creates the system tables, empty.
    pub fn create(bpm: &Arc<BufferPoolManager>) -> Result<Self> {
        Ok(Self {
            tables: TableHeap::create(Arc::clone(bpm))?,
            columns: TableHeap::create(Arc::clone(bpm))?,
            indexes: TableHeap::create(Arc::clone(bpm))?,
            index_columns: TableHeap::create(Arc::clone(bpm))?,
        })
    }

    pub fn open(bpm: &Arc<BufferPoolManager>, roots: SystemTableRoots) -> Result<Self> {
        Ok(Self {
            tables: TableHeap::open(Arc::clone(bpm), roots.tables)?,
            columns: TableHeap::open(Arc::clone(bpm), roots.columns)?,
            indexes: TableHeap::open(Arc::clone(bpm), roots.indexes)?,
            index_columns: TableHeap::open(Arc::clone(bpm), roots.index_columns)?,
        })
    }

    pub fn roots(&self) -> SystemTableRoots {
        SystemTableRoots {
            tables: self.tables.first_page_id(),
            columns: self.columns.first_page_id(),
            indexes: self.indexes.first_page_id(),
            index_columns: self.index_columns.first_page_id(),
        }
    }

    pub fn insert_table(&self, row: &TableRow) -> Result<()> {
        let values = [
            oid_value(row.oid),
            Value::Varchar(row.name.clone()),
            Value::Int(row.first_page_id),
        ];
        insert(&self.tables, &TABLES_SCHEMA, &values)
    }

    pub fn insert_column(&self, row: &ColumnRow) -> Result<()> {
        let (type_id, max_len) = encode_type(row.column.data_type);
        let values = [
            oid_value(row.table_oid),
            int_value(row.position),
            Value::Varchar(row.column.name.clone()),
            Value::Int(type_id),
            Value::Int(max_len),
            Value::Bool(row.column.nullable),
        ];
        insert(&self.columns, &COLUMNS_SCHEMA, &values)
    }

    pub fn insert_index(&self, row: &IndexRow) -> Result<()> {
        let values = [
            oid_value(row.oid),
            Value::Varchar(row.name.clone()),
            oid_value(row.table_oid),
            Value::Int(row.header_page_id),
            Value::Bool(row.is_primary_key),
        ];
        insert(&self.indexes, &INDEXES_SCHEMA, &values)
    }

    pub fn insert_index_column(&self, row: &IndexColumnRow) -> Result<()> {
        let values = [
            oid_value(row.index_oid),
            int_value(row.position),
            int_value(row.column),
        ];
        insert(&self.index_columns, &INDEX_COLUMNS_SCHEMA, &values)
    }

    pub fn scan_tables(&self) -> Result<Vec<TableRow>> {
        scan(&self.tables, &TABLES_SCHEMA, |values| {
            Ok(TableRow {
                oid: as_oid(&values[0])?,
                name: as_string(&values[1])?,
                first_page_id: as_int(&values[2])?,
            })
        })
    }

    pub fn scan_columns(&self) -> Result<Vec<ColumnRow>> {
        scan(&self.columns, &COLUMNS_SCHEMA, |values| {
            let data_type = decode_type(as_int(&values[3])?, as_int(&values[4])?)?;
            Ok(ColumnRow {
                table_oid: as_oid(&values[0])?,
                position: as_index(&values[1])?,
                column: Column::new(as_string(&values[2])?, data_type, as_bool(&values[5])?),
            })
        })
    }

    pub fn scan_indexes(&self) -> Result<Vec<IndexRow>> {
        scan(&self.indexes, &INDEXES_SCHEMA, |values| {
            Ok(IndexRow {
                oid: as_oid(&values[0])?,
                name: as_string(&values[1])?,
                table_oid: as_oid(&values[2])?,
                header_page_id: as_int(&values[3])?,
                is_primary_key: as_bool(&values[4])?,
            })
        })
    }

    pub fn scan_index_columns(&self) -> Result<Vec<IndexColumnRow>> {
        scan(&self.index_columns, &INDEX_COLUMNS_SCHEMA, |values| {
            Ok(IndexColumnRow {
                index_oid: as_oid(&values[0])?,
                position: as_index(&values[1])?,
                column: as_index(&values[2])?,
            })
        })
    }
}

fn insert(heap: &TableHeap, schema: &Schema, values: &[Value]) -> Result<()> {
    let tuple = Tuple::encode(schema, values)?;
    heap.insert(&TupleMeta::new(CATALOG_TS), &tuple)?;
    Ok(())
}

fn scan<R>(
    heap: &TableHeap,
    schema: &Schema,
    decode: impl Fn(&[Value]) -> Result<R>,
) -> Result<Vec<R>> {
    let mut rows = Vec::new();
    for row in heap.iter()? {
        let (_, tuple) = row?;
        rows.push(decode(&tuple.decode(schema)?)?);
    }
    Ok(rows)
}

fn encode_type(data_type: DataType) -> (i32, i32) {
    match data_type {
        DataType::Int => (0, 0),
        DataType::BigInt => (1, 0),
        DataType::Bool => (2, 0),
        DataType::Varchar(max_len) => (3, max_len as i32),
        DataType::Array => (4, 0),
    }
}

fn decode_type(type_id: i32, max_len: i32) -> Result<DataType> {
    Ok(match type_id {
        0 => DataType::Int,
        1 => DataType::BigInt,
        2 => DataType::Bool,
        3 => DataType::Varchar(max_len as u32),
        4 => DataType::Array,
        other => return Err(CatalogError::Corrupt(format!("unknown type id {other}"))),
    })
}

fn oid_value(oid: u32) -> Value {
    Value::Int(oid as i32)
}

fn int_value(n: usize) -> Value {
    Value::Int(n as i32)
}

fn as_int(value: &Value) -> Result<i32> {
    match *value {
        Value::Int(n) => Ok(n),
        _ => Err(corrupt_value(value)),
    }
}

fn as_oid(value: &Value) -> Result<u32> {
    as_int(value).map(|n| n as u32)
}

fn as_index(value: &Value) -> Result<usize> {
    usize::try_from(as_int(value)?).map_err(|_| corrupt_value(value))
}

fn as_bool(value: &Value) -> Result<bool> {
    match *value {
        Value::Bool(b) => Ok(b),
        _ => Err(corrupt_value(value)),
    }
}

fn as_string(value: &Value) -> Result<String> {
    match value {
        Value::Varchar(s) => Ok(s.clone()),
        _ => Err(corrupt_value(value)),
    }
}

fn corrupt_value(value: &Value) -> CatalogError {
    CatalogError::Corrupt(format!("unexpected value {value} in a system table"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn types_round_trip() {
        for data_type in [
            DataType::Int,
            DataType::BigInt,
            DataType::Bool,
            DataType::Varchar(255),
        ] {
            let (type_id, max_len) = encode_type(data_type);
            assert_eq!(decode_type(type_id, max_len).unwrap(), data_type);
        }
        assert!(matches!(decode_type(9, 0), Err(CatalogError::Corrupt(_))));
    }
}
