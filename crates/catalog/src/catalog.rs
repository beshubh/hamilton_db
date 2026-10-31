use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

use common::{Column, DataType, PageId, Schema, ValueError};
use parking_lot::RwLock;
use storage::StorageError;
use storage::buffer::arc_replacer::AccessType;
use storage::buffer::buffer_pool_manager::BufferPoolManager;
use storage::index::btree_index::BtreeIndex;
use storage::index::{Index, IndexMetadata, LocatorKind};
use storage::table::table_heap::TableHeap;

use crate::error::{CatalogError, Result};
use crate::header_page::{CATALOG_PAGE_ID, SystemTableRoots};
use crate::system_tables::{ColumnRow, IndexColumnRow, IndexRow, SystemTables, TableRow};

/// Identifies a table. Never reused.
pub type TableOid = u32;

/// Identifies an index. Never reused.
pub type IndexOid = u32;

/// Longest table, column or index name, in bytes.
pub const MAX_NAME_LEN: u32 = 128;

/// A table: its name, its schema, and the heap that stores its rows.
pub struct TableInfo {
    pub oid: TableOid,
    pub name: String,
    pub schema: Schema,
    /// The one heap for this table. Every user of the table must go through
    /// it, since inserts rely on its lock on the last page.
    pub heap: Arc<TableHeap>,
}

impl TableInfo {
    /// First page of the table's heap.
    pub fn first_page_id(&self) -> PageId {
        self.heap.first_page_id()
    }
}

impl fmt::Debug for TableInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TableInfo")
            .field("oid", &self.oid)
            .field("name", &self.name)
            .field("schema", &self.schema)
            .field("first_page_id", &self.first_page_id())
            .finish()
    }
}

/// A B+ tree index on some columns of a table.
pub struct IndexInfo {
    pub oid: IndexOid,
    pub name: String,
    pub table_oid: TableOid,
    pub table_name: String,
    /// Positions in the table's schema of the key columns, in key order.
    pub key_columns: Vec<usize>,
    /// The key columns as a schema of their own, for encoding keys.
    pub key_schema: Schema,
    /// Header page of the index's B+ tree.
    pub header_page_id: PageId,
    /// Whether this is its table's primary key, which is unique.
    pub is_primary_key: bool,
    /// Which `RowLocator` the index stores. Not saved to disk: it follows
    /// from whether the table has a primary key.
    pub locator: LocatorKind,
    /// The one open handle on the index's B+ tree.
    pub index: Arc<Index>,
}

impl fmt::Debug for IndexInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IndexInfo")
            .field("oid", &self.oid)
            .field("name", &self.name)
            .field("table_oid", &self.table_oid)
            .field("table_name", &self.table_name)
            .field("key_columns", &self.key_columns)
            .field("key_schema", &self.key_schema)
            .field("header_page_id", &self.header_page_id)
            .field("is_primary_key", &self.is_primary_key)
            .field("locator", &self.locator)
            .finish()
    }
}

/// The tables and indexes of the database.
///
/// The catalog is stored in system tables (see `system_tables`) whose first
/// pages are recorded on page 0, and is loaded into memory when opened.
/// Lookups are served from memory; creates write through to the system
/// tables. Like every other page, catalog pages reach disk when the buffer
/// pool flushes them.
pub struct Catalog {
    bpm: Arc<BufferPoolManager>,
    system: SystemTables,
    /// Creates hold the write lock from the name check until the new entry is
    /// in memory, so DDL is serialized.
    state: RwLock<CatalogState>,
}

#[derive(Default)]
struct CatalogState {
    tables: HashMap<TableOid, Arc<TableInfo>>,
    table_names: HashMap<String, TableOid>,
    indexes: HashMap<IndexOid, Arc<IndexInfo>>,
    index_names: HashMap<String, IndexOid>,
    /// Each table's indexes, in the order they were created.
    table_indexes: HashMap<TableOid, Vec<IndexOid>>,
    next_table_oid: TableOid,
    next_index_oid: IndexOid,
}

impl CatalogState {
    fn add_table(&mut self, info: Arc<TableInfo>) -> Result<()> {
        if self.table_names.contains_key(&info.name) {
            return Err(CatalogError::TableExists(info.name.clone()));
        }
        self.table_names.insert(info.name.clone(), info.oid);
        self.tables.insert(info.oid, info);
        Ok(())
    }

    fn add_index(&mut self, info: Arc<IndexInfo>) -> Result<()> {
        if self.index_names.contains_key(&info.name) {
            return Err(CatalogError::IndexExists(info.name.clone()));
        }
        self.index_names.insert(info.name.clone(), info.oid);
        self.table_indexes
            .entry(info.table_oid)
            .or_default()
            .push(info.oid);
        self.indexes.insert(info.oid, info);
        Ok(())
    }

    fn table(&self, name: &str) -> Option<&Arc<TableInfo>> {
        self.tables.get(self.table_names.get(name)?)
    }
}

impl Catalog {
    /// Opens the catalog of the database behind `bpm`. A database with no
    /// pages yet is given an empty catalog, with its header on page 0.
    pub fn open(bpm: Arc<BufferPoolManager>) -> Result<Self> {
        let roots = match bpm.checked_read_page(CATALOG_PAGE_ID, AccessType::Lookup) {
            Ok(guard) => SystemTableRoots::read_from(guard.get_data())?,
            Err(StorageError::InvalidPageId(_)) => return Self::bootstrap(bpm),
            Err(e) => return Err(e.into()),
        };
        Self::load(bpm, roots)
    }

    /// Creates the system tables and points page 0 at them.
    fn bootstrap(bpm: Arc<BufferPoolManager>) -> Result<Self> {
        let page_id = bpm.new_page();
        if page_id != CATALOG_PAGE_ID {
            return Err(CatalogError::Corrupt(format!(
                "database has pages but no catalog header (got page {page_id})"
            )));
        }
        let system = SystemTables::create(&bpm)?;
        let mut guard = bpm.checked_write_page(CATALOG_PAGE_ID, AccessType::Unknown)?;
        system.roots().write_to(guard.get_data_mut());
        drop(guard);
        Ok(Self {
            bpm,
            system,
            state: RwLock::new(CatalogState::default()),
        })
    }

    pub fn bpm(&self) -> &BufferPoolManager {
        &self.bpm
    }

    /// Reads the system tables into memory. Rows left by a create that did
    /// not finish (columns of a table with no `tables` row, and so on) are
    /// skipped, but their oids are not reused.
    fn load(bpm: Arc<BufferPoolManager>, roots: SystemTableRoots) -> Result<Self> {
        let system = SystemTables::open(&bpm, roots)?;
        let mut state = CatalogState::default();

        let mut columns: HashMap<TableOid, Vec<ColumnRow>> = HashMap::new();
        for row in system.scan_columns()? {
            state.next_table_oid = state.next_table_oid.max(row.table_oid + 1);
            columns.entry(row.table_oid).or_default().push(row);
        }
        for row in system.scan_tables()? {
            state.next_table_oid = state.next_table_oid.max(row.oid + 1);
            let columns = columns.remove(&row.oid).unwrap_or_default();
            let columns = in_position_order(columns, |c| c.position, &row.name)?;
            let info = TableInfo {
                oid: row.oid,
                schema: Schema::new(columns.into_iter().map(|c| c.column).collect()),
                heap: Arc::new(TableHeap::open(Arc::clone(&bpm), row.first_page_id)?),
                name: row.name,
            };
            state.add_table(Arc::new(info)).map_err(corrupt)?;
        }

        let mut key_columns: HashMap<IndexOid, Vec<IndexColumnRow>> = HashMap::new();
        for row in system.scan_index_columns()? {
            state.next_index_oid = state.next_index_oid.max(row.index_oid + 1);
            key_columns.entry(row.index_oid).or_default().push(row);
        }
        let index_rows = system.scan_indexes()?;
        // Find the tables with a primary key first: their other indexes
        // store it.
        let with_primary_key: HashSet<TableOid> = index_rows
            .iter()
            .filter(|row| row.is_primary_key)
            .map(|row| row.table_oid)
            .collect();
        for row in index_rows {
            state.next_index_oid = state.next_index_oid.max(row.oid + 1);
            let table = state.tables.get(&row.table_oid).ok_or_else(|| {
                CatalogError::Corrupt(format!(
                    "index {} is on missing table {}",
                    row.name, row.table_oid
                ))
            })?;
            let key = key_columns.remove(&row.oid).unwrap_or_default();
            let key = in_position_order(key, |c| c.position, &row.name)?;
            let key: Vec<usize> = key.into_iter().map(|c| c.column).collect();
            let key_schema = key_schema(&table.schema, &key).ok_or_else(|| {
                CatalogError::Corrupt(format!("index {} has a bad key column", row.name))
            })?;
            let locator = locator_kind(row.is_primary_key, with_primary_key.contains(&table.oid));
            let index = open_index(&bpm, table, &row, &key, &key_schema, locator)?;
            let info = IndexInfo {
                oid: row.oid,
                name: row.name,
                table_oid: table.oid,
                table_name: table.name.clone(),
                key_columns: key,
                key_schema,
                header_page_id: row.header_page_id,
                is_primary_key: row.is_primary_key,
                locator,
                index,
            };
            state.add_index(Arc::new(info)).map_err(corrupt)?;
        }
        // Indexes were added in scan order, which is creation order.

        Ok(Self {
            bpm,
            system,
            state: RwLock::new(state),
        })
    }

    /// Creates a table with an empty heap. Column names must be unique.
    ///
    /// A non-empty `primary_key` names the table's primary key columns, in
    /// key order, which must be NOT NULL. The table is then created with a
    /// primary key index on them, named `<name>_pkey`.
    pub fn create_table(
        &self,
        name: &str,
        schema: Schema,
        primary_key: &[&str],
    ) -> Result<Arc<TableInfo>> {
        check_name(name)?;
        let mut seen = HashSet::new();
        for column in schema.columns() {
            check_name(&column.name)?;
            if !seen.insert(column.name.as_str()) {
                return Err(CatalogError::DuplicateColumn(column.name.clone()));
            }
            if column.data_type == DataType::Array {
                return Err(ValueError::NotStorable {
                    column: column.name.clone(),
                    data_type: column.data_type,
                }
                .into());
            }
        }
        let pkey = if primary_key.is_empty() {
            None
        } else {
            let key = resolve_key(name, &schema, primary_key)?;
            if let Some(column) = key
                .iter()
                .map(|&i| &schema.columns()[i])
                .find(|c| c.nullable)
            {
                return Err(CatalogError::NullablePrimaryKey(column.name.clone()));
            }
            let index_name = format!("{name}_pkey");
            check_name(&index_name)?;
            Some((index_name, key))
        };

        let mut state = self.state.write();
        if state.table_names.contains_key(name) {
            return Err(CatalogError::TableExists(name.to_owned()));
        }
        if let Some((index_name, _)) = &pkey
            && state.index_names.contains_key(index_name)
        {
            return Err(CatalogError::IndexExists(index_name.clone()));
        }
        // Taken before writing any rows, so a failed create's rows are never
        // mistaken for a later table's.
        let oid = state.next_table_oid;
        state.next_table_oid += 1;

        let heap = TableHeap::create(Arc::clone(&self.bpm))?;
        for (position, column) in schema.columns().iter().enumerate() {
            self.system.insert_column(&ColumnRow {
                table_oid: oid,
                position,
                column: column.clone(),
            })?;
        }
        self.system.insert_table(&TableRow {
            oid,
            name: name.to_owned(),
            first_page_id: heap.first_page_id(),
        })?;

        let info = Arc::new(TableInfo {
            oid,
            name: name.to_owned(),
            schema,
            heap: Arc::new(heap),
        });
        state.add_table(Arc::clone(&info))?;
        if let Some((index_name, key)) = pkey {
            self.add_index(&mut state, &index_name, &info, key, true)?;
        }
        Ok(info)
    }

    pub fn get_table(&self, name: &str) -> Option<Arc<TableInfo>> {
        self.state.read().table(name).cloned()
    }

    pub fn get_table_by_oid(&self, oid: TableOid) -> Option<Arc<TableInfo>> {
        self.state.read().tables.get(&oid).cloned()
    }

    /// Every table, in name order.
    pub fn tables(&self) -> Vec<Arc<TableInfo>> {
        let mut tables: Vec<_> = self.state.read().tables.values().cloned().collect();
        tables.sort_by(|a, b| a.name.cmp(&b.name));
        tables
    }

    /// Records an index on `key_columns` of `table_name` and allocates the
    /// header page for its B+ tree. Index names are unique across the
    /// database. The index is not a primary key: a table gets that only when
    /// it is created.
    pub fn create_index(
        &self,
        name: &str,
        table_name: &str,
        key_columns: &[&str],
    ) -> Result<Arc<IndexInfo>> {
        check_name(name)?;
        let mut state = self.state.write();
        if state.index_names.contains_key(name) {
            return Err(CatalogError::IndexExists(name.to_owned()));
        }
        let table = state
            .table(table_name)
            .cloned()
            .ok_or_else(|| CatalogError::TableNotFound(table_name.to_owned()))?;
        let key = resolve_key(table_name, &table.schema, key_columns)?;
        self.add_index(&mut state, name, &table, key, false)
    }

    /// Writes out a new index on the `key` columns of `table`, whose name the
    /// caller has checked is free.
    fn add_index(
        &self,
        state: &mut CatalogState,
        name: &str,
        table: &TableInfo,
        key: Vec<usize>,
        is_primary_key: bool,
    ) -> Result<Arc<IndexInfo>> {
        let key_schema = key_schema(&table.schema, &key).expect("key columns were resolved");
        let table_has_primary_key = is_primary_key || primary_key_index(state, table.oid).is_some();
        let locator = locator_kind(is_primary_key, table_has_primary_key);

        let oid = state.next_index_oid;
        state.next_index_oid += 1;

        let row = IndexRow {
            oid,
            name: name.to_owned(),
            table_oid: table.oid,
            header_page_id: self.bpm.new_page(),
            is_primary_key,
        };
        // Opening the tree formats its header page, which dirties it, so it
        // is written out and keeps its id across a restart.
        let index = open_index(&self.bpm, table, &row, &key, &key_schema, locator)?;

        for (position, &column) in key.iter().enumerate() {
            self.system.insert_index_column(&IndexColumnRow {
                index_oid: oid,
                position,
                column,
            })?;
        }
        self.system.insert_index(&row)?;

        let info = Arc::new(IndexInfo {
            oid,
            name: name.to_owned(),
            table_oid: table.oid,
            table_name: table.name.clone(),
            key_columns: key,
            key_schema,
            header_page_id: row.header_page_id,
            is_primary_key,
            locator,
            index,
        });
        state.add_index(Arc::clone(&info))?;
        Ok(info)
    }

    pub fn get_index(&self, name: &str) -> Option<Arc<IndexInfo>> {
        let state = self.state.read();
        state.indexes.get(state.index_names.get(name)?).cloned()
    }

    pub fn get_index_by_oid(&self, oid: IndexOid) -> Option<Arc<IndexInfo>> {
        self.state.read().indexes.get(&oid).cloned()
    }

    /// The primary key index of the table `table_oid`, if it has one.
    pub fn primary_key_index(&self, table_oid: TableOid) -> Option<Arc<IndexInfo>> {
        primary_key_index(&self.state.read(), table_oid).cloned()
    }

    /// The indexes on `table_name`, in the order they were created. Empty if
    /// the table has none or does not exist.
    pub fn table_indexes(&self, table_name: &str) -> Vec<Arc<IndexInfo>> {
        let state = self.state.read();
        let Some(oid) = state.table_names.get(table_name) else {
            return Vec::new();
        };
        state
            .table_indexes
            .get(oid)
            .into_iter()
            .flatten()
            .map(|oid| Arc::clone(&state.indexes[oid]))
            .collect()
    }
}

fn check_name(name: &str) -> Result<()> {
    if name.is_empty() || name.len() > MAX_NAME_LEN as usize {
        return Err(CatalogError::InvalidName(name.to_owned()));
    }
    Ok(())
}

fn primary_key_index(state: &CatalogState, table_oid: TableOid) -> Option<&Arc<IndexInfo>> {
    state
        .table_indexes
        .get(&table_oid)?
        .iter()
        .map(|oid| &state.indexes[oid])
        .find(|index| index.is_primary_key)
}

/// The primary key index stores rids. A secondary index stores the primary
/// key if the table has one, and rids otherwise.
fn locator_kind(is_primary_key: bool, table_has_primary_key: bool) -> LocatorKind {
    if !is_primary_key && table_has_primary_key {
        LocatorKind::PrimaryKey
    } else {
        LocatorKind::Rid
    }
}

/// Opens the B+ tree of the index `row` describes, on the `key` columns of
/// `table`, storing `locator`s.
fn open_index(
    bpm: &Arc<BufferPoolManager>,
    table: &TableInfo,
    row: &IndexRow,
    key: &[usize],
    key_schema: &Schema,
    locator: LocatorKind,
) -> Result<Arc<Index>> {
    let metadata = IndexMetadata::new(
        row.name.clone(),
        table.name.clone(),
        key.to_vec(),
        Arc::new(key_schema.clone()),
        row.is_primary_key,
        locator,
    );
    let index = BtreeIndex::new(metadata, row.header_page_id, Arc::clone(bpm))?;
    Ok(Arc::new(index.into()))
}

/// The positions in `schema` of `columns`, the key of an index on table
/// `table_name`.
fn resolve_key(table_name: &str, schema: &Schema, columns: &[&str]) -> Result<Vec<usize>> {
    if columns.is_empty() {
        return Err(CatalogError::EmptyIndexKey);
    }
    let mut key = Vec::with_capacity(columns.len());
    for &column in columns {
        let idx = schema
            .index_of(column)
            .ok_or_else(|| CatalogError::ColumnNotFound {
                table: table_name.to_owned(),
                column: column.to_owned(),
            })?;
        if key.contains(&idx) {
            return Err(CatalogError::DuplicateColumn(column.to_owned()));
        }
        key.push(idx);
    }
    Ok(key)
}

/// The columns of `schema` at `key`, or `None` if one is out of range.
fn key_schema(schema: &Schema, key: &[usize]) -> Option<Schema> {
    let columns: Option<Vec<Column>> = key.iter().map(|&i| schema.column(i).cloned()).collect();
    columns.map(Schema::new)
}

/// Sorts the rows describing `owner`'s columns by position, checking that
/// the positions are exactly 0, 1, 2, ...
fn in_position_order<T>(
    mut rows: Vec<T>,
    position: impl Fn(&T) -> usize,
    owner: &str,
) -> Result<Vec<T>> {
    rows.sort_by_key(&position);
    if rows.iter().enumerate().any(|(i, row)| position(row) != i) {
        return Err(CatalogError::Corrupt(format!(
            "column positions of {owner} are not contiguous"
        )));
    }
    Ok(rows)
}

/// A duplicate name found while loading means the system tables are bad.
fn corrupt(err: CatalogError) -> CatalogError {
    CatalogError::Corrupt(err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::{Tuple, Value};
    use storage::disk::disk_manager::DiskManager;
    use storage::index::IndexTrait;
    use storage::page::page_header::PageType;
    use storage::table::table_page::{TablePage, TupleMeta};
    use tempfile::TempDir;

    struct Db {
        dir: TempDir,
        dm: Arc<DiskManager>,
        catalog: Catalog,
    }

    impl Db {
        fn new(num_frames: usize) -> Self {
            let dir = TempDir::new().unwrap();
            let (dm, bpm) = open_files(&dir, num_frames);
            let catalog = Catalog::open(bpm).unwrap();
            Self { dir, dm, catalog }
        }

        /// Flushes everything, shuts the files down and opens them again.
        fn restart(self) -> Self {
            let Db { dir, dm, catalog } = self;
            catalog.bpm.flush_all_pages().unwrap();
            dm.shut_down().unwrap();
            drop((catalog, dm));
            let (dm, bpm) = open_files(&dir, 8);
            let catalog = Catalog::open(bpm).unwrap();
            Self { dir, dm, catalog }
        }
    }

    fn open_files(dir: &TempDir, num_frames: usize) -> (Arc<DiskManager>, Arc<BufferPoolManager>) {
        let dm = Arc::new(DiskManager::new(dir.path().join("test.db")).unwrap());
        let bpm = Arc::new(BufferPoolManager::new(num_frames, Arc::clone(&dm)));
        (dm, bpm)
    }

    fn users() -> Schema {
        Schema::new(vec![
            Column::new("id", DataType::Int, false),
            Column::new("name", DataType::Varchar(64), true),
            Column::new("score", DataType::BigInt, false),
            Column::new("active", DataType::Bool, false),
        ])
    }

    /// Checks that `a` and `b` describe the same index.
    fn assert_same_index(a: &IndexInfo, b: &IndexInfo) {
        assert_eq!(format!("{a:?}"), format!("{b:?}"));
    }

    fn names(indexes: &[Arc<IndexInfo>]) -> Vec<&str> {
        indexes.iter().map(|i| i.name.as_str()).collect()
    }

    #[test]
    fn new_database_puts_catalog_header_on_page_0() {
        let db = Db::new(8);
        let guard = db
            .catalog
            .bpm
            .read_page(CATALOG_PAGE_ID, AccessType::Unknown);
        let roots = SystemTableRoots::read_from(guard.get_data()).unwrap();
        assert_eq!(roots, db.catalog.system.roots());
        assert!(db.catalog.get_table("users").is_none());
        assert!(db.catalog.get_index("users_id").is_none());
    }

    #[test]
    fn create_and_get_table() {
        let db = Db::new(8);
        let created = db.catalog.create_table("users", users(), &[]).unwrap();
        let got = db.catalog.get_table("users").unwrap();
        assert!(Arc::ptr_eq(&created, &got));
        assert!(Arc::ptr_eq(
            &db.catalog.get_table_by_oid(created.oid).unwrap(),
            &created
        ));
        assert_eq!(got.name, "users");
        assert_eq!(got.schema, users());

        // The heap is a fresh, usable table.
        let guard = db
            .catalog
            .bpm
            .read_page(got.first_page_id(), AccessType::Unknown);
        assert!(TablePage::new(guard.get_data()).is_ok());
        drop(guard);
        assert_eq!(got.heap.iter().unwrap().count(), 0);

        let other = db.catalog.create_table("orders", users(), &[]).unwrap();
        assert_ne!(other.oid, created.oid);
        assert_ne!(other.first_page_id(), created.first_page_id());

        let tables = db.catalog.tables();
        let names: Vec<_> = tables.iter().map(|t| t.name.as_str()).collect();
        assert_eq!(names, ["orders", "users"]);
    }

    #[test]
    fn create_table_rejects_bad_definitions() {
        let db = Db::new(8);
        db.catalog.create_table("users", users(), &[]).unwrap();
        assert!(matches!(
            db.catalog.create_table("users", users(), &[]),
            Err(CatalogError::TableExists(name)) if name == "users"
        ));

        let dup = Schema::new(vec![
            Column::new("a", DataType::Int, false),
            Column::new("a", DataType::Bool, false),
        ]);
        assert!(matches!(
            db.catalog.create_table("t", dup, &[]),
            Err(CatalogError::DuplicateColumn(name)) if name == "a"
        ));
        assert!(matches!(
            db.catalog.create_table("", users(), &[]),
            Err(CatalogError::InvalidName(_))
        ));
        let long = "x".repeat(MAX_NAME_LEN as usize + 1);
        assert!(matches!(
            db.catalog.create_table(&long, users(), &[]),
            Err(CatalogError::InvalidName(_))
        ));
        let long_column = Schema::new(vec![Column::new(&long, DataType::Int, false)]);
        assert!(matches!(
            db.catalog.create_table("t", long_column, &[]),
            Err(CatalogError::InvalidName(_))
        ));
        let array = Schema::new(vec![Column::new("xs", DataType::Array, false)]);
        assert!(matches!(
            db.catalog.create_table("t", array, &[]),
            Err(CatalogError::Value(ValueError::NotStorable { .. }))
        ));
        assert!(db.catalog.get_table("t").is_none());
    }

    #[test]
    fn create_and_get_index() {
        let db = Db::new(8);
        let table = db.catalog.create_table("users", users(), &[]).unwrap();
        let index = db
            .catalog
            .create_index("users_name_id", "users", &["name", "id"])
            .unwrap();
        assert_eq!(index.table_oid, table.oid);
        assert_eq!(index.table_name, "users");
        assert_eq!(index.key_columns, vec![1, 0]);
        assert_eq!(
            index.key_schema,
            Schema::new(vec![
                Column::new("name", DataType::Varchar(64), true),
                Column::new("id", DataType::Int, false),
            ])
        );
        assert!(Arc::ptr_eq(
            &db.catalog.get_index("users_name_id").unwrap(),
            &index
        ));
        assert!(Arc::ptr_eq(
            &db.catalog.get_index_by_oid(index.oid).unwrap(),
            &index
        ));
        assert_ne!(index.header_page_id, table.first_page_id());
    }

    #[test]
    fn create_index_rejects_bad_definitions() {
        let db = Db::new(8);
        db.catalog.create_table("users", users(), &[]).unwrap();
        db.catalog
            .create_index("users_id", "users", &["id"])
            .unwrap();
        assert!(matches!(
            db.catalog.create_index("users_id", "users", &["name"]),
            Err(CatalogError::IndexExists(name)) if name == "users_id"
        ));
        assert!(matches!(
            db.catalog.create_index("i", "missing", &["id"]),
            Err(CatalogError::TableNotFound(name)) if name == "missing"
        ));
        assert!(matches!(
            db.catalog.create_index("i", "users", &["nope"]),
            Err(CatalogError::ColumnNotFound { column, .. }) if column == "nope"
        ));
        assert!(matches!(
            db.catalog.create_index("i", "users", &["id", "id"]),
            Err(CatalogError::DuplicateColumn(name)) if name == "id"
        ));
        assert!(matches!(
            db.catalog.create_index("i", "users", &[]),
            Err(CatalogError::EmptyIndexKey)
        ));
        assert!(db.catalog.get_index("i").is_none());
    }

    #[test]
    fn create_table_with_primary_key() {
        let db = Db::new(8);
        let table = db
            .catalog
            .create_table("users", users(), &["score", "id"])
            .unwrap();
        let [pkey] = db.catalog.table_indexes("users").try_into().unwrap();
        assert_eq!(pkey.name, "users_pkey");
        assert!(pkey.is_primary_key);
        assert_eq!(pkey.table_oid, table.oid);
        assert_eq!(pkey.key_columns, vec![2, 0]);

        let index = db
            .catalog
            .create_index("users_name", "users", &["name"])
            .unwrap();
        assert!(!index.is_primary_key);

        let db = db.restart();
        assert_same_index(&db.catalog.get_index("users_pkey").unwrap(), &pkey);
        assert_same_index(&db.catalog.get_index("users_name").unwrap(), &index);
    }

    #[test]
    fn create_table_rejects_bad_primary_keys() {
        let db = Db::new(8);
        assert!(matches!(
            db.catalog.create_table("t", users(), &["nope"]),
            Err(CatalogError::ColumnNotFound { column, .. }) if column == "nope"
        ));
        assert!(matches!(
            db.catalog.create_table("t", users(), &["id", "id"]),
            Err(CatalogError::DuplicateColumn(name)) if name == "id"
        ));
        assert!(matches!(
            db.catalog.create_table("t", users(), &["name"]),
            Err(CatalogError::NullablePrimaryKey(name)) if name == "name"
        ));
        let long = "x".repeat(MAX_NAME_LEN as usize);
        assert!(matches!(
            db.catalog.create_table(&long, users(), &["id"]),
            Err(CatalogError::InvalidName(name)) if name == format!("{long}_pkey")
        ));
        db.catalog.create_table("other", users(), &[]).unwrap();
        db.catalog.create_index("t_pkey", "other", &["id"]).unwrap();
        assert!(matches!(
            db.catalog.create_table("t", users(), &["id"]),
            Err(CatalogError::IndexExists(name)) if name == "t_pkey"
        ));
        // Nothing was created by the failed attempts.
        assert!(db.catalog.get_table("t").is_none());
        assert!(db.catalog.get_table(&long).is_none());
    }

    #[test]
    fn lists_indexes_of_a_table() {
        let db = Db::new(8);
        db.catalog.create_table("users", users(), &[]).unwrap();
        db.catalog.create_table("orders", users(), &[]).unwrap();
        db.catalog
            .create_index("users_id", "users", &["id"])
            .unwrap();
        db.catalog
            .create_index("orders_id", "orders", &["id"])
            .unwrap();
        db.catalog
            .create_index("users_name", "users", &["name"])
            .unwrap();

        assert_eq!(
            names(&db.catalog.table_indexes("users")),
            ["users_id", "users_name"]
        );
        assert_eq!(names(&db.catalog.table_indexes("orders")), ["orders_id"]);
        db.catalog.create_table("empty", users(), &[]).unwrap();
        assert!(db.catalog.table_indexes("empty").is_empty());
        assert!(db.catalog.table_indexes("missing").is_empty());
    }

    /// Only secondary indexes on a table with a primary key store the
    /// primary key, before and after a restart.
    #[test]
    fn indexes_point_by_primary_key_on_tables_with_one() {
        let db = Db::new(16);
        let users_info = db.catalog.create_table("users", users(), &["id"]).unwrap();
        let logs = db.catalog.create_table("logs", users(), &[]).unwrap();
        db.catalog
            .create_index("users_name", "users", &["name"])
            .unwrap();
        db.catalog
            .create_index("logs_name", "logs", &["name"])
            .unwrap();

        let check = |db: &Db| {
            let locator = |name| {
                let index = db.catalog.get_index(name).unwrap();
                assert_eq!(index.index.metadata().locator(), index.locator);
                index.locator
            };
            assert_eq!(locator("users_pkey"), LocatorKind::Rid);
            assert_eq!(locator("users_name"), LocatorKind::PrimaryKey);
            assert_eq!(locator("logs_name"), LocatorKind::Rid);
            let pkey = db.catalog.primary_key_index(users_info.oid).unwrap();
            assert_eq!(pkey.name, "users_pkey");
            assert!(db.catalog.primary_key_index(logs.oid).is_none());
        };
        check(&db);
        check(&db.restart());
    }

    #[test]
    fn survives_restart() {
        let db = Db::new(8);
        let users_info = db.catalog.create_table("users", users(), &[]).unwrap();
        let empty = db
            .catalog
            .create_table("empty", Schema::default(), &[])
            .unwrap();
        let users_id = db
            .catalog
            .create_index("users_id", "users", &["id"])
            .unwrap();
        let users_name = db
            .catalog
            .create_index("users_name_active", "users", &["name", "active"])
            .unwrap();
        let row = Tuple::encode(
            &users(),
            &[1.into(), "ada".into(), Value::BigInt(10), true.into()],
        )
        .unwrap();
        users_info.heap.insert(&TupleMeta::new(1), &row).unwrap();

        let db = db.restart();
        let users_after = db.catalog.get_table("users").unwrap();
        assert_eq!(users_after.oid, users_info.oid);
        assert_eq!(users_after.schema, users());
        assert_eq!(users_after.first_page_id(), users_info.first_page_id());
        let rows: Vec<_> = users_after.heap.iter().unwrap().collect();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].as_ref().unwrap().1.data(), row.data());

        let empty_after = db.catalog.get_table("empty").unwrap();
        assert_eq!(empty_after.oid, empty.oid);
        assert!(empty_after.schema.is_empty());

        assert_same_index(&db.catalog.get_index("users_id").unwrap(), &users_id);
        assert_same_index(
            &db.catalog.get_index("users_name_active").unwrap(),
            &users_name,
        );
        assert_eq!(
            names(&db.catalog.table_indexes("users")),
            ["users_id", "users_name_active"]
        );

        // New objects get fresh oids and pages after the restart.
        let orders = db.catalog.create_table("orders", users(), &[]).unwrap();
        assert!(orders.oid > empty.oid);
        assert!(orders.first_page_id() > users_name.header_page_id);
        let orders_id = db
            .catalog
            .create_index("orders_id", "orders", &["id"])
            .unwrap();
        assert!(orders_id.oid > users_name.oid);

        let db = db.restart();
        assert_eq!(db.catalog.get_table("orders").unwrap().oid, orders.oid);
        assert_same_index(&db.catalog.get_index("orders_id").unwrap(), &orders_id);
    }

    #[test]
    fn many_tables_spill_system_tables_onto_more_pages() {
        let db = Db::new(4);
        for i in 0..200 {
            let name = format!("table_{i}");
            db.catalog.create_table(&name, users(), &[]).unwrap();
            db.catalog
                .create_index(&format!("{name}_id"), &name, &["id"])
                .unwrap();
        }
        let db = db.restart();
        for i in 0..200 {
            let name = format!("table_{i}");
            let table = db.catalog.get_table(&name).unwrap();
            assert_eq!(table.schema, users());
            assert_eq!(
                names(&db.catalog.table_indexes(&name)),
                [format!("{name}_id")]
            );
        }
    }

    #[test]
    fn rows_of_unfinished_creates_are_skipped() {
        let db = Db::new(8);
        // A create that wrote its columns but crashed before its table row.
        let orphan_oid = db.catalog.state.read().next_table_oid;
        db.catalog
            .system
            .insert_column(&ColumnRow {
                table_oid: orphan_oid,
                position: 0,
                column: Column::new("x", DataType::Int, false),
            })
            .unwrap();

        let db = db.restart();
        assert!(db.catalog.get_table_by_oid(orphan_oid).is_none());
        let table = db.catalog.create_table("users", users(), &[]).unwrap();
        assert!(table.oid > orphan_oid);

        let db = db.restart();
        assert_eq!(db.catalog.get_table("users").unwrap().schema, users());
    }

    #[test]
    fn rejects_database_whose_page_0_is_not_a_catalog() {
        let dir = TempDir::new().unwrap();
        let (dm, bpm) = open_files(&dir, 4);
        let page_id = bpm.new_page();
        let mut guard = bpm.write_page(page_id, AccessType::Unknown);
        storage::page::page_header::PageHeader::init(guard.get_data_mut(), PageType::Table);
        drop(guard);
        assert!(matches!(Catalog::open(bpm), Err(CatalogError::Corrupt(_))));
        drop(dm);
    }
}
