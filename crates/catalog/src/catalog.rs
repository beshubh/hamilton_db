use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

use common::{Column, DataType, IndexKind, PageId, Schema, UniqueKey, ValueError};
use parking_lot::{RwLock, RwLockReadGuard};
use storage::StorageError;
use storage::buffer::arc_replacer::AccessType;
use storage::buffer::buffer_pool_manager::BufferPoolManager;
use storage::concurrency::index_versions::IndexedTable;
use storage::concurrency::transaction::{Outcome, Transaction};
use storage::concurrency::transaction_manager::TransactionManager;
use storage::index::btree_index::BtreeIndex;
use storage::index::{Index, IndexMetadata, LocatorKind};
use storage::table::table_heap::TableHeap;
use storage::wal::Logged;

use crate::error::{CatalogError, Result};
use crate::header_page::{CATALOG_PAGE_ID, SystemTableRoots};
use crate::locks::{LockMode, TableLocks};
use crate::sequence::{self, Sequence};
use crate::system_tables::{ColumnRow, IndexColumnRow, IndexRow, SystemTables, TableRow};

/// Identifies a table or sequence. Not reused while the catalog is open.
pub type TableOid = u32;

/// Identifies an index. Not reused while the catalog is open.
pub type IndexOid = u32;

/// Longest table, column or index name, in bytes.
pub const MAX_NAME_LEN: u32 = 128;

/// Holds off index creation on a table; see [`TableInfo::lock_writes`].
pub type TableWriteGuard<'a> = RwLockReadGuard<'a, ()>;

/// The index entries [`Catalog::sweep`] holds at a time when opening the
/// database: a few MB at most, with the longest keys, and enough that each
/// batch's descent of the tree costs little.
pub const SWEEP_BATCH: usize = 1024;

/// What [`Catalog::sweep`] collected.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Swept {
    pub tuples: usize,
    pub entries: usize,
}

/// What a relation is, as Postgres's `relkind`. Tables and sequences share
/// one namespace.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RelationKind {
    Table,
    /// One row, which only `nextval` changes; see `sequence`.
    Sequence,
}

/// A table or sequence: its name, its schema, and the heap that stores its
/// rows.
pub struct TableInfo {
    pub oid: TableOid,
    pub name: String,
    pub schema: Schema,
    /// The one heap for this table. Every user of the table must go through
    /// it, since inserts rely on its lock on the last page.
    pub heap: Arc<TableHeap>,
    pub kind: RelationKind,
    /// The identity columns, in column order, with their sequences.
    identity: Vec<(usize, Arc<Sequence>)>,
    /// See `lock_writes`.
    writes: RwLock<()>,
    /// See `Catalog::lock_table`.
    locks: TableLocks,
}

impl TableInfo {
    fn new(
        oid: TableOid,
        name: String,
        schema: Schema,
        heap: Arc<TableHeap>,
        kind: RelationKind,
        identity: Vec<(usize, Arc<Sequence>)>,
    ) -> Self {
        Self {
            oid,
            name,
            schema,
            heap,
            kind,
            identity,
            writes: RwLock::new(()),
            locks: TableLocks::default(),
        }
    }

    /// This table on `heap` instead, sharing its locks.
    fn on_heap(&self, heap: Arc<TableHeap>) -> Self {
        Self {
            oid: self.oid,
            name: self.name.clone(),
            schema: self.schema.clone(),
            heap,
            kind: self.kind,
            identity: self.identity.clone(),
            writes: RwLock::new(()),
            locks: self.locks.clone(),
        }
    }

    /// First page of the table's heap.
    pub fn first_page_id(&self) -> PageId {
        self.heap.first_page_id()
    }

    /// The sequences of the identity columns, in column order.
    pub fn sequences(&self) -> impl Iterator<Item = &Arc<Sequence>> {
        self.identity.iter().map(|(_, sequence)| sequence)
    }

    /// The sequence of column `column`, if it is an identity column.
    pub fn identity(&self, column: usize) -> Option<&Arc<Sequence>> {
        self.identity
            .iter()
            .find(|(identity, _)| *identity == column)
            .map(|(_, sequence)| sequence)
    }

    /// Locks the table against index creation. A statement that writes the
    /// table takes this before listing its indexes and holds it until its
    /// last write. Creating an index waits for those statements, and makes
    /// later ones wait until the index is filled and in the catalog
    /// (`Catalog::create_index_with`), so every write either is in the
    /// table when the index is filled or adds its own entries.
    pub fn lock_writes(&self) -> TableWriteGuard<'_> {
        self.writes.read()
    }
}

impl fmt::Debug for TableInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TableInfo")
            .field("oid", &self.oid)
            .field("name", &self.name)
            .field("schema", &self.schema)
            .field("first_page_id", &self.first_page_id())
            .field("kind", &self.kind)
            .field("identity", &self.identity)
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
    /// Whether rows may share a key, and whether this is the table's primary
    /// key.
    pub kind: IndexKind,
    /// Whether a PRIMARY KEY or UNIQUE constraint made the index, which then
    /// can't be dropped on its own.
    pub constraint: bool,
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
            .field("kind", &self.kind)
            .field("constraint", &self.constraint)
            .field("locator", &self.locator)
            .finish()
    }
}

/// The tables and indexes of the database.
///
/// The catalog is stored in system tables (see `system_tables`) whose first
/// pages are recorded on page 0, and is loaded into memory when opened.
/// Lookups are served from memory; creates write through to the system
/// tables, as changes of the creating transaction (see
/// `create_table_with_constraints`). Like every other page, catalog pages
/// reach disk when the buffer pool flushes them.
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
    /// Indexes whose drop has not ended yet, still holding their names.
    dropping: HashMap<IndexOid, Arc<IndexInfo>>,
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

    /// Takes out table `oid`, with its sequences and indexes, for an aborted
    /// create or a committed drop.
    fn remove_table(&mut self, oid: TableOid) {
        let Some(table) = self.tables.remove(&oid) else {
            return;
        };
        self.table_names.remove(&table.name);
        for sequence in table.sequences() {
            self.tables.remove(&sequence.oid);
            self.table_names.remove(&sequence.name);
        }
        for index in self.table_indexes.remove(&oid).unwrap_or_default() {
            if let Some(index) = self.indexes.remove(&index) {
                self.index_names.remove(&index.name);
            }
        }
    }

    fn remove_index(&mut self, oid: IndexOid) {
        let Some(index) = self.indexes.remove(&oid) else {
            return;
        };
        self.index_names.remove(&index.name);
        if let Some(indexes) = self.table_indexes.get_mut(&index.table_oid) {
            indexes.retain(|&other| other != oid);
        }
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
        {
            let mtr = bpm.mtr(Logged::System);
            let mut page = mtr.write_page(CATALOG_PAGE_ID)?;
            system.roots().write_to(page.raw_mut());
        }
        Ok(Self {
            bpm,
            system,
            state: RwLock::new(CatalogState::default()),
        })
    }

    pub fn bpm(&self) -> &Arc<BufferPoolManager> {
        &self.bpm
    }

    /// Reads the system tables into memory.
    fn load(bpm: Arc<BufferPoolManager>, roots: SystemTableRoots) -> Result<Self> {
        let system = SystemTables::open(&bpm, roots)?;
        let mut state = CatalogState::default();

        let mut columns: HashMap<TableOid, Vec<ColumnRow>> = HashMap::new();
        for row in system.scan_columns()? {
            state.next_table_oid = state.next_table_oid.max(row.table_oid + 1);
            columns.entry(row.table_oid).or_default().push(row);
        }
        let mut tables = Vec::new();
        let mut sequences = HashMap::new();
        for row in system.scan_tables()? {
            state.next_table_oid = state.next_table_oid.max(row.oid + 1);
            match row.kind {
                RelationKind::Table => tables.push(row),
                RelationKind::Sequence => {
                    sequences.insert(row.oid, row);
                }
            }
        }
        for row in tables {
            let columns = columns.remove(&row.oid).unwrap_or_default();
            let columns = in_position_order(columns, |c| c.position, &row.name)?;
            let mut identity = Vec::new();
            for (position, column) in columns.iter().enumerate() {
                let Some(oid) = column.sequence else {
                    continue;
                };
                let max = sequence::max_value(column.column.data_type)
                    .filter(|_| !column.column.nullable)
                    .ok_or_else(|| CatalogError::InvalidIdentity(column.column.name.clone()))
                    .map_err(corrupt)?;
                let sequence = sequences.remove(&oid).ok_or_else(|| {
                    CatalogError::Corrupt(format!(
                        "column {} of {} has missing sequence {oid}",
                        column.column.name, row.name
                    ))
                })?;
                let heap = TableHeap::open(Arc::clone(&bpm), sequence.first_page_id)?;
                let (relation, sequence) = sequence_relation(&bpm, sequence, heap, max);
                state.add_table(relation).map_err(corrupt)?;
                identity.push((position, sequence));
            }
            let schema = Schema::new(columns.into_iter().map(|c| c.column).collect());
            let heap = TableHeap::open(Arc::clone(&bpm), row.first_page_id)?;
            let info = TableInfo::new(
                row.oid,
                row.name,
                schema,
                Arc::new(heap),
                RelationKind::Table,
                identity,
            );
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
            .filter(|row| row.kind.is_primary_key())
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
            let locator = locator_kind(row.kind, with_primary_key.contains(&table.oid));
            let metadata =
                index_metadata(&table.name, &row.name, row.kind, &key, &key_schema, locator);
            let index = open_index(&bpm, metadata, row.header_page_id)?;
            let info = IndexInfo {
                oid: row.oid,
                name: row.name,
                table_oid: table.oid,
                table_name: table.name.clone(),
                key_columns: key,
                key_schema,
                header_page_id: row.header_page_id,
                kind: row.kind,
                constraint: row.constraint,
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

    /// Creates a table with an empty heap in `txn`. Column names must be
    /// unique.
    ///
    /// A non-empty `primary_key` names the table's primary key columns, in
    /// key order, which must be NOT NULL. The table is then created with a
    /// primary key index on them, named `<name>_pkey` unless that is taken
    /// or too long (`choose_name`).
    pub fn create_table(
        self: &Arc<Self>,
        txn: &Transaction,
        name: &str,
        schema: Schema,
        primary_key: &[&str],
    ) -> Result<Arc<TableInfo>> {
        self.create_table_with_constraints(txn, name, schema, primary_key, &[], &[])
    }

    /// [`Catalog::create_table`], with `identity` naming the columns that are
    /// `GENERATED BY DEFAULT AS IDENTITY`, each a NOT NULL INT or BIGINT, and
    /// a unique index for each of `unique` that doesn't copy the primary key
    /// or an earlier constraint. An index takes its constraint's name, or
    /// else is named as Postgres names it (`choose_name`). Each identity
    /// column gets a sequence, named as Postgres names it:
    /// `<table>_<column>_seq`.
    ///
    /// The storage and the catalog rows are all `txn`'s changes, so an
    /// abort, or a crash before the commit, leaves nothing of the table.
    /// It is in the catalog once this returns, locked by `txn` until that
    /// ends (`LockMode::AccessExclusive`), so no other transaction puts
    /// rows in storage an abort would free. If this fails, `txn` must
    /// abort.
    pub fn create_table_with_constraints(
        self: &Arc<Self>,
        txn: &Transaction,
        name: &str,
        schema: Schema,
        primary_key: &[&str],
        identity: &[&str],
        unique: &[UniqueKey],
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
        let mut identity = if identity.is_empty() {
            Vec::new()
        } else {
            resolve_key(name, &schema, identity)?
        };
        identity.sort_unstable();
        if let Some(column) = identity
            .iter()
            .map(|&i| &schema.columns()[i])
            .find(|c| c.nullable || sequence::max_value(c.data_type).is_none())
        {
            return Err(CatalogError::InvalidIdentity(column.name.clone()));
        }
        // As Postgres does, the primary key comes first, and a constraint
        // copying an earlier one's key and NULL handling is dropped, giving
        // its name to the earlier one if that has none. Unlike Postgres, the
        // primary key's NOT NULL columns match either NULL handling.
        let mut planned: Vec<PlannedIndex> = Vec::new();
        if !primary_key.is_empty() {
            let key = resolve_key(name, &schema, primary_key)?;
            if let Some(column) = key
                .iter()
                .map(|&i| &schema.columns()[i])
                .find(|c| c.nullable)
            {
                return Err(CatalogError::NullablePrimaryKey(column.name.clone()));
            }
            planned.push(PlannedIndex {
                name: None,
                key,
                kind: IndexKind::PrimaryKey,
                columns: None,
            });
        }
        let nulls_distinct = |kind| {
            !matches!(
                kind,
                IndexKind::Unique {
                    nulls_distinct: false
                }
            )
        };
        for constraint in unique {
            let columns: Vec<&str> = constraint.columns.iter().map(String::as_str).collect();
            let key = resolve_key(name, &schema, &columns)?;
            if let Some(index_name) = &constraint.name {
                check_name(index_name)?;
            }
            let kind = constraint.kind();
            let copied = planned.iter_mut().find(|prior| {
                prior.key == key
                    && (prior.kind.is_primary_key()
                        || nulls_distinct(prior.kind) == nulls_distinct(kind))
            });
            match copied {
                Some(prior) => {
                    if prior.name.is_none() {
                        prior.name = constraint.name.clone();
                    }
                }
                None => planned.push(PlannedIndex {
                    name: constraint.name.clone(),
                    key,
                    kind,
                    columns: Some(&constraint.columns),
                }),
            }
        }

        let mut state = self.state.write();
        if state.table_names.contains_key(name) {
            return Err(CatalogError::TableExists(name.to_owned()));
        }
        let mut index_names: Vec<String> = Vec::with_capacity(planned.len());
        for index in &planned {
            let taken = |candidate: &str| {
                state.index_names.contains_key(candidate)
                    || index_names.iter().any(|chosen| chosen == candidate)
            };
            let index_name = match &index.name {
                Some(index_name) if taken(index_name) => {
                    return Err(CatalogError::IndexExists(index_name.clone()));
                }
                Some(index_name) => index_name.clone(),
                None => {
                    let columns = index.columns.map(|columns| columns.join("_"));
                    let label = if index.kind.is_primary_key() {
                        "pkey"
                    } else {
                        "key"
                    };
                    choose_name(name, columns.as_deref(), label, taken)
                }
            };
            index_names.push(index_name);
        }
        let mut sequence_names: Vec<String> = Vec::with_capacity(identity.len());
        for &column in &identity {
            let taken = |candidate: &str| {
                candidate == name
                    || state.table_names.contains_key(candidate)
                    || sequence_names.iter().any(|chosen| chosen == candidate)
            };
            let column = &schema.columns()[column].name;
            let sequence_name = choose_name(name, Some(column), "seq", taken);
            sequence_names.push(sequence_name);
        }
        let oid = state.next_table_oid;
        state.next_table_oid += 1;

        let mut sequences = Vec::with_capacity(identity.len());
        for (&column, sequence_name) in identity.iter().zip(sequence_names) {
            let (heap, _) = txn.create_table(&[])?;
            sequence::init(&heap)?;
            let row = TableRow {
                oid: state.next_table_oid,
                name: sequence_name,
                first_page_id: heap.first_page_id(),
                kind: RelationKind::Sequence,
            };
            state.next_table_oid += 1;
            let max = sequence::max_value(schema.columns()[column].data_type)
                .expect("identity columns were checked");
            sequences.push((column, row, heap, max));
        }
        let has_primary_key = !primary_key.is_empty();
        let planned: Vec<IndexPlan> = planned
            .into_iter()
            .zip(index_names)
            .map(|(index, index_name)| {
                IndexPlan::new(
                    name,
                    &schema,
                    index_name,
                    index.key,
                    index.kind,
                    has_primary_key,
                )
                .made_by_constraint()
            })
            .collect();
        let metadata: Vec<&IndexMetadata> = planned.iter().map(|plan| &plan.metadata).collect();
        let (heap, trees) = txn.create_table(&metadata)?;
        let indexes: Vec<Arc<IndexInfo>> = planned
            .into_iter()
            .zip(trees)
            .map(|(plan, tree)| {
                let index_oid = state.next_index_oid;
                state.next_index_oid += 1;
                Arc::new(plan.into_info(index_oid, oid, name, tree))
            })
            .collect();

        txn.undoable(|by| {
            for (_, row, _, _) in &sequences {
                self.system.insert_table(by, row)?;
            }
            for (position, column) in schema.columns().iter().enumerate() {
                let sequence = sequences
                    .iter()
                    .find(|(identity, ..)| *identity == position)
                    .map(|(_, row, ..)| row.oid);
                self.system.insert_column(
                    by,
                    &ColumnRow {
                        table_oid: oid,
                        position,
                        column: column.clone(),
                        sequence,
                    },
                )?;
            }
            self.system.insert_table(
                by,
                &TableRow {
                    oid,
                    name: name.to_owned(),
                    first_page_id: heap.first_page_id(),
                    kind: RelationKind::Table,
                },
            )?;
            indexes
                .iter()
                .try_for_each(|index| self.insert_index_rows(by, index))
        })?;

        // Into memory last, so a create that fails changes nothing there.
        let mut relations = Vec::with_capacity(sequences.len());
        let mut identity = Vec::with_capacity(sequences.len());
        for (column, row, heap, max) in sequences {
            let (relation, sequence) = sequence_relation(&self.bpm, row, heap, max);
            relations.push(relation);
            identity.push((column, sequence));
        }
        let info = Arc::new(TableInfo::new(
            oid,
            name.to_owned(),
            schema,
            Arc::new(heap),
            RelationKind::Table,
            identity,
        ));
        info.locks.lock(txn, name, LockMode::AccessExclusive)?;
        let catalog = Arc::clone(self);
        txn.on_end(move |outcome| {
            if outcome == Outcome::Aborted {
                catalog.state.write().remove_table(oid);
            }
        })?;
        for relation in relations {
            state.add_table(relation)?;
        }
        state.add_table(Arc::clone(&info))?;
        for index in indexes {
            state.add_index(index)?;
        }
        Ok(info)
    }

    pub fn get_table(&self, name: &str) -> Option<Arc<TableInfo>> {
        self.state.read().table(name).cloned()
    }

    pub fn get_table_by_oid(&self, oid: TableOid) -> Option<Arc<TableInfo>> {
        self.state.read().tables.get(&oid).cloned()
    }

    /// Every table, in name order, without the sequences.
    pub fn tables(&self) -> Vec<Arc<TableInfo>> {
        let mut tables: Vec<_> = self
            .state
            .read()
            .tables
            .values()
            .filter(|table| table.kind == RelationKind::Table)
            .cloned()
            .collect();
        tables.sort_by(|a, b| a.name.cmp(&b.name));
        tables
    }

    /// Creates an empty index on `key_columns` of `table_name` in `txn`;
    /// see `create_index_with`. For a table with no rows yet: CREATE INDEX
    /// fills the index from the table's rows.
    pub fn create_index(
        self: &Arc<Self>,
        txn: &Transaction,
        name: &str,
        table_name: &str,
        key_columns: &[&str],
    ) -> Result<Arc<IndexInfo>> {
        self.create_index_with(
            txn,
            name,
            table_name,
            key_columns,
            IndexKind::Plain,
            |_, _| Ok(()),
        )
    }

    /// Creates an index of `kind` on `key_columns` of `table_name` in
    /// `txn`, calls `fill` with the table and the index's B+ tree to add the
    /// entries of the rows already there, and records the index. Index
    /// names are unique across the database. `kind` must not be a primary
    /// key: a table gets that only when it is created.
    ///
    /// Writes to the table wait while `fill` runs (`TableInfo::lock_writes`),
    /// and no statement is planned on the index until it is recorded. The
    /// tree and the catalog rows are `txn`'s changes, so if `fill` fails,
    /// or `txn` aborts or a crash comes before its commit, nothing of the
    /// index is left: undo frees the tree with every page `fill` gave it.
    /// If this fails, `txn` must abort.
    pub fn create_index_with<E: From<CatalogError>>(
        self: &Arc<Self>,
        txn: &Transaction,
        name: &str,
        table_name: &str,
        key_columns: &[&str],
        kind: IndexKind,
        fill: impl FnOnce(&TableInfo, &Arc<Index>) -> Result<(), E>,
    ) -> Result<Arc<IndexInfo>, E> {
        assert!(!kind.is_primary_key(), "a primary key comes with its table");
        check_name(name)?;
        let (table, plan, index) = {
            let state = self.state.write();
            check_index_name_free(&state, name)?;
            let table = state
                .table(table_name)
                .cloned()
                .ok_or_else(|| CatalogError::TableNotFound(table_name.to_owned()))?;
            if table.kind != RelationKind::Table {
                return Err(CatalogError::NotATable(table_name.to_owned()).into());
            }
            let key = resolve_key(table_name, &table.schema, key_columns)?;
            let has_primary_key = primary_key_index(&state, table.oid).is_some();
            let plan = IndexPlan::new(
                &table.name,
                &table.schema,
                name.to_owned(),
                key,
                kind,
                has_primary_key,
            );
            let index = txn
                .create_index(&plan.metadata)
                .map_err(CatalogError::from)?;
            (table, plan, index)
        };
        let index = Arc::new(index);
        // Taken without holding `state`: a writer holding the table's lock
        // may still look something up in the catalog.
        let _writes = table.writes.write();
        fill(&table, &index)?;
        let mut state = self.state.write();
        // Another create may have taken the name meanwhile.
        check_index_name_free(&state, name)?;
        let oid = state.next_index_oid;
        state.next_index_oid += 1;
        let info = Arc::new(plan.into_info(oid, table.oid, &table.name, index));
        txn.undoable(|by| self.insert_index_rows(by, &info))?;
        let catalog = Arc::clone(self);
        txn.on_end(move |outcome| {
            if outcome == Outcome::Aborted {
                catalog.state.write().remove_index(oid);
            }
        })
        .map_err(CatalogError::from)?;
        state.add_index(Arc::clone(&info))?;
        Ok(info)
    }

    /// Writes the rows of `index`, its key columns' first, as `by`'s
    /// changes.
    fn insert_index_rows(&self, by: Logged<'_>, index: &IndexInfo) -> Result<()> {
        for (position, &column) in index.key_columns.iter().enumerate() {
            let row = IndexColumnRow {
                index_oid: index.oid,
                position,
                column,
            };
            self.system.insert_index_column(by, &row)?;
        }
        self.system.insert_index(
            by,
            &IndexRow {
                oid: index.oid,
                name: index.name.clone(),
                table_oid: index.table_oid,
                header_page_id: index.header_page_id,
                kind: index.kind,
                constraint: index.constraint,
            },
        )
    }

    /// Locks table `oid` in `mode` until `txn` ends, failing at once if
    /// another transaction holds a conflicting lock (`LockConflict`), or if a
    /// newer one truncated the table or, for `AccessExclusive`, used it at
    /// all (`ChangedSince`): MVTO orders it before them. A table that doesn't
    /// exist is left for the statement to report.
    pub fn lock_table(&self, txn: &Transaction, oid: TableOid, mode: LockMode) -> Result<()> {
        match self.get_table_by_oid(oid) {
            Some(table) => table.locks.lock(txn, &table.name, mode),
            None => Ok(()),
        }
    }

    /// Empties table `oid` for `txn`, which holds it in `AccessExclusive`,
    /// by giving it new storage, and with `restart_identity` restarts its
    /// sequences. All of it is `txn`'s changes, so an abort or a crash before
    /// its commit takes it back. If this fails, `txn` must abort.
    pub fn truncate(
        self: &Arc<Self>,
        txn: &Transaction,
        oid: TableOid,
        restart_identity: bool,
    ) -> Result<()> {
        let mut state = self.state.write();
        let old = state
            .tables
            .get(&oid)
            .cloned()
            .ok_or_else(|| CatalogError::TableNotFound(oid.to_string()))?;
        let old_indexes = indexes_of(&state, oid);
        let trees: Vec<_> = old_indexes.iter().map(|i| Arc::clone(&i.index)).collect();
        let (heap, trees) = txn.truncate(&old.heap, &trees)?;
        let new = Arc::new(old.on_heap(Arc::new(heap)));
        let new_indexes: Vec<_> = old_indexes
            .iter()
            .zip(trees)
            .map(|(index, tree)| {
                let Index::BtreeIndex(btree) = &tree;
                Arc::new(IndexInfo {
                    oid: index.oid,
                    name: index.name.clone(),
                    table_oid: oid,
                    table_name: index.table_name.clone(),
                    key_columns: index.key_columns.clone(),
                    key_schema: index.key_schema.clone(),
                    header_page_id: btree.tree().header_page_id(),
                    kind: index.kind,
                    constraint: index.constraint,
                    locator: index.locator,
                    index: Arc::new(tree),
                })
            })
            .collect();
        // The table lock keeps every other writer off these rows until `txn`
        // ends, so its undo puts back exactly what it overwrote.
        let restarted = txn.undoable(|by| {
            self.system
                .set_first_page_id(by, oid, new.first_page_id())?;
            for index in &new_indexes {
                self.system
                    .set_header_page_id(by, index.oid, index.header_page_id)?;
            }
            let restart = new.sequences().filter(|_| restart_identity);
            restart
                .map(|sequence| Ok((Arc::clone(sequence), sequence.restart(by)?)))
                .collect::<Result<Vec<_>>>()
        })?;

        let tid = txn.tid();
        let catalog = Arc::clone(self);
        txn.on_end(move |outcome| {
            if outcome == Outcome::Committed {
                return old.locks.truncated(tid);
            }
            // Undo has put the sequences' rows back.
            for (sequence, saved) in restarted {
                sequence.restore(saved);
            }
            let mut state = catalog.state.write();
            state.tables.insert(oid, old);
            for index in old_indexes {
                state.indexes.insert(index.oid, index);
            }
        })?;
        state.tables.insert(oid, new);
        for index in new_indexes {
            state.indexes.insert(index.oid, index);
        }
        Ok(())
    }

    /// Drops table `oid`, with its indexes and sequences, for `txn`, which
    /// holds it in `AccessExclusive`. The commit takes it out of memory and
    /// frees its storage once no reader needs it; an abort, or a crash
    /// before the commit, keeps it whole. If this fails, `txn` must abort.
    pub fn drop_table(self: &Arc<Self>, txn: &Transaction, oid: TableOid) -> Result<()> {
        let state = self.state.write();
        let table = state
            .tables
            .get(&oid)
            .cloned()
            .ok_or_else(|| CatalogError::TableNotFound(oid.to_string()))?;
        if table.kind != RelationKind::Table {
            return Err(CatalogError::NotATable(table.name.clone()));
        }
        let indexes = indexes_of(&state, oid);
        let sequences: Vec<_> = table
            .sequences()
            .map(|sequence| Arc::clone(&state.tables[&sequence.oid]))
            .collect();
        txn.undoable(|by| {
            // The table stays in memory until the commit, so `txn` may have
            // dropped it already; dropping it twice would free it twice.
            if !self.system.delete_table(by, oid)? {
                return Err(CatalogError::TableNotFound(table.name.clone()));
            }
            self.system.delete_columns(by, oid)?;
            for sequence in &sequences {
                self.system.delete_table(by, sequence.oid)?;
            }
            for index in &indexes {
                self.system.delete_index(by, index.oid)?;
                self.system.delete_index_columns(by, index.oid)?;
            }
            Ok::<_, CatalogError>(())
        })?;
        let heaps: Vec<PageId> = std::iter::once(&table)
            .chain(&sequences)
            .map(|relation| relation.first_page_id())
            .collect();
        let trees: Vec<PageId> = indexes.iter().map(|index| index.header_page_id).collect();
        txn.drop_storage(&heaps, &trees)?;

        let tid = txn.tid();
        let catalog = Arc::clone(self);
        txn.on_end(move |outcome| {
            if outcome == Outcome::Committed {
                table.locks.truncated(tid);
                catalog.state.write().remove_table(oid);
            }
        })?;
        Ok(())
    }

    /// `txn` must hold the index's table in `AccessExclusive`. An abort or a
    /// crash before the commit keeps the index whole, and its name stays
    /// taken until `txn` ends. An index a constraint made is refused. If this
    /// fails, `txn` must abort.
    pub fn drop_index(self: &Arc<Self>, txn: &Transaction, oid: IndexOid) -> Result<()> {
        let mut state = self.state.write();
        let index = state
            .indexes
            .get(&oid)
            .cloned()
            .ok_or_else(|| CatalogError::IndexNotFound(oid.to_string()))?;
        if index.constraint {
            return Err(CatalogError::IndexRequiredByConstraint {
                index: index.name.clone(),
                table: index.table_name.clone(),
            });
        }
        txn.drop_storage(&[], &[index.header_page_id])?;
        txn.undoable(|by| {
            self.system.delete_index(by, oid)?;
            self.system.delete_index_columns(by, oid)
        })?;

        let table_indexes = state.table_indexes.entry(index.table_oid).or_default();
        let position = table_indexes
            .iter()
            .position(|&other| other == oid)
            .expect("an index is among its table's");
        let catalog = Arc::clone(self);
        let dropped = Arc::clone(&index);
        txn.on_end(move |outcome| {
            let mut state = catalog.state.write();
            state.dropping.remove(&oid);
            match outcome {
                Outcome::Committed => {
                    state.index_names.remove(&dropped.name);
                }
                // Hooks run newest first, so later drops are back already.
                Outcome::Aborted => {
                    let indexes = state.table_indexes.entry(dropped.table_oid).or_default();
                    indexes.insert(position, oid);
                    state.indexes.insert(oid, dropped);
                }
            }
        })?;
        table_indexes.remove(position);
        state.indexes.remove(&oid);
        state.dropping.insert(oid, index);
        Ok(())
    }

    /// Collects, through `tm`, the garbage a restart left where no queue
    /// holds it: every deleted tuple no transaction can see, in the system
    /// tables and in every table, and every index entry that no version
    /// needs. Sequences are left alone: their row is rewritten in place and
    /// never deleted. Holds a page's tuples or `batch` entries at a time.
    /// For opening the database, before any transaction begins.
    pub fn sweep(&self, tm: &TransactionManager, batch: usize) -> Result<Swept> {
        debug_assert_eq!(tm.watermark(), tm.next_tid(), "a transaction is running");
        let mut swept = Swept::default();
        for heap in self.system.heaps() {
            swept.tuples += tm.sweep_heap(heap)?;
        }
        for table in self.tables() {
            swept.tuples += tm.sweep_heap(&table.heap)?;
            let primary_key = self.primary_key_index(table.oid);
            let indexed = Arc::new(IndexedTable::new(
                Arc::clone(&table.heap),
                table.schema.clone(),
                primary_key.map(|index| Arc::clone(&index.index)),
            ));
            for index in self.table_indexes(&table.name) {
                swept.entries += tm.sweep_index(&indexed, &index.index, batch)?;
            }
        }
        Ok(swept)
    }

    /// Writes each sequence's row as the last value it handed out, so the
    /// next open goes on from there instead of past the values logged ahead
    /// (see `sequence`). For a clean shutdown, before the storage closes.
    pub fn save_sequences(&self) -> Result<()> {
        let state = self.state.read();
        for table in state.tables.values() {
            table.sequences().try_for_each(|sequence| sequence.save())?;
        }
        Ok(())
    }

    pub fn get_index(&self, name: &str) -> Option<Arc<IndexInfo>> {
        let state = self.state.read();
        state.indexes.get(state.index_names.get(name)?).cloned()
    }

    /// Like `get_index`, but also finds an index another transaction is
    /// dropping, so that a second drop waits on its table's lock.
    pub fn get_index_to_drop(&self, name: &str) -> Option<Arc<IndexInfo>> {
        let state = self.state.read();
        let oid = state.index_names.get(name)?;
        state.indexes.get(oid).or(state.dropping.get(oid)).cloned()
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

fn indexes_of(state: &CatalogState, oid: TableOid) -> Vec<Arc<IndexInfo>> {
    state
        .table_indexes
        .get(&oid)
        .into_iter()
        .flatten()
        .map(|index| Arc::clone(&state.indexes[index]))
        .collect()
}

/// An index CREATE TABLE will create, before it has a name.
struct PlannedIndex<'a> {
    /// From `CONSTRAINT name`; chosen by `choose_name` otherwise.
    name: Option<String>,
    key: Vec<usize>,
    kind: IndexKind,
    /// The key's column names as written, for a unique constraint's name.
    columns: Option<&'a [String]>,
}

/// Postgres's `NAMEDATALEN - 1`, the longest name it generates.
const GENERATED_NAME_LEN: usize = 63;

/// A name for an index of `table` that `taken` says is free, as Postgres's
/// `ChooseRelationName` picks one: `table[_columns]_label`, then with 1, 2,
/// ... after `label`, each cut to fit as `generated_name` does.
fn choose_name(
    table: &str,
    columns: Option<&str>,
    label: &str,
    taken: impl Fn(&str) -> bool,
) -> String {
    let mut name = generated_name(table, columns, label);
    for n in 1.. {
        if !taken(&name) {
            break;
        }
        name = generated_name(table, columns, &format!("{label}{n}"));
    }
    name
}

/// `name1[_name2]_label`, as Postgres's `makeObjectName` builds it: the
/// longer of `name1` and `name2` loses a byte at a time until the whole fits
/// in `GENERATED_NAME_LEN` bytes, and each is then cut back to a character
/// boundary.
fn generated_name(name1: &str, name2: Option<&str>, label: &str) -> String {
    let overhead = label.len() + 1 + usize::from(name2.is_some());
    let available = GENERATED_NAME_LEN.saturating_sub(overhead);
    let (mut len1, mut len2) = (name1.len(), name2.map_or(0, str::len));
    while len1 + len2 > available {
        if len1 > len2 {
            len1 -= 1;
        } else {
            len2 -= 1;
        }
    }
    let mut name = name1[..name1.floor_char_boundary(len1)].to_owned();
    if let Some(name2) = name2 {
        name.push('_');
        name.push_str(&name2[..name2.floor_char_boundary(len2)]);
    }
    name.push('_');
    name.push_str(label);
    name
}

/// An index of a table, named and keyed, before its B+ tree is made.
struct IndexPlan {
    name: String,
    kind: IndexKind,
    constraint: bool,
    key: Vec<usize>,
    key_schema: Schema,
    locator: LocatorKind,
    metadata: IndexMetadata,
}

impl IndexPlan {
    /// An index named `name` of `kind` on the `key` columns of table
    /// `table`, whose schema is `schema`.
    fn new(
        table: &str,
        schema: &Schema,
        name: String,
        key: Vec<usize>,
        kind: IndexKind,
        table_has_primary_key: bool,
    ) -> Self {
        let key_schema = key_schema(schema, &key).expect("key columns were resolved");
        let locator = locator_kind(kind, table_has_primary_key);
        let metadata = index_metadata(table, &name, kind, &key, &key_schema, locator);
        Self {
            name,
            kind,
            constraint: false,
            key,
            key_schema,
            locator,
            metadata,
        }
    }

    /// This index, as made by a PRIMARY KEY or UNIQUE constraint.
    fn made_by_constraint(self) -> Self {
        Self {
            constraint: true,
            ..self
        }
    }

    /// The index, as `oid`, of table `table_oid` named `table_name`, on
    /// `index`, its tree.
    fn into_info(
        self,
        oid: IndexOid,
        table_oid: TableOid,
        table_name: &str,
        index: impl Into<Arc<Index>>,
    ) -> IndexInfo {
        let index = index.into();
        let Index::BtreeIndex(btree) = &*index;
        IndexInfo {
            oid,
            name: self.name,
            table_oid,
            table_name: table_name.to_owned(),
            key_columns: self.key,
            key_schema: self.key_schema,
            header_page_id: btree.tree().header_page_id(),
            kind: self.kind,
            constraint: self.constraint,
            locator: self.locator,
            index,
        }
    }
}

fn check_index_name_free(state: &CatalogState, name: &str) -> Result<()> {
    if state.index_names.contains_key(name) {
        return Err(CatalogError::IndexExists(name.to_owned()));
    }
    Ok(())
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
        .find(|index| index.kind.is_primary_key())
}

/// The primary key index stores rids. A secondary index stores the primary
/// key if the table has one, and rids otherwise.
fn locator_kind(kind: IndexKind, table_has_primary_key: bool) -> LocatorKind {
    if !kind.is_primary_key() && table_has_primary_key {
        LocatorKind::PrimaryKey
    } else {
        LocatorKind::Rid
    }
}

/// The metadata of an index named `name` on the `key` columns of `table`,
/// storing `locator`s.
fn index_metadata(
    table: &str,
    name: &str,
    kind: IndexKind,
    key: &[usize],
    key_schema: &Schema,
    locator: LocatorKind,
) -> IndexMetadata {
    IndexMetadata::new(
        name,
        table.to_owned(),
        key.to_vec(),
        Arc::new(key_schema.clone()),
        kind,
        locator,
    )
}

/// Opens the B+ tree whose header page is `header_page_id`.
fn open_index(
    bpm: &Arc<BufferPoolManager>,
    metadata: IndexMetadata,
    header_page_id: PageId,
) -> Result<Arc<Index>> {
    let index = BtreeIndex::new(metadata, header_page_id, Arc::clone(bpm))?;
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

/// The relation SELECT reads sequence `row` through, and the handle
/// `nextval` uses, both on `heap`. Its values stop at `max`.
fn sequence_relation(
    bpm: &Arc<BufferPoolManager>,
    row: TableRow,
    heap: TableHeap,
    max: i64,
) -> (Arc<TableInfo>, Arc<Sequence>) {
    let sequence = Sequence::new(
        row.oid,
        row.name.clone(),
        Arc::clone(bpm),
        heap.first_page_id(),
        max,
    );
    let relation = TableInfo::new(
        row.oid,
        row.name,
        sequence::SCHEMA.clone(),
        Arc::new(heap),
        RelationKind::Sequence,
        Vec::new(),
    );
    (Arc::new(relation), Arc::new(sequence))
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
    use storage::Storage;
    use storage::concurrency::transaction_manager::TransactionManager;
    use storage::index::IndexTrait;
    use storage::page::page_header::PageType;
    use storage::table::table_page::{TablePage, TupleMeta};
    use tempfile::TempDir;

    struct Db {
        dir: TempDir,
        storage: Storage,
        catalog: Arc<Catalog>,
        tm: TransactionManager,
    }

    impl Db {
        fn new(num_frames: usize) -> Self {
            Self::open(TempDir::new().unwrap(), num_frames)
        }

        fn open(dir: TempDir, num_frames: usize) -> Self {
            let storage = storage::open(dir.path().join("test.db"), num_frames).unwrap();
            let catalog = Arc::new(Catalog::open(Arc::clone(&storage.bpm)).unwrap());
            let tm = TransactionManager::new(Arc::clone(&storage.bpm));
            Self {
                dir,
                storage,
                catalog,
                tm,
            }
        }

        fn checkpoint(&self) {
            self.storage.bpm.checkpoint().unwrap();
        }

        /// Runs `ddl` in a transaction of its own, committed if it succeeds
        /// and aborted if it fails, as a DDL statement runs.
        fn ddl<T, E>(&self, ddl: impl FnOnce(&Transaction) -> Result<T, E>) -> Result<T, E> {
            let txn = self.tm.begin();
            let result = ddl(&txn);
            match result {
                Ok(_) => self.tm.commit(&txn).unwrap(),
                Err(_) => self.tm.abort(&txn).unwrap(),
            }
            result
        }

        fn create_table(
            &self,
            name: &str,
            schema: Schema,
            primary_key: &[&str],
        ) -> Result<Arc<TableInfo>> {
            self.ddl(|txn| self.catalog.create_table(txn, name, schema, primary_key))
        }

        fn create_table_with_constraints(
            &self,
            name: &str,
            schema: Schema,
            primary_key: &[&str],
            identity: &[&str],
            unique: &[UniqueKey],
        ) -> Result<Arc<TableInfo>> {
            self.ddl(|txn| {
                self.catalog.create_table_with_constraints(
                    txn,
                    name,
                    schema,
                    primary_key,
                    identity,
                    unique,
                )
            })
        }

        fn create_index(
            &self,
            name: &str,
            table_name: &str,
            key_columns: &[&str],
        ) -> Result<Arc<IndexInfo>> {
            self.ddl(|txn| {
                self.catalog
                    .create_index(txn, name, table_name, key_columns)
            })
        }

        fn create_index_with(
            &self,
            name: &str,
            table_name: &str,
            key_columns: &[&str],
            kind: IndexKind,
            fill: impl FnOnce(&TableInfo, &Arc<Index>) -> Result<()>,
        ) -> Result<Arc<IndexInfo>> {
            self.ddl(|txn| {
                self.catalog
                    .create_index_with(txn, name, table_name, key_columns, kind, fill)
            })
        }

        /// Drops everything and opens the files again, as after a crash:
        /// disk holds the pages something wrote, the log what was synced,
        /// and the `.meta` file the last checkpoint.
        fn crash(self) -> Self {
            let Db {
                dir,
                storage,
                catalog,
                tm,
            } = self;
            drop((catalog, tm, storage));
            Self::open(dir, 8)
        }

        /// The database as a crash now would leave it, from a copy of its
        /// files, while this one runs on.
        fn crash_copy(&self) -> Self {
            let copy = TempDir::new().unwrap();
            for file in std::fs::read_dir(self.dir.path()).unwrap() {
                let path = file.unwrap().path();
                std::fs::copy(&path, copy.path().join(path.file_name().unwrap())).unwrap();
            }
            Self::open(copy, 16)
        }

        /// Closes the files and opens them again.
        fn restart(self) -> Self {
            let Db {
                dir,
                storage,
                catalog,
                tm,
            } = self;
            drop(tm);
            catalog.save_sequences().unwrap();
            drop(catalog);
            storage.close().unwrap();
            Self::open(dir, 8)
        }
    }

    fn users() -> Schema {
        Schema::new(vec![
            Column::new("id", DataType::Int, false),
            Column::new("name", DataType::Varchar(64), true),
            Column::new("score", DataType::BigInt, false),
            Column::new("active", DataType::Bool, false),
        ])
    }

    /// What tells two `IndexInfo`s apart. Tests keep this across a restart,
    /// not the info: that holds the old files open.
    fn described(index: &IndexInfo) -> String {
        format!("{index:?}")
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
        let created = db.create_table("users", users(), &[]).unwrap();
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

        let other = db.create_table("orders", users(), &[]).unwrap();
        assert_ne!(other.oid, created.oid);
        assert_ne!(other.first_page_id(), created.first_page_id());

        let tables = db.catalog.tables();
        let names: Vec<_> = tables.iter().map(|t| t.name.as_str()).collect();
        assert_eq!(names, ["orders", "users"]);
    }

    #[test]
    fn create_table_rejects_bad_definitions() {
        let db = Db::new(8);
        db.create_table("users", users(), &[]).unwrap();
        assert!(matches!(
            db.create_table("users", users(), &[]),
            Err(CatalogError::TableExists(name)) if name == "users"
        ));

        let dup = Schema::new(vec![
            Column::new("a", DataType::Int, false),
            Column::new("a", DataType::Bool, false),
        ]);
        assert!(matches!(
            db.create_table("t", dup, &[]),
            Err(CatalogError::DuplicateColumn(name)) if name == "a"
        ));
        assert!(matches!(
            db.create_table("", users(), &[]),
            Err(CatalogError::InvalidName(_))
        ));
        let long = "x".repeat(MAX_NAME_LEN as usize + 1);
        assert!(matches!(
            db.create_table(&long, users(), &[]),
            Err(CatalogError::InvalidName(_))
        ));
        let long_column = Schema::new(vec![Column::new(&long, DataType::Int, false)]);
        assert!(matches!(
            db.create_table("t", long_column, &[]),
            Err(CatalogError::InvalidName(_))
        ));
        let array = Schema::new(vec![Column::new("xs", DataType::Array, false)]);
        assert!(matches!(
            db.create_table("t", array, &[]),
            Err(CatalogError::Value(ValueError::NotStorable { .. }))
        ));
        assert!(db.catalog.get_table("t").is_none());
    }

    #[test]
    fn create_and_get_index() {
        let db = Db::new(8);
        let table = db.create_table("users", users(), &[]).unwrap();
        let index = db
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
    fn writes_to_the_table_wait_while_an_index_is_filled() {
        let db = Db::new(8);
        let table = db.create_table("users", users(), &[]).unwrap();
        db.create_index_with(
            "users_name",
            "users",
            &["name"],
            IndexKind::Plain,
            |table, _| {
                assert!(table.writes.try_read().is_none());
                Ok(())
            },
        )
        .unwrap();
        assert!(table.writes.try_read().is_some());
    }

    /// An index whose fill fails is not recorded, in memory or on disk, and
    /// its name is free again.
    /// A unique build that conflicts fails before adding an entry, and is
    /// retried: each attempt must not keep a page or an oid.
    #[test]
    fn a_fill_that_fails_on_an_empty_tree_keeps_no_page_or_oid() {
        let db = Db::new(8);
        db.create_table("users", users(), &[]).unwrap();
        db.catalog.bpm.flush_all_pages().unwrap();
        let writes = db.storage.bpm.disk_manager().num_writes();
        for _ in 0..3 {
            let failed = db.create_index_with(
                "users_name",
                "users",
                &["name"],
                IndexKind::Plain,
                |_, _| Err(CatalogError::Corrupt("conflict".to_owned())),
            );
            assert!(failed.is_err());
        }
        // The header pages left the pool unwritten.
        db.catalog.bpm.flush_all_pages().unwrap();
        assert_eq!(db.storage.bpm.disk_manager().num_writes(), writes);
        let first = db.create_index("a", "users", &["id"]).unwrap();
        let second = db.create_index("b", "users", &["id"]).unwrap();
        assert_eq!(second.oid, first.oid + 1);
        assert_eq!(first.oid, 0);
    }

    #[test]
    fn an_index_that_fails_to_fill_is_not_created() {
        let db = Db::new(8);
        db.create_table("users", users(), &[]).unwrap();
        let failed = db.create_index_with(
            "users_name",
            "users",
            &["name"],
            IndexKind::Plain,
            |_, _| Err(CatalogError::Corrupt("fill failed".to_owned())),
        );
        assert!(matches!(failed, Err(CatalogError::Corrupt(_))));
        assert!(db.catalog.get_index("users_name").is_none());
        assert!(db.catalog.table_indexes("users").is_empty());

        let db = db.restart();
        assert!(db.catalog.table_indexes("users").is_empty());
        db.create_index("users_name", "users", &["name"]).unwrap();
        let db = db.restart();
        let names: Vec<_> = db
            .catalog
            .table_indexes("users")
            .iter()
            .map(|index| index.name.clone())
            .collect();
        assert_eq!(names, ["users_name"]);
    }

    #[test]
    fn create_index_rejects_bad_definitions() {
        let db = Db::new(8);
        db.create_table("users", users(), &[]).unwrap();
        db.create_index("users_id", "users", &["id"]).unwrap();
        assert!(matches!(
            db.create_index("users_id", "users", &["name"]),
            Err(CatalogError::IndexExists(name)) if name == "users_id"
        ));
        assert!(matches!(
            db.create_index("i", "missing", &["id"]),
            Err(CatalogError::TableNotFound(name)) if name == "missing"
        ));
        assert!(matches!(
            db.create_index("i", "users", &["nope"]),
            Err(CatalogError::ColumnNotFound { column, .. }) if column == "nope"
        ));
        assert!(matches!(
            db.create_index("i", "users", &["id", "id"]),
            Err(CatalogError::DuplicateColumn(name)) if name == "id"
        ));
        assert!(matches!(
            db.create_index("i", "users", &[]),
            Err(CatalogError::EmptyIndexKey)
        ));
        assert!(db.catalog.get_index("i").is_none());
    }

    #[test]
    fn create_table_with_primary_key() {
        let db = Db::new(8);
        let table = db.create_table("users", users(), &["score", "id"]).unwrap();
        let [pkey] = db.catalog.table_indexes("users").try_into().unwrap();
        assert_eq!(pkey.name, "users_pkey");
        assert_eq!(pkey.kind, IndexKind::PrimaryKey);
        assert_eq!(pkey.table_oid, table.oid);
        assert_eq!(pkey.key_columns, vec![2, 0]);

        let index = db.create_index("users_name", "users", &["name"]).unwrap();
        assert_eq!(index.kind, IndexKind::Plain);

        let (pkey_before, index_before) = (described(&pkey), described(&index));
        drop((table, pkey, index));
        let db = db.restart();
        let pkey = db.catalog.get_index("users_pkey").unwrap();
        assert_eq!(described(&pkey), pkey_before);
        let index = db.catalog.get_index("users_name").unwrap();
        assert_eq!(described(&index), index_before);
    }

    fn unique(columns: &[&str], nulls_distinct: bool) -> UniqueKey {
        UniqueKey {
            name: None,
            columns: columns.iter().map(|&c| c.to_owned()).collect(),
            nulls_distinct,
        }
    }

    #[test]
    fn create_table_with_unique_constraints() {
        let db = Db::new(8);
        // Taken already, so the first constraint's index gets a number.
        db.create_table("other", users(), &[]).unwrap();
        db.create_index("users_name_key", "other", &["name"])
            .unwrap();
        let named = UniqueKey {
            name: Some("users_by_score".into()),
            ..unique(&["score"], true)
        };
        let constraints = [
            unique(&["name"], true),
            unique(&["score", "id"], false),
            named,
        ];
        db.create_table_with_constraints("users", users(), &["id"], &[], &constraints)
            .unwrap();
        let indexes = db.catalog.table_indexes("users");
        assert_eq!(
            names(&indexes),
            [
                "users_pkey",
                "users_name_key1",
                "users_score_id_key",
                "users_by_score"
            ]
        );
        let unique_kind = |nulls_distinct| IndexKind::Unique { nulls_distinct };
        let kinds: Vec<_> = indexes.iter().map(|index| index.kind).collect();
        assert_eq!(
            kinds,
            [
                IndexKind::PrimaryKey,
                unique_kind(true),
                unique_kind(false),
                unique_kind(true)
            ]
        );
        assert_eq!(indexes[2].key_columns, [2, 0]);
        assert_eq!(indexes[1].locator, LocatorKind::PrimaryKey);

        let before: Vec<_> = indexes.iter().map(|index| described(index)).collect();
        drop(indexes);
        let db = db.restart();
        let after: Vec<_> = db
            .catalog
            .table_indexes("users")
            .iter()
            .map(|index| described(index))
            .collect();
        assert_eq!(after, before);
    }

    /// As in Postgres, a constraint with the key and NULL handling of an
    /// earlier one gets no index of its own, and gives its name to the
    /// earlier one if that has none. The primary key, whose columns are NOT
    /// NULL, matches either NULL handling.
    #[test]
    fn copied_unique_constraints_share_one_index() {
        let db = Db::new(8);
        let named = |name: &str, columns: &[&str]| UniqueKey {
            name: Some(name.into()),
            ..unique(columns, true)
        };
        let constraints = [
            named("users_id_uniq", &["id"]),
            unique(&["id"], false),
            unique(&["name"], true),
            named("users_name_uniq", &["name"]),
            named("ignored", &["name"]),
            unique(&["name"], false),
            unique(&["score", "name"], true),
            unique(&["name", "score"], true),
        ];
        db.create_table_with_constraints("users", users(), &["id"], &[], &constraints)
            .unwrap();
        let indexes = db.catalog.table_indexes("users");
        assert_eq!(
            names(&indexes),
            [
                "users_id_uniq",
                "users_name_uniq",
                "users_name_key",
                "users_score_name_key",
                "users_name_score_key"
            ]
        );
        assert_eq!(indexes[0].kind, IndexKind::PrimaryKey);
        assert!(db.catalog.get_index("ignored").is_none());
    }

    #[test]
    fn generated_names_are_cut_as_postgres_cuts_them() {
        assert_eq!(generated_name("t", Some("a_b"), "key"), "t_a_b_key");
        assert_eq!(generated_name("t", None, "pkey"), "t_pkey");
        let table = "t".repeat(40);
        let columns = "c".repeat(40);
        // The longer part gives way first, then both evenly.
        assert_eq!(
            generated_name(&"t".repeat(80), Some("c"), "key"),
            format!("{}_c_key", "t".repeat(57))
        );
        // On a tie the second part gives way.
        assert_eq!(
            generated_name(&table, Some(&columns), "key1"),
            format!("{}_{}_key1", "t".repeat(29), "c".repeat(28))
        );
        assert_eq!(
            generated_name(&"x".repeat(MAX_NAME_LEN as usize), None, "pkey"),
            format!("{}_pkey", "x".repeat(58))
        );
        // A character is never split.
        let wide = "é".repeat(40);
        let name = generated_name(&wide, None, "key");
        assert!(name.len() <= GENERATED_NAME_LEN, "{name}");
        assert_eq!(name, format!("{}_key", "é".repeat(29)));

        let taken = ["t_a_key", "t_a_key1"];
        assert_eq!(
            choose_name("t", Some("a"), "key", |name| taken.contains(&name)),
            "t_a_key2"
        );
    }

    #[test]
    fn long_table_and_column_names_get_index_names() {
        let db = Db::new(8);
        let table = "t".repeat(100);
        let column = "c".repeat(100);
        let schema = Schema::new(vec![
            Column::new("id", DataType::Int, false),
            Column::new(&column, DataType::Int, false),
        ]);
        db.create_table_with_constraints(&table, schema, &["id"], &[], &[unique(&[&column], true)])
            .unwrap();
        let indexes = db.catalog.table_indexes(&table);
        assert_eq!(
            names(&indexes),
            [
                format!("{}_pkey", "t".repeat(58)).as_str(),
                format!("{}_{}_key", "t".repeat(29), "c".repeat(29)).as_str()
            ]
        );
    }

    #[test]
    fn create_table_rejects_bad_unique_constraints() {
        let db = Db::new(8);
        let create =
            |constraint| db.create_table_with_constraints("t", users(), &[], &[], &[constraint]);
        assert!(matches!(
            create(unique(&["nope"], true)),
            Err(CatalogError::ColumnNotFound { column, .. }) if column == "nope"
        ));
        assert!(matches!(
            create(unique(&["id", "id"], true)),
            Err(CatalogError::DuplicateColumn(name)) if name == "id"
        ));
        db.create_table("other", users(), &["id"]).unwrap();
        let named = UniqueKey {
            name: Some("other_pkey".into()),
            ..unique(&["id"], true)
        };
        assert!(matches!(
            create(named),
            Err(CatalogError::IndexExists(name)) if name == "other_pkey"
        ));
        assert!(db.catalog.get_table("t").is_none());
    }

    #[test]
    fn create_unique_index() {
        let db = Db::new(8);
        db.create_table("users", users(), &[]).unwrap();
        let kind = IndexKind::Unique {
            nulls_distinct: false,
        };
        let index = db
            .create_index_with("users_name", "users", &["name"], kind, |_, _| Ok(()))
            .unwrap();
        assert_eq!(index.kind, kind);
        assert_eq!(index.index.metadata().kind(), kind);
        drop(index);
        let db = db.restart();
        assert_eq!(db.catalog.get_index("users_name").unwrap().kind, kind);
    }

    #[test]
    fn identity_columns_get_sequences() {
        let db = Db::new(16);
        // Taken already, so the sequence of `id` gets a number.
        db.create_table("users_id_seq", users(), &[]).unwrap();
        let table = db
            .create_table_with_constraints("users", users(), &["id"], &["score", "id"], &[])
            .unwrap();
        let (id, score) = (table.identity(0).unwrap(), table.identity(2).unwrap());
        assert!(table.identity(1).is_none());
        assert_eq!(id.name, "users_id_seq1");
        assert_eq!(score.name, "users_score_seq");
        assert_eq!(id.next_value().unwrap(), 1);
        assert_eq!(id.next_value().unwrap(), 2);
        assert_eq!(score.next_value().unwrap(), 1);

        // Each is a relation of its own, which is not listed as a table and
        // takes no index.
        let relation = db.catalog.get_table("users_score_seq").unwrap();
        assert_eq!(relation.kind, RelationKind::Sequence);
        assert_eq!(relation.oid, score.oid);
        assert_eq!(relation.schema, *sequence::SCHEMA);
        let tables = db.catalog.tables();
        let names: Vec<_> = tables.iter().map(|t| t.name.as_str()).collect();
        assert_eq!(names, ["users", "users_id_seq"]);
        assert!(matches!(
            db.create_index("i", "users_score_seq", &["last_value"]),
            Err(CatalogError::NotATable(name)) if name == "users_score_seq"
        ));
        let plain = db.create_table("plain", users(), &[]).unwrap();
        assert!(plain.identity.is_empty());
    }

    #[test]
    fn sequences_pick_up_where_they_left_off_after_a_restart() {
        let db = Db::new(8);
        let table = db
            .create_table_with_constraints("users", users(), &[], &["id"], &[])
            .unwrap();
        for expected in 1..=3 {
            assert_eq!(table.identity(0).unwrap().next_value().unwrap(), expected);
        }
        // As in Postgres, a value given explicitly doesn't move the sequence.
        let row = [
            Value::Int(9),
            Value::Null,
            Value::BigInt(0),
            Value::Bool(true),
        ];
        let tuple = Tuple::encode(&users(), &row).unwrap();
        table
            .heap
            .insert(Logged::System, &TupleMeta::new(0), &tuple)
            .unwrap();
        let sequence_oid = table.identity(0).unwrap().oid;
        drop(table);

        let db = db.restart();
        let table = db.catalog.get_table("users").unwrap();
        let sequence = table.identity(0).unwrap();
        assert_eq!(sequence.oid, sequence_oid);
        assert_eq!(sequence.name, "users_id_seq");
        assert!((1..4).all(|column| table.identity(column).is_none()));
        assert_eq!(sequence.next_value().unwrap(), 4);
        let relation = db.catalog.get_table("users_id_seq").unwrap();
        assert_eq!(relation.oid, sequence_oid);
        assert_eq!(relation.kind, RelationKind::Sequence);
        // The first value after the restart was logged ahead again.
        let (_, row) = relation.heap.iter().unwrap().next().unwrap().unwrap();
        assert_eq!(
            row.decode(&relation.schema).unwrap(),
            [Value::BigInt(36), Value::Int(0), Value::Bool(true)]
        );
    }

    /// A value reaches a committed row only after the record that logged
    /// it ahead, so a crash skips values rather than hand out one that a
    /// committed row holds.
    #[test]
    fn a_crash_never_hands_out_a_value_a_committed_row_holds() {
        let db = Db::new(8);
        let table = db
            .create_table_with_constraints("users", users(), &[], &["id"], &[])
            .unwrap();
        let next = |table: &TableInfo| table.identity(0).unwrap().next_value().unwrap();
        let insert = |db: &Db, table: &TableInfo, id: i64| {
            let row = [
                Value::Int(id as i32),
                Value::Null,
                Value::BigInt(0),
                Value::Bool(true),
            ];
            let txn = db.tm.begin();
            txn.insert(
                &table.heap,
                &table.schema,
                &Tuple::encode(&users(), &row).unwrap(),
            )
            .unwrap();
            db.tm.commit(&txn).unwrap();
        };
        for expected in 1..=5 {
            assert_eq!(next(&table), expected);
        }
        insert(&db, &table, 5);
        drop(table);
        let db = db.crash();
        let table = db.catalog.get_table("users").unwrap();
        // 1 was logged as if 1 through 33 had been handed out.
        assert_eq!(next(&table), 34);
        for expected in 35..=70 {
            assert_eq!(next(&table), expected);
        }
        insert(&db, &table, 70);
        drop(table);
        let db = db.crash();
        let table = db.catalog.get_table("users").unwrap();
        assert_eq!(next(&table), 100);
    }

    #[test]
    fn identity_columns_must_be_not_null_integers() {
        let db = Db::new(8);
        let schema = Schema::new(vec![
            Column::new("id", DataType::BigInt, false),
            Column::new("maybe", DataType::BigInt, true),
            Column::new("name", DataType::Varchar(8), false),
        ]);
        for column in ["maybe", "name"] {
            assert!(matches!(
                db.create_table_with_constraints("t", schema.clone(), &[], &[column], &[]),
                Err(CatalogError::InvalidIdentity(name)) if name == column
            ));
        }
        assert!(matches!(
            db.create_table_with_constraints("t", schema.clone(), &[], &["nope"], &[]),
            Err(CatalogError::ColumnNotFound { .. })
        ));
        assert!(matches!(
            db.create_table_with_constraints("t", schema, &[], &["id", "id"], &[]),
            Err(CatalogError::DuplicateColumn(_))
        ));
    }

    #[test]
    fn create_table_rejects_bad_primary_keys() {
        let db = Db::new(8);
        assert!(matches!(
            db.create_table("t", users(), &["nope"]),
            Err(CatalogError::ColumnNotFound { column, .. }) if column == "nope"
        ));
        assert!(matches!(
            db.create_table("t", users(), &["id", "id"]),
            Err(CatalogError::DuplicateColumn(name)) if name == "id"
        ));
        assert!(matches!(
            db.create_table("t", users(), &["name"]),
            Err(CatalogError::NullablePrimaryKey(name)) if name == "name"
        ));
        // Nothing was created by the failed attempts.
        assert!(db.catalog.get_table("t").is_none());
    }

    #[test]
    fn a_taken_primary_key_name_gets_a_number() {
        let db = Db::new(8);
        db.create_table("other", users(), &[]).unwrap();
        db.create_index("t_pkey", "other", &["id"]).unwrap();
        db.create_table("t", users(), &["id"]).unwrap();
        assert_eq!(names(&db.catalog.table_indexes("t")), ["t_pkey1"]);
    }

    #[test]
    fn lists_indexes_of_a_table() {
        let db = Db::new(8);
        db.create_table("users", users(), &[]).unwrap();
        db.create_table("orders", users(), &[]).unwrap();
        db.create_index("users_id", "users", &["id"]).unwrap();
        db.create_index("orders_id", "orders", &["id"]).unwrap();
        db.create_index("users_name", "users", &["name"]).unwrap();

        assert_eq!(
            names(&db.catalog.table_indexes("users")),
            ["users_id", "users_name"]
        );
        assert_eq!(names(&db.catalog.table_indexes("orders")), ["orders_id"]);
        db.create_table("empty", users(), &[]).unwrap();
        assert!(db.catalog.table_indexes("empty").is_empty());
        assert!(db.catalog.table_indexes("missing").is_empty());
    }

    /// Only secondary indexes on a table with a primary key store the
    /// primary key, before and after a restart.
    #[test]
    fn indexes_point_by_primary_key_on_tables_with_one() {
        let db = Db::new(16);
        let users_oid = db.create_table("users", users(), &["id"]).unwrap().oid;
        let logs_oid = db.create_table("logs", users(), &[]).unwrap().oid;
        db.create_index("users_name", "users", &["name"]).unwrap();
        db.create_index("logs_name", "logs", &["name"]).unwrap();

        let check = |db: &Db| {
            let locator = |name| {
                let index = db.catalog.get_index(name).unwrap();
                assert_eq!(index.index.metadata().locator(), index.locator);
                index.locator
            };
            assert_eq!(locator("users_pkey"), LocatorKind::Rid);
            assert_eq!(locator("users_name"), LocatorKind::PrimaryKey);
            assert_eq!(locator("logs_name"), LocatorKind::Rid);
            let pkey = db.catalog.primary_key_index(users_oid).unwrap();
            assert_eq!(pkey.name, "users_pkey");
            assert!(db.catalog.primary_key_index(logs_oid).is_none());
        };
        check(&db);
        check(&db.restart());
    }

    #[test]
    fn survives_restart() {
        let db = Db::new(8);
        let users_info = db.create_table("users", users(), &[]).unwrap();
        let empty = db.create_table("empty", Schema::default(), &[]).unwrap();
        let users_id = db.create_index("users_id", "users", &["id"]).unwrap();
        let users_name = db
            .create_index("users_name_active", "users", &["name", "active"])
            .unwrap();
        let row = Tuple::encode(
            &users(),
            &[1.into(), "ada".into(), Value::BigInt(10), true.into()],
        )
        .unwrap();
        users_info
            .heap
            .insert(Logged::System, &TupleMeta::new(1), &row)
            .unwrap();

        let (users_oid, users_first_page) = (users_info.oid, users_info.first_page_id());
        let empty_oid = empty.oid;
        let (users_id_before, users_name_before) = (described(&users_id), described(&users_name));
        let (users_name_oid, users_name_page) = (users_name.oid, users_name.header_page_id);
        drop((users_info, empty, users_id, users_name));
        let db = db.restart();
        let users_after = db.catalog.get_table("users").unwrap();
        assert_eq!(users_after.oid, users_oid);
        assert_eq!(users_after.schema, users());
        assert_eq!(users_after.first_page_id(), users_first_page);
        let rows: Vec<_> = users_after.heap.iter().unwrap().collect();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].as_ref().unwrap().1.data(), row.data());

        let empty_after = db.catalog.get_table("empty").unwrap();
        assert_eq!(empty_after.oid, empty_oid);
        assert!(empty_after.schema.is_empty());

        assert_eq!(
            described(&db.catalog.get_index("users_id").unwrap()),
            users_id_before
        );
        assert_eq!(
            described(&db.catalog.get_index("users_name_active").unwrap()),
            users_name_before
        );
        assert_eq!(
            names(&db.catalog.table_indexes("users")),
            ["users_id", "users_name_active"]
        );

        // New objects get fresh oids and pages after the restart.
        let orders = db.create_table("orders", users(), &[]).unwrap();
        assert!(orders.oid > empty_oid);
        assert!(orders.first_page_id() > users_name_page);
        let orders_id = db.create_index("orders_id", "orders", &["id"]).unwrap();
        assert!(orders_id.oid > users_name_oid);

        let (orders_oid, orders_id_before) = (orders.oid, described(&orders_id));
        drop((users_after, empty_after, orders, orders_id));
        let db = db.restart();
        assert_eq!(db.catalog.get_table("orders").unwrap().oid, orders_oid);
        assert_eq!(
            described(&db.catalog.get_index("orders_id").unwrap()),
            orders_id_before
        );
    }

    #[test]
    fn many_tables_spill_system_tables_onto_more_pages() {
        let db = Db::new(4);
        for i in 0..200 {
            let name = format!("table_{i}");
            db.create_table(&name, users(), &[]).unwrap();
            db.create_index(&format!("{name}_id"), &name, &["id"])
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

    /// Every page with a place in the db file, after writing every page.
    fn allocated(db: &Db) -> std::collections::BTreeSet<PageId> {
        db.checkpoint();
        let disk = db.storage.bpm.disk_manager();
        (0..disk.next_page_id())
            .filter(|&page_id| disk.has_page(page_id))
            .collect()
    }

    fn with_everything(db: &Db, txn: &Transaction, name: &str) -> Result<Arc<TableInfo>> {
        let unique = [unique(&["name"], true)];
        let schema = users();
        db.catalog
            .create_table_with_constraints(txn, name, schema, &["id"], &["score"], &unique)
    }

    /// An abort takes back a create whole: its rows, its storage, and its
    /// entries in memory, so its names are free again.
    #[test]
    fn a_create_that_aborts_leaves_nothing() {
        let db = Db::new(16);
        let before = allocated(&db);
        let txn = db.tm.begin();
        let table = with_everything(&db, &txn, "users").unwrap();
        assert!(db.catalog.get_table("users_score_seq").is_some());
        db.tm.abort(&txn).unwrap();
        drop((table, txn));
        assert!(db.catalog.get_table("users").is_none());
        assert!(db.catalog.get_table("users_score_seq").is_none());
        assert!(db.catalog.get_index("users_pkey").is_none());
        assert!(db.catalog.get_index("users_name_key").is_none());
        assert_eq!(allocated(&db), before, "pages the abort did not free");

        let txn = db.tm.begin();
        let table = with_everything(&db, &txn, "users").unwrap();
        db.tm.commit(&txn).unwrap();
        let oid = table.oid;
        drop((table, txn));
        let db = db.restart();
        let table = db.catalog.get_table("users").unwrap();
        assert_eq!(table.oid, oid);
        assert_eq!(table.identity(2).unwrap().next_value().unwrap(), 1);
        assert_eq!(
            names(&db.catalog.table_indexes("users")),
            ["users_pkey", "users_name_key"]
        );
    }

    /// A create whose commit is not in the log when a crash comes leaves
    /// nothing after recovery, though its records are.
    #[test]
    fn a_crash_before_a_creates_commit_leaves_nothing() {
        let db = Db::new(16);
        let before = allocated(&db);
        db.create_table("t", users(), &[]).unwrap();
        let txn = db.tm.begin();
        drop(with_everything(&db, &txn, "users").unwrap());
        db.catalog
            .create_index(&txn, "t_name", "t", &["name"])
            .unwrap();
        db.storage.bpm.log().flush_all().unwrap();
        let crashed = db.crash_copy();
        db.tm.abort(&txn).unwrap();
        let db = crashed;
        assert!(db.catalog.get_table("users").is_none());
        assert!(db.catalog.get_table("users_score_seq").is_none());
        assert!(db.catalog.table_indexes("t").is_empty());
        let t = db.catalog.get_table("t").unwrap();
        let mut expected = before;
        expected.extend(t.heap.page_ids().unwrap());
        drop(t);
        assert_eq!(allocated(&db), expected, "pages recovery did not free");
        db.create_table_with_constraints("users", users(), &["id"], &["score"], &[])
            .unwrap();
        db.create_index("t_name", "t", &["name"]).unwrap();
    }

    /// A build that fails after splitting its tree frees every page of it.
    #[test]
    fn a_fill_that_fails_frees_the_tree_it_grew() {
        let db = Db::new(16);
        db.create_table("users", users(), &[]).unwrap();
        let before = allocated(&db);
        let failed = db.create_index_with(
            "users_id",
            "users",
            &["id"],
            IndexKind::Plain,
            |_, index| {
                for id in 0..2000 {
                    let key = Tuple::encode(
                        &Schema::new(vec![users().columns()[0].clone()]),
                        &[Value::Int(id)],
                    )?;
                    let rid = common::Rid::new(0, id as u16);
                    index.insert_entry(
                        &key,
                        &storage::index::RowLocator::Rid(rid),
                        Logged::System,
                    )?;
                }
                let Index::BtreeIndex(btree) = &**index;
                assert!(
                    btree.tree().page_ids().unwrap().len() > 3,
                    "the tree never split"
                );
                Err(CatalogError::Corrupt("fill failed".to_owned()))
            },
        );
        assert!(failed.is_err());
        assert_eq!(allocated(&db), before, "pages of the failed build");
    }

    /// A rolled-back RESTART IDENTITY puts the sequence back, and the next
    /// value goes on past every row's, not from what the truncate's own
    /// inserts took.
    #[test]
    fn a_rolled_back_restart_goes_on_past_every_row() {
        let db = Db::new(16);
        let table = db
            .create_table_with_constraints("users", users(), &[], &["id"], &[])
            .unwrap();
        let sequence = Arc::clone(table.identity(0).unwrap());
        for expected in 1..=10 {
            assert_eq!(sequence.next_value().unwrap(), expected);
        }
        let txn = db.tm.begin();
        db.catalog
            .lock_table(&txn, table.oid, LockMode::AccessExclusive)
            .unwrap();
        db.catalog.truncate(&txn, table.oid, true).unwrap();
        assert_eq!(sequence.next_value().unwrap(), 1);
        assert_eq!(sequence.next_value().unwrap(), 2);
        db.tm.abort(&txn).unwrap();
        assert!(sequence.next_value().unwrap() > 10);
    }

    /// Drops table `oid` in a transaction of its own, as DROP TABLE does.
    fn drop_table(db: &Db, oid: TableOid) -> Result<()> {
        db.ddl(|txn| {
            db.catalog.lock_table(txn, oid, LockMode::AccessExclusive)?;
            db.catalog.drop_table(txn, oid)
        })
    }

    fn assert_no_users(db: &Db) {
        assert!(db.catalog.get_table("users").is_none());
        assert!(db.catalog.get_table("users_score_seq").is_none());
        assert!(db.catalog.get_index("users_pkey").is_none());
        assert!(db.catalog.get_index("users_name_key").is_none());
    }

    /// A committed drop takes out the table with its indexes and sequences,
    /// frees every page of them once garbage collection runs, and leaves
    /// their names free, after a restart too.
    #[test]
    fn a_committed_drop_leaves_nothing() {
        let db = Db::new(16);
        let before = allocated(&db);
        let table = db.ddl(|txn| with_everything(&db, txn, "users")).unwrap();
        let oid = table.oid;
        drop(table);
        drop_table(&db, oid).unwrap();
        assert_no_users(&db);
        db.tm.wait_for_gc();
        assert_eq!(allocated(&db), before, "pages the drop did not free");
        assert!(matches!(
            drop_table(&db, oid),
            Err(CatalogError::TableNotFound(_))
        ));

        let db = db.restart();
        assert_no_users(&db);
        let table = db.ddl(|txn| with_everything(&db, txn, "users")).unwrap();
        assert_eq!(table.identity(2).unwrap().next_value().unwrap(), 1);
        assert_eq!(
            names(&db.catalog.table_indexes("users")),
            ["users_pkey", "users_name_key"]
        );
        drop(table);
        let db = db.restart();
        assert_eq!(db.catalog.tables().len(), 1);
    }

    /// A rolled-back drop leaves the table as it was: its rows, indexes,
    /// sequence and pages, after a restart too.
    #[test]
    fn a_rolled_back_drop_keeps_the_table() {
        let db = Db::new(16);
        let table = db.ddl(|txn| with_everything(&db, txn, "users")).unwrap();
        let row = Tuple::encode(
            &users(),
            &[1.into(), "ada".into(), Value::BigInt(1), true.into()],
        )
        .unwrap();
        table
            .heap
            .insert(Logged::System, &TupleMeta::new(1), &row)
            .unwrap();
        let sequence = Arc::clone(table.identity(2).unwrap());
        assert_eq!(sequence.next_value().unwrap(), 1);
        let (oid, pages) = (table.oid, allocated(&db));
        let indexes: Vec<_> = db
            .catalog
            .table_indexes("users")
            .iter()
            .map(|i| described(i))
            .collect();
        drop(table);

        let txn = db.tm.begin();
        db.catalog
            .lock_table(&txn, oid, LockMode::AccessExclusive)
            .unwrap();
        db.catalog.drop_table(&txn, oid).unwrap();
        assert!(
            db.catalog.get_table("users").is_some(),
            "gone before the commit"
        );
        db.tm.abort(&txn).unwrap();
        db.tm.wait_for_gc();
        assert_eq!(allocated(&db), pages);
        assert_eq!(sequence.next_value().unwrap(), 2);
        drop(sequence);

        let db = db.restart();
        let table = db.catalog.get_table("users").unwrap();
        assert_eq!(table.oid, oid);
        let rows: Vec<_> = table.heap.iter().unwrap().collect();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].as_ref().unwrap().1.data(), row.data());
        let after: Vec<_> = db
            .catalog
            .table_indexes("users")
            .iter()
            .map(|i| described(i))
            .collect();
        assert_eq!(after, indexes);
        assert!(table.identity(2).unwrap().next_value().unwrap() > 1);
        assert!(db.catalog.get_table("users_score_seq").is_some());
    }

    #[test]
    fn a_table_is_dropped_once_per_transaction() {
        let db = Db::new(16);
        let before = allocated(&db);
        let oid = db
            .ddl(|txn| with_everything(&db, txn, "users"))
            .unwrap()
            .oid;
        db.ddl(|txn| {
            db.catalog.lock_table(txn, oid, LockMode::AccessExclusive)?;
            db.catalog.drop_table(txn, oid)?;
            assert!(matches!(
                db.catalog.drop_table(txn, oid),
                Err(CatalogError::TableNotFound(name)) if name == "users"
            ));
            Ok::<_, CatalogError>(())
        })
        .unwrap();
        assert_no_users(&db);
        db.tm.wait_for_gc();
        assert_eq!(allocated(&db), before, "pages the drop did not free");
        assert_no_users(&db.restart());
    }

    #[test]
    fn a_sequence_is_not_dropped_as_a_table() {
        let db = Db::new(16);
        drop(db.ddl(|txn| with_everything(&db, txn, "users")).unwrap());
        let sequence = db.catalog.get_table("users_score_seq").unwrap();
        assert!(matches!(
            drop_table(&db, sequence.oid),
            Err(CatalogError::NotATable(_))
        ));
    }

    fn drop_index(db: &Db, txn: &Transaction, name: &str) -> Result<()> {
        let index = db.catalog.get_index(name).unwrap();
        db.catalog
            .lock_table(txn, index.table_oid, LockMode::AccessExclusive)?;
        db.catalog.drop_index(txn, index.oid)
    }

    #[test]
    fn a_committed_drop_frees_the_index_and_then_its_name() {
        let db = Db::new(16);
        db.create_table("users", users(), &["id"]).unwrap();
        db.create_table("teams", users(), &[]).unwrap();
        let before = allocated(&db);
        db.create_index("users_name", "users", &["name"]).unwrap();
        db.create_index("users_score", "users", &["score"]).unwrap();

        let oid = db.catalog.get_index("users_name").unwrap().oid;
        let txn = db.tm.begin();
        drop_index(&db, &txn, "users_name").unwrap();
        assert!(db.catalog.get_index("users_name").is_none());
        assert_eq!(
            names(&db.catalog.table_indexes("users")),
            ["users_pkey", "users_score"]
        );
        assert!(matches!(
            db.catalog.drop_index(&txn, oid),
            Err(CatalogError::IndexNotFound(_))
        ));
        let other = db.tm.begin();
        assert!(matches!(
            db.catalog
                .create_index(&other, "users_name", "teams", &["name"]),
            Err(CatalogError::IndexExists(_))
        ));
        db.tm.abort(&other).unwrap();
        db.tm.commit(&txn).unwrap();
        db.tm.wait_for_gc();
        db.ddl(|txn| drop_index(&db, txn, "users_score")).unwrap();
        db.tm.wait_for_gc();
        assert_eq!(allocated(&db), before, "pages the drops did not free");

        db.create_index("users_name", "teams", &["name"]).unwrap();
        let db = db.restart();
        assert_eq!(names(&db.catalog.table_indexes("users")), ["users_pkey"]);
        assert_eq!(names(&db.catalog.table_indexes("teams")), ["users_name"]);
    }

    #[test]
    fn an_aborted_drop_keeps_the_index() {
        let db = Db::new(16);
        db.create_table("users", users(), &["id"]).unwrap();
        for name in ["users_name", "users_score", "users_active"] {
            let column = &name["users_".len()..];
            db.create_index(name, "users", &[column]).unwrap();
        }
        let indexes: Vec<String> = db
            .catalog
            .table_indexes("users")
            .iter()
            .map(|index| described(index))
            .collect();
        let pages = allocated(&db);

        let txn = db.tm.begin();
        drop_index(&db, &txn, "users_name").unwrap();
        drop_index(&db, &txn, "users_active").unwrap();
        assert_eq!(
            names(&db.catalog.table_indexes("users")),
            ["users_pkey", "users_score"]
        );
        db.tm.abort(&txn).unwrap();
        db.tm.wait_for_gc();
        let described_now = |db: &Db| -> Vec<String> {
            let indexes = db.catalog.table_indexes("users");
            indexes.iter().map(|index| described(index)).collect()
        };
        assert_eq!(described_now(&db), indexes);
        assert_eq!(allocated(&db), pages, "pages the abort freed");
        let db = db.restart();
        assert_eq!(described_now(&db), indexes);
    }

    #[test]
    fn a_constraints_index_cannot_be_dropped() {
        let db = Db::new(16);
        db.ddl(|txn| with_everything(&db, txn, "users")).unwrap();
        let kind = IndexKind::Unique {
            nulls_distinct: true,
        };
        db.create_index_with("users_score", "users", &["score"], kind, |_, _| Ok(()))
            .unwrap();
        let db = db.restart();
        for name in ["users_pkey", "users_name_key"] {
            let refused = db.ddl(|txn| drop_index(&db, txn, name));
            assert!(
                matches!(
                    &refused,
                    Err(CatalogError::IndexRequiredByConstraint { index, table })
                        if index == name && table == "users"
                ),
                "{name}: {refused:?}"
            );
        }
        db.ddl(|txn| drop_index(&db, txn, "users_score")).unwrap();
        assert_eq!(
            names(&db.catalog.table_indexes("users")),
            ["users_pkey", "users_name_key"]
        );
    }

    #[test]
    fn rejects_database_whose_page_0_is_not_a_catalog() {
        let dir = TempDir::new().unwrap();
        let bpm = storage::open(dir.path().join("test.db"), 4).unwrap().bpm;
        {
            let mtr = bpm.mtr(Logged::System);
            let mut page = mtr.new_page().unwrap();
            storage::page::page_header::PageHeader::init(page.raw_mut(), PageType::Table);
        }
        assert!(matches!(Catalog::open(bpm), Err(CatalogError::Corrupt(_))));
    }
}
