//! The tables an expression can name, and where their columns sit in the
//! row the expression is evaluated on.

use common::Column;

use crate::bound::BoundTable;
use crate::error::{PlannerError, Result};

#[derive(Clone, Debug, Default)]
pub(super) struct Scope {
    tables: Vec<ScopeTable>,
}

#[derive(Clone, Debug)]
struct ScopeTable {
    /// The name columns are qualified with.
    alias: String,
    columns: Vec<Column>,
    /// Position of the table's first column in the row.
    offset: usize,
}

/// A column a name resolved to.
pub(super) struct ResolvedColumn {
    pub table: String,
    pub idx: usize,
    pub column: Column,
}

impl Scope {
    /// Appends `table`'s columns to the row.
    pub fn push(&mut self, table: &BoundTable) -> Result<()> {
        if self.tables.iter().any(|t| t.alias == table.alias) {
            return Err(PlannerError::DuplicateTable(table.alias.clone()));
        }
        self.tables.push(ScopeTable {
            alias: table.alias.clone(),
            columns: table.schema.columns().to_vec(),
            offset: self.width(),
        });
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }

    /// How many tables are in scope.
    pub fn table_count(&self) -> usize {
        self.tables.len()
    }

    /// The number of columns in the row.
    fn width(&self) -> usize {
        self.tables.last().map_or(0, |t| t.offset + t.columns.len())
    }

    /// The tables from position `start` on, with offsets counted from the
    /// first of them: the scope of a join's ON when the earlier tables
    /// belong to other FROM items.
    pub fn suffix(&self, start: usize) -> Scope {
        let base = self.tables.get(start).map_or(0, |t| t.offset);
        Scope {
            tables: self.tables[start..]
                .iter()
                .map(|t| ScopeTable {
                    offset: t.offset - base,
                    ..t.clone()
                })
                .collect(),
        }
    }

    /// Makes the columns of the tables from position `start` on nullable,
    /// for the right side of a LEFT JOIN once its ON is bound.
    pub fn make_nullable(&mut self, start: usize) {
        for table in &mut self.tables[start..] {
            for column in &mut table.columns {
                column.nullable = true;
            }
        }
    }

    /// Resolves `name` or `table.name`.
    pub fn resolve(&self, table: Option<&str>, name: &str) -> Result<ResolvedColumn> {
        let candidates: Vec<&ScopeTable> = match table {
            Some(alias) => vec![self.table(alias)?],
            None => self.tables.iter().collect(),
        };
        let mut found = candidates.into_iter().filter_map(|t| {
            let pos = t.columns.iter().position(|c| c.name == name)?;
            Some(ResolvedColumn {
                table: t.alias.clone(),
                idx: t.offset + pos,
                column: t.columns[pos].clone(),
            })
        });
        let qualified = || match table {
            Some(alias) => format!("{alias}.{name}"),
            None => name.to_owned(),
        };
        let first = found
            .next()
            .ok_or_else(|| PlannerError::ColumnNotFound(qualified()))?;
        if found.next().is_some() {
            return Err(PlannerError::AmbiguousColumn(qualified()));
        }
        Ok(first)
    }

    /// Every column in the row, for `*`.
    pub fn all_columns(&self) -> Vec<ResolvedColumn> {
        self.tables.iter().flat_map(columns_of).collect()
    }

    /// The columns of the table called `alias`, for `alias.*`.
    pub fn table_columns(&self, alias: &str) -> Result<Vec<ResolvedColumn>> {
        Ok(columns_of(self.table(alias)?).collect())
    }

    fn table(&self, alias: &str) -> Result<&ScopeTable> {
        self.tables
            .iter()
            .find(|t| t.alias == alias)
            .ok_or_else(|| PlannerError::UnknownTable(alias.to_owned()))
    }
}

fn columns_of(table: &ScopeTable) -> impl Iterator<Item = ResolvedColumn> + '_ {
    table
        .columns
        .iter()
        .enumerate()
        .map(|(pos, column)| ResolvedColumn {
            table: table.alias.clone(),
            idx: table.offset + pos,
            column: column.clone(),
        })
}
