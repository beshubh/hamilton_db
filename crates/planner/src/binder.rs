//! Resolves parsed statements against the catalog.
//!
//! Binding is where a statement is checked for meaning rather than syntax:
//! tables and columns must exist, unqualified columns must be unambiguous,
//! operands must have types their operators accept, and values must fit the
//! columns they are stored in. See [`crate::bound`] for what comes out.

mod expr;
mod scope;
#[cfg(test)]
mod tests;

use std::collections::HashSet;

use catalog::Catalog;
use common::{Schema, Value, ValueError};
use parser::{
    CreateTable, Delete, Expr, FromItem, Insert, JoinKind, OrderByItem, Select, SelectItem,
    Statement, TableRef, Update,
};

use self::expr::{UNNAMED_COLUMN, assign, bind_condition, bind_expr, null_for};
use self::scope::Scope;
use crate::bound::{
    BoundCreateTable, BoundDelete, BoundExpr, BoundExprKind, BoundFrom, BoundInsert, BoundOrderBy,
    BoundSelect, BoundSelectItem, BoundStatement, BoundTable, BoundUpdate,
};
use crate::error::{PlannerError, Result};

pub struct Binder<'a> {
    catalog: &'a Catalog,
}

impl<'a> Binder<'a> {
    pub fn new(catalog: &'a Catalog) -> Self {
        Self { catalog }
    }

    pub fn bind(&self, stmt: &Statement) -> Result<BoundStatement> {
        match stmt {
            Statement::CreateTable(create) => self.bind_create_table(create),
            Statement::Insert(insert) => self.bind_insert(insert),
            Statement::Select(select) => self.bind_select(select),
            Statement::Update(update) => self.bind_update(update),
            Statement::Delete(delete) => self.bind_delete(delete),
        }
    }

    /// The catalog checks the column names when the table is created.
    fn bind_create_table(&self, create: &CreateTable) -> Result<BoundStatement> {
        if self.catalog.get_table(&create.name).is_some() {
            return Err(PlannerError::TableExists(create.name.clone()));
        }
        Ok(BoundStatement::CreateTable(BoundCreateTable {
            name: create.name.clone(),
            schema: Schema::new(create.columns.clone()),
        }))
    }

    fn bind_insert(&self, insert: &Insert) -> Result<BoundStatement> {
        let table = self.bind_table(&TableRef {
            name: insert.table.clone(),
            alias: None,
        })?;
        let columns = table.schema.columns();

        // For each table column, its position in a VALUES row, if listed.
        let mut position = vec![None; columns.len()];
        let width = if insert.columns.is_empty() {
            for (i, slot) in position.iter_mut().enumerate() {
                *slot = Some(i);
            }
            columns.len()
        } else {
            for (i, name) in insert.columns.iter().enumerate() {
                let idx = table
                    .schema
                    .index_of(name)
                    .ok_or_else(|| PlannerError::ColumnNotFound(name.clone()))?;
                if position[idx].replace(i).is_some() {
                    return Err(PlannerError::DuplicateColumn(name.clone()));
                }
            }
            insert.columns.len()
        };

        // VALUES can't refer to any table.
        let scope = Scope::default();
        let rows = insert
            .rows
            .iter()
            .map(|row| {
                if row.len() != width {
                    return Err(ValueError::ColumnCountMismatch {
                        expected: width,
                        found: row.len(),
                    }
                    .into());
                }
                columns
                    .iter()
                    .zip(&position)
                    .map(|(column, pos)| match pos {
                        Some(pos) => assign(bind_expr(&row[*pos], &scope)?, column),
                        None => null_for(column),
                    })
                    .collect()
            })
            .collect::<Result<_>>()?;

        Ok(BoundStatement::Insert(BoundInsert { table, rows }))
    }

    fn bind_select(&self, select: &Select) -> Result<BoundStatement> {
        let mut scope = Scope::default();
        let from = select
            .from
            .as_ref()
            .map(|from| self.bind_from(from, &mut scope))
            .transpose()?;
        let selection = select
            .selection
            .as_ref()
            .map(|expr| bind_condition(expr, &scope, "WHERE"))
            .transpose()?;

        let mut projection = Vec::new();
        for item in &select.projection {
            bind_select_item(item, &scope, &mut projection)?;
        }

        let order_by = select
            .order_by
            .iter()
            .map(|item| bind_order_by(item, &scope, &projection))
            .collect::<Result<_>>()?;

        Ok(BoundStatement::Select(BoundSelect {
            from,
            selection,
            projection,
            order_by,
            limit: select.limit,
        }))
    }

    /// Binds a FROM item, adding its tables to `scope`.
    fn bind_from(&self, from: &FromItem, scope: &mut Scope) -> Result<BoundFrom> {
        match from {
            FromItem::Table(table) => {
                let table = self.bind_table(table)?;
                scope.push(&table)?;
                Ok(BoundFrom::Table(table))
            }
            FromItem::Join {
                kind,
                left,
                right,
                on,
            } => {
                let start = scope.table_count();
                let left = self.bind_from(left, scope)?;
                let right_start = scope.table_count();
                let right = self.bind_from(right, scope)?;
                // ON sees only this join's tables, not earlier FROM items.
                let on = on
                    .as_ref()
                    .map(|on| bind_condition(on, &scope.suffix(start), "ON"))
                    .transpose()?;
                if *kind == JoinKind::Left {
                    scope.make_nullable(right_start);
                }
                Ok(BoundFrom::Join {
                    kind: *kind,
                    left: Box::new(left),
                    right: Box::new(right),
                    on,
                })
            }
        }
    }

    fn bind_update(&self, update: &Update) -> Result<BoundStatement> {
        let table = self.bind_table(&update.table)?;
        let mut scope = Scope::default();
        scope.push(&table)?;

        let mut seen = HashSet::new();
        let assignments = update
            .assignments
            .iter()
            .map(|(name, value)| {
                let idx = table
                    .schema
                    .index_of(name)
                    .ok_or_else(|| PlannerError::ColumnNotFound(name.clone()))?;
                if !seen.insert(idx) {
                    return Err(PlannerError::DuplicateColumn(name.clone()));
                }
                let column = &table.schema.columns()[idx];
                Ok((idx, assign(bind_expr(value, &scope)?, column)?))
            })
            .collect::<Result<_>>()?;
        let selection = update
            .selection
            .as_ref()
            .map(|expr| bind_condition(expr, &scope, "WHERE"))
            .transpose()?;

        Ok(BoundStatement::Update(BoundUpdate {
            table,
            assignments,
            selection,
        }))
    }

    fn bind_delete(&self, delete: &Delete) -> Result<BoundStatement> {
        let table = self.bind_table(&delete.table)?;
        let mut scope = Scope::default();
        scope.push(&table)?;
        let selection = delete
            .selection
            .as_ref()
            .map(|expr| bind_condition(expr, &scope, "WHERE"))
            .transpose()?;
        Ok(BoundStatement::Delete(BoundDelete { table, selection }))
    }

    fn bind_table(&self, table: &TableRef) -> Result<BoundTable> {
        let info = self
            .catalog
            .get_table(&table.name)
            .ok_or_else(|| PlannerError::TableNotFound(table.name.clone()))?;
        Ok(BoundTable {
            oid: info.oid,
            name: info.name.clone(),
            alias: table.alias.clone().unwrap_or_else(|| info.name.clone()),
            schema: info.schema.clone(),
        })
    }
}

/// Binds one select item, expanding `*` and `t.*` into their columns.
fn bind_select_item(
    item: &SelectItem,
    scope: &Scope,
    out: &mut Vec<BoundSelectItem>,
) -> Result<()> {
    let columns = match item {
        SelectItem::Expr { expr, alias } => {
            let bound = bind_expr(expr, scope)?;
            let name = match alias {
                Some(alias) => alias.clone(),
                None => default_name(&bound),
            };
            out.push(BoundSelectItem { expr: bound, name });
            return Ok(());
        }
        SelectItem::Wildcard if scope.is_empty() => {
            return Err(PlannerError::WildcardWithoutFrom);
        }
        SelectItem::Wildcard => scope.all_columns(),
        SelectItem::QualifiedWildcard(alias) => scope.table_columns(alias)?,
    };
    out.extend(columns.into_iter().map(|resolved| BoundSelectItem {
        name: resolved.column.name.clone(),
        expr: BoundExpr {
            kind: BoundExprKind::Column {
                table: resolved.table,
                idx: resolved.idx,
            },
            return_type: resolved.column,
        },
    }));
    Ok(())
}

/// The name Postgres gives an unaliased select item.
fn default_name(expr: &BoundExpr) -> String {
    match &expr.kind {
        BoundExprKind::Column { .. } => expr.return_type.name.clone(),
        BoundExprKind::Function { function, .. } => function.name().to_owned(),
        _ => UNNAMED_COLUMN.to_owned(),
    }
}

/// Binds an ORDER BY item. As in Postgres, an integer literal is a position
/// in the select list and a bare name is first looked up among the output
/// column names; anything else is an expression on the FROM row.
fn bind_order_by(
    item: &OrderByItem,
    scope: &Scope,
    projection: &[BoundSelectItem],
) -> Result<BoundOrderBy> {
    let expr = match &item.expr {
        Expr::Literal(Value::Int(n)) => select_item(projection, i64::from(*n))?,
        Expr::Literal(Value::BigInt(n)) => select_item(projection, *n)?,
        Expr::Column { table: None, name } => {
            let mut matches = projection.iter().filter(|p| &p.name == name);
            match matches.next() {
                None => bind_expr(&item.expr, scope)?,
                Some(first) => {
                    if matches.any(|other| other.expr != first.expr) {
                        return Err(PlannerError::AmbiguousColumn(name.clone()));
                    }
                    first.expr.clone()
                }
            }
        }
        expr => bind_expr(expr, scope)?,
    };
    Ok(BoundOrderBy {
        expr,
        desc: item.desc,
    })
}

/// The expression of the 1-based `position`-th select item.
fn select_item(projection: &[BoundSelectItem], position: i64) -> Result<BoundExpr> {
    usize::try_from(position)
        .ok()
        .and_then(|p| p.checked_sub(1))
        .and_then(|i| projection.get(i))
        .map(|item| item.expr.clone())
        .ok_or(PlannerError::OrderByPositionOutOfRange { position })
}
