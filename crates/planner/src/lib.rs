//! Turns parsed statements into plans.
//!
//! The [`Binder`] resolves a [`parser::Statement`] against the catalog into
//! a [`BoundStatement`]: names become table oids and column positions, and
//! every expression is type-checked. Planning works only on bound
//! statements.

pub mod binder;
pub mod bound;
pub mod error;
pub mod planner;

pub use binder::Binder;
pub use bound::{
    AggregateFunction, BoundAggregate, BoundCreateIndex, BoundCreateTable, BoundDelete, BoundExpr,
    BoundExprKind, BoundFrom, BoundInsert, BoundOrderBy, BoundSelect, BoundSelectItem,
    BoundStatement, BoundTable, BoundUpdate, BoundWindow, BoundWindowFunction, Function,
};
pub use error::{PlannerError, Result};
pub use planner::Planner;
