//! Turns parsed statements into plans, and runs them.
//!
//! The [`Binder`] resolves a [`parser::Statement`] against the catalog into
//! a [`BoundStatement`]: names become table oids and column positions, and
//! every expression is type-checked. Planning works only on bound
//! statements. The [`ExecutionEngine`] is a session that takes statements
//! through both and runs their plans, in transactions.

pub mod binder;
pub mod bound;
pub mod engine;
pub mod error;
pub mod planner;

pub use binder::Binder;
pub use bound::{
    AggregateFunction, BoundAggregate, BoundCreateIndex, BoundCreateTable, BoundDelete, BoundExpr,
    BoundExprKind, BoundFrom, BoundInsert, BoundOrderBy, BoundSelect, BoundSelectItem,
    BoundStatement, BoundTable, BoundUpdate, BoundWindow, BoundWindowFunction, Function,
    TransactionCommand,
};
pub use engine::{EngineError, ExecutionEngine, Output, TransactionStatus};
pub use error::{PlannerError, Result};
pub use planner::Planner;
