//! Sessions: statements run one at a time, each in a transaction.
//!
//! An [`ExecutionEngine`] is one session on a database. It binds, plans and
//! executes each statement it is given, and keeps the session's
//! transaction state, which only BEGIN, COMMIT and ROLLBACK change. Those
//! are run here, not by executors: an executor runs inside a transaction,
//! so it can't begin or end one.
//!
//! The rules follow Postgres:
//!
//! - Outside a transaction block, a statement runs in a transaction of its
//!   own, committed if it succeeds and aborted if it fails.
//! - BEGIN opens a block, and the statements after it run in its
//!   transaction until COMMIT or ROLLBACK ends it. They see its writes.
//! - Any statement that fails in a block, from a syntax error to a storage
//!   `Conflict`, aborts the block's transaction at once. The block is then
//!   failed: every statement but COMMIT and ROLLBACK is rejected, and
//!   either one ends the block, reporting ROLLBACK.
//! - BEGIN in a block, and COMMIT or ROLLBACK outside one, are errors that
//!   change nothing. (Postgres only warns.)
//! - CREATE TABLE and CREATE INDEX are rejected in a block, failing it: the
//!   catalog isn't transactional. Outside a block they run as other
//!   statements do.
//! - Dropping the engine with a block open rolls it back.
//!
//! [`ExecutionEngine::session`] opens another session on the same database.
//! Sessions share one `TransactionManager`, so their transactions are
//! ordered against each other by Tid as MVTO requires.
//!
//! Every commit first records the next Tid in the catalog header, so that a
//! restarted session never starts below a committed `ts` on disk (see
//! `storage::concurrency::watermark`). There is no write-ahead log: what
//! reaches disk, and when, is up to whoever flushes the buffer pool.

use std::mem;
use std::sync::Arc;

use catalog::Catalog;
use common::{Column, Value};
use execution::ExecutionError;
use execution::execution_context::{ExecutionContext, ExecutionSettings};
use execution::executors::{Executor, create_executor};
use execution::plans::AbstractPlanNode;
use parser::{ParseError, Statement};
use storage::concurrency::transaction::{Transaction, TransactionState};
use storage::concurrency::transaction_manager::TransactionManager;
use thiserror::Error;

use crate::binder::Binder;
use crate::bound::{BoundStatement, TransactionCommand};
use crate::error::PlannerError;
use crate::planner::Planner;

/// Rows pulled from the root executor at a time, unless set otherwise.
pub const DEFAULT_BATCH_SIZE: usize = 128;

/// What a statement produced.
#[derive(Debug, PartialEq)]
pub enum Output {
    /// The rows of a SELECT or EXPLAIN.
    Rows {
        columns: Vec<Column>,
        rows: Vec<Vec<Value>>,
    },
    /// How many rows an INSERT, UPDATE or DELETE changed.
    Count { command: &'static str, count: i64 },
    /// A statement that reports only that it ran: its command tag.
    Done(&'static str),
}

/// Where the session is: outside a transaction block, in one, or in one
/// whose transaction a failed statement aborted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionStatus {
    Idle,
    InBlock,
    Failed,
}

#[derive(Debug, Error)]
pub enum EngineError {
    #[error(transparent)]
    Parse(#[from] ParseError),

    #[error(transparent)]
    Planner(#[from] PlannerError),

    #[error(transparent)]
    Execution(#[from] ExecutionError),

    #[error("current transaction is aborted, commands ignored until end of transaction block")]
    InFailedTransaction,

    /// BEGIN in a transaction block.
    #[error("there is already a transaction in progress")]
    AlreadyInTransaction,

    /// COMMIT or ROLLBACK outside a transaction block.
    #[error("there is no transaction in progress")]
    NoTransaction,

    /// A statement that runs only outside a transaction block, such as
    /// CREATE TABLE.
    #[error("{0} cannot run inside a transaction block")]
    NotInTransactionBlock(&'static str),
}

pub type Result<T, E = EngineError> = std::result::Result<T, E>;

/// One session on a database. See the module docs.
pub struct ExecutionEngine {
    catalog: Arc<Catalog>,
    txn_manager: Arc<TransactionManager>,
    settings: ExecutionSettings,
    batch_size: usize,
    state: State,
}

enum State {
    Idle,
    InBlock(Arc<Transaction>),
    /// The block's transaction has been aborted.
    Failed,
}

impl ExecutionEngine {
    /// A session on the database `catalog` describes. Its first transaction
    /// gets the Tid the catalog header recorded.
    pub fn new(catalog: Arc<Catalog>) -> Result<Self> {
        let next_tid = catalog.next_tid().map_err(ExecutionError::from)?;
        Ok(Self {
            catalog,
            txn_manager: Arc::new(TransactionManager::new(next_tid)),
            settings: ExecutionSettings::default(),
            batch_size: DEFAULT_BATCH_SIZE,
            state: State::Idle,
        })
    }

    /// Another session on the same database, sharing this one's
    /// transaction manager and settings. It starts outside a transaction
    /// block.
    pub fn session(&self) -> Self {
        Self {
            catalog: Arc::clone(&self.catalog),
            txn_manager: Arc::clone(&self.txn_manager),
            settings: self.settings,
            batch_size: self.batch_size,
            state: State::Idle,
        }
    }

    /// Gives the executors of later statements `settings`.
    pub fn with_settings(mut self, settings: ExecutionSettings) -> Self {
        self.settings = settings;
        self
    }

    /// Pulls `batch_size` rows at a time from the root executor instead of
    /// [`DEFAULT_BATCH_SIZE`].
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        assert!(batch_size > 0, "a batch holds at least one row");
        self.batch_size = batch_size;
        self
    }

    pub fn catalog(&self) -> &Arc<Catalog> {
        &self.catalog
    }

    pub fn transaction_manager(&self) -> &TransactionManager {
        &self.txn_manager
    }

    pub fn status(&self) -> TransactionStatus {
        match self.state {
            State::Idle => TransactionStatus::Idle,
            State::InBlock(_) => TransactionStatus::InBlock,
            State::Failed => TransactionStatus::Failed,
        }
    }

    /// Parses `sql` into statements. A syntax error fails an open block, as
    /// any failed statement does.
    pub fn parse(&mut self, sql: &str) -> Result<Vec<Statement>> {
        parser::parse(sql).map_err(|e| self.fail(e.into()))
    }

    /// Parses and runs the statements in `sql` in order, stopping at the
    /// first that fails.
    pub fn execute_sql(&mut self, sql: &str) -> Result<Vec<Output>> {
        let stmts = self.parse(sql)?;
        stmts.iter().map(|stmt| self.execute(stmt)).collect()
    }

    /// Runs one statement, in the block's transaction or in one of its own.
    pub fn execute(&mut self, stmt: &Statement) -> Result<Output> {
        let bound = Binder::new(&self.catalog).bind(stmt);
        if matches!(self.state, State::Failed)
            && !matches!(
                bound,
                Ok(BoundStatement::Transaction(
                    TransactionCommand::Commit | TransactionCommand::Rollback
                ))
            )
        {
            return Err(EngineError::InFailedTransaction);
        }
        let result = match bound {
            Ok(BoundStatement::Transaction(command)) => self.transaction_command(command),
            Ok(bound) => self.run(&bound),
            Err(e) => Err(e.into()),
        };
        result.map_err(|e| self.fail(e))
    }

    /// Rolls back the open block, if there is one, leaving the session idle.
    pub fn close(&mut self) -> Result<()> {
        if let State::InBlock(txn) = mem::replace(&mut self.state, State::Idle) {
            self.txn_manager.abort(&txn).map_err(ExecutionError::from)?;
        }
        Ok(())
    }

    fn transaction_command(&mut self, command: TransactionCommand) -> Result<Output> {
        match (command, mem::replace(&mut self.state, State::Idle)) {
            (TransactionCommand::Begin, State::Idle) => {
                self.state = State::InBlock(self.txn_manager.begin());
                Ok(Output::Done("BEGIN"))
            }
            (TransactionCommand::Begin, state) => {
                self.state = state;
                Err(EngineError::AlreadyInTransaction)
            }
            (_, State::Idle) => Err(EngineError::NoTransaction),
            (TransactionCommand::Commit, State::InBlock(txn)) => {
                self.commit(&txn)?;
                Ok(Output::Done("COMMIT"))
            }
            (TransactionCommand::Rollback, State::InBlock(txn)) => {
                self.txn_manager.abort(&txn).map_err(ExecutionError::from)?;
                Ok(Output::Done("ROLLBACK"))
            }
            (_, State::Failed) => Ok(Output::Done("ROLLBACK")),
        }
    }

    /// Plans and runs a statement other than a transaction command.
    fn run(&mut self, bound: &BoundStatement) -> Result<Output> {
        let ddl = match bound {
            BoundStatement::CreateTable(_) => Some("CREATE TABLE"),
            BoundStatement::CreateIndex(_) => Some("CREATE INDEX"),
            _ => None,
        };
        if let (Some(ddl), State::InBlock(_)) = (ddl, &self.state) {
            return Err(EngineError::NotInTransactionBlock(ddl));
        }
        let plan = Planner::new(&self.catalog).plan(bound)?;
        if let State::InBlock(txn) = &self.state {
            return self.run_plan(txn, &plan, bound);
        }
        let txn = self.txn_manager.begin();
        let result = self
            .run_plan(&txn, &plan, bound)
            .and_then(|output| self.commit(&txn).map(|()| output));
        if txn.state() == TransactionState::Running {
            // The statement failed. Its error is the one to report.
            let _ = self.txn_manager.abort(&txn);
        }
        result
    }

    /// Records the next Tid in the catalog header, then commits `txn`.
    fn commit(&self, txn: &Transaction) -> Result<()> {
        self.catalog
            .set_next_tid(self.txn_manager.next_tid())
            .map_err(ExecutionError::from)?;
        self.txn_manager.commit(txn).map_err(ExecutionError::from)?;
        Ok(())
    }

    /// Runs `plan`, the plan of `bound`, to the end in `txn`.
    fn run_plan(
        &self,
        txn: &Arc<Transaction>,
        plan: &AbstractPlanNode,
        bound: &BoundStatement,
    ) -> Result<Output> {
        let ctx = Arc::new(ExecutionContext::new(
            Arc::clone(&self.catalog),
            Arc::clone(txn),
            self.settings,
        ));
        let mut executor = create_executor(&ctx, plan)?;
        executor.init()?;
        let (mut tuples, mut rids) = (Vec::new(), Vec::new());
        let mut rows = Vec::new();
        while executor.next(&mut tuples, &mut rids, self.batch_size)? {
            for tuple in &tuples {
                rows.push(
                    tuple
                        .decode(executor.get_output_schema())
                        .map_err(ExecutionError::from)?,
                );
            }
        }
        let command = match bound {
            BoundStatement::CreateTable(_) => return Ok(Output::Done("CREATE TABLE")),
            BoundStatement::CreateIndex(_) => return Ok(Output::Done("CREATE INDEX")),
            BoundStatement::Select(_) | BoundStatement::Explain(_) => {
                let columns = executor.get_output_schema().columns().to_vec();
                return Ok(Output::Rows { columns, rows });
            }
            BoundStatement::Insert(_) => "INSERT",
            BoundStatement::Update(_) => "UPDATE",
            BoundStatement::Delete(_) => "DELETE",
            BoundStatement::Transaction(_) => unreachable!("transaction commands are not planned"),
        };
        match rows.as_slice() {
            [row] => match row.as_slice() {
                [Value::BigInt(count)] => Ok(Output::Count {
                    command,
                    count: *count,
                }),
                _ => unreachable!("{command} returns one BIGINT, not {row:?}"),
            },
            _ => unreachable!("{command} returns one row, not {}", rows.len()),
        }
    }

    /// Handles a statement's error: one in a block aborts its transaction
    /// and fails it, except BEGIN in a block and COMMIT or ROLLBACK outside
    /// one, which change nothing. Returns `err`.
    fn fail(&mut self, err: EngineError) -> EngineError {
        if matches!(
            err,
            EngineError::AlreadyInTransaction | EngineError::NoTransaction
        ) {
            return err;
        }
        self.state = match mem::replace(&mut self.state, State::Idle) {
            State::InBlock(txn) => {
                // The statement's error is the one to report.
                let _ = self.txn_manager.abort(&txn);
                State::Failed
            }
            state => state,
        };
        err
    }
}

impl Drop for ExecutionEngine {
    fn drop(&mut self) {
        let _ = self.close();
    }
}
