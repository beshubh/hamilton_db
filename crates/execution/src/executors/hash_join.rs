use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use common::{Rid, Schema, Tuple, Value};

use super::{AbstractExecutor, Executor};
use crate::Result;
use crate::execution_context::ExecutionContext;
use crate::plans::{HashJoinPlan, PlanNode};

/// A hash join: the right child is the build side and the left child the
/// probe side.
///
/// ```text
/// for each right row:
///     add it to the hash table under its right_keys
/// for each left row:
///     for each right row in the hash table under its left_keys:
///         if the pair passes the predicate, output it
///     LEFT join: if nothing was output, output the row padded with NULLs
/// ```
///
pub struct HashJoinExecutor {
    ctx: Arc<ExecutionContext>,
    plan: HashJoinPlan,
    /// The probe side.
    left: Box<AbstractExecutor>,
    /// The build side.
    right: Box<AbstractExecutor>,
    /// The right rows by the values of their `right_keys`.
    hash_table: HashMap<Vec<Value>, Vec<Tuple>>,
    /// The current batch of left rows.
    left_batch: Vec<Tuple>,
    left_rids: Vec<Rid>,
    /// The next row of `left_batch` to probe with.
    next_left: usize,
    /// Joined rows not returned by `next` yet.
    output: VecDeque<Tuple>,
}

impl HashJoinExecutor {
    pub fn new(
        ctx: Arc<ExecutionContext>,
        plan: HashJoinPlan,
        left: Box<AbstractExecutor>,
        right: Box<AbstractExecutor>,
    ) -> Self {
        Self {
            ctx,
            plan,
            left,
            right,
            hash_table: HashMap::new(),
            left_batch: Vec::new(),
            left_rids: Vec::new(),
            next_left: 0,
            output: VecDeque::new(),
        }
    }
}

impl Executor for HashJoinExecutor {
    fn init(&mut self) -> Result<()> {
        self.left.init()?;
        self.right.init()?;
        self.hash_table.clear();
        self.left_batch.clear();
        self.next_left = 0;
        self.output.clear();
        Ok(())
    }

    fn next(
        &mut self,
        _tuples: &mut Vec<Tuple>,
        _rids: &mut Vec<Rid>,
        _batch_size: usize,
    ) -> Result<bool> {
        todo!("hash join build and probe")
    }

    fn get_output_schema(&self) -> &Schema {
        self.plan.get_output_schema()
    }
}
