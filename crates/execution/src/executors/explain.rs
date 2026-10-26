use std::sync::Arc;

use common::{Rid, Schema, Tuple, Value};

use super::Executor;
use crate::Result;
use crate::execution_context::ExecutionContext;
use crate::plans::{ExplainPlan, PlanNode};

/// Produces the lines of the explained plan, one row each. The plan itself
/// is never run, so it has no executor.
pub struct ExplainExecutor {
    plan: ExplainPlan,
    lines: Vec<String>,
    /// The next line to produce.
    next_line: usize,
}

impl ExplainExecutor {
    pub fn new(_ctx: Arc<ExecutionContext>, plan: ExplainPlan) -> Self {
        Self {
            plan,
            lines: Vec::new(),
            next_line: 0,
        }
    }
}

impl Executor for ExplainExecutor {
    fn init(&mut self) -> Result<()> {
        self.lines = self.plan.lines();
        self.next_line = 0;
        Ok(())
    }

    fn next(
        &mut self,
        tuples: &mut Vec<Tuple>,
        rids: &mut Vec<Rid>,
        batch_size: usize,
    ) -> Result<bool> {
        tuples.clear();
        rids.clear();
        for line in self.lines.iter().skip(self.next_line).take(batch_size) {
            let values = [Value::Varchar(line.clone())];
            tuples.push(Tuple::encode(self.plan.get_output_schema(), &values)?);
            self.next_line += 1;
        }
        Ok(!tuples.is_empty())
    }

    fn get_output_schema(&self) -> &Schema {
        self.plan.get_output_schema()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common::Value;

    use super::super::test_util::{Db, column};
    use crate::plans::{
        ExplainPlan, LimitPlan, PlanNode, WindowColumn, WindowFunction, WindowFunctionKind,
        WindowPlan,
    };

    /// One row per line, in batches, without running the plan: a window has
    /// no executor yet.
    #[test]
    fn produces_the_lines_without_running_the_plan() {
        let db = Db::new();
        db.insert(&[(1, Some("a"))]);
        let scan = db.scan(None);
        let row_number = WindowFunction {
            function: WindowFunctionKind::RowNumber,
            partition_by: vec![],
            order_by: vec![],
        };
        let columns = vec![
            WindowColumn::Expr(column(0)),
            WindowColumn::Window(row_number),
        ];
        let schema = scan.get_output_schema().clone();
        let window = WindowPlan::new(schema, columns, scan);
        let limit = LimitPlan::new(3, Arc::new(window.into()));
        let explain = ExplainPlan::new(Arc::new(limit.into()));
        let lines: Vec<_> = db.run(explain.into()).unwrap().concat();
        assert_eq!(
            lines,
            [
                "Limit 3",
                "  ->  Window",
                "        Output: id, row_number() OVER ()",
                "        ->  SeqScan on users",
            ]
            .map(Value::from)
        );
    }
}
