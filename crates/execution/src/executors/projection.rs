use std::sync::Arc;

use common::{Rid, Schema, Tuple, TupleProjector};

use super::{AbstractExecutor, Executor};
use crate::Result;
use crate::execution_context::ExecutionContext;
use crate::expressions::{AbstractExpr, Expr, evaluate_in_place};
use crate::plans::{PlanNode, ProjectionPlan};

/// Computes the plan's expressions on each of the child's rows. The rows it
/// produces are new, so they have no rids.
pub struct ProjectionExecutor {
    plan: ProjectionPlan,
    child: Box<AbstractExecutor>,
    child_tuples: Vec<Tuple>,
    child_rids: Vec<Rid>,
    /// Copies the columns over, when every expression is a column.
    projector: Option<TupleProjector>,
}

impl ProjectionExecutor {
    pub fn new(
        _ctx: Arc<ExecutionContext>,
        plan: ProjectionPlan,
        child: Box<AbstractExecutor>,
    ) -> Self {
        let columns: Option<Vec<usize>> = plan
            .exprs()
            .iter()
            .map(|expr| match expr {
                AbstractExpr::ColumnValueExpr(column) if column.tuple_idx() == 0 => {
                    Some(column.col_idx())
                }
                _ => None,
            })
            .collect();
        let projector = columns.and_then(|columns| {
            TupleProjector::new(
                plan.get_output_schema(),
                child.get_output_schema(),
                &columns,
            )
        });
        Self {
            plan,
            child,
            child_tuples: Vec::new(),
            child_rids: Vec::new(),
            projector,
        }
    }
}

impl Executor for ProjectionExecutor {
    fn init(&mut self) -> Result<()> {
        self.child.init()
    }

    fn next(
        &mut self,
        tuples: &mut Vec<Tuple>,
        rids: &mut Vec<Rid>,
        batch_size: usize,
    ) -> Result<bool> {
        rids.clear();
        let more = self
            .child
            .next(&mut self.child_tuples, &mut self.child_rids, batch_size)?;
        if !more {
            tuples.clear();
            return Ok(false);
        }
        let schema = self.child.get_output_schema();
        let out_schema = self.plan.get_output_schema();
        let exprs = self.plan.exprs();
        if exprs.len() != out_schema.len() {
            tuples.clear();
            for tuple in &self.child_tuples {
                let values = exprs
                    .iter()
                    .map(|expr| expr.evaluate(tuple, schema))
                    .collect::<Result<Vec<_>>>()?;
                tuples.push(Tuple::encode(out_schema, &values)?);
            }
            return Ok(true);
        }
        // Reuses the buffers of the tuples the caller handed back.
        tuples.resize_with(self.child_tuples.len(), Tuple::default);
        if let Some(projector) = &self.projector {
            for (tuple, out) in self.child_tuples.iter().zip(tuples.iter_mut()) {
                projector.project_into(out, tuple)?;
            }
            return Ok(true);
        }
        for (tuple, out) in self.child_tuples.iter().zip(tuples.iter_mut()) {
            let mut writer = out.writer(out_schema);
            for expr in exprs {
                match expr {
                    AbstractExpr::ColumnValueExpr(column) if column.tuple_idx() == 0 => {
                        writer.push_column(tuple, schema, column.col_idx())?;
                    }
                    expr => writer.push(evaluate_in_place(expr, tuple, schema)?.as_value_ref())?,
                }
            }
            writer.finish()?;
        }
        Ok(true)
    }

    fn get_output_schema(&self) -> &Schema {
        self.plan.get_output_schema()
    }
}

#[cfg(test)]
mod tests {
    use common::{Column, DataType, Schema, Value};

    use super::super::test_util::{Db, column, int};
    use crate::expressions::{ArithmeticExpr, ArithmeticOp, StringExpr, StringOp};
    use crate::plans::ProjectionPlan;

    #[test]
    fn computes_its_expressions_on_each_row() {
        let db = Db::new();
        db.insert(&[(1, Some("ann")), (2, None), (3, Some("bo"))]);
        let times_ten = ArithmeticExpr::new(ArithmeticOp::Mul, column(0), int(10)).unwrap();
        let upper = StringExpr::new(StringOp::Upper, column(1)).unwrap();
        let schema = Schema::new(vec![
            Column::new("name", DataType::Varchar(32), true),
            Column::new("id", DataType::Int, false),
        ]);
        let projection =
            ProjectionPlan::new(schema, vec![upper.into(), times_ten.into()], db.scan(None));
        assert_eq!(
            db.run(projection.into()).unwrap(),
            [
                vec![Value::from("ANN"), Value::Int(10)],
                vec![Value::Null, Value::Int(20)],
                vec![Value::from("BO"), Value::Int(30)],
            ]
        );
    }
}
