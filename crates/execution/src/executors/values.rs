use common::{Rid, Schema, Tuple};

use super::Executor;
use crate::Result;
use crate::expressions::Expr;
use crate::plans::{PlanNode, ValuesPlan};

/// Produces the plan's rows, evaluating their expressions, which read no
/// input.
pub struct ValuesExecutor {
    plan: ValuesPlan,
    /// The next row to produce.
    next_row: usize,
}

impl ValuesExecutor {
    pub fn new(plan: ValuesPlan) -> Self {
        Self { plan, next_row: 0 }
    }
}

impl Executor for ValuesExecutor {
    fn init(&mut self) -> Result<()> {
        self.next_row = 0;
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
        let (input, input_schema) = (Tuple::default(), Schema::default());
        for row in self.plan.rows().iter().skip(self.next_row).take(batch_size) {
            let values = row
                .iter()
                .map(|expr| expr.evaluate(&input, &input_schema))
                .collect::<Result<Vec<_>>>()?;
            tuples.push(Tuple::encode(self.plan.get_output_schema(), &values)?);
            self.next_row += 1;
        }
        Ok(!tuples.is_empty())
    }

    fn get_output_schema(&self) -> &Schema {
        self.plan.get_output_schema()
    }
}

#[cfg(test)]
mod tests {
    use common::{DataType, Schema, Value, ValueError};

    use super::super::test_util::{Db, constant, int, user, users_schema};
    use crate::ExecutionError;
    use crate::expressions::{AbstractExpr, ArithmeticExpr, ArithmeticOp};
    use crate::plans::ValuesPlan;

    fn name(name: &str) -> AbstractExpr {
        constant(Value::from(name), DataType::Varchar(32))
    }

    #[test]
    fn evaluates_each_row() {
        let db = Db::new();
        let sum = ArithmeticExpr::new(ArithmeticOp::Add, int(1), int(2)).unwrap();
        let rows = vec![
            vec![int(1), name("a")],
            vec![sum.into(), name("b")],
            vec![int(4), constant(Value::Null, DataType::Varchar(32))],
        ];
        let values = ValuesPlan::new(users_schema(), rows);
        assert_eq!(
            db.run(values.into()).unwrap(),
            [user(1, Some("a")), user(3, Some("b")), user(4, None)]
        );
    }

    /// The input of a SELECT without FROM.
    #[test]
    fn one_empty_row() {
        let db = Db::new();
        let values = ValuesPlan::new(Schema::default(), vec![vec![]]);
        assert_eq!(db.run(values.into()).unwrap(), [Vec::<Value>::new()]);
    }

    #[test]
    fn null_in_a_not_null_column_fails() {
        let db = Db::new();
        let null = constant(Value::Null, DataType::Int);
        let values = ValuesPlan::new(users_schema(), vec![vec![null, name("a")]]);
        assert!(matches!(
            db.run(values.into()),
            Err(ExecutionError::Value(ValueError::NullNotAllowed(column))) if column == "id"
        ));
    }
}
