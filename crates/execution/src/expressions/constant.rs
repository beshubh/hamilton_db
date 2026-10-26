use common::{Column, Schema, Tuple, Value};

use super::{AbstractExpr, Expr};
use crate::Result;

/// A literal, the same for every tuple.
#[derive(Clone, Debug)]
pub struct ConstantExpr {
    value: Value,
    return_type: Column,
}

impl ConstantExpr {
    /// `return_type` is needed because NULL carries no type of its own.
    pub fn new(value: Value, return_type: Column) -> Self {
        Self { value, return_type }
    }

    pub fn value(&self) -> &Value {
        &self.value
    }
}

impl Expr for ConstantExpr {
    fn evaluate(&self, _tuple: &Tuple, _schema: &Schema) -> Result<Value> {
        Ok(self.value.clone())
    }

    fn evaluate_join(
        &self,
        _left: &Tuple,
        _left_schema: &Schema,
        _right: &Tuple,
        _right_schema: &Schema,
    ) -> Result<Value> {
        Ok(self.value.clone())
    }

    fn children(&self) -> &[AbstractExpr] {
        &[]
    }

    fn return_type(&self) -> &Column {
        &self.return_type
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::test_util::*;

    #[test]
    fn ignores_its_input() {
        assert_eq!(eval(&int(5), &ints(None, 0)).unwrap(), Value::Int(5));
        let got = eval_join(&int(5), &ints(None, 0), &[Value::Int(1)]).unwrap();
        assert_eq!(got, Value::Int(5));
    }
}
