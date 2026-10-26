use common::{Column, DataType, Schema, Tuple, Value};

use super::{AbstractExpr, Expr, UNNAMED_COLUMN};
use crate::Result;

/// `ARRAY[e1, e2, ...]`: evaluates each element into one [`Value::Array`].
///
/// ARRAY has no element type yet, so the elements are not checked to share
/// one. The array itself is never NULL, though its elements may be.
#[derive(Clone, Debug)]
pub struct ArrayExpr {
    /// The elements, in order.
    children: Vec<AbstractExpr>,
    return_type: Column,
}

impl ArrayExpr {
    pub fn new(elements: Vec<AbstractExpr>) -> Self {
        Self {
            children: elements,
            return_type: Column::new(UNNAMED_COLUMN, DataType::Array, false),
        }
    }
}

impl Expr for ArrayExpr {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value> {
        let elements = self
            .children
            .iter()
            .map(|e| e.evaluate(tuple, schema))
            .collect::<Result<_>>()?;
        Ok(Value::Array(elements))
    }

    fn evaluate_join(
        &self,
        left: &Tuple,
        left_schema: &Schema,
        right: &Tuple,
        right_schema: &Schema,
    ) -> Result<Value> {
        let elements = self
            .children
            .iter()
            .map(|e| e.evaluate_join(left, left_schema, right, right_schema))
            .collect::<Result<_>>()?;
        Ok(Value::Array(elements))
    }

    fn children(&self) -> &[AbstractExpr] {
        &self.children
    }

    fn clone_with_children(&self, children: Vec<AbstractExpr>) -> AbstractExpr {
        debug_assert_eq!(children.len(), self.children.len());
        Self {
            children,
            ..self.clone()
        }
        .into()
    }

    fn return_type(&self) -> &Column {
        &self.return_type
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ExecutionError;
    use crate::expressions::test_util::*;
    use crate::expressions::{ArithmeticExpr, ArithmeticOp};
    use common::ValueError;

    fn array(elements: Vec<AbstractExpr>) -> AbstractExpr {
        ArrayExpr::new(elements).into()
    }

    #[test]
    fn evaluates_each_element_in_order() {
        let expr = array(vec![int(1), column(0), column(1)]);
        let got = eval(&expr, &ints(None, 7)).unwrap();
        assert_eq!(
            got,
            Value::Array(vec![Value::Int(1), Value::Null, Value::BigInt(7)])
        );
        assert_eq!(expr.return_type().data_type, DataType::Array);
        assert!(!expr.return_type().nullable);
    }

    #[test]
    fn empty_and_nested() {
        assert_eq!(
            eval(&array(vec![]), &ints(None, 0)).unwrap(),
            Value::Array(vec![])
        );
        let nested = array(vec![array(vec![int(1)]), array(vec![])]);
        assert_eq!(
            eval(&nested, &ints(None, 0)).unwrap(),
            Value::Array(vec![
                Value::Array(vec![Value::Int(1)]),
                Value::Array(vec![])
            ])
        );
    }

    #[test]
    fn element_error_propagates() {
        let div = ArithmeticExpr::new(ArithmeticOp::Div, int(1), int(0)).unwrap();
        let expr = array(vec![int(1), div.into()]);
        assert!(matches!(
            eval(&expr, &ints(None, 0)),
            Err(ExecutionError::Value(ValueError::DivisionByZero))
        ));
    }

    #[test]
    fn join_takes_elements_from_both_sides() {
        let expr = array(vec![
            side_column(0, &schema(), 1),
            side_column(1, &right_schema(), 0),
        ]);
        let got = eval_join(&expr, &ints(None, 5), &[Value::Int(6)]).unwrap();
        assert_eq!(got, Value::Array(vec![Value::BigInt(5), Value::Int(6)]));
    }
}
