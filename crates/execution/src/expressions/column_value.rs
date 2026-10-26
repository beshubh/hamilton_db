use common::{Column, Schema, Tuple, Value};

use super::{AbstractExpr, Expr};
use crate::{ExecutionError, Result};

/// Reads column `col_idx` of input `tuple_idx`: 0 for the only input of a
/// plan, or 0 for the left and 1 for the right input of a join.
#[derive(Clone, Debug)]
pub struct ColumnValueExpr {
    tuple_idx: usize,
    col_idx: usize,
    return_type: Column,
}

impl ColumnValueExpr {
    pub fn new(tuple_idx: usize, col_idx: usize, return_type: Column) -> Self {
        Self {
            tuple_idx,
            col_idx,
            return_type,
        }
    }

    pub fn tuple_idx(&self) -> usize {
        self.tuple_idx
    }

    pub fn col_idx(&self) -> usize {
        self.col_idx
    }

    fn out_of_range(&self, inputs: usize) -> ExecutionError {
        ExecutionError::TupleIndexOutOfRange {
            tuple_idx: self.tuple_idx,
            inputs,
        }
    }
}

impl Expr for ColumnValueExpr {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value> {
        if self.tuple_idx != 0 {
            return Err(self.out_of_range(1));
        }
        Ok(tuple.value(schema, self.col_idx)?)
    }

    fn evaluate_join(
        &self,
        left: &Tuple,
        left_schema: &Schema,
        right: &Tuple,
        right_schema: &Schema,
    ) -> Result<Value> {
        let (tuple, schema) = match self.tuple_idx {
            0 => (left, left_schema),
            1 => (right, right_schema),
            _ => return Err(self.out_of_range(2)),
        };
        Ok(tuple.value(schema, self.col_idx)?)
    }

    fn children(&self) -> &[AbstractExpr] {
        &[]
    }

    fn clone_with_children(&self, children: Vec<AbstractExpr>) -> AbstractExpr {
        debug_assert!(children.is_empty());
        self.clone().into()
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
    fn reads_a_column() {
        let got = eval(&column(1), &ints(Some(1), 42)).unwrap();
        assert_eq!(got, Value::BigInt(42));
        assert_eq!(eval(&column(0), &ints(None, 0)).unwrap(), Value::Null);
    }

    #[test]
    fn join_reads_each_side() {
        let left = side_column(0, &schema(), 1);
        let right = side_column(1, &right_schema(), 0);
        let (l, r) = (ints(Some(1), 10), [Value::Int(3)]);
        assert_eq!(eval_join(&left, &l, &r).unwrap(), Value::BigInt(10));
        assert_eq!(eval_join(&right, &l, &r).unwrap(), Value::Int(3));
    }

    #[test]
    fn right_column_outside_join_is_an_error() {
        let expr = side_column(1, &right_schema(), 0);
        assert!(matches!(
            eval(&expr, &ints(Some(1), 0)),
            Err(ExecutionError::TupleIndexOutOfRange {
                tuple_idx: 1,
                inputs: 1,
            })
        ));
    }

    #[test]
    fn join_column_past_right_side_is_an_error() {
        let expr = side_column(2, &right_schema(), 0);
        assert!(matches!(
            eval_join(&expr, &ints(Some(1), 0), &[Value::Int(1)]),
            Err(ExecutionError::TupleIndexOutOfRange {
                tuple_idx: 2,
                inputs: 2,
            })
        ));
    }
}
