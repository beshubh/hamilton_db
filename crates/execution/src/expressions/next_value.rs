use std::sync::Arc;

use catalog::Identity;
use common::{Column, Schema, Tuple, Value};

use super::{AbstractExpr, Expr};
use crate::Result;

/// The next value of an identity column, for a row an INSERT leaves the
/// column out of. Each evaluation takes a new value, which fails with
/// `Overflow` once it no longer fits the column's type.
#[derive(Clone, Debug)]
pub struct NextValueExpr {
    identity: Arc<Identity>,
    return_type: Column,
}

impl NextValueExpr {
    /// `return_type` is the identity column's: INT or BIGINT.
    pub fn new(identity: Arc<Identity>, return_type: Column) -> Self {
        Self {
            identity,
            return_type,
        }
    }

    fn next(&self) -> Result<Value> {
        let value = Value::BigInt(self.identity.next_value()?);
        Ok(value.cast_number(self.return_type.data_type)?)
    }
}

impl Expr for NextValueExpr {
    fn evaluate(&self, _tuple: &Tuple, _schema: &Schema) -> Result<Value> {
        self.next()
    }

    fn evaluate_join(
        &self,
        _left: &Tuple,
        _left_schema: &Schema,
        _right: &Tuple,
        _right_schema: &Schema,
    ) -> Result<Value> {
        self.next()
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
