//! SQL values and their types.
//!
//! Operators follow SQL semantics: a NULL operand makes the result NULL
//! (except where AND / OR can decide without it), and INT is widened to
//! BIGINT when the two are mixed. NULL is untyped, so `NULL + 'a'` is NULL
//! rather than a type error; catching that is left to the planner.
//!
//! `==` on [`Value`] is structural identity (`NULL == NULL`, and
//! `Int(1) != BigInt(1)`). SQL comparison is [`Value::compare`] and the
//! `cmp_*` methods.

use std::cmp::Ordering;
use std::fmt;

use crate::error::{Result, ValueError};

/// The declared type of a column.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DataType {
    /// 32-bit signed integer.
    Int,
    /// 64-bit signed integer.
    BigInt,
    Bool,
    /// UTF-8 string of at most this many bytes.
    Varchar(u32),
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Int => f.write_str("INT"),
            DataType::BigInt => f.write_str("BIGINT"),
            DataType::Bool => f.write_str("BOOL"),
            DataType::Varchar(max_len) => write!(f, "VARCHAR({max_len})"),
        }
    }
}

/// A single SQL value.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Value {
    Null,
    Int(i32),
    BigInt(i64),
    Bool(bool),
    Varchar(String),
}

#[derive(Clone, Copy)]
enum ArithOp {
    Add,
    Sub,
    Mul,
    Div,
    Rem,
}

impl ArithOp {
    fn symbol(self) -> &'static str {
        match self {
            ArithOp::Add => "+",
            ArithOp::Sub => "-",
            ArithOp::Mul => "*",
            ArithOp::Div => "/",
            ArithOp::Rem => "%",
        }
    }

    fn apply(self, l: i64, r: i64) -> Result<i64> {
        let out = match self {
            ArithOp::Add => l.checked_add(r),
            ArithOp::Sub => l.checked_sub(r),
            ArithOp::Mul => l.checked_mul(r),
            ArithOp::Div | ArithOp::Rem if r == 0 => return Err(ValueError::DivisionByZero),
            ArithOp::Div => l.checked_div(r),
            // `i64::MIN % -1` is 0, but `checked_rem` reports it as overflow.
            ArithOp::Rem => Some(l.wrapping_rem(r)),
        };
        out.ok_or(ValueError::Overflow)
    }
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// The SQL name of the value's type, for error messages.
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Null => "NULL",
            Value::Int(_) => "INT",
            Value::BigInt(_) => "BIGINT",
            Value::Bool(_) => "BOOL",
            Value::Varchar(_) => "VARCHAR",
        }
    }

    /// Orders two values, or returns `None` if either is NULL.
    ///
    /// Integers compare with each other across widths, strings compare
    /// bytewise, and `false < true`.
    pub fn compare(&self, other: &Value) -> Result<Option<Ordering>> {
        match (self, other) {
            (Value::Null, _) | (_, Value::Null) => Ok(None),
            (Value::Bool(l), Value::Bool(r)) => Ok(Some(l.cmp(r))),
            (Value::Varchar(l), Value::Varchar(r)) => Ok(Some(l.cmp(r))),
            _ => match (self.as_i64(), other.as_i64()) {
                (Some(l), Some(r)) => Ok(Some(l.cmp(&r))),
                _ => Err(ValueError::Incomparable {
                    left: self.type_name(),
                    right: other.type_name(),
                }),
            },
        }
    }

    /// SQL `=`: a BOOL, or NULL if either side is NULL.
    pub fn cmp_eq(&self, other: &Value) -> Result<Value> {
        self.compare_by(other, Ordering::is_eq)
    }

    /// SQL `<>`.
    pub fn cmp_ne(&self, other: &Value) -> Result<Value> {
        self.compare_by(other, Ordering::is_ne)
    }

    /// SQL `<`.
    pub fn cmp_lt(&self, other: &Value) -> Result<Value> {
        self.compare_by(other, Ordering::is_lt)
    }

    /// SQL `<=`.
    pub fn cmp_le(&self, other: &Value) -> Result<Value> {
        self.compare_by(other, Ordering::is_le)
    }

    /// SQL `>`.
    pub fn cmp_gt(&self, other: &Value) -> Result<Value> {
        self.compare_by(other, Ordering::is_gt)
    }

    /// SQL `>=`.
    pub fn cmp_ge(&self, other: &Value) -> Result<Value> {
        self.compare_by(other, Ordering::is_ge)
    }

    pub fn checked_add(&self, other: &Value) -> Result<Value> {
        self.arith(ArithOp::Add, other)
    }

    pub fn checked_sub(&self, other: &Value) -> Result<Value> {
        self.arith(ArithOp::Sub, other)
    }

    pub fn checked_mul(&self, other: &Value) -> Result<Value> {
        self.arith(ArithOp::Mul, other)
    }

    /// Integer division, truncating toward zero.
    pub fn checked_div(&self, other: &Value) -> Result<Value> {
        self.arith(ArithOp::Div, other)
    }

    /// Remainder of truncating division; takes the sign of `self`.
    pub fn checked_rem(&self, other: &Value) -> Result<Value> {
        self.arith(ArithOp::Rem, other)
    }

    pub fn checked_neg(&self) -> Result<Value> {
        match self {
            Value::Null => Ok(Value::Null),
            Value::Int(v) => v.checked_neg().map(Value::Int).ok_or(ValueError::Overflow),
            Value::BigInt(v) => v
                .checked_neg()
                .map(Value::BigInt)
                .ok_or(ValueError::Overflow),
            _ => Err(ValueError::InvalidOperand {
                op: "-",
                operand: self.type_name(),
            }),
        }
    }

    /// SQL `AND`: false if either side is false, even when the other is NULL.
    pub fn logical_and(&self, other: &Value) -> Result<Value> {
        Ok(match self.bool_operands(other, "AND")? {
            (Some(false), _) | (_, Some(false)) => Value::Bool(false),
            (Some(true), Some(true)) => Value::Bool(true),
            _ => Value::Null,
        })
    }

    /// SQL `OR`: true if either side is true, even when the other is NULL.
    pub fn logical_or(&self, other: &Value) -> Result<Value> {
        Ok(match self.bool_operands(other, "OR")? {
            (Some(true), _) | (_, Some(true)) => Value::Bool(true),
            (Some(false), Some(false)) => Value::Bool(false),
            _ => Value::Null,
        })
    }

    /// SQL `NOT`.
    pub fn logical_not(&self) -> Result<Value> {
        match self {
            Value::Null => Ok(Value::Null),
            Value::Bool(b) => Ok(Value::Bool(!b)),
            _ => Err(ValueError::InvalidOperand {
                op: "NOT",
                operand: self.type_name(),
            }),
        }
    }

    fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Int(v) => Some(i64::from(*v)),
            Value::BigInt(v) => Some(*v),
            _ => None,
        }
    }

    /// `Some(None)` for NULL, `Some(Some(b))` for a BOOL, `None` otherwise.
    fn as_bool(&self) -> Option<Option<bool>> {
        match self {
            Value::Null => Some(None),
            Value::Bool(b) => Some(Some(*b)),
            _ => None,
        }
    }

    fn compare_by(&self, other: &Value, pred: fn(Ordering) -> bool) -> Result<Value> {
        Ok(self
            .compare(other)?
            .map_or(Value::Null, |ord| Value::Bool(pred(ord))))
    }

    fn arith(&self, op: ArithOp, other: &Value) -> Result<Value> {
        match (self, other) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            // i32 results always fit in i64, so compute wide and narrow back.
            (Value::Int(l), Value::Int(r)) => {
                let out = op.apply(i64::from(*l), i64::from(*r))?;
                i32::try_from(out)
                    .map(Value::Int)
                    .map_err(|_| ValueError::Overflow)
            }
            _ => match (self.as_i64(), other.as_i64()) {
                (Some(l), Some(r)) => op.apply(l, r).map(Value::BigInt),
                _ => Err(ValueError::InvalidOperands {
                    op: op.symbol(),
                    left: self.type_name(),
                    right: other.type_name(),
                }),
            },
        }
    }

    fn bool_operands(
        &self,
        other: &Value,
        op: &'static str,
    ) -> Result<(Option<bool>, Option<bool>)> {
        match (self.as_bool(), other.as_bool()) {
            (Some(l), Some(r)) => Ok((l, r)),
            _ => Err(ValueError::InvalidOperands {
                op,
                left: self.type_name(),
                right: other.type_name(),
            }),
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => f.write_str("NULL"),
            Value::Int(v) => write!(f, "{v}"),
            Value::BigInt(v) => write!(f, "{v}"),
            Value::Bool(v) => write!(f, "{v}"),
            Value::Varchar(v) => f.write_str(v),
        }
    }
}

impl From<i32> for Value {
    fn from(v: i32) -> Self {
        Value::Int(v)
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::BigInt(v)
    }
}

impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Bool(v)
    }
}

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::Varchar(v.to_owned())
    }
}

impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::Varchar(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NULL: Value = Value::Null;

    fn v(x: impl Into<Value>) -> Value {
        x.into()
    }

    #[test]
    fn compares_within_a_type() {
        assert_eq!(v(1).compare(&v(2)), Ok(Some(Ordering::Less)));
        assert_eq!(v("b").compare(&v("a")), Ok(Some(Ordering::Greater)));
        assert_eq!(v(false).compare(&v(true)), Ok(Some(Ordering::Less)));
    }

    #[test]
    fn compares_int_with_bigint() {
        assert_eq!(v(5).compare(&v(5_i64)), Ok(Some(Ordering::Equal)));
        assert_eq!(
            v(i64::MAX).compare(&v(i32::MAX)),
            Ok(Some(Ordering::Greater))
        );
    }

    #[test]
    fn comparison_with_null_is_null() {
        assert_eq!(v(1).compare(&NULL), Ok(None));
        assert_eq!(NULL.cmp_eq(&NULL), Ok(NULL));
        assert_eq!(v("a").cmp_lt(&NULL), Ok(NULL));
    }

    #[test]
    fn comparison_operators() {
        assert_eq!(v(1).cmp_eq(&v(1_i64)), Ok(v(true)));
        assert_eq!(v(1).cmp_ne(&v(1)), Ok(v(false)));
        assert_eq!(v(1).cmp_lt(&v(2)), Ok(v(true)));
        assert_eq!(v(2).cmp_le(&v(2)), Ok(v(true)));
        assert_eq!(v("a").cmp_gt(&v("b")), Ok(v(false)));
        assert_eq!(v(true).cmp_ge(&v(false)), Ok(v(true)));
    }

    #[test]
    fn comparing_mismatched_types_fails() {
        let err = v(1).compare(&v("1")).unwrap_err();
        assert!(matches!(
            err,
            ValueError::Incomparable {
                left: "INT",
                right: "VARCHAR"
            }
        ));
        assert_eq!(err.to_string(), "cannot compare INT with VARCHAR");
        assert!(v(true).cmp_eq(&v(1)).is_err());
    }

    #[test]
    fn int_arithmetic_stays_int() {
        assert_eq!(v(2).checked_add(&v(3)), Ok(v(5)));
        assert_eq!(v(2).checked_sub(&v(3)), Ok(v(-1)));
        assert_eq!(v(4).checked_mul(&v(3)), Ok(v(12)));
        assert_eq!(v(7).checked_div(&v(2)), Ok(v(3)));
        assert_eq!(v(-7).checked_div(&v(2)), Ok(v(-3)));
        assert_eq!(v(-7).checked_rem(&v(2)), Ok(v(-1)));
    }

    #[test]
    fn mixed_arithmetic_widens_to_bigint() {
        assert_eq!(v(2).checked_add(&v(3_i64)), Ok(v(5_i64)));
        assert_eq!(v(i64::MAX).checked_sub(&v(1)), Ok(v(i64::MAX - 1)));
    }

    #[test]
    fn arithmetic_with_null_is_null() {
        assert_eq!(v(1).checked_add(&NULL), Ok(NULL));
        assert_eq!(NULL.checked_div(&v(0)), Ok(NULL));
        assert_eq!(NULL.checked_neg(), Ok(NULL));
    }

    #[test]
    fn overflow_is_an_error() {
        assert_eq!(v(i32::MAX).checked_add(&v(1)), Err(ValueError::Overflow));
        assert_eq!(v(i32::MIN).checked_div(&v(-1)), Err(ValueError::Overflow));
        assert_eq!(v(i64::MAX).checked_mul(&v(2)), Err(ValueError::Overflow));
        assert_eq!(v(i32::MIN).checked_neg(), Err(ValueError::Overflow));
        assert_eq!(v(i64::MIN).checked_neg(), Err(ValueError::Overflow));
    }

    #[test]
    fn min_rem_minus_one_is_zero() {
        assert_eq!(v(i32::MIN).checked_rem(&v(-1)), Ok(v(0)));
        assert_eq!(v(i64::MIN).checked_rem(&v(-1)), Ok(v(0_i64)));
    }

    #[test]
    fn division_by_zero_is_an_error() {
        assert_eq!(v(1).checked_div(&v(0)), Err(ValueError::DivisionByZero));
        assert_eq!(v(1).checked_rem(&v(0_i64)), Err(ValueError::DivisionByZero));
    }

    #[test]
    fn arithmetic_on_non_numbers_fails() {
        let err = v("a").checked_add(&v(1)).unwrap_err();
        assert_eq!(err.to_string(), "cannot apply + to VARCHAR and INT");
        assert!(matches!(
            v(true).checked_neg(),
            Err(ValueError::InvalidOperand {
                op: "-",
                operand: "BOOL"
            })
        ));
    }

    #[test]
    fn and_is_three_valued() {
        let t = v(true);
        let f = v(false);
        assert_eq!(t.logical_and(&t), Ok(v(true)));
        assert_eq!(t.logical_and(&f), Ok(v(false)));
        assert_eq!(f.logical_and(&NULL), Ok(v(false)));
        assert_eq!(NULL.logical_and(&f), Ok(v(false)));
        assert_eq!(t.logical_and(&NULL), Ok(NULL));
        assert_eq!(NULL.logical_and(&NULL), Ok(NULL));
    }

    #[test]
    fn or_is_three_valued() {
        let t = v(true);
        let f = v(false);
        assert_eq!(f.logical_or(&f), Ok(v(false)));
        assert_eq!(f.logical_or(&t), Ok(v(true)));
        assert_eq!(t.logical_or(&NULL), Ok(v(true)));
        assert_eq!(NULL.logical_or(&t), Ok(v(true)));
        assert_eq!(f.logical_or(&NULL), Ok(NULL));
    }

    #[test]
    fn not() {
        assert_eq!(v(true).logical_not(), Ok(v(false)));
        assert_eq!(NULL.logical_not(), Ok(NULL));
        assert!(v(1).logical_not().is_err());
    }

    #[test]
    fn logic_on_non_bools_fails() {
        assert!(matches!(
            v(1).logical_and(&v(true)),
            Err(ValueError::InvalidOperands { op: "AND", .. })
        ));
    }

    #[test]
    fn display() {
        assert_eq!(NULL.to_string(), "NULL");
        assert_eq!(v(-3).to_string(), "-3");
        assert_eq!(v(true).to_string(), "true");
        assert_eq!(v("hi").to_string(), "hi");
        assert_eq!(DataType::Varchar(32).to_string(), "VARCHAR(32)");
    }
}
