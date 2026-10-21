//! Encoding of a row of values as a table tuple's payload.
//!
//! ```text
//! +---------------+---------------------------------+--------------------+
//! | null bitmap   | fixed-width slots, one per      | variable-length    |
//! | ⌈n / 8⌉ B     | column, in column order         | data               |
//! +---------------+---------------------------------+--------------------+
//! ```
//!
//! Bit `i % 8` of byte `i / 8` of the bitmap is set when column `i` is NULL.
//!
//! Every column has a slot whose width depends only on its type, so a
//! column's slot is at the same offset in every tuple of a schema and can be
//! read without decoding the columns before it:
//!
//! | type    | slot                                                     |
//! |---------|----------------------------------------------------------|
//! | INT     | 4 B                                                      |
//! | BIGINT  | 8 B                                                      |
//! | BOOL    | 1 B, 0 or 1                                              |
//! | VARCHAR | offset (u16) and length (u16) of its UTF-8 bytes, with   |
//! |         | the offset counted from the start of the tuple           |
//!
//! The slot of a NULL column is zero. Strings are stored in the data area in
//! column order. All multi-byte fields are little-endian and unaligned.

use crate::error::{Result, ValueError};
use crate::rid::Rid;
use crate::schema::Schema;
use crate::value::{DataType, Value};

/// Offsets in a tuple are u16, so no tuple can be longer than this.
pub const MAX_TUPLE_SIZE: usize = u16::MAX as usize;

/// Width of a column's slot in the fixed-width part of a tuple.
pub(crate) fn fixed_width(data_type: DataType) -> usize {
    match data_type {
        DataType::Int => 4,
        DataType::BigInt => 8,
        DataType::Bool => 1,
        DataType::Varchar(_) => 4,
        // Never holds a value: encoding a non-NULL ARRAY fails.
        DataType::Array => 0,
    }
}

pub(crate) fn bitmap_len(column_count: usize) -> usize {
    column_count.div_ceil(8)
}

/// A row encoded as bytes, and where it is stored if it came from a table.
///
/// A tuple does not carry its schema: the same bytes are read with a table's
/// schema in the table heap and with a key schema in an index, so readers pass
/// the schema they mean.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Tuple {
    rid: Option<Rid>,
    data: Vec<u8>,
}

impl Tuple {
    /// Encodes a row, checking each value against its column's type,
    /// nullability and length.
    pub fn encode(schema: &Schema, values: &[Value]) -> Result<Self> {
        Ok(Self::from_bytes(encode(schema, values)?))
    }

    /// Wraps bytes already in tuple format, such as a table page's payload.
    /// They are only checked when read.
    pub fn from_bytes(data: Vec<u8>) -> Self {
        Self { rid: None, data }
    }

    pub fn with_rid(mut self, rid: Rid) -> Self {
        self.rid = Some(rid);
        self
    }

    pub fn set_rid(&mut self, rid: Rid) {
        self.rid = Some(rid);
    }

    /// Where the tuple is stored, if it was read from or inserted into a table.
    pub fn rid(&self) -> Option<Rid> {
        self.rid
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn into_data(self) -> Vec<u8> {
        self.data
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Decodes every column.
    pub fn decode(&self, schema: &Schema) -> Result<Vec<Value>> {
        (0..schema.len())
            .map(|idx| decode_column(schema, &self.data, idx))
            .collect()
    }

    /// Decodes column `idx`, reading only its null bit, its slot and, for a
    /// VARCHAR, its bytes.
    pub fn value(&self, schema: &Schema, idx: usize) -> Result<Value> {
        decode_column(schema, &self.data, idx)
    }

    /// Whether column `idx` is NULL, from the null bitmap alone.
    pub fn is_null(&self, schema: &Schema, idx: usize) -> Result<bool> {
        is_null(schema, &self.data, idx)
    }

    /// Builds an index key from columns `key_attrs` of this tuple, encoded
    /// with `key_schema`. The key has no `Rid`.
    pub fn to_key(
        &self,
        schema: &Schema,
        key_schema: &Schema,
        key_attrs: &[usize],
    ) -> Result<Self> {
        let values = key_attrs
            .iter()
            .map(|&idx| self.value(schema, idx))
            .collect::<Result<Vec<_>>>()?;
        Self::encode(key_schema, &values)
    }
}

fn encode(schema: &Schema, values: &[Value]) -> Result<Vec<u8>> {
    if values.len() != schema.len() {
        return Err(ValueError::ColumnCountMismatch {
            expected: schema.len(),
            found: values.len(),
        });
    }

    let mut buf = vec![0; schema.fixed_len()];
    for (idx, (column, value)) in schema.columns().iter().zip(values).enumerate() {
        let at = schema.fixed_offset(idx);
        match (column.data_type, value) {
            (_, Value::Null) => {
                if !column.nullable {
                    return Err(ValueError::NullNotAllowed(column.name.clone()));
                }
                buf[idx / 8] |= 1 << (idx % 8);
            }
            (DataType::Int, Value::Int(v)) => buf[at..at + 4].copy_from_slice(&v.to_le_bytes()),
            (DataType::BigInt, Value::BigInt(v)) => {
                buf[at..at + 8].copy_from_slice(&v.to_le_bytes())
            }
            (DataType::Bool, Value::Bool(v)) => buf[at] = u8::from(*v),
            (DataType::Varchar(max), Value::Varchar(s)) => {
                if s.len() > max as usize {
                    return Err(ValueError::ValueTooLong {
                        column: column.name.clone(),
                        len: s.len(),
                        max,
                    });
                }
                let offset = buf.len();
                buf.extend_from_slice(s.as_bytes());
                // Checked before the casts below, which it makes lossless.
                if buf.len() > MAX_TUPLE_SIZE {
                    return Err(ValueError::TupleTooLarge);
                }
                buf[at..at + 2].copy_from_slice(&(offset as u16).to_le_bytes());
                buf[at + 2..at + 4].copy_from_slice(&(s.len() as u16).to_le_bytes());
            }
            (DataType::Array, _) => {
                return Err(ValueError::NotStorable {
                    column: column.name.clone(),
                    data_type: column.data_type,
                });
            }
            _ => {
                return Err(ValueError::TypeMismatch {
                    column: column.name.clone(),
                    expected: column.data_type,
                    found: value.type_name(),
                });
            }
        }
    }

    if buf.len() > MAX_TUPLE_SIZE {
        return Err(ValueError::TupleTooLarge);
    }
    Ok(buf)
}

fn decode_column(schema: &Schema, bytes: &[u8], idx: usize) -> Result<Value> {
    if is_null(schema, bytes, idx)? {
        return Ok(Value::Null);
    }
    let column = &schema.columns()[idx];

    let at = schema.fixed_offset(idx);
    Ok(match column.data_type {
        DataType::Int => Value::Int(i32::from_le_bytes(read_array(bytes, at))),
        DataType::BigInt => Value::BigInt(i64::from_le_bytes(read_array(bytes, at))),
        DataType::Bool => match bytes[at] {
            0 => Value::Bool(false),
            1 => Value::Bool(true),
            _ => return Err(ValueError::CorruptTuple("BOOL is not 0 or 1")),
        },
        DataType::Varchar(_) => {
            let offset = usize::from(u16::from_le_bytes(read_array(bytes, at)));
            let len = usize::from(u16::from_le_bytes(read_array(bytes, at + 2)));
            if offset < schema.fixed_len() || offset + len > bytes.len() {
                return Err(ValueError::CorruptTuple("VARCHAR is out of bounds"));
            }
            let s = std::str::from_utf8(&bytes[offset..offset + len])
                .map_err(|_| ValueError::CorruptTuple("VARCHAR is not UTF-8"))?;
            Value::Varchar(s.to_owned())
        }
        DataType::Array => return Err(ValueError::CorruptTuple("ARRAY is not NULL")),
    })
}

/// Reads column `idx`'s null bit, checking that the tuple is long enough to
/// hold every column's slot.
fn is_null(schema: &Schema, bytes: &[u8], idx: usize) -> Result<bool> {
    if idx >= schema.len() {
        return Err(ValueError::ColumnOutOfRange {
            idx,
            count: schema.len(),
        });
    }
    if bytes.len() < schema.fixed_len() {
        return Err(ValueError::CorruptTuple(
            "shorter than its fixed-width part",
        ));
    }
    Ok(bytes[idx / 8] & (1 << (idx % 8)) != 0)
}

/// Copies `N` bytes starting at `at`. Callers have checked the bounds.
fn read_array<const N: usize>(bytes: &[u8], at: usize) -> [u8; N] {
    let mut out = [0; N];
    out.copy_from_slice(&bytes[at..at + N]);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::Column;

    fn users() -> Schema {
        Schema::new(vec![
            Column::new("id", DataType::Int, false),
            Column::new("name", DataType::Varchar(16), true),
            Column::new("balance", DataType::BigInt, true),
            Column::new("bio", DataType::Varchar(64), true),
            Column::new("active", DataType::Bool, false),
        ])
    }

    fn row() -> Vec<Value> {
        vec![
            Value::Int(7),
            Value::from("ada"),
            Value::BigInt(-12),
            Value::from("hello"),
            Value::Bool(true),
        ]
    }

    #[test]
    fn round_trips() {
        let schema = users();
        let tuple = Tuple::encode(&schema, &row()).unwrap();
        assert_eq!(tuple.decode(&schema).unwrap(), row());
        assert_eq!(tuple.rid(), None);
    }

    #[test]
    fn round_trips_through_bytes() {
        let schema = users();
        let bytes = Tuple::encode(&schema, &row()).unwrap().into_data();
        let tuple = Tuple::from_bytes(bytes).with_rid(Rid::new(3, 1));
        assert_eq!(tuple.decode(&schema).unwrap(), row());
        assert_eq!(tuple.rid(), Some(Rid::new(3, 1)));
    }

    #[test]
    fn round_trips_nulls_and_empty_strings() {
        let schema = users();
        let values = vec![
            Value::Int(1),
            Value::Null,
            Value::Null,
            Value::from(""),
            Value::Bool(false),
        ];
        let tuple = Tuple::encode(&schema, &values).unwrap();
        assert_eq!(tuple.decode(&schema).unwrap(), values);
        assert!(tuple.is_null(&schema, 1).unwrap());
        assert!(!tuple.is_null(&schema, 3).unwrap());
    }

    #[test]
    fn layout() {
        let schema = Schema::new(vec![
            Column::new("a", DataType::Int, false),
            Column::new("b", DataType::Varchar(8), true),
            Column::new("c", DataType::Bool, true),
            Column::new("d", DataType::Varchar(8), false),
        ]);
        let values = vec![
            Value::Int(0x0102_0304),
            Value::from("hi"),
            Value::Null,
            Value::from("xyz"),
        ];
        let tuple = Tuple::encode(&schema, &values).unwrap();
        #[rustfmt::skip]
        assert_eq!(tuple.data(), [
            0b0000_0100,             // null bitmap: c
            0x04, 0x03, 0x02, 0x01,  // a
            14, 0, 2, 0,             // b: offset 14, length 2
            0,                       // c
            16, 0, 3, 0,             // d: offset 16, length 3
            b'h', b'i', b'x', b'y', b'z',
        ]);
    }

    #[test]
    fn bitmap_spans_bytes() {
        let columns = (0..9)
            .map(|i| Column::new(format!("c{i}"), DataType::Bool, true))
            .collect();
        let schema = Schema::new(columns);
        let mut values = vec![Value::Bool(true); 9];
        values[8] = Value::Null;
        let tuple = Tuple::encode(&schema, &values).unwrap();
        assert_eq!(&tuple.data()[..2], [0, 1]);
        assert_eq!(tuple.decode(&schema).unwrap(), values);
    }

    #[test]
    fn reads_one_column() {
        let schema = users();
        let tuple = Tuple::encode(&schema, &row()).unwrap();
        assert_eq!(tuple.value(&schema, 2).unwrap(), Value::BigInt(-12));
        assert_eq!(tuple.value(&schema, 3).unwrap(), Value::from("hello"));
        assert_eq!(tuple.value(&schema, 4).unwrap(), Value::Bool(true));
    }

    #[test]
    fn builds_index_keys() {
        let schema = users();
        let tuple = Tuple::encode(&schema, &row())
            .unwrap()
            .with_rid(Rid::new(1, 2));
        let key_schema = Schema::new(vec![
            Column::new("active", DataType::Bool, false),
            Column::new("id", DataType::Int, false),
        ]);
        let key = tuple.to_key(&schema, &key_schema, &[4, 0]).unwrap();
        assert_eq!(
            key.decode(&key_schema).unwrap(),
            [Value::Bool(true), Value::Int(7)]
        );
        assert_eq!(key.rid(), None);
    }

    #[test]
    fn empty_schema() {
        let schema = Schema::default();
        let tuple = Tuple::encode(&schema, &[]).unwrap();
        assert!(tuple.is_empty());
        assert!(tuple.decode(&schema).unwrap().is_empty());
    }

    #[test]
    fn rejects_wrong_value_count() {
        let err = Tuple::encode(&users(), &[Value::Int(1)]).unwrap_err();
        assert!(matches!(
            err,
            ValueError::ColumnCountMismatch {
                expected: 5,
                found: 1
            }
        ));
    }

    #[test]
    fn rejects_wrong_type() {
        let mut values = row();
        values[0] = Value::BigInt(7);
        let err = Tuple::encode(&users(), &values).unwrap_err();
        assert_eq!(err.to_string(), "column id is INT, found BIGINT");
    }

    #[test]
    fn rejects_null_in_non_nullable_column() {
        let mut values = row();
        values[4] = Value::Null;
        let err = Tuple::encode(&users(), &values).unwrap_err();
        assert!(matches!(err, ValueError::NullNotAllowed(name) if name == "active"));
    }

    #[test]
    fn rejects_string_longer_than_column() {
        let mut values = row();
        values[1] = Value::from("x".repeat(17));
        let err = Tuple::encode(&users(), &values).unwrap_err();
        assert!(matches!(
            err,
            ValueError::ValueTooLong {
                len: 17,
                max: 16,
                ..
            }
        ));
    }

    #[test]
    fn rejects_tuple_over_max_size() {
        let schema = Schema::new(vec![Column::new("s", DataType::Varchar(u32::MAX), false)]);
        let values = [Value::from("x".repeat(MAX_TUPLE_SIZE))];
        assert!(matches!(
            Tuple::encode(&schema, &values),
            Err(ValueError::TupleTooLarge)
        ));
    }

    #[test]
    fn rejects_column_out_of_range() {
        let schema = users();
        let tuple = Tuple::encode(&schema, &row()).unwrap();
        assert!(matches!(
            tuple.value(&schema, 5),
            Err(ValueError::ColumnOutOfRange { idx: 5, count: 5 })
        ));
        assert!(tuple.is_null(&schema, 5).is_err());
    }

    #[test]
    fn rejects_corrupt_tuples() {
        let schema = users();
        let bytes = Tuple::encode(&schema, &row()).unwrap().into_data();
        let corrupt = |bytes: &[u8], idx| {
            matches!(
                Tuple::from_bytes(bytes.to_vec()).value(&schema, idx),
                Err(ValueError::CorruptTuple(_))
            )
        };

        assert!(corrupt(&bytes[..schema.fixed_len() - 1], 0));
        // Cut off the end of "hello".
        assert!(corrupt(&bytes[..bytes.len() - 1], 3));

        let active = schema.fixed_offset(4);
        let mut bad_bool = bytes.clone();
        bad_bool[active] = 2;
        assert!(corrupt(&bad_bool, 4));

        let mut bad_utf8 = bytes.clone();
        let last = bad_utf8.len() - 1;
        bad_utf8[last] = 0xff;
        assert!(corrupt(&bad_utf8, 3));

        // Point "name" back into the fixed-width part.
        let name = schema.fixed_offset(1);
        let mut bad_offset = bytes.clone();
        bad_offset[name..name + 2].copy_from_slice(&0u16.to_le_bytes());
        assert!(corrupt(&bad_offset, 1));
    }

    #[test]
    fn arrays_are_not_storable() {
        let schema = Schema::new(vec![Column::new("xs", DataType::Array, true)]);
        assert!(matches!(
            Tuple::encode(&schema, &[Value::Array(vec![Value::Int(1)])]),
            Err(ValueError::NotStorable { column, data_type: DataType::Array }) if column == "xs"
        ));
        let null = Tuple::encode(&schema, &[Value::Null]).unwrap();
        assert_eq!(null.value(&schema, 0).unwrap(), Value::Null);
    }
}
