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

use std::ops::Range;

use crate::error::{Result, ValueError};
use crate::rid::Rid;
use crate::schema::Schema;
use crate::value::{DataType, Value, ValueRef};

/// Offsets in a tuple are u16, so no tuple can be longer than this.
pub const MAX_TUPLE_SIZE: usize = u16::MAX as usize;

/// Width of a column's slot in the fixed-width part of a tuple.
pub(crate) fn fixed_width(data_type: DataType) -> usize {
    match data_type {
        DataType::Int => 4,
        DataType::BigInt => 8,
        DataType::Double => 8,
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

    /// How many of the tuple's bytes its columns use with `schema`: the
    /// fixed-width part and the strings, which `encode` and `join` lay out
    /// with nothing after the last one. Bytes past this are not part of any
    /// column, such as the zeros the table heap pads an uncommitted shrinking
    /// update with.
    pub fn encoded_len(&self, schema: &Schema) -> Result<usize> {
        let mut end = schema.fixed_len();
        if self.data.len() < end {
            return Err(ValueError::CorruptTuple(
                "shorter than its fixed-width part",
            ));
        }
        for (idx, column) in schema.columns().iter().enumerate() {
            if matches!(column.data_type, DataType::Varchar(_))
                && !is_null(schema, &self.data, idx)?
            {
                let at = schema.fixed_offset(idx);
                end = end.max(varchar_range(schema, &self.data, at)?.end);
            }
        }
        Ok(end)
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

    /// Reads column `idx` like [`value`](Self::value), but in place: a
    /// VARCHAR borrows the tuple's bytes instead of copying them.
    pub fn value_ref(&self, schema: &Schema, idx: usize) -> Result<ValueRef<'_>> {
        read_column(schema, &self.data, idx)
    }

    /// Joins `left` and `right` into one tuple of `schema`, whose columns
    /// are `left_schema`'s followed by `right_schema`'s, as a join outputs.
    /// The columns' bytes are copied over without decoding them. A `right`
    /// of `None` gives NULLs for its columns, as a LEFT join pads a row
    /// that matched nothing.
    pub fn join(
        schema: &Schema,
        left: &Tuple,
        left_schema: &Schema,
        right: Option<&Tuple>,
        right_schema: &Schema,
    ) -> Result<Self> {
        let sides = [
            (Some(left.data.as_slice()), left_schema),
            (right.map(|right| right.data.as_slice()), right_schema),
        ];
        Ok(Self::from_bytes(join(schema, sides)?))
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

    let strings_len: usize = values
        .iter()
        .map(|value| match value {
            Value::Varchar(s) => s.len(),
            _ => 0,
        })
        .sum();
    let mut buf = Vec::with_capacity(schema.fixed_len() + strings_len);
    buf.resize(schema.fixed_len(), 0);
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
            (DataType::Double, Value::Double(v)) => {
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

/// Lays out the columns of each side one after another, copying each
/// column's slot and, for a VARCHAR, its bytes, which get a new offset.
/// A side without bytes is all NULLs.
fn join(schema: &Schema, sides: [(Option<&[u8]>, &Schema); 2]) -> Result<Vec<u8>> {
    let found: usize = sides.iter().map(|(_, side)| side.len()).sum();
    if found != schema.len() {
        return Err(ValueError::ColumnCountMismatch {
            expected: schema.len(),
            found,
        });
    }

    let strings_len: usize = sides
        .iter()
        .map(|&(bytes, side)| bytes.map_or(0, |b| b.len().saturating_sub(side.fixed_len())))
        .sum();
    let mut buf = Vec::with_capacity(schema.fixed_len() + strings_len);
    buf.resize(schema.fixed_len(), 0);
    let mut idx = 0;
    for (bytes, side) in sides {
        for side_idx in 0..side.len() {
            let column = &schema.columns()[idx];
            let side_type = side.columns()[side_idx].data_type;
            if side_type != column.data_type {
                return Err(ValueError::TypeMismatch {
                    column: column.name.clone(),
                    expected: column.data_type,
                    found: side_type.type_name(),
                });
            }
            let bytes = match bytes {
                Some(bytes) if !is_null(side, bytes, side_idx)? => bytes,
                _ => {
                    if !column.nullable {
                        return Err(ValueError::NullNotAllowed(column.name.clone()));
                    }
                    buf[idx / 8] |= 1 << (idx % 8);
                    idx += 1;
                    continue;
                }
            };
            let (from, at) = (side.fixed_offset(side_idx), schema.fixed_offset(idx));
            match column.data_type {
                DataType::Varchar(_) => {
                    let string = varchar_range(side, bytes, from)?;
                    let offset = buf.len();
                    buf.extend_from_slice(&bytes[string.clone()]);
                    if buf.len() > MAX_TUPLE_SIZE {
                        return Err(ValueError::TupleTooLarge);
                    }
                    buf[at..at + 2].copy_from_slice(&(offset as u16).to_le_bytes());
                    buf[at + 2..at + 4].copy_from_slice(&(string.len() as u16).to_le_bytes());
                }
                DataType::Array => return Err(ValueError::CorruptTuple("ARRAY is not NULL")),
                data_type => {
                    let width = fixed_width(data_type);
                    buf[at..at + width].copy_from_slice(&bytes[from..from + width]);
                }
            }
            idx += 1;
        }
    }
    Ok(buf)
}

fn decode_column(schema: &Schema, bytes: &[u8], idx: usize) -> Result<Value> {
    read_column(schema, bytes, idx).map(ValueRef::to_value)
}

fn read_column<'a>(schema: &Schema, bytes: &'a [u8], idx: usize) -> Result<ValueRef<'a>> {
    if is_null(schema, bytes, idx)? {
        return Ok(ValueRef::Null);
    }
    let column = &schema.columns()[idx];

    let at = schema.fixed_offset(idx);
    Ok(match column.data_type {
        DataType::Int => ValueRef::Int(i32::from_le_bytes(read_array(bytes, at))),
        DataType::BigInt => ValueRef::BigInt(i64::from_le_bytes(read_array(bytes, at))),
        DataType::Double => ValueRef::Double(f64::from_le_bytes(read_array(bytes, at))),
        DataType::Bool => match bytes[at] {
            0 => ValueRef::Bool(false),
            1 => ValueRef::Bool(true),
            _ => return Err(ValueError::CorruptTuple("BOOL is not 0 or 1")),
        },
        DataType::Varchar(_) => {
            let s = std::str::from_utf8(&bytes[varchar_range(schema, bytes, at)?])
                .map_err(|_| ValueError::CorruptTuple("VARCHAR is not UTF-8"))?;
            ValueRef::Varchar(s)
        }
        DataType::Array => return Err(ValueError::CorruptTuple("ARRAY is not NULL")),
    })
}

/// Where the bytes of the VARCHAR whose slot is at `at` are in the tuple.
fn varchar_range(schema: &Schema, bytes: &[u8], at: usize) -> Result<Range<usize>> {
    let offset = usize::from(u16::from_le_bytes(read_array(bytes, at)));
    let len = usize::from(u16::from_le_bytes(read_array(bytes, at + 2)));
    if offset < schema.fixed_len() || offset + len > bytes.len() {
        return Err(ValueError::CorruptTuple("VARCHAR is out of bounds"));
    }
    Ok(offset..offset + len)
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
    fn round_trips_doubles() {
        let schema = Schema::new(vec![
            Column::new("x", DataType::Double, true),
            Column::new("y", DataType::Double, false),
        ]);
        for values in [
            vec![Value::Double(-1.5), Value::Double(1e-300)],
            vec![Value::Null, Value::Double(-0.0)],
        ] {
            let tuple = Tuple::encode(&schema, &values).unwrap();
            assert_eq!(tuple.decode(&schema).unwrap(), values);
        }
        assert!(matches!(
            Tuple::encode(&schema, &[Value::Int(1), Value::Double(1.0)]),
            Err(ValueError::TypeMismatch { .. })
        ));
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
        assert_eq!(tuple.encoded_len(&schema).unwrap(), 0);
    }

    #[test]
    fn encoded_len_ignores_trailing_bytes() {
        let schema = users();
        let mut nulls = row();
        nulls[3] = Value::Null;
        for values in [
            row(),
            nulls,
            vec![
                Value::Int(1),
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Bool(false),
            ],
        ] {
            let tuple = Tuple::encode(&schema, &values).unwrap();
            let len = tuple.len();
            assert_eq!(tuple.encoded_len(&schema).unwrap(), len);

            let mut padded = tuple.into_data();
            padded.resize(len + 9, 0);
            let padded = Tuple::from_bytes(padded);
            assert_eq!(padded.encoded_len(&schema).unwrap(), len);
            assert_eq!(padded.decode(&schema).unwrap(), values);
        }
        let short = Tuple::from_bytes(vec![0; schema.fixed_len() - 1]);
        assert!(short.encoded_len(&schema).is_err());
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

    #[test]
    fn reads_columns_in_place() {
        let schema = users();
        let tuple = Tuple::encode(&schema, &row()).unwrap();
        for (idx, value) in row().iter().enumerate() {
            assert_eq!(tuple.value_ref(&schema, idx).unwrap().to_value(), *value);
        }
        assert!(matches!(
            tuple.value_ref(&schema, 1).unwrap(),
            ValueRef::Varchar("ada")
        ));
    }

    fn orders() -> Schema {
        Schema::new(vec![
            Column::new("item", DataType::Varchar(16), true),
            Column::new("qty", DataType::Int, true),
        ])
    }

    /// The schema of `users` joined with `orders`.
    fn joined() -> Schema {
        let columns = [users().columns(), orders().columns()].concat();
        Schema::new(columns)
    }

    /// Joining copies bytes, and gives what encoding the values would.
    #[test]
    fn joins_two_rows() {
        let (users, orders) = (users(), orders());
        let left = Tuple::encode(&users, &row()).unwrap();
        for right_values in [
            vec![Value::from("pen"), Value::Int(3)],
            vec![Value::Null, Value::Int(0)],
            vec![Value::from(""), Value::Null],
        ] {
            let right = Tuple::encode(&orders, &right_values).unwrap();
            let schema = joined();
            let tuple = Tuple::join(&schema, &left, &users, Some(&right), &orders).unwrap();
            let values: Vec<_> = row().into_iter().chain(right_values).collect();
            assert_eq!(tuple, Tuple::encode(&schema, &values).unwrap());
            assert_eq!(tuple.decode(&schema).unwrap(), values);
        }
    }

    #[test]
    fn joins_with_nulls_for_a_missing_right_row() {
        let (users, orders) = (users(), orders());
        let left = Tuple::encode(&users, &row()).unwrap();
        let schema = joined();
        let tuple = Tuple::join(&schema, &left, &users, None, &orders).unwrap();
        let values: Vec<_> = row()
            .into_iter()
            .chain([Value::Null, Value::Null])
            .collect();
        assert_eq!(tuple.decode(&schema).unwrap(), values);

        let mut columns = schema.columns().to_vec();
        columns[6].nullable = false;
        let not_null = Schema::new(columns);
        assert!(matches!(
            Tuple::join(&not_null, &left, &users, None, &orders),
            Err(ValueError::NullNotAllowed(column)) if column == "qty"
        ));
    }

    #[test]
    fn join_checks_the_schema() {
        let (users, orders) = (users(), orders());
        let left = Tuple::encode(&users, &row()).unwrap();
        assert!(matches!(
            Tuple::join(&users, &left, &users, None, &orders),
            Err(ValueError::ColumnCountMismatch {
                expected: 5,
                found: 7
            })
        ));
        let swapped = Schema::new(orders.columns().iter().rev().cloned().collect());
        assert!(matches!(
            Tuple::join(&joined(), &left, &users, None, &swapped),
            Err(ValueError::TypeMismatch { column, .. }) if column == "item"
        ));
    }
}
