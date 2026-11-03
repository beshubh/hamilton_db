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
use crate::schema::{Column, Schema};
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
        DataType::Timestamptz => 8,
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

    /// Replaces the tuple's bytes with a copy of `bytes`, keeping its buffer,
    /// and forgets its rid.
    pub fn set_bytes(&mut self, bytes: &[u8]) {
        self.rid = None;
        self.data.clear();
        extend_short(&mut self.data, bytes);
    }

    /// Starts encoding a row of `schema` over this tuple's bytes, one column
    /// at a time, keeping its buffer. Forgets its rid.
    pub fn writer<'a>(&'a mut self, schema: &'a Schema) -> TupleWriter<'a> {
        self.rid = None;
        TupleWriter::new(schema, &mut self.data)
    }

    /// [`join`](Self::join) into this tuple's buffer, replacing its bytes
    /// and forgetting its rid. Its bytes are unspecified if it fails.
    pub fn join_into(
        &mut self,
        schema: &Schema,
        left: &Tuple,
        left_schema: &Schema,
        right: Option<&Tuple>,
        right_schema: &Schema,
    ) -> Result<()> {
        self.rid = None;
        let sides = [
            (Some(left.data.as_slice()), left_schema),
            (right.map(|right| right.data.as_slice()), right_schema),
        ];
        join(&mut self.data, schema, sides)
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
        let mut values = Vec::with_capacity(schema.len());
        for idx in 0..schema.len() {
            values.push(decode_column(schema, &self.data, idx)?);
        }
        Ok(values)
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

    /// [`value_ref`](Self::value_ref) of the tuple whose bytes are `data`,
    /// for callers holding just the bytes, such as a page's.
    pub fn value_ref_in<'a>(data: &'a [u8], schema: &Schema, idx: usize) -> Result<ValueRef<'a>> {
        read_column(schema, data, idx)
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
        let mut tuple = Self::default();
        tuple.join_into(schema, left, left_schema, right, right_schema)?;
        Ok(tuple)
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
        if key_attrs.len() != key_schema.len() {
            return Err(ValueError::ColumnCountMismatch {
                expected: key_schema.len(),
                found: key_attrs.len(),
            });
        }
        // The key's columns are copied over without decoding them.
        let mut key = Self::from_bytes(Vec::with_capacity(key_schema.fixed_len() + self.len()));
        let mut writer = key.writer(key_schema);
        for &idx in key_attrs {
            writer.push_column(self, schema, idx)?;
        }
        writer.finish()?;
        Ok(key)
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
    let mut writer = TupleWriter::new(schema, &mut buf);
    for value in values {
        writer.push(value.as_value_ref())?;
    }
    writer.finish()?;
    Ok(buf)
}

/// Encodes a row into a buffer one column at a time, as [`Tuple::encode`]
/// does, checking each value against its column. Made by [`Tuple::writer`].
/// The buffer's bytes are unspecified until [`finish`](Self::finish)
/// succeeds.
pub struct TupleWriter<'a> {
    schema: &'a Schema,
    buf: &'a mut Vec<u8>,
    /// The column the next value is for.
    idx: usize,
}

impl<'a> TupleWriter<'a> {
    fn new(schema: &'a Schema, buf: &'a mut Vec<u8>) -> Self {
        buf.clear();
        buf.resize(schema.fixed_len(), 0);
        Self {
            schema,
            buf,
            idx: 0,
        }
    }

    /// The next column, or `ColumnCountMismatch` if every column has a value.
    fn next_column(&self) -> Result<&'a Column> {
        self.schema
            .columns()
            .get(self.idx)
            .ok_or(ValueError::ColumnCountMismatch {
                expected: self.schema.len(),
                found: self.idx + 1,
            })
    }

    /// Encodes `value` as the next column.
    pub fn push(&mut self, value: ValueRef<'_>) -> Result<()> {
        let column = self.next_column()?;
        let (idx, at) = (self.idx, self.schema.fixed_offset(self.idx));
        let buf = &mut *self.buf;
        match (column.data_type, value) {
            (_, ValueRef::Null) => {
                if !column.nullable {
                    return Err(ValueError::NullNotAllowed(column.name.clone()));
                }
                buf[idx / 8] |= 1 << (idx % 8);
            }
            (DataType::Int, ValueRef::Int(v)) => buf[at..at + 4].copy_from_slice(&v.to_le_bytes()),
            (DataType::BigInt, ValueRef::BigInt(v)) => {
                buf[at..at + 8].copy_from_slice(&v.to_le_bytes())
            }
            (DataType::Double, ValueRef::Double(v)) => {
                buf[at..at + 8].copy_from_slice(&v.to_le_bytes())
            }
            (DataType::Timestamptz, ValueRef::Timestamptz(v)) => {
                buf[at..at + 8].copy_from_slice(&v.to_le_bytes())
            }
            (DataType::Bool, ValueRef::Bool(v)) => buf[at] = u8::from(v),
            (DataType::Varchar(max), ValueRef::Varchar(s)) => {
                push_varchar(buf, column, max, at, s.as_bytes())?;
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
        self.idx += 1;
        Ok(())
    }

    /// Encodes column `from_idx` of `from`, a row of `from_schema`, as the
    /// next column, copying its bytes without decoding them, as
    /// [`Tuple::join`] does. The two columns must have the same type, and a
    /// VARCHAR must fit the next column's length.
    pub fn push_column(
        &mut self,
        from: &Tuple,
        from_schema: &Schema,
        from_idx: usize,
    ) -> Result<()> {
        let column = self.next_column()?;
        let from_type = from_schema
            .columns()
            .get(from_idx)
            .ok_or(ValueError::ColumnOutOfRange {
                idx: from_idx,
                count: from_schema.len(),
            })?
            .data_type;
        let (idx, at) = (self.idx, self.schema.fixed_offset(self.idx));
        let bytes = from.data.as_slice();
        if is_null(from_schema, bytes, from_idx)? {
            if !column.nullable {
                return Err(ValueError::NullNotAllowed(column.name.clone()));
            }
            self.buf[idx / 8] |= 1 << (idx % 8);
            self.idx += 1;
            return Ok(());
        }
        let from_at = from_schema.fixed_offset(from_idx);
        match (column.data_type, from_type) {
            (DataType::Varchar(max), DataType::Varchar(_)) => {
                let string = varchar_range(from_schema, bytes, from_at)?;
                push_varchar(self.buf, column, max, at, &bytes[string])?;
            }
            (DataType::Array, _) | (_, DataType::Array) => {
                return Err(ValueError::CorruptTuple("ARRAY is not NULL"));
            }
            (to, from) if to == from => {
                let width = fixed_width(to);
                self.buf[at..at + width].copy_from_slice(&bytes[from_at..from_at + width]);
            }
            (to, from) => {
                return Err(ValueError::TypeMismatch {
                    column: column.name.clone(),
                    expected: to,
                    found: from.type_name(),
                });
            }
        }
        self.idx += 1;
        Ok(())
    }

    /// Checks that every column has a value and the row fits in a tuple.
    pub fn finish(self) -> Result<()> {
        if self.idx != self.schema.len() {
            return Err(ValueError::ColumnCountMismatch {
                expected: self.schema.len(),
                found: self.idx,
            });
        }
        if self.buf.len() > MAX_TUPLE_SIZE {
            return Err(ValueError::TupleTooLarge);
        }
        Ok(())
    }
}

/// Joins rows of two schemas into rows of a third, as [`Tuple::join`] does,
/// for an operator that joins many rows of the same schemas. The schemas are
/// checked once, when it is made, so that joining a row copies each side's
/// fixed-width slots as one block and only VARCHARs and the null bitmap are
/// handled column by column.
#[derive(Clone, Debug)]
pub struct TupleJoiner {
    schema: Schema,
    sides: [JoinSide; 2],
}

#[derive(Clone, Debug)]
struct JoinSide {
    schema: Schema,
    /// The output column of the side's first column.
    first: usize,
    /// Where the side's slots go in the output.
    slots_at: usize,
    /// The bits of the last byte of the side's null bitmap that are its
    /// columns'.
    last_bitmap_mask: u8,
    /// For each VARCHAR column: its index on the side and its slot in the
    /// output.
    varchars: Vec<(usize, usize)>,
    /// The side's columns whose output column is NOT NULL.
    not_null: Vec<usize>,
}

impl TupleJoiner {
    /// A joiner for `schema` from `left_schema` and `right_schema`, or `None`
    /// if `Tuple::join` would fail for every row: the columns do not line up.
    pub fn new(schema: &Schema, left_schema: &Schema, right_schema: &Schema) -> Option<Self> {
        if left_schema.len() + right_schema.len() != schema.len() {
            return None;
        }
        let mut slots_at = bitmap_len(schema.len());
        let mut sides = [left_schema, right_schema].map(|side| JoinSide {
            schema: side.clone(),
            first: 0,
            slots_at: 0,
            last_bitmap_mask: match side.len() % 8 {
                0 => u8::MAX,
                bits => (1 << bits) - 1,
            },
            varchars: Vec::new(),
            not_null: Vec::new(),
        });
        let mut idx = 0;
        for side in &mut sides {
            side.first = idx;
            side.slots_at = slots_at;
            for (side_idx, side_column) in side.schema.columns().iter().enumerate() {
                let column = &schema.columns()[idx];
                if side_column.data_type != column.data_type || column.data_type == DataType::Array
                {
                    return None;
                }
                if matches!(column.data_type, DataType::Varchar(_)) {
                    side.varchars.push((side_idx, schema.fixed_offset(idx)));
                }
                if !column.nullable {
                    side.not_null.push(side_idx);
                }
                idx += 1;
            }
            slots_at += side.schema.fixed_len() - bitmap_len(side.schema.len());
        }
        Some(Self {
            schema: schema.clone(),
            sides,
        })
    }

    /// Joins `left` and `right` into `out`, as [`Tuple::join_into`] does.
    #[inline]
    pub fn join_into(&self, out: &mut Tuple, left: &Tuple, right: Option<&Tuple>) -> Result<()> {
        out.rid = None;
        let fixed_len = self.schema.fixed_len();
        let [left_side, right_side] = &self.sides;
        let right = right.map(|right| right.data.as_slice());
        if fixed_len > FIXED_ON_STACK {
            let buf = &mut out.data;
            buf.clear();
            buf.resize(fixed_len, 0);
            left_side.copy_fixed(buf, &self.schema, Some(&left.data))?;
            right_side.copy_fixed(buf, &self.schema, right)?;
        } else {
            // Built on the stack and copied over at once, which beats
            // growing the tuple's buffer a column at a time.
            let mut fixed = [0; FIXED_ON_STACK];
            left_side.copy_fixed(&mut fixed[..fixed_len], &self.schema, Some(&left.data))?;
            right_side.copy_fixed(&mut fixed[..fixed_len], &self.schema, right)?;
            out.data.clear();
            extend_short(&mut out.data, &fixed[..fixed_len]);
        }
        left_side.copy_strings(&mut out.data, Some(&left.data))?;
        right_side.copy_strings(&mut out.data, right)
    }
}

/// Copies chosen columns of rows of one schema into rows of another, giving
/// what [`TupleWriter::push_column`] of each in turn gives, for an operator
/// that projects many rows the same way. The columns are checked once, when
/// it is made.
#[derive(Clone, Debug)]
pub struct TupleProjector {
    schema: Schema,
    input: Schema,
    /// One per output column, in order.
    columns: Vec<ProjectedColumn>,
}

#[derive(Clone, Debug)]
struct ProjectedColumn {
    /// The input column it is copied from, and that column's slot.
    from: usize,
    from_at: usize,
    /// The output column's slot.
    to_at: usize,
    /// The slot's width.
    width: usize,
    /// For a VARCHAR, the output column's length limit.
    max_len: Option<u32>,
}

impl TupleProjector {
    /// A projector of rows of `input` onto `schema`, whose column `i` is
    /// `input`'s column `from[i]`. `None` if writing them would fail for
    /// every row: the counts or the types do not line up.
    pub fn new(schema: &Schema, input: &Schema, from: &[usize]) -> Option<Self> {
        if from.len() != schema.len() {
            return None;
        }
        let mut columns = Vec::with_capacity(from.len());
        for (idx, (column, &from)) in schema.columns().iter().zip(from).enumerate() {
            let from_type = input.column(from)?.data_type;
            let max_len = match (column.data_type, from_type) {
                (DataType::Varchar(max), DataType::Varchar(_)) => Some(max),
                (DataType::Array, _) | (_, DataType::Array) => return None,
                (to, from) if to == from => None,
                _ => return None,
            };
            columns.push(ProjectedColumn {
                from,
                from_at: input.fixed_offset(from),
                to_at: schema.fixed_offset(idx),
                width: fixed_width(column.data_type),
                max_len,
            });
        }
        Some(Self {
            schema: schema.clone(),
            input: input.clone(),
            columns,
        })
    }

    /// Writes the projection of `input` into `out`, replacing its bytes and
    /// forgetting its rid. Its bytes are unspecified if it fails.
    pub fn project_into(&self, out: &mut Tuple, input: &Tuple) -> Result<()> {
        out.rid = None;
        let fixed_len = self.schema.fixed_len();
        let bytes = input.data.as_slice();
        if !self.columns.is_empty() && bytes.len() < self.input.fixed_len() {
            return Err(ValueError::CorruptTuple(
                "shorter than its fixed-width part",
            ));
        }
        let buf = &mut out.data;
        buf.clear();
        buf.resize(fixed_len, 0);
        for (idx, column) in self.columns.iter().enumerate() {
            if bytes[column.from / 8] & (1 << (column.from % 8)) != 0 {
                let out_column = &self.schema.columns()[idx];
                if !out_column.nullable {
                    return Err(ValueError::NullNotAllowed(out_column.name.clone()));
                }
                buf[idx / 8] |= 1 << (idx % 8);
                continue;
            }
            let at = column.to_at;
            match column.max_len {
                Some(max) => {
                    let string = varchar_range(&self.input, bytes, column.from_at)?;
                    if string.len() > max as usize {
                        return Err(ValueError::ValueTooLong {
                            column: self.schema.columns()[idx].name.clone(),
                            len: string.len(),
                            max,
                        });
                    }
                    let offset = buf.len();
                    extend_short(buf, &bytes[string.clone()]);
                    if buf.len() > MAX_TUPLE_SIZE {
                        return Err(ValueError::TupleTooLarge);
                    }
                    buf[at..at + 2].copy_from_slice(&(offset as u16).to_le_bytes());
                    buf[at + 2..at + 4].copy_from_slice(&(string.len() as u16).to_le_bytes());
                }
                None => {
                    let from = &bytes[column.from_at..column.from_at + column.width];
                    copy_small(&mut buf[at..at + column.width], from);
                }
            }
        }
        Ok(())
    }
}

/// The largest fixed-width part [`TupleJoiner`] builds on the stack.
const FIXED_ON_STACK: usize = 128;

impl JoinSide {
    /// Writes the side's slots and null bits from `bytes` into `fixed`, the
    /// output's fixed-width part, zeroed; a side without bytes is all NULLs.
    #[inline(always)]
    fn copy_fixed(&self, fixed: &mut [u8], out: &Schema, bytes: Option<&[u8]>) -> Result<()> {
        let schema = &self.schema;
        let Some(bytes) = bytes else {
            if let Some(&idx) = self.not_null.first() {
                let column = &out.columns()[self.first + idx];
                return Err(ValueError::NullNotAllowed(column.name.clone()));
            }
            for idx in self.first..self.first + schema.len() {
                fixed[idx / 8] |= 1 << (idx % 8);
            }
            return Ok(());
        };
        if bytes.len() < schema.fixed_len() {
            return Err(ValueError::CorruptTuple(
                "shorter than its fixed-width part",
            ));
        }
        let side_bitmap = bitmap_len(schema.len());
        let slots = &bytes[side_bitmap..schema.fixed_len()];
        copy_small(
            &mut fixed[self.slots_at..self.slots_at + slots.len()],
            slots,
        );
        // The side's bitmap, shifted to where its columns are in the output.
        let shift = self.first % 8;
        for (byte_idx, &byte) in bytes[..side_bitmap].iter().enumerate() {
            let nulls = if byte_idx + 1 == side_bitmap {
                byte & self.last_bitmap_mask
            } else {
                byte
            };
            if nulls == 0 {
                continue;
            }
            let at = self.first / 8 + byte_idx;
            fixed[at] |= nulls << shift;
            if shift != 0 && nulls >> (8 - shift) != 0 {
                fixed[at + 1] |= nulls >> (8 - shift);
            }
        }
        for &idx in &self.not_null {
            if bytes[idx / 8] & (1 << (idx % 8)) != 0 {
                let column = &out.columns()[self.first + idx];
                return Err(ValueError::NullNotAllowed(column.name.clone()));
            }
        }
        Ok(())
    }

    /// Appends the side's VARCHARs from `bytes` to `buf`, pointing their
    /// slots at them.
    #[inline(always)]
    fn copy_strings(&self, buf: &mut Vec<u8>, bytes: Option<&[u8]>) -> Result<()> {
        let Some(bytes) = bytes else {
            return Ok(());
        };
        if self.varchars.is_empty() {
            return Ok(());
        }
        for &(idx, at) in &self.varchars {
            if bytes[idx / 8] & (1 << (idx % 8)) != 0 {
                continue;
            }
            let string = varchar_range(&self.schema, bytes, self.schema.fixed_offset(idx))?;
            let offset = buf.len();
            extend_short(buf, &bytes[string.clone()]);
            if buf.len() > MAX_TUPLE_SIZE {
                return Err(ValueError::TupleTooLarge);
            }
            buf[at..at + 2].copy_from_slice(&(offset as u16).to_le_bytes());
            buf[at + 2..at + 4].copy_from_slice(&(string.len() as u16).to_le_bytes());
        }
        Ok(())
    }
}

/// `buf.extend_from_slice(src)`, with a short `src`, as most strings and
/// rows are, copied as a fixed-size block, which beats calling `memcpy`.
#[inline(always)]
fn extend_short(buf: &mut Vec<u8>, src: &[u8]) {
    match src.len() {
        0..=16 => extend_block::<16>(buf, src),
        17..=32 => extend_block::<32>(buf, src),
        33..=64 => extend_block::<64>(buf, src),
        _ => buf.extend_from_slice(src),
    }
}

#[inline(always)]
fn extend_block<const N: usize>(buf: &mut Vec<u8>, src: &[u8]) {
    // Writing the whole block would grow a buffer sized for `src` exactly.
    if buf.capacity() - buf.len() < N {
        buf.extend_from_slice(src);
        return;
    }
    let mut block = [0; N];
    copy_small(&mut block[..src.len()], src);
    let end = buf.len() + src.len();
    buf.extend_from_slice(&block);
    buf.truncate(end);
}

/// `dst.copy_from_slice(src)`, with the short copies a row's slots take
/// done as two overlapping fixed-size moves instead of a call to `memcpy`.
#[inline(always)]
fn copy_small(dst: &mut [u8], src: &[u8]) {
    let len = src.len();
    assert_eq!(dst.len(), len);
    match len {
        0 => {}
        1..=3 => {
            dst[0] = src[0];
            dst[len / 2] = src[len / 2];
            dst[len - 1] = src[len - 1];
        }
        4..=8 => {
            let head: [u8; 4] = src[..4].try_into().unwrap();
            let tail: [u8; 4] = src[len - 4..].try_into().unwrap();
            dst[..4].copy_from_slice(&head);
            dst[len - 4..].copy_from_slice(&tail);
        }
        9..=16 => {
            let head: [u8; 8] = src[..8].try_into().unwrap();
            let tail: [u8; 8] = src[len - 8..].try_into().unwrap();
            dst[..8].copy_from_slice(&head);
            dst[len - 8..].copy_from_slice(&tail);
        }
        17..=32 => {
            let head: [u8; 16] = src[..16].try_into().unwrap();
            let tail: [u8; 16] = src[len - 16..].try_into().unwrap();
            dst[..16].copy_from_slice(&head);
            dst[len - 16..].copy_from_slice(&tail);
        }
        33..=64 => {
            let head: [u8; 32] = src[..32].try_into().unwrap();
            let tail: [u8; 32] = src[len - 32..].try_into().unwrap();
            dst[..32].copy_from_slice(&head);
            dst[len - 32..].copy_from_slice(&tail);
        }
        _ => dst.copy_from_slice(src),
    }
}

/// Appends `s` to `buf` as the VARCHAR of `column`, whose slot is at `at`.
fn push_varchar(buf: &mut Vec<u8>, column: &Column, max: u32, at: usize, s: &[u8]) -> Result<()> {
    if s.len() > max as usize {
        return Err(ValueError::ValueTooLong {
            column: column.name.clone(),
            len: s.len(),
            max,
        });
    }
    let offset = buf.len();
    extend_short(buf, s);
    // Checked before the casts below, which it makes lossless.
    if buf.len() > MAX_TUPLE_SIZE {
        return Err(ValueError::TupleTooLarge);
    }
    buf[at..at + 2].copy_from_slice(&(offset as u16).to_le_bytes());
    buf[at + 2..at + 4].copy_from_slice(&(s.len() as u16).to_le_bytes());
    Ok(())
}

/// Lays out the columns of each side one after another in `buf`, copying
/// each column's slot and, for a VARCHAR, its bytes, which get a new offset.
/// A side without bytes is all NULLs.
fn join(buf: &mut Vec<u8>, schema: &Schema, sides: [(Option<&[u8]>, &Schema); 2]) -> Result<()> {
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
    buf.clear();
    buf.reserve(schema.fixed_len() + strings_len);
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
    Ok(())
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
        DataType::Timestamptz => ValueRef::Timestamptz(i64::from_le_bytes(read_array(bytes, at))),
        DataType::Bool => match bytes[at] {
            0 => ValueRef::Bool(false),
            1 => ValueRef::Bool(true),
            _ => return Err(ValueError::CorruptTuple("BOOL is not 0 or 1")),
        },
        DataType::Varchar(_) => ValueRef::Varchar(utf8(&bytes[varchar_range(schema, bytes, at)?])?),
        DataType::Array => return Err(ValueError::CorruptTuple("ARRAY is not NULL")),
    })
}

/// Reads one column of a schema's tuples in place, as
/// [`Tuple::value_ref`] does, with the column's position in the tuple found
/// once instead of on every read.
#[derive(Clone, Copy, Debug)]
pub struct ColumnReader {
    data_type: DataType,
    /// The byte of the null bitmap with the column's bit, and the bit.
    null_byte: usize,
    null_bit: u8,
    /// Where the column's slot is.
    at: usize,
    /// Where the strings start: the schema's fixed-width part ends.
    fixed_len: usize,
}

impl ColumnReader {
    /// A reader of column `idx` of `schema`, or `None` if it has no such
    /// column.
    pub fn new(schema: &Schema, idx: usize) -> Option<Self> {
        let column = schema.column(idx)?;
        Some(Self {
            data_type: column.data_type,
            null_byte: idx / 8,
            null_bit: 1 << (idx % 8),
            at: schema.fixed_offset(idx),
            fixed_len: schema.fixed_len(),
        })
    }

    pub fn data_type(&self) -> DataType {
        self.data_type
    }

    /// For an INT or BIGINT column: whether it is NULL in `tuple`, and its
    /// slot as a BIGINT, read without branching on either, so that a loop
    /// over many rows can test them without mispredicting. Fails as
    /// [`read`](Self::read) does, and for a column of another type.
    #[inline]
    pub fn read_integer(&self, tuple: &Tuple) -> Result<(bool, i64)> {
        let bytes = tuple.data.as_slice();
        if bytes.len() < self.fixed_len {
            return Err(ValueError::CorruptTuple(
                "shorter than its fixed-width part",
            ));
        }
        let null = bytes[self.null_byte] & self.null_bit != 0;
        let value = match self.data_type {
            DataType::Int => i64::from(i32::from_le_bytes(read_array(bytes, self.at))),
            DataType::BigInt => i64::from_le_bytes(read_array(bytes, self.at)),
            data_type => {
                return Err(ValueError::InvalidOperand {
                    op: "read_integer",
                    operand: data_type.type_name(),
                });
            }
        };
        Ok((null, value))
    }

    /// Whether the column of `tuple` is `value`, an INT, BIGINT or VARCHAR of
    /// the column's own type and not NULL, comparing their bytes. Unlike
    /// [`read`](Self::read), a VARCHAR's UTF-8 is not checked: for callers
    /// that read the column with `read` before, as a hash table does when it
    /// hashes a row. `None` for other types, which the caller compares itself.
    #[inline]
    pub fn equals_raw(&self, tuple: &Tuple, value: ValueRef<'_>) -> Result<Option<bool>> {
        let bytes = tuple.data.as_slice();
        if bytes.len() < self.fixed_len {
            return Err(ValueError::CorruptTuple(
                "shorter than its fixed-width part",
            ));
        }
        let null = bytes[self.null_byte] & self.null_bit != 0;
        let at = self.at;
        Ok(Some(match (self.data_type, value) {
            (DataType::Int, ValueRef::Int(v)) => {
                !null && i32::from_le_bytes(read_array(bytes, at)) == v
            }
            (DataType::BigInt, ValueRef::BigInt(v)) => {
                !null && i64::from_le_bytes(read_array(bytes, at)) == v
            }
            (DataType::Varchar(_), ValueRef::Varchar(v)) => {
                if null {
                    return Ok(Some(false));
                }
                let offset = usize::from(u16::from_le_bytes(read_array(bytes, at)));
                let len = usize::from(u16::from_le_bytes(read_array(bytes, at + 2)));
                if offset < self.fixed_len || offset + len > bytes.len() {
                    return Err(ValueError::CorruptTuple("VARCHAR is out of bounds"));
                }
                crate::value::bytes_equal(&bytes[offset..offset + len], v.as_bytes())
            }
            _ => return Ok(None),
        }))
    }

    /// The column of `tuple`, a row of the reader's schema.
    #[inline]
    pub fn read<'a>(&self, tuple: &'a Tuple) -> Result<ValueRef<'a>> {
        self.read_in(&tuple.data)
    }

    /// Orders the column of two rows of the reader's schema, whose bytes are
    /// `a` and `b`, with NULL first: what reading both and comparing them
    /// gives, without building the values for INT, BIGINT and VARCHAR.
    #[inline]
    pub fn compare_in(&self, a: &[u8], b: &[u8]) -> Result<std::cmp::Ordering> {
        if a.len() < self.fixed_len || b.len() < self.fixed_len {
            return Err(ValueError::CorruptTuple(
                "shorter than its fixed-width part",
            ));
        }
        let a_null = a[self.null_byte] & self.null_bit != 0;
        let b_null = b[self.null_byte] & self.null_bit != 0;
        if a_null || b_null {
            return Ok(b_null.cmp(&a_null));
        }
        let at = self.at;
        Ok(match self.data_type {
            DataType::Int => {
                i32::from_le_bytes(read_array(a, at)).cmp(&i32::from_le_bytes(read_array(b, at)))
            }
            DataType::BigInt => {
                i64::from_le_bytes(read_array(a, at)).cmp(&i64::from_le_bytes(read_array(b, at)))
            }
            DataType::Varchar(_) => {
                let (a, b) = (self.varchar_bytes_in(a)?, self.varchar_bytes_in(b)?);
                match compare_short_ascii(a, b) {
                    Some(order) => order,
                    None => {
                        utf8(a)?;
                        utf8(b)?;
                        a.cmp(b)
                    }
                }
            }
            _ => self
                .read_in(a)?
                .compare(self.read_in(b)?)?
                .expect("neither value is NULL"),
        })
    }

    /// The bytes of the VARCHAR column, not NULL, in `bytes`, with their
    /// bounds checked but not their UTF-8.
    #[inline]
    fn varchar_bytes_in<'a>(&self, bytes: &'a [u8]) -> Result<&'a [u8]> {
        let offset = usize::from(u16::from_le_bytes(read_array(bytes, self.at)));
        let len = usize::from(u16::from_le_bytes(read_array(bytes, self.at + 2)));
        if offset < self.fixed_len || offset + len > bytes.len() {
            return Err(ValueError::CorruptTuple("VARCHAR is out of bounds"));
        }
        Ok(&bytes[offset..offset + len])
    }

    /// [`read`](Self::read) of the tuple whose bytes are `bytes`.
    #[inline]
    pub fn read_in<'a>(&self, bytes: &'a [u8]) -> Result<ValueRef<'a>> {
        if bytes.len() < self.fixed_len {
            return Err(ValueError::CorruptTuple(
                "shorter than its fixed-width part",
            ));
        }
        if bytes[self.null_byte] & self.null_bit != 0 {
            return Ok(ValueRef::Null);
        }
        let at = self.at;
        Ok(match self.data_type {
            DataType::Int => ValueRef::Int(i32::from_le_bytes(read_array(bytes, at))),
            DataType::BigInt => ValueRef::BigInt(i64::from_le_bytes(read_array(bytes, at))),
            DataType::Double => ValueRef::Double(f64::from_le_bytes(read_array(bytes, at))),
            DataType::Timestamptz => {
                ValueRef::Timestamptz(i64::from_le_bytes(read_array(bytes, at)))
            }
            DataType::Bool => match bytes[at] {
                0 => ValueRef::Bool(false),
                1 => ValueRef::Bool(true),
                _ => return Err(ValueError::CorruptTuple("BOOL is not 0 or 1")),
            },
            DataType::Varchar(_) => {
                let offset = usize::from(u16::from_le_bytes(read_array(bytes, at)));
                let len = usize::from(u16::from_le_bytes(read_array(bytes, at + 2)));
                if offset < self.fixed_len || offset + len > bytes.len() {
                    return Err(ValueError::CorruptTuple("VARCHAR is out of bounds"));
                }
                ValueRef::Varchar(utf8(&bytes[offset..offset + len])?)
            }
            DataType::Array => return Err(ValueError::CorruptTuple("ARRAY is not NULL")),
        })
    }
}

/// Orders `a` and `b` bytewise if both are ASCII and at most 16 bytes, as
/// most keys are, comparing them as zero-padded big-endian words and then
/// by length, which gives the same order without a call to `memcmp`. `None`
/// otherwise.
#[inline]
fn compare_short_ascii(a: &[u8], b: &[u8]) -> Option<std::cmp::Ordering> {
    if a.len() > 16 || b.len() > 16 {
        return None;
    }
    let words = |s: &[u8]| {
        let mut padded = [0; 16];
        copy_small(&mut padded[..s.len()], s);
        let (high, low) = padded.split_at(8);
        (
            u64::from_be_bytes(high.try_into().unwrap()),
            u64::from_be_bytes(low.try_into().unwrap()),
        )
    };
    let ((a_high, a_low), (b_high, b_low)) = (words(a), words(b));
    if (a_high | a_low | b_high | b_low) & 0x8080_8080_8080_8080 != 0 {
        return None;
    }
    Some(
        (a_high, a_low)
            .cmp(&(b_high, b_low))
            .then(a.len().cmp(&b.len())),
    )
}

/// `bytes` as a string. Checking for ASCII first is much quicker than full
/// UTF-8 validation for the short strings rows mostly hold.
#[inline]
fn utf8(bytes: &[u8]) -> Result<&str> {
    if bytes.is_ascii() {
        // SAFETY: ASCII is valid UTF-8.
        return Ok(unsafe { std::str::from_utf8_unchecked(bytes) });
    }
    std::str::from_utf8(bytes).map_err(|_| ValueError::CorruptTuple("VARCHAR is not UTF-8"))
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
    fn round_trips_timestamps() {
        let schema = Schema::new(vec![
            Column::new("at", DataType::Timestamptz, true),
            Column::new("id", DataType::Int, false),
        ]);
        for row in [
            vec![Value::Timestamptz(1_793_616_660_123_456), Value::Int(1)],
            vec![Value::Timestamptz(-1), Value::Int(2)],
            vec![Value::Null, Value::Int(3)],
        ] {
            let tuple = Tuple::encode(&schema, &row).unwrap();
            assert_eq!(tuple.decode(&schema).unwrap(), row);
        }
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
