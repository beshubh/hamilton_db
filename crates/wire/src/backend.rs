//! Messages the server sends: encoded by the server, decoded by
//! [`Client`](crate::Client).

use std::fmt;
use std::io::{self, Write};

use crate::{Fields, TransactionStatus, begin, end, invalid, put_cstr};

/// One column of a row description.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field {
    pub name: String,
    /// One of [`oid`](crate::oid).
    pub type_oid: u32,
    /// Bytes per value, or -1 for variable length.
    pub type_size: i16,
    /// Such as a VARCHAR's limit plus 4, or -1 for none.
    pub type_modifier: i32,
}

/// An ErrorResponse or NoticeResponse, as far as its fields are read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrorFields {
    /// `ERROR`, `FATAL`, `NOTICE` and so on.
    pub severity: String,
    /// The five-character SQLSTATE.
    pub code: String,
    pub message: String,
}

impl fmt::Display for ErrorFields {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ErrorFields {}

pub fn authentication_ok(buf: &mut Vec<u8>) {
    let at = begin(buf, b'R');
    buf.extend_from_slice(&0i32.to_be_bytes());
    end(buf, at);
}

pub fn parameter_status(buf: &mut Vec<u8>, name: &str, value: &str) {
    let at = begin(buf, b'S');
    put_cstr(buf, name);
    put_cstr(buf, value);
    end(buf, at);
}

pub fn ready_for_query(buf: &mut Vec<u8>, status: TransactionStatus) {
    let at = begin(buf, b'Z');
    buf.push(status.byte());
    end(buf, at);
}

pub fn row_description(buf: &mut Vec<u8>, fields: &[Field]) {
    let at = begin(buf, b'T');
    let count = i16::try_from(fields.len()).expect("at most i16::MAX columns");
    buf.extend_from_slice(&count.to_be_bytes());
    for field in fields {
        put_cstr(buf, &field.name);
        buf.extend_from_slice(&0i32.to_be_bytes()); // table oid
        buf.extend_from_slice(&0i16.to_be_bytes()); // column number
        buf.extend_from_slice(&field.type_oid.to_be_bytes());
        buf.extend_from_slice(&field.type_size.to_be_bytes());
        buf.extend_from_slice(&field.type_modifier.to_be_bytes());
        buf.extend_from_slice(&0i16.to_be_bytes()); // text format
    }
    end(buf, at);
}

/// A DataRow being written into a buffer, one value at a time.
pub struct DataRow<'a> {
    buf: &'a mut Vec<u8>,
    at: usize,
    count_at: usize,
    count: i16,
}

impl<'a> DataRow<'a> {
    pub fn begin(buf: &'a mut Vec<u8>) -> Self {
        let at = begin(buf, b'D');
        let count_at = buf.len();
        buf.extend_from_slice(&[0; 2]);
        Self {
            buf,
            at,
            count_at,
            count: 0,
        }
    }

    pub fn null(&mut self) {
        self.buf.extend_from_slice(&(-1i32).to_be_bytes());
        self.count += 1;
    }

    /// Writes `value` in text format, as its `Display` prints it.
    pub fn text(&mut self, value: impl fmt::Display) {
        let len_at = self.buf.len();
        self.buf.extend_from_slice(&[0; 4]);
        write!(self.buf, "{value}").expect("writing to a Vec cannot fail");
        let len = i32::try_from(self.buf.len() - len_at - 4).expect("value fits in an i32 length");
        self.buf[len_at..len_at + 4].copy_from_slice(&len.to_be_bytes());
        self.count += 1;
    }

    pub fn finish(self) {
        self.buf[self.count_at..self.count_at + 2].copy_from_slice(&self.count.to_be_bytes());
        end(self.buf, self.at);
    }
}

pub fn command_complete(buf: &mut Vec<u8>, tag: &str) {
    let at = begin(buf, b'C');
    put_cstr(buf, tag);
    end(buf, at);
}

pub fn empty_query_response(buf: &mut Vec<u8>) {
    let at = begin(buf, b'I');
    end(buf, at);
}

pub fn error_response(buf: &mut Vec<u8>, code: &str, message: &str) {
    let at = begin(buf, b'E');
    for (field, value) in [
        (b'S', "ERROR"),
        (b'V', "ERROR"),
        (b'C', code),
        (b'M', message),
    ] {
        buf.push(field);
        put_cstr(buf, value);
    }
    buf.push(0);
    end(buf, at);
}

/// A message the server sent, as the client sees it.
#[derive(Debug, PartialEq, Eq)]
pub enum Message {
    AuthenticationOk,
    /// Any other authentication request, by its code. None is supported.
    Authentication(i32),
    ParameterStatus {
        name: String,
        value: String,
    },
    BackendKeyData,
    ReadyForQuery(TransactionStatus),
    RowDescription(Vec<Field>),
    DataRow(Vec<Option<String>>),
    CommandComplete(String),
    EmptyQueryResponse,
    ErrorResponse(ErrorFields),
    NoticeResponse(ErrorFields),
    /// One the client doesn't read, by its tag.
    Other(u8),
}

pub fn decode(tag: u8, body: &[u8]) -> io::Result<Message> {
    let mut fields = Fields::new(body);
    Ok(match tag {
        b'R' => match fields.i32()? {
            0 => Message::AuthenticationOk,
            code => Message::Authentication(code),
        },
        b'S' => Message::ParameterStatus {
            name: fields.cstr()?.to_owned(),
            value: fields.cstr()?.to_owned(),
        },
        b'K' => Message::BackendKeyData,
        b'Z' => Message::ReadyForQuery(TransactionStatus::from_byte(fields.u8()?)?),
        b'T' => {
            let n = fields.count()?;
            let mut columns = Vec::with_capacity(n);
            for _ in 0..n {
                let name = fields.cstr()?.to_owned();
                fields.bytes(6)?; // table oid, column number
                let type_oid = fields.i32()? as u32;
                let type_size = fields.i16()?;
                let type_modifier = fields.i32()?;
                if fields.i16()? != 0 {
                    return Err(invalid(format!("column {name} is not in text format")));
                }
                columns.push(Field {
                    name,
                    type_oid,
                    type_size,
                    type_modifier,
                });
            }
            Message::RowDescription(columns)
        }
        b'D' => {
            let n = fields.count()?;
            let mut values = Vec::with_capacity(n);
            for _ in 0..n {
                let len = fields.i32()?;
                let value = match usize::try_from(len) {
                    Ok(len) => Some(
                        String::from_utf8(fields.bytes(len)?.to_vec())
                            .map_err(|_| invalid("value is not UTF-8"))?,
                    ),
                    Err(_) if len == -1 => None,
                    Err(_) => return Err(invalid(format!("bad value length {len}"))),
                };
                values.push(value);
            }
            Message::DataRow(values)
        }
        b'C' => Message::CommandComplete(fields.cstr()?.to_owned()),
        b'I' => Message::EmptyQueryResponse,
        b'E' => Message::ErrorResponse(error_fields(&mut fields)?),
        b'N' => Message::NoticeResponse(error_fields(&mut fields)?),
        tag => Message::Other(tag),
    })
}

fn error_fields(fields: &mut Fields) -> io::Result<ErrorFields> {
    let mut error = ErrorFields {
        severity: String::new(),
        code: String::new(),
        message: String::new(),
    };
    loop {
        let field = fields.u8()?;
        if field == 0 {
            return Ok(error);
        }
        let value = fields.cstr()?;
        match field {
            // `V` is never localized, so it wins over `S`.
            b'S' if error.severity.is_empty() => error.severity = value.to_owned(),
            b'V' => error.severity = value.to_owned(),
            b'C' => error.code = value.to_owned(),
            b'M' => error.message = value.to_owned(),
            _ => {}
        }
    }
}
