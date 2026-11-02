//! The Postgres frontend/backend protocol, version 3, as far as HamiltonDB
//! speaks it: startup without authentication, then the simple query flow
//! with every value in text format. Both sides are here: `hamilton-server`
//! encodes backend messages and decodes frontend ones, and [`Client`] does
//! the reverse.
//!
//! Every message after startup is a tag byte, then a big-endian `i32`
//! length that counts itself but not the tag, then the body.

pub mod backend;
mod client;
pub mod frontend;

use std::io::{self, Read};

pub use client::{Client, ClientError, QueryResult, Response};

/// Postgres 3.0, the only protocol version spoken.
pub const PROTOCOL_VERSION: i32 = 3 << 16;

/// Not Postgres: HamiltonDB's own client sends this as a query to learn the
/// tables, since the catalog can't be queried with SQL yet. The reply is
/// two row sets, in table name order:
///
/// - `table, column, type, nullable`: one row per column, in column order.
/// - `index, table, primary_key, columns`: one row per index, in creation
///   order, `columns` being its key columns joined by `", "`.
pub const SCHEMA_REQUEST: &str = "\\schema";

/// The type oids a row description uses, Postgres's own.
pub mod oid {
    pub const BOOL: u32 = 16;
    pub const INT8: u32 = 20;
    pub const INT4: u32 = 23;
    pub const TEXT: u32 = 25;
    pub const FLOAT8: u32 = 701;
    pub const VARCHAR: u32 = 1043;
    pub const TIMESTAMPTZ: u32 = 1184;
}

/// Messages longer than this are refused before anything is allocated for
/// them.
pub const MAX_MESSAGE_LEN: usize = 256 << 20;

/// Where a session is, as each `ReadyForQuery` reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionStatus {
    Idle,
    InBlock,
    /// In a block whose transaction a failed statement aborted.
    Failed,
}

impl TransactionStatus {
    fn byte(self) -> u8 {
        match self {
            TransactionStatus::Idle => b'I',
            TransactionStatus::InBlock => b'T',
            TransactionStatus::Failed => b'E',
        }
    }

    fn from_byte(byte: u8) -> io::Result<Self> {
        match byte {
            b'I' => Ok(TransactionStatus::Idle),
            b'T' => Ok(TransactionStatus::InBlock),
            b'E' => Ok(TransactionStatus::Failed),
            other => Err(invalid(format!("unknown transaction status {other:?}"))),
        }
    }
}

/// Reads the next tagged message into `body`, replacing what it held, and
/// returns its tag. `None` if the stream ended cleanly, between messages.
pub fn read_message(r: &mut impl Read, body: &mut Vec<u8>) -> io::Result<Option<u8>> {
    let mut tag = [0u8; 1];
    if r.read(&mut tag)? == 0 {
        return Ok(None);
    }
    let len = read_len(r)?;
    read_body(r, body, len)?;
    Ok(Some(tag[0]))
}

/// The length that starts a message, less its own four bytes.
fn read_len(r: &mut impl Read) -> io::Result<usize> {
    let mut len = [0u8; 4];
    r.read_exact(&mut len)?;
    let len = i32::from_be_bytes(len);
    match usize::try_from(len) {
        Ok(len) if (4..=MAX_MESSAGE_LEN).contains(&len) => Ok(len - 4),
        _ => Err(invalid(format!("bad message length {len}"))),
    }
}

fn read_body(r: &mut impl Read, body: &mut Vec<u8>, len: usize) -> io::Result<()> {
    body.clear();
    body.resize(len, 0);
    r.read_exact(body)
}

/// Starts a message tagged `tag` in `buf`. Returns where its length goes,
/// for [`end`].
fn begin(buf: &mut Vec<u8>, tag: u8) -> usize {
    buf.push(tag);
    let at = buf.len();
    buf.extend_from_slice(&[0; 4]);
    at
}

/// Fills in the length of the message whose length goes at `at`.
fn end(buf: &mut [u8], at: usize) {
    let len = i32::try_from(buf.len() - at).expect("message fits in an i32 length");
    buf[at..at + 4].copy_from_slice(&len.to_be_bytes());
}

fn put_cstr(buf: &mut Vec<u8>, s: &str) {
    buf.extend_from_slice(s.as_bytes());
    buf.push(0);
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

/// Reads a message body front to back.
struct Fields<'a> {
    rest: &'a [u8],
}

impl<'a> Fields<'a> {
    fn new(body: &'a [u8]) -> Self {
        Self { rest: body }
    }

    fn bytes(&mut self, n: usize) -> io::Result<&'a [u8]> {
        if self.rest.len() < n {
            return Err(invalid("message is truncated"));
        }
        let (bytes, rest) = self.rest.split_at(n);
        self.rest = rest;
        Ok(bytes)
    }

    fn u8(&mut self) -> io::Result<u8> {
        Ok(self.bytes(1)?[0])
    }

    fn i16(&mut self) -> io::Result<i16> {
        Ok(i16::from_be_bytes(self.bytes(2)?.try_into().unwrap()))
    }

    fn i32(&mut self) -> io::Result<i32> {
        Ok(i32::from_be_bytes(self.bytes(4)?.try_into().unwrap()))
    }

    fn cstr(&mut self) -> io::Result<&'a str> {
        let nul = self
            .rest
            .iter()
            .position(|&b| b == 0)
            .ok_or_else(|| invalid("string is not terminated"))?;
        let s =
            std::str::from_utf8(&self.rest[..nul]).map_err(|_| invalid("string is not UTF-8"))?;
        self.rest = &self.rest[nul + 1..];
        Ok(s)
    }

    /// A count, such as of fields, which may not be negative.
    fn count(&mut self) -> io::Result<usize> {
        let n = self.i16()?;
        usize::try_from(n).map_err(|_| invalid(format!("negative count {n}")))
    }
}
