//! Messages the client sends: encoded by [`Client`](crate::Client),
//! decoded by the server.

use std::io::{self, Read};

use crate::{Fields, PROTOCOL_VERSION, begin, end, invalid, put_cstr, read_body, read_len};

const SSL_REQUEST: i32 = 80_877_103;
const GSSENC_REQUEST: i32 = 80_877_104;
const CANCEL_REQUEST: i32 = 80_877_102;

/// A startup packet is untagged and small.
const MAX_STARTUP_LEN: usize = 10_000;

/// The first message on a connection, which has no tag.
#[derive(Debug, PartialEq, Eq)]
pub enum Startup {
    /// A session's start, with its parameters, such as `user`.
    Params(Vec<(String, String)>),
    /// Asks for TLS. Answered with `N` for no; the real startup follows.
    SslRequest,
    /// Asks for GSSAPI encryption, answered as `SslRequest` is.
    GssEncRequest,
    /// Asks to cancel another connection's query.
    Cancel,
    /// A protocol version other than 3.0.
    Unsupported(i32),
}

pub fn read_startup(r: &mut impl Read) -> io::Result<Startup> {
    let len = read_len(r)?;
    if len > MAX_STARTUP_LEN {
        return Err(invalid(format!(
            "startup packet of {len} bytes is too long"
        )));
    }
    let mut body = Vec::new();
    read_body(r, &mut body, len)?;
    let mut fields = Fields::new(&body);
    let code = fields.i32()?;
    Ok(match code {
        SSL_REQUEST => Startup::SslRequest,
        GSSENC_REQUEST => Startup::GssEncRequest,
        CANCEL_REQUEST => Startup::Cancel,
        PROTOCOL_VERSION => {
            let mut params = Vec::new();
            loop {
                let name = fields.cstr()?;
                if name.is_empty() {
                    break;
                }
                params.push((name.to_owned(), fields.cstr()?.to_owned()));
            }
            Startup::Params(params)
        }
        version => Startup::Unsupported(version),
    })
}

pub fn startup(buf: &mut Vec<u8>, params: &[(&str, &str)]) {
    let at = buf.len();
    buf.extend_from_slice(&[0; 4]);
    buf.extend_from_slice(&PROTOCOL_VERSION.to_be_bytes());
    for (name, value) in params {
        put_cstr(buf, name);
        put_cstr(buf, value);
    }
    buf.push(0);
    end(buf, at);
}

pub fn query(buf: &mut Vec<u8>, sql: &str) {
    let at = begin(buf, b'Q');
    put_cstr(buf, sql);
    end(buf, at);
}

pub fn terminate(buf: &mut Vec<u8>) {
    let at = begin(buf, b'X');
    end(buf, at);
}

/// A message after startup, as the server sees it.
#[derive(Debug, PartialEq, Eq)]
pub enum Message<'a> {
    Query(&'a str),
    Terminate,
    /// Ends a run of extended query messages.
    Sync,
    Flush,
    Parse(Parse<'a>),
    Bind(Bind<'a>),
    Describe(Target<'a>),
    Execute {
        portal: &'a str,
        /// Rows to return before suspending; 0 for all.
        max_rows: i32,
    },
    Close(Target<'a>),
    /// Anything else, which has no place after startup.
    Unexpected(u8),
}

/// Prepares `query` as the statement `name`, empty for the unnamed one.
#[derive(Debug, PartialEq, Eq)]
pub struct Parse<'a> {
    pub name: &'a str,
    pub query: &'a str,
    /// The type oid of each parameter the client fixed, 0 where it left
    /// the type to the server.
    pub parameter_types: Vec<u32>,
}

/// Makes the portal `portal` from the statement `statement` with
/// parameter values.
#[derive(Debug, PartialEq, Eq)]
pub struct Bind<'a> {
    pub portal: &'a str,
    pub statement: &'a str,
    /// The parameters' formats as sent: none for all text, one for all,
    /// or one per parameter; 0 is text and 1 binary.
    pub parameter_formats: Vec<i16>,
    /// `None` for NULL.
    pub parameters: Vec<Option<&'a [u8]>>,
    /// The result columns' formats, as `parameter_formats` are given.
    pub result_formats: Vec<i16>,
}

/// A prepared statement or portal, by name.
#[derive(Debug, PartialEq, Eq)]
pub enum Target<'a> {
    Statement(&'a str),
    Portal(&'a str),
}

/// The format of the `i`-th of `n` values, given `formats` as Bind sends
/// them. Fails on a count or code Postgres refuses.
pub fn format_of(formats: &[i16], i: usize, n: usize) -> io::Result<Format> {
    let code = match formats {
        [] => 0,
        [code] => *code,
        codes if codes.len() == n => codes[i],
        codes => {
            return Err(invalid(format!(
                "{} format codes for {n} values",
                codes.len()
            )));
        }
    };
    match code {
        0 => Ok(Format::Text),
        1 => Ok(Format::Binary),
        code => Err(invalid(format!("unknown format code {code}"))),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Text,
    Binary,
}

pub fn decode(tag: u8, body: &[u8]) -> io::Result<Message<'_>> {
    let mut fields = Fields::new(body);
    Ok(match tag {
        b'Q' => Message::Query(fields.cstr()?),
        b'X' => Message::Terminate,
        b'S' => Message::Sync,
        b'H' => Message::Flush,
        b'P' => {
            let name = fields.cstr()?;
            let query = fields.cstr()?;
            let n = fields.count()?;
            let parameter_types = (0..n)
                .map(|_| fields.i32().map(|oid| oid as u32))
                .collect::<io::Result<_>>()?;
            Message::Parse(Parse {
                name,
                query,
                parameter_types,
            })
        }
        b'B' => {
            let portal = fields.cstr()?;
            let statement = fields.cstr()?;
            let n = fields.count()?;
            let parameter_formats = (0..n).map(|_| fields.i16()).collect::<io::Result<_>>()?;
            let n = fields.count()?;
            let mut parameters = Vec::with_capacity(n);
            for _ in 0..n {
                let len = fields.i32()?;
                parameters.push(match usize::try_from(len) {
                    Ok(len) => Some(fields.bytes(len)?),
                    Err(_) if len == -1 => None,
                    Err(_) => return Err(invalid(format!("bad parameter length {len}"))),
                });
            }
            let n = fields.count()?;
            let result_formats = (0..n).map(|_| fields.i16()).collect::<io::Result<_>>()?;
            Message::Bind(Bind {
                portal,
                statement,
                parameter_formats,
                parameters,
                result_formats,
            })
        }
        b'D' => Message::Describe(target(&mut fields)?),
        b'E' => Message::Execute {
            portal: fields.cstr()?,
            max_rows: fields.i32()?,
        },
        b'C' => Message::Close(target(&mut fields)?),
        tag => Message::Unexpected(tag),
    })
}

fn target<'a>(fields: &mut Fields<'a>) -> io::Result<Target<'a>> {
    let kind = fields.u8()?;
    let name = fields.cstr()?;
    match kind {
        b'S' => Ok(Target::Statement(name)),
        b'P' => Ok(Target::Portal(name)),
        kind => Err(invalid(format!(
            "unknown describe or close target {:?}",
            kind as char
        ))),
    }
}
