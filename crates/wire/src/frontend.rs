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
    /// Parse, Bind, Describe, Execute or Close, of the extended query flow,
    /// which isn't supported.
    Extended(u8),
    /// Anything else, which has no place after startup.
    Unexpected(u8),
}

pub fn decode(tag: u8, body: &[u8]) -> io::Result<Message<'_>> {
    Ok(match tag {
        b'Q' => Message::Query(Fields::new(body).cstr()?),
        b'X' => Message::Terminate,
        b'S' => Message::Sync,
        b'H' => Message::Flush,
        b'P' | b'B' | b'D' | b'E' | b'C' => Message::Extended(tag),
        tag => Message::Unexpected(tag),
    })
}
