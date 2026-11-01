use std::io::{self, BufReader, Write};
use std::net::{TcpStream, ToSocketAddrs};

use thiserror::Error;

use crate::backend::{self, ErrorFields, Field, Message};
use crate::{TransactionStatus, frontend, invalid, read_message};

#[derive(Debug, Error)]
pub enum ClientError {
    #[error(transparent)]
    Io(#[from] io::Error),

    /// The server refused the connection.
    #[error("{0}")]
    Refused(ErrorFields),

    /// The server asked for a password or another kind of authentication.
    #[error("the server asks for authentication method {0}, which is not supported")]
    Authentication(i32),
}

/// What one statement of a query produced.
#[derive(Debug, PartialEq, Eq)]
pub enum QueryResult {
    Rows {
        fields: Vec<Field>,
        rows: Vec<Vec<Option<String>>>,
        /// Such as `SELECT 2`.
        tag: String,
    },
    /// A statement that returns no rows, by its command tag, such as
    /// `INSERT 0 1`.
    Command(String),
    /// A query with no statements in it.
    Empty,
}

/// Everything the server sent back for a query: a result for each statement
/// that ran, and the error that stopped the rest, if one did.
#[derive(Debug, PartialEq, Eq)]
pub struct Response {
    pub results: Vec<QueryResult>,
    pub error: Option<ErrorFields>,
}

/// A blocking connection that runs simple queries.
pub struct Client {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
    buf: Vec<u8>,
    body: Vec<u8>,
    status: TransactionStatus,
}

impl Client {
    /// Connects as `user` and waits until the server is ready for a query.
    pub fn connect(addr: impl ToSocketAddrs, user: &str) -> Result<Self, ClientError> {
        let writer = TcpStream::connect(addr)?;
        writer.set_nodelay(true)?;
        let mut client = Self {
            reader: BufReader::new(writer.try_clone()?),
            writer,
            buf: Vec::new(),
            body: Vec::new(),
            status: TransactionStatus::Idle,
        };
        frontend::startup(&mut client.buf, &[("user", user)]);
        client.writer.write_all(&client.buf)?;
        loop {
            match client.read()? {
                Message::AuthenticationOk
                | Message::ParameterStatus { .. }
                | Message::BackendKeyData
                | Message::NoticeResponse(_) => {}
                Message::Authentication(code) => return Err(ClientError::Authentication(code)),
                Message::ErrorResponse(error) => return Err(ClientError::Refused(error)),
                Message::ReadyForQuery(status) => {
                    client.status = status;
                    return Ok(client);
                }
                other => return Err(unexpected(&other).into()),
            }
        }
    }

    /// Sends `sql` as one query, which may hold several statements, and
    /// reads everything the server sends back for it.
    pub fn simple_query(&mut self, sql: &str) -> io::Result<Response> {
        self.buf.clear();
        frontend::query(&mut self.buf, sql);
        self.writer.write_all(&self.buf)?;
        let mut response = Response {
            results: Vec::new(),
            error: None,
        };
        let mut rows = None;
        loop {
            match self.read()? {
                Message::RowDescription(fields) => rows = Some((fields, Vec::new())),
                Message::DataRow(row) => match &mut rows {
                    Some((_, rows)) => rows.push(row),
                    None => return Err(invalid("a data row came before its row description")),
                },
                Message::CommandComplete(tag) => {
                    response.results.push(match rows.take() {
                        Some((fields, rows)) => QueryResult::Rows { fields, rows, tag },
                        None => QueryResult::Command(tag),
                    });
                }
                Message::EmptyQueryResponse => response.results.push(QueryResult::Empty),
                Message::ErrorResponse(error) => response.error = Some(error),
                Message::ReadyForQuery(status) => {
                    self.status = status;
                    return Ok(response);
                }
                Message::NoticeResponse(_) | Message::ParameterStatus { .. } => {}
                other => return Err(unexpected(&other)),
            }
        }
    }

    /// Where the session was when the server last became ready.
    pub fn status(&self) -> TransactionStatus {
        self.status
    }

    /// Says goodbye and closes the connection.
    pub fn close(mut self) -> io::Result<()> {
        self.buf.clear();
        frontend::terminate(&mut self.buf);
        self.writer.write_all(&self.buf)
    }

    fn read(&mut self) -> io::Result<Message> {
        match read_message(&mut self.reader, &mut self.body)? {
            Some(tag) => backend::decode(tag, &self.body),
            None => Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "the server closed the connection",
            )),
        }
    }
}

fn unexpected(message: &Message) -> io::Error {
    invalid(format!("unexpected message from the server: {message:?}"))
}
