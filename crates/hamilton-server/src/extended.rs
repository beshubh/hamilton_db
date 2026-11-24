//! The extended query flow: statements prepared once with Parse, bound to
//! parameter values as portals with Bind, and run with Execute.
//!
//! A portal binds its values into the statement as constants before it is
//! planned, so `WHERE key = $1` plans an index scan just as a literal would.

use std::collections::HashMap;
use std::mem;
use std::sync::Arc;
use std::vec;

use common::{Column, DataType, Value, timestamp};
use parser::Statement;
use planner::{Output, TransactionStatus};
use wire::backend::{self, Field};
use wire::frontend::{self, Bind, Format, Message, Parse, Target};
use wire::oid;

use crate::connection::{command_tag, data_row, field, sqlstate, type_oid};
use crate::database::{Error, Session};

/// Microseconds from the Unix epoch to 2000-01-01, which binary timestamps
/// count from.
pub(crate) const POSTGRES_EPOCH: i64 = 946_684_800_000_000;

/// An error to answer an extended query message with.
pub(crate) struct Failure {
    pub code: &'static str,
    pub message: String,
}

impl Failure {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

impl From<Error> for Failure {
    fn from(err: Error) -> Self {
        Self::new(sqlstate(&err), err.to_string())
    }
}

impl From<std::io::Error> for Failure {
    fn from(err: std::io::Error) -> Self {
        Self::new("08P01", err.to_string()) // protocol_violation
    }
}

/// A connection's prepared statements and portals, by name; the empty name
/// is the unnamed one, which each Parse or Bind replaces.
#[derive(Default)]
pub(crate) struct Extended {
    statements: HashMap<String, Arc<Prepared>>,
    portals: HashMap<String, Portal>,
}

struct Prepared {
    /// `None` for an empty query.
    statement: Option<Statement>,
    /// The type oid of each parameter, which its values are read as.
    parameter_types: Vec<u32>,
    /// The columns of the rows it produces, if it produces rows.
    columns: Option<Vec<Column>>,
}

struct Portal {
    prepared: Arc<Prepared>,
    parameters: Vec<Value>,
    /// Whether each result column is sent in binary format.
    binary: Vec<bool>,
    state: PortalState,
}

enum PortalState {
    Ready,
    /// Stopped at Execute's row limit with these rows left.
    Suspended {
        command: &'static str,
        rows: vec::IntoIter<Vec<Value>>,
    },
    /// Run to the end; executing it again reports this tag.
    Done(String),
}

impl Extended {
    pub(crate) fn handle(
        &mut self,
        session: &mut Session,
        message: Message,
        out: &mut Vec<u8>,
    ) -> Result<(), Failure> {
        match message {
            Message::Parse(parse) => self.parse(session, parse, out),
            Message::Bind(bind) => self.bind(bind, out),
            Message::Describe(target) => self.describe(target, out),
            Message::Execute { portal, max_rows } => self.execute(session, portal, max_rows, out),
            Message::Close(target) => {
                match target {
                    Target::Statement(name) => self.statements.remove(name).map(drop),
                    Target::Portal(name) => self.portals.remove(name).map(drop),
                };
                backend::close_complete(out);
                Ok(())
            }
            _ => unreachable!("not an extended query message"),
        }
    }

    /// Portals last until the transaction they were made in ends.
    pub(crate) fn sync(&mut self, session: &Session) {
        if session.status() == TransactionStatus::Idle {
            self.portals.clear();
        }
    }

    fn parse(
        &mut self,
        session: &mut Session,
        parse: Parse,
        out: &mut Vec<u8>,
    ) -> Result<(), Failure> {
        if !parse.name.is_empty() && self.statements.contains_key(parse.name) {
            return Err(Failure::new(
                "42P05", // duplicate_prepared_statement
                format!("prepared statement \"{}\" already exists", parse.name),
            ));
        }
        let given = parse
            .parameter_types
            .iter()
            .map(|&oid| parameter_type(oid))
            .collect::<Result<Vec<_>, _>>()?;
        let prepared = match session.prepare(parse.query, &given)? {
            None => Prepared {
                statement: None,
                parameter_types: parse.parameter_types.clone(),
                columns: None,
            },
            Some((statement, described)) => {
                let parameter_types = described
                    .parameters
                    .iter()
                    .zip(given.iter().map(Some).chain(std::iter::repeat(None)))
                    .enumerate()
                    .map(|(i, (inferred, given))| match given {
                        Some(Some(_)) => parse.parameter_types[i],
                        _ => inferred.map_or(oid::TEXT, type_oid),
                    })
                    .collect();
                Prepared {
                    statement: Some(statement),
                    parameter_types,
                    columns: described.columns,
                }
            }
        };
        self.statements
            .insert(parse.name.to_owned(), Arc::new(prepared));
        backend::parse_complete(out);
        Ok(())
    }

    fn bind(&mut self, bind: Bind, out: &mut Vec<u8>) -> Result<(), Failure> {
        let prepared = self.statements.get(bind.statement).ok_or_else(|| {
            Failure::new(
                "26000", // invalid_sql_statement_name
                format!("prepared statement \"{}\" does not exist", bind.statement),
            )
        })?;
        if !bind.portal.is_empty() && self.portals.contains_key(bind.portal) {
            return Err(Failure::new(
                "42P03", // duplicate_cursor
                format!("portal \"{}\" already exists", bind.portal),
            ));
        }
        let n = prepared.parameter_types.len();
        if bind.parameters.len() != n {
            return Err(Failure::new(
                "08P01",
                format!(
                    "bind message supplies {} parameters, but prepared statement \"{}\" requires {n}",
                    bind.parameters.len(),
                    bind.statement
                ),
            ));
        }
        let mut parameters = Vec::with_capacity(n);
        for (i, (bytes, &oid)) in bind
            .parameters
            .iter()
            .zip(&prepared.parameter_types)
            .enumerate()
        {
            let format = frontend::format_of(&bind.parameter_formats, i, n)?;
            parameters.push(match bytes {
                None => Value::Null,
                Some(bytes) => read_parameter(i + 1, oid, format, bytes)?,
            });
        }
        let width = prepared.columns.as_ref().map_or(0, Vec::len);
        let binary = (0..width)
            .map(|i| {
                frontend::format_of(&bind.result_formats, i, width).map(|f| f == Format::Binary)
            })
            .collect::<Result<_, _>>()?;
        let portal = Portal {
            prepared: Arc::clone(prepared),
            parameters,
            binary,
            state: PortalState::Ready,
        };
        self.portals.insert(bind.portal.to_owned(), portal);
        backend::bind_complete(out);
        Ok(())
    }

    fn describe(&self, target: Target, out: &mut Vec<u8>) -> Result<(), Failure> {
        let (prepared, binary) = match target {
            Target::Statement(name) => {
                let prepared = self.statements.get(name).ok_or_else(|| {
                    Failure::new(
                        "26000",
                        format!("prepared statement \"{name}\" does not exist"),
                    )
                })?;
                backend::parameter_description(out, &prepared.parameter_types);
                // Formats aren't chosen until Bind.
                (prepared, &[][..])
            }
            Target::Portal(name) => {
                let portal = self.portals.get(name).ok_or_else(|| missing_portal(name))?;
                (&portal.prepared, &portal.binary[..])
            }
        };
        match &prepared.columns {
            Some(columns) => {
                let fields: Vec<Field> = columns.iter().map(field).collect();
                backend::row_description_in(out, &fields, binary);
            }
            None => backend::no_data(out),
        }
        Ok(())
    }

    fn execute(
        &mut self,
        session: &mut Session,
        name: &str,
        max_rows: i32,
        out: &mut Vec<u8>,
    ) -> Result<(), Failure> {
        let portal = self
            .portals
            .get_mut(name)
            .ok_or_else(|| missing_portal(name))?;
        let (command, mut rows) = match mem::replace(&mut portal.state, PortalState::Ready) {
            PortalState::Ready => {
                let Some(statement) = &portal.prepared.statement else {
                    portal.state = PortalState::Done(String::new());
                    backend::empty_query_response(out);
                    return Ok(());
                };
                match session.execute_prepared(statement, &portal.parameters)? {
                    Output::Rows { command, rows, .. } => (command, rows.into_iter()),
                    Output::Count { command, count } => {
                        let tag = command_tag(command, count);
                        backend::command_complete(out, &tag);
                        portal.state = PortalState::Done(tag);
                        return Ok(());
                    }
                    Output::Done(tag) => {
                        backend::command_complete(out, tag);
                        portal.state = PortalState::Done(tag.to_owned());
                        return Ok(());
                    }
                }
            }
            PortalState::Suspended { command, rows } => (command, rows),
            PortalState::Done(tag) => {
                if tag.is_empty() {
                    backend::empty_query_response(out);
                } else {
                    backend::command_complete(out, &tag);
                }
                portal.state = PortalState::Done(tag);
                return Ok(());
            }
        };
        let limit = usize::try_from(max_rows)
            .ok()
            .filter(|&n| n > 0)
            .unwrap_or(usize::MAX);
        let mut sent = 0;
        while sent < limit
            && let Some(row) = rows.next()
        {
            data_row(out, &row, &portal.binary);
            sent += 1;
        }
        if rows.len() > 0 {
            backend::portal_suspended(out);
            portal.state = PortalState::Suspended { command, rows };
        } else {
            backend::command_complete(out, &command_tag(command, sent));
            portal.state = PortalState::Done(command_tag(command, 0));
        }
        Ok(())
    }
}

fn missing_portal(name: &str) -> Failure {
    Failure::new(
        "34000", // invalid_cursor_name
        format!("portal \"{name}\" does not exist"),
    )
}

/// The type a parameter the client gave type `oid` binds as, `None` if the
/// client left it to the server.
fn parameter_type(type_oid: u32) -> Result<Option<DataType>, Failure> {
    Ok(Some(match type_oid {
        oid::UNSPECIFIED | oid::UNKNOWN => return Ok(None),
        oid::INT2 | oid::INT4 => DataType::Int,
        oid::INT8 => DataType::BigInt,
        oid::FLOAT4 | oid::FLOAT8 => DataType::Double,
        oid::BOOL => DataType::Bool,
        oid::TEXT | oid::VARCHAR | oid::BPCHAR => DataType::Varchar(u32::MAX),
        oid::TIMESTAMP | oid::TIMESTAMPTZ => DataType::Timestamptz,
        other => {
            return Err(Failure::new(
                "0A000", // feature_not_supported
                format!("parameters of type oid {other} are not supported"),
            ));
        }
    }))
}

/// The value of parameter `$n`, of type `type_oid`, sent as `bytes`.
fn read_parameter(n: usize, type_oid: u32, format: Format, bytes: &[u8]) -> Result<Value, Failure> {
    let invalid = |what: &str| {
        Failure::new(
            "22P02", // invalid_text_representation
            format!("invalid {what} for parameter ${n}"),
        )
    };
    let value = match format {
        Format::Text => {
            let text = std::str::from_utf8(bytes).map_err(|_| invalid("UTF-8"))?;
            match type_oid {
                oid::INT2 | oid::INT4 => text.trim().parse().map(Value::Int).ok(),
                oid::INT8 => text.trim().parse().map(Value::BigInt).ok(),
                oid::FLOAT4 | oid::FLOAT8 => text.trim().parse().map(Value::Double).ok(),
                oid::BOOL => parse_bool(text).map(Value::Bool),
                oid::TIMESTAMP | oid::TIMESTAMPTZ => {
                    timestamp::parse(text).map(Value::Timestamptz).ok()
                }
                _ => Some(Value::Varchar(text.to_owned())),
            }
        }
        Format::Binary => match (type_oid, bytes) {
            (oid::INT2, &[a, b]) => Some(Value::Int(i16::from_be_bytes([a, b]).into())),
            (oid::INT4, bytes) => bytes
                .try_into()
                .ok()
                .map(i32::from_be_bytes)
                .map(Value::Int),
            (oid::INT8, bytes) => bytes
                .try_into()
                .ok()
                .map(i64::from_be_bytes)
                .map(Value::BigInt),
            (oid::FLOAT4, bytes) => bytes
                .try_into()
                .ok()
                .map(|b| Value::Double(f32::from_be_bytes(b).into())),
            (oid::FLOAT8, bytes) => bytes
                .try_into()
                .ok()
                .map(f64::from_be_bytes)
                .map(Value::Double),
            (oid::BOOL, &[b]) => Some(Value::Bool(b != 0)),
            (oid::TIMESTAMP | oid::TIMESTAMPTZ, bytes) => bytes
                .try_into()
                .ok()
                .map(i64::from_be_bytes)
                .and_then(|micros| micros.checked_add(POSTGRES_EPOCH))
                .filter(|micros| (timestamp::MIN..=timestamp::MAX).contains(micros))
                .map(Value::Timestamptz),
            (oid::TEXT | oid::VARCHAR | oid::BPCHAR | oid::UNKNOWN, bytes) => {
                let text = std::str::from_utf8(bytes).map_err(|_| invalid("UTF-8"))?;
                Some(Value::Varchar(text.to_owned()))
            }
            _ => None,
        },
    };
    match value {
        Some(Value::Double(d)) if !d.is_finite() => Err(invalid("value")),
        Some(value) => Ok(value),
        None => Err(invalid("value")),
    }
}

/// A BOOL as Postgres reads one.
fn parse_bool(text: &str) -> Option<bool> {
    match text.trim().to_ascii_lowercase().as_str() {
        "t" | "true" | "y" | "yes" | "on" | "1" => Some(true),
        "f" | "false" | "n" | "no" | "off" | "0" => Some(false),
        _ => None,
    }
}
