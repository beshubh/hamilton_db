//! One client connection: the startup handshake, then queries until the
//! client leaves.

use std::io::{self, BufReader, Write};
use std::net::TcpStream;

use catalog::{Catalog, CatalogError, MAX_NAME_LEN};
use common::{Column, DataType, IndexKind, Value, ValueError};
use execution::ExecutionError;
use planner::{EngineError, Output, PlannerError, TransactionStatus};
use storage::StorageError;
use wire::backend::{self, DataRow, Field};
use wire::frontend::{self, Message, Startup};
use wire::oid;

use crate::database::{Database, Error, Session};
use crate::extended::{Extended, Failure};

/// What the server tells clients it is. Clients such as psql read the
/// version to decide what they may send, and Django sets the time zone with
/// `set_config` unless `TimeZone` already names the one it wants.
const PARAMETERS: &[(&str, &str)] = &[
    ("server_version", "16.0"),
    ("server_encoding", "UTF8"),
    ("client_encoding", "UTF8"),
    ("DateStyle", "ISO, MDY"),
    ("TimeZone", "UTC"),
    ("integer_datetimes", "on"),
    ("standard_conforming_strings", "on"),
];

/// Serves `stream` until the client terminates or hangs up. Whatever block
/// its session has open is rolled back then.
pub(crate) fn serve(db: &Database, mut stream: TcpStream) -> io::Result<()> {
    stream.set_nodelay(true)?;
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut out = Vec::new();
    if !handshake(&mut reader, &mut stream, &mut out)? {
        return Ok(());
    }
    let mut session = db.session();
    let mut extended = Extended::default();
    let mut body = Vec::new();
    // After an extended query message fails, the rest up to its Sync are
    // skipped, as Postgres does.
    let mut skipping = false;
    while let Some(tag) = wire::read_message(&mut reader, &mut body)? {
        out.clear();
        match frontend::decode(tag, &body)? {
            Message::Query(sql) => {
                query(db.catalog(), &mut session, sql, &mut out);
                backend::ready_for_query(&mut out, status(&session));
            }
            Message::Terminate => return Ok(()),
            Message::Sync => {
                skipping = false;
                extended.sync(&session);
                backend::ready_for_query(&mut out, status(&session));
            }
            Message::Flush => {}
            Message::Parse(_)
            | Message::Bind(_)
            | Message::Describe(_)
            | Message::Execute { .. }
            | Message::Close(_)
                if skipping => {}
            message @ (Message::Parse(_)
            | Message::Bind(_)
            | Message::Describe(_)
            | Message::Execute { .. }
            | Message::Close(_)) => {
                if let Err(Failure { code, message }) =
                    extended.handle(&mut session, message, &mut out)
                {
                    backend::error_response(&mut out, code, &message);
                    skipping = true;
                }
            }
            Message::Unexpected(tag) => {
                let message = format!("unexpected message type {:?}", tag as char);
                backend::error_response(&mut out, "08P01", &message);
                stream.write_all(&out)?;
                return Ok(());
            }
        }
        stream.write_all(&out)?;
    }
    Ok(())
}

/// Answers the startup packet, and any requests for encryption before it.
/// Returns whether the connection goes on to run queries.
fn handshake(
    reader: &mut BufReader<TcpStream>,
    stream: &mut TcpStream,
    out: &mut Vec<u8>,
) -> io::Result<bool> {
    loop {
        match frontend::read_startup(reader)? {
            Startup::SslRequest | Startup::GssEncRequest => stream.write_all(b"N")?,
            // Queries can't be cancelled.
            Startup::Cancel => return Ok(false),
            Startup::Unsupported(version) => {
                let message = format!(
                    "unsupported frontend protocol {}.{}: the server supports 3.0",
                    version >> 16,
                    version & 0xffff
                );
                backend::error_response(out, "0A000", &message);
                stream.write_all(out)?;
                return Ok(false);
            }
            Startup::Params(_) => break,
        }
    }
    backend::authentication_ok(out);
    for (name, value) in PARAMETERS {
        backend::parameter_status(out, name, value);
    }
    backend::ready_for_query(out, wire::TransactionStatus::Idle);
    stream.write_all(out)?;
    Ok(true)
}

/// Runs the statements in `sql`, writing what each produced and the error
/// that stopped them, if one did, to `out`.
fn query(catalog: &Catalog, session: &mut Session, sql: &str, out: &mut Vec<u8>) {
    if sql.trim() == wire::SCHEMA_REQUEST {
        for output in schema(catalog) {
            write_output(out, &output);
        }
        return;
    }
    let mut ran = false;
    let result = session.execute(sql, |output| {
        ran = true;
        write_output(out, &output);
    });
    match result {
        Err(err) => backend::error_response(out, sqlstate(&err), &err.to_string()),
        Ok(()) if !ran => backend::empty_query_response(out),
        Ok(()) => {}
    }
}

fn write_output(out: &mut Vec<u8>, output: &Output) {
    match output {
        Output::Rows {
            command,
            columns,
            rows,
        } => {
            let fields: Vec<Field> = columns.iter().map(field).collect();
            backend::row_description(out, &fields);
            for row in rows {
                data_row(out, row, &[]);
            }
            backend::command_complete(out, &command_tag(command, rows.len()));
        }
        Output::Count { command, count } => {
            backend::command_complete(out, &command_tag(command, *count))
        }
        Output::Done(command) => backend::command_complete(out, command),
    }
}

/// Writes `row`, its `i`-th value in binary format if `binary[i]` is, and
/// in text format past the end of `binary`.
pub(crate) fn data_row(out: &mut Vec<u8>, row: &[Value], binary: &[bool]) {
    let mut data = DataRow::begin(out);
    for (i, value) in row.iter().enumerate() {
        if binary.get(i).copied().unwrap_or(false) {
            match value {
                Value::Null => data.null(),
                Value::Int(n) => data.bytes(&n.to_be_bytes()),
                Value::BigInt(n) => data.bytes(&n.to_be_bytes()),
                Value::Double(n) => data.bytes(&n.to_be_bytes()),
                Value::Bool(b) => data.bytes(&[u8::from(*b)]),
                Value::Varchar(s) => data.bytes(s.as_bytes()),
                Value::Timestamptz(micros) => {
                    data.bytes(&(micros - crate::extended::POSTGRES_EPOCH).to_be_bytes())
                }
                // Described as TEXT, whose binary format is its text.
                Value::Array(_) => data.text(value),
            }
            continue;
        }
        match value {
            Value::Null => data.null(),
            Value::Bool(b) => data.text(if *b { "t" } else { "f" }),
            value => data.text(value),
        }
    }
    data.finish();
}

pub(crate) fn command_tag(command: &str, count: impl std::fmt::Display) -> String {
    match command {
        // Postgres puts an oid, always 0 now, before INSERT's count.
        "INSERT" => format!("INSERT 0 {count}"),
        command => format!("{command} {count}"),
    }
}

pub(crate) fn type_oid(data_type: DataType) -> u32 {
    field(&Column::new("", data_type, true)).type_oid
}

pub(crate) fn field(column: &Column) -> Field {
    let (type_oid, type_size, type_modifier) = match column.data_type {
        DataType::Int => (oid::INT4, 4, -1),
        DataType::BigInt => (oid::INT8, 8, -1),
        DataType::Double => (oid::FLOAT8, 8, -1),
        DataType::Bool => (oid::BOOL, 1, -1),
        DataType::Timestamptz => (oid::TIMESTAMPTZ, 8, -1),
        // A VARCHAR's modifier is its limit plus 4; some limits are too big.
        DataType::Varchar(max_len) => {
            let modifier = i32::try_from(max_len).ok().and_then(|n| n.checked_add(4));
            (oid::VARCHAR, -1, modifier.unwrap_or(-1))
        }
        DataType::Array => (oid::TEXT, -1, -1),
    };
    Field {
        name: column.name.clone(),
        type_oid,
        type_size,
        type_modifier,
    }
}

fn status(session: &Session) -> wire::TransactionStatus {
    match session.status() {
        TransactionStatus::Idle => wire::TransactionStatus::Idle,
        TransactionStatus::InBlock => wire::TransactionStatus::InBlock,
        TransactionStatus::Failed => wire::TransactionStatus::Failed,
    }
}

/// The SQLSTATE that tells clients what kind of error `err` is, so that
/// they can retry on a serialization failure, say.
pub(crate) fn sqlstate(err: &Error) -> &'static str {
    let Error::Engine(err) = err;
    // A bad timestamp literal fails while binding, other bad text when run.
    if let EngineError::Planner(PlannerError::Value(value))
    | EngineError::Execution(ExecutionError::Value(value)) = err
    {
        match value {
            ValueError::InvalidTimestamp(_) => return "22007", // invalid_datetime_format
            ValueError::TimestampOutOfRange(_) => return "22008", // datetime_field_overflow
            _ => {}
        }
    }
    if let EngineError::Planner(PlannerError::TableNotFound(_))
    | EngineError::Execution(
        ExecutionError::TableNotFound(_) | ExecutionError::Catalog(CatalogError::TableNotFound(_)),
    ) = err
    {
        return "42P01"; // undefined_table
    }
    if let EngineError::Planner(PlannerError::IndexNotFound(_))
    | EngineError::Execution(ExecutionError::Catalog(CatalogError::IndexNotFound(_))) = err
    {
        return "42704"; // undefined_object
    }
    match err {
        EngineError::Parse(_) => "42601",                 // syntax_error
        EngineError::MultipleStatements => "42601",       // syntax_error
        EngineError::Planner(_) => "42000",               // syntax_error_or_access_rule_violation
        EngineError::InFailedTransaction => "25P02",      // in_failed_sql_transaction
        EngineError::AlreadyInTransaction => "25001",     // active_sql_transaction
        EngineError::NoTransaction => "25P01",            // no_active_sql_transaction
        EngineError::NotInTransactionBlock(_) => "25001", // active_sql_transaction
        EngineError::Execution(err) => match err {
            ExecutionError::Storage(StorageError::Conflict { .. })
            | ExecutionError::Catalog(CatalogError::ChangedSince { .. })
            | ExecutionError::IndexNotFound(_) => "40001", // serialization_failure
            ExecutionError::Catalog(CatalogError::IndexRequiredByConstraint { .. }) => {
                "2BP01" // dependent_objects_still_exist
            }
            ExecutionError::Catalog(CatalogError::LockConflict { .. }) => "55P03", // lock_not_available
            ExecutionError::DuplicateKey { .. }
            | ExecutionError::Storage(StorageError::DuplicateKey) => {
                "23505" // unique_violation
            }
            ExecutionError::Value(_) => "22000", // data_exception
            ExecutionError::Catalog(CatalogError::SequenceExhausted { .. }) => {
                "2200H" // sequence_generator_limit_exceeded
            }
            _ => "XX000", // internal_error
        },
    }
}

/// The reply to [`wire::SCHEMA_REQUEST`].
fn schema(catalog: &Catalog) -> [Output; 2] {
    let name = |name: &str| Column::new(name, DataType::Varchar(MAX_NAME_LEN), false);
    let flag = |name: &str| Column::new(name, DataType::Bool, false);
    let mut columns = Vec::new();
    let mut indexes = Vec::new();
    for table in catalog.tables() {
        for column in table.schema.columns() {
            columns.push(vec![
                Value::from(table.name.as_str()),
                Value::from(column.name.as_str()),
                Value::from(column.data_type.to_string().as_str()),
                Value::Bool(column.nullable),
            ]);
        }
        for index in catalog.table_indexes(&table.name) {
            let key: Vec<&str> = index
                .key_columns
                .iter()
                .map(|&i| table.schema.columns()[i].name.as_str())
                .collect();
            indexes.push(vec![
                Value::from(index.name.as_str()),
                Value::from(table.name.as_str()),
                Value::from(kind_name(index.kind)),
                Value::from(key.join(", ").as_str()),
            ]);
        }
    }
    let text = |label: &str| Column::new(label, DataType::Varchar(u32::MAX), false);
    [
        Output::Rows {
            command: "SELECT",
            columns: vec![
                name("table"),
                name("column"),
                text("type"),
                flag("nullable"),
            ],
            rows: columns,
        },
        Output::Rows {
            command: "SELECT",
            columns: vec![name("index"), name("table"), text("kind"), text("columns")],
            rows: indexes,
        },
    ]
}

/// An index's kind as `\d` shows it, empty for a plain index.
fn kind_name(kind: IndexKind) -> &'static str {
    match kind {
        IndexKind::Plain => "",
        IndexKind::Unique {
            nulls_distinct: true,
        } => "UNIQUE",
        IndexKind::Unique {
            nulls_distinct: false,
        } => "UNIQUE NULLS NOT DISTINCT",
        IndexKind::PrimaryKey => "PRIMARY KEY",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// SQL can't run an INT sequence up to its maximum cheaply without
    /// setval, so this builds the error.
    #[test]
    fn an_exhausted_sequence_is_sequence_generator_limit_exceeded() {
        let exhausted = CatalogError::SequenceExhausted {
            name: "t_id_seq".into(),
            max: i32::MAX.into(),
        };
        let err = Error::Engine(EngineError::Execution(ExecutionError::Catalog(exhausted)));
        assert_eq!(sqlstate(&err), "2200H");
    }

    #[test]
    fn an_index_dropped_under_a_statement() {
        let engine = |err| Error::Engine(EngineError::Execution(err));
        let stale_plan = ExecutionError::IndexNotFound(7);
        assert_eq!(sqlstate(&engine(stale_plan)), "40001");
        let dropped_first = ExecutionError::Catalog(CatalogError::IndexNotFound("i".into()));
        assert_eq!(sqlstate(&engine(dropped_first)), "42704");
    }
}
