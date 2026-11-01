use std::io::Cursor;

use wire::backend::{self, DataRow, ErrorFields, Field, Message};
use wire::frontend::{self, Startup};
use wire::{TransactionStatus, oid, read_message};

/// Decodes every message in `buf`, as the client would read them.
fn backend_messages(buf: &[u8]) -> Vec<Message> {
    let mut r = Cursor::new(buf);
    let mut body = Vec::new();
    let mut messages = Vec::new();
    while let Some(tag) = read_message(&mut r, &mut body).unwrap() {
        messages.push(backend::decode(tag, &body).unwrap());
    }
    messages
}

#[test]
fn backend_messages_round_trip() {
    let fields = vec![
        Field {
            name: "id".into(),
            type_oid: oid::INT4,
            type_size: 4,
            type_modifier: -1,
        },
        Field {
            name: "name".into(),
            type_oid: oid::VARCHAR,
            type_size: -1,
            type_modifier: 12,
        },
    ];
    let mut buf = Vec::new();
    backend::authentication_ok(&mut buf);
    backend::parameter_status(&mut buf, "server_encoding", "UTF8");
    backend::row_description(&mut buf, &fields);
    let mut row = DataRow::begin(&mut buf);
    row.text(7);
    row.null();
    row.finish();
    let mut row = DataRow::begin(&mut buf);
    row.text(-1);
    row.text("ünï");
    row.finish();
    backend::command_complete(&mut buf, "SELECT 2");
    backend::empty_query_response(&mut buf);
    backend::error_response(&mut buf, "40001", "it conflicts");
    backend::ready_for_query(&mut buf, TransactionStatus::Failed);

    assert_eq!(
        backend_messages(&buf),
        [
            Message::AuthenticationOk,
            Message::ParameterStatus {
                name: "server_encoding".into(),
                value: "UTF8".into()
            },
            Message::RowDescription(fields),
            Message::DataRow(vec![Some("7".into()), None]),
            Message::DataRow(vec![Some("-1".into()), Some("ünï".into())]),
            Message::CommandComplete("SELECT 2".into()),
            Message::EmptyQueryResponse,
            Message::ErrorResponse(ErrorFields {
                severity: "ERROR".into(),
                code: "40001".into(),
                message: "it conflicts".into(),
            }),
            Message::ReadyForQuery(TransactionStatus::Failed),
        ]
    );
}

#[test]
fn startup_round_trips() {
    let mut buf = Vec::new();
    frontend::startup(&mut buf, &[("user", "ada"), ("database", "hamilton")]);
    assert_eq!(
        frontend::read_startup(&mut Cursor::new(&buf)).unwrap(),
        Startup::Params(vec![
            ("user".into(), "ada".into()),
            ("database".into(), "hamilton".into())
        ])
    );
}

#[test]
fn startup_recognizes_requests_and_old_versions() {
    let packet = |code: i32| {
        let mut buf = 8i32.to_be_bytes().to_vec();
        buf.extend_from_slice(&code.to_be_bytes());
        frontend::read_startup(&mut Cursor::new(buf)).unwrap()
    };
    assert_eq!(packet(80_877_103), Startup::SslRequest);
    assert_eq!(packet(80_877_104), Startup::GssEncRequest);
    assert_eq!(packet(2 << 16), Startup::Unsupported(2 << 16));
}

#[test]
fn frontend_messages_decode() {
    let mut buf = Vec::new();
    frontend::query(&mut buf, "SELECT 1; SELECT 2");
    frontend::terminate(&mut buf);
    buf.extend_from_slice(&[b'P', 0, 0, 0, 4, b'S', 0, 0, 0, 4]);

    let mut r = Cursor::new(&buf);
    let mut body = Vec::new();
    let mut tags = Vec::new();
    while let Some(tag) = read_message(&mut r, &mut body).unwrap() {
        tags.push(format!("{:?}", frontend::decode(tag, &body).unwrap()));
    }
    assert_eq!(
        tags,
        [
            r#"Query("SELECT 1; SELECT 2")"#,
            "Terminate",
            "Extended(80)",
            "Sync"
        ]
    );
}

#[test]
fn bad_lengths_are_refused() {
    let mut body = Vec::new();
    for len in [3i32, -1, i32::MAX] {
        let mut buf = vec![b'Q'];
        buf.extend_from_slice(&len.to_be_bytes());
        assert!(
            read_message(&mut Cursor::new(buf), &mut body).is_err(),
            "{len}"
        );
    }
    // A message cut short ends the stream with an error, not a clean end.
    let truncated = [b'Q', 0, 0, 0, 10, b'S'];
    assert!(read_message(&mut Cursor::new(truncated), &mut body).is_err());
    assert_eq!(read_message(&mut Cursor::new([]), &mut body).unwrap(), None);
}
