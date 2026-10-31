//! Transaction control statements: every spelling Postgres accepts, and the
//! options that are rejected.

use parser::{ParseError, Statement, parse};

fn one(sql: &str) -> Statement {
    let mut stmts = parse(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    assert_eq!(stmts.len(), 1, "{sql}");
    stmts.remove(0)
}

#[test]
fn every_spelling_parses() {
    let cases = [
        ("BEGIN", Statement::Begin),
        ("begin transaction", Statement::Begin),
        ("BEGIN WORK", Statement::Begin),
        ("START TRANSACTION", Statement::Begin),
        ("COMMIT", Statement::Commit),
        ("COMMIT TRANSACTION", Statement::Commit),
        ("COMMIT WORK", Statement::Commit),
        ("END", Statement::Commit),
        ("END TRANSACTION", Statement::Commit),
        ("ROLLBACK", Statement::Rollback),
        ("ROLLBACK TRANSACTION", Statement::Rollback),
        ("ROLLBACK WORK", Statement::Rollback),
        ("ABORT", Statement::Rollback),
        ("ABORT WORK", Statement::Rollback),
    ];
    for (sql, expected) in cases {
        assert_eq!(one(sql), expected, "{sql}");
    }
}

#[test]
fn a_block_parses_statement_by_statement() {
    let stmts = parse("BEGIN; SELECT 1; COMMIT;").unwrap();
    assert_eq!(stmts.len(), 3);
    assert_eq!(stmts[0], Statement::Begin);
    assert!(matches!(stmts[1], Statement::Select(_)));
    assert_eq!(stmts[2], Statement::Commit);
}

#[test]
fn options_are_rejected() {
    for sql in [
        "BEGIN ISOLATION LEVEL SERIALIZABLE",
        "BEGIN READ ONLY",
        "BEGIN TRANSACTION READ WRITE",
        "START TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        "BEGIN TRAN",
        "COMMIT AND CHAIN",
        "END AND CHAIN",
        "ROLLBACK AND CHAIN",
        "ABORT AND CHAIN",
        "ROLLBACK TO SAVEPOINT a",
        "ROLLBACK TO a",
        "SAVEPOINT a",
        "RELEASE SAVEPOINT a",
    ] {
        assert!(
            matches!(parse(sql), Err(ParseError::Unsupported(_))),
            "{sql}: {:?}",
            parse(sql)
        );
    }
}

#[test]
fn explain_of_a_transaction_statement_is_rejected() {
    for sql in ["EXPLAIN BEGIN", "EXPLAIN COMMIT", "EXPLAIN ROLLBACK"] {
        assert!(parse(sql).is_err(), "{sql}");
    }
}
