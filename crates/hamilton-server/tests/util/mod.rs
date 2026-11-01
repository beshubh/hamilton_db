//! A server on a free port for the transaction tests, and helpers for
//! talking to it.
#![allow(dead_code)]

use std::net::SocketAddr;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use hamilton_server::{Database, Server, ShutdownHandle};
use tempfile::TempDir;
use wire::{Client, QueryResult};

pub const SERIALIZATION_FAILURE: &str = "40001";

pub struct Running {
    dir: TempDir,
    conflict_timeout: Option<Duration>,
    addr: SocketAddr,
    handle: ShutdownHandle,
    thread: JoinHandle<Database>,
}

impl Running {
    /// `conflict_timeout` replaces the server's default when given.
    pub fn start(conflict_timeout: Option<Duration>) -> Self {
        Self::start_in(TempDir::new().unwrap(), conflict_timeout)
    }

    fn start_in(dir: TempDir, conflict_timeout: Option<Duration>) -> Self {
        let mut db = Database::open(dir.path().join("test.db")).unwrap();
        if let Some(timeout) = conflict_timeout {
            db = db.with_conflict_timeout(timeout);
        }
        let server = Server::bind(db, "127.0.0.1:0").unwrap();
        let (addr, handle) = (server.local_addr(), server.shutdown_handle());
        let thread = thread::spawn(move || server.run());
        Self {
            dir,
            conflict_timeout,
            addr,
            handle,
            thread,
        }
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn connect(&self) -> Client {
        Client::connect(self.addr, "test").unwrap()
    }

    pub fn stop(self) {
        self.close();
    }

    /// Shuts down and reopens the same database files.
    pub fn restart(self) -> Self {
        let timeout = self.conflict_timeout;
        Self::start_in(self.close(), timeout)
    }

    fn close(self) -> TempDir {
        self.handle.shutdown();
        self.thread.join().unwrap().close().unwrap();
        self.dir
    }
}

/// Runs `sql`, panicking if it fails.
pub fn exec(client: &mut Client, sql: &str) -> Vec<QueryResult> {
    let response = client.simple_query(sql).unwrap();
    assert_eq!(response.error, None, "{sql}");
    response.results
}

/// The integer rows of a one-statement query.
pub fn ints(results: &[QueryResult]) -> Vec<Vec<i64>> {
    let [QueryResult::Rows { rows, .. }] = results else {
        panic!("expected rows, got {results:?}");
    };
    rows.iter()
        .map(|row| {
            row.iter()
                .map(|v| v.as_deref().expect("NULL").parse().unwrap())
                .collect()
        })
        .collect()
}

/// The `kv` table as `(k, v)` pairs in key order.
pub fn kv(client: &mut Client) -> Vec<(i64, i64)> {
    ints(&exec(client, "SELECT k, v FROM kv ORDER BY k"))
        .into_iter()
        .map(|row| (row[0], row[1]))
        .collect()
}
