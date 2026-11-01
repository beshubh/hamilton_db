//! A hamilton-server running in the test process, for the client to talk to.

use std::thread::{self, JoinHandle};

use hamilton_server::{Database, Server, ShutdownHandle};
use tempfile::TempDir;

pub struct TestServer {
    _dir: TempDir,
    pub port: u16,
    handle: ShutdownHandle,
    thread: Option<JoinHandle<Database>>,
}

impl TestServer {
    /// Serves a new database on a free port.
    pub fn start() -> Self {
        let dir = TempDir::new().unwrap();
        let db = Database::open(dir.path().join("test.db")).unwrap();
        let server = Server::bind(db, "127.0.0.1:0").unwrap();
        let (port, handle) = (server.local_addr().port(), server.shutdown_handle());
        Self {
            _dir: dir,
            port,
            handle,
            thread: Some(thread::spawn(move || server.run())),
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.handle.shutdown();
        if let Some(thread) = self.thread.take() {
            thread.join().unwrap().close().unwrap();
        }
    }
}
