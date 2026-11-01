//! Accepting connections, each served on a thread of its own, and shutting
//! down.

use std::collections::HashMap;
use std::io;
use std::net::{
    IpAddr, Ipv4Addr, Ipv6Addr, Shutdown, SocketAddr, TcpListener, TcpStream, ToSocketAddrs,
};
use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

use crate::connection;
use crate::database::Database;

pub const DEFAULT_PORT: u16 = 6942;

pub struct Server {
    db: Database,
    listener: TcpListener,
    stop: Arc<AtomicBool>,
}

/// Stops a running [`Server`] from another thread, such as a signal
/// handler.
#[derive(Clone)]
pub struct ShutdownHandle {
    stop: Arc<AtomicBool>,
    addr: SocketAddr,
}

impl ShutdownHandle {
    pub fn shutdown(&self) {
        self.stop.store(true, Ordering::SeqCst);
        // `accept` doesn't notice the flag on its own; a connection wakes it.
        let _ = TcpStream::connect(self.addr);
    }
}

impl Server {
    /// Listens on `addr` for connections to `db`. Port 0 picks a free one;
    /// `local_addr` says which.
    pub fn bind(db: Database, addr: impl ToSocketAddrs) -> io::Result<Self> {
        Ok(Self {
            db,
            listener: TcpListener::bind(addr)?,
            stop: Arc::new(AtomicBool::new(false)),
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.listener
            .local_addr()
            .expect("a bound listener has an address")
    }

    pub fn shutdown_handle(&self) -> ShutdownHandle {
        let mut addr = self.local_addr();
        if addr.ip().is_unspecified() {
            addr.set_ip(match addr.ip() {
                IpAddr::V4(_) => IpAddr::V4(Ipv4Addr::LOCALHOST),
                IpAddr::V6(_) => IpAddr::V6(Ipv6Addr::LOCALHOST),
            });
        }
        ShutdownHandle {
            stop: Arc::clone(&self.stop),
            addr,
        }
    }

    /// Serves connections until a [`ShutdownHandle`] stops it. Then it
    /// hangs up on every client, waits for their statements to finish and
    /// their sessions to roll back, and returns the database to be closed.
    ///
    /// A connection whose thread panics is dropped, rolling back its block;
    /// the others carry on.
    pub fn run(self) -> Database {
        // Clones of the connections' streams, to hang up on them.
        let open: Mutex<HashMap<u64, TcpStream>> = Mutex::new(HashMap::new());
        thread::scope(|scope| {
            for (id, stream) in (0..).zip(self.listener.incoming()) {
                if self.stop.load(Ordering::SeqCst) {
                    break;
                }
                let stream = match stream {
                    Ok(stream) => stream,
                    Err(err) => {
                        eprintln!("hamilton-server: accepting a connection failed: {err}");
                        continue;
                    }
                };
                let Ok(clone) = stream.try_clone() else {
                    continue;
                };
                open.lock().unwrap().insert(id, clone);
                let (db, open) = (&self.db, &open);
                let spawned = thread::Builder::new()
                    .name(format!("connection-{id}"))
                    .spawn_scoped(scope, move || {
                        let served =
                            panic::catch_unwind(AssertUnwindSafe(|| connection::serve(db, stream)));
                        if let Ok(Err(err)) = served
                            && !is_hang_up(&err)
                        {
                            eprintln!("hamilton-server: connection {id}: {err}");
                        }
                        open.lock().unwrap().remove(&id);
                    });
                if let Err(err) = spawned {
                    eprintln!(
                        "hamilton-server: starting a thread for connection {id} failed: {err}"
                    );
                    open.lock().unwrap().remove(&id);
                }
            }
            for stream in open.lock().unwrap().values() {
                let _ = stream.shutdown(Shutdown::Both);
            }
        });
        self.db
    }
}

/// Whether `err` only says that the client went away.
fn is_hang_up(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::UnexpectedEof
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::BrokenPipe
            | io::ErrorKind::NotConnected
    )
}
