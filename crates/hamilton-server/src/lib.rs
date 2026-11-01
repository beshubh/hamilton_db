//! The HamiltonDB server: owns a database and serves it to clients over the
//! Postgres wire protocol (see the `wire` crate), one session per
//! connection.

mod connection;
pub mod database;
mod server;

pub use database::{Database, Session};
pub use server::{DEFAULT_PORT, Server, ShutdownHandle};
