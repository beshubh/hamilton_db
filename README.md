# hamilton_db

A small relational database written in Rust. It's an OLTP, row-based engine
that stores tables in fixed-size pages on disk, and it speaks a
Postgres-compatible dialect of SQL.

It's a work in progress, built mostly to learn how databases work from the
inside.

## What's in it

- **Storage**: slotted pages, a buffer pool with an ARC replacer, and a disk
  scheduler for page I/O.
- **Tables**: row-oriented table heaps; each tuple is stored whole in a page slot.
- **MVCC** (in progress): tuple versions chained through an undo log, with
  vacuum reclaiming deleted rows.
- **Catalog**: table, column and index metadata, kept in system tables.
- **SQL**: parsing with the Postgres dialect, a binder that type-checks
  queries, and an expression engine. The parser handles `CREATE TABLE`
  (with a `PRIMARY KEY`), `CREATE INDEX`, `INSERT`, `SELECT`, `UPDATE` and
  `DELETE`; executors are next.
- **Types**: `INT`, `BIGINT`, `BOOL` and `VARCHAR(n)`.

## Layout

```
crates/
  storage       pages, buffer pool, disk I/O, table heap
  catalog       system tables and schema lookups
  common        values, tuples, schemas, ids
  parser        SQL → AST
  planner       binder and planning
  execution     expressions and executors
  wire          the Postgres wire protocol, server and client sides
  hamilton-server  serves a database over the wire protocol
  hamilton-cli  interactive shell, a client of the server
  bench         benchmarks
```

## Running it

```sh
cargo build --workspace
cargo nextest run --workspace   # cargo install cargo-nextest
cargo run -p hamilton-server    # serves hamilton.db on 127.0.0.1:6942
cargo run -p hamilton-cli       # in another terminal: connects to it
```

The server speaks the Postgres wire protocol (simple queries, text format,
no authentication), so `psql -h 127.0.0.1 -p 6942` can run SQL too.
psql's own backslash commands, such as `\d`, query `pg_catalog`, which
HamiltonDB doesn't have; use `hamilton-cli` for those.

### Databases

A database is a file, with a `.meta` and a `.log` file next to it. Give the
server a path to serve another one; it creates the files if they don't
exist, though not the directory they go in:

```sh
cargo run -p hamilton-server -- data/shop.db
```

A server serves one database, and there's no `CREATE DATABASE`: the
database name a client asks for at startup (`psql -d`) is ignored. To serve
several at once, give each server its own port:

```sh
cargo run -p hamilton-server -- --port 6942 shop.db
cargo run -p hamilton-server -- --port 6943 analytics.db
cargo run -p hamilton-cli -- -p 6943   # analytics.db
```

Only one process can have a database open; a second server on the same file
fails with "already open in another process". To start a database over,
stop its server and delete all three files. Deleting only the `.db` leaves a
stale `.meta`, and opening then fails with "page 0 is not a catalog header
page".

```sh
rm shop.db shop.meta shop.log
```

To time join queries against a local Postgres on the same data (flags are
listed at the top of `crates/bench/src/main.rs`):

```sh
cargo run --release -p bench -- --pg postgres://postgres@127.0.0.1:5432
```
