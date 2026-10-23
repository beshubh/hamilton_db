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
  hamilton-cli  interactive shell
  bench         benchmarks
```

## Running it

```sh
cargo build --workspace
cargo test --workspace
cargo run -p hamilton-cli
```
