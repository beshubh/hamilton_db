# SQL tests

SQL run through `hamilton-cli`, end to end. One runner builds the CLI and
runs every suite, or the ones you name:

```sh
uv run sqltest/run.py                          # every suite
uv run sqltest/run.py transactions             # one suite
uv run sqltest/run.py -k "lost update"         # cases whose name contains the text
uv run sqltest/run.py index_storage --runs 50  # more timed runs per query (default 20)
```

It exits non-zero if any case fails. Each suite is a SQL file of cases,
each starting with `-- case: <name>`, and a Python module that runs them
(`cli.py` drives the CLI for both).

## transactions

`transactions.sql` switches between sessions with `\session` to check that
MVTO prevents non-repeatable reads, lost updates and write skew. Each
statement's expected result is a comment after it:

```sql
SELECT balance FROM accounts WHERE id = 1;   -- => 100
```

Each case runs on a fresh database with the tables at the top of the file.
`transactions.py` has the format.

## index_storage

Lookups and sorts on a 10,000-row table, with and without indexes.

- `schema.sql`: two copies of a `users` table. `users_plain` has no indexes.
  `users` has a primary key on `id` and indexes on `age` (INT) and `email`
  (VARCHAR).
- `gen_data.py`: writes the INSERTs for 10,000 rows into both tables. The CLI
  has no `INSERT ... SELECT`, so the rows are generated as literal SQL.
- `index_storage.sql`: the cases. Each is a query on `users_plain` and the
  same query on `users`. `-- index:` names the index the second must use.

A case passes if both queries return the same output and `EXPLAIN` shows
the named index on the indexed query and no index on the plain one. Each
query is then timed, after a few untimed warm-up runs, and the median
reported. Times are what `\timing` reports: the statement's execution, not
printing. The first statements after a load are slow, as pages are still
being flushed, so don't read anything into single runs.

To run the SQL by hand:

```sh
{ cat sqltest/schema.sql; python3 sqltest/gen_data.py; cat sqltest/index_storage.sql; } \
    | target/release/hamilton-cli /tmp/sqltest.db
```

Delete `/tmp/sqltest.{db,meta,log}` before running it again.
