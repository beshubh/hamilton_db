# SQL tests

SQL run through `hamilton-cli`, end to end. For now they test index storage:
lookups and sorts on a 10,000-row table, with and without indexes.

```sh
python3 sqltest/run.py                 # build the CLI, load the data, run every case
python3 sqltest/run.py --runs 50       # more timed runs per query (default 20)
```

- `schema.sql`: two copies of a `users` table. `users_plain` has no indexes.
  `users` has a primary key on `id` and indexes on `age` (INT) and `email`
  (VARCHAR).
- `gen_data.py`: writes the INSERTs for 10,000 rows into both tables. The CLI
  has no `INSERT ... SELECT`, so the rows are generated as literal SQL.
- `index_storage.sql`: the cases. Each is a query on `users_plain` and the
  same query on `users`.

For each case, `run.py` checks that both queries return the same output and
that `EXPLAIN` shows the index named in `-- index:` on the indexed query and
no index on the plain one. It then reports the median time of each, after a
few untimed warm-up runs. Any failed check makes it exit non-zero.

Times are what `\timing` reports: the statement's execution, not printing.
The first statements after a load are slow, as pages are still being flushed,
so don't read anything into single runs.

To run the SQL by hand:

```sh
{ cat sqltest/schema.sql; python3 sqltest/gen_data.py; cat sqltest/index_storage.sql; } \
    | target/release/hamilton-cli /tmp/sqltest.db
```

Delete `/tmp/sqltest.{db,meta,log}` before running it again.
