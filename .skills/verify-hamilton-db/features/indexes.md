# Indexes

A primary key, `CREATE INDEX` or `CREATE UNIQUE INDEX` gives a table an index. Queries that can use it do, which `EXPLAIN` shows, and they return the same rows as a scan would. Primary keys and unique indexes reject duplicates.

## Sub-features

- `idx-create` adds a secondary index, including over rows already in the table.
- `idx-plan` uses an index for equality and range predicates; `EXPLAIN` names it.
- `idx-same-rows` returns the same rows with and without the index.
- `idx-maintain` keeps the index right across `INSERT`, `UPDATE` and `DELETE`.
- `idx-unique` rejects a duplicate key in a primary key or unique index.
- `idx-describe` lists a table's indexes in `\d NAME`.

## How to get to it (user POV)

- `PRIMARY KEY` in `CREATE TABLE`, or `CREATE [UNIQUE] INDEX name ON table (column)`.
- `EXPLAIN <query>` to see the plan.
- `\d NAME` in `hamilton-cli` to list a table's indexes.

## Driving it with hdb

Preconditions:

- `hdb doctor` exits 0.
- No table named `ix` exists.

- **Before the index.** Run `hdb sql idx-before`. The plan has `Filter: age = 30` over `IndexScan using ix_pkey` (the primary key supplies the `ORDER BY id`; without the `ORDER BY` it is `SeqScan on ix`), and the query returns ids `1` and `3`.

  ```sql
  CREATE TABLE ix (id INT PRIMARY KEY, age INT NOT NULL, email VARCHAR(32) NOT NULL);
  INSERT INTO ix VALUES (1, 30, 'a@x'), (2, 40, 'b@x'), (3, 30, 'c@x');
  EXPLAIN SELECT id FROM ix WHERE age = 30 ORDER BY id;
  SELECT id FROM ix WHERE age = 30 ORDER BY id;
  ```

- **Create and use.** Run `hdb sql idx-create`. Output is `CREATE INDEX`, a plan containing `IndexScan using ix_age` with `Keys: 30`, and the same ids `1` and `3` as before the index.

  ```sql
  CREATE INDEX ix_age ON ix (age);
  EXPLAIN SELECT id FROM ix WHERE age = 30 ORDER BY id;
  SELECT id FROM ix WHERE age = 30 ORDER BY id;
  ```

- **Primary key plans.** Run `hdb sql idx-pkey`. The first plan is `IndexScan using ix_pkey` with `Keys: 2`; the second has `Range: [2, 4)`.

  ```sql
  EXPLAIN SELECT * FROM ix WHERE id = 2;
  EXPLAIN SELECT * FROM ix WHERE id >= 2 AND id < 4;
  ```

- **Maintenance.** Run `hdb sql idx-maintain`. The three reads return, in order: `1, 3, 4`; then `2`; then `3, 4`.

  ```sql
  INSERT INTO ix VALUES (4, 30, 'd@x');
  SELECT id FROM ix WHERE age = 30 ORDER BY id;
  UPDATE ix SET age = 41 WHERE id = 2;
  SELECT id FROM ix WHERE age = 41;
  DELETE FROM ix WHERE id = 1;
  SELECT id FROM ix WHERE age = 30 ORDER BY id;
  ```

- **Unique.** Run `hdb sql idx-unique`. Output is `CREATE INDEX`, then two `error: duplicate key value` lines naming `ix_email` with `(email)=(b@x)` and `ix_pkey` with `(id)=(2)`, then a count of `3`.

  ```sql
  CREATE UNIQUE INDEX ix_email ON ix (email);
  INSERT INTO ix VALUES (5, 50, 'b@x');
  INSERT INTO ix VALUES (2, 50, 'e@x');
  SELECT count(*) FROM ix;
  ```

- **Describe.** Run `hdb sql idx-describe` with `\d ix`. Under `Indexes` it lists `ix_pkey PRIMARY KEY (id)`, `ix_age (age)` and `ix_email UNIQUE (email)`.

- **At scale.** Run `uv run sqltest/run.py index_storage`. It starts its own server, loads 10,000 rows, and for each case checks equal output and the named index in `EXPLAIN`. Every case prints `ok`.

## Gotchas

- `EXPLAIN` output is a tree; the `IndexScan` line is usually nested under `Projection` or `Sort`. Search for the index name, don't compare the first line.
- A query an index can't serve silently falls back to `SeqScan`. An index claim needs the `EXPLAIN`, not just correct rows.
- A unique-index violation is worded `violates primary key <index>` even when the index isn't the primary key. Match on the index name.
- Timings from `index_storage` include the round trip and any checkpoint that runs meanwhile. Use `HDB_PROFILE=release` and medians before saying anything about speed.
