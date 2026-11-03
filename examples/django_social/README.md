# Chirp: a small Django app on HamiltonDB

A minimal Twitter-like site: users, tweets with an optional image, threaded
replies and quote tweets. It talks to `hamilton-server` through Django's own
PostgreSQL backend (psycopg 3), with a thin subclass for what HamiltonDB
doesn't do yet. There is no login: pick who you are acting as from the
dropdown, which is kept in a signed cookie.

## Running it

From the repository root, build and start a server on a fresh database:

```sh
cargo build --release -p hamilton-server
target/release/hamilton-server examples/django_social/social.db   # 127.0.0.1:6942
```

In another terminal:

```sh
cd examples/django_social
uv run python manage.py migrate
uv run python manage.py runserver
```

Then open http://127.0.0.1:8000/, create a user or two on the Users page,
and post. Uploaded images go to `media/`, and the database stores only their
paths. `HAMILTON_HOST` and `HAMILTON_PORT` point the app at another server,
and `SQL_LOG_LEVEL=DEBUG` prints every query Django sends.

To start over, stop the server and delete all three database files, then
`media/`:

```sh
rm examples/django_social/social.{db,meta,log}
rm -r examples/django_social/media
```

## Tests

```sh
uv run python manage.py test
```

The test runner (`social_site/test_runner.py`) starts its own
`hamilton-server` on a free port with a temporary database, so it never
touches the one you run the app on. It looks for
`target/release/hamilton-server`; set `HAMILTON_SERVER` to use another
binary.

## What the backend works around

`hamilton_backend` is `django.db.backends.postgresql` with these changes,
each for a feature HamiltonDB lacks:

| Hamilton lacks | Django would send | Override |
| --- | --- | --- |
| `pg_catalog` | `SELECT c.relname, CASE ... FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ...` to list tables | `get_table_list` uses `\schema`, Hamilton's own catalog request |
| DDL in a transaction block | `BEGIN; CREATE TABLE ...` for every migration | `can_rollback_ddl = False` |
| Foreign keys, `ALTER TABLE` | `ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY ... DEFERRABLE INITIALLY DEFERRED` | `supports_foreign_keys = False` (no referential integrity) |
| `SAVEPOINT` | `SAVEPOINT "s..._x1"` for nested `atomic()` | `uses_savepoints = False` |
| `CREATE DATABASE` | `CREATE DATABASE "test_hamilton"` for tests | test database creation is skipped; tests get their own server |
| Operator classes | `CREATE INDEX "social_user_username_..._like" ON "social_user" ("username" varchar_pattern_ops)` for a unique `varchar` | the schema editor skips the `_like` index |

Because there are no savepoints, the tests use `TransactionTestCase`, not
`TestCase`. Keep `USE_TZ = True`: the server reports its time zone
as UTC, and Django otherwise tries `SELECT set_config('TimeZone', ...)`.

Every text column is a `CharField`, so a `varchar`;
`uv run python manage.py sqlmigrate social 0001` shows the DDL.
