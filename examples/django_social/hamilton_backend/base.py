"""Django's PostgreSQL backend, minus what HamiltonDB can't do yet.

Each override works around one gap; the README lists them with the SQL that
fails without it.
"""

from django.db import NotSupportedError
from django.db.backends.postgresql import (
    base,
    creation,
    features,
    introspection,
    operations,
    schema,
)


class DatabaseFeatures(features.DatabaseFeatures):
    # CREATE TABLE and CREATE INDEX are rejected inside a transaction block,
    # so migrations can't run in one.
    can_rollback_ddl = False
    # No foreign keys (or ALTER TABLE), so none are created.
    supports_foreign_keys = False
    can_defer_constraint_checks = False
    # No SAVEPOINT; a nested atomic() rolls back the whole transaction.
    uses_savepoints = False
    can_release_savepoints = False


class DatabaseIntrospection(introspection.DatabaseIntrospection):
    def get_table_list(self, cursor):
        # There is no pg_catalog. `\schema` is the catalog request Hamilton's
        # own CLI uses; its first result has one row per column.
        cursor.execute("\\schema")
        names = sorted({row[0] for row in cursor.fetchall()})
        return [introspection.TableInfo(name, "t", None) for name in names]


class DatabaseOperations(operations.DatabaseOperations):
    def sql_flush(self, style, tables, *, reset_sequences=False, allow_cascade=False):
        # No TRUNCATE. Without foreign keys there is nothing to cascade to.
        if reset_sequences:
            raise NotSupportedError("HamiltonDB can't restart identity columns")
        return [f"DELETE FROM {self.quote_name(table)};" for table in tables]


class DatabaseCreation(creation.DatabaseCreation):
    # A server serves one database and has no CREATE DATABASE, so tests use
    # the configured one, which must be a fresh server (see the test runner).
    def _create_test_db(self, verbosity, autoclobber, keepdb=False):
        return self.connection.settings_dict["NAME"]

    def _destroy_test_db(self, test_database_name, verbosity):
        pass


class DatabaseSchemaEditor(schema.DatabaseSchemaEditor):
    def _create_like_index_sql(self, model, field):
        # No operator classes, so no varchar_pattern_ops index for LIKE on a
        # unique or indexed varchar.
        return None


class DatabaseWrapper(base.DatabaseWrapper):
    SchemaEditorClass = DatabaseSchemaEditor
    features_class = DatabaseFeatures
    introspection_class = DatabaseIntrospection
    ops_class = DatabaseOperations
    creation_class = DatabaseCreation
