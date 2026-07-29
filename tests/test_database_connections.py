import sqlite3
import types
import unittest
from unittest.mock import Mock, patch

import database


class DatabaseConnectionTests(unittest.TestCase):
    def test_executemany_batches_sqlite_writes(self):
        connection = sqlite3.connect(":memory:")
        wrapped = database.DatabaseConnection(connection, "sqlite")
        wrapped.execute("CREATE TABLE samples (id INTEGER PRIMARY KEY, value TEXT)")

        result = wrapped.executemany(
            "INSERT INTO samples (id, value) VALUES (?, ?)",
            [(1, "first"), (2, "second"), (3, "third")],
        )

        self.assertEqual(result.rowcount, 3)
        rows = wrapped.execute("SELECT id, value FROM samples ORDER BY id").fetchall()
        self.assertEqual(rows, [(1, "first"), (2, "second"), (3, "third")])
        wrapped.close()

    def test_postgres_uses_bounded_direct_connection_by_default(self):
        connection = object()
        psycopg = types.SimpleNamespace(connect=Mock(return_value=connection))

        with patch.object(database, "DB_BACKEND", "postgres"), \
             patch.object(database, "PG_POOL_ENABLED", False), \
             patch.object(database, "ConnectionPool", object()), \
             patch.object(database, "psycopg", psycopg), \
             patch.object(database, "_get_pg_pool") as get_pool:
            wrapped = database.connect_db()

        get_pool.assert_not_called()
        psycopg.connect.assert_called_once_with(
            database.DATABASE_URL, row_factory=database.dict_row,
            connect_timeout=5, sslmode="require",
        )
        self.assertIs(wrapped.connection, connection)
        self.assertIsNone(wrapped.pool)

    def test_pool_diagnostics_report_disabled_default(self):
        with patch.object(database, "PG_POOL_ENABLED", False):
            self.assertEqual(database.pool_stats(), {"enabled": False})

    def test_existing_postgres_auth_schema_skips_locking_ddl(self):
        db = Mock(backend="postgres")
        with patch.object(database, "_POSTGRES_AUTH_SCHEMA_READY", False), \
             patch.object(
                 database, "_postgres_auth_schema_complete", return_value=True
             ) as complete:
            database.ensure_auth_schema(db)

            complete.assert_called_once_with(db)
            db.execute.assert_not_called()
            self.assertTrue(database._POSTGRES_AUTH_SCHEMA_READY)

    def test_postgres_auth_schema_requires_all_tables_and_session_column(self):
        db = Mock()
        db.execute.return_value.fetchone.return_value = {
            "table_count": 4,
            "column_count": 1,
        }
        self.assertTrue(database._postgres_auth_schema_complete(db))

        db.execute.return_value.fetchone.return_value = {
            "table_count": 4,
            "column_count": 0,
        }
        self.assertFalse(database._postgres_auth_schema_complete(db))

    def test_current_postgres_schema_version_skips_historical_migrations(self):
        db = Mock(backend="postgres")
        with patch.object(database, "connect_db", return_value=db), \
             patch.object(database, "ensure_auth_schema"), \
             patch.object(
                 database, "_postgres_stored_schema_version",
                 return_value=database._POSTGRES_SCHEMA_VERSION,
             ), \
             patch.object(database, "init_postgres_db") as full_migration, \
             patch.object(database, "_set_postgres_schema_version") as set_version, \
             patch.object(
                 database, "ensure_best_effort_unique_indexes"
             ) as ensure_indexes, \
             patch.object(database, "_POSTGRES_PRODUCT_SCHEMA_READY", False):
            database.init_db()

        full_migration.assert_not_called()
        ensure_indexes.assert_not_called()
        set_version.assert_called_once_with(db)
        db.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
