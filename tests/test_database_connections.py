import types
import unittest
from unittest.mock import Mock, patch

import database


class DatabaseConnectionTests(unittest.TestCase):
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
            database.DATABASE_URL, row_factory=database.dict_row, connect_timeout=5
        )
        self.assertIs(wrapped.connection, connection)
        self.assertIsNone(wrapped.pool)

    def test_pool_diagnostics_report_disabled_default(self):
        with patch.object(database, "PG_POOL_ENABLED", False):
            self.assertEqual(database.pool_stats(), {"enabled": False})


if __name__ == "__main__":
    unittest.main()
