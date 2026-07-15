import types
import unittest
from unittest.mock import Mock, patch

import database


class DatabasePoolTests(unittest.TestCase):
    def test_pool_starts_one_connection_without_blocking_boot(self):
        created_pool = Mock()
        factory = Mock(return_value=created_pool)

        with patch.object(database, "_PG_POOL", None), \
             patch.object(database, "ConnectionPool", factory):
            self.assertIs(database._get_pg_pool(), created_pool)

        kwargs = factory.call_args.kwargs
        self.assertEqual(kwargs["min_size"], 1)
        self.assertEqual(kwargs["max_size"], 6)
        self.assertTrue(kwargs["open"])
        self.assertEqual(kwargs["timeout"], database._PG_POOL_WAIT_S)
        created_pool.open.assert_not_called()

    def test_broken_pool_fails_over_after_short_deadline(self):
        pool = Mock()
        pool.getconn.side_effect = RuntimeError("pool unavailable")
        connection = object()
        psycopg = types.SimpleNamespace(connect=Mock(return_value=connection))

        with patch.object(database, "DB_BACKEND", "postgres"), \
             patch.object(database, "ConnectionPool", object()), \
             patch.object(database, "psycopg", psycopg), \
             patch.object(database, "_get_pg_pool", return_value=pool), \
             patch.object(database, "_PG_POOL_DIRECT_FALLBACKS", 0):
            wrapped = database.connect_db()

        pool.getconn.assert_called_once_with(timeout=database._PG_POOL_WAIT_S)
        psycopg.connect.assert_called_once_with(
            database.DATABASE_URL, row_factory=database.dict_row, connect_timeout=5
        )
        self.assertIs(wrapped.connection, connection)
        self.assertIsNone(wrapped.pool)


if __name__ == "__main__":
    unittest.main()
