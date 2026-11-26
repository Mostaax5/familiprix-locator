from unittest.mock import patch
import health


def test_failed_dependency_is_visible_and_cached_without_claiming_read_failure():
    original=dict(health._state)
    try:
        health._state.update(database_available=None,checked_at=0)
        with patch('health.connect_db',side_effect=ConnectionError) as connect:
            first=health.dependency_status()
            second=health.dependency_status()
        assert first['database_available'] is False
        assert first==second
        assert connect.call_count==1
    finally:
        health._state.clear()
        health._state.update(original)
