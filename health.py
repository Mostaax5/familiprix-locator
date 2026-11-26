"""Bounded dependency checks, independent of catalogue-cache readiness."""
import threading
import time
from database import connect_db

_lock = threading.Lock()
_state = {'database_available': None, 'checked_at': 0.0}


def dependency_status():
    now = time.time()
    if now - _state['checked_at'] < 10:
        return dict(_state)
    if not _lock.acquire(blocking=False):
        return dict(_state)
    db = None
    try:
        db = connect_db()
        if db.backend == 'postgres':
            db.execute("SELECT set_config('statement_timeout', ?, true)", ('2000ms',))
        db.execute('SELECT 1').fetchone()
        _state.update(database_available=True, checked_at=time.time())
    except Exception:
        _state.update(database_available=False, checked_at=time.time())
    finally:
        if db:
            db.close()
        _lock.release()
    return dict(_state)
