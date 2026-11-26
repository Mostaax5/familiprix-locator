"""Dedicated zero-cost catalogue acquisition worker: python catalogue_worker.py."""
import signal
import threading
from database import init_db, connect_db
from catalogue.repository import ensure_schema
from catalogue.acquisition import serve

if __name__ == '__main__':
    init_db()
    db = connect_db()
    try:
        ensure_schema(db)
    finally:
        db.close()
    stop = threading.Event()
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda *_: stop.set())
    serve(stop)
