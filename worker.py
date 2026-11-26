"""Run with `python worker.py`; no web application import or startup side effects."""
import signal
import threading
from database import init_db
from jobs import serve

if __name__ == '__main__':
    init_db()
    stop = threading.Event()
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda *_: stop.set())
    serve(stop)
