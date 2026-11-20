"""Explicit idempotent schema preparation: python migrate.py."""
from database import init_db

if __name__ == '__main__':
    init_db()
