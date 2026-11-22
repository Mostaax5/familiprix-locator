"""Create a private, consistent full SQLite backup including employee accounts.

Usage: python backup_store.py /private/new-backup.sqlite
For PostgreSQL use a provider snapshot or pg_dump with a direct connection.
The app's JSON export covers business data, not employee credentials/sessions.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sqlite3
from database import connect_db


def backup(destination):
    path = Path(destination)
    db = connect_db()
    try:
        if db.backend != 'sqlite':
            raise ValueError('Use pg_dump or a provider snapshot for a complete PostgreSQL backup')
        # Exclusive creation and owner-only mode: never overwrite store data.
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        os.close(fd)
        try:
            target = sqlite3.connect(path)
            try:
                db.connection.backup(target)
                result = target.execute('PRAGMA integrity_check').fetchone()[0]
                if result != 'ok':
                    raise RuntimeError('Backup integrity check failed')
            finally:
                target.close()
        except Exception:
            path.unlink(missing_ok=True)
            raise
        return {'file':str(path), 'sha256':hashlib.sha256(path.read_bytes()).hexdigest(), 'integrity':'ok'}
    finally:
        db.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('destination')
    print(json.dumps(backup(parser.parse_args().destination), indent=2))
