"""Small application service; web requests never fetch remote product pages."""
import threading
import json
import time
from collections import OrderedDict

import database
from catalogue.repository import ensure_schema, state, refresh, documents
from catalogue.search import CatalogueIndex

_indices = OrderedDict()
_lock = threading.RLock()
_warm_lock = threading.Lock()
_warm = {'stage': 'idle', 'attempted_at': 0.0}


def readiness():
    from health import dependency_status
    status = dict(_warm)
    status['database_available'] = dependency_status()['database_available']
    status['ready'] = status['stage'] == 'ready' and status['database_available'] is True
    return status


def start_warmup():
    with _warm_lock:
        if _warm['stage'] in {'running', 'ready'} or time.monotonic() - _warm['attempted_at'] < 5:
            return
        _warm.update(stage='running', attempted_at=time.monotonic())
    def run():
        db = None
        try:
            db = database.connect_db()
            get_index(db)
            _warm['stage'] = 'ready'
        except Exception:
            _warm['stage'] = 'error'
        finally:
            if db:
                db.close()
    threading.Thread(target=run, daemon=True, name='catalogue-index-warmup').start()


def database_key(db):
    if db.backend == 'postgres':
        return ('postgres', database.DATABASE_URL)
    row = db.execute('PRAGMA database_list').fetchone()
    path = row['file']
    return ('sqlite', path or id(db.connection))


def get_index(db):
    key = database_key(db)
    with _lock:
        try:
            current = state(db)
        except Exception:
            db.rollback()
            ensure_schema(db)
            current = state(db)
        from catalogue.repository import VERSION
        if current['model_version'] != VERSION:
            ensure_schema(db)
            current = state(db)
        cached = _indices.get(key)
        if cached and cached.revision == current['revision'] and current['model_version'] == VERSION:
            return cached
        revision = refresh(db)
        if cached and current['model_version'] == VERSION and revision >= cached.revision:
            changed = [json.loads(r['document']) for r in db.execute(
                'SELECT document FROM catalogue_items WHERE updated_revision>?', (cached.revision,))]
            existing_keys = {r['product_key'] for r in db.execute('SELECT product_key FROM catalogue_items')}
            index = cached.with_updates(changed, existing_keys, revision)
        else:
            index = CatalogueIndex(documents(db), revision)
        _indices[key] = index
        _indices.move_to_end(key)
        while len(_indices) > 2:
            _indices.popitem(last=False)
        return index
