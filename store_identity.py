"""Bind an isolated app/database deployment to one server-selected store."""
import os
import re
import time

SETTING = 'deployment_store_id_v1'


def configured_store():
    store_id = os.environ.get('STORE_ID', '').strip()
    if store_id and not re.fullmatch(r'[a-z0-9][a-z0-9-]{1,63}', store_id):
        raise RuntimeError('STORE_ID must contain 2–64 lowercase letters, digits or hyphens')
    return {
        'id': store_id or 'richelieu',
        'name': os.environ.get('STORE_NAME', 'Familiprix Richelieu').strip()[:120],
        'address': os.environ.get('STORE_ADDRESS', '1111 Chemin des Patriotes, Richelieu, Quebec J3L 4W6').strip()[:250],
        'isolated': bool(store_id),
    }


def bind_database(db):
    store = configured_store()
    if not store['isolated']:
        return
    # ON CONFLICT never changes an existing binding. Two competing deployments
    # cannot reassign the database by changing an environment variable.
    db.execute('''INSERT INTO app_settings(setting_key,setting_value,updated_at)
        VALUES(?,?,?) ON CONFLICT(setting_key) DO NOTHING''', (SETTING, store['id'], int(time.time())))
    assert_database(db)


def assert_database(db):
    store = configured_store()
    if not store['isolated']:
        return
    row = db.execute('SELECT setting_value FROM app_settings WHERE setting_key=?', (SETTING,)).fetchone()
    if not row or row['setting_value'] != store['id']:
        raise RuntimeError('Store/database binding mismatch or missing migration; access refused')
