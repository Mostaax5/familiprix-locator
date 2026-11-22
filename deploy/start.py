"""Validated entrypoint for new isolated store deployments."""
import os
import sys
from pathlib import Path
from urllib.parse import urlsplit
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from database import connect_db, init_db
from employee_accounts import enabled, save_account
from store_identity import configured_store


def prepare():
    store = configured_store()
    if not store['isolated'] or not os.environ.get('STORE_NAME', '').strip():
        raise RuntimeError('Set STORE_ID and STORE_NAME for this store deployment')
    if os.environ.get('AI_ALLOW_PAID', '0') != '0':
        raise RuntimeError('Paid AI is prohibited by this deployment profile')
    origin = os.environ.get('APP_EXTERNAL_URL') or os.environ.get('RENDER_EXTERNAL_URL', '')
    parsed = urlsplit(origin)
    if not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment or parsed.path not in ('', '/'):
        raise RuntimeError('Configure an exact APP_EXTERNAL_URL origin for this deployment')
    if parsed.hostname not in {'localhost', '127.0.0.1', '::1'}:
        if parsed.scheme != 'https' or os.environ.get('AUTH_COOKIE_SECURE', '1') != '1':
            raise RuntimeError('Public deployments require HTTPS and secure cookies')
    elif parsed.scheme not in {'http', 'https'}:
        raise RuntimeError('Unsupported application URL scheme')
    init_db()
    db = connect_db()
    try:
        password = os.environ.pop('INIT_OWNER_PASSWORD', '')
        if not enabled(db):
            secret_file = os.environ.get('INIT_OWNER_PASSWORD_FILE', '')
            if secret_file:
                password = Path(secret_file).read_text().rstrip('\r\n')
            if not password:
                raise RuntimeError('Provide the first owner password using a deployment secret')
            save_account(db, {'login': os.environ.get('INIT_OWNER_LOGIN', 'owner'),
                              'display_name': os.environ.get('INIT_OWNER_NAME', 'Propriétaire'),
                              'role': 'owner', 'password': password}, bootstrap=True)
            db.commit()
        # Existing identities are never reset by a redeploy or old bootstrap secret.
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


if __name__ == '__main__':
    prepare()
    os.execvp('gunicorn', ['gunicorn', 'app:app'])
