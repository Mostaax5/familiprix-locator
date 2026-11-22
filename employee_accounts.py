"""Individual employee identities on the existing server-session boundary.

One deployment/database belongs to one store. There is no public registration:
only an existing owner or the local bootstrap command can provision accounts.
"""
import re
import secrets
import time
from werkzeug.security import check_password_hash, generate_password_hash

ROLES = {'owner', 'employee', 'viewer'}
_DUMMY_HASH = generate_password_hash(secrets.token_urlsafe(32), method='scrypt:32768:8:1')


def ensure_schema(db):
    db.execute('''CREATE TABLE IF NOT EXISTS employee_accounts (
        id TEXT PRIMARY KEY, login TEXT NOT NULL UNIQUE, display_name TEXT NOT NULL,
        password_hash TEXT NOT NULL, role TEXT NOT NULL,
        active INTEGER NOT NULL DEFAULT 1, created_at BIGINT NOT NULL,
        updated_at BIGINT NOT NULL
    )''')
    db.execute('''CREATE TABLE IF NOT EXISTS employee_sessions (
        token_hash TEXT PRIMARY KEY REFERENCES auth_sessions(token_hash) ON DELETE CASCADE,
        account_id TEXT NOT NULL REFERENCES employee_accounts(id)
    )''')
    db.execute('''CREATE TABLE IF NOT EXISTS employee_login_limits (
        account_id TEXT PRIMARY KEY REFERENCES employee_accounts(id),
        failures INTEGER NOT NULL, window_start BIGINT NOT NULL
    )''')
    db.execute('CREATE INDEX IF NOT EXISTS idx_employee_sessions_account ON employee_sessions(account_id)')
    db.execute('''INSERT INTO app_settings(setting_key, setting_value, updated_at)
        VALUES ('employee_account_lock', '', 0) ON CONFLICT(setting_key) DO NOTHING''')


def enabled(db):
    # Disabled accounts still count: deactivation must never reopen shared login.
    return db.execute('SELECT id FROM employee_accounts LIMIT 1').fetchone() is not None


def login_key(value):
    value = str(value or '').strip().casefold()
    return value if re.fullmatch(r'[a-z0-9][a-z0-9._@+\-]{2,59}', value) else ''


def authenticate(db, login, password):
    row = db.execute('SELECT * FROM employee_accounts WHERE login=?', (login_key(login),)).fetchone()
    item = dict(row) if row else None
    valid = check_password_hash(item['password_hash'] if item else _DUMMY_HASH, password or '')
    return item if item and item['active'] and valid else None


def login_retry_after(db, login):
    row = db.execute("""SELECT l.failures, l.window_start FROM employee_login_limits l
        JOIN employee_accounts a ON a.id=l.account_id WHERE a.login=?""", (login_key(login),)).fetchone()
    if not row or row["failures"] < 5:
        return 0
    return max(0, int(row["window_start"]) + 900 - int(time.time()))


def record_login_failure(db, login):
    # Only existing accounts receive durable rows, bounding storage by team size.
    row = db.execute("SELECT id FROM employee_accounts WHERE login=?", (login_key(login),)).fetchone()
    if not row:
        return
    now = int(time.time())
    db.execute("""INSERT INTO employee_login_limits(account_id, failures, window_start)
        VALUES (?,1,?) ON CONFLICT(account_id) DO UPDATE SET
        failures=CASE WHEN employee_login_limits.window_start<=? THEN 1 ELSE employee_login_limits.failures+1 END,
        window_start=CASE WHEN employee_login_limits.window_start<=? THEN excluded.window_start ELSE employee_login_limits.window_start END""",
        (row["id"], now, now - 900, now - 900))
    db.commit()


def public_account(row):
    return {key: row[key] for key in ('id', 'login', 'display_name', 'role', 'active', 'created_at', 'updated_at')}


def lock_accounts(db):
    # A real row lock serializes last-owner checks across workers/PostgreSQL
    # transactions. On SQLite this obtains the write lock before the read.
    db.execute("UPDATE app_settings SET updated_at=? WHERE setting_key='employee_account_lock'", (int(time.time()),))


def save_account(db, payload, account_id=None, bootstrap=False):
    from security import password_validation_error, _normalize_password
    lock_accounts(db)
    old = db.execute('SELECT * FROM employee_accounts WHERE id=?', (account_id,)).fetchone() if account_id else None
    if account_id and not old:
        raise ValueError('Compte introuvable.')
    old = dict(old or {})
    if bootstrap and enabled(db):
        raise ValueError('Un propriétaire existe déjà. Utilisez la gestion des comptes.')
    login = login_key(payload.get('login', old.get('login')))
    name = str(payload.get('display_name', old.get('display_name', ''))).strip()
    role = payload.get('role', old.get('role', 'employee'))
    active = payload.get('active', bool(old.get('active', True)))
    if not login or not name or len(name) > 60 or any(ord(c) < 32 for c in name):
        raise ValueError('Identifiant de 3 à 60 caractères et nom de 1 à 60 caractères requis.')
    if role not in ROLES or not isinstance(active, bool):
        raise ValueError('Rôle ou état invalide.')
    if bootstrap and (role != 'owner' or not active):
        raise ValueError('Le premier compte doit être un propriétaire actif.')
    other = db.execute('SELECT id FROM employee_accounts WHERE login=?', (login,)).fetchone()
    if other and other['id'] != account_id:
        raise ValueError('Cet identifiant existe déjà.')
    if old.get('role') == 'owner' and old.get('active') and (role != 'owner' or not active):
        count = db.execute("SELECT COUNT(*) AS n FROM employee_accounts WHERE role='owner' AND active=1").fetchone()['n']
        if count <= 1:
            raise ValueError('Conservez au moins un propriétaire actif.')
    password = _normalize_password(payload.get('password'))
    if password or not old:
        error = password_validation_error(password)
        if error:
            raise ValueError(error)
        password_hash = generate_password_hash(password, method='scrypt:32768:8:1', salt_length=32)
    else:
        password_hash = old['password_hash']
    now = int(time.time())
    account_id = account_id or secrets.token_hex(16)
    if old:
        db.execute('''UPDATE employee_accounts SET login=?, display_name=?, role=?, active=?,
            password_hash=?, updated_at=? WHERE id=?''', (login, name, role, int(active), password_hash, now, account_id))
        db.execute('''UPDATE auth_sessions SET revoked_at=? WHERE token_hash IN
            (SELECT token_hash FROM employee_sessions WHERE account_id=?) AND revoked_at=0''', (now, account_id))
    else:
        db.execute('''INSERT INTO employee_accounts
            (id,login,display_name,password_hash,role,active,created_at,updated_at)
            VALUES (?,?,?,?,?,?,?,?)''', (account_id, login, name, password_hash, role, int(active), now, now))
    return public_account(dict(db.execute('SELECT * FROM employee_accounts WHERE id=?', (account_id,)).fetchone()))
