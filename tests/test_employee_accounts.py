import sqlite3
from unittest.mock import patch
import pytest
from flask import Flask, g, jsonify
from database import DatabaseConnection, ensure_auth_schema
from security import install_security, auth_bp
import security
import employee_accounts as accounts
from store_identity import bind_database, assert_database

PASSWORD = 'An individual employee password 2026!'


@pytest.fixture
def store():
    raw = sqlite3.connect(':memory:', check_same_thread=False)
    raw.row_factory = sqlite3.Row
    raw.execute('PRAGMA foreign_keys=ON')
    db = DatabaseConnection(raw, 'sqlite')
    ensure_auth_schema(db)
    owner = accounts.save_account(db, {'login': 'owner', 'display_name': 'Owner', 'password': PASSWORD, 'role': 'owner'}, bootstrap=True)
    employee = accounts.save_account(db, {'login': 'employee', 'display_name': 'Employee', 'password': PASSWORD})
    viewer = accounts.save_account(db, {'login': 'viewer', 'display_name': 'Viewer', 'password': PASSWORD, 'role': 'viewer'})
    db.commit()
    app = Flask(__name__)
    app.testing = True
    app.config['AUTH_COOKIE_SECURE'] = True
    install_security(app)
    app.register_blueprint(auth_bp)
    @app.post('/api/test/write')
    def write():
        return jsonify({'username': g.auth_username})
    with patch('security.get_db', return_value=db):
        with security._rate_lock:
            security._login_failures.clear()
        yield app, db, owner, employee, viewer
    db.close()


def login(app, username='owner', password=PASSWORD):
    client = app.test_client()
    response = client.post('/api/auth/login', json={'username': username, 'password': password}, base_url='https://localhost')
    assert response.status_code == 200, response.json
    return client, response.json


def mutate(client, path, payload, csrf, method='POST'):
    return client.open(path, method=method, json=payload, base_url='https://localhost', headers={'X-CSRF-Token': csrf, 'Origin': 'https://localhost'})


def test_accounts_reject_shared_password_and_unknown_username(store):
    app, db, *_ = store
    with patch('security._verify_password') as legacy:
        result = app.test_client().post('/api/auth/login', json={'username':'unknown', 'password':PASSWORD}, base_url='https://localhost')
    assert result.status_code == 401
    legacy.assert_not_called()
    client, data = login(app, 'EMPLOYEE')
    assert data['username'] == 'employee'
    assert data['role'] == 'employee'
    assert data['account_mode'] is True
    status = client.get('/api/auth/status', base_url='https://localhost')
    assert status.json['display_name'] == 'Employee'
    raw = dict(db.execute("SELECT * FROM employee_accounts WHERE login='employee'").fetchone())
    assert raw['password_hash'] != PASSWORD and raw['password_hash'].startswith('scrypt:')


def test_employee_can_edit_but_cannot_administer_or_impersonate(store):
    app, *_ = store
    client, data = login(app, 'employee')
    token = data['csrf_token']
    assert mutate(client, '/api/test/write', {}, token).json['username'] == 'employee'
    assert client.get('/api/admin/employees', base_url='https://localhost').status_code == 403
    for path in ['/api/import', '/api/import/planogram-parse', '/api/gist/restore', '/api/reset']:
        assert mutate(client, path, {}, token).status_code == 403
    assert mutate(client, '/api/auth/profile', {'username':'owner'}, token).status_code == 403
    assert mutate(client, '/api/test/write', {}, 'wrong').status_code == 403


def test_viewer_cannot_write_and_account_password_can_change(store):
    app, *_ = store
    client, data = login(app, 'viewer')
    assert mutate(client, '/api/test/write', {}, data['csrf_token']).status_code == 403
    result = mutate(client, '/api/auth/password', {'current_password':PASSWORD, 'new_password':PASSWORD+' changed'}, data['csrf_token'])
    assert result.status_code == 200
    assert result.json['role'] == 'viewer'
    assert mutate(client, '/api/auth/logout', {}, result.json['csrf_token']).status_code == 200


def test_owner_manages_accounts_and_never_returns_password_hashes(store):
    app, db, owner, employee, viewer = store
    client, data = login(app)
    result = mutate(client, '/api/admin/employees', {'login':'new-user', 'display_name':'New User', 'password':PASSWORD, 'role':'employee'}, data['csrf_token'])
    assert result.status_code == 201
    assert 'password' not in result.text
    listed = client.get('/api/admin/employees', base_url='https://localhost')
    assert len(listed.json['employees']) == 4
    assert 'password' not in listed.text
    assert mutate(client, '/api/admin/employees/'+owner['id'], {'active':False}, data['csrf_token'], 'PATCH').status_code == 400
    assert mutate(client, '/api/admin/employees/'+owner['id'], {'role':'employee'}, data['csrf_token'], 'PATCH').status_code == 400
    assert mutate(client, '/api/admin/employees', {'login':'owner', 'display_name':'Duplicate', 'password':PASSWORD}, data['csrf_token']).status_code == 400


def test_deactivation_revokes_existing_session_immediately(store):
    app, db, owner, employee, _ = store
    employee_client, employee_data = login(app, 'employee')
    owner_client, owner_data = login(app)
    result = mutate(owner_client, '/api/admin/employees/'+employee['id'], {'active':False}, owner_data['csrf_token'], 'PATCH')
    assert result.status_code == 200
    assert mutate(employee_client, '/api/test/write', {}, employee_data['csrf_token']).status_code == 401
    assert employee_client.post('/api/auth/login', json={'username':'employee','password':PASSWORD}, base_url='https://localhost').status_code == 401
    assert accounts.enabled(db)


def test_account_errors_keep_validation_help_without_exposing_internal_details(store):
    app, db, owner, *_ = store
    client, data = login(app)
    response = mutate(client, '/api/admin/employees/'+owner['id'], {'active':False}, data['csrf_token'], 'PATCH')
    assert response.status_code == 400
    assert response.json['error'] == 'Conservez au moins un propriétaire actif.'

    def fail_after_write(*args):
        db.execute("UPDATE employee_accounts SET display_name='Uncommitted' WHERE id=?", (owner['id'],))
        raise ValueError('internal database path /private/store.sqlite and secret details')

    with patch('security.accounts.save_account', side_effect=fail_after_write):
        response = mutate(client, '/api/admin/employees', {}, data['csrf_token'])
    assert response.status_code == 500
    assert response.json['code'] == 'account_write_failed'
    assert 'private' not in response.text and 'secret' not in response.text
    assert db.execute('SELECT display_name FROM employee_accounts WHERE id=?', (owner['id'],)).fetchone()['display_name'] == 'Owner'


def test_password_change_revokes_other_sessions_but_not_other_employees(store):
    app, *_ = store
    first, first_data = login(app, 'employee')
    second, second_data = login(app, 'employee')
    owner, owner_data = login(app)
    result = mutate(first, '/api/auth/password', {'current_password':PASSWORD,'new_password':PASSWORD+' updated'}, first_data['csrf_token'])
    assert result.status_code == 200
    assert mutate(first, '/api/test/write', {}, result.json['csrf_token']).status_code == 200
    assert mutate(second, '/api/test/write', {}, second_data['csrf_token']).status_code == 401
    assert owner.get('/api/admin/employees', base_url='https://localhost').status_code == 200


def test_bootstrap_cannot_replace_existing_owner(store):
    _, db, *_ = store
    with pytest.raises(ValueError, match='existe'):
        accounts.save_account(db, {'login':'intruder','display_name':'Intruder','password':PASSWORD,'role':'owner'}, bootstrap=True)
    db.rollback()
    assert db.execute('SELECT COUNT(*) AS n FROM employee_accounts').fetchone()['n'] == 3


def test_store_binding_refuses_reassignment_and_browser_cannot_choose_database(store):
    _, db, *_ = store
    with patch.dict('os.environ', {'STORE_ID':'store-a'}):
        bind_database(db)
        db.commit()
        assert_database(db)
    with patch.dict('os.environ', {'STORE_ID':'store-b'}):
        with pytest.raises(RuntimeError, match='binding'):
            assert_database(db)
        with pytest.raises(RuntimeError, match='binding'):
            bind_database(db)
        db.rollback()
    assert db.execute("SELECT setting_value FROM app_settings WHERE setting_key='deployment_store_id_v1'").fetchone()['setting_value'] == 'store-a'


def test_sessions_are_not_accepted_by_another_store_database(store):
    app, db, *_ = store
    client, data = login(app)
    raw = sqlite3.connect(':memory:'); raw.row_factory = sqlite3.Row
    other = DatabaseConnection(raw, 'sqlite')
    ensure_auth_schema(other)
    try:
        with patch('security.get_db', return_value=other):
            response = mutate(client, '/api/test/write', {}, data['csrf_token'])
            assert response.status_code == 401
    finally:
        other.close()


def test_login_throttling_survives_worker_rate_cache_reset(store):
    app, db, *_ = store
    client = app.test_client()
    for _ in range(5):
        with security._rate_lock:
            security._login_failures.clear()
        result = client.post('/api/auth/login', json={'username':'employee','password':'wrong'}, base_url='https://localhost')
        assert result.status_code in {401, 429}
    with security._rate_lock:
        security._login_failures.clear()
    response = client.post('/api/auth/login', json={'username':'employee','password':PASSWORD}, base_url='https://localhost')
    assert response.status_code == 429
    assert int(response.headers['Retry-After']) > 0


def test_expiry_uses_server_store_identity_even_with_forged_client_store():
    from routes.expiry import _clean_store_key
    with patch.dict('os.environ', {'STORE_ID':'trusted-store'}):
        assert _clean_store_key('forged-store') == 'trusted-store'
        assert _clean_store_key(None) == 'trusted-store'
