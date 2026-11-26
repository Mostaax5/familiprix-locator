import os
import sqlite3
from unittest.mock import patch
import pytest
from database import DatabaseConnection, init_sqlite_db
from employee_accounts import save_account, authenticate
from store_identity import bind_database, assert_database
from backup_store import backup
from deploy.start import prepare


def test_private_full_backup_restores_accounts_upcs_and_store_binding(tmp_path):
    source = tmp_path/'source.sqlite'
    raw = sqlite3.connect(source); raw.row_factory=sqlite3.Row
    db = DatabaseConnection(raw, 'sqlite')
    init_sqlite_db(db)
    password='Store backup recovery password 2026!'
    with patch.dict(os.environ, {'STORE_ID':'restore-test'}):
        bind_database(db)
        save_account(db, {'login':'owner','display_name':'Owner','role':'owner','password':password}, bootstrap=True)
        db.execute("INSERT INTO products(name,barcode,aisle,side,shelf,position) VALUES (?,?,'1','Gauche','1','1')", ('Test product','001234567890'))
        db.commit()
        target=tmp_path/'snapshot.sqlite'
        with patch('backup_store.connect_db', return_value=db):
            result=backup(target)
        assert result['integrity']=='ok'
        assert target.stat().st_mode & 0o777 == 0o600
        restored_raw=sqlite3.connect(target); restored_raw.row_factory=sqlite3.Row
        restored=DatabaseConnection(restored_raw,'sqlite')
        try:
            assert_database(restored)
            assert authenticate(restored,'owner',password)['role']=='owner'
            assert restored.execute('SELECT barcode FROM products').fetchone()['barcode']=='001234567890'
        finally:
            restored.close()
    # Original source data was not overwritten or removed.
    assert source.exists()


def test_deployment_rejects_paid_ai_before_touching_database():
    with patch.dict(os.environ, {'STORE_ID':'test-store','STORE_NAME':'Test','AI_ALLOW_PAID':'1'}), patch('deploy.start.init_db') as initialize:
        with pytest.raises(RuntimeError, match='Paid AI'):
            prepare()
        initialize.assert_not_called()


def test_public_deployment_requires_https_and_secure_cookie():
    with patch.dict(os.environ, {'STORE_ID':'test-store','STORE_NAME':'Test','AI_ALLOW_PAID':'0',
                                'APP_EXTERNAL_URL':'http://store.example.com'}), patch('deploy.start.init_db') as initialize:
        with pytest.raises(RuntimeError, match='HTTPS'):
            prepare()
        initialize.assert_not_called()


def test_expiry_cards_never_reuse_unverified_stored_images():
    from routes.expiry import _product_from_rows, _status_payload
    row={'name':'Test','barcode':'001234567890','description':'Unproven','image_url':'https://example.com/wrong.jpg'}
    product=_product_from_rows([row],row['barcode'])
    assert product['image_url']=='' and product['description']==''
    status=_status_payload({'product_name':'Test','image_url':row['image_url']},product)
    assert status['image_url']==''


def test_store_startup_bootstraps_once_and_redeploy_never_resets_password(tmp_path):
    import database
    original='First owner deployment password 2026!'
    env={'STORE_ID':'deployment-test','STORE_NAME':'Test store','AI_ALLOW_PAID':'0',
         'APP_EXTERNAL_URL':'http://localhost:10000','INIT_OWNER_PASSWORD':original}
    with patch.dict(os.environ, env), patch.object(database,'DB_PATH',str(tmp_path/'store.sqlite')), patch.object(database,'DB_BACKEND','sqlite'):
        prepare()
        assert 'INIT_OWNER_PASSWORD' not in os.environ
        os.environ['INIT_OWNER_PASSWORD']='A different redeploy password 2026!'
        prepare()
        db=database.connect_db()
        try:
            assert authenticate(db,'owner',original)['role']=='owner'
            assert authenticate(db,'owner','A different redeploy password 2026!') is None
        finally:db.close()
        with patch.dict(os.environ, {'STORE_ID':'wrong-store'}):
            with pytest.raises(RuntimeError,match='binding'):
                database.connect_db()
