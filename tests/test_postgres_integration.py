"""Runs only against an explicitly provided disposable PostgreSQL test database."""
import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch
import pytest
import psycopg
from psycopg.rows import dict_row
from database import DatabaseConnection
import jobs


@pytest.fixture
def postgres_jobs():
    url = os.environ.get('TEST_POSTGRES_URL')
    if not url:
        pytest.skip('TEST_POSTGRES_URL is not set')
    schema = 'test_' + uuid.uuid4().hex
    admin = psycopg.connect(url, autocommit=True)
    admin.execute(psycopg.sql.SQL('CREATE SCHEMA {}').format(psycopg.sql.Identifier(schema)))
    def connect():
        conn = psycopg.connect(url, row_factory=dict_row)
        conn.execute(psycopg.sql.SQL('SET search_path TO {}').format(psycopg.sql.Identifier(schema)))
        conn.commit()
        return DatabaseConnection(conn, 'postgres')
    try:
        db=connect()
        jobs.ensure_schema(db)
        db.commit()
        db.close()
        with patch('jobs.connect_db',side_effect=connect):
            yield connect
    finally:
        admin.execute(psycopg.sql.SQL('DROP SCHEMA {} CASCADE').format(psycopg.sql.Identifier(schema)))
        admin.close()


def test_postgres_competing_claims_have_one_owner(postgres_jobs):
    jobs.enqueue_pdf('job','owner',b'%PDF-fixture')
    with ThreadPoolExecutor(max_workers=4) as pool:
        claimed=list(pool.map(lambda _:jobs.claim(),range(4)))
    owners=[job for job in claimed if job]
    assert len(owners)==1
    assert jobs.complete(owners[0],{'success':True})
    assert jobs.status('job')['status']=='done'


def test_postgres_conditional_layout_update_rejects_stale_writer(postgres_jobs):
    db=postgres_jobs()
    db.execute('CREATE TABLE layout (id INTEGER PRIMARY KEY, version TEXT, label TEXT)')
    db.execute("INSERT INTO layout VALUES(1,'v1','original')")
    db.commit()
    first=postgres_jobs()
    second=postgres_jobs()
    try:
        assert first.execute("UPDATE layout SET version='v2',label='first' WHERE id=1 AND version='v1'").rowcount==1
        first.commit()
        assert second.execute("UPDATE layout SET version='v3',label='stale' WHERE id=1 AND version='v1'").rowcount==0
        second.rollback()
        assert db.execute('SELECT label FROM layout').fetchone()['label']=='first'
    finally:
        first.close()
        second.close()
        db.close()


def test_postgres_concurrent_owner_removal_preserves_one_owner(postgres_jobs):
    import database
    import employee_accounts as accounts
    db = postgres_jobs()
    with patch.object(database, '_POSTGRES_AUTH_SCHEMA_READY', False):
        database.ensure_auth_schema(db)
    owners = [accounts.save_account(db, {'login':name,'display_name':name,'role':'owner',
              'password':'Disposable PostgreSQL owner phrase 2026!'}) for name in ['owner-one','owner-two']]
    db.commit()
    def deactivate(account):
        connection = postgres_jobs()
        try:
            accounts.save_account(connection, {'active':False}, account['id'])
            connection.commit()
            return True
        except ValueError:
            connection.rollback()
            return False
        finally:
            connection.close()
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(deactivate, owners))
        assert sorted(results) == [False, True]
        assert db.execute("SELECT COUNT(*) AS n FROM employee_accounts WHERE role='owner' AND active=1").fetchone()['n'] == 1
    finally:
        db.close()


def test_postgres_store_binding_cannot_be_reassigned(postgres_jobs):
    import database
    from store_identity import bind_database, assert_database
    db = postgres_jobs()
    try:
        with patch.object(database, '_POSTGRES_AUTH_SCHEMA_READY', False):
            database.ensure_auth_schema(db)
        with patch.dict(os.environ, {'STORE_ID':'first-store'}):
            bind_database(db)
            db.commit()
            assert_database(db)
        with patch.dict(os.environ, {'STORE_ID':'second-store'}):
            with pytest.raises(RuntimeError, match='binding'):
                bind_database(db)
            db.rollback()
        with patch.dict(os.environ, {'STORE_ID':'first-store'}):
            assert_database(db)
    finally:
        db.close()
