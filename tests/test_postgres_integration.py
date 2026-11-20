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
