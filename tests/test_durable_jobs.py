import sqlite3
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch
from database import DatabaseConnection
import jobs


class DurableJobTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.path = str(Path(self.directory.name) / 'jobs.db')
        self.connection = patch('jobs.connect_db', side_effect=self.connect)
        self.connection.start()
        db = self.connect()
        jobs.ensure_schema(db)
        db.commit()
        db.close()

    def connect(self):
        connection = sqlite3.connect(self.path, timeout=5)
        connection.row_factory = sqlite3.Row
        return DatabaseConnection(connection, 'sqlite')

    def tearDown(self):
        self.connection.stop()
        self.directory.cleanup()

    def test_payload_survives_new_connection_and_only_one_claim_wins(self):
        jobs.enqueue_pdf('job', 'owner', b'%PDF-test')
        first = jobs.claim()
        self.assertEqual(first['owner'], 'owner')
        self.assertIsNone(jobs.claim())
        self.assertEqual(jobs.status('job')['status'], 'running')
        self.assertTrue(jobs.complete(first, {'success': True, 'products': []}))
        self.assertEqual(jobs.status('job')['status'], 'done')
        db = self.connect()
        self.assertEqual(db.execute('SELECT payload FROM background_jobs').fetchone()['payload'], '')
        db.close()

    def test_crashed_job_is_reclaimed_and_old_completion_is_fenced(self):
        jobs.enqueue_pdf('job','owner',b'%PDF-test')
        old = jobs.claim()
        db = self.connect()
        db.execute('UPDATE background_jobs SET lease_until=?', (time.time()-1,))
        db.commit()
        db.close()
        new = jobs.claim()
        self.assertEqual(new['attempts'],2)
        self.assertNotEqual(old['lease_token'],new['lease_token'])
        self.assertFalse(jobs.complete(old,{'success':True,'products':['old']}))
        self.assertTrue(jobs.complete(new,{'success':True,'products':['new']}))
        self.assertEqual(jobs.status('job')['products'],['new'])

    def test_restart_retry_exhaustion_is_terminal(self):
        jobs.enqueue_pdf('job','owner',b'%PDF-test')
        db = self.connect()
        db.execute("UPDATE background_jobs SET status='running',lease_until=0,attempts=3")
        db.commit()
        db.close()
        self.assertIsNone(jobs.claim())
        self.assertEqual(jobs.status('job')['status'],'error')

    def test_parser_result_is_persisted_and_upload_removed(self):
        jobs.enqueue_pdf('job','owner',b'%PDF-test')
        with patch('routes.import_export._parse_planogram_file',return_value=([{'tablette':'2'}],{},'fixture')):
            self.assertTrue(jobs.run_once())
        result=jobs.status('job')
        self.assertEqual(result['status'],'done')
        self.assertEqual(result['tablettes'],{'2':1})
