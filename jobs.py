"""Durable, leased planogram jobs shared by web and worker processes.

Jobs are at-least-once. Parsing is side-effect free; completion is fenced by a
unique lease token, so an expired worker cannot overwrite a newer attempt.
"""
import base64
import json
import os
import secrets
import tempfile
import threading
import time
from database import connect_db

LEASE_SECONDS = 300
MAX_ATTEMPTS = 3


def ensure_schema(db):
    db.execute('''CREATE TABLE IF NOT EXISTS background_jobs (
        id TEXT PRIMARY KEY, owner TEXT NOT NULL, status TEXT NOT NULL,
        payload TEXT NOT NULL, result TEXT NOT NULL DEFAULT '',
        created_at DOUBLE PRECISION NOT NULL, available_at DOUBLE PRECISION NOT NULL,
        lease_until DOUBLE PRECISION NOT NULL DEFAULT 0,
        lease_token TEXT NOT NULL DEFAULT '', attempts INTEGER NOT NULL DEFAULT 0
    )''')
    db.execute('CREATE INDEX IF NOT EXISTS idx_background_jobs_claim ON background_jobs(status, available_at, lease_until)')


def enqueue_pdf(job_id, owner, data):
    db = connect_db()
    try:
        now = time.time()
        db.execute('''INSERT INTO background_jobs
            (id,owner,status,payload,created_at,available_at) VALUES(?,?,'queued',?,?,?)''',
            (job_id, owner, base64.b64encode(data).decode('ascii'), now, now))
        db.commit()
    finally:
        db.close()


def status(job_id):
    db = connect_db()
    try:
        row = db.execute('SELECT owner,status,result,attempts FROM background_jobs WHERE id=?', (job_id,)).fetchone()
        if not row:
            return None
        row = dict(row)
        result = json.loads(row['result']) if row['result'] else {}
        return {**result, 'owner': row['owner'],
                'status': {'queued':'running','running':'running','done':'done','failed':'error'}[row['status']]}
    finally:
        db.close()


def claim():
    db = connect_db()
    try:
        now = time.time()
        # A final crashed attempt is terminal rather than permanently pending.
        db.execute("UPDATE background_jobs SET status='failed',payload='',result=? WHERE status='running' AND lease_until<? AND attempts>=?",
                   (json.dumps({'success':False,'error':'Analyse interrompue. Réessayez le fichier.'}), now, MAX_ATTEMPTS))
        row = db.execute("SELECT id FROM background_jobs WHERE attempts<? AND ((status='queued' AND available_at<=?) OR (status='running' AND lease_until<?)) ORDER BY created_at LIMIT 1", (MAX_ATTEMPTS,now,now)).fetchone()
        if not row:
            db.commit()
            return None
        job_id = dict(row)['id']
        token = secrets.token_hex(24)
        result = db.execute("UPDATE background_jobs SET status='running',lease_token=?,lease_until=?,attempts=attempts+1 WHERE id=? AND attempts<? AND ((status='queued' AND available_at<=?) OR (status='running' AND lease_until<?))",
                            (token,now+LEASE_SECONDS,job_id,MAX_ATTEMPTS,now,now))
        if result.rowcount != 1:
            db.rollback()
            return None
        row = dict(db.execute('SELECT * FROM background_jobs WHERE id=?', (job_id,)).fetchone())
        db.commit()
        return row
    finally:
        db.close()


def complete(job, result):
    db = connect_db()
    try:
        changed = db.execute("UPDATE background_jobs SET status=?,result=?,payload='',lease_until=0 WHERE id=? AND lease_token=? AND status='running'",
                             ('done' if result.get('success') else 'failed',json.dumps(result,ensure_ascii=False),job['id'],job['lease_token']))
        db.commit()
        return changed.rowcount == 1
    finally:
        db.close()


def _heartbeat(job, stop):
    while not stop.wait(30):
        db = None
        try:
            db = connect_db()
            db.execute("UPDATE background_jobs SET lease_until=? WHERE id=? AND lease_token=? AND status='running'", (time.time()+LEASE_SECONDS,job['id'],job['lease_token']))
            db.commit()
        except Exception:
            # The lease eventually expires and another worker safely retries.
            pass
        finally:
            if db:
                db.close()


def run_once():
    job = claim()
    if not job:
        return False
    stop = threading.Event()
    heartbeat = threading.Thread(target=_heartbeat,args=(job,stop),daemon=True)
    heartbeat.start()
    path = None
    try:
        from routes.import_export import _parse_planogram_file, _PDF_PARSE_LOCK
        from memory_guard import memory_intensive_task
        fd,path = tempfile.mkstemp(suffix='.pdf',prefix='familiprix-job-')
        with os.fdopen(fd,'wb') as output:
            output.write(base64.b64decode(job['payload']))
        with memory_intensive_task('planogram_pdf',priority=True), _PDF_PARSE_LOCK:
            products,meta,method = _parse_planogram_file(path)
        shelves = {}
        for product in products:
            key = str(product['tablette'])
            shelves[key] = shelves.get(key,0)+1
        complete(job,{'success':True,'products':products,'count':len(products),
                      'tablettes':shelves,'plano':meta,'parse_method':method})
    except Exception as exc:
        print(f'[JOB] parse failed id={job["id"]} error={type(exc).__name__}',flush=True)
        complete(job,{'success':False,'error':'Le PDF est invalide ou impossible à analyser.'})
    finally:
        stop.set()
        heartbeat.join(timeout=6)
        if path:
            os.unlink(path)
    return True


def serve(stop=None):
    stop = stop or threading.Event()
    while not stop.is_set():
        try:
            worked = run_once()
            # Terminal results are retained for one day; uploaded bytes are
            # removed on completion. Active jobs are never removed by retention.
            db = connect_db()
            try:
                db.execute("DELETE FROM background_jobs WHERE status IN ('done','failed') AND created_at<?", (time.time()-86400,))
                db.commit()
            finally:
                db.close()
        except Exception as exc:
            print(f'[JOB] worker unavailable: {type(exc).__name__}',flush=True)
            worked = False
        if not worked:
            stop.wait(2)


def start_embedded():
    if os.environ.get('JOB_EXECUTION_MODE','embedded') == 'embedded':
        threading.Thread(target=serve,daemon=True,name='durable-jobs').start()
