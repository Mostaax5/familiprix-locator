"""Resumable, single-package source acquisition outside the search request.

No per-placement fanout and no description generation. Leases fence stale
workers; missing sources retry after a day, failures use bounded backoff.
"""
import json
import secrets
import threading
import time

from database import connect_db
from product_data import assess_metadata_candidate, canonical_gtin
from catalogue.model import identity, stable_json, digest
from catalogue.repository import ensure_schema


def schedule(db, now=None):
    now = time.time() if now is None else now
    rows = db.execute('SELECT product_key,document FROM catalogue_items').fetchall()
    values = []
    for row in rows:
        doc = json.loads(row['document'])
        code = next((b for b in doc['barcodes'] if canonical_gtin(b)), '')
        if code:
            values.append((doc['key'], code, now))
    if values:
        db.executemany('''INSERT INTO catalogue_tasks(product_key,barcode,status,available_at)
            VALUES(?,?,'queued',?) ON CONFLICT(product_key) DO NOTHING''', values)
    db.commit()
    return len(values)


def claim(db, now=None):
    now = time.time() if now is None else now
    row = db.execute('''SELECT * FROM catalogue_tasks WHERE available_at<=?
        AND (status IN ('queued','done','missing','failed') OR (status='running' AND lease_until<?))
        ORDER BY available_at,product_key LIMIT 1''', (now, now)).fetchone()
    if not row:
        db.commit()
        return None
    # One global acquisition slot every 30 seconds, regardless of worker count.
    if db.execute('UPDATE catalogue_acquisition_clock SET last_claim=? WHERE id=1 AND last_claim<=?',
                  (now, now - 30)).rowcount != 1:
        db.rollback()
        return None
    token = secrets.token_hex(24)
    changed = db.execute('''UPDATE catalogue_tasks SET status='running',lease_token=?,lease_until=?,attempts=attempts+1
        WHERE product_key=? AND available_at<=? AND (status<>'running' OR lease_until<?)''',
        (token, now + 180, row['product_key'], now, now))
    if changed.rowcount != 1:
        db.rollback()
        return None
    job = dict(db.execute('SELECT * FROM catalogue_tasks WHERE product_key=?', (row['product_key'],)).fetchone())
    db.commit()
    return job


def store_candidate(db, key, existing, candidate, now):
    if not isinstance(candidate, dict) or not candidate.get('barcode') or identity(candidate) != key:
        return False
    assessment = assess_metadata_candidate(existing, candidate, 'exact_gtin')
    provider_name = str(candidate.get('source') or 'Unknown')[:100]
    # Source status and every original field are retained even for conflicts.
    status = 'accepted' if assessment.auto_apply else 'unresolved'
    db.execute('''INSERT INTO catalogue_observations(product_key,provider,content_hash,payload,status,first_seen,last_seen)
        VALUES(?,?,?,?,?,?,?) ON CONFLICT(product_key,provider,content_hash) DO UPDATE SET last_seen=excluded.last_seen''',
        (key, provider_name, digest(candidate), stable_json(candidate), status, now, now))
    db.execute('''INSERT INTO catalogue_sources(product_key,provider,payload,status,priority,checked_at,reason)
        VALUES(?,?,?,?,?,?,?) ON CONFLICT(product_key,provider) DO UPDATE SET
        payload=excluded.payload,status=excluded.status,priority=excluded.priority,
        checked_at=excluded.checked_at,reason=excluded.reason''',
        (key, provider_name, stable_json(candidate), status, assessment.source_priority, now,
         stable_json(assessment.issues)))
    return assessment.auto_apply


def fetch_sources(barcode, product_code=''):
    # Reuse tested entity-bound parsers, not the former multi-site fanout.
    from routes.ai import lookup_familiprix_product, lookup_open_facts_product
    preferred = lookup_familiprix_product(barcode, [barcode], product_code=product_code)
    if preferred:
        return [preferred]
    community = lookup_open_facts_product('Open Food Facts', 'https://world.openfoodfacts.org', barcode)
    return [community] if community else []


def run_once(fetcher=fetch_sources):
    db = connect_db()
    try:
        job = claim(db)
        if not job:
            return False
        row = db.execute('SELECT document FROM catalogue_items WHERE product_key=?', (job['product_key'],)).fetchone()
        db.commit()
        doc = json.loads(row['document']) if row else None
    finally:
        db.close()
    error = ''
    candidates = []
    try:
        if doc:
            candidates = fetcher(job['barcode'], product_code=doc['store_codes'][0] if len(doc['store_codes']) == 1 else '')
    except Exception as exc:
        error = type(exc).__name__  # Never persist credentials/URLs in error logs.
    db = connect_db()
    try:
        now = time.time()
        # Lock and fence before publishing any source or changing its status.
        changed = db.execute('''UPDATE catalogue_tasks SET lease_until=lease_until WHERE product_key=?
            AND lease_token=? AND status='running' AND lease_until>?''', (job['product_key'], job['lease_token'], now))
        if changed.rowcount != 1:
            db.rollback()
            return True
        accepted = False
        if doc:
            existing = {'barcode': job['barcode'], 'name': (doc['original_labels'] or [doc['title']])[0]}
            for candidate in candidates[:3]:
                accepted = store_candidate(db, job['product_key'], existing, candidate, now) or accepted
        delay = min(86400, 300 * 2 ** min(job['attempts'], 8)) if error else (30 * 86400 if accepted else 86400)
        db.execute('''UPDATE catalogue_tasks SET status=?,available_at=?,lease_until=0,last_error=?
            WHERE product_key=? AND lease_token=?''',
            ('failed' if error else 'done' if accepted else 'missing', now + delay, error, job['product_key'], job['lease_token']))
        db.commit()
        return True
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def serve(stop=None):
    stop = stop or threading.Event()
    last_schedule = 0
    while not stop.is_set():
        try:
            # Updating the projection also discovers new imports automatically.
            from catalogue.service import get_index
            db = connect_db()
            try:
                get_index(db)
                if time.time() - last_schedule >= 300:
                    schedule(db)
                    last_schedule = time.time()
            finally:
                db.close()
            run_once()
        except Exception as exc:
            print('[CATALOGUE] acquisition delayed:', type(exc).__name__, flush=True)
        stop.wait(30)


def start_embedded():
    import os
    if os.environ.get('CATALOGUE_SOURCE_WORKER', '1') != '1':
        return
    threading.Thread(target=serve, daemon=True, name='catalogue-acquisition').start()
