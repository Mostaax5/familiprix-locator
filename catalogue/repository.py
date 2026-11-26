"""Rebuildable normalized catalogue with transactional, cross-worker revisions.

The legacy table remains the placement write model while the new read model is
rolled out. Its triggers also catch imports, deletions and direct SQL updates;
timestamps and process-local counters are not used for invalidation.
"""
import json
import threading
from collections import defaultdict

from catalogue.model import build_item, digest, identity, stable_json
from product_data import exact_gtin_variants

_lock = threading.RLock()
VERSION = 8


def ensure_schema(db):
    db.execute('''CREATE TABLE IF NOT EXISTS catalogue_state (
        id INTEGER PRIMARY KEY, revision BIGINT NOT NULL DEFAULT 1,
        projected_revision BIGINT NOT NULL DEFAULT 0, model_version INTEGER NOT NULL DEFAULT 0)''')
    db.execute('INSERT INTO catalogue_state(id) VALUES(1) ON CONFLICT(id) DO NOTHING')
    db.execute('''CREATE TABLE IF NOT EXISTS catalogue_items (
        product_key TEXT PRIMARY KEY, content_hash TEXT NOT NULL, document TEXT NOT NULL)''')
    if db.backend == 'postgres':
        db.execute('ALTER TABLE catalogue_items ADD COLUMN IF NOT EXISTS updated_revision BIGINT NOT NULL DEFAULT 0')
    elif 'updated_revision' not in {r['name'] for r in db.execute('PRAGMA table_info(catalogue_items)')}:
        db.execute('ALTER TABLE catalogue_items ADD COLUMN updated_revision BIGINT NOT NULL DEFAULT 0')
    db.execute('''CREATE TABLE IF NOT EXISTS catalogue_dirty (
        kind TEXT NOT NULL, item_key TEXT NOT NULL, PRIMARY KEY(kind,item_key))''')
    db.execute('''CREATE TABLE IF NOT EXISTS catalogue_placements (
        placement_id BIGINT PRIMARY KEY, product_key TEXT NOT NULL, barcode TEXT NOT NULL)''')
    db.execute('CREATE INDEX IF NOT EXISTS idx_catalogue_placement_key ON catalogue_placements(product_key)')
    db.execute('''CREATE TABLE IF NOT EXISTS catalogue_sources (
        product_key TEXT NOT NULL, provider TEXT NOT NULL, payload TEXT NOT NULL,
        status TEXT NOT NULL, priority INTEGER NOT NULL DEFAULT 0,
        checked_at DOUBLE PRECISION NOT NULL, reason TEXT NOT NULL DEFAULT '',
        PRIMARY KEY(product_key, provider))''')
    db.execute('''CREATE TABLE IF NOT EXISTS catalogue_tasks (
        product_key TEXT PRIMARY KEY, barcode TEXT NOT NULL, status TEXT NOT NULL,
        available_at DOUBLE PRECISION NOT NULL, lease_until DOUBLE PRECISION NOT NULL DEFAULT 0,
        lease_token TEXT NOT NULL DEFAULT '', attempts INTEGER NOT NULL DEFAULT 0,
        last_error TEXT NOT NULL DEFAULT '')''')
    db.execute('''CREATE TABLE IF NOT EXISTS catalogue_observations (
        product_key TEXT NOT NULL, provider TEXT NOT NULL, content_hash TEXT NOT NULL,
        payload TEXT NOT NULL, status TEXT NOT NULL, first_seen DOUBLE PRECISION NOT NULL,
        last_seen DOUBLE PRECISION NOT NULL, PRIMARY KEY(product_key,provider,content_hash))''')
    db.execute('CREATE INDEX IF NOT EXISTS idx_catalogue_task_due ON catalogue_tasks(status,available_at,lease_until)')
    db.execute('''CREATE TABLE IF NOT EXISTS catalogue_acquisition_clock (
        id INTEGER PRIMARY KEY, last_claim DOUBLE PRECISION NOT NULL DEFAULT -1000)''')
    db.execute('INSERT INTO catalogue_acquisition_clock(id) VALUES(1) ON CONFLICT(id) DO NOTHING')
    tables = {'products': ('placement','id'), 'product_field_evidence': ('placement','product_id'),
              'product_reference': ('reference','barcode'), 'product_reference_evidence': ('reference','barcode'),
              'catalogue_sources': ('source','product_key')}
    if db.backend == 'postgres':
        db.execute('''CREATE OR REPLACE FUNCTION catalogue_changed() RETURNS trigger AS $$
            BEGIN UPDATE catalogue_state SET revision=revision+1 WHERE id=1;
            RETURN NULL; END; $$ LANGUAGE plpgsql''')
        for table, (kind,column) in tables.items():
            # Lock the revision row BEFORE changes, consistently with projection
            # publication. Row dirty markers then cannot deadlock publication.
            db.execute(f'DROP TRIGGER IF EXISTS catalogue_{table}_changed ON {table}')
            db.execute(f'''DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_trigger
                WHERE tgname='catalogue_{table}_revision_before' AND tgrelid='{table}'::regclass) THEN
                CREATE TRIGGER catalogue_{table}_revision_before BEFORE INSERT OR UPDATE OR DELETE ON {table}
                FOR EACH STATEMENT EXECUTE FUNCTION catalogue_changed(); END IF; END $$''')
            db.execute(f'''CREATE OR REPLACE FUNCTION catalogue_dirty_{table}() RETURNS trigger AS $$ BEGIN
                IF TG_OP <> 'DELETE' THEN INSERT INTO catalogue_dirty(kind,item_key)
                  VALUES('{kind}',COALESCE(NEW.{column}::text,'')) ON CONFLICT DO NOTHING; END IF;
                IF TG_OP <> 'INSERT' THEN INSERT INTO catalogue_dirty(kind,item_key)
                  VALUES('{kind}',COALESCE(OLD.{column}::text,'')) ON CONFLICT DO NOTHING; END IF;
                RETURN NULL; END; $$ LANGUAGE plpgsql''')
            db.execute(f'''DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_trigger
                WHERE tgname='catalogue_{table}_dirty' AND tgrelid='{table}'::regclass) THEN
                CREATE TRIGGER catalogue_{table}_dirty AFTER INSERT OR UPDATE OR DELETE ON {table}
                FOR EACH ROW EXECUTE FUNCTION catalogue_dirty_{table}(); END IF; END $$''')
    else:
        for table, (kind,column) in tables.items():
            for operation in ('INSERT', 'UPDATE', 'DELETE'):
                db.execute(f'DROP TRIGGER IF EXISTS catalogue_{table}_{operation.lower()}')
                markers = []
                for owner in (('NEW',) if operation == 'INSERT' else ('OLD',) if operation == 'DELETE' else ('OLD','NEW')):
                    markers.append(f"INSERT INTO catalogue_dirty(kind,item_key) VALUES('{kind}',COALESCE(CAST({owner}.{column} AS TEXT),'')) ON CONFLICT DO NOTHING;")
                db.execute(f'''CREATE TRIGGER catalogue_{table}_{operation.lower()}
                    AFTER {operation} ON {table} BEGIN
                    UPDATE catalogue_state SET revision=revision+1 WHERE id=1;
                    {''.join(markers)} END''')
    db.commit()


def state(db):
    return dict(db.execute('SELECT * FROM catalogue_state WHERE id=1').fetchone())


def select_in(db, table, column, values):
    values = list(values)
    output = []
    for start in range(0, len(values), 350):
        chunk = values[start:start + 350]
        output.extend(dict(r) for r in db.execute(
            f'SELECT * FROM {table} WHERE {column} IN (' + ','.join('?' for _ in chunk) + ')', tuple(chunk)))
    return output


def affected_keys(db):
    keys, ids = set(), set()
    for row in db.execute('SELECT kind,item_key FROM catalogue_dirty'):
        if row['kind'] == 'placement':
            ids.add(int(row['item_key']))
        elif row['kind'] == 'reference':
            keys.add(identity({'barcode': row['item_key'], 'id': 'reference:' + row['item_key']}))
        else:
            keys.add(row['item_key'])
    keys.update(r['product_key'] for r in select_in(db, 'catalogue_placements', 'placement_id', ids))
    keys.update(identity(r) for r in select_in(db, 'products', 'id', ids))
    return keys


def package_rows(db, keys):
    barcodes, ids = set(), set()
    for key in keys:
        prefix, value = key.split(':', 1)
        if prefix == 'gtin':
            barcodes.update(exact_gtin_variants(value))
        elif prefix in {'raw', 'code'}:
            barcodes.add(value)
        elif value.startswith('reference:'):
            barcodes.add(value.removeprefix('reference:'))
        else:
            ids.add(int(value))
    placed = select_in(db, 'products', 'barcode', {b for b in barcodes if b}) + select_in(db, 'products', 'id', ids)
    refs = select_in(db, 'product_reference', 'barcode', barcodes)
    return placed, refs, barcodes


def refresh(db):
    """Publish a complete new read model atomically; no products are filtered out."""
    current = state(db)
    if current['projected_revision'] == current['revision'] and current['model_version'] == VERSION:
        return current['revision']
    with _lock:
        # Serialize publication with other processes, not just this thread.
        db.execute('UPDATE catalogue_state SET projected_revision=projected_revision WHERE id=1')
        current = state(db)
        if current['projected_revision'] == current['revision'] and current['model_version'] == VERSION:
            db.commit()
            return current['revision']
        revision = current['revision']
        full = current['model_version'] != VERSION or current['projected_revision'] == 0
        changed_keys = None if full else affected_keys(db)
        groups, refs, evidence, sources = (defaultdict(list) for _ in range(4))
        if full:
            placements = [dict(r) for r in db.execute('SELECT * FROM products ORDER BY id')]
            reference_rows = [dict(r) for r in db.execute('SELECT * FROM product_reference')]
            field_rows = [dict(r) for r in db.execute('SELECT * FROM product_field_evidence WHERE active=1')]
            reference_evidence = [dict(r) for r in db.execute('SELECT * FROM product_reference_evidence WHERE active=1')]
            source_rows = [dict(r) for r in db.execute('SELECT * FROM catalogue_sources')]
            previous_rows = db.execute('SELECT product_key,content_hash FROM catalogue_items')
        else:
            placements, reference_rows, barcodes = package_rows(db, changed_keys)
            field_rows = select_in(db, 'product_field_evidence', 'product_id', {r['id'] for r in placements})
            reference_evidence = select_in(db, 'product_reference_evidence', 'barcode', barcodes)
            source_rows = select_in(db, 'catalogue_sources', 'product_key', changed_keys)
            previous_rows = select_in(db, 'catalogue_items', 'product_key', changed_keys)
        placement_keys = {}
        for row in placements:
            key = identity(row)
            groups[key].append(row)
            placement_keys[row['id']] = key
        for row in field_rows:
            row = dict(row)
            key = placement_keys.get(row['product_id'])
            if key:
                evidence[key].append(row)
        for row in reference_rows:
            row = dict(row)
            # Barcode-less reference rows are not a package identity and do not
            # get merged into a placed item. Preserve their separate source key.
            row.setdefault('id', 'reference:' + str(row.get('barcode', '')))
            key = identity(row)
            refs[key].append(row)
        for row in reference_evidence:
            row = dict(row)
            if row.get('barcode'):
                key = identity(row)
                evidence[key].append(row)
        for row in source_rows:
            row = dict(row)
            sources[row['product_key']].append(row)
        existing = {r['product_key']: r['content_hash'] for r in previous_rows}
        keys = set(groups) | set(refs)
        updates = []
        for key in sorted(keys):
            doc = build_item(key, groups[key], evidence[key], refs[key], sources[key])
            hashed = digest(doc)
            if full or existing.get(key) != hashed:
                updates.append((key, hashed, stable_json(doc), revision))
        if updates:
            db.executemany('''INSERT INTO catalogue_items(product_key,content_hash,document,updated_revision) VALUES(?,?,?,?)
                ON CONFLICT(product_key) DO UPDATE SET content_hash=excluded.content_hash,document=excluded.document,
                updated_revision=excluded.updated_revision''', updates)
        for key in set(existing) - keys:
            db.execute('DELETE FROM catalogue_items WHERE product_key=?', (key,))
        if full:
            db.execute('DELETE FROM catalogue_placements')
        else:
            for key in changed_keys:
                db.execute('DELETE FROM catalogue_placements WHERE product_key=?', (key,))
        if placements:
            db.executemany('INSERT INTO catalogue_placements(placement_id,product_key,barcode) VALUES(?,?,?)',
                [(r['id'], placement_keys[r['id']], str(r.get('barcode') or '')) for r in placements])
        count = db.execute('SELECT COUNT(*) AS n FROM catalogue_placements').fetchone()['n']
        expected = db.execute('SELECT COUNT(*) AS n FROM products').fetchone()['n']
        if count != expected:
            raise RuntimeError('Catalogue projection lost placements')
        db.execute('DELETE FROM catalogue_dirty')
        db.execute('UPDATE catalogue_state SET projected_revision=?,model_version=? WHERE id=1', (revision, VERSION))
        db.commit()
        return revision


def documents(db):
    return [json.loads(r['document']) for r in db.execute('SELECT document FROM catalogue_items ORDER BY product_key')]
