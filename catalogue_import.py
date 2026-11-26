"""Import an authorized manufacturer/supplier JSON feed without editing placements.

Usage: python catalogue_import.py feed.json --source manufacturer
Feed shape: [{"barcode":"...", "name":"...", "brand":"...", ...}].
Use only an actual manufacturer/supplier feed, not AI-generated assertions.
"""
import argparse
import json
import time
from pathlib import Path
from catalogue.acquisition import store_candidate
from catalogue.model import identity
from catalogue.repository import ensure_schema
from catalogue.service import get_index
from database import connect_db


def import_feed(db, rows, source):
    if source not in {'manufacturer', 'supplier', 'store_catalog'}:
        raise ValueError('Unsupported source authority')
    if not isinstance(rows, list) or len(rows) > 100000:
        raise ValueError('Expected a bounded list of product records')
    # Validate the entire feed before publishing anything.
    keys = set()
    for row in rows:
        if not isinstance(row, dict) or not isinstance(row.get('barcode'), str) or not row['barcode'].strip() or not isinstance(row.get('name'), str):
            raise ValueError('Each product requires a string barcode and name')
        if any(not isinstance(v, (str, int, float, type(None))) or len(str(v)) > 12000 for v in row.values()):
            raise ValueError('Invalid or oversized product field')
        key = identity(row)
        if key in keys:
            raise ValueError('Duplicate package identity in feed')
        keys.add(key)
    index = get_index(db)
    counts = {'accepted': 0, 'unresolved': 0, 'new_reference_products': 0}
    for row in rows:
        key = identity(row)
        doc = index.docs.get(key)
        if not doc:
            # A supplier catalogue may know products that are not placed in this
            # store. Keep them searchable without inventing a shelf or stock.
            db.execute('''INSERT INTO product_reference(barcode,name,source,product_code)
                VALUES(?,?,?,?) ON CONFLICT(barcode) DO NOTHING''',
                (row['barcode'], row['name'], source, str(row.get('product_code') or '')))
            counts['new_reference_products'] += 1
        candidate = {**row, 'source': {'manufacturer':'Manufacturer', 'supplier':'Supplier', 'store_catalog':'Familiprix'}[source]}
        existing = {'barcode': row['barcode'], 'name': (doc['original_labels'] or [doc['title']])[0] if doc else row['name']}
        accepted = store_candidate(db, key, existing, candidate, time.time())
        counts['accepted' if accepted else 'unresolved'] += 1
    db.commit()
    return counts


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('file', type=Path)
    parser.add_argument('--source', required=True, choices=['manufacturer', 'supplier', 'store_catalog'])
    args = parser.parse_args()
    if args.file.stat().st_size > 32 * 1024 * 1024:
        parser.error('Feed must be at most 32 MiB')
    data = json.loads(args.file.read_text())
    db = connect_db()
    try:
        ensure_schema(db)
        print(json.dumps(import_feed(db, data, args.source)))
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
