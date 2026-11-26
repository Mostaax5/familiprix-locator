"""Evaluate catalogue preservation and speed on a disposable SQLite copy.

No provider calls, no writes to the input database, no accuracy claim about
unlabelled photos/clinical facts. Run: python evaluate_catalogue.py backup.sqlite
"""
import argparse
import hashlib
import json
import sqlite3
import statistics
import tempfile
import time
from pathlib import Path

from database import DatabaseConnection
from catalogue.repository import ensure_schema
from catalogue.service import get_index


def evaluate(path):
    with tempfile.TemporaryDirectory(prefix='catalogue-evaluation-') as directory:
        original=sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True)
        raw=sqlite3.connect(Path(directory)/'evaluation.sqlite');raw.row_factory=sqlite3.Row
        original.backup(raw);original.close()
        db=DatabaseConnection(raw,'sqlite')
        try:
            source_rows=[dict(r) for r in db.execute('SELECT id,barcode,aisle,side,section,shelf,position FROM products ORDER BY id')]
            def placement_digest():
                data=[dict(r) for r in db.execute('SELECT id,barcode,aisle,side,section,shelf,position FROM products ORDER BY id')]
                return hashlib.sha256(json.dumps(data,sort_keys=True).encode()).hexdigest()
            before=placement_digest()
            ensure_schema(db)
            started=time.perf_counter();index=get_index(db);build_ms=(time.perf_counter()-started)*1000
            failures=0;times=[]
            for barcode in sorted({r['barcode'] for r in source_rows if r['barcode']}):
                started=time.perf_counter();result=index.search(barcode);times.append((time.perf_counter()-started)*1000)
                if not any(barcode in item['barcodes'] for item in result['items']):failures+=1
            mapping={r['placement_id']:r['barcode'] for r in db.execute('SELECT placement_id,barcode FROM catalogue_placements')}
            expected={r['id']:r['barcode'] for r in source_rows}
            updates=[]
            if source_rows:
                id=source_rows[0]['id']
                name=db.execute('SELECT name FROM products WHERE id=?',(id,)).fetchone()['name']
                for n in range(6):
                    db.execute('UPDATE products SET name=? WHERE id=?',(name+' evaluation '+str(n),id));db.commit()
                    started=time.perf_counter();get_index(db);updates.append((time.perf_counter()-started)*1000)
                db.execute('UPDATE products SET name=? WHERE id=?',(name,id));db.commit();index=get_index(db)
            queries=['Advil 200 mg','Tylenol 500 mg','shampoing','toothpaste','dentifrice menthe','vitamine C','crème hydratante','sans sucre','50 comprimés']
            return {'summary':index.summary(),'placement_digest_unchanged':before==placement_digest(),
                'exact_placement_upc_mapping_preserved':mapping==expected,'exact_upc_queries':len(times),'exact_upc_failures':failures,
                'cold_build_ms':round(build_ms,2),'median_search_ms':round(statistics.median(times),3) if times else 0,
                'p95_search_ms':round(sorted(times)[int(.95*(len(times)-1))],3) if times else 0,
                'median_single_product_refresh_ms':round(statistics.median(updates),3) if updates else 0,
                'first_page_json_bytes':len(json.dumps(index.search(),ensure_ascii=False).encode()),
                'examples':[{'query':q,'results':index.search(q)['total']} for q in queries],
                'limits':'Local SQLite/index measurements, not hosted latency or independently labelled product accuracy.'}
        finally:db.close()


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('database',type=Path);args=parser.parse_args()
    print(json.dumps(evaluate(args.database),ensure_ascii=False,indent=2))
