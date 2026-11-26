"""Recover a public product-cache response into NEW private local artifacts.

Never connects to production. Original bytes and metadata are preserved. Only
observed slots can inform reconstructed layouts; empty fixtures/labels/history
are unknown. This is partial cache recovery, not a complete database restore.
"""
import argparse
from collections import Counter, defaultdict
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
from database import DatabaseConnection, init_sqlite_db
from domain.layout import normalize_layout_config, layout_metrics, valid_aisle_name
from product_backup import PRODUCT_RESTORE_COLUMNS
from product_data import gtin_identity_key
from rebuild_catalogue import rebuild
from store_identity import SETTING


def observed_layouts(products):
    grouped = defaultdict(list)
    for item in products:
        if not valid_aisle_name(item.get('aisle')):
            raise ValueError('Invalid aisle in cached location')
        grouped[str(item['aisle'])].append(item)
    layouts=[]
    for aisle, rows in grouped.items():
        config={'sides':{'Gauche':{'sections':[]},'Droite':{'sections':[]}},'facade_a':{'shelves':[]},'facade_b':{'shelves':[]},'presentoirs':[]}
        for row in rows:
            side=str(row['side'])
            try:
                section, shelf, position = (int(row.get(k,1)) for k in ('section','shelf','position'))
            except (ValueError,TypeError):
                raise ValueError('Non-numeric cached position; original retained but automatic recovery refused')
            if not (1<=section<=200 and 1<=shelf<=100 and 1<=position<=500):
                raise ValueError('Cached position exceeds supported layout limits')
            if side in {'Gauche','Droite'}:
                sections=config['sides'][side]['sections']
                while len(sections)<section: sections.append({'shelves':[]})
                shelves=sections[section-1]['shelves']
            elif side in {'Façade A','Façade B'}:
                shelves=config['facade_a' if side=='Façade A' else 'facade_b']['shelves']
            else:
                raise ValueError('Unrecognized fixture; automatic reconstruction refused')
            while len(shelves)<shelf:shelves.append(0)
            shelves[shelf-1]=max(shelves[shelf-1],position)
        config=normalize_layout_config(config,0,0,0)
        section,shelf,position=layout_metrics(config)
        layouts.append({'aisle':aisle,'config_json':json.dumps(config,ensure_ascii=False),'max_section':str(section),
                        'max_shelf':str(shelf),'max_position':str(position),'enabled':1,'sort_order':len(layouts)})
    return layouts


def recover(source, directory, store_id):
    if not re.fullmatch(r'[a-z0-9][a-z0-9-]{1,63}',store_id):
        raise ValueError('Invalid store ID')
    raw=Path(source).read_bytes()
    rows=json.loads(raw)
    if not isinstance(rows,list) or not rows or len(rows)>50_000:
        raise ValueError('Expected a nonempty product-cache array of at most 50000 rows')
    for row in rows:
        if not isinstance(row,dict) or not isinstance(row.get('barcode'),str) or not row['barcode']:
            raise ValueError('Every recovered UPC must be a nonempty string')
        if not isinstance(row.get('id'),int) or row['id']<=0:
            raise ValueError('Every cached row must have a positive original ID')
    if len({r['id'] for r in rows})!=len(rows):
        raise ValueError('Duplicate original product IDs')
    target=Path(directory);target.mkdir(parents=True,exist_ok=False,mode=0o700)
    (target/'original-cache.json').write_bytes(raw)
    payload={'export_version':2,'products':rows,'aisle_layouts':observed_layouts(rows),'product_data':{},
             'recovery_metadata':{'partial':True,'layouts':'Reconstructed from observed occupied slots only; labels and empty fixtures unknown',
                                  'missing':['employee accounts','expiry history','field evidence','unplaced reference catalogue','original layout configuration','operational history']}}
    cleaned,quarantine=rebuild(payload)
    (target/'recovered-catalogue.json').write_text(json.dumps(cleaned,ensure_ascii=False))
    (target/'quarantine.json').write_text(json.dumps(quarantine,ensure_ascii=False))
    destination=target/'recovered.sqlite'
    fd=os.open(destination,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600);os.close(fd)
    conn=sqlite3.connect(destination);conn.row_factory=sqlite3.Row
    db=DatabaseConnection(conn,'sqlite')
    try:
        init_sqlite_db(db)
        db.execute('INSERT INTO app_settings(setting_key,setting_value,updated_at) VALUES(?,?,0)',(SETTING,store_id))
        for row in cleaned['products']:
            values={k:v for k,v in row.items() if k in PRODUCT_RESTORE_COLUMNS}
            values.update(id=row['id'],gtin_key=gtin_identity_key(row['barcode']),modified_at=row.get('last_change_at',''),modified_by=row.get('last_change_by',''))
            columns=list(values)
            db.execute(f"INSERT INTO products({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})",tuple(values[k] for k in columns))
        for layout in cleaned['aisle_layouts']:
            columns=list(layout)
            db.execute(f"INSERT INTO aisle_layouts({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})",tuple(layout[k] for k in columns))
        db.commit()
        actual=[dict(r) for r in db.execute('SELECT id,barcode,aisle,side,section,shelf,position FROM products').fetchall()]
        fields=('id','barcode','aisle','side','section','shelf','position')
        signature=lambda row:tuple(str(row.get(k,'')) for k in fields)
        if Counter(map(signature,rows))!=Counter(map(signature,actual)):
            raise RuntimeError('Recovered UPC/location comparison failed')
        if db.execute('PRAGMA integrity_check').fetchone()[0]!='ok':
            raise RuntimeError('Recovered database integrity check failed')
    finally:
        db.close()
    manifest={'partial_recovery':True,'store_id':store_id,'products':len(rows),'distinct_upcs':len({r['barcode'] for r in rows}),
              'reconstructed_aisles':len(cleaned['aisle_layouts']),'quarantined_fields':len(quarantine),
              'upc_location_comparison':'exact','original_id_comparison':'exact','database_integrity':'ok',
              'missing':payload['recovery_metadata']['missing'],
              'sha256':{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in target.iterdir() if p.is_file()}}
    (target/'manifest.json').write_text(json.dumps(manifest,indent=2))
    return manifest


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('cache_json');parser.add_argument('new_private_directory');parser.add_argument('--store-id',required=True)
    args=parser.parse_args()
    print(json.dumps(recover(args.cache_json,args.new_private_directory,args.store_id),indent=2))
