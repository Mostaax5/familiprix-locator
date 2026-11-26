import json
import sqlite3
from unittest.mock import patch
from flask import Flask
from database import DatabaseConnection, init_sqlite_db
from routes.import_export import import_export_bp


def test_version_two_export_restores_layout_order_and_product():
    raw=sqlite3.connect(':memory:')
    raw.row_factory=sqlite3.Row
    db=DatabaseConnection(raw,'sqlite')
    init_sqlite_db(db)
    db.execute("INSERT INTO aisle_layouts(aisle,sort_order,max_section,max_shelf,max_position,enabled) VALUES('A',7,'1','1','2',1)")
    db.execute("INSERT INTO products(name,barcode,aisle,side,section,shelf,position) VALUES('Example','063848900022','A','Gauche','1','1','1')")
    db.commit()
    app=Flask(__name__)
    app.config['TESTING']=True
    app.register_blueprint(import_export_bp)
    with patch('routes.import_export.get_db',return_value=db), patch('routes.import_export.require_editor',return_value=('tester',None)), patch('routes.import_export.release_optional_product_caches'):
        client=app.test_client()
        response=client.get('/api/export')
        payload=json.loads(response.data)
        response.close()
        assert payload['export_version']==2
        db.execute('DELETE FROM products')
        db.execute('DELETE FROM aisle_layouts')
        db.commit()
        restored=client.post('/api/import',json=payload)
        assert restored.status_code==200,restored.json
        assert restored.json['imported_products']==1
        assert db.execute('SELECT sort_order FROM aisle_layouts').fetchone()['sort_order']==7
        assert db.execute('SELECT barcode FROM products').fetchone()['barcode']=='063848900022'
    db.close()
