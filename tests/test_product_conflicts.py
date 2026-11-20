def test_stale_product_cannot_overwrite_newer_edit():
    import json
    import sqlite3
    from unittest.mock import patch
    from flask import Flask
    from database import DatabaseConnection, init_sqlite_db
    from routes.products import products_bp
    
    raw = sqlite3.connect(':memory:')
    raw.row_factory = sqlite3.Row
    db = DatabaseConnection(raw, 'sqlite')
    init_sqlite_db(db)
    db.execute("INSERT INTO products (id,name,brand,aisle,side,section,shelf,position,modified_at) VALUES (1,'Original','Original brand','1','Gauche','1','1','1','baseline')")
    db.commit()
    app = Flask(__name__)
    app.config.update(TESTING=True)
    app.register_blueprint(products_bp)
    baseline = dict(db.execute('SELECT * FROM products WHERE id=1').fetchone())
    a = dict(baseline, expected_modified_at='baseline', name='Employee A new name')
    b = dict(baseline, expected_modified_at='baseline', brand='Employee B new brand')
    with patch('routes.products.require_editor',return_value=('audit',None)), \
         patch('routes.products.get_db',return_value=db), \
         patch('routes.products.validate_layout_slot',return_value=(True,'')), \
         patch('routes.products.find_product_at_position',return_value=None), \
         patch('routes.products.find_existing_image_for_barcode',return_value=''), \
         patch('routes.products.audit_product_data'), \
         patch('routes.products.row_to_verified_product',side_effect=lambda db,row:dict(row)), \
         patch('routes.gist._schedule_gist_backup'):
        client=app.test_client()
        first=client.put('/api/products/1',json=a)
        second=client.put('/api/products/1',json=b)
        final=dict(db.execute('SELECT * FROM products WHERE id=1').fetchone())
        assert first.status_code == 200
        assert second.status_code == 409
        assert final['name'] == 'Employee A new name'
        missing = client.put('/api/products/1',json=baseline)
        assert missing.status_code == 428
    db.close()
