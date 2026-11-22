import json
import sqlite3
import pytest
from recover_cached_catalogue import recover


def test_partial_cache_recovery_preserves_upcs_ids_and_all_placements(tmp_path):
    rows=[{'id':42,'barcode':'001234567890','name':'Product','aisle':'1','side':'Gauche',
           'section':'1','shelf':'2','position':'3','description':'Unproven','image_url':'https://example.com/wrong.jpg'},
          {'id':57,'barcode':'001234567890','name':'Product','aisle':'1','side':'Façade B',
           'section':'1','shelf':'1','position':'4'}]
    source=tmp_path/'cache.json';raw=json.dumps(rows).encode();source.write_bytes(raw)
    target=tmp_path/'recovered'
    result=recover(source,target,'test-store')
    assert result['products']==2 and result['distinct_upcs']==1
    assert result['partial_recovery'] and 'expiry history' in result['missing']
    assert (target/'original-cache.json').read_bytes()==raw
    db=sqlite3.connect(target/'recovered.sqlite')
    try:
        assert db.execute('SELECT id,barcode FROM products ORDER BY id').fetchall()==[(42,'001234567890'),(57,'001234567890')]
        assert db.execute('SELECT description,image_url FROM products WHERE id=42').fetchone()==('','')
        layout=json.loads(db.execute('SELECT config_json FROM aisle_layouts').fetchone()[0])
        assert layout['sides']['Gauche']['sections'][0]['shelves']==[0,3]
        assert layout['facade_b']['shelves']==[4]
    finally:db.close()
    with pytest.raises(FileExistsError):recover(source,target,'test-store')


def test_numeric_upc_is_not_silently_recovered(tmp_path):
    source=tmp_path/'cache.json';source.write_text(json.dumps([{'id':1,'barcode':1234567890}]))
    with pytest.raises(ValueError,match='string'):recover(source,tmp_path/'out','test-store')
