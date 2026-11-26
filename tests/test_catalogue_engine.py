import json
import sqlite3
from unittest.mock import patch

import pytest
from flask import Flask
from database import DatabaseConnection, init_sqlite_db
from catalogue.model import build_item, identity
from catalogue.repository import ensure_schema, refresh, documents, state
from catalogue.search import CatalogueIndex, query_plan
from catalogue.service import get_index
from catalogue.acquisition import claim, schedule, store_candidate, run_once
from catalogue.assistant import interpret, provider
from catalogue_import import import_feed
from catalogue.vocabulary import readable_label


@pytest.mark.parametrize('label,expected', [
    ('200MG CO24', '200 mg comprimés 24'),
    ('250\t\nML', '250 ml'),
    ('25\u00a0MCG', '25 mcg'),
    ('1KG 2G 3L', '1 kg 2 g 3 l'),
    ('200MGS', '200MGS'),
    ('MG', 'MG'),
    ('200' + ' ' * 100_000 + 'MG', '200 mg'),
    ('200' + ' ' * 100_000 + 'X', '200 X'),
    ('200 MGX ' * 10_000, ('200 MGX ' * 10_000).strip()),
])
def test_readable_units_preserve_labels_with_adversarial_whitespace(label, expected):
    assert readable_label(label) == expected


@pytest.fixture
def db():
    raw = sqlite3.connect(':memory:'); raw.row_factory = sqlite3.Row
    result = DatabaseConnection(raw, 'sqlite')
    init_sqlite_db(result); ensure_schema(result)
    yield result
    result.close()


def place(db, id=1, name='ADVIL IBUP 200MG CO 24', barcode='062107004053', aisle='1', **fields):
    row = dict(id=id, name=name, barcode=barcode, aisle=aisle, side='Gauche', section='1', shelf='2', position=str(id), **fields)
    db.execute('INSERT INTO products (' + ','.join(row) + ') VALUES (' + ','.join('?' for _ in row) + ')', tuple(row.values()))
    db.commit()
    return row


def index_for(rows):
    docs = [build_item(identity(row), [row], []) for row in rows]
    return CatalogueIndex(docs, 1)


def test_identity_separates_invalid_codes_and_preserves_zeroes():
    assert identity({'barcode': '062107004053'}) == identity({'barcode': '00062107004053'})
    assert identity({'barcode': '00123'}) != identity({'barcode': '123'})
    assert identity({'barcode': 'A123'}) != identity({'barcode': '123'})
    assert identity({'barcode':'', 'id':1}) != identity({'barcode':'', 'id':2})


def test_projection_keeps_every_placement_and_exact_barcode(db):
    first = place(db)
    second = place(db, 2, barcode='00062107004053', aisle='3')
    third = place(db, 3, name='Store special', barcode='000123')
    fourth = place(db, 4, name='No UPC', barcode='')
    refresh(db)
    docs = documents(db)
    assert len(docs) == 3
    assert sum(len(d['locations']) for d in docs) == 4
    originals = {(r['id'], r['barcode']) for r in (first, second, third, fourth)}
    assert {(r['placement_id'], r['barcode']) for r in db.execute('SELECT * FROM catalogue_placements')} == originals
    package = next(d for d in docs if len(d['locations']) == 2)
    assert package['barcodes'] == ['00062107004053','062107004053']
    assert package['attributes']['strengths'] == ['2E+2:mg']
    assert package['attributes']['counts'] == ['24:unit']
    assert 'comprimés' in package['title']


def test_bad_descriptions_do_not_pollute_search_or_remove_products(db):
    place(db, description='A toothbrush with mint and false claims', image_url='https://example.com/wrong.jpg')
    index = get_index(db)
    assert index.search('toothbrush')['total'] == 0
    assert index.search('Advil')['total'] == 1
    assert index.search('062107004053')['total'] == 1
    doc = next(iter(index.docs.values()))
    assert doc['description'] is None and doc['image_url'] == ''
    assert db.execute('SELECT description FROM products').fetchone()['description'].startswith('A toothbrush')


def test_revision_detects_edit_delete_and_rollback_without_timestamps(db):
    place(db)
    before = get_index(db)
    db.execute("UPDATE products SET name='New correct label' WHERE id=1")
    db.commit()
    after = get_index(db)
    assert after.revision > before.revision
    assert after.search('correct')['total'] == 1
    revision = state(db)['revision']
    db.execute("UPDATE products SET name='Rolled back' WHERE id=1")
    db.rollback()
    assert state(db)['revision'] == revision
    db.execute('DELETE FROM products WHERE id=1'); db.commit()
    assert get_index(db).summary()['products'] == 0


def test_duplicate_labels_with_conflicting_strength_do_not_create_combination(db):
    place(db)
    place(db, 2, name='ADVIL IBUP 400MG CO24')
    doc = next(iter(get_index(db).docs.values()))
    assert 'strengths' not in doc['attributes']
    assert 'label_strengths' in doc['conflicts']
    assert len(doc['locations']) == 2


def test_exact_source_facts_reused_across_placements(db):
    place(db); place(db,2)
    index = get_index(db)
    key = next(iter(index.docs))
    assert store_candidate(db,key,{'barcode':'062107004053','name':'ADVIL IBUP 200MG CO24'},
        {'barcode':'062107004053','name':'Advil ibuprofen 200 mg 24 tablets','brand':'Advil',
         'source':'Manufacturer','image_url':'https://example.com/exact.jpg'},1)
    db.commit()
    doc = next(iter(get_index(db).docs.values()))
    assert doc['facts']['brand']['value'] == 'Advil'
    assert doc['image_url'].endswith('/exact.jpg')
    assert len(doc['locations']) == 2


def test_source_conflict_retained_and_not_substituted(db):
    place(db); get_index(db)
    key = identity({'barcode':'062107004053'})
    assert not store_candidate(db,key,{'barcode':'062107004053','name':'ADVIL IBUP 200MG CO24'},
        {'barcode':'062107004053','name':'Advil ibuprofen 400 mg 24 tablets','source':'Manufacturer'},1)
    db.commit()
    assert get_index(db).search('Advil 200 mg')['total'] == 1
    assert db.execute('SELECT status FROM catalogue_sources').fetchone()['status'] == 'unresolved'


@pytest.mark.parametrize('query,expected', [
    ('toothpaste', ['1']), ('dentifrice menthe', ['1']), ('mint toothpaste', ['1']),
    ('shampoo', ['2']), ('après shampoing', ['3']), ('conditioner', ['3']),
    ('capsules 200 mg', ['4']), ('400 mg', []), ('sans sucre', ['5']),
    ('sugar free', ['5']), ('50 comprimés', ['5']), ('00123456789', []),
])
def test_fact_search_bilingual_constraints_and_no_fuzzy_upc(query, expected):
    names = ['Dentifrice menthe 100ML','SHAMPOING DOUX 250ML', 'Après-shampoing doux 250ML',
             'Ibuprofen 200 mg 20 capsules', 'Vitamine C S/SUCRE CO50']
    rows = [dict(id=i,barcode=str(i),name=name) for i,name in enumerate(names,1)]
    results = index_for(rows).search(query)['items']
    assert [d['barcodes'][0] for d in results] == expected


def test_pagination_has_no_duplicates_and_facets_do_not_lose_other_products():
    rows = [dict(id=i,barcode=str(i),name=f'Produit numéro {i}',aisle=str(i % 2)) for i in range(70)]
    idx = index_for(rows)
    seen = set()
    for offset in (0,24,48):
        response = idx.search(offset=offset)
        assert response['total'] == 70
        keys = {d['key'] for d in response['items']}
        assert not seen & keys
        seen |= keys
    assert len(seen) == 70
    assert idx.search(aisle='1')['total'] == 35


def test_reference_only_products_remain_findable(db):
    db.execute("INSERT INTO product_reference(barcode,name) VALUES('00123','Unplaced toothpaste')")
    db.commit()
    result = get_index(db).search('toothpaste')
    assert result['total'] == 1 and result['items'][0]['locations'] == []


def test_leases_dedupe_upcs_and_recover_crashed_workers(db):
    place(db); place(db,2); get_index(db)
    assert schedule(db,now=1) == 1
    first = claim(db,now=2)
    assert first and claim(db,now=3) is None
    second = claim(db,now=183)
    assert second['lease_token'] != first['lease_token']
    assert second['attempts'] == 2


def test_feed_import_is_exact_and_keeps_placement_data(db):
    place(db)
    before = dict(db.execute('SELECT * FROM products').fetchone())
    result = import_feed(db,[{'barcode':'062107004053','name':'Advil 200 mg 24 tablets','brand':'Advil'}], 'manufacturer')
    assert result['accepted'] == 1
    assert dict(db.execute('SELECT * FROM products').fetchone()) == before
    assert next(iter(get_index(db).docs.values()))['title'] == 'Advil 200 mg 24 tablets'


def test_feed_validation_is_atomic(db):
    place(db)
    with pytest.raises(ValueError):
        import_feed(db,[{'barcode':'062107004053','name':'Good'}, {'barcode':123,'name':'Lost leading zeros'}], 'supplier')
    assert db.execute('SELECT COUNT(*) AS n FROM catalogue_sources').fetchone()['n'] == 0


def test_ai_cannot_write_facts_or_invent_categories():
    with patch('catalogue.assistant.provider', return_value='cloudflare_free'), patch('catalogue.assistant.structured_request', return_value={'concepts':['imaginary'], 'description':'invented'}):
        assert interpret('Something for my hair')['mode'] == 'local'
    with patch('catalogue.assistant.provider', return_value='cloudflare_free'), patch('catalogue.assistant.structured_request', return_value={'concepts':['hair_shampoo']}):
        assert interpret('I want to wash my hair')['concepts'] == ['hair_shampoo']


def test_ai_never_rewrites_strength_or_barcode():
    with patch('catalogue.assistant.structured_request') as model:
        assert interpret('Advil 200 mg')['constraints']['strengths'] == ['2E+2:mg']
        interpret('062107004053')
        model.assert_not_called()


def test_ollama_only_uses_loopback_and_never_downloads_models():
    import os
    with patch.dict(os.environ, {'CATALOGUE_OLLAMA_URL':'http://example.com','CATALOGUE_OLLAMA_MODEL':'test'}), patch('catalogue.assistant.cloudflare_configured',return_value=False):
        assert provider() == 'local'


def test_api_bounded_and_public_catalogue_only(db):
    from routes.catalogue import catalogue_bp
    from security import install_security
    app = Flask(__name__); app.config.update(TESTING=True)
    install_security(app); app.register_blueprint(catalogue_bp)
    place(db)
    with patch('routes.catalogue.get_db',return_value=db), patch('security._load_session',return_value=None):
        client = app.test_client()
        assert client.get('/api/catalogue/search?q=Advil').json['total'] == 1
        assert client.get('/api/catalogue/search?limit=999').json['limit'] == 60
        assert client.get('/api/catalogue/search?q=' + 'x'*501).status_code == 400
        assert client.post('/api/catalogue/interpret',json={'q':'help'}).status_code == 401


def test_real_store_dental_abbreviations_do_not_turn_floss_or_brushes_into_toothpaste():
    # Label strings observed in the recovered store; no product data leaves the test.
    rows = [dict(id=i,barcode=str(i),name=n) for i,n in enumerate([
        'COLGATE DENT MENT FRAI 95ML', 'GUM C/SLID FIL/DENT MENT 2X40M',
        'ORAL-B GLIDE SOIE DENT MNT 40M', 'SENSODYNE BR/DENT SOUPLE 2',
        'POLIDENT NETT APP DENT CO 96'],1)]
    idx = index_for(rows)
    assert [r['barcodes'][0] for r in idx.search('toothpaste')['items']] == ['1']
    assert {r['barcodes'][0] for r in idx.search('dental floss')['items']} == {'2','3'}
    assert [r['barcodes'][0] for r in idx.search('toothbrush')['items']] == ['4']
    assert [r['barcodes'][0] for r in idx.search('denture cleaner')['items']] == ['5']


def test_vitamin_cream_is_not_an_oral_vitamin():
    idx=index_for([dict(id=1,barcode='1',name='AVENE VIT A/CG CR JR 50ML'),dict(id=2,barcode='2',name='JAMI VIT C 500MG CO100')])
    assert [r['barcodes'][0] for r in idx.search('vitamine C')['items']] == ['2']


def test_negative_claims_require_positive_evidence():
    idx=index_for([dict(id=1,barcode='1',name='Rinse'),dict(id=2,barcode='2',name='Rinse S/ALC'),dict(id=3,barcode='3',name='Rinse alcohol')])
    assert [r['barcodes'][0] for r in idx.search('alcohol free')['items']] == ['2']


def test_equivalent_volume_search_does_not_depend_on_text_units():
    idx=index_for([dict(id=1,barcode='1',name='Shampoo 250ML')])
    assert idx.search('shampoo 0.25 l')['total'] == 1


def test_acquisition_rate_is_shared_across_jobs(db):
    place(db);place(db,2,name='Sleep-eze liquide 355ML',barcode='075137108387');get_index(db);schedule(db,now=1)
    assert claim(db,now=2)
    assert claim(db,now=3) is None
    assert claim(db,now=32)


def test_source_error_retries_and_stale_lease_cannot_publish(tmp_path):
    path=tmp_path/'jobs.sqlite'
    def connect():
        raw=sqlite3.connect(path);raw.row_factory=sqlite3.Row
        return DatabaseConnection(raw,'sqlite')
    db=connect();init_sqlite_db(db);ensure_schema(db);place(db);get_index(db);schedule(db,now=1);db.close()
    def fetcher(code,**kw):
        # Simulate another worker acquiring this package after an expired lease.
        other=connect();other.execute("UPDATE catalogue_tasks SET lease_token='new-worker',lease_until=9999999999 WHERE barcode=?",(code,));other.commit();other.close()
        return [{'barcode':code,'name':'Advil 200 mg 24 tablets','source':'Manufacturer'}]
    with patch('catalogue.acquisition.connect_db',side_effect=connect):
        assert run_once(fetcher)
    db=connect()
    try:
        assert db.execute('SELECT COUNT(*) AS n FROM catalogue_sources').fetchone()['n'] == 0
        assert db.execute('SELECT lease_token FROM catalogue_tasks').fetchone()['lease_token'] == 'new-worker'
    finally:db.close()


def test_source_unit_counts_detect_different_retailer_package():
    from product_data import assess_metadata_candidate
    assessment=assess_metadata_candidate({'barcode':'062107004053','name':'ADVIL 200MG CO24'},
        {'barcode':'062107004053','name':'Advil ibuprofen 48un','source':'Familiprix'})
    assert not assessment.auto_apply
    assert any(i['reason']=='different_package_attribute' for i in assessment.issues)


def test_supplier_catalogue_adds_reference_items_without_inventing_placements(db):
    result=import_feed(db,[{'barcode':'062107004053','name':'Advil 200 mg 24 tablets'}],'supplier')
    assert result['new_reference_products']==1
    result=get_index(db).search('062107004053')
    assert result['total']==1 and result['items'][0]['locations']==[]
    assert db.execute('SELECT COUNT(*) AS n FROM products').fetchone()['n']==0


def test_incremental_update_preserves_in_flight_index_and_only_rebuilds_changed_package(db):
    from catalogue import repository
    place(db)
    place(db,2,name='Dentifrice menthe 100ML',barcode='000124')
    before=get_index(db)
    with patch.object(repository,'build_item',wraps=repository.build_item) as build:
        db.execute("UPDATE products SET name='Dentifrice fraise 100ML' WHERE id=2")
        db.commit()
        after=get_index(db)
        assert build.call_count==1
    assert before.search('dentifrice menthe')['total']==1
    assert before.search('dentifrice fraise')['total']==0
    assert after.search('dentifrice menthe')['total']==0
    assert after.search('dentifrice fraise')['total']==1
    assert after.search('Advil')['total']==1


def test_barcode_edit_splits_and_merges_packages_without_losing_locations(db):
    place(db);place(db,2)
    get_index(db)
    db.execute("UPDATE products SET barcode='000123' WHERE id=2");db.commit()
    split=get_index(db)
    assert split.summary()['products']==2 and split.summary()['placements']==2
    assert len(split.search('062107004053')['items'][0]['locations'])==1
    assert len(split.search('000123')['items'][0]['locations'])==1
    db.execute("UPDATE products SET barcode='062107004053' WHERE id=2");db.commit()
    merged=get_index(db)
    assert merged.summary()['products']==1 and merged.summary()['placements']==2
    assert merged.search('000123')['total']==0


def test_source_refresh_history_is_retained(db):
    place(db);get_index(db)
    existing={'barcode':'062107004053','name':'ADVIL 200MG CO24'}
    key=identity(existing)
    store_candidate(db,key,existing,{**existing,'source':'Manufacturer','brand':'Advil'},1)
    store_candidate(db,key,existing,{**existing,'source':'Manufacturer','brand':'Advil','image_url':'https://example.com/new.jpg'},2)
    db.commit()
    assert db.execute('SELECT COUNT(*) AS n FROM catalogue_observations').fetchone()['n']==2
    assert db.execute('SELECT COUNT(*) AS n FROM catalogue_sources').fetchone()['n']==1


def test_ai_category_suggestions_preserve_explicit_brand_anchor(db):
    from routes.catalogue import catalogue_bp
    app=Flask(__name__);app.register_blueprint(catalogue_bp)
    place(db,name='DOVE SHP 250ML');place(db,2,name='AVENE SHP 250ML',barcode='000124')
    plan={'query':'Dove for washing hair','concepts':['hair_shampoo'],'constraints':{},'claims':[],'negated':[],'mode':'ollama'}
    with patch('routes.catalogue.get_db',return_value=db),patch('routes.catalogue.interpret',return_value=plan):
        result=app.test_client().post('/api/catalogue/interpret',json={'q':plan['query']}).json
        assert result['interpretation']
        assert result['total']==1 and result['items'][0]['title'].startswith('DOVE')


def test_malformed_retailer_concentration_is_not_presented_as_fact():
    row=dict(id=1,barcode='062107004053',name='ADVIL 200MG CO24')
    source={'status':'accepted','provider':'Familiprix','priority':100,'payload':json.dumps({
        'name':'Advil comprimés 24un','strength':'200.0 MG / 0.0'})}
    doc=build_item(identity(row),[row],[],source_records=[source])
    assert 'strength' not in doc['facts']
    assert doc['attributes']['strengths']==['2E+2:mg']
    assert doc['title']=='Advil comprimés 24 unités'
    assert doc['conflicts']==['source_strength']
