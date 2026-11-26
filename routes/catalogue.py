"""Versioned catalogue APIs with bounded payloads and no network on search."""
from flask import Blueprint, jsonify, request
from database import get_db
from catalogue.service import get_index
from catalogue.assistant import interpret, provider
from catalogue.search import query_plan
from catalogue.vocabulary import tokens

catalogue_bp = Blueprint('catalogue', __name__)


def bounded_int(name, default, upper):
    try:
        return max(0, min(upper, int(request.args.get(name, default))))
    except (TypeError, ValueError):
        return default


@catalogue_bp.get('/api/catalogue/search')
def search():
    query = request.args.get('q', '').strip()
    if len(query) > 500:
        return jsonify(error='Recherche trop longue.'), 400
    index = get_index(get_db())
    return jsonify(index.search(query, category=request.args.get('category', '')[:80],
        aisle=request.args.get('aisle', '')[:80], offset=bounded_int('offset', 0, 1000000),
        limit=max(1, bounded_int('limit', 24, 60))))


@catalogue_bp.get('/api/catalogue/summary')
def summary():
    return jsonify(**get_index(get_db()).summary(), assistant=provider())


@catalogue_bp.get('/api/catalogue/item')
def item():
    doc = get_index(get_db()).docs.get(request.args.get('key', '')[:150])
    return jsonify(doc) if doc else (jsonify(error='Produit introuvable.'), 404)


@catalogue_bp.post('/api/catalogue/interpret')
def interpret_request():
    # Only a query is accepted: the caller cannot supply catalogue facts/model
    # instructions. Model calls require an employee session (not a public route).
    body = request.get_json(silent=True)
    if not isinstance(body, dict) or not isinstance(body.get('q'), str) or not 1 <= len(body['q'].strip()) <= 500:
        return jsonify(error='Recherche invalide.'), 400
    query = body['q'].strip()
    offset = body.get('offset', 0)
    if not isinstance(offset, int) or isinstance(offset, bool) or not 0 <= offset <= 1000000:
        return jsonify(error='Pagination invalide.'), 400
    index = get_index(get_db())
    direct = index.search(query, offset=offset)
    if direct['total']:
        return jsonify(direct)
    plan = interpret(query)
    if plan['mode'] == 'local':
        return jsonify(direct)
    plan['anchors'] = [t for t in tokens(query) if 'anchor_' + t in index.postings]
    # Category suggestions are explicitly identified as interpretation, not
    # exact matches. Original constraints never disappear into model output.
    result = index.search('', plan=plan, offset=offset)
    result['interpretation'] = True
    return jsonify(result)
