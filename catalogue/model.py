"""Build product identities and factual cards without generating descriptions."""
import hashlib
import json
import re
from collections import defaultdict
from urllib.parse import urlsplit

from product_data import gtin_identity_key
from domain.package_identity import package_facts
from catalogue.vocabulary import concept_ids, readable_label, normalize

FACT_FIELDS = ('brand', 'package_size', 'package_unit', 'strength', 'dosage_form',
               'flavour', 'colour', 'variant', 'ingredients', 'manufacturer',
               'category', 'compatibility', 'purpose', 'official_name_fr', 'official_name_en')


def identity(row):
    barcode = str(row.get('barcode') or '').strip()
    # Non-numeric store identifiers are preserved literally, never stripped into
    # another product's UPC. Blank barcodes remain separate, locatable records.
    if barcode.isdigit():
        return gtin_identity_key(barcode)
    return 'code:' + barcode if barcode else 'placement:' + str(row['id'])


def safe_url(value):
    try:
        url = urlsplit(str(value or ''))
        return str(value) if url.scheme == 'https' and url.hostname and not url.username and not url.password else ''
    except ValueError:
        return ''


def stable_json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))


def build_item(key, rows, evidence, reference_rows=(), source_records=()):
    """Evidence is aggregated per exact package, not per shelf occurrence."""
    choices = defaultdict(list)
    names = sorted({str(r.get('name') or '').strip() for r in rows if r.get('name')})
    all_rows = [*rows, *reference_rows]
    by_id = {r['id']: r for r in rows}
    for e in evidence:
        row = by_id.get(e.get('product_id'))
        if e.get('active') != 1 or e.get('verification_status') != 'verified':
            continue
        if row is not None and str(row.get(e['field_name']) or '').strip() != str(e['field_value']).strip():
            continue
        if row is None and not any(str(r.get(e['field_name']) or '').strip() == str(e['field_value']).strip() for r in reference_rows):
            continue
        if not e.get('field_value'):
            continue
        choices[e['field_name']].append({
            'value': str(e['field_value']).strip(), 'origin': 'source',
            'source': e.get('source') or 'Catalogue', 'url': safe_url(e.get('source_url')),
            'priority': int(e.get('source_priority') or 0),
        })
    for source in source_records:
        if source['status'] != 'accepted':
            continue
        record = json.loads(source['payload'])
        for field in (*FACT_FIELDS, 'name', 'image_url', 'description'):
            if record.get(field):
                choices[field].append({'value': str(record[field]).strip(), 'origin': 'source',
                    'source': source['provider'], 'url': safe_url(record.get('source_url')),
                    'priority': source['priority']})
    facts, conflicts = {}, []
    for field, candidates in choices.items():
        if field == 'strength':
            # Retailer feeds sometimes print a missing denominator as / 0.0.
            # Keep the raw observation, but never display it as a concentration.
            clean = [c for c in candidates if not re.search(r'/\s*0(?:[.,]0+)?(?:\s|$)', c['value'])]
            if len(clean) != len(candidates):
                conflicts.append('source_strength')
            candidates = clean
            if not candidates:
                continue
        candidates.sort(key=lambda c: (-c['priority'], c['source'], c['value']))
        top = [c for c in candidates if c['priority'] == candidates[0]['priority']]
        if len({normalize(c['value']) if field not in {'image_url'} else c['value'] for c in top}) > 1:
            conflicts.append(field)
            continue
        facts[field] = {k: v for k, v in top[0].items() if k != 'priority'}
    # A reference-only product needs verified name evidence, or its original label.
    if not names:
        names = sorted({str(r.get('name') or '').strip() for r in reference_rows if r.get('name')})
    original = names[0] if names else ''
    title = next((facts[f]['value'] for f in ('official_name_fr', 'name', 'official_name_en') if f in facts), '')
    title = title or readable_label(original) or 'Produit ' + str(all_rows[0].get('barcode') or '')
    title = re.sub(r'(?<=\d)\s*un\b', ' unités', title, flags=re.IGNORECASE)
    # Use each label independently: conflicting duplicate labels cannot create
    # a fictitious combined strength or count. Keep conflicts visible in details.
    parsed = [package_facts(n, readable_label(n)) for n in names]
    attributes = {}
    for field in ('sizes', 'strengths', 'counts', 'formats', 'flavours', 'colours'):
        sets = [p[field] for p in parsed if p[field]]
        if sets and all(s == sets[0] for s in sets):
            attributes[field] = sorted(sets[0])
        elif sets:
            conflicts.append('label_' + field)
    if title and not names:
        attributes = {k: sorted(v) for k, v in package_facts(title).items() if v}
    # Source attributes can supplement a terse label without changing the UPC.
    source_identity = ' '.join(facts[f]['value'] for f in
        ('name', 'official_name_fr', 'official_name_en', 'strength', 'dosage_form', 'flavour', 'colour') if f in facts)
    source_identity += ' ' + ' '.join(facts[f]['value'] for f in ('package_size', 'package_unit') if f in facts)
    for field, values in package_facts(source_identity).items():
        if values and field not in attributes and 'label_' + field not in conflicts:
            attributes[field] = sorted(values)
    concepts = concept_ids(' '.join([title, *names, facts.get('category', {}).get('value', '')]))
    image = safe_url(facts.get('image_url', {}).get('value'))
    facts.pop('image_url', None)
    # Narrative is available only in details, never used as identity/search data.
    description = facts.pop('description', None)
    codes = sorted({str(r.get('barcode') or '') for r in all_rows if r.get('barcode')})
    sku = sorted({str(r.get('product_code') or '') for r in all_rows if r.get('product_code')})
    locations = [{k: r.get(k, '') for k in
        ('id', 'aisle', 'side', 'section', 'shelf', 'position', 'in_stock', 'modified_at')} for r in rows]
    locations.sort(key=lambda r: tuple(str(r[k]).zfill(8) for k in ('aisle', 'side', 'section', 'shelf', 'position', 'id')))
    return {'key': key, 'title': title, 'original_labels': names, 'barcodes': codes, 'store_codes': sku,
            'facts': facts, 'attributes': attributes, 'concepts': concepts, 'image_url': image,
            'description': description, 'locations': locations, 'conflicts': sorted(set(conflicts)),
            'identity_basis': 'source' if 'name' in facts or 'official_name_fr' in facts else 'store_label'}


def digest(document):
    return hashlib.sha256(stable_json(document).encode()).hexdigest()
