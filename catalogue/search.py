"""Bounded inverted retrieval over identities and facts, independent of prose."""
import math
import re
import copy
from collections import Counter, defaultdict
from difflib import get_close_matches

from catalogue.model import identity
from catalogue.vocabulary import CONCEPTS, SYNONYMS, STOP, contains, expand_tokens, normalize, tokens, readable_label, concept_ids
from domain.package_identity import package_facts

GENERIC_ANCHOR_WORDS = {t for _, aliases in CONCEPTS.values() for phrase in aliases for t in tokens(phrase)} | {
    'produit', 'product', 'gel', 'capsule', 'capsules', 'comprime', 'comprimes'}


def query_plan(query):
    normalized = normalize(readable_label(query))
    constraints = {k: sorted(v) for k, v in package_facts(query).items() if v}
    # Claims are positive evidence requirements, not the absence of an allergen.
    claims = []
    for group in SYNONYMS:
        if group[0].startswith('sans ') and any(contains(normalized, phrase) for phrase in group):
            claims.append(group[0])
    negated = []
    for match in re.finditer(r'\b(?:sans|without)\s+([a-z]+)', normalized):
        if not any(contains(normalized, phrase) for group in SYNONYMS if group[0] in claims for phrase in group):
            claims.append(match[0])
    for match in re.finditer(r'\bpas de\s+([a-z]+)', normalized):
        negated.append(match[1])
    return {'query': query, 'concepts': concept_ids(query), 'constraints': constraints,
            'claims': sorted(set(claims)), 'negated': negated, 'mode': 'local'}


def card(doc):
    return {k: doc[k] for k in ('key', 'title', 'barcodes', 'facts', 'attributes', 'concepts',
                               'locations', 'image_url', 'identity_basis', 'conflicts')}


def document_terms(doc):
    # Never index generated or legacy descriptions, usage advice or alternatives.
    text = ' '.join([doc['title'], *doc['original_labels'], *doc['store_codes'],
        *(v['value'] for f, v in doc['facts'].items() if f not in {'purpose', 'compatibility'})])
    counts = Counter(tokens(text))
    for token in expand_tokens(text):
        counts[token] = max(counts[token], 1)
    for c in doc['concepts']:
        counts['concept_' + c] = 2
    for field, values in doc['attributes'].items():
        for value in values:
            counts['attribute_' + field + '_' + value] = 2
    brand = doc['facts'].get('brand', {}).get('value')
    anchor_words = tokens(brand) if brand else normalize((doc['original_labels'] or [doc['title']])[0]).split()[:1]
    for token in anchor_words:
        if len(token) >= 3 and token not in GENERIC_ANCHOR_WORDS:
            counts['anchor_' + token] = 1
    return normalize(text), counts


def identifier_keys(doc):
    return {identity({'barcode': code}) for code in doc['barcodes']} | {'sku:' + code for code in doc['store_codes']}


class CatalogueIndex:
    def __init__(self, docs, revision):
        self.revision = revision
        self.docs = {d['key']: d for d in docs}
        self.postings = defaultdict(dict)
        self.lengths, self.texts, self.identifiers = {}, {}, defaultdict(set)
        self.facets = Counter()
        for key, doc in self.docs.items():
            self.texts[key], counts = document_terms(doc)
            for c in doc['concepts']:
                self.facets[c] += 1
            self.lengths[key] = sum(counts.values())
            for token, count in counts.items():
                self.postings[token][key] = count
            for code in doc['barcodes']:
                self.identifiers[identity({'barcode': code})].add(key)
            for code in doc['store_codes']:
                self.identifiers['sku:' + code].add(key)
        self.average_length = sum(self.lengths.values()) / max(1, len(docs))
        self.vocabulary = tuple(t for t in self.postings if t.isalpha())

    def with_updates(self, changed, existing_keys, revision):
        """Copy only affected posting lists; in-flight readers keep their snapshot."""
        result = copy.copy(self)
        result.revision = revision
        for name in ('docs', 'postings', 'lengths', 'texts', 'identifiers'):
            setattr(result, name, dict(getattr(self, name)))
        result.facets = self.facets.copy()
        removed = set(self.docs) - set(existing_keys)
        replacing = removed | {d['key'] for d in changed}
        for key in replacing:
            old = self.docs.get(key)
            if not old:
                continue
            _, counts = document_terms(old)
            for token in counts:
                posting = dict(result.postings[token]); posting.pop(key, None)
                if posting:
                    result.postings[token] = posting
                else:
                    result.postings.pop(token, None)
            for identifier in identifier_keys(old):
                ids = set(result.identifiers.get(identifier, ())); ids.discard(key)
                if ids:
                    result.identifiers[identifier] = ids
                else:
                    result.identifiers.pop(identifier, None)
            for concept in old['concepts']:
                result.facets[concept] -= 1
                if not result.facets[concept]:
                    del result.facets[concept]
            for mapping in (result.docs, result.texts, result.lengths):
                mapping.pop(key, None)
        for doc in changed:
            key = doc['key']; result.docs[key] = doc
            result.texts[key], counts = document_terms(doc)
            result.lengths[key] = sum(counts.values())
            for token, count in counts.items():
                posting = dict(result.postings.get(token, {})); posting[key] = count
                result.postings[token] = posting
            for identifier in identifier_keys(doc):
                ids = set(result.identifiers.get(identifier, ())); ids.add(key)
                result.identifiers[identifier] = ids
            for concept in doc['concepts']:
                result.facets[concept] += 1
        result.average_length = sum(result.lengths.values()) / max(1, len(result.docs))
        result.vocabulary = tuple(t for t in result.postings if t.isalpha())
        return result

    def search(self, query='', category='', aisle='', offset=0, limit=24, plan=None):
        plan = plan or query_plan(query)
        query = str(query).strip()
        normalized = normalize(query)
        exact = set()
        barcode_query = re.sub(r'[\s-]', '', query)
        if barcode_query and barcode_query.isdigit():
            exact = self.identifiers.get(identity({'barcode': barcode_query}), set()) | self.identifiers.get('sku:' + barcode_query, set())
            # No fuzzy barcode search: the next digit can be a different package.
            candidates = set(exact)
            scores = {key: 1000 for key in candidates}
        else:
            terms = expand_tokens(query)
            for field, values in plan['constraints'].items():
                for value in values:
                    terms.add('attribute_' + field + '_' + value)
            for c in plan['concepts']:
                if c in CONCEPTS:
                    terms.add('concept_' + c)
            scores = defaultdict(float)
            for token in terms:
                posting = self.postings.get(token, {})
                idf = math.log(1 + (len(self.docs) - len(posting) + .5) / (len(posting) + .5))
                for key, frequency in posting.items():
                    denominator = frequency + 1.2 * (.25 + .75 * self.lengths[key] / max(1, self.average_length))
                    scores[key] += idf * frequency * 2.2 / denominator
            candidates = set(scores) if query else set(self.docs)
            # All meaningful query concepts/words must be supported. Synonyms
            # are OR within a group, AND between groups; no unrelated broadening.
            consumed = set()
            groups = []
            nq = normalize(readable_label(query))
            for _, aliases in CONCEPTS.values():
                for alias in aliases:
                    if contains(nq, alias):
                        consumed.update(tokens(alias))
                        break
            for synonyms in SYNONYMS:
                used = [term for term in synonyms if contains(nq, term)]
                if used:
                    consumed.update(t for term in used for t in tokens(term))
                    groups.append(set(t for term in synonyms for t in tokens(term)))
            numeric_tokens = set(re.findall(r'\d+(?:[.,]\d+)?|mg|mcg|ml|kg|ct', nq))
            for token in tokens(nq):
                if token in consumed or token in numeric_tokens or re.fullmatch(r'\d+(?:mg|mcg|ml|kg|g|l|ct)', token) or token in {'sans', 'without', 'no', 'pas'} or token in plan['negated']:
                    continue
                groups.append({token})
            if groups:
                candidates = {key for key in candidates if all(any(key in self.postings.get(t, {}) for t in group) for group in groups)}
        selected = []
        for key in candidates:
            doc = self.docs[key]
            if category and category not in doc['concepts']:
                continue
            if aisle and not any(str(p['aisle']) == aisle for p in doc['locations']):
                continue
            if not exact:
                if any(key not in self.postings.get('anchor_' + token, {}) for token in plan.get('anchors', [])):
                    continue
                if plan['concepts'] and not all(c in doc['concepts'] for c in plan['concepts']):
                    continue
                if any(not set(values).intersection(doc['attributes'].get(field, [])) for field, values in plan['constraints'].items()):
                    continue
                if any(not any(contains(self.texts[key], phrase) for phrase in next(
                    (group for group in SYNONYMS if group[0] == claim), [claim])) for claim in plan['claims']):
                    continue
                if any(contains(self.texts[key], term) for term in plan['negated']):
                    continue
            selected.append(key)
        selected.sort(key=lambda k: (-scores.get(k, 0), -bool(self.docs[k]['locations']), self.docs[k]['title'], k))
        suggestions = []
        if not selected and query and not barcode_query.isdigit():
            words = tokens(query)
            for word in words[:8]:
                if len(word) >= 4 and word not in self.postings:
                    matches = get_close_matches(word, self.vocabulary, n=1, cutoff=.78)
                    if matches:
                        suggestions.append(re.sub(r'\b' + re.escape(word) + r'\b', matches[0], normalized))
        return {'items': [card(self.docs[k]) for k in selected[offset:offset + limit]],
                'total': len(selected), 'offset': offset, 'limit': limit,
                'next_offset': offset + limit if offset + limit < len(selected) else None,
                'revision': self.revision, 'exact_identifier': bool(exact),
                'suggestions': list(dict.fromkeys(suggestions))[:3], 'plan': plan}

    def summary(self):
        docs = list(self.docs.values())
        return {'products': len(docs), 'placements': sum(len(d['locations']) for d in docs),
            'barcodes': len({b for d in docs for b in d['barcodes']}),
            'with_images': sum(bool(d['image_url']) for d in docs),
            'with_attributes': sum(bool(d['attributes']) for d in docs),
            'source_named': sum(d['identity_basis'] == 'source' for d in docs),
            'with_conflicts': sum(bool(d['conflicts']) for d in docs),
            'categories': [{'id': c, 'label': CONCEPTS[c][0], 'count': n} for c, n in sorted(self.facets.items(), key=lambda p: CONCEPTS[p[0]][0])],
            'aisles': sorted({str(p['aisle']) for d in docs for p in d['locations']}),
            'revision': self.revision}
