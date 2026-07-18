import re
import os
import json
import time
import hashlib
import math
import tempfile
import threading
import unicodedata
from collections import Counter, deque
from difflib import SequenceMatcher
from flask import Blueprint, request, jsonify
from database import get_db, DatabaseIntegrityError
from auth import require_editor, utc_now_iso, side_display_label
from routes.layout import validate_layout_slot, aisle_sort_key
from memory_guard import memory_intensive_task, release_unused_memory

products_bp = Blueprint("products", __name__)

SEARCH_STOPWORDS = {
    "a", "an", "and", "au", "aux", "avec", "ce", "ces", "cette", "client", "comme",
    "dans", "de", "des", "du", "en", "et", "for", "how", "i", "il", "ils", "je",
    "la", "le", "les", "mais", "mon", "my", "of", "on", "or", "ou", "par", "pas",
    "pour", "que", "qui", "sans", "si", "son", "sur", "the", "to", "un", "une",
    "with", "without", "y",
    # Filler words of a spoken client request ("quelque chose contre la toux",
    # "quel produit recommandez-vous"). Left in the query they became matching
    # tokens and pulled in unrelated products. Keep in sync with config.js.
    "besoin", "cherche", "cherchez", "chose", "choses", "conseil", "conseillez",
    "contre", "donner", "faudrait", "faut", "madame", "medicament", "medicaments",
    "meilleur", "meilleure", "monsieur", "peut", "peux", "plait", "prendre",
    "produit", "produits", "quelque", "quelques", "quoi", "recommande",
    "recommandez", "suggestion", "svp", "veut", "veux", "voudrais",
}

# ── Intent lexicon ───────────────────────────────────────────────────────────────
# Maps a customer's PROBLEM (symptom / need, written the way a client speaks) to the
# product / ingredient / brand words that actually appear in product names, brands,
# search_terms, usage_notes and descriptions. This lets a symptom query reach the
# right products straight from the store's own data — no AI required. Triggers and
# expansion terms are accent-free + lowercase to match normalize_search_text().
# NOTE: keep this in sync with INTENT_LEXICON in static/search.js (same shape).
INTENT_LEXICON = [
    {"label": "Douleur / fièvre",
     "triggers": ["mal de tete", "maux de tete", "tete", "migraine", "cephalee", "fievre",
                  "douleur", "douleurs", "courbature", "courbatures", "mal de dos", "arthrite",
                  "menstruel", "menstruelle", "regles", "crampes menstruelles"],
     "expand": ["acetaminophene", "tylenol", "advil", "motrin", "ibuprofene", "aspirine",
                "analgesique", "antidouleur", "naproxene", "aleve", "atasol", "tempra"]},
    {"label": "Rhume / congestion",
     "triggers": ["rhume", "congestion", "nez bouche", "sinus", "grippe", "decongestionnant", "mouchoir"],
     "expand": ["decongestionnant", "rhume", "sinus", "sudafed", "otrivin", "tylenol rhume",
                "advil rhume", "dristan", "vicks", "sirop"]},
    {"label": "Toux / gorge",
     "triggers": ["toux", "gorge", "mal de gorge", "expectorant", "enrouement", "extinction de voix"],
     # no generic 'pastille': it matched dishwasher 'pastilles' products; throat
     # lozenges are reached via 'gorge' and the brand names below
     "expand": ["sirop", "toux", "dextromethorphane", "guaifenesine", "benylin", "buckley",
                "gorge", "strepsils", "halls", "fisherman"]},
    {"label": "Allergies",
     "triggers": ["allergie", "allergies", "urticaire", "eternuement", "rhinite", "allergique"],
     "expand": ["antihistaminique", "allergie", "reactine", "cetirizine", "claritin",
                "loratadine", "aerius", "benadryl", "allegra", "blexten"]},
    {"label": "Brûlures d'estomac",
     "triggers": ["brulure d estomac", "brulures d estomac", "reflux", "acidite", "indigestion",
                  "aigreur", "estomac"],
     "expand": ["antiacide", "tums", "gaviscon", "rolaids", "omeprazole", "pepto", "famotidine", "pantoloc"]},
    {"label": "Digestion / transit",
     "triggers": ["constipation", "diarrhee", "nausee", "ballonnement", "ballonnements", "gaz",
                  "crampes", "mal de ventre", "digestion"],
     "expand": ["laxatif", "metamucil", "senokot", "imodium", "gravol", "probiotique",
                "lax a day", "restoralax", "ovol", "gaz", "pepto"]},
    {"label": "Vitamines / suppléments",
     "triggers": ["vitamine", "vitamines", "supplement", "supplements", "fer", "calcium",
                  "magnesium", "multivitamine", "immunite", "fatigue", "energie"],
     "expand": ["vitamine", "multivitamine", "centrum", "jamieson", "webber", "fer", "calcium",
                "magnesium", "vitamine d", "vitamine c", "zinc", "omega", "probiotique"]},
    {"label": "Soins de la peau",
     "triggers": ["peau", "eczema", "secheresse", "hydratant", "creme", "demangeaison", "demangeaisons",
                  "piqure", "piqures", "brulure", "coup de soleil", "acne", "psoriasis", "feu sauvage"],
     "expand": ["creme", "hydratant", "cortisone", "cortate", "lubriderm", "aveeno", "cerave",
                "calamine", "polysporin", "onguent", "vaseline", "abreva"]},
    {"label": "Soins des yeux",
     "triggers": ["yeux", "oeil", "secheresse oculaire", "conjonctivite", "larmes", "oculaire"],
     "expand": ["gouttes", "yeux", "larmes artificielles", "visine", "systane", "collyre", "refresh"]},
    {"label": "Bébé",
     "triggers": ["bebe", "couche", "couches", "poussee dentaire", "colique", "coliques",
                  "erytheme fessier", "nourrisson"],
     "expand": ["bebe", "couche", "pampers", "huggies", "tempra", "tylenol bebe", "penaten",
                "creme fesses", "lingette", "ovol"]},
    {"label": "Bébé — lait & nourriture",
     "triggers": ["bouffe", "nourriture", "manger", "boire", "lait", "formule", "preparation",
                  "biberon", "cereale", "cereales", "puree", "purees", "nourrisson", "allaitement",
                  "maternise"],
     "expand": ["enfamil", "similac", "preparation nourrisson", "nourrisson", "biberon", "cereales",
                "puree", "gerber", "maternise", "allaitement", "pablum"]},
    {"label": "Premiers soins",
     "triggers": ["pansement", "coupure", "plaie", "desinfectant", "bandage", "ampoule",
                  "echarde", "eraflure", "saignement"],
     "expand": ["pansement", "band aid", "polysporin", "peroxyde", "alcool", "gaze",
                "bandage", "antiseptique", "diachylon"]},
    {"label": "Pansement transparent",
     "triggers": ["membrane transparente", "membrane transparent", "pansement transparent",
                  "film transparent", "opsite", "upsite", "upside"],
     "expand": ["pansement transparent", "film transparent", "opsite", "tegaderm",
                "paramedic pans transp", "transp"]},
    {"label": "Ouate / boules de coton",
     "triggers": ["watte", "ouate", "boule de coton", "boules de coton", "cotton balls"],
     "expand": ["ouate", "boule coton", "boules coton", "coton", "cotton"]},
    {"label": "Sommeil / stress",
     "triggers": ["sommeil", "dormir", "insomnie", "stress", "anxiete", "relaxation", "nervosite"],
     "expand": ["sommeil", "melatonine", "nytol", "sleep", "valeriane", "unisom", "tylenol nuit"]},
]


# ── Abbreviation lexicon ─────────────────────────────────────────────────────────
# Planogram/étiquette names are heavily abbreviated (SHP=shampooing, PDRE=poudre…).
# Map the full word a user would type to the short form(s) that appear in product
# names, so "shampoing" finds "AVEENO SHP …". Also carries a few FR spelling variants.
# Keep in sync with SEARCH_ABBREVIATIONS in static/search.js.
SEARCH_ABBREVIATIONS = {
    "shampoing": ["shp", "shampooing"], "shampooing": ["shp", "shampoing"],
    "revitalisant": ["rev", "revit", "apres"], "apres": ["apres"],
    "poudre": ["pdre", "pdr", "pou"],
    "sirop": ["sir"],
    "comprime": ["co", "compr", "com"], "comprimes": ["co", "compr", "com"],
    "capsule": ["caps", "gel"], "capsules": ["caps", "gel"],
    "creme": ["cr", "crm"], "cremes": ["cr", "crm"],
    "onguent": ["ong"],
    "lotion": ["lot", "lotn"],
    "solution": ["sol", "soln"],
    "decongestionnant": ["decong", "dec"], "congestion": ["decong", "cong"],
    "enfant": ["enf"], "enfants": ["enf"],
    "savon": ["sav"],
    "deodorant": ["deo"],
    "antisudorifique": ["antisud", "a sud"],
    "dentifrice": ["dent"],
    "brosse": ["bross", "bro"],
    "rasoir": ["ras"], "rasage": ["ras"],
    "vaporisateur": ["vapo", "vap"],
    "nettoyant": ["nett", "net"],
    "traitement": ["trait", "trmt"],
    "vitamine": ["vit"], "vitamines": ["vit"],
    "gouttes": ["gtte", "gttes", "got"], "goutte": ["gtte", "got"],
    "pastille": ["past"], "pastilles": ["past"],
    "protection": ["prot"],
    "feminine": ["fem"], "feminin": ["fem"],
    "quotidien": ["quot"],
    "naturel": ["nat"], "naturels": ["nat"], "naturelle": ["nat"],
    "supplement": ["suppl", "supp"], "supplements": ["suppl", "supp"],
    "hydratant": ["hydr", "hyd"], "hydratante": ["hydr", "hyd"],
    "maquillage": ["maq", "maquill"],
    "coloration": ["color", "col"],
    "biberon": ["bib"],
    "serviette": ["serv"], "serviettes": ["serv"],
    "tampon": ["tamp"], "tampons": ["tamp"],
    "transparent": ["transp"], "transparente": ["transp"],
}


def abbreviation_terms(query):
    """Short forms implied by the full words in the query (shampoing -> shp)."""
    terms, seen = [], set()
    for token in tokenize_search_query(query):
        for short in SEARCH_ABBREVIATIONS.get(token, []):
            if short not in seen:
                seen.add(short)
                terms.append(short)
    return terms


def _abbreviation_hit(name_norm, abbrevs):
    """True if a product NAME contains an abbreviation as a whole word or as an
    abbrev+digits token (e.g. 'co50', 'ca100') — never a loose substring."""
    if not name_norm or not abbrevs:
        return False
    tokens = name_norm.split()
    for token in tokens:
        for ab in abbrevs:
            if token == ab or (token.startswith(ab) and token[len(ab):].isdigit()):
                return True
    return False


# ── Reference-catalogue search cache ─────────────────────────────────────────────
# The catalogue has ~9000+ rows. Re-normalizing them all (regex) on every keystroke
# froze the whole app on Render's single worker, so we normalize ONCE into memory.
# Freshness: a CHEAP state-key query per request (count + max updated_at — every
# write path stamps updated_at) instead of the old blanket 90s TTL, which forced a
# full re-download + re-normalization of the whole catalogue every 90 seconds and
# made a random search pay 10-20s on Render's small CPU ("forever to load").
_REF_GEN = 0
_REF_CACHE = {"gen": -1, "key": None, "rows": [], "built_at": 0.0}
_REF_LOCK = threading.Lock()
# Never rebuild more often than this. The enrichment stamps updated_at on EVERY
# row it processes, so during a run the state key changes every few seconds —
# rebuilding the ~9k-row corpus per search allocated 40-60 MB each time and ran
# the 512 MB instance out of memory. Serving a ≤2-minute-stale catalogue during
# a write burst is invisible to users; the memory spike was not.
_REF_MIN_REBUILD_S = 120.0


def bump_reference_cache():
    global _REF_GEN
    _REF_GEN += 1


def _reference_state_key(db):
    row = db.execute(
        "SELECT COUNT(*) AS n, MAX(updated_at) AS max_upd FROM product_reference"
    ).fetchone()
    return (tuple(row.values()) if isinstance(row, dict) else tuple(row))


def _reference_corpus(db):
    key = (_REF_GEN, _reference_state_key(db))

    def _fresh_enough():
        if _REF_CACHE["key"] == key and _REF_CACHE["rows"]:
            return True
        # Rate-limit ONLY background write-drift (same generation): enrichment
        # stamps rows continuously. An explicit import bumps _REF_GEN and always
        # rebuilds immediately, so freshly imported products search instantly.
        same_gen = bool(_REF_CACHE["rows"]) and _REF_CACHE["key"] is not None \
            and _REF_CACHE["key"][0] == _REF_GEN
        return same_gen and time.time() - _REF_CACHE["built_at"] < _REF_MIN_REBUILD_S

    if _fresh_enough():
        return _REF_CACHE["rows"]
    # One rebuild at a time: without the lock, several concurrent searches after a
    # catalogue change would all rebuild the 9k-row corpus and stack CPU + memory.
    with _REF_LOCK:
        if _fresh_enough():
            return _REF_CACHE["rows"]
        rows = []
        for r in db.execute("SELECT barcode, name, brand, description, product_code FROM product_reference").fetchall():
            d = dict(r)
            name = normalize_search_text(d.get("name", ""))
            brand = normalize_search_text(d.get("brand", ""))
            desc = normalize_search_text(d.get("description", ""))
            rows.append({
                "barcode": d.get("barcode", ""), "name": d.get("name", ""),
                "brand": d.get("brand", ""), "description": d.get("description", ""),
                "product_code": d.get("product_code", ""),
                "_bc": normalized_digits(d.get("barcode", "")),
                "_name": name, "_brand": brand,
                "_hay": " ".join([name, brand, desc]), "_tokens": name.split(),
            })
        _REF_CACHE.update(key=key, rows=rows, built_at=time.time())
    return rows


def _fast_reference_score(row, nq, dq, qtokens, intent_terms, abbrevs):
    """THE search scorer — used for both the catalogue rows and the placed products
    (via _product_search_row). Substring checks only, no regex: query parts
    (nq=normalized query, dq=digits, qtokens=unique tokens) are computed ONCE per
    request. Additive model with a multi-token coverage bonus so more-specific
    matches ('advil extra fort') outrank the generic one ('advil'). Intent
    (symptom→category, capped at 300) and abbreviations floor the score."""
    name, brand, hay, bc, toks = row["_name"], row["_brand"], row["_hay"], row["_bc"], row["_tokens"]
    score = 0
    if dq and bc:
        if bc == dq: score += 1200
        elif len(dq) >= 4 and bc.endswith(dq): score += 900
        elif dq in bc: score += 500
    # Best name match over the full query AND each query word (a strong single-word match
    # like 'tylenol' must beat an abbreviation-only match). Whole-word token match avoids
    # false substrings ('fort' inside 'confort').
    name_score = 0
    if nq:
        if nq == name: name_score = 800
        elif name.startswith(nq): name_score = 650
        elif nq in name: name_score = 450
    for t in qtokens:
        if name.startswith(t): name_score = max(name_score, 500)
        elif t in toks: name_score = max(name_score, 470)
        elif name_score < 460:
            # Planogram names are abbreviated: a name token that PREFIXES the query
            # word is that word abbreviated ('MELAT' ⊂ 'melatonine', 'VITAM' ⊂
            # 'vitamines'). ≥4 chars so tiny tokens ('co') never false-match.
            for tok in toks:
                if len(tok) >= 4 and t.startswith(tok):
                    name_score = 460
                    break
    score += name_score
    if nq and nq in brand:
        score += 200
    if qtokens:
        matched = 0
        for t in qtokens:
            if t in hay:
                matched += 1
        if matched:
            score += (100 + 20 * matched) if matched == len(qtokens) else (25 * matched)
    if intent_terms:                    # symptom → category surfaces, but capped so a
        ib = 0                          # direct name/UPC match (450+) always outranks it
        for t in intent_terms:          # (uncapped, 'toux' ranked dishwasher 'pastilles'
            if name.startswith(t): ib = 300; break    # above real cough syrup)
            elif t in name: ib = max(ib, 300)
            elif t in hay: ib = max(ib, 200)
            else:
                # Same abbreviated-name rule as above: 'dormir' expands to
                # 'melatonine', which must reach a product named 'MELAT …'.
                for tok in toks:
                    if len(tok) >= 4 and t.startswith(tok):
                        ib = 300
                        break
            if ib >= 300:
                break                   # already at the intent cap — stop scanning
        ib = min(ib, 300)
        if ib > score:
            score = ib
    if abbrevs and score < 430:
        for tok in row["_tokens"]:
            done = False
            for a in abbrevs:
                if tok == a or (tok.startswith(a) and tok[len(a):].isdigit()):
                    score = 430
                    done = True
                    break
            if done:
                break
    return score


# ── Placed-products search cache ─────────────────────────────────────────────────
# Same idea as the reference cache above, but for the PLACED products: scoring used
# to re-normalize every product (regex + unicode) for every query word and every
# intent term — ~17 seconds per Client-tab request with ~1000 products on Render's
# small CPU, which also froze every other request behind it on the single worker.
# The cache key (count, max id, max modified_at) changes on any insert/update/delete
# (every write path stamps modified_at), so no explicit invalidation hooks are needed.
_PROD_CACHE = {"key": None, "rows": []}


def _product_search_row(item):
    """Pre-normalized search fields for a placed product, in the shape
    _fast_reference_score expects — computed once per product, not once per term."""
    name = normalize_search_text(item.get("name", ""))
    brand = normalize_search_text(item.get("brand", ""))
    hay = " ".join([
        name, brand,
        normalize_search_text(item.get("description", "")),
        normalize_search_text(item.get("search_terms", "")),
        normalize_search_text(item.get("usage_notes", "")),
        normalize_search_text(item.get("alternative_suggestions", "")),
    ])
    return {"_bc": normalized_digits(item.get("barcode", "")),
            "_name": name, "_brand": brand, "_hay": hay, "_tokens": name.split()}


def client_etag_matches(etag):
    """True if the request's If-None-Match carries our ETag. Tolerates the
    '-gzip' suffix flask-compress appends to ETags of compressed responses."""
    try:
        tags = request.if_none_match.as_set(include_weak=True)
    except Exception:
        return False
    return any(t == etag or t.startswith(f"{etag}-") for t in tags)


def products_state_key(db):
    """Cheap fingerprint of the products table — changes on ANY insert/update/
    delete (every write path stamps modified_at). Drives the search-corpus cache
    AND the /api/products ETag."""
    key_row = db.execute(
        "SELECT COUNT(*) AS n, MAX(id) AS max_id, MAX(modified_at) AS max_mod FROM products"
    ).fetchone()
    return (tuple(key_row.values()) if isinstance(key_row, dict) else tuple(key_row))


def _products_corpus(db):
    """All placed products with their pre-normalized search fields: [(item, row)]."""
    key = products_state_key(db)
    if _PROD_CACHE["key"] == key:
        return _PROD_CACHE["rows"]
    rows = []
    for r in db.execute("SELECT * FROM products").fetchall():
        item = row_to_product(r)
        rows.append((item, _product_search_row(item)))
    _PROD_CACHE.update(key=key, rows=rows)
    return rows


def intent_expansion_terms(query):
    """Product/brand words implied by a symptom query (e.g. 'mal de tête' -> tylenol,
    advil, analgesique). Empty when the query doesn't look like a known need."""
    norm = normalize_search_text(query)
    if not norm:
        return []
    tokens = set(norm.split())
    terms, seen = [], set()
    for entry in INTENT_LEXICON:
        hit = False
        for trigger in entry["triggers"]:
            if (" " in trigger and trigger in norm) or (trigger in tokens):
                hit = True
                break
        if hit:
            for term in entry["expand"]:
                if term not in seen:
                    seen.add(term)
                    terms.append(term)
    return terms


def client_required_concept_groups(query):
    """Required semantic groups for a few high-risk spoken requests.

    A strong match on a generic word such as ``petites`` or ``transparent`` must
    not outrank the actual object. Each returned group requires at least one term
    in the product metadata before the product can enter Client-tab results.
    """
    norm = normalize_search_text(query)
    tokens = set(norm.split())
    groups = []
    cotton_ball_request = (
        bool(tokens.intersection({"watte", "ouate"})) or
        (bool(tokens.intersection({"coton", "cotton"})) and
         bool(tokens.intersection({"boule", "boules", "ball", "balls"})))
    )
    if cotton_ball_request:
        groups.extend([
            ("coton", "cotons", "cotton", "ouate", "watte"),
            ("boule", "boules", "ball", "balls", "ouate"),
        ])

    transparent_dressing_request = (
        any(marker in norm for marker in (
            "membrane transparent", "pansement transparent", "film transparent",
        )) or bool(tokens.intersection({"opsite", "upsite", "upside"}))
    )
    if transparent_dressing_request:
        groups.extend([
            ("transparent", "transparente", "transp", "opsite", "tegaderm"),
            ("pansement", "pans", "diach", "bandage", "band aid", "opsite", "tegaderm"),
        ])
    return groups


def row_matches_client_concepts(row, groups):
    if not groups:
        return True
    hay = f" {row.get('_hay', '')} "
    for group in groups:
        if not any(f" {normalize_search_text(term)} " in hay for term in group):
            return False
    return True


# ── Helpers ────────────────────────────────────────────────────────────────────

def normalized_digits(value):
    return re.sub(r"\D", "", str(value or ""))


def normalize_search_text(value):
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(character for character in text if not unicodedata.combining(character))
    return re.sub(r"[^a-z0-9]+", " ", text.lower()).strip()


def tokenize_search_query(query):
    return [
        token
        for token in normalize_search_text(query).split()
        if len(token) >= 2 and token not in SEARCH_STOPWORDS
    ]


def build_barcode_candidates(barcode):
    raw = str(barcode or "").strip()
    digits = normalized_digits(raw)
    candidates = []
    seen = set()

    def add(value):
        cleaned = str(value or "").strip()
        if cleaned and cleaned not in seen:
            seen.add(cleaned)
            candidates.append(cleaned)

    add(raw)
    add(digits)
    if len(digits) == 13 and digits.startswith("0"):
        add(digits[1:])
    if len(digits) == 12:
        add(f"0{digits}")
    if len(digits) == 14 and digits.startswith("00"):
        add(digits[2:])
    stripped = digits.lstrip("0")
    if stripped and stripped != digits:
        add(stripped)
        if len(stripped) == 12:
            add(f"0{stripped}")
    return candidates


def first_column(row):
    if row is None:
        return None
    if isinstance(row, dict):
        return next(iter(row.values()), None)
    return row[0]


def clamp_non_negative_int(value, fallback=0):
    try:
        return max(0, int(str(value)))
    except (TypeError, ValueError):
        return fallback


def location_sort_key(item):
    side_order = {"Gauche": 0, "Droite": 1}
    return (
        aisle_sort_key(item.get("aisle")),
        side_order.get(str(item.get("side", "")).strip(), 9),
        clamp_non_negative_int(item.get("section", 0)),
        clamp_non_negative_int(item.get("shelf", 0)),
        clamp_non_negative_int(item.get("position", 0)),
        str(item.get("name", "")).lower(),
    )


def row_to_product(product):
    if not product:
        return None
    item = dict(product)
    item["last_change_by"] = item.get("modified_by") or item.get("created_by") or ""
    item["last_change_at"] = item.get("modified_at") or item.get("created_at") or ""
    return item


def archive_and_delete_product(db, product, username, now=None):
    """Soft delete: archive the full product (so we can still say what it was and
    where it used to be), then remove it from the active plan."""
    return archive_and_delete_products(db, [product], username, now)


def archive_and_delete_products(db, products, username, now=None):
    """Archive and remove products in bounded batches to keep plano imports fast."""
    now = now or utc_now_iso()
    items = [dict(product) for product in products]
    for start in range(0, len(items), 100):
        chunk = items[start:start + 100]
        archive_values = []
        for pdict in chunk:
            last_loc = (
                f"Allée {pdict.get('aisle', '')} {side_display_label(pdict.get('side', ''))} "
                f"S{pdict.get('section', '')} T{pdict.get('shelf', '')} P{pdict.get('position', '')}"
            ).strip()
            archive_values.extend((
                now, username, str(pdict.get("barcode", "")), str(pdict.get("name", "")),
                last_loc, json.dumps(pdict, ensure_ascii=False, default=str),
            ))
        placeholders = ",".join("(?, ?, ?, ?, ?, ?)" for _item in chunk)
        db.execute(
            """INSERT INTO removed_products
               (removed_at, removed_by, barcode, name, last_location, product_json)
               VALUES """ + placeholders,
            tuple(archive_values),
        )
        product_ids = [item.get("id") for item in chunk if item.get("id") is not None]
        if product_ids:
            id_placeholders = ",".join("?" for _id in product_ids)
            db.execute(
                f"DELETE FROM products WHERE id IN ({id_placeholders})",
                tuple(product_ids),
            )
    return len(items)


def find_product_at_position(db, aisle, side, section, shelf, position, exclude_id=None):
    query = "SELECT * FROM products WHERE aisle=? AND side=? AND section=? AND shelf=? AND position=?"
    params = [aisle, side, section, shelf, position]
    if exclude_id is not None:
        query += " AND id != ?"
        params.append(exclude_id)
    return db.execute(query, tuple(params)).fetchone()


def find_existing_image_for_barcode(db, barcode, exclude_id=None):
    """Return an image already stored for this barcode (any location), so we
    never lose a product picture when re-adding / moving / re-importing it."""
    if not str(barcode or "").strip():
        return ""
    for candidate in build_barcode_candidates(barcode):
        q = "SELECT image_url FROM products WHERE barcode=? AND TRIM(COALESCE(image_url,'')) <> ''"
        params = [candidate]
        if exclude_id is not None:
            q += " AND id<>?"
            params.append(int(exclude_id))
        q += " ORDER BY id LIMIT 1"
        row = db.execute(q, tuple(params)).fetchone()
        if row:
            return (row["image_url"] if isinstance(row, dict) else row[0]) or ""
    # The broad catalogue is never used as Client inventory, but an image already
    # verified for the same UPC is safe to reuse on the real mapped product.
    for candidate in build_barcode_candidates(barcode):
        row = db.execute(
            "SELECT image_url FROM product_reference "
            "WHERE barcode=? AND TRIM(COALESCE(image_url,'')) <> '' LIMIT 1",
            (candidate,),
        ).fetchone()
        if row:
            return (row["image_url"] if isinstance(row, dict) else row[0]) or ""
    return ""


_REFERENCE_METADATA_FIELDS = ("brand", "description", "image_url", "product_code")


def _barcode_query_values(barcodes):
    values = []
    seen = set()
    for barcode in barcodes or []:
        for candidate in build_barcode_candidates(barcode):
            value = str(candidate or "").strip()
            if value and value not in seen:
                seen.add(value)
                values.append(value)
    return values


def _rows_for_barcodes(db, table, columns, barcodes):
    """Fetch only relevant UPC rows, in bounded IN clauses for SQLite/Postgres."""
    values = _barcode_query_values(barcodes)
    if not values:
        return []
    rows = []
    for start in range(0, len(values), 400):
        chunk = values[start:start + 400]
        placeholders = ",".join("?" for _ in chunk)
        rows.extend(db.execute(
            f"SELECT {columns} FROM {table} WHERE barcode IN ({placeholders})",
            tuple(chunk),
        ).fetchall())
    return rows


def build_reference_metadata_index(db, barcodes=None):
    """Index trusted catalogue metadata by every equivalent UPC representation.

    Planogram imports pass their UPCs so a 100-line import does not download and
    normalize the entire reference catalogue from Postgres first.
    """
    index = {}

    def quality(item):
        return sum(bool(str(item.get(field, "") or "").strip()) for field in _REFERENCE_METADATA_FIELDS)

    if barcodes is None:
        rows = db.execute(
            "SELECT barcode, brand, description, image_url, product_code FROM product_reference"
        ).fetchall()
    else:
        rows = _rows_for_barcodes(
            db, "product_reference",
            "barcode, brand, description, image_url, product_code",
            barcodes,
        )
    for row in rows:
        item = dict(row)
        keys = {
            normalized_digits(candidate)
            for candidate in build_barcode_candidates(item.get("barcode", ""))
            if normalized_digits(candidate)
        }
        for key in keys:
            current = index.get(key)
            if current is None:
                index[key] = dict(item)
                continue
            combined = dict(current)
            item_is_richer = quality(item) > quality(current)
            for field in _REFERENCE_METADATA_FIELDS:
                incoming = str(item.get(field, "") or "").strip()
                if incoming and (item_is_richer or not str(combined.get(field, "") or "").strip()):
                    combined[field] = incoming
            index[key] = combined
    return index


def reference_metadata_for_barcode(reference_index, barcode):
    """Resolve a catalogue row from a raw UPC, including leading-zero variants."""
    best = None
    best_quality = -1
    for candidate in build_barcode_candidates(barcode):
        item = reference_index.get(normalized_digits(candidate))
        if not item:
            continue
        item_quality = sum(
            bool(str(item.get(field, "") or "").strip())
            for field in _REFERENCE_METADATA_FIELDS
        )
        if item_quality > best_quality:
            best = item
            best_quality = item_quality
    return best or {}


def merge_reference_metadata(product, reference):
    """Fill blank product metadata from its UPC reference without overwriting edits."""
    merged = dict(product or {})
    reference = reference or {}
    for field in _REFERENCE_METADATA_FIELDS:
        if not str(merged.get(field, "") or "").strip():
            value = str(reference.get(field, "") or "").strip()
            if value:
                merged[field] = value
    return merged


def planogram_metadata(existing, reference, barcode, product_code=""):
    """Resolve metadata for a plano row without carrying data across different UPCs."""
    existing = dict(existing or {})
    incoming_keys = {
        normalized_digits(candidate) for candidate in build_barcode_candidates(barcode)
        if normalized_digits(candidate)
    }
    existing_keys = {
        normalized_digits(candidate)
        for candidate in build_barcode_candidates(existing.get("barcode", ""))
        if normalized_digits(candidate)
    }
    prior = existing if incoming_keys and incoming_keys & existing_keys else {}
    metadata = merge_reference_metadata(prior, reference)
    return {
        "brand": str(metadata.get("brand", "") or "").strip(),
        "description": str(metadata.get("description", "") or "").strip(),
        "image_url": str(metadata.get("image_url", "") or "").strip(),
        "product_code": str(product_code or metadata.get("product_code", "") or "").strip(),
        "source_url": str(prior.get("source_url", "") or "").strip(),
        "usage_notes": str(prior.get("usage_notes", "") or "").strip(),
        "alternative_suggestions": str(
            prior.get("alternative_suggestions", "") or ""
        ).strip(),
    }


def update_product_metadata_from_reference(db, product, reference, now=None):
    """Persist a blank-field-only metadata merge for one placed product."""
    original = dict(product or {})
    merged = merge_reference_metadata(original, reference)
    if not any(
        str(merged.get(field, "") or "").strip() != str(original.get(field, "") or "").strip()
        for field in _REFERENCE_METADATA_FIELDS
    ):
        return False
    db.execute(
        """UPDATE products SET brand=?, description=?, image_url=?, product_code=?, modified_at=?
           WHERE id=?""",
        (merged.get("brand", ""), merged.get("description", ""),
         merged.get("image_url", ""), merged.get("product_code", ""),
         now or utc_now_iso(), original["id"]),
    )
    product.update(merged)
    return True


def sync_reference_metadata_to_products(db, now=None):
    """Backfill every placed product from the enriched catalogue by equivalent UPC."""
    reference_index = build_reference_metadata_index(db)
    if not reference_index:
        return 0
    linked = 0
    rows = db.execute(
        """SELECT id, barcode, brand, description, image_url, product_code
           FROM products WHERE TRIM(COALESCE(barcode,'')) <> ''"""
    ).fetchall()
    for row in rows:
        product = dict(row)
        reference = reference_metadata_for_barcode(reference_index, product.get("barcode", ""))
        if reference and update_product_metadata_from_reference(db, product, reference, now=now):
            linked += 1
    return linked


# A coalescing queue keeps exactly one image worker alive. Repeated Client searches
# can safely request the same missing UPCs without creating waiting background threads.
_IMAGE_FILL_STATE_LOCK = threading.Lock()
_IMAGE_FILL_PENDING = deque()
_IMAGE_FILL_QUEUED = set()
_IMAGE_FILL_WORKING = set()
_IMAGE_FILL_RETRY_AFTER = {}
_IMAGE_FILL_ACTIVE = False
_IMAGE_MISS_RETRY_SECONDS = 15 * 60
_IMAGE_ERROR_RETRY_SECONDS = 30


def persist_image_for_barcode(db, barcode, image_url, now=None):
    """Fill blank placed/reference images for every equivalent UPC spelling."""
    image_url = str(image_url or "").strip()
    if not image_url:
        return 0
    changed = 0
    timestamp = now or utc_now_iso()
    for candidate in build_barcode_candidates(barcode):
        cursor = db.execute(
            "UPDATE products SET image_url=?, modified_at=? "
            "WHERE barcode=? AND TRIM(COALESCE(image_url,'')) = ''",
            (image_url, timestamp, candidate),
        )
        changed += max(0, int(cursor.rowcount or 0))
        reference_cursor = db.execute(
            "UPDATE product_reference SET image_url=?, updated_at=? "
            "WHERE barcode=? AND TRIM(COALESCE(image_url,'')) = ''",
            (image_url, timestamp, candidate),
        )
        changed += max(0, int(reference_cursor.rowcount or 0))
    return changed


def schedule_image_fill(barcodes, priority=True):
    """Fetch missing product images online in a background thread — fully
    automatic, no user action. Serialized (one fill at a time) and each lookup's
    internal fan-out is capped, so imports can be chained safely all day."""
    global _IMAGE_FILL_ACTIVE
    codes = []
    seen = set()
    for barcode in barcodes or []:
        code = str(barcode or "").strip()
        if code and code not in seen:
            seen.add(code)
            codes.append(code)
    if not codes:
        return

    with _IMAGE_FILL_STATE_LOCK:
        iterable = reversed(codes) if priority else codes
        for code in iterable:
            if _IMAGE_FILL_RETRY_AFTER.get(code, 0) > time.time():
                continue
            if code in _IMAGE_FILL_WORKING:
                continue
            if code in _IMAGE_FILL_QUEUED:
                if priority:
                    try:
                        _IMAGE_FILL_PENDING.remove(code)
                    except ValueError:
                        pass
                    _IMAGE_FILL_PENDING.appendleft(code)
                continue
            if priority:
                _IMAGE_FILL_PENDING.appendleft(code)
            else:
                _IMAGE_FILL_PENDING.append(code)
            _IMAGE_FILL_QUEUED.add(code)
        if _IMAGE_FILL_ACTIVE:
            return
        if not _IMAGE_FILL_PENDING:
            return
        _IMAGE_FILL_ACTIVE = True

    def worker():
        global _IMAGE_FILL_ACTIVE
        from database import connect_db
        from routes.ai import lookup_product_online, online_matches_catalog
        db = None
        processed = 0
        try:
            db = connect_db()
            while True:
                with _IMAGE_FILL_STATE_LOCK:
                    if not _IMAGE_FILL_PENDING:
                        break
                    bc = _IMAGE_FILL_PENDING.popleft()
                    _IMAGE_FILL_QUEUED.discard(bc)
                    _IMAGE_FILL_WORKING.add(bc)
                try:
                    # Reuse an image already known for this UPC (including the
                    # reference catalogue); only then fan out to online sources.
                    img = find_existing_image_for_barcode(db, bc)
                    if not img:
                        # Online pages and parsers are the memory-heavy part. Keep
                        # background lookups out of PDF parsing and wait for each
                        # lookup's source requests to finish before starting another.
                        with memory_intensive_task("product_image"):
                            product = lookup_product_online(
                                bc, max_workers=2, wait_for_cleanup=True,
                                require_image=True,
                            )
                        img = str((product or {}).get("image_url", "")).strip()
                        # Exact UPC sources are still checked against the imported
                        # catalogue name before their image is attached.
                        catalog_rows = _rows_for_barcodes(
                            db, "product_reference", "barcode, name, brand", [bc]
                        )
                        if not catalog_rows:
                            catalog_rows = _rows_for_barcodes(
                                db, "products", "barcode, name, brand", [bc]
                            )
                        if img and catalog_rows:
                            catalog = dict(catalog_rows[0])
                            if not online_matches_catalog(
                                catalog.get("name", ""), catalog.get("brand", ""), product
                            ):
                                img = ""
                    if img:
                        now = utc_now_iso()
                        # Save both on the shelf and in the reusable UPC catalogue
                        # so a later re-import displays the picture immediately.
                        changed = persist_image_for_barcode(db, bc, img, now=now)
                        if changed:
                            db.commit()
                        with _IMAGE_FILL_STATE_LOCK:
                            _IMAGE_FILL_RETRY_AFTER.pop(bc, None)
                    else:
                        print(f"[Images] aucune photo verifiee pour UPC {bc}")
                        with _IMAGE_FILL_STATE_LOCK:
                            _IMAGE_FILL_RETRY_AFTER[bc] = time.time() + _IMAGE_MISS_RETRY_SECONDS
                except Exception as exc:
                    print(f"[Images] echec UPC {bc}: {type(exc).__name__}: {exc}")
                    try:
                        db.rollback()
                    except Exception:
                        pass
                    with _IMAGE_FILL_STATE_LOCK:
                        _IMAGE_FILL_RETRY_AFTER[bc] = time.time() + _IMAGE_ERROR_RETRY_SECONDS
                with _IMAGE_FILL_STATE_LOCK:
                    _IMAGE_FILL_WORKING.discard(bc)
                processed += 1
                if processed % 20 == 0:
                    release_unused_memory()
        finally:
            try:
                if db is not None:
                    db.close()
            except Exception:
                pass
            with _IMAGE_FILL_STATE_LOCK:
                # A fatal connection error must not leave the queue permanently
                # marked active; a later request can start a fresh worker.
                _IMAGE_FILL_ACTIVE = False
                # Do not spin-restart immediately when the database itself is down.
                # The next normal scheduling event can retry after recovery.
                pending_snapshot = list(_IMAGE_FILL_PENDING) if db is not None else []
            if pending_snapshot:
                schedule_image_fill(pending_snapshot, priority=False)
            release_unused_memory()

    threading.Thread(target=worker, daemon=True).start()


def hydrate_candidate_images(products):
    """Attach any already-known UPC image to mapped Client results immediately,
    then queue only truly missing images for background lookup."""
    db = get_db()
    missing = [
        product for product in products
        if not str(product.get("image_url", "") or "").strip()
        and str(product.get("barcode", "") or "").strip()
    ]
    if not missing:
        return products

    barcodes = [product.get("barcode", "") for product in missing]
    image_by_barcode = {}
    for table in ("products", "product_reference"):
        for row in _rows_for_barcodes(db, table, "barcode, image_url", barcodes):
            item = dict(row)
            image_url = str(item.get("image_url", "") or "").strip()
            if not image_url:
                continue
            for candidate in build_barcode_candidates(item.get("barcode", "")):
                key = normalized_digits(candidate)
                if key:
                    image_by_barcode.setdefault(key, image_url)

    for product in missing:
        barcode = str(product.get("barcode", "") or "").strip()
        image_url = ""
        for candidate in build_barcode_candidates(barcode):
            image_url = image_by_barcode.get(normalized_digits(candidate), "")
            if image_url:
                break
        if image_url:
            product["image_url"] = image_url
    # Persist reused images and resolve unknown ones off the request thread.
    schedule_image_fill(barcodes)
    return products


def integrity_conflict_message(exc):
    text = str(exc).lower()
    if "barcode" in text:
        return "Ce code-barres existe déjà ailleurs dans la base."
    return "Cette position’est déjà occupée."


def rank_products_for_query(products, query, limit=60):
    """Rank placed products for a query. Each product is normalized ONCE and scored
    with the same fast additive scorer as the catalogue (substring checks only) —
    the old per-(product × variant × intent-term) re-normalization took seconds for
    ~1000 products on Render's small CPU and froze every other request behind it.
    The scorer keeps the invariants: barcode > exact name > name prefix > name
    contains > whole-word token, intent capped at 300, abbreviation floor 430."""
    nq = normalize_search_text(query)
    dq = normalized_digits(query)
    qtokens = list(dict.fromkeys(tokenize_search_query(query)))
    intent_terms = intent_expansion_terms(query)
    abbrevs = abbreviation_terms(query)
    if not nq and not dq and not intent_terms:
        return []
    ranked = []
    for product in products:
        score = _fast_reference_score(_product_search_row(product), nq, dq,
                                      qtokens, intent_terms, abbrevs)
        if score > 0:
            ranked.append((score, product))
    # Tiebreak: in-stock products before ruptures, then by location.
    ranked.sort(key=lambda item: (-item[0], 1 if item[1].get("in_stock") == 0 else 0,
                                   location_sort_key(item[1])))
    items = [product for _, product in ranked]
    return items[:limit] if limit else items


def rank_products_by_code(products, query, limit=60):
    """Search strictly on the Familiprix/pharmacy code — never on barcode or
    name — so this is the explicit "Code" mode and cannot be confused with a UPC."""
    needle = normalized_digits(query) or normalize_search_text(query)
    if not needle:
        return []
    ranked = []
    for product in products:
        code = str(product.get("product_code", "")).strip()
        if not code:
            continue
        haystack = normalized_digits(code) or normalize_search_text(code)
        if not haystack:
            continue
        if haystack == needle:
            score = 1000
        elif haystack.startswith(needle):
            score = 700
        elif needle in haystack:
            score = 400
        else:
            continue
        ranked.append((score, product))
    ranked.sort(key=lambda item: (-item[0], location_sort_key(item[1])))
    items = [product for _, product in ranked]
    return items[:limit] if limit else items


def rank_reference_for_query(query, limit=40, exclude_barcodes=None):
    """Rank the reference catalogue (all products imported from planograms + past
    scans) for a name/keyword query. These products have NO shelf location yet, so each
    is flagged catalog_only=True and the UI shows 'position à confirmer'. Uses the same
    scorer + intent expansion as the placed-product search."""
    db = get_db()
    nq = normalize_search_text(query)
    dq = normalized_digits(query)
    qtokens = list(dict.fromkeys(tokenize_search_query(query)))
    intent_terms = intent_expansion_terms(query)
    abbrevs = abbreviation_terms(query)
    if not nq and not dq and not intent_terms:
        return []
    exclude = exclude_barcodes or set()
    ranked = []
    for row in _reference_corpus(db):
        if row["_bc"] and row["_bc"] in exclude:
            continue
        score = _fast_reference_score(row, nq, dq, qtokens, intent_terms, abbrevs)
        if score > 0:
            ranked.append((score, row))
    ranked.sort(key=lambda x: (-x[0], x[1]["_name"]))
    return [{"barcode": r["barcode"], "name": r["name"], "brand": r["brand"],
             "description": r["description"], "product_code": r["product_code"],
             "catalog_only": True, "in_stock": 1} for _, r in ranked[:limit]]


def _fuzzy_product_score(row, query_tokens):
    """Typo-aware name/brand score. Kept deliberately strict so a misspelling such
    as ``advile`` reaches ``Advil`` without turning short, generic words into noise."""
    product_tokens = list(dict.fromkeys(row["_tokens"] + row["_brand"].split()))
    best = 0
    for query_token in query_tokens:
        if len(query_token) < 4 or query_token in product_tokens:
            continue
        variants = [query_token]
        # Spoken French is often typed without the apostrophe: "dadvile" should
        # also be compared as "advile", and "ladvil" as "advil".
        if len(query_token) >= 6 and query_token[0] in {"d", "l"}:
            variants.append(query_token[1:])
        for variant in variants:
            for product_token in product_tokens:
                if len(product_token) < 4 or variant[0] != product_token[0]:
                    continue
                ratio = SequenceMatcher(None, variant, product_token).ratio()
                if ratio >= 0.78:
                    best = max(best, int(360 + (ratio * 160)))
    return best


def _client_candidate_id(item, catalog_only=False):
    if not catalog_only and item.get("id") is not None:
        return f"product:{item['id']}"
    barcode = normalized_digits(item.get("barcode", ""))
    return f"reference:{barcode}" if barcode else f"reference-name:{normalize_search_text(item.get('name', ''))}"


def _mapped_client_products(db):
    """Return one client-facing product per UPC/name with every plan location."""
    products = []
    products_by_key = {}

    def product_key(item, row):
        return ("barcode", row["_bc"]) if row["_bc"] else (
            "name", row["_name"], row["_brand"]
        )

    def location_for(item):
        return {
            "aisle": str(item.get("aisle", "")).strip(),
            "side": str(item.get("side", "")).strip(),
            "section": str(item.get("section", "1")).strip() or "1",
            "shelf": str(item.get("shelf", "")).strip(),
            "position": str(item.get("position", "")).strip(),
        }

    for item, row in _products_corpus(db):
        key = product_key(item, row)
        if key in products_by_key:
            existing = products_by_key[key]["item"]
            location = location_for(item)
            if location not in existing["locations"]:
                existing["locations"].append(location)
            if not existing.get("image_url") and item.get("image_url"):
                existing["image_url"] = item.get("image_url")
            existing["in_stock"] = 1 if existing.get("in_stock") or item.get("in_stock") else 0
            existing["is_plano"] = 1 if existing.get("is_plano") or item.get("is_plano") else 0
            continue
        product = dict(item)
        product["client_id"] = _client_candidate_id(product)
        product["catalog_only"] = False
        product["locations"] = [location_for(product)]
        document = {"item": product, "row": row, "source_rank": 0}
        products.append(document)
        products_by_key[key] = document
    return products


def client_products_by_ids(candidate_ids, limit=60):
    """Reload trusted mapped products selected in an earlier client turn."""
    wanted = {str(value or "").strip() for value in candidate_ids or []}
    if not wanted:
        return []
    products = []
    for document in _mapped_client_products(get_db()):
        product = document["item"]
        if str(product.get("client_id", "")) in wanted:
            products.append(product)
        if len(products) >= max(1, min(int(limit), 100)):
            break
    return products


def hybrid_client_candidates(question, query_plan, limit=60):
    """Hybrid retrieval for the one-button Client search.

    A fast query plan supplies search phrases and constraints. This retriever
    combines the existing deterministic scorer, description-aware
    BM25-style relevance, strict fuzzy name matching, intent expansion and exact
    UPC matching. Only ``products`` rows are searched: ``product_reference`` may
    enrich metadata/images, but can never become store inventory in Client search.
    """
    db = get_db()
    documents = _mapped_client_products(db)
    required_concepts = client_required_concept_groups(question)

    def clean_list(value, max_items=20):
        if not isinstance(value, list):
            return []
        out = []
        for raw in value:
            text = str(raw or "").strip()
            if text and text not in out:
                out.append(text)
            if len(out) >= max_items:
                break
        return out

    corrected = str(query_plan.get("corrected_query", "") or "").strip()
    phrases = [question]
    if corrected and normalize_search_text(corrected) != normalize_search_text(question):
        phrases.append(corrected)
    phrases.extend(clean_list(query_plan.get("search_queries"), 10))
    phrases.extend(clean_list(query_plan.get("keywords"), 16))
    phrases = list(dict.fromkeys(p for p in phrases if p))

    must_include = clean_list(query_plan.get("must_include"), 10)
    exclude = [normalize_search_text(x) for x in clean_list(query_plan.get("exclude"), 10)]
    exclude = [x for x in exclude if x]

    prepared_queries = []
    for phrase in phrases:
        nq = normalize_search_text(phrase)
        dq = normalized_digits(phrase)
        qtokens = list(dict.fromkeys(tokenize_search_query(phrase)))
        intent_terms = intent_expansion_terms(phrase)
        abbrevs = abbreviation_terms(phrase)
        if nq or dq or intent_terms:
            prepared_queries.append((nq, dq, qtokens, intent_terms, abbrevs))

    retrieval_tokens = []
    for phrase in phrases + must_include:
        retrieval_tokens.extend(tokenize_search_query(phrase))
    retrieval_tokens = list(dict.fromkeys(t for t in retrieval_tokens if len(t) >= 2))[:32]

    # Query-specific BM25 statistics. Only terms from this request are counted,
    # so this remains fast over the cached 9k catalogue on Render's small CPU.
    tokenized_documents = []
    document_frequency = Counter()
    total_length = 0
    retrieval_token_set = set(retrieval_tokens)
    for document in documents:
        tokens = document["row"]["_hay"].split()
        counts = Counter(token for token in tokens if token in retrieval_token_set)
        document_length = max(1, len(tokens))
        tokenized_documents.append((counts, document_length))
        total_length += document_length
        for token in counts:
            if counts[token]:
                document_frequency[token] += 1
    doc_count = max(1, len(documents))
    average_length = total_length / doc_count

    upc_digits = set()
    for run in re.findall(r"\d[\d\s\-]{6,18}\d", question):
        digits = normalized_digits(run)
        if 8 <= len(digits) <= 14:
            upc_digits.update(normalized_digits(c) for c in build_barcode_candidates(digits))

    scored = []
    for document, token_data in zip(documents, tokenized_documents):
        counts, doc_length = token_data
        row = document["row"]
        if not row_matches_client_concepts(row, required_concepts):
            continue
        lexical = 0
        for nq, dq, qtokens, intent_terms, abbrevs in prepared_queries:
            lexical = max(lexical, _fast_reference_score(
                row, nq, dq, qtokens, intent_terms, abbrevs
            ))

        fuzzy = _fuzzy_product_score(row, retrieval_tokens)
        bm25 = 0.0
        for token in retrieval_tokens:
            frequency = counts.get(token, 0)
            if not frequency:
                continue
            df = document_frequency[token]
            inverse_frequency = math.log(1 + ((doc_count - df + 0.5) / (df + 0.5)))
            denominator = frequency + 1.2 * (1 - 0.75 + 0.75 * doc_length / average_length)
            bm25 += inverse_frequency * ((frequency * 2.2) / denominator)

        hay = row["_hay"]
        must_hits = sum(1 for value in must_include if normalize_search_text(value) in hay)
        exclusion_penalty = 260 if any(value in hay for value in exclude) else 0
        exact_upc = bool(upc_digits and row["_bc"] in upc_digits)
        score = max(lexical, fuzzy) + min(260, int(bm25 * 34)) + (must_hits * 35) - exclusion_penalty
        if exact_upc:
            score = max(score, 2000)
        if score >= 90:
            scored.append((score, document["source_rank"], document["item"]))

    scored.sort(key=lambda entry: (
        -entry[0], entry[1], 1 if entry[2].get("in_stock") == 0 else 0,
        normalize_search_text(entry[2].get("name", "")),
    ))
    return [item for _, _, item in scored[:max(1, min(int(limit), 100))]]


# ── Routes ─────────────────────────────────────────────────────────────────────

@products_bp.route("/api/products", methods=["GET"])
def get_products():
    """Full catalog for the phones' local cache. ETag'd on the products state key:
    when nothing changed since the phone's last fetch it gets an instant 304 and
    reuses its stored copy — this endpoint is fetched at every app open and tab
    switch, and used to re-serialize ~1 MB of JSON every time."""
    db = get_db()
    etag = hashlib.md5(repr(products_state_key(db)).encode()).hexdigest()
    if client_etag_matches(etag):
        return "", 304
    products = sorted((item for item, _ in _products_corpus(db)), key=location_sort_key)
    response = jsonify(products)
    response.set_etag(etag, weak=True)
    return response


@products_bp.route("/api/products/images", methods=["GET"])
def get_product_images():
    """Small polling endpoint for Client cards while background UPC image lookup
    runs. Avoids re-downloading the full product list just to reveal new pictures."""
    ids = []
    for raw in str(request.args.get("ids", "")).split(","):
        try:
            product_id = int(raw)
        except (TypeError, ValueError):
            continue
        if product_id > 0 and product_id not in ids:
            ids.append(product_id)
        if len(ids) >= 100:
            break
    if not ids:
        return jsonify({"images": {}})
    db = get_db()
    images = {}
    placeholders = ",".join("?" for _ in ids)
    rows = db.execute(
        f"SELECT id, image_url, barcode FROM products WHERE id IN ({placeholders})", tuple(ids)
    ).fetchall()
    missing_barcodes = []
    for row in rows:
        item = dict(row)
        value = str(item.get("image_url", "") or "").strip()
        if value:
            images[str(item["id"])] = value
        elif str(item.get("barcode", "") or "").strip():
            missing_barcodes.append(item["barcode"])
    # A product the employee is actively viewing jumps ahead of the background
    # backlog. The response stays instant; enrichment remains off-request.
    schedule_image_fill(missing_barcodes)
    return jsonify({"images": images})


@products_bp.route("/api/products/search", methods=["GET"])
def search_products():
    query = request.args.get("q", "").strip()
    if not query:
        return jsonify([])
    field = (request.args.get("field") or "").strip().lower()
    limit = min(max(clamp_non_negative_int(request.args.get("limit", "60"), 60), 1), 120)
    db = get_db()
    corpus = _products_corpus(db)   # cached: no per-request fetch + re-normalization
    if field == "code":
        items = rank_products_by_code([item for item, _ in corpus], query, limit=limit)
        return jsonify(items)
    nq = normalize_search_text(query)
    dq = normalized_digits(query)
    qtokens = list(dict.fromkeys(tokenize_search_query(query)))
    intent_terms = intent_expansion_terms(query)
    abbrevs = abbreviation_terms(query)
    if not nq and not dq and not intent_terms:
        return jsonify([])
    ranked = []
    for item, row in corpus:
        score = _fast_reference_score(row, nq, dq, qtokens, intent_terms, abbrevs)
        if score > 0:
            ranked.append((score, item))
    ranked.sort(key=lambda e: (-e[0], 1 if e[1].get("in_stock") == 0 else 0,
                               location_sort_key(e[1])))
    return jsonify([item for _, item in ranked[:limit]])


@products_bp.route("/api/client/find", methods=["GET"])
def client_find():
    """Fast inventory-safe lookup from the current mapped store plan only."""
    query = request.args.get("q", "").strip()
    if not query:
        return jsonify([])
    limit = min(max(clamp_non_negative_int(request.args.get("limit", "30"), 30), 1), 60)
    nq = normalize_search_text(query)
    dq = normalized_digits(query)
    qtokens = list(dict.fromkeys(tokenize_search_query(query)))
    intent_terms = intent_expansion_terms(query)
    abbrevs = abbreviation_terms(query)
    required_concepts = client_required_concept_groups(query)
    if not nq and not dq and not intent_terms:
        return jsonify([])
    db = get_db()
    scored = []
    # Minimum meaningful score for the CLIENT tab: every real signal clears it
    # (whole-word name token 470+, intent 200-300, brand 200, all-tokens-covered
    # 120+, barcode 500+). What it drops is partial-coverage-only noise (25/token)
    # — the "random products" that padded the list when little else matched.
    MIN_SCORE = 100
    # The pre-normalized in-memory corpus keeps this endpoint in milliseconds.
    # Imported-but-unplaced catalogue rows are excluded so they cannot be shown
    # to an employee as current store inventory.
    for document in _mapped_client_products(db):
        item = document["item"]
        prow = document["row"]
        if not row_matches_client_concepts(prow, required_concepts):
            continue
        s = max(
            _fast_reference_score(prow, nq, dq, qtokens, intent_terms, abbrevs),
            _fuzzy_product_score(prow, qtokens),
        )
        if s >= MIN_SCORE:
            scored.append((s, 0, item))
    scored.sort(key=lambda x: (-x[0], x[1], str(x[2].get("name", "")).lower()))
    return jsonify([it for _, _, it in scored[:limit]])


@products_bp.route("/api/products/reference-search", methods=["GET"])
def reference_search():
    """Search the reference catalogue (imported planograms) for products we carry but
    that aren't placed on a shelf yet. Excludes barcodes already placed to avoid dups."""
    query = request.args.get("q", "").strip()
    if not query:
        return jsonify([])
    limit = min(max(clamp_non_negative_int(request.args.get("limit", "40"), 40), 1), 80)
    db = get_db()
    placed = {normalized_digits(r["barcode"]) for r in
              db.execute("SELECT barcode FROM products WHERE TRIM(COALESCE(barcode,'')) <> ''").fetchall()}
    return jsonify(rank_reference_for_query(query, limit=limit, exclude_barcodes=placed))


@products_bp.route("/api/products/barcode/<barcode>", methods=["GET"])
def get_by_barcode(barcode):
    db = get_db()
    for candidate in build_barcode_candidates(barcode):
        product = db.execute(
            "SELECT * FROM products WHERE barcode = ? ORDER BY id LIMIT 1", (candidate,)
        ).fetchone()
        if product:
            return jsonify(row_to_product(product))
    return jsonify({"error": "Produit non trouvé"}), 404


@products_bp.route("/api/products", methods=["POST"])
def add_product():
    username, error = require_editor()
    if error:
        return error
    data = request.get_json(silent=True) or {}
    name     = data.get("name", "").strip()
    brand    = data.get("brand", "").strip()
    description = data.get("description", "").strip()
    image_url = data.get("image_url", "").strip()
    source_url = data.get("source_url", "").strip()
    search_terms = data.get("search_terms", "").strip()
    usage_notes = data.get("usage_notes", "").strip()
    alternative_suggestions = data.get("alternative_suggestions", "").strip()
    barcode  = data.get("barcode", "").strip()
    product_code = data.get("product_code", "").strip()
    aisle    = data.get("aisle", "").strip()
    side     = data.get("side", "").strip()
    section  = data.get("section", "").strip() or "1"
    shelf    = data.get("shelf", "").strip()
    position = data.get("position", "").strip()
    is_plano = 1 if data.get("is_plano", 0) else 0
    flipped  = 1 if data.get("flipped_label", 0) else 0
    underneath = str(data.get("underneath_label", "")).strip()
    if underneath:
        flipped = 1   # an underneath plano product implies a flipped étiquette

    if not all([name, aisle, side, section, shelf, position]):
        return jsonify({"error": "Champs obligatoires manquants"}), 400

    db = get_db()
    if not image_url:
        image_url = find_existing_image_for_barcode(db, barcode)
    is_valid_slot, slot_error = validate_layout_slot(db, aisle, side, section, shelf, position)
    if not is_valid_slot:
        return jsonify({"error": slot_error}), 400
    occupied = find_product_at_position(db, aisle, side, section, shelf, position)
    if occupied:
        return jsonify({
            "error": f'Position déjà occupée par "{occupied["name"]}" (code {occupied["barcode"] or "sans code"}).'
        }), 409

    try:
        cursor = db.execute(
            """
            INSERT INTO products (name, brand, description, image_url, source_url, search_terms, usage_notes, alternative_suggestions, barcode, product_code, aisle, side, section, shelf, position, is_plano, flipped_label, underneath_label, created_by, created_at, modified_by, modified_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (name, brand, description, image_url, source_url, search_terms, usage_notes,
             alternative_suggestions, barcode, product_code, aisle, side, section, shelf, position,
             is_plano, flipped, underneath,
             username, utc_now_iso(), username, utc_now_iso())
        )
    except DatabaseIntegrityError as exc:
        return jsonify({"error": integrity_conflict_message(exc)}), 409
    db.commit()
    product_id = cursor.lastrowid
    product = db.execute("SELECT * FROM products WHERE id=?", (product_id,)).fetchone()
    from routes.gist import _schedule_gist_backup
    _schedule_gist_backup(db)
    if not image_url and barcode:
        schedule_image_fill([barcode])   # fetch a picture online in the background
    return jsonify({
        "success": True,
        "message": f'"{name}" ajoute avec succes!',
        "product": row_to_product(product) if product else None
    })


@products_bp.route("/api/products/<int:product_id>", methods=["PUT"])
def update_product(product_id):
    username, error = require_editor()
    if error:
        return error
    data = request.get_json(silent=True) or {}
    missing = [k for k in ("name", "aisle", "side", "shelf", "position") if not str(data.get(k, "")).strip()]
    if missing:
        return jsonify({"error": f"Champs obligatoires manquants: {', '.join(missing)}"}), 400
    db = get_db()
    existing = db.execute("SELECT * FROM products WHERE id=?", (product_id,)).fetchone()
    if not existing:
        return jsonify({"error": "Produit non trouve"}), 404
    is_valid_slot, slot_error = validate_layout_slot(
        db,
        str(data["aisle"]).strip(),
        str(data["side"]).strip(),
        str(data.get("section", "1")).strip() or "1",
        str(data["shelf"]).strip(),
        str(data["position"]).strip(),
    )
    if not is_valid_slot:
        return jsonify({"error": slot_error}), 400
    occupied = find_product_at_position(
        db,
        str(data["aisle"]).strip(),
        str(data["side"]).strip(),
        str(data.get("section", "1")).strip() or "1",
        str(data["shelf"]).strip(),
        str(data["position"]).strip(),
        exclude_id=product_id,
    )
    if occupied:
        return jsonify({
            "error": f'Position déjà occupée par "{occupied["name"]}" (code {occupied["barcode"] or "sans code"}).'
        }), 409

    # Never blank an image: keep the new one, else the existing one, else any
    # image already known for this barcode.
    new_barcode = str(data.get("barcode", existing["barcode"]) or "").strip()
    resolved_image = (str(data.get("image_url", "")).strip()
                      or str(existing["image_url"] or "").strip()
                      or find_existing_image_for_barcode(db, new_barcode, exclude_id=product_id))

    try:
        result = db.execute(
            "UPDATE products SET name=?, brand=?, description=?, image_url=?, source_url=?, search_terms=?, usage_notes=?, alternative_suggestions=?, barcode=?, product_code=?, aisle=?, side=?, section=?, shelf=?, position=?, modified_by=?, modified_at=? WHERE id=?",
            (
                data["name"],
                data.get("brand", existing["brand"]),
                data.get("description", existing["description"]),
                resolved_image,
                data.get("source_url", existing["source_url"]),
                data.get("search_terms", existing["search_terms"]),
                data.get("usage_notes", existing["usage_notes"]),
                data.get("alternative_suggestions", existing["alternative_suggestions"]),
                data.get("barcode", existing["barcode"]),
                data.get("product_code", existing["product_code"]),
                data["aisle"], data["side"], data.get("section", "1"),
                data["shelf"], data["position"],
                username, utc_now_iso(), product_id,
            )
        )
    except DatabaseIntegrityError as exc:
        return jsonify({"error": integrity_conflict_message(exc)}), 409
    db.commit()
    product = db.execute("SELECT * FROM products WHERE id=?", (product_id,)).fetchone()
    from routes.gist import _schedule_gist_backup
    _schedule_gist_backup(db)
    return jsonify({"success": True, "product": row_to_product(product)})


@products_bp.route("/api/products/<int:product_id>", methods=["DELETE"])
def delete_product(product_id):
    username, error = require_editor()
    if error:
        return error
    db = get_db()
    product = db.execute("SELECT * FROM products WHERE id=?", (product_id,)).fetchone()
    if not product:
        return jsonify({"error": "Produit non trouve."}), 404
    # Soft delete: archive the full product so we can still answer questions
    # about it later (which product it was, where it used to be), then remove it
    # from the active plan (frees its slot).
    archive_and_delete_product(db, product, username)
    db.commit()
    from routes.gist import _schedule_gist_backup
    _schedule_gist_backup(db)
    return jsonify({"success": True, "message": f'Produit retiré par {username}: {product["name"]} (conservé dans l’historique)'})


@products_bp.route("/api/products/removed", methods=["GET"])
def removed_products_list():
    """Archive of removed products — searchable so a question about an old
    product can still be answered (what it was, where it used to be)."""
    db = get_db()
    q = (request.args.get("q") or "").strip().lower()
    rows = db.execute("SELECT * FROM removed_products ORDER BY id DESC LIMIT 500").fetchall()
    out = []
    for r in rows:
        d = dict(r)
        if q:
            hay = f"{d.get('name','')} {d.get('barcode','')}".lower()
            if q not in hay:
                continue
        out.append({
            "id": d.get("id"), "name": d.get("name", ""), "barcode": d.get("barcode", ""),
            "last_location": d.get("last_location", ""), "removed_at": d.get("removed_at", ""),
            "removed_by": d.get("removed_by", ""),
        })
    return jsonify(out)


@products_bp.route("/api/products/<int:product_id>/stock", methods=["POST"])
def set_product_stock(product_id):
    """Flip a product in/out of stock. Out-of-stock plano products are the ones
    whose étiquette must be flipped (price tag removed / replaced)."""
    username, error = require_editor()
    if error:
        return error
    data = request.get_json() or {}
    in_stock = 1 if data.get("in_stock", True) else 0
    db = get_db()
    result = db.execute(
        "UPDATE products SET in_stock=?, modified_by=?, modified_at=? WHERE id=?",
        (in_stock, username, utc_now_iso(), product_id)
    )
    if result.rowcount == 0:
        return jsonify({"error": "Produit non trouvé."}), 404
    db.commit()
    product = db.execute("SELECT * FROM products WHERE id=?", (product_id,)).fetchone()
    return jsonify({"success": True, "product": row_to_product(product)})


@products_bp.route("/api/products/<int:product_id>/flipped-label", methods=["POST"])
def set_flipped_label(product_id):
    """For a hors-plano product: mark that a plano étiquette is flipped underneath,
    and optionally record WHICH plano product it is (name/UPC). Passing an
    underneath value also sets flipped=1; clearing the flip clears the underneath."""
    username, error = require_editor()
    if error:
        return error
    data = request.get_json() or {}
    has_under = "underneath" in data
    underneath = str(data.get("underneath", "")).strip()
    flipped = 1 if (data.get("flipped", False) or underneath) else 0
    db = get_db()
    if has_under:
        result = db.execute(
            "UPDATE products SET flipped_label=?, underneath_label=?, modified_by=?, modified_at=? WHERE id=?",
            (flipped, underneath, username, utc_now_iso(), product_id)
        )
    else:
        # Toggle only; if turning the flip OFF, also clear any underneath product.
        result = db.execute(
            "UPDATE products SET flipped_label=?, underneath_label=CASE WHEN ?=0 THEN '' ELSE underneath_label END, modified_by=?, modified_at=? WHERE id=?",
            (flipped, flipped, username, utc_now_iso(), product_id)
        )
    if result.rowcount == 0:
        return jsonify({"error": "Produit non trouvé."}), 404
    db.commit()
    product = db.execute("SELECT * FROM products WHERE id=?", (product_id,)).fetchone()
    return jsonify({"success": True, "product": row_to_product(product)})


@products_bp.route("/api/products/<int:product_id>/plano", methods=["POST"])
def set_is_plano(product_id):
    """Flag a product as plano or hors-plano."""
    username, error = require_editor()
    if error:
        return error
    data = request.get_json() or {}
    is_plano = 1 if data.get("is_plano", False) else 0
    db = get_db()
    result = db.execute(
        "UPDATE products SET is_plano=?, modified_by=?, modified_at=? WHERE id=?",
        (is_plano, username, utc_now_iso(), product_id)
    )
    if result.rowcount == 0:
        return jsonify({"error": "Produit non trouvé."}), 404
    db.commit()
    product = db.execute("SELECT * FROM products WHERE id=?", (product_id,)).fetchone()
    return jsonify({"success": True, "product": row_to_product(product)})


def fixture_for_side(config, side):
    """The {shelves, labels} dict of a fixture side (Façade A/B or a présentoir
    façade named '<présentoir> - <façade>'), or None for the aisle côtés."""
    if side == "Façade A":
        return config.get("facade_a")
    if side == "Façade B":
        return config.get("facade_b")
    for pres in (config.get("presentoirs") or []):
        for facade in (pres.get("facades") or []):
            if side == f"{pres.get('name', '')} - {facade.get('name', '')}":
                return facade
    return None


def plan_planogram_flow(config, side, start_section, start_tablette, lines, shrink=False):
    """Flow plano lines across the côté's EXISTING sections, starting at
    (start_section, start_tablette). The number of tablettes per section is the
    plan's and is never changed; only the number of positions on a tablette is
    adjusted to fit the plano. A plano "shelf" = all lines sharing a plano
    tablette; shelves are laid into store tablettes in order, rolling into the
    next section when one is full.

    Positions follow the plano: a tablette grows to fit, and when `shrink` is on
    it also shrinks to the plano's count (the import then archives any product
    that falls past the new end). When `shrink` is off, positions only grow.

    `lines` = [{"tablette": int, "position": int, "p": <payload>}, ...].
    Returns (placements, overflow_shelf_count) and adjusts position counts in
    `config` in place. Each placement = (section_no, shelf_no, position_no, line)."""
    fixture = None if side in ("Gauche", "Droite") else fixture_for_side(config, side)
    # Slots = (shelves-container, section_no, shelf_index) in fill order.
    slots = []
    if fixture is not None:
        # Fixture sides (Façade A/B, présentoir façades) are ONE flat run of
        # tablettes with no sections: fill from start_tablette downward. Their
        # products always store section '1'.
        for ti in range(max(0, start_tablette - 1), len(fixture.get("shelves", []))):
            slots.append((fixture, 1, ti))
    single_sided = False
    if fixture is None:
        sides_cfg = (config.get("sides", {}) or {})
        sections = (sides_cfg.get(side, {}) or {}).get("sections", [])
        other = "Droite" if side == "Gauche" else "Gauche"
        # A one-sided "aisle" (Labo, Caisse, a wall/counter…) has no opposite côté,
        # so it has no real Façade A/B ends: it is read PLAINLY left→right —
        # ascending sections and positions exactly as the planogram numbers them.
        # The Façade-anchored direction rule only applies to real two-sided aisles.
        single_sided = not ((sides_cfg.get(other, {}) or {}).get("sections", []))
        # Côté A is traversed from Façade B toward Façade A, opposite the section
        # numbering. It therefore continues through decreasing section numbers
        # (for example S9, then S8). Côté B and one-sided aisles continue through
        # increasing sections. Tablettes always keep their normal top-to-bottom order.
        start_idx = min(max(0, start_section - 1), max(0, len(sections) - 1))
        descending = (side == "Gauche" and not single_sided)
        section_indices = range(start_idx, -1, -1) if descending else range(start_idx, len(sections))
        for si in section_indices:
            shelf_count = len(sections[si].get("shelves", []))
            first_t = (start_tablette - 1) if si == start_idx else 0
            for ti in range(max(0, first_t), shelf_count):
                slots.append((sections[si], si + 1, ti))

    # Group plano lines into shelves by their plano tablette, in ascending order.
    by_tablette = {}
    for ln in lines:
        by_tablette.setdefault(ln["tablette"], []).append(ln)

    placements = []
    overflow = 0
    # Section direction never changes the contents of a tablette. Product
    # positions keep the planogram numbering on both côtés and every fixture.
    for idx, ptab in enumerate(sorted(by_tablette.keys())):
        shelf_lines = sorted(by_tablette[ptab], key=lambda l: l["position"])
        if idx >= len(slots):
            overflow += 1
            continue
        container, sec_no, ti = slots[idx]
        max_pos = max((l["position"] for l in shelf_lines), default=0)
        # Positions follow the plano: always grow to fit; shrink to the plano's
        # count too when shrink is on (import archives anything past the new end).
        if shrink or max_pos > container["shelves"][ti]:
            container["shelves"][ti] = max_pos
        for ln in shelf_lines:
            placements.append((sec_no, ti + 1, ln["position"], ln))
    return placements, overflow


@products_bp.route("/api/products/bulk-import", methods=["POST"])
def bulk_import_products():
    username, error = require_editor()
    if error:
        return error

    data           = request.get_json() or {}
    aisle          = str(data.get("aisle", "")).strip()
    side           = str(data.get("side", "Droite")).strip()
    try:
        start_section  = max(1, int(data.get("start_section", data.get("section", 1)) or 1))
        start_tablette = max(1, int(data.get("start_tablette", 1) or 1))
        tablette_start = int(data.get("tablette_start", 1))
        tablette_end   = int(data.get("tablette_end", 99))
    except (TypeError, ValueError):
        return jsonify({"success": False, "error": "Bornes du planogramme invalides."}), 400
    replace        = bool(data.get("replace_existing", False))
    skip_ns        = bool(data.get("skip_non_stock", False))
    products       = data.get("products", [])

    if not aisle:
        return jsonify({"success": False, "error": "Allée requise."}), 400
    if tablette_start < 1 or tablette_end < tablette_start:
        return jsonify({"success": False, "error": "Début ou fin de tablette invalide."}), 400
    if not isinstance(products, list):
        return jsonify({"success": False, "error": "Liste de produits invalide."}), 400

    from routes.layout import get_layout_row, normalize_layout_config, layout_metrics
    db = get_db()
    row = get_layout_row(db, aisle)
    if not row:
        return jsonify({"success": False, "error": f"L'allée {aisle} n'existe pas dans le plan. Créez d'abord l'allée."}), 400
    if "expected_layout_modified_at" in data:
        expected_layout_version = str(data.get("expected_layout_modified_at") or "")
        current_layout_version = str(row["modified_at"] or "")
        if expected_layout_version != current_layout_version:
            return jsonify({
                "success": False,
                "code": "stale_layout",
                "error": (
                    "Importation annulée: le plan de cette allée a changé depuis "
                    "l'aperçu. Rechargez le planogramme avant de continuer."
                ),
            }), 409
    config = normalize_layout_config(row["config_json"], row["max_section"], row["max_shelf"], row["max_position"])
    is_fixture = side not in ("Gauche", "Droite")
    if is_fixture:
        fixture = fixture_for_side(config, side)
        if fixture is None:
            return jsonify({"success": False, "error": f"Le côté « {side} » n'existe pas dans le plan de cette allée."}), 400
        if not fixture.get("shelves"):
            return jsonify({"success": False,
                            "error": "Cette façade n'a aucune tablette. Ajoutez d'abord des tablettes dans l'onglet Plan (bouton Tablette)."}), 400
        if start_tablette > len(fixture.get("shelves", [])):
            return jsonify({"success": False, "error": "La tablette de départ n'existe pas sur cette façade."}), 400
    else:
        sections = ((config.get("sides", {}) or {}).get(side, {}) or {}).get("sections", [])
        if not sections:
            return jsonify({"success": False, "error": "Ce côté n'a aucune section dans le plan."}), 400
        if start_section > len(sections):
            return jsonify({"success": False, "error": "La section de départ n'existe pas dans le plan actuel."}), 400
        if start_tablette > len(sections[start_section - 1].get("shelves", [])):
            return jsonify({"success": False, "error": "La tablette de départ n'existe pas dans cette section."}), 400

    # Build the filtered plano lines (keep each row's full payload).
    now = utc_now_iso()
    errors = 0
    selected_products = 0
    filtered_non_stock = 0
    lines = []
    for p in products:
        try:
            tab = int(p.get("tablette", 0))
            pos = int(p.get("position", 0))
        except (ValueError, TypeError):
            errors += 1
            continue
        if not (tablette_start <= tab <= tablette_end):
            continue
        if pos < 1 or not str(p.get("name", "")).strip():
            errors += 1
            continue
        selected_products += 1
        if skip_ns and not p.get("en_stock", True):
            filtered_non_stock += 1
            continue
        lines.append({"tablette": tab, "position": pos, "p": p})

    if errors:
        return jsonify({
            "success": False,
            "error": (
                f"Importation annulée: {errors} ligne(s) sélectionnée(s) sont invalides. "
                "Aucun produit n'a été modifié."
            ),
            "errors": errors,
        }), 400

    placements, overflow = plan_planogram_flow(config, side, start_section, start_tablette, lines, shrink=replace)
    overflow_products = max(0, len(lines) - len(placements))
    destination_slots = [
        (str(section), str(shelf), str(position))
        for section, shelf, position, _line in placements
    ]
    if len(destination_slots) != len(set(destination_slots)):
        return jsonify({
            "success": False,
            "error": (
                "Importation annulée: plusieurs produits visent la même position. "
                "Corrigez les positions dans l'aperçu; aucun produit n'a été modifié."
            ),
        }), 409

    imported = 0
    skipped = filtered_non_stock
    image_barcodes = []   # barcodes still missing an image → fetched in background

    # Prefetch once instead of querying per product (an import is 100+ rows):
    #  - existing slot → product metadata (for safe same-UPC preservation)
    #  - any image already stored for a barcode, to reuse it without re-querying
    existing_rows = [
        dict(r) for r in db.execute(
            "SELECT * FROM products WHERE aisle=? AND side=?", (aisle, side)
        ).fetchall()
    ]
    existing_slots = {}
    existing_by_barcode = {}
    for d in existing_rows:
        existing_slots[(str(d["section"]), str(d["shelf"]), str(d["position"]))] = d
        metadata_quality = sum(bool(str(d.get(field, "") or "").strip()) for field in (
            "brand", "description", "image_url", "source_url", "usage_notes",
            "alternative_suggestions", "product_code",
        ))
        for candidate in build_barcode_candidates(d.get("barcode", "")):
            key = normalized_digits(candidate)
            current = existing_by_barcode.get(key) if key else None
            if key and (
                current is None or metadata_quality > current[0]
            ):
                existing_by_barcode[key] = (metadata_quality, d)

    # Replacement is tablet-level, not position-level. Otherwise an old product
    # survives whenever the new plano leaves a gap inside the same tablet. Keep a
    # full snapshot above for metadata reuse, archive every old row in the
    # destination tablets, then insert exactly what the new plano contains.
    touched_shelves = {
        (str(sec_no), str(shelf_no))
        for sec_no, shelf_no, _pos_no, _ln in placements
    }
    touched_fixture_shelves = {shelf for _section, shelf in touched_shelves}
    incoming_barcodes = [ln["p"].get("barcode", "") for ln in lines]
    reference_index = build_reference_metadata_index(db, incoming_barcodes)
    image_by_barcode = {}
    for r in _rows_for_barcodes(
        db, "products", "barcode, image_url", incoming_barcodes
    ):
        d = dict(r)
        if not str(d.get("image_url", "") or "").strip():
            continue
        for candidate in build_barcode_candidates(d.get("barcode", "")):
            key = normalized_digits(candidate)
            if key:
                image_by_barcode.setdefault(key, d["image_url"])

    replaced_removed = 0
    if replace and touched_shelves:
        replaced_rows = []
        for existing in existing_rows:
            section_s = str(existing.get("section", ""))
            shelf_s = str(existing.get("shelf", ""))
            is_touched = (
                shelf_s in touched_fixture_shelves
                if is_fixture
                else (section_s, shelf_s) in touched_shelves
            )
            if is_touched:
                replaced_rows.append(existing)
        replaced_removed = archive_and_delete_products(
            db, replaced_rows, username, now
        )

    for (sec_no, shelf_no, pos_no, ln) in placements:
        p = ln["p"]
        section_s, shelf_s, position_s = str(sec_no), str(shelf_no), str(pos_no)
        name     = str(p.get("name", "")).strip()
        barcode  = str(p.get("barcode", "")).strip()
        code     = str(p.get("code_familiprix", "")).strip()
        is_plano = 1 if p.get("is_plano", True) else 0
        flipped  = 1 if p.get("flipped_label", False) else 0
        try:
            facings = max(1, int(p.get("facings", 1) or 1))
        except (ValueError, TypeError):
            facings = 1
        # The pharmacy code lives in its own column (product_code), NOT in
        # search_terms, so a name/UPC search can never match it by accident.
        notes    = "[PLANO]" if is_plano else "[HORS-PLANO]"
        in_stock = 0 if not p.get("en_stock", True) else 1
        try:
            existing = existing_slots.get((section_s, shelf_s, position_s))
            # In replace mode every destination tablet was cleared above. The
            # old row is retained only as a metadata source, never as an UPDATE
            # target after it has been archived.
            row_id = existing.get("id") if existing and not replace else None
            if existing is not None and not replace:
                skipped += 1
                continue
            reference = reference_metadata_for_barcode(reference_index, barcode)
            metadata_source = existing
            incoming_keys = {
                normalized_digits(candidate)
                for candidate in build_barcode_candidates(barcode)
                if normalized_digits(candidate)
            }
            existing_keys = {
                normalized_digits(candidate)
                for candidate in build_barcode_candidates((existing or {}).get("barcode", ""))
                if normalized_digits(candidate)
            }
            if not incoming_keys.intersection(existing_keys):
                for key in incoming_keys:
                    prior = existing_by_barcode.get(key)
                    if prior:
                        metadata_source = prior[1]
                        break
            metadata = planogram_metadata(
                metadata_source, reference, barcode, product_code=code
            )
            brand = metadata["brand"]
            description = metadata["description"]
            image_url = metadata["image_url"]
            product_code = metadata["product_code"]
            source_url = metadata["source_url"]
            usage_notes = metadata["usage_notes"]
            alternatives = metadata["alternative_suggestions"]

            # Plano rows carry no image — reuse a verified image for the same UPC.
            for cand in build_barcode_candidates(barcode):
                key = normalized_digits(cand)
                if not image_url and key in image_by_barcode:
                    image_url = image_by_barcode[key]
                    break
            if not image_url and barcode:
                image_barcodes.append(barcode)   # fetch online in background
            if row_id is not None:
                db.execute(
                    """UPDATE products SET name=?, brand=?, description=?, image_url=?, source_url=?,
                       usage_notes=?, alternative_suggestions=?, barcode=?, product_code=?, facings=?,
                       search_terms=?, is_plano=?, in_stock=?, flipped_label=?, modified_by=?, modified_at=?
                       WHERE id=?""",
                    (name, brand, description, image_url, source_url, usage_notes, alternatives,
                     barcode, product_code, facings, notes, is_plano, in_stock, flipped,
                     username, now, row_id)
                )
            else:
                db.execute(
                    """INSERT INTO products
                       (name, brand, description, image_url, source_url, usage_notes,
                        alternative_suggestions, barcode, product_code, facings, aisle, side,
                        section, shelf, position, search_terms, is_plano, in_stock, flipped_label,
                        created_by, created_at, modified_by, modified_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (name, brand, description, image_url, source_url, usage_notes, alternatives,
                     barcode, product_code, facings, aisle, side, section_s, shelf_s, position_s,
                     notes, is_plano, in_stock, flipped, username, now, username, now)
                )
            imported += 1
        except Exception as exc:
            db.rollback()
            print(f"[Planogramme] Import annulé avant validation finale: {exc}")
            status = 409 if isinstance(exc, DatabaseIntegrityError) else 500
            return jsonify({
                "success": False,
                "error": (
                    "Importation annulée: un produit n'a pas pu être enregistré. "
                    "Les anciennes tablettes ont été conservées sans aucun changement."
                ),
            }), status

    skipped += overflow_products   # product rows on plano shelves past the physical plan

    # Kept for older clients that display the former "pruned" response field.
    pruned = replaced_removed

    # Verify the exact destination slice before committing. In replace mode the
    # touched tablets must contain exactly the incoming placements, never a
    # partial mix caused by an interrupted or malformed import.
    final_side_rows = [
        dict(product) for product in db.execute(
            "SELECT * FROM products WHERE aisle=? AND side=?", (aisle, side)
        ).fetchall()
    ]
    final_by_slot = {
        (str(product.get("section", "")), str(product.get("shelf", "")),
         str(product.get("position", ""))): product
        for product in final_side_rows
    }
    missing_slots = [slot for slot in destination_slots if slot not in final_by_slot]
    if replace:
        final_touched_count = sum(
            1 for product in final_side_rows
            if (
                str(product.get("shelf", "")) in touched_fixture_shelves
                if is_fixture else
                (str(product.get("section", "")), str(product.get("shelf", ""))) in touched_shelves
            )
        )
    else:
        final_touched_count = len(placements)
    if missing_slots or (replace and final_touched_count != len(placements)):
        db.rollback()
        return jsonify({
            "success": False,
            "error": (
                "Importation annulée pendant la vérification finale. "
                "Les anciennes tablettes ont été conservées sans aucun changement."
            ),
        }), 409

    # Persist the plan with positions adjusted to the plano (tablette count is
    # unchanged — only the number of positions on a tablette changes).
    ms, msh, mp = layout_metrics(config)
    try:
        db.execute(
            "UPDATE aisle_layouts SET config_json=?, max_section=?, max_shelf=?, max_position=?, modified_by=?, modified_at=? WHERE aisle=?",
            (json.dumps(config), ms, msh, mp, username, now, aisle),
        )
    except Exception as exc:
        db.rollback()
        print(f"[Planogramme] Import annulé pendant la sauvegarde du plan: {exc}")
        return jsonify({
            "success": False,
            "error": "Importation annulée: le plan n'a pas pu être sauvegardé. Aucun produit n'a été modifié.",
        }), 500

    # Record this import in the planogram history.
    try:
        plano = data.get("plano") or {}
        store = str(data.get("store", "")).strip()
        db.execute(
            """INSERT INTO planogram_imports
               (created_at, store, employee, plano_name, plano_number, plano_version,
                aisle, side, section, tablette_start, tablette_end, imported, skipped)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (now, store, username,
             str(plano.get("name", "")), str(plano.get("number", "")), str(plano.get("version", "")),
             aisle, side, str(start_section), str(tablette_start), str(tablette_end), imported, skipped),
        )
    except Exception as exc:
        db.rollback()
        print(f"[Planogramme] Import annulé pendant l'historique: {exc}")
        return jsonify({
            "success": False,
            "error": "Importation annulée: l'historique n'a pas pu être enregistré. Aucun produit n'a été modifié.",
        }), 500
    db.commit()

    # Return exactly the committed slice the browser must replace. This avoids
    # two full-list downloads before the planogram can visibly update.
    affected_products = [row_to_product(product) for product in final_side_rows]
    affected_products.sort(key=location_sort_key)
    aisle_product_count_row = db.execute(
        "SELECT COUNT(*) AS n FROM products WHERE aisle=?", (aisle,)
    ).fetchone()
    aisle_product_count = int(first_column(aisle_product_count_row) or 0)
    layout_payload = {
        "aisle": aisle,
        "max_section": ms,
        "max_shelf": msh,
        "max_position": mp,
        "config": config,
        "enabled": int(row.get("enabled", 1) if isinstance(row, dict) else row[5]),
        "modified_by": username,
        "modified_at": now,
        "product_count": aisle_product_count,
    }

    from routes.gist import _schedule_gist_backup
    _schedule_gist_backup(db)
    schedule_image_fill(image_barcodes)   # fetch missing plano pictures automatically
    return jsonify({"success": True, "imported": imported, "skipped": skipped,
                    "errors": errors, "overflow": overflow,
                    "overflow_shelves": overflow, "overflow_products": overflow_products,
                    "selected_products": selected_products,
                    "filtered_non_stock": filtered_non_stock,
                    "pruned": pruned, "replaced_removed": replaced_removed,
                    "layout": layout_payload,
                    "products": affected_products})


@products_bp.route("/api/planograms/history", methods=["GET"])
def planogram_history():
    db = get_db()
    rows = db.execute("SELECT * FROM planogram_imports ORDER BY id DESC").fetchall()
    return jsonify([dict(r) for r in rows])


_REFERENCE_SYNC_LOCK = threading.Lock()
_REFERENCE_SYNC_STARTED = False


def schedule_reference_metadata_sync():
    """Backfill existing plans after deploy without delaying application startup."""
    global _REFERENCE_SYNC_STARTED
    with _REFERENCE_SYNC_LOCK:
        if _REFERENCE_SYNC_STARTED:
            return
        _REFERENCE_SYNC_STARTED = True

    def worker():
        from database import connect_db

        # Let the web worker answer its first plan/products request before this
        # catalogue-wide maintenance scan competes for Postgres and CPU.
        time.sleep(12)
        for attempt in range(3):
            db = None
            try:
                db = connect_db()
                linked = sync_reference_metadata_to_products(db)
                db.commit()
                if linked:
                    print(f"[Catalogue] {linked} produit(s) placé(s) relié(s) à leur description/image.")
                return
            except Exception as exc:
                if attempt == 2:
                    print(f"[Catalogue] synchronisation des métadonnées impossible: {exc}")
                else:
                    time.sleep(5)
            finally:
                if db is not None:
                    try:
                        db.close()
                    except Exception:
                        pass

    threading.Thread(target=worker, daemon=True).start()


def schedule_backfill_missing():
    """At startup, automatically fetch any still-missing product images in the
    background — no button, no user action. Throttled to once per 12h via a
    temp-file marker: the worker recycles every ~500 requests, and re-hitting
    the image sources for the same unfindable products at every recycle was
    pure waste."""
    try:
        marker = os.path.join(tempfile.gettempdir(), "familiprix-backfill.last")
        if os.path.exists(marker) and time.time() - os.path.getmtime(marker) < 12 * 3600:
            return
        with open(marker, "w", encoding="utf-8") as fh:
            fh.write(str(time.time()))
    except OSError:
        pass
    def worker():
        time.sleep(15)
        try:
            from database import connect_db
            db = connect_db()
            try:
                rows = db.execute(
                    "SELECT barcode, MAX(COALESCE(created_at,'')) AS newest FROM products "
                    "WHERE TRIM(COALESCE(barcode,'')) <> '' AND TRIM(COALESCE(image_url,'')) = '' "
                    "GROUP BY barcode ORDER BY newest DESC"
                ).fetchall()
                codes = [(r["barcode"] if isinstance(r, dict) else r[0]) for r in rows]
            finally:
                db.close()
            schedule_image_fill(codes, priority=False)
        except Exception:
            pass

    threading.Thread(target=worker, daemon=True).start()
