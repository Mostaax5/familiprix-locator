import re
import os
import json
import time
import hashlib
import tempfile
import unicodedata
from flask import Blueprint, request, jsonify
from database import get_db, DatabaseIntegrityError
from auth import require_editor, utc_now_iso, side_display_label
from routes.layout import validate_layout_slot, aisle_sort_key

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
# The catalogue has ~6000+ rows. Re-normalizing them all (regex) on every keystroke
# froze the whole app on Render's single worker. So we normalize ONCE into memory and
# reuse it; the cache refreshes on catalogue import and every 90s (covers enrichment).
_REF_GEN = 0
_REF_CACHE = {"gen": -1, "at": 0.0, "rows": []}
_REF_CACHE_TTL = 90.0


def bump_reference_cache():
    global _REF_GEN
    _REF_GEN += 1


def _reference_corpus(db):
    now = time.time()
    if (_REF_CACHE["gen"] == _REF_GEN and _REF_CACHE["rows"]
            and now - _REF_CACHE["at"] < _REF_CACHE_TTL):
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
    _REF_CACHE.update(gen=_REF_GEN, at=now, rows=rows)
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
    now = now or utc_now_iso()
    pdict = dict(product)
    last_loc = (f"Allée {pdict.get('aisle','')} {side_display_label(pdict.get('side',''))} "
                f"S{pdict.get('section','')} T{pdict.get('shelf','')} P{pdict.get('position','')}").strip()
    db.execute(
        """INSERT INTO removed_products (removed_at, removed_by, barcode, name, last_location, product_json)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (now, username, str(pdict.get("barcode", "")), str(pdict.get("name", "")),
         last_loc, json.dumps(pdict, ensure_ascii=False, default=str))
    )
    db.execute("DELETE FROM products WHERE id=?", (pdict.get("id"),))


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
    return ""


def schedule_image_fill(barcodes):
    """Fetch missing product images online in a background thread — fully
    automatic, no user action. Mirrors the gist-backup threading pattern."""
    import threading
    codes = [str(b).strip() for b in (barcodes or []) if str(b or "").strip()]
    codes = list(dict.fromkeys(codes))
    if not codes:
        return

    def worker():
        from database import connect_db
        from routes.ai import lookup_product_online
        db = connect_db()
        try:
            for bc in codes:
                try:
                    # already has an image for this barcode? reuse it; else look up online
                    img = find_existing_image_for_barcode(db, bc)
                    if not img:
                        product = lookup_product_online(bc)
                        img = str((product or {}).get("image_url", "")).strip()
                    if img:
                        db.execute(
                            "UPDATE products SET image_url=? WHERE barcode=? AND TRIM(COALESCE(image_url,'')) = ''",
                            (img, bc)
                        )
                        db.commit()
                except Exception:
                    pass
        finally:
            try: db.close()
            except Exception: pass

    threading.Thread(target=worker, daemon=True).start()


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
    """ONE ranked list for the Client tab: placed products (with location) AND catalogue
    products (position à confirmer) scored together by the SAME rules, best match first.
    Works with zero AI — this is the reliable core of the client helper."""
    query = request.args.get("q", "").strip()
    if not query:
        return jsonify([])
    limit = min(max(clamp_non_negative_int(request.args.get("limit", "30"), 30), 1), 60)
    nq = normalize_search_text(query)
    dq = normalized_digits(query)
    qtokens = list(dict.fromkeys(tokenize_search_query(query)))
    intent_terms = intent_expansion_terms(query)
    abbrevs = abbreviation_terms(query)
    if not nq and not dq and not intent_terms:
        return jsonify([])
    db = get_db()
    scored = []
    seen_bc = set()
    # Minimum meaningful score for the CLIENT tab: every real signal clears it
    # (whole-word name token 470+, intent 200-300, brand 200, all-tokens-covered
    # 120+, barcode 500+). What it drops is partial-coverage-only noise (25/token)
    # — the "random products" that padded the list when little else matched.
    MIN_SCORE = 100
    # Placed products first (tiebreak 0) — they carry a real shelf location. Scored
    # from the pre-normalized in-memory corpus with the SAME rules as the catalogue,
    # so the two lists compete fairly and a request costs milliseconds (the old
    # per-request re-normalization took ~17s on Render and looked like "no results").
    for item, prow in _products_corpus(db):
        s = _fast_reference_score(prow, nq, dq, qtokens, intent_terms, abbrevs)
        if s >= MIN_SCORE:
            scored.append((s, 0, item))
            if prow["_bc"]:
                seen_bc.add(prow["_bc"])
    # Catalogue products (tiebreak 1) — from the cached, pre-normalized corpus (fast).
    for row in _reference_corpus(db):
        if row["_bc"] and row["_bc"] in seen_bc:
            continue
        s = _fast_reference_score(row, nq, dq, qtokens, intent_terms, abbrevs)
        if s >= MIN_SCORE:
            scored.append((s, 1, {"barcode": row["barcode"], "name": row["name"],
                                   "brand": row["brand"], "description": row["description"],
                                   "product_code": row["product_code"],
                                   "catalog_only": True, "in_stock": 1}))
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
    data = request.get_json()
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
    data = request.get_json()
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
    else:
        sections = ((config.get("sides", {}) or {}).get(side, {}) or {}).get("sections", [])
        # Direction: a planogram reads left→right, which for Côté A (Gauche) runs from the
        # Façade B end toward Façade A — the OPPOSITE of the section numbering (section 1 is
        # the Façade A end). So Côté A fills sections DESCENDING from the start section down
        # to section 1; Côté B (Droite) fills ASCENDING (Façade A → Façade B). Tablettes are
        # vertical shelves and keep their top-to-bottom order — only the section order flips.
        start_idx = min(max(0, start_section - 1), max(0, len(sections) - 1))
        section_indices = range(start_idx, -1, -1) if side == "Gauche" else range(start_idx, len(sections))
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
    # STORE CONVENTION: positions always count from the Façade A end toward the
    # Façade B end, on BOTH côtés. A planogram reads left→right as you FACE the
    # shelf, which on Côté A (Gauche) runs Façade B→A — so its positions must be
    # MIRRORED within each tablette (plano position 1 becomes the last position).
    # Côté B and the fixture sides keep the plano's numbering unchanged.
    mirror_positions = (side == "Gauche")
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
            pos = (max_pos + 1 - ln["position"]) if mirror_positions else ln["position"]
            placements.append((sec_no, ti + 1, pos, ln))
    return placements, overflow


@products_bp.route("/api/products/bulk-import", methods=["POST"])
def bulk_import_products():
    username, error = require_editor()
    if error:
        return error

    data           = request.get_json() or {}
    aisle          = str(data.get("aisle", "")).strip()
    side           = str(data.get("side", "Droite")).strip()
    start_section  = max(1, int(data.get("start_section", data.get("section", 1)) or 1))
    start_tablette = max(1, int(data.get("start_tablette", 1) or 1))
    tablette_start = int(data.get("tablette_start", 1))
    tablette_end   = int(data.get("tablette_end", 99))
    replace        = bool(data.get("replace_existing", False))
    skip_ns        = bool(data.get("skip_non_stock", False))
    products       = data.get("products", [])

    if not aisle:
        return jsonify({"success": False, "error": "Allée requise."}), 400

    from routes.layout import get_layout_row, normalize_layout_config, layout_metrics
    db = get_db()
    row = get_layout_row(db, aisle)
    if not row:
        return jsonify({"success": False, "error": f"L'allée {aisle} n'existe pas dans le plan. Créez d'abord l'allée."}), 400
    config = normalize_layout_config(row["config_json"], row["max_section"], row["max_shelf"], row["max_position"])
    is_fixture = side not in ("Gauche", "Droite")
    if is_fixture:
        fixture = fixture_for_side(config, side)
        if fixture is None:
            return jsonify({"success": False, "error": f"Le côté « {side} » n'existe pas dans le plan de cette allée."}), 400
        if not fixture.get("shelves"):
            return jsonify({"success": False,
                            "error": "Cette façade n'a aucune tablette. Ajoutez d'abord des tablettes dans l'onglet Plan (bouton Tablette)."}), 400
    else:
        sections = ((config.get("sides", {}) or {}).get(side, {}) or {}).get("sections", [])
        if not sections:
            return jsonify({"success": False, "error": "Ce côté n'a aucune section dans le plan."}), 400

    # Build the filtered plano lines (keep each row's full payload).
    now = utc_now_iso()
    errors = 0
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
        if skip_ns and not p.get("en_stock", True):
            continue
        if not str(p.get("name", "")).strip():
            errors += 1
            continue
        lines.append({"tablette": tab, "position": pos, "p": p})

    placements, overflow = plan_planogram_flow(config, side, start_section, start_tablette, lines, shrink=replace)

    imported = skipped = 0
    image_barcodes = []   # barcodes still missing an image → fetched in background

    # Prefetch once instead of querying per product (an import is 100+ rows):
    #  - existing slot → product id (for this aisle+side)
    #  - any image already stored for a barcode, to reuse it without re-querying
    existing_slots = {}
    for r in db.execute(
        "SELECT id, section, shelf, position FROM products WHERE aisle=? AND side=?", (aisle, side)
    ).fetchall():
        d = dict(r)
        existing_slots[(str(d["section"]), str(d["shelf"]), str(d["position"]))] = d["id"]
    image_by_barcode = {}
    for r in db.execute(
        "SELECT barcode, image_url FROM products "
        "WHERE TRIM(COALESCE(image_url,'')) <> '' AND TRIM(COALESCE(barcode,'')) <> ''"
    ).fetchall():
        d = dict(r)
        image_by_barcode.setdefault(str(d["barcode"]).strip(), d["image_url"])

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
            row_id = existing_slots.get((section_s, shelf_s, position_s))
            if row_id is not None and not replace:
                skipped += 1
                continue
            # Plano rows carry no image — reuse one already stored for this barcode.
            image_url = ""
            for cand in build_barcode_candidates(barcode):
                if cand in image_by_barcode:
                    image_url = image_by_barcode[cand]
                    break
            if not image_url and barcode:
                image_barcodes.append(barcode)   # fetch online in background
            if row_id is not None:
                if image_url:
                    db.execute(
                        "UPDATE products SET name=?, barcode=?, product_code=?, facings=?, search_terms=?, is_plano=?, in_stock=?, flipped_label=?, image_url=?, modified_by=?, modified_at=? WHERE id=?",
                        (name, barcode, code, facings, notes, is_plano, in_stock, flipped, image_url, username, now, row_id)
                    )
                else:
                    db.execute(
                        "UPDATE products SET name=?, barcode=?, product_code=?, facings=?, search_terms=?, is_plano=?, in_stock=?, flipped_label=?, modified_by=?, modified_at=? WHERE id=?",
                        (name, barcode, code, facings, notes, is_plano, in_stock, flipped, username, now, row_id)
                    )
            else:
                db.execute(
                    """INSERT INTO products
                       (name, barcode, product_code, facings, aisle, side, section, shelf, position,
                        search_terms, is_plano, in_stock, flipped_label, image_url, created_by, created_at, modified_by, modified_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (name, barcode, code, facings, aisle, side, section_s, shelf_s, position_s,
                     notes, is_plano, in_stock, flipped, image_url, username, now, username, now)
                )
            imported += 1
        except Exception:
            errors += 1

    skipped += overflow   # plano shelves past the end of the plan (tablettes never added)

    # When replacing, positions follow the plano exactly — so a tablette that
    # shrank now has products sitting past its new end. Archive them (kept in the
    # database, recoverable) so the plan stays consistent with the plano.
    pruned = 0
    if replace:
        touched = {}
        for (sec_no, shelf_no, pos_no, _ln) in placements:
            key = (str(sec_no), str(shelf_no))
            touched[key] = max(touched.get(key, 0), pos_no)
        for (sec_s, shelf_s), new_max in touched.items():
            # Fixture sides carry no meaningful section on their products —
            # prune their shelves across all sections (same rule as remove-shelf).
            if is_fixture:
                prune_q = "SELECT * FROM products WHERE aisle=? AND side=? AND shelf=?"
                prune_params = (aisle, side, shelf_s)
            else:
                prune_q = "SELECT * FROM products WHERE aisle=? AND side=? AND section=? AND shelf=?"
                prune_params = (aisle, side, sec_s, shelf_s)
            for r in db.execute(prune_q, prune_params).fetchall():
                try:
                    if int(str(dict(r).get("position", "0"))) > new_max:
                        archive_and_delete_product(db, r, username, now)
                        pruned += 1
                except (TypeError, ValueError):
                    continue

    # Persist the plan with positions adjusted to the plano (tablette count is
    # unchanged — only the number of positions on a tablette changes).
    try:
        ms, msh, mp = layout_metrics(config)
        db.execute(
            "UPDATE aisle_layouts SET config_json=?, max_section=?, max_shelf=?, max_position=?, modified_by=?, modified_at=? WHERE aisle=?",
            (json.dumps(config), ms, msh, mp, username, now, aisle),
        )
    except Exception:
        pass

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
    except Exception:
        pass
    db.commit()

    from routes.gist import _schedule_gist_backup
    _schedule_gist_backup(db)
    schedule_image_fill(image_barcodes)   # fetch missing plano pictures automatically
    return jsonify({"success": True, "imported": imported, "skipped": skipped,
                    "errors": errors, "overflow": overflow, "pruned": pruned})


@products_bp.route("/api/planograms/history", methods=["GET"])
def planogram_history():
    db = get_db()
    rows = db.execute("SELECT * FROM planogram_imports ORDER BY id DESC").fetchall()
    return jsonify([dict(r) for r in rows])


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
    try:
        from database import connect_db
        db = connect_db()
        try:
            rows = db.execute(
                "SELECT DISTINCT barcode FROM products "
                "WHERE TRIM(COALESCE(barcode,'')) <> '' AND TRIM(COALESCE(image_url,'')) = ''"
            ).fetchall()
            codes = [(r["barcode"] if isinstance(r, dict) else r[0]) for r in rows]
        finally:
            db.close()
        schedule_image_fill(codes)
    except Exception:
        pass
