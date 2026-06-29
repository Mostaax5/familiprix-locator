import re
import json
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
     "expand": ["sirop", "toux", "dextromethorphane", "guaifenesine", "benylin", "buckley",
                "pastille", "gorge", "strepsils", "halls", "fisherman"]},
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
                  "erytheme fessier", "biberon", "nourrisson"],
     "expand": ["bebe", "couche", "pampers", "huggies", "tempra", "tylenol bebe", "penaten",
                "creme fesses", "lingette", "ovol"]},
    {"label": "Premiers soins",
     "triggers": ["pansement", "coupure", "plaie", "desinfectant", "bandage", "ampoule",
                  "echarde", "eraflure", "saignement"],
     "expand": ["pansement", "band aid", "polysporin", "peroxyde", "alcool", "gaze",
                "bandage", "antiseptique", "diachylon"]},
    {"label": "Sommeil / stress",
     "triggers": ["sommeil", "dormir", "insomnie", "stress", "anxiete", "relaxation", "nervosite"],
     "expand": ["sommeil", "melatonine", "nytol", "sleep", "valeriane", "unisom", "tylenol nuit"]},
]


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


def query_search_variants(query):
    variants = []
    seen = set()

    def add(value):
        cleaned = str(value or "").strip()
        if cleaned and cleaned not in seen:
            seen.add(cleaned)
            variants.append(cleaned)

    normalized = normalize_search_text(query)
    digits = normalized_digits(query)
    tokens = tokenize_search_query(query)
    add(normalized)
    if tokens:
        add(" ".join(tokens))
        for token in tokens:
            add(token)
    if digits and len(digits) >= 4:
        add(digits)
    return variants


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


def product_search_text(product):
    return normalize_search_text(" ".join([
        str(product.get("name", "")),
        str(product.get("brand", "")),
        str(product.get("description", "")),
        str(product.get("search_terms", "")),
        str(product.get("usage_notes", "")),
        str(product.get("alternative_suggestions", "")),
    ]))


def product_search_score(product, query):
    lowered_query = normalize_search_text(query)
    digits_query = normalized_digits(query)
    if not lowered_query and not digits_query:
        return 0

    barcode = normalized_digits(product.get("barcode", ""))
    name = normalize_search_text(product.get("name", ""))
    brand = normalize_search_text(product.get("brand", ""))
    description = normalize_search_text(product.get("description", ""))
    search_terms = normalize_search_text(product.get("search_terms", ""))
    usage_notes = normalize_search_text(product.get("usage_notes", ""))
    alternatives = normalize_search_text(product.get("alternative_suggestions", ""))
    haystack = product_search_text(product)
    score = 0

    if digits_query and barcode:
        if barcode == digits_query:
            score += 1200
        elif len(digits_query) >= 4 and barcode.endswith(digits_query):
            score += 900
        elif digits_query in barcode:
            score += 500

    if lowered_query == name:
        score += 800
    elif name.startswith(lowered_query):
        score += 650
    elif lowered_query in name:
        score += 450

    if brand.startswith(lowered_query):
        score += 280
    elif lowered_query in brand:
        score += 180

    if lowered_query in description:
        score += 150
    if lowered_query in search_terms:
        score += 240
    if lowered_query in usage_notes:
        score += 170
    if lowered_query in alternatives:
        score += 120

    unique_tokens = list(dict.fromkeys(tokenize_search_query(query)))
    if unique_tokens:
        matched_tokens = sum(1 for token in unique_tokens if token in haystack)
        if matched_tokens == len(unique_tokens):
            score += 100 + (20 * matched_tokens)
        elif matched_tokens:
            score += 25 * matched_tokens

    return score


def rank_products_for_query(products, query, limit=60):
    variants = query_search_variants(query)
    intent_terms = intent_expansion_terms(query)
    if not variants and not intent_terms:
        return []
    ranked = []
    for product in products:
        best_score = 0
        for variant in variants:
            best_score = max(best_score, product_search_score(product, variant))
        if intent_terms:
            intent_hit = 0
            for term in intent_terms:
                intent_hit = max(intent_hit, product_search_score(product, term))
            # Discount the symptom→category match so a direct name/brand/UPC match
            # (450+) always wins, but the right category still surfaces.
            best_score = max(best_score, min(intent_hit, 300))
        if best_score > 0:
            ranked.append((best_score, product))
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


# ── Routes ─────────────────────────────────────────────────────────────────────

@products_bp.route("/api/products", methods=["GET"])
def get_products():
    db = get_db()
    products = [row_to_product(p) for p in db.execute("SELECT * FROM products").fetchall()]
    products.sort(key=location_sort_key)
    return jsonify(products)


@products_bp.route("/api/products/search", methods=["GET"])
def search_products():
    query = request.args.get("q", "").strip()
    if not query:
        return jsonify([])
    field = (request.args.get("field") or "").strip().lower()
    limit = min(max(clamp_non_negative_int(request.args.get("limit", "60"), 60), 1), 120)
    db = get_db()
    products = [row_to_product(p) for p in db.execute("SELECT * FROM products").fetchall()]
    if field == "code":
        items = rank_products_by_code(products, query, limit=limit)
    else:
        items = rank_products_for_query(products, query, limit=limit)
    return jsonify(items)


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
    sections = ((config.get("sides", {}) or {}).get(side, {}) or {}).get("sections", [])
    # Ordered store tablette slots from the start point onward (count per section fixed).
    slots = []
    for si in range(max(0, start_section - 1), len(sections)):
        shelf_count = len(sections[si].get("shelves", []))
        first_t = (start_tablette - 1) if si == (start_section - 1) else 0
        for ti in range(max(0, first_t), shelf_count):
            slots.append((si, ti))

    # Group plano lines into shelves by their plano tablette, in ascending order.
    by_tablette = {}
    for ln in lines:
        by_tablette.setdefault(ln["tablette"], []).append(ln)

    placements = []
    overflow = 0
    for idx, ptab in enumerate(sorted(by_tablette.keys())):
        shelf_lines = sorted(by_tablette[ptab], key=lambda l: l["position"])
        if idx >= len(slots):
            overflow += 1
            continue
        si, ti = slots[idx]
        max_pos = max((l["position"] for l in shelf_lines), default=0)
        # Positions follow the plano: always grow to fit; shrink to the plano's
        # count too when shrink is on (import archives anything past the new end).
        if shrink or max_pos > sections[si]["shelves"][ti]:
            sections[si]["shelves"][ti] = max_pos
        for ln in shelf_lines:
            placements.append((si + 1, ti + 1, ln["position"], ln))
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
            for r in db.execute(
                "SELECT * FROM products WHERE aisle=? AND side=? AND section=? AND shelf=?",
                (aisle, side, sec_s, shelf_s)
            ).fetchall():
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
    background — no button, no user action."""
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
