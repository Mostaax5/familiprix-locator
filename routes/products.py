import re
import unicodedata
from flask import Blueprint, request, jsonify
from database import get_db, DatabaseIntegrityError
from auth import require_editor, utc_now_iso
from routes.layout import validate_layout_slot, aisle_sort_key

products_bp = Blueprint("products", __name__)

SEARCH_STOPWORDS = {
    "a", "an", "and", "au", "aux", "avec", "ce", "ces", "cette", "client", "comme",
    "dans", "de", "des", "du", "en", "et", "for", "how", "i", "il", "ils", "je",
    "la", "le", "les", "mais", "mon", "my", "of", "on", "or", "ou", "par", "pas",
    "pour", "que", "qui", "sans", "si", "son", "sur", "the", "to", "un", "une",
    "with", "without", "y",
}


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


def find_product_at_position(db, aisle, side, section, shelf, position, exclude_id=None):
    query = "SELECT * FROM products WHERE aisle=? AND side=? AND section=? AND shelf=? AND position=?"
    params = [aisle, side, section, shelf, position]
    if exclude_id is not None:
        query += " AND id != ?"
        params.append(exclude_id)
    return db.execute(query, tuple(params)).fetchone()


def integrity_conflict_message(exc):
    text = str(exc).lower()
    if "barcode" in text:
        return "Ce code-barres existe deja ailleurs dans la base."
    return "Cette position est deja occupee."


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
    if not variants:
        return []
    ranked = []
    for product in products:
        best_score = 0
        for variant in variants:
            best_score = max(best_score, product_search_score(product, variant))
        if best_score > 0:
            ranked.append((best_score, product))
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
    limit = min(max(clamp_non_negative_int(request.args.get("limit", "60"), 60), 1), 120)
    db = get_db()
    products = [row_to_product(p) for p in db.execute("SELECT * FROM products").fetchall()]
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
    aisle    = data.get("aisle", "").strip()
    side     = data.get("side", "").strip()
    section  = data.get("section", "").strip() or "1"
    shelf    = data.get("shelf", "").strip()
    position = data.get("position", "").strip()

    if not all([name, aisle, side, section, shelf, position]):
        return jsonify({"error": "Champs obligatoires manquants"}), 400

    db = get_db()
    is_valid_slot, slot_error = validate_layout_slot(db, aisle, side, section, shelf, position)
    if not is_valid_slot:
        return jsonify({"error": slot_error}), 400
    occupied = find_product_at_position(db, aisle, side, section, shelf, position)
    if occupied:
        return jsonify({
            "error": f'Position deja occupee par "{occupied["name"]}" (code {occupied["barcode"] or "sans code"}).'
        }), 409

    try:
        cursor = db.execute(
            """
            INSERT INTO products (name, brand, description, image_url, source_url, search_terms, usage_notes, alternative_suggestions, barcode, aisle, side, section, shelf, position, created_by, created_at, modified_by, modified_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (name, brand, description, image_url, source_url, search_terms, usage_notes,
             alternative_suggestions, barcode, aisle, side, section, shelf, position,
             username, utc_now_iso(), username, utc_now_iso())
        )
    except DatabaseIntegrityError as exc:
        return jsonify({"error": integrity_conflict_message(exc)}), 409
    db.commit()
    product_id = cursor.lastrowid
    product = db.execute("SELECT * FROM products WHERE id=?", (product_id,)).fetchone()
    from routes.gist import _schedule_gist_backup
    _schedule_gist_backup(db)
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
            "error": f'Position deja occupee par "{occupied["name"]}" (code {occupied["barcode"] or "sans code"}).'
        }), 409

    try:
        result = db.execute(
            "UPDATE products SET name=?, brand=?, description=?, image_url=?, source_url=?, search_terms=?, usage_notes=?, alternative_suggestions=?, barcode=?, aisle=?, side=?, section=?, shelf=?, position=?, modified_by=?, modified_at=? WHERE id=?",
            (
                data["name"],
                data.get("brand", existing["brand"]),
                data.get("description", existing["description"]),
                data.get("image_url", existing["image_url"]),
                data.get("source_url", existing["source_url"]),
                data.get("search_terms", existing["search_terms"]),
                data.get("usage_notes", existing["usage_notes"]),
                data.get("alternative_suggestions", existing["alternative_suggestions"]),
                data.get("barcode", existing["barcode"]),
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
    product = db.execute("SELECT name FROM products WHERE id=?", (product_id,)).fetchone()
    if not product:
        return jsonify({"error": "Produit non trouve."}), 404
    db.execute("DELETE FROM products WHERE id=?", (product_id,))
    db.commit()
    from routes.gist import _schedule_gist_backup
    _schedule_gist_backup(db)
    return jsonify({"success": True, "message": f'Produit supprimé par {username}: {product["name"]}'})


@products_bp.route("/api/products/bulk-import", methods=["POST"])
def bulk_import_products():
    username, error = require_editor()
    if error:
        return error

    data           = request.get_json() or {}
    aisle          = str(data.get("aisle", "")).strip()
    side           = str(data.get("side", "Droite")).strip()
    section        = str(data.get("section", "1")).strip()
    shelf_offset   = int(data.get("shelf_offset", 0))
    tablette_start = int(data.get("tablette_start", 1))
    tablette_end   = int(data.get("tablette_end", 99))
    replace        = bool(data.get("replace_existing", False))
    skip_ns        = bool(data.get("skip_non_stock", False))
    products       = data.get("products", [])

    if not aisle:
        return jsonify({"success": False, "error": "Allee requise."}), 400

    db = get_db()
    now = utc_now_iso()
    imported = skipped = errors = 0

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

        shelf    = str(tab + shelf_offset)
        position = str(pos)
        name     = str(p.get("name", "")).strip()
        barcode  = str(p.get("barcode", "")).strip()
        code     = str(p.get("code_familiprix", "")).strip()
        notes    = f"[PLANO] {code}" if code else "[PLANO]"

        if not name:
            errors += 1
            continue

        try:
            existing = db.execute(
                "SELECT id FROM products WHERE aisle=? AND side=? AND section=? AND shelf=? AND position=?",
                (aisle, side, section, shelf, position)
            ).fetchone()

            if existing and not replace:
                skipped += 1
                continue

            if existing:
                row_id = existing["id"] if isinstance(existing, dict) else existing[0]
                db.execute(
                    "UPDATE products SET name=?, barcode=?, search_terms=?, modified_by=?, modified_at=? WHERE id=?",
                    (name, barcode, notes, username, now, row_id)
                )
            else:
                db.execute(
                    """INSERT INTO products
                       (name, barcode, aisle, side, section, shelf, position,
                        search_terms, created_by, created_at, modified_by, modified_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (name, barcode, aisle, side, section, shelf, position,
                     notes, username, now, username, now)
                )
            imported += 1
        except Exception:
            errors += 1

    db.commit()
    return jsonify({"success": True, "imported": imported, "skipped": skipped, "errors": errors})
