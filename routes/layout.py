import json
import re
from flask import Blueprint, request, jsonify
from database import get_db, DatabaseIntegrityError
from auth import require_editor, utc_now_iso

layout_bp = Blueprint("layout", __name__)


# ── Config helpers ─────────────────────────────────────────────────────────────

def clamp_non_negative_int(value, fallback=0):
    try:
        return max(0, int(str(value)))
    except (TypeError, ValueError):
        return fallback


def build_default_layout_config(max_section, max_shelf, max_position):
    section_count = clamp_non_negative_int(max_section)
    shelf_count = clamp_non_negative_int(max_shelf)
    position_count = clamp_non_negative_int(max_position)
    section_template = [
        {"shelves": [position_count for _ in range(shelf_count)], "labels": ["" for _ in range(shelf_count)]}
        for _ in range(section_count)
    ]
    return {
        "sides": {
            "Gauche": {"sections": json.loads(json.dumps(section_template))},
            "Droite": {"sections": json.loads(json.dumps(section_template))},
        },
        "presentoirs": [],
    }


def normalize_layout_config(config_value, max_section="1", max_shelf="5", max_position="8"):
    if isinstance(config_value, str):
        try:
            config = json.loads(config_value) if config_value.strip() else {}
        except json.JSONDecodeError:
            config = {}
    else:
        config = config_value or {}

    config = config if isinstance(config, dict) else {}
    sides = config.get("sides") if isinstance(config.get("sides"), dict) else {}
    normalized_sides = {}
    default = build_default_layout_config(max_section, max_shelf, max_position)

    for side in ["Gauche", "Droite"]:
        side_value = sides.get(side) if isinstance(sides.get(side), dict) else {}
        has_explicit_sections = isinstance(side_value.get("sections"), list)
        sections = side_value.get("sections") if has_explicit_sections else []
        normalized_sections = []
        for section in sections:
            shelves = section.get("shelves") if isinstance(section, dict) else None
            if not isinstance(shelves, list):
                continue
            cleaned_shelves = [clamp_non_negative_int(shelf) for shelf in shelves]
            raw_labels = section.get("labels", []) if isinstance(section, dict) else []
            if not isinstance(raw_labels, list):
                raw_labels = []
            cleaned_labels = [str(raw_labels[i]) if i < len(raw_labels) else "" for i in range(len(cleaned_shelves))]
            normalized_sections.append({"shelves": cleaned_shelves, "labels": cleaned_labels})
        if not has_explicit_sections:
            normalized_sections = default["sides"][side]["sections"]
        normalized_sides[side] = {"sections": normalized_sections}

    def norm_fixture(fx):
        if not isinstance(fx, dict):
            return {"shelves": [], "labels": []}
        raw_sh = fx.get("shelves", [])
        if not isinstance(raw_sh, list): raw_sh = []
        shelves = [clamp_non_negative_int(v) for v in raw_sh]
        raw_lab = fx.get("labels", [])
        if not isinstance(raw_lab, list): raw_lab = []
        labels = [str(raw_lab[i]) if i < len(raw_lab) else "" for i in range(len(shelves))]
        return {"shelves": shelves, "labels": labels}

    normalized_facade_a = norm_fixture(config.get("facade_a") if isinstance(config, dict) else None)
    normalized_facade_b = norm_fixture(config.get("facade_b") if isinstance(config, dict) else None)

    raw_pres = config.get("presentoirs", []) if isinstance(config, dict) else []
    if not isinstance(raw_pres, list): raw_pres = []
    normalized_pres = []
    for p in raw_pres:
        if not isinstance(p, dict):
            continue
        name = str(p.get("name", "Présentoir")).strip() or "Présentoir"
        raw_facades = p.get("facades") if isinstance(p.get("facades"), list) else None
        if raw_facades:
            facades = []
            for i, f in enumerate(raw_facades):
                if not isinstance(f, dict):
                    continue
                fname = str(f.get("name", f"Façade {i+1}")).strip() or f"Façade {i+1}"
                fx = norm_fixture(f)
                facades.append({"name": fname, "shelves": fx["shelves"], "labels": fx["labels"]})
        else:
            fx = norm_fixture(p)
            facades = [{"name": "Façade 1", "shelves": fx["shelves"], "labels": fx["labels"]}]
        normalized_pres.append({"name": name, "facades": facades})

    return {"sides": normalized_sides, "facade_a": normalized_facade_a, "facade_b": normalized_facade_b, "presentoirs": normalized_pres}


def layout_metrics(config):
    sides = config.get("sides", {})
    max_section = max(len((sides.get(side) or {}).get("sections", [])) for side in ["Gauche", "Droite"])
    max_shelf = 0
    max_position = 0
    for side in ["Gauche", "Droite"]:
        for section in (sides.get(side) or {}).get("sections", []):
            shelves = section.get("shelves", [])
            max_shelf = max(max_shelf, len(shelves))
            if shelves:
                max_position = max(max_position, max(shelves))
    return str(max_section), str(max_shelf), str(max_position)


def get_layout_row(db, aisle):
    return db.execute(
        "SELECT aisle, config_json, max_section, max_shelf, max_position, enabled FROM aisle_layouts WHERE aisle=?",
        (str(aisle).strip(),),
    ).fetchone()


def _get_shelves_for_side(config, side):
    if side in ("Gauche", "Droite"):
        return None, True
    if side == "Façade A":
        return (config.get("facade_a") or {}).get("shelves", []), False
    if side == "Façade B":
        return (config.get("facade_b") or {}).get("shelves", []), False
    for pres in (config.get("presentoirs") or []):
        pname = pres.get("name", "")
        for facade in (pres.get("facades") or []):
            fname = facade.get("name", "")
            if side == f"{pname} - {fname}":
                return facade.get("shelves", []), False
    return [], False


def product_fits_layout(product, config):
    if not hasattr(product, "get"):
        product = dict(product)
    side = str(product["side"]).strip()
    shelf_index = clamp_non_negative_int(product.get("shelf", "0")) - 1
    position_value = clamp_non_negative_int(product.get("position", "0"))
    shelves, is_sectioned = _get_shelves_for_side(config, side)
    if is_sectioned:
        section_index = clamp_non_negative_int(product.get("section", "0")) - 1
        sections = ((config.get("sides", {}) or {}).get(side, {}) or {}).get("sections", [])
        if section_index < 0 or section_index >= len(sections):
            return False
        shelves = sections[section_index].get("shelves", [])
    if shelf_index < 0 or shelf_index >= len(shelves):
        return False
    return 1 <= position_value <= clamp_non_negative_int(shelves[shelf_index])


def remove_products_outside_layout(db, aisle, config):
    rows = db.execute(
        "SELECT id, side, section, shelf, position FROM products WHERE aisle=?",
        (str(aisle).strip(),),
    ).fetchall()
    removable_ids = [int(row["id"]) for row in rows if not product_fits_layout(row, config)]
    if removable_ids:
        placeholders = ",".join("?" for _ in removable_ids)
        db.execute(f"DELETE FROM products WHERE id IN ({placeholders})", tuple(removable_ids))
    return len(removable_ids)


def validate_layout_slot(db, aisle, side, section, shelf, position):
    row = get_layout_row(db, aisle)
    if not row:
        return False, f"L’allée {aisle} n’existe pas dans le plan."
    config = normalize_layout_config(row["config_json"], row["max_section"], row["max_shelf"], row["max_position"])
    if not product_fits_layout(
        {"side": side, "section": section, "shelf": shelf, "position": position},
        config,
    ):
        return False, "Cette position n’existe pas dans le plan de l’allée."
    return True, ""


def aisle_sort_key(value):
    text = str(value or "").strip()
    if text.isdigit():
        return (0, int(text), text)
    return (1, text.lower())


# ── Routes ─────────────────────────────────────────────────────────────────────

@layout_bp.route("/api/layout/aisles", methods=["GET"])
def get_layout_aisles():
    db = get_db()
    # ETag combines the layouts state AND the products state (the response embeds
    # per-aisle product counts) — unchanged plan = instant 304 for the phone.
    import hashlib
    from routes.products import products_state_key, client_etag_matches
    layouts_key_row = db.execute(
        "SELECT COUNT(*) AS n, MAX(modified_at) AS max_mod FROM aisle_layouts"
    ).fetchone()
    layouts_key = (tuple(layouts_key_row.values()) if isinstance(layouts_key_row, dict)
                   else tuple(layouts_key_row))
    etag = hashlib.md5(repr((layouts_key, products_state_key(db))).encode()).hexdigest()
    if client_etag_matches(etag):
        return "", 304
    aisles = db.execute(
        """
        SELECT l.aisle, l.max_section, l.max_shelf, l.max_position, l.config_json, l.enabled, l.modified_by, l.modified_at,
               COUNT(p.id) AS product_count
        FROM aisle_layouts l
        LEFT JOIN products p ON p.aisle = l.aisle
        GROUP BY l.aisle, l.max_section, l.max_shelf, l.max_position, l.config_json, l.enabled, l.modified_by, l.modified_at
        """
    ).fetchall()
    result = []
    for aisle in aisles:
        item = dict(aisle)
        item["config"] = normalize_layout_config(item.get("config_json", ""), item.get("max_section"), item.get("max_shelf"), item.get("max_position"))
        item.pop("config_json", None)
        result.append(item)
    result.sort(key=lambda item: aisle_sort_key(item.get("aisle")))
    response = jsonify(result)
    response.set_etag(etag, weak=True)
    return response


@layout_bp.route("/api/layout/aisles", methods=["POST"])
def create_layout_aisle():
    username, error = require_editor()
    if error:
        return error
    data = request.get_json() or {}
    aisle = str(data.get("aisle", "")).strip()
    config = normalize_layout_config(data.get("config"), data.get("max_section", "0"), data.get("max_shelf", "0"), data.get("max_position", "0"))
    max_section, max_shelf, max_position = layout_metrics(config)
    if not aisle:
        return jsonify({"error": "Numéro ou nom d'allée requis."}), 400
    if len(aisle) > 40:
        return jsonify({"error": "Nom d'allée trop long (40 caractères max)."}), 400
    # Allow names (Caisse, Labo, Près du labo…) as well as numbers. No quotes or
    # angle brackets so the name is always safe inside inline onclick handlers.
    if not re.fullmatch(r"[A-Za-z0-9À-ÿ .\-]+", aisle):
        return jsonify({"error": "Nom d'allée invalide : lettres, chiffres, espaces, points et tirets seulement."}), 400
    db = get_db()
    exists = db.execute("SELECT aisle FROM aisle_layouts WHERE aisle=?", (aisle,)).fetchone()
    if exists:
        return jsonify({"error": f"L’allée {aisle} existe déjà."}), 409
    db.execute(
        """
        INSERT INTO aisle_layouts (aisle, max_section, max_shelf, max_position, config_json, enabled, modified_by, modified_at)
        VALUES (?, ?, ?, ?, ?, 1, ?, ?)
        """,
        (aisle, max_section, max_shelf, max_position, json.dumps(config), username, utc_now_iso()),
    )
    db.commit()
    return jsonify({"success": True})


@layout_bp.route("/api/layout/aisles/<aisle>", methods=["PUT"])
def update_layout_aisle(aisle):
    username, error = require_editor()
    if error:
        return error
    data = request.get_json() or {}
    config = normalize_layout_config(data.get("config"), data.get("max_section", "0"), data.get("max_shelf", "0"), data.get("max_position", "0"))
    max_section, max_shelf, max_position = layout_metrics(config)
    enabled = 1 if data.get("enabled", True) else 0
    db = get_db()
    result = db.execute(
        """
        UPDATE aisle_layouts
        SET max_section=?, max_shelf=?, max_position=?, config_json=?, enabled=?, modified_by=?, modified_at=?
        WHERE aisle=?
        """,
        (max_section, max_shelf, max_position, json.dumps(config), enabled, username, utc_now_iso(), aisle),
    )
    removed_products = remove_products_outside_layout(db, aisle, config) if result.rowcount else 0
    db.commit()
    if result.rowcount == 0:
        return jsonify({"error": "Allée non trouvée."}), 404
    return jsonify({"success": True, "removed_products": removed_products})


@layout_bp.route("/api/layout/aisles/<aisle>", methods=["DELETE"])
def delete_layout_aisle(aisle):
    username, error = require_editor()
    if error:
        return error
    db = get_db()
    from routes.products import first_column
    removed_products = first_column(db.execute("SELECT COUNT(*) FROM products WHERE aisle=?", (aisle,)).fetchone()) or 0
    db.execute("DELETE FROM products WHERE aisle=?", (aisle,))
    result = db.execute("DELETE FROM aisle_layouts WHERE aisle=?", (aisle,))
    db.commit()
    if result.rowcount == 0:
        return jsonify({"error": "Allée non trouvée."}), 404
    return jsonify({"success": True, "message": f"Allée {aisle} retirée par {username}. {removed_products} produit(s) supprime(s)."})


@layout_bp.route("/api/layout/aisles/<aisle>/swap-sections", methods=["POST"])
def swap_sections(aisle):
    username, error = require_editor()
    if error:
        return error
    data = request.get_json() or {}
    side  = str(data.get("side", "")).strip()
    sec_a = str(data.get("section_a", "")).strip()
    sec_b = str(data.get("section_b", "")).strip()
    if not side or not sec_a or not sec_b or sec_a == sec_b:
        return jsonify({"success": False, "error": "Paramètres invalides."}), 400
    now = utc_now_iso()
    db = get_db()
    db.execute("UPDATE products SET section='__sw__', modified_by=?, modified_at=? WHERE aisle=? AND side=? AND section=?", (username, now, aisle, side, sec_a))
    db.execute("UPDATE products SET section=?,       modified_by=?, modified_at=? WHERE aisle=? AND side=? AND section=?", (sec_a, username, now, aisle, side, sec_b))
    db.execute("UPDATE products SET section=?,       modified_by=?, modified_at=? WHERE aisle=? AND side=? AND section=?", (sec_b, username, now, aisle, side, "__sw__"))
    db.commit()
    return jsonify({"success": True})


@layout_bp.route("/api/layout/aisles/<aisle>/swap-shelves", methods=["POST"])
def swap_shelves(aisle):
    username, error = require_editor()
    if error:
        return error
    data    = request.get_json() or {}
    side    = str(data.get("side", "")).strip()
    section = str(data.get("section", "1")).strip()
    sh_a    = str(data.get("shelf_a", "")).strip()
    sh_b    = str(data.get("shelf_b", "")).strip()
    if not side or not sh_a or not sh_b or sh_a == sh_b:
        return jsonify({"success": False, "error": "Paramètres invalides."}), 400
    now = utc_now_iso()
    db = get_db()
    db.execute("UPDATE products SET shelf='__sw__', modified_by=?, modified_at=? WHERE aisle=? AND side=? AND section=? AND shelf=?", (username, now, aisle, side, section, sh_a))
    db.execute("UPDATE products SET shelf=?,        modified_by=?, modified_at=? WHERE aisle=? AND side=? AND section=? AND shelf=?", (sh_a, username, now, aisle, side, section, sh_b))
    db.execute("UPDATE products SET shelf=?,        modified_by=?, modified_at=? WHERE aisle=? AND side=? AND section=? AND shelf=?", (sh_b, username, now, aisle, side, section, "__sw__"))
    db.commit()
    return jsonify({"success": True})


@layout_bp.route("/api/layout/aisles/<aisle>/swap-positions", methods=["POST"])
def swap_positions_route(aisle):
    username, error = require_editor()
    if error:
        return error
    data    = request.get_json() or {}
    side    = str(data.get("side", "")).strip()
    section = str(data.get("section", "1")).strip()
    shelf   = str(data.get("shelf", "")).strip()
    pos_a   = str(data.get("position_a", "")).strip()
    pos_b   = str(data.get("position_b", "")).strip()
    if not side or not shelf or not pos_a or not pos_b or pos_a == pos_b:
        return jsonify({"success": False, "error": "Paramètres invalides."}), 400
    now = utc_now_iso()
    db = get_db()
    db.execute("UPDATE products SET position='__sw__', modified_by=?, modified_at=? WHERE aisle=? AND side=? AND section=? AND shelf=? AND position=?", (username, now, aisle, side, section, shelf, pos_a))
    db.execute("UPDATE products SET position=?,        modified_by=?, modified_at=? WHERE aisle=? AND side=? AND section=? AND shelf=? AND position=?", (pos_a, username, now, aisle, side, section, shelf, pos_b))
    db.execute("UPDATE products SET position=?,        modified_by=?, modified_at=? WHERE aisle=? AND side=? AND section=? AND shelf=? AND position=?", (pos_b, username, now, aisle, side, section, shelf, "__sw__"))
    db.commit()
    return jsonify({"success": True})


def _renumber_after_remove(db, username, now, aisle, side, field, removed, section=None):
    """Delete products at `field`==removed, then shift every higher `field` down by 1
    so product numbering stays aligned with the config after a middle removal.
    `field` is 'section' or 'shelf'; for 'shelf', scope to a single section when one
    is given (fixture sides — façades/présentoirs — have no sections, pass None)."""
    where = "aisle=? AND side=?"
    params = [aisle, side]
    if field == "shelf" and section is not None:
        where += " AND section=?"
        params.append(section)
    # 1. delete products in the removed section/shelf
    delete_result = db.execute(f"DELETE FROM products WHERE {where} AND {field}=?", tuple(params + [str(removed)]))
    removed_count = max(0, int(getattr(delete_result, "rowcount", 0) or 0))
    # 2. shift every higher number down by one. Process in ASCENDING order so each
    #    lower target slot is vacated before the next product shifts into it —
    #    otherwise the unique (aisle,side,section,shelf,position) index would clash.
    rows = db.execute(f"SELECT id, {field} FROM products WHERE {where}", tuple(params)).fetchall()
    shiftable = []
    for r in rows:
        val = r[field] if isinstance(r, dict) else r[1]
        rid = r["id"] if isinstance(r, dict) else r[0]
        try:
            n = int(val)
        except (TypeError, ValueError):
            continue
        if n > int(removed):
            shiftable.append((n, rid))
    for n, rid in sorted(shiftable):   # ascending → no slot collision
        db.execute(f"UPDATE products SET {field}=?, modified_by=?, modified_at=? WHERE id=?",
                   (str(n - 1), username, now, rid))
    return removed_count


def _config_for_removal(db, aisle, supplied_config):
    """Build an atomic removal from the structure currently shown to the user."""
    row = get_layout_row(db, aisle)
    if not row:
        return None, None
    source = supplied_config if isinstance(supplied_config, dict) else row["config_json"]
    return row, normalize_layout_config(
        source, row["max_section"], row["max_shelf"], row["max_position"]
    )


def _remove_section_from_config(config, side, section_number):
    if side not in ("Gauche", "Droite"):
        return False
    sections = config["sides"][side]["sections"]
    index = clamp_non_negative_int(section_number) - 1
    if index < 0 or index >= len(sections):
        return False
    sections.pop(index)
    return True


def _fixture_for_side(config, side):
    if side == "Façade A":
        return config.get("facade_a")
    if side == "Façade B":
        return config.get("facade_b")
    for presentoir in config.get("presentoirs", []):
        for facade in presentoir.get("facades", []):
            if side == f"{presentoir.get('name', '')} - {facade.get('name', '')}":
                return facade
    return None


def _remove_shelf_from_config(config, side, section_number, shelf_number):
    shelf_index = clamp_non_negative_int(shelf_number) - 1
    if shelf_index < 0:
        return False
    if side in ("Gauche", "Droite"):
        section_index = clamp_non_negative_int(section_number) - 1
        sections = config["sides"][side]["sections"]
        if section_index < 0 or section_index >= len(sections):
            return False
        fixture = sections[section_index]
    else:
        fixture = _fixture_for_side(config, side)
    if not fixture or shelf_index >= len(fixture.get("shelves", [])):
        return False
    fixture["shelves"].pop(shelf_index)
    labels = fixture.get("labels", [])
    if shelf_index < len(labels):
        labels.pop(shelf_index)
    return True


@layout_bp.route("/api/layout/aisles/<aisle>/remove-section", methods=["POST"])
def remove_section(aisle):
    username, error = require_editor()
    if error:
        return error
    data = request.get_json() or {}
    side    = str(data.get("side", "")).strip()
    section = str(data.get("section", "")).strip()
    if not side or not section:
        return jsonify({"success": False, "error": "Paramètres invalides."}), 400
    db = get_db()
    _row, config = _config_for_removal(db, aisle, data.get("config"))
    if config is None:
        return jsonify({"success": False, "error": "Allée non trouvée."}), 404
    if not _remove_section_from_config(config, side, section):
        return jsonify({"success": False, "error": "Cette section n'existe plus."}), 409
    now = utc_now_iso()
    removed_products = _renumber_after_remove(db, username, now, aisle, side, "section", section)
    removed_products += remove_products_outside_layout(db, aisle, config)
    _persist_aisle_config(db, aisle, config, username, now)
    db.commit()
    return jsonify({"success": True, "config": config, "removed_products": removed_products})


@layout_bp.route("/api/layout/aisles/<aisle>/remove-shelf", methods=["POST"])
def remove_shelf(aisle):
    username, error = require_editor()
    if error:
        return error
    data = request.get_json() or {}
    side    = str(data.get("side", "")).strip()
    section = str(data.get("section", "1")).strip()
    shelf   = str(data.get("shelf", "")).strip()
    if not side or not shelf:
        return jsonify({"success": False, "error": "Paramètres invalides."}), 400
    db = get_db()
    _row, config = _config_for_removal(db, aisle, data.get("config"))
    if config is None:
        return jsonify({"success": False, "error": "Allée non trouvée."}), 404
    if not _remove_shelf_from_config(config, side, section, shelf):
        return jsonify({"success": False, "error": "Cette tablette n'existe plus."}), 409
    # Fixture sides (Façade A/B, présentoir façades) carry no meaningful section
    # value on their products — match their shelves across all sections.
    section_scope = section if side in ("Gauche", "Droite") else None
    now = utc_now_iso()
    removed_products = _renumber_after_remove(
        db, username, now, aisle, side, "shelf", shelf, section=section_scope
    )
    removed_products += remove_products_outside_layout(db, aisle, config)
    _persist_aisle_config(db, aisle, config, username, now)
    db.commit()
    return jsonify({"success": True, "config": config, "removed_products": removed_products})


def _persist_aisle_config(db, aisle, config, username, now):
    max_section, max_shelf, max_position = layout_metrics(config)
    db.execute(
        "UPDATE aisle_layouts SET max_section=?, max_shelf=?, max_position=?, config_json=?, modified_by=?, modified_at=? WHERE aisle=?",
        (max_section, max_shelf, max_position, json.dumps(config), username, now, str(aisle).strip()),
    )


def _swap_two_sections(db, username, now, aisle, side, a, b):
    """Swap the products of two sections via a temp marker (no unique-slot clash)."""
    a, b = str(a), str(b)
    db.execute("UPDATE products SET section='__sw__', modified_by=?, modified_at=? WHERE aisle=? AND side=? AND section=?", (username, now, aisle, side, a))
    db.execute("UPDATE products SET section=?,        modified_by=?, modified_at=? WHERE aisle=? AND side=? AND section=?", (a, username, now, aisle, side, b))
    db.execute("UPDATE products SET section=?,        modified_by=?, modified_at=? WHERE aisle=? AND side=? AND section=?", (b, username, now, aisle, side, "__sw__"))


def _bubble_section(db, username, now, config, aisle, side, from_index, to_index):
    """Move a section from from_index to to_index within a côté using adjacent
    swaps (config + products stay in sync, no slot clash). Returns final index."""
    sections = config["sides"][side]["sections"]
    to_index = max(0, min(to_index, len(sections) - 1))
    step = 1 if to_index > from_index else -1
    i = from_index
    while i != to_index:
        j = i + step
        sections[i], sections[j] = sections[j], sections[i]
        _swap_two_sections(db, username, now, aisle, side, i + 1, j + 1)
        i = j
    return to_index


@layout_bp.route("/api/layout/aisles/<aisle>/move-section-to-aisle", methods=["POST"])
def move_section_to_aisle(aisle):
    """Move a section (shelves + products) to a chosen allée / côté / position.
    Same côté → reorder to the chosen position; other côté/allée → move there and
    insert at the chosen position (default: end). One request, no slot clash."""
    username, error = require_editor()
    if error:
        return error
    data = request.get_json() or {}
    side          = str(data.get("side", "")).strip()
    section_index = clamp_non_negative_int(data.get("section_index", -1), -1)
    target_aisle  = str(data.get("target_aisle", "")).strip() or str(aisle).strip()
    target_side   = str(data.get("target_side", "")).strip() or side
    raw_pos       = data.get("target_position")   # 1-based; blank/None = end
    if side not in ("Gauche", "Droite") or target_side not in ("Gauche", "Droite"):
        return jsonify({"success": False, "error": "Le déplacement de section ne s'applique qu'aux côtés A/B."}), 400
    if data.get("section_index") is None:
        return jsonify({"success": False, "error": "Paramètres invalides."}), 400
    db = get_db()
    src_row, tgt_row = get_layout_row(db, aisle), get_layout_row(db, target_aisle)
    if not src_row or not tgt_row:
        return jsonify({"success": False, "error": "Allée introuvable."}), 404
    src_cfg = normalize_layout_config(src_row["config_json"], src_row["max_section"], src_row["max_shelf"], src_row["max_position"])
    src_sections = src_cfg["sides"][side]["sections"]
    if section_index < 0 or section_index >= len(src_sections):
        return jsonify({"success": False, "error": "Section introuvable."}), 404
    now = utc_now_iso()

    # ── Same côté: just reorder to the chosen position ──────────────────────────
    if target_aisle == str(aisle).strip() and target_side == side:
        to_index = (clamp_non_negative_int(raw_pos, len(src_sections)) - 1) if raw_pos not in (None, "") else len(src_sections) - 1
        final = _bubble_section(db, username, now, src_cfg, aisle, side, section_index, to_index)
        _persist_aisle_config(db, aisle, src_cfg, username, now)
        db.commit()
        return jsonify({"success": True, "target_aisle": str(aisle).strip(), "target_side": side, "target_section": final + 1})

    # ── Other côté / allée: move there, then insert at the chosen position ──────
    tgt_cfg = normalize_layout_config(tgt_row["config_json"], tgt_row["max_section"], tgt_row["max_shelf"], tgt_row["max_position"])
    tgt_sections = tgt_cfg["sides"][target_side]["sections"]
    old_section_number = section_index + 1
    appended_number = len(tgt_sections) + 1
    tgt_sections.append(src_sections[section_index])
    db.execute(
        "UPDATE products SET aisle=?, side=?, section=?, modified_by=?, modified_at=? WHERE aisle=? AND side=? AND section=?",
        (target_aisle, target_side, str(appended_number), username, now, aisle, side, str(old_section_number)),
    )
    src_sections.pop(section_index)
    _renumber_after_remove(db, username, now, aisle, side, "section", str(old_section_number))
    final_number = appended_number
    if raw_pos not in (None, ""):
        final = _bubble_section(db, username, now, tgt_cfg, target_aisle, target_side, len(tgt_sections) - 1, clamp_non_negative_int(raw_pos, appended_number) - 1)
        final_number = final + 1
    _persist_aisle_config(db, aisle, src_cfg, username, now)
    _persist_aisle_config(db, target_aisle, tgt_cfg, username, now)
    db.commit()
    return jsonify({"success": True, "target_aisle": target_aisle, "target_side": target_side, "target_section": final_number})
