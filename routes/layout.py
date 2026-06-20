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
        return False, f"L allee {aisle} n existe pas dans le plan."
    config = normalize_layout_config(row["config_json"], row["max_section"], row["max_shelf"], row["max_position"])
    if not product_fits_layout(
        {"side": side, "section": section, "shelf": shelf, "position": position},
        config,
    ):
        return False, "Cette position n existe pas dans le plan de l allee."
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
    return jsonify(result)


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
        return jsonify({"error": "Numero d allee requis."}), 400
    if not re.fullmatch(r"\d+", aisle):
        return jsonify({"error": "Le numero d allee doit etre numerique."}), 400
    db = get_db()
    exists = db.execute("SELECT aisle FROM aisle_layouts WHERE aisle=?", (aisle,)).fetchone()
    if exists:
        return jsonify({"error": f"L allee {aisle} existe deja."}), 409
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
        return jsonify({"error": "Allee non trouvee."}), 404
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
        return jsonify({"error": "Allee non trouvee."}), 404
    return jsonify({"success": True, "message": f"Allee {aisle} retiree par {username}. {removed_products} produit(s) supprime(s)."})


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
