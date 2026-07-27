import json
import re
from flask import Blueprint, request, jsonify
from database import get_db, DatabaseIntegrityError
from auth import require_editor, utc_now_iso

layout_bp = Blueprint("layout", __name__)

MAX_LAYOUT_SECTIONS = 200
MAX_LAYOUT_SHELVES = 100
MAX_LAYOUT_POSITIONS = 500
MAX_LAYOUT_PRESENTOIRS = 100
MAX_LAYOUT_FACADES = 50
MAX_LAYOUT_LABEL_LENGTH = 160
_AISLE_NAME_RE = re.compile(r"[A-Za-z0-9À-ÿ .\-]+")


# ── Config helpers ─────────────────────────────────────────────────────────────

def clamp_non_negative_int(value, fallback=0, maximum=None):
    try:
        cleaned = max(0, int(str(value)))
        return min(cleaned, maximum) if maximum is not None else cleaned
    except (TypeError, ValueError):
        return fallback


def valid_aisle_name(value):
    aisle = str(value or "").strip()
    return bool(aisle and len(aisle) <= 40 and _AISLE_NAME_RE.fullmatch(aisle))


def _layout_label(value):
    return str(value or "")[:MAX_LAYOUT_LABEL_LENGTH]


def build_default_layout_config(max_section, max_shelf, max_position):
    section_count = clamp_non_negative_int(max_section, maximum=MAX_LAYOUT_SECTIONS)
    shelf_count = clamp_non_negative_int(max_shelf, maximum=MAX_LAYOUT_SHELVES)
    position_count = clamp_non_negative_int(max_position, maximum=MAX_LAYOUT_POSITIONS)
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
        for section in sections[:MAX_LAYOUT_SECTIONS]:
            shelves = section.get("shelves") if isinstance(section, dict) else None
            if not isinstance(shelves, list):
                continue
            cleaned_shelves = [
                clamp_non_negative_int(shelf, maximum=MAX_LAYOUT_POSITIONS)
                for shelf in shelves[:MAX_LAYOUT_SHELVES]
            ]
            raw_labels = section.get("labels", []) if isinstance(section, dict) else []
            if not isinstance(raw_labels, list):
                raw_labels = []
            cleaned_labels = [
                _layout_label(raw_labels[i]) if i < len(raw_labels) else ""
                for i in range(len(cleaned_shelves))
            ]
            normalized_sections.append({"shelves": cleaned_shelves, "labels": cleaned_labels})
        if not has_explicit_sections:
            normalized_sections = default["sides"][side]["sections"]
        normalized_sides[side] = {"sections": normalized_sections}

    def norm_fixture(fx):
        if not isinstance(fx, dict):
            return {"shelves": [], "labels": []}
        raw_sh = fx.get("shelves", [])
        if not isinstance(raw_sh, list): raw_sh = []
        shelves = [
            clamp_non_negative_int(v, maximum=MAX_LAYOUT_POSITIONS)
            for v in raw_sh[:MAX_LAYOUT_SHELVES]
        ]
        raw_lab = fx.get("labels", [])
        if not isinstance(raw_lab, list): raw_lab = []
        labels = [_layout_label(raw_lab[i]) if i < len(raw_lab) else "" for i in range(len(shelves))]
        return {"shelves": shelves, "labels": labels}

    normalized_facade_a = norm_fixture(config.get("facade_a") if isinstance(config, dict) else None)
    normalized_facade_b = norm_fixture(config.get("facade_b") if isinstance(config, dict) else None)

    raw_pres = config.get("presentoirs", []) if isinstance(config, dict) else []
    if not isinstance(raw_pres, list): raw_pres = []
    normalized_pres = []
    for p in raw_pres[:MAX_LAYOUT_PRESENTOIRS]:
        if not isinstance(p, dict):
            continue
        name = _layout_label(p.get("name", "Présentoir")).strip() or "Présentoir"
        raw_facades = p.get("facades") if isinstance(p.get("facades"), list) else None
        if raw_facades:
            facades = []
            for i, f in enumerate(raw_facades[:MAX_LAYOUT_FACADES]):
                if not isinstance(f, dict):
                    continue
                fname = _layout_label(f.get("name", f"Façade {i+1}")).strip() or f"Façade {i+1}"
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
        """SELECT aisle, config_json, max_section, max_shelf, max_position,
                  sort_order, enabled, modified_by, modified_at
           FROM aisle_layouts WHERE aisle=?""",
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
    capacity = clamp_non_negative_int(shelves[shelf_index])
    # Zero is the explicit "tablette libre" mode: positions are not capped and
    # existing products must never be treated as outside the layout.
    return position_value >= 1 and (capacity == 0 or position_value <= capacity)


def products_outside_layout(db, aisle, config):
    rows = db.execute(
        "SELECT id, side, section, shelf, position FROM products WHERE aisle=?",
        (str(aisle).strip(),),
    ).fetchall()
    return [row for row in rows if not product_fits_layout(row, config)]


def remove_products_outside_layout(db, aisle, config, username, now=None):
    outside = products_outside_layout(db, aisle, config)
    if not outside:
        return 0
    removable_ids = [int(row["id"]) for row in outside]
    placeholders = ",".join("?" for _ in removable_ids)
    products = db.execute(
        f"SELECT * FROM products WHERE id IN ({placeholders})", tuple(removable_ids)
    ).fetchall()
    from routes.products import archive_and_delete_products
    return archive_and_delete_products(db, products, username, now)


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


def _clean_product_ids(raw_ids):
    if not isinstance(raw_ids, list):
        return []
    result = []
    seen = set()
    for raw_id in raw_ids:
        try:
            product_id = int(raw_id)
        except (TypeError, ValueError):
            continue
        if product_id <= 0 or product_id in seen:
            continue
        seen.add(product_id)
        result.append(product_id)
    return result


class _StaleBulkProductError(Exception):
    pass


class _StaleStructureError(Exception):
    pass


def _product_rows_by_ids(db, product_ids):
    rows = []
    for start in range(0, len(product_ids), 400):
        chunk = product_ids[start:start + 400]
        placeholders = ",".join("?" for _id in chunk)
        rows.extend(db.execute(
            f"SELECT * FROM products WHERE id IN ({placeholders})", tuple(chunk)
        ).fetchall())
    by_id = {int(row["id"]): row for row in rows}
    return [by_id[product_id] for product_id in product_ids if product_id in by_id]


def _product_rows_for_delete_scope(db, raw_scope):
    if not isinstance(raw_scope, dict):
        return None, None, "Zone de suppression invalide."
    kind = str(raw_scope.get("kind", "") or "").strip().lower()
    if kind not in {"aisle", "side", "section", "shelf"}:
        return None, None, "Type de zone invalide."
    scope = {
        "kind": kind,
        "aisle": str(raw_scope.get("aisle", "") or "").strip(),
        "side": str(raw_scope.get("side", "") or "").strip(),
        "section": str(raw_scope.get("section", "") or "").strip(),
        "shelf": str(raw_scope.get("shelf", "") or "").strip(),
    }
    required = {
        "aisle": ("aisle",),
        "side": ("aisle", "side"),
        "section": ("aisle", "side", "section"),
        "shelf": ("aisle", "side", "section", "shelf"),
    }[kind]
    if any(not scope[field] for field in required):
        return None, None, "Coordonnees de la zone incompletes."
    if len(scope["aisle"]) > 40 or any(
        len(scope[field]) > 240 or "\x00" in scope[field]
        for field in ("side", "section", "shelf")
    ):
        return None, None, "Coordonnees de la zone invalides."

    clauses = ["aisle=?"]
    params = [scope["aisle"]]
    if kind in {"side", "section", "shelf"}:
        clauses.append("side=?")
        params.append(scope["side"])
    if kind in {"section", "shelf"}:
        clauses.append("section=?")
        params.append(scope["section"])
    if kind == "shelf":
        clauses.append("shelf=?")
        params.append(scope["shelf"])
    query = "SELECT * FROM products WHERE " + " AND ".join(clauses) + " ORDER BY id"
    if getattr(db, "backend", "sqlite") == "postgres":
        query += " FOR UPDATE"
    return db.execute(query, tuple(params)).fetchall(), scope, None


def _target_shelves(config, side, section):
    if side in ("Gauche", "Droite"):
        section_index = clamp_non_negative_int(section) - 1
        sections = config["sides"][side]["sections"]
        if section_index < 0 or section_index >= len(sections):
            return None
        return sections[section_index].get("shelves", [])
    fixture = _fixture_for_side(config, side)
    return fixture.get("shelves", []) if fixture else None


def _bulk_product_sort_key(row):
    side_order = {"Facade A": 0, "Façade A": 0, "Gauche": 1, "Droite": 2,
                  "Facade B": 3, "Façade B": 3}
    return (
        aisle_sort_key(row["aisle"]),
        side_order.get(str(row["side"]), 4),
        clamp_non_negative_int(row["section"]),
        clamp_non_negative_int(row["shelf"]),
        clamp_non_negative_int(row["position"]),
        int(row["id"]),
    )


def _bulk_move_destinations(db, selected_ids, aisle, side, section, shelf, mode, config):
    shelves = _target_shelves(config, side, section)
    if shelves is None:
        return None, "La section de destination n'existe pas."
    if not shelves:
        return None, "La section de destination ne contient aucune tablette."

    if mode == "shelf":
        shelf_number = clamp_non_negative_int(shelf)
        if shelf_number < 1 or shelf_number > len(shelves):
            return None, "La tablette de destination n'existe pas."
        shelf_numbers = [shelf_number]
    elif mode == "section" and side in ("Gauche", "Droite"):
        shelf_numbers = list(range(1, len(shelves) + 1))
    else:
        return None, "Destination invalide."

    target_rows = db.execute(
        "SELECT id, shelf, position FROM products WHERE aisle=? AND side=? AND section=?",
        (aisle, side, section),
    ).fetchall()
    occupied = {number: set() for number in shelf_numbers}
    for row in target_rows:
        if int(row["id"]) in selected_ids:
            continue
        shelf_number = clamp_non_negative_int(row["shelf"])
        position_number = clamp_non_negative_int(row["position"])
        if shelf_number in occupied and position_number > 0:
            occupied[shelf_number].add(position_number)

    destinations = []
    for shelf_number in shelf_numbers:
        capacity = clamp_non_negative_int(shelves[shelf_number - 1])
        if capacity == 0:
            position = 1
            while len(destinations) < len(selected_ids):
                if position not in occupied[shelf_number]:
                    destinations.append((shelf_number, position))
                position += 1
            break
        for position in range(1, capacity + 1):
            if position not in occupied[shelf_number]:
                destinations.append((shelf_number, position))
                if len(destinations) >= len(selected_ids):
                    break
        if len(destinations) >= len(selected_ids):
            break
    if len(destinations) < len(selected_ids):
        return None, (
            f"Espace insuffisant: {len(selected_ids)} produit(s) selectionne(s), "
            f"mais seulement {len(destinations)} position(s) libre(s) dans cette destination."
        )
    return destinations[:len(selected_ids)], ""


def _lock_layout_rows(db, rows, expected_versions=None):
    expected_versions = expected_versions if isinstance(expected_versions, dict) else {}
    ordered = sorted(rows, key=lambda row: aisle_sort_key(row["aisle"]))
    for row in ordered:
        aisle = str(row["aisle"])
        if aisle in expected_versions:
            expected = str(expected_versions.get(aisle) or "")
            if expected != str(row["modified_at"] or ""):
                raise _StaleStructureError()
    for row in ordered:
        result = db.execute(
            """UPDATE aisle_layouts SET aisle=aisle
               WHERE aisle=? AND COALESCE(modified_at, '')=?""",
            (str(row["aisle"]), str(row["modified_at"] or "")),
        )
        if result.rowcount != 1:
            raise _StaleStructureError()


def _products_in_containers(db, containers):
    unique = []
    seen = set()
    for aisle, side, section in containers:
        key = (str(aisle), str(side), str(section))
        if key not in seen:
            seen.add(key)
            unique.append(key)
    if not unique:
        return []
    clauses = []
    params = []
    for aisle, side, section in unique:
        clauses.append("(aisle=? AND side=? AND section=?)")
        params.extend([aisle, side, section])
    return db.execute(
        "SELECT id, aisle, side, section, shelf, position, modified_at "
        f"FROM products WHERE {' OR '.join(clauses)}",
        tuple(params),
    ).fetchall()


def _apply_structure_product_locations(db, rows, location_mapper, username, now):
    changes = []
    location_fields = ("aisle", "side", "section", "shelf", "position")
    for row in rows:
        current = {field: str(row[field]) for field in location_fields}
        target = location_mapper(row)
        target = {field: str(target.get(field, current[field])) for field in location_fields}
        if any(target[field] != current[field] for field in location_fields):
            changes.append((row, target))

    temporary_aisle = "__structure_move__"
    if changes:
        result = db.executemany(
            """UPDATE products
               SET aisle=?, side='__structure__', section='0', shelf='0', position=?,
                   modified_by=?, modified_at=?
               WHERE id=? AND COALESCE(modified_at, '')=?""",
            [
                (
                    temporary_aisle, str(row["id"]), username, now,
                    int(row["id"]), str(row["modified_at"] or ""),
                )
                for row, _target in changes
            ],
        )
        if result.rowcount != len(changes):
            raise _StaleStructureError()

    updates = []
    if changes:
        result = db.executemany(
            """UPDATE products
               SET aisle=?, side=?, section=?, shelf=?, position=?,
                   modified_by=?, modified_at=?
               WHERE id=? AND aisle=? AND side='__structure__' AND position=?""",
            [
                (
                    target["aisle"], target["side"], target["section"],
                    target["shelf"], target["position"], username, now,
                    int(row["id"]), temporary_aisle, str(row["id"]),
                )
                for row, target in changes
            ],
        )
        if result.rowcount != len(changes):
            raise _StaleStructureError()
    for row, target in changes:
        updates.append({
            "id": int(row["id"]), **target,
            "modified_by": username, "modified_at": now,
        })
    return updates


def _final_index_from_boundary(source_index, boundary_index, item_count):
    boundary = max(0, min(clamp_non_negative_int(boundary_index), item_count))
    if boundary > source_index:
        boundary -= 1
    return max(0, min(boundary, max(0, item_count - 1)))


def _reordered_number(old_index, source_index, final_index):
    if old_index == source_index:
        return final_index
    if source_index < final_index and source_index < old_index <= final_index:
        return old_index - 1
    if final_index < source_index and final_index <= old_index < source_index:
        return old_index + 1
    return old_index


def _configs_response(configs):
    return {str(aisle): config for aisle, config in configs.items()}


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
    etag = hashlib.sha256(repr((layouts_key, products_state_key(db))).encode()).hexdigest()
    if client_etag_matches(etag):
        return "", 304
    aisles = db.execute(
        """
        SELECT l.aisle, l.sort_order, l.max_section, l.max_shelf, l.max_position, l.config_json, l.enabled, l.modified_by, l.modified_at,
               COUNT(p.id) AS product_count
        FROM aisle_layouts l
        LEFT JOIN products p ON p.aisle = l.aisle
        GROUP BY l.aisle, l.sort_order, l.max_section, l.max_shelf, l.max_position, l.config_json, l.enabled, l.modified_by, l.modified_at
        """
    ).fetchall()
    result = []
    for aisle in aisles:
        item = dict(aisle)
        item["config"] = normalize_layout_config(item.get("config_json", ""), item.get("max_section"), item.get("max_shelf"), item.get("max_position"))
        item.pop("config_json", None)
        result.append(item)
    result.sort(key=lambda item: (
        clamp_non_negative_int(item.get("sort_order"), 10 ** 9) or 10 ** 9,
        aisle_sort_key(item.get("aisle")),
    ))
    response = jsonify(result)
    response.set_etag(etag, weak=True)
    return response


@layout_bp.route("/api/layout/aisles/reorder", methods=["POST"])
def reorder_layout_aisles():
    username, error = require_editor()
    if error:
        return error
    data = request.get_json() or {}
    raw_order = data.get("ordered_aisles")
    if not isinstance(raw_order, list):
        return jsonify({"success": False, "error": "Ordre des allees invalide."}), 400
    ordered_aisles = []
    seen = set()
    for value in raw_order:
        aisle = str(value or "").strip()
        if not aisle or aisle in seen:
            return jsonify({"success": False, "error": "Ordre des allees invalide."}), 400
        seen.add(aisle)
        ordered_aisles.append(aisle)

    db = get_db()
    rows = db.execute(
        """SELECT aisle, config_json, max_section, max_shelf, max_position,
                  sort_order, enabled, modified_by, modified_at
           FROM aisle_layouts"""
    ).fetchall()
    existing = {str(row["aisle"]) for row in rows}
    if set(ordered_aisles) != existing or len(ordered_aisles) != len(rows):
        return jsonify({
            "success": False,
            "code": "stale_layout",
            "error": "Le plan a change. Rechargez-le avant de deplacer une allee.",
        }), 409
    now = utc_now_iso()
    try:
        _lock_layout_rows(db, rows, data.get("expected_layouts"))
        result = db.executemany(
            """UPDATE aisle_layouts
               SET sort_order=?, modified_by=?, modified_at=? WHERE aisle=?""",
            [
                (sort_order, username, now, aisle)
                for sort_order, aisle in enumerate(ordered_aisles, start=1)
            ],
        )
        if result.rowcount != len(ordered_aisles):
            raise _StaleStructureError()
        db.commit()
    except _StaleStructureError:
        db.rollback()
        return jsonify({
            "success": False,
            "code": "stale_layout",
            "error": "Le plan a change pendant le deplacement. Aucune allee n'a ete deplacee.",
        }), 409
    except Exception as exc:
        db.rollback()
        print(f"[Plan] Aisle reorder rolled back: {exc}")
        return jsonify({
            "success": False,
            "error": "Deplacement annule: l'ordre des allees est reste intact.",
        }), 500
    return jsonify({
        "success": True,
        "ordered_aisles": ordered_aisles,
        "layout_versions": {aisle: now for aisle in ordered_aisles},
    })


@layout_bp.route("/api/layout/products/bulk-move", methods=["POST"])
def bulk_move_layout_products():
    username, error = require_editor()
    if error:
        return error
    data = request.get_json() or {}
    product_ids = _clean_product_ids(data.get("product_ids"))
    target = data.get("target") if isinstance(data.get("target"), dict) else {}
    aisle = str(target.get("aisle", "")).strip()
    side = str(target.get("side", "")).strip()
    section = str(target.get("section", "1")).strip() or "1"
    shelf = str(target.get("shelf", "")).strip()
    mode = str(target.get("mode", "shelf")).strip().lower()
    if not product_ids:
        return jsonify({"success": False, "error": "Aucun produit selectionne."}), 400
    if not aisle or not side or mode not in ("section", "shelf"):
        return jsonify({"success": False, "error": "Destination invalide."}), 400

    db = get_db()
    layout_row = get_layout_row(db, aisle)
    if not layout_row:
        return jsonify({"success": False, "error": "Allee de destination introuvable."}), 404
    if "expected_layout_modified_at" in data:
        stale = _stale_layout_response(layout_row, {
            "expected_modified_at": data.get("expected_layout_modified_at")
        })
        if stale:
            return stale
    rows = _product_rows_by_ids(db, product_ids)
    if len(rows) != len(product_ids):
        return jsonify({
            "success": False,
            "code": "stale_products",
            "error": "La selection a change. Rechargez le plan avant de la deplacer.",
        }), 409
    expected_products = data.get("expected_products")
    if isinstance(expected_products, dict):
        stale_ids = [
            int(row["id"]) for row in rows
            if str(row["id"]) in expected_products
            and str(row["modified_at"] or "") != str(expected_products[str(row["id"])] or "")
        ]
        if stale_ids:
            return jsonify({
                "success": False,
                "code": "stale_products",
                "error": "Un produit selectionne a ete modifie. Rechargez le plan avant de continuer.",
                "stale_product_ids": stale_ids,
            }), 409

    config = normalize_layout_config(
        layout_row["config_json"], layout_row["max_section"],
        layout_row["max_shelf"], layout_row["max_position"],
    )
    selected_ids = set(product_ids)
    destinations, destination_error = _bulk_move_destinations(
        db, selected_ids, aisle, side, section, shelf, mode, config
    )
    if destinations is None:
        return jsonify({"success": False, "error": destination_error}), 409

    ordered_rows = sorted(rows, key=_bulk_product_sort_key)
    now = utc_now_iso()
    temporary_aisle = "__bulk_move__"
    try:
        # Vacate every source slot first. This makes moves and swaps collision-free
        # even when the destination contains another selected product.
        result = db.executemany(
            """UPDATE products
               SET aisle=?, side='__bulk__', section='0', shelf='0', position=?,
                   modified_by=?, modified_at=?
               WHERE id=? AND COALESCE(modified_at, '')=?""",
            [
                (
                    temporary_aisle, str(row["id"]), username, now,
                    int(row["id"]), str(row["modified_at"] or ""),
                )
                for row in ordered_rows
            ],
        )
        if result.rowcount != len(ordered_rows):
            raise _StaleBulkProductError()
        result = db.executemany(
            """UPDATE products
               SET aisle=?, side=?, section=?, shelf=?, position=?,
                   modified_by=?, modified_at=?
               WHERE id=? AND aisle=? AND side='__bulk__'""",
            [
                (
                    aisle, side, section, str(target_shelf), str(target_position),
                    username, now, int(row["id"]), temporary_aisle,
                )
                for row, (target_shelf, target_position) in zip(ordered_rows, destinations)
            ],
        )
        if result.rowcount != len(ordered_rows):
            raise _StaleBulkProductError()
        db.commit()
    except _StaleBulkProductError:
        db.rollback()
        return jsonify({
            "success": False,
            "code": "stale_products",
            "error": "La selection a change pendant le deplacement. Rechargez le plan; aucun produit n'a ete modifie.",
        }), 409
    except Exception as exc:
        db.rollback()
        print(f"[Plan] Bulk move rolled back: {exc}")
        return jsonify({
            "success": False,
            "error": "Deplacement annule: aucun produit n'a ete modifie.",
        }), 409 if isinstance(exc, DatabaseIntegrityError) else 500

    product_updates = [
        {
            "id": int(row["id"]),
            "aisle": aisle,
            "side": side,
            "section": section,
            "shelf": str(target_shelf),
            "position": str(target_position),
            "modified_by": username,
            "modified_at": now,
        }
        for row, (target_shelf, target_position) in zip(ordered_rows, destinations)
    ]
    return jsonify({
        "success": True,
        "moved_products": len(product_updates),
        "product_updates": product_updates,
    })


@layout_bp.route("/api/layout/products/bulk-delete", methods=["POST"])
def bulk_delete_layout_products():
    username, error = require_editor()
    if error:
        return error
    data = request.get_json() or {}
    db = get_db()
    scope = None
    if "scope" in data:
        rows, scope, scope_error = _product_rows_for_delete_scope(db, data.get("scope"))
        if scope_error:
            return jsonify({"success": False, "error": scope_error}), 400
    else:
        product_ids = _clean_product_ids(data.get("product_ids"))
        if not product_ids:
            return jsonify({"success": False, "error": "Aucun produit selectionne."}), 400
        rows = _product_rows_by_ids(db, product_ids)
        if len(rows) != len(product_ids):
            return jsonify({
                "success": False,
                "code": "stale_products",
                "error": "La selection a change. Rechargez le plan; aucun produit n'a ete supprime.",
            }), 409
        expected_products = data.get("expected_products")
        if isinstance(expected_products, dict):
            stale_ids = [
                int(row["id"]) for row in rows
                if str(row["id"]) in expected_products
                and str(row["modified_at"] or "") != str(expected_products[str(row["id"])] or "")
            ]
            if stale_ids:
                return jsonify({
                    "success": False,
                    "code": "stale_products",
                    "error": "Un produit selectionne a ete modifie. Rechargez le plan avant de le supprimer.",
                    "stale_product_ids": stale_ids,
                }), 409
    if not rows:
        return jsonify({
            "success": True,
            "removed_products": 0,
            "deleted_product_ids": [],
            "scope": scope,
        })
    from routes.products import archive_and_delete_products
    try:
        if scope is None:
            # Explicit mixed selections retain optimistic concurrency checks.
            # Scoped clears are already locked by one exact SELECT FOR UPDATE.
            result = db.executemany(
                """UPDATE products SET id=id
                   WHERE id=? AND COALESCE(modified_at, '')=?""",
                [
                    (int(row["id"]), str(row["modified_at"] or ""))
                    for row in rows
                ],
            )
            if result.rowcount != len(rows):
                raise _StaleBulkProductError()
        removed_count = archive_and_delete_products(db, rows, username, utc_now_iso())
        db.commit()
    except _StaleBulkProductError:
        db.rollback()
        return jsonify({
            "success": False,
            "code": "stale_products",
            "error": "La selection a change pendant la suppression. Rechargez le plan; aucun produit n'a ete retire.",
        }), 409
    except Exception as exc:
        db.rollback()
        print(f"[Plan] Bulk delete rolled back: {exc}")
        return jsonify({
            "success": False,
            "error": "Suppression annulee: aucun produit n'a ete retire.",
        }), 500
    deleted_ids = [int(row["id"]) for row in rows]
    return jsonify({
        "success": True,
        "removed_products": removed_count,
        "deleted_product_ids": deleted_ids,
        "scope": scope,
    })


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
    if not valid_aisle_name(aisle):
        return jsonify({"error": "Nom d'allée invalide : lettres, chiffres, espaces, points et tirets seulement."}), 400
    db = get_db()
    exists = db.execute("SELECT aisle FROM aisle_layouts WHERE aisle=?", (aisle,)).fetchone()
    if exists:
        return jsonify({"error": f"L’allée {aisle} existe déjà."}), 409
    now = utc_now_iso()
    order_row = db.execute(
        "SELECT COALESCE(MAX(sort_order), 0) AS max_order FROM aisle_layouts"
    ).fetchone()
    sort_order = int(order_row["max_order"] or 0) + 1
    db.execute(
        """
        INSERT INTO aisle_layouts (aisle, sort_order, max_section, max_shelf, max_position, config_json, enabled, modified_by, modified_at)
        VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?)
        """,
        (aisle, sort_order, max_section, max_shelf, max_position, json.dumps(config), username, now),
    )
    db.commit()
    return jsonify({"success": True, "modified_at": now, "sort_order": sort_order})


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
    current = get_layout_row(db, aisle)
    if not current:
        return jsonify({"error": "Allée non trouvée."}), 404
    if "expected_modified_at" in data:
        expected = str(data.get("expected_modified_at") or "")
        actual = str(current["modified_at"] or "")
        if expected != actual:
            return jsonify({
                "success": False,
                "code": "stale_layout",
                "error": (
                    "Sauvegarde refusée: ce plan a été modifié depuis son ouverture. "
                    "Rechargez-le avant de continuer afin de ne pas écraser les changements récents."
                ),
                "current_modified_at": actual,
            }), 409
    protected = products_outside_layout(db, aisle, config)
    if protected:
        # Autosave sends the entire aisle configuration. A stale phone snapshot
        # once collapsed sections to one tablette and this endpoint silently
        # deleted every product outside that stale shape. Generic saves are now
        # strictly non-destructive; explicit remove-section/remove-shelf routes
        # remain the only operations allowed to remove placements.
        return jsonify({
            "success": False,
            "error": (
                f"Sauvegarde refusée: cette structure masquerait ou supprimerait "
                f"{len(protected)} produit(s). Rechargez le plan; utilisez les boutons "
                "Supprimer uniquement pour une suppression volontaire."
            ),
            "protected_products": len(protected),
        }), 409
    now = utc_now_iso()
    result = db.execute(
        """
        UPDATE aisle_layouts
        SET max_section=?, max_shelf=?, max_position=?, config_json=?, enabled=?, modified_by=?, modified_at=?
        WHERE aisle=?
        """,
        (max_section, max_shelf, max_position, json.dumps(config), enabled, username, now, aisle),
    )
    db.commit()
    if result.rowcount == 0:
        return jsonify({"error": "Allée non trouvée."}), 404
    return jsonify({"success": True, "removed_products": 0, "modified_at": now})


@layout_bp.route("/api/layout/aisles/<aisle>", methods=["DELETE"])
def delete_layout_aisle(aisle):
    username, error = require_editor()
    if error:
        return error
    db = get_db()
    row = get_layout_row(db, aisle)
    if not row:
        return jsonify({"error": "Allée non trouvée."}), 404
    data = request.get_json(silent=True) or {}
    stale = _stale_layout_response(row, data)
    if stale:
        return stale
    from routes.products import archive_and_delete_products
    try:
        _lock_layout_rows(db, [row])
        products = db.execute("SELECT * FROM products WHERE aisle=?", (aisle,)).fetchall()
        removed_products = archive_and_delete_products(
            db, products, username, utc_now_iso()
        )
        result = db.execute("DELETE FROM aisle_layouts WHERE aisle=?", (aisle,))
        if result.rowcount != 1:
            raise _StaleStructureError()
        db.commit()
    except _StaleStructureError:
        db.rollback()
        return jsonify({
            "success": False, "code": "stale_layout",
            "error": "Cette allée a changé. Rien n'a été supprimé; rechargez le plan.",
        }), 409
    except Exception as exc:
        db.rollback()
        print(f"[Plan] Aisle deletion rolled back: {exc}")
        return jsonify({
            "success": False,
            "error": "Suppression annulée: l'allée et ses produits sont restés intacts.",
        }), 500
    return jsonify({"success": True, "message": f"Allée {aisle} retirée par {username}. {removed_products} produit(s) supprime(s)."})


@layout_bp.route("/api/layout/structure/move-section", methods=["POST"])
def move_layout_section():
    username, error = require_editor()
    if error:
        return error
    data = request.get_json() or {}
    source = data.get("source") if isinstance(data.get("source"), dict) else {}
    target = data.get("target") if isinstance(data.get("target"), dict) else {}
    source_aisle = str(source.get("aisle", "")).strip()
    source_side = str(source.get("side", "")).strip()
    target_aisle = str(target.get("aisle", "")).strip()
    target_side = str(target.get("side", "")).strip()
    if source.get("index") is None or target.get("index") is None:
        return jsonify({"success": False, "error": "Destination de section invalide."}), 400
    source_index = clamp_non_negative_int(source.get("index"))
    target_boundary = clamp_non_negative_int(target.get("index"))
    if (
        not source_aisle or not target_aisle
        or source_side not in ("Gauche", "Droite")
        or target_side not in ("Gauche", "Droite")
    ):
        return jsonify({"success": False, "error": "Destination de section invalide."}), 400

    db = get_db()
    source_row = get_layout_row(db, source_aisle)
    target_row = get_layout_row(db, target_aisle)
    if not source_row or not target_row:
        return jsonify({"success": False, "error": "Allee introuvable."}), 404
    layout_rows = [source_row] if source_aisle == target_aisle else [source_row, target_row]
    now = utc_now_iso()
    try:
        _lock_layout_rows(db, layout_rows, data.get("expected_layouts"))
        source_config = normalize_layout_config(
            source_row["config_json"], source_row["max_section"],
            source_row["max_shelf"], source_row["max_position"],
        )
        target_config = source_config if source_aisle == target_aisle else normalize_layout_config(
            target_row["config_json"], target_row["max_section"],
            target_row["max_shelf"], target_row["max_position"],
        )
        source_sections = source_config["sides"][source_side]["sections"]
        target_sections = target_config["sides"][target_side]["sections"]
        if source_index >= len(source_sections):
            db.rollback()
            return jsonify({"success": False, "error": "Cette section n'existe plus."}), 409

        same_container = source_aisle == target_aisle and source_side == target_side
        clauses = ["(aisle=? AND side=?)"]
        params = [source_aisle, source_side]
        if not same_container:
            clauses.append("(aisle=? AND side=?)")
            params.extend([target_aisle, target_side])
        rows = db.execute(
            "SELECT id, aisle, side, section, shelf, position, modified_at "
            f"FROM products WHERE {' OR '.join(clauses)}",
            tuple(params),
        ).fetchall()

        if same_container:
            final_index = _final_index_from_boundary(
                source_index, target_boundary, len(source_sections)
            )
            moved_section = source_sections.pop(source_index)
            source_sections.insert(final_index, moved_section)
        else:
            final_index = max(0, min(target_boundary, len(target_sections)))
            moved_section = source_sections.pop(source_index)
            target_sections.insert(final_index, moved_section)

        def section_location(row):
            location = {
                field: str(row[field])
                for field in ("aisle", "side", "section", "shelf", "position")
            }
            try:
                old_index = int(str(row["section"])) - 1
            except (TypeError, ValueError):
                return location
            row_aisle, row_side = str(row["aisle"]), str(row["side"])
            if same_container:
                if row_aisle == source_aisle and row_side == source_side:
                    location["section"] = str(
                        _reordered_number(old_index, source_index, final_index) + 1
                    )
                return location
            if row_aisle == source_aisle and row_side == source_side:
                if old_index == source_index:
                    location.update({
                        "aisle": target_aisle,
                        "side": target_side,
                        "section": str(final_index + 1),
                    })
                elif old_index > source_index:
                    location["section"] = str(old_index)
            elif row_aisle == target_aisle and row_side == target_side:
                if old_index >= final_index:
                    location["section"] = str(old_index + 2)
            return location

        product_updates = _apply_structure_product_locations(
            db, rows, section_location, username, now
        )
        configs = {source_aisle: source_config, target_aisle: target_config}
        for aisle_key, config in configs.items():
            _persist_aisle_config(db, aisle_key, config, username, now)
        db.commit()
    except _StaleStructureError:
        db.rollback()
        return jsonify({
            "success": False, "code": "stale_layout",
            "error": "Le plan a change pendant le deplacement. Aucune section n'a ete deplacee.",
        }), 409
    except Exception as exc:
        db.rollback()
        print(f"[Plan] Section move rolled back: {exc}")
        return jsonify({
            "success": False,
            "error": "Deplacement annule: la section et ses produits sont restes en place.",
        }), 409 if isinstance(exc, DatabaseIntegrityError) else 500
    return jsonify({
        "success": True,
        "source": source,
        "target": {"aisle": target_aisle, "side": target_side, "index": final_index},
        "configs": _configs_response(configs),
        "layout_versions": {aisle_key: now for aisle_key in configs},
        "product_updates": product_updates,
    })


@layout_bp.route("/api/layout/structure/move-shelf", methods=["POST"])
def move_layout_shelf():
    username, error = require_editor()
    if error:
        return error
    data = request.get_json() or {}
    source = data.get("source") if isinstance(data.get("source"), dict) else {}
    target = data.get("target") if isinstance(data.get("target"), dict) else {}
    required = (
        source.get("aisle"), source.get("side"), source.get("section_index"), source.get("index"),
        target.get("aisle"), target.get("side"), target.get("section_index"), target.get("index"),
    )
    if any(value is None or str(value).strip() == "" for value in required):
        return jsonify({"success": False, "error": "Destination de tablette invalide."}), 400
    source_aisle = str(source["aisle"]).strip()
    source_side = str(source["side"]).strip()
    source_section_index = clamp_non_negative_int(source["section_index"])
    source_index = clamp_non_negative_int(source["index"])
    target_aisle = str(target["aisle"]).strip()
    target_side = str(target["side"]).strip()
    target_section_index = clamp_non_negative_int(target["section_index"])
    target_boundary = clamp_non_negative_int(target["index"])
    if source_side not in ("Gauche", "Droite") or target_side not in ("Gauche", "Droite"):
        return jsonify({"success": False, "error": "Les tablettes doivent rester dans une section du plan."}), 400

    db = get_db()
    source_row = get_layout_row(db, source_aisle)
    target_row = get_layout_row(db, target_aisle)
    if not source_row or not target_row:
        return jsonify({"success": False, "error": "Allee introuvable."}), 404
    layout_rows = [source_row] if source_aisle == target_aisle else [source_row, target_row]
    now = utc_now_iso()
    try:
        _lock_layout_rows(db, layout_rows, data.get("expected_layouts"))
        source_config = normalize_layout_config(
            source_row["config_json"], source_row["max_section"],
            source_row["max_shelf"], source_row["max_position"],
        )
        target_config = source_config if source_aisle == target_aisle else normalize_layout_config(
            target_row["config_json"], target_row["max_section"],
            target_row["max_shelf"], target_row["max_position"],
        )
        source_sections = source_config["sides"][source_side]["sections"]
        target_sections = target_config["sides"][target_side]["sections"]
        if source_section_index >= len(source_sections) or target_section_index >= len(target_sections):
            db.rollback()
            return jsonify({"success": False, "error": "La section de destination n'existe plus."}), 409
        source_section = source_sections[source_section_index]
        target_section = target_sections[target_section_index]
        if source_index >= len(source_section["shelves"]):
            db.rollback()
            return jsonify({"success": False, "error": "Cette tablette n'existe plus."}), 409

        same_container = (
            source_aisle == target_aisle and source_side == target_side
            and source_section_index == target_section_index
        )
        rows = _products_in_containers(db, [
            (source_aisle, source_side, str(source_section_index + 1)),
            (target_aisle, target_side, str(target_section_index + 1)),
        ])
        if same_container:
            final_index = _final_index_from_boundary(
                source_index, target_boundary, len(source_section["shelves"])
            )
        else:
            final_index = max(0, min(target_boundary, len(target_section["shelves"])))

        moved_capacity = source_section["shelves"].pop(source_index)
        source_labels = source_section.setdefault("labels", [])
        moved_label = source_labels.pop(source_index) if source_index < len(source_labels) else ""
        target_section["shelves"].insert(final_index, moved_capacity)
        target_labels = target_section.setdefault("labels", [])
        target_labels.insert(final_index, moved_label)

        def shelf_location(row):
            location = {
                field: str(row[field])
                for field in ("aisle", "side", "section", "shelf", "position")
            }
            try:
                old_index = int(str(row["shelf"])) - 1
            except (TypeError, ValueError):
                return location
            row_container = (
                str(row["aisle"]), str(row["side"]), str(row["section"])
            )
            source_container = (
                source_aisle, source_side, str(source_section_index + 1)
            )
            target_container = (
                target_aisle, target_side, str(target_section_index + 1)
            )
            if same_container:
                if row_container == source_container:
                    location["shelf"] = str(
                        _reordered_number(old_index, source_index, final_index) + 1
                    )
                return location
            if row_container == source_container:
                if old_index == source_index:
                    location.update({
                        "aisle": target_aisle,
                        "side": target_side,
                        "section": str(target_section_index + 1),
                        "shelf": str(final_index + 1),
                    })
                elif old_index > source_index:
                    location["shelf"] = str(old_index)
            elif row_container == target_container:
                if old_index >= final_index:
                    location["shelf"] = str(old_index + 2)
            return location

        product_updates = _apply_structure_product_locations(
            db, rows, shelf_location, username, now
        )
        configs = {source_aisle: source_config, target_aisle: target_config}
        for aisle_key, config in configs.items():
            _persist_aisle_config(db, aisle_key, config, username, now)
        db.commit()
    except _StaleStructureError:
        db.rollback()
        return jsonify({
            "success": False, "code": "stale_layout",
            "error": "Le plan a change pendant le deplacement. Aucune tablette n'a ete deplacee.",
        }), 409
    except Exception as exc:
        db.rollback()
        print(f"[Plan] Shelf move rolled back: {exc}")
        return jsonify({
            "success": False,
            "error": "Deplacement annule: la tablette et ses produits sont restes en place.",
        }), 409 if isinstance(exc, DatabaseIntegrityError) else 500
    return jsonify({
        "success": True,
        "source": source,
        "target": {
            "aisle": target_aisle, "side": target_side,
            "section_index": target_section_index, "index": final_index,
        },
        "configs": _configs_response(configs),
        "layout_versions": {aisle_key: now for aisle_key in configs},
        "product_updates": product_updates,
    })


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
    db = get_db()
    row = get_layout_row(db, aisle)
    if not row:
        return jsonify({"success": False, "error": "Allée introuvable."}), 404
    stale = _stale_layout_response(row, data)
    if stale:
        return stale
    now = utc_now_iso()
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
    db = get_db()
    row = get_layout_row(db, aisle)
    if not row:
        return jsonify({"success": False, "error": "Allée introuvable."}), 404
    stale = _stale_layout_response(row, data)
    if stale:
        return stale
    now = utc_now_iso()
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
    db = get_db()
    row = get_layout_row(db, aisle)
    if not row:
        return jsonify({"success": False, "error": "Allée introuvable."}), 404
    stale = _stale_layout_response(row, data)
    if stale:
        return stale
    now = utc_now_iso()
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
    # 1. archive products in the removed section/shelf, then remove them.
    removed_rows = db.execute(
        f"SELECT * FROM products WHERE {where} AND {field}=?",
        tuple(params + [str(removed)]),
    ).fetchall()
    from routes.products import archive_and_delete_products
    removed_count = archive_and_delete_products(db, removed_rows, username, now)
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
    """Build an atomic removal from the latest server-side structure.

    A phone can keep an old full-aisle snapshot open for hours. Trusting that
    snapshot here once made an explicit one-tablet removal collapse unrelated
    sections. The request now identifies only what to remove; the database is
    always the source of truth for everything else.
    """
    row = get_layout_row(db, aisle)
    if not row:
        return None, None
    return row, normalize_layout_config(
        row["config_json"], row["max_section"], row["max_shelf"], row["max_position"]
    )


def _stale_layout_response(row, data):
    if "expected_modified_at" not in data:
        return None
    expected = str(data.get("expected_modified_at") or "")
    actual = str(row["modified_at"] or "")
    if expected == actual:
        return None
    return jsonify({
        "success": False,
        "code": "stale_layout",
        "error": (
            "Action refusée: ce plan a été modifié depuis son ouverture. "
            "Rechargez-le afin de ne pas modifier la mauvaise section ou tablette."
        ),
        "current_modified_at": actual,
    }), 409


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
    row, config = _config_for_removal(db, aisle, data.get("config"))
    if config is None:
        return jsonify({"success": False, "error": "Allée non trouvée."}), 404
    stale = _stale_layout_response(row, data)
    if stale:
        return stale
    if not _remove_section_from_config(config, side, section):
        return jsonify({"success": False, "error": "Cette section n'existe plus."}), 409
    now = utc_now_iso()
    removed_products = _renumber_after_remove(db, username, now, aisle, side, "section", section)
    removed_products += remove_products_outside_layout(db, aisle, config, username, now)
    _persist_aisle_config(db, aisle, config, username, now)
    db.commit()
    return jsonify({
        "success": True, "config": config,
        "removed_products": removed_products, "modified_at": now,
    })


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
    row, config = _config_for_removal(db, aisle, data.get("config"))
    if config is None:
        return jsonify({"success": False, "error": "Allée non trouvée."}), 404
    stale = _stale_layout_response(row, data)
    if stale:
        return stale
    if not _remove_shelf_from_config(config, side, section, shelf):
        return jsonify({"success": False, "error": "Cette tablette n'existe plus."}), 409
    # Fixture sides (Façade A/B, présentoir façades) carry no meaningful section
    # value on their products — match their shelves across all sections.
    section_scope = section if side in ("Gauche", "Droite") else None
    now = utc_now_iso()
    removed_products = _renumber_after_remove(
        db, username, now, aisle, side, "shelf", shelf, section=section_scope
    )
    removed_products += remove_products_outside_layout(db, aisle, config, username, now)
    _persist_aisle_config(db, aisle, config, username, now)
    db.commit()
    return jsonify({
        "success": True, "config": config,
        "removed_products": removed_products, "modified_at": now,
    })


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
    if "expected_modified_at" in data:
        stale = _stale_layout_response(src_row, {
            "expected_modified_at": data.get("expected_modified_at")
        })
        if stale:
            return stale
    if "expected_target_modified_at" in data:
        stale = _stale_layout_response(tgt_row, {
            "expected_modified_at": data.get("expected_target_modified_at")
        })
        if stale:
            return stale
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
        return jsonify({
            "success": True, "target_aisle": str(aisle).strip(),
            "target_side": side, "target_section": final + 1,
            "modified_at": now,
        })

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
    return jsonify({
        "success": True, "target_aisle": target_aisle,
        "target_side": target_side, "target_section": final_number,
        "modified_at": now,
    })
