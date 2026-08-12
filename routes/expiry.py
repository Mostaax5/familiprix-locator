import json
import re
from datetime import date, datetime

from flask import Blueprint, jsonify, request

from auth import require_editor, side_display_label, utc_now_iso
from database import DatabaseIntegrityError, get_db
from product_data import exact_gtin_variants, gtin_identity_key
from routes.products import safe_http_url


expiry_bp = Blueprint("expiry", __name__)

_MAX_BOARD_ITEMS = 1000
_DEFAULT_STORE = "default"


def _clean_store_key(value):
    raw = str(value or "").strip().lower()[:80]
    cleaned = re.sub(r"[^a-z0-9_-]+", "-", raw).strip("-")
    return cleaned or _DEFAULT_STORE


def _clean_barcode(value):
    digits = re.sub(r"\D", "", str(value or ""))
    if not 6 <= len(digits) <= 14:
        return ""
    return digits


def _clean_initials(value):
    raw = " ".join(str(value or "").strip().upper().split())[:12]
    if not raw or not any(character.isalnum() for character in raw):
        return ""
    if any(not (character.isalnum() or character in " .-'") for character in raw):
        return ""
    return raw


def _parse_iso_date(value):
    raw = str(value or "").strip()
    try:
        return date.fromisoformat(raw)
    except (TypeError, ValueError):
        return None


def _today():
    try:
        from zoneinfo import ZoneInfo

        return datetime.now(ZoneInfo("America/Toronto")).date()
    except Exception:
        return date.today()


def _row_dict(row):
    return dict(row) if row else None


def _json_locations(value):
    if isinstance(value, list):
        return value
    try:
        parsed = json.loads(str(value or "[]"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
    return parsed if isinstance(parsed, list) else []


def _urgency(expiry_date, today=None):
    current_date = today or _today()
    parsed = _parse_iso_date(expiry_date)
    if not parsed:
        return "unknown", None
    days_remaining = (parsed - current_date).days
    if days_remaining < 0:
        return "expired", days_remaining
    if days_remaining <= 7:
        return "critical", days_remaining
    if days_remaining <= 30:
        return "soon", days_remaining
    if days_remaining <= 60:
        return "watch", days_remaining
    return "later", days_remaining


def _location_from_row(row):
    aisle = str(row.get("aisle", "") or "").strip()
    side = str(row.get("side", "") or "").strip()
    section = str(row.get("section", "1") or "1").strip()
    shelf = str(row.get("shelf", "") or "").strip()
    position = str(row.get("position", "") or "").strip()
    if not any((aisle, side, shelf, position)):
        return None
    display_side = side_display_label(side)
    return {
        "aisle": aisle,
        "side": side,
        "side_label": display_side,
        "section": section,
        "shelf": shelf,
        "position": position,
        "label": (
            f"Allée {aisle} · {display_side} · "
            f"S{section} T{shelf} P{position}"
        ),
    }


def _product_from_rows(rows, barcode):
    items = [dict(row) for row in rows]
    if not items:
        return None
    items.sort(
        key=lambda item: (
            -int(item.get("in_stock", 1) or 0),
            -int(item.get("is_plano", 0) or 0),
            int(item.get("id", 0) or 0),
        )
    )
    primary = items[0]
    seen_locations = set()
    locations = []
    for item in items:
        location = _location_from_row(item)
        if not location:
            continue
        key = (
            location["aisle"], location["side"], location["section"],
            location["shelf"], location["position"],
        )
        if key in seen_locations:
            continue
        seen_locations.add(key)
        locations.append(location)
    return {
        "found": True,
        "in_plan": True,
        "barcode": str(primary.get("barcode", "") or barcode),
        "product_code": str(primary.get("product_code", "") or ""),
        "name": str(primary.get("name", "") or "Produit sans nom"),
        "brand": str(primary.get("brand", "") or ""),
        "description": str(primary.get("description", "") or ""),
        "image_url": safe_http_url(primary.get("image_url", "")),
        "locations": locations,
    }


def _resolve_product(db, barcode):
    gtin_key = gtin_identity_key(barcode)
    candidates = exact_gtin_variants(barcode) or [barcode]
    placeholders = ",".join("?" for _ in candidates)
    params = list(candidates)
    where = f"barcode IN ({placeholders})"
    if gtin_key:
        where += " OR gtin_key=?"
        params.append(gtin_key)
    rows = db.execute(
        f"""SELECT id, name, brand, description, image_url, barcode,
                   product_code, aisle, side, section, shelf, position,
                   in_stock, is_plano
              FROM products
             WHERE {where}
             ORDER BY in_stock DESC, is_plano DESC, id""",
        tuple(params),
    ).fetchall()
    product = _product_from_rows(rows, barcode)
    if product:
        return product

    reference_params = list(candidates)
    reference_where = f"barcode IN ({placeholders})"
    if gtin_key:
        reference_where += " OR gtin_key=?"
        reference_params.append(gtin_key)
    reference = db.execute(
        f"""SELECT name, brand, description, image_url, barcode, product_code
              FROM product_reference
             WHERE {reference_where}
             ORDER BY CASE WHEN barcode=? THEN 0 ELSE 1 END
             LIMIT 1""",
        tuple(reference_params + [barcode]),
    ).fetchone()
    if reference:
        item = dict(reference)
        return {
            "found": True,
            "in_plan": False,
            "barcode": str(item.get("barcode", "") or barcode),
            "product_code": str(item.get("product_code", "") or ""),
            "name": str(item.get("name", "") or "Produit sans nom"),
            "brand": str(item.get("brand", "") or ""),
            "description": str(item.get("description", "") or ""),
            "image_url": safe_http_url(item.get("image_url", "")),
            "locations": [],
        }

    return {
        "found": False,
        "in_plan": False,
        "barcode": barcode,
        "product_code": "",
        "name": f"Produit à identifier · {barcode}",
        "brand": "",
        "description": "",
        "image_url": "",
        "locations": [],
    }


def _status_payload(row, product=None, today=None):
    item = dict(row)
    live_product = product or {}
    live_identity = live_product if live_product.get("found") else {}
    urgency, days_remaining = _urgency(
        item.get("earliest_expiry_date", ""), today=today
    )
    locations = live_identity.get("locations") or _json_locations(
        item.get("locations_json", "[]")
    )
    name = str(live_identity.get("name") or item.get("product_name") or "Produit sans nom")
    brand = str(live_identity.get("brand") or item.get("brand") or "")
    image_url = safe_http_url(
        live_identity.get("image_url") or item.get("image_url") or ""
    )
    product_code = str(
        live_identity.get("product_code") or item.get("product_code") or ""
    )
    return {
        "store_key": str(item.get("store_key", "") or ""),
        "gtin_key": str(item.get("gtin_key", "") or ""),
        "barcode": str(live_identity.get("barcode") or item.get("barcode") or ""),
        "product_name": name,
        "brand": brand,
        "image_url": image_url,
        "product_code": product_code,
        "earliest_expiry_date": str(item.get("earliest_expiry_date", "") or ""),
        "checked_at": str(item.get("checked_at", "") or ""),
        "checked_by": str(item.get("checked_by", "") or ""),
        "recorded_by": str(item.get("recorded_by", "") or ""),
        "note": str(item.get("note", "") or ""),
        "locations": locations,
        "revision": int(item.get("revision", 1) or 1),
        "urgency": urgency,
        "days_remaining": days_remaining,
        "in_plan": bool(live_identity.get("in_plan", bool(locations))),
    }


def _event_payload(row):
    item = dict(row)
    return {
        "id": int(item.get("id", 0) or 0),
        "action": str(item.get("action", "") or ""),
        "previous_expiry_date": str(item.get("previous_expiry_date", "") or ""),
        "expiry_date": str(item.get("expiry_date", "") or ""),
        "initials": str(item.get("initials", "") or ""),
        "recorded_by": str(item.get("recorded_by", "") or ""),
        "note": str(item.get("note", "") or ""),
        "created_at": str(item.get("created_at", "") or ""),
    }


def _current_status(db, store_key, gtin_key):
    return db.execute(
        "SELECT * FROM product_expiry_status WHERE store_key=? AND gtin_key=?",
        (store_key, gtin_key),
    ).fetchone()


def _history(db, store_key, gtin_key, limit=20):
    rows = db.execute(
        """SELECT id, action, previous_expiry_date, expiry_date, initials,
                  recorded_by, note, created_at
             FROM product_expiry_events
            WHERE store_key=? AND gtin_key=?
            ORDER BY id DESC
            LIMIT ?""",
        (store_key, gtin_key, limit),
    ).fetchall()
    return [_event_payload(row) for row in rows]


def _live_products_by_key(db, gtin_keys):
    keys = sorted({str(key or "").strip() for key in gtin_keys if str(key or "").strip()})
    grouped = {}
    for start in range(0, len(keys), 300):
        chunk = keys[start:start + 300]
        placeholders = ",".join("?" for _ in chunk)
        rows = db.execute(
            f"""SELECT id, name, brand, description, image_url, barcode,
                       product_code, aisle, side, section, shelf, position,
                       in_stock, is_plano, gtin_key
                  FROM products
                 WHERE gtin_key IN ({placeholders})
                 ORDER BY in_stock DESC, is_plano DESC, id""",
            tuple(chunk),
        ).fetchall()
        for row in rows:
            item = dict(row)
            grouped.setdefault(str(item.get("gtin_key", "") or ""), []).append(item)
    return {
        key: _product_from_rows(rows, rows[0].get("barcode", ""))
        for key, rows in grouped.items()
        if rows
    }


@expiry_bp.route("/api/expiry", methods=["GET"])
def get_expiry_board():
    _username, error = require_editor()
    if error:
        return error
    store_key = _clean_store_key(request.args.get("store"))
    try:
        limit = min(max(int(request.args.get("limit", "500")), 1), _MAX_BOARD_ITEMS)
    except (TypeError, ValueError):
        limit = 500
    query = str(request.args.get("q", "") or "").strip().lower()[:120]
    db = get_db()
    params = [store_key]
    where = "store_key=?"
    if query:
        where += (
            " AND (LOWER(product_name) LIKE ? OR LOWER(brand) LIKE ? "
            "OR barcode LIKE ? OR product_code LIKE ?)"
        )
        wildcard = f"%{query}%"
        params.extend((wildcard, wildcard, wildcard, wildcard))
    params.append(limit)
    rows = db.execute(
        f"""SELECT * FROM product_expiry_status
             WHERE {where}
             ORDER BY earliest_expiry_date, product_name
             LIMIT ?""",
        tuple(params),
    ).fetchall()
    live_products = _live_products_by_key(
        db, [dict(row).get("gtin_key", "") for row in rows]
    )
    today = _today()
    items = [
        _status_payload(
            row,
            product=live_products.get(str(dict(row).get("gtin_key", "") or "")),
            today=today,
        )
        for row in rows
    ]
    summary = {
        "total": len(items),
        "expired": sum(item["urgency"] == "expired" for item in items),
        "critical": sum(item["urgency"] == "critical" for item in items),
        "soon": sum(item["urgency"] == "soon" for item in items),
        "watch": sum(item["urgency"] == "watch" for item in items),
        "later": sum(item["urgency"] == "later" for item in items),
    }
    return jsonify({"items": items, "summary": summary, "today": today.isoformat()})


@expiry_bp.route("/api/expiry/product/<barcode>", methods=["GET"])
def get_expiry_product(barcode):
    _username, error = require_editor()
    if error:
        return error
    normalized = _clean_barcode(barcode)
    if not normalized:
        return jsonify({"success": False, "error": "Code-barres invalide."}), 400
    store_key = _clean_store_key(request.args.get("store"))
    gtin_key = gtin_identity_key(normalized)
    db = get_db()
    product = _resolve_product(db, normalized)
    current = _current_status(db, store_key, gtin_key)
    if current and not product.get("found"):
        saved = dict(current)
        product.update({
            "name": str(saved.get("product_name", "") or product.get("name", "")),
            "brand": str(saved.get("brand", "") or ""),
            "image_url": safe_http_url(saved.get("image_url", "")),
            "product_code": str(saved.get("product_code", "") or ""),
            "locations": _json_locations(saved.get("locations_json", "[]")),
        })
    return jsonify({
        "success": True,
        "product": product,
        "current": _status_payload(current, product=product) if current else None,
        "history": _history(db, store_key, gtin_key),
    })


@expiry_bp.route("/api/expiry", methods=["POST"])
def set_expiry_date():
    username, error = require_editor()
    if error:
        return error
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({"success": False, "error": "Demande invalide."}), 400
    barcode = _clean_barcode(data.get("barcode"))
    expiry_date = _parse_iso_date(data.get("earliest_expiry_date"))
    initials = _clean_initials(data.get("initials"))
    note = str(data.get("note", "") or "").strip()[:300]
    if not barcode:
        return jsonify({"success": False, "error": "Code-barres invalide."}), 400
    if not expiry_date:
        return jsonify({"success": False, "error": "Choisissez une date valide."}), 400
    if not initials:
        return jsonify({"success": False, "error": "Entrez vos initiales."}), 400
    store_key = _clean_store_key(data.get("store"))
    gtin_key = gtin_identity_key(barcode)
    try:
        expected_revision = int(data.get("expected_revision", 0) or 0)
    except (TypeError, ValueError):
        expected_revision = 0

    db = get_db()
    product = _resolve_product(db, barcode)
    current = _current_status(db, store_key, gtin_key)
    current_values = _row_dict(current) or {}
    current_revision = int(current_values.get("revision", 0) or 0)
    if expected_revision and expected_revision != current_revision:
        return jsonify({
            "success": False,
            "code": "expiry_conflict",
            "error": "Cette date a été modifiée sur un autre appareil. Rechargez le produit.",
            "current": _status_payload(current, product=product) if current else None,
        }), 409

    now = utc_now_iso()
    expiry_iso = expiry_date.isoformat()
    locations_json = json.dumps(product.get("locations", []), ensure_ascii=False)
    action = "created"
    previous_date = ""
    try:
        if current:
            previous_date = str(current_values.get("earliest_expiry_date", "") or "")
            action = "confirmed" if previous_date == expiry_iso else "updated"
            result = db.execute(
                """UPDATE product_expiry_status
                      SET barcode=?, product_name=?, brand=?, image_url=?,
                          product_code=?, earliest_expiry_date=?, checked_at=?,
                          checked_by=?, recorded_by=?, note=?, locations_json=?,
                          revision=revision+1
                    WHERE store_key=? AND gtin_key=? AND revision=?""",
                (
                    product.get("barcode") or barcode,
                    product.get("name", ""), product.get("brand", ""),
                    product.get("image_url", ""), product.get("product_code", ""),
                    expiry_iso, now, initials, username, note, locations_json,
                    store_key, gtin_key, current_revision,
                ),
            )
            if result.rowcount != 1:
                db.rollback()
                return jsonify({
                    "success": False,
                    "code": "expiry_conflict",
                    "error": "Cette date vient d'être modifiée. Rechargez le produit.",
                }), 409
        else:
            db.execute(
                """INSERT INTO product_expiry_status
                   (store_key, gtin_key, barcode, product_name, brand, image_url,
                    product_code, earliest_expiry_date, checked_at, checked_by,
                    recorded_by, note, locations_json, revision)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,1)""",
                (
                    store_key, gtin_key, product.get("barcode") or barcode,
                    product.get("name", ""), product.get("brand", ""),
                    product.get("image_url", ""), product.get("product_code", ""),
                    expiry_iso, now, initials, username, note, locations_json,
                ),
            )
        db.execute(
            """INSERT INTO product_expiry_events
               (store_key, gtin_key, barcode, action, previous_expiry_date,
                expiry_date, product_name, initials, recorded_by, note, created_at)
               VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
            (
                store_key, gtin_key, product.get("barcode") or barcode, action,
                previous_date, expiry_iso, product.get("name", ""), initials,
                username, note, now,
            ),
        )
        db.commit()
    except DatabaseIntegrityError:
        db.rollback()
        return jsonify({
            "success": False,
            "code": "expiry_conflict",
            "error": "Ce produit vient d'être enregistré ailleurs. Rechargez-le.",
        }), 409

    from routes.gist import _schedule_gist_backup
    _schedule_gist_backup(db)

    saved = _current_status(db, store_key, gtin_key)
    return jsonify({
        "success": True,
        "action": action,
        "product": product,
        "current": _status_payload(saved, product=product),
    })


@expiry_bp.route("/api/expiry/<barcode>", methods=["DELETE"])
def clear_expiry_date(barcode):
    username, error = require_editor()
    if error:
        return error
    normalized = _clean_barcode(barcode)
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({"success": False, "error": "Demande invalide."}), 400
    initials = _clean_initials(data.get("initials"))
    note = str(data.get("note", "") or "").strip()[:300]
    if not normalized:
        return jsonify({"success": False, "error": "Code-barres invalide."}), 400
    if not initials:
        return jsonify({"success": False, "error": "Entrez vos initiales."}), 400
    store_key = _clean_store_key(data.get("store") or request.args.get("store"))
    gtin_key = gtin_identity_key(normalized)
    try:
        expected_revision = int(data.get("expected_revision", 0) or 0)
    except (TypeError, ValueError):
        expected_revision = 0
    db = get_db()
    current = _current_status(db, store_key, gtin_key)
    if not current:
        return jsonify({"success": False, "error": "Aucune date active pour ce produit."}), 404
    values = dict(current)
    current_revision = int(values.get("revision", 1) or 1)
    if expected_revision and expected_revision != current_revision:
        return jsonify({
            "success": False,
            "code": "expiry_conflict",
            "error": "Cette date a été modifiée sur un autre appareil. Rechargez le produit.",
            "current": _status_payload(current),
        }), 409
    result = db.execute(
        """DELETE FROM product_expiry_status
            WHERE store_key=? AND gtin_key=? AND revision=?""",
        (store_key, gtin_key, current_revision),
    )
    if result.rowcount != 1:
        db.rollback()
        return jsonify({
            "success": False,
            "code": "expiry_conflict",
            "error": "Cette date vient d'être modifiée. Rechargez le produit.",
        }), 409
    now = utc_now_iso()
    db.execute(
        """INSERT INTO product_expiry_events
           (store_key, gtin_key, barcode, action, previous_expiry_date,
            expiry_date, product_name, initials, recorded_by, note, created_at)
           VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
        (
            store_key, gtin_key, str(values.get("barcode", "") or normalized),
            "cleared", str(values.get("earliest_expiry_date", "") or ""), "",
            str(values.get("product_name", "") or ""), initials, username, note, now,
        ),
    )
    db.commit()
    from routes.gist import _schedule_gist_backup
    _schedule_gist_backup(db)
    return jsonify({"success": True, "cleared": True})
