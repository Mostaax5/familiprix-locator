import json
import os
import threading
from datetime import datetime, timezone
from urllib.parse import urlparse
from urllib.request import HTTPRedirectHandler, Request, build_opener
from flask import Blueprint, current_app, request, jsonify
from database import get_db
from auth import require_editor, utc_now_iso
from routes.layout import layout_metrics, normalize_layout_config, valid_aisle_name
from routes.products import product_payload_error, safe_http_url
from product_backup import (
    PRODUCT_DATA_TABLE_COLUMNS,
    build_product_data_backup,
    restore_product_backup_row,
    restore_product_data_backup,
)

gist_bp = Blueprint("gist", __name__)

GITHUB_TOKEN  = os.environ.get("GITHUB_TOKEN",  "").strip()
GITHUB_GIST_ID = os.environ.get("GITHUB_GIST_ID", "").strip()
_GIST_FILENAME = "familiprix-backup.json"
_MAX_GIST_BYTES = 32 * 1024 * 1024
_MAX_GIST_PRODUCTS = 100_000
_MAX_GIST_LAYOUTS = 1_000
_GITHUB_HTTPS_HOSTS = {
    "api.github.com", "gist.githubusercontent.com", "raw.githubusercontent.com",
}


class _RejectRedirects(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _github_urlopen(request_obj, timeout):
    parsed = urlparse(request_obj.full_url)
    if (
        parsed.scheme != "https"
        or parsed.hostname not in _GITHUB_HTTPS_HOSTS
        or parsed.username
        or parsed.password
    ):
        raise ValueError("Unsafe GitHub URL")
    return build_opener(_RejectRedirects()).open(request_obj, timeout=timeout)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _as_flag(value, default):
    """Coerce a backed-up 0/1 flag to int, tolerating missing/blank values."""
    if value in ("", None):
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _bounded_int(value, default, minimum, maximum):
    try:
        return min(max(int(value), minimum), maximum)
    except (TypeError, ValueError, OverflowError):
        return default


def _normalized_backup_layout(layout):
    if not isinstance(layout, dict):
        return None
    aisle = str(layout.get("aisle", "")).strip()
    if not valid_aisle_name(aisle):
        return None
    config = normalize_layout_config(
        layout.get("config_json"), layout.get("max_section", "1"),
        layout.get("max_shelf", "5"), layout.get("max_position", "8"),
    )
    max_section, max_shelf, max_position = layout_metrics(config)
    return {
        "aisle": aisle,
        "max_section": max_section,
        "max_shelf": max_shelf,
        "max_position": max_position,
        "config_json": json.dumps(config, ensure_ascii=False, separators=(",", ":")),
        "enabled": 1 if layout.get("enabled", 1) else 0,
    }


def _normalized_backup_product(product, now):
    if not isinstance(product, dict):
        return None
    cleaned = dict(product)
    cleaned["image_url"] = safe_http_url(cleaned.get("image_url"))
    cleaned["source_url"] = safe_http_url(cleaned.get("source_url"))
    cleaned["primary_source_url"] = safe_http_url(cleaned.get("primary_source_url"))
    if product_payload_error(cleaned):
        return None
    for key in (
        "name", "brand", "description", "search_terms", "usage_notes",
        "alternative_suggestions", "barcode", "product_code", "aisle", "side",
        "section", "shelf", "position", "underneath_label",
    ):
        cleaned[key] = str(cleaned.get(key, "") or "").strip()
    cleaned["section"] = cleaned["section"] or "1"
    if not all(cleaned.get(key) for key in ("name", "aisle", "side", "shelf", "position")):
        return None
    if not valid_aisle_name(cleaned["aisle"]):
        return None
    cleaned["facings"] = _bounded_int(cleaned.get("facings"), 1, 1, 1000)
    for key, default in (("is_plano", 0), ("in_stock", 1), ("flipped_label", 0)):
        cleaned[key] = 1 if _as_flag(cleaned.get(key), default) else 0
    cleaned["created_at"] = str(cleaned.get("created_at") or now)[:64]
    return cleaned


def _build_backup_payload(db):
    products = [dict(p) for p in db.execute("SELECT * FROM products ORDER BY aisle, side, section, shelf, position").fetchall()]
    layouts  = [dict(r) for r in db.execute("SELECT * FROM aisle_layouts ORDER BY aisle").fetchall()]
    return {
        "export_version": 2,
        "exported_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "products": products,
        "aisle_layouts": layouts,
        "product_data": build_product_data_backup(db),
    }


def _gist_file_content(file_info):
    """Full content of a gist file. GitHub's API TRUNCATES `content` above ~1 MB
    (a store of ~1000 products is right at that size) — a truncated backup would
    parse as broken JSON and the restore would silently fail. When flagged
    truncated, fetch the raw_url which always returns the whole file."""
    if not file_info:
        return None
    if file_info.get("truncated") and file_info.get("raw_url"):
        raw_url = str(file_info["raw_url"])
        parsed = urlparse(raw_url)
        if parsed.scheme != "https" or parsed.hostname not in {
            "gist.githubusercontent.com", "raw.githubusercontent.com",
        } or parsed.username or parsed.password:
            raise ValueError("Unsafe Gist raw URL")
        req = Request(
            raw_url,
            headers={"Authorization": f"token {GITHUB_TOKEN}",
                     "X-GitHub-Api-Version": "2022-11-28"},
        )
        with _github_urlopen(req, timeout=30) as resp:
            raw = resp.read(_MAX_GIST_BYTES + 1)
            if len(raw) > _MAX_GIST_BYTES:
                raise ValueError("Gist backup is too large")
            return raw.decode("utf-8")
    content = file_info.get("content")
    if content is not None and len(str(content).encode("utf-8")) > _MAX_GIST_BYTES:
        raise ValueError("Gist backup is too large")
    return content


def _valid_backup_payload(payload):
    if not (
        isinstance(payload, dict)
        and payload.get("export_version") in {1, 2}
        and isinstance(payload.get("products"), list)
        and isinstance(payload.get("aisle_layouts"), list)
        and len(payload["products"]) <= _MAX_GIST_PRODUCTS
        and len(payload["aisle_layouts"]) <= _MAX_GIST_LAYOUTS
    ):
        return False
    if payload.get("export_version") == 1:
        return True
    product_data = payload.get("product_data")
    if not isinstance(product_data, dict):
        return False
    return all(
        isinstance(product_data.get(table, []), list)
        and len(product_data.get(table, [])) <= 250_000
        for table in PRODUCT_DATA_TABLE_COLUMNS
    )


def _push_to_gist(payload):
    if not GITHUB_TOKEN:
        return False, "GITHUB_TOKEN non configure"
    # Compact JSON (no indentation): pretty-printing doubled the size and pushed
    # the backup toward GitHub's 1 MB API-read threshold (see _gist_file_content).
    content = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    if len(content.encode("utf-8")) > _MAX_GIST_BYTES:
        return False, "La sauvegarde est trop volumineuse."
    body = json.dumps({
        "description": "Familiprix Locator - sauvegarde automatique",
        "public": False,
        "files": {_GIST_FILENAME: {"content": content}},
    }).encode()
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Content-Type": "application/json",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if GITHUB_GIST_ID:
        req = Request(f"https://api.github.com/gists/{GITHUB_GIST_ID}", data=body, headers=headers, method="PATCH")
    else:
        req = Request("https://api.github.com/gists", data=body, headers=headers, method="POST")
    try:
        with _github_urlopen(req, timeout=15) as resp:
            result = json.loads(resp.read())
            gist_id = result.get("id", "")
            if not GITHUB_GIST_ID:
                print(f"[Gist] Nouveau gist cree: {gist_id} — ajoutez GITHUB_GIST_ID={gist_id} dans vos variables Render")
            return True, gist_id
    except Exception as exc:
        current_app.logger.error("Gist backup failed: %s: %s", type(exc).__name__, exc)
        return False, "La sauvegarde distante a echoue. Reessayez plus tard."


# Debounced backup: during a scanning session every add/delete used to build the
# FULL ~1000-product payload synchronously inside the request AND push a gist per
# scan. Now the mutation just arms a 60s timer; when it fires, one background
# thread builds the payload with its own connection and pushes the LATEST state.
_BACKUP_DELAY_S = 60.0
_backup_timer_lock = threading.Lock()
_backup_timer = None


def _schedule_gist_backup(_db=None):
    """Arm (or leave armed) the debounced backup. `_db` is accepted for call-site
    compatibility but unused — the payload is built later, off the request path."""
    global _backup_timer
    if not GITHUB_TOKEN:
        return
    with _backup_timer_lock:
        if _backup_timer is not None:
            return   # one push already pending — it will capture this change too
        timer = threading.Timer(_BACKUP_DELAY_S, _run_scheduled_backup)
        timer.daemon = True
        _backup_timer = timer
        timer.start()


def _run_scheduled_backup():
    global _backup_timer
    with _backup_timer_lock:
        _backup_timer = None
    try:
        from database import connect_db
        db = connect_db()
        try:
            payload = _build_backup_payload(db)
        finally:
            db.close()
        # SAFETY: never let the AUTOMATIC backup overwrite a good gist with a
        # nearly-empty database (fresh/replaced Postgres, restore not run yet…).
        # The gist may be the only surviving copy of the store at that moment.
        # A deliberate wipe can still be backed up manually via /api/gist/backup.
        if len(payload.get("products") or []) < 5:
            print("[Gist] Sauvegarde automatique IGNORÉE: base quasi vide "
                  f"({len(payload.get('products') or [])} produits) — protection de la sauvegarde existante.")
            return
        _push_to_gist(payload)
    except Exception as exc:
        print(f"[Gist] Sauvegarde planifiée échouée: {exc}")


def _restore_from_gist_if_empty():
    if not GITHUB_TOKEN or not GITHUB_GIST_ID:
        return
    from database import connect_db as _connect_db
    try:
        db = _connect_db()
        count_row = db.execute("SELECT COUNT(*) AS n FROM products").fetchone()
        n = count_row["n"] if isinstance(count_row, dict) else count_row[0]
        if n > 0:
            db.close()
            return
        req = Request(
            f"https://api.github.com/gists/{GITHUB_GIST_ID}",
            headers={
                "Authorization": f"token {GITHUB_TOKEN}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
        with _github_urlopen(req, timeout=15) as resp:
            gist = json.loads(resp.read())
        file_info = gist.get("files", {}).get(_GIST_FILENAME)
        content = _gist_file_content(file_info)
        if not content:
            db.close()
            return
        payload = json.loads(content)
        if not _valid_backup_payload(payload):
            db.close()
            return
        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        for raw_layout in (payload.get("aisle_layouts") or []):
            layout = _normalized_backup_layout(raw_layout)
            if not layout:
                continue
            aisle = layout["aisle"]
            db.execute(
                """
                INSERT INTO aisle_layouts (aisle, max_section, max_shelf, max_position, config_json, enabled, modified_by, modified_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(aisle) DO UPDATE SET
                    max_section=excluded.max_section, max_shelf=excluded.max_shelf,
                    max_position=excluded.max_position, config_json=excluded.config_json,
                    enabled=excluded.enabled, modified_by=excluded.modified_by, modified_at=excluded.modified_at
                """,
                (aisle, layout["max_section"], layout["max_shelf"],
                 layout["max_position"], layout["config_json"],
                 layout["enabled"], "gist-restore", now),
            )
        imported = 0
        product_id_map = {}
        for raw_product in (payload.get("products") or []):
            p = _normalized_backup_product(raw_product, now)
            if not p:
                continue
            restored_id = restore_product_backup_row(db, p, "gist-restore", now)
            if not restored_id:
                continue
            imported += 1
            try:
                old_id = int(raw_product.get("id"))
            except (TypeError, ValueError, OverflowError):
                old_id = 0
            if old_id:
                product_id_map[old_id] = restored_id
        data_result = restore_product_data_backup(
            db,
            payload.get("product_data"),
            product_id_map,
        )
        db.commit()
        db.close()
        print(f"[Gist] Base de données restauree automatiquement ({imported} produits)")
    except Exception as exc:
        print(f"[Gist] Restauration automatique echouee: {exc}")


# ── Routes ─────────────────────────────────────────────────────────────────────

@gist_bp.route("/api/gist/status", methods=["GET"])
def gist_status():
    return jsonify({
        "configured": bool(GITHUB_TOKEN and GITHUB_GIST_ID),
        "has_token": bool(GITHUB_TOKEN),
        "has_gist_id": bool(GITHUB_GIST_ID),
        "gist_url": f"https://gist.github.com/{GITHUB_GIST_ID}" if GITHUB_GIST_ID else None,
    })


@gist_bp.route("/api/gist/backup", methods=["POST"])
def gist_backup_now():
    username, error = require_editor()
    if error:
        return error
    if not GITHUB_TOKEN:
        return jsonify({"success": False, "error": "GITHUB_TOKEN non configure sur le serveur."}), 400
    db = get_db()
    payload = _build_backup_payload(db)
    ok, result = _push_to_gist(payload)
    if ok:
        return jsonify({"success": True, "gist_id": result, "gist_url": f"https://gist.github.com/{result}"})
    return jsonify({"success": False, "error": result}), 500


@gist_bp.route("/api/gist/restore", methods=["POST"])
def gist_restore_now():
    username, error = require_editor()
    if error:
        return error
    if not GITHUB_TOKEN or not GITHUB_GIST_ID:
        return jsonify({"success": False, "error": "GITHUB_TOKEN ou GITHUB_GIST_ID non configure."}), 400
    try:
        req = Request(
            f"https://api.github.com/gists/{GITHUB_GIST_ID}",
            headers={
                "Authorization": f"token {GITHUB_TOKEN}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
        with _github_urlopen(req, timeout=15) as resp:
            gist = json.loads(resp.read())
        file_info = gist.get("files", {}).get(_GIST_FILENAME)
        content = _gist_file_content(file_info)
        if not content:
            return jsonify({"success": False, "error": "Fichier de sauvegarde introuvable dans le gist."}), 404
        payload = json.loads(content)
    except Exception as exc:
        current_app.logger.error("Gist restore read failed: %s: %s", type(exc).__name__, exc)
        return jsonify({
            "success": False,
            "error": "Impossible de lire la sauvegarde distante. Reessayez plus tard.",
        }), 500
    if not _valid_backup_payload(payload):
        return jsonify({"success": False, "error": "Format de sauvegarde non reconnu."}), 400
    db = get_db()
    imported_layouts = 0
    imported_products = 0
    skipped_products = 0
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    for raw_layout in (payload.get("aisle_layouts") or []):
        layout = _normalized_backup_layout(raw_layout)
        if not layout:
            continue
        aisle = layout["aisle"]
        db.execute(
            """
            INSERT INTO aisle_layouts (aisle, max_section, max_shelf, max_position, config_json, enabled, modified_by, modified_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(aisle) DO UPDATE SET
                max_section=excluded.max_section, max_shelf=excluded.max_shelf,
                max_position=excluded.max_position, config_json=excluded.config_json,
                enabled=excluded.enabled, modified_by=excluded.modified_by, modified_at=excluded.modified_at
            """,
            (aisle, layout["max_section"], layout["max_shelf"],
             layout["max_position"], layout["config_json"],
             layout["enabled"], username, now),
        )
        imported_layouts += 1
    product_id_map = {}
    for raw_product in (payload.get("products") or []):
        p = _normalized_backup_product(raw_product, now)
        if not p:
            skipped_products += 1
            continue
        restored_id = restore_product_backup_row(db, p, username, now)
        if not restored_id:
            skipped_products += 1
            continue
        imported_products += 1
        try:
            old_id = int(raw_product.get("id"))
        except (TypeError, ValueError, OverflowError):
            old_id = 0
        if old_id:
            product_id_map[old_id] = restored_id
    product_data_result = restore_product_data_backup(
        db,
        payload.get("product_data"),
        product_id_map,
    )
    db.commit()
    return jsonify({
        "success": True,
        "imported_layouts": imported_layouts,
        "imported_products": imported_products,
        "skipped_products": skipped_products,
        "restored_product_data": product_data_result["restored"],
        "skipped_product_data": product_data_result["skipped"],
    })
