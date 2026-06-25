import json
import os
import threading
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from flask import Blueprint, request, jsonify
from database import get_db, DatabaseIntegrityError
from auth import require_editor, utc_now_iso

gist_bp = Blueprint("gist", __name__)

GITHUB_TOKEN  = os.environ.get("GITHUB_TOKEN",  "").strip()
GITHUB_GIST_ID = os.environ.get("GITHUB_GIST_ID", "").strip()
_GIST_FILENAME = "familiprix-backup.json"


# ── Helpers ────────────────────────────────────────────────────────────────────

def _as_flag(value, default):
    """Coerce a backed-up 0/1 flag to int, tolerating missing/blank values."""
    if value in ("", None):
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _build_backup_payload(db):
    products = [dict(p) for p in db.execute("SELECT * FROM products ORDER BY aisle, side, section, shelf, position").fetchall()]
    layouts  = [dict(r) for r in db.execute("SELECT * FROM aisle_layouts ORDER BY aisle").fetchall()]
    return {
        "export_version": 1,
        "exported_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "products": products,
        "aisle_layouts": layouts,
    }


def _push_to_gist(payload):
    if not GITHUB_TOKEN:
        return False, "GITHUB_TOKEN non configure"
    content = json.dumps(payload, ensure_ascii=False, indent=2)
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
        with urlopen(req, timeout=15) as resp:
            result = json.loads(resp.read())
            gist_id = result.get("id", "")
            if not GITHUB_GIST_ID:
                print(f"[Gist] Nouveau gist cree: {gist_id} — ajoutez GITHUB_GIST_ID={gist_id} dans vos variables Render")
            return True, gist_id
    except Exception as exc:
        print(f"[Gist] Sauvegarde echouee: {exc}")
        return False, str(exc)


def _schedule_gist_backup(db):
    if not GITHUB_TOKEN:
        return
    payload = _build_backup_payload(db)
    threading.Thread(target=_push_to_gist, args=(payload,), daemon=True).start()


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
        with urlopen(req, timeout=15) as resp:
            gist = json.loads(resp.read())
        file_info = gist.get("files", {}).get(_GIST_FILENAME)
        if not file_info:
            db.close()
            return
        payload = json.loads(file_info["content"])
        if payload.get("export_version") != 1:
            db.close()
            return
        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        for layout in (payload.get("aisle_layouts") or []):
            aisle = str(layout.get("aisle", "")).strip()
            if not aisle:
                continue
            db.execute(
                """
                INSERT INTO aisle_layouts (aisle, max_section, max_shelf, max_position, config_json, enabled, modified_by, modified_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(aisle) DO UPDATE SET
                    max_section=excluded.max_section, max_shelf=excluded.max_shelf,
                    max_position=excluded.max_position, config_json=excluded.config_json,
                    enabled=excluded.enabled, modified_by=excluded.modified_by, modified_at=excluded.modified_at
                """,
                (aisle, str(layout.get("max_section", "1")), str(layout.get("max_shelf", "5")),
                 str(layout.get("max_position", "8")), str(layout.get("config_json", "")),
                 int(layout.get("enabled", 1)), "gist-restore", now),
            )
        imported = 0
        for p in (payload.get("products") or []):
            name = str(p.get("name", "")).strip()
            if not name:
                continue
            try:
                db.execute(
                    """
                    INSERT INTO products (name, brand, description, image_url, source_url, search_terms, usage_notes,
                        alternative_suggestions, barcode, product_code, facings, aisle, side, section, shelf, position,
                        is_plano, in_stock, flipped_label, underneath_label, created_by, created_at, modified_by, modified_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (name, p.get("brand", ""), p.get("description", ""), p.get("image_url", ""),
                     p.get("source_url", ""), p.get("search_terms", ""), p.get("usage_notes", ""),
                     p.get("alternative_suggestions", ""), p.get("barcode", ""), p.get("product_code", ""), _as_flag(p.get("facings"), 1),
                     p.get("aisle", ""), p.get("side", ""), p.get("section", "1"),
                     p.get("shelf", ""), p.get("position", ""),
                     _as_flag(p.get("is_plano"), 0), _as_flag(p.get("in_stock"), 1), _as_flag(p.get("flipped_label"), 0), p.get("underneath_label", ""),
                     "gist-restore", p.get("created_at", now), "gist-restore", now),
                )
                imported += 1
            except Exception:
                pass
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
        with urlopen(req, timeout=15) as resp:
            gist = json.loads(resp.read())
        file_info = gist.get("files", {}).get(_GIST_FILENAME)
        if not file_info:
            return jsonify({"success": False, "error": "Fichier de sauvegarde introuvable dans le gist."}), 404
        payload = json.loads(file_info["content"])
    except Exception as exc:
        return jsonify({"success": False, "error": f"Impossible de lire le gist: {exc}"}), 500
    if payload.get("export_version") != 1:
        return jsonify({"success": False, "error": "Format de sauvegarde non reconnu."}), 400
    db = get_db()
    imported_layouts = 0
    imported_products = 0
    skipped_products = 0
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    for layout in (payload.get("aisle_layouts") or []):
        aisle = str(layout.get("aisle", "")).strip()
        if not aisle:
            continue
        db.execute(
            """
            INSERT INTO aisle_layouts (aisle, max_section, max_shelf, max_position, config_json, enabled, modified_by, modified_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(aisle) DO UPDATE SET
                max_section=excluded.max_section, max_shelf=excluded.max_shelf,
                max_position=excluded.max_position, config_json=excluded.config_json,
                enabled=excluded.enabled, modified_by=excluded.modified_by, modified_at=excluded.modified_at
            """,
            (aisle, str(layout.get("max_section", "1")), str(layout.get("max_shelf", "5")),
             str(layout.get("max_position", "8")), str(layout.get("config_json", "")),
             int(layout.get("enabled", 1)), username, now),
        )
        imported_layouts += 1
    for p in (payload.get("products") or []):
        name = str(p.get("name", "")).strip()
        if not name:
            continue
        try:
            db.execute(
                """
                INSERT INTO products (name, brand, description, image_url, source_url, search_terms, usage_notes,
                    alternative_suggestions, barcode, product_code, facings, aisle, side, section, shelf, position,
                    is_plano, in_stock, flipped_label, underneath_label, created_by, created_at, modified_by, modified_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (name, p.get("brand", ""), p.get("description", ""), p.get("image_url", ""),
                 p.get("source_url", ""), p.get("search_terms", ""), p.get("usage_notes", ""),
                 p.get("alternative_suggestions", ""), p.get("barcode", ""), p.get("product_code", ""), _as_flag(p.get("facings"), 1),
                 p.get("aisle", ""), p.get("side", ""), p.get("section", "1"),
                 p.get("shelf", ""), p.get("position", ""),
                 _as_flag(p.get("is_plano"), 0), _as_flag(p.get("in_stock"), 1), _as_flag(p.get("flipped_label"), 0), p.get("underneath_label", ""),
                 username, p.get("created_at", now), username, now),
            )
            imported_products += 1
        except DatabaseIntegrityError:
            skipped_products += 1
    db.commit()
    return jsonify({
        "success": True,
        "imported_layouts": imported_layouts,
        "imported_products": imported_products,
        "skipped_products": skipped_products,
    })
