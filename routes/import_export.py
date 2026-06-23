import json
import re
import os
from flask import Blueprint, request, jsonify, Response
from database import get_db, DatabaseIntegrityError
from auth import require_editor, utc_now_iso

import_export_bp = Blueprint("import_export", __name__)


# ── Routes ─────────────────────────────────────────────────────────────────────

@import_export_bp.route("/api/export", methods=["GET"])
def export_database():
    db = get_db()
    products = [dict(p) for p in db.execute("SELECT * FROM products ORDER BY aisle, side, section, shelf, position").fetchall()]
    layouts  = [dict(r) for r in db.execute("SELECT * FROM aisle_layouts ORDER BY aisle").fetchall()]
    payload  = {
        "export_version": 1,
        "exported_at": utc_now_iso(),
        "products": products,
        "aisle_layouts": layouts,
    }
    data = json.dumps(payload, ensure_ascii=False, indent=2)
    filename = f"familiprix-backup-{utc_now_iso()[:10]}.json"
    return Response(
        data,
        mimetype="application/json",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@import_export_bp.route("/api/import", methods=["POST"])
def import_database():
    username, error = require_editor()
    if error:
        return error
    payload = request.get_json(silent=True) or {}
    if payload.get("export_version") != 1:
        return jsonify({"success": False, "error": "Format de fichier non reconnu."}), 400

    db = get_db()
    imported_layouts = 0
    imported_products = 0
    skipped_products = 0

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
            (
                aisle,
                str(layout.get("max_section", "1")),
                str(layout.get("max_shelf", "5")),
                str(layout.get("max_position", "8")),
                str(layout.get("config_json", "")),
                int(layout.get("enabled", 1)),
                username,
                utc_now_iso(),
            ),
        )
        imported_layouts += 1

    for product in (payload.get("products") or []):
        name = str(product.get("name", "")).strip()
        aisle = str(product.get("aisle", "")).strip()
        side = str(product.get("side", "")).strip()
        section = str(product.get("section", "1")).strip() or "1"
        shelf = str(product.get("shelf", "")).strip()
        position = str(product.get("position", "")).strip()
        if not all([name, aisle, side, shelf, position]):
            skipped_products += 1
            continue
        try:
            db.execute(
                """
                INSERT INTO products
                    (name, brand, description, image_url, source_url, search_terms, usage_notes,
                     alternative_suggestions, barcode, aisle, side, section, shelf, position,
                     created_by, created_at, modified_by, modified_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(aisle, side, section, shelf, position) DO UPDATE SET
                    name=excluded.name, brand=excluded.brand, description=excluded.description,
                    image_url=excluded.image_url, source_url=excluded.source_url,
                    search_terms=excluded.search_terms, usage_notes=excluded.usage_notes,
                    alternative_suggestions=excluded.alternative_suggestions,
                    barcode=excluded.barcode, modified_by=excluded.modified_by, modified_at=excluded.modified_at
                """,
                (
                    name,
                    str(product.get("brand", "")),
                    str(product.get("description", "")),
                    str(product.get("image_url", "")),
                    str(product.get("source_url", "")),
                    str(product.get("search_terms", "")),
                    str(product.get("usage_notes", "")),
                    str(product.get("alternative_suggestions", "")),
                    str(product.get("barcode", "")),
                    aisle, side, section, shelf, position,
                    username, str(product.get("created_at", "") or utc_now_iso()),
                    username, utc_now_iso(),
                ),
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


@import_export_bp.route("/api/reset", methods=["POST"])
def reset_database():
    username, error = require_editor()
    if error:
        return error
    data = request.get_json() or {}
    wipe_layouts = bool(data.get("wipe_layouts", False))
    db = get_db()
    from routes.products import first_column
    product_count = first_column(db.execute("SELECT COUNT(*) FROM products").fetchone()) or 0
    db.execute("DELETE FROM products")
    layout_count = 0
    if wipe_layouts:
        layout_count = first_column(db.execute("SELECT COUNT(*) FROM aisle_layouts").fetchone()) or 0
        db.execute("DELETE FROM aisle_layouts")
    db.commit()
    return jsonify({
        "success": True,
        "deleted_products": int(product_count),
        "deleted_layouts": int(layout_count),
    })


@import_export_bp.route("/api/import/planogram-parse", methods=["POST"])
def parse_planogram_pdf():
    try:
        import pdfplumber
        import io as _io
    except ImportError:
        return jsonify({"success": False, "error": "pdfplumber n’est pas installe sur ce serveur."}), 503

    if "file" not in request.files:
        return jsonify({"success": False, "error": "Aucun fichier fourni."}), 400
    f = request.files["file"]
    if not f.filename.lower().endswith(".pdf"):
        return jsonify({"success": False, "error": "Le fichier doit etre un PDF."}), 400

    products = []
    seen = {}
    plano_meta = {"name": "", "number": "", "version": ""}

    def _clean(val):
        return str(val or "").strip()

    def _is_int(val):
        try:
            int(_clean(val))
            return True
        except (ValueError, TypeError):
            return False

    try:
        with pdfplumber.open(_io.BytesIO(f.read())) as pdf:
            # Plano identity from the cover page text (best-effort).
            try:
                head = pdf.pages[0].extract_text() or ""
                m = re.search(r"PLANOGRAMME\s*:\s*([^\n]+)", head, re.IGNORECASE)
                if m: plano_meta["name"] = m.group(1).strip()[:120]
                m = re.search(r"Plano\s*#\s*([0-9]+)", head, re.IGNORECASE)
                if m: plano_meta["number"] = m.group(1).strip()
                m = re.search(r"Version\s*#\s*([A-Za-z0-9]+)", head, re.IGNORECASE)
                if m: plano_meta["version"] = m.group(1).strip()
            except Exception:
                pass

            for page in pdf.pages:
                for table in (page.extract_tables() or []):
                    current_col = None
                    last_tab = last_pos = None

                    for row in table:
                        if not row:
                            continue
                        cells = [_clean(c) for c in row]
                        lower = [c.lower() for c in cells]

                        if any(c == "upc" for c in lower) and any("description" in c for c in lower):
                            current_col = {}
                            for j, h in enumerate(lower):
                                if h == "tablette":                  current_col["t"] = j
                                elif h == "position":                current_col["p"] = j
                                elif "fa" in h and "ade" in h:       current_col["f"] = j
                                elif h == "upc":                     current_col["u"] = j
                                elif "code" in h and "upc" not in h: current_col["c"] = j
                                elif "description" in h:             current_col["d"] = j
                                elif "ajout" in h:                   current_col["a"] = j
                                elif "statut" in h:                  current_col["s"] = j
                                elif "stock" in h:                   current_col["e"] = j
                            continue

                        if not current_col or "u" not in current_col or "d" not in current_col:
                            continue

                        def g(k, _cells=cells, _col=current_col):
                            i = _col.get(k)
                            return _cells[i] if i is not None and i < len(_cells) else ""

                        tab = g("t") or last_tab
                        pos = g("p") or last_pos
                        if not _is_int(tab) or not _is_int(pos):
                            continue

                        upc  = re.sub(r"\s+", "", g("u"))
                        desc = g("d")
                        if not upc or not desc:
                            continue

                        last_tab, last_pos = tab, pos
                        t_int, p_int = int(tab), int(pos)
                        key = (t_int, p_int)
                        ajout    = g("a").lower() == "oui"
                        en_stock = g("e").lower() != "non"

                        if key in seen:
                            idx = seen[key]
                            if ajout:              products[idx]["is_new"]  = True
                            if g("e").lower() == "oui": products[idx]["en_stock"] = True
                            continue

                        try:
                            facings = int(re.sub(r"\D", "", g("f")) or "1")
                        except (ValueError, TypeError):
                            facings = 1
                        seen[key] = len(products)
                        products.append({
                            "tablette": t_int,
                            "position": p_int,
                            "barcode":  upc,
                            "code_familiprix": g("c"),
                            "facings":  max(1, facings),   # général info only, not placement
                            "name":     desc,
                            "is_new":   ajout,
                            "en_stock": en_stock,
                        })
    except Exception as exc:
        return jsonify({"success": False, "error": f"Erreur d’analyse PDF: {exc}"}), 500

    products.sort(key=lambda x: (x["tablette"], x["position"]))
    tablettes = {}
    for p in products:
        t = str(p["tablette"])
        tablettes[t] = tablettes.get(t, 0) + 1

    return jsonify({"success": True, "products": products, "count": len(products),
                    "tablettes": tablettes, "plano": plano_meta})
