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


def _clean_cell(val):
    return str(val or "").strip()


def _cell_is_int(val):
    try:
        int(_clean_cell(val))
        return True
    except (ValueError, TypeError):
        return False


def parse_planogram_tables(tables):
    """Parse planogram product rows from a sequence of tables IN DOCUMENT ORDER
    (every page's tables concatenated, first page first).

    The column mapping (`current_col`) AND the carried-down tablette/position
    persist across tables and pages. This is the fix for tablettes that span a
    page break: a planogram prints the `UPC | Description` header only on the
    first page of a tablette, so when tablette 5 continues onto the next page
    that continuation table has no header. Resetting the state per-table (the old
    behaviour) left `current_col` empty on the continuation page and silently
    dropped every one of its rows — a whole half-tablette lost. Keeping the state
    means the continuation reuses the last header and the last tablette number
    (its cells are blank/merged in the PDF), so nothing is dropped.

    Returns the ordered, de-duplicated list of product dicts."""
    products = []
    seen = {}
    current_col = None
    last_tab = last_pos = None

    for table in tables:
        if not table:
            continue
        for row in table:
            if not row:
                continue
            cells = [_clean_cell(c) for c in row]
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
            if not _cell_is_int(tab) or not _cell_is_int(pos):
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

    products.sort(key=lambda x: (x["tablette"], x["position"]))
    return products


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

    plano_meta = {"name": "", "number": "", "version": ""}
    all_tables = []

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

            # Concatenate every page's tables IN ORDER, then parse with state that
            # carries across page breaks (see parse_planogram_tables).
            for page in pdf.pages:
                all_tables.extend(page.extract_tables() or [])
    except Exception as exc:
        return jsonify({"success": False, "error": f"Erreur d’analyse PDF: {exc}"}), 500

    products = parse_planogram_tables(all_tables)
    tablettes = {}
    for p in products:
        t = str(p["tablette"])
        tablettes[t] = tablettes.get(t, 0) + 1

    return jsonify({"success": True, "products": products, "count": len(products),
                    "tablettes": tablettes, "plano": plano_meta})


@import_export_bp.route("/api/import/planogram-catalog", methods=["POST"])
def import_planogram_catalog():
    """Bulk-ingest a pre-parsed catalogue of ALL planograms (produced offline so the
    server never has to parse 78 big PDFs over HTTP). It does two things, neither of
    which touches product placement:
      1. Upserts every product into product_reference so UPC lookup instantly returns
         a real name/description — even for products that aren't placed yet.
      2. Enriches products already placed in the plan (matched by barcode) by filling
         in ONLY blank fields (pharmacy code, façades) — never overwriting your data.
    Accepts a JSON file upload (field 'file') or a raw JSON body: a list of planogram
    objects {meta:{name,...}, file, products:[{barcode,name,code_familiprix,facings}]}.
    """
    username, error = require_editor()
    if error:
        return error

    try:
        if "file" in request.files:
            payload = json.loads(request.files["file"].read().decode("utf-8"))
        else:
            payload = request.get_json(silent=True)
    except (ValueError, UnicodeDecodeError) as exc:
        return jsonify({"success": False, "error": f"JSON illisible: {exc}"}), 400

    if isinstance(payload, dict):
        payload = payload.get("catalog") or payload.get("planograms")
    if not isinstance(payload, list):
        return jsonify({"success": False, "error": "Catalogue JSON invalide (liste de planogrammes attendue)."}), 400

    from routes.products import build_barcode_candidates, normalized_digits
    db = get_db()
    now = utc_now_iso()

    # Index placed products by every barcode variant so enrichment matches reliably.
    local_by_bc = {}
    for r in db.execute("SELECT id, barcode, product_code, facings FROM products").fetchall():
        d = dict(r)
        for cand in build_barcode_candidates(d.get("barcode", "")):
            local_by_bc.setdefault(cand, []).append(d)

    planos = ref_upserts = enriched = products_seen = 0
    for plano in payload:
        if not isinstance(plano, dict):
            continue
        planos += 1
        meta = plano.get("meta") or {}
        plano_name = str(meta.get("name") or "").strip()
        source = f"Planogramme: {plano_name}" if plano_name else "Planogramme"
        source_url = str(plano.get("file") or "")

        for p in (plano.get("products") or []):
            barcode = normalized_digits(p.get("barcode", ""))
            name = str(p.get("name", "")).strip()
            if not barcode or len(name) < 2:
                continue
            products_seen += 1
            code = str(p.get("code_familiprix", "")).strip()

            # 1) reference catalogue — keep an existing real name/code, else use the
            #    plano's. Storing the Familiprix code attaches it to the UPC for lookups.
            db.execute(
                """INSERT INTO product_reference (barcode, name, brand, description, image_url, source, source_url, product_code, updated_at)
                   VALUES (?, ?, '', '', '', ?, ?, ?, ?)
                   ON CONFLICT(barcode) DO UPDATE SET
                       name = CASE WHEN TRIM(COALESCE(product_reference.name, '')) = ''
                                   THEN excluded.name ELSE product_reference.name END,
                       product_code = CASE WHEN TRIM(COALESCE(product_reference.product_code, '')) = ''
                                   THEN excluded.product_code ELSE product_reference.product_code END,
                       source = excluded.source, source_url = excluded.source_url,
                       updated_at = excluded.updated_at""",
                (barcode, name, source, source_url, code, now),
            )
            ref_upserts += 1

            # 2) enrich placed products (fill blanks only) — match once per product row.
            try:
                facings = int(p.get("facings", 1) or 1)
            except (TypeError, ValueError):
                facings = 1
            matched = {}
            for cand in build_barcode_candidates(barcode):
                for d in local_by_bc.get(cand, []):
                    matched[d["id"]] = d
            for d in matched.values():
                changed = False
                if code and not str(d.get("product_code", "")).strip():
                    db.execute("UPDATE products SET product_code=? WHERE id=?", (code, d["id"]))
                    d["product_code"] = code
                    changed = True
                if facings > 1 and int(d.get("facings") or 1) <= 1:
                    db.execute("UPDATE products SET facings=? WHERE id=?", (facings, d["id"]))
                    d["facings"] = facings
                    changed = True
                if changed:
                    enriched += 1

    db.commit()
    try:
        from routes.products import bump_reference_cache
        bump_reference_cache()   # refresh the in-memory search corpus
    except Exception:
        pass
    return jsonify({
        "success": True,
        "planograms": planos,
        "products_seen": products_seen,
        "reference_upserts": ref_upserts,
        "enriched_products": enriched,
    })
