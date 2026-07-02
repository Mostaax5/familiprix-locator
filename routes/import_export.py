import json
import re
import os
import gc
import threading
from flask import Blueprint, request, jsonify, Response
from database import get_db, DatabaseIntegrityError
from auth import require_editor, utc_now_iso

import_export_bp = Blueprint("import_export", __name__)

# Serialize PDF parsing across worker threads — pdfplumber is memory-heavy and two
# concurrent parses can exhaust Render's 512 MB instance (see parse_planogram_pdf).
_PDF_PARSE_LOCK = threading.Lock()


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


class _PlanogramParser:
    """Streaming planogram parser: feed it one table at a time (in document order)
    and it accumulates only the parsed products — never the raw tables. That lets
    the caller extract-then-free each PDF page as it goes, so peak memory stays
    small even for a 5 MB / 7000-row planogram (holding every page at once is what
    ran Render's 512 MB instance out of memory).

    The column mapping and carried-down tablette/position persist ACROSS tables and
    pages: a planogram prints the `UPC | Description` header only on the first page
    of a tablette, so a tablette continuing onto the next page has no header and
    blank/merged cells. Keeping the state means the continuation reuses the last
    header + last tablette number, so nothing is dropped at a page break."""

    def __init__(self):
        self.products = []
        self.seen = {}
        self.current_col = None
        self.last_tab = None
        self.last_pos = None

    def feed_table(self, table):
        if not table:
            return
        for row in table:
            self._feed_row(row)

    def _feed_row(self, row):
        if not row:
            return
        cells = [_clean_cell(c) for c in row]
        lower = [c.lower() for c in cells]

        if any(c == "upc" for c in lower) and any("description" in c for c in lower):
            col = {}
            for j, h in enumerate(lower):
                if h == "tablette":                  col["t"] = j
                elif h == "position":                col["p"] = j
                elif "fa" in h and "ade" in h:       col["f"] = j
                elif h == "upc":                     col["u"] = j
                elif "code" in h and "upc" not in h: col["c"] = j
                elif "description" in h:             col["d"] = j
                elif "ajout" in h:                   col["a"] = j
                elif "statut" in h:                  col["s"] = j
                elif "stock" in h:                   col["e"] = j
            self.current_col = col
            return

        col = self.current_col
        if not col or "u" not in col or "d" not in col:
            return

        def g(k, _cells=cells, _col=col):
            i = _col.get(k)
            return _cells[i] if i is not None and i < len(_cells) else ""

        tab = g("t") or self.last_tab
        pos = g("p") or self.last_pos
        if not _cell_is_int(tab) or not _cell_is_int(pos):
            return

        upc  = re.sub(r"\s+", "", g("u"))
        desc = g("d")
        if not upc or not desc:
            return

        self.last_tab, self.last_pos = tab, pos
        t_int, p_int = int(tab), int(pos)
        key = (t_int, p_int)
        ajout    = g("a").lower() == "oui"
        en_stock = g("e").lower() != "non"

        if key in self.seen:
            idx = self.seen[key]
            if ajout:                     self.products[idx]["is_new"]  = True
            if g("e").lower() == "oui":   self.products[idx]["en_stock"] = True
            return

        try:
            facings = int(re.sub(r"\D", "", g("f")) or "1")
        except (ValueError, TypeError):
            facings = 1
        self.seen[key] = len(self.products)
        self.products.append({
            "tablette": t_int,
            "position": p_int,
            "barcode":  upc,
            "code_familiprix": g("c"),
            "facings":  max(1, facings),   # général info only, not placement
            "name":     desc,
            "is_new":   ajout,
            "en_stock": en_stock,
        })

    def result(self):
        self.products.sort(key=lambda x: (x["tablette"], x["position"]))
        return self.products


def parse_planogram_tables(tables):
    """Parse product rows from a sequence of tables in document order. Thin wrapper
    over _PlanogramParser (used by tests and the offline catalogue rebuild)."""
    parser = _PlanogramParser()
    for table in tables:
        parser.feed_table(table)
    return parser.result()


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
    parser = _PlanogramParser()

    # Only ONE PDF parses at a time. pdfplumber peaks at hundreds of MB on a
    # multi-MB planogram; two concurrent uploads (8 worker threads) stacking their
    # peaks is what pushed Render's 512 MB instance over the edge. Serializing makes
    # a second uploader wait a few seconds instead of crashing the whole app.
    with _PDF_PARSE_LOCK:
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

                # Extract each page's tables, feed them to the streaming parser, then
                # FREE the page's cached objects before moving on — so we never hold
                # the whole PDF in memory. State carries across page breaks in parser.
                for page in pdf.pages:
                    for table in (page.extract_tables() or []):
                        parser.feed_table(table)
                    try:
                        page.flush_cache()          # drop pdfplumber's per-page object cache
                        page.get_textmap.cache_clear()
                    except Exception:
                        pass
        except Exception as exc:
            gc.collect()
            return jsonify({"success": False, "error": f"Erreur d’analyse PDF: {exc}"}), 500

    products = parser.result()
    gc.collect()   # reclaim the many small pdfplumber objects now, not eventually
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
