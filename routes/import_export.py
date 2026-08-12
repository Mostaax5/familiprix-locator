import json
import re
import os
import gc
import time
import uuid
import secrets
import tempfile
import threading
import unicodedata
from flask import Blueprint, request, jsonify, Response, g, stream_with_context
from database import get_db
from auth import require_editor, utc_now_iso
from security import record_security_event
from routes.layout import layout_metrics, normalize_layout_config, valid_aisle_name
from routes.products import (
    product_payload_error,
    release_optional_product_caches,
    safe_http_url,
)
from memory_guard import memory_intensive_task, release_unused_memory
from product_data import (
    assess_metadata_candidate,
    create_review_issue,
    gtin_identity_key,
    normalize_identifier,
    text_digits,
    upsert_reference_candidate,
)
from product_backup import (
    PRODUCT_DATA_ORDER_COLUMNS,
    PRODUCT_DATA_TABLE_COLUMNS,
    restore_product_backup_row,
    restore_product_data_backup,
)

import_export_bp = Blueprint("import_export", __name__)

# Serialize PDF parsing across worker threads — pdfplumber is memory-heavy and two
# concurrent parses can exhaust Render's 512 MB instance (see parse_planogram_pdf).
_PDF_PARSE_LOCK = threading.Lock()

# ── Async parse jobs ─────────────────────────────────────────────────────────────
# Parsing remains asynchronous because an unfamiliar PDF may need the slower
# compatibility reader. The upload stores the file and returns a job id while a
# background thread parses it, and the phone polls the status endpoint. Jobs live
# as files in the temp dir so a gunicorn worker recycle can relaunch an interrupted
# parse from the stored PDF instead of leaving it stuck.
_JOBS_DIR = os.path.join(tempfile.gettempdir(), "plano-parse-jobs")
_JOB_MAX_AGE_S = 6 * 3600


def _bounded_env_int(name, default, minimum, maximum):
    try:
        return min(max(int(os.environ.get(name, default)), minimum), maximum)
    except (TypeError, ValueError):
        return default


_MAX_PDF_BYTES = _bounded_env_int("PLANOGRAM_PDF_MAX_MB", 20, 1, 40) * 1024 * 1024
_MAX_PDF_PAGES = _bounded_env_int("PLANOGRAM_PDF_MAX_PAGES", 120, 1, 250)
_MAX_PDF_PRODUCTS = _bounded_env_int(
    "PLANOGRAM_PDF_MAX_PRODUCTS", 20_000, 100, 50_000
)
_MAX_CATALOG_BYTES = _bounded_env_int("PLANOGRAM_CATALOG_MAX_MB", 24, 1, 40) * 1024 * 1024
_MAX_CATALOG_PLANOGRAMS = 500
_MAX_CATALOG_PRODUCTS = 100_000


def _job_paths(job_id):
    return (os.path.join(_JOBS_DIR, f"{job_id}.json"),
            os.path.join(_JOBS_DIR, f"{job_id}.pdf"))


def _write_job(job_id, payload):
    os.makedirs(_JOBS_DIR, exist_ok=True)
    try:
        os.chmod(_JOBS_DIR, 0o700)
    except OSError:
        pass
    path = _job_paths(job_id)[0]
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False)
    try:
        os.chmod(tmp, 0o600)
    except OSError:
        pass
    os.replace(tmp, path)   # atomic: a poll never sees a half-written file


def _read_job(job_id):
    try:
        with open(_job_paths(job_id)[0], "r", encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return None


def _cleanup_old_jobs():
    try:
        cutoff = time.time() - _JOB_MAX_AGE_S
        for name in os.listdir(_JOBS_DIR):
            path = os.path.join(_JOBS_DIR, name)
            try:
                if os.path.getmtime(path) < cutoff:
                    os.remove(path)
            except OSError:
                pass
    except OSError:
        pass


def _launch_parse_thread(job_id):
    """Parse the job's stored PDF in a background thread and write the result."""
    def worker():
        json_path, pdf_path = _job_paths(job_id)
        job_meta = _read_job(job_id) or {}
        owner = str(job_meta.get("owner") or "")
        try:
            # The coordinate-aware PDFium path normally finishes in well under a
            # second. It validates its row coverage and automatically falls back
            # to pdfplumber for any unfamiliar document instead of risking a
            # silently incomplete import.
            with memory_intensive_task("planogram_pdf", priority=True):
                release_optional_product_caches()
                with _PDF_PARSE_LOCK:
                    products, plano_meta, parse_method = _parse_planogram_file(pdf_path)
            tablettes = {}
            for p in products:
                t = str(p["tablette"])
                tablettes[t] = tablettes.get(t, 0) + 1
            _write_job(job_id, {"status": "done", "success": True, "owner": owner,
                                "products": products, "count": len(products),
                                "tablettes": tablettes, "plano": plano_meta,
                                "parse_method": parse_method})
            try:
                os.remove(pdf_path)   # done — the stored PDF is no longer needed
            except OSError:
                pass
        except Exception as exc:
            print(f"[SECURITY] Planogram parse failed for job {job_id}: {type(exc).__name__}: {exc}")
            _write_job(job_id, {"status": "error", "success": False, "owner": owner,
                                "error": "Le PDF est invalide, trop volumineux ou impossible a analyser."})
            try:
                os.remove(pdf_path)
            except OSError:
                pass
        finally:
            release_unused_memory()

    threading.Thread(target=worker, daemon=True).start()


# ── Routes ─────────────────────────────────────────────────────────────────────

@import_export_bp.route("/api/export", methods=["GET"])
def export_database():
    with memory_intensive_task("database_export", priority=True):
        release_optional_product_caches()
        try:
            return _export_database_locked()
        finally:
            release_unused_memory()


def _export_database_locked():
    db = get_db()
    filename = f"familiprix-backup-{utc_now_iso()[:10]}.json"
    fd, path = tempfile.mkstemp(prefix="familiprix-backup-", suffix=".json")

    def write_rows(output, query):
        output.write("[")
        first = True
        for row in db.execute(query):
            if not first:
                output.write(",")
            json.dump(
                dict(row), output, ensure_ascii=False,
                separators=(",", ":"),
            )
            first = False
        output.write("]")

    try:
        with os.fdopen(fd, "w", encoding="utf-8") as output:
            output.write('{"export_version":2,"exported_at":')
            json.dump(utc_now_iso(), output, ensure_ascii=False)
            output.write(',"products":')
            write_rows(
                output,
                "SELECT * FROM products "
                "ORDER BY aisle, side, section, shelf, position",
            )
            output.write(',"aisle_layouts":')
            write_rows(
                output,
                "SELECT * FROM aisle_layouts ORDER BY aisle",
            )
            output.write(',"product_data":{')
            for index, table in enumerate(PRODUCT_DATA_TABLE_COLUMNS):
                if index:
                    output.write(",")
                json.dump(table, output, ensure_ascii=False)
                output.write(":")
                order_column = PRODUCT_DATA_ORDER_COLUMNS.get(table, "id")
                write_rows(
                    output,
                    f"SELECT * FROM {table} ORDER BY {order_column}",
                )
            output.write("}}")
    except Exception:
        try:
            os.remove(path)
        except OSError:
            pass
        raise
    release_unused_memory()
    cleanup_state = {"done": False}
    cleanup_lock = threading.Lock()

    def cleanup_export():
        with cleanup_lock:
            if cleanup_state["done"]:
                return
            cleanup_state["done"] = True
            try:
                os.remove(path)
            except OSError:
                pass
        release_unused_memory()

    def stream_export():
        try:
            with open(path, "rb") as source:
                while True:
                    chunk = source.read(256 * 1024)
                    if not chunk:
                        break
                    yield chunk
        finally:
            cleanup_export()

    response = Response(
        stream_with_context(stream_export()),
        mimetype="application/json",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
    response.call_on_close(cleanup_export)
    return response


@import_export_bp.route("/api/import", methods=["POST"])
def import_database():
    with memory_intensive_task("database_import", priority=True):
        release_optional_product_caches()
        try:
            return _import_database_locked()
        finally:
            release_unused_memory()


def _import_database_locked():
    username, error = require_editor()
    if error:
        return error
    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        return jsonify({"success": False, "error": "Structure de sauvegarde invalide."}), 400
    if payload.get("export_version") not in {1, 2}:
        return jsonify({"success": False, "error": "Format de fichier non reconnu."}), 400
    layouts_payload = payload.get("aisle_layouts") or []
    products_payload = payload.get("products") or []
    if not isinstance(layouts_payload, list) or not isinstance(products_payload, list):
        return jsonify({"success": False, "error": "Structure de sauvegarde invalide."}), 400
    if len(layouts_payload) > 1000 or len(products_payload) > 50_000:
        return jsonify({"success": False, "error": "Sauvegarde trop volumineuse."}), 413

    db = get_db()
    imported_layouts = 0
    imported_products = 0
    skipped_products = 0
    product_id_map = {}

    for layout in layouts_payload:
        if not isinstance(layout, dict):
            continue
        aisle = str(layout.get("aisle", "")).strip()
        if not valid_aisle_name(aisle):
            continue
        config = normalize_layout_config(
            layout.get("config_json"), layout.get("max_section", "1"),
            layout.get("max_shelf", "5"), layout.get("max_position", "8"),
        )
        max_section, max_shelf, max_position = layout_metrics(config)
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
                max_section,
                max_shelf,
                max_position,
                json.dumps(config, ensure_ascii=False, separators=(",", ":")),
                1 if layout.get("enabled", 1) else 0,
                username,
                utc_now_iso(),
            ),
        )
        imported_layouts += 1

    for product in products_payload:
        if not isinstance(product, dict):
            continue
        product = dict(product)
        product["image_url"] = safe_http_url(product.get("image_url"))
        product["source_url"] = safe_http_url(product.get("source_url"))
        product["primary_source_url"] = safe_http_url(product.get("primary_source_url"))
        if product_payload_error(product):
            skipped_products += 1
            continue
        name = str(product.get("name", "")).strip()
        aisle = str(product.get("aisle", "")).strip()
        side = str(product.get("side", "")).strip()
        section = str(product.get("section", "1")).strip() or "1"
        shelf = str(product.get("shelf", "")).strip()
        position = str(product.get("position", "")).strip()
        if not all([name, aisle, side, shelf, position]) or not valid_aisle_name(aisle):
            skipped_products += 1
            continue
        product.update({
            "name": name,
            "aisle": aisle,
            "side": side,
            "section": section,
            "shelf": shelf,
            "position": position,
        })
        restored_id = restore_product_backup_row(db, product, username, utc_now_iso())
        if not restored_id:
            skipped_products += 1
            continue
        imported_products += 1
        try:
            old_id = int(product.get("id"))
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


@import_export_bp.route("/api/reset", methods=["POST"])
def reset_database():
    username, error = require_editor()
    if error:
        return error
    data = request.get_json(silent=True)
    if not isinstance(data, dict) or not isinstance(data.get("wipe_layouts"), bool):
        return jsonify({"success": False, "error": "Demande de suppression invalide."}), 400
    wipe_layouts = data["wipe_layouts"]
    expected_confirmation = "SUPPRIMER LE PLAN" if wipe_layouts else "SUPPRIMER LES PRODUITS"
    if not secrets.compare_digest(str(data.get("confirmation", "")), expected_confirmation):
        return jsonify({
            "success": False,
            "error": "La phrase de confirmation est incorrecte.",
            "code": "confirmation_required",
        }), 400
    db = get_db()
    from routes.products import first_column
    product_count = first_column(db.execute("SELECT COUNT(*) FROM products").fetchone()) or 0
    db.execute("DELETE FROM product_relationships")
    db.execute("DELETE FROM product_aliases")
    db.execute("DELETE FROM product_data_issues")
    db.execute("DELETE FROM product_field_evidence")
    db.execute("DELETE FROM product_identifiers")
    db.execute("DELETE FROM products")
    layout_count = 0
    if wipe_layouts:
        layout_count = first_column(db.execute("SELECT COUNT(*) FROM aisle_layouts").fetchone()) or 0
        db.execute("DELETE FROM aisle_layouts")
    record_security_event(db, "database_reset", username, {
        "deleted_products": int(product_count),
        "deleted_layouts": int(layout_count),
        "wipe_layouts": wipe_layouts,
    })
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


_FAST_PLANO_COLUMNS = {
    "t": 0, "p": 1, "f": 2, "u": 3, "c": 4,
    "d": 5, "a": 6, "s": 7, "e": 8, "comments": 9,
}


def _planogram_metadata_from_text(text):
    metadata = {"name": "", "number": "", "version": ""}
    value = str(text or "")
    match = re.search(r"PLANOGRAMME\s*:\s*([^\r\n]+)", value, re.IGNORECASE)
    if match:
        metadata["name"] = match.group(1).strip()[:120]
    match = re.search(r"Plano\s*#\s*([0-9]+)", value, re.IGNORECASE)
    if match:
        metadata["number"] = match.group(1).strip()
    match = re.search(r"Version\s*#\s*([A-Za-z0-9]+)", value, re.IGNORECASE)
    if match:
        metadata["version"] = match.group(1).strip()
    return metadata


def _pdf_label(value):
    normalized = unicodedata.normalize("NFKD", str(value or ""))
    return "".join(ch for ch in normalized if not unicodedata.combining(ch)).lower()


def _pdfium_page_words(text_page):
    text = text_page.get_text_range() or ""
    words = []
    for order, match in enumerate(re.finditer(r"\S+", text)):
        boxes = []
        for index in {match.start(), match.end() - 1}:
            try:
                boxes.append(text_page.get_charbox(index))
            except Exception:
                pass
        if not boxes:
            continue
        left = min(box[0] for box in boxes)
        bottom = min(box[1] for box in boxes)
        right = max(box[2] for box in boxes)
        top = max(box[3] for box in boxes)
        words.append({
            "text": match.group(0),
            "x": (left + right) / 2,
            "y": (bottom + top) / 2,
            "order": order,
        })
    return text, words


def _cluster_pdf_words(words, axis, tolerance=2.4):
    groups = []
    for word in sorted(words, key=lambda item: (item[axis], item["order"])):
        coordinate = float(word[axis])
        if not groups or abs(coordinate - groups[-1]["coordinate"]) > tolerance:
            groups.append({"coordinate": coordinate, "words": [word]})
            continue
        group = groups[-1]
        group["words"].append(word)
        count = len(group["words"])
        group["coordinate"] += (coordinate - group["coordinate"]) / count
    groups.sort(key=lambda group: min(word["order"] for word in group["words"]))
    return groups


def _pdf_header_centers(words, column_axis):
    by_key = {}
    stock_coordinates = []
    for word in words:
        label = re.sub(r"[^a-z0-9]+", "", _pdf_label(word["text"]))
        key = ""
        if label == "tablette":
            key = "t"
        elif label == "position":
            key = "p"
        elif label.startswith("facade"):
            key = "f"
        elif label == "upc":
            key = "u"
        elif label == "code":
            key = "c"
        elif label.startswith("description"):
            key = "d"
        elif label == "ajout":
            key = "a"
        elif label == "statut":
            key = "s"
        elif label == "stock":
            key = "e"
            stock_coordinates.append(float(word[column_axis]))
        elif label.startswith("commentaire"):
            key = "comments"
        if key:
            by_key.setdefault(key, []).append(float(word[column_axis]))
    if not {"t", "p", "u", "d"}.issubset(by_key):
        return None
    centers = {
        key: sum(values) / len(values) for key, values in by_key.items()
    }
    # "En stock" is split into two words. Center the column on both words so
    # values below it are assigned consistently on portrait and rotated pages.
    en_words = [
        float(word[column_axis]) for word in words
        if re.sub(r"[^a-z0-9]+", "", _pdf_label(word["text"])) == "en"
    ]
    if stock_coordinates and en_words:
        centers["e"] = (
            sum(stock_coordinates) + sum(en_words)
        ) / (len(stock_coordinates) + len(en_words))
    return centers


def _pdf_page_groups(words, prior_row_axis=None):
    choices = []
    for row_axis, column_axis in (("x", "y"), ("y", "x")):
        groups = _cluster_pdf_words(words, row_axis)
        headers = [
            _pdf_header_centers(group["words"], column_axis) for group in groups
        ]
        score = max((len(header or {}) for header in headers), default=0)
        count = sum(1 for header in headers if header)
        choices.append((score, count, row_axis == prior_row_axis,
                        row_axis, column_axis, groups))
    best = max(choices, key=lambda choice: choice[:3])
    if best[0] < 4 and prior_row_axis:
        return (
            prior_row_axis,
            "y" if prior_row_axis == "x" else "x",
            _cluster_pdf_words(words, prior_row_axis),
        )
    return best[3], best[4], best[5]


def _pdf_cells_from_group(words, schema, column_axis):
    cells = [[] for _ in range(len(_FAST_PLANO_COLUMNS))]
    for word in words:
        key = min(
            schema,
            key=lambda column: abs(float(word[column_axis]) - schema[column]),
        )
        index = _FAST_PLANO_COLUMNS.get(key)
        if index is not None:
            cells[index].append(word)
    values = []
    for cell in cells:
        ordered = sorted(cell, key=lambda word: (word[column_axis], word["order"]))
        values.append(" ".join(word["text"] for word in ordered).strip())

    # The description starts close to the narrow code column, so nearest-center
    # assignment may put its first word beside the numeric Familiprix code.
    # Preserve the first numeric token as the code and move the rest back.
    code_parts = values[_FAST_PLANO_COLUMNS["c"]].split()
    if code_parts:
        if re.fullmatch(r"\d{3,12}", code_parts[0]):
            spill = " ".join(code_parts[1:])
            values[_FAST_PLANO_COLUMNS["c"]] = code_parts[0]
        else:
            spill = " ".join(code_parts)
            values[_FAST_PLANO_COLUMNS["c"]] = ""
        if spill:
            values[_FAST_PLANO_COLUMNS["d"]] = (
                f"{spill} {values[_FAST_PLANO_COLUMNS['d']]}".strip()
            )
    ajout_index = _FAST_PLANO_COLUMNS["a"]
    ajout_parts = values[ajout_index].split()
    ajout_value = (
        ajout_parts[-1].lower()
        if ajout_parts and ajout_parts[-1].lower() in {"oui", "non"}
        else ""
    )
    ajout_spill = " ".join(ajout_parts[:-1] if ajout_value else ajout_parts)
    if ajout_spill:
        values[_FAST_PLANO_COLUMNS["d"]] = (
            f"{values[_FAST_PLANO_COLUMNS['d']]} {ajout_spill}".strip()
        )
    values[ajout_index] = ajout_value
    return values


def _parse_planogram_pdf_fast(pdf_path):
    import pypdfium2 as pdfium

    parser = _PlanogramParser()
    parser.current_col = dict(_FAST_PLANO_COLUMNS)
    metadata = {"name": "", "number": "", "version": ""}
    stats = {
        "headers": 0, "candidate_upc_rows": 0,
        "candidate_slots": set(), "document_upc_tokens": 0, "pages": 0,
    }
    row_axis = None
    document = pdfium.PdfDocument(pdf_path)
    try:
        if len(document) > _MAX_PDF_PAGES:
            raise ValueError("PDF page limit exceeded")
        stats["pages"] = len(document)
        for page_number in range(len(document)):
            page = document[page_number]
            text_page = page.get_textpage()
            try:
                text, words = _pdfium_page_words(text_page)
                if page_number == 0:
                    metadata = _planogram_metadata_from_text(text)
                page_row_axis, column_axis, groups = _pdf_page_groups(
                    words, prior_row_axis=row_axis
                )
                page_headers = [
                    _pdf_header_centers(group["words"], column_axis)
                    for group in groups
                ]
                has_header = any(page_headers)
                schema = None
                if row_axis == page_row_axis:
                    schema = getattr(parser, "_fast_schema", None)
                # Some pages begin with rows continued from the previous table,
                # then print the next header lower on the page.
                seen_header = not has_header or schema is not None
                row_axis = page_row_axis
                page_candidate_start = stats["candidate_upc_rows"]
                last_product_coordinate = None
                for group, header in zip(groups, page_headers):
                    if header:
                        schema = header
                        parser._fast_schema = header
                        stats["headers"] += 1
                        seen_header = True
                        continue
                    if not seen_header or not schema:
                        continue
                    values = _pdf_cells_from_group(
                        group["words"], schema, column_axis
                    )
                    upc = re.sub(r"\D", "", values[_FAST_PLANO_COLUMNS["u"]])
                    if not upc:
                        continue
                    nonempty_columns = {
                        index for index, value in enumerate(values) if value
                    }
                    if (
                        nonempty_columns == {_FAST_PLANO_COLUMNS["u"]}
                        and parser.products
                        and last_product_coordinate is not None
                        and abs(group["coordinate"] - last_product_coordinate) <= 12
                        and len(upc) <= 2
                        and len(parser.products[-1]["barcode"]) < 14
                    ):
                        # A 14-digit GTIN can wrap its final digit onto a second
                        # visual line. pdfplumber joins that cell with a newline;
                        # PDFium exposes it as a one-word continuation row.
                        parser.products[-1]["barcode"] += upc
                        continue
                    stats["candidate_upc_rows"] += 1
                    tab = values[_FAST_PLANO_COLUMNS["t"]] or parser.last_tab
                    pos = values[_FAST_PLANO_COLUMNS["p"]] or parser.last_pos
                    if _cell_is_int(tab) and _cell_is_int(pos):
                        stats["candidate_slots"].add((int(tab), int(pos)))
                    parser._feed_row(values)
                    last_product_coordinate = group["coordinate"]
                    if len(parser.products) > _MAX_PDF_PRODUCTS:
                        raise ValueError("PDF product limit exceeded")
                if (
                    has_header
                    or stats["candidate_upc_rows"] > page_candidate_start
                ):
                    stats["document_upc_tokens"] += len(
                        re.findall(r"(?<!\d)\d{8,14}(?!\d)", text)
                    )
            finally:
                try:
                    text_page.close()
                except Exception:
                    pass
                try:
                    page.close()
                except Exception:
                    pass
    finally:
        try:
            document.close()
        except Exception:
            pass
    return parser.result(), metadata, stats


def _fast_planogram_is_trustworthy(products, stats):
    if not products or int(stats.get("headers", 0)) < 1:
        return False
    candidate_slots = len(stats.get("candidate_slots") or ())
    candidate_rows = int(stats.get("candidate_upc_rows", 0))
    document_upcs = int(stats.get("document_upc_tokens", 0))
    if not candidate_slots or candidate_rows < candidate_slots:
        return False
    if document_upcs and candidate_rows < (document_upcs * 0.97):
        return False
    if len(products) < (candidate_slots * 0.97):
        return False
    valid_codes = 0
    for product in products:
        if not re.fullmatch(r"\d{1,18}", str(product.get("barcode", ""))):
            return False
        if not str(product.get("name", "") or "").strip():
            return False
        if re.fullmatch(r"\d{3,12}", str(product.get("code_familiprix", ""))):
            valid_codes += 1
    return valid_codes >= max(1, int(len(products) * 0.95))


def _parse_planogram_pdf_compatibility(pdf_path):
    import pdfplumber

    metadata = {"name": "", "number": "", "version": ""}
    parser = _PlanogramParser()
    with pdfplumber.open(pdf_path) as pdf:
        if len(pdf.pages) > _MAX_PDF_PAGES:
            raise ValueError("PDF page limit exceeded")
        try:
            metadata = _planogram_metadata_from_text(
                pdf.pages[0].extract_text() or ""
            )
        except Exception:
            pass
        for page in pdf.pages:
            tables = page.extract_tables() or []
            for table in tables:
                parser.feed_table(table)
                if len(parser.products) > _MAX_PDF_PRODUCTS:
                    raise ValueError("PDF product limit exceeded")
            del tables
            try:
                page.close()
            except Exception:
                pass
            gc.collect()
    return parser.result(), metadata


def _parse_planogram_file(pdf_path):
    products = None
    stats = None
    try:
        products, metadata, stats = _parse_planogram_pdf_fast(pdf_path)
        if _fast_planogram_is_trustworthy(products, stats):
            return products, metadata, "pdfium-fast"
        print(
            "[PLANOGRAM] Fast parse validation requested compatibility fallback: "
            f"{len(products)} products, {len(stats.get('candidate_slots') or ())} slots."
        )
    except Exception as exc:
        print(
            "[PLANOGRAM] Fast parse unavailable; using compatibility fallback: "
            f"{type(exc).__name__}."
        )
    # Do not retain PDFium's full fast-parser result while pdfplumber creates its
    # own page/table structures for the compatibility pass.
    products = None
    stats = None
    release_unused_memory()
    products, metadata = _parse_planogram_pdf_compatibility(pdf_path)
    return products, metadata, "pdfplumber-fallback"


@import_export_bp.route("/api/import/planogram-parse", methods=["POST"])
def parse_planogram_pdf():
    """Accept the PDF, store it, launch the background parse, return a job id
    immediately. The normal reader is fast; the background job also protects the
    request from a slower compatibility fallback."""
    username, error = require_editor()
    if error:
        return error
    try:
        import pypdfium2  # noqa: F401 - primary fast parser
        import pdfplumber  # noqa: F401 - validated compatibility fallback
    except ImportError:
        return jsonify({
            "success": False,
            "error": "Le lecteur de planogrammes n'est pas installe sur ce serveur.",
        }), 503

    if "file" not in request.files:
        return jsonify({"success": False, "error": "Aucun fichier fourni."}), 400
    f = request.files["file"]
    filename = str(f.filename or "")
    if len(filename) > 255 or not filename.lower().endswith(".pdf"):
        return jsonify({"success": False, "error": "Le fichier doit etre un PDF."}), 400

    _cleanup_old_jobs()
    job_id = uuid.uuid4().hex
    json_path, pdf_path = _job_paths(job_id)
    os.makedirs(_JOBS_DIR, exist_ok=True)
    try:
        first = f.stream.read(5)
        if first != b"%PDF-":
            return jsonify({"success": False, "error": "Le contenu du fichier n'est pas un PDF valide."}), 400
        total = len(first)
        fd = os.open(pdf_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "wb") as output:
            output.write(first)
            while True:
                chunk = f.stream.read(1024 * 1024)
                if not chunk:
                    break
                total += len(chunk)
                if total > _MAX_PDF_BYTES:
                    raise ValueError("pdf-too-large")
                output.write(chunk)
    except ValueError:
        try:
            os.remove(pdf_path)
        except OSError:
            pass
        return jsonify({"success": False, "error": "PDF trop volumineux."}), 413
    except OSError:
        try:
            os.remove(pdf_path)
        except OSError:
            pass
        return jsonify({"success": False, "error": "Impossible de stocker le PDF en securite."}), 500
    _write_job(job_id, {
        "status": "running", "pid": os.getpid(), "created": time.time(),
        "owner": str(getattr(g, "auth_session_hash", "")), "employee": username,
    })
    _launch_parse_thread(job_id)
    return jsonify({"success": True, "job": job_id})


@import_export_bp.route("/api/import/planogram-parse/status/<job_id>", methods=["GET"])
def parse_planogram_status(job_id):
    if not re.fullmatch(r"[0-9a-f]{32}", str(job_id or "")):
        return jsonify({"success": False, "error": "Job invalide."}), 400
    job = _read_job(job_id)
    if job is None:
        return jsonify({"success": False, "status": "unknown",
                        "error": "Analyse introuvable ou expirée. Re-choisissez le PDF."}), 404
    if not secrets.compare_digest(
        str(job.get("owner") or ""), str(getattr(g, "auth_session_hash", ""))
    ):
        return jsonify({"success": False, "error": "Analyse introuvable."}), 404
    if job.get("status") == "running":
        # Self-heal: if the worker that started the parse was recycled mid-job
        # (pid changed), the thread died with it — relaunch from the stored PDF.
        if job.get("pid") != os.getpid() and os.path.exists(_job_paths(job_id)[1]):
            _write_job(job_id, {**job, "pid": os.getpid()})
            _launch_parse_thread(job_id)
        return jsonify({"success": True, "status": "running"})
    return jsonify({key: value for key, value in job.items()
                    if key not in {"owner", "employee", "pid", "created"}})


@import_export_bp.route("/api/import/planogram-catalog", methods=["POST"])
def import_planogram_catalog():
    with memory_intensive_task("planogram_catalog", priority=True):
        release_optional_product_caches()
        try:
            return _import_planogram_catalog_locked()
        finally:
            release_unused_memory()


def _import_planogram_catalog_locked():
    """Bulk-ingest a pre-parsed catalogue of all planograms.

    UPC, retailer code, and the printed planogram name are store identity data.
    Embedded descriptions and images are retained as review candidates because
    they did not originate on the planogram page itself. Placement is untouched.
    Accepts a JSON file upload (field 'file') or a raw JSON body: a list of planogram
    objects {meta:{name,...}, file, products:[{barcode,name,code_familiprix,facings}]}.
    """
    username, error = require_editor()
    if error:
        return error
    if request.content_length and request.content_length > _MAX_CATALOG_BYTES + 1024 * 1024:
        return jsonify({"success": False, "error": "Catalogue trop volumineux."}), 413

    try:
        if "file" in request.files:
            raw = request.files["file"].stream.read(_MAX_CATALOG_BYTES + 1)
            if len(raw) > _MAX_CATALOG_BYTES:
                return jsonify({"success": False, "error": "Catalogue trop volumineux."}), 413
            payload = json.loads(raw.decode("utf-8"))
        else:
            payload = request.get_json(silent=True)
    except (ValueError, UnicodeDecodeError):
        return jsonify({"success": False, "error": "Catalogue JSON illisible."}), 400

    if isinstance(payload, dict):
        payload = payload.get("catalog") or payload.get("planograms")
    if not isinstance(payload, list):
        return jsonify({"success": False, "error": "Catalogue JSON invalide (liste de planogrammes attendue)."}), 400
    if len(payload) > _MAX_CATALOG_PLANOGRAMS:
        return jsonify({"success": False, "error": "Le catalogue contient trop de planogrammes."}), 413
    total_products = 0
    for plano in payload:
        if not isinstance(plano, dict):
            continue
        products = plano.get("products") or []
        if not isinstance(products, list):
            return jsonify({"success": False, "error": "Produits de planogramme invalides."}), 400
        total_products += len(products)
        if total_products > _MAX_CATALOG_PRODUCTS:
            return jsonify({"success": False, "error": "Le catalogue contient trop de produits."}), 413

    from routes.products import (
        _record_import_identifiers, audit_product_data,
        sync_reference_metadata_to_products, update_product_metadata_from_reference,
    )
    db = get_db()
    now = utc_now_iso()

    # Exact package first, then a unique exact Familiprix code. Partial UPCs and
    # name similarity are never identity keys.
    local_by_bc = {}
    local_by_code = {}
    for r in db.execute("SELECT * FROM products").fetchall():
        d = dict(r)
        gtin_key = gtin_identity_key(d.get("barcode", ""))
        if gtin_key:
            local_by_bc.setdefault(gtin_key, []).append(d)
        code_key = normalize_identifier("FAMILIPRIX_CODE", d.get("product_code", ""))
        if code_key:
            local_by_code.setdefault(code_key, []).append(d)

    planos = ref_upserts = enriched = products_seen = review_issues = 0
    description_retry_keys = set()
    affected_ids = set()
    for plano in payload:
        if not isinstance(plano, dict):
            continue
        planos += 1
        meta = plano.get("meta") or {}
        if not isinstance(meta, dict):
            meta = {}
        plano_name = str(meta.get("name") or "").strip()[:120]
        source = f"Planogramme: {plano_name}" if plano_name else "Planogramme"
        source_url = safe_http_url(plano.get("file"))

        for p in (plano.get("products") or []):
            if not isinstance(p.get("barcode", ""), str):
                continue
            barcode = text_digits(p.get("barcode", ""))
            name = str(p.get("name", "")).strip()[:300]
            if not barcode or len(barcode) > 14 or len(name) < 2:
                continue
            products_seen += 1
            code = str(p.get("code_familiprix", "")).strip()[:64]
            brand = str(p.get("brand", "") or "").strip()[:160]
            description = str(p.get("description", "") or "").strip()[:6000]
            image_url = safe_http_url(p.get("image_url"))

            identity_candidate = {
                "barcode": barcode, "name": name,
                "product_code": code, "source": source,
                "source_url": source_url,
                "store_presence_status": "planogram_imported",
            }
            metadata_candidate = {
                "barcode": barcode, "name": name, "brand": brand,
                "description": description, "image_url": image_url,
                "product_code": code,
                "source": "Metadonnees integrees au catalogue - a verifier",
                "source_url": safe_http_url(
                    p.get("metadata_source_url") or p.get("source_url")
                ),
                "store_presence_status": "planogram_imported",
            }
            for field in (
                "package_size", "package_unit", "variant", "flavour", "colour",
                "strength", "dosage_form", "manufacturer", "category",
                "ingredients", "compatibility", "purpose",
                "route_of_administration", "official_name_fr",
                "official_name_en",
            ):
                metadata_candidate[field] = str(p.get(field, "") or "").strip()[:6000]
            identity_result = upsert_reference_candidate(
                db, identity_candidate, imported_at=now
            )
            if identity_result.get("gtin_key"):
                description_retry_keys.add(identity_result["gtin_key"])
            ref_upserts += 1
            has_supplemental_metadata = any(
                str(metadata_candidate.get(field, "") or "").strip()
                for field in (
                    "brand", "description", "image_url", "package_size",
                    "package_unit", "variant", "flavour", "colour", "strength",
                    "dosage_form", "manufacturer", "category", "ingredients",
                    "compatibility", "purpose", "route_of_administration",
                    "official_name_fr", "official_name_en",
                )
            )
            metadata_result = {"issues": [], "confidence": 0.0}
            if has_supplemental_metadata:
                metadata_result = upsert_reference_candidate(
                    db, metadata_candidate, imported_at=now
                )
                ref_upserts += 1
            reference_issues = [
                (issue, identity_candidate, identity_result)
                for issue in identity_result.get("issues", [])
            ] + [
                (issue, metadata_candidate, metadata_result)
                for issue in metadata_result.get("issues", [])
            ]

            # 2) enrich placed products (fill blanks only) — match once per product row.
            try:
                facings = int(p.get("facings", 1) or 1)
            except (TypeError, ValueError):
                facings = 1
            matched = {
                d["id"]: (d, "exact_gtin")
                for d in local_by_bc.get(gtin_identity_key(barcode), [])
            }
            if not matched and code:
                code_rows = local_by_code.get(
                    normalize_identifier("FAMILIPRIX_CODE", code), []
                )
                if len(code_rows) == 1:
                    assessment = assess_metadata_candidate(
                        code_rows[0], identity_candidate,
                        match_method="exact_familiprix_code",
                    )
                    if assessment.accepted:
                        matched[code_rows[0]["id"]] = (
                            code_rows[0], "exact_familiprix_code"
                        )
            for d, match_method in matched.values():
                affected_ids.add(int(d["id"]))
                _record_import_identifiers(
                    db, d, now, source=source, payload=p
                )
                for issue, issue_candidate, issue_result in reference_issues:
                    create_review_issue(
                        db, d["id"], issue.get("type", "multiple_possible_matches"),
                        field_name=issue.get("field", ""),
                        existing_value=issue.get("existing", d.get(issue.get("field", ""), "")),
                        candidate_value=issue.get(
                            "candidate",
                            issue_candidate.get(issue.get("field", ""), ""),
                        ),
                        source=issue_candidate.get("source", ""),
                        source_url=issue_candidate.get("source_url", ""),
                        match_method=match_method,
                        confidence=issue_result.get("confidence", 0),
                        details=issue, created_at=now,
                    )
                    review_issues += 1
                changed = False
                if code and not str(d.get("product_code", "")).strip():
                    db.execute(
                        "UPDATE products SET product_code=?, modified_at=? WHERE id=?",
                        (code, now, d["id"]),
                    )
                    d["product_code"] = code
                    changed = True
                if facings > 1 and int(d.get("facings") or 1) <= 1:
                    db.execute(
                        "UPDATE products SET facings=?, modified_at=? WHERE id=?",
                        (facings, now, d["id"]),
                    )
                    d["facings"] = facings
                    changed = True
                if update_product_metadata_from_reference(
                    db, d, identity_candidate, now=now,
                    match_method=match_method
                ):
                    changed = True
                if has_supplemental_metadata and update_product_metadata_from_reference(
                    db, d, metadata_candidate, now=now,
                    match_method=match_method,
                ):
                    changed = True
                if changed:
                    enriched += 1

    ordered_retry_keys = sorted(description_retry_keys)
    for start in range(0, len(ordered_retry_keys), 400):
        keys = ordered_retry_keys[start:start + 400]
        placeholders = ",".join("?" for _ in keys)
        db.execute(
            f"""UPDATE product_reference SET enrich_status=''
                WHERE gtin_key IN ({placeholders})
                  AND (
                    TRIM(COALESCE(description,''))=''
                    OR enrich_status LIKE ?
                  )""",
            (*keys, "no_match%"),
        )

    # Link descriptions/images that were enriched before this import to all
    # already-placed copies of the same UPC. Existing/manual values win.
    metadata_linked = sync_reference_metadata_to_products(db, now=now)
    quality = audit_product_data(
        db, sorted(affected_ids), trigger_type="planogram_catalog_import",
        employee=username, now=now,
    ) if affected_ids else {"success": True, "scanned": 0, "issues": 0, "statuses": {}}
    db.commit()
    try:
        from routes.products import bump_reference_cache
        bump_reference_cache()   # refresh the in-memory search corpus
    except Exception:
        pass
    try:
        from routes.regulatory import schedule_regulatory_enrichment
        schedule_regulatory_enrichment()
    except Exception:
        pass
    try:
        from routes.ai import schedule_catalog_enrichment
        schedule_catalog_enrichment()
    except Exception:
        pass
    return jsonify({
        "success": True,
        "planograms": planos,
        "products_seen": products_seen,
        "reference_upserts": ref_upserts,
        "enriched_products": enriched,
        "metadata_linked_products": metadata_linked,
        "review_issues": review_issues,
        "quality": quality,
    })
