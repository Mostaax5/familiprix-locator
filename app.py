import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Flask, render_template, request, jsonify, send_from_directory
from database import (
    DatabaseIntegrityError,
    close_db,
    get_backend_summary,
    get_db,
    init_db,
)
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import json
import re
import unicodedata

app = Flask(__name__)
init_db()

PHARMACY_LOOKUP_SOURCES = [
    ("Familiprix", None),
    ("Jean Coutu", "https://www.jeancoutu.com"),
    ("Brunet", "https://www.brunet.ca"),
    ("Pharmaprix", "https://www.pharmaprix.ca"),
]

PRODUCT_LOOKUP_SOURCES = [
    ("Open Products Facts", "https://world.openproductsfacts.org"),
    ("Open Beauty Facts", "https://world.openbeautyfacts.org"),
    ("Open Food Facts", "https://world.openfoodfacts.org"),
]

DEFAULT_CERT_PATH = os.path.join(os.path.dirname(__file__), "certs", "localhost.pem")
DEFAULT_KEY_PATH = os.path.join(os.path.dirname(__file__), "certs", "localhost-key.pem")
LOOKUP_FIELDS = [
    "code",
    "product_name",
    "product_name_fr",
    "product_name_en",
    "generic_name",
    "generic_name_fr",
    "brands",
    "quantity",
    "categories",
    "url",
    "image_front_url",
]
SEARCH_STOPWORDS = {
    "a", "an", "and", "au", "aux", "avec", "ce", "ces", "cette", "client", "comme",
    "dans", "de", "des", "du", "en", "et", "for", "how", "i", "il", "ils", "je",
    "la", "le", "les", "mais", "mon", "my", "of", "on", "or", "ou", "par", "pas",
    "pour", "que", "qui", "sans", "si", "son", "sur", "the", "to", "un", "une",
    "with", "without", "y",
}
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "").strip()
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash-lite").strip() or "gemini-2.5-flash-lite"
GEMINI_BASE_URL = os.environ.get("GEMINI_BASE_URL", "https://generativelanguage.googleapis.com/v1beta").rstrip("/")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini").strip() or "gpt-4o-mini"
OPENAI_BASE_URL = os.environ.get("OPENAI_BASE_URL", "https://api.openai.com/v1").rstrip("/")

# ── Pages ──────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/manifest.json")
def manifest():
    return send_from_directory("static", "manifest.json")


@app.route("/service-worker.js")
def service_worker():
    return send_from_directory("static", "service-worker.js")


@app.teardown_appcontext
def teardown_database(_error):
    close_db(_error)


@app.route("/api/system/info", methods=["GET"])
def get_system_info():
    db = get_db()
    ai_provider = configured_ai_provider()
    duplicate_slots = db.execute(
        """
        SELECT COUNT(*) AS count
        FROM (
            SELECT 1
            FROM products
            GROUP BY aisle, side, section, shelf, position
            HAVING COUNT(*) > 1
        ) duplicates
        """
    ).fetchone()
    duplicate_barcodes = db.execute(
        """
        SELECT COUNT(*) AS count
        FROM (
            SELECT 1
            FROM products
            WHERE TRIM(COALESCE(barcode, '')) <> ''
            GROUP BY barcode
            HAVING COUNT(*) > 1
        ) duplicates
        """
    ).fetchone()
    return jsonify({
        **get_backend_summary(),
        "ai_enabled": bool(ai_provider["name"]),
        "ai_provider": ai_provider["name"],
        "ai_provider_label": ai_provider["label"],
        "duplicate_slots": int(first_column(duplicate_slots) or 0),
        "duplicate_barcodes": int(first_column(duplicate_barcodes) or 0),
    })


# ── API: Products ──────────────────────────────────────────────────────────

@app.route("/api/products", methods=["GET"])
def get_products():
    db = get_db()
    products = [row_to_product(p) for p in db.execute("SELECT * FROM products").fetchall()]
    products.sort(key=location_sort_key)
    return jsonify(products)


@app.route("/api/products/search", methods=["GET"])
def search_products():
    query = request.args.get("q", "").strip()
    if not query:
        return jsonify([])
    limit = min(max(clamp_non_negative_int(request.args.get("limit", "60"), 60), 1), 120)
    db = get_db()
    products = [row_to_product(p) for p in db.execute("SELECT * FROM products").fetchall()]
    items = rank_products_for_query(products, query, limit=limit)
    return jsonify(items)


@app.route("/api/products/barcode/<barcode>", methods=["GET"])
def get_by_barcode(barcode):
    db = get_db()
    for candidate in build_barcode_candidates(barcode):
        product = db.execute(
            "SELECT * FROM products WHERE barcode = ? ORDER BY id LIMIT 1", (candidate,)
        ).fetchone()
        if product:
            return jsonify(row_to_product(product))
    return jsonify({"error": "Produit non trouvé"}), 404


@app.route("/api/layout/aisles", methods=["GET"])
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


@app.route("/api/layout/aisles", methods=["POST"])
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


@app.route("/api/layout/aisles/<aisle>", methods=["PUT"])
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


@app.route("/api/layout/aisles/<aisle>", methods=["DELETE"])
def delete_layout_aisle(aisle):
    username, error = require_editor()
    if error:
        return error
    db = get_db()
    removed_products = first_column(db.execute("SELECT COUNT(*) FROM products WHERE aisle=?", (aisle,)).fetchone()) or 0
    db.execute("DELETE FROM products WHERE aisle=?", (aisle,))
    result = db.execute("DELETE FROM aisle_layouts WHERE aisle=?", (aisle,))
    db.commit()
    if result.rowcount == 0:
        return jsonify({"error": "Allee non trouvee."}), 404
    return jsonify({"success": True, "message": f"Allee {aisle} retiree par {username}. {removed_products} produit(s) supprime(s)."})


@app.route("/api/products/lookup/<barcode>", methods=["GET"])
def lookup_barcode(barcode):
    barcode = barcode.strip()
    if not barcode:
        return jsonify({"found": False, "error": "Code-barres manquant"}), 400

    barcode_candidates = build_barcode_candidates(barcode)

    # Phase 1: fast JSON APIs run in parallel — most reliable
    json_tasks = []
    for candidate in barcode_candidates:
        json_tasks.append(lambda bc=candidate: lookup_upcitemdb(bc))
    for candidate in barcode_candidates:
        for source_name, base_url in PRODUCT_LOOKUP_SOURCES:
            json_tasks.append(
                lambda bc=candidate, sn=source_name, su=base_url:
                lookup_open_facts_product(sn, su, bc)
            )
    product = first_lookup_result(json_tasks, max_workers=8)
    if product:
        return jsonify({"found": True, "product": product})

    # Phase 2: pharmacy website scrapers as fallback
    pharmacy_tasks = []
    for candidate in barcode_candidates:
        for source_name, source_base_url in PHARMACY_LOOKUP_SOURCES:
            if source_name == "Familiprix":
                pharmacy_tasks.append(
                    lambda bc=candidate, bcs=barcode_candidates:
                    lookup_familiprix_product(bc, bcs)
                )
            else:
                pharmacy_tasks.append(
                    lambda bc=candidate, sn=source_name, su=source_base_url, bcs=barcode_candidates:
                    lookup_generic_pharmacy_product(sn, su, bc, bcs)
                )
    product = first_lookup_result(pharmacy_tasks, max_workers=4)
    if product:
        return jsonify({"found": True, "product": product})

    return jsonify({"found": False, "error": "Aucun produit trouve en ligne"})


@app.route("/api/products/assist", methods=["POST"])
def assist_product():
    data = request.get_json() or {}
    name = str(data.get("name", "")).strip()
    brand = str(data.get("brand", "")).strip()
    description = str(data.get("description", "")).strip()
    barcode = str(data.get("barcode", "")).strip()
    if not name and not description:
        return jsonify({"success": False, "error": "Nom ou description requis."}), 400
    if not configured_ai_provider()["name"]:
        return jsonify({"success": False, "error": "GEMINI_API_KEY n est pas configure sur le serveur."}), 503
    assist = generate_product_assist_payload(name, brand, description, barcode)
    if not assist:
        return jsonify({"success": False, "error": "Impossible de generer l aide client pour le moment."}), 502
    return jsonify({"success": True, "assist": assist})


@app.route("/api/client/help", methods=["POST"])
def client_help():
    data = request.get_json() or {}
    question = str(data.get("question", "")).strip()
    if not question:
        return jsonify({"success": False, "error": "Question client requise."}), 400
    if not configured_ai_provider()["name"]:
        return jsonify({"success": False, "error": "GEMINI_API_KEY n est pas configure sur le serveur."}), 503

    raw_products = data.get("products")
    if isinstance(raw_products, list):
        matched_products = [product_context_for_client_help(item) for item in raw_products[:6] if isinstance(item, dict)]
    else:
        db = get_db()
        products = [row_to_product(p) for p in db.execute("SELECT * FROM products").fetchall()]
        matched_products = [product_context_for_client_help(item) for item in rank_products_for_query(products, question, limit=6)]

    advice = generate_client_help_payload(question, matched_products)
    if not advice:
        return jsonify({"success": False, "error": "Impossible de generer la reponse client pour le moment."}), 502
    return jsonify({"success": True, "advice": advice})


def normalized_digits(value):
    return re.sub(r"\D", "", str(value or ""))


def normalize_search_text(value):
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(character for character in text if not unicodedata.combining(character))
    return re.sub(r"[^a-z0-9]+", " ", text.lower()).strip()


def tokenize_search_query(query):
    return [
        token
        for token in normalize_search_text(query).split()
        if len(token) >= 2 and token not in SEARCH_STOPWORDS
    ]


def query_search_variants(query):
    variants = []
    seen = set()

    def add(value):
        cleaned = str(value or "").strip()
        if cleaned and cleaned not in seen:
            seen.add(cleaned)
            variants.append(cleaned)

    normalized = normalize_search_text(query)
    digits = normalized_digits(query)
    tokens = tokenize_search_query(query)
    add(normalized)
    if tokens:
        add(" ".join(tokens))
        for token in tokens:
            add(token)
    if digits and len(digits) >= 4:
        add(digits)
    return variants


def build_barcode_candidates(barcode):
    raw = str(barcode or "").strip()
    digits = normalized_digits(raw)
    candidates = []
    seen = set()

    def add(value):
        cleaned = str(value or "").strip()
        if cleaned and cleaned not in seen:
            seen.add(cleaned)
            candidates.append(cleaned)

    add(raw)
    add(digits)
    if len(digits) == 13 and digits.startswith("0"):
        add(digits[1:])
    if len(digits) == 12:
        add(f"0{digits}")
    if len(digits) == 14 and digits.startswith("00"):
        add(digits[2:])
    stripped = digits.lstrip("0")
    if stripped and stripped != digits:
        add(stripped)
        if len(stripped) == 12:
            add(f"0{stripped}")
    return candidates


def first_lookup_result(tasks, max_workers=4):
    if not tasks:
        return None
    with ThreadPoolExecutor(max_workers=min(max_workers, len(tasks))) as executor:
        futures = [executor.submit(task) for task in tasks]
        for future in as_completed(futures):
            try:
                result = future.result()
            except Exception:
                result = None
            if result:
                for pending in futures:
                    pending.cancel()
                return result
    return None


def lookup_familiprix_product(barcode, barcode_candidates=None):
    barcode_candidates = barcode_candidates or build_barcode_candidates(barcode)
    search_urls = [
        f"https://magasiner.familiprix.com/fr/search?text={barcode}",
        f"https://magasiner.familiprix.com/fr/search?q={barcode}",
        f"https://magasiner.familiprix.com/fr/recherche?q={barcode}",
    ]
    for url in search_urls:
        html, final_url = fetch_text(url)
        if not html:
            continue

        product_url = find_familiprix_product_url(html, final_url, barcode_candidates)
        if not product_url:
            continue

        product_html, product_final_url = fetch_text(product_url)
        if not product_html:
            product_html, product_final_url = html, final_url

        product = parse_familiprix_product_page(product_html, product_final_url, barcode, barcode_candidates)
        if product:
            return product

    return None


def fetch_text(url):
    request_obj = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 FamiliprixLocator/0.1",
            "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
            "Accept-Language": "fr-CA,fr;q=0.9,en;q=0.6",
        },
    )
    try:
        with urlopen(request_obj, timeout=3) as response:
            body = response.read().decode("utf-8", errors="ignore")
            return body, response.geturl()
    except (HTTPError, URLError, TimeoutError, UnicodeDecodeError):
        return None, None


def find_familiprix_product_url(html, final_url, barcode_candidates):
    if "/p/" in final_url and page_mentions_barcode(html, barcode_candidates):
        return final_url

    product_links = re.findall(r'href="([^"]+/p/[0-9]{6,}[^"]*)"', html)
    for link in product_links:
        absolute = normalize_familiprix_url(link)
        product_html, product_url = fetch_text(absolute)
        if product_html and page_mentions_barcode(product_html, barcode_candidates):
            return product_url or absolute

    return None


def normalize_familiprix_url(url):
    if url.startswith("http"):
        return url
    if url.startswith("/"):
        return f"https://magasiner.familiprix.com{url}"
    return f"https://magasiner.familiprix.com/{url}"


def lookup_generic_pharmacy_product(source_name, base_url, barcode, barcode_candidates=None):
    barcode_candidates = barcode_candidates or build_barcode_candidates(barcode)
    search_urls = [
        f"{base_url}/search?text={barcode}",
        f"{base_url}/search?q={barcode}",
        f"{base_url}/recherche?q={barcode}",
        f"{base_url}/recherche?text={barcode}",
        f"{base_url}/fr/search?text={barcode}",
        f"{base_url}/fr/search?q={barcode}",
        f"{base_url}/fr/recherche?q={barcode}",
    ]
    for url in search_urls:
        html, final_url = fetch_text(url)
        if not html:
            continue

        if page_mentions_barcode(html, barcode_candidates) and looks_like_product_page(final_url):
            product = parse_generic_pharmacy_product_page(source_name, html, final_url, barcode, barcode_candidates)
            if product:
                return product

        product_url = find_generic_product_url(html, base_url, barcode_candidates)
        if not product_url:
            continue

        product_html, product_final_url = fetch_text(product_url)
        if not product_html:
            continue

        product = parse_generic_pharmacy_product_page(source_name, product_html, product_final_url, barcode, barcode_candidates)
        if product:
            return product

    return None


def looks_like_product_page(url):
    url = (url or "").lower()
    return any(token in url for token in ["/p/", "/product", "/products/", "/shop/", "/item/"])


def find_generic_product_url(html, base_url, barcode_candidates):
    hrefs = re.findall(r'href="([^"]+)"', html)
    for href in hrefs:
        absolute = normalize_url(base_url, href)
        if not looks_like_product_page(absolute):
            continue
        product_html, product_url = fetch_text(absolute)
        if product_html and page_mentions_barcode(product_html, barcode_candidates):
            return product_url or absolute
    return None


def normalize_url(base_url, url):
    if url.startswith("http"):
        return url
    if url.startswith("/"):
        return f"{base_url}{url}"
    return f"{base_url}/{url}"


def parse_generic_pharmacy_product_page(source_name, html, url, barcode, barcode_candidates=None):
    barcode_candidates = barcode_candidates or build_barcode_candidates(barcode)
    if not page_mentions_barcode(html, barcode_candidates):
        return None

    structured = extract_structured_product_data(html, barcode_candidates)
    title = sanitize_title(structured.get("name") or clean_html_text(first_regex(html, [
        r"<h1[^>]*>(.*?)</h1>",
        r'<meta property="og:title" content="([^"]+)"',
        r"<title>(.*?)</title>",
    ])), source_name)

    description = structured.get("description") or clean_html_text(first_regex(html, [
        r'<meta name="description" content="([^"]+)"',
        r'<meta property="og:description" content="([^"]+)"',
    ]))
    image_url = structured.get("image_url") or first_regex(html, [r'<meta property="og:image" content="([^"]+)"'])
    brand = structured.get("brand") or infer_brand_from_title(title)

    if not title:
        return None

    return {
        "name": title,
        "brand": brand,
        "description": description,
        "barcode": barcode,
        "source": source_name,
        "source_url": url,
        "image_url": image_url,
    }


def parse_familiprix_product_page(html, url, barcode, barcode_candidates=None):
    barcode_candidates = barcode_candidates or build_barcode_candidates(barcode)
    if not page_mentions_barcode(html, barcode_candidates):
        return None

    structured = extract_structured_product_data(html, barcode_candidates)
    title = structured.get("name") or first_regex(html, [
        r"<h1[^>]*>(.*?)</h1>",
        r'<meta property="og:title" content="([^"]+)"',
        r"<title>(.*?)</title>",
    ])
    title = sanitize_title(clean_html_text(title), "Familiprix")

    description = structured.get("description") or clean_html_text(first_regex(html, [
        r'<meta name="description" content="([^"]+)"',
        r'<meta property="og:description" content="([^"]+)"',
    ]))
    image_url = structured.get("image_url") or first_regex(html, [r'<meta property="og:image" content="([^"]+)"'])

    brand = structured.get("brand") or infer_brand_from_title(title)

    if not title:
        return None

    return {
        "name": title,
        "brand": brand,
        "description": description,
        "barcode": barcode,
        "source": "Familiprix",
        "source_url": url,
        "image_url": image_url,
    }


def first_regex(text, patterns):
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
        if match:
            return match.group(1)
    return ""


def clean_html_text(value):
    value = re.sub(r"<[^>]+>", " ", value or "")
    value = (
        value.replace("&amp;", "&")
        .replace("&quot;", '"')
        .replace("&#39;", "'")
        .replace("&nbsp;", " ")
    )
    return re.sub(r"\s+", " ", value).strip()


def sanitize_title(title, source_name):
    title = title or ""
    suffixes = [
        f"| {source_name}",
        f"- {source_name}",
        f"| {source_name} Pharmacy",
        f"| {source_name} Pharmacie",
        "| Jean Coutu",
        "| Brunet",
        "| Pharmaprix",
        "| Familiprix",
    ]
    for suffix in suffixes:
        title = title.replace(suffix, "").strip()
    return title


def page_mentions_barcode(html, barcode_candidates):
    digits_only_html = normalized_digits(html)
    for candidate in barcode_candidates or []:
        cleaned = normalized_digits(candidate)
        if not cleaned:
            continue
        if candidate in (html or "") or cleaned in digits_only_html:
            return True
    return False


def extract_structured_product_data(html, barcode_candidates=None):
    products = []
    for block in re.findall(r'<script[^>]+type="application/ld\+json"[^>]*>(.*?)</script>', html or "", flags=re.IGNORECASE | re.DOTALL):
        try:
            payload = json.loads(block.strip())
        except json.JSONDecodeError:
            continue
        collect_structured_products(payload, products)

    for product in products:
        if barcode_candidates and not structured_product_matches_barcode(product, barcode_candidates):
            continue
        name = str(product.get("name", "")).strip()
        brand = extract_structured_brand(product.get("brand"))
        description = clean_html_text(str(product.get("description", "")).strip())
        image_url = extract_structured_image(product.get("image"))
        if name or brand or description or image_url:
            return {
                "name": name,
                "brand": brand,
                "description": description,
                "image_url": image_url,
            }
    return {}


def collect_structured_products(value, bucket):
    if isinstance(value, dict):
        product_type = value.get("@type")
        types = product_type if isinstance(product_type, list) else [product_type]
        if any(str(item).lower() == "product" for item in types if item):
            bucket.append(value)
        for nested in value.values():
            if isinstance(nested, (dict, list)):
                collect_structured_products(nested, bucket)
    elif isinstance(value, list):
        for item in value:
            collect_structured_products(item, bucket)


def structured_product_matches_barcode(product, barcode_candidates):
    product_codes = []
    for key in ["gtin", "gtin8", "gtin12", "gtin13", "gtin14", "upc"]:
        cleaned = normalized_digits(product.get(key, ""))
        if cleaned:
            product_codes.append(cleaned)
    if not product_codes:
        return False
    expanded_codes = set()
    for code in product_codes:
        expanded_codes.update(build_barcode_candidates(code))
    for candidate in barcode_candidates or []:
        cleaned_candidate = str(candidate).strip()
        if cleaned_candidate and cleaned_candidate in expanded_codes:
            return True
    return False


def extract_structured_brand(value):
    if isinstance(value, dict):
        return str(value.get("name", "")).strip()
    if isinstance(value, list):
        for item in value:
            result = extract_structured_brand(item)
            if result:
                return result
        return ""
    return str(value or "").strip()


def extract_structured_image(value):
    if isinstance(value, list):
        for item in value:
            result = extract_structured_image(item)
            if result:
                return result
        return ""
    return str(value or "").strip()


def infer_brand_from_title(title):
    parts = (title or "").split()
    return parts[0] if parts else ""


def lookup_open_facts_product(source_name, base_url, barcode):
    params = urlencode({"fields": ",".join(LOOKUP_FIELDS)})
    url = f"{base_url}/api/v2/product/{barcode}.json?{params}"
    request_obj = Request(
        url,
        headers={
            "User-Agent": "FamiliprixLocator/0.1 (local testing)",
            "Accept": "application/json",
        },
    )
    try:
        with urlopen(request_obj, timeout=3) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError):
        return None

    if payload.get("status") != 1:
        return None

    product = payload.get("product", {})
    name = first_present(product, ["product_name_fr", "product_name", "product_name_en"])
    brand = first_present(product, ["brands"])
    generic_name = first_present(product, ["generic_name_fr", "generic_name"])
    quantity = first_present(product, ["quantity"])
    categories = first_present(product, ["categories"])
    description_parts = [part for part in [generic_name, quantity, categories] if part]

    if not name and not brand:
        return None

    return {
        "name": name or brand,
        "brand": brand,
        "description": " | ".join(description_parts),
        "barcode": barcode,
        "source": source_name,
        "source_url": product.get("url", ""),
        "image_url": product.get("image_front_url", ""),
    }


def first_present(product, keys):
    for key in keys:
        value = str(product.get(key, "")).strip()
        if value:
            return value
    return ""


def lookup_upcitemdb(barcode):
    digits = normalized_digits(barcode)
    if not digits:
        return None
    request_obj = Request(
        f"https://api.upcitemdb.com/prod/trial/lookup?upc={digits}",
        headers={
            "User-Agent": "FamiliprixLocator/0.1",
            "Accept": "application/json",
        },
    )
    try:
        with urlopen(request_obj, timeout=5) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError):
        return None
    items = payload.get("items") or []
    if not items:
        return None
    item = items[0]
    name = str(item.get("title", "")).strip()
    brand = str(item.get("brand", "")).strip()
    description = str(item.get("description", "")).strip()
    images = item.get("images") or []
    image_url = str(images[0]).strip() if images else ""
    if not name:
        return None
    return {
        "name": name,
        "brand": brand,
        "description": description,
        "barcode": digits,
        "source": "UPC Item DB",
        "source_url": f"https://www.upcitemdb.com/upc/{digits}",
        "image_url": image_url,
    }


def product_search_text(product):
    return normalize_search_text(" ".join([
        str(product.get("name", "")),
        str(product.get("brand", "")),
        str(product.get("description", "")),
        str(product.get("search_terms", "")),
        str(product.get("usage_notes", "")),
        str(product.get("alternative_suggestions", "")),
    ]))


def product_search_score(product, query):
    lowered_query = normalize_search_text(query)
    digits_query = normalized_digits(query)
    if not lowered_query and not digits_query:
        return 0

    barcode = normalized_digits(product.get("barcode", ""))
    name = normalize_search_text(product.get("name", ""))
    brand = normalize_search_text(product.get("brand", ""))
    description = normalize_search_text(product.get("description", ""))
    search_terms = normalize_search_text(product.get("search_terms", ""))
    usage_notes = normalize_search_text(product.get("usage_notes", ""))
    alternatives = normalize_search_text(product.get("alternative_suggestions", ""))
    haystack = product_search_text(product)
    score = 0

    if digits_query and barcode:
        if barcode == digits_query:
            score += 1200
        elif len(digits_query) >= 4 and barcode.endswith(digits_query):
            score += 900
        elif digits_query in barcode:
            score += 500

    if lowered_query == name:
        score += 800
    elif name.startswith(lowered_query):
        score += 650
    elif lowered_query in name:
        score += 450

    if brand.startswith(lowered_query):
        score += 280
    elif lowered_query in brand:
        score += 180

    if lowered_query in description:
        score += 150
    if lowered_query in search_terms:
        score += 240
    if lowered_query in usage_notes:
        score += 170
    if lowered_query in alternatives:
        score += 120

    unique_tokens = list(dict.fromkeys(tokenize_search_query(query)))
    if unique_tokens:
        matched_tokens = sum(1 for token in unique_tokens if token in haystack)
        if matched_tokens == len(unique_tokens):
            score += 100 + (20 * matched_tokens)
        elif matched_tokens:
            score += 25 * matched_tokens

    return score


def rank_products_for_query(products, query, limit=60):
    variants = query_search_variants(query)
    if not variants:
        return []
    ranked = []
    for product in products:
        best_score = 0
        for variant in variants:
            best_score = max(best_score, product_search_score(product, variant))
        if best_score > 0:
            ranked.append((best_score, product))
    ranked.sort(key=lambda item: (-item[0], location_sort_key(item[1])))
    items = [product for _, product in ranked]
    return items[:limit] if limit else items


def configured_ai_provider():
    if GEMINI_API_KEY:
        return {"name": "gemini", "label": "Gemini", "model": GEMINI_MODEL}
    if OPENAI_API_KEY:
        return {"name": "openai", "label": "OpenAI", "model": OPENAI_MODEL}
    return {"name": "", "label": "", "model": ""}


def generate_product_assist_payload(name, brand, description, barcode):
    provider = configured_ai_provider()
    if provider["name"] == "gemini":
        return generate_product_assist_payload_gemini(name, brand, description, barcode)
    if provider["name"] == "openai":
        return generate_product_assist_payload_openai(name, brand, description, barcode)
    return None


def product_context_for_client_help(product):
    return {
        "name": str(product.get("name", "")).strip(),
        "brand": str(product.get("brand", "")).strip(),
        "description": str(product.get("description", "")).strip(),
        "usage_notes": str(product.get("usage_notes", "")).strip(),
        "search_terms": str(product.get("search_terms", "")).strip(),
        "alternative_suggestions": str(product.get("alternative_suggestions", "")).strip(),
        "barcode": str(product.get("barcode", "")).strip(),
        "location": (
            f"Allee {str(product.get('aisle', '')).strip()} - "
            f"{str(product.get('side', '')).strip()} - "
            f"Section {str(product.get('section', '')).strip()} - "
            f"Tablette {str(product.get('shelf', '')).strip()} - "
            f"Position {str(product.get('position', '')).strip()}"
        ).strip(),
    }


def generate_client_help_payload(question, products):
    provider = configured_ai_provider()
    if provider["name"] == "gemini":
        return generate_client_help_payload_gemini(question, products)
    if provider["name"] == "openai":
        return generate_client_help_payload_openai(question, products)
    return None


def generate_client_help_payload_gemini(question, products):
    payload = {
        "contents": [{
            "parts": [{
                "text": (
                    "Tu aides un employe de pharmacie Familiprix au Quebec a repondre a un client. "
                    "Si des produits sont disponibles dans le contexte, base-toi sur eux pour orienter le client. "
                    "PRIORITE ABSOLUE : si un produit de marque maison Familiprix (Essentiell ou Biomedic) "
                    "repond au besoin du client, mets-le en premier dans recommended_product_names. "
                    "Si aucun produit n est fourni ou que la liste est vide, suggere en priorite les gammes "
                    "Essentiell et Biomedic de Familiprix si elles correspondent au besoin, puis d autres options. "
                    "Donne un conseil general utile en pharmacie "
                    "(categories de produits a suggerer, questions a poser au client, signes d alerte). "
                    "Ne pose pas de diagnostic. "
                    "Dis clairement quand il faut orienter le client vers le pharmacien: "
                    "grossesse, bebe ou jeune enfant, interaction medicamenteuse possible, symptomes graves, "
                    "douleur importante, difficulte respiratoire, fievre elevee, duree inhabituelle ou doute. "
                    "Dans recommended_product_names, mets les produits Essentiell ou Biomedic en premier si applicable, "
                    "puis les autres produits du magasin, sinon des types de produits a chercher. "
                    "Retourne uniquement un JSON en francais avec exactement les cles "
                    "summary (texte), recommended_product_names (tableau), follow_up_questions (tableau), "
                    "safety_flags (tableau), pharmacist_referral (booleen) et pharmacist_reason (texte).\n\n"
                    f"Question client:\n{question}\n\n"
                    f"Produits disponibles en magasin:\n{json.dumps(products, ensure_ascii=False) if products else '[]'}"
                )
            }]
        }],
        "generationConfig": {
            "temperature": 0.2,
            "responseMimeType": "application/json",
        },
    }
    request_obj = Request(
        f"{GEMINI_BASE_URL}/models/{GEMINI_MODEL}:generateContent?{urlencode({'key': GEMINI_API_KEY})}",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request_obj, timeout=14) as response:
            raw_response = json.loads(response.read().decode("utf-8"))
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError):
        return None
    raw_text = extract_gemini_output_text(raw_response)
    if not raw_text:
        return None
    try:
        parsed = json.loads(raw_text)
    except json.JSONDecodeError:
        return None
    return normalize_client_help_payload(parsed)


def generate_client_help_payload_openai(question, products):
    payload = {
        "model": OPENAI_MODEL,
        "reasoning": {"effort": "low"},
        "instructions": (
            "Tu aides un employe de pharmacie Familiprix au Quebec a repondre a un client. "
            "Si des produits sont disponibles dans le contexte, base-toi sur eux pour orienter le client. "
            "PRIORITE ABSOLUE : si un produit de marque maison Familiprix (Essentiell ou Biomedic) "
            "repond au besoin du client, mets-le en premier dans recommended_product_names. "
            "Si aucun produit n est fourni ou que la liste est vide, suggere en priorite les gammes "
            "Essentiell et Biomedic de Familiprix si elles correspondent au besoin, puis d autres options. "
            "Donne un conseil general utile en pharmacie "
            "(categories de produits a suggerer, questions a poser au client, signes d alerte). "
            "Ne pose pas de diagnostic. "
            "Dis clairement quand il faut orienter le client vers le pharmacien: "
            "grossesse, bebe ou jeune enfant, interaction medicamenteuse possible, symptomes graves, "
            "douleur importante, difficulte respiratoire, fievre elevee, duree inhabituelle ou doute. "
            "Dans recommended_product_names, mets les produits Essentiell ou Biomedic en premier si applicable, "
            "puis les autres produits du magasin, sinon des types de produits a chercher. "
            "Retourne uniquement un JSON en francais."
        ),
        "input": json.dumps({"question": question, "products": products}, ensure_ascii=False),
        "text": {
            "format": {
                "type": "json_schema",
                "name": "client_help",
                "strict": True,
                "schema": {
                    "type": "object",
                    "properties": {
                        "summary": {"type": "string"},
                        "recommended_product_names": {
                            "type": "array",
                            "items": {"type": "string"},
                            "maxItems": 4,
                        },
                        "follow_up_questions": {
                            "type": "array",
                            "items": {"type": "string"},
                            "maxItems": 4,
                        },
                        "safety_flags": {
                            "type": "array",
                            "items": {"type": "string"},
                            "maxItems": 4,
                        },
                        "pharmacist_referral": {"type": "boolean"},
                        "pharmacist_reason": {"type": "string"},
                    },
                    "required": [
                        "summary",
                        "recommended_product_names",
                        "follow_up_questions",
                        "safety_flags",
                        "pharmacist_referral",
                        "pharmacist_reason",
                    ],
                    "additionalProperties": False,
                },
            }
        },
    }
    request_obj = Request(
        f"{OPENAI_BASE_URL}/responses",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {OPENAI_API_KEY}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urlopen(request_obj, timeout=14) as response:
            raw_response = json.loads(response.read().decode("utf-8"))
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError):
        return None
    raw_text = extract_openai_output_text(raw_response)
    if not raw_text:
        return None
    try:
        parsed = json.loads(raw_text)
    except json.JSONDecodeError:
        return None
    return normalize_client_help_payload(parsed)


def generate_product_assist_payload_gemini(name, brand, description, barcode):
    prompt = {
        "name": name,
        "brand": brand,
        "description": description,
        "barcode": barcode,
    }
    payload = {
        "contents": [{
            "parts": [{
                "text": (
                    "Tu aides les employes d une pharmacie Familiprix au Quebec. "
                    "Retourne uniquement un JSON en francais avec exactement les cles "
                    "search_terms (tableau), usage_notes (texte) et alternative_suggestions (tableau). "
                    "Les mots cles doivent etre des mots que les clients utilisent, "
                    "usage_notes doit etre une courte explication utile pour guider un client, "
                    "et alternative_suggestions doit contenir quelques alternatives possibles. "
                    "Sois concis, concret, prudent sur le plan medical et ne donne pas de diagnostic.\n\n"
                    f"Produit:\n{json.dumps(prompt, ensure_ascii=False)}"
                )
            }]
        }],
        "generationConfig": {
            "temperature": 0.2,
            "responseMimeType": "application/json",
        },
    }
    request_obj = Request(
        f"{GEMINI_BASE_URL}/models/{GEMINI_MODEL}:generateContent?{urlencode({'key': GEMINI_API_KEY})}",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request_obj, timeout=12) as response:
            raw_response = json.loads(response.read().decode("utf-8"))
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError):
        return None

    raw_text = extract_gemini_output_text(raw_response)
    if not raw_text:
        return None
    try:
        parsed = json.loads(raw_text)
    except json.JSONDecodeError:
        return None
    return normalize_assist_payload(parsed)


def generate_product_assist_payload_openai(name, brand, description, barcode):
    prompt = {
        "name": name,
        "brand": brand,
        "description": description,
        "barcode": barcode,
    }
    payload = {
        "model": OPENAI_MODEL,
        "reasoning": {"effort": "low"},
        "instructions": (
            "Tu aides les employes d une pharmacie Familiprix au Quebec. "
            "Retourne un JSON en francais avec des mots cles que les clients utilisent, "
            "une courte explication utile pour guider un client, et quelques alternatives possibles. "
            "Sois concis, concret, prudent sur le plan medical et ne donne pas de diagnostic."
        ),
        "input": json.dumps(prompt, ensure_ascii=False),
        "text": {
            "format": {
                "type": "json_schema",
                "name": "product_assist",
                "strict": True,
                "schema": {
                    "type": "object",
                    "properties": {
                        "search_terms": {
                            "type": "array",
                            "items": {"type": "string"},
                            "maxItems": 12,
                        },
                        "usage_notes": {"type": "string"},
                        "alternative_suggestions": {
                            "type": "array",
                            "items": {"type": "string"},
                            "maxItems": 6,
                        },
                    },
                    "required": ["search_terms", "usage_notes", "alternative_suggestions"],
                    "additionalProperties": False,
                },
            }
        },
    }
    request_obj = Request(
        f"{OPENAI_BASE_URL}/responses",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {OPENAI_API_KEY}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urlopen(request_obj, timeout=12) as response:
            raw_response = json.loads(response.read().decode("utf-8"))
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError):
        return None

    raw_text = extract_openai_output_text(raw_response)
    if not raw_text:
        return None
    try:
        parsed = json.loads(raw_text)
    except json.JSONDecodeError:
        return None

    return normalize_assist_payload(parsed)


def normalize_assist_payload(parsed):
    search_terms = [str(item).strip() for item in parsed.get("search_terms", []) if str(item).strip()]
    alternative_suggestions = [str(item).strip() for item in parsed.get("alternative_suggestions", []) if str(item).strip()]
    usage_notes = str(parsed.get("usage_notes", "")).strip()
    return {
        "search_terms": ", ".join(dict.fromkeys(search_terms)),
        "usage_notes": usage_notes,
        "alternative_suggestions": ", ".join(dict.fromkeys(alternative_suggestions)),
    }


def normalize_client_help_payload(parsed):
    recommended = [str(item).strip() for item in parsed.get("recommended_product_names", []) if str(item).strip()]
    follow_up = [str(item).strip() for item in parsed.get("follow_up_questions", []) if str(item).strip()]
    safety_flags = [str(item).strip() for item in parsed.get("safety_flags", []) if str(item).strip()]
    return {
        "summary": str(parsed.get("summary", "")).strip(),
        "recommended_product_names": list(dict.fromkeys(recommended)),
        "follow_up_questions": list(dict.fromkeys(follow_up)),
        "safety_flags": list(dict.fromkeys(safety_flags)),
        "pharmacist_referral": bool(parsed.get("pharmacist_referral", False)),
        "pharmacist_reason": str(parsed.get("pharmacist_reason", "")).strip(),
    }


def extract_gemini_output_text(payload):
    for candidate in payload.get("candidates", []):
        content = candidate.get("content", {})
        for part in content.get("parts", []):
            text = str(part.get("text", "")).strip()
            if text:
                return text
    return ""


def extract_openai_output_text(payload):
    output_text = str(payload.get("output_text", "")).strip()
    if output_text:
        return output_text
    for item in payload.get("output", []):
        for content in item.get("content", []):
            if content.get("type") in {"output_text", "text"}:
                text = str(content.get("text", "")).strip()
                if text:
                    return text
    return ""


def find_product_at_position(db, aisle, side, section, shelf, position, exclude_id=None):
    query = "SELECT * FROM products WHERE aisle=? AND side=? AND section=? AND shelf=? AND position=?"
    params = [aisle, side, section, shelf, position]
    if exclude_id is not None:
        query += " AND id != ?"
        params.append(exclude_id)
    return db.execute(query, tuple(params)).fetchone()


def resolve_ssl_context():
    use_https = os.environ.get("FLASK_USE_HTTPS", "").strip().lower() in {"1", "true", "yes", "on"}
    cert_path = os.environ.get("FLASK_SSL_CERT", DEFAULT_CERT_PATH)
    key_path = os.environ.get("FLASK_SSL_KEY", DEFAULT_KEY_PATH)

    if not use_https:
        return None

    if os.path.exists(cert_path) and os.path.exists(key_path):
        print(f"HTTPS local actif avec certificat: {cert_path}")
        return cert_path, key_path

    print("HTTPS demande, mais certificat local introuvable.")
    print(f"Attendu: {cert_path}")
    print(f"Attendu: {key_path}")
    print("Demarrage en HTTP simple.")
    return None


def utc_now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def auth_payload_from_request():
    data = request.get_json(silent=True) or {}
    username = (
        request.headers.get("X-User-Name")
        or data.get("_username")
        or "appareil"
    ).strip()
    return username


def require_editor():
    username = auth_payload_from_request() or "appareil"
    db = get_db()
    db.execute(
        """
        INSERT INTO users (username, last_seen)
        VALUES (?, ?)
        ON CONFLICT(username) DO UPDATE SET last_seen=excluded.last_seen
        """,
        (username, utc_now_iso()),
    )
    db.commit()
    return username, None


def row_to_product(product):
    if not product:
        return None
    item = dict(product)
    item["last_change_by"] = item.get("modified_by") or item.get("created_by") or ""
    item["last_change_at"] = item.get("modified_at") or item.get("created_at") or ""
    return item


def first_column(row):
    if row is None:
        return None
    if isinstance(row, dict):
        return next(iter(row.values()), None)
    return row[0]


def clamp_non_negative_int(value, fallback=0):
    try:
        return max(0, int(str(value)))
    except (TypeError, ValueError):
        return fallback


def aisle_sort_key(value):
    text = str(value or "").strip()
    if text.isdigit():
        return (0, int(text), text)
    return (1, text.lower())


def location_sort_key(item):
    side_order = {"Gauche": 0, "Droite": 1}
    return (
        aisle_sort_key(item.get("aisle")),
        side_order.get(str(item.get("side", "")).strip(), 9),
        clamp_non_negative_int(item.get("section", 0)),
        clamp_non_negative_int(item.get("shelf", 0)),
        clamp_non_negative_int(item.get("position", 0)),
        str(item.get("name", "")).lower(),
    )


def build_default_layout_config(max_section, max_shelf, max_position):
    section_count = clamp_non_negative_int(max_section)
    shelf_count = clamp_non_negative_int(max_shelf)
    position_count = clamp_non_negative_int(max_position)
    section_template = [{"shelves": [position_count for _ in range(shelf_count)]} for _ in range(section_count)]
    return {
        "sides": {
            "Gauche": {"sections": json.loads(json.dumps(section_template))},
            "Droite": {"sections": json.loads(json.dumps(section_template))},
        }
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
            cleaned_shelves = []
            for shelf in shelves:
                cleaned_shelves.append(clamp_non_negative_int(shelf))
            normalized_sections.append({"shelves": cleaned_shelves})
        if not has_explicit_sections:
            normalized_sections = default["sides"][side]["sections"]
        normalized_sides[side] = {"sections": normalized_sections}

    return {"sides": normalized_sides}


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


def product_fits_layout(product, config):
    side = str(product["side"]).strip()
    section_index = clamp_non_negative_int(product.get("section", "0")) - 1
    shelf_index = clamp_non_negative_int(product.get("shelf", "0")) - 1
    position_value = clamp_non_negative_int(product.get("position", "0"))
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


def find_product_by_barcode(db, barcode, exclude_id=None):
    if not str(barcode or "").strip():
        return None
    for candidate in build_barcode_candidates(barcode):
        query = "SELECT id, name, aisle, side, section, shelf, position FROM products WHERE barcode=?"
        params = [candidate]
        if exclude_id is not None:
            query += " AND id<>?"
            params.append(int(exclude_id))
        query += " ORDER BY id LIMIT 1"
        row = db.execute(query, tuple(params)).fetchone()
        if row:
            return row
    return None


def integrity_conflict_message(exc):
    text = str(exc).lower()
    if "barcode" in text:
        return "Ce code-barres existe deja ailleurs dans la base."
    return "Cette position est deja occupee."


@app.route("/api/products", methods=["POST"])
def add_product():
    username, error = require_editor()
    if error:
        return error
    data = request.get_json()
    name     = data.get("name", "").strip()
    brand    = data.get("brand", "").strip()
    description = data.get("description", "").strip()
    image_url = data.get("image_url", "").strip()
    source_url = data.get("source_url", "").strip()
    search_terms = data.get("search_terms", "").strip()
    usage_notes = data.get("usage_notes", "").strip()
    alternative_suggestions = data.get("alternative_suggestions", "").strip()
    barcode  = data.get("barcode", "").strip()
    aisle    = data.get("aisle", "").strip()
    side     = data.get("side", "").strip()
    section  = data.get("section", "").strip() or "1"
    shelf    = data.get("shelf", "").strip()
    position = data.get("position", "").strip()

    if not all([name, aisle, side, section, shelf, position]):
        return jsonify({"error": "Champs obligatoires manquants"}), 400

    db = get_db()
    is_valid_slot, slot_error = validate_layout_slot(db, aisle, side, section, shelf, position)
    if not is_valid_slot:
        return jsonify({"error": slot_error}), 400
    occupied = find_product_at_position(db, aisle, side, section, shelf, position)
    if occupied:
        return jsonify({
            "error": f'Position deja occupee par "{occupied["name"]}" (code {occupied["barcode"] or "sans code"}).'
        }), 409
    duplicate_barcode = find_product_by_barcode(db, barcode)
    if duplicate_barcode:
        return jsonify({
            "error": f'Ce code-barres existe deja dans l allee {duplicate_barcode["aisle"]}, {duplicate_barcode["side"]}, section {duplicate_barcode["section"]}, tablette {duplicate_barcode["shelf"]}, position {duplicate_barcode["position"]}.'
        }), 409

    try:
        cursor = db.execute(
            """
            INSERT INTO products (name, brand, description, image_url, source_url, search_terms, usage_notes, alternative_suggestions, barcode, aisle, side, section, shelf, position, created_by, created_at, modified_by, modified_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                name,
                brand,
                description,
                image_url,
                source_url,
                search_terms,
                usage_notes,
                alternative_suggestions,
                barcode,
                aisle,
                side,
                section,
                shelf,
                position,
                username,
                utc_now_iso(),
                username,
                utc_now_iso(),
            )
        )
    except DatabaseIntegrityError as exc:
        return jsonify({"error": integrity_conflict_message(exc)}), 409
    db.commit()
    product_id = cursor.lastrowid
    product = db.execute("SELECT * FROM products WHERE id=?", (product_id,)).fetchone()
    return jsonify({
        "success": True,
        "message": f'"{name}" ajoute avec succes!',
        "product": row_to_product(product) if product else None
    })


@app.route("/api/products/<int:product_id>", methods=["PUT"])
def update_product(product_id):
    username, error = require_editor()
    if error:
        return error
    data = request.get_json()
    db = get_db()
    existing = db.execute("SELECT * FROM products WHERE id=?", (product_id,)).fetchone()
    if not existing:
        return jsonify({"error": "Produit non trouve"}), 404
    is_valid_slot, slot_error = validate_layout_slot(
        db,
        str(data["aisle"]).strip(),
        str(data["side"]).strip(),
        str(data.get("section", "1")).strip() or "1",
        str(data["shelf"]).strip(),
        str(data["position"]).strip(),
    )
    if not is_valid_slot:
        return jsonify({"error": slot_error}), 400
    occupied = find_product_at_position(
        db,
        str(data["aisle"]).strip(),
        str(data["side"]).strip(),
        str(data.get("section", "1")).strip() or "1",
        str(data["shelf"]).strip(),
        str(data["position"]).strip(),
        exclude_id=product_id,
    )
    if occupied:
        return jsonify({
            "error": f'Position deja occupee par "{occupied["name"]}" (code {occupied["barcode"] or "sans code"}).'
        }), 409
    duplicate_barcode = find_product_by_barcode(db, data.get("barcode", ""), exclude_id=product_id)
    if duplicate_barcode:
        return jsonify({
            "error": f'Ce code-barres existe deja dans l allee {duplicate_barcode["aisle"]}, {duplicate_barcode["side"]}, section {duplicate_barcode["section"]}, tablette {duplicate_barcode["shelf"]}, position {duplicate_barcode["position"]}.'
        }), 409

    try:
        result = db.execute(
            "UPDATE products SET name=?, brand=?, description=?, image_url=?, source_url=?, search_terms=?, usage_notes=?, alternative_suggestions=?, barcode=?, aisle=?, side=?, section=?, shelf=?, position=?, modified_by=?, modified_at=? WHERE id=?",
            (
                data["name"],
                data.get("brand", existing["brand"]),
                data.get("description", existing["description"]),
                data.get("image_url", existing["image_url"]),
                data.get("source_url", existing["source_url"]),
                data.get("search_terms", existing["search_terms"]),
                data.get("usage_notes", existing["usage_notes"]),
                data.get("alternative_suggestions", existing["alternative_suggestions"]),
                data.get("barcode", existing["barcode"]),
                data["aisle"],
                data["side"],
                data.get("section", "1"),
                data["shelf"],
                data["position"],
                username,
                utc_now_iso(),
                product_id,
            )
        )
    except DatabaseIntegrityError as exc:
        return jsonify({"error": integrity_conflict_message(exc)}), 409
    db.commit()
    product = db.execute("SELECT * FROM products WHERE id=?", (product_id,)).fetchone()
    return jsonify({"success": True, "product": row_to_product(product)})


@app.route("/api/products/<int:product_id>", methods=["DELETE"])
def delete_product(product_id):
    username, error = require_editor()
    if error:
        return error
    db = get_db()
    product = db.execute("SELECT name FROM products WHERE id=?", (product_id,)).fetchone()
    if not product:
        return jsonify({"error": "Produit non trouve."}), 404
    db.execute("DELETE FROM products WHERE id=?", (product_id,))
    db.commit()
    return jsonify({"success": True, "message": f'Produit supprimé par {username}: {product["name"]}'})



# ── Run ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    init_db()
    ssl_context = resolve_ssl_context()
    # host="0.0.0.0" lets phones and Zebra devices on the same network connect to this server
    debug_mode = os.environ.get("FLASK_DEBUG", "").strip().lower() in {"1", "true", "yes"}
    app.run(debug=debug_mode, host="0.0.0.0", port=5000, ssl_context=ssl_context)
