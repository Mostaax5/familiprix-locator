import os
from flask import Flask, render_template, request, jsonify, send_from_directory
from database import init_db, get_db
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import json
import re

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


# ── API: Products ──────────────────────────────────────────────────────────

@app.route("/api/products", methods=["GET"])
def get_products():
    db = get_db()
    products = db.execute("SELECT * FROM products ORDER BY aisle, side, shelf, position").fetchall()
    return jsonify([dict(p) for p in products])


@app.route("/api/products/search", methods=["GET"])
def search_products():
    query = request.args.get("q", "").strip().lower()
    db = get_db()
    products = db.execute(
        "SELECT * FROM products WHERE LOWER(name) LIKE ? OR LOWER(brand) LIKE ? OR barcode LIKE ?",
        (f"%{query}%", f"%{query}%", f"%{query}%")
    ).fetchall()
    return jsonify([dict(p) for p in products])


@app.route("/api/products/barcode/<barcode>", methods=["GET"])
def get_by_barcode(barcode):
    db = get_db()
    product = db.execute(
        "SELECT * FROM products WHERE barcode = ?", (barcode,)
    ).fetchone()
    if product:
        return jsonify(dict(product))
    return jsonify({"error": "Produit non trouvé"}), 404


@app.route("/api/products/lookup/<barcode>", methods=["GET"])
def lookup_barcode(barcode):
    barcode = barcode.strip()
    if not barcode:
        return jsonify({"found": False, "error": "Code-barres manquant"}), 400

    for source_name, source_base_url in PHARMACY_LOOKUP_SOURCES:
        if source_name == "Familiprix":
            product = lookup_familiprix_product(barcode)
        else:
            product = lookup_generic_pharmacy_product(source_name, source_base_url, barcode)
        if product:
            return jsonify({"found": True, "product": product})

    for source_name, base_url in PRODUCT_LOOKUP_SOURCES:
        product = lookup_open_facts_product(source_name, base_url, barcode)
        if product:
            return jsonify({"found": True, "product": product})

    return jsonify({"found": False, "error": "Aucun produit trouve en ligne"})


def lookup_familiprix_product(barcode):
    search_urls = [
        f"https://magasiner.familiprix.com/fr/search?text={barcode}",
        f"https://magasiner.familiprix.com/fr/search?q={barcode}",
    ]
    for url in search_urls:
        html, final_url = fetch_text(url)
        if not html:
            continue

        product_url = find_familiprix_product_url(html, final_url, barcode)
        if not product_url:
            continue

        product_html, product_final_url = fetch_text(product_url)
        if not product_html:
            product_html, product_final_url = html, final_url

        product = parse_familiprix_product_page(product_html, product_final_url, barcode)
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


def find_familiprix_product_url(html, final_url, barcode):
    if "/p/" in final_url and barcode in html:
        return final_url

    product_links = re.findall(r'href="([^"]+/p/[0-9]{6,}[^"]*)"', html)
    for link in product_links:
        absolute = normalize_familiprix_url(link)
        product_html, product_url = fetch_text(absolute)
        if product_html and barcode in product_html:
            return product_url or absolute

    return None


def normalize_familiprix_url(url):
    if url.startswith("http"):
        return url
    if url.startswith("/"):
        return f"https://magasiner.familiprix.com{url}"
    return f"https://magasiner.familiprix.com/{url}"


def lookup_generic_pharmacy_product(source_name, base_url, barcode):
    search_urls = [
        f"{base_url}/search?text={barcode}",
        f"{base_url}/search?q={barcode}",
        f"{base_url}/recherche?q={barcode}",
        f"{base_url}/fr/search?text={barcode}",
        f"{base_url}/fr/search?q={barcode}",
    ]
    for url in search_urls:
        html, final_url = fetch_text(url)
        if not html:
            continue

        if barcode in html and looks_like_product_page(final_url):
            product = parse_generic_pharmacy_product_page(source_name, html, final_url, barcode)
            if product:
                return product

        product_url = find_generic_product_url(html, base_url, barcode)
        if not product_url:
            continue

        product_html, product_final_url = fetch_text(product_url)
        if not product_html:
            continue

        product = parse_generic_pharmacy_product_page(source_name, product_html, product_final_url, barcode)
        if product:
            return product

    return None


def looks_like_product_page(url):
    url = (url or "").lower()
    return any(token in url for token in ["/p/", "/product", "/products/", "/shop/", "/item/"])


def find_generic_product_url(html, base_url, barcode):
    hrefs = re.findall(r'href="([^"]+)"', html)
    for href in hrefs:
        absolute = normalize_url(base_url, href)
        if not looks_like_product_page(absolute):
            continue
        product_html, product_url = fetch_text(absolute)
        if product_html and barcode in product_html:
            return product_url or absolute
    return None


def normalize_url(base_url, url):
    if url.startswith("http"):
        return url
    if url.startswith("/"):
        return f"{base_url}{url}"
    return f"{base_url}/{url}"


def parse_generic_pharmacy_product_page(source_name, html, url, barcode):
    if barcode not in html:
        return None

    title = clean_html_text(first_regex(html, [
        r"<h1[^>]*>(.*?)</h1>",
        r'<meta property="og:title" content="([^"]+)"',
        r"<title>(.*?)</title>",
    ]))
    title = sanitize_title(title, source_name)

    description = clean_html_text(first_regex(html, [
        r'<meta name="description" content="([^"]+)"',
        r'<meta property="og:description" content="([^"]+)"',
    ]))
    image_url = first_regex(html, [r'<meta property="og:image" content="([^"]+)"'])
    brand = infer_brand_from_title(title)

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


def parse_familiprix_product_page(html, url, barcode):
    if barcode not in html:
        return None

    title = first_regex(html, [
        r"<h1[^>]*>(.*?)</h1>",
        r'<meta property="og:title" content="([^"]+)"',
        r"<title>(.*?)</title>",
    ])
    title = sanitize_title(clean_html_text(title), "Familiprix")

    description = clean_html_text(first_regex(html, [
        r'<meta name="description" content="([^"]+)"',
        r'<meta property="og:description" content="([^"]+)"',
    ]))
    image_url = first_regex(html, [r'<meta property="og:image" content="([^"]+)"'])

    brand = infer_brand_from_title(title)

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


def find_product_at_position(db, aisle, side, shelf, position, exclude_id=None):
    query = "SELECT * FROM products WHERE aisle=? AND side=? AND shelf=? AND position=?"
    params = [aisle, side, shelf, position]
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


@app.route("/api/products", methods=["POST"])
def add_product():
    data = request.get_json()
    name     = data.get("name", "").strip()
    brand    = data.get("brand", "").strip()
    description = data.get("description", "").strip()
    barcode  = data.get("barcode", "").strip()
    aisle    = data.get("aisle", "").strip()
    side     = data.get("side", "").strip()
    shelf    = data.get("shelf", "").strip()
    position = data.get("position", "").strip()

    if not all([name, aisle, side, shelf, position]):
        return jsonify({"error": "Champs obligatoires manquants"}), 400

    db = get_db()
    occupied = find_product_at_position(db, aisle, side, shelf, position)
    if occupied:
        return jsonify({
            "error": f'Position deja occupee par "{occupied["name"]}" (code {occupied["barcode"] or "sans code"}).'
        }), 409

    cursor = db.execute(
        "INSERT INTO products (name, brand, description, barcode, aisle, side, shelf, position) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (name, brand, description, barcode, aisle, side, shelf, position)
    )
    db.commit()
    product_id = cursor.lastrowid
    product = db.execute("SELECT * FROM products WHERE id=?", (product_id,)).fetchone()
    return jsonify({
        "success": True,
        "message": f'"{name}" ajoute avec succes!',
        "product": dict(product) if product else None
    })


@app.route("/api/products/<int:product_id>", methods=["PUT"])
def update_product(product_id):
    data = request.get_json()
    db = get_db()
    occupied = find_product_at_position(
        db,
        str(data["aisle"]).strip(),
        str(data["side"]).strip(),
        str(data["shelf"]).strip(),
        str(data["position"]).strip(),
        exclude_id=product_id,
    )
    if occupied:
        return jsonify({
            "error": f'Position deja occupee par "{occupied["name"]}" (code {occupied["barcode"] or "sans code"}).'
        }), 409

    result = db.execute(
        "UPDATE products SET name=?, brand=?, description=?, barcode=?, aisle=?, side=?, shelf=?, position=? WHERE id=?",
        (
            data["name"],
            data.get("brand", ""),
            data.get("description", ""),
            data.get("barcode", ""),
            data["aisle"],
            data["side"],
            data["shelf"],
            data["position"],
            product_id,
        )
    )
    db.commit()
    if result.rowcount == 0:
        return jsonify({"error": "Produit non trouve"}), 404
    return jsonify({"success": True})


@app.route("/api/products/bulk", methods=["PUT"])
def bulk_update_products():
    data = request.get_json() or {}
    products = data.get("products", [])

    if not isinstance(products, list):
        return jsonify({"error": "Liste de produits invalide"}), 400

    required = ["id", "name", "aisle", "side", "shelf", "position"]
    db = get_db()
    seen_positions = {}
    for product in products:
        if not all(str(product.get(field, "")).strip() for field in required):
            return jsonify({"error": "Produit incomplet dans la mise a jour"}), 400
        key = (
            str(product["aisle"]).strip(),
            str(product["side"]).strip(),
            str(product["shelf"]).strip(),
            str(product["position"]).strip(),
        )
        if key in seen_positions and int(product["id"]) != seen_positions[key]:
            return jsonify({"error": "Deux produits visent la meme position dans cette mise a jour"}), 409
        seen_positions[key] = int(product["id"])

        occupied = find_product_at_position(db, key[0], key[1], key[2], key[3], exclude_id=int(product["id"]))
        if occupied:
            return jsonify({
                "error": f'Position deja occupee par "{occupied["name"]}" (code {occupied["barcode"] or "sans code"}).'
            }), 409
        db.execute(
            "UPDATE products SET name=?, brand=?, description=?, barcode=?, aisle=?, side=?, shelf=?, position=? WHERE id=?",
            (
                str(product["name"]).strip(),
                str(product.get("brand", "")).strip(),
                str(product.get("description", "")).strip(),
                str(product.get("barcode", "")).strip(),
                str(product["aisle"]).strip(),
                str(product["side"]).strip(),
                str(product["shelf"]).strip(),
                str(product["position"]).strip(),
                int(product["id"]),
            )
        )
    db.commit()
    return jsonify({"success": True, "updated": len(products)})


@app.route("/api/products/<int:product_id>", methods=["DELETE"])
def delete_product(product_id):
    db = get_db()
    db.execute("DELETE FROM products WHERE id=?", (product_id,))
    db.commit()
    return jsonify({"success": True})


@app.route("/api/aisles", methods=["GET"])
def get_aisles():
    db = get_db()
    aisles = db.execute(
        "SELECT aisle, COUNT(*) as count FROM products GROUP BY aisle ORDER BY CAST(aisle AS INTEGER)"
    ).fetchall()
    return jsonify([dict(a) for a in aisles])


# ── Run ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    init_db()
    ssl_context = resolve_ssl_context()
    # host="0.0.0.0" lets phones and Zebra devices on the same network connect to this server
    app.run(debug=True, host="0.0.0.0", port=5000, ssl_context=ssl_context)
