import json
import os
import re
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from flask import Blueprint, request, jsonify, Response
from database import get_db
from auth import require_editor, utc_now_iso, side_display_label

ai_bp = Blueprint("ai", __name__)


def log_ai_interaction(kind, question, context, response):
    """Persist every AI Q&A as a training example, tagged with store, employee
    (auto from device name — never prompted) and time. Never raises — logging
    must not break the user-facing response."""
    try:
        prov = configured_ai_provider()
        body = request.get_json(silent=True) or {}
        store = str(body.get("store", "")).strip()
        employee = (request.headers.get("X-User-Name") or body.get("_username") or "").strip()
        db = get_db()
        db.execute(
            """INSERT INTO ai_logs (created_at, kind, provider, model, question, context_json, response_json, store, employee)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (utc_now_iso(), kind, prov["name"], prov["model"], str(question or ""),
             json.dumps(context, ensure_ascii=False) if context is not None else "",
             json.dumps(response, ensure_ascii=False) if response is not None else "",
             store, employee),
        )
        db.commit()
    except Exception:
        pass

# ── Constants ──────────────────────────────────────────────────────────────────

PHARMACY_LOOKUP_SOURCES = [
    ("Jean Coutu", "https://www.jeancoutu.com"),
    ("Brunet", "https://www.brunet.ca"),
    ("Pharmaprix", "https://www.pharmaprix.ca"),
]
FAMILIPRIX_BASE_URL = "https://magasiner.familiprix.com"
PRODUCT_LOOKUP_SOURCES = [
    ("Open Products Facts", "https://world.openproductsfacts.org"),
    ("Open Beauty Facts", "https://world.openbeautyfacts.org"),
    ("Open Food Facts", "https://world.openfoodfacts.org"),
    ("Open Drug Facts", "https://world.opendrugfacts.org"),
]
LOOKUP_FIELDS = [
    "code", "product_name", "product_name_fr", "product_name_en",
    "generic_name", "generic_name_fr", "brands", "quantity", "categories",
    "ingredients_text_fr", "ingredients_text", "labels", "url", "image_front_url",
]

GEMINI_API_KEY  = os.environ.get("GEMINI_API_KEY",  "").strip()
GEMINI_MODEL    = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash-lite").strip() or "gemini-2.5-flash-lite"
GEMINI_BASE_URL = os.environ.get("GEMINI_BASE_URL", "https://generativelanguage.googleapis.com/v1beta").rstrip("/")
OPENAI_API_KEY  = os.environ.get("OPENAI_API_KEY",  "").strip()
OPENAI_MODEL    = os.environ.get("OPENAI_MODEL", "gpt-4o-mini").strip() or "gpt-4o-mini"
OPENAI_BASE_URL = os.environ.get("OPENAI_BASE_URL", "https://api.openai.com/v1").rstrip("/")

_GEMINI_INPUT_COST_PER_M  = 0.075
_GEMINI_OUTPUT_COST_PER_M = 0.30
_OPENAI_INPUT_COST_PER_M  = 0.15
_OPENAI_OUTPUT_COST_PER_M = 0.60

_AI_RATE_LIMIT  = int(os.environ.get("AI_RATE_LIMIT",  "30"))
_AI_RATE_WINDOW = int(os.environ.get("AI_RATE_WINDOW", "3600"))
_ai_rate_buckets: dict = defaultdict(list)

_SIMPLE_ANSWERS = {
    "heure":         "Pour les heures d’ouverture, consultez votre succursale Familiprix locale ou familiprix.com.",
    "ouvert":        "Pour les heures d’ouverture, consultez votre succursale Familiprix locale ou familiprix.com.",
    "ferm":          "Pour les heures d’ouverture, consultez votre succursale Familiprix locale ou familiprix.com.",
    "livraison":     "La livraison varie selon les succursales. Contactez directement votre pharmacie Familiprix.",
    "telephone":     "Le numéro de téléphone est affiché à l’entrée du magasin ou sur familiprix.com.",
    "adresse":       "L’adresse se trouve sur familiprix.com dans le localisateur de pharmacies.",
    "retour":        "La politique de retour varie. Adressez-vous au comptoir de votre succursale.",
    "stationnement": "Renseignez-vous directement auprès de votre succursale pour le stationnement.",
    "pharmacien":    "Pour parler a un pharmacien, présentez-vous au comptoir de la pharmacie.",
}


# ── AI cost helpers ────────────────────────────────────────────────────────────

def _check_ai_rate_limit() -> bool:
    ip = (request.headers.get("X-Forwarded-For") or request.remote_addr or "unknown").split(",")[0].strip()
    now = time.time()
    cutoff = now - _AI_RATE_WINDOW
    _ai_rate_buckets[ip] = [t for t in _ai_rate_buckets[ip] if t > cutoff]
    if len(_ai_rate_buckets[ip]) >= _AI_RATE_LIMIT:
        return False
    _ai_rate_buckets[ip].append(now)
    return True


def _try_simple_answer(question: str):
    q = question.lower()
    for keyword, answer in _SIMPLE_ANSWERS.items():
        if keyword in q:
            return answer
    return None


def _log_ai_usage(provider: str, input_tokens: int, output_tokens: int, question_preview: str = "") -> None:
    if provider == "gemini":
        cost = (input_tokens * _GEMINI_INPUT_COST_PER_M + output_tokens * _GEMINI_OUTPUT_COST_PER_M) / 1_000_000
    else:
        cost = (input_tokens * _OPENAI_INPUT_COST_PER_M + output_tokens * _OPENAI_OUTPUT_COST_PER_M) / 1_000_000
    preview = question_preview[:60].replace("\n", " ")
    print(f"[AI-COST] provider={provider} model={GEMINI_MODEL if provider=='gemini' else OPENAI_MODEL} "
          f"in={input_tokens} out={output_tokens} cost=${cost:.6f} q=\"{preview}\"")


def configured_ai_provider():
    if GEMINI_API_KEY:
        return {"name": "gemini", "label": "Gemini", "model": GEMINI_MODEL}
    if OPENAI_API_KEY:
        return {"name": "openai", "label": "OpenAI", "model": OPENAI_MODEL}
    return {"name": "", "label": "", "model": ""}


# ── Lookup helpers ─────────────────────────────────────────────────────────────

def normalized_digits(value):
    return re.sub(r"\D", "", str(value or ""))


def build_barcode_candidates(barcode):
    from routes.products import build_barcode_candidates as _bbc
    return _bbc(barcode)


# Score a lookup result by how trustworthy/complete it is, so we can pick the
# BEST result across all sources instead of whichever replied first.
_SOURCE_TRUST = {
    "familiprix": 9, "open beauty facts": 8, "open drug facts": 8,
    "open products facts": 8, "open food facts": 7, "datakick": 6, "upc item db": 5,
    "brocade": 5, "barcode lookup": 4, "go upc": 4, "ean search": 3,
}
_PLACEHOLDER_BITS = ("unknown", "not found", "no title", "n/a", "untitled")


def _product_quality_score(p):
    if not p:
        return 0
    name = str(p.get("name", "")).strip()
    if len(name) < 3:
        return 0
    low = name.lower()
    score = 10
    # name informativeness
    if len(name) >= 6:  score += 5
    if len(name) >= 12: score += 4
    if " " in name:     score += 3            # multi-word names are real product names
    # the name shouldn't just be the barcode digits or a placeholder
    if any(b in low for b in _PLACEHOLDER_BITS):
        score -= 10
    name_digits = re.sub(r"\D", "", name)
    bc_digits = re.sub(r"\D", "", str(p.get("barcode", "")))
    if name_digits and bc_digits and name_digits in bc_digits:
        score -= 8
    # completeness
    if str(p.get("brand", "")).strip():       score += 6
    if str(p.get("image_url", "")).strip():   score += 5
    if str(p.get("description", "")).strip(): score += 3
    # source reliability
    src = str(p.get("source", "")).lower()
    for key, weight in _SOURCE_TRUST.items():
        if key in src:
            score += weight
            break
    return score


def best_lookup_result(tasks, max_workers=8):
    """Run all tasks, return (best_product, best_score) by quality score."""
    best, best_score = None, 0
    if not tasks:
        return None, 0
    with ThreadPoolExecutor(max_workers=min(max_workers, len(tasks))) as executor:
        futures = [executor.submit(task) for task in tasks]
        for future in as_completed(futures):
            try:
                result = future.result()
            except Exception:
                result = None
            s = _product_quality_score(result)
            if s > best_score:
                best, best_score = result, s
    return best, best_score


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


def first_regex(text, patterns):
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
        if match:
            return match.group(1)
    return ""


def clean_html_text(value):
    value = re.sub(r"<[^>]+>", " ", value or "")
    value = (value.replace("&amp;", "&").replace("&quot;", '"')
             .replace("&#39;", "'").replace("&nbsp;", " "))
    return re.sub(r"\s+", " ", value).strip()


def sanitize_title(title, source_name):
    title = title or ""
    suffixes = [
        f"| {source_name}", f"- {source_name}",
        f"| {source_name} Pharmacy", f"| {source_name} Pharmacie",
        "| Jean Coutu", "| Brunet", "| Pharmaprix", "| Familiprix",
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


def infer_brand_from_title(title):
    parts = (title or "").split()
    return parts[0] if parts else ""


def first_present(product, keys):
    for key in keys:
        value = str(product.get(key, "")).strip()
        if value:
            return value
    return ""


def looks_like_product_page(url):
    url = (url or "").lower()
    return any(token in url for token in ["/p/", "/product", "/products/", "/shop/", "/item/"])


def normalize_url(base_url, url):
    if url.startswith("http"):
        return url
    if url.startswith("/"):
        return f"{base_url}{url}"
    return f"{base_url}/{url}"


def normalize_familiprix_url(url):
    if url.startswith("http"):
        return url
    if url.startswith("/"):
        return f"https://magasiner.familiprix.com{url}"
    return f"https://magasiner.familiprix.com/{url}"


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
            return {"name": name, "brand": brand, "description": description, "image_url": image_url}
    return {}


def _find_product_node(obj, barcode_candidates, depth):
    if depth > 10:
        return {}
    if isinstance(obj, dict):
        name = str(obj.get("name") or obj.get("title") or obj.get("productName") or "").strip()
        if name and len(name) > 3:
            obj_text = json.dumps(obj)
            for candidate in (barcode_candidates or []):
                cd = normalized_digits(candidate)
                if cd and cd in normalized_digits(obj_text):
                    raw_brand = obj.get("brand") or obj.get("brandName") or ""
                    brand = str(raw_brand.get("name", "") if isinstance(raw_brand, dict) else raw_brand).strip()
                    description = clean_html_text(str(obj.get("description") or obj.get("shortDescription") or ""))
                    image = str(obj.get("image") or obj.get("imageUrl") or obj.get("thumbnail") or "").strip()
                    return {"name": name, "brand": brand, "description": description, "image_url": image}
        for value in obj.values():
            if isinstance(value, (dict, list)):
                result = _find_product_node(value, barcode_candidates, depth + 1)
                if result:
                    return result
    elif isinstance(obj, list):
        for item in obj:
            if isinstance(item, (dict, list)):
                result = _find_product_node(item, barcode_candidates, depth + 1)
                if result:
                    return result
    return {}


def extract_embedded_json_product(html, barcode_candidates):
    for pattern in [
        r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>',
        r'window\.__INITIAL_STATE__\s*=\s*(\{.*?\});\s*(?:</script>|window\.)',
    ]:
        match = re.search(pattern, html, flags=re.DOTALL | re.IGNORECASE)
        if not match:
            continue
        try:
            data = json.loads(match.group(1))
        except json.JSONDecodeError:
            continue
        result = _find_product_node(data, barcode_candidates, 0)
        if result:
            return result
    return {}


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


def parse_familiprix_product_page(html, url, barcode, barcode_candidates=None):
    barcode_candidates = barcode_candidates or build_barcode_candidates(barcode)
    if not page_mentions_barcode(html, barcode_candidates):
        return None
    embedded = extract_embedded_json_product(html, barcode_candidates)
    structured = extract_structured_product_data(html, barcode_candidates)
    title = embedded.get("name") or structured.get("name") or first_regex(html, [
        r"<h1[^>]*>(.*?)</h1>", r'<meta property="og:title" content="([^"]+)"', r"<title>(.*?)</title>",
    ])
    title = sanitize_title(clean_html_text(title), "Familiprix")
    description = structured.get("description") or clean_html_text(first_regex(html, [
        r'<meta name="description" content="([^"]+)"', r'<meta property="og:description" content="([^"]+)"',
    ]))
    image_url = structured.get("image_url") or first_regex(html, [r'<meta property="og:image" content="([^"]+)"'])
    brand = structured.get("brand") or infer_brand_from_title(title)
    if not title:
        return None
    return {"name": title, "brand": brand, "description": description, "barcode": barcode,
            "source": "Familiprix", "source_url": url, "image_url": image_url}


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


def parse_generic_pharmacy_product_page(source_name, html, url, barcode, barcode_candidates=None):
    barcode_candidates = barcode_candidates or build_barcode_candidates(barcode)
    if not page_mentions_barcode(html, barcode_candidates):
        return None
    embedded = extract_embedded_json_product(html, barcode_candidates)
    structured = extract_structured_product_data(html, barcode_candidates)
    title = sanitize_title(embedded.get("name") or structured.get("name") or clean_html_text(first_regex(html, [
        r"<h1[^>]*>(.*?)</h1>", r'<meta property="og:title" content="([^"]+)"', r"<title>(.*?)</title>",
    ])), source_name)
    description = structured.get("description") or clean_html_text(first_regex(html, [
        r'<meta name="description" content="([^"]+)"', r'<meta property="og:description" content="([^"]+)"',
    ]))
    image_url = structured.get("image_url") or first_regex(html, [r'<meta property="og:image" content="([^"]+)"'])
    brand = structured.get("brand") or infer_brand_from_title(title)
    if not title:
        return None
    return {"name": title, "brand": brand, "description": description, "barcode": barcode,
            "source": source_name, "source_url": url, "image_url": image_url}


def lookup_generic_pharmacy_product(source_name, base_url, barcode, barcode_candidates=None):
    barcode_candidates = barcode_candidates or build_barcode_candidates(barcode)
    search_urls = [
        f"{base_url}/search?text={barcode}", f"{base_url}/search?q={barcode}",
        f"{base_url}/recherche?q={barcode}", f"{base_url}/recherche?text={barcode}",
        f"{base_url}/fr/search?text={barcode}", f"{base_url}/fr/search?q={barcode}",
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


def lookup_open_facts_product(source_name, base_url, barcode):
    params = urlencode({"fields": ",".join(LOOKUP_FIELDS)})
    url = f"{base_url}/api/v2/product/{barcode}.json?{params}"
    request_obj = Request(url, headers={"User-Agent": "FamiliprixLocator/0.1 (local testing)", "Accept": "application/json"})
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
    labels = first_present(product, ["labels"])
    ingredients = first_present(product, ["ingredients_text_fr", "ingredients_text"])
    if ingredients and len(ingredients) > 180:
        ingredients = ingredients[:180].rsplit(" ", 1)[0] + "…"
    description_parts = [part for part in [generic_name, quantity, categories, labels,
                                           (f"Ingrédients: {ingredients}" if ingredients else "")] if part]
    if not name and not brand:
        return None
    return {"name": name or brand, "brand": brand, "description": " | ".join(description_parts),
            "barcode": barcode, "source": source_name, "source_url": product.get("url", ""),
            "image_url": product.get("image_front_url", "")}


def lookup_upcitemdb(barcode):
    digits = normalized_digits(barcode)
    if not digits:
        return None
    request_obj = Request(f"https://api.upcitemdb.com/prod/trial/lookup?upc={digits}",
                          headers={"User-Agent": "FamiliprixLocator/0.1", "Accept": "application/json"})
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
    return {"name": name, "brand": brand, "description": description, "barcode": digits,
            "source": "UPC Item DB", "source_url": f"https://www.upcitemdb.com/upc/{digits}", "image_url": image_url}


def lookup_ean_search(barcode):
    digits = normalized_digits(barcode)
    if not digits:
        return None
    request_obj = Request(
        f"https://api.ean-search.org/api?op=barcode-lookup&ean={digits}&lang=1&format=json",
        headers={"User-Agent": "FamiliprixLocator/0.1", "Accept": "application/json"},
    )
    try:
        with urlopen(request_obj, timeout=5) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError):
        return None
    items = payload if isinstance(payload, list) else []
    if not items:
        return None
    item = items[0]
    name = str(item.get("name", "")).strip()
    if not name or name.lower() in {"unknown", "n/a", ""}:
        return None
    return {"name": name, "brand": infer_brand_from_title(name), "description": "", "barcode": digits,
            "source": "EAN Search", "source_url": f"https://www.ean-search.org/perl/ean-search.pl?q={digits}", "image_url": ""}


def lookup_barcodelookup(barcode):
    digits = normalized_digits(barcode)
    if not digits:
        return None
    url = f"https://www.barcodelookup.com/{digits}"
    request_obj = Request(url, headers={
        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1",
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.9",
        "Accept-Language": "fr-CA,fr;q=0.9,en;q=0.6",
    })
    try:
        with urlopen(request_obj, timeout=8) as response:
            html = response.read().decode("utf-8", errors="ignore")
    except (HTTPError, URLError, TimeoutError):
        return None
    candidates = build_barcode_candidates(barcode)
    structured = extract_structured_product_data(html, candidates)
    name = structured.get("name") or clean_html_text(first_regex(html, [
        r'<h4[^>]*class="[^"]*product-name[^"]*"[^>]*>(.*?)</h4>',
        r'<h1[^>]*>(.*?)</h1>', r'<meta property="og:title" content="([^"]+)"',
    ]))
    if not name or len(name) < 3 or "not found" in name.lower():
        return None
    brand = structured.get("brand") or clean_html_text(first_regex(html, [
        r'<span[^>]*class="[^"]*brand[^"]*"[^>]*>(.*?)</span>',
        r'<p[^>]*class="[^"]*brand[^"]*"[^>]*>(.*?)</p>',
    ]))
    description = structured.get("description") or clean_html_text(first_regex(html, [
        r'<meta name="description" content="([^"]+)"', r'<meta property="og:description" content="([^"]+)"',
    ]))
    image_url = structured.get("image_url") or first_regex(html, [r'<meta property="og:image" content="([^"]+)"'])
    return {"name": name.strip(), "brand": brand or infer_brand_from_title(name), "description": description,
            "barcode": digits, "source": "Barcode Lookup", "source_url": url, "image_url": image_url or ""}


def lookup_go_upc(barcode):
    digits = normalized_digits(barcode)
    if not digits:
        return None
    url = f"https://go-upc.com/barcode/{digits}"
    request_obj = Request(url, headers={
        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1",
        "Accept": "text/html,*/*", "Accept-Language": "fr-CA,fr;q=0.9,en;q=0.6",
    })
    try:
        with urlopen(request_obj, timeout=8) as response:
            html = response.read().decode("utf-8", errors="ignore")
    except (HTTPError, URLError, TimeoutError):
        return None
    candidates = build_barcode_candidates(barcode)
    structured = extract_structured_product_data(html, candidates)
    name = structured.get("name") or clean_html_text(first_regex(html, [
        r'<h1[^>]*class="[^"]*product-name[^"]*"[^>]*>(.*?)</h1>',
        r'<h1[^>]*>(.*?)</h1>', r'<meta property="og:title" content="([^"]+)"',
    ]))
    if not name or len(name) < 3 or "not found" in name.lower() or name.lower().startswith("barcode"):
        return None
    brand = structured.get("brand") or clean_html_text(first_regex(html, [r'class="[^"]*brand[^"]*"[^>]*>\s*(.*?)\s*</\w+>']))
    description = structured.get("description") or clean_html_text(first_regex(html, [
        r'<meta name="description" content="([^"]+)"', r'<meta property="og:description" content="([^"]+)"',
    ]))
    image_url = structured.get("image_url") or first_regex(html, [r'<meta property="og:image" content="([^"]+)"'])
    return {"name": name.strip(), "brand": brand or infer_brand_from_title(name), "description": description,
            "barcode": digits, "source": "Go UPC", "source_url": url, "image_url": image_url or ""}


def _fetch_json(url, timeout=5):
    req = Request(url, headers={"User-Agent": "FamiliprixLocator/0.1", "Accept": "application/json"})
    try:
        with urlopen(req, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8", errors="ignore"))
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError, ValueError):
        return None


def lookup_datakick(barcode):
    """Datakick open product database — free JSON API, no key."""
    digits = normalized_digits(barcode)
    if not digits:
        return None
    data = _fetch_json(f"https://www.datakick.org/api/items/{digits}")
    if not isinstance(data, dict):
        return None
    name = str(data.get("name", "")).strip()
    if not name or len(name) < 3:
        return None
    brand = str(data.get("brand_name", "") or "").strip()
    size = str(data.get("size", "") or "").strip()
    images = data.get("images") if isinstance(data.get("images"), list) else []
    image_url = str((images[0] or {}).get("url", "")).strip() if images and isinstance(images[0], dict) else ""
    return {"name": name, "brand": brand, "description": size, "barcode": digits,
            "source": "Datakick", "source_url": f"https://www.datakick.org/gtins/{digits}", "image_url": image_url}


def lookup_brocade(barcode):
    """Brocade open barcode database — free JSON API, no key."""
    digits = normalized_digits(barcode)
    if not digits:
        return None
    data = _fetch_json(f"https://www.brocade.io/api/items/{digits}")
    if not isinstance(data, dict):
        return None
    name = str(data.get("name", "")).strip()
    if not name or len(name) < 3:
        return None
    brand = str(data.get("brand_name", "") or data.get("brand", "") or "").strip()
    return {"name": name, "brand": brand, "description": "", "barcode": digits,
            "source": "Brocade", "source_url": f"https://www.brocade.io/items/{digits}", "image_url": ""}


# ── AI payload helpers ─────────────────────────────────────────────────────────

def product_context_for_client_help(product):
    return {
        "name":     str(product.get("name", "")).strip(),
        "brand":    str(product.get("brand", "")).strip(),
        "notes":    str(product.get("usage_notes", "") or product.get("description", "")).strip(),
        "location": f"Allée {str(product.get('aisle','')).strip()} {side_display_label(product.get('side',''))} T{str(product.get('shelf','')).strip()}",
    }


def generate_product_assist_payload(name, brand, description, barcode):
    provider = configured_ai_provider()
    if provider["name"] == "gemini":
        return generate_product_assist_payload_gemini(name, brand, description, barcode)
    if provider["name"] == "openai":
        return generate_product_assist_payload_openai(name, brand, description, barcode)
    return None


def generate_client_help_payload(question, products):
    provider = configured_ai_provider()
    if provider["name"] == "gemini":
        return generate_client_help_payload_gemini(question, products)
    if provider["name"] == "openai":
        return generate_client_help_payload_openai(question, products)
    return None


def generate_client_help_payload_gemini(question, products):
    payload = {
        "contents": [{"parts": [{"text": (
            "Tu aides un employe de pharmacie Familiprix au Quebec a repondre a un client. "
            "Donne toujours le meilleur conseil possible. "
            "Base-toi UNIQUEMENT sur les produits fournis dans la liste. "
            "Ne propose jamais un produit qui n’est pas dans la liste fournie. "
            "Si un produit de la liste est de marque Biomedic ou Essentiel (marques maison Familiprix), precise-le dans ta réponse. "
            "Si la liste est vide, donne un conseil général en pharmacie sans nommer de produits specifiques. "
            "Ne pose pas de diagnostic. "
            "Dis clairement quand il faut orienter le client vers le pharmacien: "
            "grossesse, bebe, interaction medicamenteuse, symptomes graves, douleur importante, "
            "difficulte respiratoire, fievre élevée, duree inhabituelle ou doute medical. "
            "Dans recommended_product_names, mets UNIQUEMENT les noms de produits presents dans la liste fournie. "
            "Retourne uniquement un JSON en francais avec exactement les clés "
            "summary (texte), recommended_product_names (tableau), follow_up_questions (tableau), "
            f"safety_flags (tableau), pharmacist_referral (booleen) et pharmacist_reason (texte).\n\n"
            f"Question client:\n{question}\n\n"
            f"Produits disponibles en magasin:\n{json.dumps(products, ensure_ascii=False) if products else '[]'}"
        )}]}],
        "generationConfig": {"temperature": 0.2, "responseMimeType": "application/json", "maxOutputTokens": 600},
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
    usage = raw_response.get("usageMetadata", {})
    _log_ai_usage("gemini", usage.get("promptTokenCount", 0), usage.get("candidatesTokenCount", 0), question)
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
            "Donne toujours le meilleur conseil possible. "
            "Base-toi UNIQUEMENT sur les produits fournis dans la liste. "
            "Ne propose jamais un produit qui n’est pas dans la liste fournie. "
            "Si un produit de la liste est de marque Biomedic ou Essentiel (marques maison Familiprix), precise-le dans ta réponse. "
            "Si la liste est vide, donne un conseil général en pharmacie sans nommer de produits specifiques. "
            "Ne pose pas de diagnostic. "
            "Dis clairement quand il faut orienter le client vers le pharmacien: "
            "grossesse, bebe, interaction medicamenteuse, symptomes graves, douleur importante, "
            "difficulte respiratoire, fievre élevée, duree inhabituelle ou doute medical. "
            "Dans recommended_product_names, mets UNIQUEMENT les noms de produits presents dans la liste fournie. "
            "Retourne uniquement un JSON en francais."
        ),
        "input": json.dumps({"question": question, "products": products}, ensure_ascii=False),
        "text": {"format": {
            "type": "json_schema", "name": "client_help", "strict": True,
            "schema": {
                "type": "object",
                "properties": {
                    "summary": {"type": "string"},
                    "recommended_product_names": {"type": "array", "items": {"type": "string"}, "maxItems": 4},
                    "follow_up_questions":       {"type": "array", "items": {"type": "string"}, "maxItems": 4},
                    "safety_flags":              {"type": "array", "items": {"type": "string"}, "maxItems": 4},
                    "pharmacist_referral": {"type": "boolean"},
                    "pharmacist_reason":   {"type": "string"},
                },
                "required": ["summary", "recommended_product_names", "follow_up_questions",
                             "safety_flags", "pharmacist_referral", "pharmacist_reason"],
                "additionalProperties": False,
            },
        }},
    }
    request_obj = Request(
        f"{OPENAI_BASE_URL}/responses",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request_obj, timeout=14) as response:
            raw_response = json.loads(response.read().decode("utf-8"))
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError):
        return None
    usage = raw_response.get("usage", {})
    _log_ai_usage("openai", usage.get("input_tokens", 0), usage.get("output_tokens", 0), question)
    raw_text = extract_openai_output_text(raw_response)
    if not raw_text:
        return None
    try:
        parsed = json.loads(raw_text)
    except json.JSONDecodeError:
        return None
    return normalize_client_help_payload(parsed)


def generate_product_assist_payload_gemini(name, brand, description, barcode):
    prompt = {"name": name, "brand": brand, "description": description, "barcode": barcode}
    payload = {
        "contents": [{"parts": [{"text": (
            "Tu aides les employes d’une pharmacie Familiprix au Quebec. "
            "Retourne uniquement un JSON en francais avec exactement les clés "
            "search_terms (tableau), usage_notes (texte) et alternative_suggestions (tableau). "
            "Les mots clés doivent etre des mots que les clients utilisent, "
            "usage_notes doit etre une courte explication utile pour guider un client, "
            "et alternative_suggestions doit contenir quelques alternatives possibles. "
            f"Sois concis, concret, prudent sur le plan medical et ne donne pas de diagnostic.\n\nProduit:\n{json.dumps(prompt, ensure_ascii=False)}"
        )}]}],
        "generationConfig": {"temperature": 0.2, "responseMimeType": "application/json", "maxOutputTokens": 400},
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
    usage = raw_response.get("usageMetadata", {})
    _log_ai_usage("gemini", usage.get("promptTokenCount", 0), usage.get("candidatesTokenCount", 0), name)
    raw_text = extract_gemini_output_text(raw_response)
    if not raw_text:
        return None
    try:
        parsed = json.loads(raw_text)
    except json.JSONDecodeError:
        return None
    return normalize_assist_payload(parsed)


def generate_product_assist_payload_openai(name, brand, description, barcode):
    prompt = {"name": name, "brand": brand, "description": description, "barcode": barcode}
    payload = {
        "model": OPENAI_MODEL,
        "reasoning": {"effort": "low"},
        "instructions": (
            "Tu aides les employes d’une pharmacie Familiprix au Quebec. "
            "Retourne un JSON en francais avec des mots clés que les clients utilisent, "
            "une courte explication utile pour guider un client, et quelques alternatives possibles. "
            "Sois concis, concret, prudent sur le plan medical et ne donne pas de diagnostic."
        ),
        "input": json.dumps(prompt, ensure_ascii=False),
        "text": {"format": {
            "type": "json_schema", "name": "product_assist", "strict": True,
            "schema": {
                "type": "object",
                "properties": {
                    "search_terms":           {"type": "array", "items": {"type": "string"}, "maxItems": 12},
                    "usage_notes":            {"type": "string"},
                    "alternative_suggestions":{"type": "array", "items": {"type": "string"}, "maxItems": 6},
                },
                "required": ["search_terms", "usage_notes", "alternative_suggestions"],
                "additionalProperties": False,
            },
        }},
    }
    request_obj = Request(
        f"{OPENAI_BASE_URL}/responses",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request_obj, timeout=12) as response:
            raw_response = json.loads(response.read().decode("utf-8"))
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError):
        return None
    usage = raw_response.get("usage", {})
    _log_ai_usage("openai", usage.get("input_tokens", 0), usage.get("output_tokens", 0), name)
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


def lookup_product_online(barcode):
    """Best-match product lookup across all sources. Returns a product dict or None.
    Reused by the lookup route and the image backfill."""
    barcode = str(barcode or "").strip()
    if not barcode:
        return None
    barcode_candidates = build_barcode_candidates(barcode)
    GOOD_ENOUGH = 24   # score at which we're confident and can stop early
    best, best_score = None, 0

    # Phase 1 — fast structured APIs; keep the highest-quality, not the fastest.
    json_tasks = []
    for candidate in barcode_candidates:
        json_tasks.append(lambda bc=candidate: lookup_upcitemdb(bc))
        json_tasks.append(lambda bc=candidate: lookup_ean_search(bc))
        json_tasks.append(lambda bc=candidate: lookup_datakick(bc))
        json_tasks.append(lambda bc=candidate: lookup_brocade(bc))
        for source_name, base_url in PRODUCT_LOOKUP_SOURCES:
            json_tasks.append(lambda bc=candidate, sn=source_name, su=base_url: lookup_open_facts_product(sn, su, bc))
    p1, s1 = best_lookup_result(json_tasks, max_workers=12)
    if s1 > best_score:
        best, best_score = p1, s1

    # Phase 2 — Familiprix catalog + generic barcode databases, if not confident.
    if best_score < GOOD_ENOUGH:
        tasks = []
        for candidate in barcode_candidates:
            tasks.append(lambda bc=candidate, bcs=barcode_candidates: lookup_familiprix_product(bc, bcs))
            tasks.append(lambda bc=candidate: lookup_barcodelookup(bc))
            tasks.append(lambda bc=candidate: lookup_go_upc(bc))
        p2, s2 = best_lookup_result(tasks, max_workers=8)
        if s2 > best_score:
            best, best_score = p2, s2

    # Phase 3 — other pharmacy site scrapers, last resort.
    if best_score < GOOD_ENOUGH:
        pharmacy_tasks = []
        for candidate in barcode_candidates:
            for source_name, source_base_url in PHARMACY_LOOKUP_SOURCES:
                pharmacy_tasks.append(
                    lambda bc=candidate, sn=source_name, su=source_base_url, bcs=barcode_candidates:
                    lookup_generic_pharmacy_product(sn, su, bc, bcs)
                )
        p3, s3 = best_lookup_result(pharmacy_tasks, max_workers=6)
        if s3 > best_score:
            best, best_score = p3, s3

    return best


# ── Routes ─────────────────────────────────────────────────────────────────────

@ai_bp.route("/api/products/lookup/<barcode>", methods=["GET"])
def lookup_barcode(barcode):
    if not barcode.strip():
        return jsonify({"found": False, "error": "Code-barres manquant"}), 400
    product = lookup_product_online(barcode)
    if product:
        enrich_lookup_product_with_ai(product)
        return jsonify({"found": True, "product": product})
    return jsonify({"found": False, "error": "Aucun produit trouve en ligne"})


def enrich_lookup_product_with_ai(product):
    """When a UPC is found online with a real name but a thin description, fill
    description/keywords/usage automatically via the AI — so a product is usable
    for client help even when the source only gives a name, with no manual step.
    Never raises; only runs when the AI is configured and we have a real name."""
    try:
        if not configured_ai_provider()["name"]:
            return
        name = str(product.get("name", "")).strip()
        if len(name) < 3:
            return
        desc = str(product.get("description", "")).strip()
        already = str(product.get("search_terms", "")).strip()
        if already or len(desc) >= 60:
            return   # already well described — don't spend an AI call
        assist = generate_product_assist_payload(name, str(product.get("brand", "")).strip(), desc, str(product.get("barcode", "")).strip())
        if not assist:
            return
        product["search_terms"] = assist.get("search_terms", "") or product.get("search_terms", "")
        product["usage_notes"] = assist.get("usage_notes", "") or product.get("usage_notes", "")
        product["alternative_suggestions"] = assist.get("alternative_suggestions", "") or product.get("alternative_suggestions", "")
        if not desc and assist.get("usage_notes"):
            product["description"] = assist["usage_notes"]
        product["ai_enriched"] = True
        log_ai_interaction("product_assist_auto", name, product, assist)
    except Exception:
        pass


@ai_bp.route("/api/products/assist", methods=["POST"])
def assist_product():
    data = request.get_json() or {}
    name = str(data.get("name", "")).strip()
    brand = str(data.get("brand", "")).strip()
    description = str(data.get("description", "")).strip()
    barcode = str(data.get("barcode", "")).strip()
    if not name and not description:
        return jsonify({"success": False, "error": "Nom ou description requis."}), 400
    if not configured_ai_provider()["name"]:
        return jsonify({"success": False, "error": "GEMINI_API_KEY n’est pas configure sur le serveur."}), 503
    assist = generate_product_assist_payload(name, brand, description, barcode)
    if not assist:
        return jsonify({"success": False, "error": "Impossible de générer l aide client pour le moment."}), 502
    log_ai_interaction("product_assist",
                       {"name": name, "brand": brand, "description": description, "barcode": barcode},
                       None, assist)
    return jsonify({"success": True, "assist": assist})


@ai_bp.route("/api/client/help", methods=["POST"])
def client_help():
    data = request.get_json() or {}
    question = str(data.get("question", "")).strip()
    if not question:
        return jsonify({"success": False, "error": "Question client requise."}), 400

    simple = _try_simple_answer(question)
    if simple:
        return jsonify({"success": True, "advice": {
            "summary": simple,
            "recommended_product_names": [],
            "follow_up_questions": [],
            "safety_flags": [],
            "pharmacist_referral": False,
            "pharmacist_reason": "",
        }})

    if not configured_ai_provider()["name"]:
        return jsonify({"success": False, "error": "GEMINI_API_KEY n’est pas configure sur le serveur."}), 503

    if not _check_ai_rate_limit():
        return jsonify({"success": False, "error": "Trop de requetes IA. Reessayez dans une heure."}), 429

    raw_products = data.get("products")
    if isinstance(raw_products, list):
        matched_products = [product_context_for_client_help(item) for item in raw_products[:15] if isinstance(item, dict)]
    else:
        from routes.products import row_to_product, rank_products_for_query
        db = get_db()
        products = [row_to_product(p) for p in db.execute("SELECT * FROM products").fetchall()]
        matched_products = [product_context_for_client_help(item) for item in rank_products_for_query(products, question, limit=15)]

    advice = generate_client_help_payload(question, matched_products)
    if not advice:
        return jsonify({"success": False, "error": "Impossible de générer la réponse client pour le moment."}), 502
    log_ai_interaction("client_help", question, matched_products, advice)
    return jsonify({"success": True, "advice": advice})


@ai_bp.route("/api/ai/feedback", methods=["POST"])
def ai_feedback():
    """Optional, non-blocking thumbs feedback on an AI answer. Stored as its own
    training row (kind='feedback') so we never need to mutate an existing log."""
    data = request.get_json() or {}
    question = str(data.get("question", "")).strip()
    rating = str(data.get("rating", "")).strip()  # 'up' | 'down'
    if rating not in ("up", "down"):
        return jsonify({"success": False}), 400
    log_ai_interaction("feedback", question, None, {"rating": rating})
    return jsonify({"success": True})


# ── AI training-data export (free local store) ───────────────────────────────
@ai_bp.route("/api/ai/logs/export", methods=["GET"])
def export_ai_logs():
    db = get_db()
    rows = db.execute("SELECT * FROM ai_logs ORDER BY id").fetchall()
    lines = []
    for r in rows:
        d = dict(r)
        lines.append(json.dumps({
            "kind":       d.get("kind", ""),
            "question":   d.get("question", ""),
            "context":    d.get("context_json", ""),
            "response":   d.get("response_json", ""),
            "store":      d.get("store", ""),
            "employee":   d.get("employee", ""),
            "model":      d.get("model", ""),
            "created_at": d.get("created_at", ""),
        }, ensure_ascii=False))
    body = "\n".join(lines)
    return Response(body, mimetype="application/jsonl",
                    headers={"Content-Disposition": 'attachment; filename="familiprix-ai-training.jsonl"'})


@ai_bp.route("/api/ai/logs/count", methods=["GET"])
def count_ai_logs():
    db = get_db()
    row = db.execute("SELECT COUNT(*) AS n FROM ai_logs").fetchone()
    n = row["n"] if isinstance(row, dict) else row[0]
    return jsonify({"count": int(n or 0)})
