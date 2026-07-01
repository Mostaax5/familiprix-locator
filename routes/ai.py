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

# Auto AI-enrich of online UPC lookups is OFF by default so it can NEVER cost
# anything unexpectedly. Turn it on with AI_AUTO_ENRICH=1 on Render only if you
# want thin product descriptions filled automatically (a few cents per thousand).
_AI_AUTO_ENRICH = os.environ.get("AI_AUTO_ENRICH", "").strip().lower() in {"1", "true", "yes", "on"}

# Last-resort AI web-grounded product identification, used only when every free
# database/scraper fails. OFF by default (it costs more than the free sources).
# Turn on with AI_DEEP_LOOKUP=1 + a GEMINI_API_KEY (use a grounding-capable model
# like gemini-2.5-flash). It searches the web like a human would.
_AI_DEEP_LOOKUP = os.environ.get("AI_DEEP_LOOKUP", "").strip().lower() in {"1", "true", "yes", "on"}

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


def best_lookup_result(tasks, max_workers=8, good_enough=None):
    """Run all tasks, return (best_product, best_score) by quality score. Returns
    as soon as a result reaches `good_enough` instead of waiting for the slowest
    source (e.g. an 8s scraper timeout) — this is what makes scanning fast."""
    best, best_score = None, 0
    if not tasks:
        return None, 0
    executor = ThreadPoolExecutor(max_workers=min(max_workers, len(tasks)))
    futures = [executor.submit(task) for task in tasks]
    try:
        for future in as_completed(futures):
            try:
                result = future.result()
            except Exception:
                result = None
            s = _product_quality_score(result)
            if s > best_score:
                best, best_score = result, s
            if good_enough is not None and best_score >= good_enough:
                break   # good enough — don't wait for slower sources
    finally:
        # Don't block the response on still-running network calls; abandon them.
        executor.shutdown(wait=False, cancel_futures=True)
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
    # Only trust the loose title/description regex when the page ACTUALLY shows
    # this barcode — otherwise a "no results"/wrong page yields a WRONG product.
    verified = page_mentions_barcode(html, candidates)
    name = structured.get("name") or (clean_html_text(first_regex(html, [
        r'<h4[^>]*class="[^"]*product-name[^"]*"[^>]*>(.*?)</h4>',
        r'<h1[^>]*>(.*?)</h1>', r'<meta property="og:title" content="([^"]+)"',
    ])) if verified else "")
    if not name or len(name) < 3 or "not found" in name.lower():
        return None
    brand = structured.get("brand") or (clean_html_text(first_regex(html, [
        r'<span[^>]*class="[^"]*brand[^"]*"[^>]*>(.*?)</span>',
        r'<p[^>]*class="[^"]*brand[^"]*"[^>]*>(.*?)</p>',
    ])) if verified else "")
    description = structured.get("description") or (clean_html_text(first_regex(html, [
        r'<meta name="description" content="([^"]+)"', r'<meta property="og:description" content="([^"]+)"',
    ])) if verified else "")
    image_url = structured.get("image_url") or (first_regex(html, [r'<meta property="og:image" content="([^"]+)"']) if verified else "")
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
    verified = page_mentions_barcode(html, candidates)   # guard the loose regex (see barcodelookup)
    name = structured.get("name") or (clean_html_text(first_regex(html, [
        r'<h1[^>]*class="[^"]*product-name[^"]*"[^>]*>(.*?)</h1>',
        r'<h1[^>]*>(.*?)</h1>', r'<meta property="og:title" content="([^"]+)"',
    ])) if verified else "")
    if not name or len(name) < 3 or "not found" in name.lower() or name.lower().startswith("barcode"):
        return None
    brand = structured.get("brand") or (clean_html_text(first_regex(html, [r'class="[^"]*brand[^"]*"[^>]*>\s*(.*?)\s*</\w+>'])) if verified else "")
    description = structured.get("description") or (clean_html_text(first_regex(html, [
        r'<meta name="description" content="([^"]+)"', r'<meta property="og:description" content="([^"]+)"',
    ])) if verified else "")
    image_url = structured.get("image_url") or (first_regex(html, [r'<meta property="og:image" content="([^"]+)"']) if verified else "")
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
    aisle = str(product.get("aisle", "")).strip()
    location = (f"Allée {aisle} {side_display_label(product.get('side',''))} T{str(product.get('shelf','')).strip()}"
                if aisle else "En magasin — position à confirmer")
    return {
        "name":     str(product.get("name", "")).strip(),
        "brand":    str(product.get("brand", "")).strip(),
        "notes":    str(product.get("usage_notes", "") or product.get("description", "")).strip(),
        "location": location,
    }


_HOME_BRANDS = ("familiprix", "biomedic", "essentiel")


def _is_home_brand(brand):
    b = str(brand or "").strip().lower()
    return any(b.startswith(h) for h in _HOME_BRANDS)


def _recommendation_location(product):
    """Full, human-readable shelf location for a recommended product."""
    aisle   = str(product.get("aisle", "")).strip()
    if not aisle:
        return "En magasin — position à confirmer"
    side    = side_display_label(product.get("side", ""))
    section = str(product.get("section", "") or "1").strip()
    shelf   = str(product.get("shelf", "")).strip()
    position = str(product.get("position", "")).strip()
    parts = []
    if aisle:    parts.append(f"Allée {aisle}")
    if side:     parts.append(side)
    if section:  parts.append(f"Section {section}")
    if shelf:    parts.append(f"Tablette {shelf}")
    if position: parts.append(f"Pos. {position}")
    return " · ".join(parts)


def _attach_locatable_recommendations(advice, candidate_objs):
    """Map the AI's recommended product NAMES back to REAL store products so each
    one carries its exact shelf location. A recommendation that doesn't resolve to a
    real candidate is dropped — we never show an employee a product they can't find.
    The result is added as advice['recommended_products'] (structured + locatable)."""
    from routes.products import normalize_search_text
    if not isinstance(advice, dict):
        return advice
    names = advice.get("recommended_product_names") or []
    index = []
    for obj in candidate_objs:
        nm = normalize_search_text(obj.get("name", ""))
        if nm:
            index.append((nm, obj))
    resolved, used = [], set()
    for raw in names:
        target = normalize_search_text(raw)
        if not target:
            continue
        match = None
        for nm, obj in index:                       # exact name
            if nm == target:
                match = obj; break
        if not match:
            for nm, obj in index:                   # one is a prefix of the other
                if nm.startswith(target) or target.startswith(nm):
                    match = obj; break
        if not match:
            for nm, obj in index:                   # substring either direction
                if target in nm or nm in target:
                    match = obj; break
        if not match:
            continue
        key = match.get("id") or id(match)
        if key in used:
            continue
        used.add(key)
        resolved.append({
            "name":       str(match.get("name", "")).strip(),
            "brand":      str(match.get("brand", "")).strip(),
            "location":   _recommendation_location(match),
            "barcode":    str(match.get("barcode", "")).strip(),
            "home_brand": _is_home_brand(match.get("brand", "")),
        })
    advice["recommended_products"] = resolved
    return advice


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
            "Dans recommended_product_names, mets UNIQUEMENT des noms copies EXACTEMENT de la liste fournie "
            "(copie-colle le nom tel quel, sans le reformuler), classes du plus pertinent au moins pertinent : "
            "d'abord le meilleur produit pour ce besoin precis, puis 1 a 3 vraies alternatives de la meme categorie. "
            "N'invente jamais un nom de produit et n'en propose aucun qui n'est pas dans la liste. "
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
            "Dans recommended_product_names, mets UNIQUEMENT des noms copies EXACTEMENT de la liste fournie "
            "(copie-colle le nom tel quel, sans le reformuler), classes du plus pertinent au moins pertinent : "
            "d'abord le meilleur produit pour ce besoin precis, puis 1 a 3 vraies alternatives de la meme categorie. "
            "N'invente jamais un nom de produit et n'en propose aucun qui n'est pas dans la liste. "
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


# ── Reference catalog (local, growing product database) ─────────────────────────

def _reference_upsert(db, product):
    """Insert/update one product in the reference catalog using a given db."""
    barcode = normalized_digits(product.get("barcode", ""))
    name = str(product.get("name", "")).strip()
    if not barcode or len(name) < 3:
        return False
    db.execute(
        """INSERT INTO product_reference (barcode, name, brand, description, image_url, source, source_url, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(barcode) DO UPDATE SET
               name=excluded.name, brand=excluded.brand, description=excluded.description,
               image_url=excluded.image_url, source=excluded.source,
               source_url=excluded.source_url, updated_at=excluded.updated_at""",
        (barcode, name, str(product.get("brand", "")), str(product.get("description", "")),
         str(product.get("image_url", "")), str(product.get("source", "")).replace(" · cache", ""),
         str(product.get("source_url", "")), utc_now_iso()),
    )
    return True


def online_matches_catalog(cat_name, cat_brand, online):
    """Guard against the online databases returning the WRONG product for a UPC.
    We trust the Familiprix catalogue name; an online result is only accepted if it
    shares a meaningful word (brand/product) with it. Prevents attaching a random
    description/image to the right product (the 'match super wrong' problem)."""
    from routes.products import normalize_search_text, tokenize_search_query
    online_text = normalize_search_text(f"{online.get('name','')} {online.get('brand','')}")
    cat_text = normalize_search_text(f"{cat_name} {cat_brand or ''}")
    if not online_text or not cat_text:
        return False
    tokens = [t for t in tokenize_search_query(cat_text) if len(t) >= 4]
    if not tokens:                                  # very short catalogue name — be strict
        tokens = tokenize_search_query(cat_text)
    return any(t in online_text for t in tokens)


def reference_lookup(barcode):
    """Return a product from the local reference catalog, or None. Instant & free."""
    from database import connect_db
    db = connect_db()
    try:
        for cand in build_barcode_candidates(barcode):
            row = db.execute("SELECT * FROM product_reference WHERE barcode=?", (cand,)).fetchone()
            if row:
                d = dict(row)
                if str(d.get("name", "")).strip():
                    return {"name": d.get("name", ""), "brand": d.get("brand", ""),
                            "description": d.get("description", ""), "barcode": d.get("barcode", ""),
                            "product_code": d.get("product_code", ""),
                            "source": (d.get("source", "") or "catalogue") + " · cache",
                            "source_url": d.get("source_url", ""), "image_url": d.get("image_url", "")}
    except Exception:
        return None
    finally:
        try: db.close()
        except Exception: pass
    return None


def reference_save(product):
    """Cache a found product so the next lookup of this barcode is instant & free."""
    if not product:
        return
    from database import connect_db
    db = connect_db()
    try:
        if _reference_upsert(db, product):
            db.commit()
    except Exception:
        pass
    finally:
        try: db.close()
        except Exception: pass


def reference_count():
    from database import connect_db
    db = connect_db()
    try:
        row = db.execute("SELECT COUNT(*) AS c FROM product_reference").fetchone()
        return int(dict(row).get("c", 0)) if row else 0
    except Exception:
        return 0
    finally:
        try: db.close()
        except Exception: pass


def _seed_reference_worker(jobs, pages, page_size):
    """Background: page open Canadian product databases and fill the catalog."""
    from database import connect_db
    db = connect_db()
    total = 0
    try:
        for domain, source_name in jobs:
            for page in range(1, pages + 1):
                params = urlencode({
                    "action": "process", "tagtype_0": "countries",
                    "tag_contains_0": "contains", "tag_0": "canada", "json": 1,
                    "page_size": page_size, "page": page,
                    "fields": "code,product_name_fr,product_name,brands,image_front_url",
                })
                data = _fetch_json(f"{domain}/cgi/search.pl?{params}", timeout=20)
                products = (data or {}).get("products") if isinstance(data, dict) else None
                if not products:
                    break
                for p in products:
                    name = first_present(p, ["product_name_fr", "product_name"])
                    if _reference_upsert(db, {
                        "barcode": p.get("code", ""), "name": name,
                        "brand": first_present(p, ["brands"]), "description": "",
                        "image_url": p.get("image_front_url", ""), "source": source_name, "source_url": "",
                    }):
                        total += 1
                db.commit()
                time.sleep(0.4)   # be polite to the public API
    except Exception as exc:
        print(f"[Reference] seed error: {exc}")
    finally:
        try: db.close()
        except Exception: pass
    print(f"[Reference] seed done: +{total} produits au catalogue")


def ai_grounded_product_lookup(barcode):
    """Identify a product from its barcode via an AI web search (Gemini grounding).
    Opt-in (AI_DEEP_LOOKUP=1) and conservative: returns None unless the model says
    it's confident. Never raises."""
    digits = normalized_digits(barcode)
    if not _AI_DEEP_LOOKUP or not GEMINI_API_KEY or not digits:
        return None
    prompt = (
        "Tu identifies un produit de détail (alimentation, médicament, beauté, etc.) "
        f"vendu au Québec/Canada à partir de son code-barres UPC/EAN: {digits}. "
        "Cherche sur le web avant de répondre. Réponds UNIQUEMENT par un objet JSON avec "
        "les clés name, brand, description (courte, en français) et found (true/false). "
        "Mets found=false si tu n'es pas certain à au moins 90% — ne devine jamais."
    )
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "tools": [{"google_search": {}}],
        "generationConfig": {"temperature": 0.1, "maxOutputTokens": 300},
    }
    req = Request(
        f"{GEMINI_BASE_URL}/models/{GEMINI_MODEL}:generateContent?{urlencode({'key': GEMINI_API_KEY})}",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"}, method="POST",
    )
    try:
        with urlopen(req, timeout=18) as response:
            raw = json.loads(response.read().decode("utf-8"))
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError):
        return None
    usage = raw.get("usageMetadata", {})
    _log_ai_usage("gemini", usage.get("promptTokenCount", 0), usage.get("candidatesTokenCount", 0), f"deep:{digits}")
    text = extract_gemini_output_text(raw)
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if not match:
        return None
    try:
        d = json.loads(match.group(0))
    except json.JSONDecodeError:
        return None
    name = str(d.get("name", "")).strip()
    if not d.get("found") or len(name) < 3:
        return None
    return {"name": name, "brand": str(d.get("brand", "")).strip(),
            "description": str(d.get("description", "")).strip(), "barcode": digits,
            "source": "Recherche IA", "source_url": "", "image_url": ""}


def lookup_product_online(barcode):
    """UPC lookup for a PHARMACY catalog (food, beauty, meds, vitamins, baby,
    bandages, eye care, Familiprix house brand…). Broad coverage but fast: it
    returns as soon as a trusted result is found (good_enough), so it doesn't wait
    on the slow scrapers unless the fast databases miss. No cache (always fresh),
    and the broken EAN API is not used. Returns a product dict or None."""
    barcode = str(barcode or "").strip()
    if not barcode:
        return None
    GOOD_ENOUGH = 24
    candidates = build_barcode_candidates(barcode)
    best, best_score = None, 0

    # Phase 1 — fast structured JSON databases: Open Facts (food / beauty / drug /
    # general) + UPC Item DB + Datakick + Brocade. Covers most everyday products.
    tasks = []
    for bc in candidates:
        tasks.append(lambda c=bc: lookup_upcitemdb(c))
        tasks.append(lambda c=bc: lookup_datakick(c))
        tasks.append(lambda c=bc: lookup_brocade(c))
        for sn, su in PRODUCT_LOOKUP_SOURCES:
            tasks.append(lambda c=bc, n=sn, u=su: lookup_open_facts_product(n, u, c))
    best, best_score = best_lookup_result(tasks, max_workers=16, good_enough=GOOD_ENOUGH)

    # Phase 2 — Familiprix catalog + barcode databases. The Familiprix scraper is
    # what finds house-brand and pharmacy-specific items the open DBs don't have.
    if best_score < GOOD_ENOUGH:
        tasks = []
        for bc in candidates:
            tasks.append(lambda c=bc, cs=candidates: lookup_familiprix_product(c, cs))
            tasks.append(lambda c=bc: lookup_barcodelookup(c))
            tasks.append(lambda c=bc: lookup_go_upc(c))
        p2, s2 = best_lookup_result(tasks, max_workers=8, good_enough=GOOD_ENOUGH)
        if s2 > best_score:
            best, best_score = p2, s2

    # Phase 3 — pharmacy sites (Jean Coutu / Brunet / Pharmaprix), last resort.
    if best_score < GOOD_ENOUGH:
        tasks = []
        for bc in candidates:
            for sn, su in PHARMACY_LOOKUP_SOURCES:
                tasks.append(lambda c=bc, n=sn, u=su, cs=candidates: lookup_generic_pharmacy_product(n, u, c, cs))
        p3, s3 = best_lookup_result(tasks, max_workers=6, good_enough=GOOD_ENOUGH)
        if s3 > best_score:
            best, best_score = p3, s3

    # Phase 4 — AI web-grounded identification (opt-in via AI_DEEP_LOOKUP, off by default).
    if not best:
        ai_found = ai_grounded_product_lookup(barcode)
        if ai_found:
            best = ai_found
    return best


# ── Routes ─────────────────────────────────────────────────────────────────────

@ai_bp.route("/api/products/lookup/<barcode>", methods=["GET"])
def lookup_barcode(barcode):
    if not barcode.strip():
        return jsonify({"found": False, "error": "Code-barres manquant"}), 400

    # 1) Local reference catalogue FIRST — the imported planograms + past scans. Instant,
    #    free, and guarantees a real product NAME even when the online sources miss it.
    ref = reference_lookup(barcode)

    # 2) Online sources add a nicer description + a product image (as before).
    online = lookup_product_online(barcode)

    product = None
    if ref and len(str(ref.get("name", "")).strip()) >= 2:
        # Keep the reliable catalogue name/code; enrich the rest from online where blank —
        # but ONLY if the online result actually matches this product (guards wrong hits).
        product = dict(ref)
        if online and online_matches_catalog(product.get("name", ""), product.get("brand", ""), online):
            if not str(product.get("description", "")).strip() and online.get("description"):
                product["description"] = online["description"]
            if not str(product.get("brand", "")).strip() and online.get("brand"):
                product["brand"] = online["brand"]
            if not str(product.get("image_url", "")).strip() and online.get("image_url"):
                product["image_url"] = online["image_url"]
    elif online:
        product = online

    if product:
        enrich_lookup_product_with_ai(product)
        return jsonify({"found": True, "product": product})
    return jsonify({"found": False, "error": "Aucun produit trouve"})


@ai_bp.route("/api/reference/count", methods=["GET"])
def reference_count_route():
    return jsonify({"count": reference_count()})


# ── Catalogue online-enrichment (fetch real descriptions + images, validated) ────
_CATALOG_ENRICH = {"running": False, "done": 0, "total": 0, "updated": 0, "skipped": 0}


def _catalog_enrich_worker():
    """Fill in real descriptions + images for catalogue products from the online
    databases, in the background. Each online result is validated against the Familiprix
    name (online_matches_catalog) so a WRONG product is never attached. Resumable: only
    processes rows that still have no description."""
    import time as _time
    from database import connect_db
    db = None
    try:
        db = connect_db()
        # Only products not yet tried (no description AND no enrich tag) so re-runs stay
        # fast and don't re-hammer permanent misses. Tagged rows are kept for later.
        rows = [dict(r) for r in db.execute(
            "SELECT barcode, name, brand FROM product_reference "
            "WHERE TRIM(COALESCE(description,'')) = '' AND TRIM(COALESCE(enrich_status,'')) = '' "
            "AND TRIM(COALESCE(name,'')) <> ''").fetchall()]
        _CATALOG_ENRICH.update(total=len(rows), done=0, updated=0, skipped=0, running=True)
        for r in rows:
            if not _CATALOG_ENRICH["running"]:
                break
            bc = r.get("barcode", "")
            try:
                online = lookup_product_online(bc)
            except Exception:
                online = None
            if online and online_matches_catalog(r.get("name", ""), r.get("brand", ""), online):
                db.execute(
                    """UPDATE product_reference SET
                         description   = CASE WHEN TRIM(COALESCE(description,'')) = '' THEN ? ELSE description END,
                         image_url     = CASE WHEN TRIM(COALESCE(image_url,''))   = '' THEN ? ELSE image_url END,
                         brand         = CASE WHEN TRIM(COALESCE(brand,''))       = '' THEN ? ELSE brand END,
                         enrich_status = 'done', updated_at = ?
                       WHERE barcode = ?""",
                    (str(online.get("description", "")).strip(), str(online.get("image_url", "")).strip(),
                     str(online.get("brand", "")).strip(), utc_now_iso(), bc),
                )
                db.commit()
                _CATALOG_ENRICH["updated"] += 1
            else:
                # Tag it so we know it still needs a real description (downloadable list).
                db.execute("UPDATE product_reference SET enrich_status='no_match', updated_at=? WHERE barcode=?",
                           (utc_now_iso(), bc))
                db.commit()
                _CATALOG_ENRICH["skipped"] += 1
            _CATALOG_ENRICH["done"] += 1
            _time.sleep(0.15)   # be gentle on the free open databases
    except Exception:
        pass
    finally:
        _CATALOG_ENRICH["running"] = False
        if db is not None:
            try: db.close()
            except Exception: pass


@ai_bp.route("/api/import/catalog-enrich/start", methods=["POST"])
def catalog_enrich_start():
    username, error = require_editor()
    if error:
        return error
    if _CATALOG_ENRICH["running"]:
        return jsonify({"success": True, "already_running": True, **_CATALOG_ENRICH})
    # Compute the total and flag running SYNCHRONOUSLY so the first status poll can't
    # race the worker thread's startup and stop early (the '0/0' bug).
    db = get_db()
    try:
        row = db.execute(
            "SELECT COUNT(*) AS n FROM product_reference "
            "WHERE TRIM(COALESCE(description,'')) = '' AND TRIM(COALESCE(enrich_status,'')) = '' "
            "AND TRIM(COALESCE(name,'')) <> ''").fetchone()
        total = row["n"] if isinstance(row, dict) else row[0]
    except Exception:
        total = 0
    _CATALOG_ENRICH.update(running=True, done=0, updated=0, skipped=0, total=int(total or 0))
    import threading
    threading.Thread(target=_catalog_enrich_worker, daemon=True).start()
    return jsonify({"success": True, "started": True, "total": _CATALOG_ENRICH["total"]})


@ai_bp.route("/api/import/catalog-enrich/status", methods=["GET"])
def catalog_enrich_status():
    return jsonify(dict(_CATALOG_ENRICH))


@ai_bp.route("/api/import/catalog-enrich/stop", methods=["POST"])
def catalog_enrich_stop():
    username, error = require_editor()
    if error:
        return error
    _CATALOG_ENRICH["running"] = False
    return jsonify({"success": True})


@ai_bp.route("/api/import/catalog-needs-description", methods=["GET"])
def catalog_needs_description():
    """Downloadable CSV of catalogue products that still have NO real description —
    either never enriched, or the online lookup found no reliable match (tagged
    'no_match'). These are the ones to describe by hand / from a better source later."""
    import csv, io as _io
    db = get_db()
    rows = db.execute(
        "SELECT barcode, product_code, name, source, enrich_status FROM product_reference "
        "WHERE TRIM(COALESCE(description,'')) = '' AND TRIM(COALESCE(name,'')) <> '' "
        "ORDER BY source, name").fetchall()
    buf = _io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["UPC", "Code Familiprix", "Nom (planogramme)", "Planogramme", "Statut"])
    for r in rows:
        d = dict(r)
        status = "aucune correspondance en ligne" if d.get("enrich_status") == "no_match" else "pas encore tenté"
        writer.writerow([d.get("barcode", ""), d.get("product_code", ""), d.get("name", ""),
                         str(d.get("source", "")).replace("Planogramme: ", ""), status])
    return Response("﻿" + buf.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": 'attachment; filename="produits-sans-description.csv"'})


@ai_bp.route("/api/import/catalog-needs-description/count", methods=["GET"])
def catalog_needs_description_count():
    db = get_db()
    row = db.execute(
        "SELECT COUNT(*) AS n FROM product_reference "
        "WHERE TRIM(COALESCE(description,'')) = '' AND TRIM(COALESCE(name,'')) <> ''").fetchone()
    n = row["n"] if isinstance(row, dict) else row[0]
    no_match = db.execute(
        "SELECT COUNT(*) AS n FROM product_reference WHERE enrich_status = 'no_match'").fetchone()
    nm = no_match["n"] if isinstance(no_match, dict) else no_match[0]
    return jsonify({"needs_description": int(n or 0), "no_match": int(nm or 0)})


@ai_bp.route("/api/reference/seed", methods=["POST"])
def reference_seed_route():
    """Fill the local reference catalog from the open Canadian product databases
    (free, no key) in the background, so most scanned UPCs resolve instantly."""
    username, error = require_editor()
    if error:
        return error
    data = request.get_json() or {}
    pages = min(max(int(data.get("pages", 15) or 15), 1), 60)
    domains = {
        "food":   ("https://world.openfoodfacts.org",    "Open Food Facts"),
        "beauty": ("https://world.openbeautyfacts.org",  "Open Beauty Facts"),
        "drug":   ("https://world.opendrugfacts.org",    "Open Drug Facts"),
        "products": ("https://world.openproductsfacts.org", "Open Products Facts"),
    }
    requested = data.get("sources") or ["food", "beauty", "drug", "products"]
    jobs = [domains[s] for s in requested if s in domains]
    if not jobs:
        return jsonify({"success": False, "error": "Aucune source valide."}), 400
    import threading
    threading.Thread(target=lambda: _seed_reference_worker(jobs, pages, 100), daemon=True).start()
    return jsonify({"success": True, "started": True,
                    "message": f"Remplissage du catalogue en arrière-plan : {pages} pages × {len(jobs)} source(s). "
                               "Le catalogue grandit progressivement (revenez voir le total dans quelques minutes)."})


def enrich_lookup_product_with_ai(product):
    """When a UPC is found online with a real name but a thin description, fill
    description/keywords/usage automatically via the AI — so a product is usable
    for client help even when the source only gives a name, with no manual step.
    Never raises; only runs when explicitly enabled (AI_AUTO_ENRICH=1), the AI is
    configured, and we have a real name. OFF by default → no cost."""
    try:
        if not _AI_AUTO_ENRICH or not configured_ai_provider()["name"]:
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

    # Build the candidate set from REAL store products (kept whole, with locations) so
    # every recommendation the AI returns can be mapped back to a findable shelf spot.
    raw_products = data.get("products")
    if isinstance(raw_products, list):
        candidate_objs = [item for item in raw_products[:15] if isinstance(item, dict)]
    else:
        from routes.products import row_to_product, rank_products_for_query
        db = get_db()
        products = [row_to_product(p) for p in db.execute("SELECT * FROM products").fetchall()]
        candidate_objs = rank_products_for_query(products, question, limit=15)

    # Always augment with the reference catalogue (imported planograms) so the AI can
    # recommend products we carry even if they aren't placed on a shelf yet.
    from routes.products import rank_reference_for_query, normalized_digits
    placed_bcs = {normalized_digits(str(o.get("barcode", ""))) for o in candidate_objs if o.get("barcode")}
    ref_matches = rank_reference_for_query(question, limit=10, exclude_barcodes=placed_bcs)
    candidate_objs = (candidate_objs + ref_matches)[:20]

    matched_products = [product_context_for_client_help(item) for item in candidate_objs]

    advice = generate_client_help_payload(question, matched_products)
    if not advice:
        return jsonify({"success": False, "error": "Impossible de générer la réponse client pour le moment."}), 502
    # Resolve the AI's named picks to real, locatable products (drops anything not in stock).
    advice = _attach_locatable_recommendations(advice, candidate_objs)
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
