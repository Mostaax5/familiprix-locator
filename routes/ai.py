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
DEEPSEEK_API_KEY = os.environ.get("DEEPSEEK_API_KEY", "").strip()
DEEPSEEK_MODEL   = os.environ.get("DEEPSEEK_MODEL", "deepseek-chat").strip() or "deepseek-chat"
DEEPSEEK_BASE_URL = os.environ.get("DEEPSEEK_BASE_URL", "https://api.deepseek.com").rstrip("/")

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

# Canned replies for pure LOGISTICS questions only. Keys like "pharmacien" or
# "retour" used to intercept REAL medical questions ("dois-je voir le pharmacien
# pour X ?", "un retour de rhume") with a useless canned line — the AI looked
# broken. Kept only for unambiguous store-info keywords, and _try_simple_answer
# additionally requires a SHORT question so long real questions always reach the AI.
_SIMPLE_ANSWERS = {
    "heure":         "Pour les heures d’ouverture, consultez votre succursale Familiprix locale ou familiprix.com.",
    "livraison":     "La livraison varie selon les succursales. Contactez directement votre pharmacie Familiprix.",
    "telephone":     "Le numéro de téléphone est affiché à l’entrée du magasin ou sur familiprix.com.",
    "adresse":       "L’adresse se trouve sur familiprix.com dans le localisateur de pharmacies.",
    "stationnement": "Renseignez-vous directement auprès de votre succursale pour le stationnement.",
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
    if len(q) > 60:
        return None   # a long question is a real question — let the AI answer it
    for keyword, answer in _SIMPLE_ANSWERS.items():
        if keyword in q:
            return answer
    return None


def _log_ai_usage(provider: str, input_tokens: int, output_tokens: int, question_preview: str = "") -> None:
    if provider == "gemini":
        cost = (input_tokens * _GEMINI_INPUT_COST_PER_M + output_tokens * _GEMINI_OUTPUT_COST_PER_M) / 1_000_000
    elif provider == "openai":
        cost = (input_tokens * _OPENAI_INPUT_COST_PER_M + output_tokens * _OPENAI_OUTPUT_COST_PER_M) / 1_000_000
    else:
        cost = None
    preview = question_preview[:60].replace("\n", " ")
    model = {"gemini": GEMINI_MODEL, "openai": OPENAI_MODEL,
             "deepseek": DEEPSEEK_MODEL}.get(provider, "")
    cost_text = f" cost=${cost:.6f}" if cost is not None else ""
    print(f"[AI-COST] provider={provider} model={model} in={input_tokens} out={output_tokens}"
          f"{cost_text} q=\"{preview}\"")


def configured_ai_provider():
    if DEEPSEEK_API_KEY:
        return {"name": "deepseek", "label": "DeepSeek", "model": DEEPSEEK_MODEL}
    if GEMINI_API_KEY:
        return {"name": "gemini", "label": "Gemini", "model": GEMINI_MODEL}
    if OPENAI_API_KEY:
        return {"name": "openai", "label": "OpenAI", "model": OPENAI_MODEL}
    return {"name": "", "label": "", "model": ""}


# Last AI failure reason — surfaced to the UI so we stop guessing why "no answer".
_AI_LAST_ERROR = ""


def _set_ai_error(msg):
    global _AI_LAST_ERROR
    _AI_LAST_ERROR = str(msg)
    print(f"[AI] {msg}")


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
    ctx = {
        "name":     str(product.get("name", "")).strip(),
        "brand":    str(product.get("brand", "")).strip(),
        "notes":    str(product.get("usage_notes", "") or product.get("description", "")).strip(),
        "location": location,
        # The UPC is essential context: without it a question that names a UPC
        # ("quelle saveur a le 0605388...") could never be matched to its product.
        "upc":      str(product.get("barcode", "")).strip(),
    }
    if str(product.get("in_stock", 1)) == "0":
        ctx["rupture"] = True   # only flag the exceptions to keep the context small
    return ctx


def _build_client_candidates(question, limit=35):
    """Build the AI's store context SERVER-side from the question itself.

    - Every 8-14 digit run in the question is resolved to its exact product(s)
      (placed plan + imported catalogue) and pinned FIRST — so 'quelle saveur a
      le 0605...' always carries the right product, even if the text search
      would never have found it.
    - The rest is the same fast ranked search as the Client tab, with a BIGGER
      cap (35 vs the old 20) so assortment questions ('toutes les saveurs de
      mélatonine ?') see the whole product family, not an arbitrary slice.
    All matching runs on the pre-normalized in-memory corpora — milliseconds."""
    from routes.products import (_products_corpus, _reference_corpus, _fast_reference_score,
                                 normalize_search_text, normalized_digits, tokenize_search_query,
                                 intent_expansion_terms, abbreviation_terms, build_barcode_candidates)
    db = get_db()
    out, seen_keys, seen_bc = [], set(), set()

    def add(item, bc):
        key = ("id", item.get("id")) if item.get("id") else ("bc", bc or str(id(item)))
        if key in seen_keys or (bc and bc in seen_bc and not item.get("id")):
            return
        seen_keys.add(key)
        if bc:
            seen_bc.add(bc)
        out.append(item)

    def ref_to_item(row):
        return {"barcode": row["barcode"], "name": row["name"], "brand": row["brand"],
                "description": row["description"], "product_code": row["product_code"],
                "catalog_only": True, "in_stock": 1}

    # 1) Exact UPC(s) named in the question — pinned first.
    upc_digits = set()
    for run in re.findall(r"\d[\d\s\-]{6,18}\d", question):
        digits = normalized_digits(run)
        if 8 <= len(digits) <= 14:
            for cand in build_barcode_candidates(digits):
                upc_digits.add(normalized_digits(cand))
    if upc_digits:
        for item, prow in _products_corpus(db):
            if prow["_bc"] and prow["_bc"] in upc_digits:
                add(item, prow["_bc"])
        for row in _reference_corpus(db):
            if row["_bc"] and row["_bc"] in upc_digits:
                add(ref_to_item(row), row["_bc"])

    # 2) Ranked text search — same scorer and noise floor as the Client tab.
    nq = normalize_search_text(question)
    dq = normalized_digits(question) if not upc_digits else ""   # UPCs already handled
    qtokens = list(dict.fromkeys(tokenize_search_query(question)))
    intent_terms = intent_expansion_terms(question)
    abbrevs = abbreviation_terms(question)
    scored = []
    if nq or dq or intent_terms:
        for item, prow in _products_corpus(db):
            s = _fast_reference_score(prow, nq, dq, qtokens, intent_terms, abbrevs)
            if s >= 100:
                scored.append((s, 0, item, prow["_bc"]))
        for row in _reference_corpus(db):
            s = _fast_reference_score(row, nq, dq, qtokens, intent_terms, abbrevs)
            if s >= 100:
                scored.append((s, 1, ref_to_item(row), row["_bc"]))
        scored.sort(key=lambda x: (-x[0], x[1], str(x[2].get("name", "")).lower()))
    for _, _, item, bc in scored:
        if len(out) >= limit:
            break
        add(item, bc)
    return out[:limit]


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
    if provider["name"] == "deepseek":
        return generate_product_assist_payload_deepseek(name, brand, description, barcode)
    if provider["name"] == "gemini":
        return generate_product_assist_payload_gemini(name, brand, description, barcode)
    if provider["name"] == "openai":
        return generate_product_assist_payload_openai(name, brand, description, barcode)
    return None


def generate_client_help_payload(question, products):
    provider = configured_ai_provider()
    if provider["name"] == "deepseek":
        return generate_client_help_payload_deepseek(question, products)
    if provider["name"] == "gemini":
        return generate_client_help_payload_gemini(question, products)
    if provider["name"] == "openai":
        return generate_client_help_payload_openai(question, products)
    return None


# Shared instructions for the client-help AI (both providers). Written for an
# EXPERT store assistant: answer the question asked (info, assortment or
# recommendation), decode planogram-abbreviated names, never invent products.
_CLIENT_HELP_INSTRUCTIONS = (
    "Tu es l'assistant expert d'un employé de pharmacie Familiprix au Québec. "
    "Tu connais parfaitement l'inventaire du magasin : la liste JSON fournie contient les produits pertinents "
    "(champs: name, brand, notes, location, upc, et rupture=true si non disponible). "
    "RÉPONDS D'ABORD À LA QUESTION POSÉE, directement et précisément, dans summary (2 à 5 phrases, en français). "
    "Les noms de produits sont abrégés au style planogramme — décode-les : "
    "MELAT=mélatonine, CO=comprimés, CO CROQ=comprimés croquables, CAPS=capsules, GEL=gélules, SIR=sirop, "
    "SHP=shampooing, CR=crème, PDRE=poudre, SOL=solution, VAPO=vaporisateur, GTTE=gouttes, ENF=enfants, "
    "X/F ou XF=extra fort, FRAISE/CERISE/MENTHE/ORANGE/RAISIN=saveurs, les nombres = dosage ou format. "
    "Trois types de questions :\n"
    "1. INFO sur un produit précis (nom ou UPC) : retrouve-le dans la liste (l'UPC de la question correspond au champ upc) "
    "et réponds avec ses informations (saveur, dosage, format déduits du nom et des notes). Si l'information demandée "
    "n'apparaît ni dans le nom ni dans les notes, utilise tes connaissances générales de ce produit précis et dis de "
    "confirmer sur l'emballage. Mentionne l'emplacement (location).\n"
    "2. ASSORTIMENT (« quelles saveurs / formats / dosages avons-nous ? ») : énumère dans summary TOUS les produits "
    "pertinents de la liste avec le détail demandé, et mets CHACUN d'eux dans recommended_product_names.\n"
    "3. BESOIN / SYMPTÔME d'un client : recommande le meilleur produit de la liste puis 1 à 3 vraies alternatives.\n"
    "Règles : ne propose JAMAIS un produit hors liste ; recommended_product_names contient uniquement des noms copiés "
    "EXACTEMENT de la liste (tel quel, sans reformuler), du plus pertinent au moins pertinent. "
    "Si un produit est de marque Biomedic ou Essentiel (marques maison Familiprix), signale-le. "
    "Si un produit a rupture=true, dis qu'il est en rupture de stock. "
    "Si la liste est vide, donne un conseil général de pharmacie sans nommer de produit précis. "
    "Ne pose pas de diagnostic. pharmacist_referral=true quand il faut orienter vers le pharmacien : "
    "grossesse, bébé, interaction médicamenteuse, symptômes graves, douleur importante, difficulté respiratoire, "
    "fièvre élevée, durée inhabituelle ou doute médical. "
    "Retourne uniquement un JSON en français avec exactement les clés "
    "summary (texte), recommended_product_names (tableau), follow_up_questions (tableau), "
    "safety_flags (tableau), pharmacist_referral (booléen) et pharmacist_reason (texte)."
)


def generate_client_help_payload_gemini(question, products):
    payload = {
        "contents": [{"parts": [{"text": (
            f"{_CLIENT_HELP_INSTRUCTIONS}\n\n"
            f"Question de l'employé:\n{question}\n\n"
            f"Produits du magasin:\n{json.dumps(products, ensure_ascii=False) if products else '[]'}"
        )}]}],
        "generationConfig": {"temperature": 0.2, "responseMimeType": "application/json", "maxOutputTokens": 2048},
    }
    request_obj = Request(
        f"{GEMINI_BASE_URL}/models/{GEMINI_MODEL}:generateContent?{urlencode({'key': GEMINI_API_KEY})}",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        # 20s (was 10): the richer 35-product context takes a moment longer to
        # generate; a timeout here means NO answer at all, which is worse.
        with urlopen(request_obj, timeout=20) as response:
            raw_response = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        body = ""
        try: body = exc.read().decode("utf-8", "replace")[:250]
        except Exception: pass
        _set_ai_error(f"Gemini a refusé la requête (HTTP {exc.code}, modèle {GEMINI_MODEL}). {body}")
        return None
    except (URLError, TimeoutError) as exc:
        _set_ai_error(f"Gemini injoignable (réseau ou délai dépassé) : {exc}")
        return None
    except json.JSONDecodeError:
        _set_ai_error("Gemini a renvoyé une réponse illisible.")
        return None
    usage = raw_response.get("usageMetadata", {})
    _log_ai_usage("gemini", usage.get("promptTokenCount", 0), usage.get("candidatesTokenCount", 0), question)
    raw_text = extract_gemini_output_text(raw_response)
    if not raw_text:
        cands = raw_response.get("candidates", [])
        reason = cands[0].get("finishReason", "?") if cands else "?"
        blocked = raw_response.get("promptFeedback", {}).get("blockReason", "")
        _set_ai_error(f"Gemini a renvoyé une réponse vide (finishReason={reason}"
                      + (f", bloqué={blocked}" if blocked else "") + ").")
        return None
    try:
        parsed = json.loads(raw_text)
    except json.JSONDecodeError:
        _set_ai_error("Gemini n'a pas renvoyé un JSON valide.")
        return None
    return normalize_client_help_payload(parsed)


def generate_client_help_payload_openai(question, products):
    payload = {
        "model": OPENAI_MODEL,
        "reasoning": {"effort": "low"},
        "instructions": _CLIENT_HELP_INSTRUCTIONS,
        "input": json.dumps({"question": question, "products": products}, ensure_ascii=False),
        "text": {"format": {
            "type": "json_schema", "name": "client_help", "strict": True,
            "schema": {
                "type": "object",
                "properties": {
                    "summary": {"type": "string"},
                    "recommended_product_names": {"type": "array", "items": {"type": "string"}, "maxItems": 20},
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
        with urlopen(request_obj, timeout=20) as response:
            raw_response = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        body = ""
        try: body = exc.read().decode("utf-8", "replace")[:250]
        except Exception: pass
        _set_ai_error(f"OpenAI a refusé la requête (HTTP {exc.code}, modèle {OPENAI_MODEL}). {body}")
        return None
    except (URLError, TimeoutError) as exc:
        _set_ai_error(f"OpenAI injoignable (réseau ou délai dépassé) : {exc}")
        return None
    except json.JSONDecodeError:
        _set_ai_error("OpenAI a renvoyé une réponse illisible.")
        return None
    usage = raw_response.get("usage", {})
    _log_ai_usage("openai", usage.get("input_tokens", 0), usage.get("output_tokens", 0), question)
    raw_text = extract_openai_output_text(raw_response)
    if not raw_text:
        _set_ai_error("OpenAI a renvoyé une réponse vide.")
        return None
    try:
        parsed = json.loads(raw_text)
    except json.JSONDecodeError:
        _set_ai_error("OpenAI n'a pas renvoyé un JSON valide.")
        return None
    return normalize_client_help_payload(parsed)


def _deepseek_json_request(messages, max_tokens, question_preview=""):
    """Call DeepSeek's OpenAI-compatible chat endpoint and return parsed JSON."""
    payload = {
        "model": DEEPSEEK_MODEL,
        "messages": messages,
        "temperature": 0.2,
        "max_tokens": max_tokens,
        "response_format": {"type": "json_object"},
    }
    request_obj = Request(
        f"{DEEPSEEK_BASE_URL}/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Authorization": f"Bearer {DEEPSEEK_API_KEY}",
                 "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request_obj, timeout=20) as response:
            raw_response = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        body = ""
        try:
            body = exc.read().decode("utf-8", "replace")[:250]
        except Exception:
            pass
        _set_ai_error(f"DeepSeek a refusé la requête (HTTP {exc.code}, modèle {DEEPSEEK_MODEL}). {body}")
        return None
    except (URLError, TimeoutError) as exc:
        _set_ai_error(f"DeepSeek injoignable (réseau ou délai dépassé) : {exc}")
        return None
    except json.JSONDecodeError:
        _set_ai_error("DeepSeek a renvoyé une réponse illisible.")
        return None

    usage = raw_response.get("usage", {})
    _log_ai_usage("deepseek", usage.get("prompt_tokens", 0),
                  usage.get("completion_tokens", 0), question_preview)
    choices = raw_response.get("choices", [])
    raw_text = str(((choices[0] if choices else {}).get("message") or {}).get("content", "")).strip()
    if not raw_text:
        _set_ai_error("DeepSeek a renvoyé une réponse vide.")
        return None
    try:
        return json.loads(raw_text)
    except json.JSONDecodeError:
        _set_ai_error("DeepSeek n'a pas renvoyé un JSON valide.")
        return None


def _gemini_structured_request(system_prompt, user_payload, max_tokens, question_preview=""):
    payload = {
        "contents": [{"parts": [{"text": (
            f"{system_prompt}\n\nEntrée JSON:\n{json.dumps(user_payload, ensure_ascii=False)}"
        )}]}],
        "generationConfig": {
            "temperature": 0.1,
            "responseMimeType": "application/json",
            "maxOutputTokens": max_tokens,
        },
    }
    request_obj = Request(
        f"{GEMINI_BASE_URL}/models/{GEMINI_MODEL}:generateContent?{urlencode({'key': GEMINI_API_KEY})}",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request_obj, timeout=20) as response:
            raw_response = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        _set_ai_error(f"Gemini a refusé la requête structurée (HTTP {exc.code}).")
        return None
    except (URLError, TimeoutError, json.JSONDecodeError) as exc:
        _set_ai_error(f"Gemini n'a pas pu produire la réponse structurée : {exc}")
        return None
    usage = raw_response.get("usageMetadata", {})
    _log_ai_usage("gemini", usage.get("promptTokenCount", 0),
                  usage.get("candidatesTokenCount", 0), question_preview)
    raw_text = extract_gemini_output_text(raw_response)
    try:
        return json.loads(raw_text) if raw_text else None
    except json.JSONDecodeError:
        _set_ai_error("Gemini n'a pas renvoyé un JSON valide.")
        return None


def _openai_structured_request(system_prompt, user_payload, max_tokens,
                               schema_name, schema, question_preview=""):
    payload = {
        "model": OPENAI_MODEL,
        "reasoning": {"effort": "low"},
        "instructions": system_prompt,
        "input": json.dumps(user_payload, ensure_ascii=False),
        "max_output_tokens": max_tokens,
        "text": {"format": {
            "type": "json_schema", "name": schema_name, "strict": True,
            "schema": schema,
        }},
    }
    request_obj = Request(
        f"{OPENAI_BASE_URL}/responses",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Authorization": f"Bearer {OPENAI_API_KEY}",
                 "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request_obj, timeout=20) as response:
            raw_response = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        _set_ai_error(f"OpenAI a refusé la requête structurée (HTTP {exc.code}).")
        return None
    except (URLError, TimeoutError, json.JSONDecodeError) as exc:
        _set_ai_error(f"OpenAI n'a pas pu produire la réponse structurée : {exc}")
        return None
    usage = raw_response.get("usage", {})
    _log_ai_usage("openai", usage.get("input_tokens", 0),
                  usage.get("output_tokens", 0), question_preview)
    raw_text = extract_openai_output_text(raw_response)
    try:
        return json.loads(raw_text) if raw_text else None
    except json.JSONDecodeError:
        _set_ai_error("OpenAI n'a pas renvoyé un JSON valide.")
        return None


def _provider_structured_request(system_prompt, user_payload, max_tokens,
                                 schema_name, schema, question_preview=""):
    provider = configured_ai_provider()["name"]
    if provider == "deepseek":
        return _deepseek_json_request([
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
        ], max_tokens=max_tokens, question_preview=question_preview)
    if provider == "gemini":
        return _gemini_structured_request(
            system_prompt, user_payload, max_tokens, question_preview
        )
    if provider == "openai":
        return _openai_structured_request(
            system_prompt, user_payload, max_tokens, schema_name, schema, question_preview
        )
    return None


_CLIENT_QUERY_PLAN_SCHEMA = {
    "type": "object",
    "properties": {
        "intent": {"type": "string"},
        "corrected_query": {"type": "string"},
        "search_queries": {"type": "array", "items": {"type": "string"}, "maxItems": 10},
        "keywords": {"type": "array", "items": {"type": "string"}, "maxItems": 16},
        "must_include": {"type": "array", "items": {"type": "string"}, "maxItems": 10},
        "exclude": {"type": "array", "items": {"type": "string"}, "maxItems": 10},
        "wants_all": {"type": "boolean"},
        "needs_comparison": {"type": "boolean"},
        "answer_language": {"type": "string"},
        "medical": {"type": "boolean"},
    },
    "required": ["intent", "corrected_query", "search_queries", "keywords",
                 "must_include", "exclude", "wants_all", "needs_comparison",
                 "answer_language", "medical"],
    "additionalProperties": False,
}

_CLIENT_QUERY_PLAN_INSTRUCTIONS = (
    "Tu es le planificateur de recherche d'un catalogue de pharmacie québécoise. "
    "Analyse la phrase complète sans perdre son intention. Corrige les fautes probables "
    "de marques et produits (exemple: advile/dadvile -> Advil), mais ne transforme jamais "
    "une demande de nourriture en demande de médicament. Génère des requêtes bilingues "
    "français/anglais et des synonymes qui peuvent réellement apparaître dans le nom, la "
    "marque, la description ou les notes d'un produit. Pour un symptôme, ajoute les familles "
    "ou ingrédients pertinents; pour un assortiment, conserve toutes les contraintes de "
    "catégorie, saveur, format et marque. wants_all=true pour 'tous/toutes/all/each'. "
    "needs_comparison=true quand l'utilisateur demande une différence ou comparaison. "
    "Retourne uniquement un objet JSON respectant exactement le schéma demandé."
)


def _clean_ai_string_list(value, max_items):
    if not isinstance(value, list):
        return []
    out = []
    for item in value:
        text = str(item or "").strip()
        if text and text not in out:
            out.append(text)
        if len(out) >= max_items:
            break
    return out


def normalize_client_query_plan(parsed, question):
    parsed = parsed if isinstance(parsed, dict) else {}
    language = str(parsed.get("answer_language", "fr") or "fr").lower()
    return {
        "intent": str(parsed.get("intent", "product_search") or "product_search").strip(),
        "corrected_query": str(parsed.get("corrected_query", "") or question).strip(),
        "search_queries": _clean_ai_string_list(parsed.get("search_queries"), 10),
        "keywords": _clean_ai_string_list(parsed.get("keywords"), 16),
        "must_include": _clean_ai_string_list(parsed.get("must_include"), 10),
        "exclude": _clean_ai_string_list(parsed.get("exclude"), 10),
        "wants_all": bool(parsed.get("wants_all", False)),
        "needs_comparison": bool(parsed.get("needs_comparison", False)),
        "answer_language": "en" if language.startswith("en") else "fr",
        "medical": bool(parsed.get("medical", False)),
    }


def generate_client_query_plan(question):
    parsed = _provider_structured_request(
        _CLIENT_QUERY_PLAN_INSTRUCTIONS,
        {"question": question, "required_schema": _CLIENT_QUERY_PLAN_SCHEMA},
        max_tokens=900,
        schema_name="client_query_plan",
        schema=_CLIENT_QUERY_PLAN_SCHEMA,
        question_preview=question,
    )
    return normalize_client_query_plan(parsed, question) if isinstance(parsed, dict) else None


_CLIENT_VERIFICATION_SCHEMA = {
    "type": "object",
    "properties": {
        "answer": {"type": "string"},
        "selected_product_ids": {
            "type": "array", "items": {"type": "string"}, "maxItems": 60,
        },
        "follow_up_questions": {
            "type": "array", "items": {"type": "string"}, "maxItems": 4,
        },
        "safety_flags": {
            "type": "array", "items": {"type": "string"}, "maxItems": 5,
        },
        "pharmacist_referral": {"type": "boolean"},
        "pharmacist_reason": {"type": "string"},
    },
    "required": ["answer", "selected_product_ids", "follow_up_questions",
                 "safety_flags", "pharmacist_referral", "pharmacist_reason"],
    "additionalProperties": False,
}

_CLIENT_VERIFICATION_INSTRUCTIONS = (
    "Tu es à la fois le reranker final et le rédacteur d'un assistant Familiprix. "
    "Compare chaque candidat à la QUESTION ORIGINALE, pas seulement aux mots-clés. "
    "Élimine tout produit qui n'est pas directement pertinent. selected_product_ids doit "
    "contenir uniquement des candidate_id fournis, sans en inventer. Si wants_all=true, "
    "conserve tous les candidats qui satisfont réellement toutes les contraintes, mais aucun "
    "autre. Pour une faute de marque, privilégie la vraie marque correspondante. Une demande "
    "sur ce qu'il faut manger ne justifie pas automatiquement un analgésique. Ne prétends pas "
    "connaître une saveur, un ingrédient ou un dosage absent des données. Rédige ensuite answer "
    "dans answer_language, directement selon la demande. Mentionne chaque produit sélectionné "
    "avec son nom EXACT, copié tel quel, afin que l'interface puisse le rendre cliquable. Ne "
    "nomme aucun produit non sélectionné. Pour une comparaison, explique les différences visibles "
    "dans les données. Ne pose pas de diagnostic. Signale le pharmacien pour grossesse, bébé, "
    "interaction, difficulté respiratoire, symptômes graves, fièvre élevée ou persistante, ou "
    "doute médical. Retourne uniquement le JSON demandé."
)


def product_context_for_client_rag(product):
    context = product_context_for_client_help(product)
    context.update({
        "candidate_id": str(product.get("client_id", "")),
        "description": str(product.get("description", "") or "").strip(),
        "search_terms": str(product.get("search_terms", "") or "").strip(),
        "usage_notes": str(product.get("usage_notes", "") or "").strip(),
        "product_code": str(product.get("product_code", "") or "").strip(),
    })
    return context


def normalize_verified_client_answer(parsed, valid_ids):
    parsed = parsed if isinstance(parsed, dict) else {}
    valid_ids = set(valid_ids)
    selected = []
    for raw in parsed.get("selected_product_ids", []):
        candidate_id = str(raw or "").strip()
        if candidate_id in valid_ids and candidate_id not in selected:
            selected.append(candidate_id)
    return {
        "answer": str(parsed.get("answer", "") or "").strip(),
        "selected_product_ids": selected,
        "follow_up_questions": _clean_ai_string_list(parsed.get("follow_up_questions"), 4),
        "safety_flags": _clean_ai_string_list(parsed.get("safety_flags"), 5),
        "pharmacist_referral": bool(parsed.get("pharmacist_referral", False)),
        "pharmacist_reason": str(parsed.get("pharmacist_reason", "") or "").strip(),
    }


def generate_verified_client_answer(question, query_plan, candidates):
    contexts = [product_context_for_client_rag(product) for product in candidates]
    parsed = _provider_structured_request(
        _CLIENT_VERIFICATION_INSTRUCTIONS,
        {"question": question, "query_plan": query_plan, "candidates": contexts,
         "required_schema": _CLIENT_VERIFICATION_SCHEMA},
        max_tokens=2400,
        schema_name="client_verified_answer",
        schema=_CLIENT_VERIFICATION_SCHEMA,
        question_preview=question,
    )
    if not isinstance(parsed, dict):
        return None
    return normalize_verified_client_answer(
        parsed, [product.get("client_id", "") for product in candidates]
    )


def generate_client_help_payload_deepseek(question, products):
    parsed = _deepseek_json_request([
        {"role": "system", "content": _CLIENT_HELP_INSTRUCTIONS},
        {"role": "user", "content": json.dumps({
            "question": question,
            "products": products,
        }, ensure_ascii=False)},
    ], max_tokens=2048, question_preview=question)
    return normalize_client_help_payload(parsed) if isinstance(parsed, dict) else None


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


def generate_product_assist_payload_deepseek(name, brand, description, barcode):
    prompt = {"name": name, "brand": brand, "description": description, "barcode": barcode}
    parsed = _deepseek_json_request([
        {"role": "system", "content": (
            "Tu aides les employés d'une pharmacie Familiprix au Québec. "
            "Retourne uniquement un objet JSON valide en français avec exactement les clés "
            "search_terms (tableau), usage_notes (texte) et alternative_suggestions (tableau). "
            "Sois concis, concret, prudent sur le plan médical et ne donne pas de diagnostic."
        )},
        {"role": "user", "content": json.dumps({"product": prompt}, ensure_ascii=False)},
    ], max_tokens=400, question_preview=name)
    return normalize_assist_payload(parsed) if isinstance(parsed, dict) else None


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
    # get_db(): inside a request this REUSES the request's connection instead of
    # checking a second one out of the pool (system/info did that on every ping).
    try:
        db = get_db()
        row = db.execute("SELECT COUNT(*) AS c FROM product_reference").fetchone()
        return int(dict(row).get("c", 0)) if row else 0
    except Exception:
        return 0


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


def lookup_product_online(barcode, max_workers=None):
    """UPC lookup for a PHARMACY catalog (food, beauty, meds, vitamins, baby,
    bandages, eye care, Familiprix house brand…). Broad coverage but fast: it
    returns as soon as a trusted result is found (good_enough), so it doesn't wait
    on the slow scrapers unless the fast databases miss. No cache (always fresh),
    and the broken EAN API is not used. Returns a product dict or None.

    max_workers caps EACH phase's internal thread pool. Interactive scans keep the
    full fan-out (fastest single answer); batch enrichment passes a small cap —
    several uncapped lookups in parallel meant 4×16 sockets + parsers at once,
    which is what kept blowing Render's 512 MB memory limit."""
    barcode = str(barcode or "").strip()
    if not barcode:
        return None
    GOOD_ENOUGH = 24
    candidates = build_barcode_candidates(barcode)
    best, best_score = None, 0

    def _cap(n):
        return min(n, max_workers) if max_workers else n

    # Phase 1 — fast structured JSON databases: Open Facts (food / beauty / drug /
    # general) + UPC Item DB + Datakick + Brocade. Covers most everyday products.
    tasks = []
    for bc in candidates:
        tasks.append(lambda c=bc: lookup_upcitemdb(c))
        tasks.append(lambda c=bc: lookup_datakick(c))
        tasks.append(lambda c=bc: lookup_brocade(c))
        for sn, su in PRODUCT_LOOKUP_SOURCES:
            tasks.append(lambda c=bc, n=sn, u=su: lookup_open_facts_product(n, u, c))
    best, best_score = best_lookup_result(tasks, max_workers=_cap(16), good_enough=GOOD_ENOUGH)

    # Phase 2 — Familiprix catalog + barcode databases. The Familiprix scraper is
    # what finds house-brand and pharmacy-specific items the open DBs don't have.
    if best_score < GOOD_ENOUGH:
        tasks = []
        for bc in candidates:
            tasks.append(lambda c=bc, cs=candidates: lookup_familiprix_product(c, cs))
            tasks.append(lambda c=bc: lookup_barcodelookup(c))
            tasks.append(lambda c=bc: lookup_go_upc(c))
        p2, s2 = best_lookup_result(tasks, max_workers=_cap(8), good_enough=GOOD_ENOUGH)
        if s2 > best_score:
            best, best_score = p2, s2

    # Phase 3 — pharmacy sites (Jean Coutu / Brunet / Pharmaprix), last resort.
    if best_score < GOOD_ENOUGH:
        tasks = []
        for bc in candidates:
            for sn, su in PHARMACY_LOOKUP_SOURCES:
                tasks.append(lambda c=bc, n=sn, u=su, cs=candidates: lookup_generic_pharmacy_product(n, u, c, cs))
        p3, s3 = best_lookup_result(tasks, max_workers=_cap(6), good_enough=GOOD_ENOUGH)
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


_ENRICH_CHUNK = 20        # lookups submitted per batch — Stop reacts within one batch
_ENRICH_WORKERS = 3       # parallel product lookups; each one is ALSO capped to
_ENRICH_LOOKUP_FANOUT = 6 # 6 internal source-requests (an uncapped lookup fans out
                          # to 16) — total ceiling 3×6=18 concurrent HTTP, which
                          # fits Render's 512 MB. DB writes stay on one thread.


def _enrich_marker_path():
    import tempfile
    return os.path.join(tempfile.gettempdir(), "familiprix-enrich.json")


def _write_enrich_marker():
    """Progress snapshot on disk: it survives a gunicorn worker recycle, so the
    status endpoint can detect a dead run (pid changed) and RESUME it alone."""
    try:
        with open(_enrich_marker_path(), "w", encoding="utf-8") as fh:
            json.dump({**_CATALOG_ENRICH, "pid": os.getpid()}, fh)
    except OSError:
        pass


def _read_enrich_marker():
    try:
        with open(_enrich_marker_path(), "r", encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return None


def _catalog_enrich_worker():
    """Fill in real descriptions + images for catalogue products from the online
    databases, in the background. Each online result is validated against the Familiprix
    name (online_matches_catalog) so a WRONG product is never attached; lookups run in
    parallel (network-bound) while DB writes stay per-row on this thread, so progress
    survives anything. The whole run is a RETRY LOOP: a dropped database connection or
    any transient error reconnects and re-selects what's left instead of silently
    killing the run — a single blip used to stop it at 2% while the UI said Terminé.
    It only gives up after 5 consecutive attempts that made zero progress."""
    from concurrent.futures import ThreadPoolExecutor
    from database import connect_db
    import gc

    def _lookup(r):
        try:
            return r, lookup_product_online(r.get("barcode", ""), max_workers=_ENRICH_LOOKUP_FANOUT)
        except Exception:
            return r, None

    attempts_without_progress = 0
    try:
        while attempts_without_progress < 5 and _CATALOG_ENRICH["running"]:
            db = None
            made_progress = False
            try:
                db = connect_db()
                # Only products not yet tried (no description AND no enrich tag) so
                # re-runs and reconnects resume exactly where the run stopped.
                rows = [dict(r) for r in db.execute(
                    "SELECT barcode, name, brand FROM product_reference "
                    "WHERE TRIM(COALESCE(description,'')) = '' AND TRIM(COALESCE(enrich_status,'')) = '' "
                    "AND TRIM(COALESCE(name,'')) <> ''").fetchall()]
                if not rows:
                    break                      # everything processed — real Terminé
                _CATALOG_ENRICH["total"] = _CATALOG_ENRICH["done"] + len(rows)
                _CATALOG_ENRICH.pop("error", None)
                _write_enrich_marker()
                with ThreadPoolExecutor(max_workers=_ENRICH_WORKERS) as pool:
                    for i in range(0, len(rows), _ENRICH_CHUNK):
                        if not _CATALOG_ENRICH["running"]:
                            return             # deliberate stop — finally cleans up
                        for r, online in pool.map(_lookup, rows[i:i + _ENRICH_CHUNK]):
                            bc = r.get("barcode", "")
                            # Poison-row immunity: a malformed online payload tags the
                            # row as no_match and moves on — it must never kill the run.
                            # (DB errors below still bubble up to the reconnect loop.)
                            desc = img = brand = ""
                            matched = False
                            try:
                                if online and online_matches_catalog(r.get("name", ""), r.get("brand", ""), online):
                                    desc = str(online.get("description", "")).strip()
                                    img = str(online.get("image_url", "")).strip()
                                    brand = str(online.get("brand", "")).strip()
                                    matched = True
                            except Exception:
                                matched = False
                            if matched:
                                db.execute(
                                    """UPDATE product_reference SET
                                         description   = CASE WHEN TRIM(COALESCE(description,'')) = '' THEN ? ELSE description END,
                                         image_url     = CASE WHEN TRIM(COALESCE(image_url,''))   = '' THEN ? ELSE image_url END,
                                         brand         = CASE WHEN TRIM(COALESCE(brand,''))       = '' THEN ? ELSE brand END,
                                         enrich_status = 'done', updated_at = ?
                                       WHERE barcode = ?""",
                                    (desc, img, brand, utc_now_iso(), bc),
                                )
                                db.commit()
                                _CATALOG_ENRICH["updated"] += 1
                            else:
                                # Tag it so we know it still needs a real description.
                                db.execute("UPDATE product_reference SET enrich_status='no_match', updated_at=? WHERE barcode=?",
                                           (utc_now_iso(), bc))
                                db.commit()
                                _CATALOG_ENRICH["skipped"] += 1
                            _CATALOG_ENRICH["done"] += 1
                            made_progress = True
                            attempts_without_progress = 0
                        _write_enrich_marker()   # once per batch — the resume checkpoint
                        # Free the batch's parsed online payloads NOW (some sources
                        # return hundreds of KB per product) — RSS creep OOM'd us once.
                        gc.collect()
                break                          # full pass completed
            except Exception as exc:           # DB blip, pool trouble… reconnect & continue
                _CATALOG_ENRICH["error"] = f"{type(exc).__name__}: {exc}"[:200]
                if not made_progress:
                    attempts_without_progress += 1
                print(f"[Enrich] incident, reprise dans 5s (essais sans progrès: {attempts_without_progress}): {exc}")
                time.sleep(5)
            finally:
                if db is not None:
                    try: db.close()
                    except Exception: pass
    finally:
        _CATALOG_ENRICH["running"] = False
        _write_enrich_marker()


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
    _CATALOG_ENRICH.update(running=True, done=0, updated=0, skipped=0,
                           total=int(total or 0), started_at=time.time())
    _CATALOG_ENRICH.pop("error", None)   # a fresh run must not display an old failure
    import threading
    threading.Thread(target=_catalog_enrich_worker, daemon=True).start()
    return jsonify({"success": True, "started": True, "total": _CATALOG_ENRICH["total"]})


def maybe_resume_enrichment():
    """Self-heal: if the marker says a run was active in ANOTHER process (worker
    recycle/restart killed its thread mid-run), relaunch it — progress is already
    committed row by row, so the worker naturally resumes on what's left. Called
    from the status poll AND from /api/system/info, which the keep-alive pings hit
    every 10 minutes: the run recovers even with every phone closed. Returns True
    when a resume was launched."""
    try:
        if _CATALOG_ENRICH["running"]:
            return False
        marker = _read_enrich_marker()
        if not (marker and marker.get("running") and marker.get("pid") != os.getpid()):
            return False
        _CATALOG_ENRICH.update(running=True, done=0, updated=0, skipped=0,
                               total=0, started_at=time.time())
        _CATALOG_ENRICH.pop("error", None)
        _write_enrich_marker()
        import threading
        threading.Thread(target=_catalog_enrich_worker, daemon=True).start()
        return True
    except Exception:
        return False


@ai_bp.route("/api/import/catalog-enrich/status", methods=["GET"])
def catalog_enrich_status():
    resumed = maybe_resume_enrichment()
    state = dict(_CATALOG_ENRICH)
    if resumed:
        state["resumed"] = True
    if state.get("running") and state.get("done") and state.get("started_at"):
        elapsed = max(1.0, time.time() - float(state["started_at"]))
        rate = state["done"] / elapsed
        remaining = max(0, int(state.get("total", 0)) - int(state["done"]))
        state["eta_minutes"] = int(round(remaining / max(rate, 0.01) / 60))
    state.pop("started_at", None)
    return jsonify(state)


@ai_bp.route("/api/import/catalog-enrich/stop", methods=["POST"])
def catalog_enrich_stop():
    username, error = require_editor()
    if error:
        return error
    _CATALOG_ENRICH["running"] = False
    _write_enrich_marker()   # a deliberate stop must NOT be auto-resumed
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
        return jsonify({"success": False, "error": "Aucune clé IA n’est configurée sur le serveur."}), 503
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
        advice = {
            "summary": simple,
            "recommended_product_names": [],
            "recommended_products": [],
            "follow_up_questions": [],
            "safety_flags": [],
            "pharmacist_referral": False,
            "pharmacist_reason": "",
        }
        return jsonify({"success": True, "answer": simple, "products": [],
                        "query_plan": None, "advice": advice})

    if not configured_ai_provider()["name"]:
        return jsonify({"success": False, "error": "Aucune clé IA n’est configurée sur le serveur."}), 503

    if not _check_ai_rate_limit():
        return jsonify({"success": False, "error": "Trop de requetes IA. Reessayez dans une heure."}), 429

    global _AI_LAST_ERROR
    _AI_LAST_ERROR = ""

    # Pass 1: correct spelling and transform the full request into structured,
    # bilingual catalogue queries without losing intent or constraints.
    query_plan = generate_client_query_plan(question)
    if not query_plan:
        return jsonify({"success": False,
                        "error": _AI_LAST_ERROR or "Impossible d'analyser la demande pour le moment."}), 502

    # Hybrid retrieval: UPC/name rules + descriptions + intent + BM25-style
    # relevance + fuzzy spelling. 'All' requests keep a wider candidate window.
    from routes.products import hybrid_client_candidates
    candidate_limit = 60 if query_plan.get("wants_all") else 45
    candidates = hybrid_client_candidates(question, query_plan, limit=candidate_limit)

    # Pass 2: the model may select only the real candidate IDs below. This removes
    # unrelated retrievals and writes the final answer from the verified products.
    verified = generate_verified_client_answer(question, query_plan, candidates)
    if not verified:
        return jsonify({"success": False,
                        "error": _AI_LAST_ERROR or "Impossible de vérifier les produits pour le moment."}), 502

    by_id = {str(product.get("client_id", "")): product for product in candidates}
    products = [by_id[candidate_id] for candidate_id in verified["selected_product_ids"]
                if candidate_id in by_id]
    answer = verified["answer"] or (
        "Aucun produit suffisamment lié à cette demande n'a été trouvé dans la base."
    )
    recommended_products = [{
        "candidate_id": product.get("client_id", ""),
        "name": str(product.get("name", "")).strip(),
        "brand": str(product.get("brand", "")).strip(),
        "location": _recommendation_location(product),
        "barcode": str(product.get("barcode", "")).strip(),
        "home_brand": _is_home_brand(product.get("brand", "")),
    } for product in products]
    advice = {
        "summary": answer,
        "recommended_product_names": [product["name"] for product in recommended_products],
        "recommended_products": recommended_products,
        "follow_up_questions": verified["follow_up_questions"],
        "safety_flags": verified["safety_flags"],
        "pharmacist_referral": verified["pharmacist_referral"],
        "pharmacist_reason": verified["pharmacist_reason"],
    }
    log_ai_interaction("client_rag", question, {
        "query_plan": query_plan,
        "retrieved": [product_context_for_client_rag(product) for product in candidates],
    }, advice)
    return jsonify({"success": True, "answer": answer, "products": products,
                    "query_plan": query_plan, "advice": advice})


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
