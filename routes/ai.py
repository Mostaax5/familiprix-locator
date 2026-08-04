import json
import hashlib
import os
import re
import secrets
import threading
import time
from collections import OrderedDict, defaultdict
from concurrent.futures import (
    ThreadPoolExecutor, as_completed,
)
from html import unescape
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urljoin, urlparse
from urllib.request import HTTPRedirectHandler, Request, build_opener
from flask import Blueprint, request, jsonify, Response, g, current_app
from database import connect_db, get_db
from auth import require_editor, utc_now_iso, side_display_label
from memory_guard import memory_intensive_task, release_unused_memory
from observability import record_ai_answer
from product_data import (
    assess_metadata_candidate,
    classify_source,
    gtin_identity_key,
    gtin_check_digit_valid,
    normalize_text,
    record_reference_evidence,
    upsert_reference_identifier,
    upsert_reference_candidate,
)
from regulatory_data import (
    HEALTH_CANADA_AUTHORITY,
    attach_regulatory_candidates,
    merge_regulatory_candidates,
)

ai_bp = Blueprint("ai", __name__)

# Every lookup used to create a fresh executor and return while its slower
# requests were still running. Repeated imports could therefore leave hundreds
# of source threads alive at once. One shared pool puts a hard process-wide cap
# on those requests while preserving fast early results for interactive scans.
_LOOKUP_SOURCE_WORKERS = 3
_LOOKUP_SOURCE_EXECUTOR = ThreadPoolExecutor(
    max_workers=_LOOKUP_SOURCE_WORKERS, thread_name_prefix="product-source"
)
_MAX_ONLINE_BODY_BYTES = 1_500_000
_AI_LOG_MAX_JSON_CHARS = 120_000
_AI_LOGGING_ENABLED = os.environ.get("AI_LOGGING_ENABLED", "").strip().lower() in {
    "1", "true", "yes", "on",
}
_ai_log_cleanup_lock = threading.Lock()
_ai_log_last_cleanup = 0.0
_AI_LOG_EXECUTOR = ThreadPoolExecutor(
    max_workers=1, thread_name_prefix="ai-log",
)
_AI_LOG_SLOTS = threading.BoundedSemaphore(8)
_KIMI_MODELS_CACHE = {"checked_at": 0.0, "models": set()}
_KIMI_MODELS_LOCK = threading.Lock()
_CLIENT_QUERY_PLAN_CACHE = OrderedDict()
_CLIENT_QUERY_PLAN_CACHE_LOCK = threading.Lock()
_CLIENT_QUERY_PLAN_CACHE_MAX = 256
_CLIENT_QUERY_PLAN_CACHE_TTL_SECONDS = 15 * 60


def _bounded_log_json(value):
    if value is None:
        return ""
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))[:_AI_LOG_MAX_JSON_CHARS]


def _outbound_url_allowed(url, expected_host=None):
    try:
        parsed = urlparse(str(url or ""))
        host = (parsed.hostname or "").lower().rstrip(".")
    except ValueError:
        return False
    if parsed.scheme != "https" or not host or parsed.username or parsed.password:
        return False
    if expected_host and host != str(expected_host).lower().rstrip("."):
        return False
    return True


class _SameHostRedirectHandler(HTTPRedirectHandler):
    def __init__(self, expected_host):
        super().__init__()
        self.expected_host = expected_host

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        if not _outbound_url_allowed(newurl, self.expected_host):
            raise HTTPError(newurl, 403, "Cross-host redirect blocked", headers, fp)
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def _safe_urlopen(request_obj, timeout):
    target_url = request_obj.full_url if isinstance(request_obj, Request) else str(request_obj)
    if not _outbound_url_allowed(target_url):
        raise URLError("Unsafe outbound URL")
    host = urlparse(target_url).hostname
    return build_opener(_SameHostRedirectHandler(host)).open(request_obj, timeout=timeout)


def _persist_ai_log(values):
    global _ai_log_last_cleanup
    db = None
    try:
        db = connect_db()
        if db.backend == "postgres":
            db.execute(
                "SELECT set_config('statement_timeout', ?, true)",
                ("5s",),
            )
        db.execute(
            """INSERT INTO ai_logs (created_at, kind, provider, model, question, context_json, response_json, store, employee)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            values,
        )
        now = time.time()
        with _ai_log_cleanup_lock:
            should_cleanup = now - _ai_log_last_cleanup > 86400
            if should_cleanup:
                _ai_log_last_cleanup = now
        if should_cleanup:
            db.execute(
                "DELETE FROM ai_logs WHERE id NOT IN "
                "(SELECT id FROM ai_logs ORDER BY id DESC LIMIT 5000)"
            )
        db.commit()
    except Exception:
        if db is not None:
            try:
                db.rollback()
            except Exception:
                pass
    finally:
        if db is not None:
            try:
                db.close()
            except Exception:
                pass
        _AI_LOG_SLOTS.release()


def log_ai_interaction(kind, question, context, response):
    """Persist every AI Q&A as a training example, tagged with store, employee
    (auto from device name — never prompted) and time. Never raises — logging
    must not break the user-facing response."""
    if not _AI_LOGGING_ENABLED:
        return
    try:
        prov = configured_ai_provider()
        documented_models = {
            "kimi": KIMI_DOCUMENTED_MODEL,
            "deepseek": DEEPSEEK_DOCUMENTED_MODEL,
        }
        logged_model = (
            documented_models.get(prov["name"], prov["model"])
            if kind == "client_documented_rag"
            else prov["model"]
        )
        body = request.get_json(silent=True) or {}
        store = str(body.get("store", "")).strip()
        employee = str(getattr(g, "auth_username", "") or "").strip()
        values = (
            utc_now_iso(), str(kind or "")[:80], prov["name"], logged_model,
            str(question or "")[:4000], _bounded_log_json(context),
            _bounded_log_json(response), store[:120], employee[:60],
        )
    except Exception:
        return
    if not _AI_LOG_SLOTS.acquire(blocking=False):
        return
    try:
        _AI_LOG_EXECUTOR.submit(_persist_ai_log, values)
    except RuntimeError:
        _AI_LOG_SLOTS.release()

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
    "ingredients_text_fr", "ingredients_text", "labels", "labels_tags",
    "packaging_text", "npn", "din", "din_hm", "url", "image_front_url",
]

GEMINI_API_KEY  = os.environ.get("GEMINI_API_KEY",  "").strip()
GEMINI_MODEL    = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash-lite").strip() or "gemini-2.5-flash-lite"
GEMINI_BASE_URL = os.environ.get("GEMINI_BASE_URL", "https://generativelanguage.googleapis.com/v1beta").rstrip("/")
OPENAI_API_KEY  = os.environ.get("OPENAI_API_KEY",  "").strip()
OPENAI_MODEL    = os.environ.get("OPENAI_MODEL", "gpt-4o-mini").strip() or "gpt-4o-mini"
OPENAI_BASE_URL = os.environ.get("OPENAI_BASE_URL", "https://api.openai.com/v1").rstrip("/")
KIMI_API_KEY = os.environ.get("KIMI_API_KEY", os.environ.get("MOONSHOT_API_KEY", "")).strip()
# K2.6 provides the low-latency structured planner and first answer. K3 is kept
# for the optional documented upgrade, where deeper reasoning is worth waiting
# for after the employee already has a useful answer on screen.
KIMI_MODEL = os.environ.get("KIMI_MODEL", "kimi-k2.6").strip() or "kimi-k2.6"
KIMI_REALTIME_MODEL = (
    os.environ.get("KIMI_REALTIME_MODEL", KIMI_MODEL).strip()
    or KIMI_MODEL
)
KIMI_DOCUMENTED_MODEL = (
    os.environ.get("KIMI_DOCUMENTED_MODEL", "kimi-k3").strip()
    or "kimi-k3"
)
KIMI_BASE_URL = os.environ.get(
    "KIMI_BASE_URL", "https://api.moonshot.ai/v1"
).rstrip("/")
KIMI_REALTIME_REASONING_EFFORT = (
    os.environ.get("KIMI_REALTIME_REASONING_EFFORT", "low").strip().lower()
)
if KIMI_REALTIME_REASONING_EFFORT not in {"low", "high", "max"}:
    KIMI_REALTIME_REASONING_EFFORT = "low"
# Use a fresh key for the synchronous documented path. Some existing Render
# services retained the former high-effort variable even after render.yaml was
# changed, making every employee wait more than 40 seconds.
KIMI_DOCUMENTED_REASONING_EFFORT = (
    os.environ.get(
        "KIMI_SYNC_DOCUMENTED_REASONING_EFFORT", "low"
    ).strip().lower()
)
if KIMI_DOCUMENTED_REASONING_EFFORT not in {"low", "high", "max"}:
    KIMI_DOCUMENTED_REASONING_EFFORT = "low"
_KIMI_DOCUMENTED_LONG_THINKING = (
    os.environ.get("KIMI_DOCUMENTED_LONG_THINKING", "").strip().lower()
    in {"1", "true", "yes", "on"}
)
DEEPSEEK_API_KEY = os.environ.get("DEEPSEEK_API_KEY", "").strip()
DEEPSEEK_MODEL   = os.environ.get("DEEPSEEK_MODEL", "deepseek-v4-flash").strip() or "deepseek-v4-flash"
DEEPSEEK_DOCUMENTED_MODEL = (
    os.environ.get("DEEPSEEK_DOCUMENTED_MODEL", DEEPSEEK_MODEL).strip()
    or DEEPSEEK_MODEL
)
DEEPSEEK_BASE_URL = os.environ.get("DEEPSEEK_BASE_URL", "https://api.deepseek.com").rstrip("/")

_GEMINI_INPUT_COST_PER_M  = 0.075
_GEMINI_OUTPUT_COST_PER_M = 0.30
_OPENAI_INPUT_COST_PER_M  = 0.15
_OPENAI_OUTPUT_COST_PER_M = 0.60

_AI_RATE_LIMIT  = int(os.environ.get("AI_RATE_LIMIT",  "30"))
_AI_RATE_WINDOW = int(os.environ.get("AI_RATE_WINDOW", "3600"))
_MAX_AI_RATE_KEYS = 4096
try:
    _AI_REQUEST_TIMEOUT_SECONDS = min(30, max(5, int(os.environ.get("AI_REQUEST_TIMEOUT", "12"))))
except (TypeError, ValueError):
    _AI_REQUEST_TIMEOUT_SECONDS = 12
try:
    _AI_DOCUMENTED_REQUEST_TIMEOUT_SECONDS = min(
        24, max(12, int(os.environ.get("AI_DOCUMENTED_REQUEST_TIMEOUT", "18")))
    )
except (TypeError, ValueError):
    _AI_DOCUMENTED_REQUEST_TIMEOUT_SECONDS = 18
_DOCUMENTED_AI_EXECUTOR = ThreadPoolExecutor(
    max_workers=1, thread_name_prefix="documented-ai",
)
_DOCUMENTED_AI_GATE = threading.Lock()
_DOCUMENTED_ANSWER_CACHE = OrderedDict()
_DOCUMENTED_ANSWER_CACHE_LOCK = threading.Lock()
_DOCUMENTED_ANSWER_CACHE_MAX = 96
_DOCUMENTED_ANSWER_CACHE_TTL_SECONDS = 30 * 60
_DOCUMENTED_JOBS = OrderedDict()
_DOCUMENTED_JOB_MAX = 32
_DOCUMENTED_JOB_TTL_SECONDS = 10 * 60
try:
    _AI_QUERY_PLAN_TIMEOUT_SECONDS = min(
        7, max(3, int(os.environ.get("AI_QUERY_PLAN_TIMEOUT", "5")))
    )
except (TypeError, ValueError):
    _AI_QUERY_PLAN_TIMEOUT_SECONDS = 5
_DEEPSEEK_DOCUMENTED_THINKING = (
    os.environ.get("DEEPSEEK_DOCUMENTED_THINKING", "").strip().lower()
    in {"1", "true", "yes", "on"}
)
_ai_rate_buckets: dict = defaultdict(list)
_ai_rate_lock = threading.Lock()

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
    # ProxyFix has already reduced Render's trusted proxy hop.  Never trust a
    # caller-supplied X-Forwarded-For value directly.
    identity = str(getattr(g, "auth_session_hash", "") or request.remote_addr or "unknown")
    now = time.time()
    cutoff = now - _AI_RATE_WINDOW
    with _ai_rate_lock:
        if identity not in _ai_rate_buckets and len(_ai_rate_buckets) >= _MAX_AI_RATE_KEYS:
            identity = "__overflow__"
        _ai_rate_buckets[identity] = [t for t in _ai_rate_buckets[identity] if t > cutoff]
        if len(_ai_rate_buckets[identity]) >= _AI_RATE_LIMIT:
            return False
        _ai_rate_buckets[identity].append(now)
        return True


def _try_simple_answer(question: str):
    q = question.lower()
    if len(q) > 60:
        return None   # a long question is a real question — let the AI answer it
    for keyword, answer in _SIMPLE_ANSWERS.items():
        if keyword in q:
            return answer
    return None


def _log_ai_usage(provider: str, input_tokens: int, output_tokens: int,
                  question_preview: str = "", model_override: str = "") -> None:
    if provider == "gemini":
        cost = (input_tokens * _GEMINI_INPUT_COST_PER_M + output_tokens * _GEMINI_OUTPUT_COST_PER_M) / 1_000_000
    elif provider == "openai":
        cost = (input_tokens * _OPENAI_INPUT_COST_PER_M + output_tokens * _OPENAI_OUTPUT_COST_PER_M) / 1_000_000
    else:
        cost = None
    preview = question_preview[:60].replace("\n", " ")
    model = model_override or {
        "gemini": GEMINI_MODEL,
        "openai": OPENAI_MODEL,
        "kimi": KIMI_MODEL,
        "deepseek": DEEPSEEK_MODEL,
    }.get(provider, "")
    cost_text = f" cost=${cost:.6f}" if cost is not None else ""
    print(f"[AI-COST] provider={provider} model={model} in={input_tokens} out={output_tokens}"
          f"{cost_text} q=\"{preview}\"")


def configured_ai_provider():
    if KIMI_API_KEY:
        return {
            "name": "kimi", "label": "Kimi", "model": KIMI_MODEL,
            "documented_reasoning_effort": KIMI_DOCUMENTED_REASONING_EFFORT,
        }
    if DEEPSEEK_API_KEY:
        return {"name": "deepseek", "label": "DeepSeek", "model": DEEPSEEK_MODEL}
    if GEMINI_API_KEY:
        return {"name": "gemini", "label": "Gemini", "model": GEMINI_MODEL}
    if OPENAI_API_KEY:
        return {"name": "openai", "label": "OpenAI", "model": OPENAI_MODEL}
    return {"name": "", "label": "", "model": ""}


# Last AI failure reason — surfaced to the UI so we stop guessing why "no answer".
_AI_LAST_ERROR = ""
_AI_LAST_MODEL = ""


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


def _lookup_has_image(product):
    return bool(str((product or {}).get("image_url", "")).strip())


def _prefer_lookup_result(current, current_score, candidate, candidate_score, require_image=False):
    """Prefer a real image during image enrichment, then use normal quality."""
    if not candidate:
        return current, current_score
    current_candidates = list((current or {}).get("regulatory_identifiers") or [])
    candidate_candidates = list(candidate.get("regulatory_identifiers") or [])
    merged_candidates = merge_regulatory_candidates(
        current_candidates, candidate_candidates
    )
    if require_image:
        candidate_has_image = _lookup_has_image(candidate)
        current_has_image = _lookup_has_image(current)
        if candidate_has_image != current_has_image:
            selected, score = (
                (candidate, candidate_score)
                if candidate_has_image else (current, current_score)
            )
            if selected is not None and merged_candidates:
                selected = dict(selected)
                selected["regulatory_identifiers"] = merged_candidates
            return selected, score
    if candidate_score > current_score:
        if merged_candidates:
            candidate = dict(candidate)
            candidate["regulatory_identifiers"] = merged_candidates
        return candidate, candidate_score
    if current is not None and merged_candidates:
        current = dict(current)
        current["regulatory_identifiers"] = merged_candidates
    return current, current_score


def best_lookup_result(tasks, max_workers=8, good_enough=None, wait_for_cleanup=False,
                       require_image=False):
    """Run all tasks, return (best_product, best_score) by quality score. Returns
    as soon as a result reaches `good_enough` instead of waiting for the slowest
    source (e.g. an 8s scraper timeout) — this is what makes scanning fast."""
    best, best_score = None, 0
    if not tasks:
        return None, 0
    worker_limit = min(max(1, int(max_workers or 1)), len(tasks), _LOOKUP_SOURCE_WORKERS)
    task_iter = iter(tasks)
    futures = set()

    def submit_next():
        try:
            task = next(task_iter)
        except StopIteration:
            return False
        futures.add(_LOOKUP_SOURCE_EXECUTOR.submit(task))
        return True

    for _ in range(worker_limit):
        submit_next()
    try:
        while futures:
            future = next(as_completed(tuple(futures)))
            futures.discard(future)
            try:
                result = future.result()
            except Exception:
                result = None
            s = _product_quality_score(result)
            best, best_score = _prefer_lookup_result(
                best, best_score, result, s, require_image=require_image
            )
            if (good_enough is not None and best_score >= good_enough
                    and (not require_image or _lookup_has_image(best))):
                break   # good enough — don't wait for slower sources
            submit_next()
    finally:
        running = []
        for future in futures:
            if not future.cancel():
                running.append(future)
        # Background maintenance waits for the few requests that already started,
        # so releasing its memory slot really means the work is gone. Interactive
        # scans still return immediately; the shared pool keeps their cleanup bounded.
        if wait_for_cleanup:
            for future in running:
                try:
                    future.result()
                except Exception:
                    pass
    return best, best_score


def _read_limited_response(response, max_bytes=_MAX_ONLINE_BODY_BYTES):
    return response.read(max_bytes + 1)[:max_bytes]


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
        with _safe_urlopen(request_obj, timeout=3) as response:
            body = _read_limited_response(response).decode("utf-8", errors="ignore")
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
    value = unescape(value)
    return re.sub(r"\s+", " ", value).strip()


def clean_product_description(value, max_chars=900):
    """Turn source HTML/list fragments into compact, readable catalogue prose."""
    text = str(value or "")
    text = re.sub(r"(?i)<\s*br\s*/?\s*>", "; ", text)
    text = re.sub(r"(?i)</?\s*li\b[^>]*>", "; ", text)
    text = clean_html_text(text)
    text = re.sub(r"\s*(?:;|\|)\s*(?:-\s*)?", "; ", text)
    text = re.sub(r"(?<=:)\s*-\s*", " ", text)
    text = re.sub(r"\s+-\s+", "; ", text)
    text = re.sub(r"(?:;\s*){2,}", "; ", text)
    text = re.sub(r":\s*;\s*", ": ", text)
    text = re.sub(r";\s+Et\b", "; et", text)
    text = re.sub(r"\s+([,.;:])", r"\1", text).strip(" ;")
    if not text:
        return ""
    text = text[0].upper() + text[1:]
    if text[-1] not in ".!?":
        text += "."
    if len(text) <= max_chars:
        return text
    shortened = text[:max_chars + 1]
    boundary = max(
        shortened.rfind(". "),
        shortened.rfind("; "),
        shortened.rfind(", "),
        shortened.rfind(" "),
    )
    if boundary >= max_chars // 2:
        shortened = shortened[:boundary]
    return shortened.rstrip(" ,;:.") + "."


def _json_ld_blocks(html):
    for block in re.findall(
        r'<script[^>]+type="application/ld\+json"[^>]*>(.*?)</script>',
        html or "", flags=re.IGNORECASE | re.DOTALL,
    ):
        try:
            yield json.loads(block.strip())
        except (TypeError, json.JSONDecodeError):
            continue


def _json_ld_nodes(value, expected_type):
    nodes = []

    def collect(item):
        if isinstance(item, dict):
            raw_type = item.get("@type")
            types = raw_type if isinstance(raw_type, list) else [raw_type]
            if any(
                str(node_type or "").lower() == expected_type.lower()
                for node_type in types
            ):
                nodes.append(item)
            for nested in item.values():
                if isinstance(nested, (dict, list)):
                    collect(nested)
        elif isinstance(item, list):
            for nested in item:
                collect(nested)

    collect(value)
    return nodes


def extract_familiprix_category(html, product_name=""):
    for payload in _json_ld_blocks(html):
        for breadcrumb in _json_ld_nodes(payload, "BreadcrumbList"):
            names = []
            for entry in breadcrumb.get("itemListElement") or []:
                if not isinstance(entry, dict):
                    continue
                item = entry.get("item")
                name = (
                    item.get("name", "") if isinstance(item, dict)
                    else entry.get("name", "")
                )
                name = clean_html_text(str(name or ""))
                if name:
                    names.append(name)
            if len(names) >= 3:
                # Drop Accueil and the exact product; retain a useful hierarchy.
                return " > ".join(names[1:-1][-3:])
    return ""


def extract_familiprix_sections(html):
    sections = {}
    pattern = re.compile(
        r'<button[^>]*class="[^"]*product-information-section-btn[^"]*"[^>]*>'
        r'(.*?)</button>\s*'
        r'<div[^>]*class="[^"]*product-information-section-text[^"]*"[^>]*>'
        r'(.*?)</div>',
        flags=re.IGNORECASE | re.DOTALL,
    )
    for title_html, body_html in pattern.findall(html or ""):
        title = clean_html_text(title_html).lower()
        body = clean_product_description(body_html, max_chars=3000)
        if title and body:
            sections[title] = body
    return sections


def extract_familiprix_specifications(html):
    specifications = {}
    pattern = re.compile(
        r'<div[^>]*class="[^"]*product-specification-item[^"]*"[^>]*>\s*'
        r'<span[^>]*>\s*<b[^>]*>(.*?)</b>\s*</span>\s*'
        r'<span[^>]*>(.*?)</span>',
        flags=re.IGNORECASE | re.DOTALL,
    )
    for label_html, value_html in pattern.findall(html or ""):
        label = clean_html_text(label_html).lstrip("#").strip().lower()
        value = clean_html_text(value_html)
        if (
            label and value
            and value.lower() not in {"n/a", "n.d.", "non disponible"}
        ):
            specifications[label] = value
    return specifications


def _source_package_fields(product_name):
    match = re.search(
        r"\b(\d+(?:[.,]\d+)?)\s*"
        r"(ml|l|mg|mcg|g|kg|un|unités?|comprimés?|capsules?|caplets?)\b",
        str(product_name or ""), flags=re.IGNORECASE,
    )
    if not match:
        return "", ""
    unit = match.group(2).lower()
    unit = {
        "unité": "un", "unités": "un",
        "comprimé": "comprimés", "capsule": "capsules",
        "caplet": "caplets",
    }.get(unit, unit)
    return match.group(1).replace(",", "."), unit


def build_precise_source_description(name, description, *, category="", dosage_form=""):
    cleaned = clean_product_description(description)
    if len(cleaned) >= 55:
        return cleaned
    exact_name = clean_product_description(name, max_chars=360).rstrip(".")
    details = []
    if dosage_form:
        details.append(f"forme {str(dosage_form).lower()}")
    if category:
        details.append(f"catégorie {str(category).split('>')[-1].strip()}")
    identity = exact_name
    if details:
        identity += f" ({', '.join(details)})"
    if cleaned and normalize_text(cleaned) not in normalize_text(identity):
        identity += f". {cleaned}"
    return clean_product_description(identity)


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
    try:
        absolute = urljoin(f"{str(base_url).rstrip('/')}/", str(url or ""))
        base_host = urlparse(base_url).hostname
    except ValueError:
        return ""
    return absolute if _outbound_url_allowed(absolute, base_host) else ""


def normalize_familiprix_url(url):
    return normalize_url("https://magasiner.familiprix.com", url)


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
        description = clean_product_description(
            str(product.get("description", "")).strip()
        )
        image_url = extract_structured_image(product.get("image"))
        product_code = str(
            product.get("sku") or product.get("productID")
            or product.get("productId") or ""
        ).strip()
        if name or brand or description or image_url:
            return {
                "name": name, "brand": brand, "description": description,
                "image_url": image_url, "product_code": product_code,
            }
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
                    product_code = str(
                        obj.get("sku") or obj.get("productCode")
                        or obj.get("productId") or ""
                    ).strip()
                    return {
                        "name": name, "brand": brand, "description": description,
                        "image_url": image, "product_code": product_code,
                    }
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


def _familiprix_code_key(value):
    text = str(value or "").strip()
    digits = normalized_digits(text)
    if digits:
        return digits.lstrip("0") or "0"
    return re.sub(r"[^a-z0-9]", "", text.lower())


def parse_familiprix_product_page(html, url, barcode, barcode_candidates=None,
                                  product_code=""):
    barcode_candidates = barcode_candidates or build_barcode_candidates(barcode)
    if not page_mentions_barcode(html, barcode_candidates):
        return None
    embedded = extract_embedded_json_product(html, barcode_candidates)
    structured = extract_structured_product_data(html, barcode_candidates)
    title = embedded.get("name") or structured.get("name") or first_regex(html, [
        r"<h1[^>]*>(.*?)</h1>", r'<meta property="og:title" content="([^"]+)"', r"<title>(.*?)</title>",
    ])
    title = sanitize_title(clean_html_text(title), "Familiprix")
    raw_description = structured.get("description") or first_regex(html, [
        r'<meta name="description" content="([^"]+)"', r'<meta property="og:description" content="([^"]+)"',
    ])
    image_url = structured.get("image_url") or first_regex(html, [r'<meta property="og:image" content="([^"]+)"'])
    brand = structured.get("brand") or infer_brand_from_title(title)
    category = extract_familiprix_category(html, title)
    specifications = extract_familiprix_specifications(html)
    sections = extract_familiprix_sections(html)
    dosage_form = specifications.get("forme", "")
    package_size, package_unit = _source_package_fields(title)
    purpose = ""
    warning_section = next((
        value for section_title, value in sections.items()
        if "avertissement" in section_title or "allégation" in section_title
    ), "")
    usage_match = re.search(
        r"\bUsages?\b\s*[:;.-]*\s*(.+)$",
        warning_section, flags=re.IGNORECASE | re.DOTALL,
    )
    if usage_match:
        purpose = clean_product_description(usage_match.group(1), max_chars=700)
    ingredients = next((
        value for section_title, value in sections.items()
        if "ingrédient" in section_title or "ingredient" in section_title
    ), "")
    description = build_precise_source_description(
        title, raw_description, category=category, dosage_form=dosage_form,
    )
    page_product_code = (
        embedded.get("product_code") or structured.get("product_code")
        or first_regex(html, [
            r'"sku"\s*:\s*"([^"]+)"',
            r'"productCode"\s*:\s*"([^"]+)"',
        ])
    )
    if not page_product_code:
        page_product_code = first_regex(str(url or ""), [r"/p/0*([0-9]+)(?:[/?#]|$)"])
    if product_code and page_product_code and (
        _familiprix_code_key(product_code) != _familiprix_code_key(page_product_code)
    ):
        return None
    if not title:
        return None
    return attach_regulatory_candidates({
        "name": title, "brand": brand, "description": description,
        "barcode": barcode, "source": "Familiprix", "source_url": url,
        "image_url": image_url,
        "product_code": str(page_product_code or product_code or "").strip(),
        "source_record_id": str(page_product_code or product_code or barcode).strip(),
        "category": category,
        "package_size": package_size,
        "package_unit": package_unit,
        "dosage_form": dosage_form,
        "strength": (
            specifications.get("teneur", "")
            if specifications.get("teneur", "").replace("0", "").strip(" ./") else ""
        ),
        "ingredients": ingredients,
        "purpose": purpose,
        "official_name_fr": title,
    }, html)


def lookup_familiprix_product(barcode, barcode_candidates=None, product_code="",
                              direct_only=False, search_only=False):
    barcode_candidates = barcode_candidates or build_barcode_candidates(barcode)
    clean_product_code = normalized_digits(product_code)
    if not search_only and clean_product_code and len(clean_product_code) <= 18:
        # Familiprix product URLs accept the exact 18-digit internal code and
        # redirect to the canonical page. This avoids four search-page requests
        # for the normal planogram path.
        direct_url = (
            f"{FAMILIPRIX_BASE_URL}/fr/p/"
            f"{clean_product_code.zfill(18)}"
        )
        direct_html, direct_final_url = fetch_text(direct_url)
        if direct_html:
            direct_product = parse_familiprix_product_page(
                direct_html, direct_final_url or direct_url, barcode,
                barcode_candidates, product_code=product_code,
            )
            if direct_product:
                return direct_product
    if direct_only:
        return None

    search_terms = list(dict.fromkeys(
        value for value in (
            str(product_code or "").strip(),
            str(barcode or "").strip(),
        ) if value
    ))
    search_urls = []
    for term in search_terms:
        search_urls.append(
            f"{FAMILIPRIX_BASE_URL}/fr/search?"
            f"{urlencode({'text': term})}"
        )
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
        product = parse_familiprix_product_page(
            product_html, product_final_url, barcode, barcode_candidates,
            product_code=product_code,
        )
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
    return attach_regulatory_candidates({
        "name": title, "brand": brand, "description": description,
        "barcode": barcode, "source": source_name, "source_url": url,
        "image_url": image_url,
    }, html)


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
        with _safe_urlopen(request_obj, timeout=3) as response:
            payload = json.loads(_read_limited_response(response).decode("utf-8"))
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
    return attach_regulatory_candidates({
        "name": name or brand, "brand": brand,
        "description": " | ".join(description_parts), "barcode": barcode,
        "source": source_name, "source_url": product.get("url", ""),
        "image_url": product.get("image_front_url", ""),
    }, json.dumps(product, ensure_ascii=False))


def lookup_upcitemdb(barcode):
    digits = normalized_digits(barcode)
    if not digits:
        return None
    request_obj = Request(f"https://api.upcitemdb.com/prod/trial/lookup?upc={digits}",
                          headers={"User-Agent": "FamiliprixLocator/0.1", "Accept": "application/json"})
    try:
        with _safe_urlopen(request_obj, timeout=5) as response:
            payload = json.loads(_read_limited_response(response).decode("utf-8"))
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
    return attach_regulatory_candidates({
        "name": name, "brand": brand, "description": description,
        "barcode": digits, "source": "UPC Item DB",
        "source_url": f"https://www.upcitemdb.com/upc/{digits}",
        "image_url": image_url,
    }, json.dumps(item, ensure_ascii=False))


def lookup_ean_search(barcode):
    digits = normalized_digits(barcode)
    if not digits:
        return None
    request_obj = Request(
        f"https://api.ean-search.org/api?op=barcode-lookup&ean={digits}&lang=1&format=json",
        headers={"User-Agent": "FamiliprixLocator/0.1", "Accept": "application/json"},
    )
    try:
        with _safe_urlopen(request_obj, timeout=5) as response:
            payload = json.loads(_read_limited_response(response).decode("utf-8"))
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
        with _safe_urlopen(request_obj, timeout=8) as response:
            html = _read_limited_response(response).decode("utf-8", errors="ignore")
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
    return attach_regulatory_candidates({
        "name": name.strip(), "brand": brand or infer_brand_from_title(name),
        "description": description, "barcode": digits,
        "source": "Barcode Lookup", "source_url": url,
        "image_url": image_url or "",
    }, html)


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
        with _safe_urlopen(request_obj, timeout=8) as response:
            html = _read_limited_response(response).decode("utf-8", errors="ignore")
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
    return attach_regulatory_candidates({
        "name": name.strip(), "brand": brand or infer_brand_from_title(name),
        "description": description, "barcode": digits,
        "source": "Go UPC", "source_url": url,
        "image_url": image_url or "",
    }, html)


def _fetch_json(url, timeout=5):
    req = Request(url, headers={"User-Agent": "FamiliprixLocator/0.1", "Accept": "application/json"})
    try:
        with _safe_urlopen(req, timeout=timeout) as response:
            return json.loads(_read_limited_response(response).decode("utf-8", errors="ignore"))
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
    verified_fields = set(product.get("_verified_fields") or [])
    description = str(product.get("description", "") or "").strip()
    description_verified = bool(
        description and (
            "description" in verified_fields
            or product.get("description_status") == "verified"
        )
    )

    def verified_value(field, status_field=""):
        value = str(product.get(field, "") or "").strip()
        if not value:
            return ""
        if field in verified_fields:
            return value
        if status_field and product.get(status_field) == "verified":
            return value
        return ""

    aisle = str(product.get("aisle", "")).strip()
    location = (f"Allée {aisle} {side_display_label(product.get('side',''))} T{str(product.get('shelf','')).strip()}"
                if aisle else "En magasin — position à confirmer")
    ctx = {
        "name":     str(product.get("name", "")).strip(),
        "brand":    verified_value("brand"),
        "notes":    description,
        "description_verified": description_verified,
        "description_status": str(
            product.get("description_status", "unverified") or "unverified"
        ),
        "unverified_description_included": bool(
            description and not description_verified
        ),
        "location": location,
        # The UPC is essential context: without it a question that names a UPC
        # ("quelle saveur a le 0605388...") could never be matched to its product.
        "upc":      str(product.get("barcode", "")).strip(),
        "exact_package_verified": bool(
            product.get("identity_status") == "verified"
            or gtin_check_digit_valid(product.get("barcode", ""))
        ),
        "data_status": str(
            product.get("data_status", "complete_unverified")
            or "complete_unverified"
        ),
        "unverified_information_omitted": True,
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
                                 intent_expansion_terms, abbreviation_terms, build_barcode_candidates,
                                 materialize_reference_rows)
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
        exact_reference_rows = [
            row for row in _reference_corpus(db)
            if row["_bc"] and row["_bc"] in upc_digits
        ]
        for item in materialize_reference_rows(db, exact_reference_rows):
            add(item, normalized_digits(item.get("barcode", "")))

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
                scored.append((s, 1, row, row["_bc"]))
        scored.sort(key=lambda x: (-x[0], x[1], str(x[2].get("name", "")).lower()))
    # Full catalogue metadata stays in PostgreSQL. Hydrate only the small ranked
    # window that can actually reach the model context.
    ranked_window = scored[:max(limit * 3, limit)]
    reference_items = materialize_reference_rows(
        db, [item for _, kind, item, _ in ranked_window if kind == 1],
    )
    reference_by_barcode = {
        normalized_digits(item.get("barcode", "")): item
        for item in reference_items
    }
    for _, kind, item, bc in ranked_window:
        if len(out) >= limit:
            break
        if kind == 1:
            item = reference_by_barcode.get(bc)
            if not item:
                continue
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
    if provider["name"] == "kimi":
        return generate_product_assist_payload_kimi(name, brand, description, barcode)
    if provider["name"] == "deepseek":
        return generate_product_assist_payload_deepseek(name, brand, description, barcode)
    if provider["name"] == "gemini":
        return generate_product_assist_payload_gemini(name, brand, description, barcode)
    if provider["name"] == "openai":
        return generate_product_assist_payload_openai(name, brand, description, barcode)
    return None


def generate_client_help_payload(question, products):
    provider = configured_ai_provider()
    if provider["name"] == "kimi":
        return generate_client_help_payload_kimi(question, products)
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
        # Keep employee-facing AI calls inside the configured response budget.
        with _safe_urlopen(request_obj, timeout=_AI_REQUEST_TIMEOUT_SECONDS) as response:
            raw_response = json.loads(_read_limited_response(response).decode("utf-8"))
    except HTTPError as exc:
        body = ""
        try: body = exc.read().decode("utf-8", "replace")[:250]
        except Exception: pass
        _set_ai_error(f"Le service de réponse est temporairement indisponible (HTTP {exc.code}).")
        return None
    except (URLError, TimeoutError) as exc:
        _set_ai_error("Le service de réponse est injoignable (réseau ou délai dépassé).")
        return None
    except json.JSONDecodeError:
        _set_ai_error("Le service a renvoyé une réponse illisible.")
        return None
    usage = raw_response.get("usageMetadata", {})
    _log_ai_usage("gemini", usage.get("promptTokenCount", 0), usage.get("candidatesTokenCount", 0), question)
    raw_text = extract_gemini_output_text(raw_response)
    if not raw_text:
        cands = raw_response.get("candidates", [])
        reason = cands[0].get("finishReason", "?") if cands else "?"
        blocked = raw_response.get("promptFeedback", {}).get("blockReason", "")
        _set_ai_error(f"Le service a renvoyé une réponse vide (raison={reason}"
                      + (f", bloqué={blocked}" if blocked else "") + ").")
        return None
    try:
        parsed = json.loads(raw_text)
    except json.JSONDecodeError:
        _set_ai_error("Le service a renvoyé une réponse invalide.")
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
        with _safe_urlopen(request_obj, timeout=_AI_REQUEST_TIMEOUT_SECONDS) as response:
            raw_response = json.loads(_read_limited_response(response).decode("utf-8"))
    except HTTPError as exc:
        body = ""
        try: body = exc.read().decode("utf-8", "replace")[:250]
        except Exception: pass
        _set_ai_error(f"Le service de réponse est temporairement indisponible (HTTP {exc.code}).")
        return None
    except (URLError, TimeoutError) as exc:
        _set_ai_error("Le service de réponse est injoignable (réseau ou délai dépassé).")
        return None
    except json.JSONDecodeError:
        _set_ai_error("Le service a renvoyé une réponse illisible.")
        return None
    usage = raw_response.get("usage", {})
    _log_ai_usage("openai", usage.get("input_tokens", 0), usage.get("output_tokens", 0), question)
    raw_text = extract_openai_output_text(raw_response)
    if not raw_text:
        _set_ai_error("Le service a renvoyé une réponse vide.")
        return None
    try:
        parsed = json.loads(raw_text)
    except json.JSONDecodeError:
        _set_ai_error("Le service a renvoyé une réponse invalide.")
        return None
    return normalize_client_help_payload(parsed)


def _parse_chat_json(raw_text):
    text = str(raw_text or "").strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s*```$", "", text)
    try:
        parsed = json.loads(text)
    except (TypeError, json.JSONDecodeError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _partial_json_answer(raw_text):
    """Recover a completed ``answer`` string from a bounded JSON stream."""
    match = re.search(
        r'"answer"\s*:\s*("(?:\\.|[^"\\])*")',
        str(raw_text or ""),
        flags=re.DOTALL,
    )
    if not match:
        return ""
    try:
        return str(json.loads(match.group(1)) or "").strip()
    except (TypeError, json.JSONDecodeError):
        return ""


def _partial_json_string_array(raw_text, field_name, max_items=16):
    """Recover one completed string array from an unfinished JSON stream."""
    match = re.search(
        rf'"{re.escape(str(field_name))}"\s*:\s*',
        str(raw_text or ""),
    )
    if not match:
        return []
    try:
        value, _end = json.JSONDecoder().raw_decode(
            str(raw_text or "")[match.end():]
        )
    except (TypeError, json.JSONDecodeError):
        return []
    return _clean_ai_string_list(value, max_items)


def _read_kimi_stream(response, deadline):
    content_parts = []
    usage = {}
    total_bytes = 0
    while time.monotonic() < deadline:
        remaining = max(0.05, deadline - time.monotonic())
        try:
            response_socket = getattr(
                getattr(getattr(response, "fp", None), "raw", None),
                "_sock", None,
            )
            if response_socket is not None:
                response_socket.settimeout(remaining)
            raw_line = response.readline()
        except (OSError, TimeoutError):
            break
        if not raw_line:
            break
        total_bytes += len(raw_line)
        if total_bytes > _MAX_ONLINE_BODY_BYTES:
            break
        line = raw_line.decode("utf-8", "replace").strip()
        if not line.startswith("data:"):
            continue
        data = line[5:].strip()
        if data == "[DONE]":
            break
        try:
            event = json.loads(data)
        except json.JSONDecodeError:
            continue
        event_usage = event.get("usage")
        if isinstance(event_usage, dict):
            usage = event_usage
        choices = event.get("choices") or []
        delta = (choices[0] if choices else {}).get("delta") or {}
        content = delta.get("content")
        if content:
            content_parts.append(str(content))
    raw_text = "".join(content_parts)
    parsed = _parse_chat_json(raw_text)
    if parsed is not None:
        return parsed, usage
    partial_answer = _partial_json_answer(raw_text)
    partial_selected_ids = _partial_json_string_array(
        raw_text, "selected_product_ids", max_items=16,
    )
    partial_source_ids = _partial_json_string_array(
        raw_text, "source_ids", max_items=16,
    )
    if partial_answer or partial_selected_ids:
        return {
            "_partial_answer": partial_answer,
            "_partial_selected_product_ids": partial_selected_ids,
            "_partial_source_ids": partial_source_ids,
        }, usage
    return None, usage


def _available_kimi_models():
    now = time.time()
    cached = _KIMI_MODELS_CACHE.get("models") or set()
    if now - float(_KIMI_MODELS_CACHE.get("checked_at", 0) or 0) < 3600:
        return set(cached)
    with _KIMI_MODELS_LOCK:
        cached = _KIMI_MODELS_CACHE.get("models") or set()
        if now - float(
            _KIMI_MODELS_CACHE.get("checked_at", 0) or 0
        ) < 3600:
            return set(cached)
        request_obj = Request(
            f"{KIMI_BASE_URL}/models",
            headers={"Authorization": f"Bearer {KIMI_API_KEY}"},
            method="GET",
        )
        try:
            with _safe_urlopen(request_obj, timeout=3) as response:
                payload = json.loads(
                    _read_limited_response(response, max_bytes=250_000).decode(
                        "utf-8"
                    )
                )
            models = {
                str(item.get("id", "") or "").strip()
                for item in payload.get("data", [])
                if isinstance(item, dict) and str(item.get("id", "") or "").strip()
            }
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError):
            models = set()
        _KIMI_MODELS_CACHE.update(checked_at=now, models=models)
        return models


def _select_kimi_model(quality_mode=False, realtime_model=False):
    configured = (
        KIMI_DOCUMENTED_MODEL if quality_mode
        else KIMI_REALTIME_MODEL if realtime_model
        else KIMI_MODEL
    )
    if configured.lower() not in {"auto", "latest"}:
        return configured

    available = _available_kimi_models()
    preferences = (
        "kimi-k3",
        "kimi-k2.6",
        "kimi-k2.5",
        "moonshot-v1-8k",
        "moonshot-v1-32k",
        "moonshot-v1-128k",
    )
    if available:
        return next(
            (model for model in preferences if model in available),
            "kimi-k3",
        )
    return "kimi-k3"


def _kimi_json_request(messages, max_tokens, question_preview="", quality_mode=False,
                       timeout_seconds=None, realtime_model=False,
                       schema_name="", schema=None):
    """Call Kimi's OpenAI-compatible endpoint with bounded response memory."""
    model = _select_kimi_model(
        quality_mode=quality_mode, realtime_model=realtime_model
    )
    global _AI_LAST_MODEL
    _AI_LAST_MODEL = model
    payload = {
        "model": model,
        "messages": messages,
    }
    if model.startswith("kimi-k3"):
        # K3 uses max_completion_tokens and always reasons. Both response modes
        # stay bounded by their configured effort so an employee receives a
        # completed answer inside the synchronous service window.
        payload["max_completion_tokens"] = int(max_tokens)
        payload["reasoning_effort"] = (
            KIMI_DOCUMENTED_REASONING_EFFORT
            if quality_mode else KIMI_REALTIME_REASONING_EFFORT
        )
    else:
        payload["max_tokens"] = int(max_tokens)

    # K2.6 and K3 both support strict structured output. Using the supplied
    # schema matters here: a generic JSON object can finish with an answer but
    # omit the product IDs that keep the UI grounded in store inventory.
    if isinstance(schema, dict) and schema_name:
        payload["response_format"] = {
            "type": "json_schema",
            "json_schema": {
                "name": re.sub(r"[^a-zA-Z0-9_-]", "_", schema_name)[:64],
                "strict": True,
                "schema": schema,
            },
        }
    else:
        payload["response_format"] = {"type": "json_object"}

    if model.startswith(("kimi-k2.6", "kimi-k2.5")):
        # K2.6 long-thinking shares max_tokens with the visible answer and is
        # intentionally opt-in for this synchronous employee workflow. The
        # normal documented request still analyses the full question, but does
        # not spend the HTTP budget on hidden reasoning tokens.
        if quality_mode and _KIMI_DOCUMENTED_LONG_THINKING:
            payload["thinking"] = {"type": "enabled"}
            payload["max_tokens"] = max(16_000, int(max_tokens))
        else:
            payload["thinking"] = {"type": "disabled"}
    request_obj = Request(
        f"{KIMI_BASE_URL}/chat/completions",
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {KIMI_API_KEY}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        timeout = timeout_seconds or (
            _AI_DOCUMENTED_REQUEST_TIMEOUT_SECONDS
            if quality_mode else _AI_REQUEST_TIMEOUT_SECONDS
        )
        stream_response = bool(
            realtime_model
            and model.startswith(("kimi-k3", "kimi-k2.6", "kimi-k2.5"))
        )
        if stream_response:
            payload["stream"] = True
            request_obj = Request(
                f"{KIMI_BASE_URL}/chat/completions",
                data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
                headers={
                    "Authorization": f"Bearer {KIMI_API_KEY}",
                    "Content-Type": "application/json",
                },
                method="POST",
            )
        request_deadline = time.monotonic() + timeout
        with _safe_urlopen(request_obj, timeout=timeout) as response:
            if stream_response:
                parsed, usage = _read_kimi_stream(
                    response, request_deadline
                )
                if parsed is None:
                    _set_ai_error(
                        "Le service n'a pas terminé sa réponse dans le délai prévu."
                    )
                    return None
                _log_ai_usage(
                    "kimi",
                    usage.get("prompt_tokens", 0),
                    usage.get("completion_tokens", 0),
                    question_preview,
                    model_override=model,
                )
                return parsed
            raw_response = json.loads(
                _read_limited_response(response).decode("utf-8")
            )
    except HTTPError as exc:
        body = ""
        try:
            body = exc.read().decode("utf-8", "replace")[:1000]
        except Exception:
            pass
        detail = ""
        try:
            error = json.loads(body).get("error") or {}
            detail = str(
                error.get("message", "") if isinstance(error, dict) else error
            ).strip()
        except (TypeError, json.JSONDecodeError):
            detail = ""
        _set_ai_error(
            "Le service de réponse est temporairement indisponible "
            f"(HTTP {exc.code}{': ' + detail[:180] if detail else ''})."
        )
        return None
    except (URLError, TimeoutError):
        _set_ai_error(
            "Le service de réponse est injoignable (réseau ou délai dépassé)."
        )
        return None
    except json.JSONDecodeError:
        _set_ai_error("Le service a renvoyé une réponse illisible.")
        return None

    usage = raw_response.get("usage", {})
    _log_ai_usage(
        "kimi",
        usage.get("prompt_tokens", 0),
        usage.get("completion_tokens", 0),
        question_preview,
        model_override=model,
    )
    choices = raw_response.get("choices", [])
    message = (choices[0] if choices else {}).get("message") or {}
    parsed = _parse_chat_json(message.get("content", ""))
    if parsed is None:
        _set_ai_error("Le service a renvoyé une réponse invalide.")
    return parsed


def _deepseek_json_request(messages, max_tokens, question_preview="", quality_mode=False,
                           timeout_seconds=None):
    """Call DeepSeek's OpenAI-compatible chat endpoint and return parsed JSON."""
    model = DEEPSEEK_DOCUMENTED_MODEL if quality_mode else DEEPSEEK_MODEL
    payload = {
        "model": model,
        "messages": messages,
        "max_tokens": max_tokens,
        "response_format": {"type": "json_object"},
    }
    if quality_mode and _DEEPSEEK_DOCUMENTED_THINKING:
        payload["thinking"] = {"type": "enabled"}
        payload["reasoning_effort"] = "high"
    else:
        payload["thinking"] = {"type": "disabled"}
        payload["temperature"] = 0.2
    request_obj = Request(
        f"{DEEPSEEK_BASE_URL}/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Authorization": f"Bearer {DEEPSEEK_API_KEY}",
                 "Content-Type": "application/json"},
        method="POST",
    )
    try:
        timeout = timeout_seconds or (
            _AI_DOCUMENTED_REQUEST_TIMEOUT_SECONDS
            if quality_mode else _AI_REQUEST_TIMEOUT_SECONDS
        )
        with _safe_urlopen(request_obj, timeout=timeout) as response:
            raw_response = json.loads(_read_limited_response(response).decode("utf-8"))
    except HTTPError as exc:
        body = ""
        try:
            body = exc.read().decode("utf-8", "replace")[:250]
        except Exception:
            pass
        _set_ai_error(f"Le service de réponse est temporairement indisponible (HTTP {exc.code}).")
        return None
    except (URLError, TimeoutError) as exc:
        _set_ai_error("Le service de réponse est injoignable (réseau ou délai dépassé).")
        return None
    except json.JSONDecodeError:
        _set_ai_error("Le service a renvoyé une réponse illisible.")
        return None

    usage = raw_response.get("usage", {})
    _log_ai_usage("deepseek", usage.get("prompt_tokens", 0),
                  usage.get("completion_tokens", 0), question_preview,
                  model_override=model)
    choices = raw_response.get("choices", [])
    raw_text = str(((choices[0] if choices else {}).get("message") or {}).get("content", "")).strip()
    if not raw_text:
        _set_ai_error("Le service a renvoyé une réponse vide.")
        return None
    parsed = _parse_chat_json(raw_text)
    if parsed is None:
        _set_ai_error("Le service a renvoyé une réponse invalide.")
    return parsed


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
        with _safe_urlopen(request_obj, timeout=_AI_REQUEST_TIMEOUT_SECONDS) as response:
            raw_response = json.loads(_read_limited_response(response).decode("utf-8"))
    except HTTPError as exc:
        _set_ai_error(f"Le service de réponse est temporairement indisponible (HTTP {exc.code}).")
        return None
    except (URLError, TimeoutError, json.JSONDecodeError) as exc:
        _set_ai_error("Le service n'a pas pu produire la réponse demandée.")
        return None
    usage = raw_response.get("usageMetadata", {})
    _log_ai_usage("gemini", usage.get("promptTokenCount", 0),
                  usage.get("candidatesTokenCount", 0), question_preview)
    raw_text = extract_gemini_output_text(raw_response)
    try:
        return json.loads(raw_text) if raw_text else None
    except json.JSONDecodeError:
        _set_ai_error("Le service a renvoyé une réponse invalide.")
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
        with _safe_urlopen(request_obj, timeout=_AI_REQUEST_TIMEOUT_SECONDS) as response:
            raw_response = json.loads(_read_limited_response(response).decode("utf-8"))
    except HTTPError as exc:
        _set_ai_error(f"Le service de réponse est temporairement indisponible (HTTP {exc.code}).")
        return None
    except (URLError, TimeoutError, json.JSONDecodeError) as exc:
        _set_ai_error("Le service n'a pas pu produire la réponse demandée.")
        return None
    usage = raw_response.get("usage", {})
    _log_ai_usage("openai", usage.get("input_tokens", 0),
                  usage.get("output_tokens", 0), question_preview)
    raw_text = extract_openai_output_text(raw_response)
    try:
        return json.loads(raw_text) if raw_text else None
    except json.JSONDecodeError:
        _set_ai_error("Le service a renvoyé une réponse invalide.")
        return None


def _provider_structured_request(system_prompt, user_payload, max_tokens,
                                 schema_name, schema, question_preview="",
                                 quality_mode=False, timeout_seconds=None,
                                 realtime_model=False):
    provider = configured_ai_provider()["name"]
    if provider == "kimi":
        return _kimi_json_request([
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
        ], max_tokens=max_tokens, question_preview=question_preview,
           quality_mode=quality_mode, timeout_seconds=timeout_seconds,
           realtime_model=realtime_model, schema_name=schema_name, schema=schema)
    if provider == "deepseek":
        return _deepseek_json_request([
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
        ], max_tokens=max_tokens, question_preview=question_preview,
           quality_mode=quality_mode, timeout_seconds=timeout_seconds)
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
        "retrieval_scope": {
            "type": "string",
            "enum": ["store", "context", "context_and_store"],
        },
    },
    "required": ["intent", "corrected_query", "search_queries", "keywords",
                 "must_include", "exclude", "wants_all", "needs_comparison",
                 "answer_language", "medical", "retrieval_scope"],
    "additionalProperties": False,
}

_CLIENT_QUERY_PLAN_INSTRUCTIONS = (
    "Tu es le planificateur de recherche d'un catalogue de pharmacie québécoise. "
    "Lis et analyse d'abord la phrase complète et l'historique récent sans perdre son intention. "
    "Identifie l'objet principal, le besoin, les contraintes, les variantes demandées et ce qui "
    "doit être exclu. Ne traite jamais un mot isolé comme l'intention complète. Une question "
    "de suivi comme 'et pour les enfants?' doit devenir une requête autonome qui conserve le "
    "produit ou besoin discuté juste avant. Corrige les fautes probables "
    "de marques et produits (exemple: advile/dadvile -> Advil), mais ne transforme jamais "
    "une demande de nourriture en demande de médicament. Génère des requêtes bilingues "
    "français/anglais et uniquement des synonymes précis qui peuvent réellement apparaître dans "
    "le nom, la marque, la description ou les notes d'un produit. search_queries contient des "
    "formulations de recherche courtes, pas des phrases de réponse. must_include contient les "
    "concepts indispensables à l'identité ou à l'usage demandé; exclude contient les catégories "
    "voisines mais non demandées. Pour un symptôme, ajoute les familles ou ingrédients "
    "raisonnablement pertinents sans prétendre poser un diagnostic; pour un assortiment, "
    "conserve toutes les contraintes de catégorie, saveur, format et marque. Le plan doit "
    "fonctionner pour n'importe quelle catégorie du magasin, pas seulement les médicaments. "
    "wants_all=true pour 'tous/toutes/all/each'. "
    "needs_comparison=true quand l'utilisateur demande une différence ou comparaison. "
    "retrieval_scope='context' seulement si une question de suivi vise clairement les produits "
    "déjà montrés; utilise 'context_and_store' pour demander des alternatives et 'store' pour "
    "une nouvelle recherche. "
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
        "retrieval_scope": (
            str(parsed.get("retrieval_scope", "store") or "store").strip()
            if str(parsed.get("retrieval_scope", "store") or "store").strip()
            in {"store", "context", "context_and_store"}
            else "store"
        ),
    }


def normalize_client_history(value, max_messages=10):
    if not isinstance(value, list):
        return []
    history = []
    for raw in value[-max_messages:]:
        if not isinstance(raw, dict):
            continue
        role = str(raw.get("role", "")).strip().lower()
        content = str(raw.get("content", "") or "").strip()
        if role not in {"user", "assistant"} or not content:
            continue
        history.append({"role": role, "content": content[:1600]})
    return history


def generate_client_query_plan(question, history=None):
    normalized_history = normalize_client_history(history)
    provider_info = configured_ai_provider()
    provider_name = str(
        provider_info.get("name", "") if isinstance(provider_info, dict) else ""
    )
    cache_key = hashlib.sha256(json.dumps({
        "provider": provider_name,
        "model": KIMI_REALTIME_MODEL,
        "question": re.sub(r"\s+", " ", str(question or "")).strip().lower(),
        "history": normalized_history[-6:],
    }, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
    now = time.time()
    with _CLIENT_QUERY_PLAN_CACHE_LOCK:
        expired = [
            key for key, entry in _CLIENT_QUERY_PLAN_CACHE.items()
            if now - float(entry.get("created_at", 0) or 0)
            > _CLIENT_QUERY_PLAN_CACHE_TTL_SECONDS
        ]
        for key in expired:
            _CLIENT_QUERY_PLAN_CACHE.pop(key, None)
        cached = _CLIENT_QUERY_PLAN_CACHE.pop(cache_key, None)
        if cached:
            _CLIENT_QUERY_PLAN_CACHE[cache_key] = cached
            return json.loads(json.dumps(cached["plan"], ensure_ascii=False))

    parsed = _provider_structured_request(
        _CLIENT_QUERY_PLAN_INSTRUCTIONS,
        {"conversation": normalized_history, "question": question,
         "required_schema": _CLIENT_QUERY_PLAN_SCHEMA},
        max_tokens=900,
        schema_name="client_query_plan",
        schema=_CLIENT_QUERY_PLAN_SCHEMA,
        question_preview=question,
        timeout_seconds=_AI_QUERY_PLAN_TIMEOUT_SECONDS,
        realtime_model=True,
    )
    if not isinstance(parsed, dict):
        return None
    plan = normalize_client_query_plan(parsed, question)
    with _CLIENT_QUERY_PLAN_CACHE_LOCK:
        _CLIENT_QUERY_PLAN_CACHE.pop(cache_key, None)
        _CLIENT_QUERY_PLAN_CACHE[cache_key] = {
            "created_at": now,
            "plan": json.loads(json.dumps(plan, ensure_ascii=False)),
        }
        while len(_CLIENT_QUERY_PLAN_CACHE) > _CLIENT_QUERY_PLAN_CACHE_MAX:
            _CLIENT_QUERY_PLAN_CACHE.popitem(last=False)
    return plan


def classify_client_request(question, follow_up=False, focus_product_id="", selected_text=""):
    """Route product lookups locally and reserve AI for questions needing an answer."""
    from routes.products import intent_expansion_terms, normalize_search_text

    normalized = normalize_search_text(question)
    if follow_up or focus_product_id or selected_text:
        return "detailed"
    detailed_phrases = (
        "difference", "differer", "compare", "comparaison", "versus", " vs ",
        "pour quelle", "quel pour", "quelle pour", "situation", "lequel choisir",
        "laquelle choisir", "quoi choisir", "devrais", "dois je", "peut on",
        "comment", "pourquoi", "c est quoi", "what is", "which one", "should i",
        "recommend", "conseille", "meilleur", "mieux", "avantage", "inconvenient",
        "liquide ou", "comprime ou", "capsule ou", "gel ou", "pour un enfant",
    )
    padded = f" {normalized} "
    if any(phrase in padded for phrase in detailed_phrases):
        return "detailed"
    words = set(normalized.split())
    if (("pour" in words and words.intersection({"quel", "quelle", "lequel", "laquelle"})) or
            ("for" in words and words.intersection({"what", "which"}))):
        return "detailed"
    if intent_expansion_terms(question):
        return "detailed"
    return "lookup"


def build_client_query_plan(question, mode="lookup"):
    """Fast deterministic plan used by retrieval; AI no longer blocks search."""
    from routes.products import (
        client_request_intent, intent_expansion_terms, normalize_search_text,
    )

    normalized = normalize_search_text(question)
    padded = f" {normalized} "
    specific_intent = client_request_intent(question)
    comparison_markers = ("difference", "compare", "comparaison", "versus", " vs ", " ou ")
    wants_all = any(word in normalized.split() for word in ("all", "tout", "tous", "toute", "toutes"))
    english_words = sum(word in normalized.split() for word in ("what", "which", "show", "find", "all", "tell"))
    return {
        "intent": specific_intent or (
            "product_lookup" if mode == "lookup" else "advice_or_comparison"
        ),
        "corrected_query": question,
        "search_queries": [question],
        # The complete question is already searched once. Splitting a local
        # deterministic plan into standalone words such as "comment",
        # "choisir" and "formats" multiplies scoring work and broadens results.
        # Semantic-planner keywords are still accepted later when local recall
        # is genuinely weak.
        "keywords": [],
        "must_include": [],
        "exclude": [],
        "wants_all": wants_all,
        "needs_comparison": any(marker in padded for marker in comparison_markers),
        "answer_language": "en" if english_words >= 2 else "fr",
        "medical": bool(intent_expansion_terms(question)),
        "retrieval_scope": "store",
    }


_CLIENT_VERIFICATION_SCHEMA = {
    "type": "object",
    "properties": {
        "selected_product_ids": {
            "type": "array", "items": {"type": "string"}, "maxItems": 8,
        },
        "answer": {"type": "string"},
        "follow_up_questions": {
            "type": "array", "items": {"type": "string"}, "maxItems": 4,
        },
        "safety_flags": {
            "type": "array", "items": {"type": "string"}, "maxItems": 5,
        },
        "pharmacist_referral": {"type": "boolean"},
        "pharmacist_reason": {"type": "string"},
    },
    "required": ["selected_product_ids", "answer", "follow_up_questions",
                 "safety_flags", "pharmacist_referral", "pharmacist_reason"],
    "additionalProperties": False,
}

_CLIENT_VERIFICATION_INSTRUCTIONS = (
    "Tu rédiges une réponse de travail claire pour un employé Familiprix. Les candidats viennent "
    "uniquement du plan réel du magasin et ont déjà été classés par un moteur déterministe. "
    "selected_product_ids identifie tous les produits suffisamment liés à la demande qui doivent "
    "rester dans le résultat final; utilise uniquement des candidate_id fournis, sans en inventer, "
    "et écarte tous les candidats non pertinents. Pour une faute "
    "de marque, comprends la vraie marque correspondante. Une demande "
    "sur ce qu'il faut manger ne justifie pas automatiquement un analgésique. Ne prétends pas "
    "connaître une saveur, un ingrédient ou un dosage absent des données. Rédige answer dans "
    "answer_language, directement selon la demande et en tenant compte de l'historique. "
    "Les descriptions disponibles peuvent être marquées non vérifiées: utilise-les comme contexte "
    "pratique, mais ne présente pas un attribut incertain comme un fait confirmé. Les autres champs "
    "non vérifiés sont volontairement omis: ne les reconstitue jamais à partir du nom ou de tes "
    "connaissances. Si data_status n'est pas complete_verified, "
    "indique brièvement ce que l'employé doit confirmer sur l'emballage lorsque cette "
    "information est nécessaire. "
    "verified_identifiers contient uniquement des identifiants confirmés. "
    "unconfirmed_identifier_candidates contient des DIN, NPN ou DIN-HM candidats qui peuvent "
    "être erronés: utilise-les seulement comme indices de recherche pour comprendre pourquoi un "
    "produit correspond. Si tu cites un de ces numéros, écris explicitement qu'il est à confirmer "
    "sur l'emballage. Ne le présente jamais comme l'identifiant certain du produit et ne l'utilise "
    "jamais pour déduire un ingrédient, un dosage, une autorisation, une indication, une "
    "équivalence ou toute autre propriété. Un identifiant candidat ne suffit pas non plus à "
    "rattacher automatiquement une fiche réglementaire au produit. "
    "Ne déclare jamais deux produits thérapeutiquement équivalents, interchangeables ou sûrs "
    "comme substituts; une relation de famille ou de format ne prouve pas cela. "
    "Si selected_text_from_previous_answer est fourni, réponds précisément à la question en reliant "
    "ce passage au contexte. Si focused_product_id est fourni, centre la réponse sur ce produit. "
    "Fais une réponse précise, facile à dire au client et suffisamment approfondie pour répondre à TOUTES "
    "les dimensions demandées: 2 à 5 petits paragraphes, sans Markdown, sans **, et sans recopier "
    "une longue liste de produits. Pour une comparaison de formes, distingue explicitement les "
    "liquides/suspensions, comprimés/caplets, capsules liquides/liqui-gels et mini-gels quand ils "
    "sont présents. Explique les différences pratiques pertinentes: façon de les prendre, facilité "
    "à avaler, flexibilité de dose, clientèle/âge indiqué dans les données, ingrédient, dosage et "
    "format. Tu peux employer des connaissances générales de pharmacie pour expliquer une forme, "
    "mais présente-les comme générales et n'attribue jamais au produit un fait absent de sa fiche. "
    "Si beaucoup de produits correspondent, résume les familles et sélectionne tous les produits "
    "pertinents fournis, jusqu'à 16; seules les cartes sélectionnées seront affichées. "
    "Décode les abréviations de planogramme usuelles: ENF=enfants, CO=comprimés, CAPS=capsules, "
    "SIR=sirop, CR=crème, VAPO=vaporisateur, GTTE=gouttes, X/F=extra fort; les nombres indiquent "
    "souvent le dosage ou le format. "
    "Si candidates est vide, dis clairement qu'aucun produit correspondant n'est trouvé dans le plan, "
    "réponds prudemment à la question générale sans nommer de produit et propose de préciser la demande. "
    "Mentionne chaque produit sélectionné "
    "avec son nom EXACT, copié tel quel, afin que l'interface puisse le rendre cliquable. Ne "
    "nomme aucun produit non sélectionné. Pour une comparaison, explique les différences visibles "
    "dans les données. Ne pose pas de diagnostic. Signale le pharmacien pour grossesse, bébé, "
    "interaction, difficulté respiratoire, symptômes graves, fièvre élevée ou persistante, ou "
    "doute médical. Retourne uniquement le JSON demandé."
)


_CLIENT_VERIFICATION_INSTRUCTIONS += (
    " IMPORTANT: write selected_product_ids before answer in the JSON. Select at "
    "most 8 products, and only products that satisfy the request. Never select or "
    "name a product merely to explain that it is excluded. Answer every requested "
    "dimension in 1 to 3 short paragraphs, about 80 to 170 words. Do not repeat a "
    "long product list."
)


def _client_rag_identifier_groups(product):
    """Separate authoritative identifiers from useful, explicitly unsafe clues."""
    identifiers = list(product.get("_identifiers") or [])
    if not identifiers:
        identifiers = list(
            product.get("identifiers")
            or product.get("regulatory_identifiers")
            or []
        )

    normalized = []
    verified_keys = set()
    for raw in identifiers:
        if not isinstance(raw, dict):
            continue
        identifier_type = str(raw.get("type", "") or "").upper().replace("-", "_")
        value = str(raw.get("value", "") or "").strip()
        if not identifier_type or not value:
            continue
        verification_status = str(
            raw.get("verification_status", "") or ""
        ).strip().lower()
        public_status = str(raw.get("status", "") or "").strip().lower()
        confirmed = verification_status == "verified" or public_status == "confirmed"
        record = {
            "type": identifier_type,
            "value": value,
            "authority": str(raw.get("authority", "") or "").strip(),
            "source": str(raw.get("source", "") or "").strip(),
            "match_method": str(raw.get("match_method", "") or "").strip(),
            "confidence": raw.get("confidence", 0),
            "confirmed": confirmed,
        }
        normalized.append(record)
        if confirmed:
            verified_keys.add((identifier_type, value))

    verified_identifiers = []
    unconfirmed_candidates = []
    seen_verified = set()
    seen_candidates = set()
    for identifier in normalized:
        key = (identifier["type"], identifier["value"])
        if identifier["confirmed"]:
            if key in seen_verified:
                continue
            seen_verified.add(key)
            verified_identifiers.append({
                "type": identifier["type"],
                "value": identifier["value"],
                "authority": identifier["authority"],
            })
            continue
        if (
            identifier["type"] not in {"DIN", "NPN", "DIN_HM"}
            or key in verified_keys
            or key in seen_candidates
        ):
            continue
        seen_candidates.add(key)
        try:
            confidence = round(float(identifier["confidence"] or 0), 3)
        except (TypeError, ValueError):
            confidence = 0.0
        unconfirmed_candidates.append({
            "type": identifier["type"],
            "value": identifier["value"],
            "authority": identifier["authority"],
            "source": identifier["source"],
            "match_method": identifier["match_method"],
            "confidence": confidence,
            "status": "unconfirmed",
            "usage": "retrieval_clue_only",
            "may_be_wrong": True,
            "must_confirm_on_package": True,
            "warning": (
                "Association possible mais non confirmée; vérifier le numéro "
                "sur l'emballage avant de le présenter comme exact."
            ),
        })
    return verified_identifiers, unconfirmed_candidates


def product_context_for_client_rag(product):
    context = product_context_for_client_help(product)
    verified_fields = set(product.get("_verified_fields") or [])
    description = str(product.get("description", "") or "").strip()
    verified_identifiers, unconfirmed_identifier_candidates = (
        _client_rag_identifier_groups(product)
    )

    def verified_value(field, status_field=""):
        value = str(product.get(field, "") or "").strip()
        if field in verified_fields or (
            status_field and product.get(status_field) == "verified"
        ):
            return value
        return ""

    context.update({
        "candidate_id": str(product.get("client_id", "")),
        "plan_status": "PLANO" if product.get("is_plano") else "HORS-PLANO",
        "locations": product.get("locations") or [],
        "description": description,
        "description_status": (
            "verified" if "description" in verified_fields else "unverified"
        ),
        "category": verified_value("category"),
        "package_size": verified_value("package_size"),
        "package_unit": verified_value("package_unit"),
        "variant": verified_value("variant"),
        "flavour": verified_value("flavour"),
        "colour": verified_value("colour"),
        "strength": verified_value("strength"),
        "dosage_form": verified_value("dosage_form"),
        "manufacturer": verified_value("manufacturer"),
        "ingredients": verified_value("ingredients"),
        "compatibility": verified_value("compatibility"),
        "purpose": verified_value("purpose"),
        "route_of_administration": verified_value("route_of_administration"),
        "product_code": next((
            str(identifier.get("value", "") or "")
            for identifier in verified_identifiers
            if identifier.get("type") == "FAMILIPRIX_CODE"
        ), ""),
        "field_sources": product.get("_field_sources") or {},
        "verified_fields": sorted(verified_fields),
        "verified_identifiers": verified_identifiers,
        "unconfirmed_identifier_candidates": unconfirmed_identifier_candidates,
    })
    return context


def _unconfirmed_identifier_notice(question, answer, products):
    """Return a mandatory warning when an uncertain regulatory ID is in use."""
    def digit_runs(text):
        return {
            re.sub(r"\D", "", match)
            for match in re.findall(r"\d[\d\s-]{5,18}\d", str(text or ""))
            if 6 <= len(re.sub(r"\D", "", match)) <= 18
        }

    question_text = str(question or "")
    answer_text = str(answer or "")
    referenced_values = digit_runs(question_text) | digit_runs(answer_text)
    identifier_type_requested = bool(re.search(
        r"\b(?:DIN(?:[\s-]?HM)?|NPN)\b",
        question_text,
        flags=re.IGNORECASE,
    ))
    candidates = []
    seen = set()
    for product in products or []:
        _verified, unconfirmed = _client_rag_identifier_groups(product)
        for identifier in unconfirmed:
            key = (identifier["type"], identifier["value"])
            if key in seen:
                continue
            seen.add(key)
            candidates.append(identifier)

    if not candidates:
        return ""
    referenced = [
        identifier for identifier in candidates
        if re.sub(r"\D", "", identifier["value"]) in referenced_values
    ]
    if referenced:
        labels = ", ".join(
            f"{identifier['type'].replace('_', '-')} {identifier['value']}"
            for identifier in referenced[:3]
        )
        return (
            f"{labels}: association de catalogue non confirmée qui peut être "
            "incorrecte; confirmer le numéro sur l'emballage."
        )
    if identifier_type_requested:
        return (
            "Les DIN, NPN ou DIN-HM marqués « À confirmer » sont des associations "
            "de catalogue possibles et peuvent être incorrects; vérifier chaque "
            "numéro sur l'emballage avant de le communiquer."
        )
    return ""


def normalize_verified_client_answer(parsed, valid_ids):
    parsed = parsed if isinstance(parsed, dict) else {}
    valid_ids = set(valid_ids)
    selected = []
    for raw in parsed.get("selected_product_ids", []):
        candidate_id = str(raw or "").strip()
        if candidate_id in valid_ids and candidate_id not in selected:
            selected.append(candidate_id)
        if len(selected) >= 16:
            break
    return {
        "answer": str(parsed.get("answer", "") or "").strip(),
        "selected_product_ids": selected,
        "follow_up_questions": _clean_ai_string_list(parsed.get("follow_up_questions"), 4),
        "safety_flags": _clean_ai_string_list(parsed.get("safety_flags"), 5),
        "pharmacist_referral": bool(parsed.get("pharmacist_referral", False)),
        "pharmacist_reason": str(parsed.get("pharmacist_reason", "") or "").strip(),
    }


def select_client_answer_candidates(candidates, limit=16, diversify_brands=False,
                                    question=""):
    """Keep the AI context small while retaining different product forms."""
    from routes.products import normalize_search_text, tokenize_search_query

    family_markers = (
        ("children", ("enf", "enfant", "pediat", "kids")),
        ("suspension", ("suspension", "sirop", "sir ", " liquide")),
        ("liqui_gel", ("liqui gel", "liq gel", "liquigel", "caps gel")),
        ("mini_gel", ("mini gel", "mini caps")),
        ("tablet", (" comprime", " caplet", " co ", " tablet")),
        ("topical", (" creme", " onguent", " gel topique")),
        ("combination", (" plus acet", " acetaminophene", " rhume", " sinus")),
        ("extra_strength", ("extra fort", " x f ", " forte")),
    )

    def signature(product):
        text = f" {normalize_search_text(' '.join([
            str(product.get('name', '') or ''),
            str(product.get('description', '') or ''),
            str(product.get('usage_notes', '') or ''),
        ]))} "
        families = tuple(name for name, markers in family_markers if any(marker in text for marker in markers))
        if families:
            return families
        tokens = [token for token in text.split() if not token.isdigit()]
        return tuple(tokens[:3])

    selected = []
    if question and candidates:
        searchable = [(
            product,
            f" {normalize_search_text(' '.join([
                str(product.get('name', '') or ''),
                str(product.get('brand', '') or ''),
                str(product.get('description', '') or ''),
                str(product.get('usage_notes', '') or ''),
                str(product.get('category', '') or ''),
                str(product.get('purpose', '') or ''),
            ]))} ",
        ) for product in candidates]
        search_text_by_identity = {
            id(product): text for product, text in searchable
        }
        candidate_order = {
            id(product): index for index, product in enumerate(candidates)
        }
        comparison_terms = []
        seen_comparison_stems = set()
        comparison_object_terms = {
            "bandage", "bandages", "brosse", "brosses", "dent", "dents",
            "dentifrice", "dentifrices", "dressing", "forme", "formes",
            "pansement", "pansements", "produit", "produits", "toothpaste",
            "toothbrush",
        }
        comparison_semantic_stems = {
            "blanchissant": "blanch",
            "blanchiment": "blanch",
            "sensible": "sens",
            "sensibilite": "sens",
            "whitening": "whit",
        }
        for token in tokenize_search_query(question):
            stem = token[:-1] if len(token) >= 6 and token.endswith("s") else token
            stem = comparison_semantic_stems.get(stem, stem)
            if stem in seen_comparison_stems or stem in comparison_object_terms:
                continue
            seen_comparison_stems.add(stem)
            pattern = re.compile(
                rf"(?<![a-z0-9]){re.escape(stem)}[a-z0-9]*(?![a-z0-9])"
            )
            matches = [
                product for product, text in searchable if pattern.search(text)
            ]
            if matches and len(matches) < len(candidates):
                comparison_terms.append((len(matches), pattern, matches))
        # Rare requested concepts are covered first. This prevents a comparison
        # such as "hydrocolloïde ou transparent" from sending eight transparent
        # products to the answerer and omitting the other side entirely.
        comparison_terms.sort(key=lambda item: item[0])
        comparison_patterns = [item[1] for item in comparison_terms]
        for _count, current_pattern, matches in comparison_terms:
            available = [item for item in matches if item not in selected]
            product = min(
                available,
                key=lambda item: (
                    sum(
                        1 for pattern in comparison_patterns
                        if pattern is not current_pattern
                        and pattern.search(search_text_by_identity[id(item)])
                    ),
                    candidate_order[id(item)],
                ),
                default=None,
            )
            if product is not None:
                selected.append(product)
            if len(selected) >= limit:
                return selected

    if diversify_brands:
        seen_brands = set()
        for product in selected:
            brand = normalize_search_text(product.get("brand", ""))
            if not brand:
                brand = normalize_search_text(product.get("name", "")).split(" ", 1)[0]
            if brand:
                seen_brands.add(brand)
        for product in candidates:
            brand = normalize_search_text(product.get("brand", ""))
            if not brand:
                brand = normalize_search_text(product.get("name", "")).split(" ", 1)[0]
            if not brand or brand in seen_brands:
                continue
            seen_brands.add(brand)
            selected.append(product)
            if len(selected) >= limit:
                return selected

    seen = set()
    for product in selected:
        seen.add(signature(product))
    seen_names = {
        normalize_search_text(product.get("name", ""))
        for product in selected
        if normalize_search_text(product.get("name", ""))
    }
    for product in candidates:
        if product in selected:
            continue
        name = normalize_search_text(product.get("name", ""))
        if name and name in seen_names:
            continue
        key = signature(product)
        if key in seen:
            continue
        seen.add(key)
        if name:
            seen_names.add(name)
        selected.append(product)
        if len(selected) >= limit:
            return selected
    for product in candidates:
        if product in selected:
            continue
        name = normalize_search_text(product.get("name", ""))
        if name and name in seen_names:
            continue
        if name:
            seen_names.add(name)
        selected.append(product)
        if len(selected) >= limit:
            return selected
    for product in candidates:
        if product not in selected:
            selected.append(product)
        if len(selected) >= limit:
            break
    return selected


def filter_client_answer_category(question, candidates):
    """Remove obvious cross-category matches before AI verification.

    Product descriptions can legitimately mention an ingredient used by a very
    different category. For example, Dr Teal's bath products mention melatonin
    but must not appear in a supplement comparison unless bath use was requested.
    """
    from routes.products import filter_client_request_products, normalize_search_text

    constrained = filter_client_request_products(candidates, question)
    if len(constrained) != len(candidates):
        candidates = constrained

    normalized_question = normalize_search_text(question)
    if "melaton" not in normalized_question:
        return list(candidates)
    bath_requested = any(marker in normalized_question for marker in (
        "bain", "bath", "epsom", "mousse", "dr teals",
    ))
    if bath_requested:
        return list(candidates)
    bath_name_markers = ("dr teals", "epsom", "b mous", "bain", "bath")
    focused = []
    for product in candidates:
        name = normalize_search_text(product.get("name", ""))
        if "melaton" not in name or any(marker in name for marker in bath_name_markers):
            continue
        focused.append(product)
    return focused or list(candidates)


def _answer_denies_available_inventory(answer):
    """Detect an answer that says nothing exists while products are selected."""
    from routes.products import normalize_search_text

    normalized = normalize_search_text(answer)
    if not normalized:
        return False
    direct_markers = (
        "je n ai trouve aucun", "nous n avons trouve aucun",
        "nous n avons aucun", "aucun produit", "aucune option",
        "pas de produit correspondant", "pas de produit en rayon",
        "no matching product", "could not find any", "did not find any",
    )
    if any(marker in normalized for marker in direct_markers):
        return True
    return bool(re.search(
        r"\baucun(?:e)?\b.{0,90}\b(?:article|capsule|comprime|option|produit)\b",
        normalized,
    ))


def generate_verified_client_answer(question, query_plan, candidates, history=None,
                                    selected_text="", focus_product_id=""):
    include_identifiers = bool(re.search(
        r"\b(?:DIN(?:[\s-]?HM)?|NPN|UPC|GTIN)\b",
        str(question or ""), flags=re.IGNORECASE,
    ))
    contexts = [
        _compact_documented_product_context(
            product, include_identifiers=include_identifiers,
        )
        for product in candidates
    ]
    parsed = _provider_structured_request(
        _CLIENT_VERIFICATION_INSTRUCTIONS,
        {"conversation": normalize_client_history(history), "question": question,
         "selected_text_from_previous_answer": selected_text,
         "focused_product_id": focus_product_id,
         "query_plan": query_plan, "candidates": contexts,
         "required_schema": _CLIENT_VERIFICATION_SCHEMA},
        max_tokens=800,
        schema_name="client_verified_answer",
        schema=_CLIENT_VERIFICATION_SCHEMA,
        question_preview=question,
        realtime_model=True,
        timeout_seconds=10,
    )
    if not isinstance(parsed, dict):
        return None
    partial_answer = str(parsed.get("_partial_answer", "") or "").strip()
    if partial_answer:
        parsed = {
            "answer": partial_answer,
            "selected_product_ids": parsed.get(
                "_partial_selected_product_ids", []
            ),
            "follow_up_questions": [],
            "safety_flags": [],
            "pharmacist_referral": False,
            "pharmacist_reason": "",
        }
    return normalize_verified_client_answer(
        parsed, [product.get("client_id", "") for product in candidates]
    )


_DOCUMENTED_SOURCE_IDS_SCHEMA = {
    "type": "array", "items": {"type": "string"}, "maxItems": 4,
}

_CLIENT_DOCUMENTED_SCHEMA = {
    "type": "object",
    "properties": {
        "selected_product_ids": {
            "type": "array", "items": {"type": "string"}, "maxItems": 8,
        },
        "source_ids": {
            "type": "array", "items": {"type": "string"}, "maxItems": 8,
        },
        "answer": {"type": "string"},
        "key_points": {
            "type": "array", "maxItems": 3,
            "items": {
                "type": "object",
                "properties": {
                    "heading": {"type": "string"},
                    "detail": {"type": "string"},
                    "source_ids": _DOCUMENTED_SOURCE_IDS_SCHEMA,
                },
                "required": ["heading", "detail", "source_ids"],
                "additionalProperties": False,
            },
        },
    },
    "required": [
        "selected_product_ids", "source_ids", "answer", "key_points",
    ],
    "additionalProperties": False,
}

_CLIENT_DOCUMENTED_INSTRUCTIONS = (
    "Tu produis une réponse documentée de très haute qualité pour un employé Familiprix. "
    "Le but est de répondre exactement à la demande, puis de rendre les faits importants "
    "repérables en quelques secondes. La liste de produits est une preuve d'inventaire, pas "
    "la réponse: ne commence jamais par « j'ai trouvé X produits ». Commence par la réponse "
    "ou la décision utile que l'employé peut dire au client. Les produits candidats proviennent "
    "uniquement du plan "
    "actuel du magasin. Les documents fournis sont la seule preuve autorisée pour attribuer "
    "un ingrédient, un dosage, une indication, une contre-indication, un âge, une interaction "
    "ou une propriété à un produit précis. Les fiches Santé Canada ont priorité sur les fiches "
    "de catalogue; les noms de planogramme et descriptions peuvent être abrégés ou incomplets. "
    "Évalue chaque candidat selon le sens de la phrase complète, jamais selon un mot isolé. "
    "Reformule mentalement l'objectif complet avant de répondre: ce que la personne cherche, "
    "le niveau de détail demandé, les critères de choix et la raison attendue. Cette méthode "
    "s'applique à toute question et à toute catégorie du magasin. Un candidat qui partage "
    "seulement un terme avec la demande mais ne satisfait pas son objet ou son usage principal "
    "doit être rejeté. "
    "Un mot désignant une partie du corps ne suffit pas: pour « mal de tête », garde seulement "
    "les produits réellement liés au soulagement de la douleur et rejette notamment les têtes "
    "de brosse à dents, les nettoyants tête-aux-pieds et les produits sans indication pertinente. "
    "N'inclus pas automatiquement les variantes rhume/sinus, nuit ou enfants lorsque ce contexte "
    "n'est pas demandé. Si les candidats ne répondent pas au besoin complet, sélectionne-en moins "
    "ou aucun au lieu de remplir la réponse avec des produits voisins. "
    "Tu peux expliquer une différence générale entre des formes ou catégories, mais indique "
    "clairement qu'elle est générale lorsqu'aucun document ne la confirme pour le produit. "
    "Ne compare jamais le prix, l'économie par dose ou la valeur si aucun prix n'est fourni. "
    "N'affirme une action plus rapide pour un produit précis que si sa fiche fournie le dit. "
    "N'invente jamais de dose, de durée, d'ingrédient, de bénéfice ou de source. "
    "Reste structuré et précis: la réponse complète doit être facile à parcourir rapidement. "
    "answer est une réponse directe de 2 à 4 phrases que l'employé peut dire au client. "
    "La première phrase répond sans détour à la question. Elle doit répondre à « quoi "
    "choisir et pourquoi » lorsque c'est la question, sans se contenter de nommer des produits. "
    "Lorsqu'on demande le meilleur choix, explique d'abord les critères qui changent la décision, "
    "puis indique quel candidat convient à chaque contexte réellement étayé. "
    "Traite chaque dimension explicitement demandée: par exemple types, saveurs et contexte "
    "d'utilisation doivent recevoir trois réponses distinctes, même si certaines données sont "
    "absentes et doivent être signalées comme telles. "
    "Donne de 1 à 3 key_points lorsque les preuves le permettent; chaque élément fait au plus "
    "deux phrases. key_points contient les faits décisifs, les différences pratiques et les "
    "limites de l'information, avec des titres très courts. Ne remplis jamais une section avec "
    "du texte générique uniquement pour atteindre ce nombre. Le serveur "
    "ajoutera lui-même les cartes, emplacements et comparaisons produit par produit: ne les "
    "répète pas. Le serveur ajoute les vérifications médicales. "
    "Chaque affirmation fondée sur un document cite son source_id exact. Tu peux utiliser tes "
    "connaissances générales pour expliquer un concept ou les critères de choix, mais jamais "
    "pour inventer un attribut d'un produit précis; une connaissance générale non documentée "
    "garde source_ids vide. source_ids contient toutes les sources "
    "effectivement utilisées. selected_product_ids garde les produits réellement liés, "
    "jusqu'à 12, et aucun autre. Copie les noms de produits exactement lorsqu'ils apparaissent "
    "dans le texte. Les attributs absents ou signalés non vérifiés dans les candidats ne sont "
    "pas des faits et ne doivent jamais être déduits. Réponds dans answer_language, sans Markdown. "
    "verified_identifiers contient uniquement les numéros confirmés. "
    "unconfirmed_identifier_candidates contient des DIN, NPN ou DIN-HM utilisables seulement "
    "comme indices de recherche et qui peuvent être erronés. Si tu en cites un, indique toujours "
    "« à confirmer sur l'emballage ». Ne l'utilise jamais comme preuve pour attribuer au produit "
    "les faits d'un document Santé Canada; seule une association vérifiée peut relier "
    "automatiquement ce document au produit. "
    "Ne déclare jamais deux produits thérapeutiquement équivalents, interchangeables ou sûrs "
    "comme substituts. "
    "Chaque document précise source_class, trust_level et medical_claims_allowed. Pour une "
    "affirmation médicale, utilise en priorité official_regulator, pharmacist_approved ou "
    "professional_guideline. Une source avec medical_claims_allowed=false peut seulement aider "
    "à identifier un produit ou son emplacement; elle ne prouve jamais une indication, une dose, "
    "une interaction, une contre-indication ou un conseil de traitement. Lorsqu'une source "
    "officielle pertinente est fournie, toute conclusion médicale doit la citer. "
    "Pour une demande médicale, "
    "ne pose pas de diagnostic, ne remplace pas l'étiquette et oriente vers le pharmacien en "
    "cas de grossesse, bébé, interaction, allergie, symptômes graves ou persistants, difficulté "
    "respiratoire, ou incertitude clinique. Le serveur ajoute les avertissements, questions de "
    "suivi, comparaisons et emplacements: ne les génère pas. Retourne uniquement un objet JSON "
    "avec exactement ces clés: answer, key_points, selected_product_ids et source_ids. "
    "Chaque key_point contient exactement "
    "heading, detail et source_ids."
)

_CLIENT_DOCUMENTED_INSTRUCTIONS += (
    " Use your general knowledge to answer the customer's actual question and "
    "explain useful decision criteria, even when the catalogue documentation is "
    "incomplete. Always separate general knowledge from claims about an exact "
    "product. catalogue_description and catalogue_clues_to_confirm may be used "
    "to understand a product's probable role and avoid an empty answer, but if "
    "they are unverified, describe them as catalogue indications that must be "
    "confirmed on the package. Never turn a search clue, abbreviation, or "
    "unverified description into a certain dose, indication, contraindication, "
    "ingredient, or compatibility claim. Select at most 8 products."
)

_CLIENT_DOCUMENTED_INSTRUCTIONS += (
    " IMPORTANT: write selected_product_ids and source_ids before answer in the "
    "JSON. Never select or name a product merely to explain that it is excluded. "
    "Keep the direct answer concise enough for an employee to scan immediately."
)


_HEALTH_CANADA_DPD_API = "https://health-products.canada.ca/api/drug"
_HEALTH_CANADA_DPD_INFO = "https://health-products.canada.ca/dpd-bdpp/info"
_HEALTH_CANADA_LNHPD_API = "https://health-products.canada.ca/api/natural-licences"
_HEALTH_CANADA_LNHPD_INFO = "https://health-products.canada.ca/lnhpd-bdpsnh/"
_HEALTH_CANADA_CACHE = {}
_HEALTH_CANADA_CACHE_MAX = 24
_DOCUMENTATION_SEARCH_STOPWORDS = {
    "ca", "co", "caps", "gel", "liq", "mini", "mg", "ml", "un", "une",
    "de", "des", "du", "le", "la", "les", "et", "pour", "format", "produit",
}


def _health_canada_json(endpoint, **params):
    key = (endpoint, tuple(sorted((name, str(value)) for name, value in params.items())))
    if key in _HEALTH_CANADA_CACHE:
        return _HEALTH_CANADA_CACHE[key]
    url = f"{_HEALTH_CANADA_DPD_API}/{endpoint}/?{urlencode(params)}"
    data = _fetch_json(url, timeout=2.5)
    if data is not None:
        if endpoint == "drugproduct" and isinstance(data, list):
            data = data[:120]
        if len(_HEALTH_CANADA_CACHE) >= _HEALTH_CANADA_CACHE_MAX:
            _HEALTH_CANADA_CACHE.pop(next(iter(_HEALTH_CANADA_CACHE)), None)
        _HEALTH_CANADA_CACHE[key] = data
    return data


def _health_canada_nhp_json(endpoint, **params):
    key = ("nhp", endpoint, tuple(sorted((str(k), str(v)) for k, v in params.items())))
    if key in _HEALTH_CANADA_CACHE:
        return _HEALTH_CANADA_CACHE[key]
    url = f"{_HEALTH_CANADA_LNHPD_API}/{endpoint}/?{urlencode(params)}"
    data = _fetch_json(url, timeout=3.5)
    if data is not None:
        if len(_HEALTH_CANADA_CACHE) >= _HEALTH_CANADA_CACHE_MAX:
            _HEALTH_CANADA_CACHE.pop(next(iter(_HEALTH_CANADA_CACHE)), None)
        _HEALTH_CANADA_CACHE[key] = data
    return data


def _json_records(value):
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    if isinstance(value, dict):
        nested = value.get("data")
        if isinstance(nested, list):
            return [item for item in nested if isinstance(item, dict)]
        if isinstance(nested, dict):
            return [nested]
        return [value]
    return []


def _documentation_search_term(product):
    from routes.products import normalize_search_text

    for raw in (product.get("brand", ""), product.get("name", "")):
        tokens = [
            token for token in normalize_search_text(raw).split()
            if len(token) >= 3 and not token.isdigit()
            and token not in _DOCUMENTATION_SEARCH_STOPWORDS
        ]
        if tokens:
            return " ".join(tokens[:2])
    return ""


def _dpd_match_score(product, record):
    from routes.products import normalize_search_text

    product_text = normalize_search_text(" ".join([
        str(product.get("name", "") or ""), str(product.get("brand", "") or ""),
    ]))
    record_text = normalize_search_text(" ".join([
        str(record.get("brand_name", "") or ""), str(record.get("descriptor", "") or ""),
    ]))
    product_tokens = set(product_text.split())
    record_tokens = set(record_text.split())
    shared = {
        token for token in product_tokens & record_tokens
        if len(token) >= 3 and token not in _DOCUMENTATION_SEARCH_STOPWORDS
    }
    score = len(shared) * 3
    if record_text == product_text:
        score += 8
    elif len(product_tokens) >= 2 and record_text and (
        record_text in product_text or product_text in record_text
    ):
        score += 8
    equivalent_markers = (
        ("enf", "enfant", "children", "junior", "pediatric"),
        ("rhume", "cold", "grippe", "flu"),
        ("nuit", "night"),
        ("sinus",),
        ("liq", "liqui", "capsule", "capsules"),
        ("mini",),
        ("sir", "sirop", "syrup", "suspension", "liquide", "liquid", "drops", "gouttes"),
        ("co", "comprime", "comprimes", "tablet", "tablets", "caplet", "caplets"),
        ("creme", "cream", "onguent", "ointment"),
        ("extra", "fort", "strength", "xf"),
    )
    padded_product = f" {product_text} "
    padded_record = f" {record_text} "
    for markers in equivalent_markers:
        product_has_marker = any(f" {marker} " in padded_product for marker in markers)
        record_has_marker = any(f" {marker} " in padded_record for marker in markers)
        if product_has_marker and record_has_marker:
            score += 4
    product_numbers = set(re.findall(r"\d+", product_text))
    record_numbers = set(re.findall(r"\d+", record_text))
    score += len(product_numbers & record_numbers) * 2
    return score


def _health_canada_document(match):
    product, record = match
    code = str(record.get("drug_code", "") or "").strip()
    if not code:
        return None
    ingredients = _json_records(_health_canada_json(
        "activeingredient", id=code, lang="fr", type="json"
    ))
    forms = _json_records(_health_canada_json("form", id=code, lang="fr", type="json"))
    routes = _json_records(_health_canada_json("route", id=code, lang="fr", type="json"))
    schedules = _json_records(_health_canada_json(
        "schedule", id=code, lang="fr", type="json"
    ))

    ingredient_text = []
    for item in ingredients[:5]:
        name = str(item.get("ingredient_name", "") or "").strip()
        strength = " ".join(str(item.get(field, "") or "").strip() for field in (
            "strength", "strength_unit",
        )).strip()
        if name:
            ingredient_text.append(f"{name} {strength}".strip())
    form_text = [
        str(item.get("pharmaceutical_form_name", "") or "").strip()
        for item in forms[:4]
        if str(item.get("pharmaceutical_form_name", "") or "").strip()
    ]
    route_text = [
        str(item.get("route_of_administration_name", "") or "").strip()
        for item in routes[:4]
        if str(item.get("route_of_administration_name", "") or "").strip()
    ]
    schedule_text = [
        str(item.get("schedule_name", "") or "").strip()
        for item in schedules[:4]
        if str(item.get("schedule_name", "") or "").strip()
    ]
    facts = [
        f"Nom autorisé: {str(record.get('brand_name', '') or '').strip()}",
        f"DIN: {str(record.get('drug_identification_number', '') or '').strip()}",
        "Statut: commercialisé au Canada",
    ]
    descriptor = str(record.get("descriptor", "") or "").strip()
    if descriptor:
        facts.append(f"Description réglementaire: {descriptor}")
    if ingredient_text:
        facts.append(f"Ingrédient(s) actif(s): {', '.join(ingredient_text)}")
    if form_text:
        facts.append(f"Forme(s): {', '.join(form_text)}")
    if route_text:
        facts.append(f"Voie(s): {', '.join(route_text)}")
    if schedule_text:
        facts.append(f"Annexe(s): {', '.join(schedule_text)}")
    return {
        "source_id": f"health-canada:{code}",
        "title": f"Santé Canada - {str(record.get('brand_name', '') or '').strip()}",
        "publisher": "Santé Canada",
        "url": f"{_HEALTH_CANADA_DPD_INFO}?{urlencode({'code': code, 'lang': 'fre'})}",
        "evidence": ". ".join(fact for fact in facts if not fact.endswith(": "))[:1800],
        "candidate_ids": [str(product.get("client_id", "") or "")],
    }


def health_canada_documents(products, limit=4):
    """Return official facts only for a verified exact DIN.

    Brand-name search is deliberately not used here: one brand can contain
    several regulated products, and a DIN still does not identify package size.
    """
    products_by_din = {}
    for product in products:
        for identifier in product.get("_identifiers") or []:
            if (
                identifier.get("type") != "DIN"
                or identifier.get("verification_status") != "verified"
            ):
                continue
            din = re.sub(r"\D", "", str(identifier.get("value", "") or ""))
            if len(din) == 8:
                products_by_din.setdefault(din, []).append(product)
    if not products_by_din:
        return []

    search_futures = {
        _LOOKUP_SOURCE_EXECUTOR.submit(
            _health_canada_json, "drugproduct",
            din=din, lang="fr", type="json",
        ): din
        for din in list(products_by_din)[:limit]
    }
    matches = []
    try:
        for future in as_completed(search_futures, timeout=6):
            requested_din = search_futures[future]
            try:
                result = future.result()
            except Exception:
                result = None
            for record in _json_records(result):
                returned_din = re.sub(
                    r"\D", "",
                    str(record.get("drug_identification_number", "") or ""),
                )
                if returned_din == requested_din:
                    matches.append((products_by_din[requested_din], record))
                    break
    except TimeoutError:
        pass
    for future in search_futures:
        future.cancel()
    if not matches:
        return []

    futures = {
        _LOOKUP_SOURCE_EXECUTOR.submit(
            _health_canada_document, (matched_products[0], record)
        ): matched_products
        for matched_products, record in matches
    }
    documents = []
    try:
        for future in as_completed(futures, timeout=9):
            try:
                document = future.result()
            except Exception:
                document = None
            if document:
                document["candidate_ids"] = [
                    str(product.get("client_id", "") or "")
                    for product in futures[future]
                    if product.get("client_id")
                ]
                documents.append(document)
    except TimeoutError:
        pass
    for future in futures:
        future.cancel()
    documents.sort(key=lambda item: item.get("title", ""))
    return documents


def _health_canada_nhp_document(product, identifier_type, licence, record):
    lnhpd_id = str(record.get("lnhpd_id", "") or "").strip()
    if not lnhpd_id:
        return None
    ingredients = _json_records(_health_canada_nhp_json(
        "medicinalingredient", id=lnhpd_id, lang="fr", type="json"
    ))
    purposes = _json_records(_health_canada_nhp_json(
        "productpurpose", id=lnhpd_id, lang="fr", type="json"
    ))
    routes = _json_records(_health_canada_nhp_json(
        "productroute", id=lnhpd_id, lang="fr", type="json"
    ))
    ingredient_text = []
    for item in ingredients[:8]:
        name = str(item.get("ingredient_name", "") or "").strip()
        amount = str(
            item.get("quantity", "") or item.get("potency_amount", "") or ""
        ).strip()
        unit = str(
            item.get("quantity_unit_of_measure", "")
            or item.get("potency_unit_of_measure", "") or ""
        ).strip()
        if name:
            ingredient_text.append(" ".join(
                part for part in (name, amount, unit) if part
            ))
    purpose_text = [
        str(item.get("purpose", "") or "").strip()
        for item in purposes[:3]
        if str(item.get("purpose", "") or "").strip()
    ]
    route_text = [
        str(item.get("route_type_desc", "") or "").strip()
        for item in routes[:4]
        if str(item.get("route_type_desc", "") or "").strip()
    ]
    facts = [
        f"Nom autorisé: {str(record.get('product_name', '') or '').strip()}",
        f"{identifier_type.replace('_', '-')}: {licence}",
    ]
    dosage_form = str(record.get("dosage_form", "") or "").strip()
    company = str(record.get("company_name", "") or "").strip()
    if company:
        facts.append(f"Titulaire: {company}")
    if dosage_form:
        facts.append(f"Forme: {dosage_form}")
    if ingredient_text:
        facts.append(f"Ingrédient(s) médicinal(aux): {', '.join(ingredient_text)}")
    if purpose_text:
        facts.append(f"Usage(s) homologué(s): {' '.join(purpose_text)}")
    if route_text:
        facts.append(f"Voie(s): {', '.join(route_text)}")
    return {
        "source_id": f"health-canada-nhp:{lnhpd_id}",
        "title": f"Santé Canada - {str(record.get('product_name', '') or '').strip()}",
        "publisher": "Santé Canada",
        "url": _HEALTH_CANADA_LNHPD_INFO,
        "evidence": ". ".join(fact for fact in facts if not fact.endswith(": "))[:2200],
        "candidate_ids": [str(product.get("client_id", "") or "")],
    }


def health_canada_nhp_documents(products, limit=4):
    verified = []
    for product in products:
        for identifier in product.get("_identifiers") or []:
            identifier_type = str(identifier.get("type", "") or "")
            if (
                identifier_type not in {"NPN", "DIN_HM"}
                or identifier.get("verification_status") != "verified"
            ):
                continue
            licence = re.sub(r"\D", "", str(identifier.get("value", "") or ""))
            if len(licence) == 8:
                verified.append((product, identifier_type, licence))
    documents = []
    for product, identifier_type, licence in verified[:limit]:
        records = _json_records(_health_canada_nhp_json(
            "productlicence", id=licence, lang="fr", type="json"
        ))
        record = next((
            item for item in records
            if re.sub(r"\D", "", str(item.get("licence_number", "") or "")) == licence
        ), None)
        if not record:
            continue
        document = _health_canada_nhp_document(
            product, identifier_type, licence, record
        )
        if document:
            documents.append(document)
    return documents


def _client_intent_documents(query_plan):
    from routes.products import normalize_search_text

    normalized_question = normalize_search_text(
        (query_plan or {}).get("corrected_query", "")
    )
    if "melaton" in normalized_question:
        monograph_url = (
            "https://webprod.hc-sc.gc.ca/nhpid-bdipsn/atReq"
            "?atid=melatonin.oral2&lang=eng&wbdisable=true"
        )
        return [{
            "source_id": "health-canada:melatonin-uses",
            "title": "Santé Canada - Monographie de la mélatonine orale",
            "publisher": "Santé Canada",
            "url": monograph_url,
            "evidence": (
                "La monographie vise les adultes de 18 ans et plus et l'usage au besoin pour "
                "l'insomnie occasionnelle. Les usages autorisables comprennent l'aide au sommeil, "
                "la réduction du temps d'endormissement, l'augmentation du temps total de sommeil "
                "lors d'un horaire perturbé, le décalage horaire et le réajustement du cycle "
                "veille-sommeil. La forme et la concentration exactes doivent être confirmées sur "
                "l'étiquette du produit homologué."
            ),
            "candidate_ids": [],
        }, {
            "source_id": "health-canada:melatonin-safety",
            "title": "Santé Canada - Précautions pour la mélatonine",
            "publisher": "Santé Canada",
            "url": monograph_url,
            "evidence": (
                "La monographie demande d'éviter l'alcool et les produits qui causent de la "
                "somnolence, et de ne pas conduire ni utiliser de machinerie pendant 5 heures. "
                "Elle recommande de consulter pour plusieurs médicaments ou maladies, si "
                "l'insomnie persiste plus de 4 semaines, et contre-indique l'usage pendant la "
                "grossesse ou l'allaitement."
            ),
            "candidate_ids": [],
        }, {
            "source_id": "health-canada:melatonin-pediatric",
            "title": "Santé Canada - Mélatonine et personnes de moins de 18 ans",
            "publisher": "Santé Canada",
            "url": (
                "https://www.canada.ca/en/health-canada/services/drugs-health-products/"
                "drug-products/prescription-drug-list/notices-changes/"
                "qualifier-pediatric-melatonin-intent.html"
            ),
            "evidence": (
                "Depuis le 2 juin 2026, la mélatonine vendue pour un usage lié au sommeil chez "
                "les enfants et adolescents de moins de 18 ans relève des médicaments sur "
                "ordonnance. Santé Canada indique qu'une supervision professionnelle est "
                "nécessaire pour rechercher la cause du trouble, essayer l'hygiène du sommeil "
                "et adapter la dose à la personne."
            ),
            "candidate_ids": [],
        }]
    is_toothpaste_comparison = bool(
        (query_plan or {}).get("needs_comparison")
        and any(term in normalized_question for term in (
            "dentifrice", "toothpaste",
        ))
        and "sens" in normalized_question
        and any(term in normalized_question for term in (
            "blanch", "whiten",
        ))
    )
    if is_toothpaste_comparison:
        return [{
            "source_id": "cda:sensitive-toothpaste",
            "title": "ADC - Dentifrice pour dents sensibles",
            "publisher": "Association dentaire canadienne",
            "url": (
                "https://www.cda-adc.ca/EN/oral_health/seal/products/"
                "product_page.asp?product=235"
            ),
            "evidence": (
                "Un dentifrice pour dents sensibles vise le soulagement et la protection "
                "contre la sensibilité grâce à des ingrédients actifs précis. L'Association "
                "dentaire canadienne indique de l'utiliser régulièrement selon l'étiquette "
                "et de consulter un dentiste si la sensibilité ou la douleur persiste."
            ),
            "candidate_ids": [],
        }, {
            "source_id": "cda:whitening-toothpaste",
            "title": "ADC - Dentifrices blanchissants",
            "publisher": "Association dentaire canadienne",
            "url": (
                "https://www.cda-adc.ca/en/oral_health/faqs/"
                "dental_care_faqs.asp"
            ),
            "evidence": (
                "Les dentifrices blanchissants abrasifs agissent surtout sur les taches "
                "de surface et ne sont pas équivalents à un traitement de blanchiment. "
                "Certains contiennent aussi un agent blanchissant; un usage prolongé peut "
                "accentuer la sensibilité ou l'abrasion."
            ),
            "candidate_ids": [],
        }]
    is_wound_dressing_comparison = bool(
        (query_plan or {}).get("needs_comparison")
        and any(term in normalized_question for term in (
            "pansement", "pansements", "bandage", "dressing",
        ))
        and "hydrocollo" in normalized_question
        and "transp" in normalized_question
    )
    if is_wound_dressing_comparison:
        guideline_url = (
            "https://www.elft.nhs.uk/sites/default/files/2023-08/"
            "Wound%20Management%20Guidelines%20Vs%207.1%20March%202023.pdf"
        )
        return [{
            "source_id": "nhs:wound-hydrocolloid",
            "title": "NHS - Pansements hydrocolloïdes",
            "publisher": "East London NHS Foundation Trust",
            "url": guideline_url,
            "evidence": (
                "Les hydrocolloïdes sont des pansements occlusifs. Au contact de l'exsudat, "
                "leur matrice forme un gel humide qui maintient l'humidité et favorise la "
                "granulation. Le guide précise qu'ils ne conviennent pas aux plaies fortement "
                "exsudatives."
            ),
            "candidate_ids": [],
        }, {
            "source_id": "nhs:wound-transparent-film",
            "title": "NHS - Films adhésifs transparents semi-perméables",
            "publisher": "East London NHS Foundation Trust",
            "url": guideline_url,
            "evidence": (
                "Les films adhésifs semi-perméables sont de minces feuilles transparentes de "
                "polyuréthane. Ils permettent d'observer la plaie, retiennent l'humidité et "
                "forment une barrière contre l'eau et les bactéries; ils conviennent surtout "
                "aux plaies non ou peu exsudatives."
            ),
            "candidate_ids": [],
        }]
    if str((query_plan or {}).get("intent", "") or "") not in {
        "headache_relief", "fever_relief",
    }:
        return []
    return [{
        "source_id": "health-canada:acetaminophen-safe-use",
        "title": "Santé Canada - Utilisation sécuritaire de l'acétaminophène",
        "publisher": "Santé Canada",
        "url": "https://www.canada.ca/fr/sante-canada/services/medicaments-et-appareils-medicaux/acetaminophene.html",
        "evidence": (
            "Santé Canada indique que l'acétaminophène procure un soulagement temporaire de "
            "douleurs comme le mal de tête et réduit la fièvre. Il faut lire l'étiquette, "
            "respecter la dose indiquée et éviter de prendre simultanément plus d'un produit "
            "qui en contient, car un excès peut causer des dommages graves au foie."
        ),
        "candidate_ids": [],
    }, {
        "source_id": "health-canada:nsaid-guidance",
        "title": "Santé Canada - Information sur les anti-inflammatoires non stéroïdiens",
        "publisher": "Santé Canada",
        "url": (
            "https://www.canada.ca/en/health-canada/services/drugs-health-products/"
            "drug-products/applications-submissions/guidance-documents/"
            "nonsteroidal-anti-inflammatory-drugs-nsaids/"
            "guidance-document-basic-product-monograph-information-"
            "nonsteroidal-anti-inflammatory-drugs-nsaids.html"
        ),
        "evidence": (
            "Les anti-inflammatoires non stéroïdiens (AINS) réduisent des substances qui "
            "causent la douleur et l'enflure. Santé Canada déconseille d'utiliser en même "
            "temps plusieurs AINS, notamment l'acide acétylsalicylique (AAS) et l'ibuprofène, "
            "en raison du risque d'effets indésirables additifs."
        ),
        "candidate_ids": [],
    }, {
        "source_id": "quebec:info-sante-811",
        "title": "Gouvernement du Québec - Conseils pour un problème de santé non urgent",
        "publisher": "Gouvernement du Québec",
        "url": (
            "https://www.quebec.ca/sante/systeme-et-services-de-sante/"
            "organisation-des-services/services-de-sante-et-services-sociaux-de-premiere-ligne/"
            "comment-obtenir-conseils-concernant-probleme-sante-non-urgent"
        ),
        "evidence": (
            "Pour un problème de santé non urgent ou un doute sur la nécessité de consulter, "
            "Info-Santé 811 permet d'obtenir les conseils d'une infirmière."
        ),
        "candidate_ids": [],
    }]


_AI_DESCRIPTION_FACT_RE = re.compile(
    r"(?i)(?:"
    r"\b\d+(?:[.,]\d+)?\s*(?:mcg|mg|g|kg|ml|l|%|unit(?:e|é)s?|"
    r"capsules?|comprim(?:e|é)s?|tablets?)\b|"
    r"\b(?:contient|renferme|ingredient|ingrédient|formule|formulé|"
    r"soulage|reduit|réduit|indique|indiqué|destine|destiné|concu|conçu|"
    r"compatible|convient|utilis|appliquer|nettoie|hydrate|protege|protège|"
    r"saveur|parfum|concentration|dosage|format|bouteille|recharge|"
    r"capsule|comprime|comprimé|liqui[\s-]?gel|mini[\s-]?gel)\b"
    r")"
)
_AI_DESCRIPTION_MARKETING_RE = re.compile(
    r"(?i)\b(?:"
    r"il y a des|au quotidien|dans ces moments|dans ces instants|"
    r"bien-etre merite|bien-être mérite|vie active|rythme de vie|"
    r"presence totale|présence totale|a ete imagine|a été imaginé|"
    r"une facon moderne|une façon moderne|symbole de|chaque seconde compte"
    r")\b"
)


def _description_excerpt_for_ai(value, max_chars=900):
    """Extract exact, useful catalogue facts instead of a marketing preamble."""
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    max_chars = max(120, int(max_chars or 900))
    if len(text) <= max_chars and not _AI_DESCRIPTION_MARKETING_RE.search(text):
        return text
    sentences = [
        sentence.strip(" -;\t")
        for sentence in re.split(
            r"(?<=[.!?])\s+|\s+[•·]\s+|\s+-\s+(?=[A-ZÀ-ÖØ-Ý0-9])",
            text,
        )
        if sentence.strip(" -;\t")
    ]
    if len(sentences) <= 1:
        return text[:max_chars].rsplit(" ", 1)[0].rstrip(" ,;:-")

    scored = []
    for index, sentence in enumerate(sentences):
        score = 0
        fact_hits = len(_AI_DESCRIPTION_FACT_RE.findall(sentence))
        score += min(8, fact_hits * 2)
        if re.search(r"\d", sentence):
            score += 2
        if 35 <= len(sentence) <= 360:
            score += 1
        if _AI_DESCRIPTION_MARKETING_RE.search(sentence):
            score -= 5
        scored.append((score, index, sentence))

    selected = []
    used_chars = 0
    for score, index, sentence in sorted(
        scored, key=lambda item: (-item[0], item[1])
    ):
        if score <= 0 and selected:
            break
        needed = len(sentence) + (1 if selected else 0)
        if used_chars + needed > max_chars:
            continue
        selected.append((index, sentence))
        used_chars += needed
        if len(selected) >= 6:
            break
    if not selected:
        return text[:max_chars].rsplit(" ", 1)[0].rstrip(" ,;:-")
    excerpt = " ".join(sentence for _index, sentence in sorted(selected))
    return excerpt.rstrip(" ,;:-")


def retrieve_client_documentation(products, query_plan=None, include_live_regulatory=True):
    product_names = [str(product.get("name", "") or "").strip() for product in products]
    documents = [{
        "source_id": "store-plan",
        "title": "Plan actuel du magasin",
        "publisher": "Familiprix Locator",
        "url": "",
        "evidence": (
            "Produits réellement placés dans le plan actuel: "
            + "; ".join(name for name in product_names if name)
        )[:2200],
        "candidate_ids": [
            str(product.get("client_id", "") or "") for product in products
            if product.get("client_id")
        ],
        "verification_status": "verified_inventory",
        "source_class": "store_inventory",
        "trust_level": 60,
        "medical_claims_allowed": False,
    }]
    documents.extend(_client_intent_documents(query_plan))
    if include_live_regulatory:
        try:
            documents.extend(health_canada_documents(products))
        except Exception:
            pass
        try:
            documents.extend(health_canada_nhp_documents(products))
        except Exception:
            pass

    field_labels = {
        "brand": "Marque", "description": "Description",
        "package_size": "Format", "package_unit": "Unité",
        "variant": "Variante", "flavour": "Saveur", "colour": "Couleur",
        "strength": "Concentration", "dosage_form": "Forme",
        "manufacturer": "Fabricant", "ingredients": "Ingrédients",
        "compatibility": "Compatibilité", "category": "Catégorie",
        "purpose": "Usage autorisé",
        "route_of_administration": "Voie d'administration",
    }
    source_index = 0
    for product in products[:10]:
        verified_fields = set(product.get("_verified_fields") or [])
        field_sources = product.get("_field_sources") or {}
        grouped = {}
        for field, label in field_labels.items():
            if field not in verified_fields:
                continue
            value = str(product.get(field, "") or "").strip()
            if not value:
                continue
            if field == "description":
                value = _description_excerpt_for_ai(
                    value, max_chars=1300,
                )
            provenance = field_sources.get(field) or {}
            source = str(provenance.get("source", "") or "Source vérifiée")
            source_url = str(provenance.get("source_url", "") or "")
            grouped.setdefault((source, source_url), []).append(f"{label}: {value}")
        name = str(product.get("name", "") or "").strip()
        candidate_id = str(product.get("client_id", "") or "")
        description = str(product.get("description", "") or "").strip()
        if description and "description" not in verified_fields:
            source_index += 1
            provenance = field_sources.get("description") or {}
            source = str(
                provenance.get("source", "")
                or product.get("source", "")
                or "Catalogue interne"
            ).strip()
            source_url = str(
                provenance.get("source_url", "")
                or product.get("source_url", "")
                or ""
            ).strip()
            status = str(
                product.get("description_status", "") or "unverified"
            ).strip()
            description_excerpt = _description_excerpt_for_ai(
                description, max_chars=1300,
            )
            documents.append({
                "source_id": f"catalog-description:{source_index}",
                "title": f"Description catalogue \u00e0 confirmer - {name}",
                "publisher": source,
                "url": source_url if source_url.startswith("https://") else "",
                "evidence": (
                    f"Statut: {status}; cette description aide \u00e0 identifier le "
                    f"produit mais doit \u00eatre confirm\u00e9e sur l'emballage. "
                    f"Description: {description_excerpt}"
                )[:1800],
                "candidate_ids": [candidate_id] if candidate_id else [],
                "verification_status": status,
                "source_class": "unverified_catalogue",
                "trust_level": 30,
                "medical_claims_allowed": False,
            })
        for (source, source_url), facts in grouped.items():
            source_index += 1
            source_text = normalize_text(f"{source} {source_url}")
            pharmacist_approved = "pharmacist" in source_text or "pharmacien" in source_text
            source_type, source_priority = classify_source(source, source_url)
            documents.append({
                "source_id": f"catalog:{source_index}",
                "title": f"Fiche vérifiée - {name}",
                "publisher": source,
                "url": source_url if source_url.startswith("https://") else "",
                "evidence": ". ".join(facts)[:1800],
                "candidate_ids": [candidate_id] if candidate_id else [],
                "verification_status": "verified",
                "source_class": (
                    "pharmacist_approved" if pharmacist_approved
                    else "official_regulator" if source_type == "health_canada"
                    else "verified_product_record"
                ),
                "trust_level": (
                    95 if pharmacist_approved
                    else 100 if source_type == "health_canada"
                    else min(90, max(70, int(source_priority or 0)))
                ),
                "medical_claims_allowed": bool(
                    pharmacist_approved
                    or source_type == "health_canada"
                ),
            })
    for document in documents:
        publisher_text = normalize_text(" ".join((
            str(document.get("publisher", "") or ""),
            str(document.get("source_id", "") or ""),
        )))
        if "sante canada" in publisher_text or "health canada" in publisher_text:
            document.update(
                source_class="official_regulator",
                trust_level=100,
                medical_claims_allowed=True,
                verification_status=document.get(
                    "verification_status", "verified"
                ),
            )
        elif any(name in publisher_text for name in (
            "gouvernement du quebec", "association dentaire canadienne",
            "nhs foundation trust",
        )):
            document.update(
                source_class="professional_guideline",
                trust_level=90,
                medical_claims_allowed=True,
                verification_status=document.get(
                    "verification_status", "verified"
                ),
            )
        else:
            document.setdefault("source_class", "catalogue_context")
            document.setdefault("trust_level", 50)
            document.setdefault("medical_claims_allowed", False)
            document.setdefault("verification_status", "unverified")
    return documents[:15]


def _normalize_source_ids(values, valid_source_ids, limit=4):
    selected = []
    for raw in values if isinstance(values, list) else []:
        source_id = str(raw or "").strip()
        if source_id in valid_source_ids and source_id not in selected:
            selected.append(source_id)
        if len(selected) >= limit:
            break
    return selected


def normalize_documented_client_answer(parsed, valid_ids, documents):
    parsed = parsed if isinstance(parsed, dict) else {}
    base = normalize_verified_client_answer(parsed, valid_ids)
    valid_ids = set(valid_ids)
    valid_source_ids = {
        str(document.get("source_id", "") or "") for document in documents
    }

    key_points = []
    for item in parsed.get("key_points", []) if isinstance(parsed.get("key_points"), list) else []:
        if not isinstance(item, dict):
            continue
        heading = str(item.get("heading", "") or "").strip()[:100]
        detail = str(item.get("detail", "") or "").strip()[:1000]
        if heading and detail:
            key_points.append({
                "heading": heading,
                "detail": detail,
                "source_ids": _normalize_source_ids(item.get("source_ids"), valid_source_ids),
            })
        if len(key_points) >= 6:
            break

    comparisons = []
    for item in parsed.get("comparisons", []) if isinstance(parsed.get("comparisons"), list) else []:
        if not isinstance(item, dict):
            continue
        candidate_id = str(item.get("candidate_id", "") or "").strip()
        difference = str(item.get("difference", "") or "").strip()[:800]
        practical_note = str(item.get("practical_note", "") or "").strip()[:800]
        if candidate_id in valid_ids and (difference or practical_note):
            comparisons.append({
                "candidate_id": candidate_id,
                "difference": difference,
                "practical_note": practical_note,
                "source_ids": _normalize_source_ids(item.get("source_ids"), valid_source_ids),
            })
        if len(comparisons) >= 8:
            break

    def normalize_text_items(key):
        items = []
        raw_items = parsed.get(key, []) if isinstance(parsed.get(key), list) else []
        for item in raw_items:
            if not isinstance(item, dict):
                continue
            value = str(item.get("text", "") or "").strip()[:900]
            if value:
                items.append({
                    "text": value,
                    "source_ids": _normalize_source_ids(item.get("source_ids"), valid_source_ids),
                })
            if len(items) >= 6:
                break
        return items

    useful_guidance = normalize_text_items("useful_guidance")
    important_checks = normalize_text_items("important_checks")
    cited_source_ids = _normalize_source_ids(
        parsed.get("source_ids"), valid_source_ids, limit=16
    )
    for item in key_points + comparisons + useful_guidance + important_checks:
        for source_id in item.get("source_ids", []):
            if source_id not in cited_source_ids:
                cited_source_ids.append(source_id)
    return {
        **base,
        "key_points": key_points,
        "comparisons": comparisons,
        "useful_guidance": useful_guidance,
        "important_checks": important_checks,
        "source_ids": cited_source_ids[:16],
    }


def grounded_documented_fallback(query_plan, candidates, documents, degraded=True):
    """Return a useful, source-backed response when every AI attempt fails."""
    from routes.products import normalize_search_text

    selected = [
        product for product in candidates
        if str(product.get("client_id", "") or "").strip()
    ][:16]
    selected_ids = [str(product.get("client_id", "") or "") for product in selected]
    names = [str(product.get("name", "") or "").strip() for product in selected]
    normalized_question = normalize_search_text(
        str(query_plan.get("corrected_query", "") or "")
    )
    question_words = set(normalized_question.split())
    request_intent = str(query_plan.get("intent", "") or "")
    is_headache_query = request_intent == "headache_relief"
    is_fever_query = request_intent == "fever_relief"
    is_toothbrush_query = bool(
        question_words.intersection({"brosse", "brosses", "brush", "toothbrush"})
        and (
            question_words.intersection({"dent", "dents", "tooth", "teeth"})
            or "toothbrush" in question_words
        )
    )
    asks_flavors = bool(
        question_words.intersection({"saveur", "saveurs", "gout", "gouts", "flavor", "flavors"})
    )
    is_melatonin_query = "melaton" in normalized_question
    is_wound_dressing_comparison = bool(
        query_plan.get("needs_comparison")
        and question_words.intersection({
            "pansement", "pansements", "bandage", "bandages", "dressing",
        })
        and any(word.startswith("hydrocollo") for word in question_words)
        and any(word.startswith("transp") for word in question_words)
    )
    is_toothpaste_comparison = bool(
        query_plan.get("needs_comparison")
        and question_words.intersection({
            "dentifrice", "dentifrices", "toothpaste", "toothpastes",
        })
        and any(word.startswith("sens") for word in question_words)
        and any(
            word.startswith("blanch") or word.startswith("whit")
            for word in question_words
        )
    )

    evidence_stopwords = {
        "avoir", "avec", "comment", "contexte", "dans", "difference",
        "differentes", "differents", "dire", "entre", "est", "faire",
        "lequel", "laquelle", "magasin", "meilleur", "prendre", "produit",
        "produits", "quel", "quelle", "quelles", "quels", "tout", "tous",
        "toute", "toutes", "utiliser",
    }
    evidence_query_words = {
        word for word in question_words
        if len(word) >= 4 and word not in evidence_stopwords
    }

    def product_evidence_excerpt(product, limit=240):
        raw = " ".join(str(product.get(field, "") or "").strip() for field in (
            "description", "usage_notes", "purpose", "compatibility",
        )).strip()
        raw = re.sub(r"\s+", " ", raw)
        if not raw:
            return ""
        segments = [
            segment.strip(" -•")
            for segment in re.split(r"(?<=[.!?])\s+|[;\r\n•]+", raw)
            if segment.strip(" -•")
        ]
        if not segments:
            return raw[:limit].rstrip()

        def evidence_score(segment):
            normalized = normalize_search_text(segment)
            return (
                sum(4 for word in evidence_query_words if word in normalized)
                + sum(1 for marker in (
                    "absor", "compatible", "convient", "ideal", "indique",
                    "protege", "soulage", "utilisation", "usage",
                ) if marker in normalized)
            )

        excerpt = max(segments, key=evidence_score)
        if len(excerpt) > limit:
            excerpt = excerpt[:limit].rsplit(" ", 1)[0].rstrip(" ,;:")
        return excerpt.rstrip(" .")

    def detected_product_form(name, details):
        padded_name = f" {name} "
        tokens = set(name.split())

        def coded(*prefixes):
            return any(
                any(re.fullmatch(rf"{re.escape(prefix)}\d+[a-z]*", token) for prefix in prefixes)
                for token in tokens
            )

        if ("mini" in tokens and "gel" in tokens) or "mini gel" in name:
            return "mini-gels"
        if (
            "liqui gel" in details
            or "liq gel" in details
            or ("liq" in tokens and "gel" in tokens)
        ):
            return "liqui-gels"
        if tokens.intersection({
            "susp", "suspension", "sir", "sirop", "liquide", "liquid",
        }) or coded("liq", "sir") or "solution orale" in details:
            return "liquide ou suspension"
        if tokens.intersection({"gum", "gomme", "gommes", "gummies"}) or coded("gum"):
            return "gommes"
        if tokens.intersection({"vapo", "spray", "vaporisateur", "pompe"}) or coded("vapo"):
            return "vaporisateur"
        if (
            tokens.intersection({"caps", "capsule", "capsules"})
            or re.search(r"\bca\d+\b", padded_name)
        ):
            return "capsules"
        if (
            tokens.intersection({
                "co", "comprime", "comprimes", "tablet", "tablets",
                "caplet", "caplets",
            })
            or re.search(r"\bco\d+\b", padded_name)
        ):
            return "comprimés ou caplets"
        if tokens.intersection({"creme", "cream", "onguent", "ointment"}):
            return "crème ou onguent"
        if "gel" in tokens:
            return "gel topique"
        return ""

    form_guidance = {
        "liquide ou suspension": (
            "se mesure avec le dispositif fourni et peut convenir lorsqu'une forme solide "
            "est difficile à avaler; confirmer l'âge et la dose sur l'étiquette"
        ),
        "comprimés ou caplets": (
            "forme solide à avaler; comparer surtout l'ingrédient, la concentration et le "
            "nombre d'unités"
        ),
        "capsules": (
            "forme solide à avaler entière; ne pas supposer qu'elle agit plus vite sans "
            "indication confirmée"
        ),
        "liqui-gels": (
            "capsules contenant une préparation liquide, à avaler entières; le nom seul ne "
            "prouve pas une action plus rapide"
        ),
        "mini-gels": (
            "le nom indique un format de capsule plus petit; confirmer la taille et la "
            "concentration sur l'emballage"
        ),
        "gommes": (
            "forme à mâcher; la saveur et la facilité de prise changent, mais l'usage, "
            "l'âge et la concentration doivent être vérifiés"
        ),
        "vaporisateur": (
            "forme à pulvériser; confirmer la quantité par pulvérisation et le mode "
            "d'emploi sur l'étiquette"
        ),
        "crème ou onguent": (
            "forme appliquée sur la peau; l'usage et la zone d'application doivent être "
            "confirmés sur l'étiquette"
        ),
        "gel topique": (
            "forme appliquée sur la peau; ne pas la confondre avec une capsule de type "
            "liqui-gel"
        ),
    }
    flavor_markers = (
        ("fraise", ("fraise", "strawberry")),
        ("cerise", ("cerise", "cherry")),
        ("raisin", ("raisin", "grape")),
        ("fruits", ("saveur fruit", "fruit flavor", "fruit flavour")),
        ("baies", ("baies", "berry")),
        ("orange", ("orange",)),
        ("menthe", ("menthe", "mint")),
        ("citron", ("citron", "lemon")),
    )
    forms = []
    form_candidate_ids = defaultdict(list)
    rapid_relief_form_candidate_ids = defaultdict(list)
    doses = []
    flavors = []
    features = []
    ingredient_families = []
    product_traits = {}
    toothbrush_groups = defaultdict(int)
    wound_dressing_groups = defaultdict(list)
    ingredient_markers = (
        ("acétaminophène", ("acetaminophene", "paracetamol")),
        ("ibuprofène", ("ibuprofene",)),
        ("naproxène", ("naproxene",)),
        (
            "acide acétylsalicylique",
            ("acide acetylsalicylique", "acetylsalicylique", "aspirine"),
        ),
    )
    for product in selected:
        candidate_id = str(product.get("client_id", "") or "")
        name = str(product.get("name", "") or "").strip()
        description = str(product.get("description", "") or "").strip()
        usage_notes = str(product.get("usage_notes", "") or "").strip()
        normalized_name = normalize_search_text(name)
        normalized_details = normalize_search_text(f"{name} {description} {usage_notes}")
        traits = []
        if is_wound_dressing_comparison:
            if "hydrocollo" in normalized_details:
                dressing_type = "pansement hydrocolloïde"
            elif (
                "transp" in normalized_details
                and any(marker in normalized_details for marker in (
                    "pansement", "bandage", "film", "diachylon",
                ))
            ):
                dressing_type = "film ou pansement transparent"
            else:
                dressing_type = ""
            if dressing_type:
                traits.append(dressing_type)
                wound_dressing_groups[dressing_type].append(candidate_id)
        matched_ingredients = [
            label for label, markers in ingredient_markers
            if any(marker in normalized_details for marker in markers)
        ]
        # Planogram names often carry only the well-known medicine brand. These
        # mappings identify the brand family for comparison while every package
        # strength and combination still has to be confirmed on its own label.
        brand_ingredient_markers = (
            ("acétaminophène", ("tylenol", "tempra", "atasol")),
            ("ibuprofène", ("advil", "motrin")),
            ("naproxène", ("aleve",)),
            ("acide acétylsalicylique", ("aspirin", "aspirine")),
        )
        for label, markers in brand_ingredient_markers:
            if any(
                re.search(rf"(?<![a-z0-9]){re.escape(marker)}(?![a-z0-9])", normalized_name)
                for marker in markers
            ) and label not in matched_ingredients:
                matched_ingredients.append(label)
        for label in matched_ingredients:
            if label not in ingredient_families:
                ingredient_families.append(label)
            traits.append(f"famille {label}; confirmer l'ingrédient du produit exact")
        if is_toothbrush_query:
            if "tete" in normalized_name:
                role = "tête de remplacement"
            elif "pile" in normalized_name:
                role = "brosse à pile"
            elif "rechargeable" in normalized_details or re.search(r"\brech\b", normalized_name):
                role = "brosse rechargeable"
            elif "elec" in normalized_name or "electri" in normalized_details:
                role = "brosse électrique (alimentation à confirmer)"
            else:
                role = "type d'alimentation à confirmer"
            traits.append(role)
            toothbrush_groups[role] += 1
        else:
            form = detected_product_form(normalized_name, normalized_details)
            if form:
                if form not in forms:
                    forms.append(form)
                traits.append(form)
                form_candidate_ids[form].append(candidate_id)
                description_verified = bool(
                    str(product.get("description_status", "") or "")
                    == "verified"
                    and "description" in set(
                        product.get("_verified_fields") or []
                    )
                )
                if (
                    description_verified
                    and any(marker in normalized_details for marker in (
                        "soulagement rapide",
                        "absorbe rapidement",
                        "action rapide",
                    ))
                ):
                    rapid_relief_form_candidate_ids[form].append(
                        candidate_id
                    )
        if not is_toothbrush_query:
            dose_match = re.search(r"(?<!\d)(\d+(?:[.,]\d+)?)\s*mg\b", name, re.IGNORECASE)
            if dose_match:
                dose = dose_match.group(1).replace(",", ".")
                dose_label = f"{dose} mg"
                if dose_label not in doses:
                    doses.append(dose_label)
                traits.append(dose_label)
            for label, markers in flavor_markers:
                if any(marker in normalized_details for marker in markers):
                    if label not in flavors:
                        flavors.append(label)
                    traits.append(f"saveur {label}")
                    break
            feature_checks = (
                ("double action", "double action"),
                ("dissolution rapide", "dis rap"),
                ("sans sucre", "s sucre"),
                ("force maximale", "force max"),
                ("force maximale", "f max"),
                ("force maximale", "x f"),
            )
            for label, marker in feature_checks:
                if marker in normalized_details:
                    if label not in features:
                        features.append(label)
                    if label not in traits:
                        traits.append(label)
            if re.search(r"\b(?:enf|enfant|jr|junior|children|kids)\b", normalized_name):
                traits.append("format enfant indiqué; âge à confirmer")
            if re.search(r"\b(?:rh|rhume|sin|sinus|cold|flu)\b", normalized_name):
                traits.append("formule rhume ou sinus; ingrédients à confirmer")
        product_traits[candidate_id] = traits
    doses.sort(key=lambda value: float(value.removesuffix(" mg")))
    form_comparison_terms = {
        "forme", "formes", "liquide", "suspension", "comprime", "comprimes",
        "capsule", "capsules", "gel", "gels", "gomme", "gommes", "sirop",
        "caplet", "caplets",
    }
    is_form_comparison_query = bool(
        query_plan.get("needs_comparison")
        and question_words.intersection(form_comparison_terms)
        and len(forms) >= 2
    )
    if rapid_relief_form_candidate_ids.get("liqui-gels"):
        form_guidance["liqui-gels"] = (
            "capsules contenant une préparation liquide, à avaler entières; "
            "les fiches exactes examinées les présentent pour un soulagement "
            "rapide, à confirmer sur l'emballage choisi"
        )

    source_ids_by_product = defaultdict(list)
    valid_source_ids = []
    for document in documents:
        source_id = str(document.get("source_id", "") or "").strip()
        if not source_id:
            continue
        if source_id not in valid_source_ids:
            valid_source_ids.append(source_id)
        for candidate_id in document.get("candidate_ids", []) or []:
            candidate_id = str(candidate_id or "").strip()
            if source_id not in source_ids_by_product[candidate_id]:
                source_ids_by_product[candidate_id].append(source_id)

    if names and is_toothbrush_query:
        group_parts = []
        for label, singular, plural in (
            ("brosse à pile", "brosse à pile", "brosses à pile"),
            ("brosse rechargeable", "brosse rechargeable", "brosses rechargeables"),
            (
                "brosse électrique (alimentation à confirmer)",
                "brosse électrique (alimentation à confirmer)",
                "brosses électriques (alimentation à confirmer)",
            ),
            ("tête de remplacement", "tête de remplacement", "têtes de remplacement"),
            (
                "type d'alimentation à confirmer",
                "type d'alimentation à confirmer",
                "types d'alimentation à confirmer",
            ),
        ):
            count = toothbrush_groups.get(label, 0)
            if count:
                group_parts.append(f"{count} {singular if count == 1 else plural}")
        answer = (
            f"J'ai trouvé {len(names)} produit{'s' if len(names) > 1 else ''} pertinent"
            f"{'s' if len(names) > 1 else ''} dans le plan actuel : "
            + ", ".join(group_parts) + ". "
            "Une brosse à pile utilise des piles remplaçables; une brosse rechargeable se "
            "recharge et convient généralement mieux à un usage régulier. Les produits indiqués "
            "comme têtes sont des pièces de remplacement, pas des brosses complètes. Vérifiez la "
            "compatibilité exacte de la tête et le type d'alimentation sur l'emballage."
        )
    elif names and is_headache_query:
        has_acetaminophen = "acétaminophène" in ingredient_families
        nsaid_families = [
            ingredient for ingredient in ingredient_families
            if ingredient in {
                "ibuprofène", "naproxène", "acide acétylsalicylique"
            }
        ]
        if has_acetaminophen and nsaid_families:
            choice = (
                "les familles courantes repérées sont l'acétaminophène et les AINS "
                f"({', '.join(nsaid_families)})"
            )
        elif has_acetaminophen:
            choice = "le choix clairement repéré est l'acétaminophène"
        elif nsaid_families:
            choice = (
                "les choix clairement repérés sont des AINS "
                f"({', '.join(nsaid_families)})"
            )
        else:
            choice = "choisissez un produit à un seul ingrédient actif après l'avoir confirmé"
        answer = (
            f"Pour un mal de tête occasionnel chez un adulte, {choice}. "
            "L'acétaminophène soulage la douleur; les AINS agissent sur la douleur et "
            "l'inflammation, mais nécessitent davantage de vérifications selon la personne. "
            "Vérifiez ce qui a déjà été pris et ne combinez ni deux produits contenant de "
            "l'acétaminophène, ni deux AINS."
        )
    elif names and is_fever_query:
        has_acetaminophen = "acétaminophène" in ingredient_families
        nsaid_families = [
            ingredient for ingredient in ingredient_families
            if ingredient in {
                "ibuprofène", "naproxène", "acide acétylsalicylique"
            }
        ]
        if has_acetaminophen and nsaid_families:
            choices = (
                "les choix repérés comprennent l'acétaminophène et les AINS "
                f"({', '.join(nsaid_families)})"
            )
        elif has_acetaminophen:
            choices = "le choix clairement repéré est l'acétaminophène"
        elif nsaid_families:
            choices = (
                "les choix clairement repérés sont des AINS "
                f"({', '.join(nsaid_families)})"
            )
        else:
            choices = (
                "confirmez d'abord l'ingrédient actif du produit avec le pharmacien"
            )
        answer = (
            f"Pour soulager une fièvre, {choices}. L'âge, la cause possible, les autres "
            "médicaments et les conditions de santé déterminent lequel peut convenir. "
            "Vérifiez ce qui a déjà été pris et ne combinez ni deux produits contenant "
            "de l'acétaminophène, ni deux AINS. Pour un enfant ou une fièvre importante, "
            "persistante ou accompagnée d'autres symptômes, faites confirmer la conduite "
            "à suivre par un professionnel."
        )
    elif names and is_melatonin_query:
        assortment = []
        if forms:
            assortment.append(f"formes: {', '.join(forms)}")
        if doses:
            assortment.append(f"concentrations: {', '.join(doses)}")
        if features:
            assortment.append(f"mentions: {', '.join(features)}")
        assortment_text = "; ".join(assortment)
        flavor_text = (
            f"Les saveurs explicitement indiquées sont {', '.join(flavors)}."
            if flavors else
            "Aucune saveur ne peut être confirmée avec les fiches actuelles; vérifiez l'emballage."
        )
        answer = (
            "Pour choisir une mélatonine, partez d'abord du besoin: difficulté occasionnelle "
            "à s'endormir, horaire décalé ou travail de nuit, ou décalage horaire. "
            + (f"Dans le plan actuel, on distingue {assortment_text}. " if assortment_text else "")
            + "Les gommes, comprimés et capsules changent surtout la façon de la prendre; une "
            "mention « dissolution rapide » décrit la forme, tandis qu'une mention « double "
            "action » doit être confirmée sur l'étiquette avant de conclure à une libération "
            "prolongée. "
            + flavor_text
        )
    elif names and is_wound_dressing_comparison:
        hydro_count = len(wound_dressing_groups.get("pansement hydrocolloïde", []))
        film_count = len(
            wound_dressing_groups.get("film ou pansement transparent", [])
        )
        availability = []
        if hydro_count:
            availability.append(
                f"{hydro_count} hydrocolloïde{'s' if hydro_count > 1 else ''}"
            )
        if film_count:
            availability.append(
                f"{film_count} transparent{'s' if film_count > 1 else ''}"
            )
        availability_text = (
            " Parmi les produits comparés, le plan contient "
            + " et ".join(availability) + "."
            if availability else ""
        )
        answer = (
            "Un pansement hydrocolloïde absorbe une quantité légère à modérée de liquide "
            "et forme un gel qui maintient un milieu humide protégé; il est surtout utile "
            "pour une plaie superficielle ou une ampoule qui suinte un peu. Un film "
            "transparent est beaucoup plus mince et sert surtout de barrière contre l'eau "
            "et les saletés tout en laissant la zone visible; sans coussinet absorbant, il "
            "convient plutôt à une plaie sèche ou très peu exsudative. "
            "Donc: choisir l'hydrocolloïde lorsqu'il faut absorber et protéger la "
            "cicatrisation, et le transparent lorsqu'il faut surtout voir et imperméabiliser "
            "la zone."
            + availability_text
            + " Vérifier la taille, la présence d'un coussinet, le niveau d'exsudat et les "
            "contre-indications du produit exact."
        )
    elif names and is_toothpaste_comparison:
        answer = (
            "Un dentifrice pour dents sensibles a comme objectif principal de réduire "
            "la douleur déclenchée par le froid, le chaud ou le contact; il doit contenir "
            "une indication et un ingrédient antisensibilité confirmés sur l'emballage et "
            "s'utilise généralement de façon régulière. Un dentifrice blanchissant vise "
            "surtout les taches de surface; il ne corrige pas toutes les causes de changement "
            "de couleur et peut parfois augmenter la sensibilité. Pour choisir: priorité à "
            "une formule antisensibilité si la personne a mal, et à une formule blanchissante "
            "si le besoin est seulement cosmétique. Certains produits font les deux; dans ce "
            "cas, confirmer les deux indications plutôt que de se fier au nom. Une sensibilité "
            "persistante ou qui s'aggrave doit être évaluée par un dentiste."
        )
    elif names and is_form_comparison_query:
        practical_differences = [
            f"Les {form} : {form_guidance[form]}."
            for form in forms[:4]
            if form in form_guidance
        ]
        strength_text = (
            f" Les concentrations repérées ({', '.join(doses)}) doivent être comparées "
            "séparément de la forme."
            if doses else ""
        )
        answer = (
            "La différence principale entre ces formes est la façon de les prendre; "
            "une forme différente ne signifie pas automatiquement qu'elle est plus forte "
            "ou plus rapide. "
            + " ".join(practical_differences)
            + strength_text
            + " Choisissez d'abord selon la capacité à avaler ou à mesurer le produit, "
            "puis confirmez l'ingrédient, la concentration, l'âge et les avertissements "
            "du produit exact."
        )
    elif names:
        summary_parts = []
        if forms:
            summary_parts.append(f"les formes repérées sont {', '.join(forms)}")
        if doses:
            summary_parts.append(f"les concentrations indiquées sont {', '.join(doses)}")
        if flavors:
            summary_parts.append(f"les saveurs explicitement nommées sont {', '.join(flavors)}")
        elif asks_flavors:
            summary_parts.append(
                "les saveurs ne sont pas clairement précisées dans les fiches disponibles"
            )
        if features:
            summary_parts.append(f"les mentions particulières sont {', '.join(features)}")
        if not summary_parts:
            summary_parts.append(
                "les différences confirmées sont présentées produit par produit ci-dessous"
            )
        evidence_items = []
        for product in selected:
            excerpt = product_evidence_excerpt(product)
            name = str(product.get("name", "") or "").strip()
            if name and excerpt:
                evidence_items.append((name, excerpt))
            if len(evidence_items) >= 3:
                break
        if query_plan.get("needs_comparison") and len(evidence_items) >= 2:
            answer = (
                "D'après les fiches actuelles du magasin, voici la différence la plus utile : "
                + " ".join(
                    f"{name} — {excerpt}."
                    for name, excerpt in evidence_items
                )
                + " Comparez ensuite l'usage précis, le format et les avertissements sur "
                "l'emballage; une fiche marquée non vérifiée doit être confirmée."
            )
        elif len(evidence_items) >= 2:
            answer = (
                "Les options les plus directement liées à la demande sont : "
                + " ".join(
                    f"{name} — {excerpt}."
                    for name, excerpt in evidence_items
                )
                + " Le meilleur choix dépend du symptôme précis et des différences "
                "confirmées sur l'étiquette; ne retenez pas un produit qui partage "
                "seulement un mot avec la demande."
            )
        elif evidence_items:
            name, excerpt = evidence_items[0]
            answer = (
                f"Le résultat le plus directement lié est {name}. "
                f"Sa fiche actuelle indique : {excerpt}. "
                "Les cartes ci-dessous montrent les autres options du plan et leur "
                "emplacement; confirmez sur l'emballage toute fiche marquée non vérifiée."
            )
        else:
            answer = (
                "Les options du magasin se distinguent surtout ainsi : "
                + "; ".join(summary_parts) + ". "
                "Pour répondre au client, partez du besoin précis puis comparez ces différences; "
                "confirmez sur l'emballage toute caractéristique absente de la fiche."
            )
    else:
        answer = (
            "Je n'ai trouvé aucun produit correspondant dans le plan actuel du magasin. "
            "Précisez le nom, la forme, la saveur ou le besoin recherché."
        )

    comparisons = []
    for product in selected[:8]:
        candidate_id = str(product.get("client_id", "") or "")
        traits = product_traits.get(candidate_id, [])
        details = [", ".join(traits).capitalize()] if traits else []
        evidence = product_evidence_excerpt(product, limit=260)
        if evidence:
            details.append(evidence)
        source_ids = source_ids_by_product.get(candidate_id, [])
        specific_sources = [source_id for source_id in source_ids if source_id != "store-plan"]
        comparisons.append({
            "candidate_id": candidate_id,
            "difference": ". ".join(details)[:420] or (
                f"Donnée disponible dans le plan : {str(product.get('name', '') or '').strip()}."
            ),
            "practical_note": f"Emplacement : {_recommendation_location(product)}",
            "source_ids": (specific_sources or source_ids or ["store-plan"])[:4],
        })

    key_points = []
    store_source = ["store-plan"] if "store-plan" in valid_source_ids else []
    acetaminophen_source = (
        ["health-canada:acetaminophen-safe-use"]
        if "health-canada:acetaminophen-safe-use" in valid_source_ids else []
    )
    nsaid_source = (
        ["health-canada:nsaid-guidance"]
        if "health-canada:nsaid-guidance" in valid_source_ids else []
    )
    info_sante_source = (
        ["quebec:info-sante-811"]
        if "quebec:info-sante-811" in valid_source_ids else []
    )
    melatonin_uses_source = (
        ["health-canada:melatonin-uses"]
        if "health-canada:melatonin-uses" in valid_source_ids else []
    )
    melatonin_safety_source = (
        ["health-canada:melatonin-safety"]
        if "health-canada:melatonin-safety" in valid_source_ids else []
    )
    melatonin_pediatric_source = (
        ["health-canada:melatonin-pediatric"]
        if "health-canada:melatonin-pediatric" in valid_source_ids else []
    )
    wound_hydrocolloid_source = (
        ["nhs:wound-hydrocolloid"]
        if "nhs:wound-hydrocolloid" in valid_source_ids else []
    )
    wound_transparent_source = (
        ["nhs:wound-transparent-film"]
        if "nhs:wound-transparent-film" in valid_source_ids else []
    )
    sensitive_toothpaste_source = (
        ["cda:sensitive-toothpaste"]
        if "cda:sensitive-toothpaste" in valid_source_ids else []
    )
    whitening_toothpaste_source = (
        ["cda:whitening-toothpaste"]
        if "cda:whitening-toothpaste" in valid_source_ids else []
    )
    if is_headache_query or is_fever_query:
        key_points.append({
            "heading": "Choix rapide",
            "detail": (
                (
                    "L'acétaminophène et certains AINS comme l'ibuprofène peuvent réduire "
                    "la fièvre; confirmer lequel convient selon l'âge, les médicaments et "
                    "les conditions de santé."
                )
                if is_fever_query else
                (
                    "L'acétaminophène vise la douleur et la fièvre. Les AINS comme "
                    "l'ibuprofène ou le naproxène visent la douleur et l'inflammation; "
                    "confirmer qu'ils conviennent à la personne avant de les proposer."
                )
            ),
            "source_ids": (acetaminophen_source + nsaid_source)[:4],
        })
        key_points.append({
            "heading": "Avant de proposer",
            "detail": (
                "Demander l'âge, les médicaments déjà pris, les allergies, la grossesse ou "
                "l'allaitement et les conditions de santé pertinentes."
            ),
            "source_ids": [],
        })
        key_points.append({
            "heading": "Ne pas combiner",
            "detail": (
                "Ne pas prendre deux produits contenant de l'acétaminophène. Ne pas associer "
                "deux AINS, par exemple ibuprofène, naproxène ou AAS."
            ),
            "source_ids": (acetaminophen_source + nsaid_source)[:4],
        })
        key_points.append({
            "heading": "Quand référer",
            "detail": (
                (
                    "Le pharmacien peut confirmer le produit approprié. Demander une "
                    "évaluation pour un enfant ou si la fièvre est importante, persistante "
                    "ou accompagnée de symptômes préoccupants."
                )
                if is_fever_query else
                (
                    "Le pharmacien peut confirmer le produit approprié; Info-Santé 811 "
                    "peut conseiller la personne si le mal de tête est inhabituel, important, "
                    "persistant ou accompagné d'autres symptômes."
                )
            ),
            "source_ids": (
                (acetaminophen_source + nsaid_source)[:4]
                if is_fever_query else info_sante_source
            ),
        })
    elif is_melatonin_query:
        key_points.extend([{
            "heading": "Choisir selon le besoin",
            "detail": (
                "Distinguer l'endormissement occasionnel, un horaire perturbé ou le travail "
                "de nuit, et le décalage horaire; ce contexte est plus utile que de choisir "
                "automatiquement la concentration la plus élevée."
            ),
            "source_ids": melatonin_uses_source,
        }, {
            "heading": "Forme et concentration",
            "detail": (
                "La gomme, le comprimé, la capsule ou la dissolution rapide changent surtout "
                "la prise. Confirmer sur l'étiquette l'usage autorisé, la libération et la "
                "concentration du produit exact."
            ),
            "source_ids": melatonin_uses_source,
        }, {
            "heading": "Moins de 18 ans",
            "detail": (
                "Au Canada, l'usage lié au sommeil chez les moins de 18 ans relève des "
                "médicaments sur ordonnance depuis le 2 juin 2026."
            ),
            "source_ids": melatonin_pediatric_source,
        }, {
            "heading": "Précautions",
            "detail": (
                "Éviter l'alcool et les autres produits causant de la somnolence; ne pas "
                "conduire pendant 5 heures. Référer en cas de grossesse, allaitement, "
                "médicaments, maladie pertinente ou insomnie persistante."
            ),
            "source_ids": melatonin_safety_source,
        }])
    elif is_toothbrush_query:
        if toothbrush_groups.get("brosse à pile"):
            key_points.append({
                "heading": "Modèles à pile",
                "detail": (
                    f"{toothbrush_groups['brosse à pile']} produit(s) explicitement identifié(s); "
                    "les piles sont remplaçables."
                ),
                "source_ids": store_source,
            })
        if toothbrush_groups.get("brosse rechargeable"):
            key_points.append({
                "heading": "Modèles rechargeables",
                "detail": (
                    f"{toothbrush_groups['brosse rechargeable']} produit(s) explicitement "
                    "identifié(s) comme rechargeable(s)."
                ),
                "source_ids": store_source,
            })
        if toothbrush_groups.get("tête de remplacement"):
            key_points.append({
                "heading": "Têtes de remplacement",
                "detail": (
                    f"{toothbrush_groups['tête de remplacement']} produit(s); vérifier la "
                    "compatibilité avec le manche avant de les proposer."
                ),
                "source_ids": store_source,
            })
    elif is_wound_dressing_comparison:
        key_points.extend([{
            "heading": "Hydrocolloïde",
            "detail": (
                "Absorbe l'exsudat léger à modéré, forme un gel et maintient un milieu humide. "
                "Utile notamment pour certaines plaies superficielles et ampoules; ne convient "
                "pas à une plaie fortement exsudative."
            ),
            "source_ids": wound_hydrocolloid_source,
        }, {
            "heading": "Film transparent",
            "detail": (
                "Barrière mince et transparente qui permet de voir la zone. Sans coussinet "
                "absorbant, elle vise surtout une plaie sèche ou très peu exsudative."
            ),
            "source_ids": wound_transparent_source,
        }, {
            "heading": "Choix rapide",
            "detail": (
                "Hydrocolloïde si l'objectif est d'absorber un peu et de protéger la "
                "cicatrisation; film transparent si l'objectif principal est de surveiller "
                "visuellement et d'imperméabiliser."
            ),
            "source_ids": (
                wound_hydrocolloid_source + wound_transparent_source
            )[:4],
        }, {
            "heading": "Avant d'appliquer",
            "detail": (
                "Confirmer la taille et la profondeur de la plaie, la quantité de liquide, "
                "l'état de la peau autour, les signes d'infection et la présence d'un "
                "coussinet absorbant dans le produit exact."
            ),
            "source_ids": (
                wound_hydrocolloid_source + wound_transparent_source
            )[:4],
        }])
    elif is_toothpaste_comparison:
        key_points.extend([{
            "heading": "Dents sensibles",
            "detail": (
                "Chercher une indication antisensibilité et vérifier l'ingrédient actif. "
                "L'effet dépend d'un usage régulier selon l'étiquette."
            ),
            "source_ids": sensitive_toothpaste_source,
        }, {
            "heading": "Blanchissant",
            "detail": (
                "Vise surtout à retirer les taches de surface; ce n'est pas l'équivalent "
                "d'un traitement de blanchiment et cela peut accentuer la sensibilité."
            ),
            "source_ids": whitening_toothpaste_source,
        }, {
            "heading": "Produit combiné",
            "detail": (
                "Un même dentifrice peut être antisensibilité et blanchissant. Confirmer "
                "les deux indications sur l'emballage du produit exact."
            ),
            "source_ids": (
                sensitive_toothpaste_source + whitening_toothpaste_source
            )[:4],
        }, {
            "heading": "Quand référer",
            "detail": (
                "Une douleur ou sensibilité persistante, qui s'aggrave ou touche une dent "
                "précise peut signaler un problème à faire évaluer par un dentiste."
            ),
            "source_ids": sensitive_toothpaste_source,
        }])
    elif is_form_comparison_query:
        for form in forms[:4]:
            if form not in form_guidance:
                continue
            form_sources = []
            for candidate_id in (
                rapid_relief_form_candidate_ids.get(form)
                or form_candidate_ids.get(form)
                or []
            ):
                for source_id in source_ids_by_product.get(
                    candidate_id, []
                ):
                    if (
                        source_id not in form_sources
                        and source_id != "store-plan"
                    ):
                        form_sources.append(source_id)
                    if len(form_sources) >= 4:
                        break
                if len(form_sources) >= 4:
                    break
            key_points.append({
                "heading": form.capitalize(),
                "detail": form_guidance[form].capitalize() + ".",
                "source_ids": form_sources or store_source,
            })
    elif forms:
        key_points.append({
            "heading": "Formes disponibles",
            "detail": ", ".join(forms).capitalize(),
            "source_ids": store_source,
        })
    if (
        doses and not is_headache_query and not is_fever_query
        and not is_melatonin_query
    ):
        key_points.append({
            "heading": "Concentrations repérées",
            "detail": ", ".join(doses),
            "source_ids": store_source,
        })
    if (
        not is_toothbrush_query and not is_headache_query and not is_fever_query
        and not is_melatonin_query and (flavors or asks_flavors)
    ):
        key_points.append({
            "heading": "Saveurs",
            "detail": (
                ", ".join(flavors).capitalize()
                if flavors else "Aucune saveur n'est explicitement confirmée dans les fiches examinées."
            ),
            "source_ids": store_source,
        })
    if (
        not is_toothbrush_query and not is_headache_query and not is_fever_query
        and not is_melatonin_query and features
    ):
        key_points.append({
            "heading": "Mentions particulières",
            "detail": ", ".join(features).capitalize(),
            "source_ids": store_source,
        })

    medical = bool(
        not is_toothbrush_query
        and (
            query_plan.get("medical", False)
            or doses
            or ingredient_families
            or is_melatonin_query
            or is_wound_dressing_comparison
            or is_toothpaste_comparison
        )
    )
    generic_dimensions = []
    if forms:
        generic_dimensions.append("la forme")
    if doses:
        generic_dimensions.append("la concentration")
    if flavors or asks_flavors:
        generic_dimensions.append("la saveur")
    generic_dimensions.extend(("le format", "le nombre d'unités"))
    generic_guidance = "Comparer " + ", ".join(generic_dimensions) + " sur l'emballage."
    headache_follow_ups = [
        "Quel âge a la personne et pour qui est le produit?",
        "A-t-elle déjà pris un médicament aujourd'hui, et lequel?",
        "Y a-t-il grossesse, allaitement, allergies ou conditions de santé à signaler?",
    ]
    fever_follow_ups = [
        "Quel âge a la personne et quelle température a été mesurée?",
        "Depuis combien de temps la fièvre dure-t-elle et quels autres symptômes sont présents?",
        "A-t-elle déjà pris un médicament aujourd'hui, et lequel?",
    ]
    melatonin_follow_ups = [
        "Est-ce pour un adulte de 18 ans ou plus?",
        "Le besoin principal est-il l'endormissement, un horaire décalé ou le décalage horaire?",
        "La personne prend-elle d'autres médicaments ou produits qui causent de la somnolence?",
    ]
    form_follow_ups = [
        "La personne peut-elle avaler une forme solide facilement?",
        "Quel âge a la personne et quel symptôme précis veut-elle soulager?",
        "A-t-elle déjà pris un produit contenant le même ingrédient aujourd'hui?",
    ]
    wound_follow_ups = [
        "La plaie est-elle sèche, légèrement humide ou très exsudative?",
        "Y a-t-il rougeur qui s'étend, chaleur, douleur croissante, pus ou fièvre?",
        "Faut-il surtout absorber, protéger de l'eau ou pouvoir observer la plaie?",
    ]
    toothpaste_follow_ups = [
        "Le besoin principal est-il une douleur de sensibilité ou seulement des taches?",
        "La sensibilité touche-t-elle plusieurs dents ou une dent précise?",
        "Le produit doit-il combiner antisensibilité, fluor et retrait des taches?",
    ]
    if is_toothbrush_query:
        practical_guidance = (
            "Comparer le type d'alimentation, le contenu de l'emballage et la "
            "compatibilité des têtes avant de proposer un modèle."
        )
        important_check = (
            "Ne pas confondre une tête de remplacement avec une brosse complète; "
            "confirmer la compatibilité sur l'emballage."
        )
    elif is_headache_query or is_fever_query:
        practical_guidance = (
            "Comparer d'abord l'ingrédient actif et la concentration, puis la forme "
            "et le format. Vérifier les médicaments déjà pris pour éviter un "
            "ingrédient en double."
        )
        important_check = (
            "Ne pas choisir uniquement selon la marque: confirmer l'ingrédient actif, "
            "la dose indiquée, les contre-indications et les avertissements sur l'emballage."
        )
    elif is_melatonin_query:
        practical_guidance = (
            "Choisir selon le contexte de sommeil, puis comparer la forme et la "
            "concentration. La saveur est un critère de préférence, pas une preuve "
            "d'un usage différent."
        )
        important_check = (
            "Ne pas déduire qu'un produit agit plus vite ou plus longtemps à partir "
            "du seul nom abrégé; confirmer la libération et l'usage sur l'étiquette."
        )
    elif is_wound_dressing_comparison:
        practical_guidance = (
            "Évaluer d'abord la quantité de liquide: hydrocolloïde pour absorber un peu "
            "et maintenir un milieu humide; film transparent pour une protection mince "
            "et visible lorsque l'absorption nécessaire est faible."
        )
        important_check = (
            "Ne pas choisir uniquement selon le mot « transparent »: certains produits "
            "ont un coussinet absorbant et d'autres sont seulement un film. Vérifier "
            "l'indication, la taille, la durée de port et les contre-indications."
        )
    elif is_toothpaste_comparison:
        practical_guidance = (
            "Choisir selon le besoin principal: formule antisensibilité lorsqu'il y a "
            "douleur au froid ou au chaud; formule blanchissante pour les taches de surface. "
            "Vérifier les ingrédients actifs et les indications du produit exact."
        )
        important_check = (
            "Un produit peut cumuler les deux fonctions. Ne pas déduire son effet à partir "
            "de la marque seule, et orienter vers un dentiste si la sensibilité persiste "
            "ou s'aggrave."
        )
    elif is_form_comparison_query:
        practical_guidance = (
            "Choisir la forme selon la façon de la prendre, puis comparer séparément "
            "l'ingrédient, la concentration, l'âge indiqué et le format."
        )
        important_check = (
            "Ne pas supposer qu'un liqui-gel agit plus vite ni qu'une concentration "
            "plus élevée convient mieux; confirmer ces éléments sur l'étiquette."
        )
    else:
        practical_guidance = generic_guidance
        important_check = (
            "Ne pas attribuer à un produit un usage qui n'apparaît pas sur sa fiche "
            "ou son étiquette."
        )
    return {
        "answer": answer,
        "selected_product_ids": selected_ids,
        "follow_up_questions": (
            headache_follow_ups if is_headache_query
            else fever_follow_ups if is_fever_query
            else melatonin_follow_ups if is_melatonin_query
            else wound_follow_ups if is_wound_dressing_comparison
            else toothpaste_follow_ups if is_toothpaste_comparison
            else form_follow_ups if is_form_comparison_query and medical
            else []
        ),
        "safety_flags": ([
            (
                "Vérifier l'ingrédient actif, la concentration, l'âge indiqué et les avertissements "
                "sur chaque emballage avant de proposer un produit."
                if is_headache_query or is_fever_query else
                (
                    "Confirmer que la personne a 18 ans ou plus et vérifier la concentration, "
                    "l'usage, les médicaments, les contre-indications et les avertissements."
                    if is_melatonin_query else
                    (
                        "Ne pas couvrir sans évaluation une plaie profonde, très exsudative "
                        "ou présentant des signes d'infection; vérifier l'étiquette du "
                        "pansement exact."
                        if is_wound_dressing_comparison else
                        (
                            "Vérifier l'indication antisensibilité ou blanchissante et les "
                            "ingrédients actifs; cesser et consulter si la douleur s'aggrave."
                            if is_toothpaste_comparison else
                            "Vérifier sur l'étiquette la concentration, la forme, les ingrédients, l'âge et les avertissements."
                        )
                    )
                )
            )
        ] if medical else []),
        "pharmacist_referral": medical,
        "pharmacist_reason": (
            (
                (
                    "Faire confirmer le choix pour un enfant, en présence d'autres "
                    "médicaments ou conditions de santé, ou si la fièvre est importante, "
                    "persistante ou accompagnée d'autres symptômes."
                    if is_fever_query else
                    "Faire confirmer le choix par le pharmacien en présence d'autres médicaments, "
                    "d'allergies, de grossesse, d'allaitement, pour un enfant, ou si le mal de tête "
                    "est important, inhabituel ou persistant."
                )
                if is_headache_query or is_fever_query else
                (
                    "Consulter le pharmacien pour une personne de moins de 18 ans, les "
                    "interactions, la grossesse, l'allaitement, une maladie pertinente ou "
                    "une insomnie persistante."
                    if is_melatonin_query else
                    (
                        "Demander une évaluation professionnelle si la plaie est profonde, "
                        "très exsudative, ne s'améliore pas ou présente des signes d'infection."
                        if is_wound_dressing_comparison else
                        (
                            "Orienter vers un dentiste si la sensibilité ou la douleur "
                            "persiste, s'aggrave ou touche une dent précise."
                            if is_toothpaste_comparison else
                            "Consulter le pharmacien pour les interactions, la grossesse, l'allaitement, "
                            "un enfant ou une situation médicale particulière."
                        )
                    )
                )
            )
        ) if medical else "",
        "key_points": key_points[:4],
        "comparisons": comparisons,
        "useful_guidance": [{
            "text": practical_guidance,
            "source_ids": (
                acetaminophen_source if is_headache_query or is_fever_query
                else melatonin_uses_source if is_melatonin_query
                else (
                    wound_hydrocolloid_source + wound_transparent_source
                )[:4] if is_wound_dressing_comparison
                else (
                    sensitive_toothpaste_source + whitening_toothpaste_source
                )[:4] if is_toothpaste_comparison
                else []
            ),
        }],
        "important_checks": [{
            "text": important_check,
            "source_ids": (
                (acetaminophen_source + nsaid_source)[:4]
                if is_headache_query or is_fever_query
                else melatonin_uses_source if is_melatonin_query
                else (
                    wound_hydrocolloid_source + wound_transparent_source
                )[:4] if is_wound_dressing_comparison
                else (
                    sensitive_toothpaste_source + whitening_toothpaste_source
                )[:4] if is_toothpaste_comparison
                else []
            ),
        }],
        "source_ids": valid_source_ids[:16],
        "degraded": bool(degraded),
        "local_summary": not degraded,
        "warning": (
            "La réponse détaillée n'a pas été disponible à temps; les produits et sources du magasin restent accessibles."
            if degraded else ""
        ),
    }


_DOCUMENTED_COMPACT_FACT_FIELDS = (
    "category", "package_size", "package_unit", "variant", "flavour",
    "colour", "strength", "dosage_form", "manufacturer", "ingredients",
    "compatibility", "purpose", "route_of_administration",
)


def _compact_documented_fact(field, value):
    text = str(value or "").strip()
    if field == "strength":
        zero_denominator = re.fullmatch(
            r"(\d+)(?:[.,]0+)?\s*([A-Za-zµ]+)\s*/\s*0(?:[.,]0+)?"
            r"(?:\s*[A-Za-zµ]+)?",
            text,
        )
        if zero_denominator:
            return f"{zero_denominator.group(1)} {zero_denominator.group(2)}"
    return text


def _compact_documented_product_context(product, include_identifiers=False):
    """Keep only facts the answer model can use.

    Locations, images, provenance maps and duplicate public identifier objects
    are added by the server after generation. Sending them to the model made a
    small comparison request several times larger without improving its answer.
    """
    context = product_context_for_client_rag(product)
    verified_fields = set(product.get("_verified_fields") or [])
    field_sources = product.get("_field_sources") or {}
    description_source = field_sources.get("description") or {}
    facts = {
        field: _compact_documented_fact(field, context.get(field, ""))
        for field in _DOCUMENTED_COMPACT_FACT_FIELDS
        if _compact_documented_fact(field, context.get(field, ""))
    }
    catalogue_clues = {}
    for field in ("brand", *_DOCUMENTED_COMPACT_FACT_FIELDS):
        if field in verified_fields:
            continue
        value = str(product.get(field, "") or "").strip()
        if value:
            catalogue_clues[field] = value[:500]
    search_clues = {}
    for field in ("usage_notes", "search_terms"):
        value = str(product.get(field, "") or "").strip()
        if value:
            search_clues[field] = value[:400]
    compact = {
        "candidate_id": str(context.get("candidate_id", "") or ""),
        "name": str(context.get("name", "") or "")[:300],
        "name_source": "store_planogram",
        "brand": str(product.get("brand", "") or "")[:160],
        "catalogue_description": {
            "text": _description_excerpt_for_ai(
                context.get("description", ""), max_chars=900,
            ),
            "verification_status": str(
                context.get("description_status", "") or "unverified"
            )[:60],
            "verified": (
                str(context.get("description_status", "") or "") == "verified"
            ),
            "source": str(description_source.get("source", "") or "")[:120],
        },
        "data_status": str(context.get("data_status", "") or "")[:60],
        "verified_facts": facts,
        "catalogue_clues_to_confirm": catalogue_clues,
        "search_clues_not_product_facts": search_clues,
    }
    if include_identifiers:
        compact["verified_identifiers"] = (
            context.get("verified_identifiers", []) or []
        )[:4]
        compact["unconfirmed_identifier_candidates"] = [{
            "type": str(identifier.get("type", "") or ""),
            "value": str(identifier.get("value", "") or ""),
            "status": "unconfirmed",
        } for identifier in (
            context.get("unconfirmed_identifier_candidates", []) or []
        )[:4]]
    return compact


def _compact_documented_history(history):
    return [{
        "role": item["role"],
        "content": item["content"][:600],
    } for item in normalize_client_history(history, max_messages=4)]


def _documented_answer_covers_request(
    result, query_plan, candidates, documents=None,
):
    """Reject catalogue summaries that do not answer the requested dimensions."""
    from routes.products import normalize_search_text

    answer = str(result.get("answer", "") or "").strip()
    if len(answer) < 45:
        return False
    if candidates and not result.get("selected_product_ids"):
        return False
    combined = " ".join([
        answer,
        *[
            f"{point.get('heading', '')} {point.get('detail', '')}"
            for point in result.get("key_points", [])
            if isinstance(point, dict)
        ],
    ])
    normalized_answer = normalize_search_text(combined)
    normalized_question = normalize_search_text(
        str(query_plan.get("corrected_query", "") or "")
    )
    if normalized_answer.startswith((
        "j ai trouve",
        "nous avons trouve",
        "produits correspondant",
    )):
        return False
    requested_dimensions = (
        (
            {"saveur", "saveurs", "gout", "gouts", "flavor", "flavors"},
            {"saveur", "gout", "flavor"},
        ),
        (
            {"contexte", "contextes", "situation", "situations", "utiliser"},
            {"contexte", "situation", "usage", "utilis", "besoin"},
        ),
    )
    question_words = set(normalized_question.split())
    answer_words = set(normalized_answer.split())
    for question_terms, answer_terms in requested_dimensions:
        if question_words.intersection(question_terms) and not any(
            term in normalized_answer for term in answer_terms
        ):
            return False
    if query_plan.get("needs_comparison") and not (
        answer_words.intersection({
            "difference", "choisir", "plutot", "tandis", "forme",
            "concentration", "format", "ingredient", "usage",
        })
        or "par rapport" in normalized_answer
    ):
        return False
    if query_plan.get("medical"):
        trusted_medical_ids = {
            str(document.get("source_id", "") or "")
            for document in documents or []
            if document.get("medical_claims_allowed")
            and int(document.get("trust_level", 0) or 0) >= 80
        }
        if trusted_medical_ids:
            cited_ids = set(result.get("source_ids") or [])
            for point in result.get("key_points", []):
                if isinstance(point, dict):
                    cited_ids.update(point.get("source_ids") or [])
            if not cited_ids.intersection(trusted_medical_ids):
                return False
    return True


def _generate_documented_client_answer_sync(
    question, query_plan, candidates, documents, history=None,
    selected_text="", focus_product_id="", quality_mode=True,
):
    include_identifiers = bool(re.search(
        r"\b(?:DIN(?:[\s-]?HM)?|NPN|UPC|GTIN)\b",
        str(question or ""),
        flags=re.IGNORECASE,
    ))
    contexts = [
        _compact_documented_product_context(
            product, include_identifiers=include_identifiers,
        )
        for product in candidates
    ]
    document_contexts = [{
        "source_id": str(document.get("source_id", "") or ""),
        "title": str(document.get("title", "") or "")[:180],
        "publisher": str(document.get("publisher", "") or "")[:100],
        "evidence": str(document.get("evidence", "") or "")[:700],
        "candidate_ids": (document.get("candidate_ids", []) or [])[:12],
        "verification_status": str(
            document.get("verification_status", "") or ""
        )[:60],
        "source_class": str(
            document.get("source_class", "") or ""
        )[:60],
        "trust_level": int(document.get("trust_level", 0) or 0),
        "medical_claims_allowed": bool(
            document.get("medical_claims_allowed", False)
        ),
    } for document in documents[:12]]
    compact_plan = {
        key: query_plan.get(key)
        for key in (
            "intent", "corrected_query", "wants_all", "needs_comparison",
            "answer_language", "medical", "keywords", "must_include", "exclude",
        )
    }
    parsed = _provider_structured_request(
        _CLIENT_DOCUMENTED_INSTRUCTIONS,
        {
            "conversation": _compact_documented_history(history),
            "question": question,
            "selected_text_from_previous_answer": selected_text[:500],
            "focused_product_id": focus_product_id,
            "query_plan": compact_plan,
            "candidates": contexts,
            "documents": document_contexts,
        },
        # The foreground K2.6 answer is intentionally compact; K3 keeps the
        # larger budget for the asynchronous documented upgrade.
        max_tokens=1400 if quality_mode else 800,
        schema_name="client_documented_answer",
        schema=_CLIENT_DOCUMENTED_SCHEMA,
        question_preview=question,
        quality_mode=quality_mode,
        realtime_model=True,
        timeout_seconds=None if quality_mode else 9,
    )
    if not isinstance(parsed, dict):
        return grounded_documented_fallback(query_plan, candidates, documents)
    partial_answer = str(parsed.get("_partial_answer", "") or "").strip()
    if partial_answer:
        grounded = grounded_documented_fallback(
            query_plan, candidates, documents, degraded=False
        )
        grounded["answer"] = partial_answer
        partial_ids = parsed.get("_partial_selected_product_ids", [])
        valid_ids = {
            str(product.get("client_id", "") or "")
            for product in candidates
        }
        grounded["selected_product_ids"] = [
            str(candidate_id) for candidate_id in partial_ids
            if str(candidate_id) in valid_ids
        ][:8]
        valid_source_ids = {
            str(document.get("source_id", "") or "")
            for document in documents
        }
        grounded["source_ids"] = [
            str(source_id) for source_id in parsed.get(
                "_partial_source_ids", []
            )
            if str(source_id) in valid_source_ids
        ][:16]
        grounded["degraded"] = False
        grounded["warning"] = ""
        return grounded
    result = normalize_documented_client_answer(
        parsed, [product.get("client_id", "") for product in candidates], documents
    )
    # The model has read the complete question, evidence, and inventory. Older
    # keyword heuristics replaced otherwise valid answers when they failed to
    # contain a prescribed word, which made the documented mode look canned and
    # often less relevant than the AI result. Structural validation and the
    # selected-ID allowlist are the authoritative guards here.
    if not str(result.get("answer", "") or "").strip():
        return grounded_documented_fallback(
            query_plan, candidates, documents, degraded=True,
        )
    grounded = grounded_documented_fallback(
        query_plan, candidates, documents, degraded=False
    )
    for key in ("follow_up_questions", "safety_flags"):
        if not result.get(key):
            result[key] = grounded.get(key, [])
    if not result.get("pharmacist_referral"):
        result["pharmacist_referral"] = bool(
            grounded.get("pharmacist_referral", False)
        )
    if not result.get("pharmacist_reason"):
        result["pharmacist_reason"] = str(
            grounded.get("pharmacist_reason", "") or ""
        )
    if not result.get("key_points"):
        result["key_points"] = grounded.get("key_points", [])[:3]
    selected_ids = set(result.get("selected_product_ids") or [])
    result["comparisons"] = [
        comparison for comparison in grounded.get("comparisons", [])
        if comparison.get("candidate_id") in selected_ids
    ][:4]
    if not result.get("useful_guidance"):
        result["useful_guidance"] = grounded.get("useful_guidance", [])[:2]
    if not result.get("important_checks"):
        result["important_checks"] = grounded.get("important_checks", [])[:2]
    for item in (
        result.get("comparisons", [])
        + result.get("useful_guidance", [])
        + result.get("important_checks", [])
    ):
        for source_id in item.get("source_ids", []):
            if source_id not in result["source_ids"]:
                result["source_ids"].append(source_id)
    result["source_ids"] = result["source_ids"][:16]
    result["degraded"] = False
    result["warning"] = ""
    return result


def _documented_answer_cache_key(
    question, query_plan, candidates, documents, history=None,
    selected_text="", focus_product_id="",
):
    evidence = {
        "model": KIMI_DOCUMENTED_MODEL,
        "question": re.sub(r"\s+", " ", str(question or "")).strip().lower(),
        "selected_text": str(selected_text or "").strip(),
        "focus_product_id": str(focus_product_id or ""),
        "history": _compact_documented_history(history),
        "plan": {
            key: query_plan.get(key)
            for key in (
                "intent", "corrected_query", "wants_all", "needs_comparison",
                "medical", "keywords", "must_include", "exclude",
            )
        },
        "products": [
            _compact_documented_product_context(product)
            for product in candidates
        ],
        "documents": [{
            "source_id": str(document.get("source_id", "") or ""),
            "verification_status": str(
                document.get("verification_status", "") or ""
            ),
            "evidence": str(document.get("evidence", "") or "")[:1800],
        } for document in documents],
    }
    return hashlib.sha256(
        json.dumps(
            evidence, ensure_ascii=False, sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _documented_answer_cache_get(key):
    now = time.time()
    with _DOCUMENTED_ANSWER_CACHE_LOCK:
        expired = [
            cache_key for cache_key, entry in _DOCUMENTED_ANSWER_CACHE.items()
            if now - float(entry.get("created_at", 0) or 0)
            > _DOCUMENTED_ANSWER_CACHE_TTL_SECONDS
        ]
        for cache_key in expired:
            _DOCUMENTED_ANSWER_CACHE.pop(cache_key, None)
        entry = _DOCUMENTED_ANSWER_CACHE.pop(key, None)
        if not entry:
            return None
        _DOCUMENTED_ANSWER_CACHE[key] = entry
        result = json.loads(json.dumps(entry["result"], ensure_ascii=False))
    result["_cache_hit"] = True
    return result


def _documented_answer_cache_set(key, result):
    if not _documented_answer_is_cacheable(result):
        return
    stored = json.loads(json.dumps(result, ensure_ascii=False))
    stored.pop("_cache_hit", None)
    stored.pop("_ai_pending", None)
    stored.pop("_documented_job_id", None)
    with _DOCUMENTED_ANSWER_CACHE_LOCK:
        _DOCUMENTED_ANSWER_CACHE.pop(key, None)
        _DOCUMENTED_ANSWER_CACHE[key] = {
            "created_at": time.time(), "result": stored,
        }
        while len(_DOCUMENTED_ANSWER_CACHE) > _DOCUMENTED_ANSWER_CACHE_MAX:
            _DOCUMENTED_ANSWER_CACHE.popitem(last=False)


def _documented_answer_is_cacheable(result):
    return not (
        not isinstance(result, dict)
        or result.get("degraded")
        or len(str(result.get("answer", "") or "").strip()) < 45
    )


def _documented_jobs_cleanup_locked(now=None):
    now = float(now or time.time())
    expired = [
        job_id for job_id, entry in _DOCUMENTED_JOBS.items()
        if now - float(entry.get("created_at", 0) or 0)
        > _DOCUMENTED_JOB_TTL_SECONDS
    ]
    for job_id in expired:
        _DOCUMENTED_JOBS.pop(job_id, None)
    while len(_DOCUMENTED_JOBS) > _DOCUMENTED_JOB_MAX:
        _DOCUMENTED_JOBS.popitem(last=False)


def _documented_job_create():
    job_id = secrets.token_urlsafe(24)
    with _DOCUMENTED_ANSWER_CACHE_LOCK:
        _documented_jobs_cleanup_locked()
        _DOCUMENTED_JOBS[job_id] = {
            "created_at": time.time(),
            "status": "pending",
            "result": None,
            "elapsed_ms": 0,
        }
    return job_id


def _documented_job_finish(job_id, result, elapsed_ms):
    public_result = None
    if _documented_answer_is_cacheable(result):
        public_result = {
            "answer": str(result.get("answer", "") or ""),
            "selected_product_ids": list(
                result.get("selected_product_ids") or []
            )[:16],
            "follow_up_questions": list(
                result.get("follow_up_questions") or []
            )[:4],
            "safety_flags": list(result.get("safety_flags") or [])[:5],
            "pharmacist_referral": bool(
                result.get("pharmacist_referral", False)
            ),
            "pharmacist_reason": str(
                result.get("pharmacist_reason", "") or ""
            )[:1200],
            "key_points": list(result.get("key_points") or [])[:6],
            "comparisons": list(result.get("comparisons") or [])[:8],
            "useful_guidance": list(
                result.get("useful_guidance") or []
            )[:6],
            "important_checks": list(
                result.get("important_checks") or []
            )[:6],
            "source_ids": list(result.get("source_ids") or [])[:16],
        }
    with _DOCUMENTED_ANSWER_CACHE_LOCK:
        entry = _DOCUMENTED_JOBS.get(job_id)
        if entry is None:
            return
        entry["status"] = "ready" if public_result else "failed"
        entry["result"] = public_result
        entry["elapsed_ms"] = max(0, int(elapsed_ms or 0))
        _DOCUMENTED_JOBS.move_to_end(job_id)
        _documented_jobs_cleanup_locked()


def _documented_job_status(job_id):
    with _DOCUMENTED_ANSWER_CACHE_LOCK:
        _documented_jobs_cleanup_locked()
        entry = _DOCUMENTED_JOBS.get(str(job_id or ""))
        if not entry:
            return None
        _DOCUMENTED_JOBS.move_to_end(str(job_id))
        return json.loads(json.dumps(entry, ensure_ascii=False))


def _guard_documented_inventory_contradiction(
    result, query_plan, candidates, documents,
):
    if not isinstance(result, dict) or not candidates:
        return result
    selected_ids = set(result.get("selected_product_ids") or ())
    selected_exists = any(
        str(product.get("client_id", "") or "") in selected_ids
        for product in candidates
    )
    if not _answer_denies_available_inventory(result.get("answer", "")):
        return result
    from routes.products import client_candidates_need_semantic_retry

    if not selected_exists and client_candidates_need_semantic_retry(
        str(query_plan.get("corrected_query", "") or ""), candidates,
    ):
        return result
    # A model cannot both select grounded store products and tell the employee
    # that none exist. Replace that internally contradictory result before it is
    # cached or exposed by the asynchronous documented-answer endpoint.
    return grounded_documented_fallback(
        query_plan, candidates, documents, degraded=False,
    )


def generate_documented_client_answer(
    question, query_plan, candidates, documents, history=None,
    selected_text="", focus_product_id="",
):
    """Return a fast documented answer, then optionally upgrade it with K3."""
    cache_key = _documented_answer_cache_key(
        question, query_plan, candidates, documents, history,
        selected_text=selected_text, focus_product_id=focus_product_id,
    )
    cached = _documented_answer_cache_get(cache_key)
    if cached:
        return cached

    # K2.6 without hidden thinking gives the employee a real, question-specific
    # answer in the foreground. The previous implementation showed a canned
    # fallback after 1.25 seconds while K3 was still working, so the user almost
    # never saw what the model had actually written.
    quick_result = _generate_documented_client_answer_sync(
        question, query_plan, candidates, documents, history,
        selected_text=selected_text,
        focus_product_id=focus_product_id,
        quality_mode=False,
    )
    quick_result = _guard_documented_inventory_contradiction(
        quick_result, query_plan, candidates, documents,
    )

    provider = configured_ai_provider().get("name", "")
    needs_upgrade = bool(
        provider == "kimi"
        and KIMI_DOCUMENTED_MODEL != KIMI_REALTIME_MODEL
    )
    if not needs_upgrade:
        _documented_answer_cache_set(cache_key, quick_result)
        if isinstance(quick_result, dict):
            quick_result["_cache_hit"] = False
        return quick_result

    if not _DOCUMENTED_AI_GATE.acquire(blocking=False):
        return quick_result

    def task():
        try:
            result = _generate_documented_client_answer_sync(
                question, query_plan, candidates, documents, history,
                selected_text=selected_text,
                focus_product_id=focus_product_id,
                quality_mode=True,
            )
            return _guard_documented_inventory_contradiction(
                result, query_plan, candidates, documents,
            )
        finally:
            _DOCUMENTED_AI_GATE.release()

    job_id = _documented_job_create()
    job_started_at = time.perf_counter()
    try:
        future = _DOCUMENTED_AI_EXECUTOR.submit(task)
    except RuntimeError:
        _DOCUMENTED_AI_GATE.release()
        return quick_result

    def finish_background(completed_future):
        try:
            completed_result = completed_future.result()
        except Exception:
            completed_result = None
        elapsed_ms = int(
            (time.perf_counter() - job_started_at) * 1000
        )
        if completed_result:
            _documented_answer_cache_set(cache_key, completed_result)
        _documented_job_finish(job_id, completed_result, elapsed_ms)
        record_ai_answer(
            "documented_background", elapsed_ms,
            degraded=not _documented_answer_is_cacheable(
                completed_result
            ),
            model=KIMI_DOCUMENTED_MODEL,
            product_count=len(
                (completed_result or {}).get("selected_product_ids") or []
            ),
        )

    future.add_done_callback(finish_background)
    quick_result["_ai_pending"] = True
    quick_result["_documented_job_id"] = job_id
    return quick_result


def generate_client_help_payload_deepseek(question, products):
    parsed = _deepseek_json_request([
        {"role": "system", "content": _CLIENT_HELP_INSTRUCTIONS},
        {"role": "user", "content": json.dumps({
            "question": question,
            "products": products,
        }, ensure_ascii=False)},
    ], max_tokens=2048, question_preview=question)
    return normalize_client_help_payload(parsed) if isinstance(parsed, dict) else None


def generate_client_help_payload_kimi(question, products):
    parsed = _kimi_json_request([
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
        with _safe_urlopen(request_obj, timeout=12) as response:
            raw_response = json.loads(_read_limited_response(response).decode("utf-8"))
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
        with _safe_urlopen(request_obj, timeout=12) as response:
            raw_response = json.loads(_read_limited_response(response).decode("utf-8"))
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


def generate_product_assist_payload_kimi(name, brand, description, barcode):
    prompt = {"name": name, "brand": brand, "description": description, "barcode": barcode}
    parsed = _kimi_json_request([
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

def _store_reference_regulatory_candidates(db, product, imported_at):
    barcode = normalized_digits(product.get("barcode", ""))
    stored = 0
    if not barcode:
        return stored
    for identifier in product.get("regulatory_identifiers") or []:
        source = str(identifier.get("source", "") or product.get("source", ""))
        source_url = str(
            identifier.get("source_url", "") or product.get("source_url", "")
        )
        if upsert_reference_identifier(
            db, barcode, identifier.get("type", ""),
            identifier.get("value", ""), authority=HEALTH_CANADA_AUTHORITY,
            source=source, source_url=source_url,
            source_record_id=(
                identifier.get("product_name", "")
                or product.get("name", "")
                or identifier.get("value", "")
            ),
            match_method="exact_gtin_labeled_source",
            confidence=0.92 if source.lower() == "familiprix" else 0.75,
            verification_status="requires_review",
            imported_at=imported_at,
        ):
            stored += 1
    return stored


def _reference_upsert(db, product):
    """Store an online candidate as evidence; never overwrite trusted fields."""
    barcode = normalized_digits(product.get("barcode", ""))
    name = str(product.get("name", "")).strip()
    if not barcode or len(name) < 3:
        return False
    candidate = dict(product)
    candidate["barcode"] = barcode
    candidate["source"] = str(candidate.get("source", "")).replace(" · cache", "")
    result = upsert_reference_candidate(
        db, candidate, imported_at=utc_now_iso()
    )
    imported_at = utc_now_iso()
    _store_reference_regulatory_candidates(db, candidate, imported_at)
    return bool(result.get("stored"))


def online_matches_catalog(cat_name, cat_brand, online, cat_barcode=""):
    """Guard against the online databases returning the WRONG product for a UPC.
    We trust the Familiprix catalogue name; an online result is only accepted if it
    shares a meaningful word (brand/product) with it. Prevents attaching a random
    description/image to the right product (the 'match super wrong' problem)."""
    candidate = dict(online or {})
    candidate_barcode = str(candidate.get("barcode", "") or cat_barcode or "")
    candidate["barcode"] = candidate_barcode
    catalog = {
        "barcode": str(cat_barcode or candidate_barcode),
        "name": str(cat_name or ""),
        "brand": str(cat_brand or ""),
    }
    return assess_metadata_candidate(
        catalog, candidate, match_method="exact_gtin"
    ).accepted


def reference_lookup(barcode):
    """Return a product from the local reference catalog, or None. Instant & free."""
    from database import connect_db
    db = connect_db()
    try:
        for cand in build_barcode_candidates(barcode):
            row = db.execute("SELECT * FROM product_reference WHERE barcode=?", (cand,)).fetchone()
            if row:
                d = dict(row)
                key = gtin_identity_key(d.get("barcode", ""))
                verified_values = {}
                if key:
                    for evidence_row in db.execute(
                        """SELECT field_name, field_value FROM product_reference_evidence
                           WHERE gtin_key=? AND active=1
                             AND verification_status='verified'""",
                        (key,),
                    ).fetchall():
                        evidence = dict(evidence_row)
                        verified_values[evidence["field_name"]] = evidence["field_value"]
                source_type, _priority = classify_source(
                    d.get("source", ""), d.get("source_url", "")
                )
                store_identity = source_type == "store_catalog"
                name = verified_values.get("name", "") or (
                    d.get("name", "") if store_identity else ""
                )
                if not str(name or "").strip():
                    continue
                return {"name": name,
                        "brand": verified_values.get("brand", ""),
                        "description": verified_values.get("description", ""),
                        "barcode": d.get("barcode", ""),
                        "product_code": verified_values.get("product_code", "") or (
                            d.get("product_code", "") if store_identity else ""
                        ),
                        "source": (d.get("source", "") or "catalogue") + " · cache",
                        "source_url": d.get("source_url", "") if verified_values else "",
                        "image_url": verified_values.get("image_url", ""),
                        "verification_status": "verified" if verified_values else "store_identity_only"}
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
        with _safe_urlopen(req, timeout=18) as response:
            raw = json.loads(_read_limited_response(response).decode("utf-8"))
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


def lookup_product_online(barcode, max_workers=None, wait_for_cleanup=False,
                          require_image=False, background=False,
                          skip_familiprix=False):
    """UPC lookup for a PHARMACY catalog (food, beauty, meds, vitamins, baby,
    bandages, eye care, Familiprix house brand…). Broad coverage but fast: it
    returns as soon as a trusted result is found (good_enough), so it doesn't wait
    on the slow scrapers unless the fast databases miss. No cache (always fresh),
    and the broken EAN API is not used. Returns a product dict or None.

    max_workers caps EACH phase's internal thread pool. Interactive scans keep the
    full fan-out (fastest single answer). ``background`` gives automatic image
    enrichment a strict source budget: one missing public result must never
    monopolize the only Render instance while employees are using the app."""
    barcode = str(barcode or "").strip()
    if not barcode:
        return None
    GOOD_ENOUGH = 24
    candidates = build_barcode_candidates(barcode)
    if background:
        # The original UPC plus one equivalent zero-padded GTIN cover useful
        # package variants without turning one missing image into dozens of calls.
        candidates = candidates[:2]
    best, best_score = None, 0

    def _cap(n):
        return min(n, max_workers) if max_workers else n

    def _satisfactory(product, score):
        return score >= GOOD_ENOUGH and (not require_image or _lookup_has_image(product))

    def _merge_candidate(product, score, candidate, candidate_score):
        return _prefer_lookup_result(
            product, score, candidate, candidate_score,
            require_image=require_image,
        )

    # Phase 1 — fast structured JSON databases: Open Facts (food / beauty / drug /
    # general) + UPC Item DB + Datakick + Brocade. Covers most everyday products.
    tasks = []
    for bc in candidates:
        tasks.append(lambda c=bc: lookup_upcitemdb(c))
        tasks.append(lambda c=bc: lookup_datakick(c))
        tasks.append(lambda c=bc: lookup_brocade(c))
        for sn, su in PRODUCT_LOOKUP_SOURCES:
            tasks.append(lambda c=bc, n=sn, u=su: lookup_open_facts_product(n, u, c))
    if background:
        tasks = tasks[:7]
    best, best_score = best_lookup_result(
        tasks, max_workers=_cap(16), good_enough=GOOD_ENOUGH,
        wait_for_cleanup=wait_for_cleanup,
        require_image=require_image,
    )

    # Phase 2 — Familiprix catalog + barcode databases. The Familiprix scraper is
    # what finds house-brand and pharmacy-specific items the open DBs don't have.
    if not _satisfactory(best, best_score):
        tasks = []
        for bc in candidates:
            if not skip_familiprix:
                tasks.append(
                    lambda c=bc, cs=candidates:
                    lookup_familiprix_product(c, cs)
                )
            tasks.append(lambda c=bc: lookup_barcodelookup(c))
            tasks.append(lambda c=bc: lookup_go_upc(c))
        if background:
            # Familiprix is the highest-value store-specific fallback. Avoid the
            # slower generic HTML scrapers during automatic maintenance.
            tasks = tasks[:1 if not skip_familiprix else 2]
        if tasks:
            p2, s2 = best_lookup_result(
                tasks, max_workers=_cap(8), good_enough=GOOD_ENOUGH,
                wait_for_cleanup=wait_for_cleanup,
                require_image=require_image,
            )
            best, best_score = _merge_candidate(best, best_score, p2, s2)

    # Phase 3 — pharmacy sites (Jean Coutu / Brunet / Pharmaprix), last resort.
    if not background and not _satisfactory(best, best_score):
        tasks = []
        for bc in candidates:
            for sn, su in PHARMACY_LOOKUP_SOURCES:
                tasks.append(lambda c=bc, n=sn, u=su, cs=candidates: lookup_generic_pharmacy_product(n, u, c, cs))
        p3, s3 = best_lookup_result(
            tasks, max_workers=_cap(6), good_enough=GOOD_ENOUGH,
            wait_for_cleanup=wait_for_cleanup,
            require_image=require_image,
        )
        best, best_score = _merge_candidate(best, best_score, p3, s3)

    # Phase 4 — AI web-grounded identification (opt-in via AI_DEEP_LOOKUP, off by default).
    if not background and not best:
        ai_found = ai_grounded_product_lookup(barcode)
        if ai_found:
            best = ai_found
    return best


def lookup_regulatory_product_online(barcode):
    """Inspect exact-UPC sources for explicitly labelled DIN/NPN/DIN-HM values.

    This deliberately waits for all three narrow sources so an early image/name
    result cannot hide a regulatory label returned by a slower source.
    """
    barcode = str(barcode or "").strip()
    if not barcode:
        return None
    candidates = build_barcode_candidates(barcode)
    fast_tasks = [
        lambda: lookup_familiprix_product(barcode, candidates),
        lambda: lookup_open_facts_product(
            "Open Drug Facts", "https://world.opendrugfacts.org", barcode
        ),
        lambda: lookup_open_facts_product(
            "Open Products Facts", "https://world.openproductsfacts.org", barcode
        ),
        lambda: lookup_upcitemdb(barcode),
    ]
    best, score = best_lookup_result(
        fast_tasks, max_workers=4, good_enough=None, wait_for_cleanup=True
    )
    if (best or {}).get("regulatory_identifiers"):
        return best

    # These exact-barcode pages are slower, so use them only when the structured
    # sources did not expose a labelled identifier.
    slower_tasks = [
        lambda: lookup_barcodelookup(barcode),
        lambda: lookup_go_upc(barcode),
    ]
    slower, slower_score = best_lookup_result(
        slower_tasks, max_workers=2, good_enough=None, wait_for_cleanup=True
    )
    best, _score = _prefer_lookup_result(
        best, score, slower, slower_score
    )
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
        if online and online_matches_catalog(
            product.get("name", ""), product.get("brand", ""), online,
            product.get("barcode", ""),
        ):
            assessment = assess_metadata_candidate(
                product, online, match_method="exact_gtin"
            )
            if assessment.auto_apply:
                for field in ("description", "brand", "image_url"):
                    if not str(product.get(field, "")).strip() and online.get(field):
                        product[field] = online[field]
            else:
                product["unverified_suggestion"] = {
                    field: online.get(field, "")
                    for field in ("name", "brand", "description", "image_url", "source", "source_url")
                    if str(online.get(field, "") or "").strip()
                }
                product["verification_status"] = "requires_review"
    elif online:
        product = dict(online)
        product["verification_status"] = "requires_review"
        product["unverified_suggestion"] = True

    if product:
        enrich_lookup_product_with_ai(product)
        return jsonify({"found": True, "product": product})
    return jsonify({"found": False, "error": "Aucun produit trouve"})


@ai_bp.route("/api/reference/count", methods=["GET"])
def reference_count_route():
    return jsonify({"count": reference_count()})


# ── Catalogue online-enrichment (fetch real descriptions + images, validated) ────
_CATALOG_ENRICH = {
    "running": False, "done": 0, "total": 0,
    "updated": 0, "linked": 0, "skipped": 0,
    "descriptions": 0, "images": 0, "paused": False,
}


_ENRICH_CHUNK = 1         # save each lookup immediately; no long invisible batch
_ENRICH_WORKERS = 1       # one source lookup at a time; employee requests stay fast
_ENRICH_LOOKUP_FANOUT = 2 # background online parsing while PDFs get priority
_AUTO_CATALOG_ENRICH = os.environ.get(
    "AUTO_CATALOG_ENRICH", "1"
).strip().lower() in {"1", "true", "yes", "on"}
_CATALOG_ENRICH_LOCK = threading.Lock()
_CATALOG_ENRICH_VERSION = "trusted_sources_v4"
_CATALOG_ENRICH_DONE_STATUSES = (
    _CATALOG_ENRICH_VERSION,
    "reviewed_online_v4",
    "provisional_v4",
    "no_match_v4",
)
_CATALOG_PROVISIONAL_PREFILL_STATUS = "provisional_prefill_v4"
_CATALOG_INCOMPLETE_SQL = (
    "(TRIM(COALESCE(description,''))='' "
    "OR LENGTH(TRIM(COALESCE(description,''))) < 55 "
    "OR TRIM(COALESCE(image_url,''))='' "
    f"OR enrich_status='{_CATALOG_PROVISIONAL_PREFILL_STATUS}')"
)


def structured_catalog_description(product):
    """Describe only facts already attached to this exact catalogue package.

    This is the final fallback after exact online sources miss. It deliberately
    does not infer purpose, ingredients, compatibility, or medical use.
    """
    item = dict(product or {})
    name = re.sub(r"\s+", " ", str(item.get("name", "") or "")).strip(" .")
    if len(name) < 3:
        return ""
    brand = re.sub(r"\s+", " ", str(item.get("brand", "") or "")).strip(" .")
    normalized_name = normalize_text(name)
    facts = []

    def add_fact(label, *fields):
        values = []
        for field in fields:
            value = re.sub(
                r"\s+", " ", str(item.get(field, "") or "")
            ).strip(" .")
            if (
                value and normalize_text(value) not in normalized_name
                and value not in values
            ):
                values.append(value)
        if values:
            facts.append(f"{label}: {' '.join(values)}")

    if brand and normalize_text(brand) not in normalized_name:
        facts.append(f"Marque: {brand}")
    add_fact("Format", "package_size", "package_unit")
    add_fact("Variante", "variant")
    add_fact("Saveur ou parfum", "flavour")
    add_fact("Couleur", "colour")
    add_fact("Concentration", "strength")
    add_fact("Forme", "dosage_form")
    add_fact("Fabricant", "manufacturer")
    add_fact("Catégorie", "category")
    add_fact("Compatibilité", "compatibility")
    if facts:
        return f"{name}. " + ". ".join(facts[:7]) + "."
    return (
        f"Produit du catalogue identifié sous le nom exact « {name} ». "
        "Les caractéristiques détaillées restent à confirmer sur l’emballage."
    )


def _apply_provisional_catalog_description(db, row, placed_products, now):
    """Publish a non-medical fallback while preserving its review status."""
    description = structured_catalog_description(row)
    if len(description) < 55:
        return False
    barcode = str(row.get("barcode", "") or "").strip()
    key = gtin_identity_key(barcode)
    if not barcode or not key:
        return False
    verified = db.execute(
        """SELECT 1 FROM product_reference_evidence
           WHERE gtin_key=? AND field_name='description' AND active=1
             AND verification_status='verified' LIMIT 1""",
        (key,),
    ).fetchone()
    if verified:
        return False
    current = str(row.get("description", "") or "").strip()
    if len(current) >= len(description):
        return False
    source = "Structured product identity fallback"
    record_reference_evidence(
        db, barcode, "description", description,
        source=source, source_record_id=(
            str(row.get("product_code", "") or "").strip() or barcode
        ),
        match_method="deterministic_catalog_fields",
        confidence=0.6, verification_status="requires_review",
        imported_at=now, active=False,
    )
    db.execute(
        """UPDATE product_reference
           SET description=?, updated_at=?
           WHERE barcode=? AND LENGTH(TRIM(COALESCE(description,''))) < 55""",
        (description, now, barcode),
    )
    reference = {
        **dict(row),
        "barcode": barcode,
        "description": description,
        "source": source,
        "source_url": "",
    }
    from routes.products import update_product_metadata_from_reference
    for product in placed_products.values():
        update_product_metadata_from_reference(
            db, product, reference, now=now, match_method="exact_gtin",
        )
    return True


def _placed_products_for_reference_rows(db, reference_rows):
    """Load only shelf products sharing an exact GTIN with this small batch."""
    placed_by_barcode = {}
    batch_keys = list(dict.fromkeys(
        gtin_identity_key(row.get("barcode", "")) for row in reference_rows
        if gtin_identity_key(row.get("barcode", ""))
    ))
    batch_barcodes = list(dict.fromkeys(
        str(row.get("barcode", "") or "").strip() for row in reference_rows
        if str(row.get("barcode", "") or "").strip()
    ))
    filters = []
    params = []
    if batch_keys:
        filters.append(
            "gtin_key IN (" + ",".join("?" for _ in batch_keys) + ")"
        )
        params.extend(batch_keys)
    if batch_barcodes:
        filters.append(
            "barcode IN (" + ",".join("?" for _ in batch_barcodes) + ")"
        )
        params.extend(batch_barcodes)
    if not filters:
        return placed_by_barcode
    query = (
        "SELECT id, name, barcode, brand, description, image_url, product_code "
        "FROM products WHERE " + " OR ".join(filters)
    )
    for product_row in db.execute(query, tuple(params)):
        product = dict(product_row)
        key = gtin_identity_key(product.get("barcode", ""))
        if key:
            placed_by_barcode.setdefault(key, {})[product["id"]] = product
    return placed_by_barcode


def _prefill_provisional_catalog_descriptions(db):
    """Fill sparse descriptions locally before slower exact-source lookups.

    The generated text contains only fields already attached to the same GTIN.
    It remains explicitly unverified and queued for exact-source replacement.
    """
    rows = [dict(row) for row in db.execute(
        """SELECT barcode, name, brand, description, image_url, product_code,
                  package_size, package_unit, variant, flavour, colour,
                  strength, dosage_form, manufacturer, category,
                  ingredients, compatibility, purpose, route_of_administration
           FROM product_reference
           WHERE LENGTH(TRIM(COALESCE(description,''))) < 55
             AND TRIM(COALESCE(name,'')) <> ''
             AND TRIM(COALESCE(barcode,'')) <> ''
           ORDER BY CASE WHEN EXISTS (
             SELECT 1 FROM products p
             WHERE p.gtin_key=product_reference.gtin_key
           ) THEN 0 ELSE 1 END, barcode"""
    )]
    updated = 0
    for start in range(0, len(rows), 100):
        batch = rows[start:start + 100]
        placed_by_barcode = _placed_products_for_reference_rows(db, batch)
        now = utc_now_iso()
        for row in batch:
            key = gtin_identity_key(row.get("barcode", ""))
            if not _apply_provisional_catalog_description(
                db, row, placed_by_barcode.get(key, {}), now,
            ):
                continue
            db.execute(
                """UPDATE product_reference
                   SET enrich_status=?, updated_at=?
                   WHERE barcode=?""",
                (
                    _CATALOG_PROVISIONAL_PREFILL_STATUS,
                    now,
                    str(row.get("barcode", "") or "").strip(),
                ),
            )
            updated += 1
        db.commit()
    return updated


def _enrich_marker_path():
    import tempfile
    return os.path.join(tempfile.gettempdir(), "familiprix-enrich.json")


def _write_enrich_marker():
    """Progress snapshot on disk: it survives a gunicorn worker recycle, so the
    status endpoint can detect a dead run (pid changed) and RESUME it alone."""
    try:
        with open(_enrich_marker_path(), "w", encoding="utf-8") as fh:
            json.dump({
                **_CATALOG_ENRICH,
                "pid": os.getpid(),
                "version": _CATALOG_ENRICH_VERSION,
            }, fh)
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
    from routes.products import (
        audit_product_data, build_barcode_candidates, normalized_digits,
        bump_reference_cache, invalidate_product_search_cache,
        update_product_metadata_from_reference,
    )
    def _lookup(r):
        try:
            with memory_intensive_task("catalog_enrichment"):
                barcode = r.get("barcode", "")
                online = lookup_familiprix_product(
                    barcode,
                    build_barcode_candidates(barcode),
                    product_code=r.get("product_code", ""),
                    direct_only=True,
                )
                if not online:
                    online = lookup_product_online(
                        barcode, max_workers=_ENRICH_LOOKUP_FANOUT,
                        wait_for_cleanup=True, background=True,
                        skip_familiprix=True,
                    )
                if not online:
                    # The direct product-code URL is fastest, but older catalogue
                    # rows occasionally use a code that no longer redirects.
                    # The slower Familiprix search remains exact because the
                    # resulting page must contain this UPC or product code.
                    online = lookup_familiprix_product(
                        barcode,
                        build_barcode_candidates(barcode),
                        product_code=r.get("product_code", ""),
                        search_only=True,
                    )
            return r, online
        except Exception:
            return r, None

    attempts_without_progress = 0
    prefill_attempted = False
    try:
        while attempts_without_progress < 5 and _CATALOG_ENRICH["running"]:
            db = None
            made_progress = False
            try:
                db = connect_db()
                if not prefill_attempted:
                    prefill_attempted = True
                    provisional_count = _prefill_provisional_catalog_descriptions(
                        db
                    )
                    if provisional_count:
                        _CATALOG_ENRICH["descriptions"] += provisional_count
                        bump_reference_cache()
                        invalidate_product_search_cache()
                        print(
                            "[Enrich] descriptions provisoires disponibles: "
                            f"{provisional_count}."
                        )
                status_placeholders = ",".join(
                    "?" for _ in _CATALOG_ENRICH_DONE_STATUSES
                )
                remaining_row = db.execute(
                    "SELECT COUNT(*) AS n FROM product_reference "
                    f"WHERE TRIM(COALESCE(enrich_status,'')) NOT IN ({status_placeholders}) "
                    f"AND {_CATALOG_INCOMPLETE_SQL} "
                    "AND TRIM(COALESCE(name,'')) <> '' "
                    "AND TRIM(COALESCE(barcode,'')) <> ''",
                    _CATALOG_ENRICH_DONE_STATUSES,
                ).fetchone()
                remaining = int(
                    remaining_row["n"] if isinstance(remaining_row, dict)
                    else remaining_row[0]
                )
                rows = [dict(r) for r in db.execute(
                    "SELECT barcode, name, brand, description, image_url, product_code "
                    ", enrich_status "
                    ", package_size, package_unit, variant, flavour, colour "
                    ", strength, dosage_form, manufacturer, category "
                    ", ingredients, compatibility, purpose, route_of_administration "
                    "FROM product_reference "
                    f"WHERE TRIM(COALESCE(enrich_status,'')) NOT IN ({status_placeholders}) "
                    f"AND {_CATALOG_INCOMPLETE_SQL} "
                    "AND TRIM(COALESCE(name,'')) <> '' "
                    "AND TRIM(COALESCE(barcode,'')) <> '' "
                    "ORDER BY CASE WHEN EXISTS ("
                    "SELECT 1 FROM products p WHERE p.gtin_key=product_reference.gtin_key"
                    ") THEN 0 ELSE 1 END, "
                    "CASE WHEN TRIM(COALESCE(description,''))='' THEN 0 ELSE 1 END, "
                    "barcode LIMIT 100",
                    _CATALOG_ENRICH_DONE_STATUSES,
                ).fetchall()]
                # Do not hold a database connection while external catalogues
                # respond. Search and plan edits remain independent of a slow
                # product website.
                db.close()
                db = None
                if not rows:
                    break                      # everything processed — real Terminé
                _CATALOG_ENRICH["total"] = _CATALOG_ENRICH["done"] + remaining
                _CATALOG_ENRICH.pop("error", None)
                _write_enrich_marker()
                with ThreadPoolExecutor(max_workers=_ENRICH_WORKERS) as pool:
                    for i in range(0, len(rows), _ENRICH_CHUNK):
                        if not _CATALOG_ENRICH["running"]:
                            return             # deliberate stop — finally cleans up
                        lookup_results = list(
                            pool.map(_lookup, rows[i:i + _ENRICH_CHUNK])
                        )
                        if not _CATALOG_ENRICH["running"]:
                            return
                        db = connect_db()
                        placed_by_barcode = _placed_products_for_reference_rows(
                            db, [r for r, _online in lookup_results]
                        )
                        for r, online in lookup_results:
                            bc = r.get("barcode", "")
                            # Poison-row immunity: a malformed online payload tags the
                            # row as no_match and moves on — it must never kill the run.
                            # (DB errors below still bubble up to the reconnect loop.)
                            desc = img = brand = ""
                            matched = False
                            try:
                                if online and online_matches_catalog(
                                    r.get("name", ""), r.get("brand", ""), online,
                                    r.get("barcode", ""),
                                ):
                                    desc = str(online.get("description", "")).strip()
                                    img = str(online.get("image_url", "")).strip()
                                    brand = str(online.get("brand", "")).strip()
                                    matched = True
                            except Exception:
                                matched = False
                            if matched:
                                updated_at = utc_now_iso()
                                reference = dict(online or {})
                                exact_familiprix = (
                                    str(reference.get("source", "") or "").strip().lower()
                                    == "familiprix"
                                )
                                reference.update({
                                    "barcode": bc,
                                    "brand": (
                                        brand if exact_familiprix
                                        else str(r.get("brand", "") or "").strip() or brand
                                    ),
                                    "description": desc,
                                    "image_url": (
                                        img if exact_familiprix
                                        else str(r.get("image_url", "") or "").strip() or img
                                    ),
                                    "product_code": str(r.get("product_code", "") or "").strip(),
                                })
                                upsert_reference_candidate(
                                    db, reference, imported_at=updated_at,
                                    promote_higher_priority=exact_familiprix,
                                )
                                _store_reference_regulatory_candidates(
                                    db, reference, updated_at,
                                )
                                db.execute(
                                    "UPDATE product_reference SET enrich_status=?, updated_at=? WHERE barcode=?",
                                    (
                                        _CATALOG_ENRICH_VERSION
                                        if exact_familiprix else "reviewed_online_v4",
                                        updated_at, bc,
                                    ),
                                )
                                key = gtin_identity_key(bc)
                                affected_product_ids = []
                                for product in placed_by_barcode.get(key, {}).values():
                                    update_product_metadata_from_reference(
                                        db, product, reference, now=updated_at,
                                        match_method="exact_gtin",
                                        promote_higher_priority=exact_familiprix,
                                    )
                                    affected_product_ids.append(int(product["id"]))
                                    _CATALOG_ENRICH["linked"] += 1
                                if affected_product_ids:
                                    audit_product_data(
                                        db, affected_product_ids,
                                        trigger_type="catalog_enrichment",
                                        employee="system", now=updated_at,
                                    )
                                provisional_added = False
                                if len(desc) < 55:
                                    provisional_added = (
                                        _apply_provisional_catalog_description(
                                            db, r,
                                            placed_by_barcode.get(key, {}),
                                            updated_at,
                                        )
                                    )
                                if (
                                    not desc
                                    and (
                                        provisional_added
                                        or r.get("enrich_status")
                                        == _CATALOG_PROVISIONAL_PREFILL_STATUS
                                    )
                                ):
                                    db.execute(
                                        """UPDATE product_reference
                                           SET enrich_status=?
                                           WHERE barcode=?""",
                                        ("provisional_v4", bc),
                                    )
                                db.commit()
                                _CATALOG_ENRICH["updated"] += 1
                                if desc and desc != str(r.get("description", "") or "").strip():
                                    _CATALOG_ENRICH["descriptions"] += 1
                                elif provisional_added:
                                    _CATALOG_ENRICH["descriptions"] += 1
                                if img and img != str(r.get("image_url", "") or "").strip():
                                    _CATALOG_ENRICH["images"] += 1
                            else:
                                updated_at = utc_now_iso()
                                key = gtin_identity_key(bc)
                                provisional_added = (
                                    _apply_provisional_catalog_description(
                                        db, r,
                                        placed_by_barcode.get(key, {}),
                                        updated_at,
                                    )
                                )
                                provisional_available = bool(
                                    provisional_added
                                    or r.get("enrich_status")
                                    == _CATALOG_PROVISIONAL_PREFILL_STATUS
                                )
                                db.execute(
                                    """UPDATE product_reference
                                       SET enrich_status=?, updated_at=?
                                       WHERE barcode=?""",
                                    (
                                        "provisional_v4"
                                        if provisional_available else "no_match_v4",
                                        updated_at, bc,
                                    ),
                                )
                                db.commit()
                                if provisional_added:
                                    _CATALOG_ENRICH["descriptions"] += 1
                                _CATALOG_ENRICH["skipped"] += 1
                            _CATALOG_ENRICH["done"] += 1
                            made_progress = True
                            attempts_without_progress = 0
                        db.close()
                        db = None
                        lookup_results.clear()
                        _write_enrich_marker()   # once per batch — the resume checkpoint
                        # Free the batch's parsed online payloads NOW (some sources
                        # return hundreds of KB per product) — RSS creep OOM'd us once.
                        release_unused_memory()
                # Publish one bounded page at a time. Rebuilding the 9k-row
                # search corpus after every product caused the old Render OOM;
                # waiting for the entire multi-hour pass left AI context stale.
                bump_reference_cache()
                invalidate_product_search_cache()
                # Continue with the next bounded database page. Keeping only
                # 100 references and their matching shelf rows in memory avoids
                # retaining a second full catalogue throughout enrichment.
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


def _catalog_pending_count(db):
    placeholders = ",".join("?" for _ in _CATALOG_ENRICH_DONE_STATUSES)
    row = db.execute(
        "SELECT COUNT(*) AS n FROM product_reference "
        f"WHERE TRIM(COALESCE(enrich_status,'')) NOT IN ({placeholders}) "
        f"AND {_CATALOG_INCOMPLETE_SQL} "
        "AND TRIM(COALESCE(name,'')) <> '' "
        "AND TRIM(COALESCE(barcode,'')) <> ''",
        _CATALOG_ENRICH_DONE_STATUSES,
    ).fetchone()
    return int((row["n"] if isinstance(row, dict) else row[0]) or 0)


def _catalog_description_coverage(db):
    row = db.execute(
        """SELECT
             COUNT(*) AS total,
             SUM(CASE WHEN TRIM(COALESCE(description,''))='' THEN 1 ELSE 0 END) AS missing,
             SUM(CASE WHEN LENGTH(TRIM(COALESCE(description,''))) BETWEEN 1 AND 54
                      THEN 1 ELSE 0 END) AS thin,
             SUM(CASE WHEN LENGTH(TRIM(COALESCE(description,''))) >= 55
                      THEN 1 ELSE 0 END) AS usable,
             SUM(CASE WHEN LOWER(COALESCE(source,'')) LIKE ?
                           AND TRIM(COALESCE(description,''))<>''
                      THEN 1 ELSE 0 END) AS exact_familiprix
             , SUM(CASE WHEN enrich_status IN (
                            'provisional_v4', 'provisional_prefill_v4'
                        )
                        THEN 1 ELSE 0 END) AS provisional
           FROM product_reference
           WHERE TRIM(COALESCE(name,'')) <> ''""",
        ("%familiprix%",),
    ).fetchone()
    values = dict(row) if row else {}
    return {
        key: int(values.get(key) or 0)
        for key in (
            "total", "missing", "thin", "usable", "exact_familiprix",
            "provisional",
        )
    }


def schedule_catalog_enrichment(*, force=False):
    """Start one durable, low-memory catalogue pass when rows need this version."""
    from database import connect_db

    if not force and os.environ.get("PYTEST_CURRENT_TEST"):
        return False
    with _CATALOG_ENRICH_LOCK:
        if _CATALOG_ENRICH["running"]:
            return False
        marker = _read_enrich_marker() or {}
        marker_is_current = (
            marker.get("version") == _CATALOG_ENRICH_VERSION
        )
        paused = bool(
            _CATALOG_ENRICH.get("paused")
            or (marker_is_current and marker.get("paused"))
        )
        if paused and not force:
            _CATALOG_ENRICH["paused"] = True
            return False
        if not force and not _AUTO_CATALOG_ENRICH:
            return False

        db = None
        try:
            db = connect_db()
            total = _catalog_pending_count(db)
        except Exception:
            return False
        finally:
            if db is not None:
                try:
                    db.close()
                except Exception:
                    pass
        if total <= 0:
            _CATALOG_ENRICH.update(
                running=False, total=0, done=0, paused=False,
            )
            _write_enrich_marker()
            return False

        _CATALOG_ENRICH.update(
            running=True, done=0, updated=0, linked=0, skipped=0,
            descriptions=0, images=0, paused=False,
            total=total, started_at=time.time(),
        )
        _CATALOG_ENRICH.pop("error", None)
        _write_enrich_marker()
        threading.Thread(
            target=_catalog_enrich_worker, daemon=True,
            name="catalog-description-enrichment",
        ).start()
        return True


@ai_bp.route("/api/import/catalog-enrich/start", methods=["POST"])
def catalog_enrich_start():
    username, error = require_editor()
    if error:
        return error
    if _CATALOG_ENRICH["running"]:
        return jsonify({"success": True, "already_running": True, **_CATALOG_ENRICH})
    started = schedule_catalog_enrichment(force=True)
    return jsonify({
        "success": True, "started": started,
        "total": _CATALOG_ENRICH["total"],
    })


def maybe_resume_enrichment():
    """Resume an interrupted pass or automatically start the current version."""
    try:
        if _CATALOG_ENRICH["running"]:
            return False
        marker = _read_enrich_marker()
        marker_is_current = bool(
            marker and marker.get("version") == _CATALOG_ENRICH_VERSION
        )
        if marker_is_current and marker.get("paused"):
            _CATALOG_ENRICH["paused"] = True
            return False
        return schedule_catalog_enrichment(
            force=bool(marker_is_current and marker.get("running"))
        )
    except Exception:
        return False


@ai_bp.route("/api/import/catalog-enrich/status", methods=["GET"])
def catalog_enrich_status():
    resumed = False
    if not current_app.config.get("DB_BOOT_PENDING", False):
        resumed = maybe_resume_enrichment()
    state = dict(_CATALOG_ENRICH)
    if resumed:
        state["resumed"] = True
    try:
        state["coverage"] = _catalog_description_coverage(get_db())
    except Exception:
        state["coverage"] = {}
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
    _CATALOG_ENRICH["paused"] = True
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

    def safe_csv_cell(value):
        text = str(value or "")
        # Spreadsheet tools may execute cells beginning with these characters.
        return "'" + text if text.startswith(("=", "+", "-", "@", "\t", "\r")) else text

    for r in rows:
        d = dict(r)
        status = (
            "aucune correspondance en ligne"
            if str(d.get("enrich_status", "")).startswith("no_match")
            else "pas encore tenté"
        )
        writer.writerow([safe_csv_cell(d.get("barcode", "")), safe_csv_cell(d.get("product_code", "")),
                         safe_csv_cell(d.get("name", "")),
                         safe_csv_cell(str(d.get("source", "")).replace("Planogramme: ", "")), status])
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
        "SELECT COUNT(*) AS n FROM product_reference "
        "WHERE enrich_status LIKE ?",
        ("no_match%",),
    ).fetchone()
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
        # AI may propose searchable wording, but it is never allowed to become
        # the product description or a verified package attribute automatically.
        product["ai_suggestion"] = assist
        product["ai_enriched"] = False
        product["verification_status"] = "requires_review"
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
    if any(len(value) > limit for value, limit in (
        (name, 300), (brand, 160), (description, 6000), (barcode, 64)
    )):
        return jsonify({"success": False, "error": "Informations produit trop longues."}), 400
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
    started_at = time.perf_counter()
    diagnostic_stages = {}

    def mark_stage(name):
        diagnostic_stages[name] = int(
            (time.perf_counter() - started_at) * 1000
        )

    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({"success": False, "error": "Une demande JSON valide est requise."}), 400
    question = str(data.get("question", "")).strip()
    history = normalize_client_history(data.get("history"))
    follow_up = bool(data.get("follow_up", False))
    selected_text = str(data.get("selected_text", "") or "").strip()[:500]
    focus_product_id = str(data.get("focus_product_id", "") or "").strip()[:100]
    requested_mode = str(data.get("mode", "auto") or "auto").strip().lower()
    context_product_ids = []
    for raw_id in data.get("context_product_ids", []) if isinstance(data.get("context_product_ids"), list) else []:
        candidate_id = str(raw_id or "").strip()[:100]
        if candidate_id and candidate_id not in context_product_ids:
            context_product_ids.append(candidate_id)
        if len(context_product_ids) >= 80:
            break
    if not question:
        return jsonify({"success": False, "error": "Question client requise."}), 400
    if len(question) > 2000:
        return jsonify({"success": False, "error": "La question est trop longue."}), 400

    global _AI_LAST_ERROR, _AI_LAST_MODEL
    _AI_LAST_ERROR = ""
    _AI_LAST_MODEL = ""
    if requested_mode == "documented":
        response_mode = "documented"
    elif requested_mode == "ai":
        response_mode = "detailed"
    elif requested_mode == "fast":
        response_mode = "lookup"
    else:
        response_mode = classify_client_request(
            question, follow_up=follow_up, focus_product_id=focus_product_id,
            selected_text=selected_text,
        )
    query_plan = build_client_query_plan(question, response_mode)
    if response_mode != "lookup":
        if not configured_ai_provider()["name"]:
            return jsonify({
                "success": False,
                "error": "Aucune clé IA n’est configurée sur le serveur.",
            }), 503

        if not _check_ai_rate_limit():
            return jsonify({
                "success": False,
                "error": "Trop de requêtes IA. Réessayez dans une heure.",
            }), 429

    # Retrieval is immediate and inventory-safe: only mapped store-plan products
    # can become cards. A direct reply stays inside the products from that thread.
    from routes.products import (
        classify_client_result_roles, client_products_by_ids,
        client_candidates_need_semantic_retry,
        client_candidates_satisfy_query_plan, hybrid_client_candidates,
        hydrate_candidate_images,
        normalize_search_text, public_product_payload,
    )
    context_products = client_products_by_ids(context_product_ids, limit=80)

    def retrieve_with_plan(active_plan):
        candidate_limit = 100 if active_plan.get("wants_all") else 60
        retrieval_scope = str(
            active_plan.get("retrieval_scope", "store") or "store"
        )
        if follow_up and context_products and retrieval_scope == "context":
            return list(context_products)
        retrieval_question = str(
            active_plan.get("corrected_query", "") or question
        ).strip()
        store_candidates = hybrid_client_candidates(
            retrieval_question, active_plan, limit=candidate_limit
        )
        if follow_up and context_products and retrieval_scope == "context_and_store":
            context_ids = {
                str(product.get("client_id", "") or "")
                for product in context_products
            }
            return [
                *context_products,
                *[
                    product for product in store_candidates
                    if str(product.get("client_id", "") or "") not in context_ids
                ],
            ][:candidate_limit]
        return store_candidates

    candidates = retrieve_with_plan(query_plan)
    semantic_planner_used = False
    if response_mode != "lookup":
        preliminary_candidates = filter_client_answer_category(
            question, candidates
        )
        needs_semantic_plan = bool(
            follow_up
            or selected_text
            or (
                not focus_product_id
                and client_candidates_need_semantic_retry(
                    question, preliminary_candidates
                )
            )
        )
        if needs_semantic_plan:
            semantic_plan = generate_client_query_plan(question, history)
            if isinstance(semantic_plan, dict):
                query_plan = semantic_plan
                candidates = retrieve_with_plan(query_plan)
                semantic_planner_used = True
    mark_stage("query_plan_ready_ms")
    if (
        response_mode == "lookup"
        and client_candidates_need_semantic_retry(question, candidates)
    ):
        candidates = []
    if response_mode != "lookup":
        candidates = filter_client_answer_category(question, candidates)
        if query_plan.get("must_include") and not client_candidates_satisfy_query_plan(
            query_plan, candidates,
        ):
            # The AI can still answer the general question. A blank product list
            # is safer than cards that miss an indispensable object or use.
            candidates = []
    candidates = classify_client_result_roles(candidates, question)
    # Reuse known exact-UPC images now. Unknown-image web lookups are queued
    # only for cards that will actually be displayed.
    hydrate_candidate_images(candidates, queue_missing=False)
    mark_stage("retrieval_ready_ms")

    if response_mode == "lookup":
        hydrate_candidate_images(candidates, queue_missing=True, queue_limit=12)
        elapsed_ms = int((time.perf_counter() - started_at) * 1000)
        return jsonify({
            "success": True,
            "response_mode": "lookup",
            "answer": "",
            "products": candidates,
            "highlighted_product_ids": [],
            "query_plan": query_plan,
            "advice": {
                "summary": "",
                "recommended_product_names": [],
                "recommended_products": [],
                "follow_up_questions": [],
                "safety_flags": [],
                "pharmacist_referral": False,
                "pharmacist_reason": "",
            },
            "elapsed_ms": elapsed_ms,
        })

    query_plan["context_product_ids"] = context_product_ids
    answer_candidates = list(candidates)
    if focus_product_id:
        answer_candidates.sort(
            key=lambda product: 0 if str(product.get("client_id", "")) == focus_product_id else 1
        )
    # A smaller grounded context improves response time and keeps comparisons readable.
    answer_limit = 12 if query_plan.get("wants_all") else 8
    answer_candidates = select_client_answer_candidates(
        answer_candidates,
        limit=answer_limit,
        diversify_brands=bool(query_plan.get("medical")),
        question=" ".join(filter(None, [
            question,
            str(query_plan.get("corrected_query", "") or ""),
            " ".join(query_plan.get("must_include") or []),
        ])),
    )
    documents = []
    if response_mode == "documented":
        documents = retrieve_client_documentation(
            answer_candidates,
            query_plan,
            # Product regulatory facts are synchronized in the background and
            # stored as evidence. Cold Health Canada calls on the employee's
            # request path added several seconds and could outlive the AI timeout.
            include_live_regulatory=False,
        )
        mark_stage("documentation_ready_ms")
        verified = generate_documented_client_answer(
            question, query_plan, answer_candidates, documents, history,
            selected_text=selected_text, focus_product_id=focus_product_id,
        )
    else:
        verified = generate_verified_client_answer(
            question, query_plan, answer_candidates, history,
            selected_text=selected_text, focus_product_id=focus_product_id,
        )
    mark_stage("answer_ready_ms")
    if not isinstance(verified, dict) and response_mode != "documented":
        # A provider timeout must not break the employee workflow. The fallback
        # remains limited to the already-filtered products in the current store.
        verified = grounded_documented_fallback(
            query_plan, answer_candidates, [], degraded=True,
        )
    if not isinstance(verified, dict):
        if response_mode == "documented":
            verified = grounded_documented_fallback(
                query_plan, answer_candidates, documents, degraded=True,
            )
        else:
            return jsonify({"success": False,
                            "error": _AI_LAST_ERROR or "Impossible de préparer la réponse pour le moment."}), 502

    if (
        response_mode != "documented"
        and answer_candidates
        and not verified.get("selected_product_ids")
        and (
            _AI_LAST_ERROR
            or not str(verified.get("answer", "") or "").strip()
            or _answer_denies_available_inventory(verified.get("answer", ""))
        )
    ):
        # A timed-out structured stream can still normalize into an empty dict.
        # Never turn that transport artifact into a false "nothing in store"
        # answer when deterministic retrieval has grounded candidates.
        verified = grounded_documented_fallback(
            query_plan, answer_candidates, [], degraded=True,
        )

    if response_mode != "documented":
        verified = _guard_documented_inventory_contradiction(
            verified, query_plan, answer_candidates, documents,
        )

    degraded = bool(verified.get("degraded", False))
    warning = str(verified.get("warning", "") or "").strip()
    by_id = {str(product.get("client_id", "")): product for product in answer_candidates}
    highlighted_products = [
        by_id[candidate_id] for candidate_id in verified["selected_product_ids"]
        if candidate_id in by_id
    ]
    highlighted_product_ids = [
        str(product.get("client_id", "") or "")
        for product in highlighted_products
        if product.get("client_id")
    ]
    hydrate_candidate_images(
        highlighted_products, queue_missing=True, queue_limit=12
    )
    mark_stage("answer_images_ready_ms")
    answer = verified["answer"] or (
        "Aucun produit suffisamment lié à cette demande n'a été trouvé dans la base."
    )
    identifier_notice = _unconfirmed_identifier_notice(
        question, answer, highlighted_products
    )
    if identifier_notice and not re.search(
        r"\bconfirm", answer, flags=re.IGNORECASE
    ):
        answer = f"{answer.rstrip()} {identifier_notice}".strip()
    safety_flags = list(verified.get("safety_flags") or [])
    if identifier_notice:
        safety_flags = [
            identifier_notice,
            *[flag for flag in safety_flags if flag != identifier_notice],
        ][:5]
    recommended_products = [{
        "candidate_id": product.get("client_id", ""),
        "name": str(product.get("name", "")).strip(),
        "brand": str(product.get("brand", "")).strip(),
        "location": _recommendation_location(product),
        "barcode": str(product.get("barcode", "")).strip(),
        "home_brand": _is_home_brand(product.get("brand", "")),
    } for product in highlighted_products]
    advice = {
        "summary": answer,
        "recommended_product_names": [product["name"] for product in recommended_products],
        "recommended_products": recommended_products,
        "follow_up_questions": verified["follow_up_questions"],
        "safety_flags": safety_flags,
        "pharmacist_referral": verified["pharmacist_referral"],
        "pharmacist_reason": verified["pharmacist_reason"],
    }
    if response_mode == "documented":
        if verified.get("_ai_pending"):
            source_documents = documents
        else:
            used_source_ids = set(verified.get("source_ids", []))
            source_documents = [
                document for document in documents
                if document.get("source_id") == "store-plan"
                or document.get("source_id") in used_source_ids
            ]
            if len(source_documents) == 1 and len(documents) > 1:
                source_documents = documents
        advice["documentation"] = {
            "key_points": verified.get("key_points", []),
            "comparisons": verified.get("comparisons", []),
            "useful_guidance": verified.get("useful_guidance", []),
            "important_checks": verified.get("important_checks", []),
            "sources": [{
                "source_id": str(document.get("source_id", "") or ""),
                "title": str(document.get("title", "") or "")[:240],
                "publisher": str(document.get("publisher", "") or "")[:120],
                "url": str(document.get("url", "") or "")[:1600],
                "summary": str(document.get("evidence", "") or "")[:900],
                "candidate_ids": document.get("candidate_ids", [])[:16],
                "source_class": str(
                    document.get("source_class", "") or ""
                )[:60],
                "trust_level": int(document.get("trust_level", 0) or 0),
                "verification_status": str(
                    document.get("verification_status", "") or ""
                )[:60],
            } for document in source_documents[:15]],
        }
    log_ai_interaction(
        (
            "client_documented_local" if verified.get("local_summary")
            else "client_documented_rag"
        ) if response_mode == "documented" else "client_rag",
        question,
        {
            "history": history,
            "selected_text": selected_text,
            "focus_product_id": focus_product_id,
            "query_plan": query_plan,
            "retrieved": [
                product_context_for_client_rag(product) for product in answer_candidates
            ],
            "documentation_sources": [{
                "source_id": document.get("source_id", ""),
                "title": document.get("title", ""),
                "url": document.get("url", ""),
            } for document in documents],
        },
        advice,
    )
    mark_stage("response_ready_ms")
    elapsed_ms = int((time.perf_counter() - started_at) * 1000)
    public_products = [
        public_product_payload(product) for product in highlighted_products
    ]
    response_payload = {
        "success": True,
        "response_mode": response_mode,
        "answer": answer,
        "products": public_products,
        "highlighted_product_ids": highlighted_product_ids,
        "query_plan": query_plan,
        "advice": advice,
        "elapsed_ms": elapsed_ms,
        "degraded": degraded,
        "warning": warning,
    }
    if response_mode == "documented":
        response_payload["ai_pending"] = bool(
            verified.get("_ai_pending", False)
        )
        response_payload["documented_job_id"] = str(
            verified.get("_documented_job_id", "") or ""
        )
    record_ai_answer(
        response_mode, elapsed_ms,
        degraded=(
            degraded and not verified.get("_ai_pending", False)
        ),
        cache_hit=bool(verified.get("_cache_hit", False)),
        pending=bool(verified.get("_ai_pending", False)),
        model=_AI_LAST_MODEL,
        product_count=len(highlighted_products),
    )
    if request.headers.get("X-AI-Diagnostics", "") == "1":
        response_payload["ai_diagnostics"] = {
            "model": _AI_LAST_MODEL,
            "error": _AI_LAST_ERROR,
            "stages": diagnostic_stages,
            "semantic_planner_used": semantic_planner_used,
            "documented_cache_hit": bool(verified.get("_cache_hit", False)),
        }
    return jsonify(response_payload)


@ai_bp.route("/api/client/help/documented/<job_id>", methods=["GET"])
def documented_client_help_status(job_id):
    """Return a completed background K3 upgrade without rerunning retrieval."""
    job_id = str(job_id or "").strip()
    if not re.fullmatch(r"[A-Za-z0-9_-]{20,64}", job_id):
        return jsonify({
            "success": False, "error": "Réponse documentée invalide.",
        }), 400
    entry = _documented_job_status(job_id)
    if entry is None:
        return jsonify({
            "success": False,
            "ready": False,
            "error": "Cette réponse documentée n'est plus disponible.",
        }), 404
    status = str(entry.get("status", "") or "")
    if status == "pending":
        return jsonify({
            "success": True, "ready": False, "failed": False,
        }), 202
    if status != "ready" or not entry.get("result"):
        return jsonify({
            "success": True, "ready": False, "failed": True,
        })
    return jsonify({
        "success": True,
        "ready": True,
        "failed": False,
        "elapsed_ms": int(entry.get("elapsed_ms", 0) or 0),
        "result": entry["result"],
    })


@ai_bp.route("/api/ai/feedback", methods=["POST"])
def ai_feedback():
    """Optional, non-blocking thumbs feedback on an AI answer. Stored as its own
    training row (kind='feedback') so we never need to mutate an existing log."""
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({"success": False}), 400
    question = str(data.get("question", "")).strip()
    rating = str(data.get("rating", "")).strip()  # 'up' | 'down'
    if rating not in ("up", "down"):
        return jsonify({"success": False}), 400
    question = question[:2000]
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
