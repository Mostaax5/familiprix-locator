import os
import re
import time
import traceback
import codecs
import secrets
from urllib.parse import urlsplit
from flask import Flask, render_template, send_from_directory, jsonify, request, g
from werkzeug.exceptions import HTTPException
from werkzeug.middleware.proxy_fix import ProxyFix
from database import (
    close_db, ensure_product_data_ready, get_backend_summary, get_db, init_db,
    product_data_schema_status,
)
from auth import utc_now_iso
from security import auth_bp, install_security, internal_request_headers
from routes.products import (
    products_bp, first_column, schedule_backfill_missing,
    schedule_initial_product_quality_audit, schedule_reference_metadata_sync,
)
from routes.layout import layout_bp
from routes.ai import ai_bp, configured_ai_provider, reference_count, maybe_resume_enrichment
from routes.gist import gist_bp, _restore_from_gist_if_empty
from routes.import_export import import_export_bp
from routes.regulatory import (
    maybe_resume_regulatory_enrichment, regulatory_bp,
    schedule_regulatory_enrichment_after,
)


def _preload_idna_codec():
    """Make hostname encoding available before Gunicorn starts request threads."""
    try:
        import encodings.idna  # noqa: F401
        codecs.lookup("idna")
        return
    except (ImportError, LookupError):
        # A damaged/minimal Python runtime can omit the stdlib codec. The small
        # PyPI idna fallback keeps every Flask route from failing before dispatch.
        import idna

        def encode(value, errors="strict"):
            return idna.encode(value, uts46=True), len(value)

        def decode(value, errors="strict"):
            raw = bytes(value)
            return idna.decode(raw, uts46=True), len(raw)

        def search(name):
            if name.replace("_", "-") != "idna":
                return None
            return codecs.CodecInfo(name="idna", encode=encode, decode=decode)

        codecs.register(search)
        codecs.lookup("idna")


_preload_idna_codec()

app = Flask(__name__)


def _bounded_env_int(name, default, minimum, maximum):
    try:
        return min(max(int(os.environ.get(name, default)), minimum), maximum)
    except (TypeError, ValueError):
        return default


app.config.update(
    MAX_CONTENT_LENGTH=_bounded_env_int("MAX_UPLOAD_MB", 32, 5, 64) * 1024 * 1024,
    MAX_FORM_MEMORY_SIZE=2 * 1024 * 1024,
    MAX_FORM_PARTS=30,
)

_RENDER_EXTERNAL_URL = os.environ.get("RENDER_EXTERNAL_URL", "").strip()
if _RENDER_EXTERNAL_URL:
    # Render terminates TLS one trusted proxy hop in front of Gunicorn.
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1)
    render_host = urlsplit(_RENDER_EXTERNAL_URL).hostname
    trusted = [host for host in (render_host, "localhost", "127.0.0.1", "[::1]") if host]
    extra_hosts = [host.strip() for host in os.environ.get("TRUSTED_HOSTS", "").split(",") if host.strip()]
    app.config["TRUSTED_HOSTS"] = list(dict.fromkeys(trusted + extra_hosts))


@app.before_request
def _assign_request_id():
    supplied = (request.headers.get("X-Request-ID") or "").strip()
    g.request_id = supplied if re.fullmatch(r"[A-Za-z0-9._-]{8,64}", supplied) else secrets.token_hex(8)


install_security(app)
try:
    # Gzip every JSON/HTML/JS response: /api/products alone is ~1 MB uncompressed,
    # which dominated the app's load time on store phones. Optional so a missing
    # package never prevents boot (dev environments, partial installs).
    from flask_compress import Compress
    Compress(app)
except ImportError:
    pass

# If the database is unreachable at boot (expired/suspended Render Postgres, DNS
# outage…), the app MUST still start: crash-looping here hid the real problem
# behind a dead site and blocked recovery. The failure is remembered and exposed
# by /api/system/info so the phones can say WHY there is no data.
DB_BOOT_ERROR = ""
_ASYNC_RENDER_BOOT = bool(
    os.environ.get("RENDER_EXTERNAL_URL", "").strip()
    or os.environ.get("RENDER", "").strip()
)
DB_BOOT_PENDING = _ASYNC_RENDER_BOOT

# Keep local setup deterministic. On Render the same work runs below after the
# app object is ready, so a busy PostgreSQL lock cannot block /healthz.
if not _ASYNC_RENDER_BOOT:
    try:
        init_db()
    except Exception as exc:  # noqa: BLE001 — any DB failure must not kill the boot
        DB_BOOT_ERROR = str(exc)
        print(f"[BOOT] Base de données injoignable au démarrage: {exc}")

app.register_blueprint(products_bp)
app.register_blueprint(layout_bp)
app.register_blueprint(ai_bp)
app.register_blueprint(gist_bp)
app.register_blueprint(import_export_bp)
app.register_blueprint(regulatory_bp)
app.register_blueprint(auth_bp)


_PRODUCT_DATA_BLUEPRINTS = {
    "products", "ai", "gist", "import_export", "regulatory",
}


@app.before_request
def _ensure_product_schema_before_catalogue_request():
    if request.blueprint not in _PRODUCT_DATA_BLUEPRINTS:
        return None
    try:
        ensure_product_data_ready(get_db())
    except Exception:
        app.logger.exception("Product-data schema is not ready")
        return jsonify({
            "success": False,
            "error": "Le catalogue se prépare. Réessayez dans un instant.",
            "code": "catalogue_initializing",
        }), 503
    return None


def _asset_version():
    """Stable per deployment so cached HTML only loads matching JS and CSS."""
    render_commit = os.environ.get("RENDER_GIT_COMMIT", "").strip()
    if render_commit:
        return render_commit[:12]
    tracked_assets = (
        "templates/index.html", "static/style.css", "static/scanner.js",
        "static/api.js", "static/config.js", "static/store.js", "static/lock.js",
        "static/search.js", "static/gist-ui.js", "static/ai-ui.js",
        "static/scan-ui.js", "static/layout-ui.js", "static/main.js",
        "static/vendor/zxing-library-0.21.3.min.js",
        "static/service-worker.js", "static/manifest.json", "static/icon.svg",
    )
    root = os.path.dirname(__file__)
    newest = max(os.path.getmtime(os.path.join(root, path)) for path in tracked_assets)
    return str(int(newest))


ASSET_VERSION = _asset_version()


# ── Core routes ────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html", asset_version=ASSET_VERSION)


@app.route("/healthz")
def healthz():
    return jsonify({"ok": True})


@app.route("/manifest.json")
def manifest():
    return send_from_directory("static", "manifest.json")


@app.route("/service-worker.js")
def service_worker():
    return send_from_directory("static", "service-worker.js")


@app.teardown_appcontext
def teardown_database(_error):
    close_db(_error)


@app.errorhandler(Exception)
def handle_any_error(exc):
    """API callers always get JSON (the frontend parses every response as JSON —
    an HTML error page used to break it silently), and unexpected errors land in
    the Render logs with a full traceback instead of vanishing."""
    if isinstance(exc, HTTPException):
        if request.path.startswith("/api/"):
            if exc.code == 413:
                return jsonify({
                    "success": False,
                    "error": "Fichier ou demande trop volumineuse.",
                    "code": "request_too_large",
                }), 413
            return jsonify({
                "success": False,
                "error": exc.description,
                "code": f"http_{exc.code}",
            }), exc.code
        return exc
    request_id = getattr(g, "request_id", secrets.token_hex(8))
    print(f"[ERROR] request_id={request_id} path={request.path}")
    traceback.print_exc()
    if request.path.startswith("/api/"):
        return jsonify({
            "success": False,
            "error": "Erreur interne du serveur.",
            "code": "internal_error",
            "request_id": request_id,
        }), 500
    return "Erreur interne du serveur.", 500


@app.after_request
def add_security_headers(response):
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("X-XSS-Protection", "0")
    response.headers.setdefault("X-Robots-Tag", "noindex, nofollow, noarchive")
    response.headers.setdefault("Referrer-Policy", "no-referrer")
    response.headers.setdefault("X-Permitted-Cross-Domain-Policies", "none")
    response.headers.setdefault("X-DNS-Prefetch-Control", "off")
    response.headers.setdefault("Cross-Origin-Opener-Policy", "same-origin")
    response.headers.setdefault("Cross-Origin-Resource-Policy", "same-origin")
    response.headers.setdefault("Origin-Agent-Cluster", "?1")
    response.headers.setdefault(
        "Permissions-Policy",
        "camera=(self), geolocation=(), microphone=(), payment=(), usb=(), "
        "accelerometer=(), gyroscope=(), magnetometer=(), browsing-topics=()",
    )
    csp = (
        "default-src 'self'; base-uri 'none'; frame-ancestors 'none'; frame-src 'none'; object-src 'none'; "
        "form-action 'self'; script-src 'self'; script-src-attr 'unsafe-inline'; "
        "style-src 'self' 'unsafe-inline'; img-src 'self' https: data: blob:; "
        "font-src 'self'; connect-src 'self'; media-src 'self' blob:; "
        "worker-src 'self' blob:; manifest-src 'self'"
    )
    if _RENDER_EXTERNAL_URL:
        csp += "; upgrade-insecure-requests"
    response.headers.setdefault("Content-Security-Policy", csp)
    if _RENDER_EXTERNAL_URL and request.is_secure:
        response.headers.setdefault("Strict-Transport-Security", "max-age=31536000; includeSubDomains")

    path = request.path
    if path.startswith("/api/auth/") or path in {"/api/export", "/api/ai/logs/export", "/healthz"}:
        response.headers["Cache-Control"] = "no-store, max-age=0"
    elif path.startswith("/api/"):
        response.headers["Cache-Control"] = (
            "private, no-cache, max-age=0, must-revalidate"
            if request.method in {"GET", "HEAD"}
            else "no-store, max-age=0"
        )
        response.vary.add("Cookie")
    elif path in {"/", "/service-worker.js"}:
        response.headers["Cache-Control"] = "no-cache, max-age=0, must-revalidate"
    if path.startswith("/api/") and response.status_code == 401:
        response.headers.setdefault("Clear-Site-Data", '"cache"')
    response.headers["X-Request-ID"] = getattr(g, "request_id", secrets.token_hex(8))
    return response


@app.route("/api/system/info", methods=["GET"])
def get_system_info():
    ai_provider = configured_ai_provider()
    try:
        db = get_db()
        db.execute("SELECT 1").fetchone()
    except Exception as exc:  # noqa: BLE001 — report instead of a bare 500
        return jsonify({
            **get_backend_summary(),
            "db_unreachable": True,
            "ai_enabled": bool(ai_provider["name"]),
            "ai_provider": ai_provider["name"],
            "ai_provider_label": ai_provider["label"],
        }), 503
    try:
        ensure_product_data_ready(db)
    except Exception:
        # Diagnostics stay reachable while a first-time migration runs.
        pass
    schema_status = product_data_schema_status()
    if schema_status["ready"] and not DB_BOOT_PENDING:
        # Never resume background enrichment on top of startup migrations.
        maybe_resume_enrichment()
        maybe_resume_regulatory_enrichment()
    duplicate_slots = db.execute(
        """
        SELECT COUNT(*) AS count
        FROM (
            SELECT 1 FROM products
            GROUP BY aisle, side, section, shelf, position
            HAVING COUNT(*) > 1
        ) duplicates
        """
    ).fetchone()
    duplicate_barcodes = db.execute(
        """
        SELECT COUNT(*) AS count
        FROM (
            SELECT 1 FROM products
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
        "reference_count": reference_count(),
        "version": os.environ.get("RENDER_GIT_COMMIT", "")[:7],
        "self_keepalive": _SELF_KEEPALIVE_ACTIVE,
        "catalogue_schema": schema_status,
        "database_boot_pending": bool(DB_BOOT_PENDING),
        "database_boot_error": bool(DB_BOOT_ERROR),
    })


_SELF_KEEPALIVE_ACTIVE = False


def _start_self_keepalive():
    """Render's free tier sleeps the app after ~15 idle minutes; the next visitor
    then waits 30-60s. The GitHub cron ping turned out to actually fire every
    1-4 HOURS (GitHub throttles frequent schedules hard), so the app now pings
    ITSELF every 5 minutes: the request goes through Render's public proxy and
    resets the idle timer, so a running instance never sleeps. Only active where
    Render sets RENDER_EXTERNAL_URL (never in local dev). Bonus: each ping hits
    /api/system/info, which is also the enrichment self-heal trigger."""
    global _SELF_KEEPALIVE_ACTIVE
    base_url = os.environ.get("RENDER_EXTERNAL_URL", "").strip().rstrip("/")
    if not base_url:
        print("[KEEPALIVE] RENDER_EXTERNAL_URL absent — auto-ping inactif (normal en local).")
        return
    parsed_base = urlsplit(base_url)
    if (
        parsed_base.scheme != "https"
        or not parsed_base.hostname
        or parsed_base.username
        or parsed_base.password
        or parsed_base.path not in ("", "/")
        or parsed_base.query
        or parsed_base.fragment
    ):
        print("[SECURITY] RENDER_EXTERNAL_URL invalide — auto-ping inactif.")
        return
    _SELF_KEEPALIVE_ACTIVE = True
    import threading
    from urllib.request import HTTPRedirectHandler, Request as UrlRequest, build_opener

    class RejectRedirects(HTTPRedirectHandler):
        def redirect_request(self, req, fp, code, msg, headers, newurl):
            return None

    opener = build_opener(RejectRedirects())

    internal_headers = internal_request_headers()

    def worker():
        # Warm-up FIRST: right after any (re)start, hit the endpoints that build
        # the in-memory search corpora so the first human request is instant —
        # the first visitor after a restart used to pay the whole index build.
        time.sleep(5)
        for path in ("/api/system/info", "/api/products", "/api/client/find?q=warmup&limit=1"):
            try:
                req = UrlRequest(f"{base_url}{path}", headers=internal_headers)
                with opener.open(req, timeout=60) as resp:
                    resp.read()
            except Exception:
                pass
        while True:
            time.sleep(300)
            try:
                req = UrlRequest(f"{base_url}/api/system/info", headers=internal_headers)
                with opener.open(req, timeout=30) as resp:
                    resp.read()
            except Exception:
                pass   # transient failure — the next ping is 5 minutes away

    threading.Thread(target=worker, daemon=True).start()


# ── Boot ───────────────────────────────────────────────────────────────────────


def _finish_persistence_boot():
    global DB_BOOT_ERROR, DB_BOOT_PENDING
    initialized = not _ASYNC_RENDER_BOOT
    if _ASYNC_RENDER_BOOT:
        retry_delays = (0, 3, 8, 15)
        for attempt, delay in enumerate(retry_delays, start=1):
            if delay:
                time.sleep(delay)
            try:
                init_db()
                initialized = True
                DB_BOOT_ERROR = ""
                break
            except Exception as exc:  # noqa: BLE001 — health endpoint must stay alive
                DB_BOOT_ERROR = str(exc)
                print(
                    f"[BOOT] Initialisation PostgreSQL tentative "
                    f"{attempt}/{len(retry_delays)} impossible: {exc}"
                )
        DB_BOOT_PENDING = False

    if initialized:
        _restore_from_gist_if_empty()
        schedule_reference_metadata_sync()  # connect catalogue metadata to placed UPCs
        schedule_initial_product_quality_audit()
        schedule_backfill_missing()  # fetch missing product images in background
        if _ASYNC_RENDER_BOOT:
            schedule_regulatory_enrichment_after()


if _ASYNC_RENDER_BOOT:
    import threading
    threading.Thread(target=_finish_persistence_boot, daemon=True).start()
else:
    _finish_persistence_boot()
_start_self_keepalive()

if __name__ == "__main__":
    DEFAULT_CERT_PATH = os.path.join(os.path.dirname(__file__), "certs", "localhost.pem")
    DEFAULT_KEY_PATH  = os.path.join(os.path.dirname(__file__), "certs", "localhost-key.pem")

    def resolve_ssl_context():
        use_https = os.environ.get("FLASK_USE_HTTPS", "").strip().lower() in {"1", "true", "yes", "on"}
        cert_path = os.environ.get("FLASK_SSL_CERT", DEFAULT_CERT_PATH)
        key_path  = os.environ.get("FLASK_SSL_KEY",  DEFAULT_KEY_PATH)
        if not use_https:
            return None
        if os.path.exists(cert_path) and os.path.exists(key_path):
            print(f"HTTPS local actif avec certificat: {cert_path}")
            return cert_path, key_path
        print("HTTPS demande, mais certificat local introuvable.")
        return None

    try:
        init_db()
    except Exception as exc:  # noqa: BLE001 — dev server must boot to show errors
        print(f"[BOOT] Base de données injoignable au démarrage: {exc}")
    ssl_context = resolve_ssl_context()
    debug_mode = os.environ.get("FLASK_DEBUG", "").strip().lower() in {"1", "true", "yes"}
    app.run(debug=debug_mode, host="0.0.0.0", port=5000, ssl_context=ssl_context)
