import os
import traceback
from flask import Flask, render_template, send_from_directory, jsonify, request
from werkzeug.exceptions import HTTPException
from database import close_db, get_backend_summary, get_db, init_db
from auth import utc_now_iso
from routes.products import products_bp, first_column, schedule_backfill_missing
from routes.layout import layout_bp
from routes.ai import ai_bp, configured_ai_provider, reference_count
from routes.gist import gist_bp, _restore_from_gist_if_empty
from routes.import_export import import_export_bp

app = Flask(__name__)
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


# ── Core routes ────────────────────────────────────────────────────────────────

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


@app.errorhandler(Exception)
def handle_any_error(exc):
    """API callers always get JSON (the frontend parses every response as JSON —
    an HTML error page used to break it silently), and unexpected errors land in
    the Render logs with a full traceback instead of vanishing."""
    if isinstance(exc, HTTPException):
        if request.path.startswith("/api/"):
            return jsonify({"success": False, "error": exc.description}), exc.code
        return exc
    traceback.print_exc()
    if request.path.startswith("/api/"):
        # detail = exception type + message so failures are diagnosable from the
        # response itself (Render log access is not always at hand).
        return jsonify({"success": False, "error": "Erreur interne du serveur.",
                        "detail": f"{type(exc).__name__}: {exc}"[:300]}), 500
    return "Erreur interne du serveur.", 500


@app.after_request
def add_security_headers(response):
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("Referrer-Policy", "same-origin")
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
            "db_error": DB_BOOT_ERROR or str(exc),
            "ai_enabled": bool(ai_provider["name"]),
            "ai_provider": ai_provider["name"],
            "ai_provider_label": ai_provider["label"],
        }), 503
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
    })


# ── Boot ───────────────────────────────────────────────────────────────────────

_restore_from_gist_if_empty()
schedule_backfill_missing()   # auto-fetch any missing product images in background

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
