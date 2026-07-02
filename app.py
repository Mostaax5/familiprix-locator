import os
from flask import Flask, render_template, send_from_directory, jsonify
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
init_db()

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


@app.route("/api/system/info", methods=["GET"])
def get_system_info():
    db = get_db()
    ai_provider = configured_ai_provider()
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

    init_db()
    ssl_context = resolve_ssl_context()
    debug_mode = os.environ.get("FLASK_DEBUG", "").strip().lower() in {"1", "true", "yes"}
    app.run(debug=debug_mode, host="0.0.0.0", port=5000, ssl_context=ssl_context)
