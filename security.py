"""Server-side authentication and request security for the employee app.

The browser never receives the password hash or the session token.  Session
tokens are random, stored only as SHA-256 digests in the database, and carried
in an HttpOnly cookie.  A separate per-session token protects state-changing
requests from CSRF.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import secrets
import threading
import time
import unicodedata
from collections import defaultdict
from urllib.parse import urlsplit

from flask import Blueprint, current_app, g, jsonify, request
from werkzeug.security import check_password_hash, generate_password_hash

from database import get_db


auth_bp = Blueprint("auth", __name__)

SESSION_COOKIE = "familiprix_session"
HOST_SESSION_COOKIE = "__Host-familiprix_session"
SESSION_TTL_SECONDS = 8 * 60 * 60
SESSION_IDLE_SECONDS = 30 * 60
PASSWORD_SETTING = "auth_password_hash_v1"

# One-time compatibility bridge for the old browser-only lock.  The digest was
# already public in Git history, so a legacy login is allowed only to replace it
# with a salted scrypt hash before any store API can be used.
_LEGACY_PASSWORD_SHA256 = "1158a3823fa4014569a2b5f7f475a5539429ca8d6abcbab1d1cc7972470982e8"
_INTERNAL_REQUEST_TOKEN = secrets.token_urlsafe(32)
_AUDIT_SALT = secrets.token_bytes(32)
_UNSAFE_METHODS = {"POST", "PUT", "PATCH", "DELETE"}
_PUBLIC_AUTH_PATHS = {"/api/auth/login", "/api/auth/status"}
_ROTATION_PATHS = {"/api/auth/password", "/api/auth/logout", "/api/auth/status"}
_INTERNAL_GET_PATHS = {"/api/system/info", "/api/products", "/api/client/find"}
_PUBLIC_API_GET_PATHS = {
    "/api/system/info",
    "/api/products",
    "/api/products/images",
    "/api/products/reference-images",
    "/api/products/search",
    "/api/products/reference-search",
    "/api/client/find",
    "/api/layout/aisles",
}
_PUBLIC_API_GET_PREFIXES = (
    "/api/products/barcode/",
    "/api/products/lookup/",
)
_PUBLIC_API_POST_PATHS = {
    "/api/client/help",
    "/api/ai/feedback",
}

_rate_lock = threading.Lock()
_login_failures: dict[str, list[float]] = defaultdict(list)
_sensitive_requests: dict[str, list[float]] = defaultdict(list)
_MAX_RATE_LIMIT_KEYS = 4096
_OVERFLOW_RATE_KEY = "__overflow__"

_COMMON_PASSWORDS = {
    "123456789012345", "1234567890123456", "abcdefghijklmnop",
    "adminadminadmin", "changemepassword", "familiprixfamiliprix",
    "iloveyouiloveyou", "letmeinletmeinletmein", "motdepassemotdepasse",
    "passwordpassword", "password123456", "qwertyqwertyqwerty",
    "welcome123456789", "correcthorsebatterystaple",
}


def _json_error(message: str, status: int, code: str):
    return jsonify({"success": False, "error": message, "code": code}), status


def _json_object():
    body = request.get_json(silent=True)
    return body if isinstance(body, dict) else None


def _row_value(row, key, fallback_index=0):
    if row is None:
        return None
    try:
        return row[key]
    except (KeyError, TypeError, IndexError):
        return row[fallback_index]


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _request_identity() -> str:
    remote = request.remote_addr or "unknown"
    return hmac.new(_AUDIT_SALT, remote.encode("utf-8"), hashlib.sha256).hexdigest()


def _user_agent_hash() -> str:
    return _sha256((request.headers.get("User-Agent") or "")[:1000])


def _normalize_username(value) -> str:
    text = unicodedata.normalize("NFC", str(value or "")).strip()
    text = re.sub(r"[\x00-\x1f\x7f<>]", "", text)
    return text[:60] or "appareil"


def _normalize_password(value) -> str:
    if not isinstance(value, str):
        return ""
    return unicodedata.normalize("NFC", value)


def password_validation_error(password: str) -> str | None:
    """NIST-style length and blocklist checks without brittle composition rules."""
    password = _normalize_password(password)
    if len(password) < 15:
        return "Utilisez au moins 15 caracteres (une longue phrase est recommandee)."
    if len(password) > 128:
        return "Le mot de passe ne peut pas depasser 128 caracteres."
    folded = password.casefold()
    compact = re.sub(r"[^a-z0-9]+", "", folded)
    if folded in _COMMON_PASSWORDS or compact in _COMMON_PASSWORDS:
        return "Ce mot de passe est trop connu. Choisissez une phrase unique."
    if compact in {"familiprix", "familiprixlocator", "localisateurproduits"}:
        return "Le mot de passe ne doit pas etre le nom de l'application."
    if len(set(password)) <= 2 or re.fullmatch(r"(.{1,4})\1{3,}", password):
        return "Ce mot de passe est trop repetitif."
    return None


def _get_setting(db, key: str) -> str:
    row = db.execute("SELECT setting_value FROM app_settings WHERE setting_key=?", (key,)).fetchone()
    return str(_row_value(row, "setting_value") or "")


def _set_setting(db, key: str, value: str):
    db.execute(
        """
        INSERT INTO app_settings (setting_key, setting_value, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(setting_key) DO UPDATE SET
            setting_value=excluded.setting_value, updated_at=excluded.updated_at
        """,
        (key, value, int(time.time())),
    )


def _password_record(db):
    configured = os.environ.get("APP_PASSWORD_HASH", "").strip()
    if configured:
        return configured, False, "environment"
    stored = _get_setting(db, PASSWORD_SETTING)
    if stored:
        return stored, False, "database"
    return _LEGACY_PASSWORD_SHA256, False, "legacy"


def _verify_password(db, password: str):
    password = _normalize_password(password)
    if not password or len(password) > 256:
        return False, False, "legacy"
    supplied = _sha256(password)
    # Temporary compatibility requested for the Scan and Plan tabs. The
    # plaintext is never shipped to the browser or stored in this repository.
    if hmac.compare_digest(supplied, _LEGACY_PASSWORD_SHA256):
        return True, False, "protected_area"
    stored, rotation_required, source = _password_record(db)
    if source == "legacy":
        return hmac.compare_digest(supplied, stored), False, source
    try:
        return bool(check_password_hash(stored, password)), rotation_required, source
    except (ValueError, TypeError):
        current_app.logger.error("APP_PASSWORD_HASH is malformed")
        return False, rotation_required, source


def _record_event(db, action: str, username: str = "", detail=None):
    try:
        db.execute(
            """
            INSERT INTO security_events
                (created_at, action, username, client_hash, user_agent_hash, detail_json)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                int(time.time()), str(action)[:60], _normalize_username(username),
                _request_identity(), _user_agent_hash(),
                json.dumps(detail or {}, ensure_ascii=True, separators=(",", ":"))[:500],
            ),
        )
    except Exception:
        current_app.logger.exception("Could not record security event")


def record_security_event(db, action: str, username: str = "", detail=None):
    """Record an event inside the caller's transaction without exposing secrets."""
    _record_event(db, action, username, detail)


def _create_session(db, username: str):
    now = int(time.time())
    raw_token = secrets.token_urlsafe(32)
    csrf_token = secrets.token_urlsafe(32)
    token_hash = _sha256(raw_token)
    password_hash, _rotation_required, _source = _password_record(db)
    password_fingerprint = _sha256(password_hash)
    expires_at = now + SESSION_TTL_SECONDS
    db.execute(
        """
        INSERT INTO auth_sessions
            (token_hash, csrf_token, username, created_at, expires_at, last_seen,
             revoked_at, client_hash, user_agent_hash, password_fingerprint)
        VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, ?)
        """,
        (
            token_hash, csrf_token, _normalize_username(username), now, expires_at,
            now, _request_identity(), _user_agent_hash(), password_fingerprint,
        ),
    )
    return raw_token, token_hash, csrf_token, expires_at


def _load_session():
    raw_token = request.cookies.get(_session_cookie_name(), "")
    if not raw_token or len(raw_token) > 200:
        return None
    token_hash = _sha256(raw_token)
    db = get_db()
    row = db.execute(
        """
        SELECT token_hash, csrf_token, username, created_at, expires_at, revoked_at,
               last_seen, password_fingerprint
        FROM auth_sessions WHERE token_hash=?
        """,
        (token_hash,),
    ).fetchone()
    if not row:
        return None
    session = dict(row)
    now = int(time.time())
    last_seen = int(session.get("last_seen") or session.get("created_at") or 0)
    if (
        int(session.get("revoked_at") or 0)
        or int(session.get("expires_at") or 0) <= now
        or last_seen <= now - SESSION_IDLE_SECONDS
    ):
        if not int(session.get("revoked_at") or 0):
            db.execute(
                "UPDATE auth_sessions SET revoked_at=? WHERE token_hash=? AND revoked_at=0",
                (now, token_hash),
            )
            db.commit()
        return None
    g.auth_username = _normalize_username(session.get("username"))
    g.auth_session_hash = token_hash
    g.auth_csrf_token = str(session.get("csrf_token") or "")
    g.auth_expires_at = int(session.get("expires_at") or 0)
    if last_seen < now - 60:
        db.execute("UPDATE auth_sessions SET last_seen=? WHERE token_hash=?", (now, token_hash))
        db.commit()
    return session


def _session_matches_password(db, session) -> bool:
    stored_password, _rotation_required, _source = _password_record(db)
    expected = _sha256(stored_password)
    actual = str(session.get("password_fingerprint") or "")
    return bool(actual and hmac.compare_digest(actual, expected))


def _revoke_session(db, session):
    db.execute(
        "UPDATE auth_sessions SET revoked_at=? WHERE token_hash=? AND revoked_at=0",
        (int(time.time()), str(session.get("token_hash") or "")),
    )
    db.commit()


def _session_cookie_name() -> str:
    return HOST_SESSION_COOKIE if current_app.config.get("AUTH_COOKIE_SECURE") else SESSION_COOKIE


def _set_session_cookie(response, raw_token: str):
    response.set_cookie(
        _session_cookie_name(),
        raw_token,
        max_age=SESSION_TTL_SECONDS,
        secure=bool(current_app.config.get("AUTH_COOKIE_SECURE")),
        httponly=True,
        samesite="Strict",
        path="/",
    )


def _clear_session_cookie(response):
    response.delete_cookie(
        _session_cookie_name(),
        secure=bool(current_app.config.get("AUTH_COOKIE_SECURE")),
        httponly=True,
        samesite="Strict",
        path="/",
    )
    if _session_cookie_name() != SESSION_COOKIE:
        response.delete_cookie(SESSION_COOKIE, httponly=True, samesite="Strict", path="/")


def _bounded_rate_key(buckets, key: str) -> str:
    if key in buckets or len(buckets) < _MAX_RATE_LIMIT_KEYS:
        return key
    return _OVERFLOW_RATE_KEY


def _prune_auth_data(db):
    now = int(time.time())
    db.execute("DELETE FROM auth_sessions WHERE expires_at<? OR (revoked_at>0 AND revoked_at<?)", (now, now - 86400))
    db.execute("DELETE FROM security_events WHERE created_at<?", (now - 90 * 86400,))


def _failure_key() -> str:
    return _request_identity()


def _login_retry_after(key: str) -> int:
    now = time.time()
    cutoff = now - 15 * 60
    with _rate_lock:
        key = _bounded_rate_key(_login_failures, key)
        bucket = [stamp for stamp in _login_failures.get(key, []) if stamp > cutoff]
        _login_failures[key] = bucket
        if len(bucket) < 5:
            return 0
        return max(1, int(15 * 60 - (now - bucket[0])))


def _register_login_failure(key: str) -> int:
    now = time.time()
    cutoff = now - 15 * 60
    with _rate_lock:
        key = _bounded_rate_key(_login_failures, key)
        bucket = [stamp for stamp in _login_failures.get(key, []) if stamp > cutoff]
        bucket.append(now)
        _login_failures[key] = bucket
        if len(bucket) < 5:
            return 0
        return max(1, int(15 * 60 - (now - bucket[0])))


def _clear_login_failures(key: str):
    with _rate_lock:
        key = _bounded_rate_key(_login_failures, key)
        _login_failures.pop(key, None)


def _sensitive_rate_error():
    if request.method not in _UNSAFE_METHODS:
        return None
    limits = {
        "/api/import/planogram-parse": (8, 3600),
        "/api/import/planogram-catalog": (4, 3600),
        "/api/gist/restore": (4, 3600),
        "/api/reset": (6, 3600),
    }
    rule = limits.get(request.path)
    if not rule:
        return None
    limit, window = rule
    now = time.time()
    key = f"{g.auth_session_hash}:{request.path}"
    with _rate_lock:
        key = _bounded_rate_key(_sensitive_requests, key)
        bucket = [stamp for stamp in _sensitive_requests.get(key, []) if stamp > now - window]
        if len(bucket) >= limit:
            retry = max(1, int(window - (now - bucket[0])))
            response = _json_error("Trop de demandes sensibles. Reessayez plus tard.", 429, "rate_limited")
            response[0].headers["Retry-After"] = str(retry)
            return response
        bucket.append(now)
        _sensitive_requests[key] = bucket
    return None


def _same_origin_request() -> bool:
    fetch_site = (request.headers.get("Sec-Fetch-Site") or "").lower()
    if fetch_site == "cross-site":
        return False
    origin = request.headers.get("Origin")
    if origin:
        return hmac.compare_digest(origin.rstrip("/"), request.host_url.rstrip("/"))
    referer = request.headers.get("Referer")
    if referer:
        try:
            source = urlsplit(referer)
            expected = urlsplit(request.host_url)
            return (
                source.scheme == expected.scheme
                and source.netloc == expected.netloc
            )
        except ValueError:
            return False
    return True


def _is_internal_request() -> bool:
    supplied = request.headers.get("X-Familiprix-Internal", "")
    return bool(
        request.method == "GET"
        and request.path in _INTERNAL_GET_PATHS
        and supplied
        and hmac.compare_digest(supplied, _INTERNAL_REQUEST_TOKEN)
    )


def _is_public_api_request() -> bool:
    if request.method == "GET":
        return (
            request.path in _PUBLIC_API_GET_PATHS
            or request.path.startswith(_PUBLIC_API_GET_PREFIXES)
        )
    return request.method == "POST" and request.path in _PUBLIC_API_POST_PATHS


def protect_api_request():
    """Flask before-request hook: authenticate every non-auth API endpoint."""
    if not request.path.startswith("/api/"):
        return None
    if current_app.testing and current_app.config.get("AUTH_TEST_BYPASS"):
        g.auth_username = "test-user"
        g.auth_session_hash = "test-session"
        g.auth_csrf_token = "test-csrf"
        g.auth_expires_at = int(time.time()) + 3600
        return None
    if _is_internal_request():
        g.auth_username = "internal-warmup"
        g.auth_session_hash = "internal"
        g.auth_csrf_token = ""
        g.auth_expires_at = int(time.time()) + 60
        return None
    if request.path in _PUBLIC_AUTH_PATHS:
        return None
    if _is_public_api_request():
        if request.method in _UNSAFE_METHODS and not _same_origin_request():
            return _json_error("Origine de la demande refusee.", 403, "origin_rejected")
        if (
            request.method in _UNSAFE_METHODS
            and request.content_length is not None
            and request.content_length > 256 * 1024
        ):
            return _json_error("Demande trop volumineuse.", 413, "request_too_large")
        return None
    try:
        session = _load_session()
    except Exception:
        current_app.logger.exception("Authentication database unavailable")
        return _json_error("Le service se prepare. Reessayez dans un instant.", 503, "auth_unavailable")
    if not session:
        return _json_error("Session absente ou expiree.", 401, "authentication_required")

    db = get_db()
    if not _session_matches_password(db, session):
        _revoke_session(db, session)
        return _json_error("Les identifiants ont change. Reconnectez-vous.", 401, "credentials_changed")
    _stored, rotation_required, _source = _password_record(db)
    if rotation_required and request.path not in _ROTATION_PATHS:
        return _json_error("Le mot de passe doit etre remplace avant de continuer.", 428, "password_rotation_required")

    if request.method in _UNSAFE_METHODS:
        if not _same_origin_request():
            return _json_error("Origine de la demande refusee.", 403, "origin_rejected")
        supplied = request.headers.get("X-CSRF-Token", "")
        if not supplied or not hmac.compare_digest(supplied, g.auth_csrf_token):
            return _json_error("Jeton de securite absent ou invalide.", 403, "csrf_rejected")
        limited = _sensitive_rate_error()
        if limited:
            return limited
    return None


def internal_request_headers():
    return {"X-Familiprix-Internal": _INTERNAL_REQUEST_TOKEN}


@auth_bp.route("/api/auth/status", methods=["GET"])
def auth_status():
    try:
        had_cookie = bool(request.cookies.get(_session_cookie_name(), ""))
        session = _load_session()
        if not session:
            response = jsonify({"authenticated": False})
            if had_cookie:
                _clear_session_cookie(response)
                response.headers["Clear-Site-Data"] = '"cache"'
            return response
        db = get_db()
        if not _session_matches_password(db, session):
            _revoke_session(db, session)
            response = jsonify({"authenticated": False, "reason": "credentials_changed"})
            _clear_session_cookie(response)
            response.headers["Clear-Site-Data"] = '"cache"'
            return response
        _stored, rotation_required, _source = _password_record(db)
        return jsonify({
            "authenticated": True,
            "username": g.auth_username,
            "csrf_token": g.auth_csrf_token,
            "expires_at": g.auth_expires_at,
            "rotation_required": rotation_required,
        })
    except Exception:
        current_app.logger.exception("Authentication status failed")
        return _json_error("Le service se prepare. Reessayez dans un instant.", 503, "auth_unavailable")


@auth_bp.route("/api/auth/login", methods=["POST"])
def auth_login():
    if not _same_origin_request():
        return _json_error("Origine de la demande refusee.", 403, "origin_rejected")
    if request.content_length is not None and request.content_length > 4096:
        return _json_error("Demande de connexion trop volumineuse.", 413, "request_too_large")
    if not request.is_json:
        return _json_error("Une demande JSON est requise.", 415, "json_required")
    key = _failure_key()
    retry = _login_retry_after(key)
    if retry:
        response, status = _json_error("Trop de tentatives. Reessayez plus tard.", 429, "login_locked")
        response.headers["Retry-After"] = str(retry)
        return response, status

    body = _json_object()
    if body is None:
        return _json_error("Un objet JSON est requis.", 400, "invalid_json")
    password = _normalize_password(body.get("password"))
    username = _normalize_username(body.get("username"))
    try:
        db = get_db()
        valid, rotation_required, _source = _verify_password(db, password)
    except Exception:
        current_app.logger.exception("Authentication database unavailable")
        return _json_error("Le service se prepare. Reessayez dans un instant.", 503, "auth_unavailable")

    if not valid:
        retry = _register_login_failure(key)
        # A small fixed delay makes online guessing expensive without tying up a
        # worker for the full rate-limit window.
        time.sleep(0.25)
        if retry:
            response, status = _json_error("Trop de tentatives. Reessayez plus tard.", 429, "login_locked")
            response.headers["Retry-After"] = str(retry)
            return response, status
        return _json_error("Identifiants invalides.", 401, "invalid_credentials")

    _clear_login_failures(key)
    now = int(time.time())
    current_cookie = request.cookies.get(_session_cookie_name(), "")
    if current_cookie and len(current_cookie) <= 200:
        db.execute(
            "UPDATE auth_sessions SET revoked_at=? WHERE token_hash=? AND revoked_at=0",
            (now, _sha256(current_cookie)),
        )
    db.execute(
        """
        INSERT INTO users (username, last_seen) VALUES (?, ?)
        ON CONFLICT(username) DO UPDATE SET last_seen=excluded.last_seen
        """,
        (username, str(now)),
    )
    _prune_auth_data(db)
    raw_token, token_hash, csrf_token, expires_at = _create_session(db, username)
    _record_event(db, "login", username, {"rotation_required": rotation_required})
    db.commit()
    g.auth_session_hash = token_hash
    response = jsonify({
        "success": True,
        "authenticated": True,
        "username": username,
        "csrf_token": csrf_token,
        "expires_at": expires_at,
        "rotation_required": rotation_required,
    })
    _set_session_cookie(response, raw_token)
    return response


@auth_bp.route("/api/auth/password", methods=["POST"])
def change_password():
    body = _json_object()
    if body is None:
        return _json_error("Un objet JSON est requis.", 400, "invalid_json")
    new_password = _normalize_password(body.get("new_password"))
    error = password_validation_error(new_password)
    if error:
        return _json_error(error, 400, "weak_password")
    db = get_db()
    _stored, rotation_required, source = _password_record(db)
    if source == "environment":
        return _json_error(
            "Le mot de passe est gere par APP_PASSWORD_HASH dans Render.",
            409,
            "environment_password",
        )
    if not rotation_required:
        valid, _rotation, _source = _verify_password(db, _normalize_password(body.get("current_password")))
        if not valid:
            return _json_error("Le mot de passe actuel est invalide.", 401, "invalid_credentials")

    new_hash = generate_password_hash(new_password, method="scrypt:32768:8:1", salt_length=32)
    now = int(time.time())
    _set_setting(db, PASSWORD_SETTING, new_hash)
    db.execute("UPDATE auth_sessions SET revoked_at=? WHERE revoked_at=0", (now,))
    raw_token, token_hash, csrf_token, expires_at = _create_session(db, g.auth_username)
    _record_event(db, "password_changed", g.auth_username)
    db.commit()
    g.auth_session_hash = token_hash
    response = jsonify({
        "success": True,
        "authenticated": True,
        "username": g.auth_username,
        "csrf_token": csrf_token,
        "expires_at": expires_at,
        "rotation_required": False,
    })
    _set_session_cookie(response, raw_token)
    return response


@auth_bp.route("/api/auth/profile", methods=["POST"])
def update_auth_profile():
    body = _json_object()
    if body is None:
        return _json_error("Un objet JSON est requis.", 400, "invalid_json")
    username = _normalize_username(body.get("username"))
    now = int(time.time())
    db = get_db()
    db.execute("UPDATE auth_sessions SET username=?, last_seen=? WHERE token_hash=?", (username, now, g.auth_session_hash))
    db.execute(
        """
        INSERT INTO users (username, last_seen) VALUES (?, ?)
        ON CONFLICT(username) DO UPDATE SET last_seen=excluded.last_seen
        """,
        (username, str(now)),
    )
    _record_event(db, "profile_named", username)
    db.commit()
    g.auth_username = username
    return jsonify({"success": True, "username": username})


@auth_bp.route("/api/auth/logout", methods=["POST"])
def auth_logout():
    db = get_db()
    now = int(time.time())
    db.execute("UPDATE auth_sessions SET revoked_at=? WHERE token_hash=?", (now, g.auth_session_hash))
    _record_event(db, "logout", g.auth_username)
    db.commit()
    response = jsonify({"success": True})
    _clear_session_cookie(response)
    response.headers["Clear-Site-Data"] = '"cache"'
    return response


def install_security(app):
    app.config.setdefault("AUTH_COOKIE_SECURE", bool(os.environ.get("RENDER_EXTERNAL_URL")))
    app.before_request(protect_api_request)

    @app.after_request
    def clear_rejected_session_cookie(response):
        if request.path.startswith("/api/") and response.status_code == 401:
            _clear_session_cookie(response)
        return response
