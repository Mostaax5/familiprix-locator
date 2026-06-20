from datetime import datetime, timezone
from flask import request
from database import get_db


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
