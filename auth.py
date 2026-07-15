from datetime import datetime, timezone
from flask import request
from database import get_db


def utc_now_iso():
    # Millisecond precision keeps ETag/cache fingerprints distinct when an
    # employee performs multiple edits or imports inside the same second.
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def side_display_label(side):
    """The internal side value stays 'Gauche'/'Droite' (DB + layout keys) but is
    never shown to users — every display uses 'Côté A' / 'Côté B'."""
    cleaned = str(side or "").strip()
    return {"Gauche": "Côté A", "Droite": "Côté B"}.get(cleaned, cleaned)


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
