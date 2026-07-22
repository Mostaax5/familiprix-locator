from datetime import datetime, timezone
from flask import current_app, g, jsonify
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


def require_editor():
    # Identity comes only from the verified server session.  X-User-Name used
    # to let any caller impersonate an employee and was never authentication.
    username = str(getattr(g, "auth_username", "") or "").strip()
    if not username and current_app.testing and current_app.config.get("AUTH_TEST_BYPASS"):
        username = "test-user"
    if not username:
        return None, (
            jsonify({
                "success": False,
                "error": "Session absente ou expiree.",
                "code": "authentication_required",
            }),
            401,
        )
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
