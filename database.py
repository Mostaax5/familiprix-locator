import os
import sqlite3
import threading
import time
from flask import g, has_app_context

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:  # pragma: no cover - available after requirements install
    psycopg = None
    dict_row = None

try:
    from psycopg_pool import ConnectionPool
except ImportError:  # pragma: no cover - pool is optional (psycopg[pool])
    ConnectionPool = None

DB_PATH = os.path.join(os.path.dirname(__file__), "familiprix.db")
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
DB_BACKEND = "postgres" if DATABASE_URL and not DATABASE_URL.startswith("sqlite:///") else "sqlite"
PG_POOL_ENABLED = os.environ.get("PG_POOL_ENABLED", "").strip().lower() in {
    "1", "true", "yes", "on",
}
INTEGRITY_ERRORS = [sqlite3.IntegrityError]
if psycopg is not None:
    INTEGRITY_ERRORS.append(psycopg.IntegrityError)
INTEGRITY_ERRORS = tuple(INTEGRITY_ERRORS)
_AUTH_SCHEMA_LOCK = threading.Lock()
_POSTGRES_AUTH_SCHEMA_READY = False
_PRODUCT_SCHEMA_LOCK = threading.RLock()
_POSTGRES_PRODUCT_SCHEMA_READY = False
_POSTGRES_PRODUCT_SCHEMA_ERROR = ""
_POSTGRES_SCHEMA_VERSION_SETTING = "database_schema_version"
_POSTGRES_SCHEMA_VERSION = "2026-07-29-product-data-v1"
_PRODUCT_SEARCH_GENERATION = 0
_PRODUCT_SEARCH_GENERATION_LOCK = threading.Lock()


_SEARCH_AFFECTING_PRODUCT_COLUMNS = (
    "name", "barcode", "product_code", "gtin_key",
    "aisle", "side", "section", "shelf", "position",
    "in_stock", "is_plano", "linked_position", "facings",
    "flipped_label", "underneath_label",
)


def product_search_generation():
    return _PRODUCT_SEARCH_GENERATION


def _bump_product_search_generation():
    global _PRODUCT_SEARCH_GENERATION
    with _PRODUCT_SEARCH_GENERATION_LOCK:
        _PRODUCT_SEARCH_GENERATION += 1


def _query_affects_product_search(query):
    sql = " ".join(str(query or "").strip().lower().split())
    if sql.startswith("insert into products ") or sql.startswith("delete from products"):
        return True
    if not sql.startswith("update products set "):
        return False
    assignments = sql.split(" where ", 1)[0]
    return any(f"{column}=" in assignments for column in _SEARCH_AFFECTING_PRODUCT_COLUMNS)


class DatabaseIntegrityError(Exception):
    pass


class CursorResult:
    def __init__(self, cursor, backend, lastrowid=None):
        self.cursor = cursor
        self.backend = backend
        self.lastrowid = lastrowid
        self.rowcount = getattr(cursor, "rowcount", 0)

    def fetchone(self):
        row = self.cursor.fetchone()
        if row is None:
            return None
        return row

    def fetchall(self):
        return self.cursor.fetchall()

    def fetchmany(self, size=200):
        return self.cursor.fetchmany(size)

    def __iter__(self):
        return iter(self.cursor)


class DatabaseConnection:
    def __init__(self, connection, backend, pool=None):
        self.connection = connection
        self.backend = backend
        self.pool = pool
        self._product_search_mutated = False

    def execute(self, query, params=()):
        params = tuple(params or ())
        cursor = self.connection.cursor()
        sql = query
        lastrowid = None

        try:
            if self.backend == "postgres":
                sql = query.replace("?", "%s")
                lower_sql = sql.strip().lower()
                wants_insert_id = lower_sql.startswith("insert into products ") and "returning" not in lower_sql
                if wants_insert_id:
                    sql = f"{sql.rstrip()} RETURNING id"
                cursor.execute(sql, params)
                if wants_insert_id:
                    row = cursor.fetchone()
                    lastrowid = row["id"] if isinstance(row, dict) else row[0]
            else:
                cursor.execute(sql, params)
                lastrowid = getattr(cursor, "lastrowid", None)
        except INTEGRITY_ERRORS as exc:
            self.connection.rollback()
            self._product_search_mutated = False
            raise DatabaseIntegrityError(str(exc)) from exc

        if _query_affects_product_search(query):
            self._product_search_mutated = True
        return CursorResult(cursor, self.backend, lastrowid=lastrowid)

    def executemany(self, query, param_sequences):
        cursor = self.connection.cursor()
        sql = query.replace("?", "%s") if self.backend == "postgres" else query
        params = [tuple(values or ()) for values in param_sequences]
        try:
            cursor.executemany(sql, params)
        except INTEGRITY_ERRORS as exc:
            self.connection.rollback()
            self._product_search_mutated = False
            raise DatabaseIntegrityError(str(exc)) from exc
        if _query_affects_product_search(query):
            self._product_search_mutated = True
        return CursorResult(cursor, self.backend)

    def commit(self):
        self.connection.commit()
        if self._product_search_mutated:
            _bump_product_search_generation()
            self._product_search_mutated = False

    def rollback(self):
        self.connection.rollback()
        self._product_search_mutated = False

    def close(self):
        # Pooled connections go back to the pool (rolled back + reset by putconn)
        # instead of paying a fresh TCP+TLS+auth handshake on the next request.
        if self.pool is not None:
            self.pool.putconn(self.connection)
        else:
            self.connection.close()


_PG_POOL = None
_PG_POOL_LOCK = threading.Lock()


def _get_pg_pool():
    global _PG_POOL
    if _PG_POOL is None:
        with _PG_POOL_LOCK:
            if _PG_POOL is None:
                # Small pool: 4 gunicorn threads + a couple of background workers.
                # min_size=0 so an idle app holds no connection open.
                # open=False + timeout=15: connections open lazily PER REQUEST. With
                # open=True the pool blocked 30s at import when the DB was unreachable
                # (e.g. expired Render Postgres) and gunicorn crash-looped — the app
                # must boot anyway so it can report the problem and recover once the
                # database is back.
                pool = ConnectionPool(
                    DATABASE_URL, min_size=0, max_size=6, max_idle=300, timeout=15,
                    kwargs={"row_factory": dict_row, "sslmode": "require"}, open=False,
                )
                pool.open(wait=False)   # non-blocking: getconn() waits (≤15s), boot never does
                _PG_POOL = pool
    return _PG_POOL


def pool_stats():
    """Live pool statistics (for /api/system/info diagnostics)."""
    if not PG_POOL_ENABLED:
        return {"enabled": False}
    if _PG_POOL is None:
        return {"enabled": True, "started": False}
    try:
        return {"enabled": True, **_PG_POOL.get_stats()}
    except Exception:
        return {"enabled": True, "stats_unavailable": True}


def connect_db():
    if DB_BACKEND == "postgres":
        if psycopg is None:
            raise RuntimeError("psycopg is required when DATABASE_URL is set.")
        # Render's pool workers repeatedly timed out while the exact same direct
        # connection succeeded, adding 15-40 seconds to every request. Keep the
        # pool opt-in only; the default direct connection is bounded and closed at
        # request teardown, so it cannot accumulate background worker threads.
        if ConnectionPool is not None and PG_POOL_ENABLED:
            pool = _get_pg_pool()
            try:
                return DatabaseConnection(pool.getconn(), "postgres", pool=pool)
            except Exception as exc:  # PoolTimeout/pool trouble — NEVER block the app on it
                print(f"[DB] pool indisponible ({exc}) — connexion directe de secours. stats={pool_stats()}")
                conn = psycopg.connect(
                    DATABASE_URL, row_factory=dict_row, connect_timeout=5, sslmode="require"
                )
                return DatabaseConnection(conn, "postgres")
        conn = psycopg.connect(
            DATABASE_URL, row_factory=dict_row, connect_timeout=5, sslmode="require"
        )
        return DatabaseConnection(conn, "postgres")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return DatabaseConnection(conn, "sqlite")


def get_db():
    if has_app_context():
        db = g.get("_familiprix_db")
        if db is None:
            db = connect_db()
            g._familiprix_db = db
        return db
    return connect_db()


def close_db(_error=None):
    if not has_app_context():
        return
    db = g.pop("_familiprix_db", None)
    if db is not None:
        db.close()


def _postgres_stored_schema_version(db):
    row = db.execute(
        "SELECT setting_value FROM app_settings WHERE setting_key=?",
        (_POSTGRES_SCHEMA_VERSION_SETTING,),
    ).fetchone()
    if not row:
        return ""
    values = dict(row)
    return str(values.get("setting_value", "") or "").strip()


def _set_postgres_schema_version(db):
    db.execute(
        """INSERT INTO app_settings(setting_key, setting_value, updated_at)
           VALUES(?,?,?)
           ON CONFLICT(setting_key) DO UPDATE SET
             setting_value=excluded.setting_value,
             updated_at=excluded.updated_at""",
        (
            _POSTGRES_SCHEMA_VERSION_SETTING,
            _POSTGRES_SCHEMA_VERSION,
            int(time.time()),
        ),
    )


def _postgres_existing_schema_compatible(db):
    """Recognize the complete pre-version production schema once."""
    if not _postgres_product_data_schema_complete(db):
        return False
    required_tables = (
        "products", "product_reference", "aisle_layouts", "ai_logs",
        "removed_products", "planogram_imports",
    )
    placeholders = ",".join("?" for _ in required_tables)
    row = db.execute(
        f"""SELECT COUNT(DISTINCT table_name) AS count
            FROM information_schema.tables
            WHERE table_schema=current_schema()
              AND table_name IN ({placeholders})""",
        required_tables,
    ).fetchone()
    values = dict(row) if row else {}
    return int(values.get("count") or 0) == len(required_tables)


def init_db():
    global _POSTGRES_AUTH_SCHEMA_READY, _POSTGRES_PRODUCT_SCHEMA_READY
    global _POSTGRES_PRODUCT_SCHEMA_ERROR
    db = connect_db()
    try:
        if db.backend == "postgres":
            # A deploy must never wait forever behind a transaction holding a
            # schema lock. A later worker can retry a best-effort migration.
            db.execute("SELECT set_config('lock_timeout', ?, false)", ("3s",))
            db.execute("SELECT set_config('statement_timeout', ?, false)", ("20s",))
            # Commit authentication first so Scan/Plan never waits for the
            # larger product and planogram migration running at startup.
            ensure_auth_schema(db)
            with _PRODUCT_SCHEMA_LOCK:
                stored_version = _postgres_stored_schema_version(db)
                schema_is_current = stored_version == _POSTGRES_SCHEMA_VERSION
                can_bootstrap_version = (
                    not stored_version
                    and _postgres_existing_schema_compatible(db)
                )
                if not (schema_is_current or can_bootstrap_version):
                    init_postgres_db(db)
                _set_postgres_schema_version(db)
                db.commit()
                _POSTGRES_PRODUCT_SCHEMA_READY = True
                _POSTGRES_PRODUCT_SCHEMA_ERROR = ""
            if schema_is_current or can_bootstrap_version:
                _POSTGRES_AUTH_SCHEMA_READY = True
                return
            print("Base de données partagee prete : PostgreSQL")
        else:
            init_sqlite_db(db)
            print(f"Base de données prete : {DB_PATH}")
            db.commit()
        ensure_best_effort_unique_indexes(db)
        db.commit()
        if db.backend == "postgres":
            _POSTGRES_AUTH_SCHEMA_READY = True
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass
        raise
    finally:
        db.close()


def _postgres_auth_schema_complete(db):
    row = db.execute(
        """SELECT
             (SELECT COUNT(DISTINCT table_name)
                FROM information_schema.tables
               WHERE table_schema=current_schema()
                 AND table_name IN
                     ('users','app_settings','auth_sessions','security_events'))
               AS table_count,
             (SELECT COUNT(*)
                FROM information_schema.columns
               WHERE table_schema=current_schema()
                 AND table_name='auth_sessions'
                 AND column_name='password_fingerprint') AS column_count"""
    ).fetchone()
    values = dict(row) if row else {}
    return (
        int(values.get("table_count") or 0) == 4
        and int(values.get("column_count") or 0) == 1
    )


def ensure_auth_schema(db):
    """Create only the small authentication tables when a full migration was delayed.

    Render starts the web worker before best-effort schema maintenance finishes.
    Product reads can therefore work while a lock timeout prevented the newer
    auth tables from being created. Login calls this idempotent repair before it
    reads or writes authentication state; no product or plan table is touched.
    """
    global _POSTGRES_AUTH_SCHEMA_READY
    if db.backend == "postgres" and _POSTGRES_AUTH_SCHEMA_READY:
        return
    if db.backend == "postgres" and _postgres_auth_schema_complete(db):
        _POSTGRES_AUTH_SCHEMA_READY = True
        return
    with _AUTH_SCHEMA_LOCK:
        if db.backend == "postgres" and _POSTGRES_AUTH_SCHEMA_READY:
            return
        if db.backend == "postgres" and _postgres_auth_schema_complete(db):
            _POSTGRES_AUTH_SCHEMA_READY = True
            return
        try:
            if db.backend == "postgres":
                db.execute("SELECT set_config('lock_timeout', ?, true)", ("5s",))
            db.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    username   TEXT PRIMARY KEY,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    last_seen  TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            db.execute("""
                CREATE TABLE IF NOT EXISTS app_settings (
                    setting_key   TEXT PRIMARY KEY,
                    setting_value TEXT NOT NULL DEFAULT '',
                    updated_at    BIGINT NOT NULL DEFAULT 0
                )
            """)
            db.execute("""
                CREATE TABLE IF NOT EXISTS auth_sessions (
                    token_hash      TEXT PRIMARY KEY,
                    csrf_token      TEXT NOT NULL,
                    username        TEXT NOT NULL,
                    created_at      BIGINT NOT NULL,
                    expires_at      BIGINT NOT NULL,
                    last_seen       BIGINT NOT NULL,
                    revoked_at      BIGINT NOT NULL DEFAULT 0,
                    client_hash     TEXT DEFAULT '',
                    user_agent_hash TEXT DEFAULT '',
                    password_fingerprint TEXT DEFAULT ''
                )
            """)
            event_id = (
                "BIGSERIAL PRIMARY KEY"
                if db.backend == "postgres"
                else "INTEGER PRIMARY KEY AUTOINCREMENT"
            )
            db.execute(f"""
                CREATE TABLE IF NOT EXISTS security_events (
                    id              {event_id},
                    created_at      BIGINT NOT NULL,
                    action          TEXT NOT NULL,
                    username        TEXT DEFAULT '',
                    client_hash     TEXT DEFAULT '',
                    user_agent_hash TEXT DEFAULT '',
                    detail_json     TEXT DEFAULT ''
                )
            """)
            db.execute("CREATE INDEX IF NOT EXISTS idx_auth_sessions_expires ON auth_sessions(expires_at)")
            db.execute("CREATE INDEX IF NOT EXISTS idx_security_events_created ON security_events(created_at)")
            if db.backend == "postgres":
                db.execute(
                    "ALTER TABLE auth_sessions ADD COLUMN IF NOT EXISTS "
                    "password_fingerprint TEXT DEFAULT ''"
                )
            else:
                columns = {
                    row["name"] for row in db.execute("PRAGMA table_info(auth_sessions)").fetchall()
                }
                if "password_fingerprint" not in columns:
                    db.execute(
                        "ALTER TABLE auth_sessions ADD COLUMN "
                        "password_fingerprint TEXT DEFAULT ''"
                    )
            db.commit()
            if db.backend == "postgres":
                _POSTGRES_AUTH_SCHEMA_READY = True
        except Exception:
            try:
                db.rollback()
            except Exception:
                pass
            raise


_PRODUCT_DATA_TEXT_COLUMNS = (
    "gtin_key", "data_status", "identity_status", "name_status",
    "description_status", "image_status", "quality_checked_at",
    "primary_source", "primary_source_url", "category", "package_size",
    "package_unit", "variant", "flavour", "colour", "strength",
    "dosage_form", "manufacturer", "ingredients", "compatibility",
    "purpose", "route_of_administration", "official_name_fr",
    "official_name_en",
)


def ensure_product_data_schema(db):
    """Add the auditable product-data layer without changing plan locations."""
    if db.backend == "postgres":
        for column in _PRODUCT_DATA_TEXT_COLUMNS:
            default = "complete_unverified" if column == "data_status" else "unverified" if column.endswith("_status") else ""
            db.execute(f"ALTER TABLE products ADD COLUMN IF NOT EXISTS {column} TEXT DEFAULT '{default}'")
        db.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS quality_issue_count INTEGER DEFAULT 0")
        reference_columns = (
            "gtin_key", "match_method", "verification_status", "last_verified_at",
            "store_presence_status",
            "package_size", "package_unit", "variant", "flavour", "colour",
            "strength", "dosage_form", "manufacturer", "category", "ingredients",
            "compatibility", "purpose", "route_of_administration",
            "official_name_fr", "official_name_en",
        )
        for column in reference_columns:
            db.execute(
                f"ALTER TABLE product_reference ADD COLUMN IF NOT EXISTS {column} TEXT DEFAULT ''"
            )
        db.execute("ALTER TABLE product_reference ADD COLUMN IF NOT EXISTS source_priority INTEGER DEFAULT 0")
        db.execute("ALTER TABLE product_reference ADD COLUMN IF NOT EXISTS confidence DOUBLE PRECISION DEFAULT 0")
    else:
        product_columns = {
            row["name"] for row in db.execute("PRAGMA table_info(products)").fetchall()
        }
        for column in _PRODUCT_DATA_TEXT_COLUMNS:
            if column in product_columns:
                continue
            default = "complete_unverified" if column == "data_status" else "unverified" if column.endswith("_status") else ""
            db.execute(f"ALTER TABLE products ADD COLUMN {column} TEXT DEFAULT '{default}'")
        if "quality_issue_count" not in product_columns:
            db.execute("ALTER TABLE products ADD COLUMN quality_issue_count INTEGER DEFAULT 0")

        reference_existing = {
            row["name"] for row in db.execute("PRAGMA table_info(product_reference)").fetchall()
        }
        reference_columns = (
            "gtin_key", "match_method", "verification_status", "last_verified_at",
            "store_presence_status",
            "package_size", "package_unit", "variant", "flavour", "colour",
            "strength", "dosage_form", "manufacturer", "category", "ingredients",
            "compatibility", "purpose", "route_of_administration",
            "official_name_fr", "official_name_en",
        )
        for column in reference_columns:
            if column not in reference_existing:
                db.execute(f"ALTER TABLE product_reference ADD COLUMN {column} TEXT DEFAULT ''")
        if "source_priority" not in reference_existing:
            db.execute("ALTER TABLE product_reference ADD COLUMN source_priority INTEGER DEFAULT 0")
        if "confidence" not in reference_existing:
            db.execute("ALTER TABLE product_reference ADD COLUMN confidence REAL DEFAULT 0")

    # Presence can be recovered safely from a historical planogram source even
    # when its descriptive fields remain unverified.
    db.execute(
        """UPDATE product_reference SET store_presence_status='planogram_imported'
           WHERE TRIM(COALESCE(store_presence_status,''))=''
             AND LOWER(COALESCE(source,'')) LIKE ?""",
        ("%planogram%",),
    )

    id_type = "BIGSERIAL PRIMARY KEY" if db.backend == "postgres" else "INTEGER PRIMARY KEY AUTOINCREMENT"
    real_type = "DOUBLE PRECISION" if db.backend == "postgres" else "REAL"
    db.execute(f"""
        CREATE TABLE IF NOT EXISTS product_identifiers (
            id                  {id_type},
            product_id          BIGINT NOT NULL,
            identifier_type     TEXT NOT NULL,
            identifier_value    TEXT NOT NULL,
            normalized_value    TEXT NOT NULL,
            authority           TEXT NOT NULL DEFAULT '',
            is_primary          INTEGER NOT NULL DEFAULT 0,
            package_level       TEXT NOT NULL DEFAULT 'sellable_unit',
            source              TEXT DEFAULT '',
            source_url          TEXT DEFAULT '',
            source_record_id    TEXT DEFAULT '',
            match_method        TEXT DEFAULT '',
            confidence          {real_type} NOT NULL DEFAULT 0,
            verification_status TEXT NOT NULL DEFAULT 'unverified',
            imported_at         TEXT DEFAULT '',
            last_verified_at    TEXT DEFAULT '',
            UNIQUE(product_id, identifier_type, normalized_value, authority)
        )
    """)
    db.execute(f"""
        CREATE TABLE IF NOT EXISTS product_field_evidence (
            id                  {id_type},
            product_id          BIGINT NOT NULL,
            field_name          TEXT NOT NULL,
            field_value         TEXT NOT NULL,
            source              TEXT NOT NULL DEFAULT '',
            source_type         TEXT NOT NULL DEFAULT 'unknown',
            source_priority     INTEGER NOT NULL DEFAULT 0,
            source_url          TEXT DEFAULT '',
            source_record_id    TEXT DEFAULT '',
            match_method        TEXT DEFAULT '',
            confidence          {real_type} NOT NULL DEFAULT 0,
            verification_status TEXT NOT NULL DEFAULT 'unverified',
            imported_at         TEXT DEFAULT '',
            last_verified_at    TEXT DEFAULT '',
            active              INTEGER NOT NULL DEFAULT 0,
            UNIQUE(product_id, field_name, field_value, source, source_record_id)
        )
    """)
    db.execute(f"""
        CREATE TABLE IF NOT EXISTS product_reference_evidence (
            id                  {id_type},
            gtin_key            TEXT NOT NULL,
            barcode             TEXT NOT NULL,
            field_name          TEXT NOT NULL,
            field_value         TEXT NOT NULL,
            source              TEXT NOT NULL DEFAULT '',
            source_type         TEXT NOT NULL DEFAULT 'unknown',
            source_priority     INTEGER NOT NULL DEFAULT 0,
            source_url          TEXT DEFAULT '',
            source_record_id    TEXT DEFAULT '',
            match_method        TEXT DEFAULT '',
            confidence          {real_type} NOT NULL DEFAULT 0,
            verification_status TEXT NOT NULL DEFAULT 'unverified',
            imported_at         TEXT DEFAULT '',
            last_verified_at    TEXT DEFAULT '',
            active              INTEGER NOT NULL DEFAULT 0,
            UNIQUE(gtin_key, field_name, field_value, source, source_record_id)
        )
    """)
    db.execute(f"""
        CREATE TABLE IF NOT EXISTS product_reference_identifiers (
            id                  {id_type},
            gtin_key            TEXT NOT NULL,
            barcode             TEXT NOT NULL,
            identifier_type     TEXT NOT NULL,
            identifier_value    TEXT NOT NULL,
            normalized_value    TEXT NOT NULL,
            authority           TEXT NOT NULL DEFAULT '',
            source              TEXT DEFAULT '',
            source_url          TEXT DEFAULT '',
            source_record_id    TEXT DEFAULT '',
            match_method        TEXT DEFAULT '',
            confidence          {real_type} NOT NULL DEFAULT 0,
            verification_status TEXT NOT NULL DEFAULT 'unverified',
            imported_at         TEXT DEFAULT '',
            last_verified_at    TEXT DEFAULT '',
            UNIQUE(gtin_key, identifier_type, normalized_value, authority, source_record_id)
        )
    """)
    db.execute(f"""
        CREATE TABLE IF NOT EXISTS product_data_issues (
            id              {id_type},
            product_id      BIGINT NOT NULL,
            issue_type      TEXT NOT NULL,
            field_name      TEXT DEFAULT '',
            existing_value  TEXT DEFAULT '',
            candidate_value TEXT DEFAULT '',
            source          TEXT DEFAULT '',
            source_url      TEXT DEFAULT '',
            match_method    TEXT DEFAULT '',
            confidence      {real_type} NOT NULL DEFAULT 0,
            status          TEXT NOT NULL DEFAULT 'open',
            details_json    TEXT DEFAULT '',
            created_at      TEXT DEFAULT '',
            resolved_at     TEXT DEFAULT '',
            resolved_by     TEXT DEFAULT ''
        )
    """)
    db.execute(f"""
        CREATE TABLE IF NOT EXISTS product_aliases (
            id                  {id_type},
            product_id          BIGINT NOT NULL,
            alias_type          TEXT NOT NULL,
            alias_value         TEXT NOT NULL,
            normalized_value    TEXT NOT NULL,
            language            TEXT DEFAULT '',
            source              TEXT DEFAULT '',
            confidence          {real_type} NOT NULL DEFAULT 0,
            verification_status TEXT NOT NULL DEFAULT 'unverified',
            UNIQUE(product_id, alias_type, normalized_value)
        )
    """)
    db.execute(f"""
        CREATE TABLE IF NOT EXISTS product_relationships (
            id                  {id_type},
            source_product_id   BIGINT NOT NULL,
            target_product_id   BIGINT NOT NULL,
            relationship_type   TEXT NOT NULL,
            source              TEXT DEFAULT '',
            source_url          TEXT DEFAULT '',
            confidence          {real_type} NOT NULL DEFAULT 0,
            verification_status TEXT NOT NULL DEFAULT 'unverified',
            approved_by         TEXT DEFAULT '',
            approved_role       TEXT DEFAULT '',
            created_at          TEXT DEFAULT '',
            last_verified_at    TEXT DEFAULT '',
            UNIQUE(source_product_id, target_product_id, relationship_type)
        )
    """)
    db.execute(f"""
        CREATE TABLE IF NOT EXISTS product_quality_runs (
            id           {id_type},
            started_at   TEXT DEFAULT '',
            completed_at TEXT DEFAULT '',
            trigger_type TEXT DEFAULT '',
            status       TEXT DEFAULT '',
            employee     TEXT DEFAULT '',
            scanned      INTEGER DEFAULT 0,
            updated      INTEGER DEFAULT 0,
            issues       INTEGER DEFAULT 0
        )
    """)
    db.execute(f"""
        CREATE TABLE IF NOT EXISTS regulatory_sync_state (
            source               TEXT PRIMARY KEY,
            status               TEXT DEFAULT '',
            phase                TEXT DEFAULT '',
            started_at           TEXT DEFAULT '',
            updated_at           TEXT DEFAULT '',
            completed_at         TEXT DEFAULT '',
            source_version       TEXT DEFAULT '',
            catalogue_gtins      INTEGER DEFAULT 0,
            checked_gtins        INTEGER DEFAULT 0,
            exact_matches        INTEGER DEFAULT 0,
            verified_identifiers INTEGER DEFAULT 0,
            review_candidates    INTEGER DEFAULT 0,
            conflicts            INTEGER DEFAULT 0,
            online_checked       INTEGER DEFAULT 0,
            remaining_online     INTEGER DEFAULT 0,
            error                TEXT DEFAULT ''
        )
    """)
    db.execute(f"""
        CREATE TABLE IF NOT EXISTS regulatory_gtin_checks (
            id             {id_type},
            gtin_key       TEXT NOT NULL,
            barcode        TEXT DEFAULT '',
            source         TEXT NOT NULL,
            status         TEXT DEFAULT '',
            checked_at     TEXT DEFAULT '',
            source_version TEXT DEFAULT '',
            details_json   TEXT DEFAULT '',
            UNIQUE(gtin_key, source)
        )
    """)
    db.execute("CREATE INDEX IF NOT EXISTS idx_products_gtin_key ON products(gtin_key)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_reference_gtin_key ON product_reference(gtin_key)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_product_identifiers_value ON product_identifiers(identifier_type, normalized_value, authority)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_product_evidence_active ON product_field_evidence(product_id, field_name, active)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_reference_evidence_active ON product_reference_evidence(gtin_key, field_name, active)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_reference_identifiers_value ON product_reference_identifiers(identifier_type, normalized_value, authority)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_reference_identifiers_gtin ON product_reference_identifiers(gtin_key, verification_status)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_product_issues_open ON product_data_issues(status, issue_type, product_id)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_product_issues_product ON product_data_issues(product_id)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_product_aliases_value ON product_aliases(normalized_value)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_product_relationships_source ON product_relationships(source_product_id, relationship_type)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_product_relationships_target ON product_relationships(target_product_id)")


_PRODUCT_DATA_TABLES = (
    "product_identifiers", "product_field_evidence",
    "product_reference_evidence", "product_reference_identifiers",
    "product_data_issues", "product_aliases", "product_relationships",
    "product_quality_runs", "regulatory_sync_state", "regulatory_gtin_checks",
)

_PRODUCT_REFERENCE_DATA_COLUMNS = (
    "gtin_key", "match_method", "verification_status", "last_verified_at",
    "store_presence_status", "package_size", "package_unit", "variant",
    "flavour", "colour", "strength", "dosage_form", "manufacturer",
    "category", "ingredients", "compatibility", "purpose",
    "route_of_administration", "official_name_fr", "official_name_en",
    "source_priority", "confidence",
)


def _result_count(row):
    if not row:
        return 0
    if isinstance(row, dict):
        return int(next(iter(row.values()), 0) or 0)
    return int(row[0] or 0)


def _postgres_product_data_schema_complete(db):
    product_columns = tuple(_PRODUCT_DATA_TEXT_COLUMNS) + ("quality_issue_count",)
    product_placeholders = ",".join("?" for _ in product_columns)
    product_count = _result_count(db.execute(
        f"""SELECT COUNT(DISTINCT column_name) AS count
            FROM information_schema.columns
            WHERE table_schema=current_schema() AND table_name='products'
              AND column_name IN ({product_placeholders})""",
        product_columns,
    ).fetchone())
    if product_count != len(product_columns):
        return False

    reference_placeholders = ",".join("?" for _ in _PRODUCT_REFERENCE_DATA_COLUMNS)
    reference_count = _result_count(db.execute(
        f"""SELECT COUNT(DISTINCT column_name) AS count
            FROM information_schema.columns
            WHERE table_schema=current_schema() AND table_name='product_reference'
              AND column_name IN ({reference_placeholders})""",
        _PRODUCT_REFERENCE_DATA_COLUMNS,
    ).fetchone())
    if reference_count != len(_PRODUCT_REFERENCE_DATA_COLUMNS):
        return False

    table_placeholders = ",".join("?" for _ in _PRODUCT_DATA_TABLES)
    table_count = _result_count(db.execute(
        f"""SELECT COUNT(DISTINCT table_name) AS count
            FROM information_schema.tables
            WHERE table_schema=current_schema()
              AND table_name IN ({table_placeholders})""",
        _PRODUCT_DATA_TABLES,
    ).fetchone())
    return table_count == len(_PRODUCT_DATA_TABLES)


def ensure_product_data_ready(db):
    """Repair a delayed Render migration before product-data routes run."""
    global _POSTGRES_PRODUCT_SCHEMA_READY, _POSTGRES_PRODUCT_SCHEMA_ERROR
    if db.backend != "postgres" or _POSTGRES_PRODUCT_SCHEMA_READY:
        return True

    # A new Render worker starts with an empty process-local readiness flag even
    # when the shared database was fully migrated by the previous deployment.
    # Check the committed schema before waiting on this worker's startup thread.
    if _postgres_product_data_schema_complete(db):
        _POSTGRES_PRODUCT_SCHEMA_READY = True
        _POSTGRES_PRODUCT_SCHEMA_ERROR = ""
        return True

    acquired = _PRODUCT_SCHEMA_LOCK.acquire(timeout=0.25)
    if not acquired:
        _POSTGRES_PRODUCT_SCHEMA_ERROR = "migration_in_progress"
        raise RuntimeError("product data migration is already in progress")
    try:
        if _POSTGRES_PRODUCT_SCHEMA_READY:
            return True
        if _postgres_product_data_schema_complete(db):
            _POSTGRES_PRODUCT_SCHEMA_READY = True
            _POSTGRES_PRODUCT_SCHEMA_ERROR = ""
            return True
        try:
            db.execute("SELECT set_config('lock_timeout', ?, true)", ("5s",))
            db.execute("SELECT set_config('statement_timeout', ?, true)", ("30s",))
            ensure_product_data_schema(db)
            db.commit()
            _POSTGRES_PRODUCT_SCHEMA_READY = True
            _POSTGRES_PRODUCT_SCHEMA_ERROR = ""
            return True
        except Exception as exc:
            _POSTGRES_PRODUCT_SCHEMA_ERROR = (
                f"{type(exc).__name__}: {exc}"
            )[:600]
            try:
                db.rollback()
            except Exception:
                pass
            raise
    finally:
        _PRODUCT_SCHEMA_LOCK.release()


def product_data_schema_status():
    return {
        "ready": bool(_POSTGRES_PRODUCT_SCHEMA_READY),
        "error": _POSTGRES_PRODUCT_SCHEMA_ERROR,
    }


def ensure_layout_sort_orders(db):
    """Give pre-existing aisles a stable visual order after the column migration."""
    rows = db.execute(
        "SELECT aisle, sort_order FROM aisle_layouts"
    ).fetchall()
    pending = [row for row in rows if int(row["sort_order"] or 0) <= 0]
    if not pending:
        return
    next_order = max([int(row["sort_order"] or 0) for row in rows] + [0])

    def aisle_key(row):
        value = str(row["aisle"] or "").strip()
        return (0, int(value), value) if value.isdigit() else (1, value.lower(), value)

    for row in sorted(pending, key=aisle_key):
        next_order += 1
        db.execute(
            "UPDATE aisle_layouts SET sort_order=? WHERE aisle=?",
            (next_order, str(row["aisle"])),
        )


def init_postgres_db(db):
    db.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id          BIGSERIAL PRIMARY KEY,
            name        TEXT    NOT NULL,
            brand       TEXT    DEFAULT '',
            description TEXT    DEFAULT '',
            image_url   TEXT    DEFAULT '',
            source_url  TEXT    DEFAULT '',
            search_terms TEXT   DEFAULT '',
            usage_notes TEXT    DEFAULT '',
            alternative_suggestions TEXT DEFAULT '',
            barcode     TEXT    DEFAULT '',
            aisle       TEXT    NOT NULL,
            side        TEXT    NOT NULL,
            section     TEXT    NOT NULL DEFAULT '1',
            shelf       TEXT    NOT NULL,
            position    TEXT    NOT NULL,
            modified_by TEXT    DEFAULT '',
            modified_at TEXT    DEFAULT '',
            created_by  TEXT    DEFAULT '',
            created_at  TEXT    DEFAULT ''
        )
    """)

    # Authentication is migrated and committed by ensure_auth_schema() before
    # this larger transaction begins. Do not reacquire auth-table locks here:
    # a delayed product ALTER must never block Scan/Plan login.

    db.execute("""
        CREATE TABLE IF NOT EXISTS aisle_layouts (
            aisle        TEXT PRIMARY KEY,
            sort_order   INTEGER NOT NULL DEFAULT 0,
            max_section  TEXT NOT NULL DEFAULT '1',
            max_shelf    TEXT NOT NULL DEFAULT '5',
            max_position TEXT NOT NULL DEFAULT '8',
            config_json  TEXT NOT NULL DEFAULT '',
            enabled      INTEGER NOT NULL DEFAULT 1,
            modified_by  TEXT DEFAULT '',
            modified_at  TEXT DEFAULT ''
        )
    """)

    db.execute("""
        CREATE TABLE IF NOT EXISTS ai_logs (
            id            BIGSERIAL PRIMARY KEY,
            created_at    TEXT DEFAULT '',
            kind          TEXT DEFAULT '',
            provider      TEXT DEFAULT '',
            model         TEXT DEFAULT '',
            question      TEXT DEFAULT '',
            context_json  TEXT DEFAULT '',
            response_json TEXT DEFAULT '',
            store         TEXT DEFAULT '',
            employee      TEXT DEFAULT '',
            input_tokens  INTEGER DEFAULT 0,
            output_tokens INTEGER DEFAULT 0,
            cost_usd      TEXT DEFAULT '0'
        )
    """)

    db.execute("""
        CREATE TABLE IF NOT EXISTS removed_products (
            id            BIGSERIAL PRIMARY KEY,
            removed_at    TEXT DEFAULT '',
            removed_by    TEXT DEFAULT '',
            barcode       TEXT DEFAULT '',
            name          TEXT DEFAULT '',
            last_location TEXT DEFAULT '',
            product_json  TEXT DEFAULT ''
        )
    """)

    db.execute("""
        CREATE TABLE IF NOT EXISTS planogram_imports (
            id             BIGSERIAL PRIMARY KEY,
            created_at     TEXT DEFAULT '',
            store          TEXT DEFAULT '',
            employee       TEXT DEFAULT '',
            plano_name     TEXT DEFAULT '',
            plano_number   TEXT DEFAULT '',
            plano_version  TEXT DEFAULT '',
            aisle          TEXT DEFAULT '',
            side           TEXT DEFAULT '',
            section        TEXT DEFAULT '',
            tablette_start TEXT DEFAULT '',
            tablette_end   TEXT DEFAULT '',
            imported       INTEGER DEFAULT 0,
            skipped        INTEGER DEFAULT 0
        )
    """)

    db.execute("CREATE INDEX IF NOT EXISTS idx_products_barcode ON products(barcode)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_products_location ON products(aisle, side, section, shelf, position)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_products_name_brand ON products(name, brand)")

    db.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS brand TEXT DEFAULT ''")
    db.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS description TEXT DEFAULT ''")
    db.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS image_url TEXT DEFAULT ''")
    db.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS source_url TEXT DEFAULT ''")
    db.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS search_terms TEXT DEFAULT ''")
    db.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS usage_notes TEXT DEFAULT ''")
    db.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS alternative_suggestions TEXT DEFAULT ''")
    db.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS section TEXT NOT NULL DEFAULT '1'")
    db.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS modified_by TEXT DEFAULT ''")
    db.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS modified_at TEXT DEFAULT ''")
    db.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS created_by TEXT DEFAULT ''")
    db.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS created_at TEXT DEFAULT ''")
    db.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS is_plano INTEGER DEFAULT 0")
    db.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS in_stock INTEGER DEFAULT 1")
    db.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS linked_position TEXT DEFAULT ''")
    db.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS flipped_label INTEGER DEFAULT 0")
    # Familiprix/pharmacy internal code (the shorter "Code" column in planograms).
    # Kept separate from barcode so a UPC search never matches it by accident.
    db.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS product_code TEXT DEFAULT ''")
    db.execute("CREATE INDEX IF NOT EXISTS idx_products_product_code ON products(product_code)")
    # Façades from the planogram = how many positions a product spreads across.
    # Saved for général info only; NOT used for placement (one product / position).
    db.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS facings INTEGER DEFAULT 1")
    # The plano product hidden underneath a hors-plano item (optional; its étiquette
    # is flipped under the visible product). Free text: name or UPC of that product.
    db.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS underneath_label TEXT DEFAULT ''")

    # Reference catalog: every product identified by barcode (online lookup, cache
    # or seed). Checked first on lookup so known UPCs resolve instantly & free, and
    # grows toward "everything we ever scan". Separate from `products` (the plan).
    db.execute("""
        CREATE TABLE IF NOT EXISTS product_reference (
            barcode      TEXT PRIMARY KEY,
            name         TEXT DEFAULT '',
            brand        TEXT DEFAULT '',
            description  TEXT DEFAULT '',
            image_url    TEXT DEFAULT '',
            source        TEXT DEFAULT '',
            source_url    TEXT DEFAULT '',
            product_code  TEXT DEFAULT '',
            enrich_status TEXT DEFAULT '',
            updated_at    TEXT DEFAULT ''
        )
    """)
    db.execute("ALTER TABLE product_reference ADD COLUMN IF NOT EXISTS product_code TEXT DEFAULT ''")
    db.execute("ALTER TABLE product_reference ADD COLUMN IF NOT EXISTS enrich_status TEXT DEFAULT ''")
    ensure_product_data_schema(db)

    db.execute("ALTER TABLE aisle_layouts ADD COLUMN IF NOT EXISTS max_section TEXT NOT NULL DEFAULT '1'")
    db.execute("ALTER TABLE aisle_layouts ADD COLUMN IF NOT EXISTS config_json TEXT NOT NULL DEFAULT ''")
    db.execute("ALTER TABLE aisle_layouts ADD COLUMN IF NOT EXISTS sort_order INTEGER NOT NULL DEFAULT 0")
    ensure_layout_sort_orders(db)


def init_sqlite_db(db):
    db.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT    NOT NULL,
            brand       TEXT    DEFAULT '',
            description TEXT    DEFAULT '',
            image_url   TEXT    DEFAULT '',
            source_url  TEXT    DEFAULT '',
            search_terms TEXT   DEFAULT '',
            usage_notes TEXT    DEFAULT '',
            alternative_suggestions TEXT DEFAULT '',
            barcode     TEXT    DEFAULT '',
            aisle       TEXT    NOT NULL,
            side        TEXT    NOT NULL,
            section     TEXT    NOT NULL DEFAULT '1',
            shelf       TEXT    NOT NULL,
            position    TEXT    NOT NULL,
            modified_by TEXT    DEFAULT '',
            modified_at TEXT    DEFAULT '',
            created_by  TEXT    DEFAULT '',
            created_at  TEXT    DEFAULT ''
        )
    """)

    db.execute("""
        CREATE TABLE IF NOT EXISTS users (
            username   TEXT PRIMARY KEY,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            last_seen  TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    db.execute("""
        CREATE TABLE IF NOT EXISTS app_settings (
            setting_key   TEXT PRIMARY KEY,
            setting_value TEXT NOT NULL DEFAULT '',
            updated_at    INTEGER NOT NULL DEFAULT 0
        )
    """)

    db.execute("""
        CREATE TABLE IF NOT EXISTS auth_sessions (
            token_hash      TEXT PRIMARY KEY,
            csrf_token      TEXT NOT NULL,
            username        TEXT NOT NULL,
            created_at      INTEGER NOT NULL,
            expires_at      INTEGER NOT NULL,
            last_seen       INTEGER NOT NULL,
            revoked_at      INTEGER NOT NULL DEFAULT 0,
            client_hash     TEXT DEFAULT '',
            user_agent_hash TEXT DEFAULT '',
            password_fingerprint TEXT DEFAULT ''
        )
    """)

    db.execute("""
        CREATE TABLE IF NOT EXISTS security_events (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at      INTEGER NOT NULL,
            action          TEXT NOT NULL,
            username        TEXT DEFAULT '',
            client_hash     TEXT DEFAULT '',
            user_agent_hash TEXT DEFAULT '',
            detail_json     TEXT DEFAULT ''
        )
    """)
    db.execute("CREATE INDEX IF NOT EXISTS idx_auth_sessions_expires ON auth_sessions(expires_at)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_security_events_created ON security_events(created_at)")
    auth_session_columns = {
        row["name"] for row in db.execute("PRAGMA table_info(auth_sessions)").fetchall()
    }
    if "password_fingerprint" not in auth_session_columns:
        db.execute("ALTER TABLE auth_sessions ADD COLUMN password_fingerprint TEXT DEFAULT ''")

    db.execute("""
        CREATE TABLE IF NOT EXISTS aisle_layouts (
            aisle        TEXT PRIMARY KEY,
            sort_order   INTEGER NOT NULL DEFAULT 0,
            max_section  TEXT NOT NULL DEFAULT '1',
            max_shelf    TEXT NOT NULL DEFAULT '5',
            max_position TEXT NOT NULL DEFAULT '8',
            config_json  TEXT NOT NULL DEFAULT '',
            enabled      INTEGER NOT NULL DEFAULT 1,
            modified_by  TEXT DEFAULT '',
            modified_at  TEXT DEFAULT ''
        )
    """)

    db.execute("""
        CREATE TABLE IF NOT EXISTS ai_logs (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at    TEXT DEFAULT '',
            kind          TEXT DEFAULT '',
            provider      TEXT DEFAULT '',
            model         TEXT DEFAULT '',
            question      TEXT DEFAULT '',
            context_json  TEXT DEFAULT '',
            response_json TEXT DEFAULT '',
            store         TEXT DEFAULT '',
            employee      TEXT DEFAULT '',
            input_tokens  INTEGER DEFAULT 0,
            output_tokens INTEGER DEFAULT 0,
            cost_usd      TEXT DEFAULT '0'
        )
    """)

    db.execute("""
        CREATE TABLE IF NOT EXISTS removed_products (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            removed_at    TEXT DEFAULT '',
            removed_by    TEXT DEFAULT '',
            barcode       TEXT DEFAULT '',
            name          TEXT DEFAULT '',
            last_location TEXT DEFAULT '',
            product_json  TEXT DEFAULT ''
        )
    """)

    db.execute("""
        CREATE TABLE IF NOT EXISTS planogram_imports (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at     TEXT DEFAULT '',
            store          TEXT DEFAULT '',
            employee       TEXT DEFAULT '',
            plano_name     TEXT DEFAULT '',
            plano_number   TEXT DEFAULT '',
            plano_version  TEXT DEFAULT '',
            aisle          TEXT DEFAULT '',
            side           TEXT DEFAULT '',
            section        TEXT DEFAULT '',
            tablette_start TEXT DEFAULT '',
            tablette_end   TEXT DEFAULT '',
            imported       INTEGER DEFAULT 0,
            skipped        INTEGER DEFAULT 0
        )
    """)

    db.execute("CREATE INDEX IF NOT EXISTS idx_products_barcode ON products(barcode)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_products_location ON products(aisle, side, section, shelf, position)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_products_name_brand ON products(name, brand)")

    existing_columns = {
        row["name"] for row in db.execute("PRAGMA table_info(products)").fetchall()
    }
    if "brand" not in existing_columns:
        db.execute("ALTER TABLE products ADD COLUMN brand TEXT DEFAULT ''")
    if "description" not in existing_columns:
        db.execute("ALTER TABLE products ADD COLUMN description TEXT DEFAULT ''")
    if "image_url" not in existing_columns:
        db.execute("ALTER TABLE products ADD COLUMN image_url TEXT DEFAULT ''")
    if "source_url" not in existing_columns:
        db.execute("ALTER TABLE products ADD COLUMN source_url TEXT DEFAULT ''")
    if "search_terms" not in existing_columns:
        db.execute("ALTER TABLE products ADD COLUMN search_terms TEXT DEFAULT ''")
    if "usage_notes" not in existing_columns:
        db.execute("ALTER TABLE products ADD COLUMN usage_notes TEXT DEFAULT ''")
    if "alternative_suggestions" not in existing_columns:
        db.execute("ALTER TABLE products ADD COLUMN alternative_suggestions TEXT DEFAULT ''")
    if "section" not in existing_columns:
        db.execute("ALTER TABLE products ADD COLUMN section TEXT NOT NULL DEFAULT '1'")
    if "modified_by" not in existing_columns:
        db.execute("ALTER TABLE products ADD COLUMN modified_by TEXT DEFAULT ''")
    if "modified_at" not in existing_columns:
        db.execute("ALTER TABLE products ADD COLUMN modified_at TEXT DEFAULT ''")
    if "created_by" not in existing_columns:
        db.execute("ALTER TABLE products ADD COLUMN created_by TEXT DEFAULT ''")
    if "created_at" not in existing_columns:
        db.execute("ALTER TABLE products ADD COLUMN created_at TEXT DEFAULT ''")
    if "is_plano" not in existing_columns:
        db.execute("ALTER TABLE products ADD COLUMN is_plano INTEGER DEFAULT 0")
    if "in_stock" not in existing_columns:
        db.execute("ALTER TABLE products ADD COLUMN in_stock INTEGER DEFAULT 1")
    if "linked_position" not in existing_columns:
        db.execute("ALTER TABLE products ADD COLUMN linked_position TEXT DEFAULT ''")
    if "flipped_label" not in existing_columns:
        db.execute("ALTER TABLE products ADD COLUMN flipped_label INTEGER DEFAULT 0")
    # Familiprix/pharmacy internal code (the shorter "Code" column in planograms),
    # kept separate from barcode so a UPC search never matches it by accident.
    if "product_code" not in existing_columns:
        db.execute("ALTER TABLE products ADD COLUMN product_code TEXT DEFAULT ''")
    db.execute("CREATE INDEX IF NOT EXISTS idx_products_product_code ON products(product_code)")
    # Façades from the planogram (positions a product spreads across) — saved for
    # général info only; NOT used for placement.
    if "facings" not in existing_columns:
        db.execute("ALTER TABLE products ADD COLUMN facings INTEGER DEFAULT 1")
    if "underneath_label" not in existing_columns:
        db.execute("ALTER TABLE products ADD COLUMN underneath_label TEXT DEFAULT ''")

    # Reference catalog (see postgres init for rationale): known products by barcode.
    db.execute("""
        CREATE TABLE IF NOT EXISTS product_reference (
            barcode      TEXT PRIMARY KEY,
            name         TEXT DEFAULT '',
            brand        TEXT DEFAULT '',
            description  TEXT DEFAULT '',
            image_url    TEXT DEFAULT '',
            source        TEXT DEFAULT '',
            source_url    TEXT DEFAULT '',
            product_code  TEXT DEFAULT '',
            enrich_status TEXT DEFAULT '',
            updated_at    TEXT DEFAULT ''
        )
    """)
    ref_columns = {row["name"] for row in db.execute("PRAGMA table_info(product_reference)").fetchall()}
    if "product_code" not in ref_columns:
        db.execute("ALTER TABLE product_reference ADD COLUMN product_code TEXT DEFAULT ''")
    if "enrich_status" not in ref_columns:
        db.execute("ALTER TABLE product_reference ADD COLUMN enrich_status TEXT DEFAULT ''")
    ensure_product_data_schema(db)

    layout_columns = {
        row["name"] for row in db.execute("PRAGMA table_info(aisle_layouts)").fetchall()
    }
    if "max_section" not in layout_columns:
        db.execute("ALTER TABLE aisle_layouts ADD COLUMN max_section TEXT NOT NULL DEFAULT '1'")
    if "config_json" not in layout_columns:
        db.execute("ALTER TABLE aisle_layouts ADD COLUMN config_json TEXT NOT NULL DEFAULT ''")
    if "sort_order" not in layout_columns:
        db.execute("ALTER TABLE aisle_layouts ADD COLUMN sort_order INTEGER NOT NULL DEFAULT 0")
    ensure_layout_sort_orders(db)


def ensure_best_effort_unique_indexes(db):
    try:
        db.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_products_unique_slot ON products(aisle, side, section, shelf, position)"
        )
    except DatabaseIntegrityError:
        print("Avertissement: impossible d imposer l unicité des positions car des doublons existent déjà.")

    # Barcode uniqueness intentionally removed: same product can be at multiple locations.
    try:
        db.execute("DROP INDEX IF EXISTS idx_products_unique_barcode")
    except Exception:
        pass


def get_backend_summary():
    return {
        "backend": DB_BACKEND,
        "shared_sync": DB_BACKEND == "postgres",
        "label": "PostgreSQL partage" if DB_BACKEND == "postgres" else "SQLite locale",
        "needs_shared_database": DB_BACKEND != "postgres",
    }
