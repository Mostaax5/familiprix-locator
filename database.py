import os
import sqlite3
import threading
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
INTEGRITY_ERRORS = [sqlite3.IntegrityError]
if psycopg is not None:
    INTEGRITY_ERRORS.append(psycopg.IntegrityError)
INTEGRITY_ERRORS = tuple(INTEGRITY_ERRORS)


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


class DatabaseConnection:
    def __init__(self, connection, backend, pool=None):
        self.connection = connection
        self.backend = backend
        self.pool = pool

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
            raise DatabaseIntegrityError(str(exc)) from exc

        return CursorResult(cursor, self.backend, lastrowid=lastrowid)

    def commit(self):
        self.connection.commit()

    def close(self):
        # Pooled connections go back to the pool (rolled back + reset by putconn)
        # instead of paying a fresh TCP+TLS+auth handshake on the next request.
        if self.pool is not None:
            self.pool.putconn(self.connection)
        else:
            self.connection.close()


_PG_POOL = None
_PG_POOL_LOCK = threading.Lock()
_PG_POOL_DIRECT_FALLBACKS = 0
_PG_POOL_WAIT_S = 0.5


def _get_pg_pool():
    global _PG_POOL
    if _PG_POOL is None:
        with _PG_POOL_LOCK:
            if _PG_POOL is None:
                # Start one connection in the background so normal requests receive
                # it immediately. open=True is non-blocking; getconn() below has a
                # short deadline and falls back to a direct connection if the pool's
                # background workers cannot connect.
                pool = ConnectionPool(
                    DATABASE_URL, min_size=1, max_size=6, max_idle=300,
                    timeout=_PG_POOL_WAIT_S, reconnect_timeout=15,
                    kwargs={"row_factory": dict_row}, open=True,
                )
                _PG_POOL = pool
    return _PG_POOL


def pool_stats():
    """Live pool statistics (for /api/system/info diagnostics)."""
    if _PG_POOL is None:
        return None
    try:
        return {**_PG_POOL.get_stats(), "direct_fallbacks": _PG_POOL_DIRECT_FALLBACKS}
    except Exception:
        return None


def connect_db():
    global _PG_POOL_DIRECT_FALLBACKS
    if DB_BACKEND == "postgres":
        if psycopg is None:
            raise RuntimeError("psycopg is required when DATABASE_URL is set.")
        if ConnectionPool is not None:
            pool = _get_pg_pool()
            try:
                return DatabaseConnection(
                    pool.getconn(timeout=_PG_POOL_WAIT_S), "postgres", pool=pool
                )
            except Exception as exc:  # PoolTimeout/pool trouble — NEVER block the app on it
                _PG_POOL_DIRECT_FALLBACKS += 1
                print(f"[DB] pool indisponible ({exc}) — connexion directe de secours. stats={pool_stats()}")
                conn = psycopg.connect(DATABASE_URL, row_factory=dict_row, connect_timeout=5)
                return DatabaseConnection(conn, "postgres")
        conn = psycopg.connect(DATABASE_URL, row_factory=dict_row, connect_timeout=5)
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


def init_db():
    db = connect_db()
    if db.backend == "postgres":
        init_postgres_db(db)
        print("Base de données partagee prete : PostgreSQL")
    else:
        init_sqlite_db(db)
        print(f"Base de données prete : {DB_PATH}")
    db.commit()
    ensure_best_effort_unique_indexes(db)
    db.commit()
    db.close()


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

    db.execute("""
        CREATE TABLE IF NOT EXISTS users (
            username   TEXT PRIMARY KEY,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            last_seen  TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    db.execute("""
        CREATE TABLE IF NOT EXISTS aisle_layouts (
            aisle        TEXT PRIMARY KEY,
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

    db.execute("ALTER TABLE aisle_layouts ADD COLUMN IF NOT EXISTS max_section TEXT NOT NULL DEFAULT '1'")
    db.execute("ALTER TABLE aisle_layouts ADD COLUMN IF NOT EXISTS config_json TEXT NOT NULL DEFAULT ''")


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
        CREATE TABLE IF NOT EXISTS aisle_layouts (
            aisle        TEXT PRIMARY KEY,
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

    layout_columns = {
        row["name"] for row in db.execute("PRAGMA table_info(aisle_layouts)").fetchall()
    }
    if "max_section" not in layout_columns:
        db.execute("ALTER TABLE aisle_layouts ADD COLUMN max_section TEXT NOT NULL DEFAULT '1'")
    if "config_json" not in layout_columns:
        db.execute("ALTER TABLE aisle_layouts ADD COLUMN config_json TEXT NOT NULL DEFAULT ''")


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
