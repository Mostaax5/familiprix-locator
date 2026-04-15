import os
import sqlite3
from flask import g, has_app_context

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:  # pragma: no cover - available after requirements install
    psycopg = None
    dict_row = None

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
    def __init__(self, connection, backend):
        self.connection = connection
        self.backend = backend

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
        self.connection.close()


def connect_db():
    if DB_BACKEND == "postgres":
        if psycopg is None:
            raise RuntimeError("psycopg is required when DATABASE_URL is set.")
        conn = psycopg.connect(DATABASE_URL, row_factory=dict_row)
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
        print("Base de donnees partagee prete : PostgreSQL")
    else:
        init_sqlite_db(db)
        print(f"Base de donnees prete : {DB_PATH}")
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

    db.execute("CREATE INDEX IF NOT EXISTS idx_products_barcode ON products(barcode)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_products_location ON products(aisle, side, section, shelf, position)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_products_name_brand ON products(name, brand)")

    db.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS brand TEXT DEFAULT ''")
    db.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS description TEXT DEFAULT ''")
    db.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS section TEXT NOT NULL DEFAULT '1'")
    db.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS modified_by TEXT DEFAULT ''")
    db.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS modified_at TEXT DEFAULT ''")
    db.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS created_by TEXT DEFAULT ''")
    db.execute("ALTER TABLE products ADD COLUMN IF NOT EXISTS created_at TEXT DEFAULT ''")

    db.execute("ALTER TABLE aisle_layouts ADD COLUMN IF NOT EXISTS max_section TEXT NOT NULL DEFAULT '1'")
    db.execute("ALTER TABLE aisle_layouts ADD COLUMN IF NOT EXISTS config_json TEXT NOT NULL DEFAULT ''")


def init_sqlite_db(db):
    db.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT    NOT NULL,
            brand       TEXT    DEFAULT '',
            description TEXT    DEFAULT '',
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
        print("Avertissement: impossible d imposer l unicite des positions car des doublons existent deja.")

    try:
        db.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_products_unique_barcode ON products(barcode) WHERE barcode <> ''"
        )
    except DatabaseIntegrityError:
        print("Avertissement: impossible d imposer l unicite des codes-barres car des doublons existent deja.")


def get_backend_summary():
    return {
        "backend": DB_BACKEND,
        "shared_sync": DB_BACKEND == "postgres",
        "label": "PostgreSQL partage" if DB_BACKEND == "postgres" else "SQLite locale",
        "needs_shared_database": DB_BACKEND != "postgres",
    }
