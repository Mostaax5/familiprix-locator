import os
import sqlite3

DB_PATH = os.path.join(os.path.dirname(__file__), "familiprix.db")


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    db = get_db()
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

    db.commit()
    print(f"Base de donnees prete : {DB_PATH}")
