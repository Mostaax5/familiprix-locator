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
            shelf       TEXT    NOT NULL,
            position    TEXT    NOT NULL
        )
    """)

    existing_columns = {
        row["name"] for row in db.execute("PRAGMA table_info(products)").fetchall()
    }
    if "brand" not in existing_columns:
        db.execute("ALTER TABLE products ADD COLUMN brand TEXT DEFAULT ''")
    if "description" not in existing_columns:
        db.execute("ALTER TABLE products ADD COLUMN description TEXT DEFAULT ''")

    count = db.execute("SELECT COUNT(*) FROM products").fetchone()[0]
    if count == 0:
        sample = [
            ("Advil Liqui-Gels 200mg", "", "", "0305730170109", "3", "Droite", "2", "4"),
            ("Tylenol Extra Fort 500mg", "", "", "0621038161908", "3", "Gauche", "1", "2"),
            ("Gaviscon menthe", "", "", "0305732278498", "5", "Droite", "3", "1"),
            ("Reactine 10mg", "", "", "0629014107239", "4", "Gauche", "2", "3"),
            ("Polysporin onguent", "", "", "0305730112101", "6", "Droite", "1", "5"),
            ("Pantene shampoing", "", "", "0080878015048", "8", "Gauche", "3", "2"),
        ]
        db.executemany(
            "INSERT INTO products (name, brand, description, barcode, aisle, side, shelf, position) VALUES (?,?,?,?,?,?,?,?)",
            sample,
        )

    db.commit()
    print(f"Base de donnees prete : {DB_PATH}")
