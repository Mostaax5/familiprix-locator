import json
import hashlib
import sqlite3
import unittest
from unittest.mock import patch

from flask import Flask

from routes.import_export import import_export_bp


class AisleRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        self.db.row_factory = sqlite3.Row
        self.db.execute("CREATE TABLE users (username TEXT PRIMARY KEY, last_seen TEXT)")
        self.db.execute(
            """CREATE TABLE aisle_layouts (
                aisle TEXT PRIMARY KEY, max_section TEXT, max_shelf TEXT,
                max_position TEXT, config_json TEXT, enabled INTEGER,
                modified_by TEXT DEFAULT '', modified_at TEXT DEFAULT ''
            )"""
        )
        self.db.execute(
            """CREATE TABLE products (
                id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL,
                brand TEXT DEFAULT '', description TEXT DEFAULT '', image_url TEXT DEFAULT '',
                source_url TEXT DEFAULT '', search_terms TEXT DEFAULT '', usage_notes TEXT DEFAULT '',
                alternative_suggestions TEXT DEFAULT '', barcode TEXT DEFAULT '',
                product_code TEXT DEFAULT '', facings INTEGER DEFAULT 1,
                aisle TEXT NOT NULL, side TEXT NOT NULL, section TEXT DEFAULT '1',
                shelf TEXT NOT NULL, position TEXT NOT NULL, is_plano INTEGER DEFAULT 0,
                in_stock INTEGER DEFAULT 1, linked_position TEXT DEFAULT '',
                flipped_label INTEGER DEFAULT 0, underneath_label TEXT DEFAULT '',
                created_by TEXT DEFAULT '', created_at TEXT DEFAULT '',
                modified_by TEXT DEFAULT '', modified_at TEXT DEFAULT ''
            )"""
        )
        self.db.execute(
            "CREATE UNIQUE INDEX unique_slot ON products(aisle, side, section, shelf, position)"
        )
        self.db.execute(
            """CREATE TABLE removed_products (
                id INTEGER PRIMARY KEY AUTOINCREMENT, removed_at TEXT, removed_by TEXT,
                barcode TEXT, name TEXT, last_location TEXT, product_json TEXT
            )"""
        )
        original_config = self.config([2])
        self.db.execute(
            """INSERT INTO aisle_layouts
               (aisle, max_section, max_shelf, max_position, config_json, enabled)
               VALUES ('3', '1', '1', '2', ?, 1)""",
            (json.dumps(original_config),),
        )
        self.db.executemany(
            """INSERT INTO products
               (name, barcode, aisle, side, section, shelf, position)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            [
                ("Old one", "111", "3", "Gauche", "1", "1", "1"),
                ("Old two", "222", "3", "Gauche", "1", "1", "2"),
                ("Other aisle", "999", "4", "Droite", "1", "1", "1"),
            ],
        )
        self.db.commit()
        self.app = Flask(__name__)
        self.app.register_blueprint(import_export_bp)

    def tearDown(self):
        self.db.close()

    @staticmethod
    def config(shelves):
        return {
            "sides": {
                "Gauche": {"sections": [{"shelves": shelves, "labels": ["" for _ in shelves]}]},
                "Droite": {"sections": []},
            },
            "facade_a": {"shelves": [], "labels": []},
            "facade_b": {"shelves": [], "labels": []},
            "presentoirs": [],
        }

    def request_recovery(self, payload):
        with patch("routes.import_export.get_db", return_value=self.db), \
             patch("auth.get_db", return_value=self.db), \
             patch("routes.gist._schedule_gist_backup"):
            with self.app.test_client() as client:
                return client.post(
                    "/api/import/aisle-replace", json=payload,
                    headers={"X-User-Name": "recovery-test"},
                )

    def payload(self, expected_count=2):
        config = self.config([3])
        products = []
        for position in range(1, 4):
            products.append({
                "name": f"Recovered {position}", "barcode": f"R{position}",
                "description": f"Description {position}", "image_url": f"https://img/{position}.png",
                "aisle": "3", "side": "Gauche", "section": "1",
                "shelf": "1", "position": str(position), "is_plano": 1,
                "in_stock": 1, "facings": 1,
            })
        current_rows = [dict(row) for row in self.db.execute(
            "SELECT * FROM products WHERE aisle='3' ORDER BY id"
        ).fetchall()]
        fingerprint_rows = [
            [
                row.get("id"), str(row.get("name", "")), str(row.get("barcode", "")),
                str(row.get("side", "")), str(row.get("section", "")),
                str(row.get("shelf", "")), str(row.get("position", "")),
                str(row.get("modified_at", "")),
            ]
            for row in current_rows
        ]
        fingerprint = hashlib.sha256(json.dumps(
            fingerprint_rows, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")).hexdigest()
        return {
            "aisle": "3", "expected_current_count": expected_count,
            "expected_current_fingerprint": fingerprint,
            "confirm": "REPLACE_AISLE_3",
            "layout": {"config": config, "enabled": 1},
            "products": products,
        }

    def test_recovery_is_atomic_and_scoped_to_one_aisle(self):
        response = self.request_recovery(self.payload())
        result = response.get_json()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(result["archived_products"], 2)
        self.assertEqual(result["restored_products"], 3)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM removed_products").fetchone()[0], 2)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM products WHERE aisle='3'").fetchone()[0], 3)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM products WHERE aisle='4'").fetchone()[0], 1)
        recovered = self.db.execute(
            "SELECT description, image_url FROM products WHERE aisle='3' AND position='2'"
        ).fetchone()
        self.assertEqual(recovered["description"], "Description 2")
        self.assertEqual(recovered["image_url"], "https://img/2.png")

    def test_recovery_refuses_stale_expected_count_without_changes(self):
        response = self.request_recovery(self.payload(expected_count=99))

        self.assertEqual(response.status_code, 409)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM products WHERE aisle='3'").fetchone()[0], 2)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM removed_products").fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
