import json
import sqlite3
import unittest
from unittest.mock import patch

from flask import Flask

from routes.layout import layout_bp


class LayoutDeletionTests(unittest.TestCase):
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
                id INTEGER PRIMARY KEY, aisle TEXT, side TEXT, section TEXT,
                shelf TEXT, position TEXT, modified_by TEXT DEFAULT '',
                modified_at TEXT DEFAULT ''
            )"""
        )
        self.db.execute(
            "CREATE UNIQUE INDEX unique_slot ON products(aisle, side, section, shelf, position)"
        )
        self.config = {
            "sides": {
                "Gauche": {"sections": [
                    {"shelves": [3, 4], "labels": ["Bottom", "Top"]},
                    {"shelves": [5], "labels": ["Next section"]},
                ]},
                "Droite": {"sections": []},
            },
            "facade_a": {"shelves": [], "labels": []},
            "facade_b": {"shelves": [], "labels": []},
            "presentoirs": [],
        }
        self.db.execute(
            """INSERT INTO aisle_layouts
               (aisle, max_section, max_shelf, max_position, config_json, enabled)
               VALUES ('A1', '2', '2', '5', ?, 1)""",
            (json.dumps(self.config),),
        )
        self.db.executemany(
            """INSERT INTO products (id, aisle, side, section, shelf, position)
               VALUES (?, 'A1', 'Gauche', ?, ?, '1')""",
            [(1, "1", "1"), (2, "1", "2"), (3, "2", "1")],
        )
        self.db.commit()
        self.app = Flask(__name__)
        self.app.register_blueprint(layout_bp)

    def tearDown(self):
        self.db.close()

    def post(self, path, payload):
        with patch("routes.layout.get_db", return_value=self.db), \
             patch("auth.get_db", return_value=self.db):
            with self.app.test_client() as client:
                return client.post(path, json=payload, headers={"X-User-Name": "tester"})

    def stored_config(self):
        row = self.db.execute(
            "SELECT config_json FROM aisle_layouts WHERE aisle='A1'"
        ).fetchone()
        return json.loads(row["config_json"])

    def test_remove_shelf_updates_structure_and_products_in_one_request(self):
        response = self.post(
            "/api/layout/aisles/A1/remove-shelf",
            {"side": "Gauche", "section": "1", "shelf": "1", "config": self.config},
        )

        result = response.get_json()
        rows = self.db.execute(
            "SELECT id, section, shelf FROM products ORDER BY id"
        ).fetchall()

        self.assertEqual(response.status_code, 200)
        self.assertTrue(result["success"])
        self.assertEqual(result["removed_products"], 1)
        self.assertEqual(result["config"]["sides"]["Gauche"]["sections"][0]["shelves"], [4])
        self.assertEqual(self.stored_config(), result["config"])
        self.assertEqual([(row["id"], row["section"], row["shelf"]) for row in rows], [
            (2, "1", "1"),
            (3, "2", "1"),
        ])

    def test_remove_section_updates_structure_and_products_in_one_request(self):
        response = self.post(
            "/api/layout/aisles/A1/remove-section",
            {"side": "Gauche", "section": "1", "config": self.config},
        )

        result = response.get_json()
        rows = self.db.execute(
            "SELECT id, section, shelf FROM products ORDER BY id"
        ).fetchall()

        self.assertEqual(response.status_code, 200)
        self.assertTrue(result["success"])
        self.assertEqual(result["removed_products"], 2)
        self.assertEqual(result["config"]["sides"]["Gauche"]["sections"], [
            {"shelves": [5], "labels": ["Next section"]}
        ])
        self.assertEqual(self.stored_config(), result["config"])
        self.assertEqual([(row["id"], row["section"], row["shelf"]) for row in rows], [
            (3, "1", "1")
        ])

    def test_empty_shelf_and_section_are_still_deleted(self):
        self.db.execute("DELETE FROM products")
        self.db.commit()

        shelf_response = self.post(
            "/api/layout/aisles/A1/remove-shelf",
            {"side": "Gauche", "section": "1", "shelf": "1", "config": self.config},
        )
        shelf_result = shelf_response.get_json()
        section_response = self.post(
            "/api/layout/aisles/A1/remove-section",
            {"side": "Gauche", "section": "1", "config": shelf_result["config"]},
        )
        section_result = section_response.get_json()

        self.assertEqual(shelf_response.status_code, 200)
        self.assertEqual(shelf_result["removed_products"], 0)
        self.assertEqual(section_response.status_code, 200)
        self.assertEqual(section_result["removed_products"], 0)
        self.assertEqual(section_result["config"]["sides"]["Gauche"]["sections"], [
            {"shelves": [5], "labels": ["Next section"]}
        ])

    def test_facade_shelf_removal_is_atomic_too(self):
        config = json.loads(json.dumps(self.config))
        config["facade_a"] = {"shelves": [2, 3], "labels": ["Low", "High"]}
        self.db.execute(
            "UPDATE aisle_layouts SET config_json=? WHERE aisle='A1'",
            (json.dumps(config),),
        )
        self.db.executemany(
            """INSERT INTO products (id, aisle, side, section, shelf, position)
               VALUES (?, 'A1', 'Façade A', '1', ?, '1')""",
            [(4, "1"), (5, "2")],
        )
        self.db.commit()

        response = self.post(
            "/api/layout/aisles/A1/remove-shelf",
            {"side": "Façade A", "shelf": "1", "config": config},
        )
        result = response.get_json()
        rows = self.db.execute(
            "SELECT id, shelf FROM products WHERE side='Façade A' ORDER BY id"
        ).fetchall()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(result["config"]["facade_a"], {
            "shelves": [3], "labels": ["High"]
        })
        self.assertEqual([(row["id"], row["shelf"]) for row in rows], [(5, "1")])


if __name__ == "__main__":
    unittest.main()
