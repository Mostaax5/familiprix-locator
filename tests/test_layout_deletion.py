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
                aisle TEXT PRIMARY KEY, sort_order INTEGER DEFAULT 0,
                max_section TEXT, max_shelf TEXT,
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
        self.db.execute(
            """CREATE TABLE removed_products (
                id INTEGER PRIMARY KEY AUTOINCREMENT, removed_at TEXT,
                removed_by TEXT, barcode TEXT, name TEXT,
                last_location TEXT, product_json TEXT
            )"""
        )
        for table in (
            "product_identifiers", "product_field_evidence",
            "product_data_issues", "product_aliases",
        ):
            self.db.execute(f"CREATE TABLE {table} (product_id INTEGER)")
        self.db.execute(
            """CREATE TABLE product_relationships (
                source_product_id INTEGER, target_product_id INTEGER
            )"""
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
               (aisle, sort_order, max_section, max_shelf, max_position, config_json, enabled)
               VALUES ('A1', 1, '2', '2', '5', ?, 1)""",
            (json.dumps(self.config),),
        )
        self.db.executemany(
            """INSERT INTO products (id, aisle, side, section, shelf, position)
               VALUES (?, 'A1', 'Gauche', ?, ?, '1')""",
            [(1, "1", "1"), (2, "1", "2"), (3, "2", "1")],
        )
        self.db.commit()
        self.app = Flask(__name__)
        self.app.config.update(TESTING=True, AUTH_TEST_BYPASS=True)
        self.app.register_blueprint(layout_bp)

    def tearDown(self):
        self.db.close()

    def post(self, path, payload):
        with patch("routes.layout.get_db", return_value=self.db), \
             patch("auth.get_db", return_value=self.db):
            with self.app.test_client() as client:
                return client.post(path, json=payload, headers={"X-User-Name": "tester"})

    def put(self, path, payload):
        with patch("routes.layout.get_db", return_value=self.db), \
             patch("auth.get_db", return_value=self.db):
            with self.app.test_client() as client:
                return client.put(path, json=payload, headers={"X-User-Name": "tester"})

    def delete(self, path, payload=None):
        with patch("routes.layout.get_db", return_value=self.db), \
             patch("auth.get_db", return_value=self.db):
            with self.app.test_client() as client:
                return client.delete(
                    path, json=payload, headers={"X-User-Name": "tester"}
                )

    def stored_config(self):
        row = self.db.execute(
            "SELECT config_json FROM aisle_layouts WHERE aisle='A1'"
        ).fetchone()
        return json.loads(row["config_json"])

    def add_second_aisle(self):
        config = {
            "sides": {
                "Gauche": {"sections": [
                    {"shelves": [2], "labels": ["Target"]},
                ]},
                "Droite": {"sections": []},
            },
            "facade_a": {"shelves": [], "labels": []},
            "facade_b": {"shelves": [], "labels": []},
            "presentoirs": [],
        }
        self.db.execute(
            """INSERT INTO aisle_layouts
               (aisle, sort_order, max_section, max_shelf, max_position,
                config_json, enabled, modified_at)
               VALUES ('A2', 2, '1', '1', '2', ?, 1, '')""",
            (json.dumps(config),),
        )
        self.db.execute(
            """INSERT INTO products (id, aisle, side, section, shelf, position)
               VALUES (4, 'A2', 'Gauche', '1', '1', '1')"""
        )
        self.db.commit()
        return config

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
        self.assertEqual(
            self.db.execute("SELECT COUNT(*) FROM removed_products").fetchone()[0], 1
        )

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

    def test_generic_autosave_cannot_delete_products(self):
        stale_config = json.loads(json.dumps(self.config))
        stale_config["sides"]["Gauche"]["sections"] = [
            {"shelves": [1], "labels": [""]}
        ]

        response = self.put(
            "/api/layout/aisles/A1",
            {"config": stale_config, "enabled": True},
        )

        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.get_json()["protected_products"], 2)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM products").fetchone()[0], 3)
        self.assertEqual(self.stored_config(), self.config)

    def test_free_shelf_does_not_make_products_outside_layout(self):
        free_config = json.loads(json.dumps(self.config))
        free_config["sides"]["Gauche"]["sections"][0]["shelves"][0] = 0

        response = self.put(
            "/api/layout/aisles/A1",
            {"config": free_config, "enabled": True},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM products").fetchone()[0], 3)
        self.assertEqual(self.stored_config()["sides"]["Gauche"]["sections"][0]["shelves"][0], 0)

    def test_stale_autosave_cannot_overwrite_a_newer_layout(self):
        self.db.execute(
            "UPDATE aisle_layouts SET modified_at='newer-version' WHERE aisle='A1'"
        )
        self.db.commit()
        changed = json.loads(json.dumps(self.config))
        changed["sides"]["Gauche"]["sections"][0]["shelves"][0] = 9

        response = self.put(
            "/api/layout/aisles/A1",
            {
                "config": changed,
                "enabled": True,
                "expected_modified_at": "older-version",
            },
        )

        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.get_json()["code"], "stale_layout")
        self.assertEqual(self.stored_config(), self.config)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM products").fetchone()[0], 3)

    def test_remove_shelf_ignores_stale_full_layout_snapshot(self):
        stale = json.loads(json.dumps(self.config))
        stale["sides"]["Gauche"]["sections"] = [
            {"shelves": [3], "labels": ["Old phone copy"]}
        ]

        response = self.post(
            "/api/layout/aisles/A1/remove-shelf",
            {"side": "Gauche", "section": "1", "shelf": "1", "config": stale},
        )
        rows = self.db.execute(
            "SELECT id, section, shelf FROM products ORDER BY id"
        ).fetchall()

        self.assertEqual(response.status_code, 200)
        self.assertEqual([(row["id"], row["section"], row["shelf"]) for row in rows], [
            (2, "1", "1"),
            (3, "2", "1"),
        ])
        self.assertEqual(len(self.stored_config()["sides"]["Gauche"]["sections"]), 2)

    def test_stale_explicit_removal_is_refused(self):
        self.db.execute(
            "UPDATE aisle_layouts SET modified_at='server-v2' WHERE aisle='A1'"
        )
        self.db.commit()

        response = self.post(
            "/api/layout/aisles/A1/remove-shelf",
            {
                "side": "Gauche", "section": "1", "shelf": "1",
                "expected_modified_at": "phone-v1",
            },
        )

        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.get_json()["code"], "stale_layout")
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM products").fetchone()[0], 3)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM removed_products").fetchone()[0], 0)
        self.assertEqual(self.stored_config(), self.config)

    def test_delete_aisle_archives_every_product(self):
        response = self.delete("/api/layout/aisles/A1")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM products").fetchone()[0], 0)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM aisle_layouts").fetchone()[0], 0)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM removed_products").fetchone()[0], 3)

    def test_stale_aisle_delete_preserves_structure_and_products(self):
        self.db.execute(
            "UPDATE aisle_layouts SET modified_at='server-v2' WHERE aisle='A1'"
        )
        self.db.commit()

        response = self.delete(
            "/api/layout/aisles/A1", {"expected_modified_at": "phone-v1"}
        )

        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.get_json()["code"], "stale_layout")
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM products").fetchone()[0], 3)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM aisle_layouts").fetchone()[0], 1)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM removed_products").fetchone()[0], 0)

    def test_bulk_move_packs_selected_products_atomically_into_one_section(self):
        response = self.post(
            "/api/layout/products/bulk-move",
            {
                "product_ids": [2, 1],
                "target": {
                    "aisle": "A1", "side": "Gauche", "section": "2",
                    "mode": "section",
                },
                "expected_layout_modified_at": "",
                "expected_products": {"1": "", "2": ""},
            },
        )
        result = response.get_json()
        rows = self.db.execute(
            "SELECT id, section, shelf, position FROM products ORDER BY id"
        ).fetchall()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(result["moved_products"], 2)
        self.assertEqual(
            [(item["id"], item["shelf"], item["position"]) for item in result["product_updates"]],
            [(1, "1", "2"), (2, "1", "3")],
        )
        self.assertEqual(
            [(row["id"], row["section"], row["shelf"], row["position"]) for row in rows],
            [(1, "2", "1", "2"), (2, "2", "1", "3"), (3, "2", "1", "1")],
        )
        self.assertEqual(self.stored_config(), self.config)

    def test_bulk_move_with_insufficient_space_changes_nothing(self):
        self.db.executemany(
            """INSERT INTO products (id, aisle, side, section, shelf, position)
               VALUES (?, 'A1', 'Gauche', '1', '1', ?)""",
            [(4, "2"), (5, "3")],
        )
        self.db.commit()
        before = [tuple(row) for row in self.db.execute(
            "SELECT id, aisle, side, section, shelf, position FROM products ORDER BY id"
        ).fetchall()]

        response = self.post(
            "/api/layout/products/bulk-move",
            {
                "product_ids": [1, 2],
                "target": {
                    "aisle": "A1", "side": "Gauche", "section": "1",
                    "shelf": "1", "mode": "shelf",
                },
            },
        )
        after = [tuple(row) for row in self.db.execute(
            "SELECT id, aisle, side, section, shelf, position FROM products ORDER BY id"
        ).fetchall()]

        self.assertEqual(response.status_code, 409)
        self.assertIn("Espace insuffisant", response.get_json()["error"])
        self.assertEqual(after, before)

    def test_bulk_move_rejects_a_stale_selected_product(self):
        self.db.execute("UPDATE products SET modified_at='server-v2' WHERE id=1")
        self.db.commit()

        response = self.post(
            "/api/layout/products/bulk-move",
            {
                "product_ids": [1],
                "target": {
                    "aisle": "A1", "side": "Gauche", "section": "2",
                    "mode": "section",
                },
                "expected_products": {"1": "phone-v1"},
            },
        )

        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.get_json()["code"], "stale_products")
        row = self.db.execute(
            "SELECT section, shelf, position FROM products WHERE id=1"
        ).fetchone()
        self.assertEqual(tuple(row), ("1", "1", "1"))

    def test_bulk_delete_removes_only_selected_products_and_keeps_structure(self):
        response = self.post(
            "/api/layout/products/bulk-delete",
            {"product_ids": [1, 3], "expected_products": {"1": "", "3": ""}},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["removed_products"], 2)
        self.assertEqual(
            [row[0] for row in self.db.execute("SELECT id FROM products ORDER BY id").fetchall()],
            [2],
        )
        self.assertEqual(self.stored_config(), self.config)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM removed_products").fetchone()[0], 2)

    def test_bulk_delete_with_a_stale_selection_deletes_nothing(self):
        response = self.post(
            "/api/layout/products/bulk-delete",
            {"product_ids": [1, 999999]},
        )

        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.get_json()["code"], "stale_products")
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM products").fetchone()[0], 3)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM removed_products").fetchone()[0], 0)

    def test_bulk_delete_scope_clears_current_shelf_despite_product_changes(self):
        self.db.execute(
            """INSERT INTO products
               (id, aisle, side, section, shelf, position, modified_at)
               VALUES (4, 'A1', 'Gauche', '1', '1', '2', 'enriched-after-page-load')"""
        )
        self.db.execute(
            "UPDATE products SET modified_at='also-enriched' WHERE id=1"
        )
        self.db.commit()

        response = self.post(
            "/api/layout/products/bulk-delete",
            {
                "scope": {
                    "kind": "shelf", "aisle": "A1", "side": "Gauche",
                    "section": "1", "shelf": "1",
                },
                "product_ids": [999999],
                "expected_products": {"1": "stale-phone-version"},
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["removed_products"], 2)
        self.assertEqual(response.get_json()["deleted_product_ids"], [1, 4])
        self.assertEqual(
            [row[0] for row in self.db.execute(
                "SELECT id FROM products ORDER BY id"
            ).fetchall()],
            [2, 3],
        )
        self.assertEqual(self.stored_config(), self.config)
        self.assertEqual(
            self.db.execute("SELECT COUNT(*) FROM removed_products").fetchone()[0],
            2,
        )

        repeated = self.post(
            "/api/layout/products/bulk-delete",
            {"scope": {
                "kind": "shelf", "aisle": "A1", "side": "Gauche",
                "section": "1", "shelf": "1",
            }},
        )
        self.assertEqual(repeated.status_code, 200)
        self.assertEqual(repeated.get_json()["removed_products"], 0)

    def test_bulk_delete_scope_rejects_incomplete_coordinates(self):
        response = self.post(
            "/api/layout/products/bulk-delete",
            {"scope": {
                "kind": "shelf", "aisle": "A1", "side": "Gauche",
                "section": "1",
            }},
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(
            self.db.execute("SELECT COUNT(*) FROM products").fetchone()[0], 3
        )
        self.assertEqual(
            self.db.execute("SELECT COUNT(*) FROM removed_products").fetchone()[0],
            0,
        )

    def test_reorder_aisles_changes_only_the_persistent_store_order(self):
        self.add_second_aisle()
        before_products = [tuple(row) for row in self.db.execute(
            "SELECT id, aisle, side, section, shelf, position FROM products ORDER BY id"
        ).fetchall()]

        response = self.post(
            "/api/layout/aisles/reorder",
            {
                "ordered_aisles": ["A2", "A1"],
                "expected_layouts": {"A1": "", "A2": ""},
            },
        )

        order = [tuple(row) for row in self.db.execute(
            "SELECT aisle, sort_order FROM aisle_layouts ORDER BY sort_order"
        ).fetchall()]
        after_products = [tuple(row) for row in self.db.execute(
            "SELECT id, aisle, side, section, shelf, position FROM products ORDER BY id"
        ).fetchall()]
        self.assertEqual(response.status_code, 200)
        self.assertEqual(order, [("A2", 1), ("A1", 2)])
        self.assertEqual(after_products, before_products)

    def test_move_section_between_aisles_keeps_its_tablets_and_products(self):
        target_config = self.add_second_aisle()

        response = self.post(
            "/api/layout/structure/move-section",
            {
                "source": {"aisle": "A1", "side": "Gauche", "index": 0},
                "target": {"aisle": "A2", "side": "Gauche", "index": 1},
                "expected_layouts": {"A1": "", "A2": ""},
            },
        )

        result = response.get_json()
        rows = [tuple(row) for row in self.db.execute(
            "SELECT id, aisle, side, section, shelf FROM products ORDER BY id"
        ).fetchall()]
        stored_a1 = json.loads(self.db.execute(
            "SELECT config_json FROM aisle_layouts WHERE aisle='A1'"
        ).fetchone()[0])
        stored_a2 = json.loads(self.db.execute(
            "SELECT config_json FROM aisle_layouts WHERE aisle='A2'"
        ).fetchone()[0])
        self.assertEqual(response.status_code, 200)
        self.assertEqual(stored_a1["sides"]["Gauche"]["sections"], [
            {"shelves": [5], "labels": ["Next section"]}
        ])
        self.assertEqual(stored_a2["sides"]["Gauche"]["sections"], [
            target_config["sides"]["Gauche"]["sections"][0],
            {"shelves": [3, 4], "labels": ["Bottom", "Top"]},
        ])
        self.assertEqual(rows, [
            (1, "A2", "Gauche", "2", "1"),
            (2, "A2", "Gauche", "2", "2"),
            (3, "A1", "Gauche", "1", "1"),
            (4, "A2", "Gauche", "1", "1"),
        ])
        self.assertEqual({item["id"] for item in result["product_updates"]}, {1, 2, 3})

    def test_move_shelf_reorders_labels_and_products_in_one_transaction(self):
        response = self.post(
            "/api/layout/structure/move-shelf",
            {
                "source": {
                    "aisle": "A1", "side": "Gauche",
                    "section_index": 0, "index": 0,
                },
                "target": {
                    "aisle": "A1", "side": "Gauche",
                    "section_index": 0, "index": 2,
                },
                "expected_layouts": {"A1": ""},
            },
        )

        rows = [tuple(row) for row in self.db.execute(
            "SELECT id, section, shelf FROM products ORDER BY id"
        ).fetchall()]
        first_section = self.stored_config()["sides"]["Gauche"]["sections"][0]
        self.assertEqual(response.status_code, 200)
        self.assertEqual(first_section, {
            "shelves": [4, 3], "labels": ["Top", "Bottom"]
        })
        self.assertEqual(rows, [(1, "1", "2"), (2, "1", "1"), (3, "2", "1")])

    def test_move_section_reorders_products_and_shapes_together(self):
        response = self.post(
            "/api/layout/structure/move-section",
            {
                "source": {"aisle": "A1", "side": "Gauche", "index": 0},
                "target": {"aisle": "A1", "side": "Gauche", "index": 2},
                "expected_layouts": {"A1": ""},
            },
        )

        sections = self.stored_config()["sides"]["Gauche"]["sections"]
        rows = [tuple(row) for row in self.db.execute(
            "SELECT id, section, shelf FROM products ORDER BY id"
        ).fetchall()]
        self.assertEqual(response.status_code, 200)
        self.assertEqual(sections, [
            {"shelves": [5], "labels": ["Next section"]},
            {"shelves": [3, 4], "labels": ["Bottom", "Top"]},
        ])
        self.assertEqual(rows, [(1, "2", "1"), (2, "2", "2"), (3, "1", "1")])

    def test_move_shelf_can_cross_sections_without_losing_products(self):
        response = self.post(
            "/api/layout/structure/move-shelf",
            {
                "source": {
                    "aisle": "A1", "side": "Gauche",
                    "section_index": 0, "index": 1,
                },
                "target": {
                    "aisle": "A1", "side": "Gauche",
                    "section_index": 1, "index": 1,
                },
                "expected_layouts": {"A1": ""},
            },
        )

        sections = self.stored_config()["sides"]["Gauche"]["sections"]
        rows = [tuple(row) for row in self.db.execute(
            "SELECT id, section, shelf FROM products ORDER BY id"
        ).fetchall()]
        self.assertEqual(response.status_code, 200)
        self.assertEqual(sections, [
            {"shelves": [3], "labels": ["Bottom"]},
            {"shelves": [5, 4], "labels": ["Next section", "Top"]},
        ])
        self.assertEqual(rows, [(1, "1", "1"), (2, "2", "2"), (3, "2", "1")])

    def test_stale_structure_move_rolls_back_everything(self):
        self.db.execute(
            "UPDATE aisle_layouts SET modified_at='server-v2' WHERE aisle='A1'"
        )
        self.db.commit()
        before_products = [tuple(row) for row in self.db.execute(
            "SELECT id, aisle, side, section, shelf, position FROM products ORDER BY id"
        ).fetchall()]

        response = self.post(
            "/api/layout/structure/move-shelf",
            {
                "source": {
                    "aisle": "A1", "side": "Gauche",
                    "section_index": 0, "index": 0,
                },
                "target": {
                    "aisle": "A1", "side": "Gauche",
                    "section_index": 0, "index": 2,
                },
                "expected_layouts": {"A1": "phone-v1"},
            },
        )

        after_products = [tuple(row) for row in self.db.execute(
            "SELECT id, aisle, side, section, shelf, position FROM products ORDER BY id"
        ).fetchall()]
        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.get_json()["code"], "stale_layout")
        self.assertEqual(after_products, before_products)
        self.assertEqual(self.stored_config(), self.config)


if __name__ == "__main__":
    unittest.main()
