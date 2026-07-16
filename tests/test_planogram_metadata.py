import json
import sqlite3
import unittest
from unittest.mock import patch

from flask import Flask

from routes.layout import build_default_layout_config
from routes.products import (
    build_reference_metadata_index,
    plan_planogram_flow,
    planogram_metadata,
    persist_image_for_barcode,
    products_bp,
    reference_metadata_for_barcode,
    sync_reference_metadata_to_products,
)


class PlanogramMetadataTests(unittest.TestCase):
    def make_db(self):
        db = sqlite3.connect(":memory:")
        db.row_factory = sqlite3.Row
        db.execute(
            """CREATE TABLE product_reference (
                barcode TEXT PRIMARY KEY,
                brand TEXT DEFAULT '',
                description TEXT DEFAULT '',
                image_url TEXT DEFAULT '',
                product_code TEXT DEFAULT '',
                updated_at TEXT DEFAULT ''
            )"""
        )
        db.execute(
            """CREATE TABLE products (
                id INTEGER PRIMARY KEY,
                barcode TEXT DEFAULT '',
                brand TEXT DEFAULT '',
                description TEXT DEFAULT '',
                image_url TEXT DEFAULT '',
                product_code TEXT DEFAULT '',
                modified_at TEXT DEFAULT ''
            )"""
        )
        return db

    def test_found_image_is_saved_for_placed_product_and_future_reimports(self):
        db = self.make_db()
        db.execute(
            "INSERT INTO product_reference (barcode) VALUES ('0063848966068')"
        )
        db.execute(
            "INSERT INTO products (id, barcode) VALUES (1, '063848966068')"
        )

        changed = persist_image_for_barcode(
            db, "063848966068", "https://img.test/razor.jpg",
            now="2026-07-16T13:00:00+00:00",
        )

        placed = db.execute("SELECT image_url FROM products WHERE id=1").fetchone()[0]
        reference = db.execute(
            "SELECT image_url FROM product_reference WHERE barcode='0063848966068'"
        ).fetchone()[0]
        self.assertEqual(changed, 2)
        self.assertEqual(placed, "https://img.test/razor.jpg")
        self.assertEqual(reference, "https://img.test/razor.jpg")

    def make_plan_db(self):
        db = self.make_db()
        db.execute("CREATE TABLE users (username TEXT PRIMARY KEY, last_seen TEXT)")
        db.execute(
            """CREATE TABLE aisle_layouts (
                aisle TEXT PRIMARY KEY, max_section TEXT, max_shelf TEXT,
                max_position TEXT, config_json TEXT, enabled INTEGER,
                modified_by TEXT DEFAULT '', modified_at TEXT DEFAULT ''
            )"""
        )
        db.execute("DROP TABLE products")
        db.execute(
            """CREATE TABLE products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT DEFAULT '', brand TEXT DEFAULT '', description TEXT DEFAULT '',
                image_url TEXT DEFAULT '', source_url TEXT DEFAULT '', search_terms TEXT DEFAULT '',
                usage_notes TEXT DEFAULT '', alternative_suggestions TEXT DEFAULT '',
                barcode TEXT DEFAULT '', product_code TEXT DEFAULT '', facings INTEGER DEFAULT 1,
                aisle TEXT DEFAULT '', side TEXT DEFAULT '', section TEXT DEFAULT '1',
                shelf TEXT DEFAULT '', position TEXT DEFAULT '', is_plano INTEGER DEFAULT 0,
                in_stock INTEGER DEFAULT 1, flipped_label INTEGER DEFAULT 0,
                created_by TEXT DEFAULT '', created_at TEXT DEFAULT '',
                modified_by TEXT DEFAULT '', modified_at TEXT DEFAULT ''
            )"""
        )
        db.execute(
            """CREATE TABLE planogram_imports (
                id INTEGER PRIMARY KEY AUTOINCREMENT, created_at TEXT, store TEXT,
                employee TEXT, plano_name TEXT, plano_number TEXT, plano_version TEXT,
                aisle TEXT, side TEXT, section TEXT, tablette_start INTEGER,
                tablette_end INTEGER, imported INTEGER, skipped INTEGER
            )"""
        )
        db.execute(
            """CREATE TABLE removed_products (
                id INTEGER PRIMARY KEY AUTOINCREMENT, removed_at TEXT,
                removed_by TEXT, barcode TEXT, name TEXT, last_location TEXT,
                product_json TEXT
            )"""
        )
        config = build_default_layout_config(1, 1, 3)
        db.execute(
            """INSERT INTO aisle_layouts
               (aisle, max_section, max_shelf, max_position, config_json, enabled)
               VALUES ('1', '1', '1', '2', ?, 1)""",
            (json.dumps(config),),
        )
        db.commit()
        return db

    def test_catalogue_metadata_backfills_equivalent_upcs_without_overwriting_edits(self):
        db = self.make_db()
        db.execute(
            """INSERT INTO product_reference
               (barcode, brand, description, image_url, product_code)
               VALUES (?, ?, ?, ?, ?)""",
            ("0123456789012", "Example", "Reference description", "https://img.test/p.jpg", "F123"),
        )
        db.execute("INSERT INTO products (id, barcode) VALUES (1, '123456789012')")
        db.execute(
            "INSERT INTO products (id, barcode, description) VALUES (2, '0123456789012', 'Manual description')"
        )
        db.execute("INSERT INTO products (id, barcode) VALUES (3, '999999999999')")

        linked = sync_reference_metadata_to_products(db, now="2026-07-12T00:00:00+00:00")

        first = dict(db.execute("SELECT * FROM products WHERE id=1").fetchone())
        second = dict(db.execute("SELECT * FROM products WHERE id=2").fetchone())
        third = dict(db.execute("SELECT * FROM products WHERE id=3").fetchone())
        self.assertEqual(linked, 2)
        self.assertEqual(first["description"], "Reference description")
        self.assertEqual(first["image_url"], "https://img.test/p.jpg")
        self.assertEqual(first["product_code"], "F123")
        self.assertEqual(second["description"], "Manual description")
        self.assertEqual(second["brand"], "Example")
        self.assertEqual(third["description"], "")

    def test_planogram_import_preserves_same_upc_metadata_and_drops_stale_other_upc_data(self):
        reference = {
            "brand": "New Brand",
            "description": "New product description",
            "image_url": "https://img.test/new.jpg",
            "product_code": "REF1",
        }
        existing = {
            "barcode": "123456789012",
            "brand": "Edited Brand",
            "description": "Edited description",
            "image_url": "https://img.test/edited.jpg",
            "product_code": "OLD",
            "source_url": "https://source.test/item",
            "usage_notes": "Employee note",
            "alternative_suggestions": "Alternative",
        }

        same = planogram_metadata(existing, reference, "0123456789012", product_code="PDF1")
        changed = planogram_metadata(existing, reference, "777777777777", product_code="PDF2")

        self.assertEqual(same["description"], "Edited description")
        self.assertEqual(same["usage_notes"], "Employee note")
        self.assertEqual(same["product_code"], "PDF1")
        self.assertEqual(changed["description"], "New product description")
        self.assertEqual(changed["usage_notes"], "")
        self.assertEqual(changed["source_url"], "")
        self.assertEqual(changed["product_code"], "PDF2")

    def test_reference_index_prefers_the_richest_equivalent_upc_row(self):
        db = self.make_db()
        db.execute(
            """INSERT INTO product_reference (barcode, brand, product_code)
               VALUES ('123456789012', 'Thin', 'F999')"""
        )
        db.execute(
            """INSERT INTO product_reference
               (barcode, brand, description, image_url) VALUES
               ('0123456789012', 'Rich', 'Useful description', 'https://img.test/rich.jpg')"""
        )

        index = build_reference_metadata_index(db)
        reference = reference_metadata_for_barcode(index, "123456789012")

        self.assertEqual(reference["brand"], "Rich")
        self.assertEqual(reference["description"], "Useful description")
        self.assertEqual(reference["product_code"], "F999")

    def test_bulk_planogram_import_attaches_reference_metadata_and_clears_old_upc_data(self):
        db = self.make_plan_db()
        db.execute(
            """INSERT INTO product_reference
               (barcode, brand, description, image_url, product_code)
               VALUES ('0123456789012', 'Reference Brand', 'Reference description',
                       'https://img.test/ref.jpg', 'REF123')"""
        )
        app = Flask(__name__)
        app.register_blueprint(products_bp)
        base_payload = {
            "aisle": "1", "side": "Gauche", "start_section": 1,
            "start_tablette": 1, "tablette_start": 1, "tablette_end": 1,
            "replace_existing": True,
        }

        with patch("routes.products.get_db", return_value=db), \
             patch("auth.get_db", return_value=db), \
             patch("routes.products.schedule_image_fill"), \
             patch("routes.gist._schedule_gist_backup"):
            with app.test_client() as client:
                first_response = client.post("/api/products/bulk-import", json={
                    **base_payload,
                    "products": [{
                        "tablette": 1, "position": 1, "barcode": "123456789012",
                        "name": "PLAN NAME", "code_familiprix": "", "facings": 1,
                    }],
                })
                after_first = dict(db.execute("SELECT * FROM products").fetchone())
                second_response = client.post("/api/products/bulk-import", json={
                    **base_payload,
                    "products": [{
                        "tablette": 1, "position": 1, "barcode": "777777777777",
                        "name": "OTHER PRODUCT", "code_familiprix": "NEW777", "facings": 1,
                    }],
                })
                after_second = dict(db.execute("SELECT * FROM products").fetchone())

        self.assertEqual(first_response.status_code, 200)
        first_result = first_response.get_json()
        self.assertEqual(first_result["errors"], 0)
        self.assertEqual(first_result["layout"]["aisle"], "1")
        self.assertEqual(first_result["products"][0]["barcode"], "123456789012")
        self.assertEqual(first_result["products"][0]["description"], "Reference description")
        self.assertEqual(after_first["description"], "Reference description")
        self.assertEqual(after_first["image_url"], "https://img.test/ref.jpg")
        self.assertEqual(after_first["brand"], "Reference Brand")
        self.assertEqual(after_first["product_code"], "REF123")
        self.assertEqual(second_response.status_code, 200)
        self.assertEqual(second_response.get_json()["errors"], 0)
        self.assertEqual(after_second["barcode"], "777777777777")
        self.assertEqual(after_second["description"], "")
        self.assertEqual(after_second["image_url"], "")
        self.assertEqual(after_second["product_code"], "NEW777")
        db.close()

    def test_bulk_import_reports_overflow_shelves_and_products_separately(self):
        db = self.make_plan_db()
        app = Flask(__name__)
        app.register_blueprint(products_bp)
        payload = {
            "aisle": "1", "side": "Gauche", "start_section": 1,
            "start_tablette": 1, "tablette_start": 1, "tablette_end": 2,
            "replace_existing": True,
            "products": [
                {"tablette": 1, "position": 1, "barcode": "111", "name": "FIRST"},
                {"tablette": 2, "position": 1, "barcode": "222", "name": "SECOND"},
                {"tablette": 2, "position": 2, "barcode": "333", "name": "THIRD"},
            ],
        }

        with patch("routes.products.get_db", return_value=db), \
             patch("auth.get_db", return_value=db), \
             patch("routes.products.schedule_image_fill"), \
             patch("routes.gist._schedule_gist_backup"):
            with app.test_client() as client:
                response = client.post("/api/products/bulk-import", json=payload)

        result = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(result["imported"], 1)
        self.assertEqual(result["overflow_shelves"], 1)
        self.assertEqual(result["overflow_products"], 2)
        self.assertEqual(result["skipped"], 2)
        db.close()

    def test_bulk_import_reports_non_stock_products_excluded_by_filter(self):
        db = self.make_plan_db()
        app = Flask(__name__)
        app.register_blueprint(products_bp)
        payload = {
            "aisle": "1", "side": "Gauche", "start_section": 1,
            "start_tablette": 1, "tablette_start": 1, "tablette_end": 1,
            "replace_existing": True, "skip_non_stock": True,
            "products": [
                {
                    "tablette": 1, "position": 1, "barcode": "111111111111",
                    "name": "IN STOCK", "en_stock": True,
                },
                {
                    "tablette": 1, "position": 2, "barcode": "222222222222",
                    "name": "OUT OF STOCK", "en_stock": False,
                },
            ],
        }

        with patch("routes.products.get_db", return_value=db), \
             patch("auth.get_db", return_value=db), \
             patch("routes.products.schedule_image_fill"), \
             patch("routes.gist._schedule_gist_backup"):
            with app.test_client() as client:
                response = client.post("/api/products/bulk-import", json=payload)

        result = response.get_json()
        placed = db.execute("SELECT name, position FROM products").fetchall()
        history = db.execute(
            "SELECT imported, skipped FROM planogram_imports ORDER BY id DESC LIMIT 1"
        ).fetchone()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(result["selected_products"], 2)
        self.assertEqual(result["filtered_non_stock"], 1)
        self.assertEqual(result["imported"], 1)
        self.assertEqual(result["skipped"], 1)
        self.assertEqual([tuple(row) for row in placed], [("IN STOCK", "1")])
        self.assertEqual(tuple(history), (1, 1))
        db.close()

    def test_bulk_replace_removes_old_products_from_gaps_on_target_tablet(self):
        db = self.make_plan_db()
        db.executemany(
            """INSERT INTO products
               (name, barcode, aisle, side, section, shelf, position)
               VALUES (?, ?, '1', 'Gauche', '1', '1', ?)""",
            [
                ("OLD AT ONE", "111111111111", "1"),
                ("OLD IN GAP", "222222222222", "2"),
                ("OLD AT THREE", "333333333333", "3"),
            ],
        )
        db.execute(
            "UPDATE products SET image_url='https://img.test/moved.jpg', "
            "description='Enriched description' "
            "WHERE barcode='222222222222'"
        )
        app = Flask(__name__)
        app.register_blueprint(products_bp)
        payload = {
            "aisle": "1", "side": "Gauche", "start_section": 1,
            "start_tablette": 1, "tablette_start": 1, "tablette_end": 1,
            "replace_existing": True,
            "products": [
                {"tablette": 1, "position": 1, "barcode": "444444444444", "name": "NEW ONE"},
                {"tablette": 1, "position": 3, "barcode": "222222222222", "name": "NEW THREE"},
            ],
        }

        with patch("routes.products.get_db", return_value=db), \
             patch("auth.get_db", return_value=db), \
             patch("routes.products.schedule_image_fill"), \
             patch("routes.gist._schedule_gist_backup"):
            with app.test_client() as client:
                response = client.post("/api/products/bulk-import", json=payload)

        result = response.get_json()
        remaining = [
            tuple(row) for row in db.execute(
                "SELECT name, position FROM products ORDER BY CAST(position AS INTEGER)"
            ).fetchall()
        ]
        archived = [
            row[0] for row in db.execute(
                "SELECT name FROM removed_products ORDER BY id"
            ).fetchall()
        ]

        self.assertEqual(response.status_code, 200)
        self.assertEqual(result["errors"], 0)
        self.assertEqual(result["replaced_removed"], 3)
        self.assertEqual(remaining, [("NEW ONE", "1"), ("NEW THREE", "3")])
        self.assertEqual(archived, ["OLD AT ONE", "OLD IN GAP", "OLD AT THREE"])
        moved = db.execute("SELECT * FROM products WHERE position='3'").fetchone()
        self.assertEqual(moved["image_url"], "https://img.test/moved.jpg")
        self.assertEqual(moved["description"], "Enriched description")
        db.close()

    def test_bulk_replace_rejects_duplicate_destinations_without_touching_old_products(self):
        db = self.make_plan_db()
        db.execute(
            """INSERT INTO products
               (name, barcode, aisle, side, section, shelf, position)
               VALUES ('OLD PRODUCT', '111', '1', 'Gauche', '1', '1', '1')"""
        )
        db.commit()
        app = Flask(__name__)
        app.register_blueprint(products_bp)
        payload = {
            "aisle": "1", "side": "Gauche", "start_section": 1,
            "start_tablette": 1, "tablette_start": 1, "tablette_end": 1,
            "replace_existing": True,
            "products": [
                {"tablette": 1, "position": 1, "barcode": "222", "name": "NEW ONE"},
                {"tablette": 1, "position": 1, "barcode": "333", "name": "NEW TWO"},
            ],
        }

        with patch("routes.products.get_db", return_value=db), \
             patch("auth.get_db", return_value=db), \
             patch("routes.products.schedule_image_fill"), \
             patch("routes.gist._schedule_gist_backup"):
            with app.test_client() as client:
                response = client.post("/api/products/bulk-import", json=payload)

        self.assertEqual(response.status_code, 409)
        self.assertEqual(
            [tuple(row) for row in db.execute("SELECT name, barcode FROM products").fetchall()],
            [("OLD PRODUCT", "111")],
        )
        self.assertEqual(db.execute("SELECT COUNT(*) FROM removed_products").fetchone()[0], 0)
        self.assertEqual(db.execute("SELECT COUNT(*) FROM planogram_imports").fetchone()[0], 0)
        db.close()

    def test_bulk_replace_rolls_back_old_products_when_an_insert_fails(self):
        class FailingProductInsertDb:
            def __init__(self, connection):
                self.connection = connection

            def execute(self, query, params=()):
                normalized = " ".join(str(query).lower().split())
                if normalized.startswith("insert into products"):
                    raise RuntimeError("simulated insert failure")
                return self.connection.execute(query, tuple(params or ()))

            def commit(self):
                self.connection.commit()

            def rollback(self):
                self.connection.rollback()

        connection = self.make_plan_db()
        connection.execute(
            """INSERT INTO products
               (name, barcode, aisle, side, section, shelf, position)
               VALUES ('OLD PRODUCT', '111', '1', 'Gauche', '1', '1', '1')"""
        )
        connection.commit()
        db = FailingProductInsertDb(connection)
        app = Flask(__name__)
        app.register_blueprint(products_bp)
        payload = {
            "aisle": "1", "side": "Gauche", "start_section": 1,
            "start_tablette": 1, "tablette_start": 1, "tablette_end": 1,
            "replace_existing": True,
            "products": [
                {"tablette": 1, "position": 1, "barcode": "222", "name": "NEW PRODUCT"},
            ],
        }

        with patch("routes.products.get_db", return_value=db), \
             patch("auth.get_db", return_value=db), \
             patch("routes.products.schedule_image_fill"), \
             patch("routes.gist._schedule_gist_backup"):
            with app.test_client() as client:
                response = client.post("/api/products/bulk-import", json=payload)

        self.assertEqual(response.status_code, 500)
        self.assertEqual(
            [tuple(row) for row in connection.execute("SELECT name, barcode FROM products").fetchall()],
            [("OLD PRODUCT", "111")],
        )
        self.assertEqual(connection.execute("SELECT COUNT(*) FROM removed_products").fetchone()[0], 0)
        self.assertEqual(connection.execute("SELECT COUNT(*) FROM planogram_imports").fetchone()[0], 0)
        connection.close()

    def test_bulk_import_refuses_a_stale_layout_version(self):
        db = self.make_plan_db()
        db.execute("UPDATE aisle_layouts SET modified_at='server-v2' WHERE aisle='1'")
        db.execute(
            """INSERT INTO products
               (name, barcode, aisle, side, section, shelf, position)
               VALUES ('OLD PRODUCT', '111', '1', 'Gauche', '1', '1', '1')"""
        )
        db.commit()
        app = Flask(__name__)
        app.register_blueprint(products_bp)
        payload = {
            "aisle": "1", "side": "Gauche", "start_section": 1,
            "start_tablette": 1, "tablette_start": 1, "tablette_end": 1,
            "replace_existing": True,
            "expected_layout_modified_at": "phone-v1",
            "products": [
                {"tablette": 1, "position": 1, "barcode": "222", "name": "NEW PRODUCT"},
            ],
        }

        with patch("routes.products.get_db", return_value=db), \
             patch("auth.get_db", return_value=db), \
             patch("routes.products.schedule_image_fill"), \
             patch("routes.gist._schedule_gist_backup"):
            with app.test_client() as client:
                response = client.post("/api/products/bulk-import", json=payload)

        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.get_json()["code"], "stale_layout")
        self.assertEqual(db.execute("SELECT name FROM products").fetchone()[0], "OLD PRODUCT")
        self.assertEqual(db.execute("SELECT COUNT(*) FROM removed_products").fetchone()[0], 0)
        db.close()

    def test_planogram_flow_uses_manual_section_shelf_counts_as_boundaries(self):
        config = {
            "sides": {
                "Gauche": {"sections": [{"shelves": [8]}]},
                "Droite": {"sections": [
                    {"shelves": [8] * 7},
                    {"shelves": [8] * 7},
                    {"shelves": [8] * 7},
                ]},
            },
            "facade_a": {"shelves": [], "labels": []},
            "facade_b": {"shelves": [], "labels": []},
            "presentoirs": [],
        }
        lines = [
            {"tablette": shelf, "position": 1, "p": {"name": f"SHELF {shelf}"}}
            for shelf in range(1, 23)
        ]

        placements, overflow = plan_planogram_flow(
            config, "Droite", start_section=1, start_tablette=1, lines=lines
        )
        destinations = [(section, shelf) for section, shelf, _position, _line in placements]

        self.assertEqual(destinations[0], (1, 1))
        self.assertEqual(destinations[6], (1, 7))
        self.assertEqual(destinations[7], (2, 1))
        self.assertEqual(destinations[13], (2, 7))
        self.assertEqual(destinations[14], (3, 1))
        self.assertEqual(destinations[20], (3, 7))
        self.assertEqual(len(destinations), 21)
        self.assertEqual(overflow, 1)

    def test_cote_a_descends_sections_without_reversing_product_positions(self):
        def config():
            return {
                "sides": {
                    "Gauche": {"sections": [{"shelves": [3]} for _ in range(9)]},
                    "Droite": {"sections": [{"shelves": [3]} for _ in range(9)]},
                },
                "facade_a": {"shelves": [], "labels": []},
                "facade_b": {"shelves": [], "labels": []},
                "presentoirs": [],
            }

        lines = [
            {"tablette": 1, "position": 1, "p": {"name": "SHELF 1 P1"}},
            {"tablette": 1, "position": 2, "p": {"name": "SHELF 1 P2"}},
            {"tablette": 2, "position": 1, "p": {"name": "SHELF 2 P1"}},
            {"tablette": 2, "position": 2, "p": {"name": "SHELF 2 P2"}},
        ]

        cote_a, overflow_a = plan_planogram_flow(
            config(), "Gauche", start_section=9, start_tablette=1, lines=lines
        )
        cote_b, overflow_b = plan_planogram_flow(
            config(), "Droite", start_section=8, start_tablette=1, lines=lines
        )

        self.assertEqual(
            [(section, shelf, position) for section, shelf, position, _line in cote_a],
            [(9, 1, 1), (9, 1, 2), (8, 1, 1), (8, 1, 2)],
        )
        self.assertEqual(
            [(section, shelf, position) for section, shelf, position, _line in cote_b],
            [(8, 1, 1), (8, 1, 2), (9, 1, 1), (9, 1, 2)],
        )
        self.assertEqual((overflow_a, overflow_b), (0, 0))


if __name__ == "__main__":
    unittest.main()
