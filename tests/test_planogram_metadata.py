import json
import sqlite3
import unittest
from unittest.mock import patch

from flask import Flask
from database import DatabaseConnection, init_sqlite_db
from product_data import record_field_evidence, upsert_reference_candidate

from routes import products as products_module
from routes.layout import build_default_layout_config
from routes.products import (
    _process_planogram_post_import_job,
    build_reference_metadata_index,
    plan_planogram_flow,
    planogram_metadata,
    persist_image_for_barcode,
    products_bp,
    rank_reference_for_query,
    reference_metadata_for_barcode,
    sync_reference_metadata_to_products,
)


class PlanogramMetadataTests(unittest.TestCase):
    def setUp(self):
        self.post_import_patcher = patch(
            "routes.products.schedule_planogram_post_import",
            return_value=True,
        )
        self.schedule_post_import = self.post_import_patcher.start()
        self.addCleanup(self.post_import_patcher.stop)

    def make_test_app(self):
        app = Flask(__name__)
        app.config.update(TESTING=True, AUTH_TEST_BYPASS=True)
        app.register_blueprint(products_bp)
        return app

    def make_db(self):
        raw = sqlite3.connect(":memory:")
        raw.row_factory = sqlite3.Row
        db = DatabaseConnection(raw, "sqlite")
        init_sqlite_db(db)
        return db

    def test_found_image_is_saved_for_placed_product_and_future_reimports(self):
        db = self.make_db()
        db.execute(
            "INSERT INTO product_reference (barcode) VALUES ('0063848966068')"
        )
        db.execute(
            """INSERT INTO products (id, name, barcode, aisle, side, section, shelf, position)
               VALUES (1, 'Razor', '063848966068', '1', 'Gauche', '1', '1', '1')"""
        )

        changed = persist_image_for_barcode(
            db, "063848966068", "https://img.test/razor.jpg",
            now="2026-07-16T13:00:00+00:00",
            source="Manufacturer exact product page",
            source_url="https://manufacturer.test/063848966068",
        )

        placed = db.execute("SELECT image_url FROM products WHERE id=1").fetchone()[0]
        reference = db.execute(
            "SELECT image_url FROM product_reference WHERE barcode='0063848966068'"
        ).fetchone()[0]
        self.assertEqual(changed, 2)
        self.assertEqual(placed, "https://img.test/razor.jpg")
        self.assertEqual(reference, "https://img.test/razor.jpg")

    def test_visible_missing_image_moves_to_front_of_background_queue(self):
        with products_module._IMAGE_FILL_STATE_LOCK:
            original_active = products_module._IMAGE_FILL_ACTIVE
            products_module._IMAGE_FILL_ACTIVE = True
            products_module._IMAGE_FILL_PENDING.clear()
            products_module._IMAGE_FILL_QUEUED.clear()
            products_module._IMAGE_FILL_WORKING.clear()
            products_module._IMAGE_FILL_RETRY_AFTER.clear()
        try:
            products_module.schedule_image_fill(["old-1", "visible", "old-2"], priority=False)
            products_module.schedule_image_fill(["visible"], priority=True)
            self.assertEqual(
                list(products_module._IMAGE_FILL_PENDING),
                ["visible", "old-1", "old-2"],
            )
        finally:
            with products_module._IMAGE_FILL_STATE_LOCK:
                products_module._IMAGE_FILL_PENDING.clear()
                products_module._IMAGE_FILL_QUEUED.clear()
                products_module._IMAGE_FILL_WORKING.clear()
                products_module._IMAGE_FILL_RETRY_AFTER.clear()
                products_module._IMAGE_FILL_ACTIVE = original_active

    def test_image_queue_is_bounded_and_visible_items_replace_background_work(self):
        with products_module._IMAGE_FILL_STATE_LOCK:
            original_active = products_module._IMAGE_FILL_ACTIVE
            products_module._IMAGE_FILL_ACTIVE = True
            products_module._IMAGE_FILL_PENDING.clear()
            products_module._IMAGE_FILL_QUEUED.clear()
            products_module._IMAGE_FILL_WORKING.clear()
            products_module._IMAGE_FILL_RETRY_AFTER.clear()
        try:
            background = [
                f"background-{index}"
                for index in range(products_module._IMAGE_FILL_MAX_PENDING + 20)
            ]
            products_module.schedule_image_fill(background, priority=False)
            self.assertEqual(
                len(products_module._IMAGE_FILL_PENDING),
                products_module._IMAGE_FILL_MAX_PENDING,
            )

            products_module.schedule_image_fill(["visible-now"], priority=True)
            self.assertEqual(
                products_module._IMAGE_FILL_PENDING[0], "visible-now"
            )
            self.assertEqual(
                len(products_module._IMAGE_FILL_PENDING),
                products_module._IMAGE_FILL_MAX_PENDING,
            )
        finally:
            with products_module._IMAGE_FILL_STATE_LOCK:
                products_module._IMAGE_FILL_PENDING.clear()
                products_module._IMAGE_FILL_QUEUED.clear()
                products_module._IMAGE_FILL_WORKING.clear()
                products_module._IMAGE_FILL_RETRY_AFTER.clear()
                products_module._IMAGE_FILL_ACTIVE = original_active

    def make_plan_db(self):
        db = self.make_db()
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
        upsert_reference_candidate(
            db, {
                "barcode": "0123456789012", "name": "Example exact package",
                "brand": "Example", "description": "Reference description",
                "image_url": "https://img.test/p.jpg", "product_code": "F123",
                "source": "Manufacturer exact product page",
                "source_url": "https://source.test/reference",
            }, imported_at="2026-07-12T00:00:00+00:00",
        )
        db.execute("""INSERT INTO products (id, name, barcode, aisle, side, section, shelf, position)
                      VALUES (1, 'Example', '123456789012', '1', 'Gauche', '1', '1', '1')""")
        db.execute(
            """INSERT INTO products (id, name, barcode, description, aisle, side, section, shelf, position)
               VALUES (2, 'Example', '0123456789012', 'Manual description', '1', 'Gauche', '1', '1', '2')"""
        )
        db.execute("""INSERT INTO products (id, name, barcode, aisle, side, section, shelf, position)
                      VALUES (3, 'Other', '999999999999', '1', 'Gauche', '1', '1', '3')""")

        linked = sync_reference_metadata_to_products(db, now="2026-07-12T00:00:00+00:00")

        first = dict(db.execute("SELECT * FROM products WHERE id=1").fetchone())
        second = dict(db.execute("SELECT * FROM products WHERE id=2").fetchone())
        third = dict(db.execute("SELECT * FROM products WHERE id=3").fetchone())
        self.assertEqual(linked, 2)
        self.assertEqual(first["description"], "Reference description")
        self.assertEqual(first["image_url"], "https://img.test/p.jpg")
        self.assertEqual(first["product_code"], "F123")
        self.assertEqual(first["source_url"], "https://source.test/reference")
        self.assertEqual(second["description"], "Manual description")
        self.assertEqual(second["brand"], "Example")
        self.assertEqual(third["description"], "")

    def test_available_exact_upc_media_backfills_before_manual_review(self):
        db = self.make_db()
        upsert_reference_candidate(
            db, {
                "barcode": "063848966068", "name": "Razor",
                "description": "Available catalogue description",
                "image_url": "https://img.test/unverified-razor.jpg",
                "source": "Open Products Facts",
            }, imported_at="2026-07-22T00:00:00+00:00",
        )
        db.execute(
            """INSERT INTO products
               (id, name, barcode, aisle, side, section, shelf, position)
               VALUES (1, 'Razor', '063848966068', '1', 'Gauche', '1', '1', '1')"""
        )

        linked = sync_reference_metadata_to_products(
            db, now="2026-07-22T00:00:00+00:00"
        )
        product = dict(db.execute("SELECT * FROM products WHERE id=1").fetchone())

        self.assertEqual(linked, 1)
        self.assertEqual(product["description"], "Available catalogue description")
        self.assertEqual(product["image_url"], "https://img.test/unverified-razor.jpg")
        db.close()

    def test_planogram_import_preserves_same_upc_metadata_and_drops_stale_other_upc_data(self):
        reference = {
            "brand": "New Brand",
            "description": "New product description",
            "image_url": "https://img.test/new.jpg",
            "product_code": "REF1",
            "source_url": "https://source.test/new",
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
        self.assertEqual(changed["description"], "")
        self.assertEqual(changed["usage_notes"], "")
        self.assertEqual(changed["source_url"], "")
        self.assertEqual(changed["product_code"], "PDF2")

    def test_reference_index_never_combines_conflicting_equivalent_upc_rows(self):
        db = self.make_db()
        first = upsert_reference_candidate(
            db, {
                "barcode": "123456789012", "name": "Thin product",
                "brand": "Thin", "product_code": "F999",
                "source": "Planogramme magasin",
            }, imported_at="2026-07-12T00:00:00+00:00",
        )
        second = upsert_reference_candidate(
            db, {
                "barcode": "0123456789012", "name": "Rich product",
                "brand": "Rich", "description": "Useful description",
                "image_url": "https://img.test/rich.jpg",
                "source": "Planogramme magasin",
            }, imported_at="2026-07-12T00:01:00+00:00",
        )

        index = build_reference_metadata_index(db)
        reference = reference_metadata_for_barcode(index, "123456789012")

        self.assertEqual(first["verification_status"], "verified")
        self.assertEqual(second["verification_status"], "requires_review")
        self.assertEqual(reference["brand"], "Thin")
        self.assertEqual(reference.get("description", ""), "")
        self.assertEqual(reference["product_code"], "F999")

    def test_reference_search_includes_a_saved_product_image(self):
        db = self.make_db()
        row = {
            "barcode": "041388316000", "name": "BLISTEX LIP MEDEX POT 7G",
            "brand": "Blistex", "description": "", "product_code": "699496",
            "store_presence_status": "planogram_imported",
            "_bc": "041388316000", "_name": "blistex lip medex pot 7g",
            "_brand": "blistex", "_hay": "blistex lip medex pot 7g blistex",
            "_tokens": ["blistex", "lip", "medex", "pot", "7g"],
        }
        with patch("routes.products.get_db", return_value=db), \
             patch("routes.products._reference_corpus", return_value=[row]), \
             patch("routes.products.build_reference_metadata_index", return_value={
                 "041388316000": {"image_url": "https://img.test/blistex.jpg"},
             }):
            results = rank_reference_for_query("blistex")

        self.assertEqual(results[0]["image_url"], "https://img.test/blistex.jpg")
        self.assertTrue(results[0]["catalog_only"])
        db.close()

    def test_reference_image_endpoint_returns_known_images_and_queues_missing(self):
        db = self.make_db()
        app = self.make_test_app()
        with patch("routes.products.get_db", return_value=db), \
             patch("routes.products.build_reference_metadata_index", return_value={
                 "041388316000": {"image_url": "https://img.test/blistex.jpg"},
                 "012345678901": {"image_url": ""},
             }), \
             patch("routes.products.schedule_image_fill") as schedule:
            with app.test_client() as client:
                response = client.get(
                    "/api/products/reference-images?barcodes=041388316000,012345678901"
                )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["images"], {
            "041388316000": "https://img.test/blistex.jpg",
        })
        schedule.assert_called_once_with(["012345678901"], priority=True)
        db.close()

    def test_product_image_endpoint_returns_unreviewed_exact_upc_image(self):
        db = self.make_db()
        db.execute(
            """INSERT INTO products
               (id, name, barcode, aisle, side, section, shelf, position)
               VALUES (1, 'Razor', '063848966068', '1', 'Gauche', '1', '1', '1')"""
        )
        db.execute(
            """INSERT INTO product_reference
               (barcode, name, image_url, source, verification_status)
               VALUES ('063848966068', 'Razor', 'https://img.test/catalogue-razor.jpg',
                       'Open Products Facts', 'requires_review')"""
        )
        app = self.make_test_app()
        with patch("routes.products.get_db", return_value=db), \
             patch("routes.products.schedule_image_fill") as schedule:
            with app.test_client() as client:
                response = client.get("/api/products/images?ids=1")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["images"], {
            "1": "https://img.test/catalogue-razor.jpg",
        })
        schedule.assert_called_once_with([])
        db.close()

    def test_bulk_planogram_import_attaches_reference_metadata_and_clears_old_upc_data(self):
        db = self.make_plan_db()
        upsert_reference_candidate(
            db, {
                "barcode": "0123456789012", "name": "PLAN NAME",
                "brand": "Reference Brand", "description": "Reference description",
                "image_url": "https://img.test/ref.jpg", "product_code": "REF123",
                "source": "Planogramme magasin",
                "store_presence_status": "planogram_imported",
            }, imported_at="2026-07-12T00:00:00+00:00",
        )
        app = self.make_test_app()
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

    def test_bulk_import_returns_before_identifier_and_quality_enrichment(self):
        db = self.make_plan_db()
        app = self.make_test_app()
        payload = {
            "aisle": "1", "side": "Gauche", "start_section": 1,
            "start_tablette": 1, "tablette_start": 1, "tablette_end": 1,
            "replace_existing": True,
            "products": [{
                "tablette": 1, "position": 1,
                "barcode": "063848966068", "name": "FAST PRODUCT",
                "code_familiprix": "FAST1", "din": "01938371",
            }],
        }

        with patch("routes.products.get_db", return_value=db), \
             patch("auth.get_db", return_value=db), \
             patch("routes.products.schedule_image_fill"), \
             patch("routes.gist._schedule_gist_backup"), \
             patch(
                 "routes.products._record_import_identifiers",
                 side_effect=AssertionError("identifier work ran in request"),
             ), \
             patch(
                 "routes.products.audit_product_data",
                 side_effect=AssertionError("quality audit ran in request"),
             ), \
             patch(
                 "routes.products.upsert_reference_candidate",
                 side_effect=AssertionError("reference work ran in request"),
             ):
            with app.test_client() as client:
                response = client.post("/api/products/bulk-import", json=payload)

        result = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(result["imported"], 1)
        self.assertTrue(result["quality"]["queued"])
        queued_items, employee, imported_at = self.schedule_post_import.call_args.args
        self.assertEqual(employee, "test-user")
        self.assertTrue(imported_at)
        self.assertEqual(queued_items[0]["barcode"], "063848966068")
        self.assertEqual(
            queued_items[0]["identifier_payload"]["din"], "01938371"
        )
        self.assertEqual(
            db.execute("SELECT name FROM products").fetchone()[0],
            "FAST PRODUCT",
        )
        db.close()

    def test_deferred_planogram_work_links_identifiers_and_reference(self):
        db = self.make_plan_db()
        db.execute(
            """INSERT INTO products
               (id, name, barcode, product_code, image_url, aisle, side,
                section, shelf, position, modified_at)
               VALUES (10, 'DEFERRED PRODUCT', '063848966068', 'FAST1',
                       'https://img.test/fast.jpg', '1', 'Gauche',
                       '1', '1', '1', 'import-v1')"""
        )
        db.commit()

        class KeepOpenDb:
            def __init__(self, connection):
                self.connection = connection

            def __getattr__(self, name):
                return getattr(self.connection, name)

            def close(self):
                pass

        job = {
            "employee": "test",
            "imported_at": "2026-07-23T12:00:00+00:00",
            "items": [{
                "id": 10,
                "barcode": "063848966068",
                "gtin_key": products_module.gtin_identity_key("063848966068"),
                "modified_at": "import-v1",
                "identifier_payload": {"din": "01938371"},
                "verified_fields": ["image_url"],
            }],
        }
        with patch("database.connect_db", return_value=KeepOpenDb(db)), \
             patch("routes.products.audit_product_data", return_value={
                 "success": True, "scanned": 1, "issues": 0, "statuses": {},
             }), \
             patch("routes.products.release_unused_memory"):
            _process_planogram_post_import_job(job)

        identifiers = {
            (row["identifier_type"], row["identifier_value"])
            for row in db.execute(
                "SELECT identifier_type, identifier_value "
                "FROM product_identifiers WHERE product_id=10"
            ).fetchall()
        }
        reference = db.execute(
            "SELECT name, product_code FROM product_reference "
            "WHERE gtin_key=?",
            (products_module.gtin_identity_key("063848966068"),),
        ).fetchone()
        evidence = db.execute(
            """SELECT verification_status FROM product_field_evidence
               WHERE product_id=10 AND field_name='image_url' AND active=1"""
        ).fetchone()

        self.assertIn(("GTIN", "063848966068"), identifiers)
        self.assertIn(("FAMILIPRIX_CODE", "FAST1"), identifiers)
        self.assertIn(("DIN", "01938371"), identifiers)
        self.assertEqual(tuple(reference), ("DEFERRED PRODUCT", "FAST1"))
        self.assertEqual(evidence[0], "verified")
        db.close()

    def test_bulk_import_reports_overflow_shelves_and_products_separately(self):
        db = self.make_plan_db()
        app = self.make_test_app()
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
        app = self.make_test_app()
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
                ("OLD IN GAP", "063848966068", "2"),
                ("OLD AT THREE", "333333333333", "3"),
            ],
        )
        db.execute(
            "UPDATE products SET image_url='https://img.test/moved.jpg', "
            "description='Enriched description', image_status='verified', "
            "description_status='verified' WHERE barcode='063848966068'"
        )
        source_id = db.execute(
            "SELECT id FROM products WHERE barcode='063848966068'"
        ).fetchone()[0]
        record_field_evidence(
            db, source_id, "image_url", "https://img.test/moved.jpg",
            source="Manual verified", verification_status="verified", active=True,
        )
        record_field_evidence(
            db, source_id, "description", "Enriched description",
            source="Manual verified", verification_status="verified", active=True,
        )
        app = self.make_test_app()
        payload = {
            "aisle": "1", "side": "Gauche", "start_section": 1,
            "start_tablette": 1, "tablette_start": 1, "tablette_end": 1,
            "replace_existing": True,
            "products": [
                {"tablette": 1, "position": 1, "barcode": "444444444444", "name": "NEW ONE"},
                {"tablette": 1, "position": 3, "barcode": "063848966068", "name": "NEW THREE"},
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
        app = self.make_test_app()
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

            def executemany(self, query, params):
                normalized = " ".join(str(query).lower().split())
                if normalized.startswith("insert into products"):
                    raise RuntimeError("simulated insert failure")
                return self.connection.executemany(query, params)

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
        app = self.make_test_app()
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
        app = self.make_test_app()
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
        self.assertEqual(response.get_json()["layout"]["modified_at"], "server-v2")
        self.assertEqual(db.execute("SELECT name FROM products").fetchone()[0], "OLD PRODUCT")
        self.assertEqual(db.execute("SELECT COUNT(*) FROM removed_products").fetchone()[0], 0)
        db.close()

    def test_bulk_import_accepts_newer_timestamp_when_structure_is_unchanged(self):
        db = self.make_plan_db()
        current_config = json.loads(
            db.execute(
                "SELECT config_json FROM aisle_layouts WHERE aisle='1'"
            ).fetchone()[0]
        )
        db.execute("UPDATE aisle_layouts SET modified_at='server-v2' WHERE aisle='1'")
        db.execute(
            """INSERT INTO products
               (name, barcode, aisle, side, section, shelf, position)
               VALUES ('OLD PRODUCT', '111', '1', 'Gauche', '1', '1', '1')"""
        )
        db.commit()
        app = self.make_test_app()
        payload = {
            "aisle": "1", "side": "Gauche", "start_section": 1,
            "start_tablette": 1, "tablette_start": 1, "tablette_end": 1,
            "replace_existing": True,
            "expected_layout_modified_at": "phone-v1",
            "expected_layout_config": current_config,
            "products": [
                {
                    "tablette": 1, "position": 1,
                    "barcode": "222", "name": "NEW PRODUCT",
                },
            ],
        }

        with patch("routes.products.get_db", return_value=db), \
             patch("auth.get_db", return_value=db), \
             patch("routes.products.schedule_image_fill"), \
             patch("routes.gist._schedule_gist_backup"):
            with app.test_client() as client:
                response = client.post("/api/products/bulk-import", json=payload)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            db.execute("SELECT name FROM products").fetchone()[0],
            "NEW PRODUCT",
        )
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

    def test_planogram_section_direction_can_be_overridden_on_either_side(self):
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

        cote_a_as_b, overflow_a = plan_planogram_flow(
            config(), "Gauche", start_section=8, start_tablette=1, lines=lines,
            section_direction="ascending",
        )
        cote_b_as_a, overflow_b = plan_planogram_flow(
            config(), "Droite", start_section=9, start_tablette=1, lines=lines,
            section_direction="descending",
        )

        self.assertEqual(
            [
                (section, shelf, position)
                for section, shelf, position, _line in cote_a_as_b
            ],
            [(8, 1, 1), (8, 1, 2), (9, 1, 1), (9, 1, 2)],
        )
        self.assertEqual(
            [
                (section, shelf, position)
                for section, shelf, position, _line in cote_b_as_a
            ],
            [(9, 1, 1), (9, 1, 2), (8, 1, 1), (8, 1, 2)],
        )
        self.assertEqual((overflow_a, overflow_b), (0, 0))

    def test_bulk_import_honors_explicit_section_direction(self):
        db = self.make_db()
        config = build_default_layout_config(3, 1, 2)
        db.execute(
            """INSERT INTO aisle_layouts
               (aisle, max_section, max_shelf, max_position, config_json, enabled)
               VALUES ('1', '3', '1', '2', ?, 1)""",
            (json.dumps(config),),
        )
        db.commit()
        app = self.make_test_app()
        payload = {
            "aisle": "1",
            "side": "Gauche",
            "start_section": 1,
            "start_tablette": 1,
            "tablette_start": 1,
            "tablette_end": 2,
            "section_direction": "ascending",
            "replace_existing": True,
            "products": [
                {
                    "tablette": 1,
                    "position": 1,
                    "barcode": "111111111111",
                    "name": "SECTION ONE",
                },
                {
                    "tablette": 2,
                    "position": 1,
                    "barcode": "222222222222",
                    "name": "SECTION TWO",
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
        placed = [
            tuple(row)
            for row in db.execute(
                "SELECT name, section, shelf, position FROM products "
                "ORDER BY CAST(section AS INTEGER)"
            ).fetchall()
        ]

        self.assertEqual(response.status_code, 200)
        self.assertEqual(result["section_direction"], "ascending")
        self.assertEqual(result["effective_section_direction"], "ascending")
        self.assertEqual(
            placed,
            [
                ("SECTION ONE", "1", "1", "1"),
                ("SECTION TWO", "2", "1", "1"),
            ],
        )
        db.close()

    def test_bulk_import_rejects_unknown_section_direction_without_changes(self):
        db = self.make_plan_db()
        app = self.make_test_app()
        payload = {
            "aisle": "1",
            "side": "Gauche",
            "start_section": 1,
            "start_tablette": 1,
            "tablette_start": 1,
            "tablette_end": 1,
            "section_direction": "sideways",
            "replace_existing": True,
            "products": [
                {
                    "tablette": 1,
                    "position": 1,
                    "barcode": "111111111111",
                    "name": "MUST NOT IMPORT",
                },
            ],
        }

        with patch("routes.products.get_db", return_value=db), \
             patch("auth.get_db", return_value=db):
            with app.test_client() as client:
                response = client.post("/api/products/bulk-import", json=payload)

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.get_json()["error"], "Sens des sections invalide.")
        self.assertEqual(db.execute("SELECT COUNT(*) FROM products").fetchone()[0], 0)
        self.assertEqual(
            db.execute("SELECT COUNT(*) FROM planogram_imports").fetchone()[0], 0
        )
        db.close()


if __name__ == "__main__":
    unittest.main()
