import sqlite3
import unittest
from unittest.mock import patch

from flask import Flask

import database as database_module
from database import DatabaseConnection, ensure_product_data_ready, init_sqlite_db
from product_backup import (
    build_product_data_backup,
    restore_product_backup_row,
    restore_product_data_backup,
)
from product_data import (
    assess_metadata_candidate,
    exact_gtin_variants,
    gtin_identity_key,
    record_reference_evidence,
    upsert_product_identifier,
    upsert_reference_candidate,
)
from routes.ai import product_context_for_client_rag
from routes import products as products_module
from routes.products import (
    audit_product_data,
    build_reference_metadata_index,
    reference_metadata_for_barcode,
    row_to_product,
)


class ProductDataAccuracyTests(unittest.TestCase):
    def make_db(self):
        raw = sqlite3.connect(":memory:")
        raw.row_factory = sqlite3.Row
        db = DatabaseConnection(raw, "sqlite")
        init_sqlite_db(db)
        return db

    def insert_product(self, db, name="Test product", barcode="063848966068", position="1"):
        return db.execute(
            """INSERT INTO products
               (name, barcode, aisle, side, section, shelf, position)
               VALUES (?, ?, '1', 'Gauche', '1', '1', ?)""",
            (name, barcode, position),
        ).lastrowid

    def test_standard_leading_zero_representation_is_same_exact_gtin(self):
        self.assertEqual(
            gtin_identity_key("063848966068"),
            gtin_identity_key("0063848966068"),
        )
        self.assertIn("0063848966068", exact_gtin_variants("063848966068"))

    def test_arbitrary_zero_stripping_never_creates_an_identity_match(self):
        self.assertNotEqual(
            gtin_identity_key("00063848966068"),
            gtin_identity_key("63848966068"),
        )

    def test_same_upc_with_different_package_count_requires_review(self):
        result = assess_metadata_candidate(
            {"barcode": "063848966068", "name": "Advil 50 comprimes"},
            {
                "barcode": "063848966068", "name": "Advil 100 comprimes",
                "source": "Planogramme magasin",
            },
        )
        self.assertFalse(result.auto_apply)
        self.assertIn("package_size_conflict", {issue["type"] for issue in result.issues})

    def test_abbreviated_and_written_package_counts_are_equivalent(self):
        result = assess_metadata_candidate(
            {"barcode": "063848966068", "name": "Advil 200MG CO50"},
            {
                "barcode": "063848966068",
                "name": "Advil 200 mg 50 tablets",
                "source": "Manufacturer exact product page",
            },
        )
        self.assertTrue(result.auto_apply)
        self.assertNotIn(
            "package_size_conflict", {issue["type"] for issue in result.issues}
        )

    def test_untrusted_online_media_remains_available_and_flagged(self):
        db = self.make_db()
        result = upsert_reference_candidate(
            db,
            {
                "barcode": "063848966068", "name": "Test product",
                "description": "Unverified web description",
                "image_url": "https://web.test/image.jpg",
                "source": "Open Products Facts",
            },
            imported_at="2026-07-22T00:00:00+00:00",
        )
        reference = reference_metadata_for_barcode(
            build_reference_metadata_index(db, ["063848966068"]),
            "063848966068",
        )
        self.assertEqual(result["verification_status"], "requires_review")
        self.assertEqual(reference.get("description", ""), "Unverified web description")
        self.assertEqual(reference.get("image_url", ""), "https://web.test/image.jpg")
        self.assertEqual(
            set(reference.get("_unverified_fields", [])),
            {"description", "image_url"},
        )
        evidence = db.execute(
            """SELECT verification_status, active FROM product_reference_evidence
               WHERE field_name='image_url'"""
        ).fetchone()
        self.assertEqual(evidence["verification_status"], "requires_review")
        self.assertEqual(evidence["active"], 0)
        db.close()

    def test_media_is_found_across_equivalent_upc_and_gtin_forms(self):
        db = self.make_db()
        db.execute(
            """INSERT INTO product_reference (barcode, name, source)
               VALUES ('063848966068', 'Test product', 'Planogramme magasin')"""
        )
        db.execute(
            """INSERT INTO product_reference
               (barcode, name, description, image_url, source)
               VALUES ('0063848966068', 'Test product', 'Exact package text',
                       'https://img.test/exact.jpg', 'Open Products Facts')"""
        )

        reference = reference_metadata_for_barcode(
            build_reference_metadata_index(db, ["063848966068"]),
            "063848966068",
        )

        self.assertEqual(reference["description"], "Exact package text")
        self.assertEqual(reference["image_url"], "https://img.test/exact.jpg")
        db.close()

    def test_rejected_reference_evidence_cannot_be_reactivated_automatically(self):
        db = self.make_db()
        kwargs = {
            "source": "Manufacturer exact product page",
            "verification_status": "verified",
            "active": True,
        }
        record_reference_evidence(
            db, "063848966068", "image_url", "https://manufacturer.test/a.jpg",
            **kwargs,
        )
        db.execute(
            """UPDATE product_reference_evidence
               SET verification_status='rejected', active=0"""
        )
        record_reference_evidence(
            db, "063848966068", "image_url", "https://manufacturer.test/a.jpg",
            **kwargs,
        )
        evidence = db.execute(
            "SELECT verification_status, active FROM product_reference_evidence"
        ).fetchone()
        self.assertEqual(evidence["verification_status"], "rejected")
        self.assertEqual(evidence["active"], 0)
        db.close()

    def test_exact_trusted_package_is_applied_and_audited(self):
        db = self.make_db()
        upsert_reference_candidate(
            db,
            {
                "barcode": "063848966068", "name": "Test product",
                "description": "Exact package description",
                "image_url": "https://manufacturer.test/image.jpg",
                "source": "Manufacturer exact product page",
            },
            imported_at="2026-07-22T00:00:00+00:00",
        )
        product_id = self.insert_product(db)
        result = audit_product_data(
            db, [product_id], now="2026-07-22T00:00:00+00:00"
        )
        product = dict(db.execute(
            "SELECT * FROM products WHERE id=?", (product_id,)
        ).fetchone())
        self.assertEqual(result["statuses"], {"complete_verified": 1})
        self.assertEqual(product["description_status"], "verified")
        self.assertEqual(product["image_status"], "verified")
        self.assertEqual(product["image_url"], "https://manufacturer.test/image.jpg")
        db.close()

    def test_legacy_unproven_values_are_shown_and_sent_to_review(self):
        db = self.make_db()
        product_id = self.insert_product(db)
        db.execute(
            """UPDATE products SET description='Maybe wrong',
               image_url='https://unknown.test/image.jpg' WHERE id=?""",
            (product_id,),
        )
        audit_product_data(db, [product_id], now="2026-07-22T00:00:00+00:00")
        raw = db.execute("SELECT * FROM products WHERE id=?", (product_id,)).fetchone()
        public = row_to_product(raw)
        issue_types = {
            row["issue_type"] for row in db.execute(
                "SELECT issue_type FROM product_data_issues WHERE product_id=? AND status='open'",
                (product_id,),
            ).fetchall()
        }
        self.assertEqual(public["description"], "Maybe wrong")
        self.assertEqual(public["image_url"], "https://unknown.test/image.jpg")
        self.assertTrue(public["description_available_unverified"])
        self.assertTrue(public["image_available_unverified"])
        self.assertIn("possible_wrong_description", issue_types)
        self.assertIn("possible_wrong_image", issue_types)
        db.close()

    def test_din_and_billing_identifiers_remain_separate_from_upc(self):
        db = self.make_db()
        first = self.insert_product(db, position="1")
        second = self.insert_product(db, barcode="041388316000", position="2")
        self.assertTrue(upsert_product_identifier(
            db, first, "DIN", "12345678", source="Health Canada"
        ))
        self.assertTrue(upsert_product_identifier(
            db, second, "DIN", "12345678", source="Health Canada"
        ))
        self.assertFalse(upsert_product_identifier(
            db, first, "PSEUDO_DIN", "99123456"
        ))
        self.assertTrue(upsert_product_identifier(
            db, first, "PSEUDO_DIN", "99123456", authority="RAMQ"
        ))
        din_rows = db.execute(
            "SELECT product_id FROM product_identifiers WHERE identifier_type='DIN'"
        ).fetchall()
        self.assertEqual({row["product_id"] for row in din_rows}, {first, second})
        db.close()

    def test_ai_context_includes_unverified_description_with_status(self):
        context = product_context_for_client_rag({
            "client_id": "product:1", "name": "Test product",
            "brand": "Wrong brand", "description": "Wrong description",
            "barcode": "063848966068", "aisle": "1", "side": "Gauche",
            "shelf": "1", "data_status": "requires_manual_review",
            "description_status": "unverified", "name_status": "unverified",
            "_verified_fields": [],
        })
        self.assertEqual(context["brand"], "")
        self.assertEqual(context["description"], "Wrong description")
        self.assertEqual(context["notes"], "Wrong description")
        self.assertFalse(context["description_verified"])
        self.assertTrue(context["unverified_description_included"])
        self.assertTrue(context["unverified_information_omitted"])

    def test_verified_name_status_does_not_verify_brand(self):
        context = product_context_for_client_rag({
            "client_id": "product:1", "name": "Test product",
            "brand": "Unproven brand", "barcode": "063848966068",
            "name_status": "verified", "identity_status": "verified",
            "_verified_fields": ["name"],
        })
        self.assertEqual(context["brand"], "")

    def test_unverified_structured_fields_do_not_influence_search(self):
        db = self.make_db()
        product_id = self.insert_product(db, name="Neutral item")
        db.execute(
            """UPDATE products SET category='secretwidget',
               ingredients='madeupingredient', brand='Wrong Brand' WHERE id=?""",
            (product_id,),
        )
        products_module._PROD_CACHE.update(key=None, rows=[])
        _item, search_row = products_module._products_corpus(db)[0]
        self.assertNotIn("secretwidget", search_row["_hay"])
        self.assertNotIn("madeupingredient", search_row["_hay"])
        self.assertEqual(search_row["_brand"], "")
        db.close()

    def test_available_description_influences_search_before_review(self):
        db = self.make_db()
        product_id = self.insert_product(db, name="Neutral item")
        db.execute(
            "UPDATE products SET description='transparent wound membrane' WHERE id=?",
            (product_id,),
        )
        products_module._PROD_CACHE.update(key=None, rows=[])
        item, search_row = products_module._products_corpus(db)[0]
        self.assertEqual(item["description"], "transparent wound membrane")
        self.assertIn("transparent wound membrane", search_row["_hay"])
        self.assertTrue(item["description_available_unverified"])
        db.close()

    def test_verified_aliases_and_identifiers_expand_search_without_changing_name(self):
        db = self.make_db()
        product_id = self.insert_product(db, name="ACETAMINOPHENE 500MG")
        db.execute(
            """INSERT INTO product_aliases
               (product_id, alias_type, alias_value, normalized_value, source,
                confidence, verification_status)
               VALUES (?, 'common_name', 'Tylenol', 'tylenol', 'Manual', 1, 'verified')""",
            (product_id,),
        )
        upsert_product_identifier(
            db, product_id, "DIN", "12345678", source="Health Canada",
            verification_status="verified",
        )
        products_module._PROD_CACHE.update(key=None, rows=[])
        item, search_row = products_module._products_corpus(db)[0]
        self.assertEqual(item["name"], "ACETAMINOPHENE 500MG")
        self.assertIn("tylenol", search_row["_hay"])
        self.assertIn("12345678", search_row["_hay"])
        db.close()

    def test_backup_restores_provenance_with_remapped_product_ids(self):
        source = self.make_db()
        first = self.insert_product(source, name="Exact package 50", position="1")
        second = self.insert_product(
            source, name="Exact package 100", barcode="041388316000", position="2"
        )
        upsert_product_identifier(
            source, first, "DIN", "12345678", source="Health Canada",
            verification_status="verified",
        )
        source.execute(
            """INSERT INTO product_aliases
               (product_id, alias_type, alias_value, normalized_value, source,
                confidence, verification_status)
               VALUES (?, 'common_name', 'Exact fifty', 'exact fifty',
                       'Manual', 1, 'verified')""",
            (first,),
        )
        source.execute(
            """INSERT INTO product_relationships
               (source_product_id, target_product_id, relationship_type, source,
                confidence, verification_status, created_at)
               VALUES (?, ?, 'same_product_family', 'Manual', 1, 'verified', ?)""",
            (first, second, "2026-07-22T00:00:00+00:00"),
        )
        upsert_reference_candidate(
            source,
            {
                "barcode": "063848966068", "name": "Exact package 50",
                "description": "Verified exact package",
                "source": "Manufacturer exact product page",
            },
            imported_at="2026-07-22T00:00:00+00:00",
        )
        audit_product_data(
            source, [first, second], now="2026-07-22T00:00:00+00:00"
        )
        products = [dict(row) for row in source.execute(
            "SELECT * FROM products ORDER BY id"
        ).fetchall()]
        backup = build_product_data_backup(source)

        target = self.make_db()
        self.insert_product(
            target, name="Existing product", barcode="012345678905", position="99"
        )
        id_map = {}
        for product in products:
            restored = restore_product_backup_row(
                target, product, "restore-test", "2026-07-23T00:00:00+00:00"
            )
            id_map[product["id"]] = restored
        result = restore_product_data_backup(target, backup, id_map)

        self.assertGreater(result["restored"], 0)
        self.assertNotEqual(first, id_map[first])
        identifier = target.execute(
            "SELECT product_id, identifier_type, identifier_value FROM product_identifiers"
        ).fetchone()
        self.assertEqual(identifier["product_id"], id_map[first])
        self.assertEqual(identifier["identifier_type"], "DIN")
        relation = target.execute(
            "SELECT source_product_id, target_product_id FROM product_relationships"
        ).fetchone()
        self.assertEqual(relation["source_product_id"], id_map[first])
        self.assertEqual(relation["target_product_id"], id_map[second])
        evidence = target.execute(
            """SELECT verification_status, active FROM product_reference_evidence
               WHERE barcode='063848966068' AND field_name='description'"""
        ).fetchone()
        self.assertEqual(evidence["verification_status"], "verified")
        self.assertEqual(evidence["active"], 1)
        source.close()
        target.close()

    def test_delayed_postgres_product_schema_repairs_once(self):
        class FakePostgres:
            backend = "postgres"

            def __init__(self):
                self.executed = []
                self.commits = 0
                self.rollbacks = 0

            def execute(self, query, params=()):
                self.executed.append((query, params))
                return None

            def commit(self):
                self.commits += 1

            def rollback(self):
                self.rollbacks += 1

        db = FakePostgres()
        original_ready = database_module._POSTGRES_PRODUCT_SCHEMA_READY
        database_module._POSTGRES_PRODUCT_SCHEMA_READY = False
        try:
            with patch.object(
                database_module, "_postgres_product_data_schema_complete",
                return_value=False,
            ), patch.object(database_module, "ensure_product_data_schema") as migrate:
                self.assertTrue(ensure_product_data_ready(db))
                self.assertTrue(ensure_product_data_ready(db))
                migrate.assert_called_once_with(db)
            self.assertEqual(db.commits, 1)
            self.assertEqual(db.rollbacks, 0)
        finally:
            database_module._POSTGRES_PRODUCT_SCHEMA_READY = original_ready

    def test_existing_postgres_schema_does_not_wait_for_startup_lock(self):
        class FakePostgres:
            backend = "postgres"

        class BusyStartupLock:
            def acquire(self, timeout=0):
                raise AssertionError("the startup lock should not be consulted")

        original_ready = database_module._POSTGRES_PRODUCT_SCHEMA_READY
        original_error = database_module._POSTGRES_PRODUCT_SCHEMA_ERROR
        database_module._POSTGRES_PRODUCT_SCHEMA_READY = False
        database_module._POSTGRES_PRODUCT_SCHEMA_ERROR = "migration_in_progress"
        try:
            with patch.object(
                database_module, "_postgres_product_data_schema_complete",
                return_value=True,
            ), patch.object(
                database_module, "_PRODUCT_SCHEMA_LOCK", BusyStartupLock(),
            ):
                self.assertTrue(ensure_product_data_ready(FakePostgres()))
            self.assertTrue(database_module._POSTGRES_PRODUCT_SCHEMA_READY)
            self.assertEqual(database_module._POSTGRES_PRODUCT_SCHEMA_ERROR, "")
        finally:
            database_module._POSTGRES_PRODUCT_SCHEMA_READY = original_ready
            database_module._POSTGRES_PRODUCT_SCHEMA_ERROR = original_error

    def test_search_generation_tracks_plan_changes_not_background_descriptions(self):
        self.assertTrue(database_module._query_affects_product_search(
            "INSERT INTO products (name, aisle) VALUES (?, ?)"
        ))
        self.assertTrue(database_module._query_affects_product_search(
            "UPDATE products SET section=?, shelf=?, modified_at=? WHERE id=?"
        ))
        self.assertTrue(database_module._query_affects_product_search(
            "DELETE FROM products WHERE id=?"
        ))
        self.assertFalse(database_module._query_affects_product_search(
            "UPDATE products SET description=?, image_url=?, quality_checked_at=? WHERE id=?"
        ))

    def test_quality_summary_reports_identifier_and_field_coverage(self):
        db = self.make_db()
        product_id = self.insert_product(db, name="Verified package")
        upsert_product_identifier(
            db, product_id, "DIN", "12345678", source="Health Canada",
            verification_status="verified",
        )
        audit_product_data(
            db, [product_id], now="2026-07-22T00:00:00+00:00"
        )
        app = Flask(__name__)
        app.register_blueprint(products_module.products_bp)
        with patch.object(products_module, "get_db", return_value=db):
            response = app.test_client().get("/api/product-quality/summary")
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["identifier_coverage"]["DIN"]["verified"], 1)
        self.assertEqual(payload["identifier_coverage"]["GTIN"]["verified"], 1)
        self.assertEqual(payload["verified_field_coverage"]["name"], 1)
        self.assertEqual(payload["unchecked_products"], 0)
        db.close()


if __name__ == "__main__":
    unittest.main()
