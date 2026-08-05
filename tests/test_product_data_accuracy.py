import hashlib
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
    description_quality_issue,
    exact_gtin_variants,
    gtin_identity_key,
    normalize_identifier,
    record_reference_evidence,
    repair_quarantined_descriptions,
    upsert_product_identifier,
    upsert_reference_candidate,
)
from routes.ai import (
    _prefill_provisional_catalog_descriptions,
    product_context_for_client_rag,
)
from routes import products as products_module
from routes.products import (
    _product_search_row,
    _indexed_client_search_entries,
    _indexed_identifier_products,
    _materialize_mapped_products,
    _products_corpus,
    _verified_current_product_field_sources,
    audit_product_data,
    build_reference_metadata_index,
    hybrid_client_candidates,
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

    def test_all_zero_regulatory_placeholders_are_not_identifiers(self):
        self.assertEqual(normalize_identifier("DIN", "00000000"), "")
        self.assertEqual(normalize_identifier("NPN", "0000-0000"), "")
        self.assertEqual(normalize_identifier("DIN-HM", "00000000"), "")
        self.assertEqual(
            normalize_identifier("DIN", "00559407"), "00559407"
        )

    def test_exact_familiprix_page_replaces_lower_quality_description(self):
        db = self.make_db()
        barcode = "063848966068"
        upsert_reference_candidate(db, {
            "barcode": barcode,
            "name": "BIOMEDIC PRODUCT",
            "description": "Generic marketing description.",
            "image_url": "https://images.example/generic.jpg",
            "source": "Open Products Facts",
        }, imported_at="2026-01-01T00:00:00Z")

        upsert_reference_candidate(db, {
            "barcode": barcode,
            "name": "BIOMEDIC PRODUCT",
            "description": "Exact package description from the retailer.",
            "image_url": "https://images.example/exact.jpg",
            "product_code": "189026",
            "source": "Familiprix",
            "source_url": "https://magasiner.familiprix.com/fr/p/189026",
        }, imported_at="2026-01-02T00:00:00Z", promote_higher_priority=True)

        row = dict(db.execute(
            "SELECT * FROM product_reference WHERE barcode=?", (barcode,)
        ).fetchone())
        self.assertEqual(
            row["description"],
            "Exact package description from the retailer.",
        )
        self.assertEqual(row["image_url"], "https://images.example/exact.jpg")

    def test_local_description_prefill_is_immediate_but_remains_unverified(self):
        db = self.make_db()
        barcode = "063848966068"
        self.insert_product(
            db, name="ACME VITAMINE C 500MG CO100", barcode=barcode,
        )
        upsert_reference_candidate(db, {
            "barcode": barcode,
            "name": "ACME VITAMINE C 500MG CO100",
            "brand": "Acme",
            "package_size": "100",
            "package_unit": "comprimés",
            "strength": "500 mg",
            "source": "Planogramme",
        }, imported_at="2026-01-01T00:00:00Z")

        updated = _prefill_provisional_catalog_descriptions(db)

        self.assertEqual(updated, 1)
        reference = dict(db.execute(
            """SELECT description, enrich_status FROM product_reference
               WHERE barcode=?""",
            (barcode,),
        ).fetchone())
        self.assertIn("ACME VITAMINE C", reference["description"])
        self.assertEqual(
            reference["enrich_status"], "provisional_prefill_v4"
        )
        placed = dict(db.execute(
            "SELECT description FROM products WHERE barcode=?", (barcode,)
        ).fetchone())
        self.assertEqual(placed["description"], reference["description"])
        evidence = dict(db.execute(
            """SELECT verification_status, active, match_method
               FROM product_reference_evidence
               WHERE gtin_key=? AND field_name='description'""",
            (gtin_identity_key(barcode),),
        ).fetchone())
        self.assertEqual(evidence["verification_status"], "requires_review")
        self.assertEqual(evidence["active"], 0)
        self.assertEqual(
            evidence["match_method"], "deterministic_catalog_fields"
        )

    def test_exact_familiprix_refresh_does_not_overwrite_manual_description(self):
        db = self.make_db()
        barcode = "063848966068"
        upsert_reference_candidate(db, {
            "barcode": barcode,
            "name": "BIOMEDIC PRODUCT",
            "description": "Description vérifiée par la gestionnaire.",
            "source": "Manual manager verification",
        }, imported_at="2026-01-01T00:00:00Z")

        upsert_reference_candidate(db, {
            "barcode": barcode,
            "name": "BIOMEDIC PRODUCT",
            "description": "Description Familiprix plus récente.",
            "source": "Familiprix",
            "source_url": "https://magasiner.familiprix.com/fr/p/189026",
        }, imported_at="2026-01-02T00:00:00Z", promote_higher_priority=True)

        row = dict(db.execute(
            "SELECT * FROM product_reference WHERE barcode=?", (barcode,)
        ).fetchone())
        self.assertEqual(
            row["description"], "Description vérifiée par la gestionnaire."
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

    def test_exact_gtin_same_brand_allows_abbreviated_bilingual_name(self):
        result = assess_metadata_candidate(
            {
                "barcode": "063848966068",
                "name": "MARCELLE TAMP DEMAQ DCE 2X85",
                "brand": "Marcelle",
            },
            {
                "barcode": "063848966068",
                "name": "Gentle Makeup Remover Pads for Sensitive Eyes",
                "brand": "Marcelle",
                "source": "Open Beauty Facts",
            },
        )
        self.assertTrue(result.accepted)
        self.assertNotIn(
            "product_name_conflict", {issue["type"] for issue in result.issues}
        )

    def test_exact_gtin_different_brand_still_blocks_wrong_image_family(self):
        result = assess_metadata_candidate(
            {
                "barcode": "063848966068",
                "name": "ACME VITAMINE C 500MG",
                "brand": "Acme",
            },
            {
                "barcode": "063848966068",
                "name": "Canned tuna in spring water",
                "brand": "Ocean Fish",
                "source": "Open Food Facts",
            },
        )
        issue_types = {issue["type"] for issue in result.issues}
        self.assertFalse(result.accepted)
        self.assertIn("brand_conflict", issue_types)
        self.assertIn("product_name_conflict", issue_types)

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

    def test_foreign_description_is_quarantined_without_losing_exact_image(self):
        db = self.make_db()
        barcode = "063848966068"
        foreign_description = (
            "Verfris je haar met deze droogshampoo tussen wasbeurten. "
            "Deze verzorging geeft je haar glans en helpt uitgroei camoufleren."
        )

        result = upsert_reference_candidate(db, {
            "barcode": barcode,
            "name": "TEST PRODUCT 100ML",
            "description": foreign_description,
            "image_url": "https://magasiner.familiprix.com/medias/exact.png",
            "source": "Familiprix",
            "source_url": "https://magasiner.familiprix.com/fr/p/123",
        }, imported_at="2026-08-04T00:00:00Z")

        stored = dict(db.execute(
            "SELECT description, image_url FROM product_reference WHERE barcode=?",
            (barcode,),
        ).fetchone())
        reference = reference_metadata_for_barcode(
            build_reference_metadata_index(db, [barcode]), barcode,
        )
        rejected = dict(db.execute(
            """SELECT verification_status, active
               FROM product_reference_evidence
               WHERE field_name='description'"""
        ).fetchone())

        self.assertIn(
            "foreign_language_description",
            {issue["type"] for issue in result["issues"]},
        )
        self.assertEqual(stored["description"], "")
        self.assertEqual(
            stored["image_url"],
            "https://magasiner.familiprix.com/medias/exact.png",
        )
        self.assertEqual(reference.get("description", ""), "")
        self.assertIn("description", reference["_quarantined_fields"])
        self.assertEqual(rejected["verification_status"], "rejected")
        self.assertEqual(rejected["active"], 0)
        db.close()

    def test_foreign_description_never_reaches_cards_search_or_ai(self):
        foreign_description = (
            "Vitamin supplements for kids мултивитрини за деца нителна "
            "добавка с подсладител и дъвчащи таблетки за деца."
        )
        raw = {
            "id": 1, "name": "TEST VITAMIN", "barcode": "063848966068",
            "description": foreign_description,
            "description_status": "possible_wrong",
            "_verified_fields": [],
        }

        public = row_to_product(raw)
        search_row = _product_search_row(raw)
        context = product_context_for_client_rag(raw)

        self.assertEqual(public["description"], "")
        self.assertTrue(public["description_quarantined"])
        self.assertNotIn("мултивитрини", search_row["_hay"])
        self.assertEqual(context["description"], "")
        self.assertEqual(context["notes"], "")
        self.assertTrue(context["description_quarantined"])

    def test_french_scientific_description_is_not_quarantined(self):
        description = (
            "Formule française avec bêta-carotène et vitamine C. "
            "Convient au format de 100 comprimés et contient 500 mg par unité."
        )
        self.assertEqual(description_quality_issue(description), "")

    def test_quality_repair_restores_saved_verified_french_description(self):
        db = self.make_db()
        barcode = "063848966068"
        foreign_description = (
            "Verfris je haar met deze droogshampoo tussen wasbeurten. "
            "Deze verzorging geeft je haar glans en helpt uitgroei camoufleren."
        )
        french_description = (
            "Shampooing sec rafraîchissant pour cheveux bruns, format 200 ml. "
            "Il absorbe l'excès de sébum entre les lavages."
        )
        db.execute(
            """INSERT INTO product_reference
               (barcode, gtin_key, name, description, source)
               VALUES (?, ?, 'TEST SHP SEC 200ML', ?, 'Planogramme')""",
            (barcode, gtin_identity_key(barcode), foreign_description),
        )
        product_id = self.insert_product(
            db, name="TEST SHP SEC 200ML", barcode=barcode,
        )
        db.execute(
            "UPDATE products SET description=? WHERE id=?",
            (foreign_description, product_id),
        )
        record_reference_evidence(
            db, barcode, "description", french_description,
            source="Familiprix", source_record_id="123",
            verification_status="verified", active=False,
        )

        result = repair_quarantined_descriptions(
            db, now="2026-08-04T12:00:00Z"
        )

        reference = db.execute(
            "SELECT description FROM product_reference WHERE barcode=?", (barcode,)
        ).fetchone()
        product = db.execute(
            "SELECT description, description_status FROM products WHERE id=?",
            (product_id,),
        ).fetchone()
        old_evidence = db.execute(
            """SELECT verification_status FROM product_reference_evidence
               WHERE field_value=?""",
            (foreign_description,),
        ).fetchone()
        self.assertEqual(result["reference_descriptions_repaired"], 1)
        self.assertEqual(result["product_descriptions_repaired"], 1)
        self.assertEqual(reference["description"], french_description)
        self.assertEqual(product["description"], french_description)
        self.assertEqual(product["description_status"], "verified")
        self.assertEqual(old_evidence["verification_status"], "rejected")
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

    def test_cold_corpus_loads_only_winning_evidence_for_current_values(self):
        db = self.make_db()
        product_id = self.insert_product(db, name="Neutral item")
        db.execute(
            "UPDATE products SET brand='Exact Brand' WHERE id=?",
            (product_id,),
        )
        db.execute(
            """INSERT INTO product_field_evidence
               (product_id, field_name, field_value, source, source_priority,
                confidence, verification_status, active)
               VALUES (?, 'brand', 'Wrong Brand', 'weak-source', 10,
                       0.5, 'verified', 1)""",
            (product_id,),
        )
        db.execute(
            """INSERT INTO product_field_evidence
               (product_id, field_name, field_value, source, source_priority,
                confidence, verification_status, active)
               VALUES (?, 'brand', 'Exact Brand', 'trusted-source', 90,
                       0.95, 'verified', 1)""",
            (product_id,),
        )

        evidence = [
            dict(row) for row in _verified_current_product_field_sources(db)
        ]
        self.assertEqual(len(evidence), 1)
        self.assertEqual(evidence[0]["source"], "trusted-source")
        self.assertNotIn("field_value", evidence[0])

        products_module._PROD_CACHE.update(
            key=None, rows=[], initialized=False, database_token=None,
        )
        item, search_row = _products_corpus(db)[0]
        self.assertIn("brand", item["_verified_fields"])
        self.assertEqual(search_row["_brand"], "exact brand")
        db.close()

    def test_aggregated_evidence_fingerprints_reject_stale_field_values(self):
        db = self.make_db()
        product_id = self.insert_product(db, name="Neutral item")
        db.execute(
            "UPDATE products SET brand='Exact Brand', category='Current' WHERE id=?",
            (product_id,),
        )
        digest = lambda value: hashlib.md5(value.encode("utf-8")).hexdigest()
        evidence = [{
            "product_id": product_id,
            "field_names": "brand,category",
            "field_hashes": f"{digest('Exact Brand')},{digest('Old category')}",
            "source": "trusted-source",
            "source_url": "https://example.com/product",
            "last_verified_at": "2026-08-03T00:00:00Z",
        }]

        products_module._PROD_CACHE.update(
            key=None, rows=[], initialized=False, database_token=None,
        )
        with patch(
            "routes.products._verified_current_product_field_sources",
            return_value=evidence,
        ):
            item, search_row = _products_corpus(db)[0]

        self.assertIn("brand", item["_verified_fields"])
        self.assertNotIn("category", item["_verified_fields"])
        self.assertEqual(search_row["_brand"], "exact brand")
        self.assertNotIn("current", search_row["_identity_hay"])
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

    def test_cold_corpus_keeps_probable_reference_identifiers_searchable(self):
        db = self.make_db()
        barcode = "063848966068"
        product_id = self.insert_product(
            db, name="ACETAMINOPHENE 500MG", barcode=barcode,
        )
        gtin_key = gtin_identity_key(barcode)
        db.execute(
            "UPDATE products SET gtin_key=? WHERE id=?",
            (gtin_key, product_id),
        )
        db.execute(
            """INSERT INTO product_reference_identifiers
               (gtin_key, barcode, identifier_type, identifier_value, normalized_value,
                source, confidence, verification_status)
               VALUES (?, ?, 'DIN', '00559407', '00559407',
                       'Health Canada candidate', 0.8, 'requires_review')""",
            (gtin_key, barcode),
        )
        products_module._PROD_CACHE.update(
            key=None, rows=[], initialized=False, database_token=None,
        )

        corpus = _products_corpus(db, allow_identifier_stale=False)
        matches = _indexed_identifier_products(
            corpus, "00559407", "din", limit=10,
        )

        self.assertEqual([item["id"] for item in matches], [product_id])
        self.assertEqual(
            matches[0]["_identifiers"][-1]["verification_status"],
            "requires_review",
        )
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

    def test_warm_inverted_index_avoids_full_catalogue_scans(self):
        db = self.make_db()
        original_cache = dict(products_module._PROD_CACHE)

        def restore_cache():
            products_module._PROD_CACHE.clear()
            products_module._PROD_CACHE.update(original_cache)
            products_module._PROD_CACHE.update(
                key=None, rows=[], initialized=False, generation=-1,
                database_token=None,
                statistics_rows_id=0, token_postings={}, token_prefixes={},
                name_token_postings={}, name_tokens_by_initial={},
                mapped_indices_by_key={}, document_in_stock={},
                representative_indices=(), document_barcodes=(),
                identifier_postings={}, product_id_to_key={},
            )

        self.addCleanup(restore_cache)
        products_module._PROD_CACHE.update(
            key=None, rows=[], initialized=False, generation=-1,
            database_token=None,
            statistics_rows_id=0, token_postings={}, token_prefixes={},
            name_token_postings={}, name_tokens_by_initial={},
            mapped_indices_by_key={}, document_in_stock={},
            representative_indices=(), document_barcodes=(),
            identifier_postings={}, product_id_to_key={},
        )
        rows = [
            (
                f"UNRELATED PRODUCT {index}",
                f"900000{index:06d}",
                str(index + 1),
            )
            for index in range(2000)
        ]
        rows.extend([
            ("A GAGNON MELAT 5MG GUM 120", "063848966068", "2001"),
            ("ADVIL 200MG CO100", "012345678905", "2002"),
            ("ADVIL 200MG CO100", "012345678905", "2003"),
            ("OFF CHASSE MOUST VAPO 142G", "062600000001", "2004"),
            ("AFTER BITE G/TRAIT 20G", "062600000002", "2005"),
        ])
        db.executemany(
            """INSERT INTO products
               (name, barcode, aisle, side, section, shelf, position)
               VALUES (?, ?, '1', 'Gauche', '1', '1', ?)""",
            rows,
        )
        db.execute(
            "UPDATE products SET description='Spray anti generique.' "
            "WHERE name LIKE 'UNRELATED PRODUCT %'"
        )
        db.execute(
            "UPDATE products SET description='Traitement des piqures de moustiques.' "
            "WHERE name='AFTER BITE G/TRAIT 20G'"
        )
        db.commit()

        corpus = _products_corpus(db, allow_identifier_stale=False)
        melatonin_subset = _indexed_client_search_entries(
            corpus, literal_terms=["melatonine"],
        )
        typo_subset = _indexed_client_search_entries(
            corpus, literal_terms=["advile"], fuzzy_terms=["advile"],
        )
        missing_subset = _indexed_client_search_entries(
            corpus, literal_terms=["zzzznotfound"],
            fuzzy_terms=["zzzznotfound"],
        )
        broad_mosquito_subset = _indexed_client_search_entries(
            corpus, literal_terms=["spray", "anti", "moustique"],
        )
        focused_mosquito_subset = _indexed_client_search_entries(
            corpus, literal_terms=["spray", "anti", "moustique"],
            anchor_terms=["moustique"],
        )

        self.assertLess(len(melatonin_subset), 10)
        self.assertTrue(any(
            "MELAT" in item["name"] for item, _row in melatonin_subset
        ))
        self.assertLess(len(typo_subset), 10)
        self.assertTrue(any(
            item["name"].startswith("ADVIL") for item, _row in typo_subset
        ))
        self.assertEqual(missing_subset, [])
        self.assertGreater(len(broad_mosquito_subset), 1000)
        self.assertEqual(
            [item["name"] for item, _row in focused_mosquito_subset],
            ["OFF CHASSE MOUST VAPO 142G"],
        )
        with patch.object(products_module, "get_db", return_value=db):
            matches = hybrid_client_candidates(
                "melatonine",
                {
                    "corrected_query": "melatonine",
                    "search_queries": [],
                    "keywords": [],
                    "must_include": [],
                    "exclude": [],
                },
                limit=20,
            )
        self.assertEqual(
            [item["name"] for item in matches],
            ["A GAGNON MELAT 5MG GUM 120"],
        )
        advil = _materialize_mapped_products(
            corpus, [("barcode", "012345678905")], limit=10,
        )
        self.assertEqual(len(advil), 1)
        self.assertEqual(len(advil[0]["locations"]), 2)
        advil_product_id = next(
            item["id"] for item, _row in corpus
            if item["name"].startswith("ADVIL")
        )
        upsert_product_identifier(
            db, advil_product_id, "DIN", "00559407",
            source="Health Canada",
            verification_status="requires_review",
        )
        products_module._PROD_CACHE.update(key=None)
        corpus = _products_corpus(db, allow_identifier_stale=False)
        din_matches = _indexed_identifier_products(
            corpus, "00559407", "din", limit=10,
        )
        self.assertTrue(din_matches)
        self.assertTrue(all(
            item["name"].startswith("ADVIL") for item in din_matches
        ))
        self.assertTrue(any(
            identifier.get("verification_status") == "requires_review"
            for identifier in din_matches[0].get("_identifiers", [])
        ))
        db.close()

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
