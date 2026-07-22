import os
import sqlite3
import tempfile
import unittest
import zipfile

from database import DatabaseConnection, init_sqlite_db
from product_data import (
    sync_reference_identifiers_to_product,
    upsert_reference_identifier,
)
from regulatory_data import (
    extract_regulatory_identifiers,
    group_unambiguous_dpd_matches,
    parse_dpd_extracts,
    verify_regulatory_candidate,
)
from routes.ai import _prefer_lookup_result, product_context_for_client_rag
from routes.products import (
    _PROD_CACHE,
    _fast_reference_score,
    _products_corpus,
    normalize_search_text,
    normalized_digits,
    tokenize_search_query,
)


class RegulatoryDataTests(unittest.TestCase):
    def make_db(self):
        raw = sqlite3.connect(":memory:")
        raw.row_factory = sqlite3.Row
        db = DatabaseConnection(raw, "sqlite")
        init_sqlite_db(db)
        return db

    def make_zip(self, rows):
        handle, path = tempfile.mkstemp(suffix=".zip")
        os.close(handle)
        content = "\n".join(
            ",".join(f'"{str(value)}"' for value in row) for row in rows
        )
        with zipfile.ZipFile(path, "w") as archive:
            archive.writestr("extract.txt", content)
        self.addCleanup(lambda: os.path.exists(path) and os.remove(path))
        return path

    def test_labeled_identifier_extraction_never_guesses_unlabeled_numbers(self):
        found = extract_regulatory_identifiers(
            "DIN-HM 80012345, NPN: 80123456, DIN no. 01234567, lot 87654321"
        )
        self.assertEqual(
            {(item["type"], item["value"]) for item in found},
            {
                ("DIN_HM", "80012345"),
                ("NPN", "80123456"),
                ("DIN", "01234567"),
            },
        )

    def test_dpd_extract_joins_din_only_through_exact_upc(self):
        package_path = self.make_zip([
            ["42", "063848966068", "EA", "Bottle", "50", "50 tablets", "", ""],
            ["43", "041388316000", "EA", "Bottle", "100", "100 tablets", "", ""],
        ])
        drug_path = self.make_zip([
            ["42", "", "Human", "01234567", "EXACT DRUG", "200 MG", "", "", "1", "2026-07-01", "", "", "MÉDICAMENT EXACT", "200 MG"],
            ["43", "", "Human", "07654321", "OTHER DRUG", "", "", "", "1", "2026-07-01", "", "", "", ""],
        ])
        matches = parse_dpd_extracts(
            package_path, drug_path, {"gtin:00063848966068"}
        )
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0]["din"], "01234567")
        self.assertEqual(matches[0]["barcode"], "063848966068")

    def test_multiple_dins_for_one_exact_upc_are_a_conflict(self):
        rows = [
            {"gtin_key": "gtin:00063848966068", "din": "01234567"},
            {"gtin_key": "gtin:00063848966068", "din": "07654321"},
        ]
        verified, conflicts = group_unambiguous_dpd_matches(rows)
        self.assertFalse(verified)
        self.assertIn("gtin:00063848966068", conflicts)

    def test_verified_reference_identifier_follows_upc_to_placed_product(self):
        db = self.make_db()
        product_id = db.execute(
            """INSERT INTO products
               (name, barcode, aisle, side, section, shelf, position)
               VALUES ('Exact drug', '063848966068', '1', 'Gauche', '1', '1', '1')"""
        ).lastrowid
        upsert_reference_identifier(
            db, "063848966068", "DIN", "01234567",
            authority="Health Canada", source="Health Canada DPD",
            source_record_id="42",
            match_method="exact_gtin_health_canada_packaging",
            confidence=1.0, verification_status="verified",
            last_verified_at="2026-07-22T00:00:00+00:00",
        )
        product = dict(db.execute(
            "SELECT * FROM products WHERE id=?", (product_id,)
        ).fetchone())
        copied = sync_reference_identifiers_to_product(
            db, product, imported_at="2026-07-22T00:00:00+00:00"
        )
        row = db.execute(
            """SELECT identifier_value, verification_status, match_method
               FROM product_identifiers WHERE product_id=? AND identifier_type='DIN'""",
            (product_id,),
        ).fetchone()
        self.assertEqual(copied, 1)
        self.assertEqual(row["identifier_value"], "01234567")
        self.assertEqual(row["verification_status"], "verified")
        self.assertEqual(row["match_method"], "exact_gtin_health_canada_packaging")
        db.close()

    def test_probable_exact_upc_identifier_is_searchable_but_not_ai_fact(self):
        db = self.make_db()
        product_id = db.execute(
            """INSERT INTO products
               (name, barcode, aisle, side, section, shelf, position)
               VALUES ('Natural sleep product', '063848966068', '1', 'Gauche', '1', '1', '1')"""
        ).lastrowid
        upsert_reference_identifier(
            db, "063848966068", "NPN", "80123456",
            authority="Health Canada", source="Exact UPC product page",
            source_record_id="Natural sleep product",
            match_method="exact_gtin_labeled_source",
            confidence=0.75, verification_status="requires_review",
        )
        product = dict(db.execute(
            "SELECT * FROM products WHERE id=?", (product_id,)
        ).fetchone())
        self.assertEqual(sync_reference_identifiers_to_product(db, product), 1)

        _PROD_CACHE.update(key=None, rows=[])
        item, row = _products_corpus(db)[0]
        query = "80123456"
        score = _fast_reference_score(
            row, normalize_search_text(query), normalized_digits(query),
            tokenize_search_query(query), [], [],
        )
        self.assertGreaterEqual(score, 100)
        self.assertEqual(
            item["regulatory_identifiers"][0]["status"], "probable"
        )
        self.assertEqual(
            product_context_for_client_rag(item)["verified_identifiers"], []
        )

        upsert_reference_identifier(
            db, "063848966068", "NPN", "80123456",
            authority="Health Canada", source="Health Canada LNHPD",
            source_record_id="Natural sleep product",
            match_method="exact_gtin_label_plus_health_canada_licence",
            confidence=0.98, verification_status="verified",
            last_verified_at="2026-07-22T00:00:00+00:00",
        )
        sync_reference_identifiers_to_product(db, product)
        promoted = db.execute(
            """SELECT verification_status FROM product_identifiers
               WHERE product_id=? AND identifier_type='NPN'""",
            (product_id,),
        ).fetchone()
        self.assertEqual(promoted["verification_status"], "verified")
        db.close()

    def test_npn_requires_exact_official_licence_and_matching_name(self):
        def fetch(url):
            if "productlicence" in url:
                return {"data": [{
                    "lnhpd_id": 123,
                    "licence_number": "80123456",
                    "product_name": "Jamieson Melatonin 5 mg",
                    "company_name": "Jamieson Laboratories",
                    "dosage_form": "Tablet",
                    "flag_product_status": 1,
                }]}
            if "medicinalingredient" in url:
                return {"data": [{
                    "ingredient_name": "Melatonin", "quantity": 5,
                    "quantity_unit_of_measure": "mg",
                }]}
            if "productpurpose" in url:
                return {"data": [{"purpose": "Helps increase total sleep time."}]}
            if "productroute" in url:
                return {"data": [{"route_type_desc": "Oral"}]}
            return {"data": []}

        result = verify_regulatory_candidate({
            "type": "NPN", "value": "80123456",
            "barcode": "063848966068",
            "product_name": "Jamieson Melatonin 5 mg 100 tablets",
        }, fetch_json=fetch)
        self.assertTrue(result["verified"])
        self.assertEqual(result["ingredients"], "Melatonin 5 mg")
        self.assertIn("sleep time", result["purpose"])

        rejected = verify_regulatory_candidate({
            "type": "NPN", "value": "80123456",
            "barcode": "063848966068",
            "product_name": "Unrelated shampoo",
        }, fetch_json=fetch)
        self.assertFalse(rejected["verified"])
        self.assertTrue(rejected["probable"])

        abbreviated = verify_regulatory_candidate({
            "type": "NPN", "value": "80123456",
            "barcode": "063848966068",
            "product_name": "MELAT COMP 100",
        }, fetch_json=fetch)
        self.assertTrue(abbreviated["verified"])

    def test_slower_lookup_regulatory_evidence_is_not_lost(self):
        current = {
            "name": "Exact product", "barcode": "063848966068",
            "source": "Familiprix",
        }
        candidate = {
            "name": "Exact product", "barcode": "063848966068",
            "source": "Open Drug Facts",
            "regulatory_identifiers": [{
                "type": "NPN", "value": "80123456",
                "source_url": "https://example.test/exact",
            }],
        }
        selected, score = _prefer_lookup_result(current, 50, candidate, 20)
        self.assertEqual(score, 50)
        self.assertEqual(
            selected["regulatory_identifiers"][0]["value"], "80123456"
        )


if __name__ == "__main__":
    unittest.main()
