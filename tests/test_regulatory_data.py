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
    HEALTH_CANADA_DPD_UPC_NOTICE,
    download_dpd_matches,
    extract_regulatory_identifiers,
    find_dpd_name_candidates,
    group_unambiguous_dpd_matches,
    parse_dpd_api_records,
    parse_dpd_extracts,
    verify_regulatory_candidate,
)
from routes.regulatory import _seed_catalogue_label_candidates
from routes.ai import _prefer_lookup_result, product_context_for_client_rag
from routes.products import (
    _PROD_CACHE,
    _direct_identifier_products,
    _fast_reference_score,
    _products_corpus,
    normalize_search_text,
    normalized_digits,
    public_product_payload,
    rank_products_by_field,
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

    def test_labeled_identifier_extraction_handles_retailer_html(self):
        found = extract_regulatory_identifiers(
            "<dl><dt>#DIN</dt><dd><span>00559407</span></dd></dl>"
        )
        self.assertEqual(found, [{"type": "DIN", "value": "00559407"}])

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

    def test_dpd_api_joins_din_only_through_exact_upc(self):
        packages = [
            {
                "drug_code": 42,
                "upc": "063848966068",
                "package_size_unit": "EA",
                "package_type": "Bottle",
                "package_size": "50",
                "product_information": "50 tablets",
            },
            {
                "drug_code": 43,
                "upc": "041388316000",
                "package_size_unit": "EA",
                "package_type": "Bottle",
                "package_size": "100",
                "product_information": "100 tablets",
            },
        ]
        drugs = [
            {
                "drug_code": 42,
                "drug_identification_number": "01234567",
                "brand_name": "EXACT DRUG",
                "descriptor": "200 MG",
                "last_update_date": "2026-07-01",
            },
            {
                "drug_code": 43,
                "drug_identification_number": "07654321",
                "brand_name": "OTHER DRUG",
            },
        ]
        matches = parse_dpd_api_records(
            packages, drugs, {"gtin:00063848966068"}
        )
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0]["din"], "01234567")
        self.assertEqual(matches[0]["barcode"], "063848966068")

    def test_current_dpd_bulk_feed_is_not_used_for_upc_inference(self):
        phases = []
        matches, version = download_dpd_matches(
            {"gtin:00063848966068"}, progress=phases.append
        )
        self.assertEqual(matches, [])
        self.assertEqual(version, HEALTH_CANADA_DPD_UPC_NOTICE)
        self.assertEqual(phases, ["dpd_upc_retired"])

    def test_exact_upc_label_plus_official_din_name_is_verified(self):
        def fetch(url):
            if "drugproduct" in url:
                return [{
                    "drug_code": 42,
                    "drug_identification_number": "01234567",
                    "brand_name": "Advil Liqui-Gels 200 mg",
                    "company_name": "Haleon",
                }]
            if "packaging" in url:
                return [{"drug_code": 42, "upc": ""}]
            return []

        result = verify_regulatory_candidate({
            "type": "DIN", "value": "01234567",
            "barcode": "063848966068",
            "product_name": "Advil Liqui-Gels 200 mg 40 capsules",
        }, fetch_json=fetch)
        self.assertTrue(result["verified"])
        self.assertEqual(result["confidence"], 0.98)
        self.assertEqual(
            result["match_method"],
            "exact_gtin_label_plus_health_canada_drug",
        )

        probable = verify_regulatory_candidate({
            "type": "DIN", "value": "01234567",
            "barcode": "063848966068",
            "product_name": "Unrelated shampoo",
        }, fetch_json=fetch)
        self.assertFalse(probable["verified"])
        self.assertTrue(probable["probable"])

    def test_official_name_candidate_is_real_but_requires_review(self):
        def fetch(url):
            self.assertIn("brandname=advil", url)
            return [
                {
                    "drug_code": 42,
                    "drug_identification_number": "01234567",
                    "brand_name": "Advil Liqui-Gels 200 mg",
                    "company_name": "Haleon",
                },
                {
                    "drug_code": 99,
                    "drug_identification_number": "07654321",
                    "brand_name": "Unrelated shampoo",
                },
            ]

        candidates = find_dpd_name_candidates(
            "Advil Liqui-Gels 200 mg 40 capsules", fetch_json=fetch
        )
        self.assertEqual([item["value"] for item in candidates], ["01234567"])
        self.assertEqual(
            candidates[0]["match_method"], "health_canada_name_candidate"
        )
        self.assertLess(candidates[0]["confidence"], 1.0)

        self.assertEqual(
            find_dpd_name_candidates(
                "Natural melatonin 5 mg",
                fetch_json=lambda _url: self.fail("generic terms must not query DPD"),
            ),
            [],
        )

    def test_french_extra_strength_abbreviation_ranks_the_correct_din(self):
        records = [
            {
                "drug_code": 1,
                "drug_identification_number": "00559393",
                "brand_name": "TYLENOL REGULAR STRENGTH",
            },
            {
                "drug_code": 2,
                "drug_identification_number": "00559407",
                "brand_name": "TYLENOL EXTRA STRENGTH",
            },
            {
                "drug_code": 3,
                "drug_identification_number": "02046040",
                "brand_name": "CHILDREN'S TYLENOL",
            },
            {
                "drug_code": 4,
                "drug_identification_number": "02046059",
                "brand_name": "INFANTS' TYLENOL",
            },
        ]
        candidates = find_dpd_name_candidates(
            "TYLENOL 500MG X/F FAC CO100",
            fetch_json=lambda _url: records,
        )
        self.assertEqual(candidates[0]["value"], "00559407")

    def test_existing_exact_upc_description_seeds_identifier_candidate(self):
        db = self.make_db()
        db.execute(
            """INSERT INTO product_reference
               (barcode, name, description, source, source_url)
               VALUES (?, ?, ?, ?, ?)""",
            (
                "063848966068", "Natural sleep product",
                "Melatonin tablets. NPN 80123456.",
                "Exact retailer page", "https://example.test/product",
            ),
        )
        seeded = _seed_catalogue_label_candidates(
            db, "2026-07-22T00:00:00+00:00"
        )
        row = db.execute(
            """SELECT identifier_type, identifier_value, match_method,
                      verification_status
               FROM product_reference_identifiers"""
        ).fetchone()
        self.assertEqual(seeded, 1)
        self.assertEqual(row["identifier_type"], "NPN")
        self.assertEqual(row["identifier_value"], "80123456")
        self.assertEqual(row["match_method"], "exact_gtin_labeled_source")
        self.assertEqual(row["verification_status"], "requires_review")
        db.close()

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
        public_item = public_product_payload(item)
        query = "80123456"
        score = _fast_reference_score(
            row, normalize_search_text(query), normalized_digits(query),
            tokenize_search_query(query), [], [],
        )
        self.assertGreaterEqual(score, 100)
        self.assertEqual(
            public_item["regulatory_identifiers"][0]["status"], "probable"
        )
        ai_context = product_context_for_client_rag(item)
        self.assertEqual(ai_context["verified_identifiers"], [])
        self.assertEqual(
            [
                identifier["value"]
                for identifier in ai_context["unconfirmed_identifier_candidates"]
            ],
            ["80123456"],
        )
        self.assertTrue(
            ai_context["unconfirmed_identifier_candidates"][0][
                "must_confirm_on_package"
            ]
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

    def test_name_candidate_is_searchable_and_never_an_ai_fact(self):
        db = self.make_db()
        product_id = db.execute(
            """INSERT INTO products
               (name, barcode, aisle, side, section, shelf, position)
               VALUES ('Advil 200 mg', '063848966068', '1', 'Gauche', '1', '1', '1')"""
        ).lastrowid
        upsert_reference_identifier(
            db, "063848966068", "DIN", "01234567",
            authority="Health Canada", source="Health Canada DPD",
            source_record_id="42", match_method="health_canada_name_candidate",
            confidence=0.61, verification_status="requires_review",
        )
        product = dict(db.execute(
            "SELECT * FROM products WHERE id=?", (product_id,)
        ).fetchone())
        self.assertEqual(sync_reference_identifiers_to_product(db, product), 1)
        _PROD_CACHE.update(key=None, rows=[])
        item, _row = _products_corpus(db)[0]
        public_item = public_product_payload(item)
        self.assertEqual(
            [match["id"] for match in rank_products_by_field(
                [item], "01234567", "din"
            )],
            [product_id],
        )
        self.assertEqual(
            [match["id"] for match in _direct_identifier_products(
                db, "01234567", "din", limit=5
            )],
            [product_id],
        )
        self.assertEqual(
            public_item["regulatory_identifiers"][0]["status"], "probable"
        )
        self.assertEqual(
            public_item["regulatory_identifiers"][0]["match_method"],
            "health_canada_name_candidate",
        )
        ai_context = product_context_for_client_rag(item)
        self.assertEqual(ai_context["verified_identifiers"], [])
        self.assertEqual(
            ai_context["unconfirmed_identifier_candidates"][0]["usage"],
            "retrieval_clue_only",
        )
        self.assertTrue(
            ai_context["unconfirmed_identifier_candidates"][0]["may_be_wrong"]
        )
        db.close()

    def test_reference_candidate_is_searchable_before_product_copy_finishes(self):
        db = self.make_db()
        product_id = db.execute(
            """INSERT INTO products
               (name, barcode, aisle, side, section, shelf, position)
               VALUES ('Advil 200 mg', '063848966068', '1', 'Gauche', '1', '1', '1')"""
        ).lastrowid
        upsert_reference_identifier(
            db, "063848966068", "DIN", "01234567",
            authority="Health Canada", source="Health Canada DPD",
            source_record_id="42", match_method="health_canada_name_candidate",
            confidence=0.61, verification_status="requires_review",
        )

        self.assertEqual(
            db.execute(
                "SELECT COUNT(*) FROM product_identifiers WHERE product_id=?",
                (product_id,),
            ).fetchone()[0],
            0,
        )
        matches = _direct_identifier_products(
            db, "01234567", "din", limit=5
        )
        self.assertEqual([match["id"] for match in matches], [product_id])
        self.assertEqual(
            [
                match["id"] for match in _direct_identifier_products(
                    db, "01234567", "identifier", limit=5
                )
            ],
            [product_id],
        )
        self.assertEqual(
            matches[0]["regulatory_identifiers"][0]["status"], "probable"
        )
        self.assertEqual(
            matches[0]["regulatory_identifiers"][0]["label"], "À confirmer"
        )
        db.close()

    def test_all_identifier_candidates_survive_a_replaced_planogram_row(self):
        db = self.make_db()
        old_product_id = db.execute(
            """INSERT INTO products
               (name, barcode, aisle, side, section, shelf, position)
               VALUES ('Example medication', '063848966068', '1', 'Gauche', '1', '1', '1')"""
        ).lastrowid
        candidates = (
            ("01234567", 0.62, "requires_review"),
            ("02345678", 0.18, "requires_review"),
            ("03456789", 0.05, "rejected"),
        )
        for index, (value, confidence, status) in enumerate(candidates):
            upsert_reference_identifier(
                db, "063848966068", "DIN", value,
                authority="Health Canada", source="Health Canada DPD",
                source_record_id=str(index),
                match_method="health_canada_name_candidate",
                confidence=confidence, verification_status=status,
            )
        product = dict(db.execute(
            "SELECT * FROM products WHERE id=?", (old_product_id,)
        ).fetchone())
        self.assertEqual(sync_reference_identifiers_to_product(db, product), 2)

        # Replace mode archives the old placement and creates a new row. The
        # copied rows disappear, while the GTIN-linked candidates must remain.
        db.execute(
            "DELETE FROM product_identifiers WHERE product_id=?",
            (old_product_id,),
        )
        db.execute("DELETE FROM products WHERE id=?", (old_product_id,))
        new_product_id = db.execute(
            """INSERT INTO products
               (name, barcode, aisle, side, section, shelf, position)
               VALUES ('Example medication', '063848966068', '1', 'Gauche', '1', '1', '1')"""
        ).lastrowid
        self.assertEqual(
            db.execute(
                "SELECT COUNT(*) FROM product_identifiers WHERE product_id=?",
                (new_product_id,),
            ).fetchone()[0],
            0,
        )

        _PROD_CACHE.update(key=None, rows=[], built_at=0.0)
        item, _row = _products_corpus(db)[0]
        public_item = public_product_payload(item)
        self.assertEqual(
            {
                identifier["value"]
                for identifier in public_item["regulatory_identifiers"]
            },
            {value for value, _confidence, _status in candidates},
        )
        self.assertTrue(all(
            identifier["status"] == "probable"
            for identifier in public_item["regulatory_identifiers"]
        ))
        for value, _confidence, _status in candidates:
            self.assertEqual(
                [
                    match["id"] for match in _direct_identifier_products(
                        db, value, "din", limit=5
                    )
                ],
                [new_product_id],
            )
        ai_context = product_context_for_client_rag(item)
        self.assertEqual(ai_context["verified_identifiers"], [])
        self.assertEqual(
            {
                identifier["value"]
                for identifier in ai_context["unconfirmed_identifier_candidates"]
            },
            {value for value, _confidence, _status in candidates},
        )
        db.close()

    def test_ai_context_never_promotes_or_duplicates_uncertain_identifiers(self):
        context = product_context_for_client_rag({
            "name": "Example product",
            "barcode": "063848966068",
            "_identifiers": [
                {
                    "type": "DIN", "value": "01234567",
                    "authority": "Health Canada",
                    "verification_status": "verified",
                },
                {
                    "type": "DIN", "value": "01234567",
                    "authority": "Health Canada",
                    "verification_status": "requires_review",
                    "confidence": 0.4,
                },
                {
                    "type": "NPN", "value": "80123456",
                    "authority": "Health Canada",
                    "verification_status": "requires_review",
                    "confidence": 0.7,
                },
                {
                    "type": "DIN-HM", "value": "80012345",
                    "authority": "Health Canada",
                    "verification_status": "rejected",
                    "confidence": 0.05,
                },
            ],
        })
        self.assertEqual(
            context["verified_identifiers"],
            [{
                "type": "DIN", "value": "01234567",
                "authority": "Health Canada",
            }],
        )
        self.assertEqual(
            {
                (identifier["type"], identifier["value"])
                for identifier in context["unconfirmed_identifier_candidates"]
            },
            {("NPN", "80123456"), ("DIN_HM", "80012345")},
        )
        self.assertTrue(all(
            identifier["status"] == "unconfirmed"
            and identifier["must_confirm_on_package"]
            and identifier["may_be_wrong"]
            for identifier in context["unconfirmed_identifier_candidates"]
        ))

    def test_low_confidence_review_candidate_is_copied_and_searchable(self):
        db = self.make_db()
        product_id = db.execute(
            """INSERT INTO products
               (name, barcode, aisle, side, section, shelf, position)
               VALUES ('Example natural product', '063848966068', '1', 'Gauche', '1', '1', '1')"""
        ).lastrowid
        upsert_reference_identifier(
            db, "063848966068", "NPN", "80123456",
            authority="Health Canada", source="External candidate",
            source_record_id="low-confidence",
            match_method="health_canada_name_candidate",
            confidence=0.05, verification_status="requires_review",
        )
        product = dict(db.execute(
            "SELECT * FROM products WHERE id=?", (product_id,)
        ).fetchone())
        self.assertEqual(sync_reference_identifiers_to_product(db, product), 1)

        _PROD_CACHE.update(key=None, rows=[], built_at=0.0)
        item, _row = _products_corpus(db)[0]
        public_item = public_product_payload(item)
        self.assertEqual(
            [
                identifier["value"]
                for identifier in public_item["regulatory_identifiers"]
            ],
            ["80123456"],
        )
        self.assertEqual(
            [
                match["id"] for match in _direct_identifier_products(
                    db, "80123456", "npn", limit=5
                )
            ],
            [product_id],
        )
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
