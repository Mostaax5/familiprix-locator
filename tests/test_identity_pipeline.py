import json
import unittest
from pathlib import Path
from unittest.mock import patch

from product_data import assess_metadata_candidate, classify_source
from routes.ai import (
    _find_product_node, online_matches_catalog, page_mentions_barcode,
    parse_familiprix_product_page, parse_generic_pharmacy_product_page,
)


UPC = "063848966068"
OTHER = "041388316000"

# Synthetic regression cases, not a claim about real-store accuracy.
# Each tuple is (label, existing name, candidate name, may auto-apply).
IDENTITY_CASES = json.loads((Path(__file__).resolve().parents[1] / "evaluation" / "identity_cases.json").read_text())


class IdentityPipelineTests(unittest.TestCase):
    def test_identity_benchmark(self):
        for label, existing, incoming, expected in IDENTITY_CASES:
            with self.subTest(case=label):
                result = assess_metadata_candidate(
                    {"barcode": UPC, "brand": "Acme", "name": existing},
                    {"barcode": UPC, "brand": "Acme", "name": incoming,
                     "source": "Manufacturer exact product"},
                )
                self.assertEqual(result.auto_apply, expected, result.issues)

    def test_trusted_exact_source_beats_rich_open_database(self):
        from routes import ai
        open_record = {"barcode": UPC, "name": "Acme shampoo large bottle", "source": "Open Beauty Facts",
                       "image_url": "https://example.com/open.jpg", "description": "Long description"}
        trusted = {"barcode": UPC, "name": "Acme shampoo", "source": "Familiprix"}
        with patch.object(ai, "best_lookup_result", side_effect=[(open_record, 100), (trusted, 30)]) as lookup:
            result = ai.lookup_product_online(UPC)
        self.assertEqual(result, trusted)
        self.assertEqual(lookup.call_count, 2)

    def test_parallel_lookup_waits_for_verified_identity(self):
        from routes import ai
        open_record = {"barcode": UPC, "name": "Acme shampoo large bottle", "source": "Open Beauty Facts"}
        trusted = {"barcode": UPC, "name": "Acme shampoo", "source": "Familiprix"}
        verified = lambda row: bool(row) and assess_metadata_candidate({"barcode": UPC}, row).auto_apply
        result, _ = ai.best_lookup_result([lambda: open_record, lambda: trusted],
                                         max_workers=1, good_enough=1, verified=verified)
        self.assertEqual(result, trusted)

    def test_nested_or_decorated_identifier_is_not_exact_barcode(self):
        from routes.ai import structured_product_matches_barcode
        for value in ({"value": UPC}, [UPC], "product-" + UPC):
            self.assertFalse(structured_product_matches_barcode({"gtin": value}, [UPC]))

    def test_explicit_strength_uses_compatible_mass_units(self):
        for candidate_strength, expected in (("1000 mg", True), ("200 mg", False)):
            result = assess_metadata_candidate(
                {"barcode": UPC, "name": "Acme", "strength": "1 g"},
                {"barcode": UPC, "name": "Acme", "strength": candidate_strength, "source": "Manufacturer"},
            )
            self.assertEqual(result.auto_apply, expected)

    def test_known_scan_never_waits_for_online_sources(self):
        from flask import Flask
        from routes.ai import ai_bp
        app = Flask(__name__)
        app.register_blueprint(ai_bp)
        with patch("routes.ai.reference_lookup", return_value={"barcode": UPC, "name": "Acme", "image_url": ""}), \
             patch("routes.ai.lookup_product_online") as online, \
             patch("routes.products.schedule_image_fill") as schedule:
            result = app.test_client().get("/api/products/lookup/" + UPC)
        self.assertTrue(result.json["cache_hit"])
        online.assert_not_called()
        schedule.assert_called_once_with([UPC])

    def test_package_unit_is_part_of_identity(self):
        result = assess_metadata_candidate(
            {"barcode": UPC, "name": "Product", "package_size": "100", "package_unit": "ml"},
            {"barcode": UPC, "name": "Product", "package_size": "200", "package_unit": "ml", "source": "Manufacturer"},
        )
        self.assertFalse(result.auto_apply)

    def test_dosage_in_instructions_cannot_hide_wrong_package_strength(self):
        result = assess_metadata_candidate(
            {"barcode": UPC, "name": "Acme 200mg", "description": "Maximum daily dose 400mg"},
            {"barcode": UPC, "name": "Acme 400mg", "description": "Compare to 200mg version", "source": "Manufacturer"},
        )
        self.assertFalse(result.auto_apply)

    def test_missing_identity_is_not_filled_from_request(self):
        self.assertFalse(online_matches_catalog("Acme", "", {"name": "Acme", "source": "Familiprix"}, UPC))
        self.assertFalse(assess_metadata_candidate({}, {"source": "Familiprix"}, "exact_familiprix_code").auto_apply)

    def test_ai_citation_does_not_turn_generated_text_into_retailer_evidence(self):
        self.assertEqual(classify_source("Recherche IA", "https://magasiner.familiprix.com/fr/p/123")[0], "ai_suggestion")

    def test_parent_record_does_not_inherit_child_upc(self):
        node = {"name": "Wrong shampoo", "gtin12": OTHER, "image": "https://example.com/wrong.jpg",
                "recommendations": [{"name": "Correct toothpaste", "gtin12": UPC, "image": "https://example.com/right.jpg"}]}
        result = _find_product_node(node, [UPC], 0)
        self.assertEqual(result["name"], "Correct toothpaste")
        self.assertEqual(result["image_url"], "https://example.com/right.jpg")

    def test_page_numbers_cannot_be_concatenated_into_upc(self):
        self.assertFalse(page_mentions_barcode("063848</span><span>966068", [UPC]))
        self.assertFalse(page_mentions_barcode("123" + UPC + "99", [UPC]))

    def test_related_product_cannot_prove_page_identity(self):
        html = '<h1>Wrong product</h1><p>Related item: ' + UPC + '</p><meta property="og:image" content="https://example.com/wrong.jpg">'
        self.assertIsNone(parse_familiprix_product_page(html, "https://magasiner.familiprix.com/fr/p/123", UPC))
        self.assertIsNone(parse_generic_pharmacy_product_page("Brunet", html, "https://brunet.ca/product/123", UPC))

    def test_recommendation_entity_does_not_import_main_page_photo(self):
        entity = {"@type": "Product", "name": "Correct product", "gtin12": UPC}
        html = '<script type="application/ld+json">' + json.dumps(entity) + '</script><meta property="og:image" content="https://example.com/wrong.jpg">'
        result = parse_familiprix_product_page(html, "https://magasiner.familiprix.com/fr/p/123", UPC)
        self.assertEqual(result["image_url"], "")

    def test_recommendation_sku_does_not_override_page_sku(self):
        entity = {"@type": "Product", "name": "Correct product", "gtin12": UPC, "sku": "456"}
        html = '<script type="application/ld+json">' + json.dumps(entity) + '</script>'
        self.assertIsNone(parse_familiprix_product_page(html, "https://magasiner.familiprix.com/fr/p/123", UPC))
