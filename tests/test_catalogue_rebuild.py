import copy
import json
import tempfile
import unittest
from pathlib import Path

from rebuild_catalogue import rebuild, prepare


class CatalogueRebuildTests(unittest.TestCase):
    def fixture(self):
        return {"export_version": 2, "products": [
            {"id": 7, "barcode": "0063848966068", "name": "ACME VIT C CO50", "aisle": "2", "side": "Gauche",
             "section": "1", "shelf": "3", "position": "4", "image_url": "https://wrong.test/photo.jpg",
             "description": "Generated description", "search_terms": "unrelated terms"},
        ], "aisle_layouts": [{"aisle": "2", "config_json": "{}"}], "product_data": {
            "product_field_evidence": [{"product_id": 7, "field_name": "description", "field_value": "Generated description",
                                        "source_type": "ai_suggestion", "verification_status": "verified", "active": 1}],
            "product_expiry_status": [{"barcode": "0063848966068", "earliest_expiry_date": "2027-03-01", "revision": 3}],
        }}

    def test_reset_preserves_upc_locations_layout_and_expiry(self):
        original = self.fixture()
        untouched = copy.deepcopy(original)
        clean, quarantine = rebuild(original)
        self.assertEqual(original, untouched)
        for field in ("id", "barcode", "name", "aisle", "side", "section", "shelf", "position"):
            self.assertEqual(clean["products"][0][field], original["products"][0][field])
        self.assertEqual(clean["aisle_layouts"], original["aisle_layouts"])
        self.assertEqual(clean["product_data"]["product_expiry_status"], original["product_data"]["product_expiry_status"])
        self.assertEqual(clean["products"][0]["description"], "")
        self.assertEqual(clean["products"][0]["image_url"], "")
        self.assertEqual(clean["product_data"]["product_field_evidence"][0]["active"], 0)
        self.assertEqual({entry["field"] for entry in quarantine}, {"description", "image_url", "search_terms"})

    def test_manual_confirmation_is_field_specific(self):
        original = self.fixture()
        original["product_data"]["product_field_evidence"][0]["source_type"] = "manual"
        clean, _ = rebuild(original)
        self.assertEqual(clean["products"][0]["description"], "Generated description")
        self.assertEqual(clean["products"][0]["image_url"], "")

    def test_numeric_upc_is_rejected_rather_than_losing_leading_zeros(self):
        original = self.fixture()
        original["products"][0]["barcode"] = 63848966068
        with self.assertRaisesRegex(ValueError, "strings"):
            rebuild(original)

    def test_original_bytes_saved_and_existing_directory_never_overwritten(self):
        with tempfile.TemporaryDirectory() as folder:
            source = Path(folder) / "backup.json"
            source.write_bytes(json.dumps(self.fixture(), indent=3).encode())
            output = Path(folder) / "review"
            manifest = prepare(source, output)
            self.assertEqual((output / "original.json").read_bytes(), source.read_bytes())
            self.assertEqual(manifest["products"], 1)
            with self.assertRaises(FileExistsError):
                prepare(source, output)
