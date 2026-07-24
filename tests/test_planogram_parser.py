import unittest
from unittest.mock import patch

from routes.import_export import (
    _fast_planogram_is_trustworthy,
    _parse_planogram_file,
    _pdf_cells_from_group,
)


class FastPlanogramParserTests(unittest.TestCase):
    def test_coordinate_row_keeps_description_and_ajout_separate(self):
        schema = {
            "t": 59, "p": 105, "f": 151, "u": 213, "c": 275,
            "d": 403, "a": 530, "s": 583, "e": 632, "comments": 707,
        }
        values = [
            ("3", 59), ("4", 105), ("1", 151),
            ("3616306830165", 213), ("244041", 275),
            ("SALLY", 316), ("G", 338), ("REHAB", 362),
            ("SER", 392), ("REPAR", 422), ("ONG", 453),
            ("2", 473), ("oui", 530), ("Actif", 583), ("oui", 635),
        ]
        words = [
            {"text": text, "x": 62, "y": coordinate, "order": order}
            for order, (text, coordinate) in enumerate(values)
        ]

        cells = _pdf_cells_from_group(words, schema, "y")

        self.assertEqual(cells[3], "3616306830165")
        self.assertEqual(cells[4], "244041")
        self.assertEqual(cells[5], "SALLY G REHAB SER REPAR ONG 2")
        self.assertEqual(cells[6], "oui")

    def test_validation_rejects_incomplete_row_coverage(self):
        product = {
            "barcode": "063848966068",
            "code_familiprix": "189026",
            "name": "BIOMEDIC PRODUCT",
        }
        complete = {
            "headers": 1, "candidate_upc_rows": 1,
            "candidate_slots": {(1, 1)}, "document_upc_tokens": 1,
        }
        incomplete = {
            **complete,
            "document_upc_tokens": 2,
        }
        self.assertTrue(_fast_planogram_is_trustworthy([product], complete))
        self.assertFalse(_fast_planogram_is_trustworthy([product], incomplete))

    def test_untrusted_fast_parse_uses_compatibility_fallback(self):
        fallback_products = [{"barcode": "1", "name": "Fallback"}]
        with (
            patch(
                "routes.import_export._parse_planogram_pdf_fast",
                return_value=([], {}, {"headers": 0, "candidate_slots": set()}),
            ),
            patch(
                "routes.import_export._parse_planogram_pdf_compatibility",
                return_value=(fallback_products, {"name": "Plano"}),
            ),
        ):
            products, metadata, method = _parse_planogram_file("example.pdf")

        self.assertEqual(products, fallback_products)
        self.assertEqual(metadata["name"], "Plano")
        self.assertEqual(method, "pdfplumber-fallback")


if __name__ == "__main__":
    unittest.main()
