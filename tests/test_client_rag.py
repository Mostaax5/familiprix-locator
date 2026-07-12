import unittest
from unittest.mock import patch

from app import app
from routes.ai import classify_client_request, normalize_verified_client_answer
from routes.products import (
    find_existing_image_for_barcode,
    hybrid_client_candidates,
    normalize_search_text,
)


def search_row(name, brand="", description="", barcode=""):
    normalized_name = normalize_search_text(name)
    normalized_brand = normalize_search_text(brand)
    haystack = " ".join([
        normalized_name,
        normalized_brand,
        normalize_search_text(description),
    ])
    return {
        "_bc": barcode,
        "_name": normalized_name,
        "_brand": normalized_brand,
        "_hay": haystack,
        "_tokens": normalized_name.split(),
    }


class ClientRagTests(unittest.TestCase):
    def test_hybrid_retrieval_corrects_spoken_french_brand_typo(self):
        advil = {"id": 1, "name": "Advil Extra Fort", "brand": "Advil", "barcode": "111"}
        unrelated = {"id": 2, "name": "Tylenol Regular", "brand": "Tylenol", "barcode": "222"}
        corpus = [
            (advil, search_row(advil["name"], advil["brand"], barcode="111")),
            (unrelated, search_row(unrelated["name"], unrelated["brand"], barcode="222")),
        ]
        plan = {
            "corrected_query": "J'ai besoin d'Advil",
            "search_queries": ["Advil"],
            "keywords": ["Advil", "ibuprofen"],
            "must_include": ["Advil"],
            "exclude": [],
        }
        with patch("routes.products.get_db", return_value=object()), \
             patch("routes.products._products_corpus", return_value=corpus), \
             patch("routes.products._reference_corpus", return_value=[]):
            matches = hybrid_client_candidates("Jai besoin dadvile", plan, limit=10)

        self.assertEqual([item["name"] for item in matches], ["Advil Extra Fort"])

    def test_duplicate_plan_positions_are_one_product_with_all_locations(self):
        first = {
            "id": 1, "name": "Advil", "brand": "Advil", "barcode": "111",
            "aisle": "2", "side": "Gauche", "section": "1", "shelf": "2", "position": "1",
        }
        second = {
            **first, "id": 2, "aisle": "5", "side": "Droite", "shelf": "3", "position": "4",
        }
        corpus = [
            (first, search_row(first["name"], first["brand"], barcode="111")),
            (second, search_row(second["name"], second["brand"], barcode="111")),
        ]
        plan = {
            "corrected_query": "Advil", "search_queries": ["Advil"],
            "keywords": ["Advil"], "must_include": ["Advil"], "exclude": [],
        }
        with patch("routes.products.get_db", return_value=object()), \
             patch("routes.products._products_corpus", return_value=corpus):
            matches = hybrid_client_candidates("Advil", plan, limit=10)

        self.assertEqual(len(matches), 1)
        self.assertEqual(len(matches[0]["locations"]), 2)

    def test_product_can_reuse_reference_catalogue_image_by_upc(self):
        class Result:
            def __init__(self, row):
                self.row = row

            def fetchone(self):
                return self.row

        class FakeDb:
            def __init__(self):
                self.calls = 0

            def execute(self, _query, _params):
                self.calls += 1
                return Result(None if self.calls == 1 else {"image_url": "https://example.test/advil.jpg"})

        image = find_existing_image_for_barcode(FakeDb(), "12345678")
        self.assertEqual(image, "https://example.test/advil.jpg")

    def test_assortment_retrieval_keeps_matching_flavours_only(self):
        strawberry = {"id": 1, "name": "Milk Strawberry", "brand": "Test", "barcode": "111"}
        banana = {"id": 2, "name": "Milk Banana", "brand": "Test", "barcode": "222"}
        medicine = {"id": 3, "name": "Advil", "brand": "Advil", "barcode": "333"}
        corpus = [
            (strawberry, search_row(strawberry["name"], description="fruit flavoured milk", barcode="111")),
            (banana, search_row(banana["name"], description="fruit flavoured milk", barcode="222")),
            (medicine, search_row(medicine["name"], medicine["brand"], barcode="333")),
        ]
        plan = {
            "corrected_query": "all fruit flavoured milk",
            "search_queries": ["fruit flavoured milk", "milk strawberry", "milk banana"],
            "keywords": ["milk", "fruit", "strawberry", "banana"],
            "must_include": ["milk", "fruit"],
            "exclude": [],
        }
        with patch("routes.products.get_db", return_value=object()), \
             patch("routes.products._products_corpus", return_value=corpus), \
             patch("routes.products._reference_corpus", return_value=[]):
            matches = hybrid_client_candidates("Give all the fruit flavor milk", plan, limit=10)

        self.assertEqual({item["name"] for item in matches}, {"Milk Strawberry", "Milk Banana"})

    def test_verifier_can_only_return_real_candidate_ids(self):
        parsed = {
            "answer": "Advil",
            "selected_product_ids": ["product:1", "invented:99", "product:1"],
            "follow_up_questions": [],
            "safety_flags": [],
            "pharmacist_referral": False,
            "pharmacist_reason": "",
        }
        result = normalize_verified_client_answer(parsed, ["product:1"])
        self.assertEqual(result["selected_product_ids"], ["product:1"])

    def test_request_router_separates_fast_lookup_from_detailed_advice(self):
        self.assertEqual(classify_client_request("Advil"), "lookup")
        self.assertEqual(classify_client_request("Montre-moi les Advil"), "lookup")
        self.assertEqual(
            classify_client_request("Quelle est la différence entre liquide et comprimés?"),
            "detailed",
        )
        self.assertEqual(classify_client_request("Que prendre pour la fièvre?"), "detailed")
        self.assertEqual(classify_client_request("Quel Advil pour enfant?"), "detailed")
        self.assertEqual(classify_client_request("Et le liquide?", follow_up=True), "detailed")

    def test_client_retrieval_never_uses_reference_catalogue(self):
        placed = {"id": 1, "name": "Tylenol", "brand": "Tylenol", "barcode": "111"}
        corpus = [(placed, search_row(placed["name"], placed["brand"], barcode="111"))]
        plan = {
            "corrected_query": "Advil", "search_queries": ["Advil"],
            "keywords": ["Advil"], "must_include": ["Advil"], "exclude": [],
        }
        reference_mock = patch("routes.products._reference_corpus")
        with patch("routes.products.get_db", return_value=object()), \
             patch("routes.products._products_corpus", return_value=corpus), \
             reference_mock as reference:
            matches = hybrid_client_candidates("Advil", plan, limit=10)

        self.assertEqual(matches, [])
        reference.assert_not_called()

    def test_client_endpoint_returns_all_plan_matches_and_separate_highlights(self):
        candidate = {
            "id": 1, "client_id": "product:1", "name": "Advil", "brand": "Advil",
            "barcode": "111", "aisle": "2", "side": "Gauche", "section": "1",
            "shelf": "3", "position": "2", "in_stock": 1,
        }
        second_candidate = {
            "id": 2, "client_id": "product:2", "name": "Advil Enfants", "brand": "Advil",
            "barcode": "222", "aisle": "2", "side": "Gauche", "section": "1",
            "shelf": "3", "position": "3", "in_stock": 1,
        }
        plan = {
            "intent": "specific_product", "corrected_query": "Advil",
            "search_queries": ["Advil"], "keywords": ["Advil"],
            "must_include": ["Advil"], "exclude": [], "wants_all": False,
            "needs_comparison": False, "answer_language": "fr", "medical": True,
        }
        verified = {
            "answer": "Advil est le produit trouve.",
            "selected_product_ids": ["product:1"],
            "follow_up_questions": [], "safety_flags": [],
            "pharmacist_referral": False, "pharmacist_reason": "",
        }
        with patch("routes.ai.configured_ai_provider", return_value={"name": "deepseek", "label": "DeepSeek", "model": "test"}), \
             patch("routes.ai._check_ai_rate_limit", return_value=True), \
             patch("routes.ai.generate_client_query_plan") as old_planner, \
             patch("routes.products.hybrid_client_candidates", return_value=[candidate, second_candidate]), \
             patch("routes.products.hydrate_candidate_images"), \
             patch("routes.ai.generate_verified_client_answer", return_value=verified) as verifier, \
             patch("routes.ai.log_ai_interaction"):
            with app.test_client() as client:
                response = client.post("/api/client/help", json={
                    "question": "Et pour les enfants?",
                    "history": [{"role": "user", "content": "Jai besoin dadvile"}],
                    "follow_up": True,
                })

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["success"])
        self.assertEqual(payload["response_mode"], "detailed")
        self.assertEqual(
            [item["client_id"] for item in payload["products"]],
            ["product:1", "product:2"],
        )
        self.assertEqual(payload["highlighted_product_ids"], ["product:1"])
        old_planner.assert_not_called()
        verifier.assert_called_once()

    def test_simple_product_lookup_does_not_call_ai(self):
        candidate = {
            "id": 1, "client_id": "product:1", "name": "Advil", "brand": "Advil",
            "barcode": "111", "aisle": "2", "side": "Gauche", "section": "1",
            "shelf": "3", "position": "2", "in_stock": 1,
        }
        with patch("routes.products.hybrid_client_candidates", return_value=[candidate]), \
             patch("routes.products.hydrate_candidate_images"), \
             patch("routes.ai.configured_ai_provider") as provider, \
             patch("routes.ai.generate_verified_client_answer") as verifier:
            with app.test_client() as client:
                response = client.post("/api/client/help", json={"question": "Advil"})

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["response_mode"], "lookup")
        self.assertEqual(payload["products"][0]["name"], "Advil")
        provider.assert_not_called()
        verifier.assert_not_called()


if __name__ == "__main__":
    unittest.main()
