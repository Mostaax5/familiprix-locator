import unittest
from unittest.mock import patch

from app import app
from routes.ai import (
    classify_client_request,
    normalize_documented_client_answer,
    normalize_verified_client_answer,
    select_client_answer_candidates,
)
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

    def test_fast_client_lookup_handles_spoken_brand_typo(self):
        advil = {"id": 1, "name": "Advil Extra Fort", "brand": "Advil", "barcode": "111"}
        unrelated = {"id": 2, "name": "Tylenol Regular", "brand": "Tylenol", "barcode": "222"}
        corpus = [
            (advil, search_row(advil["name"], advil["brand"], barcode="111")),
            (unrelated, search_row(unrelated["name"], unrelated["brand"], barcode="222")),
        ]
        with patch("routes.products.get_db", return_value=object()), \
             patch("routes.products._products_corpus", return_value=corpus):
            with app.test_client() as client:
                response = client.get("/api/client/find?q=Jai%20besoin%20dadvile")

        self.assertEqual(response.status_code, 200)
        self.assertEqual([item["name"] for item in response.get_json()], ["Advil Extra Fort"])

    def test_fast_client_lookup_can_return_more_than_sixty_matches(self):
        corpus = []
        for index in range(75):
            product = {
                "id": index + 1,
                "name": f"Advil variante {index + 1}",
                "brand": "Advil",
                "barcode": str(100000 + index),
            }
            corpus.append((
                product,
                search_row(product["name"], product["brand"], barcode=product["barcode"]),
            ))
        with patch("routes.products.get_db", return_value=object()), \
             patch("routes.products._products_corpus", return_value=corpus):
            with app.test_client() as client:
                response = client.get("/api/client/find?q=Advil&limit=100")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.get_json()), 75)

    def test_fast_client_lookup_requires_an_electric_toothbrush_match(self):
        products = [
            {"id": 1, "name": "ORAL-B P100 BR/DENTS ELEC NR 1", "brand": "Oral-B", "barcode": "101"},
            {"id": 2, "name": "ORAL-B D/C BR/DENTS A PILE 1", "brand": "Oral-B", "barcode": "102"},
            {"id": 3, "name": "SONICARE BR/DENT BH1022/04 TE2", "brand": "Sonicare", "barcode": "103"},
            {"id": 4, "name": "SONICARE RECH BROS HX9023/64 3", "brand": "Sonicare", "barcode": "104"},
            {"id": 5, "name": "ORAL-B IO TETE BR/DENTS BLC 4", "brand": "Oral-B", "barcode": "105"},
            {"id": 6, "name": "GUM BR/DENT CRAYOLA MARQ/ELEC1", "brand": "Gum", "barcode": "106"},
            {"id": 11, "name": "PHILIPS SONI RECH HX6012/77 2", "brand": "Philips", "barcode": "111",
             "description": "Compatible avec les manches Sonicare."},
            {"id": 7, "name": "CURAPROX BR/DENT SMART 1", "brand": "Curaprox", "barcode": "107"},
            {"id": 8, "name": "DENTA RINSE PRO .2% MENT 500ML", "brand": "Denta", "barcode": "108"},
            {"id": 9, "name": "SONICARE IRR S/FIL HX3826/23 1", "brand": "Sonicare", "barcode": "109",
             "description": "Utilisé en complément d'une brosse à dents manuelle."},
            {"id": 10, "name": "GUM PROXABRUSH RECH BROS LG 10", "brand": "Gum", "barcode": "110"},
        ]
        corpus = [
            (product, search_row(
                product["name"], product["brand"], product.get("description", ""),
                barcode=product["barcode"],
            ))
            for product in products
        ]
        with patch("routes.products.get_db", return_value=object()), \
             patch("routes.products._products_corpus", return_value=corpus):
            with app.test_client() as client:
                response = client.get("/api/client/find?q=brosse%20a%20dent%20electric&limit=100")

        self.assertEqual(response.status_code, 200)
        names = {item["name"] for item in response.get_json()}
        self.assertEqual(names, {product["name"] for product in products[:7]})
        self.assertNotIn("CURAPROX BR/DENT SMART 1", names)
        self.assertNotIn("DENTA RINSE PRO .2% MENT 500ML", names)
        self.assertNotIn("SONICARE IRR S/FIL HX3826/23 1", names)
        self.assertNotIn("GUM PROXABRUSH RECH BROS LG 10", names)

    def test_fast_client_lookup_understands_transparent_dressing_language(self):
        transparent = {
            "id": 1, "name": "PARAMEDIC PANS TRANSP 5CMX1M", "brand": "Paramedic",
            "barcode": "111",
        }
        unrelated = {"id": 2, "name": "BANDAGE ELASTIQUE", "brand": "Test", "barcode": "222"}
        nasal_strip = {
            "id": 3, "name": "BREATHE RIGHT BDE NAS TRANSP 30", "brand": "Breathe Right",
            "barcode": "333",
        }
        cup = {"id": 4, "name": "TASSE TRANSPARENTE", "brand": "Test", "barcode": "444"}
        corpus = [
            (transparent, search_row(transparent["name"], transparent["brand"], barcode="111")),
            (unrelated, search_row(unrelated["name"], unrelated["brand"], barcode="222")),
            (nasal_strip, search_row(nasal_strip["name"], nasal_strip["brand"], barcode="333")),
            (cup, search_row(cup["name"], cup["brand"], barcode="444")),
        ]
        with patch("routes.products.get_db", return_value=object()), \
             patch("routes.products._products_corpus", return_value=corpus):
            with app.test_client() as client:
                response = client.get(
                    "/api/client/find?q=membrane%20transparente%20pour%20blessure"
                )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            [item["name"] for item in response.get_json()],
            ["PARAMEDIC PANS TRANSP 5CMX1M"],
        )

    def test_fast_client_lookup_understands_watte_as_cotton_balls(self):
        cotton = {
            "id": 1, "name": "PERSONNEL OUATE BOULES 100", "brand": "Personnelle",
            "barcode": "111",
        }
        unrelated = {"id": 2, "name": "MOUCHOIRS 100", "brand": "Test", "barcode": "222"}
        cotton_swabs = {"id": 3, "name": "Q-TIPS COTONS-TIGES 400", "brand": "Q-Tips", "barcode": "333"}
        laxative = {"id": 4, "name": "CARTER PETITES PILULES LAX", "brand": "Carter", "barcode": "444"}
        bowls = {"id": 5, "name": "MUNCHKIN PETITS BOLS", "brand": "Munchkin", "barcode": "555"}
        corpus = [
            (cotton, search_row(cotton["name"], cotton["brand"], barcode="111")),
            (unrelated, search_row(unrelated["name"], unrelated["brand"], barcode="222")),
            (cotton_swabs, search_row(cotton_swabs["name"], cotton_swabs["brand"], barcode="333")),
            (laxative, search_row(laxative["name"], laxative["brand"], barcode="444")),
            (bowls, search_row(bowls["name"], bowls["brand"], barcode="555")),
        ]
        with patch("routes.products.get_db", return_value=object()), \
             patch("routes.products._products_corpus", return_value=corpus):
            with app.test_client() as client:
                response = client.get(
                    "/api/client/find?q=je%20cherche%20de%20la%20watte%20des%20petites%20boules"
                )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            [item["name"] for item in response.get_json()],
            ["PERSONNEL OUATE BOULES 100"],
        )

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

    def test_documented_answer_rejects_invented_products_and_sources(self):
        documents = [{"source_id": "store-plan"}, {"source_id": "health-canada:12"}]
        parsed = {
            "answer": "Réponse documentée.",
            "selected_product_ids": ["product:1", "invented:9"],
            "key_points": [{
                "heading": "Ingrédient", "detail": "Ibuprofène 200 mg.",
                "source_ids": ["health-canada:12", "invented-source"],
            }],
            "comparisons": [{
                "candidate_id": "invented:9", "difference": "Fausse différence",
                "practical_note": "", "source_ids": ["invented-source"],
            }],
            "useful_guidance": [], "important_checks": [],
            "follow_up_questions": [], "safety_flags": [],
            "pharmacist_referral": False, "pharmacist_reason": "",
            "source_ids": ["health-canada:12", "invented-source"],
        }

        result = normalize_documented_client_answer(parsed, ["product:1"], documents)

        self.assertEqual(result["selected_product_ids"], ["product:1"])
        self.assertEqual(result["key_points"][0]["source_ids"], ["health-canada:12"])
        self.assertEqual(result["comparisons"], [])
        self.assertEqual(result["source_ids"], ["health-canada:12"])

    def test_small_ai_context_keeps_different_product_forms(self):
        candidates = [
            {"id": index, "name": f"Advil 200 mg liqui-gel {index}"}
            for index in range(1, 12)
        ] + [
            {"id": 20, "name": "Advil enfants suspension liquide"},
            {"id": 21, "name": "Advil comprimes 200 mg"},
        ]

        selected = select_client_answer_candidates(candidates, limit=4)

        self.assertIn(20, [item["id"] for item in selected])
        self.assertIn(21, [item["id"] for item in selected])

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

    def test_client_endpoint_returns_only_ai_verified_products(self):
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
        self.assertEqual([item["client_id"] for item in payload["products"]], ["product:1"])
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

    def test_explicit_ai_mode_forces_grounded_answer_for_simple_name(self):
        candidate = {
            "id": 1, "client_id": "product:1", "name": "Advil", "brand": "Advil",
            "barcode": "111", "aisle": "2", "side": "Gauche", "section": "1",
            "shelf": "3", "position": "2", "in_stock": 1,
        }
        verified = {
            "answer": "Advil est disponible dans le plan.",
            "selected_product_ids": ["product:1"],
            "follow_up_questions": [], "safety_flags": [],
            "pharmacist_referral": False, "pharmacist_reason": "",
        }
        with patch("routes.products.hybrid_client_candidates", return_value=[candidate]), \
             patch("routes.products.hydrate_candidate_images"), \
             patch("routes.ai.configured_ai_provider", return_value={"name": "deepseek"}), \
             patch("routes.ai._check_ai_rate_limit", return_value=True), \
             patch("routes.ai.generate_verified_client_answer", return_value=verified) as verifier, \
             patch("routes.ai.log_ai_interaction"):
            with app.test_client() as client:
                response = client.post("/api/client/help", json={
                    "question": "Advil", "mode": "ai",
                })

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["response_mode"], "detailed")
        self.assertEqual([item["client_id"] for item in payload["products"]], ["product:1"])
        verifier.assert_called_once()

    def test_explicit_fast_mode_never_calls_ai_for_detailed_question(self):
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
                response = client.post("/api/client/help", json={
                    "question": "Quelle est la différence?", "mode": "fast",
                })

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["response_mode"], "lookup")
        provider.assert_not_called()
        verifier.assert_not_called()

    def test_explicit_documented_mode_returns_structured_sources(self):
        candidate = {
            "id": 1, "client_id": "product:1", "name": "Advil 200 mg",
            "brand": "Advil", "description": "Comprimés", "barcode": "111",
            "aisle": "2", "side": "Gauche", "section": "1", "shelf": "3",
            "position": "2", "in_stock": 1,
        }
        documents = [{
            "source_id": "health-canada:12", "title": "Santé Canada - ADVIL",
            "publisher": "Santé Canada", "url": "https://example.test/source",
            "evidence": "Ibuprofène 200 mg", "candidate_ids": ["product:1"],
        }]
        documented = {
            "answer": "Ce format contient 200 mg d'ibuprofène.",
            "selected_product_ids": ["product:1"],
            "follow_up_questions": ["Quel âge a la personne?"],
            "safety_flags": [], "pharmacist_referral": False,
            "pharmacist_reason": "", "source_ids": ["health-canada:12"],
            "key_points": [{
                "heading": "Ingrédient", "detail": "Ibuprofène 200 mg",
                "source_ids": ["health-canada:12"],
            }],
            "comparisons": [], "useful_guidance": [], "important_checks": [],
        }
        with patch("routes.products.hybrid_client_candidates", return_value=[candidate]), \
             patch("routes.products.hydrate_candidate_images"), \
             patch("routes.ai.configured_ai_provider", return_value={"name": "deepseek"}), \
             patch("routes.ai._check_ai_rate_limit", return_value=True), \
             patch("routes.ai.retrieve_client_documentation", return_value=documents) as retriever, \
             patch("routes.ai.generate_documented_client_answer", return_value=documented) as generator, \
             patch("routes.ai.log_ai_interaction"):
            with app.test_client() as client:
                response = client.post("/api/client/help", json={
                    "question": "Explique-moi cet Advil en détail", "mode": "documented",
                })

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["response_mode"], "documented")
        self.assertEqual(payload["advice"]["documentation"]["key_points"][0]["heading"], "Ingrédient")
        self.assertEqual(payload["advice"]["documentation"]["sources"][0]["publisher"], "Santé Canada")
        retriever.assert_called_once()
        generator.assert_called_once()


if __name__ == "__main__":
    unittest.main()
